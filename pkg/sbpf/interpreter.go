package sbpf

import (
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
	"github.com/Overclock-Validator/wide"
	"github.com/gagliardetto/solana-go"
	//"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// Interpreter implements the SBF core in pure Go.
type Interpreter struct {
	textVA         uint64
	textBytes      []byte
	text           []Slot
	ro             []byte
	stack          Stack
	heap           []byte
	input          []byte
	inputRegions   []InputRegion
	inputDataVaddr uint64

	entry    uint64
	heapSize uint64

	syscalls          func(uint32) (Syscall, bool)
	funcs             map[uint32]int64
	vmContext         any
	trace             TraceSink
	enableTracing     bool
	computeMeter      *cu.ComputeMeter
	dueInstrCount     uint64
	prevInstrMeter    uint64
	initialInstrMeter uint64
	sbpfVersion       sbpfver.SbpfVersion
	programId         solana.PublicKey
	txSignature       solana.Signature
}

type TraceSink interface {
	Printf(format string, v ...any)
}

func newHeap() []byte {
	return make([]byte /*sealevel.MaxHeapFrameBytes=*/, 256*1024)
}

var (
	heapPool = &sync.Pool{
		New: func() interface{} {
			return newHeap()
		},
	}
)

// NewInterpreter creates a new interpreter instance for a program execution.
//
// The caller must create a new interpreter object for every new execution.
// In other words, Run may only be called once per interpreter.
func NewInterpreter(p *Program, opts *VMOpts) *Interpreter {
	var heap []byte
	if UsePool {
		heap = heapPool.Get().([]byte)
		if len(heap) < opts.HeapMax {
			heap = slices.Grow(heap, opts.HeapMax-len(heap))
		}
		heap = heap[:opts.HeapMax]
		clear(heap)
	} else {
		heap = newHeap()
	}

	return &Interpreter{
		textVA:            p.TextVA,
		textBytes:         p.TextBytes,
		text:              p.Text,
		ro:                p.RO,
		stack:             NewStack(p.SbpfVersion, opts.DisableStackFrameGaps),
		heap:              heap,
		input:             opts.Input,
		inputRegions:      opts.InputRegions,
		inputDataVaddr:    opts.InputDataVaddr,
		entry:             p.Entrypoint,
		syscalls:          opts.Syscalls,
		funcs:             p.Funcs,
		vmContext:         opts.Context,
		trace:             opts.Tracer,
		computeMeter:      opts.ComputeMeter,
		prevInstrMeter:    opts.ComputeMeter.Remaining(),
		initialInstrMeter: opts.ComputeMeter.Remaining(),
		enableTracing:     opts.EnableTracing,
		sbpfVersion:       p.SbpfVersion,
		programId:         opts.ProgramId,
		txSignature:       opts.TxSignature,
	}
}

func (ip *Interpreter) Finish() {
	if UsePool {
		heapPool.Put(ip.heap)
	}
	ip.stack.Finish()
}

func (ip *Interpreter) executeJmp32(ins Slot, pc int64, r *[11]uint64) (int64, error) {
	var taken bool
	dst := uint32(r[ins.Dst()])
	src := uint32(r[ins.Src()])
	imm := ins.Uimm()

	switch ins.Op() & 0xf0 {
	case JumpEq:
		if ins.Op()&SrcX != 0 {
			taken = dst == src
		} else {
			taken = dst == imm
		}
	case JumpGt:
		if ins.Op()&SrcX != 0 {
			taken = dst > src
		} else {
			taken = dst > imm
		}
	case JumpGe:
		if ins.Op()&SrcX != 0 {
			taken = dst >= src
		} else {
			taken = dst >= imm
		}
	case JumpLt:
		if ins.Op()&SrcX != 0 {
			taken = dst < src
		} else {
			taken = dst < imm
		}
	case JumpLe:
		if ins.Op()&SrcX != 0 {
			taken = dst <= src
		} else {
			taken = dst <= imm
		}
	case JumpSet:
		if ins.Op()&SrcX != 0 {
			taken = dst&src != 0
		} else {
			taken = dst&imm != 0
		}
	case JumpNe:
		if ins.Op()&SrcX != 0 {
			taken = dst != src
		} else {
			taken = dst != imm
		}
	case JumpSgt:
		if ins.Op()&SrcX != 0 {
			taken = int32(dst) > int32(src)
		} else {
			taken = int32(dst) > ins.Imm()
		}
	case JumpSge:
		if ins.Op()&SrcX != 0 {
			taken = int32(dst) >= int32(src)
		} else {
			taken = int32(dst) >= ins.Imm()
		}
	case JumpSlt:
		if ins.Op()&SrcX != 0 {
			taken = int32(dst) < int32(src)
		} else {
			taken = int32(dst) < ins.Imm()
		}
	case JumpSle:
		if ins.Op()&SrcX != 0 {
			taken = int32(dst) <= int32(src)
		} else {
			taken = int32(dst) <= ins.Imm()
		}
	default:
		return pc, ExcUnsupportedInstruction
	}

	if taken {
		pc += int64(ins.Off())
	}
	return pc + 1, nil
}

// Run executes the program.
//
// This function may panic given code that doesn't pass the static verifier.
func (ip *Interpreter) Run() (ret uint64, cuConsumed uint64, err error) {
	if sbpfProfileEnabled {
		defer profileRunAt(profileRunEnter(), profileNow())
	}
	var r [11]uint64
	r[1] = VaddrInput
	r[2] = ip.inputDataVaddr

	// initialise fp
	var sz uint64
	if ip.sbpfVersion.DynamicStackFrames() {
		sz = StackMax
	} else {
		sz = StackFrameSize
	}
	r[10] = VaddrStack + sz

	// initialize pc to program entry point
	pc := int64(ip.entry)

mainLoop:
	for i := 0; true; i++ {
		// Charge before fetching, matching Agave solana-sbpf 0.21.1
		// interpreter.rs:188-193, which tests the instruction meter and only
		// then bounds-checks pc. Two observables depend on this order:
		//
		//   - the instruction that runs off the end of the text section is
		//     still charged. Agave increments due_insn_count at :191 and
		//     throws ExecutionOverrun at :193, so it bills that step;
		//   - an exhausted meter takes precedence over the overrun.
		//
		// Checking pc first billed one CU less for every overrun, which left
		// later instructions in the same transaction a larger budget than
		// mainnet gave them. Neither verifier requires the last instruction to
		// be exit -- Agave's only rejects a truncated lddw (verifier.rs:416) --
		// so a one-instruction program reaches this.
		// ConsumeOne rather than Consume(1) so this inlines. Run is a "big"
		// function in the inliner's model, which caps callees at cost 20;
		// Consume is 22, so this call was real, and it spilled pc and the
		// instruction counter around itself every iteration. See ConsumeOne's
		// comment for why the two are equivalent.
		err = ip.computeMeter.ConsumeOne()
		if err != nil {
			break mainLoop
		}

		// Fetch
		if pc < 0 || pc >= int64(len(ip.text)) {
			return 0, 0, &Exception{
				PC:     pc,
				Detail: fmt.Errorf("tx: %s, programId: %s - %w:", ip.txSignature, ip.programId, ExcExecutionOverrun),
			}
		}
		ins := ip.getSlot(pc)
		if sbpfProfileEnabled {
			profileInstruction(ins.Op())
		}
		if ip.enableTracing {
			regsDump := fmt.Sprintf("%016x, %016x, %016x, %016x, %016x, %016x, %016x, %016x, %016x, %016x, %016x",
				r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10])
			fmt.Printf("% 5d [%s]: %s\n",
				i, strings.ToUpper(regsDump), ip.disassemble(ins, 0))
		}

		// Execute
		if ip.sbpfVersion.EnableJmp32() && ins.Op()&0x07 == ClassPqr {
			pc, err = ip.executeJmp32(ins, pc, &r)
			goto postExecute
		}
		switch ins.Op() {
		case OpLdxb:
			if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
			var v uint8
			v, err = ip.Read8(vma)
			r[ins.Dst()] = uint64(v)
			pc++
		case OpLdxh:
			if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
			var v uint16
			v, err = ip.Read16(vma)
			r[ins.Dst()] = uint64(v)
			pc++
		case OpLdxw:
			if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
			var v uint32
			v, err = ip.Read32(vma)
			r[ins.Dst()] = uint64(v)
			pc++
		case OpLdxdw:
			if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
			var v uint64
			v, err = ip.Read64(vma)
			r[ins.Dst()] = v
			pc++
		case OpStb:
			if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			err = ip.Write8(vma, uint8(ins.Uimm()))
			pc++
		case OpSth:
			if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			err = ip.Write16(vma, uint16(ins.Uimm()))
			pc++
		case OpStw:
			if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			err = ip.Write32(vma, ins.Uimm())
			pc++
		case OpStdw:
			if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			err = ip.Write64(vma, uint64(ins.Imm()))
			pc++
		case OpStxb:
			if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			err = ip.Write8(vma, uint8(r[ins.Src()]))
			pc++
		case OpStxh:
			if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			err = ip.Write16(vma, uint16(r[ins.Src()]))
			pc++
		case OpStxw:
			if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			err = ip.Write32(vma, uint32(r[ins.Src()]))
			pc++
		case OpStxdw:
			if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			err = ip.Write64(vma, r[ins.Src()])
			pc++
		case OpAdd32Imm:
			r[ins.Dst()] = ip.signExtension(int32(r[ins.Dst()]) + ins.Imm())
			pc++
		case OpAdd32Reg:
			r[ins.Dst()] = ip.signExtension(int32(r[ins.Dst()]) + int32(r[ins.Src()]))
			pc++
		case OpAdd64Imm:
			r[ins.Dst()] += uint64(ins.Imm())
			pc++
		case OpAdd64Reg:
			r[ins.Dst()] += r[ins.Src()]
			pc++
		case OpSub32Imm:
			if ip.sbpfVersion.SwapSubRegImmOperands() {
				r[ins.Dst()] = ip.signExtension(ins.Imm() - int32(r[ins.Dst()]))
			} else {
				r[ins.Dst()] = ip.signExtension(int32(r[ins.Dst()]) - ins.Imm())
			}
			pc++
		case OpSub32Reg:
			r[ins.Dst()] = ip.signExtension(int32(r[ins.Dst()]) - int32(r[ins.Src()]))
			pc++
		case OpSub64Imm:
			if ip.sbpfVersion.SwapSubRegImmOperands() {
				r[ins.Dst()] = uint64(int64(ins.Imm())) - r[ins.Dst()]
			} else {
				r[ins.Dst()] -= uint64(int64(ins.Imm()))
			}
			pc++
		case OpSub64Reg:
			r[ins.Dst()] -= r[ins.Src()]
			pc++
		case OpMul32Imm:
			r[ins.Dst()] = uint64(int32(r[ins.Dst()]) * ins.Imm())
			pc++
		case OpMul32Reg:
			if !ip.sbpfVersion.EnablePqr() {
				r[ins.Dst()] = uint64(int32(r[ins.Dst()]) * int32(r[ins.Src()]))
				pc++
			} else if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				// OpLd1BReg
				vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
				var v uint8
				v, err = ip.Read8(vma)
				r[ins.Dst()] = uint64(v)
				pc++
			}
		case OpMul64Imm:
			if !ip.sbpfVersion.EnablePqr() {
				r[ins.Dst()] *= uint64(ins.Imm())
				pc++
			} else if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				// OpSt1BImm
				vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
				err = ip.Write8(vma, uint8(ins.Uimm()))
				pc++
			}
		case OpMul64Reg:
			if !ip.sbpfVersion.EnablePqr() {
				r[ins.Dst()] *= r[ins.Src()]
				pc++
			} else if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				// OpSt1BReg
				vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
				err = ip.Write8(vma, uint8(r[ins.Src()]))
				pc++
			}
		case OpDiv32Imm:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) / ins.Uimm())
			pc++
		case OpDiv32Reg:
			if !ip.sbpfVersion.EnablePqr() {
				if src := uint32(r[ins.Src()]); src != 0 {
					r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) / src)
				} else {
					err = ExcDivideByZero
				}
				pc++
			} else if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				// OpLd2BReg
				vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
				var v uint16
				v, err = ip.Read16(vma)
				r[ins.Dst()] = uint64(v)
				pc++
			}
		case OpLd4BReg:
			if !ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
			var v uint32
			v, err = ip.Read32(vma)
			r[ins.Dst()] = uint64(v)
			pc++
		case OpDiv64Imm:
			if !ip.sbpfVersion.EnablePqr() {
				r[ins.Dst()] /= uint64(ins.Imm())
				pc++
			} else if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				// OpSt2BImm
				vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
				err = ip.Write16(vma, uint16(ins.Uimm()))
				pc++
			}
		case OpDiv64Reg:
			if !ip.sbpfVersion.EnablePqr() {
				if src := r[ins.Src()]; src != 0 {
					r[ins.Dst()] /= src
				} else {
					err = ExcDivideByZero
				}
				pc++
			} else if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				// OpSt2BReg
				vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
				err = ip.Write16(vma, uint16(r[ins.Src()]))
				pc++
			}
		case OpSt4BReg:
			if !ip.sbpfVersion.MoveMemoryInstructionClasses() {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			err = ip.Write32(vma, uint32(r[ins.Src()]))
			pc++
		case OpLmul32Imm:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) * ins.Uimm())
			pc++
		case OpLmul32Reg:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) * uint32(r[ins.Src()]))
			pc++
		case OpLmul64Imm:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			r[ins.Dst()] *= uint64(int64(ins.Imm()))
			pc++
		case OpLmul64Reg:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			r[ins.Dst()] *= r[ins.Src()]
			pc++
		case OpUhmul64Imm:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			dst128 := wide.Uint128FromUint64(r[ins.Dst()])
			imm128 := wide.Uint128FromUint64(uint64(ins.Uimm()))
			r[ins.Dst()] = dst128.Mul(imm128).RShiftN(64).Uint64()
			pc++
		case OpUhmul64Reg:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			dst128 := wide.Uint128FromUint64(r[ins.Dst()])
			regSrc128 := wide.Uint128FromUint64(r[ins.Src()])
			r[ins.Dst()] = dst128.Mul(regSrc128).RShiftN(64).Uint64()
			pc++
		case OpShmul64Imm:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			dst128 := wide.Int128FromInt64(int64(r[ins.Dst()]))
			imm128 := wide.Int128FromInt64(int64(ins.Imm()))
			r[ins.Dst()] = dst128.Mul(imm128).Uint128().RShiftN(64).Uint64()
			pc++
		case OpShmul64Reg:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			dst128 := wide.Int128FromInt64(int64(r[ins.Dst()]))
			src128 := wide.Int128FromInt64(int64(r[ins.Src()]))
			r[ins.Dst()] = dst128.Mul(src128).Uint128().RShiftN(64).Uint64()
			pc++
		case OpUdiv32Imm:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) / ins.Uimm())
			pc++
		case OpUdiv32Reg:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			if src := uint32(r[ins.Src()]); src != 0 {
				r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) / src)
			} else {
				err = ExcDivideByZero
			}
			pc++
		case OpUdiv64Imm:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			r[ins.Dst()] /= uint64(ins.Uimm())
			pc++
		case OpUdiv64Reg:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			if src := r[ins.Src()]; src != 0 {
				r[ins.Dst()] /= src
			} else {
				err = ExcDivideByZero
			}
			pc++
		case OpUrem32Imm:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) % ins.Uimm())
			pc++
		case OpUrem32Reg:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			if src := r[ins.Src()]; src != 0 {
				r[ins.Dst()] = uint64(r[ins.Dst()] % src)
			} else {
				err = ExcDivideByZero
			}
			pc++
		case OpUrem64Imm:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			r[ins.Dst()] %= uint64(ins.Uimm())
			pc++
		case OpUrem64Reg:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			if src := r[ins.Src()]; src != 0 {
				r[ins.Dst()] %= r[ins.Src()]
			} else {
				err = ExcDivideByZero
			}
			pc++
		case OpSdiv32Imm:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			if int32(r[ins.Dst()]) == math.MinInt32 && ins.Imm() == -1 {
				err = ExcDivideOverflow
				break
			}
			r[ins.Dst()] = uint64(uint32(int32(r[ins.Dst()]) / ins.Imm()))
			pc++
		case OpSdiv32Reg:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			if src := int32(r[ins.Src()]); src != 0 {
				if int32(r[ins.Dst()]) == math.MinInt32 && src == -1 {
					err = ExcDivideOverflow
					break
				}
				r[ins.Dst()] = uint64(uint32(int32(r[ins.Dst()]) / src))
			} else {
				err = ExcDivideByZero
				break
			}
			pc++
		case OpSdiv64Imm:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			if int64(r[ins.Dst()]) == math.MinInt64 && ins.Imm() == -1 {
				err = ExcDivideOverflow
				break
			}
			r[ins.Dst()] = uint64(int64(r[ins.Dst()]) / int64(ins.Imm()))
			pc++
		case OpSdiv64Reg:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			if src := int64(r[ins.Src()]); src != 0 {
				if int64(r[ins.Dst()]) == math.MinInt64 && src == -1 {
					err = ExcDivideOverflow
					break
				}
				r[ins.Dst()] = uint64(int64(r[ins.Dst()]) / src)
			} else {
				err = ExcDivideByZero
				break
			}
			pc++
		case OpSrem32Imm:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			if int32(r[ins.Dst()]) == math.MinInt32 && ins.Imm() == -1 {
				err = ExcDivideOverflow
				break
			}
			r[ins.Dst()] = uint64(uint32(int32(r[ins.Dst()]) % ins.Imm()))
			pc++
		case OpSrem32Reg:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			if src := int32(r[ins.Src()]); src != 0 {
				if int32(r[ins.Dst()]) == math.MinInt32 && src == -1 {
					err = ExcDivideOverflow
					break
				}
				r[ins.Dst()] = uint64(uint32(int32(r[ins.Dst()]) % int32(r[ins.Src()])))
			} else {
				err = ExcDivideByZero
				break
			}
			pc++
		case OpSrem64Imm:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			if int64(r[ins.Dst()]) == math.MinInt64 && ins.Imm() == -1 {
				err = ExcDivideOverflow
				break
			}
			r[ins.Dst()] = uint64(int64(r[ins.Dst()]) % int64(ins.Imm()))
			pc++
		case OpSrem64Reg:
			if !ip.sbpfVersion.EnablePqr() {
				err = ExcInvalidInstr
				break
			}
			if src := int64(r[ins.Src()]); src != 0 {
				if int64(r[ins.Dst()]) == math.MinInt64 && src == -1 {
					err = ExcDivideOverflow
					break
				}
				r[ins.Dst()] = uint64(int64(r[ins.Dst()]) % int64(r[ins.Src()]))
			} else {
				err = ExcDivideByZero
				break
			}
			pc++
		case OpOr32Imm:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) | ins.Uimm())
			pc++
		case OpOr32Reg:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) | uint32(r[ins.Src()]))
			pc++
		case OpOr64Imm:
			r[ins.Dst()] |= uint64(ins.Imm())
			pc++
		case OpOr64Reg:
			r[ins.Dst()] |= r[ins.Src()]
			pc++
		case OpAnd32Imm:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) & ins.Uimm())
			pc++
		case OpAnd32Reg:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) & uint32(r[ins.Src()]))
			pc++
		case OpAnd64Imm:
			r[ins.Dst()] &= uint64(ins.Imm())
			pc++
		case OpAnd64Reg:
			r[ins.Dst()] &= r[ins.Src()]
			pc++
		case OpLsh32Imm:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) << (ins.Uimm() & 0x1f))
			pc++
		case OpLsh32Reg:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) << (uint32(r[ins.Src()]) & 0x1f))
			pc++
		case OpLsh64Imm:
			r[ins.Dst()] <<= uint64(ins.Imm()) & 0x3f
			pc++
		case OpLsh64Reg:
			r[ins.Dst()] <<= r[ins.Src()] & 0x3f
			pc++
		case OpRsh32Imm:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) >> (ins.Uimm() & 0x1f))
			pc++
		case OpRsh32Reg:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) >> (uint32(r[ins.Src()]) & 0x1f))
			pc++
		case OpRsh64Imm:
			r[ins.Dst()] >>= uint64(ins.Imm()) & 0x3f
			pc++
		case OpRsh64Reg:
			r[ins.Dst()] >>= r[ins.Src()] & 0x3f
			pc++
		case OpNeg32:
			if ip.sbpfVersion.DisableNeg() {
				err = ExcInvalidInstr
				break
			}
			r[ins.Dst()] = uint64(-int32(r[ins.Dst()]))
			pc++
		case OpNeg64:
			if !ip.sbpfVersion.DisableNeg() {
				r[ins.Dst()] = uint64(-int64(r[ins.Dst()]))
				pc++
			} else if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				// OpSt4BImm
				vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
				err = ip.Write32(vma, ins.Uimm())
				pc++
			}
		case OpMod32Imm:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) % ins.Uimm())
			pc++
		case OpMod32Reg:
			if !ip.sbpfVersion.EnablePqr() {
				if src := uint32(r[ins.Src()]); src != 0 {
					r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) % src)
				} else {
					err = ExcDivideByZero
				}
				pc++
			} else if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				// OpLd8BReg
				vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
				var v uint64
				v, err = ip.Read64(vma)
				r[ins.Dst()] = v
				pc++
			}
		case OpMod64Imm:
			if !ip.sbpfVersion.EnablePqr() {
				r[ins.Dst()] %= uint64(ins.Imm())
				pc++
			} else if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				// OpSt8BImm
				vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
				err = ip.Write64(vma, uint64(ins.Imm()))
				pc++
			}
		case OpMod64Reg:
			if !ip.sbpfVersion.EnablePqr() {
				if src := r[ins.Src()]; src != 0 {
					r[ins.Dst()] %= src
				} else {
					err = ExcDivideByZero
				}
				pc++
			} else if ip.sbpfVersion.MoveMemoryInstructionClasses() {
				// OpSt8BReg
				vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
				err = ip.Write64(vma, r[ins.Src()])
				pc++
			}
		case OpXor32Imm:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) ^ ins.Uimm())
			pc++
		case OpXor32Reg:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) ^ uint32(r[ins.Src()]))
			pc++
		case OpXor64Imm:
			r[ins.Dst()] ^= uint64(ins.Imm())
			pc++
		case OpXor64Reg:
			r[ins.Dst()] ^= r[ins.Src()]
			pc++
		case OpMov32Imm:
			r[ins.Dst()] = uint64(ins.Uimm())
			pc++
		case OpMov32Reg:
			if ip.sbpfVersion.ExplicitSignExtensionOfResults() {
				r[ins.Dst()] = uint64(int64(int32(r[ins.Src()])))
			} else {
				r[ins.Dst()] = uint64(uint32(r[ins.Src()]))
			}
			pc++
		case OpMov64Imm:
			r[ins.Dst()] = uint64(ins.Imm())
			pc++
		case OpMov64Reg:
			r[ins.Dst()] = r[ins.Src()]
			pc++
		case OpArsh32Imm:
			r[ins.Dst()] = uint64(uint32(int32(r[ins.Dst()]) >> ins.Uimm()))
			pc++
		case OpArsh32Reg:
			r[ins.Dst()] = uint64(uint32(int32(r[ins.Dst()]) >> uint32(r[ins.Src()])))
			pc++
		case OpArsh64Imm:
			r[ins.Dst()] = uint64(int64(r[ins.Dst()]) >> ins.Imm())
			pc++
		case OpArsh64Reg:
			r[ins.Dst()] = uint64(int64(r[ins.Dst()]) >> (r[ins.Src()]))
			pc++
		case OpHor64Imm:
			if !ip.sbpfVersion.DisableLddw() {
				err = ExcInvalidInstr
				break
			}
			r[ins.Dst()] |= uint64(ins.Uimm()) << 32
			pc++
		case OpLe:
			if ip.sbpfVersion.DisableLe() {
				err = ExcInvalidInstr
				break
			}
			switch ins.Uimm() {
			case 16:
				r[ins.Dst()] &= math.MaxUint16
			case 32:
				r[ins.Dst()] &= math.MaxUint32
			case 64:
				r[ins.Dst()] &= math.MaxUint64
			default:
				err = ExcUnsupportedInstruction
			}
			pc++
		case OpBe:
			switch ins.Uimm() {
			case 16:
				r[ins.Dst()] = uint64(bits.ReverseBytes16(uint16(r[ins.Dst()])))
			case 32:
				r[ins.Dst()] = uint64(bits.ReverseBytes32(uint32(r[ins.Dst()])))
			case 64:
				r[ins.Dst()] = bits.ReverseBytes64(r[ins.Dst()])
			default:
				err = ExcUnsupportedInstruction
			}
			pc++
		case OpLddw:
			if ip.sbpfVersion.DisableLddw() {
				err = ExcInvalidInstr
				break
			}
			r[ins.Dst()] = uint64(ins.Uimm()) | (uint64(ip.getSlot(pc+1).Uimm()) << 32)
			pc += 2
		case OpJa:
			pc += int64(ins.Off())
			pc++
		case OpJeqImm:
			if r[ins.Dst()] == uint64(ins.Imm()) {
				pc += int64(ins.Off())
			}
			pc++
		case OpJeqReg:
			if r[ins.Dst()] == r[ins.Src()] {
				pc += int64(ins.Off())
			}
			pc++
		case OpJgtImm:
			if r[ins.Dst()] > uint64(ins.Imm()) {
				pc += int64(ins.Off())
			}
			pc++
		case OpJgtReg:
			if r[ins.Dst()] > r[ins.Src()] {
				pc += int64(ins.Off())
			}
			pc++
		case OpJgeImm:
			if r[ins.Dst()] >= uint64(ins.Imm()) {
				pc += int64(ins.Off())
			}
			pc++
		case OpJgeReg:
			if r[ins.Dst()] >= r[ins.Src()] {
				pc += int64(ins.Off())
			}
			pc++
		case OpJltImm:
			if r[ins.Dst()] < uint64(ins.Imm()) {
				pc += int64(ins.Off())
			}
			pc++
		case OpJltReg:
			if r[ins.Dst()] < r[ins.Src()] {
				pc += int64(ins.Off())
			}
			pc++
		case OpJleImm:
			if r[ins.Dst()] <= uint64(ins.Imm()) {
				pc += int64(ins.Off())
			}
			pc++
		case OpJleReg:
			if r[ins.Dst()] <= r[ins.Src()] {
				pc += int64(ins.Off())
			}
			pc++
		case OpJsetImm:
			if r[ins.Dst()]&uint64(ins.Imm()) != 0 {
				pc += int64(ins.Off())
			}
			pc++
		case OpJsetReg:
			if r[ins.Dst()]&r[ins.Src()] != 0 {
				pc += int64(ins.Off())
			}
			pc++
		case OpJneImm:
			if r[ins.Dst()] != uint64(ins.Imm()) {
				pc += int64(ins.Off())
			}
			pc++
		case OpJneReg:
			if r[ins.Dst()] != r[ins.Src()] {
				pc += int64(ins.Off())
			}
			pc++
		case OpJsgtImm:
			if int64(r[ins.Dst()]) > int64(ins.Imm()) {
				pc += int64(ins.Off())
			}
			pc++
		case OpJsgtReg:
			if int64(r[ins.Dst()]) > int64(r[ins.Src()]) {
				pc += int64(ins.Off())
			}
			pc++
		case OpJsgeImm:
			if int64(r[ins.Dst()]) >= int64(ins.Imm()) {
				pc += int64(ins.Off())
			}
			pc++
		case OpJsgeReg:
			if int64(r[ins.Dst()]) >= int64(r[ins.Src()]) {
				pc += int64(ins.Off())
			}
			pc++
		case OpJsltImm:
			if int64(r[ins.Dst()]) < int64(ins.Imm()) {
				pc += int64(ins.Off())
			}
			pc++
		case OpJsltReg:
			if int64(r[ins.Dst()]) < int64(r[ins.Src()]) {
				pc += int64(ins.Off())
			}
			pc++
		case OpJsleImm:
			if int64(r[ins.Dst()]) <= int64(ins.Imm()) {
				pc += int64(ins.Off())
			}
			pc++
		case OpJsleReg:
			if int64(r[ins.Dst()]) <= int64(r[ins.Src()]) {
				pc += int64(ins.Off())
			}
			pc++
		case OpCall:
			if ip.sbpfVersion.EnableStaticSyscalls() {
				if ins.Src() == 0 {
					sc, ok := ip.syscalls(ins.Uimm())
					if !ok {
						err = ExcCallDest{ins.Uimm()}
						break
					}
					scStart := profileNow()
					r[0], err = sc.Invoke(ip, r[1], r[2], r[3], r[4], r[5])
					if sbpfProfileEnabled {
						profileSyscall(ins.Uimm(), scStart)
					}
					if err != nil {
						err = ExcSyscallError{Err: err}
					}
					pc++
				} else if ins.Src() == 1 {
					targetPC := ip.sbpfVersion.CalculateCallImmTargetPC(pc, ins.Imm())
					if targetPC < 0 || targetPC >= int64(len(ip.text)) {
						err = ExcCallDest{uint32(targetPC)}
						break
					}
					if ok := ip.stack.Push(r[:], pc+1); !ok {
						err = ExcCallDepth
					}
					pc = targetPC
				} else {
					err = ExcUnsupportedInstruction
				}
			} else {
				if sc, ok := ip.syscalls(ins.Uimm()); ok {
					scStart := profileNow()
					r[0], err = sc.Invoke(ip, r[1], r[2], r[3], r[4], r[5])
					if sbpfProfileEnabled {
						profileSyscall(ins.Uimm(), scStart)
					}
					if err != nil {
						err = ExcSyscallError{Err: err}
					}
					pc++
				} else if target, ok := ip.funcs[ins.Uimm()]; ok {
					ok = ip.stack.Push(r[:], pc+1)
					if !ok {
						err = ExcCallDepth
					}
					pc = target
				} else {
					err = ExcCallDest{ins.Uimm()}
				}
			}
		case OpCallx:
			var target uint64
			if ip.sbpfVersion.CallXUsesSrcReg() {
				target = r[ins.Src()]
			} else if ip.sbpfVersion.CallXUsesDstReg() {
				target = r[ins.Dst()]
			} else {
				target = r[ins.Uimm()]
			}

			if target < ip.textVA || target >= VaddrStack || target >= ip.textVA+uint64(len(ip.text)*8) {
				err = NewExcBadAccess(target, 8, false, "jump out-of-bounds")
				break
			}
			targetPC := int64((target - ip.textVA) / 8)
			if ok := ip.stack.Push(r[:], pc+1); !ok {
				err = ExcCallDepth
				break
			}
			pc = targetPC
		case OpExit:
			var ok bool
			pc, ok = ip.stack.Pop(r[:])
			if !ok {
				ret = r[0]
				break mainLoop
			}
		default:
			err = ExcUnsupportedInstruction
			return
		}

		// Post execute
	postExecute:
		if err == cu.ErrComputeExceeded {
			err = ExcOutOfCU
		}

		if err != nil {
			exc := &Exception{
				PC:     pc,
				Detail: fmt.Errorf("tx: %s, programId: %s - %w:", ip.txSignature, ip.programId, err),
			}
			if IsLongIns(ins.Op()) {
				exc.PC-- // fix reported PC
			}

			return 0, 0, exc
		}
	}

	cuConsumed = ip.initialInstrMeter - ip.computeMeter.Remaining()

	return
}

func (ip *Interpreter) signExtension(val int32) uint64 {
	if ip.sbpfVersion.ExplicitSignExtensionOfResults() {
		return uint64(uint32(val))
	} else {
		return uint64(int64(val))
	}
}

func (ip *Interpreter) getSlot(pc int64) Slot {
	return ip.text[pc]
}

func (ip *Interpreter) VMContext() any {
	return ip.vmContext
}

func (ip *Interpreter) HeapMax() uint64 {
	return uint64(len(ip.heap))
}

func (ip *Interpreter) HeapSize() uint64 {
	return ip.heapSize
}

func (ip *Interpreter) UpdateHeapSize(size uint64) {
	ip.heapSize = size
}

var emptyArray [0]byte
var emptySlice = reflect.ValueOf(emptyArray[:]).UnsafePointer()

func (ip *Interpreter) translateInternal(addr uint64, size uint64, write bool) (unsafe.Pointer, error) {
	hi, lo := addr>>32, addr&math.MaxUint32
	switch hi {
	case 0:
		if !ip.sbpfVersion.EnableLowerRodataVaddr() {
			if size == 0 {
				return emptySlice, nil
			}
			return nil, NewExcBadAccess(addr, size, write, "unmapped region")
		}
		if write {
			return nil, NewExcBadAccess(addr, size, write, "write to program")
		}
		if size == 0 {
			return emptySlice, nil
		}
		if addr+size < addr || addr+size > uint64(len(ip.ro)) {
			return nil, NewExcBadAccess(addr, size, write, "out-of-bounds program read")
		}
		return unsafe.Pointer(&ip.ro[addr]), nil
	case VaddrProgram >> 32:
		if ip.sbpfVersion.EnableLowerRodataVaddr() {
			if size == 0 {
				return emptySlice, nil
			}
			return nil, NewExcBadAccess(addr, size, write, "unmapped region")
		}
		if write {
			return nil, NewExcBadAccess(addr, size, write, "write to program")
		}
		if size == 0 {
			return emptySlice, nil
		}
		if lo+size > uint64(len(ip.ro)) {
			return nil, NewExcBadAccess(addr, size, write, "out-of-bounds program read")
		}
		return unsafe.Pointer(&ip.ro[lo]), nil
	case VaddrStack >> 32:
		mem := ip.stack.GetFrame(uint32(addr))
		if size > uint64(len(mem)) {
			return nil, NewExcBadAccess(addr, size, write, "out-of-bounds stack access")
		}
		if size == 0 {
			return emptySlice, nil
		}
		if write {
			// The only place stack memory is handed out for writing, which is
			// what lets Stack.Finish clear a prefix instead of all 256 KiB.
			// GetFrame returns mem[off:], so off is StackMax-len(mem) -- already
			// gap-remapped, so this is the physical offset into the buffer.
			ip.stack.markWritten(StackMax - uint64(len(mem)) + size)
		}
		return unsafe.Pointer(&mem[0]), nil
	case VaddrHeap >> 32:
		if size == 0 {
			return emptySlice, nil
		}
		if lo+size > uint64(len(ip.heap)) {
			return nil, NewExcBadAccess(addr, size, write, "out-of-bounds heap access")
		}
		return unsafe.Pointer(&ip.heap[lo]), nil
	case VaddrInput >> 32:
		if size == 0 {
			return emptySlice, nil
		}
		if len(ip.inputRegions) != 0 {
			return ip.translateInputRegion(lo, size, write)
		}
		if lo+size > uint64(len(ip.input)) {
			return nil, NewExcBadAccess(addr, size, write, "out-of-bounds input access")
		}
		return unsafe.Pointer(&ip.input[lo]), nil
	default:
		if size == 0 {
			return emptySlice, nil
		}
		return nil, NewExcBadAccess(addr, size, write, "unmapped region")
	}
}

func (ip *Interpreter) inputRegionIndex(offset uint64) int {
	idx, found := slices.BinarySearchFunc(ip.inputRegions, offset, func(region InputRegion, target uint64) int {
		if target < region.Offset {
			return 1
		}
		if target >= region.Offset+region.AddressSpaceReserved {
			return -1
		}
		return 0
	})
	if !found {
		return -1
	}
	return idx
}

func (ip *Interpreter) translateInputRegion(offset, size uint64, write bool) (unsafe.Pointer, error) {
	idx := ip.inputRegionIndex(offset)
	if idx < 0 {
		return nil, NewExcBadAccess(VaddrInput+offset, size, write, "unmapped input region")
	}

	region := &ip.inputRegions[idx]
	regionOffset := offset - region.Offset
	requestedLen := regionOffset + size
	if requestedLen < regionOffset || requestedLen > region.AddressSpaceReserved {
		return nil, NewExcBadAccess(VaddrInput+offset, size, write, "out-of-bounds input access")
	}
	if write && (!region.Writable || requestedLen > region.RegionSize) && region.OnWrite != nil {
		if err := region.OnWrite(region, requestedLen); err != nil {
			return nil, err
		}
	}
	if requestedLen > region.RegionSize {
		if !write || !region.Writable {
			return nil, NewExcBadAccess(VaddrInput+offset, size, write, "out-of-bounds input access")
		}
		region.RegionSize = region.AddressSpaceReserved
	}
	if write && !region.Writable {
		return nil, NewExcBadAccess(VaddrInput+offset, size, write, "write to readonly input region")
	}
	if region.Data != nil {
		if requestedLen > uint64(len(region.Data)) {
			return nil, NewExcBadAccess(VaddrInput+offset, size, write, "out-of-bounds input access")
		}
		return unsafe.Pointer(&region.Data[regionOffset]), nil
	}

	hostOffset := region.HostOffset + regionOffset
	if hostOffset < region.HostOffset || hostOffset+size < hostOffset || hostOffset+size > uint64(len(ip.input)) {
		return nil, NewExcBadAccess(VaddrInput+offset, size, write, "out-of-bounds input access")
	}
	return unsafe.Pointer(&ip.input[hostOffset]), nil
}

func (ip *Interpreter) TranslateInput(addr uint64, size uint64) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	if addr < VaddrInput {
		return nil, NewExcBadAccess(addr, size, false, "unmapped input region")
	}
	offset := addr - VaddrInput
	if len(ip.inputRegions) != 0 {
		idx := ip.inputRegionIndex(offset)
		if idx < 0 {
			return nil, NewExcBadAccess(addr, size, false, "unmapped input region")
		}
		region := ip.inputRegions[idx]
		regionOffset := offset - region.Offset
		if regionOffset+size < regionOffset || regionOffset+size > region.AddressSpaceReserved {
			return nil, NewExcBadAccess(addr, size, false, "out-of-bounds input access")
		}
		if region.Data != nil {
			if regionOffset+size > uint64(len(region.Data)) {
				return nil, NewExcBadAccess(addr, size, false, "out-of-bounds input access")
			}
			return region.Data[regionOffset : regionOffset+size], nil
		}
		hostOffset := region.HostOffset + regionOffset
		if hostOffset < region.HostOffset || hostOffset+size < hostOffset || hostOffset+size > uint64(len(ip.input)) {
			return nil, NewExcBadAccess(addr, size, false, "out-of-bounds input access")
		}
		return ip.input[hostOffset : hostOffset+size], nil
	}
	if offset+size < offset || offset+size > uint64(len(ip.input)) {
		return nil, NewExcBadAccess(addr, size, false, "out-of-bounds input access")
	}
	return ip.input[offset : offset+size], nil
}

func (ip *Interpreter) SetInputRegionData(addr uint64, data []byte, length uint64, writable bool) bool {
	if addr < VaddrInput || len(ip.inputRegions) == 0 {
		return false
	}
	idx := ip.inputRegionIndex(addr - VaddrInput)
	if idx < 0 {
		return false
	}
	region := &ip.inputRegions[idx]
	if addr != VaddrInput+region.Offset || length > region.AddressSpaceReserved {
		return false
	}
	if data != nil {
		region.Data = data
	}
	region.RegionSize = length
	region.Writable = writable
	return true
}

func (ip *Interpreter) SetInputRegionLength(addr uint64, length uint64, writable bool) bool {
	return ip.SetInputRegionData(addr, nil, length, writable)
}

func (ip *Interpreter) Translate(addr uint64, size uint64, write bool) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}

	ptr, err := ip.translateInternal(addr, size, write)
	if err != nil {
		// Guarded because the arguments, not the logging, are the cost here:
		// runtime.Caller unwinds the stack and FuncForPC does a symbol-table
		// lookup, and Go evaluates both before Debugf can decide to discard
		// them. A node runs at error level, so this ran on every failed
		// translate and produced nothing. Failed translates are ordinary
		// traffic -- CPI argument translation probes addresses that legitimately
		// do not resolve -- so this is a live path, not just a crash path.
		if mlog.Log.DebugEnabled() {
			pc, filename, line, _ := runtime.Caller(1)
			mlog.Log.Debugf("[error] in %s[%s:%d] %v. calling translate on addr = %x, size = %d", runtime.FuncForPC(pc).Name(), filename, line, err, addr, size)
		}
		return nil, err
	}

	mem := unsafe.Slice((*uint8)(ptr), size)
	return mem, nil
}

func (ip *Interpreter) DueInstrCount() uint64 {
	return ip.dueInstrCount
}

func (ip *Interpreter) PrevInstrMeter() uint64 {
	return ip.prevInstrMeter
}

func (ip *Interpreter) SetPrevInstrMeter(num uint64) {
	ip.prevInstrMeter = num
}

func (ip *Interpreter) ComputeMeter() *cu.ComputeMeter {
	return ip.computeMeter
}

func (ip *Interpreter) Read(addr uint64, p []byte) error {
	ptr, err := ip.translateInternal(addr, uint64(len(p)), false)
	if err != nil {
		return err
	}
	mem := unsafe.Slice((*uint8)(ptr), len(p))
	copy(p, mem)
	return nil
}

func (ip *Interpreter) Read8(addr uint64) (uint8, error) {
	ptr, err := ip.translateInternal(addr, 1, false)
	if err != nil {
		return 0, err
	}
	return *(*uint8)(ptr), nil
}

func (ip *Interpreter) Read16(addr uint64) (uint16, error) {
	ptr, err := ip.translateInternal(addr, 2, false)
	if err != nil {
		return 0, err
	}
	return *(*uint16)(ptr), nil
}

func (ip *Interpreter) Read32(addr uint64) (uint32, error) {
	ptr, err := ip.translateInternal(addr, 4, false)
	if err != nil {
		return 0, err
	}
	return *(*uint32)(ptr), nil
}

func (ip *Interpreter) Read64(addr uint64) (uint64, error) {
	ptr, err := ip.translateInternal(addr, 8, false)
	if err != nil {
		return 0, err
	}
	return *(*uint64)(ptr), nil
}

func (ip *Interpreter) Write(addr uint64, p []byte) error {
	ptr, err := ip.translateInternal(addr, uint64(len(p)), true)
	if err != nil {
		return err
	}
	mem := unsafe.Slice((*uint8)(ptr), len(p))
	copy(mem, p)
	return nil
}

func (ip *Interpreter) Write8(addr uint64, x uint8) error {
	ptr, err := ip.translateInternal(addr, 1, true)
	if err != nil {
		return err
	}
	*(*uint8)(ptr) = x
	return nil
}

func (ip *Interpreter) Write16(addr uint64, x uint16) error {
	ptr, err := ip.translateInternal(addr, 2, true)
	if err != nil {
		return err
	}
	*(*uint16)(ptr) = x
	return nil
}

func (ip *Interpreter) Write32(addr uint64, x uint32) error {
	ptr, err := ip.translateInternal(addr, 4, true)
	if err != nil {
		return err
	}
	*(*uint32)(ptr) = x
	return nil
}

func (ip *Interpreter) Write64(addr uint64, x uint64) error {
	ptr, err := ip.translateInternal(addr, 8, true)
	if err != nil {
		return err
	}
	*(*uint64)(ptr) = x
	return nil
}
