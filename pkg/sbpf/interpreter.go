package sbpf

import (
	"errors"
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

	// callTargets[pc] is the resolved internal-function target of the `call imm`
	// at pc (or -1). Computed once per Program at load time.
	callTargets []int64

	// Fast-path translation table indexed by (vaddr >> 32); see fastmem.go.
	regions [numFastRegions]memRegion
	// dirtyLo/dirtyHi: byte range written through translateInternal;
	// memRegion.dirty: 4 KiB page bitmap of writes through the fast path.
	dirtyLo [numFastRegions]uint64
	dirtyHi [numFastRegions]uint64
}

// dirtyRange returns the union of the byte ranges that may have been written
// in window idx (stack or heap), as [lo, hi).
func (ip *Interpreter) dirtyRange(idx uint64, size uint64) (lo, hi uint64) {
	lo, hi = ip.dirtyLo[idx], ip.dirtyHi[idx]
	if pages := ip.regions[idx].dirty; pages != 0 {
		plo := uint64(bits.TrailingZeros64(pages)) << 12
		phi := uint64(bits.Len64(pages)) << 12
		lo = min(lo, plo)
		hi = max(hi, phi)
	}
	hi = min(hi, size)
	return lo, hi
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
		// Buffers in the pool are zeroed (for their dirty range) in Finish, so
		// no clear is needed here.
	} else {
		heap = newHeap()
	}

	ip := &Interpreter{
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
		callTargets:       p.CallTargets,
	}
	ip.initRegions()
	return ip
}

// initRegions fills the fast-path translation table. Windows that need the
// full logic in translateInternal are left empty (rlen = wlen = 0).
func (ip *Interpreter) initRegions() {
	for i := range ip.dirtyLo {
		ip.dirtyLo[i] = math.MaxUint64
		ip.dirtyHi[i] = 0
		ip.regions[i].gapShift = 63
	}
	if len(ip.ro) != 0 {
		idx := VaddrProgram >> 32
		if ip.sbpfVersion.EnableLowerRodataVaddr() {
			idx = 0
		}
		ip.regions[idx] = memRegion{base: unsafe.Pointer(&ip.ro[0]), rlen: uint64(len(ip.ro)), gapShift: 63}
	}
	if len(ip.stack.mem) != 0 {
		r := memRegion{base: unsafe.Pointer(&ip.stack.mem[0]), rlen: StackMax, wlen: StackMax, gapShift: 63}
		if ip.stack.stackFrameGaps {
			r.gapShift = 12 // log2(StackFrameSize)
			r.gapMask = GapMask
		}
		ip.regions[VaddrStack>>32] = r
	}
	if len(ip.heap) != 0 {
		ip.regions[VaddrHeap>>32] = memRegion{base: unsafe.Pointer(&ip.heap[0]), rlen: uint64(len(ip.heap)), wlen: uint64(len(ip.heap)), gapShift: 63}
	}
	// Finish relies on complete write tracking before returning pooled storage.
	// Larger heaps (or a future larger stack) must use translateInternal's byte
	// ranges: shifting the fast-path bitmap beyond page 63 silently loses writes.
	// Reads remain fast; current <=256 KiB writable mappings are unchanged.
	for _, idx := range []uint64{VaddrStack >> 32, VaddrHeap >> 32} {
		if ip.regions[idx].wlen > fastDirtyBytes {
			ip.regions[idx].wlen = 0
		}
	}
	if len(ip.inputRegions) == 0 && len(ip.input) != 0 {
		ip.regions[VaddrInput>>32] = memRegion{base: unsafe.Pointer(&ip.input[0]), rlen: uint64(len(ip.input)), wlen: uint64(len(ip.input)), gapShift: 63}
	}
}

func (ip *Interpreter) Finish() {
	if UsePool {
		lo, hi := ip.dirtyRange(VaddrHeap>>32, uint64(len(ip.heap)))
		if hi > lo {
			clear(ip.heap[lo:hi])
		}
		heapPool.Put(ip.heap)
	}
	ip.stack.MarkDirty(ip.dirtyRange(VaddrStack>>32, StackMax))
	ip.stack.Finish()
}

func (ip *Interpreter) executeJmp32(ins Slot, pc int64, r *[16]uint64) (int64, error) {
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
	var r [16]uint64 // 16 (not 11) so that r[ins.Dst()] (4-bit field) needs no bounds check
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

	// Loop-invariant state hoisted into locals so the compiler can keep them in
	// registers (fields of ip may alias with the unsafe stores in the loop and
	// would otherwise be reloaded on every instruction).
	text := ip.text
	tracing := ip.enableTracing
	jmp32 := ip.sbpfVersion.EnableJmp32()
	moveMem := ip.sbpfVersion.MoveMemoryInstructionClasses()
	pqr := ip.sbpfVersion.EnablePqr()
	staticSyscalls := ip.sbpfVersion.EnableStaticSyscalls()
	callTargets := ip.callTargets

	// Instruction metering (mirrors Agave's due_insn_count / previous_instruction_meter):
	// count executed instructions locally and only sync with the shared compute
	// meter around syscalls and on exit. `budget` is the number of instructions
	// we may still execute before the meter would be exhausted.
	meter := ip.computeMeter
	var budget, due uint64
	reloadBudget := func() {
		if meter.Disabled() {
			budget = math.MaxUint64
		} else {
			budget = meter.Remaining()
		}
		due = 0
		// A syscall (CPI in particular) may have changed the input regions;
		// drop the cached input-region fast path entry, it is re-populated on
		// the next slow-path translation. (With a plain, region-less input the
		// entry is static and stays.)
		if len(ip.inputRegions) != 0 {
			ip.regions[VaddrInput>>32] = emptyRegion
		}
	}
	flushDue := func() {
		if due != 0 {
			_ = meter.Consume(due)
			due = 0
		}
	}
	reloadBudget()

mainLoop:
	for i := 0; true; i++ {
		// Fetch
		if pc < 0 || pc >= int64(len(text)) {
			flushDue()
			return 0, 0, &Exception{
				PC:     pc,
				Detail: fmt.Errorf("tx: %s, programId: %s - %w:", ip.txSignature, ip.programId, ExcExecutionOverrun),
			}
		}
		ins := text[pc]
		if tracing {
			regsDump := fmt.Sprintf("%016x, %016x, %016x, %016x, %016x, %016x, %016x, %016x, %016x, %016x, %016x",
				r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10])
			fmt.Printf("% 5d [%s]: %s\n",
				i, strings.ToUpper(regsDump), ip.disassemble(ins, 0))
		}

		// Meter: identical semantics to Consume(1) before each instruction.
		if due == budget {
			err = cu.ErrComputeExceeded
			break mainLoop
		}
		due++

		// Execute
		if jmp32 && ins.Op()&0x07 == ClassPqr {
			pc, err = ip.executeJmp32(ins, pc, &r)
			goto postExecute
		}
		switch ins.Op() {
		case OpLdxb:
			if moveMem {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
			if p := ip.fastRead(vma, 1); p != nil {
				r[ins.Dst()] = uint64(*(*uint8)(p))
			} else {
				var v uint8
				v, err = ip.Read8(vma)
				r[ins.Dst()] = uint64(v)
			}
			pc++
		case OpLdxh:
			if moveMem {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
			if p := ip.fastRead(vma, 2); p != nil {
				r[ins.Dst()] = uint64(*(*uint16)(p))
			} else {
				var v uint16
				v, err = ip.Read16(vma)
				r[ins.Dst()] = uint64(v)
			}
			pc++
		case OpLdxw:
			if moveMem {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
			if p := ip.fastRead(vma, 4); p != nil {
				r[ins.Dst()] = uint64(*(*uint32)(p))
			} else {
				var v uint32
				v, err = ip.Read32(vma)
				r[ins.Dst()] = uint64(v)
			}
			pc++
		case OpLdxdw:
			if moveMem {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
			if p := ip.fastRead(vma, 8); p != nil {
				r[ins.Dst()] = uint64(*(*uint64)(p))
			} else {
				var v uint64
				v, err = ip.Read64(vma)
				r[ins.Dst()] = uint64(v)
			}
			pc++
		case OpStb:
			if moveMem {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			if p := ip.fastWrite(vma, 1); p != nil {
				*(*uint8)(p) = uint8(ins.Uimm())
			} else {
				err = ip.Write8(vma, uint8(ins.Uimm()))
			}
			pc++
		case OpSth:
			if moveMem {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			if p := ip.fastWrite(vma, 2); p != nil {
				*(*uint16)(p) = uint16(ins.Uimm())
			} else {
				err = ip.Write16(vma, uint16(ins.Uimm()))
			}
			pc++
		case OpStw:
			if moveMem {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			if p := ip.fastWrite(vma, 4); p != nil {
				*(*uint32)(p) = uint32(ins.Uimm())
			} else {
				err = ip.Write32(vma, uint32(ins.Uimm()))
			}
			pc++
		case OpStdw:
			if moveMem {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			if p := ip.fastWrite(vma, 8); p != nil {
				*(*uint64)(p) = uint64(ins.Imm())
			} else {
				err = ip.Write64(vma, uint64(ins.Imm()))
			}
			pc++
		case OpStxb:
			if moveMem {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			if p := ip.fastWrite(vma, 1); p != nil {
				*(*uint8)(p) = uint8(r[ins.Src()])
			} else {
				err = ip.Write8(vma, uint8(r[ins.Src()]))
			}
			pc++
		case OpStxh:
			if moveMem {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			if p := ip.fastWrite(vma, 2); p != nil {
				*(*uint16)(p) = uint16(r[ins.Src()])
			} else {
				err = ip.Write16(vma, uint16(r[ins.Src()]))
			}
			pc++
		case OpStxw:
			if moveMem {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			if p := ip.fastWrite(vma, 4); p != nil {
				*(*uint32)(p) = uint32(r[ins.Src()])
			} else {
				err = ip.Write32(vma, uint32(r[ins.Src()]))
			}
			pc++
		case OpStxdw:
			if moveMem {
				err = ExcInvalidInstr
				break
			}
			vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
			if p := ip.fastWrite(vma, 8); p != nil {
				*(*uint64)(p) = uint64(r[ins.Src()])
			} else {
				err = ip.Write64(vma, uint64(r[ins.Src()]))
			}
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
			if staticSyscalls {
				if ins.Src() == 0 {
					sc, ok := ip.syscalls(ins.Uimm())
					if !ok {
						err = ExcCallDest{ins.Uimm()}
						break
					}
					flushDue()
					r[0], err = sc.Invoke(ip, r[1], r[2], r[3], r[4], r[5])
					reloadBudget()
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
					if ok := ip.stack.Push(&r, pc+1); !ok {
						err = ExcCallDepth
					}
					pc = targetPC
				} else {
					err = ExcUnsupportedInstruction
				}
			} else {
				if sc, ok := ip.syscalls(ins.Uimm()); ok {
					flushDue()
					r[0], err = sc.Invoke(ip, r[1], r[2], r[3], r[4], r[5])
					reloadBudget()
					if err != nil {
						err = ExcSyscallError{Err: err}
					}
					pc++
				} else {
					var target int64
					var ok bool
					if callTargets != nil {
						target = callTargets[pc]
						ok = target >= 0
					} else {
						target, ok = ip.funcs[ins.Uimm()]
					}
					if !ok {
						err = ExcCallDest{ins.Uimm()}
						break
					}
					if !ip.stack.Push(&r, pc+1) {
						err = ExcCallDepth
					}
					pc = target
				}
			}
		case OpExit:
			var ok bool
			pc, ok = ip.stack.Pop(&r)
			if !ok {
				ret = r[0]
				break mainLoop
			}
		case OpMul32Reg:
			if pqr {
				pc, err = ip.executeCold(ins, pc, &r)
				break
			}
			r[ins.Dst()] = uint64(int32(r[ins.Dst()]) * int32(r[ins.Src()]))
			pc++
		case OpMul64Imm:
			if pqr {
				pc, err = ip.executeCold(ins, pc, &r)
				break
			}
			r[ins.Dst()] *= uint64(ins.Imm())
			pc++
		case OpMul64Reg:
			if pqr {
				pc, err = ip.executeCold(ins, pc, &r)
				break
			}
			r[ins.Dst()] *= r[ins.Src()]
			pc++
		case OpDiv32Reg:
			if pqr {
				pc, err = ip.executeCold(ins, pc, &r)
				break
			}
			if src := uint32(r[ins.Src()]); src != 0 {
				r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) / src)
			} else {
				err = ExcDivideByZero
			}
			pc++
		case OpDiv64Imm:
			if pqr {
				pc, err = ip.executeCold(ins, pc, &r)
				break
			}
			r[ins.Dst()] /= uint64(ins.Imm())
			pc++
		case OpDiv64Reg:
			if pqr {
				pc, err = ip.executeCold(ins, pc, &r)
				break
			}
			if src := r[ins.Src()]; src != 0 {
				r[ins.Dst()] /= src
			} else {
				err = ExcDivideByZero
			}
			pc++
		case OpMod32Reg:
			if pqr {
				pc, err = ip.executeCold(ins, pc, &r)
				break
			}
			if src := uint32(r[ins.Src()]); src != 0 {
				r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) % src)
			} else {
				err = ExcDivideByZero
			}
			pc++
		case OpMod64Imm:
			if pqr {
				pc, err = ip.executeCold(ins, pc, &r)
				break
			}
			r[ins.Dst()] %= uint64(ins.Imm())
			pc++
		case OpMod64Reg:
			if pqr {
				pc, err = ip.executeCold(ins, pc, &r)
				break
			}
			if src := r[ins.Src()]; src != 0 {
				r[ins.Dst()] %= src
			} else {
				err = ExcDivideByZero
			}
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
		default:
			pc, err = ip.executeCold(ins, pc, &r)
			if err == errUnknownOpcode {
				// Preserve the original behaviour for an unknown opcode:
				// a bare (unwrapped) ExcUnsupportedInstruction.
				flushDue()
				return 0, 0, ExcUnsupportedInstruction
			}
		}

		// Post execute
	postExecute:
		if err != nil {
			flushDue()
			if err == cu.ErrComputeExceeded {
				err = ExcOutOfCU
			}
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

	flushDue()
	// NB: when the loop exits because the meter is exhausted, err is the bare
	// cu.ErrComputeExceeded (not wrapped in an Exception), as before.
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
		if lo+size < lo || lo+size > uint64(len(ip.ro)) {
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
			off := StackMax - uint64(len(mem))
			ip.dirtyLo[VaddrStack>>32] = min(ip.dirtyLo[VaddrStack>>32], off)
			ip.dirtyHi[VaddrStack>>32] = max(ip.dirtyHi[VaddrStack>>32], off+size)
		}
		return unsafe.Pointer(&mem[0]), nil
	case VaddrHeap >> 32:
		if size == 0 {
			return emptySlice, nil
		}
		if lo+size < lo || lo+size > uint64(len(ip.heap)) {
			return nil, NewExcBadAccess(addr, size, write, "out-of-bounds heap access")
		}
		if write {
			ip.dirtyLo[VaddrHeap>>32] = min(ip.dirtyLo[VaddrHeap>>32], lo)
			ip.dirtyHi[VaddrHeap>>32] = max(ip.dirtyHi[VaddrHeap>>32], lo+size)
		}
		return unsafe.Pointer(&ip.heap[lo]), nil
	case VaddrInput >> 32:
		if size == 0 {
			return emptySlice, nil
		}
		if len(ip.inputRegions) != 0 {
			return ip.translateInputRegion(lo, size, write)
		}
		if lo+size < lo || lo+size > uint64(len(ip.input)) {
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
		// The callback may replace region.Data / grow the region: drop the cache.
		ip.regions[VaddrInput>>32] = emptyRegion
		if err := region.OnWrite(region, requestedLen); err != nil {
			return nil, err
		}
	}
	if requestedLen > region.RegionSize {
		if !write || !region.Writable {
			return nil, NewExcBadAccess(VaddrInput+offset, size, write, "out-of-bounds input access")
		}
		ip.regions[VaddrInput>>32] = emptyRegion
		region.RegionSize = region.AddressSpaceReserved
	}
	if write && !region.Writable {
		return nil, NewExcBadAccess(VaddrInput+offset, size, write, "write to readonly input region")
	}
	var base unsafe.Pointer
	if region.Data != nil {
		if requestedLen > uint64(len(region.Data)) {
			return nil, NewExcBadAccess(VaddrInput+offset, size, write, "out-of-bounds input access")
		}
		base = unsafe.Pointer(unsafe.SliceData(region.Data))
	} else {
		hostOffset := region.HostOffset + regionOffset
		if hostOffset < region.HostOffset || hostOffset+size < hostOffset || hostOffset+size > uint64(len(ip.input)) {
			return nil, NewExcBadAccess(VaddrInput+offset, size, write, "out-of-bounds input access")
		}
		base = unsafe.Pointer(&ip.input[region.HostOffset])
	}
	// Cache this region for the interpreter's fast path (one-entry cache,
	// same idea as Agave's MappingCache). Only the currently mapped
	// RegionSize bytes are exposed; anything beyond takes the slow path
	// again so that OnWrite / growth semantics are preserved.
	if region.RegionSize != 0 && (region.Data == nil || uint64(len(region.Data)) >= region.RegionSize) {
		cached := memRegion{base: base, start: region.Offset, rlen: region.RegionSize, gapShift: 63}
		if region.Writable {
			cached.wlen = region.RegionSize
		}
		ip.regions[VaddrInput>>32] = cached
	}
	return unsafe.Add(base, regionOffset), nil
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
	ip.regions[VaddrInput>>32] = emptyRegion
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
		pc, filename, line, _ := runtime.Caller(1)
		mlog.Log.Debugf("[error] in %s[%s:%d] %v. calling translate on addr = %x, size = %d", runtime.FuncForPC(pc).Name(), filename, line, err, addr, size)
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

// executeCold handles the less frequently executed opcodes. Keeping them out of
// Run keeps that function below the compiler's "big function" threshold so the
// hot helpers (metering, fast memory translation, stack push/pop) stay inlinable.
func (ip *Interpreter) executeCold(ins Slot, pc int64, r *[16]uint64) (int64, error) {
	var err error
	switch ins.Op() {
	// In v2 these encodings are memory operations, not MUL/DIV/MOD.
	// Run dispatches their non-v2 arithmetic forms on the hot path.
	case OpLd1BReg:
		if !ip.sbpfVersion.MoveMemoryInstructionClasses() {
			err = ExcInvalidInstr
			break
		}
		vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
		var v uint8
		v, err = ip.Read8(vma)
		r[ins.Dst()] = uint64(v)
		pc++
	case OpSt1BImm:
		if !ip.sbpfVersion.MoveMemoryInstructionClasses() {
			err = ExcInvalidInstr
			break
		}
		vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
		err = ip.Write8(vma, uint8(ins.Uimm()))
		pc++
	case OpSt1BReg:
		if !ip.sbpfVersion.MoveMemoryInstructionClasses() {
			err = ExcInvalidInstr
			break
		}
		vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
		err = ip.Write8(vma, uint8(r[ins.Src()]))
		pc++
	case OpLd2BReg:
		if !ip.sbpfVersion.MoveMemoryInstructionClasses() {
			err = ExcInvalidInstr
			break
		}
		vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
		var v uint16
		v, err = ip.Read16(vma)
		r[ins.Dst()] = uint64(v)
		pc++
	case OpSt2BImm:
		if !ip.sbpfVersion.MoveMemoryInstructionClasses() {
			err = ExcInvalidInstr
			break
		}
		vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
		err = ip.Write16(vma, uint16(ins.Uimm()))
		pc++
	case OpSt2BReg:
		if !ip.sbpfVersion.MoveMemoryInstructionClasses() {
			err = ExcInvalidInstr
			break
		}
		vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
		err = ip.Write16(vma, uint16(r[ins.Src()]))
		pc++
	case OpLd8BReg:
		if !ip.sbpfVersion.MoveMemoryInstructionClasses() {
			err = ExcInvalidInstr
			break
		}
		vma := uint64(int64(r[ins.Src()]) + int64(ins.Off()))
		var v uint64
		v, err = ip.Read64(vma)
		r[ins.Dst()] = v
		pc++
	case OpSt8BImm:
		if !ip.sbpfVersion.MoveMemoryInstructionClasses() {
			err = ExcInvalidInstr
			break
		}
		vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
		err = ip.Write64(vma, uint64(ins.Imm()))
		pc++
	case OpSt8BReg:
		if !ip.sbpfVersion.MoveMemoryInstructionClasses() {
			err = ExcInvalidInstr
			break
		}
		vma := uint64(int64(r[ins.Dst()]) + int64(ins.Off()))
		err = ip.Write64(vma, r[ins.Src()])
		pc++
	case OpDiv32Imm:
		r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) / ins.Uimm())
		pc++
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
		if ok := ip.stack.Push(r, pc+1); !ok {
			err = ExcCallDepth
			break
		}
		pc = targetPC
	default:
		err = errUnknownOpcode
	}
	return pc, err
}

// errUnknownOpcode is an internal sentinel returned by executeCold for an
// opcode that is not handled by either switch; Run turns it into the bare
// ExcUnsupportedInstruction return of the original implementation.
var errUnknownOpcode = errors.New("unknown opcode")
