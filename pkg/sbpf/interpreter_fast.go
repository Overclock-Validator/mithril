package sbpf

// useFastLoop lets tests compare Run with and without runFast.
var useFastLoop = true

// fastFlags holds the SBPF version gates runFast needs, hoisted out of the loop.
type fastFlags struct {
	jmp32          bool
	moveMem        bool
	pqr            bool
	swapSub        bool
	explicitSext   bool
	disableLddw    bool
	staticSyscalls bool
	// fastCalls: callTargets[pc] >= 0 implies the immediate is not a syscall,
	// so an internal call can skip Run's syscall lookup (see
	// Program.ResolveCallTargetsExcluding).
	fastCalls bool
}

func (ip *Interpreter) fastFlags() fastFlags {
	v := ip.sbpfVersion
	return fastFlags{
		jmp32:          v.EnableJmp32(),
		moveMem:        v.MoveMemoryInstructionClasses(),
		pqr:            v.EnablePqr(),
		swapSub:        v.SwapSubRegImmOperands(),
		explicitSext:   v.ExplicitSignExtensionOfResults(),
		disableLddw:    v.DisableLddw(),
		staticSyscalls: v.EnableStaticSyscalls(),
		fastCalls:      ip.callTargetsExcludeSyscalls && len(ip.callTargets) == len(ip.text),
	}
}

func sext32(explicit bool, val int32) uint64 {
	if explicit {
		return uint64(uint32(val))
	}
	return uint64(int64(val))
}

// runFast executes the common instructions of Run without any calls, so that
// the loop state stays in registers. Each instruction is either executed with
// exactly Run's semantics (meter check, due++, effect, next pc) or not touched:
// runFast returns pc/due at the first instruction it cannot complete (calls,
// exits, slow memory translation, zero divisors, cold opcodes, an out-of-range
// pc or an exhausted budget) and Run handles that instruction itself.
func (ip *Interpreter) runFast(r *[16]uint64, pc int64, due, budget uint64, f fastFlags) (int64, uint64) {
	text := ip.text
	callTargets := ip.callTargets
	stack := &ip.stack
	for {
		if uint64(pc) >= uint64(len(text)) || due == budget {
			return pc, due
		}
		ins := text[pc]
		op := ins.Op()
		if f.jmp32 && op&0x07 == ClassPqr {
			return pc, due
		}
		switch op {
		case OpLdxb:
			if f.moveMem {
				return pc, due
			}
			p := ip.fastRead(uint64(int64(r[ins.Src()])+int64(ins.Off())), 1)
			if p == nil {
				return pc, due
			}
			r[ins.Dst()] = uint64(*(*uint8)(p))
		case OpLdxh:
			if f.moveMem {
				return pc, due
			}
			p := ip.fastRead(uint64(int64(r[ins.Src()])+int64(ins.Off())), 2)
			if p == nil {
				return pc, due
			}
			r[ins.Dst()] = uint64(*(*uint16)(p))
		case OpLdxw:
			if f.moveMem {
				return pc, due
			}
			p := ip.fastRead(uint64(int64(r[ins.Src()])+int64(ins.Off())), 4)
			if p == nil {
				return pc, due
			}
			r[ins.Dst()] = uint64(*(*uint32)(p))
		case OpLdxdw:
			if f.moveMem {
				return pc, due
			}
			p := ip.fastRead(uint64(int64(r[ins.Src()])+int64(ins.Off())), 8)
			if p == nil {
				return pc, due
			}
			r[ins.Dst()] = *(*uint64)(p)
		case OpStb:
			if f.moveMem {
				return pc, due
			}
			p := ip.fastWrite(uint64(int64(r[ins.Dst()])+int64(ins.Off())), 1)
			if p == nil {
				return pc, due
			}
			*(*uint8)(p) = uint8(ins.Uimm())
		case OpSth:
			if f.moveMem {
				return pc, due
			}
			p := ip.fastWrite(uint64(int64(r[ins.Dst()])+int64(ins.Off())), 2)
			if p == nil {
				return pc, due
			}
			*(*uint16)(p) = uint16(ins.Uimm())
		case OpStw:
			if f.moveMem {
				return pc, due
			}
			p := ip.fastWrite(uint64(int64(r[ins.Dst()])+int64(ins.Off())), 4)
			if p == nil {
				return pc, due
			}
			*(*uint32)(p) = ins.Uimm()
		case OpStdw:
			if f.moveMem {
				return pc, due
			}
			p := ip.fastWrite(uint64(int64(r[ins.Dst()])+int64(ins.Off())), 8)
			if p == nil {
				return pc, due
			}
			*(*uint64)(p) = uint64(ins.Imm())
		case OpStxb:
			if f.moveMem {
				return pc, due
			}
			p := ip.fastWrite(uint64(int64(r[ins.Dst()])+int64(ins.Off())), 1)
			if p == nil {
				return pc, due
			}
			*(*uint8)(p) = uint8(r[ins.Src()])
		case OpStxh:
			if f.moveMem {
				return pc, due
			}
			p := ip.fastWrite(uint64(int64(r[ins.Dst()])+int64(ins.Off())), 2)
			if p == nil {
				return pc, due
			}
			*(*uint16)(p) = uint16(r[ins.Src()])
		case OpStxw:
			if f.moveMem {
				return pc, due
			}
			p := ip.fastWrite(uint64(int64(r[ins.Dst()])+int64(ins.Off())), 4)
			if p == nil {
				return pc, due
			}
			*(*uint32)(p) = uint32(r[ins.Src()])
		case OpStxdw:
			if f.moveMem {
				return pc, due
			}
			p := ip.fastWrite(uint64(int64(r[ins.Dst()])+int64(ins.Off())), 8)
			if p == nil {
				return pc, due
			}
			*(*uint64)(p) = r[ins.Src()]
		case OpAdd32Imm:
			r[ins.Dst()] = sext32(f.explicitSext, int32(r[ins.Dst()])+ins.Imm())
		case OpAdd32Reg:
			r[ins.Dst()] = sext32(f.explicitSext, int32(r[ins.Dst()])+int32(r[ins.Src()]))
		case OpAdd64Imm:
			r[ins.Dst()] += uint64(ins.Imm())
		case OpAdd64Reg:
			r[ins.Dst()] += r[ins.Src()]
		case OpSub32Imm:
			if f.swapSub {
				r[ins.Dst()] = sext32(f.explicitSext, ins.Imm()-int32(r[ins.Dst()]))
			} else {
				r[ins.Dst()] = sext32(f.explicitSext, int32(r[ins.Dst()])-ins.Imm())
			}
		case OpSub32Reg:
			r[ins.Dst()] = sext32(f.explicitSext, int32(r[ins.Dst()])-int32(r[ins.Src()]))
		case OpSub64Imm:
			if f.swapSub {
				r[ins.Dst()] = uint64(int64(ins.Imm())) - r[ins.Dst()]
			} else {
				r[ins.Dst()] -= uint64(int64(ins.Imm()))
			}
		case OpSub64Reg:
			r[ins.Dst()] -= r[ins.Src()]
		case OpMul32Imm:
			r[ins.Dst()] = uint64(int32(r[ins.Dst()]) * ins.Imm())
		case OpOr32Imm:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) | ins.Uimm())
		case OpOr32Reg:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) | uint32(r[ins.Src()]))
		case OpOr64Imm:
			r[ins.Dst()] |= uint64(ins.Imm())
		case OpOr64Reg:
			r[ins.Dst()] |= r[ins.Src()]
		case OpAnd32Imm:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) & ins.Uimm())
		case OpAnd32Reg:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) & uint32(r[ins.Src()]))
		case OpAnd64Imm:
			r[ins.Dst()] &= uint64(ins.Imm())
		case OpAnd64Reg:
			r[ins.Dst()] &= r[ins.Src()]
		case OpLsh32Imm:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) << (ins.Uimm() & 0x1f))
		case OpLsh32Reg:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) << (uint32(r[ins.Src()]) & 0x1f))
		case OpLsh64Imm:
			r[ins.Dst()] <<= uint64(ins.Imm()) & 0x3f
		case OpLsh64Reg:
			r[ins.Dst()] <<= r[ins.Src()] & 0x3f
		case OpRsh32Imm:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) >> (ins.Uimm() & 0x1f))
		case OpRsh32Reg:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) >> (uint32(r[ins.Src()]) & 0x1f))
		case OpRsh64Imm:
			r[ins.Dst()] >>= uint64(ins.Imm()) & 0x3f
		case OpRsh64Reg:
			r[ins.Dst()] >>= r[ins.Src()] & 0x3f
		case OpXor32Imm:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) ^ ins.Uimm())
		case OpXor32Reg:
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) ^ uint32(r[ins.Src()]))
		case OpXor64Imm:
			r[ins.Dst()] ^= uint64(ins.Imm())
		case OpXor64Reg:
			r[ins.Dst()] ^= r[ins.Src()]
		case OpMov32Imm:
			r[ins.Dst()] = uint64(ins.Uimm())
		case OpMov32Reg:
			if f.explicitSext {
				r[ins.Dst()] = uint64(int64(int32(r[ins.Src()])))
			} else {
				r[ins.Dst()] = uint64(uint32(r[ins.Src()]))
			}
		case OpMov64Imm:
			r[ins.Dst()] = uint64(ins.Imm())
		case OpMov64Reg:
			r[ins.Dst()] = r[ins.Src()]
		case OpLddw:
			if f.disableLddw || uint64(pc+1) >= uint64(len(text)) {
				return pc, due
			}
			r[ins.Dst()] = uint64(ins.Uimm()) | (uint64(text[pc+1].Uimm()) << 32)
			pc++
		case OpJa:
			pc += int64(ins.Off())
		case OpJeqImm:
			if r[ins.Dst()] == uint64(ins.Imm()) {
				pc += int64(ins.Off())
			}
		case OpJeqReg:
			if r[ins.Dst()] == r[ins.Src()] {
				pc += int64(ins.Off())
			}
		case OpJgtImm:
			if r[ins.Dst()] > uint64(ins.Imm()) {
				pc += int64(ins.Off())
			}
		case OpJgtReg:
			if r[ins.Dst()] > r[ins.Src()] {
				pc += int64(ins.Off())
			}
		case OpJgeImm:
			if r[ins.Dst()] >= uint64(ins.Imm()) {
				pc += int64(ins.Off())
			}
		case OpJgeReg:
			if r[ins.Dst()] >= r[ins.Src()] {
				pc += int64(ins.Off())
			}
		case OpJltImm:
			if r[ins.Dst()] < uint64(ins.Imm()) {
				pc += int64(ins.Off())
			}
		case OpJltReg:
			if r[ins.Dst()] < r[ins.Src()] {
				pc += int64(ins.Off())
			}
		case OpJleImm:
			if r[ins.Dst()] <= uint64(ins.Imm()) {
				pc += int64(ins.Off())
			}
		case OpJleReg:
			if r[ins.Dst()] <= r[ins.Src()] {
				pc += int64(ins.Off())
			}
		case OpJsetImm:
			if r[ins.Dst()]&uint64(ins.Imm()) != 0 {
				pc += int64(ins.Off())
			}
		case OpJsetReg:
			if r[ins.Dst()]&r[ins.Src()] != 0 {
				pc += int64(ins.Off())
			}
		case OpJneImm:
			if r[ins.Dst()] != uint64(ins.Imm()) {
				pc += int64(ins.Off())
			}
		case OpJneReg:
			if r[ins.Dst()] != r[ins.Src()] {
				pc += int64(ins.Off())
			}
		case OpJsgtImm:
			if int64(r[ins.Dst()]) > int64(ins.Imm()) {
				pc += int64(ins.Off())
			}
		case OpJsgtReg:
			if int64(r[ins.Dst()]) > int64(r[ins.Src()]) {
				pc += int64(ins.Off())
			}
		case OpJsgeImm:
			if int64(r[ins.Dst()]) >= int64(ins.Imm()) {
				pc += int64(ins.Off())
			}
		case OpJsgeReg:
			if int64(r[ins.Dst()]) >= int64(r[ins.Src()]) {
				pc += int64(ins.Off())
			}
		case OpJsltImm:
			if int64(r[ins.Dst()]) < int64(ins.Imm()) {
				pc += int64(ins.Off())
			}
		case OpJsltReg:
			if int64(r[ins.Dst()]) < int64(r[ins.Src()]) {
				pc += int64(ins.Off())
			}
		case OpJsleImm:
			if int64(r[ins.Dst()]) <= int64(ins.Imm()) {
				pc += int64(ins.Off())
			}
		case OpJsleReg:
			if int64(r[ins.Dst()]) <= int64(r[ins.Src()]) {
				pc += int64(ins.Off())
			}
		case OpMul32Reg:
			if f.pqr {
				return pc, due
			}
			r[ins.Dst()] = uint64(int32(r[ins.Dst()]) * int32(r[ins.Src()]))
		case OpMul64Imm:
			if f.pqr {
				return pc, due
			}
			r[ins.Dst()] *= uint64(ins.Imm())
		case OpMul64Reg:
			if f.pqr {
				return pc, due
			}
			r[ins.Dst()] *= r[ins.Src()]
		case OpDiv32Reg:
			src := uint32(r[ins.Src()])
			if f.pqr || src == 0 {
				return pc, due
			}
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) / src)
		case OpDiv64Imm:
			imm := uint64(ins.Imm())
			if f.pqr || imm == 0 {
				return pc, due
			}
			r[ins.Dst()] /= imm
		case OpDiv64Reg:
			src := r[ins.Src()]
			if f.pqr || src == 0 {
				return pc, due
			}
			r[ins.Dst()] /= src
		case OpMod32Reg:
			src := uint32(r[ins.Src()])
			if f.pqr || src == 0 {
				return pc, due
			}
			r[ins.Dst()] = uint64(uint32(r[ins.Dst()]) % src)
		case OpMod64Imm:
			imm := uint64(ins.Imm())
			if f.pqr || imm == 0 {
				return pc, due
			}
			r[ins.Dst()] %= imm
		case OpMod64Reg:
			src := r[ins.Src()]
			if f.pqr || src == 0 {
				return pc, due
			}
			r[ins.Dst()] %= src
		case OpArsh32Imm:
			r[ins.Dst()] = uint64(uint32(int32(r[ins.Dst()]) >> ins.Uimm()))
		case OpArsh32Reg:
			r[ins.Dst()] = uint64(uint32(int32(r[ins.Dst()]) >> uint32(r[ins.Src()])))
		case OpArsh64Imm:
			r[ins.Dst()] = uint64(int64(r[ins.Dst()]) >> ins.Imm())
		case OpArsh64Reg:
			r[ins.Dst()] = uint64(int64(r[ins.Dst()]) >> (r[ins.Src()]))
		case OpCall:
			// Internal calls only; syscalls and unresolved targets take Run's path.
			var target int64
			if f.staticSyscalls {
				if ins.Src() != 1 {
					return pc, due
				}
				target = ip.sbpfVersion.CalculateCallImmTargetPC(pc, ins.Imm())
				if target < 0 || target >= int64(len(text)) {
					return pc, due
				}
			} else {
				if !f.fastCalls {
					return pc, due
				}
				target = callTargets[pc]
				if target < 0 {
					return pc, due
				}
			}
			// Stack.Push, inlined (it exceeds the inlining budget).
			n := len(stack.shadow)
			if n >= cap(stack.shadow) {
				return pc, due
			}
			stack.shadow = stack.shadow[:n+1]
			fr := &stack.shadow[n]
			fr.RetAddr = pc + 1
			fr.NVRegs[0] = r[6]
			fr.NVRegs[1] = r[7]
			fr.NVRegs[2] = r[8]
			fr.NVRegs[3] = r[9]
			fr.FramePtr = r[10]
			if n+1 > stack.maxDepth {
				stack.maxDepth = n + 1
			}
			if !stack.dynamicStackFrames {
				if stack.stackFrameGaps {
					r[10] += StackFrameSize * 2
				} else {
					r[10] += StackFrameSize
				}
			}
			// Keep one loop latch (the shared due++/pc++ below): a second
			// back edge moves that tail away from the dispatch and costs a
			// taken jump per instruction.
			pc = target - 1
		case OpExit:
			// Stack.Pop, inlined. Returning from the entry frame is left to Run.
			n := len(stack.shadow)
			if n <= 1 {
				return pc, due
			}
			fr := &stack.shadow[n-1]
			r[6] = fr.NVRegs[0]
			r[7] = fr.NVRegs[1]
			r[8] = fr.NVRegs[2]
			r[9] = fr.NVRegs[3]
			r[10] = fr.FramePtr
			stack.shadow = stack.shadow[:n-1]
			pc = fr.RetAddr - 1
		default:
			return pc, due
		}
		due++
		pc++
	}
}
