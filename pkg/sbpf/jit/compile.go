package jit

import (
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
)

// ErrUnsupported means the program uses instructions the compiler does
// not handle; callers fall back to the interpreter.
var ErrUnsupported = errors.New("jit: unsupported instruction")

// Compiled is a program lowered to native code.
type Compiled struct {
	mem      *execMem
	entryOff int32
	// refundAfter[pc] is the number of instructions in pc's basic block
	// strictly after pc: block charging happens at block entry, so a
	// fault at pc refunds this many instructions.
	refundAfter []uint32
	textLen     int
}

func (c *Compiled) Free() {
	if c.mem != nil {
		c.mem.free()
	}
}

// compiler carries per-compilation state.
type compiler struct {
	asm       asm
	text      []sbpf.Slot
	insnOff   []int32 // sbf pc -> native offset; -1 for lddw's second slot
	stubFix   map[int][]int32
	jumpFix   []jumpFixup
	blockLen  map[int]uint32 // leader pc -> instruction count
	leaders   map[int]bool
	lddwSlot2 map[int]bool
	stackGaps bool // when set, memory ops are unsupported (can't prove region)
}

type jumpFixup struct {
	site     int32
	targetPC int
}

const (
	stubExit = iota
	stubOOCU
	stubDiv0
	stubOverrun
	stubBadAccess
)

// Compile lowers a verified SBPF v0 program to amd64 code. Programs
// using unsupported instructions return ErrUnsupported. stackGaps
// reports whether the runtime maps the stack with frame gaps (v0
// default); when set, memory instructions are unsupported because their
// region cannot be proven at compile time.
func Compile(text []sbpf.Slot, ver sbpfver.SbpfVersion, entry uint64, stackGaps bool) (*Compiled, error) {
	if ver.Version != sbpfver.SbpfVersionV0 {
		return nil, ErrUnsupported
	}
	if len(text) == 0 || entry >= uint64(len(text)) {
		return nil, ErrUnsupported
	}

	c := &compiler{
		text:      text,
		insnOff:   make([]int32, len(text)),
		stubFix:   map[int][]int32{},
		blockLen:  map[int]uint32{},
		leaders:   map[int]bool{},
		lddwSlot2: map[int]bool{},
		stackGaps: stackGaps,
	}

	if err := c.analyze(int(entry)); err != nil {
		return nil, err
	}
	refund := c.computeRefunds()

	if err := c.emit(); err != nil {
		return nil, err
	}

	mem, err := newExecMem(c.asm.buf)
	if err != nil {
		return nil, err
	}
	return &Compiled{
		mem:         mem,
		entryOff:    c.insnOff[entry],
		refundAfter: refund,
		textLen:     len(text),
	}, nil
}

// analyze finds lddw second slots, validates ops and jump targets, and
// marks basic-block leaders.
func (c *compiler) analyze(entry int) error {
	for pc := 0; pc < len(c.text); pc++ {
		if c.text[pc].Op() == sbpf.OpLddw {
			if pc+1 >= len(c.text) {
				return ErrUnsupported
			}
			c.lddwSlot2[pc+1] = true
			pc++
		}
	}
	if c.lddwSlot2[entry] {
		return ErrUnsupported
	}
	c.leaders[entry] = true

	for pc := 0; pc < len(c.text); pc++ {
		if c.lddwSlot2[pc] {
			continue
		}
		ins := c.text[pc]
		if !c.supported(ins) {
			return ErrUnsupported
		}
		if op := ins.Op(); isJump(op) {
			if op != sbpf.OpJa {
				// conditional: fall-through starts a block
				if pc+1 < len(c.text) {
					c.leaders[pc+1] = true
				}
			}
			target := pc + int(ins.Off()) + 1
			if target < 0 || target >= len(c.text) || c.lddwSlot2[target] {
				return ErrUnsupported
			}
			c.leaders[target] = true
			if pc+1 < len(c.text) && op == sbpf.OpJa {
				c.leaders[pc+1] = true
			}
		}
		if ins.Op() == sbpf.OpExit && pc+1 < len(c.text) {
			c.leaders[pc+1] = true
		}
	}
	return nil
}

// computeRefunds derives per-block instruction counts and the per-pc
// refund table.
func (c *compiler) computeRefunds() []uint32 {
	refund := make([]uint32, len(c.text))
	blockStart := 0
	count := uint32(0)
	flush := func(end int) {
		c.blockLen[blockStart] = count
		remaining := count
		for pc := blockStart; pc < end; pc++ {
			if c.lddwSlot2[pc] {
				continue
			}
			remaining--
			refund[pc] = remaining
		}
	}
	for pc := 0; pc < len(c.text); pc++ {
		if c.lddwSlot2[pc] {
			continue
		}
		if c.leaders[pc] && pc != blockStart {
			flush(pc)
			blockStart = pc
			count = 0
		}
		count++
	}
	flush(len(c.text))
	return refund
}

func isJump(op uint8) bool {
	switch op {
	case sbpf.OpJa,
		sbpf.OpJeqImm, sbpf.OpJeqReg, sbpf.OpJgtImm, sbpf.OpJgtReg,
		sbpf.OpJgeImm, sbpf.OpJgeReg, sbpf.OpJltImm, sbpf.OpJltReg,
		sbpf.OpJleImm, sbpf.OpJleReg, sbpf.OpJsetImm, sbpf.OpJsetReg,
		sbpf.OpJneImm, sbpf.OpJneReg, sbpf.OpJsgtImm, sbpf.OpJsgtReg,
		sbpf.OpJsgeImm, sbpf.OpJsgeReg, sbpf.OpJsltImm, sbpf.OpJsltReg,
		sbpf.OpJsleImm, sbpf.OpJsleReg:
		return true
	}
	return false
}

func isMemOp(op uint8) bool {
	switch op {
	case sbpf.OpLdxb, sbpf.OpLdxh, sbpf.OpLdxw, sbpf.OpLdxdw,
		sbpf.OpStb, sbpf.OpSth, sbpf.OpStw, sbpf.OpStdw,
		sbpf.OpStxb, sbpf.OpStxh, sbpf.OpStxw, sbpf.OpStxdw:
		return true
	}
	return false
}

func memSize(op uint8) int {
	switch op {
	case sbpf.OpLdxb, sbpf.OpStb, sbpf.OpStxb:
		return 1
	case sbpf.OpLdxh, sbpf.OpSth, sbpf.OpStxh:
		return 2
	case sbpf.OpLdxw, sbpf.OpStw, sbpf.OpStxw:
		return 4
	default:
		return 8
	}
}

func (c *compiler) supported(ins sbpf.Slot) bool {
	op := ins.Op()
	if isJump(op) || op == sbpf.OpExit || op == sbpf.OpLddw {
		return true
	}
	if isMemOp(op) {
		return !c.stackGaps
	}
	switch op {
	case sbpf.OpAdd32Imm, sbpf.OpAdd32Reg, sbpf.OpAdd64Imm, sbpf.OpAdd64Reg,
		sbpf.OpSub32Imm, sbpf.OpSub32Reg, sbpf.OpSub64Imm, sbpf.OpSub64Reg,
		sbpf.OpMul32Imm, sbpf.OpMul32Reg, sbpf.OpMul64Imm, sbpf.OpMul64Reg,
		sbpf.OpOr32Imm, sbpf.OpOr32Reg, sbpf.OpOr64Imm, sbpf.OpOr64Reg,
		sbpf.OpAnd32Imm, sbpf.OpAnd32Reg, sbpf.OpAnd64Imm, sbpf.OpAnd64Reg,
		sbpf.OpXor32Imm, sbpf.OpXor32Reg, sbpf.OpXor64Imm, sbpf.OpXor64Reg,
		sbpf.OpMov32Imm, sbpf.OpMov32Reg, sbpf.OpMov64Imm, sbpf.OpMov64Reg,
		sbpf.OpLsh32Imm, sbpf.OpLsh32Reg, sbpf.OpLsh64Imm, sbpf.OpLsh64Reg,
		sbpf.OpRsh32Imm, sbpf.OpRsh32Reg, sbpf.OpRsh64Imm, sbpf.OpRsh64Reg,
		sbpf.OpArsh32Imm, sbpf.OpArsh32Reg, sbpf.OpArsh64Reg,
		sbpf.OpNeg32, sbpf.OpNeg64,
		sbpf.OpDiv32Reg, sbpf.OpDiv64Reg, sbpf.OpMod32Reg, sbpf.OpMod64Reg:
		return true
	case sbpf.OpArsh64Imm:
		return ins.Imm() >= 0
	case sbpf.OpDiv32Imm, sbpf.OpDiv64Imm, sbpf.OpMod32Imm, sbpf.OpMod64Imm:
		return ins.Uimm() != 0
	case sbpf.OpLe, sbpf.OpBe:
		switch ins.Uimm() {
		case 16, 32, 64:
			return true
		}
		return false
	}
	return false
}

func (c *compiler) stubJump(stub int) {
	site := c.asm.jmp()
	c.stubFix[stub] = append(c.stubFix[stub], site)
}

// fault emits a conditional-taken fault exit: pc into RAX, jump to stub.
func (c *compiler) fault(stub, pc int) {
	c.asm.movRegImm32(rax, uint32(pc))
	c.stubJump(stub)
}

func (c *compiler) emit() error {
	a := &c.asm
	for pc := 0; pc < len(c.text); pc++ {
		if c.lddwSlot2[pc] {
			c.insnOff[pc] = -1
			continue
		}
		c.insnOff[pc] = a.here()

		if c.leaders[pc] {
			// Charge the block and check the budget.
			a.addCtxImm32(offCuDue, int32(c.blockLen[pc]))
			a.loadCtx(rax, offCuDue)
			a.cmpRegCtx(rax, offCuLeft)
			ok := a.jcc(ccBE)
			c.fault(stubOOCU, pc)
			a.patch(ok, a.here())
		}

		if err := c.emitIns(pc); err != nil {
			return err
		}
	}

	// Fall off the end of text: execution overrun.
	c.fault(stubOverrun, len(c.text))

	// Stubs.
	stubOff := map[int]int32{}
	stubOff[stubExit] = a.here()
	a.storeCtxImm32(offExitReason, exitExited)
	a.ret()
	for _, s := range []struct {
		id     int
		reason int32
	}{{stubOOCU, exitOOCU}, {stubDiv0, exitDivZero}, {stubOverrun, exitOverrun}, {stubBadAccess, exitBadAccess}} {
		stubOff[s.id] = a.here()
		a.storeCtx(offExitPC, rax)
		a.storeCtxImm32(offExitReason, s.reason)
		a.ret()
	}

	for stub, sites := range c.stubFix {
		for _, site := range sites {
			a.patch(site, stubOff[stub])
		}
	}
	for _, f := range c.jumpFix {
		a.patch(f.site, c.insnOff[f.targetPC])
	}
	return nil
}

func (c *compiler) emitIns(pc int) error {
	a := &c.asm
	ins := c.text[pc]
	op := ins.Op()
	dst := sbfToX86[ins.Dst()]
	var src uint8
	if s := ins.Src(); s < 11 {
		src = sbfToX86[s]
	} else if opUsesSrc(op) {
		return ErrUnsupported
	}
	imm := ins.Imm()

	switch op {
	case sbpf.OpLdxb, sbpf.OpLdxh, sbpf.OpLdxw, sbpf.OpLdxdw:
		c.emitLoad(dst, src, ins.Off(), memSize(op), pc)
	case sbpf.OpStxb, sbpf.OpStxh, sbpf.OpStxw, sbpf.OpStxdw:
		c.emitStoreReg(dst, src, ins.Off(), memSize(op), pc)
	case sbpf.OpStb, sbpf.OpSth, sbpf.OpStw, sbpf.OpStdw:
		c.emitStoreImm(dst, ins.Off(), imm, memSize(op), pc)

	case sbpf.OpAdd32Imm:
		a.aluImm(extAdd, false, dst, imm)
		a.movsxd(dst, dst)
	case sbpf.OpAdd32Reg:
		a.aluRegReg(aluAdd, false, src, dst)
		a.movsxd(dst, dst)
	case sbpf.OpAdd64Imm:
		a.aluImm(extAdd, true, dst, imm)
	case sbpf.OpAdd64Reg:
		a.aluRegReg(aluAdd, true, src, dst)
	case sbpf.OpSub32Imm:
		a.aluImm(extSub, false, dst, imm)
		a.movsxd(dst, dst)
	case sbpf.OpSub32Reg:
		a.aluRegReg(aluSub, false, src, dst)
		a.movsxd(dst, dst)
	case sbpf.OpSub64Imm:
		a.aluImm(extSub, true, dst, imm)
	case sbpf.OpSub64Reg:
		a.aluRegReg(aluSub, true, src, dst)
	case sbpf.OpMul32Imm:
		a.imulRegImm(false, dst, dst, imm)
		a.movsxd(dst, dst)
	case sbpf.OpMul32Reg:
		a.imulRegReg(false, dst, src)
		a.movsxd(dst, dst)
	case sbpf.OpMul64Imm:
		a.imulRegImm(true, dst, dst, imm)
	case sbpf.OpMul64Reg:
		a.imulRegReg(true, dst, src)
	case sbpf.OpOr32Imm:
		a.aluImm(extOr, false, dst, imm)
	case sbpf.OpOr32Reg:
		a.aluRegReg(aluOr, false, src, dst)
	case sbpf.OpOr64Imm:
		a.aluImm(extOr, true, dst, imm)
	case sbpf.OpOr64Reg:
		a.aluRegReg(aluOr, true, src, dst)
	case sbpf.OpAnd32Imm:
		a.aluImm(extAnd, false, dst, imm)
	case sbpf.OpAnd32Reg:
		a.aluRegReg(aluAnd, false, src, dst)
	case sbpf.OpAnd64Imm:
		a.aluImm(extAnd, true, dst, imm)
	case sbpf.OpAnd64Reg:
		a.aluRegReg(aluAnd, true, src, dst)
	case sbpf.OpXor32Imm:
		a.aluImm(extXor, false, dst, imm)
	case sbpf.OpXor32Reg:
		a.aluRegReg(aluXor, false, src, dst)
	case sbpf.OpXor64Imm:
		a.aluImm(extXor, true, dst, imm)
	case sbpf.OpXor64Reg:
		a.aluRegReg(aluXor, true, src, dst)
	case sbpf.OpMov32Imm:
		a.movRegImm32(dst, ins.Uimm())
	case sbpf.OpMov32Reg:
		a.movRegReg32(dst, src)
	case sbpf.OpMov64Imm:
		a.movRegImm32sx(dst, imm)
	case sbpf.OpMov64Reg:
		a.movRegReg64(dst, src)
	case sbpf.OpNeg32:
		a.group3(3, false, dst)
		a.movsxd(dst, dst)
	case sbpf.OpNeg64:
		a.group3(3, true, dst)

	case sbpf.OpLsh32Imm:
		a.shiftImm(shlExt, false, dst, byte(ins.Uimm()&0x1f))
	case sbpf.OpRsh32Imm:
		a.shiftImm(shrExt, false, dst, byte(ins.Uimm()&0x1f))
	case sbpf.OpArsh32Imm:
		count := ins.Uimm()
		if count > 31 {
			count = 31
		}
		a.shiftImm(sarExt, false, dst, byte(count))
	case sbpf.OpLsh64Imm:
		a.shiftImm(shlExt, true, dst, byte(uint64(imm)&0x3f))
	case sbpf.OpRsh64Imm:
		a.shiftImm(shrExt, true, dst, byte(uint64(imm)&0x3f))
	case sbpf.OpArsh64Imm:
		count := imm
		if count > 63 {
			count = 63
		}
		a.shiftImm(sarExt, true, dst, byte(count))
	case sbpf.OpLsh32Reg:
		c.shiftReg(shlExt, false, dst, src, 0)
	case sbpf.OpRsh32Reg:
		c.shiftReg(shrExt, false, dst, src, 0)
	case sbpf.OpArsh32Reg:
		c.shiftReg(sarExt, false, dst, src, 31)
	case sbpf.OpLsh64Reg:
		c.shiftReg(shlExt, true, dst, src, 0)
	case sbpf.OpRsh64Reg:
		c.shiftReg(shrExt, true, dst, src, 0)
	case sbpf.OpArsh64Reg:
		c.shiftReg(sarExt, true, dst, src, 63)

	case sbpf.OpDiv32Imm:
		a.movRegImm32(r15, ins.Uimm())
		c.divide(false, false, dst, r15)
	case sbpf.OpDiv64Imm:
		a.movRegImm32sx(r15, imm)
		c.divide(true, false, dst, r15)
	case sbpf.OpMod32Imm:
		a.movRegImm32(r15, ins.Uimm())
		c.divide(false, true, dst, r15)
	case sbpf.OpMod64Imm:
		a.movRegImm32sx(r15, imm)
		c.divide(true, true, dst, r15)
	case sbpf.OpDiv32Reg:
		c.divRegChecked(false, false, dst, src, pc)
	case sbpf.OpDiv64Reg:
		c.divRegChecked(true, false, dst, src, pc)
	case sbpf.OpMod32Reg:
		c.divRegChecked(false, true, dst, src, pc)
	case sbpf.OpMod64Reg:
		c.divRegChecked(true, true, dst, src, pc)

	case sbpf.OpLe:
		switch ins.Uimm() {
		case 16:
			a.aluImm(extAnd, true, dst, 0xFFFF)
		case 32:
			a.movRegReg32(dst, dst)
		case 64:
			// no-op
		}
	case sbpf.OpBe:
		switch ins.Uimm() {
		case 16:
			a.rolReg16Imm8(dst, 8)
			a.movzxReg16(dst, dst)
		case 32:
			a.bswap(false, dst)
		case 64:
			a.bswap(true, dst)
		}

	case sbpf.OpLddw:
		lo := uint64(ins.Uimm())
		hi := uint64(c.text[pc+1].Uimm())
		a.movRegImm64(dst, lo|hi<<32)

	case sbpf.OpJa:
		c.jumpFix = append(c.jumpFix, jumpFixup{a.jmp(), pc + int(ins.Off()) + 1})

	case sbpf.OpJeqImm, sbpf.OpJgtImm, sbpf.OpJgeImm, sbpf.OpJltImm,
		sbpf.OpJleImm, sbpf.OpJneImm, sbpf.OpJsgtImm, sbpf.OpJsgeImm,
		sbpf.OpJsltImm, sbpf.OpJsleImm:
		a.aluImm(extCmp, true, dst, imm)
		c.jumpFix = append(c.jumpFix, jumpFixup{a.jcc(jumpCC(op)), pc + int(ins.Off()) + 1})
	case sbpf.OpJeqReg, sbpf.OpJgtReg, sbpf.OpJgeReg, sbpf.OpJltReg,
		sbpf.OpJleReg, sbpf.OpJneReg, sbpf.OpJsgtReg, sbpf.OpJsgeReg,
		sbpf.OpJsltReg, sbpf.OpJsleReg:
		a.aluRegReg(aluCmp, true, src, dst)
		c.jumpFix = append(c.jumpFix, jumpFixup{a.jcc(jumpCC(op)), pc + int(ins.Off()) + 1})
	case sbpf.OpJsetImm:
		a.testImm(dst, imm)
		c.jumpFix = append(c.jumpFix, jumpFixup{a.jcc(ccNE), pc + int(ins.Off()) + 1})
	case sbpf.OpJsetReg:
		a.aluRegReg(aluTest, true, src, dst)
		c.jumpFix = append(c.jumpFix, jumpFixup{a.jcc(ccNE), pc + int(ins.Off()) + 1})

	case sbpf.OpExit:
		c.stubJump(stubExit)

	default:
		return fmt.Errorf("%w: op %#x", ErrUnsupported, op)
	}
	return nil
}

func opUsesSrc(op uint8) bool {
	// SrcX-form ALU/jump ops read the src register field.
	return op&0x08 != 0
}

func jumpCC(op uint8) byte {
	switch op {
	case sbpf.OpJeqImm, sbpf.OpJeqReg:
		return ccE
	case sbpf.OpJneImm, sbpf.OpJneReg:
		return ccNE
	case sbpf.OpJgtImm, sbpf.OpJgtReg:
		return ccA
	case sbpf.OpJgeImm, sbpf.OpJgeReg:
		return ccAE
	case sbpf.OpJltImm, sbpf.OpJltReg:
		return ccB
	case sbpf.OpJleImm, sbpf.OpJleReg:
		return ccBE
	case sbpf.OpJsgtImm, sbpf.OpJsgtReg:
		return ccG
	case sbpf.OpJsgeImm, sbpf.OpJsgeReg:
		return ccGE
	case sbpf.OpJsltImm, sbpf.OpJsltReg:
		return ccL
	case sbpf.OpJsleImm, sbpf.OpJsleReg:
		return ccLE
	}
	panic("not a conditional jump")
}

// shiftReg emits a shift of dst by the count in src. clamp caps the
// count for SBF's unmasked arithmetic shifts (0 = rely on hardware
// masking, which matches SBF's masked shifts).
func (c *compiler) shiftReg(ext uint8, w bool, dst, src uint8, clamp uint32) {
	a := &c.asm
	// Count into RAX. Unmasked 32-bit shifts truncate the count to
	// uint32 first, so load accordingly.
	if clamp != 0 && !w {
		a.movRegReg32(rax, src)
	} else {
		a.movRegReg64(rax, src)
	}
	if clamp != 0 {
		a.aluImm(extCmp, true, rax, int32(clamp))
		ok := a.jcc(ccBE)
		a.movRegImm32(rax, clamp)
		a.patch(ok, a.here())
	}
	// Save RCX (may hold SBF r1), move the count in, shift, restore.
	a.movRegReg64(r15, rcx)
	a.movRegReg64(rcx, rax)
	if dst == rcx {
		a.shiftCL(ext, w, r15)
		a.movRegReg64(rcx, r15)
	} else {
		a.shiftCL(ext, w, dst)
		a.movRegReg64(rcx, r15)
	}
}

// divide emits unsigned division dst /= by (or dst %= by when mod).
// The divisor must not be RAX or RDX.
func (c *compiler) divide(w bool, mod bool, dst, by uint8) {
	a := &c.asm
	if w {
		a.movRegReg64(rax, dst)
	} else {
		a.movRegReg32(rax, dst)
	}
	a.xorEdxEdx()
	a.group3(6, w, by)
	res := uint8(rax)
	if mod {
		res = rdx
	}
	if w {
		a.movRegReg64(dst, res)
	} else {
		a.movRegReg32(dst, res)
	}
}

// divRegChecked emits the zero check and fault exit around divide.
func (c *compiler) divRegChecked(w bool, mod bool, dst, src uint8, pc int) {
	a := &c.asm
	a.aluRegReg(aluTest, w, src, src)
	ok := a.jcc(ccNE)
	c.fault(stubDiv0, pc)
	a.patch(ok, a.here())
	c.divide(w, mod, dst, src)
}
