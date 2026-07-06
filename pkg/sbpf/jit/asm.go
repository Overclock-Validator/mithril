package jit

import "encoding/binary"

// x86-64 register numbers.
const (
	rax = 0
	rcx = 1
	rdx = 2
	rbx = 3
	rsp = 4
	rbp = 5
	rsi = 6
	rdi = 7
	r8  = 8
	r9  = 9
	r10 = 10
	r11 = 11
	r12 = 12
	r13 = 13
	r14 = 14
	r15 = 15
)

// sbfToX86 maps SBF r0..r10 onto x86 registers. RAX, RDX and R15 stay
// scratch (RAX:RDX are implicit in mul/div, R15 holds immediates), RBP
// is the context pointer and RSP the native stack.
var sbfToX86 = [11]uint8{rbx, rcx, rsi, rdi, r8, r9, r10, r11, r12, r13, r14}

// asm is a minimal x86-64 byte emitter.
type asm struct {
	buf []byte
}

func (a *asm) here() int32     { return int32(len(a.buf)) }
func (a *asm) byte(bs ...byte) { a.buf = append(a.buf, bs...) }
func (a *asm) u32(v uint32)    { a.buf = binary.LittleEndian.AppendUint32(a.buf, v) }
func (a *asm) u64(v uint64)    { a.buf = binary.LittleEndian.AppendUint64(a.buf, v) }

// rex emits a REX prefix. w selects 64-bit operands; reg/rm are full
// 4-bit register numbers whose high bits land in REX.R/REX.B.
func (a *asm) rex(w bool, reg, rm uint8) {
	b := byte(0x40)
	if w {
		b |= 8
	}
	if reg >= 8 {
		b |= 4
	}
	if rm >= 8 {
		b |= 1
	}
	if b != 0x40 {
		a.byte(b)
	}
}

// rex32 emits REX only when needed for extended registers.
func (a *asm) rex32(reg, rm uint8) {
	b := byte(0x40)
	if reg >= 8 {
		b |= 4
	}
	if rm >= 8 {
		b |= 1
	}
	if b != 0x40 {
		a.byte(b)
	}
}

func modrmReg(reg, rm uint8) byte {
	return 0xC0 | (reg&7)<<3 | (rm & 7)
}

// modrmMemBP emits a [RBP+disp32] operand for the given reg field.
func (a *asm) modrmMemBP(reg uint8, disp int32) {
	a.byte(0x80 | (reg&7)<<3 | rbp)
	a.u32(uint32(disp))
}

// aluRegReg emits `op r/m64, r64` style ALU (opcode is the /r store form).
func (a *asm) aluRegReg(opcode byte, w bool, src, dst uint8) {
	a.rex(w, src, dst)
	a.byte(opcode, modrmReg(src, dst))
}

const (
	aluAdd  = 0x01
	aluOr   = 0x09
	aluAnd  = 0x21
	aluSub  = 0x29
	aluXor  = 0x31
	aluCmp  = 0x39
	aluTest = 0x85
	aluMov  = 0x89
)

// aluImm emits `op r/m64, imm32` (81 /ext, imm sign-extended).
func (a *asm) aluImm(ext uint8, w bool, dst uint8, imm int32) {
	a.rex(w, 0, dst)
	a.byte(0x81, modrmReg(ext, dst))
	a.u32(uint32(imm))
}

const (
	extAdd = 0
	extOr  = 1
	extAnd = 4
	extSub = 5
	extXor = 6
	extCmp = 7
)

func (a *asm) movRegReg64(dst, src uint8) { a.aluRegReg(aluMov, true, src, dst) }
func (a *asm) movRegReg32(dst, src uint8) { a.aluRegReg(aluMov, false, src, dst) } // zero-extends

// movRegImm64 emits movabs dst, imm64.
func (a *asm) movRegImm64(dst uint8, imm uint64) {
	a.rex(true, 0, dst)
	a.byte(0xB8 + dst&7)
	a.u64(imm)
}

// movRegImm32 emits mov dst32, imm32 (zero-extends).
func (a *asm) movRegImm32(dst uint8, imm uint32) {
	a.rex32(0, dst)
	a.byte(0xB8 + dst&7)
	a.u32(imm)
}

// movRegImm32sx emits mov dst64, imm32 sign-extended (C7 /0).
func (a *asm) movRegImm32sx(dst uint8, imm int32) {
	a.rex(true, 0, dst)
	a.byte(0xC7, modrmReg(0, dst))
	a.u32(uint32(imm))
}

// movsxd emits movsxd dst64, src32.
func (a *asm) movsxd(dst, src uint8) {
	a.rex(true, dst, src)
	a.byte(0x63, modrmReg(dst, src))
}

// loadCtx emits mov dst, [rbp+disp].
func (a *asm) loadCtx(dst uint8, disp int32) {
	a.rex(true, dst, rbp)
	a.byte(0x8B)
	a.modrmMemBP(dst, disp)
}

// storeCtx emits mov [rbp+disp], src.
func (a *asm) storeCtx(disp int32, src uint8) {
	a.rex(true, src, rbp)
	a.byte(0x89)
	a.modrmMemBP(src, disp)
}

// storeCtxImm32 emits mov qword [rbp+disp], imm32 (sign-extended).
func (a *asm) storeCtxImm32(disp int32, imm int32) {
	a.rex(true, 0, rbp)
	a.byte(0xC7)
	a.modrmMemBP(0, disp)
	a.u32(uint32(imm))
}

// addCtxImm32 emits add qword [rbp+disp], imm32.
func (a *asm) addCtxImm32(disp int32, imm int32) {
	a.rex(true, 0, rbp)
	a.byte(0x81)
	a.modrmMemBP(extAdd, disp)
	a.u32(uint32(imm))
}

// cmpRegCtx emits cmp reg, [rbp+disp].
func (a *asm) cmpRegCtx(reg uint8, disp int32) {
	a.rex(true, reg, rbp)
	a.byte(0x3B)
	a.modrmMemBP(reg, disp)
}

// group3 emits F7 /ext ops: neg (3), mul (4), div (6), idiv (7).
func (a *asm) group3(ext uint8, w bool, rm uint8) {
	a.rex(w, 0, rm)
	a.byte(0xF7, modrmReg(ext, rm))
}

// imulRegReg emits imul dst, src (0F AF).
func (a *asm) imulRegReg(w bool, dst, src uint8) {
	a.rex(w, dst, src)
	a.byte(0x0F, 0xAF, modrmReg(dst, src))
}

// imulRegImm emits imul dst, src, imm32 (69 /r).
func (a *asm) imulRegImm(w bool, dst, src uint8, imm int32) {
	a.rex(w, dst, src)
	a.byte(0x69, modrmReg(dst, src))
	a.u32(uint32(imm))
}

// testImm emits test r/m64, imm32 (F7 /0, imm sign-extended).
func (a *asm) testImm(rm uint8, imm int32) {
	a.rex(true, 0, rm)
	a.byte(0xF7, modrmReg(0, rm))
	a.u32(uint32(imm))
}

// cqo sign-extends RAX into RDX:RAX.
func (a *asm) cqo() { a.byte(0x48, 0x99) }

// xorEdxEdx zeroes RDX.
func (a *asm) xorEdxEdx() { a.byte(0x31, 0xD2) }

// shiftCL emits shl/shr/sar dst by CL (D3 /ext: shl=4, shr=5, sar=7).
func (a *asm) shiftCL(ext uint8, w bool, dst uint8) {
	a.rex(w, 0, dst)
	a.byte(0xD3, modrmReg(ext, dst))
}

// shiftImm emits shl/shr/sar dst by imm8 (C1 /ext).
func (a *asm) shiftImm(ext uint8, w bool, dst uint8, imm byte) {
	a.rex(w, 0, dst)
	a.byte(0xC1, modrmReg(ext, dst), imm)
}

const (
	shlExt = 4
	shrExt = 5
	sarExt = 7
)

// x86 condition codes for Jcc (0F 8x).
const (
	ccE  = 0x4
	ccNE = 0x5
	ccA  = 0x7
	ccAE = 0x3
	ccB  = 0x2
	ccBE = 0x6
	ccG  = 0xF
	ccGE = 0xD
	ccL  = 0xC
	ccLE = 0xE
)

// jcc emits a Jcc rel32 with a placeholder and returns the fixup site.
func (a *asm) jcc(cc byte) int32 {
	a.byte(0x0F, 0x80|cc)
	site := a.here()
	a.u32(0)
	return site
}

// jmp emits a JMP rel32 with a placeholder and returns the fixup site.
func (a *asm) jmp() int32 {
	a.byte(0xE9)
	site := a.here()
	a.u32(0)
	return site
}

// patch writes target-relative displacement into a rel32 fixup site.
func (a *asm) patch(site int32, target int32) {
	binary.LittleEndian.PutUint32(a.buf[site:], uint32(target-(site+4)))
}

func (a *asm) ret() { a.byte(0xC3) }

// bswap emits bswap on the full or 32-bit register.
func (a *asm) bswap(w bool, reg uint8) {
	a.rex(w, 0, reg)
	a.byte(0x0F, 0xC8+reg&7)
}

// rolReg16Imm8 rotates the 16-bit register left by imm.
func (a *asm) rolReg16Imm8(reg uint8, imm byte) {
	a.byte(0x66)
	a.rex32(0, reg)
	a.byte(0xC1, modrmReg(0, reg), imm)
}

// movzxReg16 emits movzx dst64, src16.
func (a *asm) movzxReg16(dst, src uint8) {
	a.rex(true, dst, src)
	a.byte(0x0F, 0xB7, modrmReg(dst, src))
}
