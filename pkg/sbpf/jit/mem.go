package jit

import "github.com/Overclock-Validator/mithril/pkg/sbpf"

// emitTranslate lowers a virtual-address bounds check. On entry RAX
// holds the virtual address; on success RAX holds the host pointer.
// Any out-of-bounds or wrong-permission access jumps to a fault block
// that records pc and exits. Clobbers RAX, RDX, R15.
//
// Only the four v0 regions with linear host mappings are handled:
// program rodata (read-only, hi=1), stack (hi=2), heap (hi=3) and a
// single input region (hi=4). Programs needing stack-frame gaps or
// multiple input regions are rejected at compile time.
func (c *compiler) emitTranslate(size int, write bool, pc int) {
	a := &c.asm

	a.movRegReg64(rdx, rax) // rdx = region tag
	a.shiftImm(shrExt, true, rdx, 32)
	a.movRegReg32(r15, rax) // r15 = low32 offset

	a.aluImm(extCmp, true, rdx, 3)
	jHeap := a.jcc(ccE)
	a.aluImm(extCmp, true, rdx, 4)
	jInput := a.jcc(ccE)
	a.aluImm(extCmp, true, rdx, 2)
	jStack := a.jcc(ccE)
	a.aluImm(extCmp, true, rdx, 1)
	jRo := a.jcc(ccE)

	// Fall-through: unmapped region.
	fault := a.here()
	a.movRegImm32(rax, uint32(pc))
	c.stubJump(stubBadAccess)

	// bounds emits `lo+size <= [ctx+lenOff]` else jump to fault, then
	// host = [ctx+baseOff] + lo.
	bounds := func(baseOff, lenOff int32) {
		a.movRegReg64(rax, r15)
		a.aluImm(extAdd, true, rax, int32(size))
		a.cmpRegCtx(rax, lenOff)
		a.patch(a.jcc(ccA), fault)
		a.loadCtx(rax, baseOff)
		a.aluRegReg(aluAdd, true, r15, rax)
	}

	a.patch(jHeap, a.here())
	bounds(offHeapBase, offHeapLen)
	dHeap := a.jmp()

	a.patch(jInput, a.here())
	bounds(offInputBase, offInputLen)
	dInput := a.jmp()

	a.patch(jStack, a.here())
	// Stack is a fixed StackMax window (gaps rejected at compile time).
	a.movRegReg64(rax, r15)
	a.aluImm(extAdd, true, rax, int32(size))
	a.aluImm(extCmp, true, rax, int32(sbpf.StackMax))
	a.patch(a.jcc(ccA), fault)
	a.loadCtx(rax, offStackBase)
	a.aluRegReg(aluAdd, true, r15, rax)
	dStack := a.jmp()

	a.patch(jRo, a.here())
	if write {
		a.patch(a.jmp(), fault)
	} else {
		bounds(offRoBase, offRoLen)
	}

	done := a.here()
	a.patch(dHeap, done)
	a.patch(dInput, done)
	a.patch(dStack, done)
}

// emitLoad emits `dst = zeroext(mem[r[baseSbf]+off])`.
func (c *compiler) emitLoad(dst, baseX86 uint8, off int16, size, pc int) {
	a := &c.asm
	a.movRegReg64(rax, baseX86)
	a.aluImm(extAdd, true, rax, int32(off))
	c.emitTranslate(size, false, pc)
	a.loadZX(dst, size)
}

// emitStoreReg emits `mem[r[baseSbf]+off] = low_size(r[srcSbf])`.
func (c *compiler) emitStoreReg(baseX86, srcX86 uint8, off int16, size, pc int) {
	a := &c.asm
	a.movRegReg64(rax, baseX86)
	a.aluImm(extAdd, true, rax, int32(off))
	c.emitTranslate(size, true, pc)
	a.storeReg(srcX86, size)
}

// emitStoreImm emits `mem[r[baseSbf]+off] = imm`.
func (c *compiler) emitStoreImm(baseX86 uint8, off int16, imm int32, size, pc int) {
	a := &c.asm
	a.movRegReg64(rax, baseX86)
	a.aluImm(extAdd, true, rax, int32(off))
	c.emitTranslate(size, true, pc)
	a.storeImm(size, imm)
}
