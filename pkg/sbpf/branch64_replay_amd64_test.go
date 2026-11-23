//go:build amd64 && !purego

package sbpf

import (
	"testing"
)

// Compare the complete opcode table against the unmodified table and use the
// Go fast path as a reference for a one-instruction budget and exact PC.
func TestReplayBranch64Selection(t *testing.T) {
	if !useAsmLoop {
		t.Skip("requires threaded ASM")
	}
	eqID, neID := asmJeqImmJcc, asmJneImmJcc
	const startPC = 2
	for _, version := range []uint32{0, 1, 2, 3} {
		vm := newAsmStepVM(t, version, false, 3)
		ip := vm.ip
		flags := ip.fastFlags()
		ip.text = make([]Slot, startPC+1)
		table := ip.asmLoopTable(flags)
		if table[OpJeqImm] != asmHandlerTable()[eqID] || table[OpJneImm] != asmHandlerTable()[neID] {
			t.Fatal("unexpected equality dispatch")
		}
		for _, op := range []uint8{OpJeqImm, OpJneImm} {
			for _, imm := range []uint32{0, 1, 0x80000000, 0xffffffff} {
				equal := uint64(int64(int32(imm)))
				for _, dst := range []uint64{equal, equal ^ 1, equal ^ (1 << 32)} {
					for _, offset := range []int16{-3, -1, 0, 2, 32767} {
						ip.text[startPC] = slot(op, 3, 0, offset, imm)
						regs := [16]uint64{3: dst}
						gr, ar := regs, regs
						gpc, due := ip.runFast(&gr, startPC, 0, 1, flags)
						apc, left := runAsmLoop(&ip.text[0], int64(len(ip.text)), &ar, &ip.regions, table, &ip.stack, startPC, 1, nil)
						expected := int64(startPC + 1)
						if (op == OpJeqImm) == (dst == equal) {
							expected += int64(offset)
						}
						if gpc != expected || apc != expected || due != 1 || left != 0 || gr != regs || ar != regs {
							t.Fatalf("v%d op=%#x dst=%#x imm=%#x offset=%d got Go/asm/expected %d/%d/%d", version, op, dst, imm, offset, gpc, apc, expected)
						}
					}
				}
			}
		}
		vm.finish()
	}
}
