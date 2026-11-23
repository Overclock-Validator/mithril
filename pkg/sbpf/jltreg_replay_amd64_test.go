//go:build amd64 && !purego

package sbpf

import (
	"testing"
)

func TestReplayJltRegIncremental(t *testing.T) {
	eqID, neID, ltID := asmJeqImmJcc, asmJneImmJcc, asmJltRegJcc
	if !useAsmLoop {
		t.Skip("requires threaded ASM")
	}
	for version := range uint32(4) {
		vm := newAsmStepVM(t, version, false, 3)
		ip := vm.ip
		flags := ip.fastFlags()
		const pc = 2
		ip.text = make([]Slot, pc+1)
		table := ip.asmLoopTable(flags)
		if table[OpJeqImm] != asmHandlerTable()[eqID] || table[OpJneImm] != asmHandlerTable()[neID] {
			t.Fatal("unexpected equality dispatch")
		}
		values := []uint64{0, 1, (1 << 32) - 1, 1 << 32, (1 << 63) - 1, 1 << 63, ^uint64(0)}
		if table[OpJltReg] != asmHandlerTable()[ltID] {
			t.Fatal("unexpected less-than dispatch")
		}
		for _, registers := range [][2]uint8{{0, 1}, {6, 2}, {8, 8}, {9, 0}} {
			dst, src := registers[0], registers[1]
			for _, x := range values {
				for _, y := range values {
					for _, offset := range []int16{-32768, -3, -1, 0, 1, 2, 32767} {
						ip.text[pc] = slot(OpJltReg, dst, src, offset, 0)
						var before [16]uint64
						before[dst], before[src] = x, y
						goRegs, asmRegs := before, before
						expected := int64(pc + 1)
						if before[dst] < before[src] {
							expected += int64(offset)
						}
						gpc, due := ip.runFast(&goRegs, pc, 0, 1, flags)
						apc, left := runAsmLoop(&ip.text[0], int64(len(ip.text)), &asmRegs, &ip.regions, table, &ip.stack, pc, 1, nil)
						if gpc != expected || apc != expected || due != 1 || left != 0 || goRegs != before || asmRegs != before {
							t.Fatalf("v%d r%d=%#x r%d=%#x off %d: Go, ASM, expected %d/%d/%d", version, dst, before[dst], src, before[src], offset, gpc, apc, expected)
						}
					}
				}
			}
		}
		vm.finish()
	}
}

func TestReplayCPIBaseline(t *testing.T) {
	eqID, neID, ltID := asmJeqImmJcc, asmJneImmJcc, asmJltRegJcc
	h := asmHandlerTable()
	for ver := range uint32(4) {
		vm := newAsmStepVM(t, ver, false, 2)
		f := vm.ip.fastFlags()
		for gap := range 2 {
			table := buildAsmTable(f, gap != 0)
			loadOp := uint8(OpLdxdw)
			if ver == 2 {
				loadOp = OpLd8BReg
				if table[OpLmul64Imm] != h[asmBail] || table[OpLmul64Reg] != h[asmBail] {
					t.Fatal("v2 fast multiply unexpectedly active")
				}
			}
			load := asmLdxdwNoGap
			if gap != 0 {
				load = asmLdxdw
			}
			if table[loadOp] != h[load] || table[OpJeqImm] != h[eqID] || table[OpJneImm] != h[neID] || table[OpJltReg] != h[ltID] {
				t.Fatalf("incorrect handler baseline v%d gap=%d", ver, gap)
			}
		}
		vm.finish()
	}
}
