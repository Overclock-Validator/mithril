package sbpf

import (
	"fmt"
	"testing"
)

// Is pre-decoding the instruction stream worth it?
//
// Today Slot is a packed uint64 and Op/Dst/Src/Off/Imm are bit-extractions
// performed on every execution, so a loop body running a thousand times decodes
// itself a thousand times. Pre-decoding into a struct removes that work, and is
// also the front-end a JIT would need.
//
// The catch is footprint. A packed slot is 8 bytes; the decoded form below is
// 12, and Go pads it to 12 with imm first. Real programs are large -- the
// biggest mainnet program by compute has 925 KB of .text, about 115k
// instructions -- so a 1.5x instruction-stream expansion can push a hot loop
// out of a cache level. Two earlier "remove work" changes to this interpreter
// both failed for exactly that reason: bounds-check removal came out 4.3%
// slower, and a slice-free translate path was neutral, because the binding
// constraint here is memory traffic rather than instruction count.
//
// So this measures the trade directly, at program sizes spanning L1 to L3,
// without touching the interpreter. If decoded loses at realistic sizes, the
// 118-opcode refactor is not worth attempting.

// decodedSlot is the pre-decoded form. Field order puts imm first so the
// struct packs to 12 bytes rather than 16.
type decodedSlot struct {
	imm      int32
	off      int16
	op       uint8
	dst, src uint8
}

// The two loops below are deliberately identical apart from how they obtain
// the fields, so the difference measured is decode-vs-footprint and nothing
// else. Both do the same trivial ALU work and the same dispatch.
func runPacked(text []Slot, regs *[11]uint64, steps int) uint64 {
	pc := 0
	for i := 0; i < steps; i++ {
		ins := text[pc]
		switch ins.Op() {
		case OpAdd64Imm:
			regs[ins.Dst()] += uint64(int64(ins.Imm()))
		case OpMov64Reg:
			regs[ins.Dst()] = regs[ins.Src()]
		case OpAdd64Reg:
			regs[ins.Dst()] += regs[ins.Src()]
		case OpLsh64Imm:
			regs[ins.Dst()] <<= uint64(ins.Imm()) & 63
		default:
			regs[ins.Dst()] ^= uint64(ins.Off())
		}
		pc++
		if pc == len(text) {
			pc = 0
		}
	}
	return regs[0]
}

func runDecoded(text []decodedSlot, regs *[11]uint64, steps int) uint64 {
	pc := 0
	for i := 0; i < steps; i++ {
		ins := &text[pc]
		switch ins.op {
		case OpAdd64Imm:
			regs[ins.dst] += uint64(int64(ins.imm))
		case OpMov64Reg:
			regs[ins.dst] = regs[ins.src]
		case OpAdd64Reg:
			regs[ins.dst] += regs[ins.src]
		case OpLsh64Imm:
			regs[ins.dst] <<= uint64(ins.imm) & 63
		default:
			regs[ins.dst] ^= uint64(ins.off)
		}
		pc++
		if pc == len(text) {
			pc = 0
		}
	}
	return regs[0]
}

// buildMixedProgram synthesises a program whose opcode mix approximates what
// real mainnet traffic executes: mostly register moves and adds, measured at
// 19.3% mov64 reg, 11.5% add64 imm, 6.7% add64 reg, 3.2% lsh64.
func buildMixedProgram(n int) ([]Slot, []decodedSlot) {
	packed := make([]Slot, n)
	decoded := make([]decodedSlot, n)
	ops := []uint8{OpMov64Reg, OpMov64Reg, OpAdd64Imm, OpMov64Reg, OpAdd64Reg, OpLsh64Imm}
	for i := range packed {
		op := ops[i%len(ops)]
		dst := uint8(i%9) + 1
		src := uint8((i+3)%9) + 1
		off := int16(i & 0x7f)
		imm := int32(i&0x1f) + 1
		packed[i] = testSlot(op, dst, src, off, uint32(imm))
		decoded[i] = decodedSlot{imm: imm, off: off, op: op, dst: dst, src: src}
	}
	return packed, decoded
}

// sizes span a program that fits in L1 through one the size of the largest
// mainnet program by compute.
var predecodeSizes = []struct {
	name  string
	insns int
}{
	{"1k_L1", 1 << 10},
	{"8k_L2", 8 << 10},
	{"32k_L2L3", 32 << 10},
	{"115k_mainnet", 115 << 10},
}

func BenchmarkDispatchPacked(b *testing.B) {
	for _, sz := range predecodeSizes {
		packed, _ := buildMixedProgram(sz.insns)
		b.Run(fmt.Sprintf("%s/bytes=%d", sz.name, len(packed)*8), func(b *testing.B) {
			var regs [11]uint64
			b.ResetTimer()
			runPacked(packed, &regs, b.N)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/instr")
		})
	}
}

func BenchmarkDispatchDecoded(b *testing.B) {
	for _, sz := range predecodeSizes {
		_, decoded := buildMixedProgram(sz.insns)
		b.Run(fmt.Sprintf("%s/bytes=%d", sz.name, len(decoded)*12), func(b *testing.B) {
			var regs [11]uint64
			b.ResetTimer()
			runDecoded(decoded, &regs, b.N)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/instr")
		})
	}
}

// TestPredecodeLoopsAgree pins the two loops to the same results, so a
// benchmark difference is a cost difference and not a behaviour difference.
func TestPredecodeLoopsAgree(t *testing.T) {
	for _, sz := range []int{1 << 10, 8 << 10} {
		packed, decoded := buildMixedProgram(sz)
		var a, c [11]uint64
		for i := range a {
			a[i], c[i] = uint64(i*7+1), uint64(i*7+1)
		}
		steps := sz * 3
		if got, want := runDecoded(decoded, &c, steps), runPacked(packed, &a, steps); got != want {
			t.Fatalf("size %d: decoded=%d packed=%d", sz, got, want)
		}
		if a != c {
			t.Fatalf("size %d: register files diverged\npacked =%v\ndecoded=%v", sz, a, c)
		}
	}
}
