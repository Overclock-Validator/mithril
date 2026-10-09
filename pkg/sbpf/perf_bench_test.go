package sbpf

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
)

func slot(op uint8, dst uint8, src uint8, off int16, imm uint32) Slot {
	return Slot(op) | Slot(dst)<<8 | Slot(src)<<12 | Slot(uint16(off))<<16 | Slot(imm)<<32
}

func slotsToBytes(slots []Slot) []byte {
	out := make([]byte, len(slots)*SlotSize)
	for i, s := range slots {
		binary.LittleEndian.PutUint64(out[i*SlotSize:], uint64(s))
	}
	return out
}

func mkProgram(text []Slot, ver uint32) *Program {
	return &Program{
		TextBytes:   slotsToBytes(text),
		Text:        text,
		TextVA:      VaddrProgram,
		Entrypoint:  0,
		Funcs:       map[uint32]int64{},
		SbpfVersion: sbpfver.SbpfVersion{Version: ver},
	}
}

var noSyscalls = SyscallRegistry(func(uint32) (Syscall, bool) { return nil, false })

// resolveCallTargetsIfSupported precomputes internal call targets on trees
// that have Program.ResolveCallTargets (the loader does this at load time);
// it is a no-op on the baseline tree so the same benchmark code runs on both.
func resolveCallTargetsIfSupported(p *Program) {
	if r, ok := any(p).(interface{ ResolveCallTargets() }); ok {
		r.ResolveCallTargets()
	}
}

// aluLoop: r1 = N; loop: r2 += r1; r2 ^= r3; r3 = r2; r3 *= 7; r3 >>= 3; r1 -= 1; jne r1,0 loop; exit
// 7 instructions per iteration.
func aluLoopProgram(n uint32, ver uint32) *Program {
	text := []Slot{
		slot(OpMov64Imm, 1, 0, 0, n),
		slot(OpMov64Imm, 2, 0, 0, 1),
		slot(OpMov64Imm, 3, 0, 0, 3),
		// loop @3
		slot(OpAdd64Reg, 2, 1, 0, 0),
		slot(OpXor64Reg, 2, 3, 0, 0),
		slot(OpMov64Reg, 3, 2, 0, 0),
		slot(OpMul64Imm, 3, 0, 0, 7),
		slot(OpRsh64Imm, 3, 0, 0, 3),
		slot(OpSub64Imm, 1, 0, 0, 1),
		slot(OpJneImm, 1, 0, -7, 0),
		slot(OpMov64Reg, 0, 2, 0, 0),
		slot(OpExit, 0, 0, 0, 0),
	}
	return mkProgram(text, ver)
}

// memLoop: writes and reads 8 byte values on the stack frame and heap.
// r1 = N; r4 = r10 - 4096 (frame base); r5 = heap base
// loop: stxdw [r4+0], r1; ldxdw r6, [r4+0]; add r2, r6; stxdw [r5+8], r2; ldxdw r7,[r5+8]; xor r2,r7 ; r1 -= 1; jne
func memLoopProgram(n uint32, ver uint32) *Program {
	text := []Slot{
		slot(OpMov64Imm, 1, 0, 0, n),
		slot(OpMov64Imm, 2, 0, 0, 1),
		slot(OpMov64Reg, 4, 10, 0, 0),
		slot(OpAdd64Imm, 4, 0, 0, uint32(0xfffff000)), // r4 = r10 - 4096
		slot(OpLddw, 5, 0, 0, uint32(VaddrHeap&0xffffffff)),
		slot(0, 0, 0, 0, uint32(VaddrHeap>>32)),
		// loop @6
		slot(OpStxdw, 4, 1, 0, 0),
		slot(OpLdxdw, 6, 4, 0, 0),
		slot(OpAdd64Reg, 2, 6, 0, 0),
		slot(OpStxdw, 5, 2, 8, 0),
		slot(OpLdxdw, 7, 5, 8, 0),
		slot(OpXor64Reg, 2, 7, 0, 0),
		slot(OpStxw, 5, 2, 16, 0),
		slot(OpLdxb, 8, 5, 16, 0),
		slot(OpAdd64Reg, 2, 8, 0, 0),
		slot(OpSub64Imm, 1, 0, 0, 1),
		slot(OpJneImm, 1, 0, -11, 0),
		slot(OpMov64Reg, 0, 2, 0, 0),
		slot(OpExit, 0, 0, 0, 0),
	}
	return mkProgram(text, ver)
}

// callLoop: calls a tiny function N times (tests Push/Pop + call resolution)
func callLoopProgram(n uint32, ver uint32) *Program {
	fnPC := int64(7)
	text := []Slot{
		slot(OpMov64Imm, 1, 0, 0, n),
		slot(OpMov64Imm, 2, 0, 0, 1),
		// loop @2
		slot(OpCall, 0, 0, 0, 0), // patched below
		slot(OpSub64Imm, 1, 0, 0, 1),
		slot(OpJneImm, 1, 0, -3, 0),
		slot(OpMov64Reg, 0, 2, 0, 0),
		slot(OpExit, 0, 0, 0, 0),
		// fn @7
		slot(OpAdd64Imm, 2, 0, 0, 3),
		slot(OpXor64Reg, 2, 1, 0, 0),
		slot(OpExit, 0, 0, 0, 0),
	}
	p := mkProgram(text, ver)
	if ver >= sbpfver.SbpfVersionV3 {
		// relative call: target = pc + imm + 1 ; pc=2 -> imm = 7-2-1 = 4
		text[2] = slot(OpCall, 0, 1, 0, uint32(fnPC-2-1))
	} else {
		h := PCHash(uint64(fnPC))
		p.Funcs[h] = fnPC
		text[2] = slot(OpCall, 0, 0, 0, h)
	}
	p.TextBytes = slotsToBytes(text)
	return p
}

func runProgram(b *testing.B, p *Program, input []byte, syscalls SyscallRegistry, budget uint64) uint64 {
	cm := cu.NewComputeMeter(budget)
	ip := NewInterpreter(p, &VMOpts{
		HeapMax:      32 * 1024,
		Syscalls:     syscalls,
		ComputeMeter: &cm,
		Input:        input,
	})
	ret, _, err := ip.Run()
	ip.Finish()
	if err != nil {
		b.Fatal(err)
	}
	return ret
}

func benchLoop(b *testing.B, p *Program, insnsPerRun uint64) {
	if err := p.Verify(); err != nil {
		b.Fatal(err)
	}
	resolveCallTargetsIfSupported(p)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runProgram(b, p, nil, noSyscalls, 1<<40)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(uint64(b.N)*insnsPerRun), "ns/insn")
}

const loopN = 200_000

func BenchmarkAluLoopV0(b *testing.B) { benchLoop(b, aluLoopProgram(loopN, 0), 3+7*loopN+2) }
func BenchmarkAluLoopV3(b *testing.B) { benchLoop(b, aluLoopProgram(loopN, 3), 3+7*loopN+2) }
func BenchmarkMemLoopV0(b *testing.B) { benchLoop(b, memLoopProgram(loopN, 0), 5+11*loopN+2) }
func BenchmarkMemLoopV3(b *testing.B) { benchLoop(b, memLoopProgram(loopN, 3), 5+11*loopN+2) }
func BenchmarkCallLoopV0(b *testing.B) {
	benchLoop(b, callLoopProgram(loopN, 0), 2+6*loopN+2)
}
func BenchmarkCallLoopV3(b *testing.B) {
	benchLoop(b, callLoopProgram(loopN, 3), 2+6*loopN+2)
}

// Interpreter setup/teardown cost only (tiny program).
func BenchmarkNewInterpreterAndExit(b *testing.B) {
	p := mkProgram([]Slot{slot(OpExit, 0, 0, 0, 0)}, 0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		runProgram(b, p, nil, noSyscalls, 1000)
	}
}

func TestSyntheticProgramsRun(t *testing.T) {
	for _, ver := range []uint32{0, 3} {
		for _, p := range []*Program{aluLoopProgram(1000, ver), memLoopProgram(1000, ver), callLoopProgram(1000, ver)} {
			if err := p.Verify(); err != nil {
				t.Fatal(err)
			}
			cm := cu.NewComputeMeter(1 << 30)
			ip := NewInterpreter(p, &VMOpts{HeapMax: 32 * 1024, Syscalls: noSyscalls, ComputeMeter: &cm})
			ret, used, err := ip.Run()
			ip.Finish()
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("ver=%d ret=%d cu=%d", ver, ret, used)
		}
	}
}
