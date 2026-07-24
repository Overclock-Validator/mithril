package sbpf

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
)

// Benchmarks for the sBPF interpreter hot path and VM lifecycle.
//
// Programs run at sBPF v0 (static frames + stack-frame gaps), which is the
// representative mainnet configuration. Per-instruction benchmarks report a
// "ns/ins" metric so different instruction mixes can be compared directly; the
// lifecycle benchmark reports ns/op because it measures fixed setup/teardown
// cost rather than steady-state dispatch.
//
// These are the durable baseline for the VM performance work (bounded
// stack/heap clearing, memory-translation fast paths, PGO A/B). Capture with:
//
//	go test -bench=BenchmarkInterp -benchmem -count=10 ./pkg/sbpf/
//
// and compare with benchstat.

// benchLoopIters is the number of guest loop iterations executed per Run. Large
// enough that per-Run setup/teardown is amortized to noise in the per-ins
// benchmarks.
const benchLoopIters = 1_000_000

func benchProgram(ver uint32, text []Slot, funcs map[uint32]int64) *Program {
	if funcs == nil {
		funcs = map[uint32]int64{}
	}
	return &Program{
		TextBytes:   testSlotsToBytes(text),
		Text:        text,
		TextVA:      VaddrProgram,
		Entrypoint:  0,
		Funcs:       funcs,
		SbpfVersion: sbpfver.SbpfVersion{Version: ver},
	}
}

// emitLddw encodes a 64-bit load-immediate (two slots) into dst.
func emitLddw(dst uint8, imm uint64) []Slot {
	return []Slot{
		testSlot(OpLddw, dst, 0, 0, uint32(imm)),
		Slot(uint64(uint32(imm>>32)) << 32), // op=0 continuation slot, imm=high32
	}
}

type benchOpts struct {
	heapMax      int
	input        []byte
	inputRegions []InputRegion
	syscalls     SyscallRegistry
}

// runIP runs the program b.N times and reports ns per executed guest
// instruction. insPerRun is the (exact) number of guest instructions executed
// in a single Run.
func runIP(b *testing.B, program *Program, opts benchOpts, insPerRun float64) {
	b.Helper()
	syscalls := opts.syscalls
	if syscalls == nil {
		syscalls = func(uint32) (Syscall, bool) { return nil, false }
	}
	heapMax := opts.heapMax
	if heapMax == 0 {
		heapMax = 32 * 1024
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		meter := cu.NewComputeMeter(1 << 62)
		ip := NewInterpreter(program, &VMOpts{
			HeapMax:      heapMax,
			Input:        opts.input,
			InputRegions: opts.inputRegions,
			Syscalls:     syscalls,
			ComputeMeter: &meter,
		})
		_, _, err := ip.Run()
		ip.Finish()
		if err != nil {
			b.Fatalf("run failed: %v", err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*insPerRun), "ns/ins")
}

// BenchmarkInterpALU: pure ALU + a loop branch (no memory access).
// Body: sub64, add64, xor64, jne = 4 ins/iter.
func BenchmarkInterpALU(b *testing.B) {
	text := []Slot{
		testSlot(OpMov64Imm, 1, 0, 0, benchLoopIters),
		testSlot(OpSub64Imm, 1, 0, 0, 1),
		testSlot(OpAdd64Reg, 2, 1, 0, 0),
		testSlot(OpXor64Reg, 3, 2, 0, 0),
		testSlot(OpJneImm, 1, 0, -4, 0),
		testSlot(OpExit, 0, 0, 0, 0),
	}
	runIP(b, benchProgram(sbpfver.SbpfVersionV0, text, nil), benchOpts{}, benchLoopIters*4)
}

// BenchmarkInterpBranch: data-dependent branch every iteration.
// Body: mov, and, jeq(±), add(half), sub, jne. Exactly 5.5 ins/iter averaged
// over an even iteration count (half odd -> 6, half even -> 5).
func BenchmarkInterpBranch(b *testing.B) {
	text := []Slot{
		testSlot(OpMov64Imm, 1, 0, 0, benchLoopIters),
		testSlot(OpMov64Reg, 2, 1, 0, 0),
		testSlot(OpAnd64Imm, 2, 0, 0, 1),
		testSlot(OpJeqImm, 2, 0, 1, 0), // even -> skip the add
		testSlot(OpAdd64Imm, 3, 0, 0, 1),
		testSlot(OpSub64Imm, 1, 0, 0, 1),
		testSlot(OpJneImm, 1, 0, -6, 0),
		testSlot(OpExit, 0, 0, 0, 0),
	}
	runIP(b, benchProgram(sbpfver.SbpfVersionV0, text, nil), benchOpts{}, benchLoopIters*5.5)
}

// BenchmarkInterpStackSpill: frame-relative store+load through r10 (exercises
// the stack translation path incl. v0 gap compression). 4 ins/iter.
func BenchmarkInterpStackSpill(b *testing.B) {
	text := []Slot{
		testSlot(OpMov64Imm, 1, 0, 0, benchLoopIters),
		testSlot(OpStxdw, 10, 1, -8, 0),
		testSlot(OpLdxdw, 2, 10, -8, 0),
		testSlot(OpSub64Imm, 1, 0, 0, 1),
		testSlot(OpJneImm, 1, 0, -4, 0),
		testSlot(OpExit, 0, 0, 0, 0),
	}
	runIP(b, benchProgram(sbpfver.SbpfVersionV0, text, nil), benchOpts{}, benchLoopIters*4)
}

// BenchmarkInterpHeap: store+load into the heap region. 4 ins/iter.
func BenchmarkInterpHeap(b *testing.B) {
	text := []Slot{
		testSlot(OpMov64Imm, 1, 0, 0, benchLoopIters),
	}
	text = append(text, emitLddw(4, VaddrHeap)...)
	text = append(text,
		testSlot(OpStxdw, 4, 1, 0, 0),
		testSlot(OpLdxdw, 2, 4, 0, 0),
		testSlot(OpSub64Imm, 1, 0, 0, 1),
		testSlot(OpJneImm, 1, 0, -4, 0),
		testSlot(OpExit, 0, 0, 0, 0),
	)
	runIP(b, benchProgram(sbpfver.SbpfVersionV0, text, nil), benchOpts{}, benchLoopIters*4)
}

// BenchmarkInterpInputFlat: load from a single flat input buffer (no regions).
// r1 holds VaddrInput at entry. 4 ins/iter.
func BenchmarkInterpInputFlat(b *testing.B) {
	text := []Slot{
		testSlot(OpMov64Imm, 5, 0, 0, benchLoopIters),
		testSlot(OpLdxdw, 2, 1, 0, 0),
		testSlot(OpAdd64Reg, 3, 2, 0, 0),
		testSlot(OpSub64Imm, 5, 0, 0, 1),
		testSlot(OpJneImm, 5, 0, -4, 0),
		testSlot(OpExit, 0, 0, 0, 0),
	}
	runIP(b, benchProgram(sbpfver.SbpfVersionV0, text, nil), benchOpts{input: make([]byte, 64)}, benchLoopIters*4)
}

const (
	benchRegionCount = 16
	benchRegionSize  = 256
)

func benchInputRegions() []InputRegion {
	regions := make([]InputRegion, benchRegionCount)
	for i := range regions {
		regions[i] = InputRegion{
			Offset:               uint64(i * benchRegionSize),
			RegionSize:           benchRegionSize,
			AddressSpaceReserved: benchRegionSize,
			Data:                 make([]byte, benchRegionSize),
			Writable:             false,
			AccountIndex:         i,
		}
	}
	return regions
}

// regionLoadText builds a 2-load loop reading region-backed input at the two
// given base VAs. 4 ins/iter.
func regionLoadText(baseA, baseB uint64) []Slot {
	text := emitLddw(6, baseA)
	text = append(text, emitLddw(7, baseB)...)
	text = append(text,
		testSlot(OpMov64Imm, 5, 0, 0, benchLoopIters),
		testSlot(OpLdxdw, 2, 6, 0, 0),
		testSlot(OpLdxdw, 3, 7, 0, 0),
		testSlot(OpSub64Imm, 5, 0, 0, 1),
		testSlot(OpJneImm, 5, 0, -4, 0),
		testSlot(OpExit, 0, 0, 0, 0),
	)
	return text
}

// BenchmarkInterpInputRegionSame: both loads hit region 0 (baseline for a
// future MRU region cache — the second load is a repeat lookup).
func BenchmarkInterpInputRegionSame(b *testing.B) {
	text := regionLoadText(VaddrInput, VaddrInput)
	runIP(b, benchProgram(sbpfver.SbpfVersionV0, text, nil),
		benchOpts{inputRegions: benchInputRegions()}, benchLoopIters*4)
}

// BenchmarkInterpInputRegionAlt: loads alternate between region 0 and the last
// region, defeating single-entry locality (worst case for binary search).
func BenchmarkInterpInputRegionAlt(b *testing.B) {
	last := uint64((benchRegionCount - 1) * benchRegionSize)
	text := regionLoadText(VaddrInput, VaddrInput+last)
	runIP(b, benchProgram(sbpfver.SbpfVersionV0, text, nil),
		benchOpts{inputRegions: benchInputRegions()}, benchLoopIters*4)
}

// BenchmarkInterpCall: internal function call+return each iteration (v0 shadow
// stack push/pop + funcs-map resolution). Executed: call, leaf exit, sub, jne
// = 4 ins/iter.
func BenchmarkInterpCall(b *testing.B) {
	const leafHash = 1
	text := []Slot{
		testSlot(OpMov64Imm, 1, 0, 0, benchLoopIters),
		testSlot(OpCall, 0, 0, 0, leafHash), // -> leaf at pc 5
		testSlot(OpSub64Imm, 1, 0, 0, 1),
		testSlot(OpJneImm, 1, 0, -3, 0),
		testSlot(OpExit, 0, 0, 0, 0),
		testSlot(OpExit, 0, 0, 0, 0), // leaf
	}
	program := benchProgram(sbpfver.SbpfVersionV0, text, map[uint32]int64{leafHash: 5})
	runIP(b, program, benchOpts{}, benchLoopIters*4)
}

// BenchmarkInterpSyscall: no-op syscall invocation each iteration (interface
// dispatch + meter sync). Executed: call, sub, jne = 3 ins/iter.
func BenchmarkInterpSyscall(b *testing.B) {
	const scHash = 0x1234
	noop := SyscallFunc0(func(VM) (uint64, error) { return 0, nil })
	syscalls := SyscallRegistry(func(h uint32) (Syscall, bool) {
		if h == scHash {
			return noop, true
		}
		return nil, false
	})
	text := []Slot{
		testSlot(OpMov64Imm, 1, 0, 0, benchLoopIters),
		testSlot(OpCall, 0, 0, 0, scHash),
		testSlot(OpSub64Imm, 1, 0, 0, 1),
		testSlot(OpJneImm, 1, 0, -3, 0),
		testSlot(OpExit, 0, 0, 0, 0),
	}
	runIP(b, benchProgram(sbpfver.SbpfVersionV0, text, nil), benchOpts{syscalls: syscalls}, benchLoopIters*3)
}

// BenchmarkInterpLifecycle measures NewInterpreter + Run + Finish for a trivial
// program: the fixed per-execution cost (heap clear on construction, stack
// clear on teardown, pool traffic) paid by every top-level invocation and every
// CPI. This is the primary target of the bounded-clearing optimization.
func BenchmarkInterpLifecycle(b *testing.B) {
	program := benchProgram(sbpfver.SbpfVersionV0, []Slot{testSlot(OpExit, 0, 0, 0, 0)}, nil)
	syscalls := SyscallRegistry(func(uint32) (Syscall, bool) { return nil, false })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		meter := cu.NewComputeMeter(1 << 62)
		ip := NewInterpreter(program, &VMOpts{
			HeapMax:      32 * 1024,
			Syscalls:     syscalls,
			ComputeMeter: &meter,
		})
		if _, _, err := ip.Run(); err != nil {
			b.Fatalf("run failed: %v", err)
		}
		ip.Finish()
	}
}
