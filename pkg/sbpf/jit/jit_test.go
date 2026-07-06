package jit

import (
	"errors"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
	"github.com/stretchr/testify/require"
)

func TestExecContextLayout(t *testing.T) {
	var ctx ExecContext
	require.Equal(t, uintptr(0x00), unsafe.Offsetof(ctx.Regs))
	require.Equal(t, uintptr(0x58), unsafe.Offsetof(ctx.HostSP))
	require.Equal(t, uintptr(0x60), unsafe.Offsetof(ctx.HostBP))
	require.Equal(t, uintptr(0x68), unsafe.Offsetof(ctx.HostBX))
	require.Equal(t, uintptr(0x70), unsafe.Offsetof(ctx.HostR12))
	require.Equal(t, uintptr(0x78), unsafe.Offsetof(ctx.HostR13))
	require.Equal(t, uintptr(0x80), unsafe.Offsetof(ctx.HostR14))
	require.Equal(t, uintptr(0x88), unsafe.Offsetof(ctx.HostR15))
	require.Equal(t, uintptr(0x90), unsafe.Offsetof(ctx.NativeStackTop))
	require.Equal(t, uintptr(0x98), unsafe.Offsetof(ctx.Resume))
	require.Equal(t, uintptr(offExitReason), unsafe.Offsetof(ctx.ExitReason))
	require.Equal(t, uintptr(offExitPC), unsafe.Offsetof(ctx.ExitPC))
	require.Equal(t, uintptr(offCuDue), unsafe.Offsetof(ctx.CuDue))
	require.Equal(t, uintptr(offCuLeft), unsafe.Offsetof(ctx.CuLeft))
}

func ins(op uint8, dst, src uint8, off int16, imm uint32) sbpf.Slot {
	return sbpf.Slot(op) | sbpf.Slot(dst)<<8 | sbpf.Slot(src)<<12 |
		sbpf.Slot(uint16(off))<<16 | sbpf.Slot(imm)<<32
}

var v0 = sbpfver.SbpfVersion{Version: sbpfver.SbpfVersionV0}

// testFuncs is set per-test for programs that use OpCall.
var testFuncs map[uint32]int64

// runInterpreter executes text with the given budget and input region.
// Stack frame gaps are disabled to match the JIT configuration.
func runInterpreter(text []sbpf.Slot, budget uint64, input []byte) (uint64, uint64, error) {
	prog := &sbpf.Program{
		Text:        text,
		TextVA:      sbpf.VaddrProgram,
		Entrypoint:  0,
		Funcs:       testFuncs,
		SbpfVersion: v0,
	}
	meter := cu.NewComputeMeter(budget)
	interp := sbpf.NewInterpreter(prog, &sbpf.VMOpts{
		HeapMax:               4096,
		Input:                 input,
		Syscalls:              func(uint32) (sbpf.Syscall, bool) { return nil, false },
		ComputeMeter:          &meter,
		DisableStackFrameGaps: true,
	})
	defer interp.Finish()
	return interp.Run()
}

// runJIT compiles and executes text with the given budget and input.
func runJIT(t *testing.T, text []sbpf.Slot, budget uint64, input []byte) (uint64, uint64, error) {
	t.Helper()
	compiled, err := Compile(text, v0, 0, false, testFuncs)
	require.NoError(t, err)
	defer compiled.Free()
	meter := cu.NewComputeMeter(budget)
	return compiled.Run(&meter, &Memory{
		Stack: make([]byte, sbpf.StackMax),
		Heap:  make([]byte, 4096),
		Input: input,
	})
}

// errClass folds errors into comparable categories.
func errClass(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, cu.ErrComputeExceeded) {
		return "oocu"
	}
	var exc *sbpf.Exception
	if errors.As(err, &exc) {
		switch {
		case errors.Is(exc.Detail, sbpf.ExcDivideByZero):
			return "div0"
		case errors.Is(exc.Detail, sbpf.ExcExecutionOverrun):
			return "overrun"
		}
		var bad sbpf.ExcBadAccess
		if errors.As(exc.Detail, &bad) {
			return "badaccess"
		}
		if errors.Is(exc.Detail, sbpf.ExcCallDepth) {
			return "calldepth"
		}
		return "exception:" + exc.Detail.Error()
	}
	return err.Error()
}

// diff runs text through both engines and requires identical results.
func diff(t *testing.T, text []sbpf.Slot, budget uint64) {
	t.Helper()
	diffIn(t, text, budget, nil)
}

func diffIn(t *testing.T, text []sbpf.Slot, budget uint64, input []byte) {
	t.Helper()
	// Each engine gets its own input copy: a program may write to the
	// input region, and a shared slice would leak the first run's
	// mutations into the second.
	iInput := append([]byte(nil), input...)
	jInput := append([]byte(nil), input...)
	iRet, iCU, iErr := runInterpreter(text, budget, iInput)
	jRet, jCU, jErr := runJIT(t, text, budget, jInput)
	require.Equal(t, errClass(iErr), errClass(jErr), "error mismatch")
	require.Equal(t, iCU, jCU, "cu mismatch")
	if iErr == nil {
		require.Equal(t, iRet, jRet, "result mismatch")
	}
	require.Equal(t, iInput, jInput, "input region mismatch after execution")
}

func TestJITBasicALU(t *testing.T) {
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 0, 0, 0, 7),
		ins(sbpf.OpAdd64Imm, 0, 0, 0, 35),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}, 100)
}

func TestJITSext32(t *testing.T) {
	// v0 32-bit add sign-extends its result.
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 0, 0, 0, 0x7fffffff),
		ins(sbpf.OpAdd32Imm, 0, 0, 0, 1), // 0x80000000 -> sext
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}, 100)
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov32Imm, 0, 0, 0, 0xffffffff),
		ins(sbpf.OpNeg32, 0, 0, 0, 0),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}, 100)
}

func TestJITLddw(t *testing.T) {
	diff(t, []sbpf.Slot{
		ins(sbpf.OpLddw, 0, 0, 0, 0xdeadbeef),
		ins(0, 0, 0, 0, 0xcafebabe),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}, 100)
}

func TestJITLoop(t *testing.T) {
	// r0 = sum of 1..100 via a branch loop.
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 0, 0, 0, 0), // 0: r0 = 0
		ins(sbpf.OpMov64Imm, 1, 0, 0, 1), // 1: r1 = 1
		ins(sbpf.OpJgtImm, 1, 0, 3, 100), // 2: if r1 > 100 goto 6
		ins(sbpf.OpAdd64Reg, 0, 1, 0, 0), // 3: r0 += r1
		ins(sbpf.OpAdd64Imm, 1, 0, 0, 1), // 4: r1++
		ins(sbpf.OpJa, 0, 0, -4, 0),      // 5: goto 2
		ins(sbpf.OpExit, 0, 0, 0, 0),     // 6
	}, 10000)
}

func TestJITDivZero(t *testing.T) {
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 0, 0, 0, 10),
		ins(sbpf.OpMov64Imm, 1, 0, 0, 0),
		ins(sbpf.OpDiv64Reg, 0, 1, 0, 0),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}, 100)
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 0, 0, 0, 10),
		ins(sbpf.OpMov64Imm, 1, 0, 0, 0),
		ins(sbpf.OpMod32Reg, 0, 1, 0, 0),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}, 100)
}

func TestJITComputeExhaustion(t *testing.T) {
	// The loop above with a budget too small to finish.
	text := []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 0, 0, 0, 0),
		ins(sbpf.OpMov64Imm, 1, 0, 0, 1),
		ins(sbpf.OpJgtImm, 1, 0, 3, 1000000),
		ins(sbpf.OpAdd64Reg, 0, 1, 0, 0),
		ins(sbpf.OpAdd64Imm, 1, 0, 0, 1),
		ins(sbpf.OpJa, 0, 0, -4, 0),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}
	for _, budget := range []uint64{1, 2, 3, 50, 51, 52, 53, 1000} {
		diff(t, text, budget)
	}
}

func TestJITOverrun(t *testing.T) {
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 0, 0, 0, 1),
	}, 100)
}

func TestJITShifts(t *testing.T) {
	for _, count := range []uint32{0, 1, 31, 32, 33, 63, 64, 65, 1000} {
		text := []sbpf.Slot{
			ins(sbpf.OpLddw, 0, 0, 0, 0xdeadbeef),
			ins(0, 0, 0, 0, 0x8000cafe),
			ins(sbpf.OpMov64Imm, 1, 0, 0, count),
			ins(sbpf.OpArsh64Reg, 0, 1, 0, 0),
			ins(sbpf.OpExit, 0, 0, 0, 0),
		}
		diff(t, text, 100)
		text[3] = ins(sbpf.OpArsh32Reg, 0, 1, 0, 0)
		diff(t, text, 100)
		text[3] = ins(sbpf.OpLsh64Reg, 0, 1, 0, 0)
		diff(t, text, 100)
		text[3] = ins(sbpf.OpRsh32Reg, 0, 1, 0, 0)
		diff(t, text, 100)
	}
}

func TestJITByteSwap(t *testing.T) {
	for _, width := range []uint32{16, 32, 64} {
		diff(t, []sbpf.Slot{
			ins(sbpf.OpLddw, 0, 0, 0, 0x11223344),
			ins(0, 0, 0, 0, 0x55667788),
			ins(sbpf.OpBe, 0, 0, 0, width),
			ins(sbpf.OpExit, 0, 0, 0, 0),
		}, 100)
		diff(t, []sbpf.Slot{
			ins(sbpf.OpLddw, 0, 0, 0, 0x11223344),
			ins(0, 0, 0, 0, 0x55667788),
			ins(sbpf.OpLe, 0, 0, 0, width),
			ins(sbpf.OpExit, 0, 0, 0, 0),
		}, 100)
	}
}

func TestJITStackRoundTrip(t *testing.T) {
	// Store a 64-bit value below the frame pointer, read it back.
	diff(t, []sbpf.Slot{
		ins(sbpf.OpLddw, 1, 0, 0, 0x01020304),
		ins(0, 0, 0, 0, 0x05060708),
		ins(sbpf.OpStxdw, 10, 1, -8, 0),
		ins(sbpf.OpLdxdw, 0, 10, -8, 0),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}, 100)
}

func TestJITHeapSizes(t *testing.T) {
	// r1 = heap base vaddr.
	base := func() []sbpf.Slot {
		return []sbpf.Slot{
			ins(sbpf.OpLddw, 1, 0, 0, uint32(sbpf.VaddrHeap&0xffffffff)),
			ins(0, 0, 0, 0, uint32(sbpf.VaddrHeap>>32) /*=3*/),
		}
	}
	// Store imm then load with each width.
	for _, st := range []struct {
		stOp, ldOp uint8
	}{
		{sbpf.OpStb, sbpf.OpLdxb}, {sbpf.OpSth, sbpf.OpLdxh},
		{sbpf.OpStw, sbpf.OpLdxw}, {sbpf.OpStdw, sbpf.OpLdxdw},
	} {
		text := append(base(),
			ins(st.stOp, 1, 0, 16, 0xdeadbeef),
			ins(st.ldOp, 0, 1, 16, 0),
			ins(sbpf.OpExit, 0, 0, 0, 0),
		)
		diff(t, text, 100)
	}
}

func TestJITInputRead(t *testing.T) {
	input := make([]byte, 64)
	for i := range input {
		input[i] = byte(i * 7)
	}
	text := []sbpf.Slot{
		ins(sbpf.OpMov64Reg, 1, 1, 0, 0), // r1 already = VaddrInput
		ins(sbpf.OpLdxdw, 0, 1, 8, 0),
		ins(sbpf.OpLdxb, 2, 1, 40, 0),
		ins(sbpf.OpAdd64Reg, 0, 2, 0, 0),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}
	diffIn(t, text, 100, input)
}

func TestJITBadAccess(t *testing.T) {
	// Load from an unmapped region (hi=5).
	diff(t, []sbpf.Slot{
		ins(sbpf.OpLddw, 1, 0, 0, 0),
		ins(0, 0, 0, 0, 5),
		ins(sbpf.OpLdxdw, 0, 1, 0, 0),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}, 100)
	// Store past the end of the heap.
	diff(t, []sbpf.Slot{
		ins(sbpf.OpLddw, 1, 0, 0, uint32(sbpf.VaddrHeap&0xffffffff)),
		ins(0, 0, 0, 0, uint32(sbpf.VaddrHeap>>32) /*=3*/),
		ins(sbpf.OpStdw, 1, 0, 8192, 0),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}, 100)
}

// aluFuzzOps are (op, usesSrc) pairs for random straightline programs.
var aluFuzzOps = []struct {
	op      uint8
	usesSrc bool
}{
	{sbpf.OpAdd32Imm, false}, {sbpf.OpAdd32Reg, true},
	{sbpf.OpAdd64Imm, false}, {sbpf.OpAdd64Reg, true},
	{sbpf.OpSub32Imm, false}, {sbpf.OpSub32Reg, true},
	{sbpf.OpSub64Imm, false}, {sbpf.OpSub64Reg, true},
	{sbpf.OpMul32Imm, false}, {sbpf.OpMul32Reg, true},
	{sbpf.OpMul64Imm, false}, {sbpf.OpMul64Reg, true},
	{sbpf.OpOr32Imm, false}, {sbpf.OpOr32Reg, true},
	{sbpf.OpOr64Imm, false}, {sbpf.OpOr64Reg, true},
	{sbpf.OpAnd32Imm, false}, {sbpf.OpAnd32Reg, true},
	{sbpf.OpAnd64Imm, false}, {sbpf.OpAnd64Reg, true},
	{sbpf.OpXor32Imm, false}, {sbpf.OpXor32Reg, true},
	{sbpf.OpXor64Imm, false}, {sbpf.OpXor64Reg, true},
	{sbpf.OpMov32Imm, false}, {sbpf.OpMov32Reg, true},
	{sbpf.OpMov64Imm, false}, {sbpf.OpMov64Reg, true},
	{sbpf.OpLsh32Imm, false}, {sbpf.OpLsh32Reg, true},
	{sbpf.OpLsh64Imm, false}, {sbpf.OpLsh64Reg, true},
	{sbpf.OpRsh32Imm, false}, {sbpf.OpRsh32Reg, true},
	{sbpf.OpRsh64Imm, false}, {sbpf.OpRsh64Reg, true},
	{sbpf.OpArsh32Imm, false}, {sbpf.OpArsh32Reg, true},
	{sbpf.OpArsh64Reg, true},
	{sbpf.OpNeg32, false}, {sbpf.OpNeg64, false},
	{sbpf.OpDiv32Reg, true}, {sbpf.OpDiv64Reg, true},
	{sbpf.OpMod32Reg, true}, {sbpf.OpMod64Reg, true},
	{sbpf.OpBe, false}, {sbpf.OpLe, false},
}

// TestJITRandomALU compares both engines on seeded random straightline
// programs over r0..r9, folding all registers into r0 at the end.
func TestJITRandomALU(t *testing.T) {
	rng := rand.New(rand.NewSource(12345))
	for round := 0; round < 500; round++ {
		var text []sbpf.Slot
		// Seed registers with random 64-bit values.
		for reg := uint8(0); reg < 10; reg++ {
			text = append(text,
				ins(sbpf.OpLddw, reg, 0, 0, rng.Uint32()),
				ins(0, 0, 0, 0, rng.Uint32()))
		}
		for i := 0; i < 50; i++ {
			pick := aluFuzzOps[rng.Intn(len(aluFuzzOps))]
			dst := uint8(rng.Intn(10))
			src := uint8(rng.Intn(10))
			imm := rng.Uint32()
			if pick.op == sbpf.OpBe || pick.op == sbpf.OpLe {
				imm = []uint32{16, 32, 64}[rng.Intn(3)]
			}
			if !pick.usesSrc {
				src = 0
			}
			text = append(text, ins(pick.op, dst, src, 0, imm))
		}
		// Fold everything into r0 so all registers are observed.
		for reg := uint8(1); reg < 10; reg++ {
			text = append(text, ins(sbpf.OpXor64Reg, 0, reg, 0, 0))
		}
		text = append(text, ins(sbpf.OpExit, 0, 0, 0, 0))
		diff(t, text, 10000)
	}
}

// benchLoop is a 10M-instruction counting loop.
var benchMem = Memory{Stack: make([]byte, sbpf.StackMax), Heap: make([]byte, 4096)}

var benchLoop = []sbpf.Slot{
	ins(sbpf.OpMov64Imm, 0, 0, 0, 0),
	ins(sbpf.OpMov64Imm, 1, 0, 0, 1),
	ins(sbpf.OpJgtImm, 1, 0, 3, 2500000),
	ins(sbpf.OpAdd64Reg, 0, 1, 0, 0),
	ins(sbpf.OpAdd64Imm, 1, 0, 0, 1),
	ins(sbpf.OpJa, 0, 0, -4, 0),
	ins(sbpf.OpExit, 0, 0, 0, 0),
}

func BenchmarkInterpreterLoop(b *testing.B) {
	prog := &sbpf.Program{Text: benchLoop, TextVA: sbpf.VaddrProgram, SbpfVersion: v0}
	for i := 0; i < b.N; i++ {
		meter := cu.NewComputeMeter(100000000)
		interp := sbpf.NewInterpreter(prog, &sbpf.VMOpts{
			HeapMax:      1024,
			Syscalls:     func(uint32) (sbpf.Syscall, bool) { return nil, false },
			ComputeMeter: &meter,
		})
		_, cuUsed, err := interp.Run()
		interp.Finish()
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(cuUsed), "insns")
	}
}

func BenchmarkJITLoop(b *testing.B) {
	compiled, err := Compile(benchLoop, v0, 0, false, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer compiled.Free()
	for i := 0; i < b.N; i++ {
		meter := cu.NewComputeMeter(100000000)
		_, cuUsed, err := compiled.Run(&meter, &benchMem)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(cuUsed), "insns")
	}
}

// TestJITRandomMemory fuzzes loads and stores across all regions with a
// mix of in-bounds and out-of-bounds offsets, comparing both engines.
func TestJITRandomMemory(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	memOps := []struct {
		st, ld uint8
		size   int16
	}{
		{sbpf.OpStxb, sbpf.OpLdxb, 1},
		{sbpf.OpStxh, sbpf.OpLdxh, 2},
		{sbpf.OpStxw, sbpf.OpLdxw, 4},
		{sbpf.OpStxdw, sbpf.OpLdxdw, 8},
	}
	for round := 0; round < 300; round++ {
		input := make([]byte, 128)
		for i := range input {
			input[i] = byte(rng.Intn(256))
		}
		var text []sbpf.Slot
		// r1=heap, r2=input (already VaddrInput but reset explicitly), r10=stack fp.
		text = append(text,
			ins(sbpf.OpLddw, 1, 0, 0, uint32(sbpf.VaddrHeap&0xffffffff)),
			ins(0, 0, 0, 0, uint32(sbpf.VaddrHeap>>32)),
			ins(sbpf.OpLddw, 2, 0, 0, uint32(sbpf.VaddrInput&0xffffffff)),
			ins(0, 0, 0, 0, uint32(sbpf.VaddrInput>>32)),
			ins(sbpf.OpLddw, 3, 0, 0, rng.Uint32()),
			ins(0, 0, 0, 0, rng.Uint32()),
		)
		for i := 0; i < 20; i++ {
			mo := memOps[rng.Intn(len(memOps))]
			base := uint8([]int{1, 2, 10}[rng.Intn(3)])
			// Mostly small in-bounds offsets; occasionally wild.
			var off int16
			if rng.Intn(6) == 0 {
				off = int16(rng.Intn(70000) - 35000)
			} else {
				off = int16(rng.Intn(64))
				if base == 10 {
					off = -off - int16(mo.size) // below frame pointer
				}
			}
			if rng.Intn(2) == 0 {
				text = append(text, ins(mo.st, base, 3, off, 0))
			} else {
				text = append(text, ins(mo.ld, 3, base, off, 0))
			}
		}
		text = append(text, ins(sbpf.OpMov64Reg, 0, 3, 0, 0), ins(sbpf.OpExit, 0, 0, 0, 0))
		diffIn(t, text, 10000, input)
	}
}

// hashOf is an arbitrary distinct key for a test function-table entry.
func hashOf(pc int) uint32 { return uint32(0x1000 + pc) }

func TestJITSimpleCall(t *testing.T) {
	// main: r0 = f(); exit. f: r0 = 42; exit.
	testFuncs = map[uint32]int64{hashOf(3): 3}
	defer func() { testFuncs = nil }()
	diff(t, []sbpf.Slot{
		ins(sbpf.OpCall, 0, 0, 0, hashOf(3)), // 0: call f
		ins(sbpf.OpMov64Imm, 1, 0, 0, 99),    // 1: r1=99 (clobber, must survive? r1 caller-saved)
		ins(sbpf.OpExit, 0, 0, 0, 0),         // 2: exit -> return r0
		ins(sbpf.OpMov64Imm, 0, 0, 0, 42),    // 3: f: r0=42
		ins(sbpf.OpExit, 0, 0, 0, 0),         // 4: return
	}, 100)
}

func TestJITCalleeSavedRegs(t *testing.T) {
	// r6 must be preserved across a call that clobbers it.
	testFuncs = map[uint32]int64{hashOf(4): 4}
	defer func() { testFuncs = nil }()
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 6, 0, 0, 7), // 0: r6=7
		ins(sbpf.OpCall, 0, 0, 0, hashOf(4)),
		ins(sbpf.OpMov64Reg, 0, 6, 0, 0),   // 2: r0=r6 (should be 7)
		ins(sbpf.OpExit, 0, 0, 0, 0),       // 3
		ins(sbpf.OpMov64Imm, 6, 0, 0, 123), // 4: f clobbers r6
		ins(sbpf.OpExit, 0, 0, 0, 0),       // 5
	}, 100)
}

func TestJITRecursiveFactorial(t *testing.T) {
	// r1 = n; fact(n): if n<=1 return 1 else return n*fact(n-1).
	testFuncs = map[uint32]int64{hashOf(2): 2}
	defer func() { testFuncs = nil }()
	text := []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 1, 0, 0, 8),     // 0: r1 = 8
		ins(sbpf.OpCall, 0, 0, 0, hashOf(2)), // 1: call fact
		ins(sbpf.OpJgtImm, 1, 0, 2, 1),       // 2: fact: if r1 > 1 goto 5
		ins(sbpf.OpMov64Imm, 0, 0, 0, 1),     // 3: r0 = 1
		ins(sbpf.OpExit, 0, 0, 0, 0),         // 4: return
		ins(sbpf.OpMov64Reg, 6, 1, 0, 0),     // 5: r6 = r1 (save n across call)
		ins(sbpf.OpSub64Imm, 1, 0, 0, 1),     // 6: r1 = n-1
		ins(sbpf.OpCall, 0, 0, 0, hashOf(2)), // 7: r0 = fact(n-1)
		ins(sbpf.OpMul64Reg, 0, 6, 0, 0),     // 8: r0 = r0 * r6
		ins(sbpf.OpExit, 0, 0, 0, 0),         // 9: return
	}
	diff(t, text, 100000)
	// Also the top-level exit is instruction 1's return point + an exit;
	// add an explicit final exit path.
}

func TestJITCallDepthOverflow(t *testing.T) {
	// Unbounded recursion must fault with CallDepth on both engines.
	testFuncs = map[uint32]int64{hashOf(1): 1}
	defer func() { testFuncs = nil }()
	diff(t, []sbpf.Slot{
		ins(sbpf.OpCall, 0, 0, 0, hashOf(1)), // 0: call self-ish
		ins(sbpf.OpCall, 0, 0, 0, hashOf(1)), // 1: recurse forever
		ins(sbpf.OpExit, 0, 0, 0, 0),         // 2
	}, 1000000)
}
