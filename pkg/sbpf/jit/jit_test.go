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
	require.Equal(t, uintptr(offEnterSP), unsafe.Offsetof(ctx.EnterSP))
	require.Equal(t, uintptr(offCallxTable), unsafe.Offsetof(ctx.CallxTable))
	require.Equal(t, uintptr(offCodeBase), unsafe.Offsetof(ctx.CodeBase))
}

func ins(op uint8, dst, src uint8, off int16, imm uint32) sbpf.Slot {
	return sbpf.Slot(op) | sbpf.Slot(dst)<<8 | sbpf.Slot(src)<<12 |
		sbpf.Slot(uint16(off))<<16 | sbpf.Slot(imm)<<32
}

var v0 = sbpfver.SbpfVersion{Version: sbpfver.SbpfVersionV0}

// testFuncs is set per-test for programs that use local OpCall.
var testFuncs map[uint32]int64

// testRegistry / testIsSyscall are set per-test for programs that call
// syscalls; nil means no syscalls.
var testRegistry sbpf.SyscallRegistry

// testGaps runs both engines with v0 stack-frame gaps enabled.
var testGaps bool

// meterVM is a minimal sbpf.VM exposing a compute meter; the memory-less
// test syscalls use only ComputeMeter.
type meterVM struct{ m *cu.ComputeMeter }

func (v *meterVM) VMContext() any                                 { return nil }
func (v *meterVM) HeapMax() uint64                                { return 0 }
func (v *meterVM) HeapSize() uint64                               { return 0 }
func (v *meterVM) UpdateHeapSize(uint64)                          {}
func (v *meterVM) Translate(uint64, uint64, bool) ([]byte, error) { return nil, nil }
func (v *meterVM) DueInstrCount() uint64                          { return 0 }
func (v *meterVM) PrevInstrMeter() uint64                         { return 0 }
func (v *meterVM) SetPrevInstrMeter(uint64)                       {}
func (v *meterVM) ComputeMeter() *cu.ComputeMeter                 { return v.m }
func (v *meterVM) Read(uint64, []byte) error                      { return nil }
func (v *meterVM) Read8(uint64) (uint8, error)                    { return 0, nil }
func (v *meterVM) Read16(uint64) (uint16, error)                  { return 0, nil }
func (v *meterVM) Read32(uint64) (uint32, error)                  { return 0, nil }
func (v *meterVM) Read64(uint64) (uint64, error)                  { return 0, nil }
func (v *meterVM) Write(uint64, []byte) error                     { return nil }
func (v *meterVM) Write8(uint64, uint8) error                     { return nil }
func (v *meterVM) Write16(uint64, uint16) error                   { return nil }
func (v *meterVM) Write32(uint64, uint32) error                   { return nil }
func (v *meterVM) Write64(uint64, uint64) error                   { return nil }

func testInterpRegistry() sbpf.SyscallRegistry {
	if testRegistry != nil {
		return testRegistry
	}
	return func(uint32) (sbpf.Syscall, bool) { return nil, false }
}

func testIsSyscall(h uint32) bool {
	if testRegistry == nil {
		return false
	}
	_, ok := testRegistry(h)
	return ok
}

func memWith(input []byte) *Memory {
	return &Memory{Stack: make([]byte, sbpf.StackMax), Heap: make([]byte, 4096), Input: input}
}

// runInterpreter executes text with the given budget and input region.
func runInterpreter(text []sbpf.Slot, budget uint64, input []byte) (uint64, uint64, error) {
	meter := cu.NewComputeMeter(budget)
	interp := sbpf.NewInterpreter(testProg(text), &sbpf.VMOpts{
		HeapMax:               4096,
		Input:                 input,
		Syscalls:              testInterpRegistry(),
		ComputeMeter:          &meter,
		DisableStackFrameGaps: !testGaps,
	})
	defer interp.Finish()
	return interp.Run()
}

// testProg wraps raw text in a Program the way the loader would.
func testProg(text []sbpf.Slot) *sbpf.Program {
	return &sbpf.Program{
		Text:        text,
		TextVA:      sbpf.VaddrProgram,
		Entrypoint:  0,
		Funcs:       testFuncs,
		SbpfVersion: v0,
	}
}

// runJIT compiles and executes text with the given budget and input.
func runJIT(t *testing.T, text []sbpf.Slot, budget uint64, input []byte) (uint64, uint64, error) {
	t.Helper()
	compiled, err := Compile(testProg(text), testGaps, testIsSyscall)
	require.NoError(t, err)
	defer compiled.Free()
	meter := cu.NewComputeMeter(budget)
	return compiled.Run(&meter, memWith(input), testRegistry, &meterVM{&meter})
}

// diffGaps is diff with v0 stack-frame gaps enabled in both engines.
func diffGaps(t *testing.T, text []sbpf.Slot, budget uint64) {
	t.Helper()
	testGaps = true
	defer func() { testGaps = false }()
	diffIn(t, text, budget, nil)
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
		var cd sbpf.ExcCallDest
		if errors.As(exc.Detail, &cd) {
			return "calldest"
		}
		var se sbpf.ExcSyscallError
		if errors.As(exc.Detail, &se) {
			return "syscallerr:" + se.Err.Error()
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
	compiled, err := Compile(testProg(benchLoop), false, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer compiled.Free()
	for i := 0; i < b.N; i++ {
		meter := cu.NewComputeMeter(100000000)
		_, cuUsed, err := compiled.Run(&meter, &benchMem, testRegistry, &meterVM{&meter})
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

func TestJITGappedStackRoundTrip(t *testing.T) {
	// Store/load through r10 with frame gaps on: the vaddr page must be
	// squeezed onto the flat backing identically in both engines.
	diffGaps(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 1, 0, 0, 0x1234),
		ins(sbpf.OpStxdw, 10, 1, -8, 0),
		ins(sbpf.OpLdxdw, 0, 10, -8, 0),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}, 100)
}

func TestJITGappedStackGapAccess(t *testing.T) {
	// r10 starts at VaddrStack+0x1000, the first gap page: a direct load
	// there faults on both engines, as does any odd page.
	for _, off := range []int16{0, 8, 0x7f8} {
		diffGaps(t, []sbpf.Slot{
			ins(sbpf.OpLdxdw, 0, 10, off, 0),
			ins(sbpf.OpExit, 0, 0, 0, 0),
		}, 100)
	}
}

func TestJITGappedStackPageSpan(t *testing.T) {
	// An 8-byte access starting in a frame page and running past its end
	// reads physically contiguous bytes in the interpreter; the JIT must
	// do the same rather than fault.
	diffGaps(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 1, 0, 0, 0xfffffff1),
		ins(sbpf.OpStxdw, 10, 1, -4, 0), // spans 0xffc..0x1004
		ins(sbpf.OpLdxdw, 0, 10, -4, 0),
		ins(sbpf.OpLdxw, 6, 10, -4, 0), // re-read halves
		ins(sbpf.OpAdd64Reg, 0, 6, 0, 0),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}, 100)
}

func TestJITGappedStackCallFrames(t *testing.T) {
	// With gaps the frame pointer advances two pages per call. The callee
	// writes its own frame and reads the caller's through a passed
	// pointer; the caller checks its locals survive.
	testFuncs = map[uint32]int64{hashOf(6): 6}
	defer func() { testFuncs = nil }()
	diffGaps(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 1, 0, 0, 111), // 0
		ins(sbpf.OpStxdw, 10, 1, -8, 0),    // 1: caller local
		ins(sbpf.OpMov64Reg, 1, 10, 0, 0),  // 2: r1 = caller fp
		ins(sbpf.OpCall, 0, 0, 0, hashOf(6)),
		ins(sbpf.OpLdxdw, 6, 10, -8, 0), // 4: reload caller local
		ins(sbpf.OpAdd64Reg, 0, 6, 0, 0),
		ins(sbpf.OpMov64Imm, 2, 0, 0, 222), // 6: callee
		ins(sbpf.OpStxdw, 10, 2, -8, 0),    // 7: callee local (new frame)
		ins(sbpf.OpLdxdw, 0, 1, -8, 0),     // 8: read caller's local via r1
		ins(sbpf.OpLdxdw, 3, 10, -8, 0),    // 9: reload own local
		ins(sbpf.OpAdd64Reg, 0, 3, 0, 0),   // 10: r0 = 111 + 222
		ins(sbpf.OpExit, 0, 0, 0, 0),       // 11: return
		ins(sbpf.OpExit, 0, 0, 0, 0),       // 12: (unreached)
	}, 1000)
}

func TestJITGappedStackDepth(t *testing.T) {
	// Recursion under gaps advances fp by 0x2000 each level; the 64th
	// frame ends exactly at the top of the doubled address space.
	testFuncs = map[uint32]int64{hashOf(1): 1}
	defer func() { testFuncs = nil }()
	diffGaps(t, []sbpf.Slot{
		ins(sbpf.OpCall, 0, 0, 0, hashOf(1)), // 0
		ins(sbpf.OpStxdw, 10, 1, -8, 0),      // 1: touch each frame
		ins(sbpf.OpCall, 0, 0, 0, hashOf(1)), // 2: recurse to depth fault
		ins(sbpf.OpExit, 0, 0, 0, 0),         // 3
	}, 1000000)
}

func TestJITGappedStackRandom(t *testing.T) {
	// Random offsets and widths around page boundaries, compared exactly
	// (values, faults and CU) against the interpreter.
	rng := rand.New(rand.NewSource(7))
	ops := []struct {
		st, ld uint8
		size   int
	}{
		{sbpf.OpStxb, sbpf.OpLdxb, 1},
		{sbpf.OpStxh, sbpf.OpLdxh, 2},
		{sbpf.OpStxw, sbpf.OpLdxw, 4},
		{sbpf.OpStxdw, sbpf.OpLdxdw, 8},
	}
	for round := 0; round < 200; round++ {
		op := ops[rng.Intn(len(ops))]
		// Bias offsets toward page edges. r10-relative, so negative
		// offsets land in frame 0 and positive ones probe the gap.
		off := int16(rng.Intn(0x1100) - 0x1000)
		if rng.Intn(2) == 0 {
			off = int16(rng.Intn(16) - 8)
		}
		text := []sbpf.Slot{
			ins(sbpf.OpMov64Imm, 1, 0, 0, uint32(rng.Uint64())),
			ins(op.st, 10, 1, off, 0),
			ins(op.ld, 0, 10, off, 0),
			ins(sbpf.OpExit, 0, 0, 0, 0),
		}
		diffGaps(t, text, 100)
	}
}

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

// callxTo builds `r9 = textVA + 8*pc + skew; callx r9` as two slots.
func callxTo(pc int, skew uint32) []sbpf.Slot {
	target := uint64(sbpf.VaddrProgram) + 8*uint64(pc) + uint64(skew)
	return []sbpf.Slot{
		ins(sbpf.OpLddw, 9, 0, 0, uint32(target)),
		sbpf.Slot(target >> 32 << 32),
		ins(sbpf.OpCallx, 0, 0, 0, 9), // v0: imm names the register
	}
}

func TestJITCallxBasic(t *testing.T) {
	// main: r0 = f() via callx; exit. f at pc 5.
	text := append(callxTo(5, 0),
		ins(sbpf.OpAdd64Imm, 0, 0, 0, 1), // 3: r0 = 43
		ins(sbpf.OpExit, 0, 0, 0, 0),     // 4
		ins(sbpf.OpMov64Imm, 0, 0, 0, 42), // 5: f
		ins(sbpf.OpExit, 0, 0, 0, 0),      // 6
	)
	diff(t, text, 100)
	diffGaps(t, text, 100)
}

func TestJITCallxUnaligned(t *testing.T) {
	// An unaligned target rounds down to the containing instruction.
	for _, skew := range []uint32{1, 3, 7} {
		text := append(callxTo(5, skew),
			ins(sbpf.OpAdd64Imm, 0, 0, 0, 1),
			ins(sbpf.OpExit, 0, 0, 0, 0),
			ins(sbpf.OpMov64Imm, 0, 0, 0, 42), // 5: f
			ins(sbpf.OpExit, 0, 0, 0, 0),
		)
		diff(t, text, 100)
	}
}

func TestJITCallxMidBlock(t *testing.T) {
	// Landing mid-block must charge exactly the remaining instructions
	// of the entered block; probe budgets around the boundary.
	text := append(callxTo(5, 0),
		ins(sbpf.OpMov64Reg, 0, 6, 0, 0), // 3: r0 = r6
		ins(sbpf.OpExit, 0, 0, 0, 0),     // 4
		ins(sbpf.OpMov64Imm, 6, 0, 0, 1), // 5: block start (skipped)
		ins(sbpf.OpAdd64Imm, 6, 0, 0, 2), // 6: callx lands here
		ins(sbpf.OpAdd64Imm, 6, 0, 0, 3), // 7
		ins(sbpf.OpExit, 0, 0, 0, 0),     // 8
	)
	entered := append(callxTo(6, 0), text[3:]...)
	for budget := uint64(1); budget < 12; budget++ {
		diff(t, text, budget)
		diff(t, entered, budget)
	}
}

func TestJITCallxOutOfBounds(t *testing.T) {
	// Below text, past the end, and far out: all must be bad accesses.
	for _, target := range []uint64{
		0, uint64(sbpf.VaddrProgram) - 8, uint64(sbpf.VaddrProgram) + 8*100,
		uint64(sbpf.VaddrStack), uint64(sbpf.VaddrStack) + 1, ^uint64(0),
	} {
		text := []sbpf.Slot{
			ins(sbpf.OpLddw, 9, 0, 0, uint32(target)),
			sbpf.Slot(target >> 32 << 32),
			ins(sbpf.OpCallx, 0, 0, 0, 9),
			ins(sbpf.OpExit, 0, 0, 0, 0),
		}
		diff(t, text, 100)
	}
}

func TestJITCallxDepthOverflow(t *testing.T) {
	// callx recursing into itself must hit the depth limit exactly.
	text := append(callxTo(0, 0),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	)
	diff(t, text, 1000000)
	diffGaps(t, text, 1000000)
}

func TestJITCallxIntoLddwImmediate(t *testing.T) {
	// pc 1 is lddw's second slot: the interpreter decodes the raw
	// immediate; the JIT deliberately refuses. Only the JIT side is
	// asserted — this is a documented divergence.
	text := append(callxTo(1, 0),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	)
	_, _, err := runJIT(t, text, 100, nil)
	require.ErrorIs(t, err, sbpf.ExcUnsupportedInstruction)
}

func TestJITCallxPreservesRegs(t *testing.T) {
	// Callee-saved registers must survive a callx like a static call.
	text := []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 6, 0, 0, 7),  // 0
		ins(sbpf.OpMov64Imm, 7, 0, 0, 11), // 1
		ins(sbpf.OpLddw, 9, 0, 0, uint32((uint64(sbpf.VaddrProgram)+8*8)&0xFFFFFFFF)), // 2
		sbpf.Slot((uint64(sbpf.VaddrProgram) + 8*8) >> 32 << 32),                     // 3
		ins(sbpf.OpCallx, 0, 0, 0, 9),                                                // 4
		ins(sbpf.OpAdd64Reg, 0, 6, 0, 0),                                 // 5: r0 += 7
		ins(sbpf.OpAdd64Reg, 0, 7, 0, 0),                                 // 6: r0 += 11
		ins(sbpf.OpExit, 0, 0, 0, 0),                                     // 7
		ins(sbpf.OpMov64Imm, 6, 0, 0, 100), // 8: f clobbers r6/r7
		ins(sbpf.OpMov64Imm, 7, 0, 0, 200), // 9
		ins(sbpf.OpMov64Imm, 0, 0, 0, 1),   // 10
		ins(sbpf.OpExit, 0, 0, 0, 0),       // 11
	}
	diff(t, text, 100)
}

func TestJITCallxSyscallInCallee(t *testing.T) {
	// A syscall inside a callx-reached function: exercises the resume
	// path with a dynamic call frame live.
	h := sbpf.SymbolHash("sum5")
	setSyscall(t, h, cuSyscall(2))
	text := append(callxTo(5, 0),
		ins(sbpf.OpAdd64Imm, 0, 0, 0, 1), // 3
		ins(sbpf.OpExit, 0, 0, 0, 0),     // 4
		ins(sbpf.OpMov64Imm, 1, 0, 0, 9), // 5: f
		ins(sbpf.OpCall, 0, 0, 0, h),     // 6: r0 = 9
		ins(sbpf.OpExit, 0, 0, 0, 0),     // 7
	)
	diff(t, text, 1000)
}

// cuSyscall is a syscall that consumes `cost` compute units and returns
// the sum of its five arguments. It ignores the VM.
func cuSyscall(cost uint64) sbpf.Syscall {
	return sbpf.SyscallFunc5(func(vm sbpf.VM, r1, r2, r3, r4, r5 uint64) (uint64, error) {
		if vm != nil {
			if err := vm.ComputeMeter().Consume(cost); err != nil {
				return 0, err
			}
		}
		return r1 + r2 + r3 + r4 + r5, nil
	})
}

func setSyscall(t *testing.T, hash uint32, sc sbpf.Syscall) {
	t.Helper()
	testRegistry = func(h uint32) (sbpf.Syscall, bool) {
		if h == hash {
			return sc, true
		}
		return nil, false
	}
	t.Cleanup(func() { testRegistry = nil })
}

func TestJITSyscallArgsAndReturn(t *testing.T) {
	h := sbpf.SymbolHash("sum5")
	setSyscall(t, h, cuSyscall(0))
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 1, 0, 0, 10),
		ins(sbpf.OpMov64Imm, 2, 0, 0, 20),
		ins(sbpf.OpMov64Imm, 3, 0, 0, 30),
		ins(sbpf.OpMov64Imm, 4, 0, 0, 40),
		ins(sbpf.OpMov64Imm, 5, 0, 0, 50),
		ins(sbpf.OpCall, 0, 0, 0, h), // r0 = 150
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}, 100)
}

func TestJITSyscallPreservesRegs(t *testing.T) {
	// r6 (callee-saved) and r1 must survive a syscall; only r0 changes.
	h := sbpf.SymbolHash("sum5")
	setSyscall(t, h, cuSyscall(0))
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 6, 0, 0, 777),
		ins(sbpf.OpMov64Imm, 1, 0, 0, 1),
		ins(sbpf.OpCall, 0, 0, 0, h),
		ins(sbpf.OpAdd64Reg, 0, 6, 0, 0), // r0 += 777
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}, 100)
}

func TestJITSyscallComputeCost(t *testing.T) {
	h := sbpf.SymbolHash("expensive")
	setSyscall(t, h, cuSyscall(30))
	// Budget lets the syscall run once but not twice.
	for _, budget := range []uint64{5, 35, 40, 70, 100} {
		diff(t, []sbpf.Slot{
			ins(sbpf.OpMov64Imm, 1, 0, 0, 1),
			ins(sbpf.OpCall, 0, 0, 0, h),
			ins(sbpf.OpCall, 0, 0, 0, h),
			ins(sbpf.OpExit, 0, 0, 0, 0),
		}, budget)
	}
}

func TestJITUnknownSyscall(t *testing.T) {
	// A call whose imm is neither a function nor (at JIT time) a syscall
	// stays on the interpreter; verify Compile refuses it.
	_, err := Compile(testProg([]sbpf.Slot{
		ins(sbpf.OpCall, 0, 0, 0, 0xdeadbeef),
		ins(sbpf.OpExit, 0, 0, 0, 0),
	}), false, nil)
	require.ErrorIs(t, err, ErrUnsupported)
}

func TestJITSyscallInCall(t *testing.T) {
	// A syscall inside a local function: the yield and resume must
	// preserve the native call frame so f's return still works.
	h := sbpf.SymbolHash("sum5")
	setSyscall(t, h, cuSyscall(2))
	testFuncs = map[uint32]int64{hashOf(4): 4}
	defer func() { testFuncs = nil }()
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 6, 0, 0, 500),   // 0: r6 = 500
		ins(sbpf.OpCall, 0, 0, 0, hashOf(4)), // 1: r0 = f()
		ins(sbpf.OpAdd64Reg, 0, 6, 0, 0),     // 2: r0 += r6
		ins(sbpf.OpExit, 0, 0, 0, 0),         // 3
		ins(sbpf.OpMov64Imm, 1, 0, 0, 7),     // 4: f: r1 = 7
		ins(sbpf.OpCall, 0, 0, 0, h),         // 5: r0 = sum5 = 7
		ins(sbpf.OpAdd64Imm, 0, 0, 0, 1),     // 6: r0 = 8
		ins(sbpf.OpExit, 0, 0, 0, 0),         // 7: return
	}, 1000)
}

func TestJITSyscallInNestedCalls(t *testing.T) {
	// Syscalls at depth 1, 2 and after returning; exercises repeated
	// yields with live native frames above and below.
	h := sbpf.SymbolHash("sum5")
	setSyscall(t, h, cuSyscall(3))
	testFuncs = map[uint32]int64{hashOf(6): 6, hashOf(12): 12}
	defer func() { testFuncs = nil }()
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 6, 0, 0, 1000),  // 0
		ins(sbpf.OpCall, 0, 0, 0, hashOf(6)), // 1: r0 = f()
		ins(sbpf.OpMov64Imm, 1, 0, 0, 1),     // 2: r1 = 1
		ins(sbpf.OpCall, 0, 0, 0, h),         // 3: top-level syscall
		ins(sbpf.OpAdd64Reg, 0, 6, 0, 0),     // 4: r0 += r6
		ins(sbpf.OpExit, 0, 0, 0, 0),         // 5
		ins(sbpf.OpMov64Imm, 7, 0, 0, 30),     // 6: f: r7 = 30
		ins(sbpf.OpCall, 0, 0, 0, hashOf(12)), // 7: r0 = g()
		ins(sbpf.OpMov64Reg, 1, 0, 0, 0),      // 8: r1 = r0
		ins(sbpf.OpCall, 0, 0, 0, h),          // 9: syscall at depth 1
		ins(sbpf.OpAdd64Reg, 0, 7, 0, 0),      // 10: r0 += r7
		ins(sbpf.OpExit, 0, 0, 0, 0),          // 11: return
		ins(sbpf.OpMov64Imm, 1, 0, 0, 5), // 12: g: r1 = 5
		ins(sbpf.OpMov64Imm, 2, 0, 0, 6), // 13: r2 = 6
		ins(sbpf.OpCall, 0, 0, 0, h),     // 14: syscall at depth 2
		ins(sbpf.OpExit, 0, 0, 0, 0),     // 15: return r0 = 11
	}, 1000)
}

func TestJITSyscallInRecursion(t *testing.T) {
	// fact-with-syscall: each level syscalls before recursing.
	h := sbpf.SymbolHash("sum5")
	setSyscall(t, h, cuSyscall(1))
	testFuncs = map[uint32]int64{hashOf(2): 2}
	defer func() { testFuncs = nil }()
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 1, 0, 0, 6),     // 0: r1 = 6
		ins(sbpf.OpCall, 0, 0, 0, hashOf(2)), // 1: r0 = fact(6)
		ins(sbpf.OpMov64Reg, 6, 1, 0, 0),     // 2: fact: r6 = n
		ins(sbpf.OpCall, 0, 0, 0, h),         // 3: r0 = sum5(n,..) = n
		ins(sbpf.OpJgtImm, 6, 0, 3, 1),       // 4: if n > 1 goto 8
		ins(sbpf.OpMov64Imm, 0, 0, 0, 1),     // 5: r0 = 1
		ins(sbpf.OpMov64Reg, 1, 6, 0, 0),     // 6: r1 = n (restore)
		ins(sbpf.OpExit, 0, 0, 0, 0),         // 7: return
		ins(sbpf.OpMov64Reg, 1, 6, 0, 0),     // 8: r1 = n
		ins(sbpf.OpSub64Imm, 1, 0, 0, 1),     // 9: r1 = n-1
		ins(sbpf.OpCall, 0, 0, 0, hashOf(2)), // 10: r0 = fact(n-1)
		ins(sbpf.OpMul64Reg, 0, 6, 0, 0),     // 11: r0 *= n
		ins(sbpf.OpMov64Reg, 1, 6, 0, 0),     // 12: r1 = n
		ins(sbpf.OpExit, 0, 0, 0, 0),         // 13: return
	}, 100000)
}

func TestJITSyscallInLoop(t *testing.T) {
	h := sbpf.SymbolHash("sum5")
	setSyscall(t, h, cuSyscall(2))
	// Call the syscall 5 times accumulating into r6.
	diff(t, []sbpf.Slot{
		ins(sbpf.OpMov64Imm, 6, 0, 0, 0), // 0: acc=0
		ins(sbpf.OpMov64Imm, 7, 0, 0, 5), // 1: count=5
		ins(sbpf.OpJeqImm, 7, 0, 5, 0),   // 2: if count==0 goto 8
		ins(sbpf.OpMov64Imm, 1, 0, 0, 3), // 3: arg
		ins(sbpf.OpCall, 0, 0, 0, h),     // 4: r0 = 3
		ins(sbpf.OpAdd64Reg, 6, 0, 0, 0), // 5: acc += r0
		ins(sbpf.OpSub64Imm, 7, 0, 0, 1), // 6: count--
		ins(sbpf.OpJa, 0, 0, -5, 0),      // 7: goto 2
		ins(sbpf.OpMov64Reg, 0, 6, 0, 0), // 8: r0 = acc
		ins(sbpf.OpExit, 0, 0, 0, 0),     // 9
	}, 1000)
}
