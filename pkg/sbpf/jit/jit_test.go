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

// runInterpreter executes text on the interpreter with the given budget.
func runInterpreter(text []sbpf.Slot, budget uint64) (uint64, uint64, error) {
	prog := &sbpf.Program{
		Text:        text,
		TextVA:      sbpf.VaddrProgram,
		Entrypoint:  0,
		SbpfVersion: v0,
	}
	meter := cu.NewComputeMeter(budget)
	interp := sbpf.NewInterpreter(prog, &sbpf.VMOpts{
		HeapMax:      1024,
		Syscalls:     func(uint32) (sbpf.Syscall, bool) { return nil, false },
		ComputeMeter: &meter,
	})
	defer interp.Finish()
	return interp.Run()
}

// runJIT compiles and executes text with the given budget.
func runJIT(t *testing.T, text []sbpf.Slot, budget uint64) (uint64, uint64, error) {
	t.Helper()
	compiled, err := Compile(text, v0, 0)
	require.NoError(t, err)
	defer compiled.Free()
	meter := cu.NewComputeMeter(budget)
	return compiled.Run(&meter)
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
		return "exception:" + exc.Detail.Error()
	}
	return err.Error()
}

// diff runs text through both engines and requires identical results.
func diff(t *testing.T, text []sbpf.Slot, budget uint64) {
	t.Helper()
	iRet, iCU, iErr := runInterpreter(text, budget)
	jRet, jCU, jErr := runJIT(t, text, budget)
	require.Equal(t, errClass(iErr), errClass(jErr), "error mismatch")
	require.Equal(t, iCU, jCU, "cu mismatch")
	if iErr == nil {
		require.Equal(t, iRet, jRet, "result mismatch")
	}
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
	compiled, err := Compile(benchLoop, v0, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer compiled.Free()
	for i := 0; i < b.N; i++ {
		meter := cu.NewComputeMeter(100000000)
		_, cuUsed, err := compiled.Run(&meter)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(cuUsed), "insns")
	}
}
