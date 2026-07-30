package sbpf

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/stretchr/testify/require"
)

// runForCU executes one program and reports the compute units the meter
// actually gave up, plus the error. It reads the meter rather than Run's
// cuConsumed return value because Run returns zero for that on the error paths
// that build an *Exception, while the caller in pkg/sealevel reads the shared
// meter. The meter is what feeds the rest of the transaction, so the meter is
// what these tests pin.
func runForCU(t *testing.T, text []Slot, budget uint64, syscalls SyscallRegistry) (uint64, error) {
	t.Helper()
	program := testV3Program(text, nil)
	require.NoError(t, program.Verify())

	meter := cu.NewComputeMeter(budget)
	interpreter := NewInterpreter(program, &VMOpts{
		HeapMax:      1024,
		Syscalls:     syscalls,
		ComputeMeter: &meter,
	})
	_, _, err := interpreter.Run()
	interpreter.Finish()
	return meter.Used(), err
}

// movThenFallOffEnd builds count copies of `mov64 r0, 1` and no exit, so
// execution runs off the end of the text section. Neither verifier rejects
// this: Agave's only rejects a truncated lddw at the tail
// (solana-sbpf 0.21.1 verifier.rs:411-417), so a one-instruction program is
// enough to reach the overrun path.
func movThenFallOffEnd(count int) []Slot {
	text := make([]Slot, count)
	for i := range text {
		text[i] = testSlot(OpMov64Imm, 0, 0, 0, 1)
	}
	return text
}

// TestInterpreterChargesTheOverrunningInstruction pins the compute-unit and
// fault-class behaviour of the top of the Run loop against Agave.
//
// Agave charges the instruction that runs off the end of the text section, and
// prefers an exhausted meter over the overrun, because solana-sbpf 0.21.1
// interpreter.rs tests the meter at :188, increments due_insn_count at :191 and
// only then bounds-checks pc at :193.
//
// The expectations below are ground truth, measured by running the exact crate
// Agave pins (agave Cargo.toml:471 -> solana-sbpf =0.21.1) over these same
// programs:
//
//	n=1 budget=1  -> Err(ExceededMaxInstructions) insn_count=1
//	n=1 budget=11 -> Err(ExecutionOverrun)        insn_count=2
//	n=3 budget=3  -> Err(ExceededMaxInstructions) insn_count=3
//	n=3 budget=13 -> Err(ExecutionOverrun)        insn_count=4
//
// Both Agave fault classes reach the runtime as
// InstructionError::ProgramFailedToComplete -- Agave's own bpf_loader test
// pins the exhausted-meter case to it (programs/bpf_loader/src/lib.rs, "Case:
// limited budget") -- and normalizeProgramRunErr maps Mithril's two
// corresponding errors to InstrErrProgramFailedToComplete as well. So the fault
// class is not independently observable here; the charged compute units are,
// because they set the budget the rest of the transaction runs on.
func TestInterpreterChargesTheOverrunningInstruction(t *testing.T) {
	const (
		exhausted = "meter exhausted"
		overrun   = "execution overrun"
	)
	cases := []struct {
		instructions int
		budget       uint64
		wantCU       uint64
		wantFault    string
	}{
		{instructions: 1, budget: 1, wantCU: 1, wantFault: exhausted},
		{instructions: 1, budget: 11, wantCU: 2, wantFault: overrun},
		{instructions: 3, budget: 3, wantCU: 3, wantFault: exhausted},
		{instructions: 3, budget: 13, wantCU: 4, wantFault: overrun},
	}

	for _, tc := range cases {
		name := fmt.Sprintf("n=%d/budget=%d", tc.instructions, tc.budget)
		t.Run(name, func(t *testing.T) {
			used, err := runForCU(t, movThenFallOffEnd(tc.instructions), tc.budget, noSyscalls())
			require.Error(t, err)

			gotFault := ""
			switch {
			case errors.Is(err, cu.ErrComputeExceeded), errors.Is(err, ExcOutOfCU):
				gotFault = exhausted
			case errors.Is(err, ExcExecutionOverrun):
				gotFault = overrun
			default:
				t.Fatalf("unexpected fault class: %v", err)
			}
			require.Equal(t, tc.wantFault, gotFault, "fault class")
			require.Equal(t, tc.wantCU, used, "compute units charged")
		})
	}
}

// TestBenchProgramCUCounts is the compute-unit parity guard for interpreter
// optimisation work. The benchmarks in interpreter_bench_test.go measure only
// speed, so a change that made the interpreter faster by charging a different
// number of compute units would show up as a clean win and as a bank hash
// mismatch in production. This test fails instead.
//
// Every expectation is derived from the program shape rather than recorded from
// a run, so a wrong count is a real disagreement and not a rubber stamp. The
// interpreter charges exactly one unit per instruction it dispatches, and none
// of these syscalls consume units of their own.
func TestBenchProgramCUCounts(t *testing.T) {
	const n = 8

	cases := []struct {
		name     string
		text     []Slot
		syscalls SyscallRegistry
		wantCU   uint64
		why      string
	}{
		{
			name:     "alu",
			text:     benchALUProgram(n),
			syscalls: noSyscalls(),
			wantCU:   n + 1,
			why:      "n adds, then the exit that ends the run",
		},
		{
			name:     "loadstore",
			text:     benchLoadStoreProgram(n),
			syscalls: noSyscalls(),
			wantCU:   2*n + 2,
			why:      "one seeding mov, n store/load pairs, then exit",
		},
		{
			name:     "callreturn",
			text:     benchCallReturnProgram(n),
			syscalls: noSyscalls(),
			wantCU:   2*n + 1,
			why:      "each call plus the shared subroutine's exit, then the trailing exit",
		},
		{
			name:     "syscall-mapped",
			text:     benchSyscallProgramN(n),
			syscalls: translateSyscall(benchMappedAddr),
			wantCU:   n + 1,
			why:      "n syscall dispatches, then exit; the syscall consumes nothing itself",
		},
		{
			name:     "syscall-unmapped",
			text:     benchSyscallProgramN(n),
			syscalls: translateSyscall(benchUnmappedAddr),
			wantCU:   n + 1,
			why:      "a failed translation inside a syscall costs no extra units",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			used, err := runForCU(t, tc.text, benchComputeBudget, tc.syscalls)
			require.NoError(t, err, "benchmark program must run to completion")
			require.Equal(t, tc.wantCU, used, "compute units: %s", tc.why)
		})
	}
}
