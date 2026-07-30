package sbpf

import (
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/stretchr/testify/require"
)

// Microbenchmarks that isolate one interpreter cost each.
//
// Why these exist. The conformance corpus (3,398 vm-program fixtures) is the
// right gate for "did a change break anything", but ~0.8 ms per fixture is
// dominated by protobuf decoding and account setup, so a per-instruction
// saving disappears into harness noise there. These benchmarks execute long
// straight-line programs so that per-instruction cost is what the timer sees.
//
// Each benchmark reports ns/instr so results are comparable across shapes.
// Interpreter construction is per-iteration because Run mutates interpreter
// state; benchProgramLength is large enough that the setup amortises to well
// under a percent of the measurement. benchmarkSetupOverhead measures that
// claim rather than asserting it.

// benchProgramLength is the number of body instructions in each generated
// program. Large enough to amortise NewInterpreter, small enough that the text
// section stays comfortably inside L2.
const benchProgramLength = 100_000

// benchComputeBudget must exceed the instruction count: the interpreter
// consumes one compute unit per instruction, and a budget exhaustion would
// silently truncate the run and make the benchmark measure a shorter program
// than intended. runBenchProgram asserts the program ran to completion.
const benchComputeBudget = uint64(benchProgramLength * 4)

// benchProgram compiles the text once, outside the timed region.
//
// testV3Program serialises every slot into a fresh TextBytes buffer, which for
// a 100,000-instruction body is 800 KB of allocation and copying. Doing that
// per iteration put ~923 KB/op and its GC pressure inside the measurement,
// enough to bury the few-percent dispatch changes these benchmarks exist to
// detect. Reusing one Program also matches production, which caches Programs in
// accountsdb.ProgramCacheEntry and replays them; the interpreter treats text
// and rodata as read-only and keeps its mutable state in the heap, stack and
// input regions it allocates per run.
func benchProgram(tb testing.TB, text []Slot) *Program {
	tb.Helper()
	program := testV3Program(text, nil)
	if err := program.Verify(); err != nil {
		tb.Fatalf("benchmark program fails verification: %v", err)
	}
	return program
}

// runBenchProgram executes one program and fails the benchmark if it did not
// terminate normally. A benchmark whose program aborts early still produces a
// plausible-looking ns/op, so the error check is load-bearing rather than
// decorative.
func runBenchProgram(b *testing.B, program *Program, syscalls SyscallRegistry) {
	b.Helper()
	meter := cu.NewComputeMeter(benchComputeBudget)
	interpreter := NewInterpreter(program, &VMOpts{
		HeapMax:      1024,
		Syscalls:     syscalls,
		ComputeMeter: &meter,
	})
	_, _, err := interpreter.Run()
	interpreter.Finish()
	if err != nil {
		b.Fatalf("benchmark program did not complete: %v", err)
	}
}

func noSyscalls() SyscallRegistry {
	return func(uint32) (Syscall, bool) { return nil, false }
}

// silenceDebugLogging pins the log level at error before any benchmark runs.
//
// Two reasons, and the second is the important one. Cosmetically, Translate's
// failure path writes a debug line to stdout that lands mid-benchmark-result
// and eats the number. Substantively, error level is what a node actually runs
// with, and the point of the unmapped-translate benchmark is that
// runtime.Caller and runtime.FuncForPC still execute at that level, because
// they are arguments evaluated before Debugf decides whether to print. With
// debug logging left on, the benchmark would also be timing the log write and
// would overstate the effect.
//
// Dir is empty so this stays stderr-only and creates no files. Initialize is
// idempotent, so a package that has already configured logging keeps its own
// settings.
func silenceDebugLogging() {
	_ = mlog.Initialize(mlog.LogConfig{Level: "error", ToStdout: false}, "sbpf-bench")
}

func TestMain(m *testing.M) {
	silenceDebugLogging()
	os.Exit(m.Run())
}

// reportPerInstruction converts ns/op into ns per interpreted instruction.
// b.Elapsed covers only the timed region, so this stays correct if the
// benchmark body ever grows an untimed setup phase.
func reportPerInstruction(b *testing.B, instructionsPerOp int) {
	b.Helper()
	total := float64(b.N) * float64(instructionsPerOp)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/total, "ns/instr")
}

// benchALUProgram is straight-line 64-bit adds followed by an exit.
func benchALUProgram(instructions int) []Slot {
	text := make([]Slot, 0, instructions+1)
	for i := 0; i < instructions; i++ {
		text = append(text, testSlot(OpAdd64Imm, 1, 0, 0, 1))
	}
	return append(text, testSlot(OpExit, 0, 0, 0, 0))
}

// benchLoadStoreProgram alternates a 64-bit store and load through r10, so
// every access hits freshly written, always-mapped stack memory. r1 is seeded
// first so the stored bytes do not depend on uninitialised register state.
func benchLoadStoreProgram(pairs int) []Slot {
	text := make([]Slot, 0, pairs*2+2)
	text = append(text, testSlot(OpMov64Imm, 1, 0, 0, 0x5a5a5a5a))
	for i := 0; i < pairs; i++ {
		text = append(text, testSlot(OpStxdw, 10, 1, -8, 0))
		text = append(text, testSlot(OpLdxdw, 0, 10, -8, 0))
	}
	return append(text, testSlot(OpExit, 0, 0, 0, 0))
}

// BenchmarkInterpreterALUDispatch is the dispatch-cost benchmark. The body is
// straight-line 64-bit adds: no memory translation, no syscalls, no branches.
// What it measures is everything the interpreter does *around* an instruction
// rather than the instruction itself — the per-instruction version predicate
// (ip.sbpfVersion.EnableJmp32 is evaluated before the switch on every
// instruction, though the version is fixed for the whole run), the
// enableTracing field load and branch, computeMeter.Consume, getSlot, and the
// switch dispatch itself.
//
// This is the benchmark that should move if loop-invariant version predicates
// are hoisted into locals.
func BenchmarkInterpreterALUDispatch(b *testing.B) {
	text := benchALUProgram(benchProgramLength)
	program := benchProgram(b, text)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBenchProgram(b, program, noSyscalls())
	}
	reportPerInstruction(b, benchProgramLength)
}

// BenchmarkInterpreterLoadStore isolates translateInternal, the address
// translation every load and store pays. The body alternates a 64-bit store
// and a 64-bit load through r10, so the access is always to freshly written,
// always-mapped stack memory: the fast path, with no error construction.
//
// Compare against BenchmarkInterpreterALUDispatch to price translation alone.
func BenchmarkInterpreterLoadStore(b *testing.B) {
	const pairs = benchProgramLength / 2

	text := benchLoadStoreProgram(pairs)
	program := benchProgram(b, text)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBenchProgram(b, program, noSyscalls())
	}
	reportPerInstruction(b, pairs*2)
}

// benchSyscallCount is the number of syscall invocations per program. Syscalls
// are far more expensive than ALU instructions, so a shorter body keeps the
// benchmark quick while still amortising setup.
const benchSyscallCount = 20_000

// benchMappedAddr is an address that is genuinely mapped for the V3 programs
// these benchmarks build.
//
// Getting this wrong silently ruins the measurement: if the "mapped" control
// arm also fails translation, both arms run the same error path and the delta
// between them is zero. VaddrProgram is the obvious-looking choice and is
// *wrong* — sBPF v3 enables the lower rodata vaddr, so translateInternal
// rejects the VaddrProgram range as unmapped and puts program bytes at 0.
//
// The stack is the reliable choice. DynamicStackFrames is true only for v1 and
// v2, so a v3 program starts with r10 = VaddrStack + StackFrameSize, and the
// slot just below the frame pointer is the same one the load/store benchmark
// writes through r10-8.
const benchMappedAddr = VaddrStack + StackFrameSize - 8

// benchUnmappedAddr sits far above every mapped region, so translateInternal
// rejects it on the high-32-bits region switch rather than on a bounds check
// inside a region. That keeps the failure path short and identical across
// benchmarks.
const benchUnmappedAddr = uint64(0xdead_0000_0000_0000)

// translateSyscall returns a syscall that performs exactly one vm.Translate at
// a caller-chosen address, swallowing the error. Pointing it at a mapped
// address exercises the success path; pointing it at an unmapped address
// exercises Translate's error path, which calls runtime.Caller(1) and
// runtime.FuncForPC(pc).Name() to build a debug log line.
//
// Those two calls are *arguments* to mlog.Log.Debugf, so Go evaluates them
// before the logging call runs and therefore regardless of the configured log
// level. runtime.Caller unwinds the stack and FuncForPC does a symbol table
// lookup.
func translateSyscall(addr uint64) SyscallRegistry {
	hash := SymbolHash("bench_translate")
	fn := SyscallFunc0(func(vm VM) (uint64, error) {
		//nolint:errcheck // the error is the point; the benchmark measures how
		// expensive producing it is.
		_, _ = vm.Translate(addr, 8, false)
		return 0, nil
	})
	return func(got uint32) (Syscall, bool) {
		if got != hash {
			return nil, false
		}
		return fn, true
	}
}

// benchSyscallProgramN issues calls syscall invocations. src is zero here, so
// the call immediate is a symbol hash resolved through the registry, unlike
// the relative calls in benchCallReturnProgram which set src to one.
func benchSyscallProgramN(calls int) []Slot {
	hash := SymbolHash("bench_translate")
	text := make([]Slot, 0, calls+1)
	for i := 0; i < calls; i++ {
		text = append(text, testSlot(OpCall, 0, 0, 0, hash))
	}
	return append(text, testSlot(OpExit, 0, 0, 0, 0))
}

func benchSyscallProgram() []Slot { return benchSyscallProgramN(benchSyscallCount) }

// BenchmarkInterpreterSyscallTranslateMapped is the control arm: a syscall
// doing one successful Translate per call, against the mapped stack slot
// described at benchMappedAddr.
func BenchmarkInterpreterSyscallTranslateMapped(b *testing.B) {
	text := benchSyscallProgram()
	program := benchProgram(b, text)
	syscalls := translateSyscall(benchMappedAddr)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBenchProgram(b, program, syscalls)
	}
	reportPerInstruction(b, benchSyscallCount)
}

// BenchmarkInterpreterSyscallTranslateUnmapped is the measurement arm. The
// only difference from the mapped case is that Translate fails, so the delta
// between the two benchmarks is the cost of Translate's error path — the
// runtime.Caller stack unwind plus the FuncForPC symbol lookup.
//
// This is an amplification measurement, not just a performance one: a program
// can drive failed translations in a loop, and each one is charged the same
// single compute unit as an add.
func BenchmarkInterpreterSyscallTranslateUnmapped(b *testing.B) {
	text := benchSyscallProgram()
	program := benchProgram(b, text)
	// Far above every mapped region, so translateInternal rejects it on the
	// region switch rather than on a bounds check inside a region.
	syscalls := translateSyscall(benchUnmappedAddr)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBenchProgram(b, program, syscalls)
	}
	reportPerInstruction(b, benchSyscallCount)
}

// benchCallReturnProgram builds `calls` relative calls that all target one
// shared single-instruction subroutine.
//
// The layout matters and the obvious version is wrong. A relative call jumps
// to pc+imm+1, not pc+imm, so a naive "call +1 / exit" pair targets the *next
// call* rather than the exit beside it, and the program recurses until the
// call stack overflows instead of measuring frame push and pop.
//
//	pc 0 .. n-1 : call -> subroutine, each returning to the following call
//	pc n         : exit, reached once the last call returns; ends the program
//	pc n+1       : exit, the shared subroutine body
//
// Depth therefore stays at one frame throughout.
func benchCallReturnProgram(calls int) []Slot {
	text := make([]Slot, 0, calls+2)
	subroutine := calls + 1
	for pc := 0; pc < calls; pc++ {
		// target = pc + imm + 1, so imm = subroutine - pc - 1.
		imm := uint32(subroutine - pc - 1)
		text = append(text, testSlot(OpCall, 0, 1, 0, imm))
	}
	text = append(text, testSlot(OpExit, 0, 0, 0, 0))
	text = append(text, testSlot(OpExit, 0, 0, 0, 0))
	return text
}

// BenchmarkInterpreterCallReturn prices call frame push and pop. Each call
// enters the shared subroutine and returns immediately, so the measurement is
// frame handling rather than deep recursion. Two instructions execute per
// call: the call itself and the subroutine's exit.
func BenchmarkInterpreterCallReturn(b *testing.B) {
	const calls = benchProgramLength / 4

	text := benchCallReturnProgram(calls)
	program := benchProgram(b, text)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBenchProgram(b, program, noSyscalls())
	}
	reportPerInstruction(b, calls*2)
}

// benchTranslateInterpreter builds a minimal interpreter for the direct
// translate benchmarks below. It is never Run; only its memory map is used.
func benchTranslateInterpreter(b *testing.B) *Interpreter {
	b.Helper()
	program := testV3Program([]Slot{testSlot(OpExit, 0, 0, 0, 0)}, nil)
	meter := cu.NewComputeMeter(benchComputeBudget)
	return NewInterpreter(program, &VMOpts{
		HeapMax:      1024,
		Syscalls:     noSyscalls(),
		ComputeMeter: &meter,
	})
}

// The four benchmarks below call the translation layer directly rather than
// through the interpreter loop, which is the only way to attribute the cost of
// a failed translation between its two components.
//
// Translate wraps translateInternal and, on the error path only, adds
// runtime.Caller(1), runtime.FuncForPC(pc).Name(), and a Debugf call.
// translateInternal is what the interpreter's own loads and stores use, and it
// has none of that. So:
//
//	(TranslateUnmapped - TranslateMapped)                 = full failure cost
//	(TranslateInternalUnmapped - TranslateInternalMapped) = error construction only
//	difference between those two                          = runtime.Caller + logging
//
// Going through the interpreter cannot separate these, because a failing load
// aborts the program and only one failure per run is reachable.

func BenchmarkTranslateMappedDirect(b *testing.B) {
	ip := benchTranslateInterpreter(b)
	defer ip.Finish()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		//nolint:errcheck // success path; error is always nil here.
		_, _ = ip.Translate(benchMappedAddr, 8, false)
	}
}

func BenchmarkTranslateUnmappedDirect(b *testing.B) {
	ip := benchTranslateInterpreter(b)
	defer ip.Finish()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		//nolint:errcheck // the error is what is being measured.
		_, _ = ip.Translate(benchUnmappedAddr, 8, false)
	}
}

func BenchmarkTranslateInternalMappedDirect(b *testing.B) {
	ip := benchTranslateInterpreter(b)
	defer ip.Finish()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		//nolint:errcheck // success path.
		_, _ = ip.translateInternal(benchMappedAddr, 8, false)
	}
}

func BenchmarkTranslateInternalUnmappedDirect(b *testing.B) {
	ip := benchTranslateInterpreter(b)
	defer ip.Finish()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		//nolint:errcheck // the error is what is being measured.
		_, _ = ip.translateInternal(benchUnmappedAddr, 8, false)
	}
}

// BenchmarkInterpreterSetupOverhead measures construction and teardown with a
// one-instruction program, which is the fixed cost every other benchmark pays
// once per iteration. Divide it by benchProgramLength to confirm it is a
// negligible share of the per-instruction numbers above rather than assuming
// so.
func BenchmarkInterpreterSetupOverhead(b *testing.B) {
	text := []Slot{testSlot(OpExit, 0, 0, 0, 0)}
	program := benchProgram(b, text)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBenchProgram(b, program, noSyscalls())
	}
}

// TestBenchProgramsAreValid guards the benchmarks themselves. Each generated
// program must pass the verifier and run to completion; otherwise a benchmark
// could measure an early abort and report a fast, meaningless number. This is
// the same failure mode as a benchmark whose body is optimised away.
//
// Every case calls the same generator its benchmark does. An earlier version
// hand-wrote a short equivalent per case instead, which is exactly how the
// call/return program shipped broken: the three-instruction stand-in happened
// to terminate while the generated one recursed until the stack overflowed.
// Only the shared generator makes this test load-bearing.
func TestBenchProgramsAreValid(t *testing.T) {
	cases := []struct {
		name     string
		text     []Slot
		syscalls SyscallRegistry
	}{
		{"alu", benchALUProgram(16), noSyscalls()},
		{"loadstore", benchLoadStoreProgram(8), noSyscalls()},
		{"callreturn", benchCallReturnProgram(8), noSyscalls()},
		{"syscall-mapped", benchSyscallProgramN(8), translateSyscall(benchMappedAddr)},
		{"syscall-unmapped", benchSyscallProgramN(8), translateSyscall(benchUnmappedAddr)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			program := testV3Program(tc.text, nil)
			require.NoError(t, program.Verify(), "benchmark program fails verification")

			meter := cu.NewComputeMeter(benchComputeBudget)
			interpreter := NewInterpreter(program, &VMOpts{
				HeapMax:      1024,
				Syscalls:     tc.syscalls,
				ComputeMeter: &meter,
			})
			defer interpreter.Finish()

			_, _, err := interpreter.Run()
			require.NoError(t, err, "benchmark program does not run to completion")
		})
	}
}
