# SHA-256 syscall validation

The syscall decodes the translated slice descriptors directly and writes the
final digest into the translated output buffer. It retains streaming SHA-256,
slice order, memory translations, CU charges and validation order. Output is
written only after all inputs have been read, preserving overlapping-buffer
behavior.

The frozen reference implementation in the test harness supports differential
checks and before/after benchmarks. Both variants use the same VM, including
its contiguous-region bounds-overflow fix.

Differential tests compare hashes, return/error values, remaining CU and all
input/output memory over valid inputs, invalid descriptors/addresses, depleted
budgets and output aliasing. The existing SHA program fixture also executes.

## Benchmarks

`BenchmarkSha256Syscall` measures the syscall through a real interpreter's memory
translation and CU meter, with VM creation outside the timed region. It covers
empty, single-slice, multiple-slice and larger inputs; it excludes instruction
dispatch and transaction execution.

`BenchmarkSha256CapturedLoop` uses a captured SBF v0 hash loop with 1,000
iterations and zero initial state. It retains descriptor setup, stack accesses,
digest copying, counter updates and branching. Its test checks the result against
a Go hash chain and checks equal CU consumption for both syscall implementations.
The captured ELF hash and extracted instruction range are recorded in the test.
Transaction loading, CPI and the rest of the original program are excluded.

The dispatch-only control omits hashing, translations and syscall CU charging;
it is an overhead diagnostic, not a valid execution implementation. The raw Go
hash chain provides another comparison outside the VM. Neither control can
establish a full-block speedup.

```sh
go test ./pkg/sealevel -run 'TestSha256SyscallDifferential|TestInterpreter_Sha256|TestSha256CapturedLoop'
go test -race ./pkg/sealevel -run 'TestSha256SyscallDifferential|TestInterpreter_Sha256'
go test ./pkg/sealevel -run '^$' -bench '^BenchmarkSha256(Syscall|CapturedLoop)$' -benchtime=1s -count=5 -benchmem
```

Alternate baseline/candidate run order and record commit IDs, Go version,
hardware and CPU affinity. Report component timings separately from full-program
and replay timings; hardware SHA acceleration also affects these results.
