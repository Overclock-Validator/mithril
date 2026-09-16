# SHA-256 syscall overhead

The syscall decodes the already-translated slice descriptor array directly and
writes the final digest into the translated output buffer. It retains streaming
SHA-256, slice order, memory translations, CU charges and validation order. Output
is written only after all inputs have been read, preserving overlapping-buffer
behavior. No special case for a particular on-chain program is introduced.

A bounded 55-byte input-buffer prototype was slower than this simpler path and
is retained only as a benchmark comparison. The baseline reference is copied
from combined review commit `bd17683a`.

Local Apple M4 Pro, Go 1.26.4, GOMAXPROCS=2, five 200 ms samples per case;
medians below. Each benchmark runs serially through a real interpreter's memory
translation and CU meter, with VM creation outside the timed region. This does
not include VM instruction dispatch, a complete program, or block replay.

| Input | Original | Direct decoding/output | Buffered prototype |
|---|---:|---:|---:|
| 36 contiguous bytes | 74.69 ns | 43.84 ns | 51.98 ns |
| 32 + 4 bytes, two slices | 89.87 ns | 47.22 ns | 56.38 ns |
| 1,232 bytes | 428.4 ns | 382.1 ns | 396.8 ns |
| 4,096 bytes | 1,292 ns | 1,242 ns | 1,258 ns |

The two-slice case removes four allocations (112 bytes) per call. This is an
ARM64 component result, not a Zen 5 or full-block speedup claim. Measure native
Zen 5 and captured heavy-block replay before deployment decisions.

Reproduce with:

```sh
go test ./pkg/sealevel -run '^$' -bench '^BenchmarkSha256Syscall$' -benchtime=200ms -count=5
go test -race ./pkg/sealevel -run 'TestSha256SyscallDifferential|TestInterpreter_Sha256'
```

Differential tests compare hashes, return/error values, remaining CU and all
input/output memory over valid inputs, invalid descriptors/addresses, depleted
budgets and output aliasing. The existing SHA program fixture also executes.
Testing exposed pre-existing overflow in contiguous VM region bounds checks;
that correction and its regression test are a separate preceding commit. Both
benchmark variants use the corrected VM. The old SHA fixture also needed its
compute-meter pointer initialized for the current interpreter API.


## Zen 5 acceleration and captured loop

The live Go 1.26.4 validator on Ryzen 7 9700X had its actual
`crypto/internal/fips140/sha256.useSHANI` flag set to true. SHA acceleration is
already active. No validator runtime setting was changed.

The isolated loop harness copies text slots 518–545 from the captured SBF v0
program, resolves the SHA syscall relocation, and supplies 1,000 iterations and
zero initial state. It retains descriptor setup, stack accesses, digest copying,
counter update and loop branching. A test checks its result against a Go hash
chain and checks equal CU consumption for both syscall implementations. This
excludes transaction loading, account dependencies, CPI and the remaining program.

A locally cross-compiled Go 1.26.4 Linux/amd64 test binary ran with GOMAXPROCS=1,
affinity to CPU 15 and nice=19 on Zen 5. No build or deployment ran on that host.
Three 150 ms samples (medians, per hash iteration):

| Isolated loop | Time |
|---|---:|
| Original syscall | 234.5 ns |
| Optimized syscall | 165.9 ns |
| Dispatch-only diagnostic control | 109.2 ns |
| Go hash chain without VM | 54.69 ns |

The optimized loop takes about 29% less time. The dispatch-only control replaces
the syscall with a no-op: it omits hashing, translations and syscall CU charging,
and is only an overhead diagnostic, never a valid execution implementation.
The direct two-slice syscall measured 126–157 ns before and 59–63 ns after;
the buffered-input prototype remained slower at 71–73 ns. These short tests
share a host with other processes; they are not isolated-core latency guarantees.

A separate short CPU profile of the optimized loop attributed 36.2% cumulative
sampled CPU to the entire SHA syscall, including 14.8% of total CPU in the SHA-NI
compression routine. Most remaining sampled work was VM execution: instruction
dispatch/decoding, stack address translation, loads/stores and compute metering.
Cumulative and flat percentages overlap and must not be added. This profile is
of the harness, not of full-block replay or the live validator.

The next execution experiment should target measured VM overhead and then replay
captured blocks; these results do not justify a claimed 29% block-time improvement.

```sh
go test ./pkg/sealevel -run '^TestSha256CapturedLoop$'
go test ./pkg/sealevel -run '^$' -bench '^BenchmarkSha256CapturedLoop$' -benchtime=150ms -count=3
```
