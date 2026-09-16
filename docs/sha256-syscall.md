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
