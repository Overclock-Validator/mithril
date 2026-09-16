# Interpreter performance validation

This experiment compares `9db7dcb` plus identical benchmark code with the same
base plus the interpreter changes in `2ed5e533`. The separate arithmetic-shift
semantics patch is excluded. Three VASA tests were updated to pass the new
`*[16]uint64` register type; no further production optimization was added during
this measurement pass.

## Individual programs

Zen 5 / Ryzen 7 9700X, Go 1.26.4, `GOMAXPROCS=1`, CPU 15, nice 19, five
one-second samples per variant with alternating run order. The existing validator
continued running. CPU 15 shares a physical core with CPU 7; these are shared-host
measurements rather than an isolated-machine throughput ceiling.

The harness has a preloaded program cache and asserts a cache hit before timing.
Every invocation gets fresh account data and execution context. It measures
instruction setup, serialization, execution and publication together; it is not
just the interpreter loop. Timing instrumentation and instruction recording are
enabled identically on both versions. Tests check arithmetic return values,
post-transfer balances, allocation results, and recorded CPI. Requested budgets
are equal and the measured CU charges match between variants.

| Workload | CU | Median before → after | Speedup |
|---|---:|---:|---:|
| Token-2022 TransferChecked, no extensions | 1,720 | 19.52 → 13.70 µs | 1.42× |
| Same, VASA | 1,720 | 21.78 → 16.11 µs | 1.35× |
| Arithmetic, 500 iterations | 5,631 | 20.91 → 12.71 µs | 1.65× |
| Arithmetic, 5,000 iterations | 55,131 | 167.24 → 94.55 µs | 1.77× |
| Rust CPI to System Allocate | 2,346 | 16.75 → 13.77 µs | 1.22× |
| Same, VASA | 2,346 | 18.97 → 15.50 µs | 1.22× |
| BPF lamport-transfer fixture | 2,895 | 22.09 → 17.57 µs | 1.26× |

The arithmetic VASA cases measured 20.09 → 12.07 µs and 170.24 → 97.90 µs.
The BPF lamport-transfer VASA case measured 23.92 → 20.33 µs. These differ from
the earlier SPL Token loader-only benchmark: they use different program binaries,
instructions, and include execution-context setup.

Set `MITHRIL_PROGRAM_BENCH_DIR` to a directory containing `rotation_compute.so`
and `token2022.so` to enable those external fixtures. Without it, the in-repository
BPF/CPI fixtures still run. Pinned input SHA-256 values:

- Arithmetic ELF: `db7c55d6563c879e35dfe2b24edb0fe0515d5a5ae627fe00e3e247001c786441`.
  Source: `ag-transaction-bench` at `7e5a263fa5a1c72088f191daf5b7c5d2484c997c`,
  `transaction-bench/program/src/rotation_compute.c`.
- Token-2022 ELF: `a794161408080f690dac00832f45b3c3e2b71f1339586667ad1f979cf91d5b68`.
  Public Alpenglow program `TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb`,
  fetched at RPC context slot 4,231,444, program-data account
  `DoU57AYuPFu2QU514RktNPG22QhApEjnKxnBcu4BHDTY`. Strip its 45-byte upgradeable
  loader metadata before saving the ELF. Verify the hash; do not silently replace
  it with a later deployment.

```
MITHRIL_PROGRAM_BENCH_DIR=/path/to/pinned-fixtures GOMAXPROCS=1 \
  go test ./pkg/sealevel -run '^TestProgramWorkloadResults$' -v
MITHRIL_PROGRAM_BENCH_DIR=/path/to/pinned-fixtures GOMAXPROCS=1 \
  go test ./pkg/sealevel -run '^$' -bench '^BenchmarkProgramWorkloads$' \
  -benchtime=1s -count=5 -benchmem
```

## Correctness and comparison boundaries

The generated-program harness compares return values, errors, CU usage and memory
for 100,000 programs. Set `SBPF_DIFF_OUT` separately on the reference and candidate
and compare the files; `SBPF_CHECK_POOL_ZERO=1` also checks reused memory. ARSH and
verifier semantics changes are excluded from this performance work.

For replay comparisons, use fresh isolated AccountsDBs from the same snapshots,
identical transaction parallelism, and the same slot interval. Compare normalized
per-slot bank hashes and slot sets before interpreting timings. Compare exact
`ProcessBlock` wall-clock timers, not summed instruction/worker timers. Alternate
run order and retain raw outputs plus commit IDs outside the merge diff.

The PR description links the recorded Alpenglow replay results and raw evidence.
Single-core shared-host results do not establish multicore contention or live FAST
inclusion gains. The baseline has failing legacy BPF-loader tests; do not describe
a targeted test pass as a complete sealevel-suite pass. `TestInterpreter_Noop` now
supplies its execution context's compute meter.

## Memory syscalls and LtHash

Memory syscalls retain CU charges, source-before-destination error order,
zero-length behavior, memcpy overlap rejection and memmove overlap support.
Tests cover copy-on-write/growing regions and differential memory/CU results.

LtHash uses AVX2 only when supported by both CPU and OS; other architectures and
`-tags purego` use portable loops. The vector path preserves 16-bit wraparound and
in-place operand aliasing. Randomized, unaligned, inverse and fallback tests cover
both paths. Component speedups are not block-latency speedups.

```sh
go test ./pkg/metrics ./pkg/lthash ./pkg/sbpf ./pkg/sbpf/loader ./pkg/replay
go test -tags purego ./pkg/lthash
go test -race ./pkg/sealevel -run 'TestSyscallMem|TestMemoryCopyDifferential|TestProgramWorkloadResults'
SBPF_DIFF_OUT=/tmp/candidate-diff.txt SBPF_CHECK_POOL_ZERO=1 go test ./pkg/sbpf -run TestDifferentialDump -count=1
go test ./pkg/lthash -run '^$' -bench BenchmarkMix -benchmem -count=5
```
