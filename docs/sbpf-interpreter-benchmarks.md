# Execution performance validation

## Program workloads

The program harness measures instruction setup, serialization, execution and
publication with a warm program cache and fresh account data per invocation.
It checks return values, account updates, CPI and CU consumption. Loader-only
benchmarks separately measure VM execution and program loading.

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

Record tested commit IDs, hardware, Go version, affinity and parallelism with results.
Single-core shared-host results do not establish multicore contention or live FAST
inclusion gains.

## Memory syscalls and LtHash

Memory syscalls retain CU charges, source-before-destination error order,
zero-length behavior, memcpy overlap rejection and memmove overlap support.
Tests cover copy-on-write/growing regions and differential memory/CU results.

LtHash uses AVX2 only when supported by both CPU and OS; other architectures and
`-tags purego` use portable loops. The vector path preserves 16-bit wraparound and
in-place operand aliasing. Randomized, unaligned, inverse and fallback tests cover
both paths. Component speedups are not block-latency speedups.

```sh
go test ./pkg/lthash ./pkg/sbpf ./pkg/sbpf/loader
go test -tags purego ./pkg/lthash
go test -race ./pkg/sealevel -run 'TestSyscallMem|TestMemoryCopyDifferential|TestProgramWorkloadResults'
SBPF_DIFF_OUT=/tmp/candidate-diff.txt SBPF_CHECK_POOL_ZERO=1 go test ./pkg/sbpf -run TestDifferentialDump -count=1
go test ./pkg/lthash -run '^$' -bench BenchmarkMix -benchmem -count=5
```
