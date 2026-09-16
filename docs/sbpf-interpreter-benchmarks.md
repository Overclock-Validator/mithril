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

## Recorded Alpenglow blocks

Each run bootstrapped a fresh isolated AccountsDB from the same public full
snapshot at 4,150,503 and incremental snapshot at 4,231,162. Non-voting RPC replay
covered slots **4,231,163–4,231,418**: 233 replayed blocks, 23 skipped slots, and
142 blocks with sBPF execution. It ran with `--txpar 1`, `GOMAXPROCS=1`, CPU 15,
nice 19. Two paired runs used baseline/candidate then candidate/baseline order.
The live validator's AccountsDB and configuration were not used or changed.

All 233 per-slot bank hashes matched between baseline and candidate in both
pairs, excluding the run-specific comment header in `bankhash.log`.

| Work measured | Pair 1 before → after | Pair 2 before → after |
|---|---:|---:|
| All-block median ProcessBlock | 210.34 → 139.08 ms | 206.12 → 142.83 ms |
| All-block total ProcessBlock | 46.12 → 35.49 s | 45.16 → 35.94 s |
| sBPF-block median ProcessBlock | 253.17 → 158.95 ms | 232.03 → 164.95 ms |
| All-block p95 ProcessBlock | 433.13 → 437.99 ms | 420.52 → 442.43 ms |
| All-block p99 ProcessBlock | 556.05 → 552.07 ms | 607.83 → 568.22 ms |

The sample includes blocks around 40–46 million CU. Three inspected non-empty
blocks used the System program, the AogGeA81 hash-loop workload, SPL Token, and
Memo. This is not evidence for DEX or lending workloads. RPC per-program summaries
attribute whole-transaction CU to every participating program and must not be
summed as if they were exclusive per-program execution costs.

Whole-block p95 did not improve, and the p99 changes are small/variable. The
slowest candidate blocks in the first pair contained no sBPF execution; their
large timers were dispatch and signature verification. These single-CPU replay
results do not establish a live voting/FAST improvement or production parallel
replay latency. They exclude network wait from ProcessBlock and are not elapsed
end-to-end catch-up times.

## Correctness and limits

- Native Zen 5 baseline and candidate differential outputs match for 100,000
  deterministic generated programs; candidate pool-zero checks pass.
- Baseline/candidate workload effects and CU charges match. Targeted race tests
  for the interpreter, loader, and workload harness pass; vet passes.
- An older `TestInterpreter_Noop` test panics on both the baseline and candidate;
  therefore no complete sealevel test-suite pass is claimed. Broader conformance
  testing remains separate from this performance experiment.
- No candidate was deployed and no validator restart was needed.
