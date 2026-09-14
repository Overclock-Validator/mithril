# Signature batching and shred-arrival overlap on Zen 5

Implementation: `05ded9b0` on `7layer/planner-pr257-trial-20260912`, building on
`8a94482e`. Narya is pinned to `c265ee9667131a556cd332c252b79de0860c7640`.

The change forms signature groups before dispatch and keeps workers supplied
without wave barriers. It also decodes and verifies complete entry components
while later shreds arrive. The selected default is **two workers, target eight,
no batching timer**. Four available signatures run immediately; they do not wait
for eight. Four workers remain an explicit option for catch-up.

## Environment and interpretation

- Ryzen 7 9700X, eight physical cores / sixteen hardware threads; Go 1.26.4,
  native Narya `r51`, `GOMAXPROCS=8`, affinity logical CPUs 0–7 (distinct cores).
- Benchmark processes ran at nice 10 while the existing Mithril validator and
  Lightbringer continued running. These are shared-host measurements.
- Each assembly condition has three repetitions of three blocks. Tables show
  medians of the reported per-repetition values; tail columns elsewhere are
  medians of per-repetition p95s, not an aggregate percentile or confidence bound.
- Captured pool measurements include three public testnet blocks, each with
  33,760 one-signature V1 transactions, 228 wire bytes / 164 signed message bytes.
  Assembly uses block 2622581's transactions in newly generated valid Merkle
  shreds. The maximum-size case uses 33,760 distinct signed generated transactions
  of exactly 1,232 wire bytes, including their signatures.
- Tip arrival schedules distribute complete component bursts across 200 ms.
  Components use a 60 KiB transaction-byte budget, a benchmark choice rather than
  a protocol requirement. Assembly generates real entry overhead and FEC padding.
  This is not a recorded arrival trace or a whole-validator replay benchmark.

## Full assembly: what remains after the final shred

These cases run production assembly, entry decoding, final Merkle identity
checks, transaction signature verification, and block publication. Shred
construction, packet parsing, and shred-signature authentication are outside the
measurement. Transaction execution and commit are excluded.

Both sides use the **new batching pool**. “Off” waits for all shreds before
transaction decoding/verification; “on” overlaps that work with arrivals. This
isolates overlap, rather than comparing against an older Narya or batching PR.

| Wire bytes/tx | Workers | Full-to-ready off → on | Total arrival-to-ready off → on | CPU-ms/block off → on |
|---|---:|---:|---:|---:|
| 228 | 2 | 85.88 → **7.76 ms** | 287.68 → **208.06 ms** | 177.4 → 185.8 |
| 228 | 4 | 49.88 → **7.83 ms** | 250.71 → **207.82 ms** | 177.6 → 186.4 |
| 1,232 | 2 | 118.00 → **28.97 ms** | 318.80 → **229.37 ms** | 242.5 → 266.5 |
| 1,232 | 4 | 77.04 → **25.69 ms** | 276.59 → **227.10 ms** | 241.8 → 264.9 |

Every row has 33,760 transactions and target eight. Actual shred collection stayed
around 200–202 ms, so the improvement did not come from delaying the synthetic
feed. With two workers, early verification covered all 33,760 captured
transactions and about 33,744 maximum-size transactions before full assembly.
Final signature joins were approximately 0.019 and 0.122 ms, respectively.
The remaining tail lies predominantly in final assembly work; it is not hidden
signature work. Additional overlap costs some CPU and retained memory.

For all-at-once catch-up, two-worker total assembly was about 93 ms for captured
transactions and 140 ms for maximum-size transactions with overlap enabled.
Four workers yielded roughly 67 and 85 ms. Overlap itself provides little
opportunity when all data is immediately available, and some conditions showed
additional overhead. These results do not justify an automatic mode-switching
policy; the default prioritizes tip behavior.

## Pool batching: worker and target trade-off

Decoding is outside these measurements. CPU time sums work across cores, so it
must not be interpreted as single-thread wall time.

| Transactions | Workers / target | Catch-up wall time | CPU-ms/block |
|---|---:|---:|---:|
| Captured 228 B | 2 / 4 | 155.92 ms | 317.8 |
| Captured 228 B | **2 / 8** | **77.79 ms** | **158.7** |
| Captured 228 B | 4 / 8 | 39.48 ms | 158.5 |
| Generated 1,232 B | 2 / 4 | 165.23 ms | 342.6 |
| Generated 1,232 B | **2 / 8** | **86.11 ms** | **180.0** |
| Generated 1,232 B | 4 / 8 | 56.22 ms | 223.0 |

At the synthetic tip, the two-worker/eight-target pool's residual p95 was about
1.34 ms for captured transactions and 0.64 ms for maximum-size transactions.
These smaller values exclude the final assembly measured above. Scheduling lag
was around a millisecond, so small differences between worker counts are noisy.
Sparse four-transaction components dispatched immediately at width four even
with target eight. Setting target four globally roughly doubled signature CPU
work in the captured workload without a meaningful tip-latency advantage.

## Effect on transaction execution

The real `BenchmarkLoadAndExecuteTransferResultMode/lean` system-transfer probe
ran before and during signature load on the same eight physical cores. Each
condition has three shuffled paired samples. The two workloads use **separate
processes**, so this measures hardware/OS contention and excludes shared Go
scheduler/heap interactions, dependency planning, commit, and full replay.

| Signature policy | Catch-up execution slowdown | Tip execution slowdown |
|---|---:|---:|
| 2 workers / target 4 | +1.9% | +7.0% |
| **2 workers / target 8** | **+21.8%** | **+1.7%** |
| 4 workers / target 4 | +1.2% | +21.4% |
| **4 workers / target 8** | **+21.5%** | **−0.9%** |

Values are medians of paired slowdown ratios, not ratios of independently
aggregated times. Background load was variable: tip/target-eight paired ranges
were −5.1% to +3.4% for two workers and −10.1% to +17.9% for four. The small
negative median is noise, not evidence that signature work accelerates
execution. **This test does not show a consistent execution penalty from four
rather than two workers at target eight.** It does show that sustained catch-up
signature work can contend with execution at either worker count.

Two workers are the conservative default because their tip latency is already
similar and their peak verifier concurrency is lower. Four workers are useful
when catch-up latency matters; the data do not establish that they are unsafe
or universally worse. A live same-process replay comparison would be needed to
measure scheduler, heap, planner and commit interactions. The running validator
was not switched for these tests.

## Validation

Native unit tests passed for Turbine, signature verification, transaction
verification, block metadata, statsd, and node configuration. Native repeated
race checks passed for the new streaming, cache, transaction-pool and receiver
paths. Local full Turbine race tests, selected package tests and vet passed.

Regressions cover early verification before the last shred, gap/duplicate
handling, cross-FEC component boundaries, reset generations, bounded admission,
byte-budget/oversized fallback, shutdown ownership, canceled-completion retry,
exact-byte cache binding, retained invalid signatures, discarded UpdateParent
prefixes, and padded Agave decoding parity. Assembly benchmarks additionally
assert exactly one signature verification per retained transaction and matching
block identity/transaction coverage. No internal native fallback occurred.

The local replay suite passed on rerun. Its first run hit an existing
`promotion_test.go` assertion requiring a tiny lookup duration to be positive;
the isolated test passed twenty repetitions and the full suite then passed.
No production replay or timing assertion was changed to address that flake.

See the raw logs and JSON in this directory, the source benchmark comments, and
[BENCHMARKS.md](BENCHMARKS.md) for pool and execution-probe reproduction details.

To reproduce the maximum-size assembly comparison on an AVX-512 IFMA machine:

```sh
go test -c ./pkg/turbine -o /tmp/turbine-streaming.test
GOMAXPROCS=8 MITHRIL_SIGVERIFY_FLOW_BACKEND=r51 \
MITHRIL_SIGVERIFY_FLOW_COUNT=33760 \
taskset --cpu-list 0-7 /tmp/turbine-streaming.test \
  -test.run='^$' -test.benchtime=3x -test.count=3 \
  -test.bench='^BenchmarkEntryPrefetchAssembly$/^generated_1232B$/^workers_.*$/^target_8$/.*'
```

Use the captured fixture environment and `captured_2622581` sub-benchmark instead
for the short-transaction comparison. Select the physical CPU IDs appropriate
for the machine; the CPU IDs above describe this server's topology.
