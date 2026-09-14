# Shred ordering, authenticated roots, and larger verification jobs

For 33,760 maximum-size transactions arriving over 200 ms, the final-shred-to-ready
tail fell from **11.24 to 6.44 ms**, a **42.7% reduction**. Catch-up assembly with
overlap enabled improved from **105.36 to 99.75 ms**. These measurements compare
against `e063695f5b22a68990007c99c7dc7b64eef5568e`, which already contains streaming
verification and direct shred-to-cache byte comparison. This is not an
`alpenglow-dev` or whole-validator replay comparison.

## Changes

- Complete contiguous slots walk shred indexes directly. The decoder accepts
  that order without sorting again; sparse or extra-tail input retains a sorted
  fallback that includes every shred.
- The receiver passes the root it authenticated into assembly. Each FEC state
  retains at most one selected source snapshot. Completion reuses that root only
  if the source, parsed root inputs, and every payload byte still match. It keeps
  the previous lowest-data-index / lowest-coding-position selection and error
  behavior. Root selection avoids sorting, and relative data index zero avoids
  a map scan. Repair, reset, and missing-hint paths retain root computation.
- Large ready signature requests dispatch four vector groups per job, normally
  32 one-signature transactions. Narya still processes eight-wide groups. With
  two workers and target eight, requests below 128 ready transactions retain one
  group per job. There is no timer or wait for more transactions. A request owns
  at most two outstanding jobs under the tested configuration. Cancellation
  joins admitted work and can stop between vector groups in a larger job.

## Method and scope

- Ryzen 7 9700X, eight physical cores / sixteen hardware threads; Go 1.26.4,
  Narya `r51` at `c265ee9667131a556cd332c252b79de0860c7640`.
- `GOMAXPROCS=8`, affinity CPUs 0–7 (distinct physical cores), nice 10. Mithril
  and Lightbringer continued running; the validator binary and PID were unchanged.
- Both generated fixtures contain 33,760 distinct valid one-signature
  transactions, exactly 228 or 1,232 wire bytes each. Complete component bursts
  span 200 ms at the synthetic tip. Catch-up offers all shreds immediately.
  Components use a 60 KiB transaction-byte budget, a benchmark choice rather
  than a protocol requirement, with real entry overhead and FEC padding.
- The final comparison has six alternating baseline/candidate pairs, each with
  five timed blocks per condition. Tables show medians of the six reported
  values. Residual columns are medians of per-run p50s, not pooled percentiles.
- The assembly benchmark now supplies already-authenticated roots as the receiver
  does. Both binaries use the same benchmark source; a test-only baseline adapter
  ignores the roots and calls the old admission method. Root snapshot creation
  is inside the timed candidate path. Authentication and fixture construction
  remain outside timing on both sides.
- Timed work includes assembly, entry decoding, Merkle/block identity checks,
  transaction signature verification, and publication. Packet parsing, shred
  authentication, network loss/recovery, disk I/O, transaction execution, PoH,
  and commit are excluded. This is not a recorded arrival trace.

## Final assembly results

Both sides below have overlap enabled and use two signature workers. CPU-ms
sums process CPU across cores, rather than single-thread wall time. MB means
decimal megabytes of total Go allocation volume (`B/op`), not peak heap.

| Wire bytes/tx | Final-shred-to-ready | First-arrival-to-ready | CPU-ms/block | Allocated MB/block |
|---|---:|---:|---:|---:|
| 228 | 2.35 → **1.40 ms** | 203.13 → **202.30 ms** | 175.25 → 170.25 | 33.09 → 33.44 |
| 1,232 | 11.24 → **6.44 ms** | 212.58 → **207.64 ms** | 235.40 → 233.55 | 114.14 → 115.56 |

Every maximum-size pair improved residual latency, by 40.8–44.3%. The whole
arrival-to-ready reduction is 2.3%, since the approximately 201 ms collection
period remains. Root snapshots add about 1.4 MB of allocation volume per large
block net of the removed ordering allocations; this change primarily reduces
the completion tail, rather than total tip CPU work.

| Catch-up wire bytes/tx | Overlap | Total assembly baseline → final |
|---|---|---:|
| 228 | Off | 83.98 → **80.58 ms** |
| 228 | On | 81.77 → **79.35 ms** |
| 1,232 | Off | 108.87 → **102.78 ms** |
| 1,232 | On | 105.36 → **99.75 ms** |

These catch-up cases supply authenticated roots, as fresh verified ingress can.
Spool hydration has no retained root hints. A separate three-pair, five-block
comparison exercised that fallback using the same final production code:

| Catch-up wire bytes/tx | Overlap | No-hint baseline → final |
|---|---|---:|
| 228 | Off | 83.71 → **82.23 ms** |
| 228 | On | 81.80 → **80.67 ms** |
| 1,232 | Off | 108.08 → **104.34 ms** |
| 1,232 | On | 105.57 → **101.32 ms** |

This models root-hint availability, not actual spool read performance.

## Choosing the dispatch size

Four rotated screening rounds compared ordering alone and nominal job sizes of
8, 32, and 64 signatures. Each condition had five timed blocks per round. These
intermediate variants predate the final direct FEC index-zero lookup; their
assembly times must not be substituted for the final comparison above.
The job-size table holds the new dispatcher code constant to isolate policy;
the final assembly comparison instead uses the prior production implementation.

| Signature-only catch-up | 8 per job | 32 per job | 64 per job |
|---|---:|---:|---:|
| 228-byte transactions, wall time | 77.53 ms | **75.04 ms** | 73.78 ms |
| 1,232-byte transactions, wall time | 86.66 ms | **83.57 ms** | 83.20 ms |
| 228-byte transactions, CPU-ms | 157.60 | **151.35** | 148.30 |
| 1,232-byte transactions, CPU-ms | 181.60 | **174.45** | 173.55 |

32-signature jobs save about 3–4% catch-up wall time in this pool test. Sparse
four-transaction arrivals still dispatch immediately; their observed p95
availability-to-verdict remained approximately 1 ms, dominated by synthetic
feed scheduling. The 64 policy is somewhat faster in catch-up, but offers little
additional benefit for maximum-size transactions and doubles the work one job
can put ahead of another request. We selected 32 as the smaller useful policy;
the measurements do not establish that 64 is harmful.

The real lean transfer load/execute probe also ran before and during signature
load on the same eight cores. It used captured block 2622581's 33,760 228-byte
transactions, two workers, and target eight:

| Nominal signatures/job | Catch-up execution slowdown | Tip execution slowdown |
|---|---:|---:|
| 8 | +6.71% | +2.28% |
| **32** | **+6.64%** | **+3.35%** |
| 64 | +5.56% | +3.96% |

These are medians of three paired slowdown ratios, not ratios of aggregate
times. For 32-signature jobs, tip pairs ranged from +2.38% to +4.40%; for eight,
from +0.81% to +4.16%. There is no clear large contention penalty from the wider
jobs in this test, but the small differences should not be overinterpreted.
The two workloads run in separate processes, excluding shared Go scheduler/heap
effects, dependency planning, commit, and full replay.

The initial contention round overlapped an artifact download/compression and
was excluded in full. An additional complete round replaced it. All original
logs are retained; the table uses rounds 2, 3, and 4 consistently for every policy.

## Remaining completion work

A separate 30-block diagnostic profile of the final maximum-size candidate
averaged 6.42 ms after full assembly:

| Completion stage | Mean ms/block |
|---|---:|
| Entry decoding and cache-byte validation | 4.30 |
| Construct ordered shred slice | 0.82 |
| Collect/check FEC roots | 0.67 |
| Construct block and footer metadata | 0.46 |

The final signature join's median was 0.106 ms. The stage values are distinct
coarse timers; bookkeeping and the join account for the remainder. The earlier
current-baseline profile measured approximately 3.1 ms in the two ordering
steps and 3.5 ms in root collection. Exact byte validation now dominates the
remaining work.

## Validation and reproduction

Native Turbine, replay, and node tests, the full native Turbine race suite, and
native vet passed. The full local Turbine race suite and vet passed on the final
source. Local txverify/sigverify/node tests passed. The local replay suite hit
its existing `promotion_test.go:349` zero-duration assertion, also reproduced
in isolated repetition, then passed on full rerun. That test and production
replay code are unchanged from the baseline; no timing assertion was relaxed.

New regressions cover sparse/extra-tail ordering, public unsorted decode parity,
every root-snapshot payload byte and parsed input, replacement/reset/duplicate
admission, recovered-data/coding precedence, multisignature groups, failure
index attribution, bounded yielding, and cancellation ownership. The final
root-selector fuzz run passed 64,721 executions against the original sorted
algorithm, including unsupported types; an earlier screening run passed 128,732.
Every timed assembly asserts transaction coverage, exactly one verification per
retained signature, expected block identity/footer metadata, and no internal
native-verifier fallback.

`raw-results.tar.gz` contains the raw screening, final, fallback, contention,
profile, and validation logs. `summary.json` is regenerated from it by
`python3 summarize.py`. Source/binary hashes and exact commands are included.
The three screening patches in the archive's `sources/` directory recreate
their intermediate source from `e063695f`;
for the 32/64 screening variants, change `defaultTransactionJobGroups` from one
to four/eight in the roots patch result.

For the final candidate, build this checkout. For the baseline, check out
`e063695f`, copy the candidate's `entry_prefetch_benchmark_test.go` into it, and
copy `baseline-root-adapter.go.txt` to
`pkg/turbine/benchmark_root_adapter_test.go`. Then build and alternate binaries:

```sh
go test -c -o /tmp/turbine-completion.test ./pkg/turbine
GOMAXPROCS=8 MITHRIL_SIGVERIFY_FLOW_BACKEND=r51 \
MITHRIL_SIGVERIFY_FLOW_COUNT=33760 \
taskset --cpu-list 0-7 /tmp/turbine-completion.test \
  -test.run='^$' -test.benchtime=5x -test.count=1 \
  -test.bench='^BenchmarkEntryPrefetchAssembly$/^generated_.*$/^workers_2$/^target_8$/.*'
```

Select the target machine's distinct physical core IDs. The archived runners
record this server's paths and settings. The live validator was not deployed
or restarted by these experiments.
