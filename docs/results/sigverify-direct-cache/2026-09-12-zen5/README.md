# Direct shred-to-cache comparison on Zen 5

For 33,760 maximum-size transactions arriving over 200 ms, removing redundant
component-buffer construction reduced the work remaining after the final shred
from **25.71 to 11.44 ms**. Whole measured assembly allocated **307.49 to
114.25 MB per block**, a **62.8% reduction** in allocation volume.

## Change and comparison

Baseline is `11939e2fda29c60381370242e4a26308fd09c9f9`, which already includes
the new signature batching pool and shred-arrival overlap. Candidate changes
only production `pkg/turbine/entries.go`: completion compares each original
shred data slice directly against the corresponding cached bytes instead of
concatenating the slices into another temporary component buffer first. A cache
miss allocates one buffer at the known size, avoiding repeated append growth.

Cache reuse still requires matching generation/range and every byte, including
padding. Preparation must finish before its data are read. Changed bytes or
lengths trigger fresh decoding and signature verification. Marker handling,
discarded UpdateParent prefixes, transaction ownership, and final block identity
checks retain their existing behavior. Worker counts, batch target, and arrival
scheduling are unchanged.

This comparison isolates this optimization on our streaming branch. It is not
a comparison against `alpenglow-dev`, an older Narya revision, or overlap off.
The original streaming report's approximately 29 ms maximum-size tail was from
an earlier run; the fresh baseline here is 25.71 ms. Use these paired runs to
evaluate this change.

## Method

- AMD Ryzen 7 9700X, eight physical cores / sixteen hardware threads; Go 1.26.4,
  native Narya `r51` at `c265ee9667131a556cd332c252b79de0860c7640`.
- `GOMAXPROCS=8`, affinity CPUs 0–7 (distinct physical cores), nice 10. Existing
  Mithril and Lightbringer continued running. These are shared-host measurements;
  the live validator was not replaced or restarted.
- Two signature workers, target eight, two preparation workers. No batching timer.
- Six baseline/candidate pairs with order alternating between pairs; five timed
  blocks per condition in each process, for 30 timed blocks per variant/condition.
  Tables show medians of the six reported values. The residual column is the
  median of six per-run p50s, not a pooled percentile or confidence interval.
- Both fixtures contain 33,760 distinct valid one-signature generated transactions
  at exactly 228 or 1,232 wire bytes. The earlier short-transaction report used a
  captured fixture; this A/B uses generated fixtures on both sides.
- Tip scenarios schedule complete component bursts over 200 ms; catch-up offers
  all shreds immediately. A 60 KiB transaction-byte budget defines benchmark
  components, with actual entry overhead, FEC padding, and Merkle shreds. This
  budget is a fixture choice, not a protocol requirement or recorded arrival trace.
- The unchanged `BenchmarkEntryPrefetchAssembly` exercises actual assembler
  ingestion, component decoding, final Merkle identity checks, transaction
  signature verification, and block publication. Fixture construction, packet
  parsing, shred authentication, transaction execution, PoH, and commit are excluded.

## Tip results with overlap enabled

All arrows are baseline → candidate. MB denotes decimal megabytes. Allocation
volume is total Go `B/op` for the measured assembly, not peak memory or retained
heap. CPU-ms sums process CPU time across cores; it is not single-thread wall time.

| Wire bytes/tx | Final-shred-to-ready | First-arrival-to-ready | CPU-ms/block | Allocated MB/block |
|---|---:|---:|---:|---:|
| 228 | 5.70 → **2.29 ms** | 207.24 → **203.11 ms** | 184.95 → 176.10 | 68.21 → **33.09** |
| 1,232 | 25.71 → **11.44 ms** | 228.25 → **212.79 ms** | 273.20 → 236.80 | 307.49 → **114.25** |

The maximum-size residual fell **55.5% (2.25× faster)**; first-arrival-to-ready
fell 6.8% because the same approximately 201 ms arrival period remains. Each of
the six maximum-size pairs improved residual latency, with reductions of
49.7–60.5%. The median CPU reduction was 13.3%, but paired CPU changes ranged
from a 5.1% increase to a 16.4% decrease on the shared host.

Median shred collection remained about 201 ms in both maximum-size variants.
The final signature join remained about 0.11 ms; this optimization removes
assembly copies and allocation work, not cryptographic work. The remaining
11.44 ms includes final assembly and identity checks and has not been separately
profiled in this candidate. These results are not a whole-validator replay speedup.

## Catch-up and cache-miss checks

The miss path also improves because it can allocate an exactly sized buffer.
Overlap off exercises full decoding/verification without a prefetch cache.
With all shreds available immediately, overlap on has limited time to prepare
transactions before completion, so it also exercises mostly missed cache work.

| Arrival | Wire bytes/tx | Overlap | First-arrival-to-ready | Allocated MB/block |
|---|---:|---|---:|---:|
| Catch-up | 228 | Off | 86.28 → 84.63 ms | 59.26 → 32.25 |
| Catch-up | 228 | On | 87.25 → 82.78 ms | 61.99 → 34.79 |
| Catch-up | 1,232 | Off | 122.59 → 109.94 ms | 260.02 → 111.95 |
| Catch-up | 1,232 | On | 118.47 → 108.86 ms | 265.47 → 114.43 |
| Tip, 200 ms | 228 | Off | 286.10 → 284.51 ms | 59.26 → 32.25 |
| Tip, 200 ms | 1,232 | Off | 319.12 → 306.41 ms | 260.10 → 112.08 |

No tested condition regressed at the median. Small short-transaction differences
should be treated cautiously given background load. No worker-policy or execution
contention experiment was added for this change.

## Validation and artifacts

- Local full Turbine race suite passed; Turbine, replay, and node package tests
  and Turbine vet passed.
- Native full Turbine tests and three repetitions of targeted decoder/prefetch
  race tests passed.
- Five-second local fuzz run completed 132,207 executions without a failure,
  checking equivalence to bytewise comparison of concatenated data across varied
  fragmentation, changed bytes, and different lengths.
- New regressions reject reuse of a successful signature verdict after signature
  bytes change, and reject cached buffers of changed length. Existing padding,
  cancellation, UpdateParent, and captured Agave decoding tests also passed.
- Every measured assembly asserts complete transaction coverage, exactly one
  signature verification per retained transaction, expected block identity and
  footer metadata, and absence of internal native-verifier fallback.

`sample-*.txt` contains the native raw measurements with trailing whitespace
trimmed. `summary.json` contains all
parsed metrics, medians, and paired ratios; regenerate it with `python3
summarize.py`. `method.json` records the baseline commit and candidate source
hashes, and `binaries.json` records both native binary hashes. The runner and
`commands.jsonl` record exact native commands; runner paths describe the isolated
benchmark directory on this host. Native validation logs are included alongside
the samples.

To reproduce each variant on an AVX-512 IFMA host after checking out its source:

```sh
go test -c -o /tmp/turbine-direct-cache.test ./pkg/turbine
GOMAXPROCS=8 MITHRIL_SIGVERIFY_FLOW_BACKEND=r51 \
MITHRIL_SIGVERIFY_FLOW_COUNT=33760 \
taskset --cpu-list 0-7 /tmp/turbine-direct-cache.test \
  -test.run='^$' -test.benchtime=5x -test.count=1 \
  -test.bench='^BenchmarkEntryPrefetchAssembly$/^generated_.*$/^workers_2$/^target_8$/.*'
```

Choose distinct physical core IDs for the target host, and alternate the two
variants six times rather than running all baseline samples first.
