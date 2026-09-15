# Prepare transaction message identities during shred arrival

The live Zen 5 admission trace found ~12.45 ms of message serialization,
hashing and identity-cache preparation after assembly on large blocks, followed
by ~1.66 ms of same-block duplicate-plan work. Most blocks reached this stage
while replay was already waiting.

Turbine now asks signature-verification workers to retain a message identity
derived from the exact canonical bytes they already serialize for verification.
The existing two workers process available groups without waiting for more
transactions. TPU callers of the ordinary verifier do not compute these extra
identities.

Each opaque result binds a successful signature verdict to the transaction
pointer, message version and recent blockhash. Completion joins requests and
imports only identities covering the final ordered transaction slice. Canceled
requests are reverified; discarded UpdateParent prefixes are excluded. Results
are caller-owned and cannot refer to reusable verifier scratch. The block cache
owns its imported storage and remains nonserialized. The existing requirement
that signed message contents remain immutable still applies; arbitrary in-place
message edits require invalidation, as before.

Same-block duplicate rejection and mutable ancestor/status checks remain in
place. This change does not cache the duplicate-check plan or alter epoch
processing, vote persistence, worker counts, scheduling deadlines, or packing.

## Zen 5 comparison

Baseline: the currently deployed shred-retention source, including its existing
FEC and peer-isolation integrations. Candidate: that same source plus streaming
identities. Both binaries use the identical new benchmark harness.

`BenchmarkEntryMessageIdentityArrival` feeds real generated data shreds through
the assembler, entry prefetch and signature-verification pipeline, then includes
the admission-time identity lookup. Each block has 33,760 single-signature
transactions, either 228 or 1,232 bytes on the wire. The tip model schedules
component arrivals across 200 ms; catchup offers every shred immediately.
Fixture construction, packet parsing and shred-signature authentication are
outside the timer. There is no network loss, transaction execution, PoH/reward
processing or final whole-block duplicate map in this benchmark.

AMD Ryzen 7 9700X, Narya `r51` AVX-512 backend, two signature workers, target eight
signature lanes, GOMAXPROCS=16. Five iterations per scenario per run, two runs
per variant in baseline/candidate/candidate/baseline order. Values below are the
median of the two run medians, not a percentile computed over all ten samples.
The live validator and load services continued running, so host contention can
affect the results.

| Arrival model | Transaction size | Last shred → identities available, baseline | Candidate |
| --- | ---: | ---: | ---: |
| Over 200 ms | 228 bytes | 13.62 ms | 2.66 ms |
| Over 200 ms | 1,232 bytes | 73.72 ms | 7.81 ms |
| Catchup | 228 bytes | 92.90 ms | 88.40 ms |
| Catchup | 1,232 bytes | 154.75 ms | 118.20 ms |

The table includes completion work; it does not merely move that work out of
the admission timer. Admission's identity lookup alone falls from ~12.1 to
0.24 ms for 228-byte tip transactions and ~66.9 to 0.26 ms for maximum-size tip
transactions. The block-wide duplicate check remains additional work.

CPU time per tip block was 189.75→197.20 ms for the small fixture (~4% higher)
and 316.40→304.80 ms for the maximum-size fixture (~4% lower). The intended gain
is less work after arrival, not a blanket claim of lower CPU. Maximum-size
fixtures allocate roughly 37 MB fewer bytes per block by avoiding a second
message serialization; retaining opaque identities also has a memory cost.

The initial runs without an explicit backend used the library's unconfigured
default and are excluded from these deployment-relevant results.

## Validation and current limits

Local affected-package tests, race tests for txverify/block/turbine/replay, Go
vet, and a full validator build passed. Tests cover legacy/v0/v1 canonical
identities, failed signatures, scratch reuse, pointer/order/blockhash mismatch,
storage ownership, JSON round-trips, early preparation and mixed canceled-batch
fallback. Existing turbine tests cover invalid and discarded prefixes and
completion cancellation.

The final revision passed native Zen 5 race tests covering txstatus, txverify,
block, turbine, replay, consensus and node startup, plus vet and a full build.
It was deployed at 21:25:24 UTC on September 14 with clean reserved-history
shutdown and all prior live integrations preserved. Continuous leader load
resumed at 21:26:04 UTC after three advancing healthy vote checks. Live FAST
comparison excludes the first two minutes after load resumes; benchmark gains
alone do not establish a FAST improvement.

The first completed live comparison covers 1,336 post-warmup FAST proofs over
7.3 minutes. Large-block median full-assembly-to-admission fell from 18.15 to
7.04 ms (admission alone 14.25→1.82 ms). FAST inclusion changed from 98.08% to
98.35% overall and 88.42% to 90.50% on large blocks. These small score changes
are observational; the baseline and candidate windows span an epoch change.
Several remaining misses coincide with unusually slow replay during local
load sends, while others have late signature completions or shred collection.
No claim of eliminating FAST misses or proving a score improvement is made.


## Recovery from inconsistent prefetch metadata

Missing/oversized retained ranges, partial identities and identity-binding
mismatches now log a warning and fall back to full signature verification of the
final block. Bounds are checked before slicing the final transaction array. Old
readers are joined first, including on cancellation. Valid final transactions
can therefore recover from an optimization bookkeeping fault; a successful
cached verdict for different bytes cannot authorize the final transaction.
Normal cached signature failures remain errors, as do failed re-verification,
cancellation and a closed verifier. No signatures or duplicate checks are skipped.

Regression tests cover valid and invalid final blocks for each metadata fault,
cache identities matching the final transaction order, canceled-reader ownership
and verifier failure. The ordinary path retains exact-range/byte checks and
verified identity reuse. Full re-verification is exceptional and costs additional
work; this change is a correctness/availability fix, not a throughput claim.

The `[sigverify]` starter configuration and its test moved here from the voting
branch, because this branch reads those keys and supports the legacy backend key.
The unrelated explicit `tuning.use_pool` template setting is omitted: enabling
pooling by default and fixing retained vote ownership belong to the runtime
branch. In the combined build, its configuration defaults still enable pooling.
The default remains two Turbine verification workers (bounded by GOMAXPROCS).
Many-core catch-up tuning remains a separate measurement question.
