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

The benchmarked candidate passed native Zen 5 race tests. A subsequent explicit
per-batch length guard and two additional ownership/fallback tests passed the
final local race suite. Automatic approval review blocked uploading those final
source/test updates; final-revision native verification is still pending. No
new binary has been deployed and no live FAST improvement is claimed.
