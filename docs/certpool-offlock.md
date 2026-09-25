# Incoming vote verification outside the pool lock

The incoming vote pool previously held one mutex while checking BLS batches and folding signatures into aggregates. A 45-second trace on the live Zen 5 validator observed a 9.49 ms p95 acquisition wait and a 57.68 ms maximum across 34,580 normally completed AddVote calls. This blocked other incoming votes and readers; durable pruning could also wait for this lock while holding the consensus output mutex.

The change retains one expensive BLS batch at a time using a separate verification mutex, while releasing the pool-state mutex around cryptography. One caller owns processing for each slot. Ordinary arrivals may join that slot's pending maps while verification runs, and the owner revisits state after those arrivals. Admission that needs authentication to resolve quota pressure or competing signatures waits instead of weakening bounds or first-packet-poisoning checks.

Candidates remain pending until verification finishes, so in-flight votes count against admission limits and exact duplicates consume no additional space. Only selected candidates are removed on completion. Pruning and eviction may remove a slot during verification; pointer identity checks prevent stale work from resurrecting it or releasing accounting twice. If the epoch lookup, installed validator-set identity or shred version changes, the old slot state is retired instead of mixing bindings. Normal engine validator sets are immutable within an epoch.

Each completed verified batch publishes promptly, outside both mutexes. New arrivals do not defer an authenticated quorum until the slot stops receiving votes. Reward-footer flushing waits for the active owner, drains relevant pending votes, and honors the publication barrier. Verified stake reads preserve freshness by waiting for that slot's owner. Snapshot counters and durable pruning do not wait for cryptography.

Point reuse is included: aggregation uses already-verified signature points, failed batches subdivide parsed members, and the unused tally public-key aggregate is removed. Randomized coefficients, signature checks, stake thresholds, equivocation budgets and paired-vote disjointness remain.

## Lock ownership

`verifyAndFoldTallyWithLockReleased` requires the pool lock on entry and returns
with it held, including early exits. It releases the lock during crypto and
revalidates slot and validator bindings after reacquiring it. The caller owns
that slot's processing marker. `finishSlotAndUnlock` consumes lock ownership:
it releases the lock, emits certificates and returns unlocked. Callers must not
pair it with a deferred unlock.

## Bounded multi-scalar aggregation

With more than one Go execution thread, batches of at least 16 parsed votes
use gnark `MultiExp` for each weighted
public-key/signature sum. G1 and G2 run sequentially with `NbTasks: 1`; the
existing pool verification mutex still admits one expensive batch at a time.
Smaller batches keep the scalar loop because bucket setup costs more than it
saves, particularly during failed-batch subdivision and two-candidate checks.
Single-thread configurations also retain the scalar path: native contention
tests found that MultiExp task handoffs could increase certificate wall time
there, despite reducing arithmetic.

Each member still receives a fresh, independent, nonzero coefficient sampled
from the full scalar field. Its public key and signature use the same
coefficient. Entropy/aggregation errors retain the individual-verification
fallback. Parsing, subgroup/infinity checks, failed-batch subdivision,
publication and stake accounting are unchanged.

### Native staging validation

AMD Ryzen 7 9700X, Go 1.26.4, GOMAXPROCS=2, Nice 15, two-core CPU quota, with
the normal validator workload still running. The baseline restores the scalar
implementation from the preceding certificate split head (`395e4566`) with
identical fixtures. Baseline/candidate/candidate/baseline runs were followed by
a final check after adding the single-thread safeguard. Full-fold ranges:

| Votes in fold | Scalar baseline | Final implementation |
| --- | ---: | ---: |
| 32 | 7.385–7.386 ms | 4.238–4.344 ms |
| 64 | 14.217–14.280 ms | 6.800–7.135 ms |

A controlled scheduling experiment runs 64 valid votes every 200 ms alongside
4,096 repeated transfer executions. With two Go execution threads, the final
alternating native comparison reduced certificate processing from
16.25–16.39 ms to 8.43–10.22 ms. Execution results were noisy: 13.80–14.38 ms
baseline versus 13.84–17.56 ms candidate. The earlier native comparison and the
local comparison showed no execution regression, but the slower final sample
is retained; the shared host does not establish absence of interference.
This workload excludes account commits, network delivery and vote persistence.

The initial prototype's single-thread certificate latency could worsen while
execution improved slightly. The final implementation therefore keeps the
original scalar arithmetic whenever GOMAXPROCS is one. No worker-count or
verification-admission changes accompany this optimization.

## Reproduce and validate

Run `go test -race ./pkg/alpenglow` for concurrency, pending-budget, stale-binding,
invalid-share, equivocation and publication-barrier coverage.
Run `go test ./pkg/alpenglow -run '^$' -bench '^BenchmarkCertPool(WeightedPairing|FoldVerifiedBatch)$' -benchmem -benchtime=500ms -count=3` with the same fixture on each revision.

The earlier concurrent 64-vote diagnostic reduced Snapshot p95 from
8.57–8.72 ms with point reuse alone to 0.008–0.038 ms with verification outside
the lock. This measures reader latency, not vote throughput. The diagnostic's
ns/op includes intentional sampling pauses. Historical component baselines,
live observations and full qualifications are preserved in the
[evidence archive](certificate-processing-evidence.md).
