# Incoming vote verification outside the pool lock

The incoming vote pool previously held one mutex while checking BLS batches and folding signatures into aggregates. A 45-second trace on the live Zen 5 validator observed a 9.49 ms p95 acquisition wait and a 57.68 ms maximum across 34,580 normally completed AddVote calls. This blocked other incoming votes and readers; durable pruning could also wait for this lock while holding the consensus output mutex.

The change retains one expensive BLS batch at a time using a separate verification mutex, while releasing the pool-state mutex around cryptography. One caller owns processing for each slot. Ordinary arrivals may join that slot's pending maps while verification runs, and the owner revisits state after those arrivals. Admission that needs authentication to resolve quota pressure or competing signatures waits instead of weakening bounds or first-packet-poisoning checks.

Candidates remain pending until verification finishes, so in-flight votes count against admission limits and exact duplicates consume no additional space. Only selected candidates are removed on completion. Pruning and eviction may remove a slot during verification; pointer identity checks prevent stale work from resurrecting it or releasing accounting twice. If the epoch lookup, installed validator-set identity or shred version changes, the old slot state is retired instead of mixing bindings. Normal engine validator sets are immutable within an epoch.

Each completed verified batch publishes promptly, outside both mutexes. New arrivals do not defer an authenticated quorum until the slot stops receiving votes. Reward-footer flushing waits for the active owner, drains relevant pending votes, and honors the publication barrier. Verified stake reads preserve freshness by waiting for that slot's owner. Snapshot counters and durable pruning do not wait for cryptography.

Point reuse is included: aggregation uses already-verified signature points, failed batches subdivide parsed members, and the unused tally public-key aggregate is removed. Randomized coefficients, signature checks, stake thresholds, equivocation budgets and paired-vote disjointness remain.

## Measurements

Native AMD Ryzen 7 9700X, Go 1.26.4, with the validator and its normal continuous load running. Diagnostic processes used Nice 15; compilation used GOMAXPROCS 2. Diagnostic intervals are excluded from live comparisons.

Point reuse alone, four alternating samples per version, median full pending-vote fold:

| Batch size | Original | Point reuse | Time reduction |
| --- | ---: | ---: | ---: |
| 1 | 0.681 ms | 0.655 ms | 3.8% |
| 8 | 2.567 ms | 2.357 ms | 8.2% |
| 32 | 8.460 ms | 7.580 ms | 10.4% |
| 64 | 16.637 ms | 15.127 ms | 9.1% |

The concurrent diagnostic uses eight producers submitting 64 votes, samples Snapshot latency, then flushes and checks that all 64 votes published exactly once. In two alternating samples per version, Snapshot p95 was 8.57–8.72 ms with point reuse alone and 0.008–0.038 ms with off-lock verification. Benchmark ns/op includes deliberately spaced sampling; it is not production throughput.

The combined build deployed September 14 at 22:46 UTC. The 45-second live trace measured 36,133 normally completed incoming calls: initial mutex wait median 1.24us, p95 3.64us, p99 9.10us and maximum 0.369ms. None waited over 1ms, versus 11,742 in the earlier baseline. Different live windows and probe overhead are limitations; this is not an equivalent end-to-end voting speedup. Verification-gate and admission-condition waits are separate from the measured initial mutex acquisition. Two durable-floor calls waited at most 1.95us.

Live voting/FAST results, exact deployment evidence and raw measurements are in the workspace at `mithril-run-20260911/certpool-offlock-20260914/RESULTS.md`. Native artifacts are under `/srv/mithril-certpool-offlock-20260914`. Four TPU workers, two shred workers, GOMAXPROCS 8, reserved vote history, previous live integrations, continuous load policy and signing/payer ledgers are preserved.

## Validation

New deterministic race tests park real work at the verification gate and cover concurrent admission, in-flight accounting, duplicate admission, capacity wakeup, pruning, eviction, changed bindings and reward-flush waiting. Existing invalid-share cancellation, malformed signature, aggregate-certificate, equivocation, fallback and publication tests remain.

Native race tests passed for Alpenglow, consensus, replay and node integration. Peer isolation tests separately passed three native runs; an intermittent timeout had reproduced on the unchanged local baseline earlier. Native vet and the application build passed. The four deployed source/test files match local hashes. Instruction probes and nearby stack/register operations were checked against old/new disassembly, normalizing linker relocations. A live sanity capture observed 133 replays and 133 notarize events with no reservation exhaustion or admission rejection.
