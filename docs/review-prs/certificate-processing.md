Incoming BLS verification and aggregation held the certificate-pool mutex, blocking unrelated arrivals, snapshots and pruning. Keep expensive verification behind a separate bounded gate while releasing the pool-state lock; publish verified batches promptly and reject stale results after pruning or validator-binding changes.

Reuse verified signature points, retain randomized verification and invalid-batch fallback, cache observer statistics until certificate/replay inputs change, and publish trusted replay progress without waiting on cryptography. Admission limits, pending accounting, duplicate/equivocation checks, reward-flush barriers and publication ordering remain enforced.

### Evidence

On Zen5, the concurrent 64-vote diagnostic reduced Snapshot p95 from8.57–8.72ms with point reuse alone to0.008–0.038ms with off-lock verification. This measures reader latency, not vote throughput. Point reuse separately reduced the full32-vote fold from8.460 to7.580ms. Live initial-lock waits also improved, but different windows do not establish a corresponding FAST gain. Methods and limitations are in `docs/certpool-offlock.md`; raw comparisons are in `docs/results/certificate-processing/2026-09-14`.

Fresh standalone Alpenglow race tests and vet passed. Tests cover concurrent admission, in-flight accounting, changed bindings, pruning/eviction, invalid signatures, capacity wakeups and reward publication. Fresh logs: `docs/results/pr-split-2026-09-15/certificate`.

Voting transport and persistence are a separate dependent PR. This replaces the certificate/observer portion of #279 and includes the subsequent measured improvements.

Split from #279; base development commit: `33dde4050d9250557583395810799aaac2f54017`. Historical native measurements retain their original tested source; these reorganized heads have fresh local validation.

### Bounded multi-scalar aggregation

Use sequential G1/G2 MultiExp with one arithmetic task for batches of at least 16 votes when GOMAXPROCS exceeds one. Keep independent, nonzero full-field random coefficients and the existing single-batch verification gate. Small batches and single-thread runtimes retain scalar verification; the latter avoids the scheduling regression found during native contention testing.

On Zen 5 with GOMAXPROCS=2, full 64-vote folds improved from **14.22–14.28 ms to 6.80–7.13 ms**. The scalar baseline is the preceding certificate split head (`395e4566`), not alpenglow-dev. A controlled 64-vote/200 ms workload also reduced certificate time, but native execution samples were noisy, including a slower candidate sample; no absence of interference or live FAST gain is claimed. Details: `docs/certpool-offlock.md`.

Final native combined race tests, one-/two-thread adversarial verification tests, vet and build passed. The voting branch is updated to this base with its own scoped diff unchanged. This change has not been deployed; raw run artifacts remain outside the source tree.
