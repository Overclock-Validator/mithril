# Reuse verified BLS signature points in the incoming vote pool

This records the isolated first step. It was subsequently measured on Zen 5 and included in the [off-lock verification deployment](certpool-offlock.md); the local measurements and validation history below are retained.

Incoming vote verification parsed each BLS signature, returned the original wire message, and parsed the same signature again when folding it into the tally. Failed aggregate checks recursively reparsed their subsets too. The tally also accumulated a public-key sum that was never read.

The pool now returns the verified parsed members and uses their signature points directly for tally aggregation. Failed batches subdivide parsed members, and the individual-verification fallback returns verified parsed members as well. The unused tally public-key aggregate is removed; the randomized verifier's essential weighted public-key aggregate remains.

This is the first, contained optimization from the September 14 certificate-pool investigation. The shared mutex, thresholds, publication ordering, pending limits, stake accounting, duplicate/equivocation checks, paired-vote disjointness, subgroup/infinity checks, and randomized coefficients remain in place. Moving verification outside the lock is a separate future change. Installed validator sets already cache parsed public keys.

## Benchmark

Local Apple M4 Pro, darwin/arm64, Go 1.26.4. `BenchmarkCertPoolFoldVerifiedBatch` exercises pending-map setup, verification, tally aggregation and accounting for valid same-payload batches, with installed public-key caches. Signing and fixture preparation are outside the timer. Each benchmark runs one goroutine with `-test.cpu=1` and `-test.benchtime=500ms`.

The baseline is the pre-change production source at commit `0af2e094`, with the identical benchmark added. Separate before/after test binaries were built before measurement. Four measurements per version alternate in before/after/after/before order twice; the table shows medians. Tests and compilation finished before benchmarking.

| Votes per batch | Before | After | Time reduction | Allocations before → after |
| --- | ---: | ---: | ---: | ---: |
| 1 | 0.985 ms | 0.939 ms | 4.7% | 80 → 73 |
| 8 | 3.845 ms | 3.606 ms | 6.2% | 222 → 208 |
| 32 | 12.724 ms | 11.089 ms | 12.8% | 679 → 641 |
| 64 | 25.290 ms | 21.449 ms | 15.2% | 1,263 → 1,193 |

These are local component measurements, not Zen 5 or live FAST results. No validator restart/deployment, load-policy change, or live profiling was performed for this implementation. Large-batch time improves by several milliseconds locally, but this does not establish the reduction in lock waits or end-to-end voting latency.

Raw samples, binary hashes and the runner are preserved in the workspace under `mithril-run-20260911/non-replay-latency-20260914/point-reuse/` and `benchmark-points.py` in its parent directory.

## Validation

Passed:

* All targeted `TestCertPool` and `TestIndividuallyVerifiedBatch` tests, including three runs with race detection.
* New differential coverage checks retained points/messages against individual verification for valid batches, wrong-payload signatures in both recursive halves, malformed encodings, infinity and invalid ranks. The individual-verification fallback is checked separately.
* Existing cancellation-attack, invalid-candidate poisoning, aggregate-certificate verification, pruning, equivocation and reward-publication barrier coverage.
* Full `pkg/consensus` race suite.
* `pkg/alpenglow` race suite excluding `TestVotorBroadcasterIsolatesBlockedPeer`.
* `go vet ./pkg/alpenglow ./pkg/consensus`, `go build ./cmd/mithril`, and `git diff --check`.

The unfiltered Alpenglow race suite hit an intermittent timeout in `TestVotorBroadcasterIsolatesBlockedPeer/reconnect`, at `peer_sender_test.go:209` waiting for the stalled connection to close. The same timeout reproduced with the unchanged certificate-pool source through a Go overlay. No transport code or timeout was changed to mask it. This pre-existing failure remains an explicit qualification limitation; the full suite is not reported as passing.
