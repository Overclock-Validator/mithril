# Status Checkpoint Expiry: benchmark evidence

The maintained subsystem documentation and reusable Go benchmarks describe the
implementation and reproduction method. Historical raw results and session
notes are retained at [the tested source snapshot](https://github.com/Overclock-Validator/mithril/blob/a511ad3b0bc77cf8b5ae4ee16359ac6b453fc7bf)
(tag `review-evidence-20260916-status-checkpoint-expiry`). They are omitted from this proposed merge.

[Historical result files](https://github.com/Overclock-Validator/mithril/tree/a511ad3b0bc77cf8b5ae4ee16359ac6b453fc7bf/docs/results)

Measurements retain their original baselines. Rebasing onto PR #278 does not
turn an intermediate-version benchmark into a comparison with the new base.
Component timings and short live observations do not establish sustained FAST
inclusion gains. The final review description records validation of the rebased
source separately from historical benchmark results.

The tagged snapshot also preserves the later 64-partition visible-status-map
experiment. That experiment is deliberately excluded from this review: it added
preparation work and did not demonstrate an overall large-block p99 benefit.
Earlier publication preparation and immutable-node encoding reuse remain.
