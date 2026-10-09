# Vote Delivery Persistence: benchmark evidence

The maintained subsystem documentation and reusable Go benchmarks describe the
implementation and reproduction method. Historical raw results and session
notes are retained at [the tested source snapshot](https://github.com/Overclock-Validator/mithril/tree/54b233ff0e27fb929644f7d53bf4a699cb590cd8)
(tag `review-evidence-20260916-vote-delivery-persistence`). They are omitted from this proposed merge.

[Historical result files](https://github.com/Overclock-Validator/mithril/tree/54b233ff0e27fb929644f7d53bf4a699cb590cd8/docs/results)

Measurements retain their original baselines. Rebasing onto PR #278 does not
turn an intermediate-version benchmark into a comparison with the new base.
Component timings and short live observations do not establish sustained FAST
inclusion gains. The final review description records validation of the rebased
source separately from historical benchmark results.
