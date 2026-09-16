# Leader Packing: benchmark evidence

The maintained subsystem documentation and reusable Go benchmarks describe the
implementation and reproduction method. Historical raw results and session
notes are retained at [the tested source snapshot](https://github.com/Overclock-Validator/mithril/blob/06ef067798c99947e8cc527450ad28430a9a7333)
(tag `review-evidence-20260916-leader-packing`). They are omitted from this proposed merge.

[Historical result files](https://github.com/Overclock-Validator/mithril/tree/06ef067798c99947e8cc527450ad28430a9a7333/docs/results)

Measurements retain their original baselines. Rebasing onto PR #278 does not
turn an intermediate-version benchmark into a comparison with the new base.
Component timings and short live observations do not establish sustained FAST
inclusion gains. The final review description records validation of the rebased
source separately from historical benchmark results.
