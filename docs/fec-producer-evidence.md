# FEC producer and recovery evidence

The original #259 source, standalone producer benchmarks, raw samples and
recovery derivation are preserved at
[the historical snapshot](https://github.com/Overclock-Validator/mithril/tree/a3b16ebaaf803807ad04a7975f3eccf1c15649ea)
(tag `review-evidence-20260916-fec-producer`).

The September 6 comparison used development head `7e4e8af1`, not today's #278
base. Fifty thousand 1,232-byte legacy transactions across three slots took
734.052 → 282.296 ms on one pinned Zen 5 CPU. With both versions using the same
30,816-byte batch target, the result was 734.052 → 288.378 ms. This measures
serial producer work, excluding transaction execution, admission verification,
worker queue overlap, routing and network delivery. It is not a whole-validator
speedup or a benchmark of the rebased combined Turbine review.

The rebase preserves newer slot-byte reservations and the asynchronous shred
worker. Fixtures explicitly use the legacy 1,232-byte limit rather than the
newer 4,096-byte transport maximum. Reusable benchmark code remains in source.
