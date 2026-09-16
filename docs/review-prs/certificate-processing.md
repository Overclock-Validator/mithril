Incoming BLS verification held the certificate-pool mutex while doing expensive crypto, delaying arrivals, snapshots and pruning. Release the state lock during verification, recheck slot/validator bindings before installing results, and publish verified batches promptly. Keep in-flight votes counted against admission limits and preserve equivocation, malformed-share and publication-barrier checks.

Reuse verified signature points and use bounded MultiExp for batches of at least 16 votes when GOMAXPROCS > 1. G1/G2 run sequentially with one arithmetic task; small batches and single-thread runtimes retain scalar verification. Full-field random coefficients and the one-batch verification gate are unchanged. The observer also revisits only certificates still awaiting replay.

Lock ownership is explicit: `verifyAndFoldTallyWithLockReleased` returns locked; `finishSlotAndUnlock` returns unlocked.

| Historical Zen 5 component | Baseline → result |
|---|---|
| Full 64-vote fold | **14.22–14.28 → 6.80–7.13 ms** |
| Snapshot p95 during concurrent 64-vote processing | **8.57–8.72 → 0.008–0.038 ms** |

The fold baseline is the preceding scalar implementation (`395e4566`); Snapshot compares point reuse alone with verification outside the lock. These are separate experiments, not full-PR comparisons against #278. A contention experiment included slower execution samples; no isolated FAST or whole-validator gain is established.

[Design, method and limitations](https://github.com/Overclock-Validator/mithril/blob/659290426735e9bd78f9073fdef24435a709f781/docs/certpool-offlock.md).
Fresh local Alpenglow race tests passed, including concurrency, stale binding, invalid signatures and publication barriers. Combined integration validation also covers #278 and the other performance reviews. Historical native benchmarks retain their original baselines; this preparation pass ran locally, without touching the live validator.

[Historical benchmark evidence](https://github.com/Overclock-Validator/mithril/tree/72514abc5a2a98d2a2823fe92f0bfbbeedbcc9bc/docs/results) is preserved outside the proposed merge; reusable benchmarks and maintained contracts remain in source.

Based on #278 at `e1204b32`; retarget to `alpenglow-dev` after that PR merges.

[Rebased validation and exact source heads](https://github.com/Overclock-Validator/mithril/blob/7layer/review-integration-20260915/docs/results/review-preparation/2026-09-16/README.md).
