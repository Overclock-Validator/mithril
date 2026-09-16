Incoming BLS verification held the certificate-pool mutex while doing expensive crypto, delaying arrivals, snapshots and pruning. Release the state lock during verification, recheck slot/validator bindings before installing results, and publish verified batches promptly. Keep in-flight votes counted against admission limits and preserve equivocation, malformed-share and publication-barrier checks.

Reuse verified signature points and use bounded MultiExp for batches of at least 16 votes when GOMAXPROCS > 1. G1/G2 run sequentially with one arithmetic task; small batches and single-thread runtimes retain scalar verification. Full-field random coefficients and the one-batch verification gate are unchanged. The observer also revisits only certificates still awaiting replay.

Lock ownership is explicit: `verifyAndFoldTallyWithLockReleased` returns locked; `finishSlotAndUnlock` returns unlocked. Transitive folding helpers document temporary unlocks and possible slot retirement.

Queued reward-footer flushes take precedence over new owners and arrivals for the same slot. Existing owners finish first; a waiter count preserves priority across overlapping flushes and is released if pruning replaces the slot. Publication barriers remain unchanged. Compute each incoming signature's dedupe hash once, reuse it across admission retries, and retain the existing keys through verification for removal.

| Historical Zen 5 component | Baseline → result |
|---|---|
| Full 64-vote fold | **14.22–14.28 → 6.80–7.13 ms** |
| Snapshot p95 during concurrent 64-vote processing | **8.57–8.72 → 0.008–0.038 ms** |

The fold baseline is the preceding scalar implementation (`395e4566`); Snapshot compares point reuse alone with verification outside the lock. These are separate experiments, not full-PR comparisons against #278. A contention experiment included slower execution samples; no isolated FAST or whole-validator gain is established.

[Design, method and limitations](https://github.com/Overclock-Validator/mithril/blob/3f074edd24d4ffc94c4a70e2d41d997aaf9ece61/docs/certpool-offlock.md).
Fresh local Alpenglow and consensus race tests and vet passed on the standalone and combined branches with GOMAXPROCS=2. Reward-flush priority passed 30 repeated race runs; single-thread flush and aggregation coverage passed three runs. Coverage includes overlapping flushes, pruning, stale bindings, invalid signatures and publication barriers. Combined integration validation also covers #278 and the other performance reviews. Historical native benchmarks retain their original baselines; this preparation pass ran locally, without touching the live validator.

[Historical benchmark evidence](https://github.com/Overclock-Validator/mithril/tree/72514abc5a2a98d2a2823fe92f0bfbbeedbcc9bc/docs/results) is preserved outside the proposed merge; reusable benchmarks and maintained contracts remain in source.

Targets `alpenglow-dev` directly at `33dde405`; independent of #278. The combined testing branch retains #278 and includes these follow-ups. The scheduling and hash changes have no new live performance claim.

[Rebased validation and exact source heads](https://github.com/Overclock-Validator/mithril/blob/7layer/review-integration-20260915/docs/results/review-preparation/2026-09-16/README.md).
