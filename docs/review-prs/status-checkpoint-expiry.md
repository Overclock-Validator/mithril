Transaction-status publication and checkpoint preparation can stall replay before voting. Prepare immutable status deltas during execution, pre-size maps, reuse ancestor validation only for the same cache and exact identities, and capture checkpoint lineage for encoding by the existing promotion worker. Memoize immutable node encoding and expire blockhash groups in batches. Reject incomplete fold batches before copying account-write lists.

Commit still checks block binding, coverage and parent lineage under the cache lock; offset revalidation also rebuilds when a previously nonzero-offset group disappears, matching fresh preparation; every exit joins preparation. Small banks and single-thread configurations remain inline. Checkpoint format, capture-before-prune ordering, 300-root retention, pinned views and durable-root ordering are unchanged. Completed rewards bookkeeping retires only after the exact inactive distribution is durably rooted; uncertain state retains checkpoint fallback.

| Historical Zen 5 component | Baseline → result |
|---|---|
| Capture approximately 30 MB status checkpoint on replay | **202–207 ms → 5.92–6.06 µs**; encoding moves to worker |
| Encode moving 300-root window, 5,000 keys/root, 128 new roots | **194.52 → 85.02 ms** |
| Remaining commit for 33,760 prepared identities | **4.6–5.2 → 1.6–2.9 ms** |

Capture uses the original synchronous algorithm; encoding uses the uncached encoder; commit uses the original functions at `33dde405` and excludes prior preparation. These are different stage experiments, not additive gains or a new-base benchmark. Encoding retains about one additional encoded window (~30 MB in this fixture); cold windows still sort fully. Preparation can compete with execution.

The later 64-partition status-map experiment is excluded: it increased preparation work without establishing an overall p99 gain. Its source and measurements remain in the evidence snapshot.

[Design, method and limitations](https://github.com/Overclock-Validator/mithril/blob/939aa261933133a47b45f5867f7f2eabadc69e3c/docs/status-checkpoint-capture.md).
[Design, method and limitations](https://github.com/Overclock-Validator/mithril/blob/939aa261933133a47b45f5867f7f2eabadc69e3c/docs/transaction-status-publication.md).
Fresh local replay, accounts, block and transaction-status race tests and vet passed on both the direct-development branch and combined testing branch. Added 200 seeded randomized cold/warm cached-versus-uncached encoding comparisons and a disappeared-group regression against inline publication. The frozen benchmark helper explicitly documents that its omitted validation-version updates are unsuitable for production. Historical native benchmarks retain their original baselines; this preparation pass ran locally, without touching the live validator.

[Historical benchmark evidence](https://github.com/Overclock-Validator/mithril/tree/a511ad3b0bc77cf8b5ae4ee16359ac6b453fc7bf/docs/results) is preserved outside the proposed merge; reusable benchmarks and maintained contracts remain in source.

Targets `alpenglow-dev` directly at `33dde405`; it does not require #278. The combined testing branch retains #278.

[Rebased validation and exact source heads](https://github.com/Overclock-Validator/mithril/blob/7layer/review-integration-20260915/docs/results/review-preparation/2026-09-16/README.md).
