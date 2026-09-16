Omitting `tuning.use_pool` bypassed the CLI's intended pooling default. Enable the default through configuration, resolve booleans in explicit-CLI → configured-value → flag-default order, and preserve explicit `false`. Copy retained vote-deque storage so pooled invocation memory cannot overwrite state used by later transactions.

Tests cover omitted settings, TOML values, CLI overrides, heap/stack reset and retained-deque ownership. This is a small allocation/correctness change; it makes no whole-validator speedup claim.

Fresh local race tests passed for config, SBPF pooling and node startup/configuration; the retained-deque regression passed in the combined build. Combined integration validation also covers #278 and the other performance reviews. Historical native benchmarks retain their original baselines; this preparation pass ran locally, without touching the live validator.

[Historical benchmark evidence](https://github.com/Overclock-Validator/mithril/tree/752ef97369e5b1614eeff64a242dfa87df673045/docs/results) is preserved outside the proposed merge; reusable benchmarks and maintained contracts remain in source.

Based on #278 at `e1204b32`; retarget to `alpenglow-dev` after that PR merges.

[Rebased validation and exact source heads](https://github.com/Overclock-Validator/mithril/blob/7layer/review-integration-20260915/docs/results/review-preparation/2026-09-16/README.md).
