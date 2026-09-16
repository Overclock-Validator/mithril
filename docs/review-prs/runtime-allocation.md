Omitting `tuning.use_pool` silently disabled the CLI's intended pooling default. Resolve boolean options in explicit CLI → configured value → flag default order, preserving explicit `false`. The CLI flag remains the single source of the pooling default; the config editor and dashboard do not consume this setting.

Copy retained vote-deque storage so returning and reusing pooled invocation memory cannot overwrite saved votes or the V4 vote-cache state.

Tests cover omitted settings, TOML values, CLI overrides, heap/stack isolation during nested and concurrent executions, and retained-deque ownership after scratch reuse. Targeted race tests and vet passed on `alpenglow-dev` (`33dde405`). This change makes no whole-validator performance claim.

[Historical benchmark evidence](https://github.com/Overclock-Validator/mithril/tree/752ef97369e5b1614eeff64a242dfa87df673045/docs/results) retains its original baselines and does not measure this rebase.

Targets `alpenglow-dev` directly; independent of #278. Two commits separate boolean configuration resolution from retained vote ownership.
