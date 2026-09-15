The run command resolves pooling through Viper, so its CLI default alone did not enable the intended pooling behavior when TOML omitted the option. Set the configuration default and verify reuse isolation. Also copy retained vote-deque state into owned storage so a later transaction cannot overwrite it through reused invocation memory.

This extracts the small runtime/default changes and the vote-deque ownership correction from #279. It includes configuration override tests, VM pool reset/isolation tests and `TestProcessNewVoteStateOwnsRetainedDeque`.

Config and SBF race suites, the exact vote-deque race regression and vet passed. Fresh logs: `docs/results/pr-split-2026-09-15/runtime`. An initial name filter selected no ownership tests; the separate ownership-test log records the corrected actual run. Full sealevel-suite success is not claimed: #279 records unrelated BPF-loader failures reproduced on unchanged alpenglow-dev. No end-to-end performance claim is attached to this split.

Split from #279; base development commit: `33dde4050d9250557583395810799aaac2f54017`. Historical native measurements retain their original tested source; these reorganized heads have fresh local validation.
