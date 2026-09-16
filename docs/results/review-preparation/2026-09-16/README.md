# Performance review validation — 2026-09-16

Base: PR #278 at `e1204b3280f67e95a5f118fdff50f33f3d49b495`. The final manifest records each published review head, the combined integration commit and local production-binary hash. This evidence lives only on the review-index branch, not in the proposed review diffs.

Validation ran locally on Apple M4 Pro / Go 1.26.4 with race detection. No source was built or deployed on the live validator during this pass. The historical Zen 5 benchmarks retain their original baselines; no new performance number is claimed for the rebase.

The affected-package standalone suites passed. The combined race run passed every selected package except blockprod initially, where an old benchmark referred to a helper removed by FEC optimization. The correction uses the public component serializer; the complete combined blockprod/scheduler rerun passed. The initial output is preserved alongside the rerun rather than represented as one uninterrupted green invocation. Final affected-package vet and the production build passed. The retained vote-deque ownership regression passed separately.

Selected combined packages: alpenglow, consensus, replay, turbine (including recovery and simulator), repair, blockstream, block, blockprod/scheduler, txverify, txstatus, sigverify, config, accounts, sbpf, costmodel, merkletree, statsd, tpu/txfixture, node, configcmd and repair-sim. This does not claim a green whole-repository or complete sealevel suite; the older base/candidate BPF-loader failures remain outside this scoped validation.

Integration required unioning independent voting/leader configuration fields in node.go, config.example.toml and pkg/config/config.go. The FEC/streaming merge preserves both authenticated-root reuse and the exported simulator verifier, and both encoder pooling and final-component boundaries. The FEC port preserves the current slot-byte reservation accounting and asynchronous worker. Legacy 1,232-byte fixtures were corrected to avoid using the newer 4,096-byte transport limit.

No raw logs, profiles, generated patches, runtime directories or session notes remain in the proposed seven diffs. The archive-link checker resolved all local documentation links and pinned historical Git objects. Historical snapshots are tagged `review-evidence-20260916-*`.
