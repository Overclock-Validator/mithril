# Transaction-status checkpoint capture

Replay used to encode and sort the full status checkpoint while preparing a promotion. Capture now pins immutable lineage and coverage metadata; the existing promotion worker encodes the same checkpoint later. Capture occurs before pruning and publication ordering stays unchanged. Pinned snapshots retain their nodes across pruning/unwind.

The native Zen5 benchmark captured roughly30MB of status data: original202–207ms versus5.92–6.06microseconds on replay. Encoding moved to the worker and was not eliminated. Ten live captures took29–34microseconds; worker encoding179–367ms. These are historical stage measurements, not a promise of total validator speedup; see docs/results/status-cache/2026-09-14.

This PR also includes batched expiry; see transaction-status-expiry.md. The proposed transaction-status publication optimization has not been implemented or included. Fresh standalone replay race and vet checks are under docs/results/pr-split-2026-09-15/status.
