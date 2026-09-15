Preparing a transaction-status checkpoint on replay could stall voting for hundreds of milliseconds. Capture immutable lineage and coverage metadata on replay, then encode on the existing promotion worker. During durable promotion, discard fully expired blockhash groups directly and update partially retained groups by visiting the smaller side.

Preserves checkpoint bytes/format, capture-before-prune ordering, immutable producer views, the300-root retention rule, duplicate reference counts and unwind checks. The proposed transaction-status **publication** optimization is not included; this PR only organizes the already implemented checkpoint and expiry work.

### Benchmarks

Native Zen5: capturing a roughly30MB checkpoint on replay changed from202–207ms to5.92–6.06microseconds. Encoding moved to the worker; it did not disappear. In ten historical live checkpoints, replay-side capture took29–34microseconds and worker encoding179–367ms.

Synthetic expiry of128banks ×33,760keys with one retained bank: four-bank blockhash groups changed from306–311ms to0.049–0.057ms; a group crossing the retention boundary changed from717–735ms to1.85–2.05ms. Setup and later GC are excluded; this workload exceeds the live stall samples and is not a total replay/FAST speedup.

Methods: `docs/status-checkpoint-capture.md` and `docs/transaction-status-expiry.md`. Raw native evidence: `docs/results/status-cache/2026-09-14`.

Fresh standalone replay race tests and vet passed. Coverage includes concurrent snapshot/prune behavior, exact checkpoint equivalence, randomized expiry against the old per-key oracle, pinned views, restore and unwind. Fresh logs: `docs/results/pr-split-2026-09-15/status`.

Split from #279; base development commit: `33dde4050d9250557583395810799aaac2f54017`. Historical native measurements retain their original tested source; these reorganized heads have fresh local validation.
