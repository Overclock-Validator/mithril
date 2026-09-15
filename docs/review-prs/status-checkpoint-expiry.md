Replay's transaction-status work can delay voting both at ordinary block commit and during checkpoint preparation. Prepare large banks' immutable status deltas during execution, pre-size status maps, capture checkpoint lineage on replay for encoding by the promotion worker, and expire blockhash groups in batches.

Commit still rechecks exact block binding, coverage, parent lineage and ancestor duplicates under the publication lock. Changed snapshot slice offsets trigger a safe rebuild. Preparation never publishes statuses, and every ProcessBlock exit joins its worker. Blocks with at most 32 transactions and single-thread configurations stay inline. Checkpoint format, capture-before-prune ordering, the 300-root retention rule, pinned producer views and unwind semantics remain intact.

### Publication benchmark

Zen 5 Ryzen 7 9700X, Go 1.26.4, GOMAXPROCS=2; 33,760 prepared identities, one or four blockhash groups, with/without existing ancestor groups. Five samples of ten iterations on the shared validator host. Baseline functions exactly match alpenglow-dev at `33dde4050d9250557583395810799aaac2f54017`.

Remaining commit work measured **4.6–5.2 ms before versus 1.6–2.9 ms after prior preparation**. This excludes the preparation that can overlap execution. Total-work results were mixed in the first run; the slower case and its alternating-order recheck are preserved in the method document. The recheck measured 4.40–4.56 ms before versus 2.98–3.13 ms for preparation plus commit. New-group allocations halved, approximately 6.30 MB to 3.15 MB.

A separate controlled transfer-execution workload measured 20.68 ms baseline, 18.57 ms with sizing alone, and 17.09 ms with overlap using two Go execution threads. This tests contention, not complete bank replay. No live FAST improvement is claimed and this publication change has not been deployed.

Method, all cases and raw evidence: `docs/transaction-status-publication.md` and `docs/results/status-publication/2026-09-15`.

### Existing checkpoint and expiry measurements

Historical native Zen 5 capture of a roughly 30 MB checkpoint changed from 202–207 ms to 5.92–6.06 microseconds. Encoding moved to the worker; it did not disappear. Ten historical live checkpoints took 29–34 microseconds to capture and 179–367 ms to encode.

Synthetic expiry of 128 banks × 33,760 keys with one retained bank changed from 306–311 ms to 0.049–0.057 ms for four-bank blockhash groups, and from 717–735 ms to 1.85–2.05 ms for a group crossing the retention boundary. Setup and later GC are excluded; these are not whole-replay gains. Historical methods and baselines remain in `docs/status-checkpoint-capture.md`, `docs/transaction-status-expiry.md` and `docs/results/status-cache/2026-09-14`.

Full replay/block race suites passed locally and natively; native replay/metrics vet and validator build passed. Regression coverage includes concurrent siblings, late ancestor duplicates, stale preparation, snapshot offset changes, rejected banks, pinned views, restore/unwind and scheduling boundaries. Existing tests cover snapshot/prune behavior, exact checkpoint equivalence and randomized expiry against the old oracle.

Split from #279, with the subsequent publication optimization added to this status-cache branch. Base: `33dde4050d9250557583395810799aaac2f54017`.

### Checkpoint encoding reuse

Memoize each immutable node's MTS2 bytes and share the cache across snapshot capture and pruning. Snapshot headers remain per-capture, output buffers remain caller-owned, and cached data retains no excluded parent chain. The cache adds approximately one encoded window of retained memory (about 30 MB for the benchmark).

On Zen 5, a moving 300-root window with 5,000 keys per root and the default 128-root fold cadence encoded in **194.52 ms before versus 85.02 ms after** (three-sample medians). Allocation fell from 99.12 MB to 57.33 MB per encoding. The baseline is the original uncached encoder from this split branch; this is an incremental encoding comparison, not the whole PR against dev. Entirely new windows were roughly unchanged. Method and other cadences: `docs/status-checkpoint-capture.md`.

Final combined native race suites, vet and build passed. No deployment or live durable-root/FAST gain is claimed. Raw run artifacts remain outside the source tree.
