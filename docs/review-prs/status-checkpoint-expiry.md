Replay's transaction-status work can delay voting both at ordinary block commit and during checkpoint preparation. Prepare large banks' immutable status deltas during execution, pre-size status maps, capture checkpoint lineage on replay for encoding by the promotion worker, and expire blockhash groups in batches.

Commit checks exact block binding, coverage and parent lineage under the publication lock. Ancestor duplicates are rescanned unless pre-execution validation belongs to the same unchanged cache and exact immutable identities. Changed snapshot slice offsets trigger a safe rebuild. Preparation never publishes statuses, and every ProcessBlock exit joins its worker. Blocks with at most 32 transactions and single-thread configurations stay inline. Checkpoint format, capture-before-prune ordering, the 300-root retention rule, pinned producer views and unwind semantics remain intact.

### Publication benchmark

Zen 5 Ryzen 7 9700X, Go 1.26.4, GOMAXPROCS=2; 33,760 prepared identities, one or four blockhash groups, with/without existing ancestor groups. Five samples of ten iterations on the shared validator host. Baseline functions exactly match alpenglow-dev at `33dde4050d9250557583395810799aaac2f54017`.

Remaining commit work measured **4.6–5.2 ms before versus 1.6–2.9 ms after prior preparation**. This excludes the preparation that can overlap execution. Total-work results were mixed in the first run; the slower case and its alternating-order recheck are preserved in the method document. The recheck measured 4.40–4.56 ms before versus 2.98–3.13 ms for preparation plus commit. New-group allocations halved, approximately 6.30 MB to 3.15 MB.

A separate controlled transfer-execution workload measured 20.68 ms baseline, 18.57 ms with sizing alone, and 17.09 ms with overlap using two Go execution threads. This tests contention, not complete bank replay. This publication change is now deployed in the combined testnet validator; no isolated live FAST improvement is claimed.

Method, all cases and raw evidence: `docs/transaction-status-publication.md` and `docs/results/status-publication/2026-09-15`.

### Existing checkpoint and expiry measurements

Historical native Zen 5 capture of a roughly 30 MB checkpoint changed from 202–207 ms to 5.92–6.06 microseconds. Encoding moved to the worker; it did not disappear. Ten historical live checkpoints took 29–34 microseconds to capture and 179–367 ms to encode.

Synthetic expiry of 128 banks × 33,760 keys with one retained bank changed from 306–311 ms to 0.049–0.057 ms for four-bank blockhash groups, and from 717–735 ms to 1.85–2.05 ms for a group crossing the retention boundary. Setup and later GC are excluded; these are not whole-replay gains. Historical methods and baselines remain in `docs/status-checkpoint-capture.md`, `docs/transaction-status-expiry.md` and `docs/results/status-cache/2026-09-14`.

Full replay/block race suites passed locally and natively; native replay/metrics vet and validator build passed. Regression coverage includes concurrent siblings, late ancestor duplicates, stale preparation, snapshot offset changes, rejected banks, pinned views, restore/unwind and scheduling boundaries. Existing tests cover snapshot/prune behavior, exact checkpoint equivalence and randomized expiry against the old oracle.

Split from #279, with the subsequent publication optimization added to this status-cache branch. Base: `33dde4050d9250557583395810799aaac2f54017`.

### Checkpoint encoding reuse

Memoize each immutable node's MTS2 bytes and share the cache across snapshot capture and pruning. Snapshot headers remain per-capture, output buffers remain caller-owned, and cached data retains no excluded parent chain. The cache adds approximately one encoded window of retained memory (about 30 MB for the benchmark).

On Zen 5, a moving 300-root window with 5,000 keys per root and the default 128-root fold cadence encoded in **194.52 ms before versus 85.02 ms after** (three-sample medians). Allocation fell from 99.12 MB to 57.33 MB per encoding. The baseline is the original uncached encoder from this split branch; this is an incremental encoding comparison, not the whole PR against dev. Entirely new windows were roughly unchanged. Method and other cadences: `docs/status-checkpoint-capture.md`.

Final combined native race suites, vet and build passed. Encoding reuse is now deployed in the combined testnet validator; an isolated live durable-root/FAST gain is not established. Raw run artifacts remain outside the source tree.


Fold admission now checks the held-slot count before materializing account-write lists and copies only the selected oldest batch. This removes repeated work when empty blocks or skipped-slot runs arrive before a checkpoint batch fills. Selection and collection share one WorkingSet read lock. The verified upper bound, forced partial folds, required resume context, checkpoint validation and durable commit/root ordering remain unchanged.

`BenchmarkBuildFoldJobWaitingForBatch` (Ryzen 9700X, GOMAXPROCS=8, 127 held slots × 512 account writes, batch 128, three 300 ms runs) measured **426 µs → 31.8 ns**, with **627,008 bytes / 134 allocations → zero** per ineligible check. This is not a full-checkpoint or FAST-score speedup. Native account and targeted replay recovery/checkpoint race tests, vet and the combined build passed. Boundary tests cover eligibility, gaps, selected-prefix bounds and forced partial chunks. See `docs/status-checkpoint-capture.md`. Fix commit: `d55c7962`.

### Empty-block deployment measurement

Fold preflight (`d55c7962`) and the pending-certificate observer index (`72514abc`, separate certificate PR) were deployed together on September 15. Initial empty-block replay-admission p99 was **4.486 → 0.483 ms** (565 before, 401 after). Post-deployment fold admission across 1,386 calls measured 0.0071 ms median, 0.0125 ms p99 and 1.136 ms maximum. These are short observational windows, exclude the native benchmark interval, and do not establish either patch’s isolated contribution or sustained overall FAST gains.

### Retire rewards bookkeeping after durable completion

A finished distribution retained its descriptor and therefore blocked in-memory fork unwind long after rewards were durable. Track an inactive EpochRewards snapshot from a successfully executed bank, bound to the exact distribution descriptor, and retire it only when a successfully committed fold advances the durable root through that bank. Active, unknown and uncommitted completion still force the existing fallback; all other unwind and restart/signing safety checks remain intact. No new persisted format or sync is added.

At the motivating fork, rewards had finished at 3,942,001 and the durable root had reached 3,944,067. Recovery nevertheless re-fetched blocks, recording 2.739 s and 0.967 s waits; an assembled 665-transaction block waited 3.614 s for admission before 7.520 ms execution. These are incident measurements, not candidate speedups or checkpoint encoding timings.

Commit `c78e35cd` adds boundary/generation/failed-fold tests and exact-parent account, resume-state and rewards-snapshot checks. Full replay/rewards race suites passed on M4 and Zen 5; native node recovery/checkpoint race tests, vet and combined build passed. Deployed in the combined Zen 5 validator September 15 at 12:20 UTC after a clean stop; three advancing health checks passed before resuming the existing paced sender. The first post-restart epoch completion and comparable fork switch are still needed to establish live benefit. Method and contract: `docs/rewards-unwind-retirement.md`.

### Reuse ancestor validation at publication

Commit `d04ad985` avoids the second full ancestor scan when the cache instance, immutable transaction identities and mutation version still match the successful pre-execution check. Commit, unwind, root/prune, tip binding and restore invalidate reuse; cache-version saturation falls back permanently. Direct commits without a receipt still scan. This receipt is never serialized and changes no crash-recovery, checkpoint durability or voting-resume guarantee.

Native Zen 5 incremental benchmark: 33,760 transactions, existing one/four blockhash groups, GOMAXPROCS=2, five samples × 20 iterations. Publication measured **2.510 → 1.364 ms** (one group) and **2.412 → 1.283 ms** (four groups). New-group cases showed no clear gain. Invalidating the receipt restored full-scan cost. The benchmark excludes pre-execution validation and delta preparation; it is neither overall replay speedup nor p99 evidence. A separate baseline live trace attributed 1.805 ms median / 3.180 ms maximum to the second scan among 145 publications taking at least 1 ms. Lock/preparation waits were negligible in that sample. Method and all cases: `docs/transaction-status-publication.md`.

Full native replay/block race tests, targeted node recovery race tests, vet and combined build passed; local replay race tests and vet passed. Regression tests cover late fork duplicates, concurrent siblings, cache/identity mismatch, root/prune, restore, transaction replacement and version saturation. Raw timing records stay on the server. Deployment validation and FAST attribution are reported separately.


Deployed September 15 at approximately 12:39 UTC after a clean stop, preserving signing/checkpoint state. The post-deployment 180-second trace showed no second scan in 63 executed slots with at least 30,000 fee checks. Publication median was 5.270 ms before (123 slots) versus 3.719 ms after (63 slots); these cohorts are not matched for leader/type/CU and are not p99 evidence. Across all publication callers, including direct local-leader adoption which still scans, trace p99 was 7.665→8.296 ms; no overall tail improvement is claimed. First complete candidate FAST sample: 769/774 (99.35%), clean parsing and verified binary metadata. Five completed sender runs each submitted 200,000 transactions with zero errors; maximum observed own block 48,622 transactions. Voting, load and monitors remain running. These operational checks do not establish sustained FAST improvement.
