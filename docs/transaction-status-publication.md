# Preparing transaction-status publication during execution

Replay previously built the immutable per-bank transaction-status delta and grew the visible duplicate index only after execution and bank-state publication. In a prior live sample of 25 large blocks, TransactionStatusCommit took 7.704 ms median and 10.206 ms maximum. Those live timings motivate this change; they are not the controlled benchmark baseline below.

Count identities by recent blockhash and allocate each delta map at its final capacity. Pre-size newly created visible maps too. For banks with more than 32 transactions and GOMAXPROCS greater than one, prepare the immutable delta during account loading and execution. Smaller banks and single-thread configurations keep the work inline. There is at most one preparation task per ProcessBlock call, and every return joins it, including rejected banks. No status becomes visible during preparation.

The worker reads immutable prepared message identities and briefly snapshots only blockhash slice offsets under the cache read lock. It builds its private maps outside the lock. Commit checks exact block/identity binding, complete coverage and parent lineage under the publication lock. It rechecks ancestor duplicates unless the successful pre-execution validation belongs to the same cache instance, immutable identity set and unchanged cache version (see below). A changed slice offset or mismatched preparation triggers a rebuild from the actual block's identities. Publication still happens only after successful bank-state commit. Failed instructions within an accepted bank remain processed; rejected banks publish nothing. Pinned views, snapshots, reference counts and unwind keep their existing semantics.

TransactionStatusPreparation measures worker wall time, which overlaps execution; it is not additive with replay wall time. TransactionStatusPreparationWait measures the residual join and is nested inside TransactionStatusCommit. The latter still includes waiting, final checks, visible-index updates and node publication. Preparation time excludes initial goroutine scheduling delay; any residual scheduling delay remains in the join/commit timer.

## Native benchmark

AMD Ryzen 7 9700X (Zen 5), Go 1.26.4, GOMAXPROCS=2. Tests ran in a separate process on the validator host with Nice=15 and a 200% CPU quota; the validator and loader continued running. This is a shared-host microbenchmark, with observable timing variation. Five samples per case, ten iterations per sample; values below are medians of sample means, not per-block percentiles.

Each block has 33,760 unique prepared message identities spread across one or four recent blockhashes. Existing-group cases seed 33,760 different ancestor transactions. Fixture creation, hashing, seeding and unwind are untimed. Existing maps retain capacity after unwind: the first timed commit's growth is amortized across the ten iterations. This does not model an index growing indefinitely across live blocks.

The frozen baseline functions exactly match alpenglow-dev commit `33dde4050d9250557583395810799aaac2f54017`. Both versions use the same prepared identities, parent/duplicate checks and fixtures.

| Recent blockhash groups | Parent has keys in these groups | Baseline commit | Sized maps, inline | Preparation + commit, no overlap | Commit after preparation |
|---|---|---:|---:|---:|---:|
| 1 | No | 4.990 ms | 3.332 ms | 3.393 ms | 1.587 ms |
| 1 | Yes | 4.991 ms | 4.657 ms | 4.434 ms | 2.757 ms |
| 4 | No | 4.625 ms | 3.744 ms | 5.916 ms | 2.205 ms |
| 4 | Yes | 5.224 ms | 4.644 ms | 4.949 ms | 2.858 ms |

The last column deliberately excludes delta preparation: it measures the work remaining if execution hides preparation completely. It is not total replay or CPU work. Total publication allocations with new groups fell from approximately 6.30 MB to 3.15 MB per block. Existing-group allocation figures include the amortized first growth described above.

The four-new-group total-work sample was slower. Preserve that result rather than claiming improvement in every sample. A subsequent baseline/candidate/candidate/baseline comparison of that same case, with 50 iterations per sample, measured baseline **4.400 and 4.565 ms**, candidate **2.985 and 3.131 ms**. This supports a reduction in work but does not isolate the cause of the earlier timing variation.

## Execution contention and small blocks

A separate controlled benchmark performs 4,096 load-and-execute calls using the existing transfer fixture while preparing 33,760 independent status keys. It does not commit transfer accounts, and its status fixture differs from the repeated transfer fixture. It tests scheduling/allocation contention, not whole-block replay or a valid block workload.

With two Go execution threads, the final implementation measured **20.678 ms baseline**, **18.574 ms with sizing alone**, and **17.091 ms with overlap**. Execution itself measured 15.070, 14.369 and 14.967 ms respectively. Thus preparation competed with execution relative to sizing alone, but the shorter final stage outweighed that cost in this controlled workload. These are separate medians and need not add exactly.

The initial unrestricted version showed no additional total-time benefit from overlap with GOMAXPROCS=1. Tiny-block measurements also showed roughly a microsecond of avoidable scheduling overhead. The final implementation therefore does no background preparation with one Go execution thread or at most 32 transactions. Empty and one-transaction cases retain the baseline allocation counts. The 32-transaction case benefits from sizing without launching a worker. Threshold and single-thread behavior have regression coverage.

## Validation and limits

Full replay and block race suites passed on both Zen 5 and M4 Pro. Metrics has no tests. Native vet for replay/metrics and the validator build passed. Tests cover fork replacement introducing a duplicate after preparation, concurrent sibling publication, stale identity binding, changed snapshot slice offsets, rejected/incomplete banks, mismatched preparation, pinned views, snapshot restore, unwind, empty banks and scheduling boundaries.

Raw logs, source hashes, summaries and the alternating recheck are in [results/status-publication/2026-09-15](https://github.com/Overclock-Validator/mithril/blob/a511ad3b0bc77cf8b5ae4ee16359ac6b453fc7bf/docs/results/status-publication/2026-09-15). The baseline comparison covers only status publication. No live replay or FAST improvement is claimed. The staging binary was not deployed; the existing validator remained active and voting throughout the tests.

Reproduce from this branch:

```sh
GOMAXPROCS=2 go test -race -p 2 ./pkg/replay ./pkg/block ./pkg/metrics -count=1
GOMAXPROCS=2 go vet -p 2 ./pkg/replay ./pkg/metrics
GOMAXPROCS=2 go build -p 2 ./cmd/mithril
GOMAXPROCS=2 go test ./pkg/replay -run '^$' -bench '^BenchmarkTransactionStatusPublication$' -benchtime=10x -count=5
go test ./pkg/replay -run '^$' -bench '^BenchmarkTransactionStatus(ExecutionOverlap|SmallPublication)$' -benchtime=100ms -count=5 -cpu=1,2
```

## Reusing pre-execution ancestor validation

`ProcessBlock` now carries a private validation receipt from its successful ancestor scan to status publication. Under the commit lock, an unchanged receipt avoids scanning all transaction messages again. Publication still checks block binding, complete coverage and parent lineage every time; a missing, foreign or stale receipt performs the full ancestor scan. Direct `CommitBlock` callers retain the full scan.

The receipt is bound to the cache instance and exact immutable prepared-identity pointer. Visible-index insertion/removal, tip binding, root/prune and restore invalidate the version, including empty commits. Committing and then unwinding back to an identical parent cannot revive a receipt. Version saturation disables reuse permanently rather than wrapping. Snapshot/Agave recovery creates a new cache instance. Receipts are never persisted, and no checkpoint format, durability, voting-resume or crash-recovery guarantee changes.

The publication benchmark adds `validated_commit` and `invalidated_commit` alongside `prepared_commit`. All three exclude delta preparation and the pre-execution scan. The first reuses that scan; the second calls `Root` between validation and publication, forcing revalidation. Each iteration unwinds and obtains a fresh receipt outside the timer. These are incremental publication comparisons, not the full PR against alpenglow-dev or per-block tail latency. Tests exercise fork replacement introducing duplicates, concurrent sibling commits, cross-cache and cross-identity misuse, snapshot replacement, pruning/root invalidation, binding changes, transaction replacement and version saturation.

Zen 5 incremental measurement (Ryzen 9700X, Go 1.26.4, GOMAXPROCS=2, five samples × 20 iterations, Nice=19 / 200% CPU quota on the running validator host):

| Recent blockhash groups | Existing ancestor groups | Full recheck | Reused validation | Invalidated validation |
|---|---|---|---|---|
| 1 | yes | 2.510 ms | 1.364 ms | 2.536 ms |
| 4 | yes | 2.412 ms | 1.283 ms | 2.440 ms |
| 1 | no | 1.461 ms | 1.472 ms | 1.769 ms |
| 4 | no | 1.323 ms | 1.395 ms | 1.303 ms |

Values are medians of sample means. Existing-group cases remove approximately 1.1 ms of repeated lookup work; new-group cases show no clear gain and shared-host variation. Full native replay/block race suites, targeted node recovery race tests, vet and the combined build passed. Local replay race tests and vet also passed.

A separate 180-second pre-change live trace observed 728 publications. In the 145 publications taking at least 1 ms, the repeated scan measured 1.805 ms median / 3.180 ms maximum; insertion 2.198 / 6.774 ms. Lock acquisition was at most 0.0058 ms across all publications, and the preparation join at most 0.0010 ms. This latency-selected cohort is not a fixed transaction-size sample or a before/after p99 comparison. Probe overhead is included. These measurements identify removable work; they do not establish a sustained FAST improvement. Raw traces, native test windows and exact combined source stay on the validator host at `/srv/mithril-status-validation-20260915`.
