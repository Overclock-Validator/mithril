# Transaction-status checkpoint capture and encoding

Replay captures immutable lineage and coverage metadata before submitting a
checkpoint to the promotion worker. Sorting, encoding and writing happen on
that worker. Capture does not retain parent links outside the selected window.
Publication and durable-root ordering are unchanged.

Each node memoizes its canonical encoded body on first serialization. Capture
and pruning share the same cache object when copying a node header; they never
copy a used synchronization primitive. Encoding depends on the immutable slot,
block-ID presence/value and status delta, not its parent link. Concurrent
encoders synchronize through `sync.Once` without taking the live cache lock.
Each snapshot still constructs its own coverage header and returns an owned
output buffer. The MTS2 format and restore validation are unchanged.

The cache retains roughly one extra encoded window (30 MB for 1.5 million
keys), plus any nodes pinned by older views. There is no global encoding map:
caches become collectible with their last node/view. A completely new window
still pays for all sorting. Output copying and checkpoint I/O remain necessary.

## Encoding benchmark

`BenchmarkTransactionStatusCheckpointEncoding` uses a 300-root window with
5,000 keys per root (1.5 million keys, roughly 30 MB encoded). Each iteration
replaces the specified number of roots. Fixture creation and initial warming
are excluded; new node headers, sorting and output allocations are included.
The baseline is the original uncached wire encoder retained in tests.

Apple M4 Pro, Go 1.26.4, one caller, GOMAXPROCS=12; medians of three runs:

| New roots per checkpoint | Original encoding | Cached encoding |
| --- | ---: | ---: |
| 1 | 158.05 ms | 1.35 ms |
| 8 | 157.14 ms | 5.09 ms |
| 32 | 155.62 ms | 17.51 ms |
| 128 (default fold cadence) | 153.74 ms | 67.11 ms |
| 300 (entirely new) | 155.90 ms | 156.29 ms |

At the default cadence, allocated bytes per encoding fell from 99.12 MB to
57.33 MB; this excludes retained heap. These are encoding measurements, not
end-to-end fold/replay timings or live FAST improvements. Data distribution
matters: newly rooted large blocks can account for most keys in the window.

Run `go test ./pkg/replay -run '^$' -bench '^BenchmarkTransactionStatusCheckpointEncoding$' -benchmem -benchtime=1s -count=3`.

Tests compare exact bytes with the original encoder across coverage flags,
block IDs and sorted groups; check concurrent encoding during pruning/unwind;
verify cache sharing before and after warming; and restore checkpoints after
callers mutate their own output buffers. The replay race suite and vet pass.

Related behavior: [status expiry](transaction-status-expiry.md) and
[status publication](transaction-status-publication.md).

## Native Zen 5 validation

AMD Ryzen 7 9700X, Go 1.26.4, GOMAXPROCS=2, Nice 15 and a two-core CPU quota,
while the validator continued its normal workload. Same moving-window fixture;
three samples per case, medians below. This compares the original uncached
encoder with memoization, not the whole status-publication change against dev.

| New roots per checkpoint | Original encoding | Cached encoding |
| --- | ---: | ---: |
| 1 | 195.34 ms | 3.73 ms |
| 8 | 195.15 ms | 8.56 ms |
| 32 | 194.77 ms | 23.54 ms |
| 128 (default fold cadence) | 194.52 ms | 85.02 ms |
| 300 (entirely new) | 201.88 ms | 198.55 ms |

The default-cadence result is approximately 2.3x, with the same 99.12 → 57.33 MB
allocation reduction. Cold/all-new windows remain roughly unchanged. Native
combined race suites, vet and the validator build passed. These are staging
measurements: the encoding cache has not been deployed, so a live reduction in
durable-root lag or missed FAST votes has not yet been established.

## Fold admission before collecting account writes

Replay checks for a checkpoint batch on every iteration, including skipped
slots. `WorkingSet.PromotionChunk` first counts eligible held slots under its
read lock. If fewer than the configured batch size are available, ordinary
admission returns nil without allocating account-pointer lists. When ready,
it collects only the oldest batch, not the entire eligible suffix. Forced
partial folds still collect the available prefix.

This preflight is not a finality shortcut or a new recovery policy. Replay's
existing finality/verification gates supply the upper bound. Selection and
collection hold the same lock; account pointers retain their existing ownership
contract. Preparation does not prune the suffix or advance the durable root.
The worker's write/commit order, required resume context, checkpoint reference
validation, completion bookkeeping, and forced shutdown/epoch-boundary paths
are unchanged.

`BenchmarkBuildFoldJobWaitingForBatch` holds 127 slots with 512 account writes
each while waiting for the default 128-slot batch. On Ryzen 9700X,
GOMAXPROCS=8, three 300 ms runs, median admission-check time fell from 426 µs
to 31.8 ns; 627,008 bytes and 134 allocations per rejected preparation became
zero. This measures an ineligible batch check, not encoding, disk I/O, or a
ready checkpoint. Boundary tests cover gaps, the finality upper bound, a full
batch, forced partial batches, and selection after promotion; existing replay
checkpoint/recovery tests cover the unchanged durable path.
