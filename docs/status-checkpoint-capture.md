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
