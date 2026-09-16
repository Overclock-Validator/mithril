# Batched transaction-status expiry

Applying an asynchronous checkpoint still calls `TransactionStatusCache.Root`
on replay. Live Zen 5 instruction probes measured 43–101 ms inside that function.
The previous expiry path visited every key in every retired bank, even when an
entire recent-blockhash group could be discarded.

Expiry now examines the expired and retained bank deltas by blockhash. It drops
fully expired groups directly. For a group spanning the cutoff, it either
subtracts the expired keys or rebuilds the visible reference counts from the
retained deltas, whichever requires fewer key visits. Retained unrooted banks
are included. Physical map reclamation is still Go GC work; this is not a claim
that memory reclamation costs disappear.

The 300-root retention rule, immediate logical expiry, duplicate-key reference
counts, selected-parent validation, checkpoint format and immutable producer
views are unchanged. All index changes remain under the existing cache lock.
This does not move unsafe mutable state to another goroutine or delay expiry.
A long-lived blockhash with many transactions on both sides of the cutoff can
still require substantial per-key work. This patch reduces that work to the
smaller side; it does not give a constant-time worst-case bound.

## Validation

The replay race suite, replay vet and validator production build pass. New tests
compare exact visible indexes against the original per-key removal for 100
random lineages with shared hashes, collisions and empty groups, then unwind
surviving banks. A Root integration test checks pinned producer views,
checkpoint bytes, restored duplicate detection and rooted-unwind rejection.

M4 Pro, Go benchmark, single caller, two iterations per case. Each iteration
expires 128 banks of 33,760 unique keys (4,321,280 entries) and retains another
33,760 entries. Setup is outside the timer. The baseline invokes the original
per-key removal; the new path invokes batched expiry. These are **expiry-path**
measurements, not end-to-end Root/replay or a prediction of live FAST scores.

| Recent-blockhash grouping | Old expiry | Batched expiry |
|---|---:|---:|
| Groups shared by four expired banks | 185–189 ms | 0.037–0.080 ms |
| One fully expired group | 604–614 ms | 0.025–0.026 ms |
| One group shared by expired and retained banks | 590 ms | 1.63–2.36 ms |

Run `go test ./pkg/replay -run '^$' -bench '^BenchmarkTransactionStatusBatchExpiry$' -benchtime=1x -count=2`.

## Native benchmark

Ryzen 7 9700X, Go 1.26.4, original per-key expiry versus batched expiry.
Benchmarks ran with GOMAXPROCS=2, nice=15, one caller and three iterations per
case, while the validator and loader remained active. Setup and later GC are
excluded from the expiry timer. Each case expires 4,321,280 entries (128 banks
of 33,760) and retains 33,760 entries. These synthetic batches exceed the earlier
live stall samples and are not an end-to-end replay or FAST-score comparison.

| Shape | Original expiry | New expiry |
|---|---:|---:|
| Four-bank blockhash groups | 306–311 ms | 0.049–0.057 ms |
| One fully expired blockhash group | 700–718 ms | 0.024–0.031 ms |
| Group crossing the retention boundary | 717–735 ms | 1.85–2.05 ms |

[Historical evidence](status-checkpoint-expiry-evidence.md) preserves the original
source revisions, raw measurements and validation.
