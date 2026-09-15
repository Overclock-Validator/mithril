# Transaction signature verification during shred arrival

Turbine verifies transaction signatures as complete entry batches arrive. The
default shared transaction pool has `min(2, GOMAXPROCS)` workers and targets eight signature
lanes. A ready batch containing four transactions runs immediately; there is no
timer or minimum occupancy requirement. The same policy handles live reception
and repair catch-up without a mode transition or a 200 ms batching delay.

## Order of work

1. The receiver authenticates the shred and performs existing assembly/FEC
   recovery. The packet reader advances a contiguous data-shred frontier and
   attempts a nonblocking background enqueue. Assembly retains an authenticated
   root and a snapshot of its source shred for each FEC set when available.
2. Two background preparation workers copy and decode complete DATA_COMPLETE
   entry batches. A batch may span several FEC sets. They submit its immutable
   transactions to the shared signature pool while later shreds arrive.
3. Signature groups contain available transactions up to the configured lane
   target. Transactions with multiple signatures remain indivisible. Large
   already-decoded requests bundle four vector groups per dispatch job (normally
   32 one-signature transactions). Requests smaller than
   `2 * workers * batch_target * 4` transactions keep one group per job; the
   default threshold is 128 ready transactions. A rolling per-request window
   refills when any job finishes, without a wave barrier or batching timer.
4. Once the full slot is assembled, completion compares shred slices directly
   against the cached component bytes, including padding. Cache hits avoid a
   second component buffer; misses allocate a fresh buffer at its exact size.
   Complete contiguous slots supply shred order directly, avoiding two sorts.
   Completion processes all Alpenglow markers and FEC roots and constructs the
   final ordered block. It submits any transactions not already covered and joins
   signature work for the retained batches before marking the block verified.
5. The existing replay pipeline receives the verified block. This change does
   not execute transactions before full-slot admission, alter execution batching,
   or move parent-dependent validation ahead of its required state.

The 200 ms slot interval provides an opportunity to overlap work. It is not a
mandatory local wait: replay can continue as soon as the block is available and
its checks complete. If all shreds arrive in a burst during catch-up, full-slot
completion uses the same efficient groups with whatever early work was able to
start. Four workers can improve catch-up latency but occupy more cores at once.

## Bounds and correctness

- At most eight slot generations hold early preparation reservations, with a
  combined 64 MiB budget for raw component bytes and a 1 MiB per-component limit.
  Decoded transaction objects add heap overhead. Saturation skips optional early
  work; normal full-slot verification still covers every retained transaction.
- One queued/active preparation token per generation coalesces packet arrivals.
  Verifier requests and each request's outstanding jobs are also bounded.
  Request admission can wait behind existing requests; this is not a strict
  replay-head priority scheduler.
- No transaction decoding or verifier admission occurs under the assembler mutex or on the
  packet reader. A gap prevents early component decoding until recovery or
  arrival closes it. Duplicate shreds do not create duplicate requests.
- Cached results belong to one generation, shred range and exact byte sequence.
  Reset/eviction cancels that generation. Reservations remain charged until
  admitted readers have relinquished their transaction buffers.
- A FEC root cache retains at most one source snapshot per FEC state, in addition
  to the entry-prefetch budget. Completion preserves the deterministic choice of
  the lowest-index non-recovered data proof, then lowest coding position. Cached
  roots require the same source, parsed root inputs, and exact payload bytes;
  mismatches, unauthenticated callers, and spool hydration recompute the root.
- `UpdateParent` can discard an optimistic prefix. Parse and marker checks still
  cover that prefix, while its transaction signature verdict is discarded along
  with its transactions. Retained signatures must all pass before replay.
- Cancellation is not an invalid-signature verdict. A retry on the same slot
  generation verifies transactions again if an earlier request was canceled.
  An admitted job finishes its first vector group; cancellation can skip later
  groups in that job. The request joins all admitted jobs before releasing input.

## Configuration and observability

```toml
[sigverify]
backend = "auto"
workers = 0                 # min(2, GOMAXPROCS)
batch_target = 8            # 4 or 8; short groups never wait to fill
disable_shred_overlap = false
```

Equivalent CLI flags are `--sigverify-workers`, `--sigverify-batch-target` and
`--sigverify-disable-shred-overlap`. These settings apply to Turbine transaction
signatures; TPU, shred signatures, consensus BLS and replay's fallback verifier
retain their existing configuration.

Use `TurbineFullToReady` to measure residual wall time after the slot becomes
complete. `TurbineEarlyVerifiedTransactions` counts retained transactions whose
verification finished before full assembly. `TurbineTransactionSigverify` now
measures completion's outstanding-signature join/fallback work. The remaining
wait for an already claimed background preparation job, including
any outstanding admission delay, appears in `TurbineEarlyPreparationWait`,
separately from active completion decoding. It does not sum every background
admission wait. Early parse and signature durations are component sums observed
at completion;
a discarded prefix still being verified is not included. These durations
overlap reception and each other and must not be added as sequential stages
or interpreted as CPU time.

## Benchmark scope

`BenchmarkTransactionVerificationFlow` compares two/four workers and targets
four/eight using catch-up, synthetic 200 ms component arrivals, and sparse
four/seven/eight-transaction components. It supports captured public transaction
fixtures and generated, distinct valid transactions of exactly 228 or 1,232 wire
bytes. Decoding and fixture construction are outside those pool measurements.

The execution contention probe runs the real transfer load/execute benchmark
alongside signature work on the same eight physical cores. Its separate
processes measure hardware/OS contention, excluding shared Go scheduler/heap
effects, full-block dependency planning, commit and live network timing. Pool
throughput alone is insufficient evidence of end-to-end replay improvement.

Measured Zen 5 results, raw logs, and validation details are in the
[September 12 benchmark report](results/sigverify-streaming/2026-09-12-zen5/README.md).
The subsequent [direct cache-comparison report](results/sigverify-direct-cache/2026-09-12-zen5/README.md)
isolates the removal of redundant component-buffer construction at completion.
The [completion follow-up report](results/completion-followup/2026-09-12-zen5/README.md)
measures direct ordering, authenticated-root reuse, and the four-vector job policy.

The [standalone PR review](results/streaming-pr-review/2026-09-13/README.md)
records extraction onto current `alpenglow-dev`, the small shared component-boundary
prerequisite, final allocation improvement, and the scope of the live trial.

## Worker default compatibility

The automatic transaction-verifier default changes from `(GOMAXPROCS + 1) / 2` workers to
`min(2, GOMAXPROCS)`, including when shred overlap is disabled. This favors spare
CPU capacity for execution and other verification at the tip. It is not a claim
of maximum catch-up throughput on every core count or backend. Set
`--sigverify-workers N` or `[sigverify] workers = N` explicitly when tuning a
larger machine; disabling overlap alone does not restore the previous worker
count. Existing two/four-worker contention measurements are in the September 12
report above. No 32-core comparison was performed.
