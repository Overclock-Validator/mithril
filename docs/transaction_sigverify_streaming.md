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
[September 12 benchmark report](https://github.com/Overclock-Validator/mithril/blob/1c1171d3661d0404b013a9bf9391e23eb660706e/docs/results/sigverify-streaming/2026-09-12-zen5/README.md).
The subsequent [direct cache-comparison report](https://github.com/Overclock-Validator/mithril/blob/1c1171d3661d0404b013a9bf9391e23eb660706e/docs/results/sigverify-direct-cache/2026-09-12-zen5/README.md)
isolates the removal of redundant component-buffer construction at completion.
The [completion follow-up report](https://github.com/Overclock-Validator/mithril/blob/1c1171d3661d0404b013a9bf9391e23eb660706e/docs/results/completion-followup/2026-09-12-zen5/README.md)
measures direct ordering, authenticated-root reuse, and the four-vector job policy.

The [standalone PR review](https://github.com/Overclock-Validator/mithril/blob/1c1171d3661d0404b013a9bf9391e23eb660706e/docs/results/streaming-pr-review/2026-09-13/README.md)
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

## Reserved admission for completion

Decoded prefetch components now use a separate admission class. The existing total request limit remains `2 * workers`; at most `2 * workers - 1` requests may prefetch. Thus the default two-worker pool keeps four total permits, with at most three occupied by prefetch. Waiting completion/full-block recovery requests win the next free permit over prefetch. Their admission, cancellation and close registration share the verifier mutex; notification channels are allocated only when callers must wait.

No worker or job queue is added, and verification, vector width, job grouping and per-request rolling windows are unchanged. Accepted jobs finish normally and are joined before transaction memory can be reused. No signature checks are skipped. Prefetch may wait while completion callers remain queued and resumes when that backlog drains. This is completion-class priority, not exact replay-head priority: future-slot completions also qualify, and already admitted prefetch work is not promoted or preempted. Checkpoint, persistence and voting-recovery contracts are unchanged.

### Saturation benchmark

`BenchmarkVerifierCompletionReservation` verifies four already-ready prefetch components (256 or 4,096 signed 228-byte transactions each) plus a 32-transaction completion request. All requests are joined; total-work timing includes all four components. Two verifier workers, eight signature lanes, four vector groups, GOMAXPROCS=8, Narya r51, Ryzen 9700X / Go1.26.4. Three runs of 100 iterations each, Nice19 and a 200% CPU quota on the shared validator host. Test intervals are excluded from live FAST comparisons.

The before comparison uses the previously deployed source (status-validation combined build, SHA256 `2c81fc403e8a6eca73b87ede51890041347e1af0e3c63df005fcfeb26448e872`) with only the benchmark added via a Go test overlay. The candidate also measures the shared-class control to distinguish policy from incidental overhead. These are incremental admission results, not the whole PR versus alpenglow-dev.

For four 4,096-transaction components, medians of the three per-run statistics were:

| Measurement | Previous deployment | Reserved admission |
|---|---:|---:|
| Completion admission p50 | 37.19 ms | 0.000742 ms |
| Completion admission p99 | 45.78 ms | 0.004599 ms |
| Completion finished p99 | 47.31 ms | 1.488 ms |
| All work finished p50 | 39.43 ms | 39.11 ms |
| All work finished p99 | 49.09 ms | 50.65 ms |

Completion-finished p99 ranged 46.43–54.52 ms before and 1.477–1.719 ms after. Admission p99 ranged 44.27–53.76 ms before and 0.003206–0.06401 ms after. Every iteration reached its intended request occupancy. For 256-transaction components, completion-finished p99 medians were 3.215→1.165 ms. Shared-host scheduling introduces variation; reserving admission does not remove queued-job or CPU delays, and these 100-sample tails are not a live p99/FAST claim. Total-work throughput was roughly unchanged; no total-work tail improvement is claimed.

Original run artifacts are retained in the [evidence archive](streaming-preparation-evidence.md).

### Completion-critical shred diagnostics

The optional `MITHRIL_ENTRY_TRACE_MOD`, `MITHRIL_ENTRY_TRACE_SECONDS` (at most
1,800), and `MITHRIL_ENTRY_TRACE_FILE` settings are read at process startup.
Use a new output file for each capture. Tracing is disabled by default.

Large-block batch records include `critical_shred_index` and its admission
source: `non_repair`, `repair`, or `fec_recovery`. The critical index maximizes
local availability time across the batch **and its preceding DATA_COMPLETE
boundary**. Equal timestamps select the lowest index and report the tie count;
unknown coverage still sets `availability_known=false`. This identifies the
last locally available dependency, not necessarily the replay cursor's current
blocking range. Correlate with execution groups before calling it a replay stall.

Recovered shreds identify the triggering packet's index, FEC set, coding/data
type and repair status. `non_repair` can include spool hydration. Admission entry
timestamps precede the assembler lock; they are not socket/NIC timestamps. A zero
admission-entry timestamp means it was not sampled. Admission-to-availability
includes local processing and, for recovered data, reconstruction.

The same JSONL file also contains `event="repair_send"` records. Consumers must
separate these from block reports. Join by `origin_unix_ns`, slot and shred index,
then order by send timestamps; attempt IDs can reset. Start/end bracket the UDP
write, and `success` means only that the local write succeeded. Highest-index
probes are explicitly marked and must not be treated as exact-index requests.
Request records can exist for slots without a large-block report. Send records include the peer endpoint and nonce for response correlation.

Both queues are bounded and producers never wait for the writer. The cumulative
`dropped_reports` counter covers queue drops and encoding failures; an absent
repair record is not proof of no request if records were dropped. Tracing does
not change repair scheduling, retry intervals, fanout or verification checks.

### Repair ordering for streaming

When a streaming subscriber is installed, the first priority repair slot puts
its earliest missing data span ahead of the usual cheapest-FEC-unlock ordering.
Known FEC sets still request only their recovery deficit; an unknown-layout hole
prioritizes one missing index without guessing its FEC shape. Remaining work,
other priority slots and freshness repair retain their previous ordering.
Request budgets, admission shares, retry intervals and fanout are unchanged.

This trades completing cheap later sets first for making the contiguous input
prefix available sooner. It helps when request capacity is constrained; it does
not accelerate requests already in flight, guarantee an earlier full block, or
prove improved voting latency. The subscriber and priority head are used as the
scope; the selector does not read the execution cursor.

`go test ./pkg/turbine/repairsim -run TestStreamingPrefixRepairUnderLimitedBudget -v`
compares both policies using authenticated generated shreds and production FEC
recovery, at fixed request budgets and a 20ms simulated round trip. It checks
identical assembled entries/transactions, request counts and full completion,
and measures availability of the first data span. It does not model production
retry timers, peer loss, execution timing, or reproduce a captured live slot.


Response-effectiveness tracing also emits `repair_response` and
`repair_admission` events. Treat every record with an `event` field as an event,
not a block report. Match sends/responses by origin, peer, nonce and requested
slot/index; use timestamps to disambiguate nonce reuse. Responses record the
returned index, request-registration timestamp and whether the request had
expired. Registration precedes signing/write; use `send_start_ns` for the closer
approximation to network elapsed time. A matched response is not proof that
assembly accepted it: later receive-path checks can still reject it.

Admission events cover sampled matched-repair shreds reaching an active assembly;
join to responses by slot/returned index and chronology (no nonce is carried into
the assembler). They report accepted/duplicate/rejected, the coding-layout
recovery deficit before/after, and the number of reconstructed data shreds.
Deficit `-1` means unknown layout; zero means enough shards, not necessarily
successful recovery. Admission timestamps are local assembler entry/exit,
including lock wait and processing, not NIC timestamps. Already-completed,
evicted, or completing slots return before this instrumentation. An unmatched
or canceled response is not emitted as a matched response. Missing records,
particularly with drops or capture boundaries, cannot establish packet loss.

### Bounded child repair lookahead

Replay supplies its exact streaming generation as a repair anchor. The
asynchronous decoder can then recognize a decoded header for the immediate next
slot naming that parent, even while replay executes a parent group. A header
published earlier is recovered from the assembler's ready batches. The hint
permits fetching only; it does not establish fork choice, validate the final
parent block ID, or permit child execution before parent completion.

After ordinary priority and freshness repair, leftover tokens may request up to
four missing data shreds from the child's earliest incomplete FEC span. Existing
in-flight requests for that child count against the four-request lookahead
allowance. Normal repair can independently exceed that allowance. Global rate,
per-scan and admission limits remain in force; lookahead uses bulk single-attempt
policy, with no new retry/fanout or highest-index probing. If no capacity remains,
the child waits.

Only one child is tracked. The anchor expires after two seconds and is cleared
on parent finalize/discard, parent reset/update, or stream unsubscribe. Child
completion/reset and changed parent markers invalidate its hint. Generation
checks reject stale headers. Already-sent requests still use normal response and
expiry handling. Notifications and repair wakeups remain nonblocking/coalesced;
no extra workers or polling loop are introduced.

### Highest-index repair followups

A matched highest-index response triggers followup selection only after receiver
admission and FEC recovery. The assembler supplies its current deficit-aware
selection (at most 256 data requests), rather than treating the interval below
the response as missing. Completed, completing, evicted and absent assembler
slots produce no immediate followups. During disk-only catchup, selection waits
for hydration instead of blindly fetching data that may already be spooled.

Followups retain the shared token bucket, admission limits, bulk retry policy,
and a reserved token for continued highest-index discovery when needed. A
snapshot can still race with subsequent arrivals; this removes known redundant
requests, not every possible duplicate. No assembler lock is held while signing
or sending requests. Response matching and peer credit are unchanged.


### Bounded speculative verification waits

Replay joins a streaming group's signature verification with one shared 100 ms
budget, capped by the stream's remaining open lifetime. The watchdog stage is
`streaming_sigverify_wait`. Expiry discards the speculative overlay with reason
`sigverify_timeout`; it is not a signature verdict. Whole-block replay still
requires normal verification before accepting the block.

The streaming wait only observes immutable verifier results. Timing out does not
cancel the shared request or wait for its workers: turbine continues owning its
transactions and retains reservations until readers finish. The owning completion
and cleanup paths retain their joining waits. This bounds speculative replay's
wait, not the duration of whole-block verification or recovery from a failed worker.

`StreamingExecution.VerificationWait` records wall time spent joining groups,
including failed joins and discarded streams. It overlaps `GroupJoinAssembly` for
successful groups; do not add them together or interpret it as cryptographic CPU
cost. The 100 ms limit is a conservative fallback budget, not a measured optimum.


### Runtime pooling default

Omitting `tuning.use_pool` now preserves the CLI default (`true`), instead of
silently disabling VM pooling through the config reader's zero value. Explicit
TOML `false` remains supported, and an explicitly supplied CLI flag takes
precedence. This is a runtime behavior change for previously minimal configs;
pooled memory is cleared before reuse. The flag remains the single default source.
