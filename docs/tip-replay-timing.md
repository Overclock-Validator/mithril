# Measuring replay at the tip

A node writes one row per replayed block to `replay_timings.jsonl`. This page
says which of its numbers answer which question, and how to break down live
replay latency with `mithril timings tip`.

## Three different numbers

"How long a block takes to replay" means one of three things:

| Question | Field | Notes |
|---|---|---|
| What does the block cost to execute? | `ReplayWork` | SlotReplay plus streamed execution before it (`StreamingExecution.TxLoopBeforeReplay`). Same meaning in catch-up and at the tip, so compare this across runs and machines, with CU/ns. Not CPU time: groups can overlap. |
| How far behind the network do we finish? | `FullToReplayed` and `TipPath` | From the block's last shred to its replay result reaching consensus. This sets vote timing. |
| How fast does catch-up get through blocks? | `SinceLastRow`, or backtest's `sec_per_slot` | Wall time per block including loop overhead. `LoopSourceWait` is time spent waiting for the next block (repair during a live catch-up). |

Do not compare SlotReplay at the tip with SlotReplay from a corpus replay:
with streaming execution most of a tip block runs before SlotReplay starts.

## The tip path

For every block that arrived as shreds, `TipPath` divides the latency into
consecutive intervals measured in the node from wall-clock instants:

| Interval | From, to | Owner |
|---|---|---|
| `ReceiveToFull` | the kernel received the shred that completed the block, to the block assembled full | Mithril: socket queueing, shred checks, FEC recovery, assembly |
| `FullToReady` | full, to decoded with transaction signatures verified | Mithril |
| `WaitParentArrival` | ready, to the parent's last shred arriving | network |
| `WaitParentReplay` | to the parent's replay result | Mithril (the parent's replay) |
| `LoopUntilAdmitted` | to the replay loop admitting this block | Mithril: the parent's post-replay tail, this block's streamed execution, other loop work |
| `WaitUnclassified` | ready to admitted, when the parent's instants are unknown (first block, fork switch) | either |
| `AdmittedToReplay` | admitted, to SlotReplay's start | Mithril |
| `ReplayToHandoff` | SlotReplay's start, to the result reaching consensus | Mithril |
| `Unattributed` | whatever the instants could not place | none; nonzero means a missing instant |

`FullToReplayed` equals the sum of every interval except `ReceiveToFull`,
which lies before it. `ShredReceivedNanos` is the kernel receive time itself.

Kernel receive times come from `SO_TIMESTAMPNS` on Linux and are recorded for
the shred whose arrival completed the block; elsewhere, and for a block
completed by recovery or a hydrated spool, `ReceiveToFull` is absent. The
breakdown inside SlotReplay is in the row's other fields (`ProcessBlock`,
`LoadBlockAccounts`, `TxLoop`, `BankHash`, `ReplayStages` and the rest), and
the ingest stages in `TurbineIngress`.

## Runtime interference

`RuntimeInterference` covers the same window as `SinceLastRow`, read from Go's
`runtime/metrics` once per row:

- `GCCycles`, `GCPauseCPU` (stop-the-world CPU time over all Ps) and
  `GCAssistCPU` (mutator time assisting the GC).
- `GCPauseMax` and `SchedLatencyMax`: upper bounds of the longest
  stop-the-world pause and the longest wait of a runnable goroutine for a
  thread, from the runtime's histogram buckets. `SchedLatencyOver1ms` counts
  waits of 1 ms or more.
- `KernelShredDrops`: packets the kernel dropped on shred receive sockets
  because their buffers were full (`SO_RXQ_OVFL`, Linux only). These are
  process-wide counts, not per block.

## The report

```bash
mithril timings tip /path/to/replay_timings.jsonl --top 10
```

It reads every row that has a tip path (a slot replayed again after a fork
switch counts once, by its last row) and prints, per interval, its share of
total `FullToReplayed` and its mean, p50, p90, p99 and max per block in
milliseconds; the time Mithril controls (everything except
`WaitParentArrival`); `ReplayWork` with average consumed CU per block and
CU/ns; runtime interference; and the slowest blocks by controlled time with
their largest intervals.

## Measuring well

- **Catch-up:** `mithril backtest` on a corpus reproduces it exactly (every
  block local and complete). Run an A/A first to know the noise floor, report
  all three numbers with warm-up excluded, and state cache state, prefetch
  workers and CPU pinning.
- **Tip:** only a live run shows it today. Keep runs short (30 to 60 minutes is
  enough for distributions) and report the tip path at p50, p99 and max with
  `ReplayWork` and CU/ns. Different runs replay different blocks, so two
  builds cannot be compared block for block.
- Per-transaction phase timers cost about 0.9% of SlotReplay; the fields on
  this page are per block and cost far less.
