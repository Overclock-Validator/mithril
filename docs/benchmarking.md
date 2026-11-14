# Benchmarking replay

A reference for measuring Mithril's replay: the tools, what each recorded
number means, what to look at in a result, and how to compare with another
client or another machine.

## Tools

| Tool | Use it for | Reference |
|---|---|---|
| `mithril backtest` | Repeatable replay of a shredcap capture from a snapshot or AccountsDB, with a summary in Firedancer's format | [backtest.md](backtest.md) |
| `mithril corpus import-shredcap` / `export-shredcap` | Moving the same blocks between Mithril and Firedancer | [replay-corpus.md](replay-corpus.md) |
| `mithril run --offline` | Replaying a local shred spool with a full node configuration, no network | [replay-corpus.md](replay-corpus.md) |
| `mithril timings tip` | Breaking down live replay latency from a run's timing rows | [below](#the-tip-path) |
| `scripts/run-commits.py` | Building several commits and replaying the same slots with each | the script's `--help` |
| `scripts/replay_timings_viewer.py` | Charting a run's timing rows (marimo notebook) | the script |
| Go benchmarks and the interpreter suites | Interpreter and runtime work in isolation | [sbpf-interpreter-benchmarks.md](sbpf-interpreter-benchmarks.md) |

Every replay writes one JSON row per block to `replay_timings.jsonl` in the
run's log directory. Everything below reads those rows.

## Three questions, three numbers

"How long does a block take to replay" means one of three things. Pick the
number for the question; they are not interchangeable.

| Question | Number | Notes |
|---|---|---|
| What does the block cost to execute? | `ReplayWork` | SlotReplay plus streamed execution that ran before it (`StreamingExecution.TxLoopBeforeReplay`). Means the same in catch-up and at the tip. A sum of wall intervals, not CPU time. |
| How far behind the network does replay finish, and when does the vote leave? | `TipPath.LastShredToReplayed`, then the block's vote stages | From the block's last shred to its replay result reaching consensus, and on to its notarize vote being handed to QUIC for the first peer. Live runs only. |
| How fast does catch-up get through blocks? | `SinceLastRow`, backtest `sec_per_slot` | Wall time per block including the loop between blocks. |

SlotReplay starts when replay takes the complete block. With streaming
execution (on in backtest and at the tip) part of the block has already run
by then, so never compare a tip block's SlotReplay with a corpus block's.

## The timing row

Intervals are `{"Count", "SumNanoseconds"}` per block. The groups below add up
exactly where marked "closure"; nested timers inside them do not add up and
must not be summed with their parent.

**Size of the work.** `Slot`, `ComputeUnitsConsumed` (exact consumed CU, the
normalizer), `Transactions`.

**The block (closure).** `SlotReplay = PreprocessBlock + ProcessBlock +
PostProcessBlock`. Nested inside: `LoadBlockAccounts` (with its breakdown in
`AccountLoader`), `TxLoop`, dependency planning, `BankHash` and the lt-hash
steps, `BlockUpdateAccounts`, transaction status work, vote rewards, footer
verification, and `ReplayStages` (the watchdog's disjoint stages). `ReplayWork`
adds the streamed execution before SlotReplay.

**The loop (closure).** `SinceLastRow = LoopSourceWait + LoopBeforeReplay +
SlotReplay + LoopAfterReplay`. `LoopSourceWait` is time spent waiting for the
next block (repair or the network in a live catch-up).

**Ingest.** `TurbineIngress`: shred collection (first to last shred), the
completion queue, block decode, transaction parse and signature checks, replay
admission, and `FullToReady`. The `Early*` fields overlap reception and are not
additive.

**Streaming.** `StreamingExecution`: groups and transactions executed before
the block was complete, `TxLoopBeforeFull` (finished before the last shred),
`TxLoopBeforeReplay`, discards and their reason, and the open timeline
(header ready, parent full, parent replayed, opened, first group, full,
finalize).

**Storage.** `FoldOverlap` (background folds running during this block, which
compete for CPU, the index and the disk), `UnrootedRetainedBytes`,
`UnrootedHeldSlots`.

**Per-transaction phase timers** (`IxLoop`, `CalcAndDeductFees`,
`ExecIxNativeProgram*` and others) cost about 0.9% of SlotReplay. They are off
in backtest (`--phase-timers` turns them on) and on in `mithril run`.

### The tip path

For every block that arrived as shreds, `TipPath` splits `FullToReplayed` into
consecutive intervals that add up to it (closure):

| Interval | From, to | Owner |
|---|---|---|
| `ReceiveToFull` | the kernel received the shred that completed the block, to the block assembled full (before `FullToReplayed`) | Mithril: socket queue, shred checks, FEC recovery, assembly |
| `FullToReady` | full, to decoded with signatures verified | Mithril |
| `WaitParentArrival` | ready, to the parent's last shred arriving | network |
| `WaitParentReplay` | to the parent's replay result | Mithril (the parent's replay) |
| `LoopUntilAdmitted` | to the replay loop admitting this block | Mithril: the parent's post-replay tail, this block's streamed execution, other loop work |
| `WaitUnclassified` | ready to admitted when the parent's instants are unknown (first block, fork switch) | either |
| `AdmittedToReplay` | admitted, to SlotReplay's start | Mithril |
| `ReplayToHandoff` | SlotReplay's start, to the result reaching consensus | Mithril |
| `Unattributed` | whatever the instants could not place | nonzero means a missing instant |

Alongside:

- `LastShredToReplayed`: the headline tip number, from the last shred to the
  replay result reaching consensus. It starts at the kernel receive time when
  known (`ReceiveToFull + FullToReplayed`) and at full assembly otherwise.
- `ShredReceivedNanos`: the kernel receive time (Linux `SO_TIMESTAMPNS`; zero
  elsewhere, and for blocks completed by recovery or from the spool), and
  `HandedOffNanos`, the hand-off instant.
- Where the block's shreds came from (`DataShreds`, `RepairedShreds`,
  `RecoveredShreds`), `TipLagSlots` (how far the newest received data shred
  was ahead at hand-off) and `ForkSwitch`.

### The vote

In validator mode each vote passes three stages, recorded in `VoteEvents`:

| Stage | When |
|---|---|
| `queued` | signed, recorded in vote history and queued for broadcast |
| `first_send` | the first peer's copy handed to QUIC |
| `fanout_done` | every connected peer's copy sent or failed; `Peers` counts those sent (zero means the vote reached no peer) |

A row's `VoteEvents` holds the stages reached since the previous row, for any
slot, so a block's own vote usually appears in the next row. `mithril timings
tip` joins each block's notarize vote to the block by slot and reports
`HandoffToVote` (hand-off to queued), `VoteQueuedToSend`,
`LastShredToVoteSent` (last shred to the first copy handed to QUIC) and
`LastShredToFanout`. Handing a datagram to QUIC is the last point Mithril
controls; time on the wire and at the peer is not measured.

### Runtime and host interference

`RuntimeInterference` covers the same window as `SinceLastRow`:

- Go runtime (`runtime/metrics`): `GCCycles`, `GCPauseCPU` (stop-the-world CPU
  over all Ps), `GCAssistCPU`, and upper bounds from histogram buckets on the
  longest GC pause (`GCPauseMax`) and the longest wait of a runnable goroutine
  (`SchedLatencyMax`, with `SchedLatencyOver1ms`).
- Process (`getrusage`): involuntary and voluntary context switches, major and
  minor page faults.
- Host (Linux PSI, system-wide): `PressureCPU`, `PressureIO`,
  `PressureMemory`, the time at least one task stalled on each.
- `KernelShredDrops`: packets the kernel dropped on shred sockets because their
  buffers were full (Linux `SO_RXQ_OVFL`).

```bash
mithril timings tip /path/to/replay_timings.jsonl --top 10
```

prints each tip interval's share of `FullToReplayed` with mean, p50, p90, p99
and max per block; the time Mithril controls (everything except
`WaitParentArrival`); `ReplayWork` with average CU per block and CU/ns; last
shred to replay done and to the vote; shred sources, tip lag and fork
switches; interference; and the slowest blocks with their largest intervals
and anything unusual about them.

## Running a comparison

- **Run an A/A first.** Two or more runs of the same binary and settings show
  the noise floor. A difference smaller than the A/A spread is not a result.
- **One binary, switches for the arms**, where the change allows it (an
  environment variable or flag). Separately built binaries can differ by
  about 2% from code placement alone (the interpreter loop's address mod 64);
  check with `go tool nm` and pad to match when you must compare two builds.
- **Interleave the arms** (for example O1 O2 U1 U2 U2 U1 O2 O1) so drift in
  cache state or background load does not favour one side.
- **Same blocks, same start.** Backtest rewinds the store each run; offline
  replay needs the same checkpoint. Exclude a warm-up (backtest's default is
  30 blocks) from both arms, and the same `--measure-from` if you use one.
- **Pin and fix the environment.** CPU set (`taskset`), `GOMAXPROCS`,
  `GOMEMLIMIT`, `setarch -R` to fix the address layout, the same prefetch,
  fold and interpreter settings. Record them with the commit; backtest prints
  its settings block for this.
- **Warm or cold, on purpose.** Repeat runs replay with the OS page cache
  warm; the first run after building an AccountsDB is colder and slower. Say
  which you measured.
- **Check the machine is idle**, and that nothing else (another benchmark,
  background writes) runs during the measurement. PSI and the interference
  fields show it after the fact.
- **Size runs to the question.** A corpus A/B with a few repetitions resolves
  small differences; a long live run does not. Live runs are for tip
  behaviour, not for comparing two builds.

## What to look at

**Always, per arm and per interval** (SlotReplay, TxLoop, ReplayWork; the tip
path for live runs):

- Average consumed CU per block, and the mean and total in ms.
- CU/ns as total CU over total nanoseconds (a ratio of sums, never a mean of
  per-block ratios). Higher is better. It makes blocks of different size
  comparable; it does not make different workloads equivalent, and it does
  not measure CPU instructions.
- p50, p90, p99 and max, with the number of blocks and runs.
- Run-to-run spread of the mean and of p99, next to the effect.
- That every block's bank hash matched and consumed CU is identical between
  arms: the same CU means the same work was done.

**Tails.** Join p99 and max blocks across runs by slot. A tail that recurs on
the same slots is the workload (a heavy block); one that moves between runs
is the machine (GC, I/O, scheduling, folds). For recurring tails, read the
block's own breakdown: `LoadBlockAccounts` and its `AccountLoader` split,
program loading, `ReplayStages`, `BankHash`, `BlockUpdateAccounts`. For moving
tails, read the interference fields and `FoldOverlap` for that row.

**CU/ns outliers.** Sort blocks by ReplayWork per CU. Low CU/ns blocks spend
their time on work CU does not price: account loading (many or large
accounts, cold reads), program loading and verification, large account
writes, hashing, signature checks. High CU/ns blocks are compute-bound and
are where interpreter and runtime work shows. Compare the share of each phase
in the low and the high group; the phase that grows in the low group is the
lead.

**Where the time goes.** The share of SlotReplay in each nested phase, over
all measured blocks, says where an optimization can matter. A change to a
phase worth 2% of SlotReplay cannot move the total by more than 2%.

**At the tip.** `LastShredToReplayed` and `LastShredToVoteSent` at p50, p99
and max; the time Mithril controls versus `WaitParentArrival`; which interval
dominates the slowest blocks; `ReceiveToFull` (a growing value means ingest is
falling behind, not the network); votes that reached no peer; kernel drops;
repaired and recovered shreds in slow blocks; tip lag.

## Comparing with other clients and other hardware

**Use the same blocks.** Export a corpus with `mithril corpus export-shredcap`
and run `firedancer-dev backtest` on it, or import a Firedancer capture. Live
numbers from different runs or hosts are different blocks at different
network positions and cannot be compared block for block.

**Compare like spans.** `BACKTEST_SUMMARY` uses Firedancer's field names and
spans (`replay_s`, `slot_exec_s`, `sec_per_slot`, `tps` and the rest);
[backtest.md](backtest.md) lists what still differs. Firedancer's per-slot
time runs from preparation to completion; the nearest Mithril per-block
figure is ReplayWork, not SlotReplay. Firedancer keeps accounts in memory and
loads the snapshot every run; Mithril reads an on-disk AccountsDB, rewound
between runs, through its caches.

**State the hardware**, since it decides the result as much as the code: CPU
model, cores and threads used, SMT, clock and governor, memory size and
speed, storage and filesystem, kernel. Report thread counts on both sides
(Firedancer's exec tiles against Mithril's `--txpar`). Where possible, run
both clients on the same machine with the same core count.

**What carries across machines and what does not.** CU/ns and per-block
latency move with the CPU; their ratio between two clients on the same
machine is the comparable figure. Throughput (`sec_per_slot`) and tail
latency depend on core count and storage as much as on the client. A result
on one machine (for example a gain on Zen 4 and none on Zen 5) is a result
for that machine.

**How the other clients report live replay**, for mapping their metrics to
these fields:

| Concept | Agave | Firedancer | Mithril |
|---|---|---|---|
| Per-slot replay | `replay-slot-stats` (`replay_total_elapsed`, `confirmation_time_us`, `replay_time`, `bank_complete_time_us`) | `block_completed` event: preparation begin, first transaction scheduled, last finished, frozen, completion | SlotReplay, ReplayWork and nested phases |
| Shred arrival | first-shred and slot-full times at blockstore insert (wall clock, ms) | per-FEC first and completed times (CPU tick counter at the shred stage) | `ShredFirstNanos`, `ShredFullNanos`, kernel receive time of the completing shred |
| Last shred to replay done | joined from separate datapoints by slot | joined from separate events by slot | `LastShredToReplayed` and `TipPath` |
| Vote out | Alpenglow: notarize vote sent, relative to the slot's first event | vote broadcast start and done times | `VoteEvents` stages, joined by slot |
| Loop overhead | `replay-loop-timing-stats` | per-tile busy, idle and backpressure time | the loop closure fields |
| Interference | `/proc/net/snmp` receive errors, allocator stats | per-thread wait, context switches, faults, PSI | `RuntimeInterference` |

## Pitfalls

- Summing nested timers with their parent, or SlotReplay with ReplayWork.
- Averaging per-block CU/ns instead of total CU over total time.
- Comparing arms with a different warm-up, start slot or number of blocks.
- Reading a difference smaller than the A/A spread as a result.
- Comparing SlotReplay across tip and corpus runs.
- Comparing two separately built binaries without checking code placement.
- Forgetting that the first run after a store build is cold.
- Leaving per-transaction phase timers on in one arm and off in the other.
