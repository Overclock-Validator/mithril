# Fork ownership and epoch rewards

A reward distribution now has an immutable partition plan and bank-local
progress. Reading a partition does not consume its spool file. Replay and
production use the same distribution function, choosing the partition from the
selected parent's EpochRewards sysvar and the child's block height. Skipped
slots do not consume partitions. Production applies the partition before
transactions; ordered replay independently validates its receipt and advances
capitalization once.

Replay observes the inactive EpochRewards sysvar in a successfully executed
bank's immutable snapshot, with zero partitions remaining. It remembers that
bank's slot and the exact distribution descriptor. Only applying a successful
durable fold through that slot retires the descriptor. Later bank observations
do not move the completion slot forward. A new descriptor/epoch invalidates the
old evidence. Durable completion retires the plan; an abandoned boundary plan
is discarded only after its retained banks are removed. Both operations join
active plan readers before deleting files. Each calculation owns a separate
directory, so sibling boundary calculations cannot overwrite one another.

## Safety and recovery contract

- Completion in memory, certificate finality, and submitting a fold do not
  authorize retirement. Failed folds leave the durable watermark unchanged.
- Retained banks carry a copy of reward progress, completion evidence, feature
  state, capitalization and promotion limits. Their epoch-stake payloads and
  leader schedules are immutable shared generations. A fork restores the exact
  parent's metadata along with its account overlay and immutable sysvars.
- Accepted vote-cache changes have a per-bank undo journal. Speculative leader
  and streaming banks keep their changes private until ordered acceptance.
  The journal retains only changed keys and prunes after successful durability.
- The initial durable parent and each subsequent fold retain a metadata anchor,
  allowing ordinary forks to return to the root without rebuilding replay.
  Missing parent state and contradictions at/below durability still require
  checkpoint recovery. Consensus signing history is never rewound.
- Turbine reception belongs to the outer node lifecycle. Checkpoint recovery
  pauses delivery, joins the old prefetch hook, and resumes from the new parent
  without rebinding the socket. Decoded buffers remain bounded; evicted blocks
  can use retained spool packets or network repair. Pruned history still needs
  repair and is not guaranteed to be available locally.
- Completion evidence is replay-thread-owned and process-local. It does not
  change checkpoint formats, signing reservations, persisted vote history,
  clean-shutdown rules or restart authorization. Restart retains the existing
  persisted EpochRewards validation and reconstructs non-durable reward plans.

## Epoch work and remaining limits

Epoch processing reads and decodes stake accounts once. A temporary, fixed-width
112-byte delegation stream supplies the new-epoch stake aggregation and reward
points pass. Its 256 KiB write buffer is bounded; the temporary file is removed
after the transition. This preserves the completed-epoch StakeHistory ordering
and reward arithmetic without keeping all decoded stake accounts in RAM.

The implementation still requires a durable exact parent for epoch-wide scans.
Leader production across the epoch boundary itself remains disabled. Leaders
can produce during partition distribution once replay has calculated the plan.
Locally produced reward slots currently repeat partition reads in ordered replay
to validate their receipt; live performance of that path must be measured.

## Incident motivating the change

On Zen 5, distribution completed at slot 3,942,001. At a later parent-linked
switch, the durable checkpoint was already 3,944,067; child 3,944,076 selected
parent 3,944,073, abandoning the suffix from 3,944,074. The remaining descriptor
forced the rewards-window fallback even though completion was below the root.
Checkpoint recovery re-fetched previously received blocks, with logged waits
of 2.739 seconds and 0.967 seconds. A buffered 665-transaction block waited
3,613.510 ms for replay admission and then executed in 7.520 ms.

These are incident observations, not a before/after benchmark or a measurement
of checkpoint encoding/fsync time. Thirteen observed FAST aggregates omitted
our vote during the recovery interval; that does not prove absence from every
FAST aggregate or a single cause for all thirteen omissions. No live latency
improvement is established until a comparable switch exercises the new path.

## Validation

`rewards_retirement_test.go` covers active/missing bank state, unknown completion,
the exact durable boundary, later-bank observations, generation changes, failed
and successful folds, and an exact-parent unwind after retirement (including
account values, resume state and immutable rewards sysvars). Guards still
require fallback when fork metadata is unavailable, cache mutations were not
journaled, or parent snapshots are invalid.

Additional tests cover reward-plan reuse and retirement, leader/replay account
and bank-hash parity, accepted vote-cache undo, restored epoch generations,
initial root anchors, stale local-bank rejection, paused reception bounds and
prefetch-hook shutdown races. These are software tests, not mainnet qualification.

The reproducible `BenchmarkEpochStakeScanReuse` compares three account-store
passes with one pass plus the temporary stream, including reward calculation
and cleanup. On a warm 16,384-account fixture with GOMAXPROCS=2 on a Ryzen 9950X,
the three-sample median was 27.52 ms versus 15.98 ms, with 49.4 MB versus 27.9 MB
allocated per iteration. This does not establish the improvement for a full
mainnet boundary. After deployment, compare boundary phase timings, replay lag,
`unwind_ms`, checkpoint-fallback counts and leader outcomes across matched runs.
