# Retiring durable rewards bookkeeping

A completed partitioned-rewards distribution used to leave its in-memory
descriptor alive for the rest of the replay attempt. The fork-switch guard
rejects any such descriptor because account-overlay unwind cannot restore the
consumed spool or its distribution counters. This is necessary while completion
is speculative, but unnecessarily forces checkpoint replay after completion
has become durable.

Replay now observes the inactive EpochRewards sysvar in a successfully executed
bank's immutable snapshot, with zero partitions remaining. It remembers that
bank's slot and the exact distribution descriptor. Only applying a successful
durable fold through that slot retires the descriptor. Later bank observations
do not move the completion slot forward. A new descriptor/epoch invalidates the
old evidence; missing sysvars or unknown completion retain the old fallback.

## Safety and recovery contract

- Completion in memory, certificate finality, and submitting a fold do not
  authorize retirement. Failed folds leave the durable watermark unchanged.
- Active distribution and completed-but-not-durable distribution retain the
  existing rewards guard. No spool reconstruction or rewards rollback is added.
- After retirement, in-memory switches still require the existing epoch,
  vote/stake-cache, parent-context, sysvar and transaction-status checks.
  Switches at/below the durable watermark still require durable recovery.
- Completion evidence is replay-thread-owned and process-local. It does not
  change checkpoint formats, signing reservations, persisted vote history,
  clean-shutdown rules or restart authorization. Restart retains the existing
  persisted EpochRewards validation. No extra file or disk sync is introduced.

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
account values, resume state and immutable rewards sysvars). Existing unwind
tests still require fallback for zero-remaining bookkeeping without retirement,
cross-epoch switches, dirty vote/stake caches and invalid parent snapshots.

Full replay/rewards race suites passed locally and in the combined native
build; native node recovery/checkpoint race tests, vet and validator build also
passed. These are software tests, not mainnet power-loss qualification.
