# Genesis replay checkpoints and resume

The offline genesis replay path can now checkpoint a completed child bank to
Pebble AccountsDB, close, reopen and continue from the next slot. The native-producer
fixture checkpoints its independent non-voting receiver at slots 2 and 4. After
restarting at slot 2, replay matches the uninterrupted native producer and pinned
Agave through slot 4, including every live account and the bank hash.

The short-epoch fixture also replays 66 slots through two 32-slot epochs, verifies
both boundaries against pinned Agave, and reopens checkpoints before and after
reward distribution. Add this optional setting to a genesis TOML for faster tests:

```toml
test_slots_per_epoch = 32
```

The default remains 8,192 slots. The override accepts 32 through 8,192, keeps the
leader schedule offset equal to the epoch length, and preserves the fixed feature
set, non-warmup schedule and slot duration. The genesis bytes/hash and resolved
summary record the actual schedule; both implementations execute that same genesis.

```sh
go test ./pkg/replay -run '^TestGenesisFastEpochCheckpointAgave$' -count=1 -v
go test ./pkg/replay -run '^TestGenesisNativeProducerCheckpointAgave$' -count=1 -v
go test ./pkg/replay -run '^TestGenesisCheckpoint' -count=1 -v
```

To compare with the live pinned helper, set `MITHRIL_AGAVE_ORACLE` as described in
[genesis-native-production.md](genesis-native-production.md). The checkpoint test
uses the same independently produced native entries and oracle expectations.
Agave is only a development dependency.

## Reusable API and scope

`replay.OpenGenesisReplay(ctx, accountsPath)` owns the shared AccountsDB exclusive store
lock. `ReplayBlock(ctx, receivedBlock, workers)` executes a consecutive block with
verified transaction signatures, signed block identity and bank-hash footer. It
derives the execution parent from its own genesis or reopened bank. It invokes the
existing `handleEpochTransition` and shared reward-distribution flow before
`ProcessBlock`, with no local-producer adoption path.
`LeaderForSlot(slot)` derives signature-verification schedules from the session's
current/future computed stakes and can be installed on the UDP receiver.
Before execution it verifies the native entry hash chain, exact transaction
coverage and one ending Alpenglow tick (`num_hashes=1`). The earlier sleep-mode
oracle's zero-hash tick fixture remains a separate bank-API test.

`CheckpointTrusted(ctx)` explicitly selects the completed local branch as an
offline checkpoint. This selection is recorded as `trusted-offline-replay`; it
does not assert consensus finality or create a certificate. `NextReplaySlot()`
reports the next position. `Close()` releases ownership and discards the
uncheckpointed account tail. As with existing replay, the caller initializes the
worker account arenas and runs only one execution owner per process; the session
serializes its own methods. Returned slot contexts are for inspection, not mutation.

The adapter supports consecutive blocks across epochs. It preserves the normal
replay rules: checkpoint the parent explicitly before account-wide boundary scans;
hold checkpoints while `EpochRewards.Active` is true; after distribution completes,
checkpoint and resume from the exact next slot. A restart inside the reward window
selects the pre-boundary checkpoint and recalculates the partitions. No reward spool
is treated as durable progress, and stale files for a recalculated partition are
removed before producing its replacement.

This is a Go API and integration harness; `run --bootstrap genesis` remains
disabled. Networking setup, live consensus, validator launch and snapshot production
are separate milestones. The receiver does not vote. Lightbringer is unchanged.

## Durable boundary

The implementation reuses `alpenglow-dev`'s Pebble `CommitBatch`, manifest format and file allocation.
A shared `accountsdb.lock` guard spans initialization and open-database ownership.
Genesis recovery validates every decided manifest and segment before ordinary
recovery can remove orphans. It does not introduce another account index or commit protocol.

1. Capture the completed bank and scan the current account view. a pinned Pebble iterator's ordered
   enumeration is merged with the bounded speculative write set. The scan checks
   AccountsLtHash and records capitalization, data length, live account count and
   a canonical SHA-256 digest of all account fields, including rent epoch.
2. Write the immutable transaction-status sidecar and verify its root, lineage,
   coverage and checksum. It retains duplicate-transaction protection on restart.
3. Publish the schema-7 compatibility fence before writing child account state.
   Older binaries reject this schema. The immutable slot-0 genesis files and
   marker remain the origin anchor, never the current child checkpoint.
4. Commit account deltas, bank hashes and the complete checkpoint envelope through
   Pebble AccountsDB. Its fsynced manifest is the durable decision; recovery finishes any decided
   index publication. No subsequent state-file update is needed to select the tip.

Checkpoint envelope version 2 includes the pinned genesis/runtime profile,
computed current/future and retained historical epoch stakes, and versioned vote
states for delayed commissions. Reopen replaces historical process caches with
this bank's state, including an explicitly empty future stake set. Up to five
stake generations and three commission-history generations are retained. Old
version-1 checkpoints remain readable in epoch 0; old readers reject version 2
before Pebble AccountsDB recovery cleanup.

The envelope also contains the child's bank and parent hashes, consensus block ID and chained shred root;
fees, vote timestamps, recent/evicted blockhashes, slot hashes, clock, tick height,
block height, capitalization, account-data length, AccountsLtHash and exact
transaction count. Account bytes, including the nanosecond clock, live in Pebble AccountsDB.
These identities remain distinct from the genesis hash and slot-0 certificate.

Reopen validates manifest metadata, the complete fold/segment chain and referenced
status files before recovery cleanup. It restores all manifest bank hashes even
when their separate WAL was lost after the account-index commit, then verifies the complete recovered account state, bank-hash formula,
parent hash, sysvars and runtime profile. A corrupt decided checkpoint fails;
startup does not silently choose genesis or another checkpoint. Ordinary snapshot
resume checks remain unchanged. Generic state loading cannot mistake the
schema-7 origin marker for the current replay position.

Cancellation is honored before starting Pebble AccountsDB's commit. Once it starts, the commit
must finish or report its durability decision; cancellation cannot cut an fsync
sequence in half. An uncertain commit fences the session until close/reopen.
Unselected immutable sidecars can remain after interruption; they do not select
a checkpoint and this milestone adds no new retention/garbage-collection policy.

## Validation

- A 32-slot schedule and synthetic native entries, signed shreds, real UDP ingress,
  and independent replay against Agave through slot 66. Every account, bank hash,
  AccountsLtHash, capitalization, recent blockhash and epoch stake generation is
  compared. The transactions are system transfers; no votes are submitted.
- Checkpoints/reopens at 31, 33, 63, 65 and 66. Closing at active boundary slots 32
  and 64 reopens at the boundary and re-executes to the same hash; process caches
  are cleared between opens. Empty reward partitions finish at 33 and 65.
- The fixture's 2-SOL vote account pays its admission ticket at 32. At 64 it is
  underfunded for future admission, so epoch 3 has no leaders. The persisted empty
  future set survives reopen instead of reverting to the funded genesis seed.
- The pinned revision predates the newer epoch-inflation metadata PDA and uses
  a zero reward budget for zero vote credits. Those profile-specific differences
  are tested without changing the later deployed profile's reward semantics.

- Native production → signed UDP ingress → independent replay, checkpoint at 2,
  reopen at 3, and continued equality with the producer and pinned Agave at 4.
- Execute slot 3 without checkpointing, close, reopen at 3, and re-execute it to
  the same bank hash. Deliberately stale process transaction counts are replaced
  by the resumed count before execution.
- Full account and bootstrap metadata verification after reopen; restored fees,
  vote timestamps, epoch stakes and transaction-status history; duplicate transfer
  rejection after restart.
- Cancellation before capture, after sidecar creation and before commit; actual
  subprocess exits before and after the commit, without calling `Shutdown`.
- Invalid versions/profiles, parent/hash/fee/tick/position metadata, incomplete
  status references, missing/corrupted sidecars and corrupt decided manifests.
- Account corruption in rent epoch, which is outside AccountsLtHash, and missing
  ending ticks or entry hashes that do not extend the selected parent.
- Pebble AccountsDB exclusive ownership, schema fencing and refusal of unsupported node launch.

The existing Pebble AccountsDB crash-matrix tests cover interruption inside `CommitBatch` itself.
The new subprocess tests cover the genesis adapter's publication boundaries.
See [native-genesis-validation.md](native-genesis-validation.md) for test outcomes
and existing full-suite failures.

## Next steps

This checkpoint deliberately performs a full streaming integrity scan. It is a
correctness milestone, not a claim of mainnet checkpoint throughput. Account and
bank metadata now share one durable boundary that future full/incremental snapshot
export can consume. Snapshot generation still needs generation pinning/retention,
archive serialization, incremental-base identity and concurrent replay validation.

Further coverage should include nonzero reward credits and payouts, multiple
validators, skipped slots, and persistence/reconstruction inside a multi-partition
reward window. The current boundary fixture has no vote credits and exercises the
empty-partition lifecycle; it does not establish nonzero payout parity.
Live cluster startup also needs a consensus-based checkpoint selection policy in
place of the explicit offline trust operation. Snapshot export must preserve the
same computed epoch state and respect the existing reward-window hold until active
partition reconstruction is implemented.
