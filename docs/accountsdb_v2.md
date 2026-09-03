# AccountsDB V2 account index

AccountsDB V2 is Mithril's Pebble-free account-location index. It combines a
compact immutable StreamHash base with exact, bounded mutable state and rolls
individual shards forward without stopping readers.

Pebble is still used for `bankhash_db`; it is not on the account-location lookup
or update path.

## Design goals

- Keep account loading off an LSM read path.
- Make large block lookups expose memory-level parallelism.
- Bound Go-heap use independently of the number of accounts in the snapshot.
- Preserve exact newest-wins and tombstone semantics.
- Make every durable transition recoverable after process or power failure.
- Reclaim immutable files only after readers of their generation have drained.

## Layout

The key space is split by the high bits of SipHash-1-3 over the complete
32-byte Solana public key. A cryptographically random, non-zero 128-bit routing
key is generated with a new root, persisted in the root catalog and every base
header, and retained unchanged by every successor publication and rewind. The
default, persisted shard count is 1,024. This keyed routing keeps expected load
balanced while preventing valid, remotely chosen raw-key prefixes from forcing
point and batch traffic onto one shard. Changing either the shard count or
routing key requires rebuilding the complete index.

Each shard has three lookup tiers:

1. An exact active/frozen Go-map head for the newest WAL-backed changes.
2. An optional exact immutable delta checkpoint. Its StreamHash table points to
   fixed-width records containing the complete key, live locator or tombstone.
3. An immutable StreamHash base. It stores a six-byte packed appendvec locator
   and a one-byte fingerprint. A base hit is only a candidate: AccountsDB always
   compares the complete key in the appendvec before returning the account.

The base's packed locator is a 24-bit stable extent ordinal plus a 24-bit offset
in eight-byte units. One append-only extent catalog maps ordinals to
`(slot, file, 128 MiB base offset)`. This avoids storing a full 24-byte
`AccountIndexEntry` for every snapshot account.

Every base shard also has a sorted, exact `.scan` sidecar containing the full
key and packed locator. It is streamed, not mapped into the Go heap, and exists
for deterministic rolling rebuilds and enumeration. Because keyed routing
deliberately removes the raw-prefix/shard correspondence, an exact raw-prefix
scan merges all shards. A context-aware single-scan gate bounds that uncommon
maintenance/rent path to at most `ShardCount` sidecar descriptors (1,024 by
default), one 16 KiB read buffer and one merge-heap item per shard (16 MiB of
base buffers by default), plus 32 bytes of sort scratch per captured hot key.
The scan checks `RLIMIT_NOFILE` with explicit descriptor headroom before it
opens sidecars and fails with an actionable error instead of partially opening
a scan. Sidecars remain off the normal lookup path.

## Reads

Point reads check the exact mutable state before the base. Large reads first
deduplicate keys, pin one coherent mutable epoch and immutable root generation,
then probe independent keys over static worker ranges. Candidate appendvec
locations are sorted by file and offset, adjacent reads are coalesced into
bounded `ReadAt` ranges, and each base candidate receives a full-key check.
Both point and batch routing use the persisted keyed hash, so deliberately
skewed raw public-key prefixes do not create a predictable hot shard.

The pin is important. A reader sees one root generation even if a checkpoint or
rebase publishes concurrently. Replaced mappings are closed and deleted only
after the last reader and mutable owner releases them.

## Writes and bounded state

One CRC32C-framed global WAL gives each bounded cross-shard apply an atomic
publication unit; ordinary folds and rewinds fit in one such frame. A
successful durable apply writes and syncs the complete frame before exposing
its mutations in RAM. Recovery validates sequence,
length, flags, record structure and CRC before applying a frame. Only an
ordinary incomplete final write is truncated; corruption inside the journal or
inside its compact-state prefix fails closed.

A valid slot whose changed-key set exceeds the physical 64 MiB WAL-frame limit
is encoded as several bounded, idempotent frames. The segment manifest is the
durable decision record and only the final frame advances fold metadata. While
the frames are being synced, `pendingFold` supplies point and batch readers with
the complete logical changed-key view; range enumeration holds the writer fence
only long enough to capture a coherent snapshot. A crash or any post-manifest
failure leaves the decided manifest in place, fences the current process, and
startup replays the whole assignment transaction idempotently. No partial
logical fold is exposed as a successful commit.

The mutable head is bounded by both a key limit and conservative bytes-per-key
accounting. Busy or old shards are frozen and checkpointed. Under global memory
pressure, the scheduler selects the largest eligible shards and runs a bounded
number of independent checkpoint builds concurrently. Writers wait for a
progress epoch only when admitting the next complete frame would exceed the
configured envelope. They never evict an exact mutation to make space.

A transient checkpoint failure leaves the original frozen map, WAL cut and
proposed coverage intact. The one maintenance loop retries that exact epoch
with per-shard exponential backoff from one second to one minute, gives due
retries priority over new freezes and never overlaps two builders for a shard.
ENOSPC, quota/resource pressure, transient local I/O, deadline cancellation and
a lost root compare-and-swap are retryable. Durable format/integrity errors,
invalid catalogs and an ambiguous root publication poison the index instead.
If the hot bound is reached while a retry is sleeping, the next write returns a
capacity error containing the checkpoint failure and retry delay; callers are
backpressured without an unbounded blocked goroutine or a retry busy-loop.
Unselected files from a retryable failed attempt are reclaimed before its next
attempt, so a persistent disk-full or lost-CAS condition cannot consume one
checkpoint generation of additional disk per retry.

After a shard's exact checkpoint grows past its rebase threshold, it is merged
with that shard's base into a new StreamHash base. Other shards keep their
existing resources. Base rebases currently serialize because all shards extend
one stable extent-ordinal lineage; checkpoint construction remains concurrent.
Before allocating the next base build, the rebase worker waits (cancellably)
for the prior obsolete base generation to close and be deleted. This gives
rolling bases a hard one-obsolete-generation bound even when a slow reader pins
an old root. A generation's retirement group includes its replaced base shard
and, when the extent lineage advanced, the replaced extent catalog.
`ObsoleteBaseGenerationsPending` is nonzero while that reclamation gate can hold
a rebase; delta artifacts remain governed by their byte budgets.

The WAL is periodically rewritten as a bounded state image. The rewrite keeps
the complete exact hot state, retirement markers and fold metadata, writes and
syncs a replacement, atomically renames it, then syncs the directory.

## Publication and crash invariants

`accounts_index.root` is the only selector for immutable state. It contains a
CRC-protected generation, chain lineage, per-shard logical coverage and the
size/SHA-256 identity of every selected artifact.

`accounts_index_v2.lock` is the persistent store-wide ownership inode. An
initializer and an opened validator hold it exclusively; read-only validation
holds it shared. Startup orphan collection and snapshot replacement also take
the exclusive lock. The file is intentionally never removed during cleanup:
unlinking a live lock would allow another process to lock a different inode
for the same AccountsDB.

Publication always follows this order:

1. Write, sync and close all new immutable data files.
2. Sync their directory.
3. Write and sync a complete successor root catalog.
4. Atomically replace the root selector and sync the AccountsDB directory.
5. Install the same generation for new in-process readers.
6. Retire old resources after all pinned readers drain.

A failure before step 4 leaves unselected orphan files, collected on the next
startup. A failure at or after the root replacement is treated as ambiguous and
poisons the running index; continuing against a different in-memory root would
break the restart contract. The process must restart and select the durable
root.

Fold manifests use the same commit-decided rule: `.manifest.tmp` is disposable,
but a final `.manifest` is never quarantined or deleted merely because it is
unreadable. Recovery first validates every final manifest's canonical identity,
structure, flags and CRC, then validates the contiguous slot/sequence chain and
segment data before mutating the index or reclaiming orphans. Any failure
preserves the evidence and all potentially referenced data.

Each `.stmh` artifact embeds the SHA-256 identity of its exact `.scan` sidecar
and, after the builder has completely validated that sidecar, a receipt for its
Linux device, inode, size, modification time and change time. The root-selected
StreamHash identity authenticates that receipt. On an ordinary Linux reopen,
`O_NOFOLLOW` plus a matching strong file identity reduces sidecar validation to
its fixed header; a missing or stale receipt falls back to a complete semantic,
CRC and SHA-256 pass. Platforms without the strong Linux identity use the full
validation path.

Delta checkpoint identities selected by the root are first matched to their
CRC-protected descriptor and then hashed once, rather than once per trust link.
If `E`, `B`, `D`, `R` and `W` are the total extent-catalog, base StreamHash,
selected-delta, selected-delta-record and WAL bytes (`R <= D`), normal
ready-state validation followed by runtime open performs approximately
`2E + 4B + 2D + R + 2W` logical full-file bytes, plus small headers. The
additional `R` pass proves that every exact-delta record is strictly ordered,
CRC-valid and assigned to the shard selected by the persisted keyed router; at
the default 1 GiB selected-delta budget it is bounded by 1 GiB. Relative to the
earlier immediate two-pass sidecar design, the authenticated receipt avoids
roughly 76 GB of sequential startup reads at one billion accounts.

This fast path assumes the documented process-local storage threat model. A
root/raw-device adversary or storage that can alter bytes without changing inode
metadata requires an integrity facility such as fs-verity or checksummed
storage. Exact scans and rebases still check record structure and CRC while
consuming the sidecar and prove that the strong path identity is stable before
and after use.

Rolling shard bases may bind different prefixes of the shared extent catalog.
Startup and rolling publication collect all required prefix lengths and compute
their SHA-256 bindings in one ascending cumulative pass. Append-only catalog
successors inherit the already verified prefix cache. Prefix work is therefore
bounded by the largest required prefix, rather than the sum of every shard's
required count.

After WAL replay, but before maintenance starts, a fresh process drops and
crash-atomically rewrites retirement markers at or below the global minimum
base coverage. This is safe because no root view from the previous process can
still be pinned; it also prevents markers left by a crash between root
publication and ordinary pruning from consuming the bounded hot-byte budget on
every subsequent restart.

Rewind rotates the root lineage. Any background rebase built against the old
chain incarnation then fails its final compare-and-swap instead of publishing
stale state.

Appendvec compaction publishes every relocation durably before recording a
retirement marker and removing the old file. Retirement markers remain exact
until all current bases cover them and every older pinned base has drained.

Background appendvec compaction is enabled by default and driven by filesystem
free-space pressure. Routine cycles run every 30 seconds, take the fold lock
only with a non-blocking attempt, stop at the configured target free space, and
reject any source above the default hard 64 MiB source cap before scanning it.
Move and scan limits remain soft per-cycle targets checked between admitted
source files.

Before a fold can allocate a file ID or write any output, a hard disk-admission
check conservatively reserves the new segment, manifest and metadata footprint.
When free space is below the minimum, the manager runs synchronous emergency
compaction without the routine source cap and repeats the check. A complete
emergency pass that cannot establish the reserve returns a typed disk-pressure
error and halts before any fold side effect. Compaction similarly preflights its
output, complete relocation WAL, and metadata footprint before allocating an
output file ID; candidates that do not fit are deferred so a later fully dead
file can still be reclaimed. Unexpected compaction errors cancel replay.

Disabling compaction while retaining disk-reserve enforcement therefore grows
the store only until it can no longer admit a fold, then halts safely. Disabling
the reserve as well is an explicit unsafe escape hatch. Resumable/concurrent
processing of large source files remains follow-up work; emergency compaction
deliberately trades latency for disk safety.

## Snapshot bootstrap and migration

Snapshot construction externally sorts newest-wins account locations, streams
them into all base shards, initializes an empty V2 WAL, and publishes the root
catalog last. The complete bootstrap holds the store-wide lock from cleanup
through extraction, index construction, state verification and database open;
ownership transfers to the live index only after every fallible open step has
succeeded. A partially built snapshot therefore never looks like a complete
index and a failed open cannot create an unlocked observation window.

Archive identity is fail-closed: canonical filename slots and the filename's
base58 snapshot hash are bound to the decoded manifest, incremental archives
are bound to their selected full snapshot, and the manifest bytes seen during
extraction must exactly match the first pass. Appendvecs are streamed to
no-replace temporary files, synced, atomically published and checked against
the manifest's exact member set and sizes.

Alpenglow replay restores the snapshot manifest's exact block ID, persists it
as the fresh-replay parent anchor, and never substitutes the PoH bank hash.
After a rooted fold, the rooted resume context supersedes that snapshot seed.
A missing, zero, malformed or noncanonical manifest/rooted block ID fails
closed before replay rather than fabricating chain identity.

Before the snapshot is accepted, AccountsDB sequentially scans every declared
appendvec in bounded batches, asks the newly built immutable index which exact
location wins for every full public key, and recomputes both capitalization and
AccountsLtHash from those selected records. The calculated values, selected-key
count and filename snapshot hash must all match the manifest. Missing, extra,
malformed, replaced or symlinked appendvecs fail bootstrap while the exclusive
guard remains held.

The verifier retains that fully opened immutable generation for the final
AccountsDB handoff; it does not start the mutable writer or any maintenance
goroutine. Immediately before adoption, AccountsDB revalidates the root
selector, empty WAL header, exact configuration, store-lock identity, and the
inode/size/modification identity of every selected immutable artifact. It then
constructs the mutable runtime around the same verified mappings. This avoids a
second complete extent-catalog pass and a second SHA/CRC/semantic/StreamHash
pass over every base shard while still failing closed if anything changed
during finalization.

V2 does not silently open a Pebble account index or the transitional global
StreamHash format. Existing installations must bootstrap from a fresh snapshot
or use a separately reviewed offline converter. Changing the persisted shard
count likewise requires a fresh snapshot.

## Configuration

The settings live under `[tuning]` and have matching command-line flags.

| Setting | Default | Meaning |
| --- | ---: | --- |
| `account_index_shards` | 1024 | Persistent power-of-two routing count. |
| `account_index_max_hot_keys` | 2097152 | Maximum exact active/frozen keys. |
| `account_index_max_hot_mb` | 512 | Conservative accounted hot-state ceiling. |
| `account_index_max_checkpoint_selected_mb` | 1024 | Maximum exact checkpoint bytes selected by the current root. |
| `account_index_max_checkpoint_physical_mb` | 2048 | Maximum selected, in-flight and reader-pinned obsolete checkpoint bytes. |
| `account_index_seal_keys` | 8192 | Per-shard checkpoint trigger. |
| `account_index_seal_max_age_ms` | 30000 | Age trigger for a non-empty shard. |
| `account_index_max_concurrent_seals` | adaptive, at most 8 | Concurrent independent shard checkpoints. |
| `account_index_rebase_keys` | 65536 | Exact checkpoint size that requests a base merge. |
| `account_index_journal_rewrite_mb` | 512 | WAL growth before bounded rewrite. |
| `account_index_checkpoint_workers` | `GOMAXPROCS` | Approximate CPU-worker budget divided across concurrent StreamHash checkpoint builds. |
| `account_index_rebase_workers` | 1 | Required while extent lineage is shared. |
| `working_set_max_mb` | 1024 | Hard conservative charged-memory limit for unrooted account layers. |

Limits are validated at startup. Invalid settings fail rather than silently
falling back to an unbounded or incompatible mode.

Shutdown is also fail-closed and retryable. New operations are fenced, mutable
maintenance is stopped, generation readers are drained with the caller's
context, and the store lock is retained if that context expires. Bank-hash and
other store resources close before the production index releases the lock, so
another process cannot clean or reopen a half-closed AccountsDB.

## Capacity model

The 512 MiB default is the accounted active/frozen Go-map budget, not the total
AccountsDB size. Immutable StreamHash bases, exact delta checkpoint files and
scan sidecars are file-backed. Their resident pages compete in the operating
system page cache and should be evaluated under the validator's real memory
limit, not inferred from virtual address space.

The unrooted account `WorkingSet` has a separate hard 1 GiB default limit. Its
accounting conservatively charges every retained account object, both lookup
maps, its undo entry and the full capacity of its data backing array. Before
publication, replay computes the complete incoming slot charge while preparing
the slot once. If current plus incoming charge exceeds the limit, it returns a
typed capacity error and publishes neither accounts nor bank hash. There is no
one-slot overshoot. Operators must configure the limit above the largest valid
slot they intend to admit; a disk-spill path for an individually oversized slot
is future work. Per-slot metrics report current and high-water charges and the
largest slot charge. Resume contexts and the rest of the process are outside
this accounting, so total RSS still requires a representative cgroup soak.

The base lookup artifact is approximately its six-byte payload, one-byte
fingerprint and StreamHash structure overhead per account. The exact `.scan`
sidecar costs 38 bytes per account on disk but is not resident during normal
lookups. Exact delta records cost 64 bytes per changed key plus their compact
StreamHash directory until that shard rebases.

With the current six-byte locator and one-byte fingerprint, measured base
artifacts are about 7.35 bytes per account in the StreamHash file, plus the
38-byte exact sidecar. At one billion accounts, the StreamHash mapping alone is
therefore about 6.84 GiB on disk/address space. It is memory-mapped and its
clean pages are reclaimable, but resident mapped pages still count toward RSS
and a cgroup memory limit. Adding the mutable heap, exact delta mappings,
runtime state and account/program caches means this design must not advertise a
hard sub-8-GiB RSS guarantee. Sub-16-GiB operation is the realistic initial
target; an 8-GiB deployment is an empirical tuning target that must be proven
under its actual access distribution and memory controller.

The normal root does not retain one file descriptor per shard. A base `.scan`
sidecar with a matching authenticated Linux receipt needs only fixed-header and
strong-identity validation at generation open, then is reopened with the same
identity checks for the duration of an exact scan or rebase. A stale receipt
forces complete validation. Delta `.rec` descriptors are closed immediately
after their read-only mappings validate, and StreamHash likewise closes each
`.stmh` descriptor after mapping it.
Steady descriptor use is therefore driven by the WAL, appendvec reads and
bounded concurrent maintenance rather than the 1,024-shard count. An exact
raw-prefix enumeration is exceptional: its single-scan gate can temporarily
open at most one sidecar per shard, and its `RLIMIT_NOFILE` preflight reserves
64 additional descriptors for the rest of the process. Operators must set the
service file-descriptor limit above this checked scan envelope with comfortable
headroom for appendvec I/O.

The six-byte base locator reserves 24 bits for an append-only extent ordinal,
giving one lineage a hard capacity of 16,777,216 distinct `(slot, file,
128 MiB base offset)` extents. Ordinals are not reclaimed by appendvec
compaction. Rebases containing only already-known extents reuse the selected
catalog without catalog I/O; a rebase introducing any new extent still
republishes the complete monolithic catalog. At 200 ms slots and the default
128-slot fold, one new extent per non-empty fold would consume approximately
1.23 million ordinals per year and reach the format limit in roughly 13.6
years; multi-extent files, shorter adaptive folds and compaction outputs reduce
that interval. Operators must alert on `ExtentCatalogRemaining`; until online
lineage rollover exists, this is a monitored operational lifetime bound rather
than indefinite unattended operation.

At 1.25 million extents, the current monolithic catalog measured about 30 MiB
on disk, 114 MiB resident, and 0.63 seconds for the background clone, write,
sync, verification and adoption pipeline, with roughly 235 MiB of temporary
two-generation overlap. Batched prefix verification keeps that work out of the
per-shard hot path, so this is acceptable for a monitored production candidate,
but not the indefinite format. The planned successor is a small versioned
descriptor over immutable append-only extent segments, preserving existing
ordinals and root publication while eliminating whole-catalog rewrites and
enabling online lineage rollover.

Checkpointing also has a deliberate write-amplification trade-off: sealing a
shard rebuilds its complete exact checkpoint, including older keys not yet in
the base. Uniform write pressure can force checkpoints smaller than
`account_index_seal_keys`, increasing that amplification before the shard
reaches `account_index_rebase_keys`. This is bounded and correct, but default
thresholds are not considered validated until maintenance bytes, retry counts,
fold latency and NVMe bandwidth have survived a representative mainnet write
soak.

## Operations and acceptance

Use `replay_timings.jsonl` to judge the complete account-loading path. The
important loader fields are `IndexLookup`, `ReadPlanning`, `AppendVecRead`,
base candidate/false-positive counts, physical `pread` calls and requested vs
physical bytes. Index maintenance statistics expose hot keys/bytes, WAL bytes,
active checkpoint/rebase work, pending retries, extent-catalog capacity and the
root coverage/generation. `WorkingSetRetainedBytes`,
`WorkingSetHighWaterBytes`, `WorkingSetLargestSlotBytes`, and
`WorkingSetHighWaterOverageBytes` expose speculative account memory pressure;
the overage field is retained for metric compatibility and remains zero under
strict configured admission.
`FoldCommits`, `FoldWALFrames`,
`OversizedFoldCommits`, and `LargestFoldKeys` reveal when valid oversized slots
exercise the multi-frame path.

Before changing defaults, benchmark and soak all of the following:

- 30,000-key all-base, all-hot and mixed batches;
- sustained uniform writes across all 1,024 shards;
- skewed hot-shard writes;
- checkpoint publication while readers remain pinned;
- repeated rolling rebases and appendvec compaction;
- restart after injected failure at each data, WAL and root publication stage;
- zero-lamport deletion through crash replay, checkpoint, rebase, compaction,
  rewind and reopen;
- corrupt/torn final manifests, sequence gaps and invalid slot chains, all of
  which must fail closed without deleting possibly committed data;
- a blocked range visitor while unrelated shards checkpoint/rebase, proving
  that publication and bounded-memory backpressure continue to make progress;
- recovery under `go test -race` and under the intended cgroup/RSS limit.

The target is the end-to-end replay budget, not an isolated index nanosecond
number. A faster lookup that moves the bottleneck into random appendvec I/O or
background write amplification is not a successful configuration.
