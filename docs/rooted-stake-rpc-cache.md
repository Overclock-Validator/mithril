# Rooted stake totals for getVoteAccounts

`getVoteAccounts` maintains activated stake by vote account in memory. The
initial request loads the existing stake-candidate index and reads accounts in
batches of 512. Subsequent account commits update only changed delegations.
Filtered requests read one vote account and one total; unfiltered requests still
enumerate vote accounts, but neither path rescans stake accounts on each scrape.

The retained data is one compact delegation/contribution per live delegated
stake account, plus totals per vote account. Full account buffers are not retained.
Memory is proportional to delegated stake accounts. Bootstrap temporarily tracks
changed keys while loading candidates; recalculation temporarily holds a key list.

## Publication and recovery contract

- AccountsDB brackets writes with a process-local version. An odd version,
  pending legacy write, or failed write cannot serve a committed cache view.
- Observers receive borrowed changed accounts after successful writes, before the
  version becomes readable. They perform no I/O and retain only decoded values.
- A response must match both the account version and the RPC's published bank
  identity from start to finish. A durable fold or completed legacy write alone
  does not publish its account values under the previous RPC bank context.
- Initialization merges commits while scanning. Placeholders cannot resurrect
  closed accounts, and accounts created after the candidate snapshot are supplied
  by the observer. No unchanged-root interval spanning the full scan is required.
- Epoch, stake-history, or warmup-feature changes trigger a RAM-only recalculation
  in bounded chunks. Per-entry calculation tags count concurrent updates exactly
  once; new activation inputs cancel an unfinished calculation.
- Rewind, recovery, and failed writes invalidate the derived view. Recovery also
  clears old account-read caches before a rebuild. Reusing the same slot on another
  fork cannot reuse its old stake totals. Failed writes require successful repair
  or a subsequent successful write before the account version is readable again.

This cache adds no durable index, disk writes, or manifest format changes. It is
reconstructed after process restart.

The durable vote-candidate index also keeps the original version-1 manifest
format: `PrevValid` is exactly 0 or 1, as required by older rewind readers.
Vote membership is an in-memory hint during normal commits. Recovery derives it
from account headers in the CRC-checked segment before atomically installing the
primary index, vote candidates, and fold watermark. This adds header reads only
for batches whose index application must be replayed. Metadata/read failures
leave that batch's files intact and do not advance its watermark.

The reader still accepts the 2/3 flag bytes emitted by earlier revisions of the
RPC branch, preserving their undo pointers. Existing files are not automatically
rewritten: stores that already contain those flags must use the corrected reader
for recovery/rewind, or be rebuilt before downgrading to a pre-RPC binary. Newly
written manifests are compatible with the original reader. A format-version bump
alone would not make downgrade safe, because older recovery treats unknown
manifest headers as orphans.

## Work and validation

The first request and recovery still require account reads. Epoch/input changes
require work proportional to delegated stake accounts, but no disk scan. Normal
commits inspect changed accounts and decode only stake candidates; this cost is
paid by the account writer. RPC callers waiting for initialization can cancel.
The cache checks cancellation between read/calculation batches.

```sh
GOMAXPROCS=2 go test -race ./pkg/accountsdb ./pkg/rpcserver
GOMAXPROCS=2 go vet ./pkg/accountsdb ./pkg/rpcserver
GOMAXPROCS=2 go test ./pkg/rpcserver -run '^$' \
  -bench '^BenchmarkRootedStakeTotals$' -benchtime=500ms -count=3
```

The benchmark uses 20,000 stake accounts and 1,000 vote accounts. It compares the
old per-query scan against warm aggregate reads, and separately measures applying
64 stake updates among 20,000 changed accounts. It reports time and allocations.
These are component measurements: they exclude JSON encoding, network latency,
initial cache loading, and an epoch transition. They do not establish mainnet
capacity or live validator performance.
