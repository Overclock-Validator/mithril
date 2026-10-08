# Rooted stake totals for getVoteAccounts

`getVoteAccounts` maintains activated stake by vote account in memory. The
initial request loads the existing stake-candidate index and reads accounts in
batches of 512. Subsequent account commits update only changed delegations.
Filtered requests select one vote record and one total; unfiltered requests still
enumerate vote accounts, but neither path rescans stake accounts on each scrape.
Initialized vote-account records are decoded lazily and reused until a committed
write touches that account. The cache owns the RPC fields and up to five credit
entries, not account buffers. Stake totals, epoch membership and delinquency are
applied per request. Returned credit slices are copies owned by the caller.

The writer only invalidates touched vote records; it performs no additional vote
decoding or I/O. Rewind, recovery and failed writes clear all parsed records. Loads
check the account version under the invalidation lock before entering the cache,
so an old in-flight decode cannot resurrect a record after a concurrent commit.
The final response still checks bank identity and account version even on a fully
cached read. Unknown or invalid filtered keys do not create negative cache entries.

The retained data is one compact delegation/contribution per live delegated
stake account, plus totals per vote account. Full account buffers are not retained.
Memory is proportional to delegated stake accounts. Bootstrap temporarily tracks
changed keys while loading candidates. Recalculation walks the existing map
without allocating or copying a whole-cache key list.

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
- Recalculation holds the cache mutex for every map iterator step, read and write,
  releasing it after each 512 visited entries, including already-counted entries.
  Commits between chunks may delete or insert keys: the observer removes old
  contributions and counts updated/new entries with the current calculation tag.
  Those tags prevent double counting whether the iterator sees an insertion or
  skips it, as permitted by [Go's map range semantics](https://go.dev/ref/spec#For_range).
  After each lock reacquisition, generation and calculation checks reject resets
  or changed activation inputs before the iterator advances again. This bounds
  entries processed per lock hold, not a hard wall-clock latency guarantee.
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

`BenchmarkRootedStakeRecalculation` isolates RAM traversal at 20,000 and 2 million
delegations with 1,000 voters. The delegation cache is seeded directly and all
stakes are bootstrap-active; it measures recalculation time and allocations,
not startup, disk, or writer latency. For concurrent commits and mixed activation
history, use the on-disk sizing experiment below. An optional Go mutex profile
can attribute writer/reader lock wait; profile times sum waiting goroutines and
must not be reported as a single request's wall-clock latency. The sizing test's
full-scan correctness oracles also appear in whole-process profiles.

```sh
GOMAXPROCS=2 GOMEMLIMIT=4GiB go test ./pkg/rpcserver -run '^$' \
  -bench '^BenchmarkRootedStakeRecalculation$' -benchtime=700ms -count=3
```

`BenchmarkGetVoteAccountsRecords` measures the warm handler plus JSON encoding
for 1,000 V4 vote accounts (31 lockouts and 64 credit-history entries each), or one
filtered account. It isolates vote records with initialized empty stake totals.
The changed-record cases invalidate 10% or 100% of decoded records before every
request, including invalidation cost, while account buffers remain warm. They
simulate cache churn without disk commits; they do not measure writer contention.
Reuse benefits depend on how many vote accounts change between requests. A full
refresh still pays decoding plus cache maintenance and response ownership costs.

```sh
GOMAXPROCS=2 go test ./pkg/rpcserver -run '^$' \
  -bench '^BenchmarkGetVoteAccountsRecords$' -benchtime=700ms -count=3
```

## Larger on-disk sizing experiment

The opt-in `TestRootedStakeCacheScale` exercises 1,000 initialized vote accounts
and a configurable count of delegated stake accounts (up to 2 million). Its
synthetic mix is 80% active, 10% activating, and 10% deactivating, with stake-history
changes at epoch boundaries. Run each size in a fresh process:

```sh
MITHRIL_STAKE_SCALE=1000000 GOMAXPROCS=2 GOMEMLIMIT=4GiB \
  go test ./pkg/rpcserver -run '^TestRootedStakeCacheScale$' \
  -count=1 -v -timeout=10m
```

JSON log records report initial loading, warm all-voter/single-voter RPC handler
latencies and allocations, epoch recalculation, and concurrent bootstrap and
epoch changes. Four callers pause 50 ms between requests while 100 durable folds
update 64 stake accounts each, with a 20 ms pause between folds. A separate
no-reader stage measures fold latency. Every stage checks totals against a full
account scan; concurrent root advancement must not restart the bootstrap scan.

Handler timings include JSON response encoding but exclude HTTP. Startup clears
application account caches; fixture creation still warms the OS page cache, so
this is not a cold-storage benchmark. The retained-cache estimate is the change
in live Go heap after dropping only the derived cache and collecting garbage.
Whole-process peak memory includes fixture construction and full-scan oracles;
it must not be described as the cache size or a complete RPC-node memory budget.
Reported tail percentiles describe these short samples, not production SLOs.
