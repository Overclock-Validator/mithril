# Dashboard API reference

For configuration and deployment, see the [setup guide](dashboard-api.md).

## Optional data

`/api/v1/features?include=advanced_status` reports optional route families enabled by the reader. It is
not a complete list of disclosure flags. The reader does not request public
providers; a separately run collector writes the optional cluster snapshot.
The reader writes aggregate history only when a history directory is configured.
Use the query above to discover node-display: the default features response
does not advertise that route, even when it is enabled.

## Optional routes

These read-only routes are available only when their reader options are enabled.
Related node observations must also be enabled for linked-node details. Missing,
stale or unverified inputs must not be interpreted as zero.

| Reader option | Route family |
| --- | --- |
| `--replay-details` | `/api/v1/details/replay`, `/api/v1/details/selected`, `/api/v1/details/selected-window`, `/api/v1/details/activity-history`, `/api/v1/details/slots`, `/api/v1/details/schedule` |
| `--fast-executed` | `/api/v1/details/recent-executed` |
| `--network-details` | `/api/v1/details/network` |
| `--cache-details` | `/api/v1/details/cache` |
| `--production-details` | `/api/v1/details/production` |
| `--storage-details` | `/api/v1/details/storage` |
| `--machine-details` | `/api/v1/details/machine` |
| `--local-peers` | `/api/v1/details/peers` |
| `--reception-history` | `/api/v1/details/reception` |
| `--history-dir` | `/api/v1/history` |
| `--cluster-file` | `/api/v1/cluster`, `/api/v1/cluster-map`, `/api/v1/cluster-versions`, `/api/v1/cluster-performance-history`, `/api/v1/node-display` |

The fast execution route requires node `--dashboard-fast-executed` (or
`dashboard.fast_executed = true`), reader `--fast-executed`, and replay
details on both. It reports provisional local execution, not finality. Connect
to `/websocket?include=advanced_status` on the configured origin. Its
`recent_executed` value is a change hint (`true`) or
clear (`null`); fetch the bounded tail over HTTP and honor both remaining
lifetime headers from the request start: use the smaller of
`Mithril-Status-Remaining-Milliseconds` and
`Mithril-Detail-Remaining-Milliseconds`. A repeat of the same sequence or a
reconnect does not extend an existing observation's lifetime.

The response contains `schema_version`, `family`, `scope`, `state`,
`sequence`, `sampled_at`, `binding` and `records`. Match the binding's
`run_id`, `genesis_hash`, `node_started_at`, `attempt`, `rewinds` and
`attempt_started_at` to the current advanced status. Each record has `at`,
`slot`, `bankhash`, `transactions`, `compute_units` and
`work_counters_saturated`, and may include `block_id`. A present non-null
`block_id` is a canonical nonzero 32-byte base58 Alpenglow block identity;
absence or `null` means unknown. It is distinct from `bankhash` and does not
establish finality by itself. A finalized bar requires an exact slot/block-ID
proof from fresh, safe consensus with the same current binding. Sequence,
slots and counters are decimal strings.
Update readers before enabling producers that emit `block_id`: older strict
readers reject that additional record field.
Clear the observation on a null hint,
disconnect, binding change or expiry. Polling does not guarantee every update.

`/api/v1/details/selected-window` returns one JSON object with a `manifest`
and the latest six `pages` at most, in ascending page order (up to 48 rows).
The manifest describes the entire retained window, so its `page_count` and
`count` may exceed the bundled pages and rows. Each page includes its page
number, offset, binding and digest; check these against the manifest. The
digest covers the complete window, not just the bundled tail. Use the paginated
`/api/v1/details/selected` route when the entire window is needed. The bundle
shares the same status and source freshness limits and must not be displayed
as fresh after either remaining-lifetime header expires.

The optional reception-history reader samples immutable chunks intersecting the
eight newest sequence positions admitted by the current source root. A short
newest chunk can be collected with its retained predecessor. A confirmed gap at
the start of the window can trigger one retry at the newest position. The cache
holds partial observations. Skipped transport spans count toward `cache_gaps`
and describe cache coverage. Crossing a missing span returns HTTP 409 `cache_gap`.
Under `/api/v1/details/reception`, the root has no query. Allow only the
GET routes the viewer needs: `/index/{recording}` and `/cursor/{recording}`
take `after`; `/query/{recording}` takes `after`, `end_time`, `start_time`
in that order (UTC RFC3339, at most one minute). `/chunk/{recording}/{digest}`
and `/metadata/{recording}/{digest}/{kind}/{page}` take no query.

For live reception, honor `Mithril-Detail-Remaining-Milliseconds` and the
root's `Mithril-Status-Remaining-Milliseconds`. Without a positive detail
lease, a response is not live. Expired metadata returns `metadata_expired`
(HTTP 503 for the root, 409 for a selection); refetch the root before retrying.

The cluster directory returns at most nine rows per page and accepts `offset`,
`order`, `country`, `search` and `generation`. The cluster map, versions and
performance-history routes accept only `generation`; node-display accepts no
query string. Reuse a response's generation when combining or paging cluster
views; a changed generation returns HTTP 409. Check the
`Mithril-Cluster-Remaining-Milliseconds` response header: zero means the cached
response is no longer fresh. Its shared lifetime is limited by the snapshot,
fresh providers, included certificate windows and RPC performance receipt times.
Check each provider's state independently as well. See the [collector guide](../cmd/mithril-dashboard-collect/README.md)
for provider provenance, limits and country attribution.

`/api/v1/cluster-map` may include `rpc_nodes` with an exact decimal-string
`advertised` count and independent `observed_at` time from the existing
`getClusterNodes` response. It counts unique identities advertising a valid RPC
socket address; zero is observed, while missing/invalid data omits the aggregate.
It does not establish reachability or complete cluster coverage, and no addresses
are retained. Its source age also caps the cluster response lease. Upgrade every
reader sharing the cache before a new collector emits this field; for rollback,
stop the new writer and obtain a fresh legacy-shaped snapshot before reverting
readers. The existing community contact limit remains 2048; mainnet allows 8192.

`/api/v1/cluster-map` may include `rpc_stake` with exact decimal-string
`current_lamports` and `delinquent_lamports`. It is present only for a fresh
mainnet snapshot and a fresh, epoch/genesis-matched RPC provider whose
`rpc_genesis_before_after` binding and declared row count cover every cached
vote-account row. Every row must be RPC-sourced and report activated stake and
delinquency; stake must convert to integral uint64 lamports per account. The
aggregate can exceed uint64. Sum vote accounts, including distinct vote
accounts sharing one identity; `total_nodes` separately counts unique
identities. Stale, community, incomplete or invalid inputs omit this group;
absence is unknown, not zero. These cached external observations do not add a
collector or establish the linked node's chain state.

For a separately hosted browser UI, route API requests through the UI's
origin and preserve this header. The reader does not add CORS headers, so a
direct cross-origin browser request cannot read the response by default.
Treat a missing or invalid remaining-lifetime header as unknown, not fresh.

The history route returns available metrics and ranges; request bucketed data
with `?metric=<name>&range=<range>`. History is limited to 30 days, may contain
gaps and includes resource metrics only with `--public-resources`. For machine
details, fetch a manifest's pages using its `generation` and zero-based `page`;
request a new manifest after the generation expires. Both route families have
shared request limits; configure per-client limits at the reverse proxy.

Details and histories have bounded retention; they are not a ledger archive.
Cluster routes reflect their named external providers and have their own
freshness and availability. They do not establish the linked node's verified
chain state. The four cluster routes share a limit of five requests per second,
with a burst of ten, across all clients. Excess requests receive HTTP 429;
one busy client can therefore affect other viewers. Apply appropriate
per-client limits at the reverse proxy. See the [setup guide](dashboard-api.md#optional-dashboard-data)
before enabling these options.
Votor UDP socket memory is not provided because the QUIC listener owns its
socket; an absent Votor socket observation does not mean zero usage or drops.

## Experimental fast payload WebSocket (v1)

**Experimental and off by default.** Keep the standard status path available
when an anchor cannot be established or expires. With the existing
fast-execution options above, the exact query
`/websocket?include=advanced_status&recent_executed=v1` selects complete cached
tail payloads. Reordered, duplicated or extra v1 parameters are rejected;
existing snapshot and boolean-hint queries are unchanged. This session sends
only anchor/frame envelopes. Keep a separate,
independently leased advanced-status HTTP response as the binding authority.

Send this closed canonical grammar, with a fresh cryptographically random
32-character lowercase hexadecimal ID for each challenge (the example ID is
illustrative). `type` must precede `id`; whitespace, duplicate keys and extra
fields are rejected:

```json
{"type":"challenge","id":"00000000000000000000000000000001"}
```

The server acknowledges the ID and then sends full replacement values:

```json
{"type":"anchor","challenge":"00000000000000000000000000000001"}
{"type":"frame","topic":"mithril","key":"recent_executed_v1","challenge":"00000000000000000000000000000001","expires_after_challenge_ms":0,"value":null}
```

`value` is the fast-tail body described above, or null. Null requires a zero
lifetime; a non-null value requires an integer lifetime N from 1 to 5000 ms. For
server challenge receipt R and the already admitted original status/source
expiry D, N is `floor(0.999 * min(D-R, 5000 ms) / 1 ms) - 10`, required positive.
The client deadline is its **original challenge-send monotonic time + N**,
never ACK receipt or payload receipt + N. Keep the parent, record-age, binding
and duplicate-publication caps; a new anchor or reconnect does not renew them.
Serialized qualified status is authoritative; retain per-binding sequence
fences rather than assuming every previously seen run is permanently invalid.

This expiry bound assumes client monotonic advance at least 0.999 of server
advance and client timer precision no worse than 1 ms. The 10 ms guard covers
precision/serialization under those assumptions, not arbitrary clock drift.
Clients must detect unsupported/coarse or discontinuous clocks and withdraw
on suspend/hidden/resume or an unknown/expired anchor. Keep one active and one
pending challenge, and close/withdraw when an anchor reaches 2 s. Refreshing
the challenge does not extend the server horizon.

The existing 16-viewer cap is shared with ordinary sessions. Challenges are
capped at 128 bytes and 2/s with burst 2, with one pending item; controls retain
their 2/s, burst 4 limit. Each viewer has a 12/s cached-frame limit with burst 4,
plus bounded challenge ACKs. A cached body is at most 16 KiB plus its envelope.
Source/status invalidation or expiry interrupts a blocked payload write;
bytes already buffered cannot be recalled and retain their original deadline.
This increases bounded public cached egress, not node sampling or private-feed
limits; measured performance and viewer isolation remain deployment gates.

## Experimental fast payload WebSocket (v2)

With the same node/reader fast-execution options, the exact query
`/websocket?include=advanced_status&recent_executed=v2` negotiates v2. It keeps
the v1 challenge/anchor grammar, binding checks, clock assumptions, two-second
anchor limit and shared capacity limits. Reordered, duplicated or extra query
parameters are rejected. V1 envelopes and existing snapshot/hint queries do not
gain fields or change meaning.

V2 frames use `key: "recent_executed_v2"` and add one integer field:

```json
{"type":"frame","topic":"mithril","key":"recent_executed_v2","challenge":"00000000000000000000000000000001","expires_after_challenge_ms":0,"value":null,"source_expires_after_challenge_ms":0}
```

For a non-null value, both lifetimes are positive integers at most 5000 ms,
and `source_expires_after_challenge_ms` is at least
`expires_after_challenge_ms`. Both use the v1 conservative conversion and the
client's **original challenge-send monotonic time**. The existing field bounds
this response by the admitted status and source deadlines; the new field comes
from the reader's retained publication deadline, independently of the current
status lease. Repeated source bytes/sequence do not renew that publication.
Null requires both fields to be zero and withdraws the current value.

A later validated response may admit the same payload under a later response
bound while its original source bound remains live. Clients must retain that
source cap and issue a new token; previously published tokens cannot be
extended. Parent, record-age, rewind, conflict, expiry and withdrawal fences
still apply. A new anchor, status response or reconnect does not revive a
cleared sequence. These ceilings do not establish consensus finality, client
admission, rendering or a network-latency guarantee.

Upgrade the reader before enabling v2 clients: old readers reject the v2
query. Clients opt in to v2 separately and can still use the standard
dashboard routes. V2 requires no node private-feed schema change.

### Optional TPU signature-verification worker gauge

With queue observation enabled, network detail's optional
`tpu_queues.sigverify_workers` contains decimal-string `batches_in_flight` and
`configured` counts. The gauge counts workers holding a batch from first packet
receipt through drain, verification and final output handoff, including a blocked
handoff. It is bounded by the positive configured worker count; zero is observed,
while omission is unknown. It is neither CPU utilization, packets in flight nor
an atomic queue/backlog measurement. It retains the queue observation's original
sample time, attachment and freshness guards; stopped pipelines are withheld.

Upgrade the matching reader before enabling a producer that emits this field:
older canonical decoders reject the whole network detail. New readers accept
older producers that omit the optional field. This rollout requirement is
separate from v2 WebSocket negotiation.

When network details are enabled, setting `MITHRIL_DASHBOARD_TPU_WORKER_TIMING=1`
additionally enables per-worker batch-pass wall timing; it is off by default.
Each optional `passes` row is scoped to the TPU pipeline lifetime and reports
worker index, sample time, cumulative active nanoseconds and elapsed
nanoseconds. Active time includes scheduling delay and blocked output handoff;
it is not CPU use or a pinned core. The list is omitted when disabled, invalid,
the configured pool exceeds 64 workers, or the network response budget would
otherwise displace existing details. A positive elapsed interval and
original network/status leases are required; samples are not live countdowns.

Install a compatible reader before enabling this option: older strict readers
reject the extended network detail. New readers accept producers that omit the
optional timings.

## Bounded slot timeline

`replay.slot_history`, when available, describes the latest 32 consecutive
selected-and-folded slots. Each entry is `produced`, `selected_fork_skipped` or
`unknown`. A skipped span requires matching parent slot, block identity and
executed parent bank hash between retained committed receipts. Missing records
or conflicting ancestry stay unknown. This is neither local forging nor a
direct finality-certificate view.

The same optional replay recorder works for voting and non-voting Alpenglow
nodes with a local consensus engine and durable folds. Classic replay and
delegated-finality paths without that evidence omit the group. It is an
attempt-local observation, not a persistent block archive; a restart resets it.
The sample has its own age and coverage flag. No account or history-store scan
runs for a viewer. The unmerged history RPCs remain separate.

## Selected-chain activity

`GET /api/v1/details/activity-history` requires replay details on both node and
reader, with no query string or request body. It returns up to 32 contiguous,
non-overlapping selected-and-folded bank-clock intervals, newest first, in at
most 16 KiB. Source observation/capture dates are separate from row bank-clock
durations. Validate the full replay binding and both original status/detail
leases; a repeated captured publication cannot renew its deadline. It shares
the selected-response rate/active caps but has separate bounded collection
workers. These are local historical observations, not finalized or cluster TPS;
missing coverage must not be interpolated.

`replay.chain_activity` uses the same bounded selected-and-folded receipts.
Its interval counts transactions, non-vote transactions, included failures and
recorded compute units after an anchor block through the last included block.
Included failures can include zero-fee no-ops. A no-op can record its
compute-unit limit without executing instructions. Recorded compute units are
not native block cost.

For these local counts, a transaction is classified as a vote when any
top-level instruction resolves to the Vote program; all others are counted as
non-vote. This includes Vote-program administration and mixed transactions in
the vote bucket. This classification does not determine a transaction's
purpose or count Alpenglow's off-chain votes. The same rule applies to
selected-slot counts and retained `chain_*_vote_tps` history; externally
reported RPC performance samples retain their provider's own definitions.

Both timestamps come from the accepted banks' Clock sysvar. The anchor's counts
are excluded. The reader divides those counts by the bank-time difference,
never by the time spent replaying them. This differs from `throughput`, which
measures local replay work and can be much faster during catch-up.
Consumers can derive successful transactions by subtracting included failures
from the transaction count, using exact integers before calculating a rate over
the same bank-time interval.

The interval targets ten bank seconds while retaining at most 512 carriers;
shorter positive intervals can appear after startup. Exact parent block and
bank hashes must connect the interval. Missing evidence, conflicting folds,
clock regression, zero elapsed time or overflow yields a null interval, not
zero activity. Bank time is second-granularity and is not a precision latency
measurement. The slot/time bounds remain visible: historical catch-up data is
not advertised as current cluster-tip TPS. Counts reset with the attempt and
are not an archive or an external dashboard score.

Serial, parallel and streamed replay record admitted transaction results on
the bank owner. Local production records only accepted commits, including
fee-only failures; rejected bundles and speculative banks do not publish
settled counts. Simulation does not update these counters. No transaction
payload, account data or error text is exposed. These observations do not alter
the bank hash, transaction result or persistence format.

## Epoch-scoped validator observations

`replay.epoch_outcomes` is available for a bound Alpenglow validator. It splits
the retained observations into the epoch containing `through_slot` (the sampled
durable slot) and its predecessor. This is not necessarily the cluster's current
epoch, and neither row is a persisted lifetime score.

- Leader windows cover at most the latest 512 slots. The known assignment
  denominator is `produced + selected_fork_skipped + scheduled_unknown`.
  Slots with unknown schedules cannot be assigned to this identity; unknown
  ancestry remains visible even when another identity owns the slot.
- Reward windows cover at most 512 target slots, ending eight slots before
  `through_slot`, as required by the current reward-certificate protocol. A
  carrier just after an epoch boundary can therefore report the prior epoch.
  `included + excluded` is the known eligible-carrier denominator. Ineligible
  accounts, invalid/unknown admission evidence and absent retained carriers
  (`unobserved`) are separate categories. Missing carriers are not vote misses.
- Each range reports `complete_epoch` only when it covers that entire epoch
  without the relevant unknowns or lost fold evidence. A short-epoch test can
  meet that condition; a long epoch will normally be partial in this bounded
  window. `null` means that no range has been observed, not a zero score.
- These chain observations describe selected and folded outcomes under the
  recorded local replay scope.
  The feed binds them to the node identity, vote account, run and replay attempt;
  a new attempt clears them. The public projection rejects
  mismatched identities, overlapping epochs and inconsistent counts.

Keep the epoch ranges and partial/complete status separate from carrier-wide
certificate observations, whose retained window can span different epochs.

## Public fields and freshness

For a fresh `GET /api/v1/status` response,
`Mithril-Status-Remaining-Milliseconds` gives the remaining lifetime as a
canonical nonnegative decimal number of milliseconds. The reader bounds it by
the original sample-age deadline and, when present, the private source lease;
repeated requests cannot renew it. Clients must subtract the monotonic time
elapsed since immediately before their HTTP request and treat the status as
stale once the lease expires. Stale or unavailable status has no lease header;
an expired cached fresh status returns 503 without a lease.
The reader may shorten a previously observed deadline when a later private-feed
response supplies a shorter bound. It cannot retract a response already sent.
WebSocket closure cannot guarantee when buffered data reaches a client. Clients
must age observations using their source timestamps and treat disconnects as a
loss of current updates. A source failure, expired status, or node run or
identity replacement can close the connection; clients should reconnect with
backoff and wait for fresh data.

The reader compares elapsed wall and monotonic time. If they diverge by more
than one second, it withholds fresh status for the rest of the reader's
lifetime. Clock slewing or suspend can trigger this; inspect the clock before
restarting the reader. This safeguard does not mean the node or validator has
failed. It may miss a clock jump undone between checks, and restarting does not
establish that the clock is trustworthy. Freshness can remain unavailable after
a restart until node and client timestamps no longer lie in the future.

The object contains `schema_version`, `source_state`, `sampled_at`,
`collector_at`, and nullable `node`, `replay`, `consensus`, `voting`, `rpc`,
`input`, `throughput` and `pipeline` groups. `configured_network` is the
configured label restricted to recognized values or `unknown`; it is not a
genesis-derived cluster identification. `configured_mode`, `alpenglow` and
`rpc_configured` describe configuration, not readiness. A null group can mean
disabled, not yet observed, stopped or unavailable.

`source_state` is `fresh`, `stale` or `unavailable`. A feed sample becomes stale
after five seconds, and a read failure or invalid sample makes the last accepted
data stale. Each present producer group also has `state` and `sampled_at`: when
the feed is fresh, a group older than 30 seconds is stale. Timestamp checks allow
up to one second ahead of the collector clock; missing timestamps or values
beyond that tolerance are unavailable. The nested Turbine and gossip
groups have their own timestamps and state. Clients must also expire their
display when the collector connection or updates stop. Fresh collection is not
proof of node health or catch-up.

The optional `startup` group adds the node-run start and dated observations:

- `snapshot_manifests`, only with one of the `snapshot_manifests` opt-in
  profiles: the current
  bootstrap attempt's successful full and incremental manifest parses, each
  with its own slot and parse time when known. Missing observations are not
  proof that an archive was absent. Parsing is not AccountsDB readiness or a
  successful bootstrap; a later stage can still fail.

- `snapshot_archive` or `incremental_archive`, when present: the latest archive
  processing attempt, its
  actual start/sample times, full snapshot slot and `local`/`remote` input kind.
  `compressed_input_bytes` counts compressed bytes consumed from the file or
  HTTP response. `compressed_reported_total` is a positive reported size or
  null; bytes may exceed it. `account_payload_bytes` counts fully consumed
  status-cache and AppendVec members, including data a worker later rejects.
  It excludes other tar members and is not committed-account or all
  decompressed bytes. Phase is `active`, `complete` or `interrupted`.
  The incremental child also carries `incremental_slot`, the parsed end slot
  beyond its selected full base. Only one archive child is present at a time;
  incremental bytes are never published as full-archive bytes. Complete covers
  that archive and its worker drain only, not the preceding manifest read,
  index flush, database opening or node readiness. This existing
  500 ms owner has a five-second observation freshness window. Its terminal
  date and values remain historical when stale; a fresh feed or viewer does
  not renew them. A new attempt clears the old record. AccountsDB resume,
  pre-Start failure and older producers have no archive observation. Older
  public readers ignore the distinct incremental child instead of labelling
  its bytes as a full archive.

- `recovery`: the historical startup selection time and snapshot/stored/rooted
  slots. It is not a continuously sampled database tip, recovery result or
  finality guarantee. Bank hashes and file paths stay private.
- `replay`: the current attempt's start, first input slot, optional resume
  parent and `initialized_at`. Initialization is recorded when replay enters
  its execution loop after initialization gates; it is not catch-up or voting
  readiness. A new attempt clears the old initialization observation.
- `input`: source-instance start, configured source type, RPC block-fetch
  permission and pending repair range. Bounds are independently sampled
  requested work, not completed work or a cluster-tip estimate. Inactive or
  invalid bounds are null. No percentage or ETA is derived from them.
  `repair_pending` describes the pre-decision hold and can clear when repair
  starts; false does not establish that repair is inactive or catch-up complete.

Replay and input carry their own timestamps and freshness. Recovery is labelled
historical because it records a decision made at startup. Each record must
belong to the current run; absent or invalid records stay null. The startup
group is omitted when the node source is unavailable. Strict clients need this
schema update and should continue accepting earlier responses that omit it.

#### Optional process, host and storage observations

`--public-resources` adds a `resources` group; it requires `--public-origin`.
Without the flag the group is omitted, preserving the default response shape.
Update strict clients before enabling it, while preserving support for the
shape without the optional group.

Optional `resources.go_runtime` exposes six already-collected Go runtime
gauges with the original diagnostics-cycle date and observation state:
`heap_bytes`, `go_bytes` (MemStats.Sys minus HeapReleased), `next_gc_bytes`,
`gc_count`, `memory_limit_bytes` and `goroutines`. All are exact decimal strings.
This child is omitted for older/invalid samples and when public resources are
disabled. Managed Go memory is not RSS, goroutines are not OS threads, and the
soft Go memory limit is not a process/cgroup cap. MaxInt64 disables that runtime
limit. No latency/GC-pressure estimates are copied from the private source.


The group contains its own `state` and `sampled_at`, process `rss_bytes`,
cumulative `cpu_seconds`, an optional measured `cpu` interval, and `root`,
`accounts` and `ledger` capacity summaries. These are the node process's
measurements, not the reader's. CPU intervals require two increasing same-run
sample times no more than 30 seconds apart and a nondecreasing CPU counter.
`cpu.percent_one_core` is CPU seconds / wall seconds × 100; 250% means 2.5
cores, not 250% of the host or a container limit. Source loss, stale resource
samples, resets and invalid counters remove the rate until a new interval is
available. An idle measured interval is zero; no interval is null.

Each filesystem has string-encoded `total_bytes`, `available_bytes`, `inodes`
and `free_inodes`. Optional `used_bytes` excludes reserved free blocks; an
older source may omit it. Several roles may share a filesystem: do not sum them.
Zero inode count means the platform did not report an inode capacity. A full
filesystem has zero availability; an unavailable or invalid measurement is
null. Paths, devices, process arguments and host identifiers remain excluded.
An enabled group without a valid sample reports `state: "unavailable"` and
null measurements rather than disappearing or inventing zero usage.

Optional `resources.compaction` separates configuration (`enabled`) from worker
activity. `active` covers a compaction cycle, including lock wait, at one
historical atomic state read. Optional `active_sample_started_at` and
`active_sample_completed_at` bracket that read by at most one second; they are
not cycle duration, utilization or current health. `last_completed` retains the
last returned cycle, including partial work on failure; its byte counts are not
physical I/O or filesystem free space. Older undated activity remains untimed;
invalid new timing withholds activity fields while valid cycle history may
remain. Original resource freshness and leases still apply. Upgrade strict
node-feed readers before producers emit these dates, and strict public clients
before exposing the optional fields.

The node option `--dashboard-host-resources` (`dashboard.host_resources` in
TOML) adds optional `resources.host` on Linux. It defaults off, requires the
private dashboard socket, and is disclosed publicly only when the reader also
enables `--public-resources`. The existing ten-second diagnostics owner samples
the aggregate inputs. Unsupported platforms omit the aggregate host group.
Readers never perform these OS reads.


`host.scope` is `visible_host`: these are the kernel's visible aggregate host
observations, not this process's usage, a container limit or a cgroup allowance.
The group has its own `state`, `sampled_at`, `capture_started_at` and independently
nullable `cpu`, `load` and `memory`. Its clock is separate from the parent
process resource sample; a fresh host can coexist with unavailable process
values. A dated attempt can have all three scopes unknown. Captures exceeding
one second suppress all values and reset the CPU baseline. The budget stops
subsequent reads; it cannot cancel a kernel read already blocked in progress.

Host `cpu` reports its own `observed_at`, online CPU count and optional measured
window. Window percentages are ordered user, nice, system, idle, I/O wait, IRQ,
soft IRQ and steal; guest ticks are not double-counted. `busy_percent` sums
user, nice, system, IRQ and soft IRQ, excluding idle, I/O wait and steal. The
interval requires the same online CPU set, nondecreasing counters, a nonzero
total delta and a source-measured duration no greater than thirty seconds.
Warmup, reset, stale host/CPU observations and source loss remove the rate,
not fabricate zero. The client also expires it between frames. Serialized UTC
dates may reverse after a wall-clock correction; they never replace the source's
monotonic elapsed duration.

`load` contains one-, five- and fifteen-minute averages plus exact string-encoded
`runnable` and `tasks` counts. Averages are not percentages. `memory` contains
exact byte strings for total/free and nullable available, buffers, cached,
reclaimable slab, swap total/free, dirty and writeback values. Missing available
memory is not estimated by adding overlapping categories. Host, process and
storage observations have independent freshness. No OS paths or host identifiers are
added to the public allowlist.

The public replay group includes the attempt, local completed block and work
counters, frontier, rewinds and durable slot. The frontier includes ordered
skips and can rewind; it is not a completed bank or verified finality. The
durable slot is a local rooted-state observation, not proof that every preceding
slot had a directly matched local certificate. The separate consensus group
reports the verified tracker's direct-finality slot and block ID when available;
that block ID is not a bank hash. The optional nested `certificate_outcomes`
group is described below; it is published only for an Alpenglow validator
observation whose current voting vote account matches its fixed subject. Its
own timestamp and state can be stale independently of the replay group.
The optional `leader_outcomes` group similarly requires a matching current
validator identity and has its own freshness state. `next_known_assignment`
is a separate local schedule observation, described below. It can be available
on a non-voting node and does not establish voting or leader readiness.

Voting data is published only for a validator-mode observation bound to the
current identity. Eligibility and admitted stake refer to the observed epoch
set, not total delegated stake. Activity counters use `voter_instance` scope
and can span identity changes. `network_landed_votes` counts verified network
certificate evidence across multiple vote types.

RPC reports serving
and the last published slot, not readiness. When valid monitoring is available,
its optional `activity` object adds `started_at`, string-encoded `completed`,
`errors` and `active` totals, six aggregate duration buckets and a bounded list
of method names/counts. Unknown names, duplicate rows, overflow or inconsistent
histogram totals suppress the activity object rather than reporting partial
totals. This build instruments nine methods. Other registered methods are not
included in these totals until their counters and allowlist are added. Older node feeds
without a monitoring start time still provide listener/publication observations.
Strict clients must accept the optional activity object and its absence.

`input` describes the ordered source and optional Turbine and gossip samples.
Turbine counters reset with the receiver session; use its `started_at` and
`session` when comparing samples. Gossip counters belong to the gossip client
instance, which can span receiver reconnects and identity changes. Its known
peer counts are local contacts, not verified active cluster population or
connected stake. `pipeline` is available for validator mode when sampled. It
reports local TPU ingress, deduplication, signature verification, scheduler and
optional QUIC counters. `scheduler_buffered_total` is cumulative;
`scheduler_queue_depth` is a nullable current gauge. The independently sampled
counters are not a disjoint transaction waterfall and do not prove inclusion.

Two optional decimal-string fields preserve the narrower queue outcomes:
`scheduler_expired_removals` counts Cleanup removals classified expired by the
current bank; `scheduler_receive_evictions` counts queued entries displaced by
Receive admission. Rebuffer replacements are not included in the latter.
Neither is a unique-transaction loss total, latest-block cause or a balanceable
stage. They share the pipeline's service start and sample date. Both are absent
if an older/private producer omits either value; measured zero is `"0"`, not
absence. Clients should accept older responses without the pair; update strict
clients with the reader when enabling the new shape.

The optional `input.turbine.diagnostics` object adds observed-header and
reconstructed session high-water slots, block-emission attempts, recovered
data shreds, parse/assembly errors, active/evicted assembly slots, ignored
old/completed/currently-completing shreds and signature-verification/cache counts. Slot fields are
nullable decimal strings: known slot zero is not missing evidence. A complete
shred set can fail decoding, and an observed header can fail assembly. Neither
maximum expires on silence or slot reset, so neither is a current cluster tip,
catch-up verdict, executed block or certified chain position. A new receiver
session resets both. Header observations come from the assembler: on the
network path they follow version/leader/signature checks, while future shreds
written only to the catch-up spool do not reach this observation until hydration.
They are not the maximum of every parsed packet. Block-emission attempts include a blocked send abandoned
on shutdown; they are not successful replay or selected-block counts.

The optional `admission` object counts parsed shreds rejected for a version
mismatch or a failed lookup when a leader source is configured. These are
receiver-session occurrences before signature authentication and can include
duplicates.
An older producer omits the object; measured zero is `"0"`. A zero missing
leader count does not establish that leader lookup was configured.

The optional `peer_payload_bytes` decimal string counts peer UDP payload bytes
reported accepted by valid retransmit batch results, including accepted
prefixes returned with an error. It excludes auxiliary shred-receiver copies,
protocol headers and invalid sender over-reports. It is cumulative within the
receiver session, not delivery confirmation or a bandwidth measurement. An
absent retransmitter or older producer omits the field; an observed relay with
no accepted payload reports `"0"`. The existing ten-second owner sample and
freshness gates apply. These aggregate diagnostics use the ordinary public
profile, not the separate socket-memory resource opt-in. Use a matching reader
and compatible client; no additional endpoint or polling worker is required.

Its nullable `repair` object includes request/response/late/timeout/error
counters plus outstanding requests, known peers and peers with a matched
response in the last sixty seconds as gauges. Timely and late responses are independent of the timeout counter;
late responses may have already timed out. Repair is null when not configured,
not a zero-error health verdict. No peer addresses or raw packet data are
published. All diagnostics inherit the parent receiver's source time, session
and freshness. Sampling remains on the existing ten-second owner cycle,
independent of viewers. Older feeds without the optional object remain valid.

Public `throughput` uses up to 16 distinct replay observations and 15 intervals.
It divides same-run cumulative recorded local replay work by the time between
replay observation timestamps. Repeated feed copies do not create new intervals or
extend the window. Work later rewound remains counted. These rates are not
cluster TPS or finalized transaction rates. The window clears on stale or
inactive replay, a gap over three seconds, counter saturation or rollback, and
run, identity, genesis or replay-attempt changes. Until two new observations are
available, rates are null. History is in memory and does not survive a reader
restart.

Public 64-bit counts, slots, byte sizes and compute units are decimal strings;
durations and rates are JSON numbers. Timestamps use UTC RFC 3339. Missing
measurements are null, not zero. The public JSON object is bounded to 32 KiB;
the local node-to-reader snapshot is separately bounded to 33 KiB. Update the
reader before the producer when rolling out the larger private-feed limit:
older readers reject private snapshots above 32 KiB.

## Private WebSocket contract

Connect to `ws://127.0.0.1:8080/websocket`. The server sends an initial snapshot as
separate JSON messages, then changed values about once a second:

```json
{"topic":"summary","key":"version","value":"0.0.0-dev"}
```

Each message replaces the previous value for its `(topic, key)` pair. On a new
connection, discard the old cache. Apply messages in order. The messages in a
snapshot are not a transaction or an atomic bank view.

The envelope and fields described below are specific to Mithril; other dashboard
protocols are not interchangeable with this API.

| Topic and key | Value and source |
| --- | --- |
| `summary.version` | The node binary's version, from `pkg/version` |
| `summary.commit_hash` | The node binary's embedded commit or Go build revision; `unknown` when absent |
| `summary.identity_key` | Current identity holder's public key; `null` until that holder is available. Refreshes after identity swaps |
| `summary.is_alpenglow` | Configured protocol, independent of voting and network label |
| `summary.cluster` | `mainnet-beta`, `devnet` or `testnet` when the observed genesis hash matches a known hash; otherwise `unknown`, regardless of the configured network label |
| `summary.startup_time_nanos` | Start of this node run, as Unix nanoseconds in a decimal string; not host boot time |
| `summary.server_time_nanos` | Collector sample time as Unix nanoseconds in a decimal string |
| `mithril.status` | Versioned source status and Mithril-specific progress described below |
| `mithril.runtime` | Node Go memory and runtime pressure samples |
| `mithril.resources` | Node process RSS/CPU and filesystem capacity by storage role |
| `mithril.replay` | Current replay attempt and latest successfully executed block |
| `mithril.consensus` | Verified certificate progress, Votor ingress and optional voting observations |
| `mithril.rpc` | Listener activity, published slot and bounded per-method counters |
| `mithril.recovery` | Reconciled state selected at startup, before replay initialization |
| `mithril.snapshot_archive` | Optional dated full-archive byte counters and terminal phase; not whole bootstrap completion |
| `mithril.incremental_archive` | Optional dated incremental-archive counters, full base and incremental end slot; mutually exclusive with the full archive |
| `mithril.block_source` | Ordered input progress and cached Turbine/repair counters |
| `mithril.pipeline` | Cached local TPU, signature verification and scheduler counters |

Node-derived summary values are `null` before the first valid sample, except
`summary.cluster`, which is `unknown`. The collector's clock remains available.
The observed genesis hash comes from bootstrap; this label is not a separate
cryptographic attestation of the chain.
Optional startup-stage topics remain absent from an initial snapshot
when unavailable. If a previously sent topic disappears, the current private
reader sends one `null` value to clear connected viewers' cached value.
All 64-bit integer counters, byte sizes, slots and nanoseconds use decimal
strings to avoid JavaScript integer rounding. Durations use seconds as JSON
numbers. Timestamps use UTC RFC 3339. A missing measurement is `null`, not zero.

### `mithril.status`

| Field | Meaning |
| --- | --- |
| `schema_version` | `1`, the version of this Mithril payload |
| `source_state` | `unavailable`, `fresh` or `stale` |
| `run_id` | Node run identifier; changes when the node starts again |
| `sampled_at` | Node feed observation time in UTC RFC 3339 format |
| `collector_at` | Collector observation time in UTC RFC 3339 format |
| `phase` | `starting`, `bootstrap`, `replaying` or `stopping`; describes the lifecycle, not readiness |
| `network` | Configured network label; independent of `is_alpenglow` |
| `genesis_hash` | Genesis obtained during node bootstrap; `null` until available |
| `mode` | Configured `verifying` or `validator` mode, not proof that a voter is enabled or eligible |
| `rpc_configured` | Whether the node was configured with an RPC port; not proof that the listener started or is healthy |
| `replay_frontier` | Ordered frontier from the same replay observation as the attempt and rewind count, as a decimal string. Includes known skips and may move backward during recovery |

`replay_frontier` is not a completed bank slot, received-slot high-water mark or
verified finality. It is unavailable before the replay attempt, while a
rewind is restoring its parent, while stopping, and when the frontier is zero.
This release does not report catch-up lag or readiness.

`fresh` means the collector successfully read a valid feed sample no more than
five seconds old. A stalled replay loop can still have a fresh feed. A read
failure, invalid response or expired sample marks the last accepted data
`stale`; it does not turn missing metrics into zeros. Before any accepted sample,
the source is `unavailable`. Neither state diagnoses why a node stopped.

A new run replaces all node fields, including values that have become unknown.
For the same run, an older sample cannot replace a newer one. Clients must apply
the source state to all node-derived summary fields, not display stale values as
current. Clients must also expire their display if the collector connection or
updates stop; a cached `fresh` message does not remain fresh indefinitely.

### Measurement age and capabilities

`source_state` describes the feed, not every producer. Keep each group's own
timestamp visible. Runtime and resources normally update every ten seconds;
they can remain unavailable until the first sample. CPU rates need two valid
samples. Runtime pressure summarizes about sixty seconds. Turbine, gossip and voting
observations come from their respective diagnostic cycles and may stop when
those loops stop. The pipeline timestamp records when the node copied its
counters, not when a worker last made progress. Service attachment, detachment
and counter copies are not one atomic observation.
Treat runtime/resources, Turbine, gossip, pipeline or voting older than thirty seconds, and runtime
pressure older than two minutes, as stale even when the feed is fresh. These are
display expiry thresholds, not validator failure diagnoses.

Replay timestamps advance with loop activity. An unchanged replay timestamp
shows how long no recorded activity has occurred; it is not a measured cluster
lag. Recovery is a fixed startup selection and does not expire as if it were a
heartbeat. RPC and consensus scalar counters are copied each feed sample.

Use `mode`, `is_alpenglow` and `rpc_configured` for context. A `null` group
means no observation is available: it can be disabled, not started yet or already
stopped. Configuration and lifecycle provide that context. Do not call a node
healthy just because its feed is fresh, RPC is serving, or a voter is configured.

Optional `node.elapsed_nanoseconds` is the monotonic run duration at the feed
sample, encoded as a decimal string. It is absent when the original monotonic
start is unavailable or the duration cannot be represented. Clients must not
derive this duration from wall-clock dates.

### Runtime and resources

`runtime` reports `heap_bytes`, `go_bytes` (Go-managed memory minus released heap),
`next_gc_bytes`, cumulative `gc_count`, the Go soft `memory_limit_bytes` and
`goroutines`. A memory limit of `9223372036854775807` is Go's unlimited sentinel,
not available physical RAM. `pressure` has its own timestamp and window length;
it reports scheduler latency p99/max, GC CPU fraction, maximum GC pause,
allocation bytes per second and mutex wait seconds per second. These values
reuse the existing diagnostics sampler; dashboard readers do not reset it.
The GC CPU fraction is a share of non-idle Go CPU, not host CPU utilization.

The process fields in `resources` belong to Mithril, not the dashboard reader. `rss_bytes`
uses Linux `/proc/self/statm` and is `null` where unavailable. `cpu_seconds` is
cumulative user plus system CPU time. CPU utilization requires two same-run
samples: divide the CPU-time difference by elapsed wall time; one full CPU is
100%, so a multithreaded process can exceed 100%.

`root`, `accounts` and `ledger` contain `total_bytes`, `available_bytes`, `inodes`
and `free_inodes`, with optional `used_bytes` that excludes reserved free blocks.
Available bytes exclude filesystem blocks reserved from
unprivileged users. Roles may share a filesystem; do not add their capacities.
A failed or unconfigured path yields `null`. Paths, device names, mount options,
hostnames, environment variables and process arguments are not exported.

### Replay and recovery

`replay` reports the attempt number, start time and requested `start_slot`.
`resume_parent` is available when replay received a resume state. `ready_at` is
set only after initialization and block-source construction, just before the
loop begins. It does not mean caught up. The phase is `starting`, `running`,
`stopped`, `halted` or `cancelled`. Counters reset at each attempt.

`executed_blocks`, `skipped_slots` and `rewinds` count local work; executed blocks
may later be discarded by a fork switch. `durable_slot` is the last rooted slot
observed by replay. Successful folds and forced flushes publish it with their
settled observations, without waiting for another block. It is not a fresh disk
read, and the last received feed sample is not a guaranteed final shutdown receipt.

`completed` describes the latest successfully executed block: slot, bank hash,
block height, epoch, epoch-relative slot index, actual slots in that epoch,
transaction count, recorded compute units, execution seconds and block-source
wait seconds. These are local replay measurements, not cluster TPS, a protocol
cost limit or consensus finality. A rewind clears this record until another
block executes. Skips do not manufacture a completed bank.

`replay.executed_compute_units` retains its wire name and accumulates
runtime-reported transaction amounts for the current attempt. Some zero-fee
no-ops contribute their compute-unit limit without executing instructions, so
neither this total nor the completed block's recorded total is a measure of
instruction execution alone. Recorded compute units are not native block cost.

The existing slot log, rolling compute-unit summaries and replay-timings JSON
continue to use the legacy replay counter. It can differ from the dashboard's
transaction counter for locally produced blocks, which do not populate that
replay-only field. If a bank's transaction counter saturates, that bank stops
accumulating and the attempt's totals can be incomplete. The attempt-level
saturation flag remains set and throughput rates become unavailable; it does not
identify whether the latest completed block itself saturated.

`recovery` copies `snapshot_slot`, optional stored slot/bank hash and rooted
slot/bank hash after startup reconciliation. `selected_at` identifies that
observation. Later attempts and their initialization outcome are in `replay`.
The API does not read recovery history files or expose signing state.

### Certificate outcomes

The optional `replay.certificate_outcomes` records observations for one fixed
vote account across a rolling window of at most 512 selected-and-folded carrier
records in the current replay attempt. `provenance` is `selected_and_folded`:
the carrier survived local selection and its matching bank was folded into
rooted state. This is not a direct certificate of finality for every carrier.
The account is the subject, not proof that the current identity signed a vote.
The group is absent before an observation is available. Its `sampled_at` is
the recorder's latest publication time, not a liveness heartbeat.

`has_window` says whether `first_carrier` and `last_carrier` are meaningful.
`carriers`, `gaps` and `slots_without_carrier` describe coverage in that window;
slots without a carrier are not classified as skipped or missed. `pending` is
unsettled local evidence, while `dropped_this_attempt` is cumulative over the
attempt. `coverage_incomplete` includes unknown or conflicting evidence and
missing coverage; after certain evidence loss it remains true for the attempt
even if the affected records leave the window. Counts reset with the attempt.
Before publishing, the reader checks the carrier bounds, count partitions and
voter binding. Inconsistent observations are withheld rather than changed to
zero; pending and attempt-wide dropped counts remain separate from the window.

`reward_included`, `reward_excluded`, `reward_unknown` and `reward_ineligible`
classify observed reward-carrier evidence using the target epoch's admitted
vote-account set. Unknown is not exclusion, and inclusion does not establish
payment or a locally cast vote. `final_fast` and `final_slow` count the first
observation of each target within the retained carrier window, not the first
ever or the strongest later certificate. `fast_member`, `fast_nonmember` and
`fast_membership_unknown` describe that account's membership in those first
observed fast certificates. `final_duplicates`, `final_variants` and
`final_conflict` expose repeated or conflicting evidence; gaps can hide an
earlier observation. These are partial window counts, not epoch-wide scores
or percentages. A percentage for known
eligible outcomes uses included / (included + excluded), with its denominator
and incomplete-window status; unknown and ineligible targets do not become
exclusions.

The v3 profile can expose `consensus.safety_fault` from the sampled engine.
An omitted flag is unknown, not false; a reported false is not an overall
readiness verdict. A sampled flag may miss a fault before rapid shutdown.

### Local account balances

Optional `replay.account_facts` describes the configured identity and vote
account at a successful local bank. The replay owner samples after execution, at
most once per second unless subjects or epoch change. It reads at most two
distinct keys using the current-bank/parent precedence; native shared reads
avoid copying account data. Nothing is read from a mutable bank by a viewer.
An unconfigured ephemeral identity does not enable this observation.

The sample retains its own successful local-bank slot, hash, epoch and capture
time, even while the next completed slot advances. It is not a finalized or
durable balance. Known zero remains zero; missing, dummy, mismatched or failed
account reads produce nullable balances. Lamports use exact decimal strings.
An explicitly configured vote key can remain visible before admission, even
when its account balance is unavailable. Private keys, authority records and raw
account bytes are excluded. A conflicting current voting subject suppresses
all vote fields, including the optional children below.

New attempts and rewinds clear the observation; skip/phase updates do not
refresh its timestamp. The public projection checks current identity, attempt
time and completed-bank bounds. Full nodes can expose their configured identity
balance without a voter. Older producers may omit the observation, and vote
fields require a configured vote account.

New producers also include optional `stored_commissions` for the configured
vote account, with nullable `inflation_rewards_bps` and `block_revenue_bps`.
These are exact JSON integers from 0 to 65535, in basis points (100 BPS = 1%).
Legacy layouts have no separate stored block commission, so that field stays
null. Missing or unreadable headers leave nulls; older producers omit the
object. A conflicting current voting subject suppresses the object together
with the vote balance. The same two configured-account reads supply these
fields; no additional account scan or full variable-length vote-state decode
is performed.

These fields describe only the stored header in the sampled bank, not complete
vote-state validity, admission or an epoch's effective payout rate. Raw values
above 10000 are retained rather than capped. The client displays percentages
with the same two-decimal precision and labels them as stored commissions.

#### Stored header and funding rules

Optional `stored_vote_header` contains nullable `node_identity`,
`pending_delegator_rewards_lamports` and `bls_registration_present`. A stored
zero public key is a known raw value. BLS presence reports only a complete
fixed-header option; it does not validate the compressed key or authorize
signing. Pending delegator rewards are reserved delegator funds, not expected
validator income or available balance. These values reuse the same account
reads as balances and commissions.

Optional `funding_rules` retains the account sample's slot as `reference_slot`,
the bank's `vat_filtering_active` and `vat_charge_gate_active` feature states,
and exact `rent_minimum_lamports`, `burn_lamports` and `floor_lamports`. The
floor uses the runtime's saturating V4 rent-plus-burn rule at that reference
slot. Unknown rent, features or schedule omit the group; they do not mean zero
cost or inactive VAT. The boundary charge condition includes its actual
partitioned-rewards feature condition, not just the burn-rate helper.

`meets_floor`, `shortfall_lamports` and `surplus_lamports` are either all null
or an exact numerical comparison with the sampled vote balance. Equality has
known true and two zero differences. This comparison does not validate a vote
account or predict admission. Inactive feature conditions remain explicit;
a below-floor number does not become an admission warning when filtering is
inactive. A charge condition does not prove an actual debit occurred.

#### Vote ticket capacity

For a configured validator with a vote account, the public projection adds
`ticket_capacity_no_income` using the validated account facts above. `tickets`
is the exact decimal-string result of
`floor(max(vote_balance - rent - pending_delegator_rewards, 0) / burn)`.
The calculation uses one sampled bank and adds no account reads. It assumes
the current ticket cost and no new income. It does not add an already-paid
epoch or establish membership, eligibility, a paid seat or future admission.

`status=known` includes known zero. Otherwise `tickets` is null, with the first
applicable status in this order: `rules_unknown`, `charge_inactive`,
`zero_ticket`, `balance_unknown`, `liability_unknown`. Missing liability is
unknown even when the balance is already below rent. A zero ticket does not
mean infinite capacity. Malformed rule/header children remain unavailable.
Full nodes and missing or mismatched vote subjects omit this child; producers
without the extension remain supported. This calculation is not a readiness
verdict.

#### Current and next epoch material

Optional `epoch_stake_sets` has provenance `local_epoch_stakes_cache`. `current`
refers to the sampled bank's epoch; `next` refers to E+1 and is absent only when
that epoch cannot be represented. Each row retains its own `generation` and
`material_known` flag. The two generations are individually coherent, not an
atomic pair or a cryptographically ranked signer set.

Known material reports `member`, `selected_stake_lamports` and
`total_stake_lamports`; membership means positive configured-key stake.
Missing material leaves all membership,
stake and metadata values null, even when a cleared generation is known.
Known absence is a nonmember, not missing material. A positive-stake member
can have no metadata. Optional metadata contains the selection node identity,
its match with the configured identity, and `selection_balance_lamports`.
That balance belongs to the cached selection/snapshot, not the local bank
balance above. Raw cache totals can be incomplete, so no stake share is inferred.

Optional `payer_reference` retains the first slot of the bank epoch and the
runtime's leader-schedule epoch for that slot. `member` is known only when
that reference matches a known captured current/next row. A different required
epoch remains unknown without a third lookup. The debit path uses its actual
boundary block's slot, so this reference is not a charge receipt.

All new uint64 values are exact decimal strings; nullable booleans distinguish
unknown from false. The reader checks reference slots, current/next epoch
keys, null patterns, metadata binding and numerical comparisons. Malformed
children are omitted without discarding otherwise valid balances. Rewind,
attempt and voter-subject rules remain those of the parent account sample.
Full nodes with a configured vote subject can expose these facts without
running a signer.

### Settled ordinary reward health

Optional `replay.reward_health` is a fixed identity/vote-account summary derived
on the replay owner from the existing retained selected-and-folded carriers.
It does not scan accounts, rebuild validator sets or add another history store.
`through_slot` is the successful fold frontier. Its mature-target window ends
eight slots earlier and covers at most 128 targets. It is not a delinquency
label.

Each target uses its captured target-epoch admission and identity. `included`,
`excluded`, `ineligible`, `unknown` and `unobserved` partition the stated window.
Missing carriers and cold/conflicting admission evidence are not exclusions.
The newest missing target forces `health=unknown`, even when an older retained
inclusion exists. A strict `no_inclusion_128` result requires 128 known eligible
exclusions with complete coverage. Non-admission is reported separately.

`latest_resolved` and `latest_included` are nullable retained references, not
lifetime high-water marks. They can disappear when their carriers age out of
the 512-entry ring. Target zero remains known zero. These are certificate
membership observations, not proof that this process signed or received funds.
Run/attempt, current identity/vote binding, fold frontier, source time, ranges
and counts are validated before publication. Stale source/voter/settlement or
stopped replay clears the current health claim to `unknown`; dated references
remain historical context. Clients must age the claim between frames. Non-voting
nodes and older producers omit the group.

### Leader outcomes

The optional `replay.leader_outcomes` describes schedule assignments for the
fixed startup identity within the last 512 slots ending at a locally folded
prefix. The public group is withheld when that identity no longer matches the
current node and voter observation. `produced` means a scheduled slot has a
selected-and-folded block; it does not mean this process forged the block.
`selected_fork_skipped` requires a matched selected parent-child edge and is
not a globally certified skip. `scheduled_unknown`, `unknown_schedule_slots`
and `unknown_ancestry_slots` expose missing evidence. `covered_start` and
`covered_end`, when `has_covered_range` is true, form an envelope that may
contain holes; `coverage_incomplete` signals partial coverage. These counts
also inherit the certificate recorder's attempt-wide evidence-loss flag, so
`coverage_incomplete` can remain true after the affected slots leave the window.
They are not epoch-wide rates or percentages and include no future assignments.
Before publication, the reader checks the window against the durable slot and
rejects counts or coverage ranges that cannot fit it. A gap share derived from
`selected_fork_skipped / (produced + selected_fork_skipped)` requires that
known-outcome denominator and incomplete-coverage status; unknown slots are
neither successes nor failures.
The selected-and-folded label is not independent direct-finality proof and
does not establish full-epoch coverage or public-cluster performance.

### Next known assignment

`replay.next_known_assignment` searches the captured local current- and
next-epoch schedules for the observed identity. It records `sampled_at`,
`identity`, the inclusive `reference_slot`, `searched_through_slot`, and a
nullable `next_known_slot`. The reference is the slot after the latest consumed
replay outcome; it is not an estimate of the cluster tip. The search bound does
not establish continuous schedule coverage. Missing or partial schedules can
hide an earlier assignment, so a hit is the earliest known assignment, not a
guaranteed next leader slot. No hit means unknown, not no future leadership.

Replay captures this observation after completing or consuming a slot, without
per-viewer schedule scans. A new attempt or rewind clears it; an identity
mismatch suppresses it until a matching observation arrives. Its public
freshness follows its own timestamp. It provides no wall-clock ETA and no claim
that the node is caught up, eligible, or ready to produce the assigned block.
The reader withholds a hit outside its inclusive search bounds or outside the
current replay-attempt timing. The full search bounds are available in either role.

### Consensus and voting

`consensus` reports observed blocks, replayed slots, accepted certificates and a
latched safety-fault flag. `direct_finalized_slot` and `direct_finalized_block_id`
come from the verified chain tracker when it has a hash-bound direct-finality
observation. The block ID is not a bank hash. This does not claim that replay,
durable storage or the RPC publication has reached that slot.

`notarized_slot` is the engine-session maximum slot from accepted notarize,
fallback-notarize, fast-finalize or skip certificates. It remains `null` before
the first such observation; an accepted slot zero is `"0"`. Standalone slow
finalize and genesis certificates do not advance it. Retention/pruning does not
lower this maximum; a new engine starts without an observation. It is not a
replayed, durable, directly finalized or estimated cluster-tip slot. Its
`sampled_at` dates the monitoring observation, not the certificate's arrival.

`receiver` reports cumulative Votor QUIC ingress for the receiver instance,
including across identity changes and replay attempts. Datagrams are counted
after rate limiting; `rate_limited_datagrams` is separate, not a subset to
subtract. Decoded messages and `last_message_at` precede admission and can
include duplicates or rejected proofs. Decode/read errors and rejected
connections are counts, not raw errors or remote addresses.

The public `consensus.receiver` child is optional and inherits consensus sample
freshness in both node roles. Missing means unavailable; an observed empty
receiver has zero counters and a null message date. Refreshing the consensus
sample does not change the last-message date or establish listener health.
These are not verified votes, Turbine, gossip or host-interface statistics.

`voting_enabled` means the engine has a voter instance. Its `voting` object stays
`null` until the owner loop publishes an observation. `identity` and `vote_account`
identify that observation's binding. `root_slot` and `epoch` select its admitted
epoch validator set. `key_eligible` compares identity, vote account and the public
BLS key; it stays `null` when that set is unavailable. An eligible node has a
zero-based epoch-set `rank` and admitted stake in lamports. Rank is not a dashboard
performance ranking. Admitted stake is not total delegated stake.

`joined_live` records that the voter passed its initial live-window check or
reached its first accepted/enqueued vote. It is a sticky voter-instance milestone,
not a current catch-up or health verdict. Older feeds omit it; measured false
remains distinct from missing. It does not assert that every later vote is
allowed. Safety, finality, reservation and per-vote checks still govern signing.
`votes_cast_this_run`, queued/dropped
messages, peer send/queue errors, send timeouts and connection counts retain the
existing voter's definitions. Counters span that voter instance, including
identity changes within a node run; do not attribute an entire run to its latest
identity or compute rates across run changes.

`network_landed_votes` counts unique votes in the voter's history whose rank
appeared in an exact BLS-verified certificate proof received over Votor QUIC.
In reserved mode the history snapshot can still be queued, so the counter is
not proof that the exact history was durably written. The latest slot
is `null` before such evidence. `last_network_landed_at`, when present, is the
local time the voter instance recorded its latest match. It is omitted when
unobserved or when its time and associated evidence cannot be validated; older
readers may not expose it. It can describe an earlier identity after a swap and
is not the chain's timestamp, a delivery latency or a current health verdict.
This counter includes multiple vote types. It is not a vote-inclusion rate or
a count of missed votes. Certificate outcome counts cover only the observed
window.

### Block source and repair

In shreds-only mode, repair can bootstrap a locally covered resume frontier
from a confirmed-tip signal when peers and a nonzero local frontier leader are
known, even before a verified live shred edge exists. Moving confirmed tips
can extend that repair window. This does not enable RPC block fetching or
replace per-shred verification and certificate-backed skip checks; RPC-enabled
admission and its gap threshold remain unchanged.

`block_source` is unavailable before the source is attached and after teardown.
`configured_source` identifies the selected input. `next_slot` is its next ordered
emission, not a completed bank or verified tip. `live_connected`, `live_active`
and `rpc_block_fetch_allowed` are separate observations: a connected receiver
does not imply that replay has switched to its live stream. Repair catch-up can
run while RPC block fetch is disabled. `repair_catchup_pending` and the optional
`repair_catchup_from`/`repair_catchup_until` range describe that local work, not
cluster lag. These scalars are not an atomic bank view.

`turbine` is `null` before its first ten-second diagnostic sample and while
disconnected. Its counters reset on receiver reconnection. Use the source's
`started_at` together with the receiver's `session` when computing differences;
never subtract across sessions. Packets, parsed data/coding shreds, parse and
signature errors, useful/spooled repair shreds and `last_packet_at` describe
ingress. Parsed shreds are not verified blocks or proof of replay progress.

The optional `repair` object reports requests, matched timely responses, matched
late responses, timeouts and errors from that receiver session. It is `null`
when repair is not configured. Counters do not expose peer addresses or raw
packet data. Sampling reuses the receiver's existing diagnostic read; the feed
does not call receiver code, scan slot maps or reset counters.

The optional `gossip` object is the live stream client's cached aggregate. Its
`started_at` identifies the client counter lifetime, which can span receiver
reconnections and identity changes. Peer counts are locally known contacts,
not verified active population. Counters and peer counts are sampled
independently and do not expose peer addresses. Gossip messages and contacts
are different measures.

### Recent TVU reception

The optional `block_source.reception` group is independent of the ten-second
receiver counters. Monitoring enables a fixed 64-distinct-slot observer and a
one-second sample in the existing stream-owner loop. It records only
leader-authenticated TVU-socket packets after the existing admission path,
before spool/assembly. Dedicated repair traffic and disk hydration do not
refresh it. Timestamps mark userspace packet-processing entry, not kernel
arrival or completed authentication. A successful retransmit admission is not
proof that every hop signature was checked. Internal comparisons retain Go's
monotonic readings; exported times are UTC.

It reports receiver start/session, producer window milliseconds, nullable
`slot`/`observed_at`, retained distinct-slot count and `coverage_limited`.
The current window is 4.8 seconds. Known slot zero remains zero. A full capture
discards new distinct events and suppresses the reference until every discarded
event has aged out; overflow cannot silently underestimate the maximum.
Out-of-order processing never moves a slot's receipt time backwards.

Public `input.reception` validates run/source/session/time binding and is
omitted for older producers. Sampling older than three seconds is stale; its
reference is cleared at the receipt-window boundary even while the feed stays
fresh. Clients must also expire it using elapsed monotonic time between frames.
This estimate can still reflect a valid leader's misleading header or an
incomplete local network view. It is not a certified cluster tip, proof of
catch-up, ETA or permission to vote. Older producers may omit this observation.

Clients can derive an exact unsigned execution distance from this API; no extra
telemetry or query endpoint is needed. Compare the last successful execution
slot with the recent receive slot and preserve the
`ahead`, `behind` or same-slot direction. Both must be fresh, execution must
follow the current receiver start, and source/replay-attempt times must agree.
The execution is allowed at most three seconds of age. Its slot, timestamp,
attempt and rewind count provide context. Receipt expiry, receiver
replacement, stopped replay or rewind without a new execution clears the
comparison. This local distance is not a global catch-up milestone or ETA.

### TPU pipeline

`mithril.pipeline` is null until the local TPU/scheduler service publishes a
sample and after it stops. Its `started_at` marks the service instance. Ingress,
deduplication and signature-verification fields count packets and bytes, not
finalized transactions. Scheduler `buffered_total` is cumulative, while
`queue_depth` is a nullable current gauge. `accepted` counts local forge
admissions; retried scheduler decisions mean the counters do not form a
disjoint waterfall. QUIC connection and stream counts are separate gauges and
may be unavailable. Samples are not atomic across the independent producers.

Private scheduler `dropped_expired` and `evicted_capacity` retain their existing
names and decimal-string encoding, with optional presence for legacy producers.
The node supplies both, including measured zeros. Public projection withholds
both if either is absent/null and preserves all unrelated pipeline facts.

### RPC

`rpc.serving` says the HTTP serving loop is active, not that the node is caught
up. `published_slot` and `published_at` copy the last `SetSlotCtx` publication;
they are `null` before publication or after it is cleared. The slot has the
existing RPC's semantics; monitoring does not change its commitment behavior.
`history_recovery_pending` is the server's existing recovery flag.
`monitoring_started_at` is the start of this server's counter instance; repeated
monitor enable calls do not reset it. A zero time means monitoring is absent.

Each `methods` entry has its name, cumulative completed and errored calls,
active calls, and six non-cumulative `duration_buckets`: at most 1 ms, 10 ms,
100 ms, 1 s, 10 s, then greater than 10 s. The bins count calls, not seconds.
They cannot provide an exact latency percentile. Method execution includes its
handler work, not HTTP parsing or response delivery. Batch members and JSON-RPC
notifications count separately. Invalid envelopes, unknown methods and other
requests rejected before method invocation are excluded. Request bodies,
parameters, responses and error strings are never stored by monitoring.

Compute rates from differences between same-run, same-monitoring-start samples. Zero means no observed
invocations since monitoring started; it is not proof the method works. The
monitor attaches only when the dashboard feed starts successfully and covers
only the explicitly instrumented public RPC methods.

### Client requests

The only client request is a ping:

```json
{"topic":"summary","key":"ping","id":1}
```

The response repeats the `id` and has `value: null`. Request IDs are unsigned
64-bit integers; browser clients should choose IDs that their JSON implementation
can represent exactly. Unsupported requests close the connection. There are no
administrative routes, arbitrary RPC forwarding, file reads or log endpoints.

## Resource limits

The node copies bounded status observations once per second into a snapshot
capped at 33 KiB. The existing diagnostics goroutine samples process/filesystem resources
every ten seconds; it does not run on replay or viewer request paths. Feed readers
get cached bytes; requests never call replay, voting or account-database code. The
private feed accepts at most four simultaneous connections. For status, the
collector reads one fixed Unix socket, does not follow redirects and bounds each
request to one second and 33 KiB. The public status JSON value is capped at
32 KiB; a WebSocket frame includes a small JSON envelope, so readers should
allow at least 32 KiB plus 128 bytes per frame. Optional detail routes have
separate documented limits.

If a feed sample fails validation, JSON encoding or the size limit, the node
keeps the last accepted bytes and their original timestamp. The reader can
therefore mark them stale. The first rejection in an episode emits one `slog`
warning with `component=dashboard`, `operation=feed_sample`,
`entrypoint=private_feed` and a fixed reason: `sample_invalid`, `marshal_failed`
or `snapshot_oversized`. Only the oversize warning includes `encoded_bytes` and
`limit_bytes`. Further failures stay quiet, even if their reason changes; the
first accepted sample logs one recovery at info level. No sample fields, paths
or raw errors are logged. By default these records go to process stderr, which
systemd captures in its journal, not necessarily Mithril's rotating log file.

The viewer service caps TCP connections at 32 and WebSocket sessions at 16. A legacy
snapshot/hint session holds only its last delivered snapshot and at most one
queued ping response. The opt-in v1 bounds are described above. It sends latest values, not every intermediate sample. A socket write
has a two-second deadline. Incoming data messages are capped at 1 KiB and two per
second, with a burst of two; control frames have a separate two-per-second limit
with a burst of four. Each connection also has a 4 KiB/s inbound byte budget
with an 8 KiB burst, including upgrade headers and all WebSocket framing. This
bounds fragmented messages even when their frames carry no payload.
A slow or flooding viewer is disconnected. HTTP headers,
handshakes and idle connections also have explicit limits.

The separate process isolates browser parsing and slow writes from replay. It
still shares CPU and memory with the node. Actual node overhead must be measured
before a deployment recommendation.


### Account request and fold-record window

With `dashboard.storage_details` enabled, attaching the successful runtime AccountsDB owner opts in to fixed scalar operation counters. Recording starts at that attachment, not at process start or snapshot ingest. An optional `account_operations` object in the existing storage detail contains scope `runtime_db_requested_keys_and_committed_records`, `recording_started_at`, `started_at`, `ended_at`, and decimal-string `duration_nanoseconds`, `requested_keys`, `committed_records`.

Requested keys count each `GetAccount` and `GetAccountWithStats` call once, and each key supplied through the shared batch implementation once. Misses, duplicates, errors and canceled batch requests are included; empty batches add zero. Nil/unopened owners provide no measurement. This excludes account accesses satisfied by replay overlays without a DB call. Committed records count `len(records)` only after a successful `CommitBatch` commit, after deduplication and including tombstones. Failed folds, recovery reapplication and uncommitted overlay writes are excluded. These are account-level operations, not disk I/O, unique live accounts or transaction throughput.

The node serializes the counter snapshot and baseline update under the attached-owner mutex. Rates require two valid observations with the same recorder, nondecreasing non-exhausted counters, original monotonic elapsed time of at most 30 seconds, and wall/monotonic agreement within 250 ms. Counter exhaustion, closing/replaced owners and regressing counters cannot fabricate zero or renew an old window. Sampling gaps withhold the rate and require a new bounded interval. Independent counters are approximate under concurrent operations; the dated interval and exact definitions remain visible in existing Accounts/Overview detail views.

The existing storage/status bindings, original response and source deadlines, body budgets and opt-ins remain unchanged. Readers that lack this optional field reject the canonical new document; upgrade the reader first, then the node, with no deployment implied by local adoption. New readers retain compatibility with old documents that omit it. The UI uses existing Reads/Writes positions, labeled DB keys requested (`keys/s`) and Folded records committed (`records/s`) for this scope; ordinary legacy sources keep their previous labels.


## Physical file allocation census

The optional `physical_usage` member of private/public storage detail reports a dated filesystem-block observation. It is independently opt-in: the node requires `dashboard.storage_details` and the exact environment value `MITHRIL_DASHBOARD_PHYSICAL_CENSUS=1`. It is default off. Reader-first coordinated rollout is required because older canonical readers reject the additional optional member.

Its fixed scope is `primary_db_tree_and_secondary_account_trees`: the primary database tree (including index, bank-hash, metadata, temporary and rewound named files) and each secondary accounts subtree. It includes directory blocks. `allocated_bytes` sums filesystem-attributed 512-byte blocks; `logical_file_bytes` sums regular-file lengths. Both byte fields, `regular_files`, `directories` and `duration_nanoseconds` are uint64 decimal strings. `started_at` and `completed_at` are UTC timestamps. This is allocation attributed to tree-visible named files/directories, not exclusive device use, open-but-unlinked files, live account payload, fragmentation, capacity, or reusable free. The logical Used/Fragmented/Allocated ring remains independent and unavailable without its own valid source.

One worker per runtime DB performs at most one attempt per 60 seconds, outside DB/replay locks. It reads metadata only. Two complete matching traversal fingerprints and totals are required. The attempt allows at most 2,000,000 total entries across both passes, 32 roots, depth32 and path length4096, with a five-second observation ceiling checked between filesystem operations. A blocked kernel metadata call cannot be interrupted by that check: there is at most one worker, no new attempt overlaps it, and database close/owner retirement cancel without joining it. Active churn or a limit/error yields no total; partial counters never escape. Directory iteration differences may conservatively produce unavailable.

Absolute root components and descendant entries are opened FD-relative without following symlinks. Linux uses metadata-only `O_PATH` handles and requires `statx` mount identities; different-device and same-device nested mounts, overlapping roots, duplicate inodes, hardlinks, unsupported objects/statistics and changed metadata are refused. Unsupported kernels/platforms, including Darwin, yield unavailable. Two matching passes describe a stable bounded metadata observation, not a transactional filesystem snapshot.

The original runtime node/DB-generation binding and status/detail/source/response deadlines still apply. The physical census is withheld at 75 seconds of age. When included, its original completion time plus 75 seconds also bounds the enclosing detail deadline. Physical census and I/O observations require at least 13 seconds remaining on their original deadlines at capture. This budget covers the one-second node sample interval, the reader’s ten-second delay between acquisitions, and two one-second private requests. The original deadline still applies if polling is delayed. Duration is positive and at most five seconds, with wall and monotonic clocks agreeing within 250 ms. Failure, close and owner retirement clear the current observation. Stopped recorders cannot restart or revive through a late store. Existing storage observations remain available when census is disabled or fails.
