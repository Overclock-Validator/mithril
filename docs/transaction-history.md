# Transaction history and signature statuses

These optional features run on an Alpenglow node in `verifying` mode. Voting
and block-producing configurations reject them. Both options are off by default.
They do not provide archival backfill or query an external history provider.

## Setup

Add these settings to an otherwise working verifying-node configuration:

```toml
[consensus]
mode = "verifying"

[rpc]
port = 8899
transaction_history = true
transaction_history_slots = 16384
transaction_history_max_mib = 1024
transaction_history_reserve_mib = 2048
signature_statuses = true
```

History and signature statuses can be enabled independently. The corresponding
flags are `--rpc-transaction-history`, `--rpc-transaction-history-slots`,
`--rpc-transaction-history-max-mib`, `--rpc-transaction-history-reserve-mib` and
`--rpc-signature-statuses`. RPC must have a valid, nonzero port. The example uses
default budgets, not measured hardware recommendations. The logical history
budget must be at least 80 MiB; retention and the free-space reserve must be positive.

History lives in a separate `rpc-transaction-history` database below the accounts
directory and is bound to the genesis hash. Its WAL remains enabled independently
of the AccountsDB index-WAL setting. Use the same access controls as other RPCs.
Either optional feature makes this RPC server HTTP-only; WebSocket upgrades are
refused. Configuration must remain unchanged while the server runs.

## Block and transaction responses

```json
{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[123,{"encoding":"json","maxSupportedTransactionVersion":0}]}
```

```json
{"jsonrpc":"2.0","id":2,"method":"getTransaction","params":["TRANSACTION_SIGNATURE",{"encoding":"base64","maxSupportedTransactionVersion":0}]}
```

Replace the slot and first signature with retained data from this node. Full
responses support JSON and base64 with transaction-version checks. `getBlock`
also accepts `transactionDetails: "none"` and `rewards: false` without reading
transaction payloads. Full details are the default; unavailable payloads never
silently become summary-only responses. Parsed JSON, signature-only and
account-only block details are not supported by this profile.

Recorded metadata includes execution errors, fees, pre/post lamport and token
balances, inner instructions, resolved addresses, compute units and return data.
Complete runtime logs and per-transaction rewards are not recorded and return
null. Unrecorded cost units are omitted. Block rewards come from block history.

Confirmed and finalized reads share the published durable canonical view.
Retaining a receipt does not establish finality: its slot, block identity and
bank hash must match the node's selected durable state. Processed reads are refused.

`getTransaction` returns null when there is no retained canonical match, including
pruned and not-yet-rooted transactions. Disabled history returns `-32011`.
Damaged or ambiguous records return an error, not a partial receipt. Full block
requests fail when payloads are missing or invalid.

## Signature statuses

```json
{"jsonrpc":"2.0","id":3,"method":"getSignatureStatuses","params":[["TRANSACTION_SIGNATURE"],{"searchTransactionHistory":true}]}
```

Requests accept at most 256 first signatures within 32 KiB of parameters and
preserve order and duplicates. Omit the config or pass null for recent-only
lookup. A supplied object requires an explicit `searchTransactionHistory` boolean.

The response context is the selected replay slot. Accepted outcomes above the
durable root are `processed` with zero confirmations. Outcomes covered by the
verified durable root are `finalized` with null confirmations. The cache does
not infer `confirmed` from elapsed slots or establish finality itself.

A clean miss returns null. An unresolved miss across a retained capture gap,
recovery or conflicting identities returns `Signature status unavailable`
(`-32000`). The in-memory cache starts cold after restart; its 128 MiB logical
entry budget is not a whole-process memory limit or a guarantee that 300 dense
banks fit. Historical search requires the separately enabled history store,
even for a signature present in the cache; otherwise it returns `-32011`.
Historical results must match canonical block summaries and bank hashes.

## Resource limits and recovery

Observer replay waits for its previous history write before admitting another
capture. Oversized or incomplete captures leave gaps. Recording failures do not
change execution decisions; conversion or storage errors stop new recording,
while retention refusals allow later writes to retry.

Retention counts candidate blocks, including forks, from the newest retained or
incoming candidate. A deep rewind can leave gaps until replay catches up. A
write prunes at most 64 candidates. Logical-byte limits exclude WALs, engine
overhead, obsolete files and versions pinned by readers. Deletion does not
guarantee immediate filesystem space recovery. Opening and writing require the
free-space reserve plus one maximum batch; low space can also refuse pruning.

Two full-history responses and two signature-status responses may be in flight.
Each HTTP request can make two calls in each group, including batch members.
Admission remains held through serialization and the final flush. Mixed batches
share one fixed 30-second socket-write deadline, which does not bound CPU work,
database mutex waits or total handler lifetime.

Shutdown joins HTTP handlers, the peer-refresh loop and the history writer before
closing borrowed stores. A timeout leaves databases open until process exit.
Recovery blocks new canonical reads until account recovery, resume reconstruction
and root publication complete. Existing owned read snapshots may finish against
their previously selected view.

## Diagnostics

The existing metrics endpoint reports `transaction_history_captures_total{result}`,
write duration, pending-write start time, candidate count, logical bytes and first
and last candidate slots. A pending-write timestamp of zero means no write is
pending. Storage gauges describe recorded candidates, not gap-free coverage or
verified finality. Compare capture refusals, write duration and actual disk space
when diagnosing missing receipts. Pebble fatal storage errors can terminate the
process; this profile is restricted to non-voting nodes.
