# Block-range and multi-account queries

`getBlocks` and `getMultipleAccounts` use the node's published durable root.
Confirmed and finalized requests select that same view; processed commitment is
not supported. These methods are HTTP-only and do not query an external RPC.

## Block ranges

```json
{"jsonrpc":"2.0","id":1,"method":"getBlocks","params":[123,150,{"commitment":"finalized","minContextSlot":150}]}
```

The response lists produced slots in ascending order, with inclusive endpoints.
The end defaults to the start plus 500,000, saturating on overflow, and is clamped
to the published root. After clamping, endpoints may differ by at most 500,000.
An end below the start returns an empty array. `minContextSlot` is checked even
for an empty range.

Every slot in the selected range must have retained rooted summary coverage.
Known skipped slots are omitted; an unknown or pruned slot returns
`Block not available` (`-32004`), rather than a partial list. Summary recording
is provided by the Alpenglow block-history integration. This method does not
add backfill or change summary retention.

A listed slot does not guarantee its transaction payload is still retained.
Consumers needing full blocks must also enable transaction history and handle
unavailable payloads; see [transaction history](transaction-history.md). A scanner
should persist the last consumed block identity separately from the last scanned
slot, verify that identity on resume, and use `minContextSlot` before advancing
through a requested end beyond the node's current root.

## Multiple accounts

```json
{"jsonrpc":"2.0","id":2,"method":"getMultipleAccounts","params":[["11111111111111111111111111111111"],{"encoding":"base64","commitment":"finalized"}]}
```

Requests accept up to 100 public keys, including an empty array, within 16 KiB of
parameters. Results retain request order and duplicates. Missing or zero-lamport
accounts return null. All results share one context slot.

Base64 is the default. Explicit `base58`, legacy `binary`, `base64` and
`base64+zstd` encodings are supported. Base58 and binary are limited to 128 bytes
of selected data. `dataSlice` requires unsigned integer `offset` and `length`;
`space` continues to report the account's full data length. `minContextSlot`
returns `-32016` when the selected root is behind the requested slot. Parsed JSON
is not supported by this bounded multi-account method. The single-account
method keeps its separate legacy binary default and parsed-vote support.

Each request has a 16 MiB full-account-data budget. Slicing does not reduce the
charge. Duplicate occurrences, attempted cold decodes and discarded publication
retries count toward it. Reads refuse data beyond the remaining budget before
copying or allocating its body. This is not a whole-process memory limit;
encoding buffers and data already in caches are separate.

## Admission and recovery

These methods share two in-flight response permits with `getSignatureStatuses`.
An HTTP request, including a JSON-RPC batch, can admit two calls from that group.
Admission is held through serialization and the final response flush. Expensive
queries can be refused while other RPC methods remain available. Mixed history
and query batches share one fixed 30-second write deadline; it does not bound
all CPU work or database waits.

Queries check the selected root and AccountsDB publication version. Block ranges
also check summary publication and the history-recovery gate. A changed view
cannot return a partially accumulated range; account reads retry within their
remaining data budget. No query infers finality from retention alone.

Contracts: [getBlocks](https://solana.com/docs/rpc/http/getblocks) and
[getMultipleAccounts](https://solana.com/docs/rpc/http/getmultipleaccounts).
