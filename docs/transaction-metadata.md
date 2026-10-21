# Transaction metadata capture

Replay has an optional internal collector for executed transaction and block
metadata. Ordinary `ProcessTransaction` and `ProcessBlock` calls
leave it disabled. There is no node flag or RPC method that enables it here.

A captured record owns its transaction bytes, resolved account keys, status,
fee, pre/post lamport balances, inner instructions, return data and compute
consumption. Token balances retain raw amounts, decimals and mint classification.
Initial mint state is read before workers start and resolved in transaction order after
they join; resolution does not query accounts while other workers modify them.

Capture observes committed post-state. Failed instruction changes are rolled
back by the existing execution path, while charged fees remain visible.
Observation failures discard the candidate metadata without changing execution,
fees, account publication or bank hashes. An absent account is distinct from a
failed or over-limit account read.

## Limits

The collector refuses incomplete records rather than publish partial history.

| Budget | Limit |
| --- | ---: |
| Reserved metadata per transaction | 1 MiB |
| Reserved metadata per block | 64 MiB |
| Group preflight charge | 32 MiB |
| Transactions per block capture | 16,384 |
| Declared keys per group | 131,072 |
| Concurrent capture workers | 16 |
| Data per backing-account read | 64 KiB |
| Backing data hydrated per group | 16 MiB |

Reservations include conservative collection overhead. They are not a process
RSS limit. Log recording and inner-instruction recording have separate ceilings;
exceeding the inner-instruction ceiling invalidates the record.

## Scope

A sealed record is an executed candidate, not proof of finality or durable
storage. Retention, canonical selection and history RPC activation are separate
consumers of these records.

Captured logs contain syscall messages, not the complete runtime invoke/success/
failure stream. They must not be returned as complete standard RPC
`logMessages`. Token-2022 extension presence is classified, but extension-aware
UI conversion is not part of raw balance capture.

The target response fields are defined in the
[Solana RPC JSON structures](https://solana.com/docs/rpc/json-structures).
