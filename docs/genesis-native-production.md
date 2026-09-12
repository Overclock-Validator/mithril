# Native production from genesis

Mithril's real `LeaderLoop` now produces the first four banks from a verified,
reopened genesis database. It creates entries with `WorkingBank.Forge`, flushes
them through `ShredSink`, freezes with `CommitLeaderSlot`, and broadcasts its own
bank-hash footer and single Alpenglow ending tick. A separate, non-voting UDP
receiver executes those blocks against an independently initialized Pebble AccountsDB.

```sh
go test ./pkg/replay -run '^TestGenesisNativeProducerAgave$' -count=1 -v
go test ./pkg/blockprod -run '^TestGenesisLeaderParentException$' -count=1 -v
```

The test uses two isolated databases and account tails in one test process. It
does not launch a validator cluster. Public test keys sign transfers; transactions
are signature-checked before admission to the producer's forge API. Slots 1 and 3
are empty, slot 2 transfers 2,000,000 lamports, and slot 4 transfers 4,000,000.
Duplicate submission is rejected without another entry or fee. The receiver has
no consensus engine, voter, external RPC, or local-bank adoption shortcut. The
producer's local commit is removed before independent replay with two workers.

The test clock advances explicitly; it does not require the machine's clock to
match the genesis creation time. Each real leader loop starts its bank, accepts
transactions, observes the slot boundary, and completes the normal finalization
and broadcast path. The next producer parent comes from its own frozen bank and
account tail; the receiving instance derives its parent from its own replay result.

## Independent Agave execution

The pinned Agave helper accepts only genesis, emitted entry bytes, slot/parent
numbers and footer times. It receives no expected hashes or account states.
It independently verifies the entry chain and transaction signatures, executes
transactions, applies the footer clock, and freezes each child bank.

For the single ending tick it uses the exact sequence in pinned Agave's
`core/src/block_creation_loop.rs::record_and_complete_block`: set tick height to
`max_tick_height - 1`, then register the final tick. Unlike the earlier sleep-mode
fixture's 64 zero-hash ticks, every native entry has `num_hashes=1`.

| Slot | Native producer / UDP replay / Agave bank hash |
| --- | --- |
| 1 | `CAxytEjoNzB664cFMiT8sXcK7UKC6UVbYHDkWvwwgEX5` |
| 2 | `77RxqF7E1Cbp78rfTHyNnej5o9TpvhqRPYtoBoqq8Nh7` |
| 3 | `R4epx6bDW1oi3NzfmBJEvtwf7FeHZBjhLa9gwyxi49T` |
| 4 | `7T2PuhQ3cxrD3NnjGZUC6d2nGCbvDA3rq8dLu1CmaRvC` |

Comparisons include all account balances, owners, data, executable flags and rent
epochs; capitalization, account-data length, AccountsLtHash and bank hashes;
recent blockhashes through complete sysvar data; signatures, transaction count,
fees and epoch stakes. Producer accounts are checked both after child setup and
after freezing. Entry bytes and footer times bind the cached oracle expectations
to the actual native output. Producer and receiver block IDs and footer hashes
must agree before replay.

```sh
scripts/genesis-oracle/run.sh /absolute/path/to/agave
MITHRIL_AGAVE_ORACLE=/absolute/path/to/mithril_genesis_oracle \
  go test ./pkg/replay -run '^TestGenesisNativeProducerAgave$' -count=1 -v
```

The helper mode is `replay-native GENESIS.bin ENTRIES.json BANKS.json`. Its current
transaction fixture supports legacy signed system transfers. Set
`MITHRIL_UPDATE_ORACLE_FIXTURES=1` with the live helper to regenerate
`pkg/replay/testdata/genesis-native-producer.json.gz`. Ordinary Go tests use this
checked-in record and need no Rust installation. Agave remains a development oracle.

## Production changes

- `ParentContext.GenesisParent` carries the verified genesis capability into
  leader setup. `FirstChildSlotHashes` requires the exact parent sysvar view,
  frozen bank hash, slot 0 and child slot 1. It creates the account only in the
  child, retaining the absent parent before-image for AccountsLtHash. Ordinary
  missing-sysvar checks remain strict, and parent-coherence checks include this
  capability.
- Leader sysvar updates retain the allocated `SlotHashes` length when the early
  history is short, matching replay and Agave.
- Leader preparation carries feature, vote-timestamp and stake context into its
  clock update. `NewLeaderSlotCtx` copies the parent's vote timestamps.
- Footer production uses the leader loop's configured clock, as its scheduling
  already does; the default remains `time.Now`.

Both databases close and reopen with the original slot-0 bank verified unchanged.
The four child banks remain in separate unrooted tails. This milestone adds no
durable child-bank promotion, finality certificates, live consensus startup,
gossip/TPU integration or snapshot production. `run --bootstrap genesis` remains
disabled, and Lightbringer is unchanged.

The follow-up [checkpoint/resume fixture](genesis-checkpoint-resume.md) now
persists the receiver at slot 2, reopens it at slot 3 and continues to matching
slot-4 state. It uses an explicit offline trust policy and keeps the bank/account
boundary coherent for future snapshot work. Live consensus compatibility and
genesis startup still need separate validation.
