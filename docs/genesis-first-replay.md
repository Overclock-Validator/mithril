# Replaying the first banks from genesis

The pinned Agave oracle now creates a deterministic four-slot bank fixture from
Mithril-generated genesis. Mithril replays its entry streams using the production
`ProcessBlock` path, starting with a verified, reopened AccountsDB initial bank.
The test runs with serial execution and two transaction workers. A second path
transports the same banks through the production signed-shred broadcaster and
UDP receiver before replay.

```sh
go test ./pkg/replay -run '^TestGenesisFirstBlocksAgave$' -count=1 -v
```

Ordinary runs use `pkg/replay/testdata/genesis-replay.json.gz` and need no Rust,
Agave installation, network, external RPC, or running validator. To regenerate
expectations independently, build `scripts/genesis-oracle/run.sh` against the
local Agave checkout, then run:

```sh
MITHRIL_AGAVE_ORACLE=/absolute/path/to/mithril_genesis_oracle \
  go test ./pkg/replay -run '^TestGenesisFirstBlocksAgave$' -count=1 -v
```

The helper's `replay GENESIS.bin OUTPUT.json` mode requires the deterministic
funded test keys in `replayGenesisConfig`. Ed25519 seeds 7 and 8 are public test
material. Production genesis commands still never generate private keys or invoke
Agave. `MITHRIL_UPDATE_ORACLE_FIXTURES=1` explicitly refreshes the checked-in fixture.

## What is compared

The fixture has one funded bootstrap validator, an existing sender and an existing
recipient. Slot 1 is empty; slot 2 transfers 2,000,000 lamports; slot 3 is empty;
slot 4 transfers another 4,000,000 lamports. Both transfers are signed and executed
independently in Agave and Mithril. Agave chooses the leader using its leader
schedule and constructs each child with `Bank::new_from_parent`.

| Slot | Transactions | Frozen bank hash |
| --- | --- | --- |
| 1 | 0 | `ADhrGxLvRvqY1drSQyDyDLahGf33BXrEHLRcivZ5wdE6` |
| 2 | 1 | `8auu32Vexx4PkogL7WCrg2kSCnkbiAXCuMauxqFJeLDu` |
| 3 | 0 | `Cx3JGPC9rTNvyTsATHkbor23DYvaVYA4X2oLNEzvpkzL` |
| 4 | 1 | `JBumnFecGhpT5uHrmpsuYQwgEcgmH9QzzWdCaf76okQW` |

Before execution and after freezing, the test compares every account's data,
owner, balance, executable flag and rent epoch, plus total capitalization,
account-data length and AccountsLtHash. It verifies entry hashes and signatures,
then checks frozen bank hashes, recent blockhashes and fees, leader, block height,
signature/transaction counts and epoch stake totals. A wrong expected footer hash
must fail before publishing accounts or transaction statuses.

The genesis parent is created through `NewGenesisReplayBootstrap`, which calls the
same native initial-bank constructor as `genesis init`. `ConfigureFirstBlock`
provides its fee state, features, immutable sysvars, leader schedule and stakes.
Only this verified parent can authorize creation of the initially absent
`SlotHashes` account. An ordinary snapshot parent missing that sysvar still fails.
No consensus block ID or chained shred root is invented.

## Compatibility fixes found by the fixture

- Preserve the allocated lengths of `SlotHashes` and `RecentBlockhashes` while
  early banks have few entries. Their unused bytes are zero-filled, as in Agave.
- Re-registering the same blockhash refreshes its queue entry rather than adding
  a duplicate. Empty blocks in the pinned sleep-mode genesis exercise this.
- Select Alpenglow metadata PDAs from the bank's actual feature ID. The pinned
  revision uses `mustRekeyVm2QHYB3JPefBiU4BY3Z6JkW2k3Scw5GWP`; the deployed Mithril
  network uses a different ID. The optional Agave dev-context feature-set build
  uses a third ID. Existing deployed startup validation remains unchanged.
- Match the pinned revision's child-bank clock update from parent vote timestamps
  before its footer clock update. The deployed Alpenglow path continues preserving
  the parent's timestamp until the footer.
- Count rooted transaction-status banks even when coverage began complete at
  genesis. Checkpoints now reload after the first root and after the 300-root
  retention limit without weakening snapshot coverage validation.

## Scope and next integration

The fixtures now cover serialized entries, signed Merkle/FEC shreds, UDP ingress,
footer validation and independent Agave bank states. The bank oracle drives
Agave's Bank API directly. The separate ledger-crate oracle validates wire
serialization and recovery; neither runs Agave's full ledger processor or a live
consensus cluster. In particular, Mithril's double-Merkle consensus block IDs are
checked between its broadcaster and receiver; the pinned Agave ledger code still
uses the last FEC root for its blockstore chaining check. That revision remains
the bank/profile and shred-codec oracle, not proof of live consensus compatibility.
The database's durable root remains slot 0 while the four child banks use the
existing unrooted overlay. Reopening verifies the original initial bank unchanged.
The transaction-status checkpoint is round-tripped separately; no claim is made
that the complete slot-4 bank is persisted or that snapshots are produced yet.

The [native producer integration](genesis-native-production.md) now creates its
own entries and footer and sends them through this path into independent replay.
Live startup can later start one voting validator and a separate non-voting full node. It still
needs the genesis consensus-parent case in live consensus admission, epoch/leader
startup, and complete durable child-bank checkpoints before enabling `run
--bootstrap genesis`; the current early launch refusal remains in place. The full
node must never join the voting path, and bootstrap must not fall back to external
RPC. The test retains Agave's 64 sleep-mode ticks, including a final tick with
`num_hashes=0`; it uses `BroadcastComponent` to preserve that tick. The native
producer's `BroadcastEndingTickLast` convenience method emits `num_hashes=1`;
the native producer fixture now verifies that behavior independently with Agave.

Full and incremental snapshots remain a later requirement. Keep the account root,
bank metadata, epoch stakes, recent-blockhash/status history and snapshot cut at
the same rooted bank when adding persistence and snapshot generation. The verified
slot-0 seed and the transaction-status regression tests provide reusable pieces,
not a substitute for a full replay-checkpoint implementation.

## Signed shreds and non-voting ingress

```sh
go test ./pkg/replay ./pkg/turbine \
  -run '^Test(GenesisSignedShredIngressAgave|GenesisIngressRejects|GenesisWireParent|AssembledGenesisParent)' -count=1 -v
```

Each slot sends a header, entry batch, footer and ending tick through
`BroadcastSession`, `Shredder`, `UDPBroadcaster`, and the normal `UDPReceiver`.
The receiver obtains leaders from the verified genesis epoch's schedule and
filters on the genesis-derived shred version. It verifies leader signatures and
Merkle proofs, recovers missing data, decodes components, verifies transaction
signatures, and hands the reconstructed block to `ProcessBlock`. No voter,
consensus engine, RPC endpoint, repair peer or finality callback is configured.

The test sends component batches and packets out of order, duplicates packets,
and drops the first and last data shred of every component, including the ending
tick's slot-complete flag. Coding shreds recover the missing data. All four banks
still pass the same complete account/bank comparisons. Invalid leader signatures,
mutated payloads, signatures by a different leader, another shred version, and
valid leader-signed shreds containing an invalid transaction are rejected.
Insufficient data/parity never produces a replay delivery; cancellation joins the
receiver. A wrong footer bank hash is rejected before state publication.

The first header explicitly names the genesis certificate parent `(0, zero ID)`.
Ingress preserves that explicit zero only at slot 0; ordinary zero parent IDs
retain their previous behavior. `ConfigureFirstBlock` rejects a missing parent on
an ingress-verified block and rejects the genesis hash, frozen bank hash or another
ID substituted for the certificate parent. Subsequent headers use the preceding
block's actual computed double-Merkle ID; the shred chain separately uses its last
FEC root. This does not populate a fabricated slot-0 consensus ID in AccountsDB.

The transport anchor is derived from Agave's canonical slot-0 ticks chained to the
genesis hash, as in `ledger/src/blockstore.rs::create_new_ledger`. For this fixture
the shred version is `51254` and the slot-0 last FEC root is
`91HZLJHqLYAHhfN89ZaB7zZ5msQ3YRxg3k619K1Zekw4`. The public test leader seed is 1;
the signature does not alter that root. This anchor is constructed in the test;
live startup still needs to establish and retain its actual genesis ledger anchor.

To run the independent wire oracle:

```sh
scripts/genesis-oracle/run.sh /absolute/path/to/agave --with-shreds
MITHRIL_AGAVE_ORACLE=/absolute/path/to/mithril_genesis_oracle \
MITHRIL_AGAVE_SHRED_ORACLE=/absolute/path/to/mithril_shred_oracle \
  go test ./pkg/replay -run '^TestGenesisSignedShredIngressAgave$' -count=1 -v
```

The optional ledger helper adds Agave's RocksDB build dependencies; production Go
commands do not need them. It independently decodes the components, generates all
1,088 data/coding shreds across 17 components (including slot 0), applies Agave's
first-hop signing API, compares every packet byte, checks the FEC/slot chain, and
recovers a missing data shred in each component using Agave. Ordinary Go tests
compare a digest of the exact generated wire request against its checked-in
verification record in `pkg/replay/testdata/genesis-shreds-agave.txt`. Only a run
with the live shred helper and `MITHRIL_UPDATE_ORACLE_FIXTURES=1` refreshes that record.
