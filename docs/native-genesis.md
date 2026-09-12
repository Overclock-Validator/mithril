# Native genesis and slot-0 bootstrap

This milestone creates a deterministic Solana-format genesis and a verified,
reopenable AccountsDB containing the completed slot-0 bank. The offline harness also exercises native block production and signed UDP replay.
Live cluster startup, voting and snapshot production remain separate work.
Mithril's verifying full-node mode does not vote.

```sh
go build -o mithril ./cmd/mithril
./mithril genesis create --config examples/genesis.toml --output ./genesis-output
./mithril genesis init --genesis ./genesis-output/genesis.tar.bz2 --accounts-path ./genesis-accounts
# A raw ./genesis-output/genesis.bin is also accepted.
```

The example is public test material, not a set of operational validator keys.
Provide identities, vote/stake account addresses, authorities and compressed BLS
public keys from your own key-management process. The commands never generate,
read or save private keys. BLS keys use exactly 96 hexadecimal characters and
must be nonidentity points in the correct subgroup. Genesis initialization uses
Agave's genesis privilege; no transaction proof of possession is generated.

`creation_time` is required, in RFC3339 at whole-second precision. `[[accounts]]`
contains funded system accounts. Each `[[validators]]` entry creates three
accounts; identity, vote and stake allocations are separately minted, not
transfers from a funding account. Authorities and reward collectors default to
the identity; the summary records every resolved value. Vote accounts must have
at least 1,627,074,400 lamports (rent plus one 1.6 SOL admission ticket); stake
accounts need at least 1,002,282,880 (rent plus 1 SOL delegated stake). Identity
allocations must be positive. Commissions range from 0 to 10,000 basis points.
Duplicate addresses, duplicate BLS keys, reserved protocol addresses, unknown
fields, invalid material and overflows are errors.

V1 supports 1–2000 validators, one stake account per validator and a unique
highest-staked validator. Agave selects the initial leader using a hash-map
maximum without a stable tie-break, so tied highest stakes are rejected.

## Fixed interoperability profile

`agave-alpenglow-development-v1` is pinned to Agave
`7e51da963aee49622a395f562386a6bd8ba0e717`. Its genesis defaults correspond to a
development genesis with Alpenglow enabled, `--hashes-per-tick sleep`, no warmup
epochs, no custom programs and no feature deactivations:

- All feature accounts named in `pkg/genesis/profile/features.json`, active at 0.
- 64 ticks per slot; target tick duration 6,250,000 ns; no hashes-per-tick or
  target-tick-count option; 400,000,000 ns per slot.
- 8,192 slots per epoch and leader schedule offset by default; first normal epoch/slot 0.
  Explicit `test_slots_per_epoch = 32` (range 32–8,192) shortens both for tests;
  the resolved summary and genesis hash record the chosen schedule.
- Genesis rent: 3,480 lamports per byte-year, threshold 2, burn 50%. The bank
  applies SIMD-0194 to 6,960 and threshold 1, preserving minimum balances.
- Fee governor: target 10,000 lamports/signature, 20,000 signatures/slot,
  min/max 0, burn 50%; deserialized initial lamports/signature is 0.
- Inflation: initial .08, terminal .015, taper .15, foundation .05 for 7 years.
- The stake configuration account, zero epoch-rewards account, and Agave's
  special `Genesis(0, zero-hash)` certificate with empty bitmap/zero signature.

The binary profile contains only fixed protocol accounts and parameters; user
accounts are built natively in Go. Its exact features, reserved addresses and
runtime parameters are checked into the repository. Updating Agave or Mithril's
feature registry does not activate another feature in this profile. A profile
feature-set change requires an explicit new version and new oracle verification.
The bounded test epoch override is explicit and covered by the same pinned oracle.

`genesis create` requires a new output directory. It emits canonical
`genesis.bin`, deterministic `genesis.tar.bz2` and `genesis-summary.json`.
Accounts are ordered by raw public-key bytes; the genesis hash is SHA-256 of
the exact canonical genesis bytes. The archive contains `genesis.bin`, not an
Agave RocksDB ledger. This milestone does not provide an Agave validator launch
bundle; slot-0 ledger/network startup belongs to the next milestone.
Archive input verifies the complete compression stream, bounds unpacked data
to Agave's default 10 MiB, and reads members without extracting paths.

## Bank semantics and persistence

`genesis.ConstructInitialBank` returns independent initialized and frozen bank
states. It initializes builtins, precompiles, sysvars, stake/leader data and the
blockhash queue. It then processes 64 canonical ticks and freezes the bank,
including SlotHistory and AccountsLtHash. Sleep-mode ticks perform zero hashes;
the terminal tick replaces the existing genesis-hash queue entry at index 1.
Genesis hash, bank hash and consensus block identity are separate fields.
Consensus block identity is absent. No post-genesis Alpenglow clock, parent
block identity, slot hashes or fictitious snapshot manifest is created.

`genesisinit.Initialize` and `genesisinit.Open` are the reusable storage adapters.
Initialization acquires the shared exclusive AccountsDB store lock before checking
occupancy or writing. It writes durable intent, appendvecs, the stake index,
file-ID high-water marks, complete bank metadata and the account index, then verifies
all account contents and the bank hash. It publishes `mithril_state.json` last.
The CLI closes and independently reopens the database before reporting success.

Genesis state uses schema 6 with an explicit storage-format tag and a durable
root at slot 0; next replay is slot 1. Existing snapshot-origin schema 3 remains supported. Schema-3 readers reject
genesis state as unsupported. `genesis_bank.json` retains the complete seed and
is bound to the marker with SHA-256. The raw genesis is also retained and checked
on reopen; the native constructor re-derives metadata and accounts for comparison.
This seed includes fee/rent/inflation/epoch parameters, stake balances and
accounts, tick/blockhash context, capitalization, account-data length and the
full LtHash, for later replay and full/incremental snapshot work.

An occupied destination is refused. The store lock's persistent inode is never
removed. After interruption, durable partial artifacts remain without a ready
marker, and initialization refuses to overwrite them. Use a new empty directory
after inspecting an interrupted attempt. Writers clean up only temporary files
they created. Both completed and interrupted genesis databases are recognized by
`mithril run`, which refuses launch before cleanup or external-RPC fallback.
Ordinary snapshot and resume validation does not receive a slot-zero exemption.

## Verification

Ordinary Go tests use compressed, checked-in results from the pinned Rust oracle.
The oracle decodes and reserializes the Go-produced genesis, creates an Agave
bank, processes canonical slot-0 ticks and freezes it. Tests compare every
account's bytes/owner/balance/rent epoch/executable flag, capitalization,
AccountsLtHash, bank hash, recent-blockhash queue, epoch stakes and runtime
parameters at both stages, with single- and multiple-validator configurations.

To rebuild the independent helper without modifying an Agave checkout:

```sh
./scripts/genesis-oracle/run.sh /path/to/agave
MITHRIL_AGAVE_ORACLE=/absolute/path/printed/by/script \
  go test ./pkg/genesis -run TestAgaveOracle -v
```

The script archives the exact pinned commit into a new temporary directory. Its
only source adjustment exposes an existing read-only bank serialization accessor;
no bank behavior is changed. To deliberately refresh test fixtures, also set
`MITHRIL_UPDATE_ORACLE_FIXTURES=1`. The helper's `profile OUTPUT.bin` mode writes
fixed profile bytes plus feature/reserved-address manifests for audit. None of
these development tools is invoked by production genesis commands.

Tests also cover canonical ordering, encoder errors, malformed configurations,
profile tampering, cancellation, interruption at publication boundaries,
destination conflicts, concurrent ownership, metadata corruption and reopening.
The repository's full suite uses a separately fetched Firedancer corpus. See
`docs/native-genesis-validation.md` for the actual full-suite results and
baseline failures, including missing fixtures in the currently pinned corpus.


The deterministic first-block replay test now also has a signed-shred/UDP-ingress
path. See [genesis-first-replay.md](genesis-first-replay.md) for the four slots,
independent Agave bank and wire comparisons, rejection cases, and the remaining
work before a live validator/full-node cluster.

The [native production fixture](genesis-native-production.md) also drives the real
leader loop, producing its own four banks and signed shreds into an independently
initialized, non-voting replay instance. Both agree with Agave. The
[checkpoint/resume follow-up](genesis-checkpoint-resume.md) persists completed
child banks and verifies continued replay after restart. Live startup remains
separate work.

Genesis databases must use the supported storage format and state schema.
Incompatible database formats are rejected without implicit conversion. Existing
snapshot-origin schema 3 and the account/stake-index formats remain supported.
