# Mithril

Mithril is a Solana validator and verifying full node client written in Go. It began
as a non-voting node with lower hardware requirements and now also implements
experimental Alpenglow voting and block production. It builds on
[Radiance](https://github.com/firedancer-io/radiance), created by Richard Patel with
contributions from Leo Luk.

Mithril is under active development and is not yet production-ready. Use
**`alpenglow-dev`** for the current Alpenglow and classic-cluster implementations.
The `dev` branch contains the older pre-Alpenglow full node path.

## Choose a node mode

| Mode | Networks on this branch | Setup guide |
| --- | --- | --- |
| **Verifying full node** (non-voting) | Standard Solana testnet, devnet, mainnet-beta; Alpenglow community cluster | [Set up a verifying node](docs/setup-verifying.md) |
| **Validator** (voting and block production) | Alpenglow community cluster only | [Set up a validator](docs/setup-validator.md) |

Standard Solana testnet uses `cluster = "testnet"`, `source = "rpc"` and
`mode = "verifying"`. It needs no validator keypairs or vote account. The separate
Alpenglow community cluster uses `cluster = "alpenglow"` and native turbine shreds
with certificate-driven fork choice. Its validator mode requires an identity,
vote-account address, authorized voter, gossip and reachable QUIC listeners.

## Build and run

**Ubuntu 26.04 LTS** is the primary server target. Go is pinned in `go.mod`
(currently **1.27.2**); CI tests that version on Ubuntu 26.04. The installer downloads
and checks the official Go archive instead of relying on the distribution's Go package.

```bash
git clone --branch alpenglow-dev https://github.com/Overclock-Validator/mithril.git
cd mithril
sudo apt-get update
sudo apt-get install -y build-essential curl ca-certificates python3
sudo bash scripts/install-go.sh
export PATH=/usr/local/go/bin:$PATH
make build
```

A C compiler is required for native dependencies, including zstd. `make build`
embeds the version, branch and commit; `make release` additionally strips debug symbols.
Run Mithril as your normal user.

Generate a configuration with `./mithril config init` (Alpenglow verifying profile)
or `./mithril config init --validator`. For standard testnet, use the explicit
[verifying guide's configuration](docs/setup-verifying.md#standard-solana-testnet).
Review [config.example.toml](config.example.toml) for all supported options, then:

```bash
./mithril doctor --config config.toml
./mithril run --config config.toml
```

The default `bootstrap.mode = "auto"` resumes valid AccountsDB state, otherwise it
bootstraps from a snapshot. Do not use `new-snapshot` for routine service restarts;
it clears existing state. The guides include a [systemd service](scripts/mithril.service)
for background operation and clean shutdown.

## Server requirements

Use a high-clock CPU and NVMe storage with enough capacity for AccountsDB,
full/incremental snapshots, index staging and retained blocks. Two NVMe drives
can separate AccountsDB random I/O from snapshot and ledger writes. Memory and disk
requirements vary with cluster state and cache settings; monitor bootstrap and
live replay before reducing memory or increasing concurrency.

The [system setup guide](scripts/README.md) covers Ubuntu installation, SSH hardening,
storage and performance scripts. Inspect existing provider partitions, LVM and RAID
before formatting anything. Ubuntu 26.04 defaults `/tmp` to tmpfs: keep snapshots
and large temporary build/index files on disk.

Public RPC endpoints can throttle replay and are intended for initial testing.
Use a dedicated endpoint with sufficient `getBlock` capacity for sustained verifying
operation. All configured fallback RPCs must point to the same cluster. Alpenglow
validators use RPC for control-plane information and verify execution against the
certified footer bank hash; they do not use the verifying mode's trailing RPC oracle.

For NixOS, nix-darwin and Home Manager, see [the Nix guide](docs/nix.md).

## RPC interface

Mithril exposes a developing subset of Solana JSON-RPC methods. Set `[rpc].port`
to `8899` to enable it or `0` to leave it disabled. The listener binds on all
interfaces; restrict access with your firewall if you only need local queries.

```bash
curl http://localhost:8899 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getBlockHeight"}'
```

See the implementation in [pkg/rpcserver](pkg/rpcserver) for current method coverage.
The `getBankHash` method is a Mithril extension.

## Current limitations

- **RPC still required**: live near-tip blocks stream from turbine shreds, but RPC `getBlock` is still used for catchup and by the trailing execution verifier (Alpenglow certificates attest block *data*, not execution results, so an external oracle cross-checks execution until peer bankhash cross-checking lands).
- **Alpenglow voting is experimental**: validator mode now signs, persists, self-verifies, and broadcasts Votor votes, but should be exercised on the community cluster before production use. Vote history is identity-bound and startup fails closed if it is corrupt. A `vote landed source=votor-quic proof=verified-aggregate` log is emitted only when an exact network-received, BLS-verified certificate includes the validator's rank for a vote present in its durable history; periodic `alpenglow voting stats` lines expose the cumulative confirmation and broadcast counters.
- **Leader edge cases fail closed**: local production intentionally misses epoch-transition slots and slots with active partitioned epoch rewards until producer-side transition handling is implemented. TPU sanitation currently accepts legacy transactions only; versioned transactions are dropped rather than produced incorrectly.
- **Remaining validator services**: repair serving and Rotor relay duty are still future work.

## Operation and updates

Check that replayed/rooted slots advance and verification succeeds; a running process
alone does not establish synchronization. Stop cleanly with Ctrl+C or
`sudo systemctl stop mithril` so state can flush. Stop before pulling/building an update,
re-run the Go installer when `go.mod` changes, then replace the binary and restart.

See [troubleshooting](docs/TROUBLESHOOTING.md) and
[compatibility](COMPATIBILITY.md) for additional operational details.

## Community

- **Discord**: Join the [Overclock Validator Discord](https://discord.gg/overclock) for support and discussion
- **Hardware Discussion**: `#mithril-hardware` channel for hardware recommendations
- **GitHub Issues**: Report bugs and feature requests on the GitHub repository
