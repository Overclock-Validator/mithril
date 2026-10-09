# Set up a non-voting verifying node

A verifying node downloads account state, executes blocks, and checks execution
results without casting votes or producing blocks. No Solana identity, vote account,
stake, or validator keypairs are needed for the standard testnet/RPC configuration below.

## Prepare the server

Use **Ubuntu 26.04 LTS** and the Go version pinned in `go.mod` (currently **1.27.2**).
Follow the [server setup guide](../scripts/README.md) for SSH hardening, storage and
performance tuning. Build as your normal user:

```bash
git clone --branch alpenglow-dev https://github.com/Overclock-Validator/mithril.git
cd mithril
sudo apt-get update
sudo apt-get install -y build-essential curl ca-certificates python3
sudo bash scripts/install-go.sh
export PATH=/usr/local/go/bin:$PATH
make build
```

Allow space for AccountsDB, full and incremental snapshots, snapshot index staging,
and retained blocks. Use NVMe storage; two drives can separate AccountsDB random I/O
from snapshot/ledger traffic. RAM requirements depend on the cluster's account state
and caches; monitor memory during bootstrap before increasing worker counts.

## Standard Solana testnet

Create `config.toml` with these explicit cluster and mode settings. Change the
storage paths to match your server. Keep testnet data separate from other clusters.

```toml
name = "mithril-testnet-verifying"

[bootstrap]
mode = "auto"

[storage]
accounts = "/mnt/mithril-accounts"
shredstore = "/mnt/mithril-ledger/shredstore"
snapshots = "/mnt/mithril-ledger/snapshots"
logs = "/mnt/mithril-logs"

[network]
cluster = "testnet"
rpc = ["https://api.testnet.solana.com"]

[consensus]
mode = "verifying"

[block]
source = "rpc"

[rpc]
port = 0

[lightbringer]
enabled = false

[tuning]
# Defaults stage snapshot indexes on the AccountsDB disk, not /tmp.
snapshot_index_temp_dir = ""
```

The public testnet RPC is suitable for initial connectivity checks but can throttle
continuous replay. For sustained operation use a testnet RPC provider with sufficient
`getBlock` capacity. Every fallback endpoint must belong to the same cluster.

Create the data directories as your normal user if they do not already exist:

```bash
sudo install -d -o "$USER" -g "$(id -gn)" \
  /mnt/mithril-accounts /mnt/mithril-ledger/shredstore \
  /mnt/mithril-ledger/snapshots /mnt/mithril-ledger/tmp /mnt/mithril-logs
./mithril doctor --config config.toml
./mithril run --config config.toml
```

`auto` resumes valid existing state, or bootstraps from a snapshot on first startup.
Avoid `new-snapshot` in a restartable service: it discards state on every restart.
Standard testnet, devnet and mainnet-beta use RPC replay on this branch; they do not
switch to the Alpenglow turbine protocol.

Check bootstrap progress and then confirm that replayed slots continue advancing,
bankhash verification succeeds, and the gap to the finalized RPC slot shrinks. A
running process alone does not establish that the node is synchronized. Stop with
Ctrl+C to flush state cleanly.

## Run under systemd

The [service unit](../scripts/mithril.service) assumes an `ubuntu` user and the
`/mnt/mithril-*` paths above. Edit its user and `RequiresMountsFor` if needed.
Review your configuration before enabling it:

```bash
sudo install -m 0755 mithril /usr/local/bin/mithril
sudo install -d -m 0755 /etc/mithril
sudo install -m 0600 -o "$USER" -g "$(id -gn)" config.toml /etc/mithril/config.toml
sudo install -m 0644 scripts/mithril.service /etc/systemd/system/mithril.service
sudo systemctl daemon-reload
sudo systemctl enable --now mithril
sudo journalctl -u mithril -f
```

On a 32 GiB server, `GOMEMLIMIT=24GiB` in `/etc/mithril/mithril.env` is a useful initial
soft Go heap budget. It does not cap total RSS or reserve memory for the OS: mmap,
native allocations and the page cache still need headroom. Adjust using observed
bootstrap/replay memory and avoid placing large downloads or build scratch on `/tmp`,
which defaults to tmpfs on Ubuntu 26.04. The service uses disk-backed scratch.

Update by stopping the service, pulling/building as your normal user, reinstalling
the binary, and starting the service. Re-run `scripts/install-go.sh` when `go.mod`
changes. Check `systemctl status mithril` and the journal after each update.

## Alpenglow verifying mode

For the separate Alpenglow community cluster, generate `./mithril config init` and
set its cluster-specific RPC, turbine gossip entrypoint, public advertised address
and UDP listeners. Allow the configured turbine/gossip ports through both host and
provider firewalls. Keep `[consensus].mode = "verifying"`.

Alpenglow shreds carry certificates used for fork choice; verifying mode still uses
an external RPC execution oracle. An optional staked identity can improve shred
delivery. Use fresh cluster-specific data paths; do not reuse standard testnet state.
See [config.example.toml](../config.example.toml) for the complete configuration.
