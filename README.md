# Mithril

Mithril is a Solana full node client written in Golang with the goal of serving as a "verifying full node" with lower hardware requirements than that of Solana validators and RPC nodes. This project is being developed upon the foundations of [Radiance](https://github.com/firedancer-io/radiance), which was built by Richard Patel (@ripatel) with contributions from @leoluk.

This project is under active development. We are completing an audit with [Runtime Verification](https://runtimeverification.com/) and expect a more polished, feature-rich release in early Q1 2026.

While Mithril is already functional and runs reliably for many use cases, it is not yet considered production-ready. Users should expect occasional bugs, incomplete features, and ongoing changes as development progresses. Please use with appropriate caution and follow the **alpha** branch for the latest stable updates.

### Release Channels

- **alpha** (recommended): Latest tagged alpha release. Tested for mainnet and suitable for public use.
- **dev** (cutting-edge): New features land here first and may be less stable.

Use **alpha** for reliability; use **dev** if you want the newest changes and can tolerate breakage.

---

## Running Mithril

The `run` command starts Mithril as a live full node - it bootstraps from a Solana snapshot and continuously verifies new blocks as they are produced on mainnet-beta.

### Hardware Requirements

> **Tip for new users:** The easiest way to run Mithril is on a dedicated small server or mini PC. This avoids disk management headaches from sharing storage with other applications and simplifies the setup process significantly.

**Operating System**
- Ubuntu 24.04 LTS (recommended)

**CPU**
- Higher core speed Ryzen series recommended, at least 3.5 GHz base clock
- The 6-core AMD Ryzen 5 7640HS performs exceptionally well in our testing
- We haven't extensively tested a wide range of hardware yet - join the `#mithril-hardware` channel on the [Overclock Validator Discord](https://discord.gg/KHAs9ujrN8) to discuss hardware configurations

**Storage**
- Minimum 1 TB PCIe 4.0 NVMe SSD (more storage can be nice, espeecially to retain larger ledger size)
- Two NVMe drives preferable for optimal performance:
  - **Fast NVMe**: Mithril's AccountsDB (requires high IOPS)
  - **Secondary NVMe**: Block storage and snapshots (can be slower)
- Samsung 990 Pro has been exceptional in our testing
- **Filesystem**: We are still testing optimal filesystem configurations (xfs vs ext4) - more guidance coming soon

**Network**
- Mithril works on a home internet connections
- Slower internet speeds will result in longer snapshot download times
- The integrated snapshot finder automatically selects the fastest available snapshot source

### Getting Started

**Step 1: Clone the repository**

```bash
git clone https://github.com/Overclock-Validator/mithril.git
cd mithril
git checkout alpha  # More stable alpha branch
# OR
git checkout dev    # Cutting-edge, may be less stable
```

**Step 2 (Optional): Run setup scripts**

We provide helper scripts for setting up your system. See the [scripts documentation](scripts/README.md) for a detailed walkthrough:
- **Server Setup** - Fresh Ubuntu install (rescue mode) or security hardening (existing Ubuntu)
- **Disk Setup** - Benchmark NVMe drives, format with optimal settings, reset Mithril data
- **Performance Tuning** - Kernel settings, CPU optimization, etc.

```bash
# These scripts require root privileges
sudo ./scripts/disk-setup.sh --setup
sudo ./scripts/performance-tune.sh
```

The scripts create the following directory structure and automatically set ownership to your user:

```
/mnt/mithril-accounts/   # AccountsDB (needs higher random IOPS - use fastest drive)
/mnt/mithril-ledger/     # Blockstore and snapshots (can use slower drive)
    ├── blockstore/
    └── snapshots/
/mnt/mithril-logs/       # Log files (auto-rotated)
```

**Step 3: Install build dependencies**

Mithril requires a C compiler for CGO dependencies:

```bash
sudo apt-get update && sudo apt-get install -y build-essential
```

**Step 4: Install Go 1.25 or later**

Go 1.25 introduced the "green tea" garbage collector improvements which provide better performance for memory-intensive applications like Mithril.

```bash
# Set Go version and detect architecture
GO_VERSION="1.25.0"
case $(uname -m) in
    x86_64)  GOARCH="amd64" ;;
    aarch64) GOARCH="arm64" ;;
    *) echo "Unsupported architecture"; exit 1 ;;
esac

# Download and install
wget "https://go.dev/dl/go${GO_VERSION}.linux-${GOARCH}.tar.gz"
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf "go${GO_VERSION}.linux-${GOARCH}.tar.gz"
echo 'export PATH=/usr/local/go/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
go version
```

**Step 5: Build Mithril**

```bash
make build
```

This builds the `mithril` binary with version, commit, and branch information embedded. Alternatively, you can use `make release` for a smaller binary with debug symbols stripped.

### Configuration

Generate a starter config with sensible defaults:

```bash
./mithril config init
```

This creates `config.toml`. **We strongly recommend reviewing [`config.example.toml`](config.example.toml)** for all available options and detailed documentation.

**Important: RPC Configuration**

The default config uses `api.mainnet-beta.solana.com` as the RPC endpoint, but this public endpoint has rate limits. For reliable operation, add a dedicated RPC provider (Helius, Triton, etc.) as the primary endpoint:

```toml
[network]
    # Primary RPC first, public endpoint as fallback
    rpc = ["https://your-rpc-provider.com", "https://api.mainnet-beta.solana.com"]
```

Mithril will use the first endpoint for block fetching and fall back to others if needed.

### Running Mithril

From inside the `mithril` directory (where you built the binary):

```bash
# If you're not already there:
cd ~/mithril

# Start Mithril (uses config.toml in current directory by default)
./mithril run
```

The `./` prefix tells your shell to run the `mithril` binary in the current directory. Without it, your shell will look for `mithril` in your system PATH.

You can also specify a config file explicitly:
```bash
./mithril run --config /path/to/custom-config.toml
```

**Note:** Do not run Mithril with `sudo`. The setup scripts automatically configure directory permissions for your user.

**What happens:**
1. Mithril queries the Solana cluster to find reliable snapshot sources
2. The full snapshot is streamed and processed (optionally saved to disk for faster restarts)
3. An incremental snapshot is fetched to bring the state closer to the tip
4. Mithril block execution (aka replay) is initiated and blocks are retrieved with RPC `getBlock` calls and verified
5. Mithril keeps up very close to the tip of the chain with recommended hardware specs

### Mithril's Simple RPC Server

Mithril includes a basic RPC server implementation that exposes a subset of Solana-compatible JSON-RPC methods. This is still under active development and not yet feature-complete.

To enable it, set the port in your config:

```toml
[rpc]
    port = 8899
```

Once running, you can query Mithril like any Solana RPC:

```bash
# Query from the same machine
curl http://localhost:8899 -X POST -H "Content-Type: application/json" -d '
  {"jsonrpc":"2.0","id":1,"method":"getBlockHeight"}
'

# Query from another server (replace with your Mithril host IP)
curl http://YOUR_MITHRIL_IP:8899 -X POST -H "Content-Type: application/json" -d '
  {"jsonrpc":"2.0","id":1,"method":"getAccountInfo","params":["YOUR_PUBKEY",{"encoding":"base64"}]}
'
```

**Currently supported RPC methods:**
- `getAccountInfo` - Get account data and lamports
- `getBankHash` - Get bankhash for a slot (Mithril extension, not standard Solana RPC)
- `getBlockHeight` - Get current block height
- `getEpochInfo` - Get current epoch info
- `getLatestBlockhash` - Get recent blockhash

We're actively expanding RPC method coverage. Upcoming methods include transaction simulation, send transaction, and get leader schedule.

### Current Limitations

- **Block Catchup**: Mithril currently relies on `getBlock` RPC calls to catch up to the tip of mainnet-beta. This dependency is temporary — we are actively working on direct shred replay, which will eliminate the need for external RPC sources entirely.

### RPC Sources

Mithril fetches blocks via `getBlock` RPC calls during catchup. For **short-term testing**, most free Solana RPC plans are sufficient to try out Mithril.

For **extended testing** or if you'd like to help with longer-running nodes, reach out to us on the [Overclock Validator Discord](https://discord.gg/overclock) — we can provide access to our RPC endpoints.

Once direct shred replay is implemented, external RPC sources will no longer be required for block fetching.

### Troubleshooting

**Slow snapshot downloads**
- The snapshot finder automatically tests many nodes and selects the fastest. If downloads are consistently slow, your network may be the bottleneck, but try increasing stage 2 parameters.
- Enable `verbose = true` in the `[snapshot]` config section to see detailed node discovery statistics.

**High disk I/O**
- Ensure AccountsDB is on your fastest NVMe drive
- Consider using a higher-endurance drive (Samsung 990 Pro or better recommended)

**Out of memory**
- By default, Mithril saves snapshots to disk (`max_full_snapshots = 1`). Set `max_full_snapshots = 0` for stream-only mode which doesn't require disk space for snapshot files.
- Initial sync uses more RAM than steady-state replay

### Updating Mithril

To update Mithril to a newer version:

```bash
# Stop Mithril (Ctrl+C or kill the process)

# Pull latest changes and rebuild
cd mithril
git pull
make build

# Restart
./mithril run --config config.toml
```

**Note:** The default `bootstrap.mode = "auto"` will reuse an existing valid AccountsDB when available, otherwise it downloads a snapshot. Set `bootstrap.mode = "snapshot"` to use an existing snapshot if available, or `bootstrap.mode = "new-snapshot"` to always download a fresh one.

### Operational Best Practices

**Clean Shutdown**: Always use `Ctrl+C` to stop Mithril cleanly rather than killing the terminal or closing the SSH session. This allows Mithril to flush data and exit gracefully.

For detailed troubleshooting tips, see [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).

---

## Compatibility

See [COMPATIBILITY.md](COMPATIBILITY.md) for supported networks and feature gate requirements per release.

---

## Development Milestones

### Milestone 1 (Completed): Reimplementation of the Solana Virtual Machine in Golang
- Completed in August 2024, read more [here](https://overclock.one/rnd/unveiling-mithril).
- Reimplementation of all syscalls, with a comprehensive test suite developed and exercised; bugs found as a result fixed.
- Reimplementation of all native programs, with a comprehensive test suite developed and exercised; bugs found as a result fixed.
- Implementation of the remainder of the runtime and VM, with a comprehensive test suite also developed. Any bugs found as a result of testing and review to be fixed.
- Successfully tested against [Firedancer's Solana conformance suite](https://github.com/firedancer-io/solana-conformance)

### Milestone 2 (Completed): Block Replay and Simple RPC Interface
- Snapshot retrieval and loading.
- Full implementation of transaction (and therefore block) handling (consistently match Mainnet bankhashes).
- Incorporate the AccountsDB and Blockstore facilities that are necessary for data storage and retrieval.
- Minimal RPC interface.
- **Status**: Mithril can retrieve and replay current MainnetBeta blocks. There are rare bankhash mismatches that we are actively investigating.

### Milestone 3 (In Progress): Alpha Release and System Optimization
- First formal audit (https://runtimeverification.com/ team is nearing end of audit). Includes development and intensive use of a robust and comprehensive 'conformance suite' for verification of compliance of the VM, interpreter, and runtime as a complete unit. Differential fuzzing will be used to detect differences versus relevant versions of the Labs client, and guided fuzzing will be used generally to uncover security and loss-of-availability issues. Any bugs identified during this phase will be remediated.
- Thorough optimization work on entire system, including on components such as the Virtual Machine and AccountsDB.
- Consensus verification implementation.
- Direct shred replay support (alternative to RPC-based block fetching and requires consensus implementation).
- Achieve multi-epoch runs without bugs (e.g. bankhash mismatches with mainnet)
- Transaction simulation and transaction sending
- Earlier testing on testnet environments.
- **Target**: More substantial release in early Q1 2026 following alpha release in December 2025.

### Future Directions
- Implement Alpenglow consensus verification.
- Add Agave ledger-tool type features for Mithril
- gRPC interface support.
- Expanded RPC feature set.
- Implementation of 'archival node' features including historical replay compatibility.
- Ongoing performance improvements and any bug fixes.
- We welcome community suggestions - please open an issue or join our Discord to discuss ideas!

---

## Community

- **Discord**: Join the [Overclock Validator Discord](https://discord.gg/overclock) for support and discussion
- **Hardware Discussion**: `#mithril-hardware` channel for hardware recommendations
- **GitHub Issues**: Report bugs and feature requests on the GitHub repository
