# Mithril System Setup Guide

These scripts are **optional** but recommended for optimal Mithril performance.

---

## Quick Start

Target **Ubuntu 26.04 LTS**. Prefer your provider's Ubuntu installer when available;
use `install` only from a current Ubuntu rescue environment with UEFI and a
debootstrap package that supports `resolute`. The script checks this before wiping.

These scripts prepare the server; choose the separate
[non-voting verifying guide](../docs/setup-verifying.md) or
[Alpenglow validator guide](../docs/setup-validator.md) to build and run Mithril.
The Go installer reads `go.mod` and verifies the official archive checksum:

```bash
sudo apt-get update
sudo apt-get install -y build-essential curl ca-certificates python3
sudo bash scripts/install-go.sh
export PATH=/usr/local/go/bin:$PATH
make build
```

Inspect `lsblk -f`, `findmnt /`, and `df -h` before disk setup. Provider-installed
RAID/LVM may occupy both drives. Do not format those disks: create Mithril directories
on the existing filesystem instead. Automatic disk setup refuses roots spanning
multiple physical disks. Use the non-voting guide's directory commands for this case.
Ubuntu 26.04 defaults `/tmp` to tmpfs; keep snapshots, index staging and build scratch
on disk. Run `disk-setup.sh --setup` only when preparing unused storage.

For a fresh two-drive Mithril server, choose a **separate-disk/no-RAID** provider
layout. Put Ubuntu on a small partition of the ledger/snapshot drive and reserve the
other physical drive for AccountsDB. Mirroring both disks removes this I/O isolation
and limits usable capacity to the smaller disk. A fixed-size OS partition also
leaves room for the disk script to create a ledger partition. If the provider only
offers a mirror, use rescue mode and the script's single-disk OS install instead.

Detailed explanations for each step are below.

**Fresh Ubuntu install (rescue/live boot):**
```bash
# 1. Download and run install script (as root)
curl -O https://raw.githubusercontent.com/Overclock-Validator/mithril/alpenglow-dev/scripts/server-setup.sh
chmod +x server-setup.sh
sudo ./server-setup.sh install

# 2. Verify your SSH key was installed (the script shows it at completion)
#    To manually re-check after script finishes (remount first):
mount /dev/nvme0n1p2 /mnt   # Replace with your OS disk partition
cat /mnt/home/YOUR_USERNAME/.ssh/authorized_keys
umount /mnt
#    Then reboot into Ubuntu:
reboot
#    Note: Hetzner rescue is one-boot-only. Other providers: disable rescue mode first.

# 3. Clear old SSH host key (if you SSHed to rescue), then connect
ssh-keygen -R YOUR_SERVER_IP
ssh -i ~/.ssh/YOUR_KEY YOUR_USERNAME@YOUR_SERVER_IP
# Tip: Add an entry to ~/.ssh/config to avoid typing the key path each time

# 4. Clone repo and run remaining setup
git clone --branch alpenglow-dev https://github.com/Overclock-Validator/mithril.git
cd mithril
chmod +x scripts/*.sh
sudo ./scripts/disk-setup.sh --setup        # Configure storage (offers benchmarks)
sudo ./scripts/performance-tune.sh          # Apply performance tuning
```

**Existing Ubuntu (just hardening + setup):**
```bash
git clone --branch alpenglow-dev https://github.com/Overclock-Validator/mithril.git
cd mithril
chmod +x scripts/*.sh
sudo ./scripts/server-setup.sh harden       # Add SSH keys, firewall, fail2ban
sudo ./scripts/disk-setup.sh --setup        # Configure storage (offers benchmarks)
sudo ./scripts/performance-tune.sh          # Apply performance tuning
```

**Resetting Mithril data** (when needed):
```bash
sudo ./scripts/disk-setup.sh --clean-accounts    # After bankhash mismatch / bug fix
sudo ./scripts/disk-setup.sh --clean-ledger      # Clear snapshots + blockstore
sudo ./scripts/disk-setup.sh --clean-all         # Complete reset
```

---

## Detailed Guide

> **New to Linux?** The sections below explain each step in detail. If you get stuck, join our [Discord](https://discord.gg/overclock) and ask in `#mithril-hardware`.

> **Note:** All commands assume you're in the mithril repository root directory (e.g., `~/mithril`).

---

## Table of Contents

1. [Before You Start](#before-you-start)
2. [Server Setup Script](#server-setup-script) (fresh OS install or hardening)
3. [Understanding Mithril's Storage Needs](#understanding-mithrils-storage-needs)
4. [Disk Setup Script](#disk-setup-script)
5. [Performance Tuning Script](#performance-tuning-script)
6. [Resetting Mithril Data](#resetting-mithril-data)
7. [Troubleshooting](#troubleshooting)

---

## Before You Start

### What You'll Need

- **Ubuntu 26.04 LTS** (fresh install recommended)
- **At least one NVMe SSD** (1 TB or larger)
- **Terminal access** (Ctrl+Alt+T opens a terminal)
- **sudo privileges** (you can run admin commands)

### Opening a Terminal

1. Press `Ctrl + Alt + T` on your keyboard
2. A black window with white text should appear - this is your terminal
3. You type commands here and press Enter to run them

### What is `sudo`?

`sudo` means "run this command as administrator". When you see `sudo` at the start of a command, Linux may ask for your password. Newly created key-only admins use passwordless sudo because their account password is locked.

```bash
# Example: this command needs admin privileges
sudo ./scripts/disk-setup.sh --benchmark
```

When you type your password, **you won't see any characters appear** - this is a security feature. Just type your password and press Enter.

---

## Server Setup Script

The server setup script helps you install Ubuntu from scratch or harden an existing installation. This is useful for dedicated servers, bare metal, or VPS providers that offer rescue/live environments.

When hardening, keep your original session open and test key login **and sudo** as
the admin in a second terminal before confirming `ACCESS VERIFIED`. The script
checks effective SSH settings (including admin Match rules), restores its previous
configuration on validation/reload failure, and permits the active SSH port in UFW.
`status` displays effective password, keyboard-interactive and root login settings.

### When to Use This Script

**Use `install` mode if:**
- You have a bare metal server booted into rescue/live mode
- You want a clean Ubuntu 26.04 installation optimized for Mithril
- Your provider doesn't offer Ubuntu 26.04 directly

**Use `harden` mode if:**
- You already have Ubuntu installed
- You want to add SSH key authentication
- You want to configure security packages (fail2ban, UFW, etc.)

**Use `status` mode to:**
- Check current security configuration
- Verify SSH settings and firewall status

### Install Mode (Fresh OS Install)

> ⚠️ **WARNING**: Install mode **erases the OS disk** you select. Only use in rescue/live environments on drives you want to dedicate to the OS.

Boot your server into rescue mode (via your provider's panel), then:

```bash
# Download the script (or clone the repo)
curl -O https://raw.githubusercontent.com/Overclock-Validator/mithril/alpenglow-dev/scripts/server-setup.sh
chmod +x server-setup.sh

sudo ./server-setup.sh install
```

The script installs rescue dependencies and checks Ubuntu 26.04 support before touching disks. It will:
1. Ask which disk to install Ubuntu on
2. Create a fixed-size root partition (not consuming entire disk)
3. Optionally create a data partition with remaining space
4. Set up an admin user with your SSH key
5. Configure security: fail2ban, UFW, unattended-upgrades
6. Disable SSH password login (key-only)
7. Install chrony (time sync) and build dependencies
8. Set journald limits to prevent log disk fill

After installation, disable rescue boot in your provider panel and reboot.

### After Install: What To Do Next

After rebooting, you'll need to connect as your new admin user (not root):

```bash
# SSH to your server as the admin user you created (default: ubuntu)
ssh ubuntu@YOUR_SERVER_IP
```

> **Note:** You're now logged in as `ubuntu` (or whatever username you chose), not `root`. This user has `sudo` privileges for admin tasks.

From here, clone the Mithril repo and run the remaining setup scripts:

```bash
# Clone the mithril repository
git clone --branch alpenglow-dev https://github.com/Overclock-Validator/mithril.git
cd mithril

# Run disk setup (benchmarks and formats drives)
sudo ./scripts/disk-setup.sh --benchmark    # Safe: just shows drive speeds
sudo ./scripts/disk-setup.sh --setup        # Formats drives - follow prompts

# Run performance tuning
sudo ./scripts/performance-tune.sh          # Interactive mode - explains each change
```

**Why `sudo`?** The disk and performance scripts need administrator privileges to format drives, change system settings, etc. Your admin user can run these commands with `sudo`.

### Harden Mode (Existing Ubuntu)

Safe to run on an existing Ubuntu system:

```bash
sudo ./scripts/server-setup.sh harden
```

This mode will:
1. Create/ensure an admin user with sudo access
2. Help you add an SSH public key
3. Optionally disable SSH password login
4. Install and configure fail2ban, UFW, unattended-upgrades

### Status Mode

Check current security configuration (no changes made):

```bash
./scripts/server-setup.sh status
```

Shows SSH settings, fail2ban status, UFW rules, and more.

### What Gets Configured

| Component | What It Does |
|-----------|--------------|
| **SSH hardening** | Disable password login, key-only access |
| **fail2ban** | Blocks IPs after failed SSH attempts |
| **UFW firewall** | Default deny incoming, allow SSH |
| **unattended-upgrades** | Auto-install security patches |
| **chrony** | Time synchronization (critical for blockchain) |
| **haveged** | Entropy generation for cryptographic operations |
| **journald limits** | Prevents logs from filling disk (2GB max) |

---

## Understanding Mithril's Storage Needs

Before running the scripts, it helps to understand what Mithril stores and why it matters.

### What Mithril Stores

Mithril stores three types of data:

| Storage Type | What It Is | Size | Speed Needed |
|-------------|------------|------|--------------|
| **AccountsDB** | All Solana account balances and data | ~500 GB (reserve 700 GB) | **Very fast** (random reads) |
| **Snapshots** | Downloaded network state | ~100 GB | Moderate (streaming) |
| **Blockstore** | Verified blocks | Configurable | Moderate (sequential writes) |

### Why AccountsDB Needs Your Fastest Drive

When Mithril verifies blocks, it constantly looks up account data. These lookups are **random** - jumping around the drive millions of times per second. A fast NVMe with low latency makes a huge difference here.

Snapshots and blockstore are different - they read/write in order (sequentially), which almost any modern SSD handles well.

### Single Drive vs Two Drives

**Two drives aren't required** - Mithril runs well on a single high-quality NVMe (like a Samsung 990 Pro). Two drives are ideal for I/O isolation but not necessary for good performance.

**If you have ONE NVMe drive:**
- Put everything on it
- No special partitioning needed
- The scripts will set this up for you

**If you have TWO NVMe drives:**
- Put AccountsDB on the **faster** one
- Put snapshots/blockstore on the second one
- The benchmark command helps identify which is faster

### Do Separate Partitions Help?

Separate partitions on the same drive do **NOT** improve I/O performance - they still share the same physical hardware. You need separate **physical** drives for actual I/O isolation.

However, partitions do provide operational benefits:
- Prevent disk-full issues in one area from crashing others
- Allow reformatting just AccountsDB without touching your OS
- Enable different mount options per partition (e.g., noatime)

### SSD Over-Provisioning

This is important for SSD longevity and consistent performance.

**What is Over-Provisioning?**
Leave 15-20% of your SSD unallocated (no partition at all). This gives the SSD controller guaranteed "scratch space" for garbage collection and wear leveling. SSD performance degrades significantly when drives exceed 80% capacity.

**Static vs Dynamic Over-Provisioning:**
- **Static OP** (unallocated disk space): The controller always knows these blocks are free. Best performance.
- **Dynamic OP** (free space within partitions): Relies on TRIM commands to tell the controller what's free. Less reliable.

**Recommended unallocated space:**
| Drive Size | Leave Unallocated |
|------------|-------------------|
| 1 TB | 150-200 GB |
| 2 TB | 300-400 GB |

### Recommended Partition Layout (Fresh Install)

If you're doing a fresh Ubuntu installation and want to optimize for Mithril, here's a recommended layout.

**Single Drive Setup (1 TB):**

| Partition | Size | Filesystem | Purpose |
|-----------|------|------------|---------|
| EFI | 512 MB | FAT32 | Boot (required) |
| Ubuntu OS | 30-50 GB | ext4 | Operating system |
| Mithril Data | 650-700 GB | ext4 | AccountsDB, snapshots, blockstore |
| (unallocated) | 150-200 GB | - | Over-provisioning |

**Two Drive Setup:**

*Drive 1 (faster NVMe) - dedicated to AccountsDB:*
| Partition | Size | Filesystem | Purpose |
|-----------|------|------------|---------|
| AccountsDB | 80% of drive | ext4 | AccountsDB only |
| (unallocated) | 20% of drive | - | Over-provisioning |

*Drive 2 (can be slower) - OS and other data:*
| Partition | Size | Filesystem | Purpose |
|-----------|------|------------|---------|
| EFI | 512 MB | FAT32 | Boot (required) |
| Ubuntu OS | 30-50 GB | ext4 | Operating system |
| Snapshots/Blockstore | Remaining | ext4 or xfs | Snapshots and blocks |

**Tips for Ubuntu Installer:**
1. Choose "Something else" (manual partitioning) during installation
2. Create the EFI partition first (if UEFI boot)
3. Create the OS partition and set mount point to `/`
4. Create the Mithril data partition and set mount point (e.g., `/mnt/mithril-accounts`)
5. Leave the remaining space unallocated (do not create a partition)

### Getting Disk Info for Manual Setup

To see your disk UUIDs and device info (useful for `/etc/fstab`):

```bash
./scripts/disk-setup.sh --disk-info
```

This shows:
- All NVMe drives with model and serial numbers
- Existing partitions with their UUIDs
- Example fstab entries for mounting

---

## Disk Setup Script

The disk setup script helps you:
- Find your fastest NVMe drive
- Format drives with optimal settings
- Create the directory structure Mithril needs

### Checking Your Drives (Safe - No Changes Made)

First, let's see what drives you have and how fast they are:

```bash
sudo ./scripts/disk-setup.sh --benchmark
```

This command:
1. Lists all your NVMe drives
2. Shows the model and size of each
3. Runs a random 4K read benchmark (~15 seconds per drive)
4. Recommends which drive to use for AccountsDB

**Why random 4K reads?** AccountsDB constantly looks up account data in a random access pattern. Sequential read speed (what most benchmarks show) is not a good predictor of AccountsDB performance. Random 4K IOPS is the metric that matters.

**Required tool:** The benchmark uses `fio` for accurate random I/O testing. Install it with:
```bash
sudo apt install fio
```

**Example output:**
```
  ┌─────────────────────────────────────────────────────────────────────────┐
  │ BENCHMARK RESULTS (random 4K    read)                                   │
  ├─────────────────────────────────────────────────────────────────────────┤
  │ DEVICE        SPEED         SIZE      MODEL                             │
  ├─────────────────────────────────────────────────────────────────────────┤
  │ /dev/nvme0n1  185.3 K IOPS  1.8T      Samsung SSD 990 PRO               │
  │ /dev/nvme1n1   95.7 K IOPS  2.0T      WD Blue SN580                     │
  └─────────────────────────────────────────────────────────────────────────┘

  ✓ Recommended for AccountsDB (fastest): /dev/nvme0n1

  Random 4K IOPS is the key metric for AccountsDB performance.
  Higher IOPS = faster block verification.
```

If `fio` is not installed, the script falls back to `hdparm` sequential read tests (less accurate but better than nothing).

### Viewing Current Storage Status

To see how your drives are currently configured:

```bash
./scripts/disk-setup.sh --status
```

This shows mounted drives, filesystems, and available space.

### Setting Up Drives (Formats Drives - Erases Data!)

> ⚠️ **WARNING**: This will **erase all data** on the drives you select. Only use on drives you want to dedicate to Mithril.

```bash
sudo ./scripts/disk-setup.sh --setup
```

The script will:
1. Offer to run benchmarks first (recommended)
2. Ask which drive to use for AccountsDB
3. Ask which drive for snapshots/blockstore (if you have multiple)
4. Ask which filesystem to use (ext4 or xfs)
5. Show a summary of what will happen
6. Ask you to type a confirmation phrase before making changes

### Choosing a Filesystem: ext4 vs xfs

The script will ask you to choose between ext4 and xfs. Here's what you need to know:

**ext4** (Recommended for most users)
- More mature, widely tested
- Better for AccountsDB's random read/write pattern
- Can shrink partitions if needed later
- The safer default choice

**xfs**
- Can be faster for large sequential operations
- Good for snapshots/blockstore workloads
- Cannot shrink partitions (would need to reformat)
- Faster crash recovery

**Our recommendation:**
- Single drive: Use ext4
- Two drives: ext4 for AccountsDB, xfs for snapshots/blockstore is reasonable
- When in doubt: ext4

We're still benchmarking to determine optimal configurations. Join `#mithril-hardware` on Discord for updates.

---

## Performance Tuning Script

The performance tuning script optimizes your Ubuntu system for running Mithril. All changes are optional and explained in detail.

### Checking Current System Settings

See what's currently configured (no changes made):

```bash
./scripts/performance-tune.sh --status
```

This shows your current kernel settings, CPU mode, I/O scheduler, and more.

### Running Interactively (Recommended for Beginners)

The safest way to run the script - it explains each optimization and asks before applying:

```bash
sudo ./scripts/performance-tune.sh
```

For each optimization, you'll see:
1. **What it does** - Plain English explanation
2. **Why it helps** - How it benefits Mithril
3. **Yes/No prompt** - You choose whether to apply it

### Applying All Optimizations at Once

If you want everything without prompts:

```bash
sudo ./scripts/performance-tune.sh --all
```

### Preview Mode (Dry Run)

See what would happen without making any changes:

```bash
sudo ./scripts/performance-tune.sh --all --dry-run
```

### What Gets Optimized

#### Basic Optimizations (Low Risk)

| Optimization | What It Does | Why It Helps |
|-------------|--------------|--------------|
| **SSD TRIM** | Weekly cleanup of deleted blocks | Extends NVMe lifespan |
| **Kernel tuning** | Adjusts memory/network settings | Better resource management |
| **CPU performance** | Disables power saving throttling | Consistent speed, no slowdowns |
| **noatime** | Stops tracking file access times | Reduces unnecessary disk writes |

#### Advanced Optimizations (Some Risk)

| Optimization | What It Does | Why It Helps |
|-------------|--------------|--------------|
| **I/O scheduler** | Changes how disk requests are ordered | Lower latency for AccountsDB |
| **Read-ahead** | Controls how much data is prefetched | Tuned per-drive for workload |
| **Huge Pages** | Uses larger memory pages | Can improve performance |

### Go Runtime Tips

Mithril is written in Go. The script can show you Go-specific optimizations:

```bash
./scripts/performance-tune.sh --go-tuning   # View Go runtime tuning tips
./scripts/performance-tune.sh --goamd64     # Auto-configure GOAMD64 for your CPU
```

**`--go-tuning`** displays:
- Environment variables for garbage collection tuning
- Build-time CPU optimizations (GOAMD64)
- Diagnostic commands for latency issues

**`--goamd64`** auto-detects your CPU capabilities (AVX2, AVX-512) and configures GOAMD64 in your shell profile for optimized Go builds. After running, rebuild Mithril to get CPU-optimized binaries.

---

## Resetting Mithril Data

Mithril stores three types of data that can be reset independently:
- **AccountsDB** - Solana account state (~500 GB)
- **Snapshots** - Downloaded network state files (~100 GB)
- **Blockstore** - Verified blocks (size varies)

Sometimes you need to start fresh. These commands delete this data so you can re-sync from scratch.

### Clean AccountsDB Only

Forces Mithril to rebuild from a fresh snapshot on next run:

```bash
sudo ./scripts/disk-setup.sh --clean-accounts
```

**When to use:**
- After encountering errors during block replay (e.g., bankhash mismatch) where AccountsDB state has diverged from mainnet
- After updating Mithril to a patched version that fixes a bug you hit
- AccountsDB got corrupted, or you want a clean slate without re-downloading snapshots

After deleting, Mithril will fetch a fresh snapshot on next run and rebuild AccountsDB from scratch.

### Clean Ledger Only (Snapshots + Blockstore)

Removes both snapshots and blockstore while preserving AccountsDB:

```bash
sudo ./scripts/disk-setup.sh --clean-ledger
```

**When to use:**
- Starting fresh with new snapshots while keeping AccountsDB intact
- Reclaiming space from old snapshots and blocks together
- When you want to clear all "ledger" data (everything except account state)

### Clean Snapshots Only

Removes downloaded snapshots (Mithril will re-download them):

```bash
sudo ./scripts/disk-setup.sh --clean-snapshots
```

**When to use:** Snapshots are old or corrupted.

> **Tip:** We recommend retaining snapshots when possible (current + previous) as they're helpful for debugging. If you encounter issues, having the snapshot you synced from makes it easier for the Mithril team to reproduce and fix bugs. Only delete if you need disk space.

### Clean Blockstore Only

Removes stored blocks (Mithril will rebuild them):

```bash
sudo ./scripts/disk-setup.sh --clean-blockstore
```

**When to use:**
- After restarting from a new snapshot (older blocks are no longer relevant)
- You want to clear block history but keep AccountsDB
- Reclaiming disk space from old block data

### Clean Everything

Complete reset - removes all Mithril data:

```bash
sudo ./scripts/disk-setup.sh --clean-all
```

**When to use:** Starting completely fresh, or decommissioning the node.

### Safety Features

All delete commands:
- Show you exactly what will be deleted and how much space
- Require you to type a confirmation phrase
- Recreate empty directories after deletion

---

## Troubleshooting

### "Permission denied" errors

You need to run with `sudo`:
```bash
sudo ./scripts/disk-setup.sh --benchmark
```

### "Command not found" errors

Make sure you're in the mithril directory:
```bash
cd ~/mithril
```

### Script shows "No NVMe devices found"

- Your drives might not be NVMe (SATA SSDs won't appear)
- Check with: `lsblk` to see all block devices

### Can't format a drive - "has mounted partitions"

Unmount the drive first:
```bash
sudo umount /dev/nvme0n1p1
```

Or if it's in use, you may need to reboot and run the script before the drive gets auto-mounted.

### Benchmark shows low speeds

- Make sure the drive isn't being heavily used during the test
- Check for firmware updates for your NVMe
- Some drives throttle when hot - check temperatures with: `sudo nvme smart-log /dev/nvme0n1`

### Changes didn't persist after reboot

The scripts create systemd services to persist changes. Check if they're enabled:
```bash
systemctl is-enabled mithril-cpu-performance.service
systemctl is-enabled mithril-io-scheduler.service
systemctl is-enabled mithril-readahead.service
```

If any show "disabled", the performance-tune script may not have completed successfully. Try running it again.

---

## Getting Help

- **Discord**: Join [Overclock Validator Discord](https://discord.gg/overclock)
- **Hardware Channel**: `#mithril-hardware` for setup questions
- **GitHub Issues**: Report bugs at the GitHub repository

---

## Quick Reference

```bash
# Server Setup (OS install and security hardening)
sudo ./scripts/server-setup.sh install           # Fresh Ubuntu install (DESTRUCTIVE)
sudo ./scripts/server-setup.sh harden            # Harden existing Ubuntu (safe)
./scripts/server-setup.sh status                 # Show security status

# Disk Setup
sudo ./scripts/disk-setup.sh --benchmark         # Test drive speeds (safe)
sudo ./scripts/disk-setup.sh --setup             # Format and configure drives
./scripts/disk-setup.sh --status                 # Show current configuration
./scripts/disk-setup.sh --disk-info              # Show UUIDs and device info

# Performance Tuning
sudo ./scripts/performance-tune.sh               # Interactive mode
sudo ./scripts/performance-tune.sh --all         # Apply all optimizations
sudo ./scripts/performance-tune.sh --status      # Show current settings
./scripts/performance-tune.sh --go-tuning        # Go runtime tips
./scripts/performance-tune.sh --goamd64          # Configure GOAMD64 for CPU

# Reset Data
sudo ./scripts/disk-setup.sh --clean-accounts   # Clean AccountsDB
sudo ./scripts/disk-setup.sh --clean-ledger     # Clean ledger (snapshots + blockstore)
sudo ./scripts/disk-setup.sh --clean-snapshots  # Clean snapshots only
sudo ./scripts/disk-setup.sh --clean-blockstore # Clean blockstore only
sudo ./scripts/disk-setup.sh --clean-all        # Clean everything
```
