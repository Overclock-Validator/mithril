#!/usr/bin/env bash
# ==============================================================================
# performance-tune.sh - Ubuntu 26.04 System Optimization for Mithril
# ==============================================================================
#
# PURPOSE:
# This script optimizes your Ubuntu 26.04 system for running Mithril, a high-
# performance Solana ledger verification tool. Mithril is I/O intensive and
# benefits from:
#   - Fast disk access (reduced filesystem overhead)
#   - Maximum CPU performance (no power-saving throttling)
#   - Optimized memory management (efficient caching, reduced swap usage)
#
# WHAT THIS SCRIPT DOES:
#   1. SSD TRIM      - Extends SSD lifespan by cleaning up deleted blocks
#   2. Kernel Tuning - Optimizes memory, file handles, and network settings
#   3. CPU Mode      - Locks CPU to maximum performance (no frequency scaling)
#   4. Filesystem    - Adds 'noatime' mount option to reduce disk writes
#   5. I/O Scheduler - Sets NVMe scheduler for minimum latency
#   6. Read-ahead    - Tunes disk prefetching per workload type
#   7. Huge Pages    - Configures THP to prevent latency spikes
#   8. Mount Options - (EXPERIMENTAL) barrier=0, data=writeback for ext4
#   9. Go Tuning     - Shows Go runtime optimization tips
#
# ==============================================================================
# OPTIMIZATION CONFIDENCE LEVELS
# ==============================================================================
#
# These optimizations have varying levels of testing. Use this guide:
#
# [PROVEN] - Well-established, widely used, minimal risk
#   - SSD TRIM (fstrim.timer)     - Standard Linux practice
#   - noatime mount option        - Standard for all SSDs
#   - CPU performance mode        - Standard for servers
#   - vm.max_map_count increase   - Required for many databases
#
# [LIKELY BENEFICIAL] - Theoretically sound, commonly recommended, low risk
#   - I/O scheduler 'none'        - Recommended for NVMe by kernel docs
#   - THP 'madvise'               - Prevents unexpected latency spikes
#   - vm.swappiness reduction     - Common server tuning
#   - Go 1.25 defaults            - Go team's recommended settings
#
# [THEORETICAL] - Makes sense for Mithril's workload, needs benchmarking
#   - Read-ahead 64 KB            - Theory: smaller = better for random I/O
#   - Differential read-ahead     - Theory: tune per-device by workload
#   - GODEBUG=madvdontneed=1      - May help memory-constrained systems
#
# [EXPERIMENTAL] - May help, may hurt, test carefully
#   - vm.vfs_cache_pressure=50    - Workload dependent
#   - kyber io-scheduler          - Alternative to 'none', needs comparison
#
# We recommend starting with [PROVEN] and [LIKELY BENEFICIAL] optimizations,
# then benchmarking [THEORETICAL] changes one at a time.
#
# REQUIREMENTS:
#   - Ubuntu 26.04 LTS (may work on other versions)
#   - Root/sudo access
#   - NVMe SSD storage
#
# USAGE:
#   sudo ./performance-tune.sh [OPTIONS]
#
# OPTIONS:
#   --all           Apply all optimizations (non-interactive)
#   --trim          Enable SSD TRIM only
#   --sysctl        Apply kernel tuning only
#   --cpu           Set CPU performance mode only
#   --noatime       Add noatime to fstab only
#   --mount-perf    Configure experimental ext4 mount options (barrier=0, data=writeback)
#   --io-scheduler  Set optimal I/O scheduler for NVMe
#   --readahead     Configure disk read-ahead buffer
#   --hugepages     Configure transparent huge pages
#   --go-tuning     Show Go runtime tuning recommendations
#   --goamd64       Configure GOAMD64 for optimized builds (detects CPU capabilities)
#   --dry-run       Show what would be done without making changes
#   --status        Show current system tuning status
#   --help          Show this help message
#
# ==============================================================================

set -euo pipefail

# Colors for output (makes it easier to read)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script state
DRY_RUN=false
VERBOSE=false

# ------------------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------------------

die() {
    echo -e "\n${RED}[ERROR]${NC} $*\n" >&2
    exit 1
}

info() {
    echo -e "\n${BLUE}[INFO]${NC} $*"
}

success() {
    echo -e "${GREEN}[OK]${NC} $*"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

# Ask yes/no question with configurable default
# Usage: yesno "prompt" [default]
#   default: "y" for default yes [Y/n], "n" for default no [y/N]
yesno() {
    local prompt="$1"
    local default="${2:-n}"  # Default to "no" if not specified
    local answer

    if [[ "${default,,}" == "y" ]]; then
        read -r -p "$prompt [Y/n]: " answer
        # Default yes: return true unless explicitly "n"
        [[ ! "${answer,,}" =~ ^n ]]
    else
        read -r -p "$prompt [y/N]: " answer
        # Default no: return true only if explicitly "y"
        [[ "${answer,,}" == "y" ]]
    fi
}

# Check if running as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        die "This script must be run as root. Try: sudo $0"
    fi
}

# Get system RAM in GB
get_ram_gb() {
    awk '/MemTotal/ {printf "%.0f", $2/1024/1024}' /proc/meminfo
}

# ------------------------------------------------------------------------------
# STATUS: Show Current System Configuration
# ------------------------------------------------------------------------------

show_status() {
    echo ""
    echo "================================================================================"
    echo "                    CURRENT SYSTEM TUNING STATUS"
    echo "================================================================================"
    echo ""

    # RAM info
    local ram_gb
    ram_gb=$(get_ram_gb)
    echo "System RAM: ${ram_gb} GB"
    echo ""

    # SSD TRIM status
    echo "--- SSD TRIM ---"
    if systemctl is-enabled fstrim.timer &>/dev/null; then
        success "fstrim.timer is ENABLED (weekly SSD TRIM active)"
    else
        warn "fstrim.timer is NOT enabled"
    fi
    echo ""

    # Kernel parameters
    echo "--- Kernel Parameters (sysctl) ---"
    echo "  vm.swappiness        = $(cat /proc/sys/vm/swappiness) (recommended: 10-30)"
    echo "  vm.vfs_cache_pressure= $(cat /proc/sys/vm/vfs_cache_pressure) (recommended: 50)"
    echo "  vm.max_map_count     = $(cat /proc/sys/vm/max_map_count) (recommended: 1000000+)"
    echo "  fs.file-max          = $(cat /proc/sys/fs/file-max) (recommended: 2097152)"
    echo ""

    # CPU performance mode (check both EPP and governor)
    echo "--- CPU Performance Mode ---"

    # Check EPP (modern Intel/AMD pstate drivers)
    local epp_values
    epp_values=$(cat /sys/devices/system/cpu/cpufreq/policy*/energy_performance_preference 2>/dev/null | sort -u | tr '\n' ' ' || true)
    if [[ -n "$epp_values" ]]; then
        echo "  Energy Performance Preference (EPP): ${epp_values}"
        if [[ "$epp_values" == *"performance"* ]]; then
            success "EPP is set to 'performance'"
        else
            warn "EPP is NOT set to 'performance'"
        fi
    fi

    # Check governor (legacy cpufreq)
    local governors
    governors=$(cat /sys/devices/system/cpu/cpufreq/policy*/scaling_governor 2>/dev/null | sort -u | tr '\n' ' ' || echo "unknown")
    echo "  Scaling governor(s): ${governors}"
    if [[ "$governors" == *"performance"* ]]; then
        success "Governor is set to 'performance'"
    elif [[ -n "$epp_values" && "$epp_values" == *"performance"* ]]; then
        # EPP is performance but governor is not - this is fine on modern systems
        echo "  (Governor may stay as schedutil when EPP controls performance)"
    else
        warn "Governor is NOT set to 'performance'"
    fi
    echo ""

    # noatime status
    echo "--- Filesystem Mount Options ---"
    echo "  Mounts with 'noatime' option:"
    if mount | grep -q noatime; then
        mount | grep noatime | awk '{print "    " $1 " -> " $3}'
    else
        warn "  No filesystems currently mounted with noatime"
    fi
    echo ""

    # I/O Scheduler
    echo "--- I/O Scheduler (NVMe) ---"
    for sched_file in /sys/block/nvme*/queue/scheduler; do
        if [[ -f "$sched_file" ]]; then
            local dev_name
            dev_name=$(echo "$sched_file" | cut -d'/' -f4)
            local current_sched
            current_sched=$(cat "$sched_file" | grep -oP '\[\K[^\]]+' || echo "unknown")
            local size
            size=$(lsblk -d -o SIZE "/dev/$dev_name" --noheadings 2>/dev/null | xargs || echo "?")
            echo "  /dev/$dev_name (${size}): ${current_sched}"
        fi
    done
    if ! ls /sys/block/nvme*/queue/scheduler &>/dev/null; then
        echo "  No NVMe devices found"
    fi
    echo ""

    # Read-ahead
    echo "--- Disk Read-Ahead ---"
    for ra_file in /sys/block/nvme*/queue/read_ahead_kb; do
        if [[ -f "$ra_file" ]]; then
            local dev_name
            dev_name=$(echo "$ra_file" | cut -d'/' -f4)
            local current_ra
            current_ra=$(cat "$ra_file")
            echo "  /dev/$dev_name: ${current_ra} KB"
        fi
    done
    if ! ls /sys/block/nvme*/queue/read_ahead_kb &>/dev/null; then
        echo "  No NVMe devices found"
    fi
    echo ""

    # Transparent Huge Pages
    echo "--- Transparent Huge Pages ---"
    if [[ -f /sys/kernel/mm/transparent_hugepage/enabled ]]; then
        local thp_enabled
        thp_enabled=$(cat /sys/kernel/mm/transparent_hugepage/enabled | grep -oP '\[\K[^\]]+' || echo "unknown")
        echo "  THP enabled: ${thp_enabled} (recommended: madvise)"
    else
        echo "  THP not available on this system"
    fi
    if [[ -f /sys/kernel/mm/transparent_hugepage/defrag ]]; then
        local thp_defrag
        thp_defrag=$(cat /sys/kernel/mm/transparent_hugepage/defrag | grep -oP '\[\K[^\]]+' || echo "unknown")
        echo "  THP defrag:  ${thp_defrag} (recommended: defer+madvise)"
    fi
    echo ""

    # Performance tuning config file
    echo "--- Mithril Performance Config Files ---"
    local configs_found=false
    if [[ -f /etc/sysctl.d/99-mithril-performance.conf ]]; then
        success "/etc/sysctl.d/99-mithril-performance.conf exists"
        configs_found=true
    fi
    if systemctl is-enabled mithril-cpu-performance.service &>/dev/null; then
        success "mithril-cpu-performance.service enabled"
        configs_found=true
    fi
    if systemctl is-enabled mithril-io-scheduler.service &>/dev/null; then
        success "mithril-io-scheduler.service enabled"
        configs_found=true
    fi
    if systemctl is-enabled mithril-readahead.service &>/dev/null; then
        success "mithril-readahead.service enabled"
        configs_found=true
    fi
    if [[ -f /etc/tmpfiles.d/mithril-thp.conf ]]; then
        success "/etc/tmpfiles.d/mithril-thp.conf exists"
        configs_found=true
    fi
    if ! $configs_found; then
        warn "No Mithril performance configs found"
    fi

    echo ""
    echo "================================================================================"
}

# ------------------------------------------------------------------------------
# OPTIMIZATION 1: Enable SSD TRIM
# ------------------------------------------------------------------------------
#
# WHAT IS TRIM?
# When you delete a file, the SSD still thinks those blocks are in use until
# TRIM tells it otherwise. Without TRIM, the SSD has to do extra work during
# writes, which slows things down and wears out the drive faster.
#
# WHY WEEKLY?
# Running TRIM too frequently adds overhead. Weekly is a good balance between
# keeping the SSD healthy and not impacting performance during normal use.
#
# ------------------------------------------------------------------------------

enable_trim() {
    info "Enabling weekly SSD TRIM..."

    echo "  WHY: TRIM tells your SSD which blocks are no longer in use, allowing it"
    echo "       to perform background garbage collection. This maintains write speed"
    echo "       and extends the lifespan of your NVMe drive."
    echo ""

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would run: systemctl enable --now fstrim.timer"
        return
    fi

    if systemctl enable --now fstrim.timer; then
        success "fstrim.timer enabled - TRIM will run weekly"
    else
        warn "Failed to enable fstrim.timer - your SSD may not support TRIM"
    fi
}

# ------------------------------------------------------------------------------
# OPTIMIZATION 2: Kernel & VM Tuning
# ------------------------------------------------------------------------------
#
# These settings adjust how Linux manages memory, files, and network connections.
# Each parameter is explained below:
#
# vm.swappiness (default: 60, recommended for Mithril: 10-30)
#   Controls how aggressively Linux moves data from RAM to swap.
#   - Higher values = more swapping (saves RAM but slower)
#   - Lower values = less swapping (uses more RAM but faster)
#   For Mithril: We want to keep hot data in RAM, so we use a low value.
#   If you have 32GB+ RAM, use 10. For 16GB, use 30 as a safety net.
#
# vm.vfs_cache_pressure (default: 100, recommended: 50)
#   Controls how aggressively the kernel reclaims memory used for caching
#   directory and inode (file metadata) information.
#   - Higher values = less caching (frees RAM faster)
#   - Lower values = more caching (faster file lookups)
#   For Mithril: We read many files repeatedly, so caching metadata helps.
#
# vm.max_map_count (default: 65530, recommended: 1000000)
#   Maximum number of memory-mapped regions a process can have.
#   Mithril's AccountsDB uses memory-mapped files extensively. Without
#   increasing this, you may hit "cannot allocate memory" errors.
#
# fs.file-max (default: varies, recommended: 2097152)
#   System-wide limit on open file descriptors.
#   Mithril keeps many files open simultaneously (ledger, accounts, etc.)
#
# net.core.somaxconn (default: 4096)
#   Maximum number of pending connections in the socket queue.
#   Helps with burst RPC traffic.
#
# net.ipv4.tcp_fastopen (default: 1, recommended: 3)
#   Enables TCP Fast Open for both client (1) and server (2) = 3.
#   Reduces latency for repeated connections to the same endpoints.
#
# ------------------------------------------------------------------------------

apply_sysctls() {
    local ram_gb
    ram_gb=$(get_ram_gb)

    info "Applying kernel performance tuning..."
    echo ""
    echo "  Detected RAM: ${ram_gb} GB"

    # Adjust swappiness based on available RAM
    local swappiness
    if [[ $ram_gb -ge 32 ]]; then
        swappiness=10
        echo "  -> Using aggressive settings (32GB+ RAM detected)"
    elif [[ $ram_gb -ge 16 ]]; then
        swappiness=30
        echo "  -> Using balanced settings (16-32GB RAM detected)"
    else
        swappiness=50
        echo "  -> Using conservative settings (<16GB RAM detected)"
        warn "  Mithril works best with 16GB+ RAM"
    fi
    echo ""

    local config_file="/etc/sysctl.d/99-mithril-performance.conf"

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would create ${config_file} with:"
        cat <<EOF
# ==============================================================================
# Mithril Performance Tuning
# Generated by performance-tune.sh
# System RAM: ${ram_gb} GB
# ==============================================================================

# SWAP BEHAVIOR
# ------------------------------------------------------------------------------
# vm.swappiness = ${swappiness}
# How aggressively to swap RAM to disk (0-100, lower = less swapping)
# Lower values keep more data in RAM, which is faster but uses more memory.
# We use ${swappiness} because you have ${ram_gb}GB RAM.
vm.swappiness = ${swappiness}

# FILESYSTEM CACHE
# ------------------------------------------------------------------------------
# vm.vfs_cache_pressure = 50
# How aggressively to reclaim directory/inode cache (default: 100)
# Lower = keep more file metadata in cache = faster repeated file lookups
# Mithril reads the same files repeatedly, so caching helps.
vm.vfs_cache_pressure = 50

# MEMORY MAPPING
# ------------------------------------------------------------------------------
# vm.max_map_count = 1000000
# Maximum memory-mapped regions per process (default: 65530)
# Mithril's AccountsDB uses mmap extensively. Too low = "cannot allocate memory"
vm.max_map_count = 1000000

# FILE HANDLES
# ------------------------------------------------------------------------------
# fs.file-max = 2097152
# System-wide limit on open files (default varies)
# Mithril keeps many ledger/account files open simultaneously.
fs.file-max = 2097152

# NETWORK TUNING
# ------------------------------------------------------------------------------
# net.core.somaxconn = 4096
# Max pending connections in socket queue (helps with RPC bursts)
net.core.somaxconn = 4096

# net.ipv4.tcp_fastopen = 3
# Enable TCP Fast Open for client (1) + server (2) = 3
# Reduces connection latency for repeated RPC calls
net.ipv4.tcp_fastopen = 3
EOF
        return
    fi

    # Create the config file with detailed comments
    cat >"${config_file}" <<EOF
# ==============================================================================
# Mithril Performance Tuning
# Generated by performance-tune.sh on $(date)
# System RAM: ${ram_gb} GB
# ==============================================================================

# SWAP BEHAVIOR
# ------------------------------------------------------------------------------
# How aggressively to swap RAM to disk (0-100, lower = less swapping)
# Lower values keep more data in RAM, which is faster but uses more memory.
# We use ${swappiness} because you have ${ram_gb}GB RAM.
vm.swappiness = ${swappiness}

# FILESYSTEM CACHE
# ------------------------------------------------------------------------------
# How aggressively to reclaim directory/inode cache (default: 100)
# Lower = keep more file metadata in cache = faster repeated file lookups
# Mithril reads the same files repeatedly, so caching helps.
vm.vfs_cache_pressure = 50

# MEMORY MAPPING
# ------------------------------------------------------------------------------
# Maximum memory-mapped regions per process (default: 65530)
# Mithril's AccountsDB uses mmap extensively. Too low = "cannot allocate memory"
vm.max_map_count = 1000000

# FILE HANDLES
# ------------------------------------------------------------------------------
# System-wide limit on open files (default varies)
# Mithril keeps many ledger/account files open simultaneously.
fs.file-max = 2097152

# NETWORK TUNING
# ------------------------------------------------------------------------------
# Max pending connections in socket queue (helps with RPC bursts)
net.core.somaxconn = 4096

# Enable TCP Fast Open for client (1) + server (2) = 3
# Reduces connection latency for repeated RPC calls
net.ipv4.tcp_fastopen = 3
EOF

    echo "  Created ${config_file}"

    # Apply immediately
    if sysctl --system >/dev/null 2>&1; then
        success "Kernel parameters applied"
    else
        warn "Some parameters may not have applied correctly"
    fi
}

# ------------------------------------------------------------------------------
# OPTIMIZATION 3: CPU Performance Mode
# ------------------------------------------------------------------------------
#
# WHAT IS CPU FREQUENCY SCALING?
# Modern CPUs dynamically adjust their clock speed to save power. When idle,
# they run slower; under load, they speed up. This is great for laptops but
# adds latency for high-performance workloads.
#
# WHY PERFORMANCE MODE?
# In "performance" mode, the CPU always runs at maximum speed. This eliminates
# the brief delay when the CPU needs to ramp up, which matters for latency-
# sensitive applications like Mithril.
#
# POWER CONSUMPTION:
# Your system will use more power (10-30W more at idle). This is usually fine
# for dedicated servers but might matter for home setups.
#
# This creates a systemd service that sets performance mode on every boot.
#
# ------------------------------------------------------------------------------

set_cpu_perf() {
    info "Setting CPU to Performance Mode..."
    echo ""
    echo "  WHY: Prevents CPU frequency scaling delays. The CPU will always run at"
    echo "       maximum speed instead of ramping up when needed. This reduces"
    echo "       latency spikes during block processing."
    echo ""
    echo "  NOTE: This increases power consumption by ~10-30W at idle."
    echo ""

    local service_file="/etc/systemd/system/mithril-cpu-performance.service"

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would create ${service_file}"
        echo "  [DRY-RUN] Would enable and start mithril-cpu-performance.service"
        return
    fi

    # Check if we can actually set performance mode
    if [[ ! -d /sys/devices/system/cpu/cpufreq ]]; then
        warn "CPU frequency scaling not available on this system"
        echo "  This might be a VM or the kernel doesn't support cpufreq"
        return
    fi

    # Create a systemd service that sets performance mode on boot
    cat >"${service_file}" <<'EOF'
[Unit]
Description=Set CPU to performance mode for Mithril
After=multi-user.target

[Service]
Type=oneshot
RemainAfterExit=yes

# Set energy_performance_preference to performance (Intel/AMD P-state drivers)
ExecStart=/usr/bin/bash -c '\
    for f in /sys/devices/system/cpu/cpufreq/policy*/energy_performance_preference; do \
        [ -f "$f" ] && echo performance > "$f" 2>/dev/null || true; \
    done'

# Set scaling_governor to performance (legacy cpufreq drivers)
ExecStart=/usr/bin/bash -c '\
    for g in /sys/devices/system/cpu/cpufreq/policy*/scaling_governor; do \
        [ -f "$g" ] && echo performance > "$g" 2>/dev/null || true; \
    done'

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload

    if systemctl enable --now mithril-cpu-performance.service >/dev/null 2>&1; then
        # Verify it worked - check both EPP (modern) and governor (legacy)
        local governor epp success_msg=""
        governor=$(cat /sys/devices/system/cpu/cpufreq/policy0/scaling_governor 2>/dev/null || echo "unknown")
        epp=$(cat /sys/devices/system/cpu/cpufreq/policy0/energy_performance_preference 2>/dev/null || echo "")

        # Check EPP first (modern Intel/AMD pstate drivers)
        if [[ -n "$epp" ]]; then
            if [[ "$epp" == "performance" ]]; then
                success_msg="EPP set to 'performance'"
            fi
        fi

        # Check governor (legacy cpufreq or as fallback)
        if [[ "$governor" == "performance" ]]; then
            if [[ -n "$success_msg" ]]; then
                success_msg="${success_msg}, governor set to 'performance'"
            else
                success_msg="Governor set to 'performance'"
            fi
        fi

        if [[ -n "$success_msg" ]]; then
            success "CPU performance mode enabled: ${success_msg}"
        else
            # Neither EPP nor governor is "performance" - but service was created
            warn "Service created but performance mode not fully verified"
            echo "    Current governor: ${governor}"
            [[ -n "$epp" ]] && echo "    Current EPP: ${epp}"
            echo "    This might require a reboot or manual intervention"
        fi
    else
        warn "Failed to enable CPU performance service"
    fi
}

# ------------------------------------------------------------------------------
# OPTIMIZATION 4: Add 'noatime' Mount Option
# ------------------------------------------------------------------------------
#
# WHAT IS ATIME?
# Every time you read a file, Linux updates the file's "access time" (atime).
# This means every file READ also causes a WRITE. On an SSD with millions of
# file accesses, this adds significant overhead.
#
# WHAT IS NOATIME?
# The 'noatime' mount option disables access time updates. Files still track
# when they were created (ctime) and modified (mtime), just not when they
# were last read.
#
# IS IT SAFE?
# Yes, for almost all use cases. Very few applications actually need atime.
# (Some mail servers use it to detect unread mail, but that's rare.)
#
# This function helps you add noatime to your fstab, but requires a reboot
# (or remount) to take effect for the root filesystem.
#
# ------------------------------------------------------------------------------

apply_noatime() {
    info "Configuring 'noatime' mount option..."
    echo ""
    echo "  WHY: Linux normally updates 'access time' every time a file is read."
    echo "       This means every READ causes a WRITE, adding I/O overhead."
    echo "       'noatime' disables this, reducing disk writes significantly."
    echo ""
    echo "  SAFE? Yes. Very few applications need access time tracking."
    echo ""

    # Show current ext4/xfs mounts
    echo "  Current ext4/xfs mounts:"
    findmnt -rn -o TARGET,FSTYPE,OPTIONS | awk '$2=="ext4" || $2=="xfs" {
        noatime = ($3 ~ /noatime/) ? "(has noatime)" : "(no noatime)";
        print "    " $1 " [" $2 "] " noatime
    }'
    echo ""

    # Find Mithril mount points that need noatime
    local mithril_mounts=()
    local needs_noatime=()
    for mnt in /mnt/mithril-accounts /mnt/mithril-ledger; do
        if findmnt -n "$mnt" >/dev/null 2>&1; then
            mithril_mounts+=("$mnt")
            if ! findmnt -rn -o OPTIONS "$mnt" | grep -q noatime; then
                needs_noatime+=("$mnt")
            fi
        fi
    done

    if [[ ${#mithril_mounts[@]} -gt 0 ]]; then
        echo "  Mithril mount points:"
        for mnt in "${mithril_mounts[@]}"; do
            local status="needs noatime"
            findmnt -rn -o OPTIONS "$mnt" | grep -q noatime && status="already has noatime"
            echo "    $mnt ($status)"
        done
        echo ""
    fi

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would add noatime to ${#needs_noatime[@]} mount(s)"
        return
    fi

    # If all mounts already have noatime, we're done
    if [[ ${#needs_noatime[@]} -eq 0 ]]; then
        if [[ ${#mithril_mounts[@]} -gt 0 ]]; then
            success "All Mithril mounts already have noatime"
        else
            echo "  No Mithril mounts detected. Skipping."
        fi
        return
    fi

    # Ask once for all mounts
    echo "  Add noatime to: ${needs_noatime[*]}"
    read -r -p "  Apply noatime to all Mithril mounts? [Y/n]: " response
    if [[ "$response" =~ ^[Nn] ]]; then
        echo "  Skipping noatime configuration"
        return
    fi

    # Backup fstab once
    cp /etc/fstab /etc/fstab.bak.$(date +%Y%m%d_%H%M%S)
    echo "  Backed up /etc/fstab"

    local remount_list=()

    for mp in "${needs_noatime[@]}"; do
        # Add noatime to the mount options using awk
        local temp_fstab="/etc/fstab.tmp.$$"

        if awk -v mp="$mp" '
            BEGIN { found = 0 }
            {
                if (/^[[:space:]]*#/ || /^[[:space:]]*$/) {
                    print
                    next
                }
                n = split($0, fields)
                if (n >= 4 && fields[2] == mp) {
                    if (fields[4] !~ /noatime/) {
                        fields[4] = fields[4] ",noatime"
                    }
                    printf "%s\t%s\t%s\t%s", fields[1], fields[2], fields[3], fields[4]
                    for (i = 5; i <= n; i++) printf "\t%s", fields[i]
                    printf "\n"
                    found = 1
                } else {
                    print
                }
            }
            END { exit (found ? 0 : 1) }
        ' /etc/fstab > "$temp_fstab"; then
            mv "$temp_fstab" /etc/fstab
            success "Added noatime to ${mp}"
            remount_list+=("$mp")
        else
            rm -f "$temp_fstab"
            warn "Failed to add noatime to ${mp}"
        fi
    done

    if [[ ${#remount_list[@]} -gt 0 ]]; then
        echo ""
        echo "  ${YELLOW}IMPORTANT:${NC} Changes take effect after reboot, or remount now:"
        for mp in "${remount_list[@]}"; do
            echo "    sudo mount -o remount,noatime ${mp}"
        done
        echo ""
    fi
}

# ------------------------------------------------------------------------------
# OPTIMIZATION 4b: Advanced ext4 Mount Options (EXPERIMENTAL)
# ------------------------------------------------------------------------------
#
# These options can improve write performance but RISK DATA LOSS on power failure.
# Only use if you have UPS protection or can afford to resync from scratch.
#
# barrier=0:
#   Disables write barriers. Normally, the kernel sends cache flush commands
#   to ensure data is actually on disk when a write "completes". Without this,
#   data in the drive's cache can be lost on power failure.
#   Performance gain: ~5-15% for write-heavy workloads
#   Risk: Filesystem corruption on unexpected power loss
#
# data=writeback:
#   Changes how ext4 journals data. With 'ordered' (default), file data is
#   written before metadata. With 'writeback', metadata can be committed
#   before the actual file data is on disk.
#   Performance gain: ~10-30% for small random writes
#   Risk: After crash, files may contain stale/garbage data
#
# RECOMMENDATION:
#   Only use these if:
#   - You have a UPS with automatic shutdown
#   - The data can be rebuilt (Mithril can resync from fresh snapshot)
#   - You understand you may lose hours of sync progress on power loss
#
# ------------------------------------------------------------------------------

configure_advanced_mount_options() {
    info "Advanced ext4 Mount Options (EXPERIMENTAL)"
    echo ""
    echo "  ┌─────────────────────────────────────────────────────────────────────────┐"
    echo "  │ ${RED}WARNING: THESE OPTIONS CAN CAUSE DATA LOSS ON POWER FAILURE${NC}            │"
    echo "  └─────────────────────────────────────────────────────────────────────────┘"
    echo ""
    echo "  These options improve WRITE performance but disable safety guarantees:"
    echo ""
    echo "    barrier=0      - Disables write barriers (~5-15% write improvement)"
    echo "                     Risk: Filesystem corruption on power loss"
    echo ""
    echo "    data=writeback - Allows metadata before data (~10-30% write improvement)"
    echo "                     Risk: Files may contain garbage data after crash"
    echo ""
    echo "  WHEN TO USE:"
    echo "    - You have a UPS with automatic shutdown"
    echo "    - You're okay rebuilding from snapshot if power fails"
    echo "    - Write performance is your bottleneck (usually it's not for Mithril)"
    echo ""
    echo "  NOTE: Mithril's bottleneck during replay is usually READS, not writes."
    echo "        These options help write-heavy workloads but have minimal impact"
    echo "        on block verification speed."
    echo ""

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would prompt for advanced mount options"
        return
    fi

    # Show current ext4 mounts
    echo "  Current ext4 mounts:"
    findmnt -rn -o TARGET,FSTYPE,OPTIONS | awk '$2=="ext4" {
        opts = ""
        if ($3 ~ /barrier=0/) opts = opts " barrier=0"
        if ($3 ~ /data=writeback/) opts = opts " data=writeback"
        if (opts == "") opts = " (safe defaults)"
        print "    " $1 " -" opts
    }'
    echo ""

    echo "  Do you want to enable these experimental options?"
    echo ""
    echo "    1) Skip - Keep safe defaults (recommended)"
    echo "    2) Enable barrier=0 only (moderate risk)"
    echo "    3) Enable data=writeback only (moderate risk)"
    echo "    4) Enable both (highest risk, highest performance)"
    echo ""

    local choice
    while true; do
        read -r -p "  Choice [1-4]: " choice
        case "$choice" in
            1) echo "  Keeping safe defaults"; return ;;
            2) apply_advanced_mount_opts "barrier=0"; break ;;
            3) apply_advanced_mount_opts "data=writeback"; break ;;
            4) apply_advanced_mount_opts "barrier=0,data=writeback"; break ;;
            *) echo "  Invalid choice. Please enter 1-4." ;;
        esac
    done
}

apply_advanced_mount_opts() {
    local new_opts="$1"

    echo ""
    read -r -p "  Enter the ext4 mountpoint to modify (e.g., /mnt/mithril-accounts): " mp

    if [[ -z "$mp" ]]; then
        echo "  Skipping"
        return
    fi

    # Verify it's a valid ext4 mountpoint
    local fstype
    fstype=$(findmnt -n -o FSTYPE "$mp" 2>/dev/null)
    if [[ "$fstype" != "ext4" ]]; then
        warn "'$mp' is not an ext4 mountpoint (found: ${fstype:-none})"
        echo "  These options only work with ext4"
        return
    fi

    # Final warning
    echo ""
    echo "  ${RED}FINAL WARNING:${NC}"
    echo "  You are about to add '${new_opts}' to ${mp}"
    echo "  This can cause data loss if power fails unexpectedly."
    echo ""
    read -r -p "  Type 'I UNDERSTAND THE RISK' to continue: " confirm

    if [[ "$confirm" != "I UNDERSTAND THE RISK" ]]; then
        echo "  Aborted"
        return
    fi

    # Backup fstab
    cp /etc/fstab /etc/fstab.bak.$(date +%Y%m%d_%H%M%S)
    echo "  Backed up /etc/fstab"

    # Add options using awk
    local temp_fstab="/etc/fstab.tmp.$$"

    if awk -v mp="$mp" -v new_opts="$new_opts" '
        BEGIN { found = 0 }
        {
            if (/^[[:space:]]*#/ || /^[[:space:]]*$/) {
                print
                next
            }

            n = split($0, fields)
            if (n >= 4 && fields[2] == mp) {
                # Add new options if not present
                split(new_opts, opts_arr, ",")
                for (i in opts_arr) {
                    opt = opts_arr[i]
                    if (fields[4] !~ opt) {
                        fields[4] = fields[4] "," opt
                    }
                }
                printf "%s\t%s\t%s\t%s", fields[1], fields[2], fields[3], fields[4]
                for (i = 5; i <= n; i++) printf "\t%s", fields[i]
                printf "\n"
                found = 1
            } else {
                print
            }
        }
        END { exit (found ? 0 : 1) }
    ' /etc/fstab > "$temp_fstab"; then
        mv "$temp_fstab" /etc/fstab
        success "Added '${new_opts}' to ${mp} in /etc/fstab"
        echo ""
        echo "  ${YELLOW}IMPORTANT:${NC} Changes require remount or reboot to take effect."
        echo ""
        echo "  ${RED}WARNING:${NC} Do NOT remount while Mithril is running!"
        echo "  Stop Mithril first, then apply changes:"
        echo ""
        echo "    # Stop Mithril"
        echo "    sudo systemctl stop mithril  # or kill the process"
        echo ""
        echo "    # Remount with new options"
        echo "    sudo mount -o remount ${mp}"
        echo ""
        echo "    # Restart Mithril"
        echo "    sudo systemctl start mithril"
        echo ""
        echo "  To revert, edit /etc/fstab and remove these options,"
        echo "  or restore from backup: /etc/fstab.bak.*"
    else
        rm -f "$temp_fstab"
        warn "Failed to modify /etc/fstab"
    fi
}

# ------------------------------------------------------------------------------
# OPTIMIZATION 5: I/O Scheduler for NVMe
# ------------------------------------------------------------------------------
#
# WHAT IS AN I/O SCHEDULER?
# The I/O scheduler decides the order in which disk read/write requests are
# processed. Different schedulers optimize for different workloads.
#
# SCHEDULER OPTIONS FOR NVME:
#
#   none (noop):
#     - No scheduling, requests go directly to the drive
#     - Lowest possible latency
#     - Best when: You trust the NVMe's internal scheduler completely
#     - Downside: No prioritization between sync/async I/O
#
#   kyber:
#     - Designed specifically for fast storage (NVMe, Optane)
#     - Separates I/O into two queues: sync (latency-sensitive) and async
#     - Maintains target latencies for each queue type
#     - Best when: Mixed workload with both latency-sensitive and bulk I/O
#     - For Mithril: Good choice! AccountsDB needs low latency, snapshots need throughput
#
#   mq-deadline:
#     - Prioritizes latency with deadlines for each request
#     - Good all-around choice for SSDs
#     - Best when: You want guaranteed response times
#
# RECOMMENDATION FOR MITHRIL:
#   - none:  RECOMMENDED - Minimum latency for AccountsDB random I/O during block replay
#   - kyber: Alternative if you have mixed workloads and want throughput/latency balance
#
# ------------------------------------------------------------------------------

set_io_scheduler() {
    info "Configuring I/O scheduler for NVMe drives..."
    echo ""
    echo "  SCHEDULERS FOR NVME:"
    echo ""
    echo "    none   - No kernel scheduling, requests go directly to the drive"
    echo "             BEST FOR RANDOM I/O - lowest latency for AccountsDB block replay"
    echo "             NVMe drives have their own internal scheduler"
    echo ""
    echo "    kyber  - Separates sync (latency) and async (throughput) I/O"
    echo "             Alternative if you need throughput for sequential workloads"
    echo ""

    # Find NVMe devices
    local nvme_devices
    nvme_devices=$(lsblk -d -o NAME,TYPE | awk '$2=="disk" && $1~/^nvme/ {print $1}')

    if [[ -z "$nvme_devices" ]]; then
        warn "No NVMe devices found"
        return
    fi

    echo "  Found NVMe devices:"
    for dev in $nvme_devices; do
        local current_sched
        current_sched=$(cat /sys/block/$dev/queue/scheduler 2>/dev/null | grep -oP '\[\K[^\]]+' || echo "unknown")
        local size
        size=$(lsblk -d -o SIZE /dev/$dev --noheadings | xargs)
        echo "    /dev/$dev (${size}) - current scheduler: ${current_sched}"
    done
    echo ""

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would prompt for scheduler choice (kyber or none)"
        return
    fi

    # Ask user which scheduler to use
    echo "  Which scheduler would you like to use?"
    echo "    1) none  (RECOMMENDED - lowest latency for random I/O)"
    echo "    2) kyber (alternative for mixed sequential/random workloads)"
    echo "    3) skip  (leave current settings)"
    echo ""

    local scheduler choice
    while true; do
        read -r -p "  Choice [1/2/3]: " choice
        case "$choice" in
            1) scheduler="none"; break ;;
            2) scheduler="kyber"; break ;;
            3) echo "  Skipping I/O scheduler configuration"; return ;;
            *) echo "  Invalid choice. Please enter 1, 2, or 3." ;;
        esac
    done

    local service_file="/etc/systemd/system/mithril-io-scheduler.service"

    # Create a systemd service to set scheduler on boot
    cat >"${service_file}" <<EOF
[Unit]
Description=Set I/O scheduler to ${scheduler} for NVMe drives (Mithril optimization)
After=local-fs.target

[Service]
Type=oneshot
RemainAfterExit=yes

# Set scheduler to '${scheduler}' for all NVMe devices
ExecStart=/usr/bin/bash -c '\
    for dev in /sys/block/nvme*/queue/scheduler; do \
        [ -f "\$dev" ] && echo ${scheduler} > "\$dev" 2>/dev/null || true; \
    done'

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload

    if systemctl enable --now mithril-io-scheduler.service >/dev/null 2>&1; then
        # Apply immediately
        for dev in /sys/block/nvme*/queue/scheduler; do
            [[ -f "$dev" ]] && echo "$scheduler" > "$dev" 2>/dev/null || true
        done
        success "I/O scheduler set to '${scheduler}' for NVMe devices"
    else
        warn "Failed to enable I/O scheduler service"
    fi
}

# ------------------------------------------------------------------------------
# OPTIMIZATION 6: Disk Read-Ahead (Per-Device)
# ------------------------------------------------------------------------------
#
# WHAT IS READ-AHEAD?
# When you read data from disk, the kernel can speculatively read more data
# than requested, anticipating you'll need it next.
#
# WHY DIFFERENT VALUES FOR DIFFERENT DRIVES?
#
# AccountsDB NVMe (random I/O - PRIMARY WORKLOAD):
#   - Access pattern: Random reads across millions of accounts during block replay
#   - Large read-ahead WASTES bandwidth (prefetched data won't be used)
#   - RECOMMENDED: 64 KB (minimum latency for random I/O)
#   - This is the most important drive to optimize for latency
#
# Blocks/Snapshots NVMe (sequential I/O - SECONDARY):
#   - Access pattern: Sequential streaming during snapshot download
#   - Large read-ahead helps here (prefetched data WILL be used)
#   - Recommended: 512 KB - 1 MB (throughput matters more than latency)
#
# SINGLE DRIVE SETUP:
#   - Use 128 KB as a compromise (leans toward latency)
#
# ------------------------------------------------------------------------------

set_readahead() {
    info "Configuring disk read-ahead for minimum latency..."
    echo ""
    echo "  WHY TUNE READ-AHEAD?"
    echo "    Read-ahead speculatively loads data before it's requested."
    echo "    For random I/O (AccountsDB), read-ahead WASTES bandwidth and adds latency."
    echo ""
    echo "    AccountsDB (random I/O):   64 KB  - MINIMUM LATENCY (recommended)"
    echo "    Snapshots (sequential I/O): 512+ KB - throughput focused"
    echo ""

    # Find block devices
    local nvme_devices
    nvme_devices=$(lsblk -d -o NAME,TYPE,SIZE | awk '$2=="disk" && $1~/^nvme/ {print $1, $3}')

    if [[ -z "$nvme_devices" ]]; then
        warn "No NVMe devices found"
        return
    fi

    echo "  Found NVMe devices:"
    while read -r dev size; do
        local current_ra
        current_ra=$(cat /sys/block/$dev/queue/read_ahead_kb 2>/dev/null || echo "unknown")
        echo "    /dev/$dev (${size}) - current read-ahead: ${current_ra} KB"
    done <<< "$nvme_devices"
    echo ""

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would prompt for per-device read-ahead configuration"
        return
    fi

    local device_count
    device_count=$(echo "$nvme_devices" | wc -l)

    # Track per-device settings for persistence
    declare -A device_readahead

    if [[ $device_count -ge 2 ]]; then
        echo "  You have multiple NVMe devices. Configure them separately?"
        echo "  (Recommended: smaller read-ahead for AccountsDB, larger for snapshots)"
        echo ""

        if yesno "  Configure devices separately?" y; then
            while read -r dev size; do
                echo ""
                # Get mount points for this device's partitions
                local mounts
                mounts=$(lsblk -n -o MOUNTPOINT "/dev/$dev" 2>/dev/null | grep -v "^$" | tr '\n' ', ' | sed 's/, $//')
                if [[ -n "$mounts" ]]; then
                    echo "  /dev/$dev (${size}) - mounted at: ${mounts}"
                else
                    echo "  /dev/$dev (${size}) - not mounted"
                fi
                echo "    1) 64 KB   - MINIMUM LATENCY (recommended for AccountsDB)"
                echo "    2) 128 KB  - Low latency (single-drive compromise)"
                echo "    3) 256 KB  - Balanced"
                echo "    4) 512 KB  - Sequential throughput (snapshots/blocks)"
                echo "    5) 1024 KB - Maximum throughput (streaming snapshots)"
                echo "    6) Skip this device"

                local ra_kb ra_choice
                while true; do
                    read -r -p "    Choice for /dev/$dev [1-6]: " ra_choice </dev/tty
                    case "$ra_choice" in
                        1) ra_kb=64; break ;;
                        2) ra_kb=128; break ;;
                        3) ra_kb=256; break ;;
                        4) ra_kb=512; break ;;
                        5) ra_kb=1024; break ;;
                        6) echo "    Skipping /dev/$dev"; continue 2 ;;
                        *) echo "    Invalid choice. Please enter 1-6." ;;
                    esac
                done

                # Track the setting for this device
                device_readahead[$dev]=$ra_kb

                echo "$ra_kb" > "/sys/block/$dev/queue/read_ahead_kb" 2>/dev/null && \
                    success "/dev/$dev read-ahead set to ${ra_kb} KB" || \
                    warn "Failed to set read-ahead for /dev/$dev"
            done <<< "$nvme_devices"

            # Create persistent config with per-device settings
            create_readahead_service_perdevice
            return
        fi
    fi

    # Single device or user chose uniform setting
    echo ""
    echo "  Choose read-ahead for all NVMe devices:"
    echo "    1) 64 KB  - MINIMUM LATENCY (recommended for block replay)"
    echo "    2) 128 KB - Low latency (good default)"
    echo "    3) 256 KB - Balanced"
    echo "    4) 512 KB - Sequential throughput"
    echo "    5) Skip   - Leave current settings"

    local ra_kb choice
    while true; do
        read -r -p "  Choice [1-5]: " choice
        case "$choice" in
            1) ra_kb=64; break ;;
            2) ra_kb=128; break ;;
            3) ra_kb=256; break ;;
            4) ra_kb=512; break ;;
            5) echo "  Skipping read-ahead configuration"; return ;;
            *) echo "  Invalid choice. Please enter 1-5." ;;
        esac
    done

    # Apply to all devices
    for dev in /sys/block/nvme*/queue/read_ahead_kb; do
        [[ -f "$dev" ]] && echo "$ra_kb" > "$dev" 2>/dev/null || true
    done

    create_readahead_service "$ra_kb"
    success "Read-ahead set to ${ra_kb} KB for all NVMe devices"
}

create_readahead_service() {
    local default_ra="${1:-64}"  # Default to minimum latency
    local service_file="/etc/systemd/system/mithril-readahead.service"

    cat >"${service_file}" <<EOF
[Unit]
Description=Set disk read-ahead for Mithril optimization
After=local-fs.target

[Service]
Type=oneshot
RemainAfterExit=yes

# Restore read-ahead settings (uniform value for all devices)
ExecStart=/usr/bin/bash -c '\
    for dev in /sys/block/nvme*/queue/read_ahead_kb; do \
        [ -f "\$dev" ] && echo ${default_ra} > "\$dev" 2>/dev/null || true; \
    done'

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable mithril-readahead.service >/dev/null 2>&1 || true
}

# Create per-device read-ahead service (persists actual values chosen for each device)
create_readahead_service_perdevice() {
    local service_file="/etc/systemd/system/mithril-readahead.service"

    # Build ExecStart commands for each device based on current settings
    local exec_lines=""
    for dev in /sys/block/nvme*/queue/read_ahead_kb; do
        if [[ -f "$dev" ]]; then
            local dev_name
            dev_name=$(echo "$dev" | cut -d'/' -f4)
            local current_ra
            current_ra=$(cat "$dev" 2>/dev/null || echo "64")
            exec_lines+="ExecStart=/usr/bin/bash -c 'echo ${current_ra} > /sys/block/${dev_name}/queue/read_ahead_kb 2>/dev/null || true'\n"
        fi
    done

    cat >"${service_file}" <<EOF
[Unit]
Description=Set per-device disk read-ahead for Mithril optimization
After=local-fs.target

[Service]
Type=oneshot
RemainAfterExit=yes

# Per-device read-ahead settings (configured by performance-tune.sh)
$(echo -e "$exec_lines")
[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable mithril-readahead.service >/dev/null 2>&1 || true
    success "Per-device read-ahead settings persisted to systemd service"
}

# ------------------------------------------------------------------------------
# OPTIMIZATION 7: Transparent Huge Pages (THP)
# ------------------------------------------------------------------------------
#
# WHAT ARE HUGE PAGES?
# Normal memory pages are 4 KB. Huge pages are 2 MB (or larger).
# Using larger pages means fewer page table entries, which can improve
# performance for applications that use lots of memory.
#
# TRANSPARENT HUGE PAGES (THP):
# THP automatically promotes 4 KB pages to 2 MB pages when beneficial.
# This happens in the background without application changes.
#
# FOR MITHRIL (LATENCY-FOCUSED):
# Mithril's AccountsDB workload is random I/O, not particularly memory-intensive.
# THP is less critical here than for memory-heavy applications.
#
# However, THP can still cause latency issues:
# - Memory fragmentation triggers background compaction (khugepaged)
# - Compaction can cause latency spikes during block replay
#
# RECOMMENDATION:
# Set THP to 'madvise' - only use huge pages when explicitly requested.
# This prevents unexpected latency from background THP activity.
# Go's runtime can use madvise hints when beneficial.
#
# ------------------------------------------------------------------------------

configure_hugepages() {
    info "Configuring Transparent Huge Pages..."
    echo ""
    echo "  BACKGROUND: THP automatically uses 2 MB pages instead of 4 KB pages."
    echo "              For latency-sensitive workloads, THP can cause problems:"
    echo "              - Background compaction (khugepaged) uses CPU"
    echo "              - Memory fragmentation can trigger latency spikes"
    echo ""
    echo "  OPTIONS:"
    echo "    always  - Always use huge pages (can cause latency spikes - NOT recommended)"
    echo "    madvise - Only use huge pages when apps explicitly request them"
    echo "    never   - Disable huge pages entirely (safest for latency)"
    echo ""
    echo "  RECOMMENDED: 'madvise' - prevents unexpected THP latency"
    echo ""

    local current_thp
    current_thp=$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null | grep -oP '\[\K[^\]]+' || echo "unknown")
    echo "  Current THP setting: ${current_thp}"
    echo ""

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would prompt for THP setting"
        return
    fi

    echo "  Choose THP setting:"
    echo "    1) madvise (RECOMMENDED - app-controlled, prevents unexpected latency)"
    echo "    2) never   (most conservative - disables THP entirely)"
    echo "    3) always  (NOT recommended - can cause latency spikes)"
    echo "    4) skip    (keep current setting: ${current_thp})"
    echo ""

    local thp_setting choice
    while true; do
        read -r -p "  Choice [1-4]: " choice
        case "$choice" in
            1) thp_setting="madvise"; break ;;
            2) thp_setting="never"; break ;;
            3) thp_setting="always"; break ;;
            4) echo "  Keeping current THP setting"; return ;;
            *) echo "  Invalid choice. Please enter 1-4." ;;
        esac
    done

    # Set THP
    if [[ -f /sys/kernel/mm/transparent_hugepage/enabled ]]; then
        echo "$thp_setting" > /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true
    fi

    # Set defrag based on THP setting
    local defrag_setting="defer+madvise"
    if [[ "$thp_setting" == "never" ]]; then
        defrag_setting="never"
    fi
    if [[ -f /sys/kernel/mm/transparent_hugepage/defrag ]]; then
        echo "$defrag_setting" > /sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null || true
    fi

    # Make persistent via tmpfiles.d config
    local thp_config="/etc/tmpfiles.d/mithril-thp.conf"
    cat >"${thp_config}" <<EOF
# Mithril: Set THP to ${thp_setting}
w /sys/kernel/mm/transparent_hugepage/enabled - - - - ${thp_setting}
w /sys/kernel/mm/transparent_hugepage/defrag - - - - ${defrag_setting}
EOF

    success "THP set to '${thp_setting}'"
}

# ------------------------------------------------------------------------------
# OPTIMIZATION 8: Go Runtime Tuning for Minimum Latency
# ------------------------------------------------------------------------------
#
# Mithril is a Go application optimized for MINIMUM LATENCY during block replay.
# The primary workload is random I/O to AccountsDB. These settings reduce GC
# pause times and optimize for consistent low-latency performance.
#
# GO 1.25 "GREEN TEA" GC IMPROVEMENTS:
#   Go 1.25 introduced significant garbage collector improvements:
#   - Better GC pacing reduces unnecessary GC cycles
#   - Improved concurrent marking reduces stop-the-world pauses
#   - More efficient memory allocation patterns
#   These improvements make Go 1.25 significantly better for latency-sensitive apps.
#
# GOGC (default: 100)
#   Controls garbage collection frequency. Value is a percentage.
#   - GOGC=100: GC triggers when heap grows to 2x the live data
#   - GOGC=200: GC triggers at 3x (fewer but potentially longer pauses)
#   - GOGC=off: Disable GC entirely (dangerous, only for short-lived processes)
#   For Mithril: Default (100) is usually fine with Go 1.25's improvements.
#   Only increase if you see frequent GC pauses in logs.
#
# GOMEMLIMIT (Go 1.19+)
#   Soft memory limit - Go's GC works harder to stay under this.
#   Helps prevent OOM kills on memory-constrained systems.
#
# GOMAXPROCS (default: number of CPUs)
#   Number of OS threads for goroutine execution.
#   For latency: Leave at default to maximize parallelism.
#
# GODEBUG OPTIONS FOR LATENCY:
#   gctrace=1        - Print GC stats (use to diagnose latency issues)
#   madvdontneed=1   - Return memory to OS more aggressively (Go 1.16+)
#
# ------------------------------------------------------------------------------

show_go_tuning() {
    info "Go Runtime Tuning for Minimum Latency"
    echo ""
    echo "  Mithril's primary workload is block replay with random I/O to AccountsDB."
    echo "  These settings optimize for MINIMUM LATENCY during block processing."
    echo ""

    local ram_gb
    ram_gb=$(get_ram_gb)

    echo "  ┌─────────────────────────────────────────────────────────────────────────┐"
    echo "  │ GO 1.25 'GREEN TEA' GC IMPROVEMENTS                                     │"
    echo "  ├─────────────────────────────────────────────────────────────────────────┤"
    echo "  │ Go 1.25 has significantly improved garbage collection:                  │"
    echo "  │ • Better GC pacing reduces unnecessary collection cycles                │"
    echo "  │ • Improved concurrent marking = shorter stop-the-world pauses           │"
    echo "  │ • More efficient memory allocation patterns                             │"
    echo "  │ These improvements mean less tuning is needed vs older Go versions.     │"
    echo "  └─────────────────────────────────────────────────────────────────────────┘"
    echo ""
    echo "  ENVIRONMENT VARIABLES FOR LATENCY OPTIMIZATION:"
    echo ""
    echo "    GOGC=100 (default)"
    echo "      Usually fine with Go 1.25. Only increase if you see GC pauses."
    echo ""
    echo "    GODEBUG=gctrace=1"
    echo "      Enable this temporarily to diagnose latency spikes from GC."
    echo "      Look for long 'STW' (stop-the-world) times in the output."
    echo ""
    echo "    GODEBUG=madvdontneed=1"
    echo "      Returns unused memory to OS more aggressively."
    echo "      Can help if the system is memory-constrained."
    echo ""
    echo "  Example launch command (with GC tracing for debugging):"
    echo ""
    echo "    GODEBUG=gctrace=1 ./mithril run --config config.toml"
    echo ""
    echo "  Example systemd service for production:"
    echo ""
    echo "    [Service]"
    echo "    # Go 1.25 defaults are good for latency, minimal tuning needed"
    echo "    Environment=\"GODEBUG=madvdontneed=1\""
    echo ""
    echo "  DIAGNOSING LATENCY ISSUES:"
    echo ""
    echo "    If you see latency spikes during block replay:"
    echo "    1. Enable gctrace=1 and look for long STW pauses"
    echo "    2. If GC is the issue, try GOGC=200 (fewer but longer pauses)"
    echo "    3. Check I/O wait with 'iostat -x 1' - NVMe should have low latency"
    echo "    4. Ensure 'none' I/O scheduler is set (see --io-scheduler)"
    echo ""

    # Build-time optimizations
    echo "  ┌─────────────────────────────────────────────────────────────────────────┐"
    echo "  │ BUILD-TIME OPTIMIZATIONS (GOAMD64)                                       │"
    echo "  └─────────────────────────────────────────────────────────────────────────┘"
    echo ""
    echo "  Go can generate optimized code for modern CPU instruction sets."
    echo "  Set GOAMD64 before building to enable CPU-specific optimizations."
    echo ""

    # Detect CPU capabilities
    local has_avx2=false
    local has_avx512=false
    local cpu_model=""

    if [[ -f /proc/cpuinfo ]]; then
        cpu_model=$(grep -m1 "model name" /proc/cpuinfo | cut -d: -f2 | xargs)
        grep -q "avx2" /proc/cpuinfo && has_avx2=true
        grep -q "avx512" /proc/cpuinfo && has_avx512=true
    fi

    if [[ -n "$cpu_model" ]]; then
        echo "  Your CPU: ${cpu_model}"
        echo ""
    fi

    echo "  GOAMD64 Levels:"
    echo "    v1 (default) - SSE2 only         - Works on all x86-64 CPUs"
    echo "    v3           - +AVX, AVX2, BMI   - Ryzen 1000-5000, Intel Haswell+ (2013+)"
    echo "    v4           - +AVX-512          - Ryzen 7000+, Intel Ice Lake+ (2019+)"
    echo ""

    # Recommend based on detected capabilities
    if $has_avx512; then
        success "Your CPU supports AVX-512 → recommended: GOAMD64=v4"
    elif $has_avx2; then
        success "Your CPU supports AVX2 → recommended: GOAMD64=v3"
    else
        echo "  Your CPU: using default (v1) is recommended"
    fi
    echo ""

    echo "  To build with optimizations:"
    echo ""
    if $has_avx512; then
        echo "    GOAMD64=v4 go build -o mithril ./cmd/mithril"
    elif $has_avx2; then
        echo "    GOAMD64=v3 go build -o mithril ./cmd/mithril"
    else
        echo "    go build -o mithril ./cmd/mithril"
    fi
    echo ""

    echo "  To make permanent (add to ~/.bashrc):"
    echo ""
    if $has_avx512; then
        echo "    echo 'export GOAMD64=v4' >> ~/.bashrc"
    elif $has_avx2; then
        echo "    echo 'export GOAMD64=v3' >> ~/.bashrc"
    fi
    echo ""
}

# Configure GOAMD64 in shell profile
configure_goamd64() {
    info "Configuring GOAMD64 for Optimized Go Builds"
    echo ""

    # Detect CPU capabilities
    local has_avx2=false
    local has_avx512=false
    local cpu_model=""
    local goamd64_level=""

    if [[ -f /proc/cpuinfo ]]; then
        cpu_model=$(grep -m1 "model name" /proc/cpuinfo | cut -d: -f2 | xargs)
        grep -q "avx2" /proc/cpuinfo && has_avx2=true
        grep -q "avx512" /proc/cpuinfo && has_avx512=true
    fi

    if [[ -n "$cpu_model" ]]; then
        echo "  Your CPU: ${cpu_model}"
        echo ""
    fi

    # Determine recommended level
    if $has_avx512; then
        goamd64_level="v4"
        success "Your CPU supports AVX-512 → GOAMD64=v4"
    elif $has_avx2; then
        goamd64_level="v3"
        success "Your CPU supports AVX2 → GOAMD64=v3"
    else
        warn "Your CPU only supports baseline (v1) - no optimization needed"
        echo "  The default GOAMD64=v1 is appropriate for your system."
        return 0
    fi
    echo ""

    # Check if already configured
    local bashrc="$HOME/.bashrc"
    local profile="$HOME/.profile"
    local target_file=""

    if [[ -f "$bashrc" ]] && grep -q "export GOAMD64=" "$bashrc" 2>/dev/null; then
        local current
        current=$(grep "export GOAMD64=" "$bashrc" | tail -1 | cut -d= -f2)
        if [[ "$current" == "$goamd64_level" ]]; then
            success "GOAMD64=$goamd64_level already configured in $bashrc"
            return 0
        else
            warn "GOAMD64 already set to $current in $bashrc"
            echo "  Current: GOAMD64=$current"
            echo "  Recommended: GOAMD64=$goamd64_level"
            echo ""
            if ! prompt_yes_no "Update to GOAMD64=$goamd64_level?"; then
                echo "  Skipped."
                return 0
            fi
            # Remove old setting and add new one
            if $DRY_RUN; then
                echo "  [DRY RUN] Would update GOAMD64 in $bashrc"
            else
                sed -i '/export GOAMD64=/d' "$bashrc"
                echo "export GOAMD64=$goamd64_level" >> "$bashrc"
                success "Updated GOAMD64=$goamd64_level in $bashrc"
            fi
            return 0
        fi
    fi

    # Not configured yet - add it
    if [[ -f "$bashrc" ]]; then
        target_file="$bashrc"
    elif [[ -f "$profile" ]]; then
        target_file="$profile"
    else
        target_file="$bashrc"  # Create bashrc if neither exists
    fi

    echo "  GOAMD64=$goamd64_level enables CPU-optimized code generation."
    echo "  This can improve performance for compute-intensive operations."
    echo ""
    if ! prompt_yes_no "Add 'export GOAMD64=$goamd64_level' to $target_file?"; then
        echo "  Skipped."
        echo ""
        echo "  To configure manually, run:"
        echo "    echo 'export GOAMD64=$goamd64_level' >> $target_file"
        return 0
    fi

    if $DRY_RUN; then
        echo "  [DRY RUN] Would add GOAMD64=$goamd64_level to $target_file"
    else
        echo "" >> "$target_file"
        echo "# GOAMD64 for optimized Go builds (added by mithril performance-tune.sh)" >> "$target_file"
        echo "export GOAMD64=$goamd64_level" >> "$target_file"
        success "Added GOAMD64=$goamd64_level to $target_file"
        echo ""
        echo "  To apply now, run: source $target_file"
        echo "  Or start a new terminal session."
    fi
    echo ""
}

# ------------------------------------------------------------------------------
# Main Entry Point
# ------------------------------------------------------------------------------

show_help() {
    head -60 "$0" | tail -n +2 | grep -E "^#" | sed 's/^# \?//'
    exit 0
}

main() {
    local do_all=false
    local do_trim=false
    local do_sysctl=false
    local do_cpu=false
    local do_noatime=false
    local do_mount_perf=false
    local do_io_scheduler=false
    local do_readahead=false
    local do_hugepages=false
    local do_go_tuning=false
    local do_goamd64=false
    local do_status=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --all)          do_all=true ;;
            --trim)         do_trim=true ;;
            --sysctl)       do_sysctl=true ;;
            --cpu)          do_cpu=true ;;
            --noatime)      do_noatime=true ;;
            --mount-perf)   do_mount_perf=true ;;
            --io-scheduler) do_io_scheduler=true ;;
            --readahead)    do_readahead=true ;;
            --hugepages)    do_hugepages=true ;;
            --go-tuning)    do_go_tuning=true ;;
            --goamd64)      do_goamd64=true ;;
            --dry-run)      DRY_RUN=true ;;
            --status)       do_status=true ;;
            --help|-h)      show_help ;;
            *)              die "Unknown option: $1. Use --help for usage." ;;
        esac
        shift
    done

    # Status doesn't require root
    if $do_status; then
        show_status
        exit 0
    fi

    # Go tuning is informational, doesn't require root
    if $do_go_tuning; then
        show_go_tuning
        exit 0
    fi

    # GOAMD64 configuration modifies user's bashrc, doesn't require root
    if $do_goamd64; then
        configure_goamd64
        exit 0
    fi

    # Everything else requires root
    check_root

    echo ""
    echo "================================================================================"
    echo "           MITHRIL PERFORMANCE TUNING SCRIPT"
    echo "================================================================================"

    if $DRY_RUN; then
        echo ""
        warn "DRY-RUN MODE: No changes will be made"
    fi

    # If --all, do everything except go-tuning (which is just informational)
    if $do_all; then
        do_trim=true
        do_sysctl=true
        do_cpu=true
        do_noatime=true
        do_io_scheduler=true
        do_readahead=true
        # do_hugepages excluded - THP options are all "disable/limit", not a positive optimization
    fi

    # If specific options given, run only those
    if $do_trim || $do_sysctl || $do_cpu || $do_noatime || $do_mount_perf || $do_io_scheduler || $do_readahead || $do_hugepages; then
        $do_trim && enable_trim
        $do_sysctl && apply_sysctls
        $do_cpu && set_cpu_perf
        $do_noatime && apply_noatime
        $do_mount_perf && configure_advanced_mount_options
        $do_io_scheduler && set_io_scheduler
        $do_readahead && set_readahead
        $do_hugepages && configure_hugepages
    else
        # Interactive mode: ask for each
        echo ""
        echo "No options specified. Running in interactive mode."
        echo "Run with --help to see available options."
        echo ""

        echo "=== RECOMMENDED OPTIMIZATIONS ==="
        echo ""

        if yesno "Enable weekly SSD TRIM?" y; then
            enable_trim
        fi

        if yesno "Apply kernel performance tuning (sysctl)?" y; then
            apply_sysctls
        fi

        if yesno "Set CPU to performance mode?" y; then
            set_cpu_perf
        fi

        echo ""
        if yesno "Show Go build/runtime tuning recommendations?" y; then
            show_go_tuning
        fi

        if yesno "Configure noatime mount option?" y; then
            apply_noatime
        fi

        if yesno "Configure I/O scheduler for NVMe?"; then
            set_io_scheduler
        fi

        if yesno "Configure disk read-ahead? (per-device tuning available)" y; then
            set_readahead
        fi

        # THP (Transparent Huge Pages) configuration commented out.
        # All THP options are essentially "disable or limit" - not a positive optimization.
        # Modern kernels default to "madvise" which is already reasonable.
        # Uncomment if you need to force "never" for extreme latency sensitivity:
        # if yesno "Configure Transparent Huge Pages?"; then
        #     configure_hugepages
        # fi

        echo ""
        echo "=== EXPERIMENTAL OPTIMIZATIONS ==="
        echo "(These options trade safety for performance - use with caution)"
        echo ""

        if yesno "Configure advanced ext4 mount options? (barrier=0, data=writeback - RISKY)"; then
            configure_advanced_mount_options
        fi
    fi

    echo ""
    echo "================================================================================"
    if $DRY_RUN; then
        echo "  DRY-RUN COMPLETE - No changes were made"
    else
        success "Performance tuning complete!"
        echo ""
        echo "  Run '$0 --status' to verify current settings"
        echo "  Run '$0 --go-tuning' for Go runtime environment variable tips"
    fi
    echo "================================================================================"
    echo ""
}

main "$@"
