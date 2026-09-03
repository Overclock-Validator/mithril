#!/usr/bin/env bash
# ==============================================================================
# disk-setup.sh - Mithril Storage Setup
# ==============================================================================
#
# PURPOSE:
# Sets up NVMe storage optimally for Mithril. This script helps you configure
# your drives for the best performance during block verification.
#
# ==============================================================================
# UNDERSTANDING MITHRIL'S STORAGE NEEDS
# ==============================================================================
#
# Mithril stores three types of data, each with different I/O patterns:
#
# 1. ACCOUNTSDB (Most Critical - Needs Fastest Drive)
#    What: The state of all Solana accounts (balances, program data, etc.)
#    I/O pattern: Small RANDOM reads/writes - millions per second
#    Why critical: Slow random I/O = slow block verification
#    Size: ~500 GB currently (reserve 700 GB to be safe)
#
# 2. SNAPSHOTS (Can Use Slower Drive)
#    What: Downloaded Solana snapshots from the network
#    I/O pattern: Large SEQUENTIAL streaming reads during sync
#    Size: ~100 GB per snapshot
#
# 3. BLOCKSTORE (Optional - Can Use Slower Drive)
#    What: Verified blocks after processing
#    I/O pattern: SEQUENTIAL writes, parallel I/O during verification
#    Size: Configurable - depends on how many blocks you want to store
#
# OVER-PROVISIONING (IMPORTANT FOR SSD LONGEVITY AND PERFORMANCE):
#   Leave 15-20% of your SSD unallocated (no partition at all). This gives the
#   SSD controller guaranteed "scratch space" for garbage collection and wear
#   leveling. Performance degrades significantly when drives exceed 80% full.
#
#   Static OP (unallocated space) is better than dynamic OP (free space in a
#   partition that relies on TRIM commands). Static OP is always available to
#   the controller immediately.
#
#   For a 1 TB drive: leave ~150-200 GB unallocated.
#   For a 2 TB drive: leave ~300-400 GB unallocated.
#
# RECOMMENDED SETUP:
#   - Single Drive:  Put everything on your fastest NVMe (keep 20% unallocated)
#   - Two Drives:    AccountsDB on fast drive, snapshots/blockstore on second
#
# NOTE: Separate partitions on the SAME drive won't improve I/O performance -
#       the underlying storage is still shared. Partitions help with:
#       - Organization (e.g., reformat just AccountsDB without touching OS)
#       - Preventing disk-full issues in one area from affecting others
#       - Different mount options per partition
#       You need separate PHYSICAL drives for actual I/O isolation.
#
# ==============================================================================
# FILESYSTEM CHOICE: ext4 vs xfs
# ==============================================================================
#
# On modern NVMe, the difference is usually 0-10% for general workloads.
# Specific patterns can see 20-50% swings either way.
#
# ext4 tends to be better for:
#   + Small-file / metadata-heavy workloads (like AccountsDB random I/O)
#   + Lower latency variance on mixed workloads
#   + Safer default for general Linux use
#
# xfs tends to be better for:
#   + High parallelism / many threads doing I/O
#   + Large files + sustained throughput (streaming snapshots)
#   + Huge directories at scale
#   + Fast crash recovery (no long fsck times)
#
# RECOMMENDATION FOR MITHRIL:
#   - AccountsDB drive: ext4 (random I/O, small reads/writes)
#   - Snapshots/Blockstore drive: xfs may have edge (sequential, parallel I/O)
#   - Single drive setup: ext4 is the safer default
#
# We're still benchmarking - join #mithril-hardware on Discord for updates.
#
# ==============================================================================
# USAGE
# ==============================================================================
#
#   sudo ./scripts/disk-setup.sh --benchmark          # Test drive speeds
#   sudo ./scripts/disk-setup.sh --setup              # Format & configure
#   ./scripts/disk-setup.sh --status                  # Show current config
#   ./scripts/disk-setup.sh --disk-info               # Show UUIDs and device info
#   ./scripts/disk-setup.sh --disk-summary            # Show Mithril data usage & diagnostics
#
# CLEAN OPTIONS (for resetting Mithril - start fresh):
#   --clean-accounts     Clear accounts data (forces new snapshot sync)
#   --clean-ledger       Clear ledger data (snapshots + blockstore)
#   --clean-snapshots    Clear downloaded snapshots only
#   --clean-blockstore   Clear verified blocks only
#   --clean-all          Clear everything (complete reset)
#
# NUCLEAR OPTIONS (full teardown):
#   --reset              Unmount, remove fstab entries, delete all directories
#   --reset --wipe-disks Also wipefs the Mithril partitions (requires extra confirmation)
#
# MAINTENANCE:
#   --fix-noatime        Add noatime mount option to existing Mithril partitions
#
# SAFETY:
#   - Preserves existing OS partitions (only uses free space in single-drive mode)
#   - NEVER formats drives with mounted partitions
#   - Requires typing a confirmation phrase before any destructive action
#   - Creates backups of /etc/fstab before modification
#
# ==============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

die()     { echo -e "\n${RED}[ERROR]${NC} $*\n" >&2; exit 1; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*" >&2; }
info()    { echo -e "\n${BLUE}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC} $*"; }

# Cleanup trap - ensures temp files are removed on exit/interrupt
cleanup() {
    rm -f /tmp/mithril_bench_* 2>/dev/null || true
}

# Handle Ctrl+C gracefully
interrupt_handler() {
    echo ""
    echo "Interrupted."
    cleanup
    exit 130
}
trap cleanup EXIT
trap interrupt_handler INT TERM

# Spinner for long-running operations
spinner() {
    local pid=$1
    local msg="${2:-Working}"
    local spin='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'
    local i=0
    while kill -0 "$pid" 2>/dev/null; do
        printf "\r  %s %s... " "${spin:i++%${#spin}:1}" "$msg"
        sleep 0.1
    done
    printf "\r%-80s\n" ""
}

check_root() {
    [[ $EUID -eq 0 ]] || die "This script must be run as root. Try: sudo $0"
}

# Get the real user (the one who ran sudo, not root)
get_real_user() {
    # SUDO_USER is set when running with sudo
    if [[ -n "${SUDO_USER:-}" && "$SUDO_USER" != "root" ]]; then
        echo "$SUDO_USER"
    else
        # Fallback: try to find a non-root user who owns common home dirs
        for user in ubuntu mithril; do
            if id -u "$user" &>/dev/null; then
                echo "$user"
                return
            fi
        done
        # Last resort: current user (might be root)
        whoami
    fi
}

# Fix ownership of Mithril directories so non-root user can access
fix_mithril_ownership() {
    local real_user
    real_user=$(get_real_user)

    if [[ "$real_user" == "root" ]]; then
        warn "Could not determine non-root user. Directories will be owned by root."
        warn "Run: sudo chown -R YOUR_USER:YOUR_USER /mnt/mithril-*"
        return
    fi

    info "Setting ownership to $real_user for Mithril directories..."

    for dir in /mnt/mithril-accounts /mnt/mithril-ledger /mnt/mithril-logs; do
        if [[ -d "$dir" ]]; then
            chown -R "$real_user:$real_user" "$dir"
            success "Set ownership: $dir -> $real_user"
        fi
    done
}

# Check and install required dependencies for disk operations
check_disk_deps() {
    local missing=()

    # Check for required commands
    command -v parted >/dev/null 2>&1 || missing+=("parted")
    command -v wipefs >/dev/null 2>&1 || missing+=("util-linux")  # wipefs is in util-linux
    command -v mkfs.ext4 >/dev/null 2>&1 || missing+=("e2fsprogs")
    command -v mkfs.xfs >/dev/null 2>&1 || missing+=("xfsprogs")
    command -v fuser >/dev/null 2>&1 || missing+=("psmisc")       # fuser, killall
    command -v lsof >/dev/null 2>&1 || missing+=("lsof")
    command -v pvs >/dev/null 2>&1 || missing+=("lvm2")           # LVM tools

    if [[ ${#missing[@]} -gt 0 ]]; then
        echo ""
        warn "Missing required tools: ${missing[*]}"
        echo ""
        echo "These tools are needed for disk formatting operations."
        echo ""

        if yesno "Install missing dependencies now?"; then
            echo ""
            info "Installing: ${missing[*]}..."
            if apt-get update -qq && apt-get install -y -qq "${missing[@]}"; then
                info "Dependencies installed successfully."
            else
                die "Failed to install dependencies. Please install manually: sudo apt install ${missing[*]}"
            fi
        else
            die "Cannot proceed without required tools. Install with: sudo apt install ${missing[*]}"
        fi
    fi
}

yesno() {
    local prompt="$1" default="${2:-n}"
    local hint="[y/N]"
    [[ "${default,,}" == "y" ]] && hint="[Y/n]"
    local answer
    read -r -p "$prompt $hint: " answer
    answer="${answer:-$default}"
    [[ "${answer,,}" == "y" ]]
}

confirm_destructive() {
    local phrase="$1"
    echo ""
    echo -e "${RED}!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo "DESTRUCTIVE ACTION - DATA WILL BE PERMANENTLY ERASED"
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo -e "${NC}"
    echo "To proceed, type exactly:"
    echo -e "  ${YELLOW}$phrase${NC}"
    echo ""
    read -r -p "> " got
    [[ "$got" == "$phrase" ]] || die "Confirmation failed. Aborting."
}

# ------------------------------------------------------------------------------
# Disk Detection
# ------------------------------------------------------------------------------

get_root_disk() {
    local root_src root_pk
    root_src="$(findmnt -n -o SOURCE / 2>/dev/null || true)"

    # Handle rescue mode where root is overlay/tmpfs/loop
    if [[ -z "$root_src" ]] || [[ "$root_src" == "overlay" ]] || [[ "$root_src" == tmpfs* ]] || [[ "$root_src" == /dev/loop* ]]; then
        echo "none"  # No physical root disk (rescue/live mode)
        return 0
    fi

    root_pk="$(lsblk -no PKNAME "$root_src" 2>/dev/null || true)"
    if [[ -z "$root_pk" ]]; then
        echo "none"  # Can't determine parent disk
        return 0
    fi
    echo "/dev/$root_pk"
}

# SAFETY: Check if a path is on the root/OS disk
# Returns 0 (true) if the path is on the OS disk, 1 (false) otherwise
path_on_root_disk() {
    local path="$1"
    local root_disk
    root_disk=$(get_root_disk)

    # In rescue mode, there's no root disk to protect
    [[ "$root_disk" == "none" ]] && return 1

    # Get the device backing this path
    local path_device path_disk
    path_device="$(findmnt -n -o SOURCE --target "$path" 2>/dev/null || true)"

    # If path isn't mounted, it's not on root disk
    [[ -z "$path_device" ]] && return 1

    # Get the parent disk of this device
    path_disk="$(lsblk -no PKNAME "$path_device" 2>/dev/null || true)"
    [[ -z "$path_disk" ]] && return 1

    # Compare with root disk
    [[ "/dev/$path_disk" == "$root_disk" ]]
}

list_nvme_disks() {
    lsblk -dn -o NAME,TYPE | awk '$2=="disk" && $1~/^nvme/ {print "/dev/"$1}'
}

disk_has_mounts() {
    local disk="$1"
    lsblk -nr -o MOUNTPOINT "$disk" 2>/dev/null | grep -qv '^$'
}

disk_info() {
    local disk="$1"
    local size model
    size=$(lsblk -dn -o SIZE "$disk" 2>/dev/null | xargs)
    model=$(lsblk -dn -o MODEL "$disk" 2>/dev/null | xargs)
    echo "$size - $model"
}

disk_model() {
    local disk="$1"
    lsblk -dn -o MODEL "$disk" 2>/dev/null | xargs
}

disk_size() {
    local disk="$1"
    lsblk -dn -o SIZE "$disk" 2>/dev/null | xargs
}

part_path() {
    local disk="$1" partnum="$2"
    # NVMe uses 'p' prefix: /dev/nvme0n1p1, traditional drives use just number: /dev/sda1
    if [[ "$disk" == *nvme* || "$disk" == *mmcblk* || "$disk" == *loop* ]]; then
        echo "${disk}p${partnum}"
    else
        echo "${disk}${partnum}"
    fi
}

# ------------------------------------------------------------------------------
# Benchmarking
# ------------------------------------------------------------------------------

# Check if fio is available
has_fio() {
    command -v fio >/dev/null 2>&1
}

# Check if hdparm is available
has_hdparm() {
    command -v hdparm >/dev/null 2>&1
}

# Random 4K read benchmark using fio on raw block device
# This is READ-ONLY and safe - does not write to the device
benchmark_fio_random_read() {
    local disk="$1"
    local runtime="${2:-15}"  # default 15 seconds

    if ! has_fio; then
        echo "0"
        return
    fi

    # Run fio directly on raw block device (read-only, no writes)
    local iops
    iops=$(fio --name=randread \
        --filename="$disk" \
        --direct=1 \
        --ioengine=libaio \
        --rw=randread \
        --bs=4k \
        --iodepth=32 \
        --numjobs=1 \
        --runtime="$runtime" \
        --time_based \
        --readonly \
        --group_reporting \
        --output-format=json 2>/dev/null | \
        grep -oP '"iops"\s*:\s*[\d.]+' | head -1 | grep -oP '[\d.]+' || echo "0")

    # Return IOPS (thousands)
    if [[ -n "$iops" && "$iops" != "0" ]]; then
        # Convert to K IOPS for cleaner display
        awk "BEGIN {printf \"%.1f\", $iops / 1000}"
    else
        echo "0"
    fi
}

# Sequential read benchmark using hdparm (fallback)
benchmark_sequential_read() {
    local disk="$1"

    if ! has_hdparm; then
        echo "0"
        return
    fi

    local speed
    speed=$(hdparm -t "$disk" 2>/dev/null | grep -oP '[\d.]+\s*MB/sec' | grep -oP '[\d.]+' | head -1 || echo "0")
    echo "${speed:-0}"
}

# Convert human-readable size (e.g., "1.9T", "476.9G") to GB
size_to_gb() {
    local size="$1"
    local num unit

    # Extract number and unit
    num=$(echo "$size" | grep -oP '[\d.]+')
    unit=$(echo "$size" | grep -oP '[A-Za-z]+$')

    case "${unit^^}" in
        T|TB)  awk "BEGIN {printf \"%.0f\", $num * 1024}" ;;
        G|GB)  awk "BEGIN {printf \"%.0f\", $num}" ;;
        M|MB)  awk "BEGIN {printf \"%.0f\", $num / 1024}" ;;
        *)     echo "0" ;;
    esac
}

# Minimum size for AccountsDB in GB (currently ~500GB, reserve 700GB to be safe)
MIN_ACCOUNTSDB_SIZE_GB=700

# Get unallocated (free) space on a disk in GB
# Returns the largest contiguous free space
get_disk_free_space_gb() {
    local disk="$1"

    # Use parted to find free space
    # parted output example: "1049kB  500GB  500GB  Free Space"
    local free_space
    free_space=$(parted -s "$disk" unit GB print free 2>/dev/null | \
        grep -i "free space" | \
        awk '{print $3}' | \
        sed 's/GB//' | \
        sort -rn | \
        head -1)

    if [[ -z "$free_space" ]]; then
        echo "0"
    else
        # Round to integer
        printf "%.0f" "$free_space"
    fi
}

# Get the end position of the last partition in GB (for creating new partition)
get_last_partition_end_gb() {
    local disk="$1"

    local end_pos
    end_pos=$(parted -s "$disk" unit GB print 2>/dev/null | \
        grep -E "^\s*[0-9]+" | \
        tail -1 | \
        awk '{print $3}' | \
        sed 's/GB//')

    if [[ -z "$end_pos" ]]; then
        echo "1"  # Start at 1GB if no partitions
    else
        printf "%.0f" "$end_pos"
    fi
}

# Global variable set by format functions (avoids command substitution stdout issues)
FORMATTED_PARTITION=""

# Create a partition on existing disk (without wiping)
# Sets FORMATTED_PARTITION to the new partition path
create_partition_on_disk() {
    local disk="$1"
    local fstype="$2"
    local label="$3"
    local size_gb="${4:-0}"  # 0 = use all remaining space (minus over-provisioning)

    local start_gb end_gb disk_size_gb free_space_gb next_partnum

    # Get disk total size
    disk_size_gb=$(size_to_gb "$(disk_size "$disk")")

    # Get where to start (after last partition)
    start_gb=$(get_last_partition_end_gb "$disk")

    # Calculate end position
    if [[ "$size_gb" -eq 0 ]]; then
        # Use 80% of remaining space (20% over-provisioning)
        free_space_gb=$((disk_size_gb - start_gb))
        local usable_gb=$((free_space_gb * 80 / 100))
        end_gb=$((start_gb + usable_gb))
    else
        end_gb=$((start_gb + size_gb))
    fi

    # Find next partition number
    next_partnum=$(( $(parted -s "$disk" print 2>/dev/null | grep -cE "^\s*[0-9]+") + 1 ))

    info "Creating partition on $disk..."
    echo "  Start: ${start_gb}GB, End: ${end_gb}GB"
    echo "  Over-provisioning: 20% of remaining space left unallocated"

    # Create the partition
    parted -s "$disk" mkpart primary "${start_gb}GB" "${end_gb}GB"

    # Wait for partition to appear
    sleep 1
    partprobe "$disk" 2>/dev/null || true
    sleep 1

    FORMATTED_PARTITION=$(part_path "$disk" "$next_partnum")

    if [[ ! -b "$FORMATTED_PARTITION" ]]; then
        die "Failed to create partition. Expected $FORMATTED_PARTITION but it doesn't exist."
    fi

    # Format the partition
    info "Formatting $FORMATTED_PARTITION with $fstype..."
    case "$fstype" in
        ext4)
            mkfs.ext4 -F -L "$label" "$FORMATTED_PARTITION"
            ;;
        xfs)
            mkfs.xfs -f -L "$label" "$FORMATTED_PARTITION"
            ;;
        *)
            die "Unknown filesystem: $fstype"
            ;;
    esac
}

run_benchmarks() {
    info "Benchmarking NVMe drives..."
    echo ""
    echo "  This tests read performance to help determine which drive"
    echo "  should be used for AccountsDB (requires fastest random I/O)."
    echo ""

    local root_disk
    root_disk=$(get_root_disk)

    mapfile -t nvme_disks < <(list_nvme_disks)

    if [[ ${#nvme_disks[@]} -eq 0 ]]; then
        warn "No NVMe drives detected"
        return 1
    fi

    # Show detailed disk info
    echo "  ┌─────────────────────────────────────────────────────────────────────────┐"
    echo "  │ DETECTED NVME DRIVES                                                     │"
    echo "  ├─────────────────────────────────────────────────────────────────────────┤"
    for disk in "${nvme_disks[@]}"; do
        local model size note=""
        model=$(disk_model "$disk")
        size=$(disk_size "$disk")
        [[ "$disk" == "$root_disk" ]] && note=" (OS)"
        printf "  │ %-12s  %-8s  %-45s│\n" "$disk" "$size" "$model"
        if [[ -n "$note" ]]; then
            printf "  │              ${YELLOW}%s${NC}%-*s│\n" "$note" $((54 - ${#note})) ""
        fi
    done
    echo "  └─────────────────────────────────────────────────────────────────────────┘"
    echo ""

    # Ensure fio is installed for accurate benchmarking
    if ! has_fio; then
        echo "  Installing fio for accurate random I/O benchmarking..."
        if apt-get update -qq && apt-get install -y -qq fio; then
            echo "  fio installed successfully."
        else
            if has_hdparm; then
                warn "Failed to install fio - falling back to hdparm (less accurate)"
            else
                die "Failed to install fio and hdparm not available. Install manually: sudo apt install fio"
            fi
        fi
        echo ""
    fi

    # Determine benchmark mode based on available tools
    local use_fio=false
    local use_hdparm=false

    if has_fio; then
        use_fio=true
        echo "  Using fio for random 4K read benchmark (best for AccountsDB selection)"
        echo "  Test duration: ~15 seconds per drive"
    elif has_hdparm; then
        use_hdparm=true
        warn "fio not installed - using hdparm sequential read test (less accurate)"
    else
        die "Neither fio nor hdparm installed. Install fio: sudo apt install fio"
    fi
    echo ""

    echo "  Running benchmarks on ${#nvme_disks[@]} drive(s)..."
    echo ""

    declare -A results
    declare -A disk_models
    declare -A disk_is_os
    local metric_label="MB/s"
    local metric_desc="sequential"

    if $use_fio; then
        metric_label="K IOPS"
        metric_desc="random 4K"
    fi

    for disk in "${nvme_disks[@]}"; do
        local model
        model=$(disk_model "$disk")
        disk_models[$disk]="$model"
        disk_is_os[$disk]="no"
        [[ "$disk" == "$root_disk" ]] && disk_is_os[$disk]="yes"

        local display_note=""
        [[ "$disk" == "$root_disk" ]] && display_note=" (OS)"

        # Run appropriate benchmark in background with spinner
        if $use_fio; then
            benchmark_fio_random_read "$disk" 15 > /tmp/mithril_bench_result_$$ 2>/dev/null &
        else
            benchmark_sequential_read "$disk" > /tmp/mithril_bench_result_$$ 2>/dev/null &
        fi

        local bench_pid=$!
        spinner $bench_pid "Testing $disk ($model)$display_note"
        wait $bench_pid 2>/dev/null || true

        local result
        result=$(cat /tmp/mithril_bench_result_$$ 2>/dev/null || echo "0")
        rm -f /tmp/mithril_bench_result_$$ 2>/dev/null || true

        results[$disk]="$result"

        success "  $disk: ${result} ${metric_label}$display_note"
    done

    echo ""
    echo "  ┌─────────────────────────────────────────────────────────────────────────┐"
    printf "  │ BENCHMARK RESULTS (%-13s read)                                   │\n" "$metric_desc"
    echo "  ├─────────────────────────────────────────────────────────────────────────┤"
    printf "  │ %-12s  %-12s  %-8s  %-32s│\n" "DEVICE" "SPEED" "SIZE" "MODEL"
    echo "  ├─────────────────────────────────────────────────────────────────────────┤"

    local best_disk="" best_speed=0
    local best_large_disk="" best_large_speed=0
    local fastest_too_small=""

    for disk in "${!results[@]}"; do
        local speed="${results[$disk]}"
        local model="${disk_models[$disk]}"
        local size size_gb os_marker=""
        size=$(disk_size "$disk")
        size_gb=$(size_to_gb "$size")

        # Mark OS disk
        [[ "${disk_is_os[$disk]}" == "yes" ]] && os_marker=" (OS)"

        # Truncate model if too long (account for OS marker)
        local max_model_len=$((32 - ${#os_marker}))
        [[ ${#model} -gt $max_model_len ]] && model="${model:0:$((max_model_len - 3))}..."

        printf "  │ %-12s  %6s %-5s  %-8s  %-32s│\n" "$disk" "$speed" "$metric_label" "$size" "${model}${os_marker}"

        # Track absolute best non-OS disk (regardless of size)
        if [[ "${disk_is_os[$disk]}" != "yes" ]]; then
            if awk "BEGIN {exit !($speed > $best_speed)}" 2>/dev/null; then
                best_speed="$speed"
                best_disk="$disk"
            fi

            # Track best drive that's large enough for AccountsDB (>= 700GB)
            if [[ "$size_gb" -ge "$MIN_ACCOUNTSDB_SIZE_GB" ]]; then
                if awk "BEGIN {exit !($speed > $best_large_speed)}" 2>/dev/null; then
                    best_large_speed="$speed"
                    best_large_disk="$disk"
                fi
            fi
        fi
    done

    echo "  └─────────────────────────────────────────────────────────────────────────┘"
    echo ""

    # Determine which disk to recommend
    local recommended_disk="$best_disk"
    local best_size_gb

    if [[ -n "$best_disk" ]]; then
        best_size_gb=$(size_to_gb "$(disk_size "$best_disk")")

        # If the fastest disk is too small, recommend the fastest large disk instead
        if [[ "$best_size_gb" -lt "$MIN_ACCOUNTSDB_SIZE_GB" && -n "$best_large_disk" ]]; then
            recommended_disk="$best_large_disk"
            fastest_too_small="$best_disk"
        fi
    fi

    if [[ -n "$recommended_disk" ]]; then
        local rec_model="${disk_models[$recommended_disk]}"
        local rec_size
        rec_size=$(disk_size "$recommended_disk")

        success "Recommended for AccountsDB: $recommended_disk"
        echo "    Model: $rec_model"
        echo "    Size:  $rec_size"

        # Explain why we didn't pick the fastest if applicable
        if [[ -n "$fastest_too_small" ]]; then
            local small_size small_model
            small_size=$(disk_size "$fastest_too_small")
            small_model="${disk_models[$fastest_too_small]}"
            echo ""
            warn "$fastest_too_small is slightly faster but too small ($small_size)"
            echo "    AccountsDB needs at least ${MIN_ACCOUNTSDB_SIZE_GB}GB (currently ~500GB, growing)."
            echo "    The speed difference is negligible (<1%), but size matters."
        fi

        echo ""
        if $use_fio; then
            echo "  Random 4K IOPS is the key metric for AccountsDB performance."
            echo "  Higher IOPS = faster block verification."
        else
            echo "  Note: Sequential read speed is a rough approximation."
            echo "  Install fio for accurate random I/O testing: sudo apt install fio"
        fi
        echo ""
        echo "  Use your fastest drive for AccountsDB, slower drive(s) for snapshots/blocks."
    elif [[ -n "$best_disk" ]]; then
        # All drives are too small
        local best_model="${disk_models[$best_disk]}"
        local best_size
        best_size=$(disk_size "$best_disk")

        warn "No drives meet the minimum size requirement (${MIN_ACCOUNTSDB_SIZE_GB}GB)"
        echo ""
        echo "  Fastest drive: $best_disk ($best_size)"
        echo "    Model: $best_model"
        echo ""
        echo "  AccountsDB currently needs ~500GB and is growing."
        echo "  You may run out of space. Consider a larger drive."
    elif [[ ${#results[@]} -gt 0 ]]; then
        # Only OS disk(s) available - show performance for reference
        echo "  Only OS disk(s) tested. Performance shown above for reference."
        echo ""
        echo "  In single-drive mode, the setup will create a partition on the OS disk"
        echo "  for Mithril data. Use --setup to proceed."
    fi
}

# ------------------------------------------------------------------------------
# Disk Info (for manual partitioning)
# ------------------------------------------------------------------------------

show_disk_info() {
    echo ""
    echo "================================================================================"
    echo "                    DISK INFORMATION FOR MANUAL PARTITIONING"
    echo "================================================================================"
    echo ""
    echo "Use this information when manually partitioning during Ubuntu installation"
    echo "or with tools like fdisk/parted/gparted."
    echo ""

    local root_disk
    root_disk=$(get_root_disk 2>/dev/null || echo "unknown")
    echo "Current root disk: $root_disk"
    echo ""

    echo "--- All Block Devices ---"
    echo ""
    lsblk -o NAME,SIZE,TYPE,MODEL,SERIAL,TRAN 2>/dev/null || lsblk -o NAME,SIZE,TYPE,MODEL
    echo ""

    echo "--- NVMe Drives (Detailed) ---"
    echo ""
    mapfile -t nvme_disks < <(list_nvme_disks)

    if [[ ${#nvme_disks[@]} -eq 0 ]]; then
        echo "  No NVMe drives detected"
    else
        for disk in "${nvme_disks[@]}"; do
            local model size serial
            model=$(lsblk -dn -o MODEL "$disk" 2>/dev/null | xargs)
            size=$(lsblk -dn -o SIZE "$disk" 2>/dev/null | xargs)
            serial=$(lsblk -dn -o SERIAL "$disk" 2>/dev/null | xargs)

            echo "  Device: $disk"
            echo "    Model:  $model"
            echo "    Size:   $size"
            [[ -n "$serial" ]] && echo "    Serial: $serial"

            # Show partitions and their UUIDs
            echo "    Partitions:"
            while IFS= read -r line; do
                local part_name part_size part_fstype part_uuid part_label
                part_name=$(echo "$line" | awk '{print $1}')
                part_size=$(echo "$line" | awk '{print $2}')
                part_fstype=$(echo "$line" | awk '{print $3}')
                part_uuid=$(echo "$line" | awk '{print $4}')
                part_label=$(echo "$line" | awk '{print $5}')

                if [[ "$part_name" != "$disk" && -n "$part_name" ]]; then
                    echo "      /dev/$part_name  ($part_size, $part_fstype)"
                    [[ -n "$part_uuid" && "$part_uuid" != "-" ]] && echo "        UUID:  $part_uuid"
                    [[ -n "$part_label" && "$part_label" != "-" ]] && echo "        Label: $part_label"
                fi
            done < <(lsblk -rno NAME,SIZE,FSTYPE,UUID,LABEL "$disk" 2>/dev/null)
            echo ""
        done
    fi

    echo "--- Partition UUIDs (for /etc/fstab) ---"
    echo ""
    echo "  Use these UUIDs in /etc/fstab for reliable mounting:"
    echo ""
    blkid | grep -E "nvme|sd" | while IFS= read -r line; do
        local dev uuid fstype label
        dev=$(echo "$line" | cut -d: -f1)
        uuid=$(echo "$line" | grep -oP 'UUID="[^"]+"' | head -1)
        fstype=$(echo "$line" | grep -oP 'TYPE="[^"]+"' | head -1)
        label=$(echo "$line" | grep -oP 'LABEL="[^"]+"' | head -1)

        echo "  $dev"
        [[ -n "$uuid" ]] && echo "    $uuid"
        [[ -n "$fstype" ]] && echo "    $fstype"
        [[ -n "$label" ]] && echo "    $label"
    done
    echo ""

    echo "--- Example fstab Entry ---"
    echo ""
    echo "  UUID=<your-uuid>  /mnt/mithril-accounts  ext4  defaults,noatime,nofail  0  2"
    echo ""
    echo "  The 'noatime' option reduces unnecessary writes (better for SSDs)."
    echo "  The 'nofail' option allows boot to continue if the drive is missing."
    echo ""

    echo "================================================================================"
}

# ------------------------------------------------------------------------------
# Status
# ------------------------------------------------------------------------------

show_status() {
    echo ""
    echo "================================================================================"
    echo "                    MITHRIL STORAGE STATUS"
    echo "================================================================================"
    echo ""

    local root_disk
    root_disk=$(get_root_disk)
    if [[ "$root_disk" == "none" ]]; then
        echo "Mode: Rescue/Live (no physical root disk)"
    else
        echo "Root disk: $root_disk (OS - existing partitions preserved)"
    fi
    echo ""

    echo "--- NVMe Drives ---"
    mapfile -t nvme_disks < <(list_nvme_disks)

    if [[ ${#nvme_disks[@]} -eq 0 ]]; then
        warn "No NVMe drives detected"
    else
        for disk in "${nvme_disks[@]}"; do
            local info note=""
            info=$(disk_info "$disk")
            [[ "$disk" == "$root_disk" ]] && note=" (ROOT)"
            echo "  $disk: $info$note"

            # Show partitions and mounts
            lsblk -o NAME,SIZE,FSTYPE,MOUNTPOINT "$disk" 2>/dev/null | tail -n +2 | sed 's/^/    /'
        done
    fi
    echo ""

    echo "--- Mithril Directories ---"
    local mithril_dirs=(
        "/mnt/mithril-accounts"
        "/mnt/mithril-ledger"
        "/mnt/mithril-ledger/snapshots"
        "/mnt/mithril-ledger/blockstore"
        "/mnt/mithril-logs"
    )

    for dir in "${mithril_dirs[@]}"; do
        if [[ -d "$dir" ]]; then
            local mount_info
            mount_info=$(df -h "$dir" 2>/dev/null | tail -1 | awk '{print $1 " (" $4 " free)"}')
            success "$dir exists - $mount_info"
        else
            echo "  $dir - not found"
        fi
    done
    echo ""

    echo "--- Mithril fstab Entries ---"
    if grep -q mithril /etc/fstab 2>/dev/null; then
        grep mithril /etc/fstab | sed 's/^/  /'
    else
        echo "  No Mithril entries in fstab"
    fi

    echo ""
    echo "================================================================================"
}

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

# Format entire disk (wipes all data)
# Sets FORMATTED_PARTITION to the new partition path
format_disk() {
    local disk="$1" fstype="$2" label="$3" overprovision="${4:-20}"
    local use_percent=$((100 - overprovision))

    info "Formatting $disk..."
    echo "  Over-provisioning: ${overprovision}% unallocated (${use_percent}% usable)"
    echo "  This improves SSD longevity and maintains consistent performance."

    # Wipe and create GPT
    local wipefs_output
    if ! wipefs_output=$(wipefs -a "$disk" 2>&1); then
        if [[ "$wipefs_output" == *"busy"* ]]; then
            echo ""
            die "Device $disk is busy and cannot be wiped.

  This usually happens when the kernel still holds a reference to the device.

  Solution: Reboot your machine and try again.

  After rebooting, run:
    sudo ./scripts/disk-setup.sh --setup"
        else
            die "Failed to wipe $disk: $wipefs_output"
        fi
    fi
    parted -s "$disk" mklabel gpt
    parted -s "$disk" mkpart primary 1MiB "${use_percent}%"

    # Wait for partition to appear
    sleep 1
    partprobe "$disk" 2>/dev/null || true
    sleep 1

    FORMATTED_PARTITION=$(part_path "$disk" 1)

    if [[ ! -b "$FORMATTED_PARTITION" ]]; then
        die "Failed to create partition. Expected $FORMATTED_PARTITION but it doesn't exist."
    fi

    # Format the partition
    info "Formatting $FORMATTED_PARTITION with $fstype..."
    case "$fstype" in
        ext4)
            mkfs.ext4 -F -L "$label" "$FORMATTED_PARTITION"
            ;;
        xfs)
            mkfs.xfs -f -L "$label" "$FORMATTED_PARTITION"
            ;;
        *)
            die "Unknown filesystem: $fstype"
            ;;
    esac
}

ask_filesystem() {
    local disk="$1"
    local purpose="${2:-general}"  # "accountsdb" or "data" or "general"
    local model
    model=$(disk_model "$disk")

    # All prompts go to stderr so they don't get captured in command substitution
    echo "" >&2
    echo "  Filesystem for $disk ($model):" >&2
    echo "" >&2
    echo "    ext4: Mature, widely supported" >&2
    echo "          + Better for small random I/O (like AccountsDB)" >&2
    echo "          + Can shrink partitions offline" >&2
    echo "          + Safer default choice" >&2
    echo "" >&2
    echo "    xfs:  High-performance for parallel/sequential I/O" >&2
    echo "          + May be better for snapshots/blockstore" >&2
    echo "          - Cannot shrink (must reformat to resize down)" >&2
    echo "" >&2

    if [[ "$purpose" == "accountsdb" ]]; then
        echo "  For AccountsDB (random I/O): ext4 recommended" >&2
    elif [[ "$purpose" == "data" ]]; then
        echo "  For snapshots/blockstore (sequential I/O): xfs may have edge" >&2
    fi
    echo "" >&2

    local choice
    while true; do
        read -r -p "Enter filesystem (ext4/xfs) [ext4]: " choice
        choice="${choice:-ext4}"  # Default to ext4
        choice="${choice,,}"      # Lowercase

        case "$choice" in
            ext4|1) echo "ext4"; return ;;
            xfs|2)  echo "xfs"; return ;;
            *)
                echo "  Invalid choice '$choice'. Please enter 'ext4' or 'xfs'." >&2
                ;;
        esac
    done
}

add_fstab_entry() {
    local part="$1" mountpoint="$2" fstype="$3"

    local uuid
    uuid=$(blkid -s UUID -o value "$part")

    # Backup fstab
    cp /etc/fstab "/etc/fstab.bak.$(date +%Y%m%d_%H%M%S)"

    # Check if entry already exists
    if grep -q "$uuid" /etc/fstab 2>/dev/null; then
        warn "UUID $uuid already in fstab, skipping"
        return
    fi

    # Add entry with mithril comment
    echo "# Mithril storage" >> /etc/fstab
    echo "UUID=$uuid  $mountpoint  $fstype  defaults,noatime,nofail  0  2" >> /etc/fstab

    success "Added fstab entry for $mountpoint"
}

interactive_setup() {
    info "MITHRIL DISK SETUP"
    echo ""
    echo "  This wizard helps you configure storage for Mithril."
    echo ""
    echo "  Mithril storage layout:"
    echo "    - AccountsDB: Requires FASTEST drive (random I/O during block replay)"
    echo "    - Snapshots:  Can use slower drive (sequential I/O)"
    echo "    - Blockstore: Can use slower drive (sequential I/O)"
    echo ""
    echo "  Recommended: Use separate drives for AccountsDB and snapshots/blockstore"
    echo "               if you have multiple NVMe drives."
    echo ""

    local root_disk
    root_disk=$(get_root_disk)

    if [[ "$root_disk" == "none" ]]; then
        echo "  Rescue/Live mode detected - no physical root disk to protect"
    else
        echo "  Root disk: $root_disk (OS - existing partitions preserved)"
    fi
    echo ""

    # Find available disks
    mapfile -t nvme_disks < <(list_nvme_disks)
    local available_disks=()

    for disk in "${nvme_disks[@]}"; do
        [[ "$disk" == "$root_disk" ]] && continue
        available_disks+=("$disk")
    done

    # Handle single-drive scenario (only the OS disk is available)
    local single_drive_mode=false
    local root_free_space_gb=0

    # Check for free space on root disk (used in both single and multi-drive modes)
    if [[ "$root_disk" != "none" ]]; then
        root_free_space_gb=$(get_disk_free_space_gb "$root_disk")
    fi

    if [[ ${#available_disks[@]} -eq 0 ]]; then
        if [[ "$root_disk" == "none" ]]; then
            die "No NVMe drives detected and no root disk. Cannot proceed."
        fi

        if [[ "$root_free_space_gb" -lt 100 ]]; then
            die "No non-root NVMe drives available and root disk has insufficient free space (${root_free_space_gb}GB < 100GB minimum)."
        fi

        single_drive_mode=true

        echo ""
        echo -e "  ${YELLOW}╔═══════════════════════════════════════════════════════════════════════╗${NC}"
        echo -e "  ${YELLOW}║                    SINGLE-DRIVE MODE DETECTED                        ║${NC}"
        echo -e "  ${YELLOW}╠═══════════════════════════════════════════════════════════════════════╣${NC}"
        echo -e "  ${YELLOW}║${NC}  Your only NVMe drive ($root_disk) contains the OS.                   ${YELLOW}║${NC}"
        echo -e "  ${YELLOW}║${NC}  Free space available: ${root_free_space_gb}GB                                          ${YELLOW}║${NC}"
        echo -e "  ${YELLOW}║${NC}                                                                       ${YELLOW}║${NC}"
        echo -e "  ${YELLOW}║${NC}  This script can create partitions for Mithril on the same drive.    ${YELLOW}║${NC}"
        echo -e "  ${YELLOW}║${NC}                                                                       ${YELLOW}║${NC}"
        echo -e "  ${YELLOW}║  ⚠  IMPORTANT NOTES:                                                   ║${NC}"
        echo -e "  ${YELLOW}║${NC}    • I/O will be shared with the OS (slightly reduced performance)   ${YELLOW}║${NC}"
        echo -e "  ${YELLOW}║${NC}    • This script assumes Ubuntu - errors may occur on other distros  ${YELLOW}║${NC}"
        echo -e "  ${YELLOW}║${NC}    • OS partitions will NOT be touched - only free space is used     ${YELLOW}║${NC}"
        echo -e "  ${YELLOW}╚═══════════════════════════════════════════════════════════════════════╝${NC}"
        echo ""

        if ! yesno "  Create Mithril partitions on $root_disk using ${root_free_space_gb}GB free space?" "y"; then
            echo ""
            echo "  Alternatives:"
            echo "    1. Add another NVMe drive for dedicated Mithril storage"
            echo "    2. Manually partition using: fdisk $root_disk"
            echo "    3. Use existing directories without dedicated partitions"
            echo ""
            die "Setup cancelled. Re-run when ready."
        fi
    else
        echo "  Available NVMe drives for Mithril:"
        for disk in "${available_disks[@]}"; do
            local info
            info=$(disk_info "$disk")
            local mounted=""
            disk_has_mounts "$disk" && mounted=" ${YELLOW}(HAS MOUNTED PARTITIONS)${NC}"
            echo -e "    $disk: $info$mounted"
        done
        echo ""

        if yesno "  Would you like to run benchmarks first to find the fastest drive?" "y"; then
            run_benchmarks
            echo ""
        fi
    fi

    # Configuration collection
    local accountsdb_disk="" accountsdb_mount="/mnt/mithril-accounts"
    local data_disk="" data_mount="/mnt/mithril-ledger"
    local accountsdb_fstype="" data_fstype=""
    local use_root_partition=false

    echo ""
    echo "  STEP 1: AccountsDB Drive (most important - needs fastest drive)"
    echo ""

    if $single_drive_mode; then
        # Single-drive mode - create partition on root disk
        use_root_partition=true
        accountsdb_disk="$root_disk"

        echo "  Creating partition on OS disk: $root_disk"
        echo "  Free space: ${root_free_space_gb}GB"
        echo ""

        # For single-drive, recommend ext4 for simplicity
        echo "  Filesystem recommendation: ext4 (safer for mixed OS/data workload)"
        accountsdb_fstype=$(ask_filesystem "$root_disk" "general")
    elif [[ ${#available_disks[@]} -eq 1 ]]; then
        echo "  Only one non-OS drive available: ${available_disks[0]}"
        echo "  AccountsDB, snapshots, and blockstore will share this drive."
        accountsdb_disk="${available_disks[0]}"

        if disk_has_mounts "$accountsdb_disk"; then
            die "Drive $accountsdb_disk has mounted partitions. Unmount first or use existing partitions."
        fi

        # Ask filesystem for the single drive (general purpose since it holds everything)
        accountsdb_fstype=$(ask_filesystem "$accountsdb_disk" "general")
    else
        echo "  Which drive should be used for AccountsDB?"
        echo "  (Choose your FASTEST drive based on benchmarks)"
        echo ""

        # Build selection options - include OS drive if it has meaningful free space
        local select_options=("${available_disks[@]}")
        local os_drive_option=""
        if [[ "$root_disk" != "none" && "$root_free_space_gb" -ge 100 ]]; then
            os_drive_option="$root_disk (OS - partition only, ${root_free_space_gb}GB free)"
            select_options+=("$os_drive_option")
        fi
        select_options+=("Skip (use existing)")

        select disk in "${select_options[@]}"; do
            if [[ "$disk" == "Skip (use existing)" ]]; then
                accountsdb_disk=""
                break
            elif [[ "$disk" == "$os_drive_option" ]]; then
                # OS drive selected - use partition mode
                accountsdb_disk="$root_disk"
                use_root_partition=true
                echo ""
                echo -e "  ${YELLOW}Note: Will create partition on OS disk (existing partitions preserved)${NC}"
                break
            elif [[ -n "$disk" ]]; then
                accountsdb_disk="$disk"
                break
            fi
        done

        if [[ -n "$accountsdb_disk" ]]; then
            if $use_root_partition; then
                # OS drive - recommend ext4 for mixed workload
                echo "  Filesystem recommendation: ext4 (safer for mixed OS/data workload)"
                accountsdb_fstype=$(ask_filesystem "$accountsdb_disk" "general")
            else
                if disk_has_mounts "$accountsdb_disk"; then
                    die "Drive $accountsdb_disk has mounted partitions. Unmount first."
                fi
                # Ask filesystem for AccountsDB drive
                accountsdb_fstype=$(ask_filesystem "$accountsdb_disk" "accountsdb")
            fi
        fi

        echo ""
        echo "  STEP 2: Snapshots/Blockstore Drive"
        echo ""

        # Remove accountsdb disk from options
        local remaining_disks=()
        for disk in "${available_disks[@]}"; do
            [[ "$disk" != "$accountsdb_disk" ]] && remaining_disks+=("$disk")
        done

        # Build selection options for data drive
        local data_select_options=("${remaining_disks[@]}")
        local data_os_drive_option=""

        # Include OS drive if it has free space and wasn't used for AccountsDB
        if [[ "$root_disk" != "none" && "$root_free_space_gb" -ge 100 && "$accountsdb_disk" != "$root_disk" ]]; then
            data_os_drive_option="$root_disk (OS - partition only, ${root_free_space_gb}GB free)"
            data_select_options+=("$data_os_drive_option")
        fi
        data_select_options+=("Use same drive as AccountsDB" "Skip")

        if [[ ${#data_select_options[@]} -gt 2 ]]; then  # More than just "Use same" and "Skip"
            echo "  Would you like to use a separate drive for snapshots/blockstore?"
            echo "  (This can be a slower drive)"
            echo ""
            select disk in "${data_select_options[@]}"; do
                case "$disk" in
                    "Use same drive as AccountsDB"|"Skip")
                        data_disk=""
                        ;;
                    "$data_os_drive_option")
                        # OS drive selected for data
                        data_disk="$root_disk"
                        use_root_partition=true
                        echo ""
                        echo -e "  ${YELLOW}Note: Will create partition on OS disk for snapshots/blockstore${NC}"
                        ;;
                    *)
                        if [[ -n "$disk" ]]; then
                            data_disk="$disk"
                        fi
                        ;;
                esac
                break
            done

            if [[ -n "$data_disk" ]]; then
                if [[ "$data_disk" == "$root_disk" ]]; then
                    # OS drive - recommend ext4 for mixed workload
                    echo "  Filesystem recommendation: ext4 (safer for mixed OS/data workload)"
                    data_fstype=$(ask_filesystem "$data_disk" "general")
                elif disk_has_mounts "$data_disk"; then
                    die "Drive $data_disk has mounted partitions. Unmount first."
                else
                    # Ask filesystem for data drive (snapshots/blockstore)
                    data_fstype=$(ask_filesystem "$data_disk" "data")
                fi
            fi
        fi
    fi

    # Mount points use defaults: /mnt/mithril-accounts and /mnt/mithril-ledger

    # Summary and confirmation
    echo ""
    echo "  ┌─────────────────────────────────────────────────────────────────────────┐"
    echo "  │ SETUP SUMMARY                                                           │"
    echo "  ├─────────────────────────────────────────────────────────────────────────┤"

    if [[ -n "$accountsdb_disk" ]]; then
        local adb_model
        adb_model=$(disk_model "$accountsdb_disk")
        printf "  │ AccountsDB:  %-58s │\n" "$accountsdb_disk -> $accountsdb_mount"
        printf "  │              %-58s │\n" "Model: $adb_model"
        printf "  │              %-58s │\n" "Filesystem: $accountsdb_fstype"
        if [[ "$accountsdb_disk" == "$root_disk" ]]; then
            echo -e "  │              ${YELLOW}NEW PARTITION on OS disk (free space only)${NC}            │"
        else
            echo -e "  │              ${RED}THIS DRIVE WILL BE ERASED${NC}                                │"
        fi
    else
        echo "  │ AccountsDB:  (skipped - using existing)                               │"
    fi

    if [[ -n "$data_disk" ]]; then
        local data_model
        data_model=$(disk_model "$data_disk")
        printf "  │ Ledger:      %-58s │\n" "$data_disk -> $data_mount"
        printf "  │              %-58s │\n" "Model: $data_model"
        printf "  │              %-58s │\n" "Filesystem: $data_fstype"
        if [[ "$data_disk" == "$root_disk" ]]; then
            echo -e "  │              ${YELLOW}NEW PARTITION on OS disk (free space only)${NC}            │"
        else
            echo -e "  │              ${RED}THIS DRIVE WILL BE ERASED${NC}                                │"
        fi
    elif [[ -n "$accountsdb_disk" ]]; then
        echo "  │ Data:        (same drive as AccountsDB)                               │"
    fi

    echo "  │                                                                         │"
    echo "  │ Directory structure to be created:                                      │"
    printf "  │   %-69s │\n" "$accountsdb_mount (AccountsDB data)"
    if [[ -n "$data_disk" ]]; then
        printf "  │   %-69s │\n" "$data_mount/snapshots"
        printf "  │   %-69s │\n" "$data_mount/blockstore"
    else
        # When no data disk, ledger dirs always go to /mnt/mithril-ledger
        printf "  │   %-69s │\n" "/mnt/mithril-ledger/snapshots"
        printf "  │   %-69s │\n" "/mnt/mithril-ledger/blockstore"
    fi
    printf "  │   %-69s │\n" "/mnt/mithril-logs (log files)"
    echo "  └─────────────────────────────────────────────────────────────────────────┘"
    echo ""

    if [[ -z "$accountsdb_disk" && -z "$data_disk" ]]; then
        info "No drives selected for formatting. Creating directories only."

        # Warn if mountpoint isn't actually a mounted disk
        if ! findmnt "$accountsdb_mount" >/dev/null 2>&1; then
            echo ""
            warn "WARNING: $accountsdb_mount is NOT a mounted disk."
            echo "  Directories will be created on your OS disk (likely /)"
            echo "  This may fill up your root filesystem with Mithril data."
            echo ""
            if ! yesno "  Continue anyway?" "n"; then
                echo "  Aborting. Mount a disk to $accountsdb_mount first."
                exit 1
            fi
        fi

        mkdir -p "$accountsdb_mount"
        mkdir -p /mnt/mithril-ledger/snapshots
        mkdir -p /mnt/mithril-ledger/blockstore
        mkdir -p /mnt/mithril-logs/snapshot-finder

        success "Directories created at $accountsdb_mount, /mnt/mithril-ledger, /mnt/mithril-logs"
        return
    fi

    # Final confirmation
    local uses_os_partition=false
    [[ "$accountsdb_disk" == "$root_disk" || "$data_disk" == "$root_disk" ]] && uses_os_partition=true

    if $uses_os_partition; then
        # At least one drive is the OS disk - confirm partition
        confirm_destructive "PARTITION ${root_disk}"
    fi

    # Confirm erase for any non-OS disks
    local disks_to_erase=""
    [[ -n "$accountsdb_disk" && "$accountsdb_disk" != "$root_disk" ]] && disks_to_erase="$accountsdb_disk"
    [[ -n "$data_disk" && "$data_disk" != "$root_disk" ]] && disks_to_erase="$disks_to_erase $data_disk"
    if [[ -n "$disks_to_erase" ]]; then
        confirm_destructive "ERASE${disks_to_erase}"
    fi

    # Execute setup
    info "Starting disk setup..."

    # Format/partition AccountsDB disk
    if [[ -n "$accountsdb_disk" ]]; then
        if [[ "$accountsdb_disk" == "$root_disk" ]]; then
            # OS drive - create partition instead of wiping
            echo ""
            echo -e "  ${YELLOW}Creating partition on OS disk for AccountsDB...${NC}"
            echo "  This may take a moment. Do NOT interrupt."
            echo ""
            create_partition_on_disk "$accountsdb_disk" "$accountsdb_fstype" "mithril"
        else
            # Normal mode: format entire disk
            format_disk "$accountsdb_disk" "$accountsdb_fstype" "accountsdb"
        fi

        # FORMATTED_PARTITION is set by the format functions
        local accountsdb_part="$FORMATTED_PARTITION"

        mkdir -p "$accountsdb_mount"
        mount "$accountsdb_part" "$accountsdb_mount"
        add_fstab_entry "$accountsdb_part" "$accountsdb_mount" "$accountsdb_fstype"

        # Always create ledger directories at consistent location (even with single disk)
        if [[ -z "$data_disk" ]]; then
            mkdir -p /mnt/mithril-ledger/snapshots
            mkdir -p /mnt/mithril-ledger/blockstore
        fi

        if [[ "$accountsdb_disk" == "$root_disk" ]]; then
            success "AccountsDB partition created on OS disk: $accountsdb_part -> $accountsdb_mount ($accountsdb_fstype)"
        else
            success "AccountsDB drive configured: $accountsdb_disk -> $accountsdb_mount ($accountsdb_fstype)"
        fi
    fi

    # Format data disk
    if [[ -n "$data_disk" ]]; then
        if [[ "$data_disk" == "$root_disk" ]]; then
            # OS drive - create partition instead of wiping
            echo ""
            echo -e "  ${YELLOW}Creating partition on OS disk for snapshots/blockstore...${NC}"
            echo "  This may take a moment. Do NOT interrupt."
            echo ""
            create_partition_on_disk "$data_disk" "$data_fstype" "ledger"
        else
            # Normal mode: format entire disk
            format_disk "$data_disk" "$data_fstype" "blockstore"
        fi

        # FORMATTED_PARTITION is set by the format functions
        local data_part="$FORMATTED_PARTITION"

        mkdir -p "$data_mount"
        mount "$data_part" "$data_mount"
        add_fstab_entry "$data_part" "$data_mount" "$data_fstype"

        mkdir -p "$data_mount/snapshots"
        mkdir -p "$data_mount/blockstore"
    fi

    # Create logs directory (always on root filesystem)
    mkdir -p /mnt/mithril-logs/snapshot-finder

    if [[ -n "$data_disk" ]]; then
        if [[ "$data_disk" == "$root_disk" ]]; then
            success "Ledger partition created on OS disk: $data_part -> $data_mount ($data_fstype)"
        else
            success "Data drive configured: $data_disk -> $data_mount ($data_fstype)"
        fi
    fi

    # Reload systemd to pick up fstab changes
    systemctl daemon-reload

    # Fix ownership so non-root user can run Mithril
    fix_mithril_ownership

    echo ""
    echo "  ┌─────────────────────────────────────────────────────────────────────────┐"
    echo "  │ SETUP COMPLETE                                                           │"
    echo "  ├─────────────────────────────────────────────────────────────────────────┤"
    echo "  │                                                                          │"
    echo "  │ Directories created:                                                     │"
    echo "  │   /mnt/mithril-accounts      - AccountsDB storage                        │"
    echo "  │   /mnt/mithril-ledger        - Snapshots and blockstore                  │"
    echo "  │   /mnt/mithril-logs          - Log files                                 │"
    echo "  │                                                                          │"
    echo "  │ Update your config.toml:                                                 │"
    echo "  │                                                                          │"
    echo "  │   [ledger]                                                               │"
    echo "  │       accounts_path = \"/mnt/mithril-accounts\"                            │"
    echo "  │       snapshot_archive_path = \"/mnt/mithril-ledger/snapshots\"            │"
    echo "  │       path = \"/mnt/mithril-ledger/blockstore\"                            │"
    echo "  │                                                                          │"
    echo "  │   [log]                                                                  │"
    echo "  │       dir = \"/mnt/mithril-logs\"                                          │"
    echo "  │                                                                          │"
    echo "  │ Next: Run performance-tune.sh to optimize I/O settings                   │"
    echo "  │                                                                          │"
    echo "  └─────────────────────────────────────────────────────────────────────────┘"
}

# ------------------------------------------------------------------------------
# Clean/Reset
# ------------------------------------------------------------------------------

find_mithril_dirs() {
    # Find directories that look like Mithril data dirs
    local dirs=()

    # Check common locations
    local common_paths=(
        "/mnt/mithril-accounts"
        "/mnt/mithril-ledger"
    )

    for path in "${common_paths[@]}"; do
        [[ -d "$path" ]] && dirs+=("$path")
    done

    # Also check any mount with "mithril" in the label
    while IFS= read -r mp; do
        [[ -d "$mp" ]] && [[ ! " ${dirs[*]} " =~ " ${mp} " ]] && dirs+=("$mp")
    done < <(findmnt -n -o TARGET | grep -i mithril 2>/dev/null || true)

    printf '%s\n' "${dirs[@]}"
}

# Release AccountsDB store locks acquired by acquire_accounts_store_locks.
# The stable lock inode is deliberately left in place: unlinking it could let a
# running process and a cleaner lock two different inodes for the same store.
release_accounts_store_locks() {
    local lock_fd

    for lock_fd in "$@"; do
        flock -u "$lock_fd" 2>/dev/null || true
        exec {lock_fd}>&- 2>/dev/null || true
    done
}

# Conservatively lock every non-OS Mithril data root before a destructive
# command enumerates it. Accounts artifacts have historically been allowed at
# any discovered Mithril root, so deciding which roots to lock by inspecting
# their contents would itself race a running node.
#
# The caller must keep the returned descriptors open for the complete scan and
# deletion. clean_accounts and clean_all run in subshells as a final safety net,
# so Bash also closes every held descriptor on return, error, interrupt, or exit.
acquire_accounts_store_locks() {
    local held_fds_name="$1"
    shift
    local -n held_fds="$held_fds_name"
    local accounts_root lock_path lock_fd fd_path

    if ! command -v flock >/dev/null 2>&1; then
        warn "Cannot safely clean AccountsDB data: required command 'flock' is not installed"
        return 1
    fi

    for accounts_root in "$@"; do
        # These roots are not deletion targets because of the existing OS-disk
        # guard, so do not create a lockfile in them either.
        if path_on_root_disk "$accounts_root"; then
            continue
        fi
        if [[ -L "$accounts_root" || ! -d "$accounts_root" ]]; then
            warn "Cannot safely lock AccountsDB root: $accounts_root is not a real directory"
            release_accounts_store_locks "${held_fds[@]}"
            held_fds=()
            return 1
        fi

        lock_path="$accounts_root/accounts_index_v2.lock"
        if [[ -L "$lock_path" ]]; then
            warn "Cannot safely lock AccountsDB root: $lock_path is a symbolic link"
            release_accounts_store_locks "${held_fds[@]}"
            held_fds=()
            return 1
        fi

        lock_fd=""
        if ! exec {lock_fd}<>"$lock_path"; then
            warn "Cannot open AccountsDB store lock: $lock_path"
            release_accounts_store_locks "${held_fds[@]}"
            held_fds=()
            return 1
        fi
        if ! flock -xn "$lock_fd"; then
            warn "Refusing to clean $accounts_root: its AccountsDB store is in use"
            exec {lock_fd}>&- 2>/dev/null || true
            release_accounts_store_locks "${held_fds[@]}"
            held_fds=()
            return 1
        fi

        # Match the runtime's no-follow/same-inode checks. A replaced lock path
        # must fail closed, otherwise two processes could each hold a different
        # inode and both believe they have exclusive access to the store.
        fd_path="/proc/$BASHPID/fd/$lock_fd"
        if [[ -L "$lock_path" || ! -f "$lock_path" || ! "$lock_path" -ef "$fd_path" ]]; then
            warn "Cannot safely clean $accounts_root: its store lock path changed while locking"
            exec {lock_fd}>&- 2>/dev/null || true
            release_accounts_store_locks "${held_fds[@]}"
            held_fds=()
            return 1
        fi
        held_fds+=("$lock_fd")
    done
}

dir_size() {
    local dir="$1"
    if [[ -d "$dir" ]]; then
        du -sh "$dir" 2>/dev/null | awk '{print $1}'
    else
        echo "0"
    fi
}

# Get disk space info for a directory's mount point
# Returns: "used_bytes total_bytes free_bytes mount_point fstype"
get_mount_disk_space() {
    local dir="$1"
    # Find the mount point for this directory
    local mount_point
    mount_point=$(df "$dir" 2>/dev/null | tail -1 | awk '{print $NF}')

    if [[ -z "$mount_point" ]]; then
        echo "0 0 0 unknown unknown"
        return
    fi

    # Get disk space in 1K blocks and convert to bytes
    local used_kb total_kb free_kb
    read -r total_kb used_kb free_kb < <(df -k "$dir" 2>/dev/null | tail -1 | awk '{print $2, $3, $4}')

    local used_bytes=$((used_kb * 1024))
    local total_bytes=$((total_kb * 1024))
    local free_bytes=$((free_kb * 1024))

    # Get filesystem type
    local fstype
    fstype=$(df -T "$dir" 2>/dev/null | tail -1 | awk '{print $2}')
    [[ -z "$fstype" ]] && fstype="unknown"

    echo "$used_bytes $total_bytes $free_bytes $mount_point $fstype"
}

# Format bytes to human-readable (e.g., "1.5 TB")
format_bytes() {
    local bytes="$1"

    if [[ $bytes -ge $((1024*1024*1024*1024)) ]]; then
        awk "BEGIN {printf \"%.2f TB\", $bytes / (1024*1024*1024*1024)}"
    elif [[ $bytes -ge $((1024*1024*1024)) ]]; then
        awk "BEGIN {printf \"%.2f GB\", $bytes / (1024*1024*1024)}"
    elif [[ $bytes -ge $((1024*1024)) ]]; then
        awk "BEGIN {printf \"%.2f MB\", $bytes / (1024*1024)}"
    elif [[ $bytes -ge 1024 ]]; then
        awk "BEGIN {printf \"%.2f KB\", $bytes / 1024}"
    else
        echo "${bytes} B"
    fi
}

# Show disk space before/after deletion
show_disk_space_summary() {
    local mount_point="$1"
    local before_used="$2"
    local before_free="$3"
    local after_used="$4"
    local after_free="$5"
    local total="$6"
    local fstype="${7:-unknown}"

    local freed=$((before_used - after_used))

    echo ""
    echo "  ┌─────────────────────────────────────────────────────────────────────────┐"
    echo "  │ DISK SPACE SUMMARY                                                       │"
    echo "  ├─────────────────────────────────────────────────────────────────────────┤"
    printf "  │ Mount point: %-50s (%s) │\n" "$mount_point" "$fstype"
    echo "  │                                                                          │"
    printf "  │   Before:  %12s used  /  %12s free                      │\n" "$(format_bytes $before_used)" "$(format_bytes $before_free)"
    printf "  │   After:   %12s used  /  %12s free                      │\n" "$(format_bytes $after_used)" "$(format_bytes $after_free)"
    echo "  │                                                                          │"
    printf "  │   ${GREEN}Freed:     %12s${NC}                                              │\n" "$(format_bytes $freed)"
    echo "  └─────────────────────────────────────────────────────────────────────────┘"
}

# Generic clean function for a specific subdirectory type
clean_subdir() {
    local subdir_name="$1"  # e.g., "accountsdb", "snapshots", "blockstore"
    local description="$2"  # e.g., "AccountsDB", "Snapshots", "Blockstore"

    info "DELETING ${description^^}"
    echo ""

    # Find Mithril directories
    mapfile -t mithril_dirs < <(find_mithril_dirs)

    if [[ ${#mithril_dirs[@]} -eq 0 ]]; then
        warn "No Mithril data directories found"
        echo ""
        echo "  Checked: /mnt/mithril-accounts, /mnt/mithril-ledger"
        return 1
    fi

    # Find all matching subdirectories
    local paths_to_clean=()
    local primary_dir=""

    echo "  Found $description directories:"
    echo ""

    for mithril_dir in "${mithril_dirs[@]}"; do
        if [[ -d "$mithril_dir/$subdir_name" ]]; then
            # SAFETY: Never delete anything on the OS disk
            if path_on_root_disk "$mithril_dir/$subdir_name"; then
                warn "SKIPPING $mithril_dir/$subdir_name - on OS disk (safety protection)"
                continue
            fi
            local size
            size=$(dir_size "$mithril_dir/$subdir_name")
            paths_to_clean+=("$mithril_dir/$subdir_name")
            [[ -z "$primary_dir" ]] && primary_dir="$mithril_dir"
            echo "    $mithril_dir/$subdir_name  ($size)"
        fi
    done

    if [[ ${#paths_to_clean[@]} -eq 0 ]]; then
        warn "No $description directories found"
        return 1
    fi

    echo ""
    echo -e "  ${RED}These directories will be CLEANED (contents erased):${NC}"
    for path in "${paths_to_clean[@]}"; do
        echo "    $path"
    done
    echo ""

    confirm_destructive "CLEAN $description"

    # Capture disk space BEFORE deletion
    local before_used before_total before_free mount_point fstype
    read -r before_used before_total before_free mount_point fstype < <(get_mount_disk_space "$primary_dir")

    # Execute deletion
    info "Deleting $description..."

    for path in "${paths_to_clean[@]}"; do
        echo "  Removing $path..."
        rm -rf "$path"
        # Recreate empty directory
        mkdir -p "$path"
        success "Cleaned: $path"
    done

    # Sync to ensure filesystem updates are reflected
    sync

    # Capture disk space AFTER deletion
    local after_used after_total after_free
    read -r after_used after_total after_free _ _ < <(get_mount_disk_space "$primary_dir")

    echo ""
    success "$description has been deleted"

    # Fix ownership so non-root user can run Mithril
    fix_mithril_ownership

    # Show before/after disk space summary
    show_disk_space_summary "$mount_point" "$before_used" "$before_free" "$after_used" "$after_free" "$before_total" "$fstype"
}

clean_accounts() (
    info "CLEANING Accounts data"
    echo ""

    # Accounts artifacts (stored directly in mount point, not a subdirectory)
    local artifacts=("accounts" "accounts_index.root" "accounts_delta_v2.journal" "accounts_delta_v2.journal.rewrite.partial" "accounts_delta_shards" "accounts_index.stmh" "accounts_index.stmh.partial" "accounts_index.manifest" "accounts_index.manifest.tmp" "accounts_delta.journal" "accounts_delta.journal.rewrite.partial" "accounts_index_runs" "mithril_db" "mithril_db_build" "mithril_db_log_shards" "bankhash_db" "largest_file_id" "bootstrap_high_file_id" "bank_hash" "stake_pubkeys.idx" "manifest" "mithril_state.json")

    # Find Mithril directories
    mapfile -t mithril_dirs < <(find_mithril_dirs)

    if [[ ${#mithril_dirs[@]} -eq 0 ]]; then
        warn "No Mithril data directories found"
        echo ""
        echo "  Checked: /mnt/mithril-accounts, /mnt/mithril-ledger"
        return 1
    fi

    # Hold all possible AccountsDB roots before inspecting any account
    # artifact. The descriptors remain live through the complete deletion.
    local account_store_lock_fds=()
    if ! acquire_accounts_store_locks account_store_lock_fds "${mithril_dirs[@]}"; then
        return 1
    fi

    # Find all matching artifacts
    local paths_to_clean=()
    local primary_dir=""

    echo "  Found accounts artifacts:"
    echo ""

    for mithril_dir in "${mithril_dirs[@]}"; do
        for artifact in "${artifacts[@]}"; do
            if [[ -e "$mithril_dir/$artifact" ]]; then
                # SAFETY: Never delete anything on the OS disk
                if path_on_root_disk "$mithril_dir/$artifact"; then
                    warn "SKIPPING $mithril_dir/$artifact - on OS disk (safety protection)"
                    continue
                fi
                local size
                size=$(dir_size "$mithril_dir/$artifact")
                paths_to_clean+=("$mithril_dir/$artifact")
                [[ -z "$primary_dir" ]] && primary_dir="$mithril_dir"
                echo "    $mithril_dir/$artifact  ($size)"
            fi
        done
        while IFS= read -r -d '' parts_dir; do
            if path_on_root_disk "$parts_dir"; then
                warn "SKIPPING $parts_dir - on OS disk (safety protection)"
                continue
            fi
            local size
            size=$(dir_size "$parts_dir")
            paths_to_clean+=("$parts_dir")
            [[ -z "$primary_dir" ]] && primary_dir="$mithril_dir"
            echo "    $parts_dir  ($size)"
        done < <(find "$mithril_dir" -maxdepth 1 -type d -name 'streamhash-parts-*' -print0 2>/dev/null)
        while IFS= read -r -d '' checkpoint_artifact; do
            if path_on_root_disk "$checkpoint_artifact"; then
                warn "SKIPPING $checkpoint_artifact - on OS disk (safety protection)"
                continue
            fi
            local size
            size=$(dir_size "$checkpoint_artifact")
            paths_to_clean+=("$checkpoint_artifact")
            [[ -z "$primary_dir" ]] && primary_dir="$mithril_dir"
            echo "    $checkpoint_artifact  ($size)"
        done < <(find "$mithril_dir" -maxdepth 1 \( -type f -name 'accounts_delta_checkpoint.*' -o -type d -name '.delta-checkpoint-build-*' \) -print0 2>/dev/null)
        while IFS= read -r -d '' v2_artifact; do
            local v2_name
            v2_name=$(basename "$v2_artifact")
            if [[ ! "$v2_name" =~ ^accounts-index-v2-g[0-9]{20}-[0-9a-f]{32}$ &&
                  ! "$v2_name" =~ ^accounts_index\.root\.tmp-[A-Za-z0-9]+$ ]]; then
                continue
            fi
            if path_on_root_disk "$v2_artifact"; then
                warn "SKIPPING $v2_artifact - on OS disk (safety protection)"
                continue
            fi
            local size
            size=$(dir_size "$v2_artifact")
            paths_to_clean+=("$v2_artifact")
            [[ -z "$primary_dir" ]] && primary_dir="$mithril_dir"
            echo "    $v2_artifact  ($size)"
        done < <(find "$mithril_dir" -maxdepth 1 \( -type d -name 'accounts-index-v2-g*' -o -type f -name 'accounts_index.root.tmp-*' \) -print0 2>/dev/null)
    done

    if [[ ${#paths_to_clean[@]} -eq 0 ]]; then
        echo "  No accounts artifacts found to delete."
        release_accounts_store_locks "${account_store_lock_fds[@]}"
        return 0
    fi

    echo ""

    if ! yesno "  Delete these accounts files?" "n"; then
        die "Aborted"
    fi

    # Capture disk space BEFORE deletion
    local before_used before_total before_free mount_point fstype
    read -r before_used before_total before_free mount_point fstype < <(get_mount_disk_space "$primary_dir")

    echo ""

    for path in "${paths_to_clean[@]}"; do
        echo "  Deleting $path..."
        rm -rf "$path"
    done

    # Sync to ensure filesystem updates are reflected
    sync

    # Capture disk space AFTER deletion
    local after_used after_total after_free
    read -r after_used after_total after_free _ _ < <(get_mount_disk_space "$primary_dir")

    echo ""
    success "Accounts data has been cleaned"

    # Show before/after disk space summary
    show_disk_space_summary "$mount_point" "$before_used" "$before_free" "$after_used" "$after_free" "$before_total" "$fstype"

    echo ""
    echo "  On next run, Mithril will rebuild accounts from a fresh snapshot."
    release_accounts_store_locks "${account_store_lock_fds[@]}"
)

clean_snapshots() {
    info "CLEANING Snapshots"
    echo ""

    # Find Mithril directories
    mapfile -t mithril_dirs < <(find_mithril_dirs)

    if [[ ${#mithril_dirs[@]} -eq 0 ]]; then
        warn "No Mithril data directories found"
        echo ""
        echo "  Checked: /mnt/mithril-accounts, /mnt/mithril-ledger"
        return 1
    fi

    # Find all snapshot files and directories
    local paths_to_clean=()
    local primary_dir=""

    echo "  Found snapshot files/directories:"
    echo ""

    for mithril_dir in "${mithril_dirs[@]}"; do
        # SAFETY: Never delete anything on the OS disk
        if path_on_root_disk "$mithril_dir"; then
            warn "SKIPPING $mithril_dir - on OS disk (safety protection)"
            continue
        fi

        # Check for snapshots subdirectory
        if [[ -d "$mithril_dir/snapshots" ]]; then
            local size
            size=$(dir_size "$mithril_dir/snapshots")
            paths_to_clean+=("$mithril_dir/snapshots")
            [[ -z "$primary_dir" ]] && primary_dir="$mithril_dir"
            echo "    $mithril_dir/snapshots/  ($size)"
        fi

        # Also check for snapshot files at root level (snapshot-*.tar.*, incremental-snapshot-*.tar.*)
        for pattern in "snapshot-*.tar.*" "incremental-snapshot-*.tar.*"; do
            while IFS= read -r -d '' snapshot_file; do
                local size
                size=$(du -sh "$snapshot_file" 2>/dev/null | awk '{print $1}')
                paths_to_clean+=("$snapshot_file")
                [[ -z "$primary_dir" ]] && primary_dir="$mithril_dir"
                echo "    $snapshot_file  ($size)"
            done < <(find "$mithril_dir" -maxdepth 1 -name "$pattern" -print0 2>/dev/null)
        done
    done

    if [[ ${#paths_to_clean[@]} -eq 0 ]]; then
        warn "No snapshot files or directories found"
        return 1
    fi

    echo ""
    echo -e "  ${RED}These files/directories will be CLEANED (contents erased):${NC}"
    for path in "${paths_to_clean[@]}"; do
        echo "    $path"
    done
    echo ""

    confirm_destructive "CLEAN SNAPSHOTS"

    # Capture disk space BEFORE deletion
    local before_used before_total before_free mount_point fstype
    read -r before_used before_total before_free mount_point fstype < <(get_mount_disk_space "$primary_dir")

    # Execute deletion
    info "Deleting snapshots..."

    for path in "${paths_to_clean[@]}"; do
        echo "  Removing $path..."
        rm -rf "$path"
        # Recreate directory if it was a directory (not a file)
        if [[ "$path" == */snapshots ]]; then
            mkdir -p "$path"
        fi
        success "Cleaned: $path"
    done

    # Sync to ensure filesystem updates are reflected
    sync

    # Capture disk space AFTER deletion
    local after_used after_total after_free
    read -r after_used after_total after_free _ _ < <(get_mount_disk_space "$primary_dir")

    echo ""
    success "Snapshots have been cleaned"

    # Fix ownership so non-root user can run Mithril
    fix_mithril_ownership

    # Show before/after disk space summary
    show_disk_space_summary "$mount_point" "$before_used" "$before_free" "$after_used" "$after_free" "$before_total" "$fstype"

    echo ""
    echo "  On next run, Mithril will download fresh snapshots from the network."
}

clean_blockstore() {
    clean_subdir "blockstore" "Blockstore"
    echo ""
    echo "  On next run, Mithril will rebuild blockstore from verified blocks."
}

clean_ledger() {
    info "CLEANING Ledger data (Snapshots + Blockstore)"
    echo ""
    echo "  This will delete snapshots, blockstore, and any snapshot files."
    echo "  (AccountsDB will be preserved)"
    echo ""

    # Find Mithril directories
    mapfile -t mithril_dirs < <(find_mithril_dirs)

    if [[ ${#mithril_dirs[@]} -eq 0 ]]; then
        warn "No Mithril data directories found"
        echo ""
        echo "  Checked: /mnt/mithril-accounts, /mnt/mithril-ledger"
        return 1
    fi

    # Find all ledger subdirectories and snapshot files
    local paths_to_clean=()
    local primary_dir=""

    echo "  Found ledger files/directories:"
    echo ""

    for mithril_dir in "${mithril_dirs[@]}"; do
        # SAFETY: Never delete anything on the OS disk
        if path_on_root_disk "$mithril_dir"; then
            warn "SKIPPING $mithril_dir - on OS disk (safety protection)"
            continue
        fi

        # Check for snapshots and blockstore subdirectories
        for subdir in snapshots blockstore; do
            if [[ -d "$mithril_dir/$subdir" ]]; then
                local size
                size=$(dir_size "$mithril_dir/$subdir")
                paths_to_clean+=("$mithril_dir/$subdir")
                [[ -z "$primary_dir" ]] && primary_dir="$mithril_dir"
                echo "    $mithril_dir/$subdir/  ($size)"
            fi
        done

        # Also check for snapshot files at root level (snapshot-*.tar.*, incremental-snapshot-*.tar.*)
        for pattern in "snapshot-*.tar.*" "incremental-snapshot-*.tar.*"; do
            while IFS= read -r -d '' snapshot_file; do
                local size
                size=$(du -sh "$snapshot_file" 2>/dev/null | awk '{print $1}')
                paths_to_clean+=("$snapshot_file")
                [[ -z "$primary_dir" ]] && primary_dir="$mithril_dir"
                echo "    $snapshot_file  ($size)"
            done < <(find "$mithril_dir" -maxdepth 1 -name "$pattern" -print0 2>/dev/null)
        done
    done

    if [[ ${#paths_to_clean[@]} -eq 0 ]]; then
        warn "No ledger files/directories (snapshots/blockstore) found"
        return 1
    fi

    echo ""
    echo -e "  ${RED}These files/directories will be CLEANED (contents erased):${NC}"
    for path in "${paths_to_clean[@]}"; do
        echo "    $path"
    done
    echo ""

    confirm_destructive "CLEAN LEDGER"

    # Capture disk space BEFORE deletion
    local before_used before_total before_free mount_point fstype
    read -r before_used before_total before_free mount_point fstype < <(get_mount_disk_space "$primary_dir")

    # Execute deletion
    info "Deleting ledger data..."

    for path in "${paths_to_clean[@]}"; do
        echo "  Removing $path..."
        rm -rf "$path"
        # Recreate directory if it was a directory (not a file)
        if [[ "$path" == */snapshots || "$path" == */blockstore ]]; then
            mkdir -p "$path"
        fi
        success "Cleaned: $path"
    done

    # Sync to ensure filesystem updates are reflected
    sync

    # Capture disk space AFTER deletion
    local after_used after_total after_free
    read -r after_used after_total after_free _ _ < <(get_mount_disk_space "$primary_dir")

    echo ""
    success "Ledger data has been cleaned"

    # Fix ownership so non-root user can run Mithril
    fix_mithril_ownership

    # Show before/after disk space summary
    show_disk_space_summary "$mount_point" "$before_used" "$before_free" "$after_used" "$after_free" "$before_total" "$fstype"

    echo ""
    echo "  On next run, Mithril will download fresh snapshots and rebuild blockstore."
}

clean_all() (
    info "CLEANING ALL MITHRIL DATA"
    echo ""
    echo "  This will delete Accounts, Snapshots, AND Blockstore."
    echo ""

    # Accounts artifacts (stored directly in mount point)
    local accounts_artifacts=("accounts" "accounts_index.root" "accounts_delta_v2.journal" "accounts_delta_v2.journal.rewrite.partial" "accounts_delta_shards" "accounts_index.stmh" "accounts_index.stmh.partial" "accounts_index.manifest" "accounts_index.manifest.tmp" "accounts_delta.journal" "accounts_delta.journal.rewrite.partial" "accounts_index_runs" "mithril_db" "mithril_db_build" "mithril_db_log_shards" "bankhash_db" "largest_file_id" "bootstrap_high_file_id" "bank_hash" "stake_pubkeys.idx" "manifest" "mithril_state.json")

    # Find Mithril directories
    mapfile -t mithril_dirs < <(find_mithril_dirs)

    if [[ ${#mithril_dirs[@]} -eq 0 ]]; then
        warn "No Mithril data directories found"
        return 1
    fi

    # Lock before enumerating either AccountsDB or colocated ledger artifacts.
    # The persistent lockfiles themselves are intentionally never deletion
    # targets and remain held until every selected path has been cleaned.
    local account_store_lock_fds=()
    if ! acquire_accounts_store_locks account_store_lock_fds "${mithril_dirs[@]}"; then
        return 1
    fi

    # Find all data to clean
    local paths_to_clean=()
    local primary_dir=""

    echo "  Found Mithril directories:"
    echo ""

    for mithril_dir in "${mithril_dirs[@]}"; do
        # SAFETY: Never delete anything on the OS disk
        if path_on_root_disk "$mithril_dir"; then
            warn "SKIPPING $mithril_dir - on OS disk (safety protection)"
            continue
        fi
        echo "    $mithril_dir"
        [[ -z "$primary_dir" ]] && primary_dir="$mithril_dir"

        # Check for accounts artifacts at root level
        for artifact in "${accounts_artifacts[@]}"; do
            if [[ -e "$mithril_dir/$artifact" ]]; then
                local size
                size=$(dir_size "$mithril_dir/$artifact")
                paths_to_clean+=("$mithril_dir/$artifact")
                echo "      $artifact  ($size)"
            fi
        done
        while IFS= read -r -d '' parts_dir; do
            local size
            size=$(dir_size "$parts_dir")
            paths_to_clean+=("$parts_dir")
            echo "      $(basename "$parts_dir")/  ($size)"
        done < <(find "$mithril_dir" -maxdepth 1 -type d -name 'streamhash-parts-*' -print0 2>/dev/null)
        while IFS= read -r -d '' checkpoint_artifact; do
            local size
            size=$(dir_size "$checkpoint_artifact")
            paths_to_clean+=("$checkpoint_artifact")
            echo "      $(basename "$checkpoint_artifact")  ($size)"
        done < <(find "$mithril_dir" -maxdepth 1 \( -type f -name 'accounts_delta_checkpoint.*' -o -type d -name '.delta-checkpoint-build-*' \) -print0 2>/dev/null)
        while IFS= read -r -d '' v2_artifact; do
            local v2_name
            v2_name=$(basename "$v2_artifact")
            if [[ ! "$v2_name" =~ ^accounts-index-v2-g[0-9]{20}-[0-9a-f]{32}$ &&
                  ! "$v2_name" =~ ^accounts_index\.root\.tmp-[A-Za-z0-9]+$ ]]; then
                continue
            fi
            local size
            size=$(dir_size "$v2_artifact")
            paths_to_clean+=("$v2_artifact")
            echo "      $v2_name  ($size)"
        done < <(find "$mithril_dir" -maxdepth 1 \( -type d -name 'accounts-index-v2-g*' -o -type f -name 'accounts_index.root.tmp-*' \) -print0 2>/dev/null)

        # Check for subdirectories (snapshots, blockstore)
        for subdir in snapshots blockstore; do
            if [[ -d "$mithril_dir/$subdir" ]]; then
                local size
                size=$(dir_size "$mithril_dir/$subdir")
                paths_to_clean+=("$mithril_dir/$subdir")
                echo "      $subdir/  ($size)"
            fi
        done

        # Also check for snapshot files at root level (snapshot-*.tar.*, incremental-snapshot-*.tar.*)
        for pattern in "snapshot-*.tar.*" "incremental-snapshot-*.tar.*"; do
            while IFS= read -r -d '' snapshot_file; do
                local size
                size=$(du -sh "$snapshot_file" 2>/dev/null | awk '{print $1}')
                paths_to_clean+=("$snapshot_file")
                echo "      $(basename "$snapshot_file")  ($size)"
            done < <(find "$mithril_dir" -maxdepth 1 -name "$pattern" -print0 2>/dev/null)
        done
    done

    if [[ ${#paths_to_clean[@]} -eq 0 ]]; then
        warn "No Mithril data directories found"
        return 1
    fi

    echo ""
    echo -e "  ${RED}ALL of the above directories will be CLEANED (contents erased)${NC}"
    echo ""

    confirm_destructive "CLEAN ALL MITHRIL DATA"

    # Collect all distinct mount points from paths being cleaned
    declare -A mount_points_map
    for path in "${paths_to_clean[@]}"; do
        local mp
        mp=$(df "$path" 2>/dev/null | tail -1 | awk '{print $NF}')
        if [[ -n "$mp" ]]; then
            mount_points_map["$mp"]=1
        fi
    done
    local unique_mount_points=("${!mount_points_map[@]}")

    # Capture disk space BEFORE deletion for each mount point
    declare -A before_used_map before_total_map before_free_map fstype_map
    for mp in "${unique_mount_points[@]}"; do
        local used total free mount fstype
        read -r used total free mount fstype < <(get_mount_disk_space "$mp")
        before_used_map["$mp"]="$used"
        before_total_map["$mp"]="$total"
        before_free_map["$mp"]="$free"
        fstype_map["$mp"]="$fstype"
    done

    # Execute deletion
    info "Deleting all Mithril data..."

    for path in "${paths_to_clean[@]}"; do
        echo "  Removing $path..."
        # Check if it was a directory (before deletion) to know whether to recreate
        local was_dir=false
        [[ -d "$path" ]] && was_dir=true
        rm -rf "$path"
        # Only recreate if it was a directory (not a file like snapshot-*.tar.zst)
        if $was_dir; then
            mkdir -p "$path"
        fi
        success "Cleaned: $path"
    done

    # Sync to ensure filesystem updates are reflected
    sync

    echo ""
    success "All Mithril data has been cleaned"

    # Fix ownership so non-root user can run Mithril
    fix_mithril_ownership

    # Show before/after disk space summary for each mount point
    for mp in "${unique_mount_points[@]}"; do
        local after_used after_total after_free
        read -r after_used after_total after_free _ _ < <(get_mount_disk_space "$mp")
        show_disk_space_summary "$mp" "${before_used_map[$mp]}" "${before_free_map[$mp]}" "$after_used" "$after_free" "${before_total_map[$mp]}" "${fstype_map[$mp]}"
    done

    echo ""
    echo "  On next run, Mithril will start completely fresh."
    release_accounts_store_locks "${account_store_lock_fds[@]}"
)

# ------------------------------------------------------------------------------
# Reset (Nuclear Option)
# ------------------------------------------------------------------------------

reset_all() {
    local wipe_disks="${1:-false}"

    info "MITHRIL FULL RESET"
    echo ""
    echo "  This will:"
    echo "    1. Unmount all Mithril mount points"
    echo "    2. Remove Mithril entries from /etc/fstab"
    echo "    3. Delete all Mithril directories"
    if [[ "$wipe_disks" == "true" ]]; then
        echo -e "    4. ${RED}WIPE Mithril partitions (wipefs)${NC}"
    fi
    echo ""

    # Find all mithril mount points
    local mithril_mounts=()
    local mithril_dirs=(
        "/mnt/mithril-accounts"
        "/mnt/mithril-ledger"
        "/mnt/mithril-logs"
    )

    echo "  Checking mount points..."
    for dir in "${mithril_dirs[@]}"; do
        if findmnt "$dir" >/dev/null 2>&1; then
            local device
            device=$(findmnt -n -o SOURCE "$dir" 2>/dev/null || echo "unknown")
            echo -e "    ${YELLOW}MOUNTED${NC}: $dir -> $device"
            mithril_mounts+=("$dir")
        elif [[ -d "$dir" ]]; then
            echo -e "    ${GREEN}EXISTS${NC}:  $dir (not mounted)"
        else
            echo "    -       $dir (not found)"
        fi
    done

    # Find partitions that would be wiped
    local partitions_to_wipe=()
    if [[ "$wipe_disks" == "true" ]]; then
        echo ""
        echo "  Partitions that will be wiped:"
        for dir in "${mithril_mounts[@]}"; do
            local device
            device=$(findmnt -n -o SOURCE "$dir" 2>/dev/null)
            if [[ -n "$device" && "$device" != "/" ]]; then
                # Safety: never wipe root partition or OS disk partitions
                local parent_disk
                parent_disk=$(lsblk -no PKNAME "$device" 2>/dev/null | head -1)
                local root_disk
                root_disk=$(get_root_disk)
                if [[ "$parent_disk" == "$root_disk" ]]; then
                    echo -e "    ${RED}SKIPPING${NC}: $device (on OS disk - protected)"
                else
                    echo -e "    ${RED}WILL WIPE${NC}: $device"
                    partitions_to_wipe+=("$device")
                fi
            fi
        done
    fi

    echo ""

    # Require confirmation
    if [[ "$wipe_disks" == "true" && ${#partitions_to_wipe[@]} -gt 0 ]]; then
        echo -e "  ${RED}!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!${NC}"
        echo -e "  ${RED}DESTRUCTIVE ACTION - PARTITIONS WILL BE PERMANENTLY WIPED${NC}"
        echo -e "  ${RED}!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!${NC}"
        echo ""
        echo "  To proceed, type exactly:"
        echo "    RESET AND WIPE"
        echo ""
        read -r -p "> " confirm
        if [[ "$confirm" != "RESET AND WIPE" ]]; then
            echo "  Aborted."
            exit 1
        fi
    else
        confirm_destructive "RESET MITHRIL"
    fi

    # Step 1: Unmount all mithril mount points
    info "Unmounting Mithril directories..."
    for mount_point in "${mithril_mounts[@]}"; do
        echo "  Unmounting $mount_point..."
        if ! umount "$mount_point" 2>/dev/null; then
            # Try lazy unmount if normal unmount fails
            warn "Normal unmount failed, trying lazy unmount..."
            if ! umount -l "$mount_point" 2>/dev/null; then
                warn "Could not unmount $mount_point - is mithril still running?"
                echo "  Try: sudo lsof +D $mount_point"
                exit 1
            fi
        fi
        success "Unmounted $mount_point"
    done

    # Step 2: Remove fstab entries
    info "Removing Mithril entries from /etc/fstab..."
    if grep -q mithril /etc/fstab 2>/dev/null; then
        # Backup fstab
        cp /etc/fstab /etc/fstab.bak.$(date +%Y%m%d%H%M%S)
        # Remove mithril lines
        sed -i '/mithril/d' /etc/fstab
        success "Removed Mithril entries from fstab (backup created)"
        systemctl daemon-reload
    else
        echo "  No Mithril entries found in fstab"
    fi

    # Step 3: Delete directories
    info "Removing Mithril directories..."
    for dir in "${mithril_dirs[@]}"; do
        if [[ -d "$dir" ]]; then
            echo "  Removing $dir..."
            rm -rf "$dir"
            success "Deleted $dir"
        fi
    done

    # Step 4: Wipe partitions (if requested)
    if [[ "$wipe_disks" == "true" && ${#partitions_to_wipe[@]} -gt 0 ]]; then
        info "Wiping Mithril partitions..."
        local wipe_had_busy=false
        for partition in "${partitions_to_wipe[@]}"; do
            echo "  Wiping $partition..."
            local wipe_output
            if wipe_output=$(wipefs -a "$partition" 2>&1); then
                success "Wiped $partition"
            elif [[ "$wipe_output" == *"busy"* ]]; then
                warn "Could not wipe $partition - device is busy"
                wipe_had_busy=true
            else
                warn "Could not wipe $partition: $wipe_output"
            fi
        done
        if [[ "$wipe_had_busy" == "true" ]]; then
            echo ""
            warn "Some partitions could not be wiped because they are busy."
            echo "      This usually happens when the kernel still holds a reference."
            echo ""
            echo "      Solution: Reboot your machine and run again:"
            echo "        sudo ./scripts/disk-setup.sh --reset --wipe-disks"
            echo ""
        fi
    fi

    echo ""
    success "Mithril has been completely reset"
    echo ""
    echo "  Next steps:"
    echo "    1. Run: sudo ./scripts/disk-setup.sh --setup"
    echo "    2. Update config.toml with new paths"
    echo "    3. Run: ./mithril run"
    echo ""
}

# ------------------------------------------------------------------------------
# Disk Summary
# ------------------------------------------------------------------------------

show_disk_summary() {
    echo ""
    echo "================================================================================"
    echo "                    DISK SUMMARY"
    echo "================================================================================"

    # ============================================================================
    # PART 1: PER-DRIVE DETAILS
    # ============================================================================

    # Iterate through each physical disk
    lsblk -dn -o NAME,SIZE,TYPE,MODEL 2>/dev/null | while read -r name size dtype model; do
        # Only show disk types (not partitions, loops, etc.)
        if [[ "$dtype" == "disk" ]]; then
            local device="/dev/$name"
            local drive_type="HDD"

            # Detect drive type
            if [[ "$name" == nvme* ]]; then
                drive_type="NVMe"
            elif [[ -f "/sys/block/$name/queue/rotational" ]]; then
                local rotational
                rotational=$(cat "/sys/block/$name/queue/rotational" 2>/dev/null || echo "1")
                if [[ "$rotational" == "0" ]]; then
                    drive_type="SSD"
                fi
            fi

            # Clean up model (may be empty for some drives)
            [[ -z "$model" ]] && model="(unknown)"

            echo ""
            echo "--------------------------------------------------------------------------------"
            echo -e "  ${CYAN}$device${NC}  ($drive_type)"
            echo "--------------------------------------------------------------------------------"
            echo "  Model:      $model"
            echo "  Size:       $size"

            # Get UUID (from first partition or the device itself)
            local uuid="-"
            # Try the device itself first
            uuid=$(blkid -s UUID -o value "$device" 2>/dev/null || true)
            # If no UUID, try first partition
            if [[ -z "$uuid" ]]; then
                local first_part
                first_part=$(lsblk -ln -o NAME "$device" 2>/dev/null | head -2 | tail -1)
                if [[ -n "$first_part" && "$first_part" != "$name" ]]; then
                    uuid=$(blkid -s UUID -o value "/dev/$first_part" 2>/dev/null || true)
                fi
            fi
            [[ -z "$uuid" ]] && uuid="-"
            echo "  UUID:       $uuid"

            # I/O Scheduler and read-ahead
            local scheduler_file="/sys/block/$name/queue/scheduler"
            local readahead_file="/sys/block/$name/queue/read_ahead_kb"
            if [[ -f "$scheduler_file" ]]; then
                local scheduler
                scheduler=$(cat "$scheduler_file" 2>/dev/null | grep -oP '\[\w+\]' | tr -d '[]')
                local readahead="?"
                [[ -f "$readahead_file" ]] && readahead=$(cat "$readahead_file" 2>/dev/null)
                echo "  Scheduler:  $scheduler"
                echo "  Read-ahead: ${readahead}KB"

                # Recommendation for NVMe
                if [[ "$name" == nvme* && "$scheduler" != "none" && "$scheduler" != "mq-deadline" ]]; then
                    echo -e "              ${YELLOW}(Tip: 'none' or 'mq-deadline' recommended for NVMe)${NC}"
                fi
            fi

            # Check TRIM support for SSDs/NVMe
            if [[ "$drive_type" == "NVMe" || "$drive_type" == "SSD" ]]; then
                local discard_gran="/sys/block/$name/queue/discard_granularity"
                if [[ -f "$discard_gran" ]]; then
                    local gran
                    gran=$(cat "$discard_gran" 2>/dev/null || echo "0")
                    if [[ "$gran" != "0" ]]; then
                        echo -e "  TRIM:       ${GREEN}supported${NC}"
                    else
                        echo -e "  TRIM:       ${YELLOW}not supported${NC}"
                    fi
                fi
            fi

            # Show partitions/mount points on this disk
            echo ""
            echo "  Partitions:"
            local has_partitions=false
            lsblk -ln -o NAME,SIZE,FSTYPE,MOUNTPOINT "$device" 2>/dev/null | tail -n +2 | while read -r pname psize pfstype pmount; do
                has_partitions=true
                local pdevice="/dev/$pname"
                [[ -z "$pfstype" ]] && pfstype="-"
                [[ -z "$pmount" ]] && pmount="(not mounted)"

                # Get mount options if mounted
                local mount_opts=""
                if [[ "$pmount" != "(not mounted)" ]]; then
                    local opts
                    opts=$(grep -E "^$pdevice " /proc/mounts 2>/dev/null | awk '{print $4}' || true)
                    [[ "$opts" == *noatime* ]] && mount_opts+="noatime "
                    [[ "$opts" == *discard* ]] && mount_opts+="discard "
                    mount_opts="${mount_opts% }"
                fi

                printf "    %-15s  %8s  %-6s  %-20s" "$pdevice" "$psize" "$pfstype" "$pmount"
                [[ -n "$mount_opts" ]] && printf "  [%s]" "$mount_opts"
                echo ""

                # Usage warning for mounted partitions
                if [[ "$pmount" != "(not mounted)" ]]; then
                    local pct
                    pct=$(df "$pmount" 2>/dev/null | tail -1 | awk '{print $5}' | tr -d '%')
                    if [[ -n "$pct" && "$pct" -ge 80 ]]; then
                        echo -e "                  ${YELLOW}⚠ ${pct}% full - SSD performance degrades above 80%${NC}"
                    fi
                fi
            done
        fi
    done

    echo ""

    # ============================================================================
    # PART 2: SYSTEM-WIDE TRIM STATUS
    # ============================================================================
    echo "--------------------------------------------------------------------------------"
    echo "  TRIM Timer Status"
    echo "--------------------------------------------------------------------------------"
    echo ""
    if systemctl is-active --quiet fstrim.timer 2>/dev/null; then
        success "  fstrim.timer is ACTIVE (weekly TRIM enabled)"
        local next_run
        next_run=$(systemctl show fstrim.timer --property=NextElapseUSecRealtime 2>/dev/null | cut -d= -f2 || true)
        if [[ -n "$next_run" && "$next_run" != "n/a" ]]; then
            echo "    Next scheduled run: $next_run"
        fi
    elif systemctl is-enabled --quiet fstrim.timer 2>/dev/null; then
        warn "  fstrim.timer is ENABLED but not running"
        echo "    Start with: sudo systemctl start fstrim.timer"
    else
        warn "  fstrim.timer is NOT enabled"
        echo "    Enable with: sudo systemctl enable --now fstrim.timer"
        echo "    (Weekly TRIM improves SSD performance and longevity)"
    fi

    echo ""
    echo ""
    echo "================================================================================"
    echo "                    MITHRIL DATA DIRECTORIES"
    echo "================================================================================"
    echo ""

    # ============================================================================
    # PART 3: MITHRIL-SPECIFIC DIRECTORIES
    # ============================================================================

    echo "--- Mithril Data Usage ---"
    echo ""

    # Find Mithril directories
    mapfile -t mithril_dirs < <(find_mithril_dirs)

    if [[ ${#mithril_dirs[@]} -eq 0 ]]; then
        echo "  No Mithril directories found."
        echo ""
        echo "  Mithril typically stores data in:"
        echo "    /mnt/mithril-accounts  - AccountsDB and index"
        echo "    /mnt/mithril-ledger    - Snapshots and blockstore"
        echo ""
        echo "  Run 'sudo ./scripts/disk-setup.sh --setup' to configure storage."
    else
        # Accounts artifacts
        local accounts_artifacts=("accounts" "accounts_index.root" "accounts_delta_v2.journal" "accounts_delta_v2.journal.rewrite.partial" "accounts_delta_shards" "accounts_index.stmh" "accounts_index.stmh.partial" "accounts_index.manifest" "accounts_index.manifest.tmp" "accounts_delta.journal" "accounts_delta.journal.rewrite.partial" "accounts_index_runs" "mithril_db" "mithril_db_build" "mithril_db_log_shards" "bankhash_db" "largest_file_id" "bootstrap_high_file_id" "bank_hash" "stake_pubkeys.idx" "manifest")

        for mithril_dir in "${mithril_dirs[@]}"; do
            # Get mount point info for this directory
            local used_bytes total_bytes free_bytes mount_point fstype
            read -r used_bytes total_bytes free_bytes mount_point fstype < <(get_mount_disk_space "$mithril_dir")

            # Calculate usage percentage
            local usage_pct=0
            [[ $total_bytes -gt 0 ]] && usage_pct=$((used_bytes * 100 / total_bytes))

            echo "  ${CYAN}$mithril_dir${NC}"
            printf "    Mount: %s (%s)  |  Total: %s  |  Free: %s  |  %d%% used\n" \
                "$mount_point" "$fstype" "$(format_bytes $total_bytes)" "$(format_bytes $free_bytes)" "$usage_pct"

            # Over-provisioning recommendation
            if [[ $usage_pct -ge 80 ]]; then
                echo -e "    ${YELLOW}⚠ Consider freeing space - SSD performance degrades above 80% capacity${NC}"
            elif [[ $usage_pct -ge 70 ]]; then
                echo -e "    ${GREEN}✓ Good: ~$((100 - usage_pct))% free for SSD over-provisioning${NC}"
            fi
            echo ""

            # Check for accounts artifacts
            local has_accounts=false
            local accounts_total=0
            for artifact in "${accounts_artifacts[@]}"; do
                if [[ -e "$mithril_dir/$artifact" ]]; then
                    has_accounts=true
                    break
                fi
            done
            if find "$mithril_dir" -maxdepth 1 \( -type f -name 'accounts_delta_checkpoint.*' -o -type d -name '.delta-checkpoint-build-*' \) -print -quit 2>/dev/null | grep -q .; then
                has_accounts=true
            fi
            if find "$mithril_dir" -maxdepth 1 \( -type d -name 'accounts-index-v2-g*' -o -type f -name 'accounts_index.root.tmp-*' \) -print -quit 2>/dev/null | grep -q .; then
                has_accounts=true
            fi

            if $has_accounts; then
                echo "    AccountsDB artifacts:"
                for artifact in "${accounts_artifacts[@]}"; do
                    if [[ -e "$mithril_dir/$artifact" ]]; then
                        local size
                        size=$(dir_size "$mithril_dir/$artifact")
                        printf "      %-30s  %10s\n" "$artifact" "$size"
                    fi
                done
                while IFS= read -r -d '' checkpoint_artifact; do
                    local size
                    size=$(dir_size "$checkpoint_artifact")
                    printf "      %-30s  %10s\n" "$(basename "$checkpoint_artifact")" "$size"
                done < <(find "$mithril_dir" -maxdepth 1 \( -type f -name 'accounts_delta_checkpoint.*' -o -type d -name '.delta-checkpoint-build-*' \) -print0 2>/dev/null)
                while IFS= read -r -d '' v2_artifact; do
                    local v2_name
                    v2_name=$(basename "$v2_artifact")
                    if [[ ! "$v2_name" =~ ^accounts-index-v2-g[0-9]{20}-[0-9a-f]{32}$ &&
                          ! "$v2_name" =~ ^accounts_index\.root\.tmp-[A-Za-z0-9]+$ ]]; then
                        continue
                    fi
                    local size
                    size=$(dir_size "$v2_artifact")
                    printf "      %-30s  %10s\n" "$v2_name" "$size"
                done < <(find "$mithril_dir" -maxdepth 1 \( -type d -name 'accounts-index-v2-g*' -o -type f -name 'accounts_index.root.tmp-*' \) -print0 2>/dev/null)
                echo ""
            fi

            # Check for snapshots
            if [[ -d "$mithril_dir/snapshots" ]]; then
                local size
                size=$(dir_size "$mithril_dir/snapshots")
                echo "    Snapshots:"
                printf "      %-30s  %10s\n" "snapshots/" "$size"
                echo ""
            fi

            # Check for snapshot files at root level
            local found_root_snapshots=false
            for pattern in "snapshot-*.tar.*" "incremental-snapshot-*.tar.*"; do
                while IFS= read -r -d '' snapshot_file; do
                    if ! $found_root_snapshots; then
                        echo "    Snapshot files (root level):"
                        found_root_snapshots=true
                    fi
                    local size
                    size=$(du -sh "$snapshot_file" 2>/dev/null | awk '{print $1}')
                    printf "      %-50s  %10s\n" "$(basename "$snapshot_file")" "$size"
                done < <(find "$mithril_dir" -maxdepth 1 -name "$pattern" -print0 2>/dev/null)
            done
            $found_root_snapshots && echo ""

            # Check for blockstore
            if [[ -d "$mithril_dir/blockstore" ]]; then
                local size
                size=$(dir_size "$mithril_dir/blockstore")
                echo "    Blockstore:"
                printf "      %-30s  %10s\n" "blockstore/" "$size"
                echo ""
            fi
        done
    fi

    # Recommendations (only show if mithril directories exist)
    if [[ ${#mithril_dirs[@]} -gt 0 ]]; then
        echo "--- Recommendations ---"
        echo ""

        # Check noatime on mithril mounts
        local noatime_missing=false
        for mithril_dir in "${mithril_dirs[@]}"; do
            local device
            device=$(df "$mithril_dir" 2>/dev/null | tail -1 | awk '{print $1}')
            local opts
            opts=$(grep -E "^$device " /proc/mounts 2>/dev/null | awk '{print $4}' || true)
            if [[ "$opts" != *noatime* ]]; then
                noatime_missing=true
                warn "  $mithril_dir is missing 'noatime' mount option"
                echo "    Add 'noatime' to fstab entry to reduce unnecessary disk writes"
            fi
        done
        if ! $noatime_missing; then
            success "  All Mithril mounts have 'noatime' enabled"
        fi

        echo ""
    fi

    echo "================================================================================"
}

# ------------------------------------------------------------------------------
# Fix noatime mount option
# ------------------------------------------------------------------------------

fix_noatime() {
    info "FIXING NOATIME MOUNT OPTIONS"
    echo ""

    # Find Mithril directories
    mapfile -t mithril_dirs < <(find_mithril_dirs)

    if [[ ${#mithril_dirs[@]} -eq 0 ]]; then
        warn "No Mithril data directories found"
        return 1
    fi

    local needs_fix=()
    local already_ok=()

    for mithril_dir in "${mithril_dirs[@]}"; do
        local device
        device=$(df "$mithril_dir" 2>/dev/null | tail -1 | awk '{print $1}')
        local opts
        opts=$(grep -E "^$device " /proc/mounts 2>/dev/null | awk '{print $4}' || true)
        if [[ "$opts" != *noatime* ]]; then
            needs_fix+=("$mithril_dir")
        else
            already_ok+=("$mithril_dir")
        fi
    done

    if [[ ${#already_ok[@]} -gt 0 ]]; then
        echo "  Already have noatime:"
        for dir in "${already_ok[@]}"; do
            echo "    $dir"
        done
        echo ""
    fi

    if [[ ${#needs_fix[@]} -eq 0 ]]; then
        success "All Mithril mounts already have noatime enabled"
        return 0
    fi

    echo "  Need noatime added:"
    for dir in "${needs_fix[@]}"; do
        echo "    $dir"
    done
    echo ""

    if ! yesno "  Add noatime to these mounts?" "y"; then
        die "Aborted"
    fi

    # Backup fstab
    local backup="/etc/fstab.bak.$(date +%Y%m%d_%H%M%S)"
    cp /etc/fstab "$backup"
    success "Backed up fstab to $backup"

    for mithril_dir in "${needs_fix[@]}"; do
        local device uuid
        device=$(df "$mithril_dir" 2>/dev/null | tail -1 | awk '{print $1}')
        uuid=$(blkid -s UUID -o value "$device" 2>/dev/null || true)

        if [[ -z "$uuid" ]]; then
            warn "Could not find UUID for $device, skipping"
            continue
        fi

        # Check if there's an fstab entry for this UUID
        if grep -q "$uuid" /etc/fstab 2>/dev/null; then
            # Check if it already has noatime
            if grep "$uuid" /etc/fstab | grep -q noatime; then
                echo "  $mithril_dir: fstab already has noatime (but mount doesn't - remount needed)"
            else
                # Add noatime to existing entry
                # Replace 'defaults' with 'defaults,noatime' or add noatime if other options exist
                if grep "$uuid" /etc/fstab | grep -q "defaults"; then
                    sed -i "/$uuid/s/defaults/defaults,noatime/" /etc/fstab
                else
                    # Add noatime to the options field (4th field)
                    sed -i "/$uuid/s/\([^ \t]*[ \t]*[^ \t]*[ \t]*[^ \t]*[ \t]*[^ \t]*\)/\1,noatime/" /etc/fstab
                fi
                success "Updated fstab entry for $mithril_dir"
            fi
        else
            warn "No fstab entry found for $mithril_dir (UUID: $uuid)"
            echo "    You may need to add one manually"
            continue
        fi

        # Remount with new options
        echo "  Remounting $mithril_dir..."
        if mount -o remount "$mithril_dir" 2>/dev/null; then
            success "Remounted $mithril_dir with noatime"
        else
            warn "Could not remount $mithril_dir - may need reboot"
        fi
    done

    echo ""
    success "noatime fix complete"
    echo ""
    echo "  If any mounts failed to remount, they will take effect after reboot."
}

# ------------------------------------------------------------------------------
# Move Mount Point
# ------------------------------------------------------------------------------

move_mount() {
    info "MOVE MOUNT POINT"
    echo ""
    echo "  This command helps you relocate a Mithril mount to a different path."
    echo "  It will update /etc/fstab and remount the filesystem."
    echo ""

    # Find Mithril-related entries in fstab
    local fstab_entries=()
    local mount_points=()
    local uuids=()

    while IFS= read -r line; do
        # Skip comments and empty lines
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ -z "$line" ]] && continue

        # Check if line contains mithril-related paths
        if [[ "$line" == *mithril* ]] || [[ "$line" == */mnt/accounts* ]] || [[ "$line" == */mnt/ledger* ]]; then
            local mp uuid
            # Extract mount point (2nd field)
            mp=$(echo "$line" | awk '{print $2}')
            # Extract UUID or device (1st field)
            uuid=$(echo "$line" | awk '{print $1}')

            if [[ -n "$mp" ]]; then
                fstab_entries+=("$line")
                mount_points+=("$mp")
                uuids+=("$uuid")
            fi
        fi
    done < /etc/fstab

    if [[ ${#mount_points[@]} -eq 0 ]]; then
        warn "No Mithril-related mount points found in /etc/fstab"
        echo ""
        echo "  Looking for entries containing 'mithril', '/mnt/accounts', or '/mnt/ledger'"
        echo "  You may need to add fstab entries first with --setup"
        return 1
    fi

    echo "  Found ${#mount_points[@]} Mithril-related mount(s) in /etc/fstab:"
    echo ""

    local i=1
    for mp in "${mount_points[@]}"; do
        local device="${uuids[$((i-1))]}"
        # Check if currently mounted
        local status="not mounted"
        if mountpoint -q "$mp" 2>/dev/null; then
            status="mounted"
        fi
        printf "    %d) %-40s (%s)\n" "$i" "$mp" "$status"
        i=$((i + 1))
    done
    echo ""

    # Let user select which mount to move
    local selection
    while true; do
        read -rp "  Select mount to move (1-${#mount_points[@]}, or 'q' to quit): " selection
        if [[ "$selection" == "q" ]]; then
            echo "  Cancelled."
            return 0
        fi
        if [[ "$selection" =~ ^[0-9]+$ ]] && [[ "$selection" -ge 1 ]] && [[ "$selection" -le ${#mount_points[@]} ]]; then
            break
        fi
        echo "  Invalid selection. Please enter a number between 1 and ${#mount_points[@]}"
    done

    local old_mount="${mount_points[$((selection-1))]}"
    local old_uuid="${uuids[$((selection-1))]}"
    local old_entry="${fstab_entries[$((selection-1))]}"

    echo ""
    echo "  Selected: $old_mount"
    echo ""

    # Get new mount point path
    local new_mount
    read -rp "  Enter new mount path (e.g., /mnt/blockstore): " new_mount

    if [[ -z "$new_mount" ]]; then
        die "Mount path cannot be empty"
    fi

    # Normalize path (remove trailing slash)
    new_mount="${new_mount%/}"

    if [[ "$new_mount" == "$old_mount" ]]; then
        die "New path is the same as the old path"
    fi

    # Check if new mount point already exists as a mount
    if mountpoint -q "$new_mount" 2>/dev/null; then
        die "Path $new_mount is already a mount point"
    fi

    echo ""
    echo "  Summary:"
    echo "    From: $old_mount"
    echo "    To:   $new_mount"
    echo ""

    if ! yesno "  Proceed with moving this mount?" "n"; then
        echo "  Cancelled."
        return 0
    fi

    # Backup fstab
    local backup="/etc/fstab.bak.$(date +%Y%m%d_%H%M%S)"
    cp /etc/fstab "$backup"
    success "Backed up fstab to $backup"

    # Create new mount point directory
    if [[ ! -d "$new_mount" ]]; then
        mkdir -p "$new_mount"
        success "Created directory $new_mount"
    fi

    # Check if old mount is currently mounted
    local was_mounted=false
    if mountpoint -q "$old_mount" 2>/dev/null; then
        was_mounted=true
        echo "  Unmounting $old_mount..."
        if ! umount "$old_mount" 2>/dev/null; then
            warn "Could not unmount $old_mount - it may be in use"
            echo "  Try: lsof +D $old_mount"
            echo "  Or reboot after fstab is updated"
        else
            success "Unmounted $old_mount"
        fi
    fi

    # Update fstab - replace old mount point with new one
    # Be careful to only replace the mount point field (2nd field)
    local escaped_old escaped_new
    escaped_old=$(printf '%s\n' "$old_mount" | sed 's/[[\.*^$()+?{|]/\\&/g')
    escaped_new=$(printf '%s\n' "$new_mount" | sed 's/[&/\]/\\&/g')

    # Use awk to precisely replace only the 2nd field
    awk -v old="$old_mount" -v new="$new_mount" '
    {
        if ($2 == old) {
            $2 = new
        }
        print
    }' /etc/fstab > /etc/fstab.tmp && mv /etc/fstab.tmp /etc/fstab

    success "Updated /etc/fstab"

    # Mount at new location
    echo "  Mounting at $new_mount..."
    if mount "$new_mount" 2>/dev/null; then
        success "Mounted at $new_mount"
    else
        warn "Could not mount at $new_mount - may need reboot"
        echo "  You can try: mount $new_mount"
    fi

    # Remind about config.toml
    echo ""
    echo "  ┌──────────────────────────────────────────────────────────────────────────┐"
    echo "  │ IMPORTANT: Update your config.toml                                       │"
    echo "  ├──────────────────────────────────────────────────────────────────────────┤"
    echo "  │                                                                          │"
    echo "  │ The mount point has been moved, but you need to update config.toml:     │"
    echo "  │                                                                          │"
    printf "  │   Old path: %-55s │\n" "$old_mount"
    printf "  │   New path: %-55s │\n" "$new_mount"
    echo "  │                                                                          │"
    echo "  │ Update the relevant [storage] section in your config.toml:              │"
    echo "  │   accounts = \"...\"                                                       │"
    echo "  │   blockstore = \"...\"                                                     │"
    echo "  │   snapshots = \"...\"                                                      │"
    echo "  │                                                                          │"
    echo "  └──────────────────────────────────────────────────────────────────────────┘"
    echo ""

    success "Mount point moved successfully"
}

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

show_help() {
    head -106 "$0" | tail -n +2 | grep -E "^#" | sed 's/^# \?//'
    exit 0
}

main() {
    local mode="${1:-}"

    case "$mode" in
        --benchmark)
            check_root
            run_benchmarks
            ;;
        --setup)
            check_root
            check_disk_deps
            interactive_setup
            ;;
        --clean-accounts)
            check_root
            clean_accounts
            ;;
        --clean-snapshots)
            check_root
            clean_snapshots
            ;;
        --clean-blockstore)
            check_root
            clean_blockstore
            ;;
        --clean-ledger)
            check_root
            clean_ledger
            ;;
        --clean-all)
            check_root
            clean_all
            ;;
        --reset)
            check_root
            check_disk_deps
            # Check for --wipe-disks flag
            if [[ "${2:-}" == "--wipe-disks" ]]; then
                reset_all true
            else
                reset_all false
            fi
            ;;
        --fix-noatime)
            check_root
            fix_noatime
            ;;
        --move)
            check_root
            move_mount
            ;;
        --status)
            show_status
            ;;
        --disk-info)
            show_disk_info
            ;;
        --disk-summary)
            show_disk_summary
            ;;
        --help|-h)
            show_help
            ;;
        "")
            echo ""
            echo "Mithril Disk Setup"
            echo ""
            echo "Usage:"
            echo "  sudo ./scripts/disk-setup.sh --benchmark          # Test NVMe speeds (safe)"
            echo "  sudo ./scripts/disk-setup.sh --setup              # Interactive setup (formats drives)"
            echo "  ./scripts/disk-setup.sh --status                  # Show current storage status"
            echo "  ./scripts/disk-setup.sh --disk-info               # Show UUIDs and device info"
            echo "  ./scripts/disk-setup.sh --disk-summary            # Show Mithril data usage breakdown"
            echo ""
            echo "Clean commands (for resetting Mithril):"
            echo "  sudo ./scripts/disk-setup.sh --clean-accounts     # Clear accounts (AccountsDB, index)"
            echo "  sudo ./scripts/disk-setup.sh --clean-ledger       # Clear ledger (snapshots + blockstore)"
            echo "  sudo ./scripts/disk-setup.sh --clean-snapshots    # Clear snapshots only"
            echo "  sudo ./scripts/disk-setup.sh --clean-blockstore   # Clear blockstore only"
            echo "  sudo ./scripts/disk-setup.sh --clean-all          # Clear everything"
            echo ""
            echo "Nuclear options (full teardown):"
            echo "  sudo ./scripts/disk-setup.sh --reset              # Unmount + remove fstab + delete dirs"
            echo "  sudo ./scripts/disk-setup.sh --reset --wipe-disks # Also wipe partitions"
            echo ""
            echo "Maintenance:"
            echo "  sudo ./scripts/disk-setup.sh --fix-noatime        # Add noatime to existing mounts"
            echo "  sudo ./scripts/disk-setup.sh --move               # Move a mount to a different path"
            echo ""
            echo "  ./scripts/disk-setup.sh --help                    # Show detailed help"
            echo ""
            exit 2
            ;;
        *)
            die "Unknown option: $mode. Use --help for usage."
            ;;
    esac
}

main "$@"
