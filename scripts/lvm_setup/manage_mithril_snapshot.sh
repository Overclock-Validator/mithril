#!/usr/bin/env bash
# Purpose: Script to create, mount, and remove LVM snapshots for Mithril runs.
# Assumes LVM infrastructure (VG, Thin Pool, Master LV) was created by setup_mithril_lvm.sh
set -euo pipefail

### CONFIGURATION ###
# --- LVM Naming (MUST MATCH setup_mithril_lvm.sh) ---
VG_NAME="accounts_vg_run"          # LVM Volume Group name
MASTER_LV_NAME="accounts_master"   # LVM Logical Volume name for master data (Origin for snapshots)

# --- Snapshot Mount Point ---
SNAPSHOT_MOUNT_BASE="/mnt"             # Base directory where snapshot subdirectories will be created/mounted
### END CONFIGURATION ###

# --- Sanity Checks ---
if [ "$EUID" -ne 0 ]; then
  echo "Error: This script must be run as root (e.g., using sudo)." >&2
  exit 1
fi

# Check commands needed for snapshot management
if ! command -v lvs &> /dev/null || ! command -v lvcreate &> /dev/null || \
   ! command -v lvchange &> /dev/null || ! command -v lvremove &> /dev/null || \
   ! command -v e2fsck &> /dev/null || ! command -v findmnt &> /dev/null || \
   ! command -v mkdir &> /dev/null || ! command -v rmdir &> /dev/null || \
   ! command -v mount &> /dev/null || ! command -v umount &> /dev/null ; then
  echo "Error: Required commands (LVM tools(lvs/lvcreate/lvchange/lvremove), e2fsck, findmnt, mkdir, rmdir, mount, umount) not found." >&2
  echo "Please install lvm2, e2fsprogs, util-linux." >&2
  exit 1
fi
# --- End Sanity Checks ---

usage(){
  cat <<EOF
Usage: $0 <snapshot|remove_snapshot> <name> [snapshot_args]

snapshot <name> [mount_point_suffix]
              — Create an LVM thin snapshot of ${VG_NAME}/${MASTER_LV_NAME}.
                <name>: Name for the snapshot LV (e.g., 'run1', 'palmer').
                [mount_point_suffix]: Optional. If provided, mounts at ${SNAPSHOT_MOUNT_BASE}/<mount_point_suffix>.
                                     If omitted, mounts at ${SNAPSHOT_MOUNT_BASE}/<name>.
                Example: '$0 snapshot run1' creates '${VG_NAME}/run1'
                         and mounts it at '${SNAPSHOT_MOUNT_BASE}/run1'.
                Example: '$0 snapshot run1 run1_mount' creates '${VG_NAME}/run1'
                         and mounts it at '${SNAPSHOT_MOUNT_BASE}/run1_mount'.
                NOTE: Snapshot size limit is implicitly the free space in the thin pool.
                NOTE: Assumes origin filesystem is ext4 for e2fsck check.

remove_snapshot <name>
              — Unmounts, deactivates, and removes the specified LVM snapshot LV.
                <name>: The exact name of the snapshot LV to remove (e.g., 'run1', 'palmer').
                Example: '$0 remove_snapshot run1'

Configuration (Edit script to change):
  VG Name:          ${VG_NAME} (Must match setup script)
  Master LV Name:   ${MASTER_LV_NAME} (Must match setup script)
  Snapshot Base:    ${SNAPSHOT_MOUNT_BASE}
EOF
  exit 1
}

# Ensure a command was provided
if [ $# -lt 1 ]; then
  usage
fi
cmd="$1"; shift

# --- Main Logic ---
case "$cmd" in

  snapshot)
    # Argument parsing (Removed size argument)
    if [ $# -lt 1 ]; then
      echo "Error: Missing arguments for snapshot command." >&2
      usage
      exit 1
    fi
    SNAPSHOT_NAME="$1"
    SNAPSHOT_MOUNT_SUFFIX="${2:-$1}" # Mount suffix is now the second arg (optional)
    SNAPSHOT_MOUNTPOINT="${SNAPSHOT_MOUNT_BASE}/${SNAPSHOT_MOUNT_SUFFIX}"
    ORIGIN_PATH="${VG_NAME}/${MASTER_LV_NAME}"
    SNAPSHOT_PATH="${VG_NAME}/${SNAPSHOT_NAME}"
    SNAPSHOT_DEV_MAPPER_PATH="/dev/mapper/${VG_NAME}-${SNAPSHOT_NAME}"

    echo "▶ Preparing snapshot '${SNAPSHOT_NAME}'..."
    echo "  Origin LV:    ${ORIGIN_PATH}"
    echo "  Size Limit:   (Uses Thin Pool Space)" # Clarification
    echo "  Mount Point:  ${SNAPSHOT_MOUNTPOINT}"

    # --- Checks ---
    echo "  Checking if origin LV exists..."
    if ! lvs "${ORIGIN_PATH}" &>/dev/null; then
        echo "Error: Origin LV '${ORIGIN_PATH}' not found. Did 'setup_mithril_lvm.sh setup' complete successfully?" >&2
        exit 1
    fi
    echo "  Checking if snapshot LV already exists..."
    if lvs "${SNAPSHOT_PATH}" &>/dev/null; then
        echo "Error: Snapshot LV '${SNAPSHOT_PATH}' already exists. Remove it first ('$0 remove_snapshot ${SNAPSHOT_NAME}') or choose a different name." >&2
        exit 1
    fi
     echo "  Checking if mount point is already in use..."
     if [ -d "${SNAPSHOT_MOUNTPOINT}" ] && findmnt -rno TARGET "${SNAPSHOT_MOUNTPOINT}" > /dev/null; then
         echo "Error: Mount point '${SNAPSHOT_MOUNTPOINT}' already exists and seems to have something mounted on it." >&2
         exit 1
     fi
    # --- End Checks ---

    echo "▶ Creating LVM snapshot..."
    # Corrected lvcreate command - removed -L argument
    if ! lvcreate -s -n "${SNAPSHOT_NAME}" "${ORIGIN_PATH}"; then
        echo "Error: Failed to create snapshot LV '${SNAPSHOT_PATH}'." >&2
        exit 1
    fi
    echo "  Snapshot LV '${SNAPSHOT_PATH}' created."

    echo "▶ Activating LVM snapshot..."
    # Corrected lvchange command - added -K flag
    if ! lvchange -ay -K "${SNAPSHOT_PATH}"; then
        echo "Error: Failed to activate snapshot LV '${SNAPSHOT_PATH}'." >&2
        # Attempt cleanup if activation fails
        lvremove -f "${SNAPSHOT_PATH}" 2> /dev/null || true
        exit 1
    fi
    # Add a delay for device node propagation (adjustable if needed)
    sleep 5
    if [ ! -b "${SNAPSHOT_DEV_MAPPER_PATH}" ]; then
        echo "Error: Device node '${SNAPSHOT_DEV_MAPPER_PATH}' did not appear after activation." >&2
        lvremove -f "${SNAPSHOT_PATH}" 2> /dev/null || true
        exit 1
    fi
    echo "  Snapshot LV '${SNAPSHOT_PATH}' activated (${SNAPSHOT_DEV_MAPPER_PATH})."

    echo "▶ Checking snapshot filesystem (assuming ext4)..."
    # Use -p for automatic repair without prompting, suitable for automation
    e2fsck -p -f "${SNAPSHOT_DEV_MAPPER_PATH}"
    E2FSK_EXIT=$?
    if [ $E2FSK_EXIT -ge 4 ]; then
        echo "Error: Filesystem check failed with critical errors (exit code $E2FSK_EXIT) on '${SNAPSHOT_DEV_MAPPER_PATH}'. Manual intervention required." >&2
        echo "       You might need to run e2fsck manually without -p." >&2
        # Deactivate but leave snapshot for inspection
        lvchange -an "${SNAPSHOT_PATH}" 2> /dev/null || true
        exit 1
    elif [ $E2FSK_EXIT -ne 0 ]; then
        echo "Warning: Filesystem check found and corrected errors (exit code $E2FSK_EXIT)." >&2
    else
         echo "  Filesystem check clean."
    fi

    echo "▶ Creating mount point '${SNAPSHOT_MOUNTPOINT}'..."
    mkdir -p "${SNAPSHOT_MOUNTPOINT}"

    echo "▶ Mounting snapshot..."
    if ! mount "${SNAPSHOT_DEV_MAPPER_PATH}" "${SNAPSHOT_MOUNTPOINT}"; then
        echo "Error: Failed to mount '${SNAPSHOT_DEV_MAPPER_PATH}' on '${SNAPSHOT_MOUNTPOINT}'." >&2
        # Attempt cleanup
        lvchange -an "${SNAPSHOT_PATH}" 2> /dev/null || true
        exit 1
    fi

    echo ""
    echo "✅ Snapshot '${SNAPSHOT_NAME}' is ready!"
    echo "--------------------------------------------------"
    lvs "${SNAPSHOT_PATH}"
    echo ""
    df -h "${SNAPSHOT_MOUNTPOINT}"
    echo "--------------------------------------------------"
    echo " • Snapshot LV (${SNAPSHOT_DEV_MAPPER_PATH}) mounted at ${SNAPSHOT_MOUNTPOINT}"
    echo " • Ready for Mithril run using the snapshot mount point."
    ;;

  remove_snapshot)
    # Argument parsing
    if [ $# -lt 1 ]; then
      echo "Error: Missing snapshot name for remove_snapshot command." >&2
      usage
      exit 1
    fi
    SNAPSHOT_NAME="$1"
    SNAPSHOT_PATH="${VG_NAME}/${SNAPSHOT_NAME}"
    SNAPSHOT_DEV_MAPPER_PATH="/dev/mapper/${VG_NAME}-${SNAPSHOT_NAME}"

    echo "▶ Preparing to remove snapshot '${SNAPSHOT_NAME}'..."

    # --- Checks ---
    echo "  Checking if snapshot LV exists..."
    if ! lvs "${SNAPSHOT_PATH}" &>/dev/null; then
        echo "Info: Snapshot LV '${SNAPSHOT_PATH}' not found. Nothing to remove." >&2
        exit 0 # Exit cleanly if it doesn't exist
    fi
    # --- End Checks ---

    # Find where it's mounted (if anywhere)
    # findmnt returns non-zero if not found, use || true to prevent script exit
    SNAPSHOT_MOUNTPOINT=$(findmnt -nr -o TARGET --source "${SNAPSHOT_DEV_MAPPER_PATH}" || true)

    if [ -n "$SNAPSHOT_MOUNTPOINT" ]; then
        echo "▶ Unmounting snapshot from '${SNAPSHOT_MOUNTPOINT}'..."
        # Use -R for recursive unmount, handle errors gracefully
        if ! umount -R "${SNAPSHOT_MOUNTPOINT}"; then
             echo "Warning: Unmount command failed ('$?'). Attempting lazy unmount..." >&2
             sleep 1 # Give a moment before lazy unmount
             umount -l "${SNAPSHOT_MOUNTPOINT}" || echo "Warning: Lazy unmount also failed. Proceeding with LV removal might be risky." >&2
        fi
    else
        echo "  Snapshot not currently mounted."
    fi

    echo "▶ Deactivating LVM snapshot..."
    # Ignore error if already inactive
    lvchange -an "${SNAPSHOT_PATH}" 2>/dev/null || true

    echo "▶ Removing LVM snapshot..."
    # Use -f to force removal without confirmation
    if ! lvremove -f "${SNAPSHOT_PATH}"; then
        echo "Error: Failed to remove snapshot LV '${SNAPSHOT_PATH}'. Check LVM state manually ('sudo lvs ${VG_NAME}')." >&2
        exit 1
    fi

    # Optional: Try removing the mount point directory if it exists and is empty
    if [ -n "$SNAPSHOT_MOUNTPOINT" ] && [ -d "$SNAPSHOT_MOUNTPOINT" ]; then
        if rmdir "$SNAPSHOT_MOUNTPOINT" 2>/dev/null; then
            echo "  Removed empty mount point directory '${SNAPSHOT_MOUNTPOINT}'."
        else
            # Don't echo warning if directory simply doesn't exist after unmount race condition
            if [ -d "$SNAPSHOT_MOUNTPOINT" ]; then
                echo "  Note: Mount point directory '${SNAPSHOT_MOUNTPOINT}' was not removed (likely not empty or permissions issue)."
            fi
        fi
    fi

    echo ""
    echo "✅ Snapshot '${SNAPSHOT_NAME}' removed successfully."
    ;;

  *)
    usage
    ;;
esac