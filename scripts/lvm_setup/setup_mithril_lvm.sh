#!/usr/bin/env bash
# Purpose: Script to set up and tear down LVM infrastructure for Mithril accounts DB
set -euo pipefail

### CONFIGURATION ###
# --- Disk and Partitioning ---
DEVICE="/dev/nvme3n1"              # The disk to use
LVM_PCT=80                         # % of DEVICE for LVM PV partition (p1) - Set to 80 for 20% other partition

# --- LVM Naming ---
VG_NAME="accounts_vg_run"          # LVM Volume Group name
THINPOOL_NAME="thinpool"           # LVM Thin Pool name
MASTER_LV_NAME="accounts_master"   # LVM Logical Volume name for master data (Origin for snapshots)

# --- Mount Points ---cccccbkblrnltenlrkecbkurufdegujicncghfueutiv

MASTER_MOUNTPOINT="/mnt/accounts_master" # Mount point for the master LV
OTHER_MOUNTPOINT="/var/lib/mithril"    # Mount point for the other partition (p2)

# --- User for Ownership ---
# Set the user:group for the snapshots subdirectory ownership
# If this user doesn't exist, ownership won't be changed from root.
SNAPSHOTS_DIR_OWNER="ubuntu:ubuntu"

# --- LVM Sizing Configuration ---
# Safety margin in GiB to leave unused in the VG (for LVM metadata, snapshots headroom if needed)
SAFETY_GIB=1
# Metadata size for thin-pool in GiB (1G is usually sufficient unless you expect thousands of snapshots)
META_GIB=1
### END CONFIGURATION ###

# --- Sanity Checks ---
if [ "$EUID" -ne 0 ]; then
  echo "Error: This script must be run as root (e.g., using sudo)." >&2
  exit 1
fi

# Check commands needed for setup and wipe
if ! command -v lsblk &> /dev/null || ! command -v parted &> /dev/null || \
   ! command -v pvcreate &> /dev/null || ! command -v vgcreate &> /dev/null || \
   ! command -v lvcreate &> /dev/null || ! command -v lvchange &> /dev/null || \
   ! command -v vgremove &> /dev/null || ! command -v pvremove &> /dev/null || \
   ! command -v lvs &> /dev/null || \
   ! command -v mkfs.ext4 &> /dev/null || \
   ! command -v wipefs &> /dev/null || ! command -v partprobe &> /dev/null || \
   ! command -v systemd-escape &> /dev/null || ! command -v findmnt &> /dev/null || \
   ! command -v blockdev &> /dev/null || ! command -v sed &> /dev/null || \
   ! command -v mkdir &> /dev/null || ! command -v chown &> /dev/null || ! command -v id &> /dev/null ; then
  echo "Error: Required commands (lsblk, parted, LVM(pv/vg/lvcreate/lvchange/vg/pvremove/lvs), mkfs.ext4, wipefs, partprobe, systemd-escape, findmnt, blockdev, sed, mkdir, chown, id) not found." >&2
  echo "Please install lvm2, e2fsprogs, util-linux, coreutils, and ensure systemd tools are available." >&2
  exit 1
fi


if [ ! -b "${DEVICE}" ]; then
  echo "Error: Device '${DEVICE}' not found or is not a block device." >&2
  lsblk -o NAME,SIZE,TYPE
  exit 1
fi
# --- End Sanity Checks ---

usage(){
  cat <<EOF
Usage: $0 <wipe|setup>

wipe          — Unmount filesystems created by 'setup', destroy LVM structure '${VG_NAME}'
                on ${DEVICE}p1, remove related fstab entries, and reset ${DEVICE}
                to a blank GPT partition table. USE WITH CAUTION.

setup         — Partition ${DEVICE} (${LVM_PCT}% for LVM, rest for other data),
                Initialize LVM PV -> VG '${VG_NAME}' -> Thin Pool '${THINPOOL_NAME}' -> Master LV '${MASTER_LV_NAME}',
                Format the Master LV and the other partition with ext4,
                Create mount points, mount the filesystems (${MASTER_MOUNTPOINT}, ${OTHER_MOUNTPOINT}),
                Create subdirectory ${OTHER_MOUNTPOINT}/snapshots and set owner to ${SNAPSHOTS_DIR_OWNER},
                Add entries to /etc/fstab for automatic mounting on reboot.

Configuration (Edit script to change):
  DEVICE:           ${DEVICE}
  LVM Partition %:  ${LVM_PCT}% -> ${DEVICE}p1
  Other Partition %: (100 - ${LVM_PCT})% -> ${DEVICE}p2
  VG Name:          ${VG_NAME}
  Thin Pool Name:   ${THINPOOL_NAME}
  Master LV Name:   ${MASTER_LV_NAME}
  Master Mount:     ${MASTER_MOUNTPOINT}
  Other Mount:      ${OTHER_MOUNTPOINT}
  Snapshots Subdir Owner: ${SNAPSHOTS_DIR_OWNER}
  LVM Safety/Meta:  ${SAFETY_GIB}G / ${META_GIB}G
EOF
  exit 1
}

# Ensure a command was provided
if [ $# -lt 1 ]; then
  usage
fi
cmd="$1"; shift

# --- Helper Function for Fstab ---
add_fstab_entry() {
  local device="$1"
  local mountpoint="$2"
  local fstype="$3"
  local options="$4"
  local dump="$5"
  local pass="$6"

  # Check if mountpoint or device is already in fstab
  if grep -qs " ${mountpoint} " /etc/fstab || grep -qs "^${device} " /etc/fstab; then
    echo " • Skipping fstab entry for ${mountpoint} (already present or device exists)."
  else
    echo " • Adding fstab entry for ${mountpoint}..."
    # Use printf for potentially safer output than echo
    printf "%s\t%s\t%s\t%s\t%d\t%d\n" "${device}" "${mountpoint}" "${fstype}" "${options}" "${dump}" "${pass}" >> /etc/fstab
    echo "   Reloading systemd manager configuration..."
    # Use systemctl check to avoid errors on non-systemd systems
    if command -v systemctl > /dev/null && systemctl is-system-running -q; then
      systemctl daemon-reload
    else
       echo "Warning: systemctl not found or system not running. Skipping daemon-reload." >&2
    fi
  fi
}


# --- Main Logic ---
case "$cmd" in

  wipe)
    read -p "WARNING: This will completely WIPE ${DEVICE} and destroy LVM data associated with partition ${DEVICE}p1. Type 'yes' to proceed: " confirm
    if [[ "$confirm" != "yes" ]]; then
        echo "Aborted."
        exit 1
    fi

    echo "▶ Stopping any dependent services (e.g., Mithril)..."
    # Try stopping common mount units, ignore errors if they don't exist
    systemctl stop "$(systemd-escape --path "${MASTER_MOUNTPOINT}").mount" "$(systemd-escape --path "${OTHER_MOUNTPOINT}").mount" 2>/dev/null || true
    systemctl stop mithril.service 2>/dev/null || true

    echo "▶ Unmounting filesystems setup by this script (Attempting forcefully)..."
    # Define partitions explicitly
    LVM_PART_WIPE="${DEVICE}p1"
    OTHER_PART_WIPE="${DEVICE}p2"

    # Attempt unmounts directly, using lazy unmount (-l) as a fallback if -R fails
    # These should be less likely to halt the script due to `|| true`
    if ! umount -R "${MASTER_MOUNTPOINT}" 2>/dev/null; then umount -l "${MASTER_MOUNTPOINT}" 2>/dev/null || true; fi
    if ! umount -R "${OTHER_MOUNTPOINT}" 2>/dev/null; then umount -l "${OTHER_MOUNTPOINT}" 2>/dev/null || true; fi
    sleep 1 # Give kernel a moment

    # Attempt to find the actual VG name on the target PV
    ACTUAL_VG=$(pvs --noheadings -o vg_name "${LVM_PART_WIPE}" 2>/dev/null | xargs || echo "")
    VG_TO_WIPE=""
    if [ -n "$ACTUAL_VG" ]; then
        VG_TO_WIPE="$ACTUAL_VG"
        echo "  Found existing VG '${ACTUAL_VG}' on PV ${LVM_PART_WIPE}. Targeting this for removal."
    else
        VG_TO_WIPE="$VG_NAME" # Use configured name as fallback
        echo "  No VG found directly on PV ${LVM_PART_WIPE}. Attempting removal using configured VG name '${VG_NAME}'."
    fi

    # Proceed only if we have a VG name (either found or configured)
    if [ -n "$VG_TO_WIPE" ]; then
      echo "▶ Deactivating and removing LVM structures for VG '${VG_TO_WIPE}'..."
      # Add verbose output and check status manually within the block
      echo "  Deactivating LVs in ${VG_TO_WIPE}..."
      lvchange -an "${VG_TO_WIPE}" # Attempt; ignore exit code below if needed
      if [ $? -ne 0 ]; then echo "  Warning: lvchange -an failed. Might already be inactive or other issue."; fi

      echo "  Removing LVs in ${VG_TO_WIPE}..."
      lvremove -f "${VG_TO_WIPE}" # Attempt
      if [ $? -ne 0 ]; then echo "  Warning: lvremove failed. LVs might not exist or be removable."; fi

      echo "  Deactivating VG ${VG_TO_WIPE}..."
      vgchange -an "${VG_TO_WIPE}" # Attempt
      if [ $? -ne 0 ]; then echo "  Warning: vgchange -an failed. Might already be inactive."; fi

      echo "  Removing VG ${VG_TO_WIPE}..."
      vgremove -f "${VG_TO_WIPE}" # Attempt
      if [ $? -ne 0 ]; then echo "  Warning: vgremove failed. VG might not exist or PVs might still be tagged."; fi
    else
        echo "  Skipping LVM VG/LV removal steps as no VG name determined."
    fi

    # Remove PV signature regardless - crucial step
    echo "▶ Removing PV signature from ${LVM_PART_WIPE}..."
    if ! pvremove -ff -y "${LVM_PART_WIPE}"; then
         echo "Error: Failed to remove PV signature from ${LVM_PART_WIPE}. Manual check required ('sudo pvremove -ff -y ${LVM_PART_WIPE}')." >&2
         # Exit here as subsequent partitioning will fail if PV isn't removed
         exit 1
    fi
    # Attempt removal from other partition just in case
    pvremove -ff -y "${OTHER_PART_WIPE}" 2>/dev/null || true

    echo "▶ Removing fstab entries created by setup..."
    MASTER_MOUNTPOINT_ESC=$(printf '%s\n' "$MASTER_MOUNTPOINT" | sed 's:[][\\/.^$*]:\\&:g')
    OTHER_MOUNTPOINT_ESC=$(printf '%s\n' "$OTHER_MOUNTPOINT" | sed 's:[][\\/.^$*]:\\&:g')
    # Use sed with error checking
    if ! sed -i.bak -e "\#\s${MASTER_MOUNTPOINT_ESC}\s#d" -e "\#\s${OTHER_MOUNTPOINT_ESC}\s#d" /etc/fstab; then
        echo "Warning: Failed to modify /etc/fstab with sed. Check /etc/fstab and /etc/fstab.bak manually." >&2
    else
        echo "   (Backup of /etc/fstab saved to /etc/fstab.bak)"
    fi

    echo "▶ Resetting partition table on ${DEVICE}..."
    # Wipe signatures again before partitioning
    wipefs -a "${LVM_PART_WIPE}" 2>/dev/null || true
    wipefs -a "${OTHER_PART_WIPE}" 2>/dev/null || true
    wipefs -a "${DEVICE}" 2>/dev/null || true
    # Create partition table
    if ! parted --script "${DEVICE}" mklabel gpt; then
        echo "Error: Failed to create new GPT label on ${DEVICE}." >&2
        exit 1
    fi

    echo "   Forcing kernel to re-read partition table..."
    partprobe "${DEVICE}" || echo "Warning: partprobe failed, a reboot might be needed to see changes." >&2
    sleep 1

    echo "✅ Wipe attempt finished. Check LVM status manually ('pvs', 'vgs', 'lvs'). ${DEVICE} should be blank."
    ;;

  setup)
    echo "▶ Checking if device ${DEVICE} is already in use..."
    if lsblk -no MOUNTPOINT "${DEVICE}" | grep -qv '^$'; then
       echo "Error: ${DEVICE} or its partitions appear to be mounted. Please run 'wipe' first or manually unmount." >&2
       lsblk -o NAME,SIZE,FSTYPE,MOUNTPOINT "${DEVICE}"
       exit 1
    fi
     LVM_PART_CHECK="${DEVICE}p1"
     if pvs "${LVM_PART_CHECK}" &>/dev/null || vgs "${VG_NAME}" &>/dev/null; then
       # If the configured VG name exists, that's an error state for setup
       if vgs "${VG_NAME}" &>/dev/null; then
            echo "Error: Volume Group '${VG_NAME}' already exists. Please run 'wipe' first." >&2
            vgs "${VG_NAME}" || true
            exit 1
       fi
       # If a PV exists on the target partition, that's also an error state
        if pvs "${LVM_PART_CHECK}" &>/dev/null; then
           echo "Error: LVM Physical Volume seems to exist on ${LVM_PART_CHECK}. Please run 'wipe' first." >&2
           pvs "${LVM_PART_CHECK}" || true
           exit 1
       fi
     fi

    echo "▶ Partitioning ${DEVICE}..."
    wipefs -a "${DEVICE}" 2>/dev/null || true
    parted --script "${DEVICE}" mklabel gpt
    parted --script --align optimal "${DEVICE}" \
      mkpart primary ext4 1MiB "${LVM_PCT}%" \
      mkpart primary ext4 "${LVM_PCT}%" 100%
    parted --script "${DEVICE}" set 1 lvm on
    parted --script "${DEVICE}" name 1 "${VG_NAME}_pv" name 2 "mithril_data" # Use updated VG_NAME

    echo "   Forcing kernel to re-read partition table..."
    partprobe "${DEVICE}" || echo "Warning: partprobe failed, partition changes might not be immediately visible." >&2
    sleep 2

    LVM_PART="${DEVICE}p1"
    OTHER_PART="${DEVICE}p2"

    echo "  Partition 1 (LVM PV): ${LVM_PART}"
    echo "  Partition 2 (Other):  ${OTHER_PART}"

    if [ ! -b "${LVM_PART}" ] || [ ! -b "${OTHER_PART}" ]; then
        echo "Error: Failed to create or detect partitions on ${DEVICE}. Waiting and retrying partprobe..." >&2
        sleep 5
        partprobe "${DEVICE}"
        sleep 2
        if [ ! -b "${LVM_PART}" ] || [ ! -b "${OTHER_PART}" ]; then
          echo "Error: Still cannot detect partitions. Please check manually. Output of lsblk:" >&2
          lsblk "${DEVICE}"
          exit 1
        fi
        echo "   Partitions detected after retry."
    fi

    echo "▶ Initializing LVM (VG: ${VG_NAME})..."
    if ! pvcreate -ff -y "${LVM_PART}"; then echo "Error: pvcreate failed on ${LVM_PART}"; exit 1; fi
    if ! vgcreate "${VG_NAME}" "${LVM_PART}"; then echo "Error: vgcreate failed for ${VG_NAME}"; pvremove -ff -y "${LVM_PART}" 2>/dev/null; exit 1; fi


    # Compute sizes for Thin Pool
    TOTAL_BYTES=$(blockdev --getsize64 "${LVM_PART}")
    PV_GIB=$((TOTAL_BYTES/1024/1024/1024))
    DATA_GIB=$((PV_GIB - META_GIB - SAFETY_GIB))

    if [ "$DATA_GIB" -le 0 ]; then
      echo "Error: Calculated DATA_GIB ($DATA_GIB) is zero or negative. Check PV size (${PV_GIB}G) vs META_GIB (${META_GIB}G) and SAFETY_GIB (${SAFETY_GIB}G)." >&2
      vgremove -f "${VG_NAME}" 2>/dev/null || true
      pvremove -ff -y "${LVM_PART}" 2>/dev/null || true
      exit 1
    fi

    echo "  Creating Thin Pool '${THINPOOL_NAME}' (${DATA_GIB}G data, ${META_GIB}G meta)..."
    if ! lvcreate --type thin-pool -L "${DATA_GIB}G" --poolmetadatasize "${META_GIB}G" -n "${THINPOOL_NAME}" "${VG_NAME}"; then
        echo "Error: Failed to create thin pool '${THINPOOL_NAME}'." >&2
        vgremove -f "${VG_NAME}" 2>/dev/null || true
        pvremove -ff -y "${LVM_PART}" 2>/dev/null || true
        exit 1
    fi

    echo "  Creating Master LV '${MASTER_LV_NAME}' (Virtual Size: ${DATA_GIB}G)..."
    if ! lvcreate --type thin --virtualsize "${DATA_GIB}G" -n "${MASTER_LV_NAME}" "${VG_NAME}/${THINPOOL_NAME}"; then
        echo "Error: Failed to create master LV '${MASTER_LV_NAME}'." >&2
        lvremove -f "${VG_NAME}/${THINPOOL_NAME}" 2>/dev/null || true # Attempt cleanup
        vgremove -f "${VG_NAME}" 2>/dev/null || true
        pvremove -ff -y "${LVM_PART}" 2>/dev/null || true
        exit 1
    fi

    sleep 3 # Give udev time

    MASTER_DEV_MAPPER_PATH="/dev/mapper/${VG_NAME}-${MASTER_LV_NAME}"
    FSTAB_MASTER_DEV_PATH="${MASTER_DEV_MAPPER_PATH}"

    if [ ! -b "${MASTER_DEV_MAPPER_PATH}" ] || [ ! -b "${OTHER_PART}" ]; then
        echo "Error: Device node did not appear after LVM creation or partition missing (${MASTER_DEV_MAPPER_PATH} or ${OTHER_PART})." >&2
        ls -l /dev/mapper/
        lsblk "${DEVICE}"
        exit 1
    fi

    echo "▶ Formatting filesystems (ext4)..."
    echo "  Formatting ${MASTER_DEV_MAPPER_PATH}..."
    if ! mkfs.ext4 -q -L "${MASTER_LV_NAME}" "${MASTER_DEV_MAPPER_PATH}"; then echo "Error: mkfs.ext4 failed on ${MASTER_DEV_MAPPER_PATH}"; exit 1; fi
    echo "  Formatting ${OTHER_PART}..."
    if ! mkfs.ext4 -q -L "mithril_data" "${OTHER_PART}"; then echo "Error: mkfs.ext4 failed on ${OTHER_PART}"; exit 1; fi

    echo "▶ Creating mount points..."
    mkdir -p "${MASTER_MOUNTPOINT}" "${OTHER_MOUNTPOINT}"
    echo "  Created ${MASTER_MOUNTPOINT}"
    echo "  Created ${OTHER_MOUNTPOINT}"

    echo "▶ Mounting filesystems..."
    # Activate the master LV explicitly before mounting (good practice)
    if ! lvchange -ay "${VG_NAME}/${MASTER_LV_NAME}"; then
        echo "Warning: Failed to activate ${VG_NAME}/${MASTER_LV_NAME}. Mount might fail." >&2
    fi
    sleep 1 # Small delay after activation

    # Mount master LV
    if ! mount "${MASTER_DEV_MAPPER_PATH}" "${MASTER_MOUNTPOINT}"; then
        echo "Error: Failed to mount Master LV ${MASTER_DEV_MAPPER_PATH} on ${MASTER_MOUNTPOINT}." >&2
        exit 1
    fi
    # Mount other partition
    if ! mount "${OTHER_PART}" "${OTHER_MOUNTPOINT}"; then
         echo "Error: Failed to mount Other Partition ${OTHER_PART} on ${OTHER_MOUNTPOINT}." >&2
         # Attempt unmount of master before exiting
         umount "${MASTER_MOUNTPOINT}" 2>/dev/null || true
         exit 1
    fi

    echo "  Mounted ${MASTER_DEV_MAPPER_PATH} -> ${MASTER_MOUNTPOINT}"
    echo "  Mounted ${OTHER_PART} -> ${OTHER_MOUNTPOINT}"

    ####################################################################
    # ADDED SECTION: Create snapshots subdirectory and set ownership   #
    ####################################################################
    echo "▶ Creating snapshots subdirectory..."
    SNAPSHOTS_SUBDIR="${OTHER_MOUNTPOINT}/snapshots"
    # Create directory
    if ! mkdir -p "${SNAPSHOTS_SUBDIR}"; then
        echo "Error: Failed to create directory ${SNAPSHOTS_SUBDIR}" >&2
        # Attempt unmounts before exiting
        umount -R "${MASTER_MOUNTPOINT}" 2>/dev/null || true
        umount -R "${OTHER_MOUNTPOINT}" 2>/dev/null || true
        exit 1
    fi
    # Set ownership (extract user from config)
    TARGET_USER=$(echo "$SNAPSHOTS_DIR_OWNER" | cut -d: -f1)
    if id "$TARGET_USER" &>/dev/null; then
        if ! chown "$SNAPSHOTS_DIR_OWNER" "${SNAPSHOTS_SUBDIR}"; then
             echo "Warning: Failed to change ownership of ${SNAPSHOTS_SUBDIR} to ${SNAPSHOTS_DIR_OWNER}." >&2
             # Non-fatal warning, script can continue
        else
             echo "  Created ${SNAPSHOTS_SUBDIR} and set ownership to ${SNAPSHOTS_DIR_OWNER}."
        fi
    else
        echo "  Created ${SNAPSHOTS_SUBDIR}. User '${TARGET_USER}' not found, skipping chown."
    fi
    ####################################################################
    # END ADDED SECTION                                                #
    ####################################################################

    echo "▶ Updating /etc/fstab for auto-mount..."
    add_fstab_entry "${FSTAB_MASTER_DEV_PATH}" "${MASTER_MOUNTPOINT}" "ext4" "defaults,nofail" 0 2
    add_fstab_entry "${OTHER_PART}" "${OTHER_MOUNTPOINT}" "ext4" "defaults,nofail" 0 2

    echo ""
    echo "✅ Setup complete!"
    echo "--------------------------------------------------"
    lvs "${VG_NAME}" # Show LVM layout
    echo ""
    lsblk -o NAME,SIZE,FSTYPE,MOUNTPOINT "${DEVICE}"
    echo "--------------------------------------------------"
    echo " • Master LV (${MASTER_DEV_MAPPER_PATH}) mounted at ${MASTER_MOUNTPOINT}"
    echo " • Data partition (${OTHER_PART}) mounted at ${OTHER_MOUNTPOINT}"
    echo " • Subdirectory ${SNAPSHOTS_SUBDIR} created."
    echo " • /etc/fstab updated for automatic mounting on reboot."
    echo ""
    echo "Next: Build your initial database in ${MASTER_MOUNTPOINT} using 'mithril verifier --snapshot ...'"
    echo "Then use the separate snapshot management script."
    ;;

  *)
    usage
    ;;
esac