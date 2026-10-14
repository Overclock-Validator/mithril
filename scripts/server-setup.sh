#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# server-setup.sh - Beginner-friendly Ubuntu server setup for Mithril
# ==============================================================================
#
# PURPOSE:
# This script helps you set up a fresh Ubuntu server or harden an existing one.
# It focuses on the OS installation and security - NOT storage/performance tuning.
#
# MODES:
#   install : Fresh Ubuntu 26.04 install from rescue/live environment (DESTRUCTIVE)
#   harden  : Safe on existing Ubuntu - user setup, SSH keys, security packages
#   status  : Show current security configuration (no changes)
#
# RELATED SCRIPTS (use AFTER this one):
#   disk-setup.sh      - Configure NVMe drives for Mithril (benchmarks, formatting)
#   performance-tune.sh - Kernel tuning, I/O scheduler, CPU performance mode
#
# TYPICAL WORKFLOW:
#   1. ./server-setup.sh install   # Fresh OS install (from rescue boot)
#   2. Reboot into new OS
#   3. ./server-setup.sh harden    # (if needed) add keys, security packages
#   4. ./scripts/disk-setup.sh --setup      # Format drives for Mithril (offers benchmarks)
#   5. ./scripts/performance-tune.sh        # Apply performance optimizations
#
# DESIGN GOALS:
#   - Explain what is happening before doing it
#   - Never silently lock you out of SSH
#   - Keep OS partition from consuming the entire OS disk
#   - Beginner-friendly prompts and confirmations
#
# ==============================================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

die()     { echo -e "\n${RED}[ERROR]${NC} $*\n" >&2; exit 1; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
info()    { echo -e "\n${BLUE}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC} $*"; }

need() { command -v "$1" >/dev/null 2>&1 || die "Missing command: $1"; }

is_root() { [[ ${EUID:-0} -eq 0 ]]; }
in_uefi() { [[ -d /sys/firmware/efi ]]; }

pause() { read -r -p "Press Enter to continue..."; }

prompt() {
    local var="$1" msg="$2" def="${3:-}"
    read -r -p "$msg${def:+ [$def]}: " val
    printf -v "$var" '%s' "${val:-$def}"
}

yesno() {
    local q="$1" def="${2:-n}" ans
    local hint="[y/N]"
    [[ "${def,,}" == "y" ]] && hint="[Y/n]"
    read -r -p "$q $hint: " ans
    ans="${ans:-$def}"
    [[ "${ans,,}" == "y" ]]
}

confirm_phrase() {
    local phrase="$1"
    echo
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo "  DESTRUCTIVE ACTION CONFIRMATION"
    echo "  This will ERASE data on disk(s)."
    echo "  To proceed, type exactly:"
    echo "    $phrase"
    echo "  Anything else will abort."
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    read -r -p "> " got
    [[ "$got" == "$phrase" ]] || die "Confirmation failed. Aborting."
}

# ------------------------------------------------------------------------------
# Disk helpers (minimal - just for OS install, not storage setup)
# ------------------------------------------------------------------------------

list_disks() {
    lsblk -dn -o NAME,TYPE | awk '$2=="disk"{print "/dev/"$1}'
}

disk_summary() {
    # Use lsblk with explicit output to handle empty MODEL/SERIAL fields
    local disk name size model serial
    for disk in $(list_disks); do
        name=$(basename "$disk")
        size=$(lsblk -dn -o SIZE "$disk" 2>/dev/null | tr -d ' ')
        model=$(lsblk -dn -o MODEL "$disk" 2>/dev/null | sed 's/^ *//;s/ *$//')
        serial=$(lsblk -dn -o SERIAL "$disk" 2>/dev/null | sed 's/^ *//;s/ *$//')
        printf "  /dev/%-10s  %-8s  %-30s  %s\n" "$name" "$size" "${model:-(unknown)}" "${serial:-}"
    done
}

part_path() {
    # NVMe: /dev/nvme0n1p1 ; SATA: /dev/sda1
    # Detect by name pattern, not by checking if device exists (partitions may not exist yet)
    local disk="$1" partnum="$2"
    if [[ "$disk" == *nvme* ]]; then
        echo "${disk}p${partnum}"
    else
        echo "${disk}${partnum}"
    fi
}

# Check if disk has any mounted partitions
disk_has_mounts() {
    local disk="$1"
    lsblk -nr -o MOUNTPOINT "$disk" 2>/dev/null | grep -qv '^$'
}

# Check if disk contains the running root filesystem
disk_contains_root() {
    local disk="$1"
    local root_dev
    root_dev="$(findmnt -n -o SOURCE / 2>/dev/null || true)"
    [[ "$root_dev" == /dev/* ]] || return 1
    # Walk through LVM and RAID too; string prefixes miss their backing disks.
    lsblk -snrpo NAME "$root_dev" | grep -qxF "$disk"
}

# Get MAC address for interface
get_mac() {
    local iface="$1"
    cat "/sys/class/net/$iface/address" 2>/dev/null || echo ""
}

# Get disk size in GiB
disk_size_gib() {
    local disk="$1"
    local bytes
    bytes=$(lsblk -bdn -o SIZE "$disk" 2>/dev/null || echo "0")
    echo $((bytes / 1024 / 1024 / 1024))
}

# Get disk serial number
disk_serial() {
    local disk="$1"
    lsblk -dn -o SERIAL "$disk" 2>/dev/null | tr -d ' ' || echo "unknown"
}

# ------------------------------------------------------------------------------
# User / SSH helpers
# ------------------------------------------------------------------------------

ensure_user() {
    local user="$1"
    if id -u "$user" >/dev/null 2>&1; then
        echo "  User '$user' already exists."
    else
        info "Creating user '$user' (no password)."
        adduser --disabled-password --gecos "" "$user"
    fi
    # Provider images may already contain a locked-password ubuntu account.
    # A locked password cannot authenticate sudo, even in the sudo group.
    if [[ "$(passwd -S "$user" | awk '{print $2}')" == L ]]; then
        info "Configuring passwordless sudo for key-only admin '$user'."
        local sudoers="/etc/sudoers.d/mithril-$user"
        printf '%s ALL=(ALL) NOPASSWD:ALL\n' "$user" > "$sudoers"
        chmod 0440 "$sudoers"
        visudo -cf "$sudoers"
    fi
    usermod -aG sudo "$user"
}

get_home() { getent passwd "$1" | cut -d: -f6; }

show_authorized_keys() {
    local user="$1" home
    home="$(get_home "$user")"
    echo
    echo "  ---- ${user} authorized_keys ----"
    if [[ -f "$home/.ssh/authorized_keys" ]]; then
        nl -ba "$home/.ssh/authorized_keys" | sed -e 's/\t/: /' | sed 's/^/  /'
    else
        echo "  (none)"
    fi
    echo "  ----------------------------------"
}

# Validate SSH public key format
validate_ssh_pubkey() {
    local pubkey="$1"
    [[ -n "$pubkey" ]] || die "SSH public key is empty."

    [[ "$pubkey" != *$'\n'* && "$pubkey" != *$'\r'* ]] || die "Paste a single public key line."
    local key_file
    key_file=$(mktemp)
    printf '%s\n' "$pubkey" > "$key_file"
    if ! ssh-keygen -l -f "$key_file" >/dev/null 2>&1; then
        rm -f "$key_file"
        die "Invalid OpenSSH public key."
    fi
    rm -f "$key_file"

}

ensure_pubkey_for_user() {
    local user="$1" pubkey="$2"

    validate_ssh_pubkey "$pubkey"

    local home
    home="$(get_home "$user")"
    install -d -m 0700 -o "$user" -g "$user" "$home/.ssh"
    touch "$home/.ssh/authorized_keys"
    chown "$user:$user" "$home/.ssh/authorized_keys"
    chmod 0600 "$home/.ssh/authorized_keys"

    if grep -qxF "$pubkey" "$home/.ssh/authorized_keys"; then
        echo "  Key already present in $user authorized_keys."
    else
        echo "$pubkey" >> "$home/.ssh/authorized_keys"
        success "Added key to $user authorized_keys."
    fi
}

sshd_password_auth_enabled() {
    if command -v sshd >/dev/null 2>&1; then
        sshd -T 2>/dev/null | awk '$1=="passwordauthentication"{print $2}' | grep -qi '^yes$'
    else
        return 0
    fi
}

sshd_effective() {
    if command -v sshd >/dev/null 2>&1; then
        sshd -T 2>/dev/null | grep -Ei 'passwordauthentication|permitrootlogin|kbdinteractiveauthentication' || true
    fi
}

apply_sshd_dropin() {
    local disable_password="$1" disable_root="$2"
    local config_dir="${3:-/etc/ssh/sshd_config.d}"
    local config="${4:-/etc/ssh/sshd_config}"
    local dropin="$config_dir/00-mithril-hardening.conf" backup effective valid=yes
    install -d -m 0755 "$config_dir"
    backup=$(mktemp -d)
    [[ ! -e "$dropin" ]] || cp -p "$dropin" "$backup/previous"
    # OpenSSH uses the FIRST value, so a late 99-* file loses to cloud-init.
    cat > "$dropin" <<EOF
# Managed by server-setup.sh
# Remove this file to revert to defaults.
KbdInteractiveAuthentication no
$( [[ "$disable_password" == "yes" ]] && echo "PasswordAuthentication no" )
$( [[ "$disable_root" == "yes" ]] && echo "PermitRootLogin no" )
EOF
    chmod 0644 "$dropin"
    # Validate both global settings and Match rules for this administrator/client.
    local context client_addr="${SSH_CONNECTION:-}"
    client_addr=${client_addr%% *}
    for context in "" "user=${ADMIN_USER:-ubuntu},host=localhost,addr=${client_addr:-127.0.0.1}" "user=root,host=localhost,addr=${client_addr:-127.0.0.1}"; do
        local args=(-f "$config")
        [[ -z "$context" ]] || args+=(-C "$context")
        if ! sshd -f "$config" -t || ! effective=$(sshd "${args[@]}" -T); then
            valid=no
            break
        fi
        grep -qx 'kbdinteractiveauthentication no' <<< "$effective" || valid=no
        if [[ "$disable_password" == "yes" ]]; then
            grep -qx 'passwordauthentication no' <<< "$effective" || valid=no
        fi
        if [[ "$disable_root" == "yes" ]]; then
            grep -qx 'permitrootlogin no' <<< "$effective" || valid=no
        fi
    done
    if [[ "$valid" == yes ]] && systemctl reload ssh; then
        rm -rf "$backup"
        return 0
    fi
    warn "SSH hardening could not be applied; restoring the previous configuration."
    if [[ -f "$backup/previous" ]]; then
        cp -p "$backup/previous" "$dropin"
    else
        rm -f "$dropin"
    fi
    rm -rf "$backup"
    return 1
}

confirm_admin_access() {
    local home
    home=$(get_home "$ADMIN_USER")
    [[ -s "$home/.ssh/authorized_keys" ]] || die "No SSH keys for $ADMIN_USER. Leaving SSH unchanged."
    echo "  Keep this session open. In a SECOND terminal, log in using the admin key:"
    echo "    ssh $ADMIN_USER@<server_ip>"
    echo "  Then confirm administrative access: sudo -v && sudo true"
    local confirmed
    read -r -p "  After BOTH commands succeed, type ACCESS VERIFIED: " confirmed
    [[ "$confirmed" == "ACCESS VERIFIED" ]] || die "Admin access not confirmed. Leaving SSH unchanged."
}

# ------------------------------------------------------------------------------
# Security helpers
# ------------------------------------------------------------------------------

check_security_status() {
    echo
    echo "  Security status:"
    echo "    - sshd installed:      $([[ -x /usr/sbin/sshd ]] && echo yes || echo no)"
    echo "    - ssh service:         $(systemctl is-enabled ssh 2>/dev/null || true)"
    echo "    - ssh socket:          $(systemctl is-enabled ssh.socket 2>/dev/null || true)"
    echo "    - fail2ban installed:  $([[ -x /usr/bin/fail2ban-client ]] && echo yes || echo no)"
    if [[ -x /usr/bin/fail2ban-client ]]; then
        local f2b_status
        f2b_status=$(fail2ban-client status sshd 2>/dev/null | grep -oP 'Currently banned:\s*\K\d+' || echo "?")
        echo "    - fail2ban banned IPs: $f2b_status"
    fi
    echo "    - ufw installed:       $([[ -x /usr/sbin/ufw ]] && echo yes || echo no)"
    if [[ -x /usr/sbin/ufw ]]; then
        local ufw_status
        ufw_status=$(ufw status 2>/dev/null | head -1 || echo "unknown")
        echo "    - ufw status:          $ufw_status"
    fi
    echo "    - unattended-upgrades: $([[ -f /etc/apt/apt.conf.d/20auto-upgrades ]] && echo configured || echo no)"
    echo "    - chrony (time sync):  $(systemctl is-enabled chrony 2>/dev/null || echo not installed)"
    echo "    - haveged (entropy):   $(systemctl is-enabled haveged 2>/dev/null || echo not installed)"
    if command -v sshd >/dev/null 2>&1; then
        echo "    - SSH effective config:"
        sshd_effective | sed 's/^/        /'
    else
        echo "    - SSH effective config: unknown (sshd not installed)"
    fi
    echo
}

install_security_packages() {
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y openssh-server sudo fail2ban ufw unattended-upgrades \
                       htop iotop nload vim nano tmux
}

configure_fail2ban() {
    install -d -m 0755 /etc/fail2ban/jail.d
    cat > /etc/fail2ban/jail.d/sshd.local <<'EOF'
[sshd]
enabled = true
backend = systemd
bantime = 1h
findtime = 10m
maxretry = 5
EOF
    systemctl enable fail2ban >/dev/null 2>&1 || true
    systemctl restart fail2ban >/dev/null 2>&1 || true
}

configure_ufw() {
    # Set secure defaults
    ufw default deny incoming >/dev/null
    ufw default allow outgoing >/dev/null
    # Preserve access on custom SSH ports and with ssh.socket activation.
    local ports port
    ports=$(sshd -T | awk '$1=="port" {print $2}')
    [[ -n "$ports" ]] || die "Cannot determine SSH ports; refusing firewall changes."
    for port in $ports; do
        ufw allow "$port/tcp" >/dev/null
    done
    if [[ -n "${SSH_CONNECTION:-}" ]]; then
        port=${SSH_CONNECTION##* }
        [[ "$port" =~ ^[0-9]+$ ]] || die "Invalid active SSH port."
        ufw allow "$port/tcp" >/dev/null
    fi
}

enable_unattended() {
    dpkg-reconfigure -f noninteractive unattended-upgrades >/dev/null 2>&1 || true
    systemctl enable unattended-upgrades >/dev/null 2>&1 || true
}

# ------------------------------------------------------------------------------
# Choose admin user (shared UX)
# ------------------------------------------------------------------------------

choose_admin_user() {
    local default="ubuntu"
    echo
    echo "  Admin user setup:"
    echo "    Recommended default username: ubuntu"
    if yesno "  Use username '$default'?" "y"; then
        ADMIN_USER="$default"
    else
        prompt ADMIN_USER "  Enter admin username" ""
        [[ -n "$ADMIN_USER" ]] || die "Username cannot be empty."
    fi
    [[ "$ADMIN_USER" =~ ^[a-z_][a-z0-9_-]*$ && "$ADMIN_USER" != root ]] || die "Choose a valid non-root admin username."
}

# ==============================================================================
# MODE: status
# ==============================================================================

mode_status() {
    echo
    echo "================================================================================"
    echo "                    SERVER SECURITY STATUS"
    echo "================================================================================"

    check_security_status

    # Show users with sudo access
    echo "  Users with sudo access:"
    getent group sudo 2>/dev/null | cut -d: -f4 | tr ',' '\n' | sed 's/^/    - /' || echo "    (unable to determine)"
    echo

    # Show SSH keys for common users
    for user in root ubuntu; do
        if id -u "$user" >/dev/null 2>&1; then
            local home
            home="$(get_home "$user")"
            if [[ -f "$home/.ssh/authorized_keys" ]]; then
                local key_count
                key_count=$(grep -c '^ssh-\|^ecdsa-\|^sk-' "$home/.ssh/authorized_keys" 2>/dev/null || echo "0")
                echo "  SSH keys for $user: $key_count"
            fi
        fi
    done

    echo
    echo "================================================================================"
}

# ==============================================================================
# MODE: harden
# ==============================================================================

mode_harden() {
    is_root || die "Run as root. Try: sudo ./server-setup.sh harden"

    echo
    echo "================================================================================"
    echo "                    HARDEN MODE (safe - no disk changes)"
    echo "================================================================================"
    echo
    echo "  This mode will help you:"
    echo "    - Create/ensure an admin user with sudo access"
    echo "    - Add an SSH public key (ed25519 recommended)"
    echo "    - Optionally disable SSH password login (ONLY if a key exists)"
    echo "    - Install/configure: fail2ban, ufw (SSH allowed), unattended upgrades"
    echo
    pause

    need apt-get
    install_security_packages

    choose_admin_user
    ensure_user "$ADMIN_USER"

    info "Existing SSH keys for $ADMIN_USER:"
    show_authorized_keys "$ADMIN_USER"

    if yesno "  Add an SSH public key for '$ADMIN_USER' now?" "y"; then
        echo
        echo "  Paste a PUBLIC key line. Supported formats:"
        echo "    - ssh-ed25519 AAAA... (recommended)"
        echo "    - sk-ssh-ed25519@openssh.com AAAA... (FIDO2 hardware key)"
        echo "    - ssh-rsa AAAA..."
        echo
        echo "  If you don't have one yet, generate on your laptop:"
        echo "    ssh-keygen -t ed25519"
        echo "  Then paste the contents of ~/.ssh/id_ed25519.pub"
        echo
        prompt SSH_PUBKEY "  SSH public key" ""
        ensure_pubkey_for_user "$ADMIN_USER" "$SSH_PUBKEY"
    fi

    info "Updated authorized_keys for $ADMIN_USER:"
    show_authorized_keys "$ADMIN_USER"

    # Password auth handling
    local home
    home="$(get_home "$ADMIN_USER")"
    local has_keys="no"
    [[ -s "$home/.ssh/authorized_keys" ]] && has_keys="yes"

    if sshd_password_auth_enabled; then
        echo
        warn "SSH password login is currently ENABLED."
        echo "  Disabling password login is recommended, but ONLY if you can log in"
        echo "  with an SSH key. If you disable passwords without a working key,"
        echo "  you can lock yourself out."
        echo
        if [[ "$has_keys" == "yes" ]] && yesno "  Disable SSH password login now?" "n"; then
            confirm_admin_access
            apply_sshd_dropin "yes" "yes" || die "SSH settings were not changed. Check earlier directives or Match rules."
            success "Password login disabled (and root SSH login disabled)."
            echo
            echo "  ${YELLOW}NOTE:${NC} You can still log in as '$ADMIN_USER' using your SSH key."
            echo "        Direct root login via SSH is now denied."
        else
            info "Leaving password login enabled."
        fi
    else
        info "SSH password login already disabled."
        echo
        echo "  ${YELLOW}NOTE:${NC} If you disable root SSH login, you'll need to log in as"
        echo "        '$ADMIN_USER' and use 'sudo' for admin tasks."
        if yesno "  Ensure root SSH login is disabled (recommended)?" "y"; then
            confirm_admin_access
            # Preserve password hardening instead of replacing it with root-only settings.
            apply_sshd_dropin "yes" "yes" || die "SSH settings were not changed. Check earlier directives or Match rules."
            success "Root SSH login disabled."
        fi
    fi

    # Security components
    if [[ -x /usr/bin/fail2ban-client ]]; then
        configure_fail2ban
        success "fail2ban configured (sshd jail with systemd backend)."
    else
        warn "fail2ban not installed (unexpected)."
    fi

    if [[ -x /usr/sbin/ufw ]]; then
        configure_ufw
        echo
        echo "  UFW firewall configured with secure defaults:"
        echo "    - Default: deny incoming, allow outgoing"
        echo "    - Configured and active SSH ports allowed"
        if yesno "  Enable ufw firewall now?" "y"; then
            ufw --force enable >/dev/null
            success "ufw enabled."
        else
            info "ufw configured but left disabled. Enable later with: sudo ufw enable"
        fi
    else
        warn "ufw not installed (unexpected)."
    fi

    enable_unattended
    success "Unattended security updates configured."

    check_security_status

    echo
    echo "================================================================================"
    success "Harden mode complete!"
    echo
    echo "  Test your SSH access now (in another terminal):"
    echo "    ssh -i ~/.ssh/id_ed25519 $ADMIN_USER@<server_ip>"
    echo
    echo "  If you can't connect, check:"
    echo "    - Your provider's firewall/security groups (port 22 must be open)"
    echo "    - Your SSH key matches what's in authorized_keys"
    echo
    echo "  Next steps for Mithril:"
    echo "    1. Configure storage:    sudo ./scripts/disk-setup.sh --setup"
    echo "    2. Apply optimizations:  sudo ./scripts/performance-tune.sh"
    echo "================================================================================"
    echo
}

# ==============================================================================
# MODE: install
# ==============================================================================

mode_install() {
    is_root || die "Run as root (in rescue/live environment)."

    # Install rescue dependencies BEFORE checking them or touching any disk.
    need apt-get
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq debootstrap parted dosfstools e2fsprogs util-linux \
                          grub-efi-amd64 efibootmgr xfsprogs ca-certificates
    need lsblk; need parted; need wipefs; need mkfs.ext4; need mkfs.fat
    need mount; need umount; need debootstrap
    [[ -f /usr/share/debootstrap/scripts/resolute ]] || \
        die "This rescue environment's debootstrap does not support Ubuntu 26.04 (resolute). Use a current Ubuntu rescue image or the provider's Ubuntu installer. No disks have been changed."

    # Check boot mode
    if ! in_uefi; then
        echo
        warn "UEFI mode not detected (/sys/firmware/efi missing)."
        echo
        echo "  This script currently requires UEFI boot mode."
        echo "  Most modern servers and cloud providers use UEFI."
        echo
        echo "  If you're in rescue/live mode:"
        echo "    - Check your provider panel for 'UEFI boot' option"
        echo "    - Reboot rescue in UEFI mode and try again"
        echo
        echo "  If your server only supports legacy BIOS:"
        echo "    - Use Ubuntu's standard installer instead"
        echo "    - Or request BIOS mode support on GitHub"
        echo
        die "UEFI mode required."
    fi

    echo
    echo "================================================================================"
    echo "             INSTALL MODE (DESTRUCTIVE - erases OS disk)"
    echo "================================================================================"
    echo
    echo "  This mode will:"
    echo "    - ERASE the OS disk you select"
    echo "    - Create EFI + fixed-size root partition (OS won't consume whole disk)"
    echo "    - Optionally use remaining OS disk space for data"
    echo "    - Create an admin user with your SSH key"
    echo "    - Configure basic security (fail2ban, ufw, unattended-upgrades)"
    echo "    - Disable SSH password login (key-only access)"
    echo
    echo "  After install:"
    echo "    1. Disable Rescue boot in your provider panel"
    echo "    2. Reboot from disk"
    echo "    3. SSH in with your key"
    echo "    4. Run disk-setup.sh and performance-tune.sh for Mithril"
    echo
    pause

    echo
    echo "  Detected disks:"
    disk_summary
    echo

    # Loop until valid disk is selected
    while true; do
        echo "  Choose ONE disk to install Ubuntu onto (this disk WILL be erased)."
        echo "    Tip: If you have multiple drives, Ubuntu can go on a slower one."
        echo "         Save your fastest NVMe for AccountsDB (configured later in disk-setup.sh)."
        echo "    (Press Ctrl+C to exit)"
        echo ""
        read -r -p "  OS disk (e.g. /dev/nvme0n1): " OS_DISK

        # Validate block device
        if [[ ! -b "$OS_DISK" ]]; then
            echo
            warn "Not a block device: $OS_DISK"
            echo "  Please enter a valid disk from the list above."
            echo
            continue
        fi

        # Safety checks
        if disk_has_mounts "$OS_DISK"; then
            echo
            warn "Disk $OS_DISK has mounted partitions!"
            echo "  Current mounts:"
            lsblk -o NAME,MOUNTPOINT "$OS_DISK" | grep -v '^$' | sed 's/^/    /'
            echo
            warn "Refusing to erase a disk with mounted partitions."
            echo "  Unmount first or choose another disk."
            echo
            continue
        fi

        if disk_contains_root "$OS_DISK"; then
            echo
            warn "Disk $OS_DISK appears to contain the running root filesystem."
            echo "  This shouldn't happen in rescue mode. Choose another disk."
            echo
            continue
        fi

        # Valid disk selected
        break
    done

    choose_admin_user

    echo
    read -r -p "  Hostname for this server [e.g. mithril-node]: " HOSTNAME
    HOSTNAME="${HOSTNAME:-mithril-node}"
    [[ "$HOSTNAME" =~ ^[a-zA-Z0-9][a-zA-Z0-9.-]*$ ]] || die "Invalid hostname."

    echo
    echo "  SSH key for '$ADMIN_USER':"
    echo "    This key allows you to log into the server remotely."
    echo "    After install, you'll SSH in from your personal computer using this key."
    echo
    echo "    On the computer you'll use to access this server, check if you have a key:"
    echo "      cat ~/.ssh/id_ed25519.pub    (or id_rsa.pub)"
    echo
    echo "    If you don't have one, generate a new key:"
    echo "      ssh-keygen -t ed25519"
    echo "      (You can optionally set a passphrase to protect the key)"
    echo "      Then: cat ~/.ssh/id_ed25519.pub"
    echo
    echo "    Supported formats: ssh-ed25519, ssh-rsa, sk-ssh-ed25519 (FIDO2)"
    echo
    prompt SSH_PUBKEY "  Paste SSH public key" ""
    [[ -n "$SSH_PUBKEY" ]] || die "SSH public key is required to avoid lockout."
    validate_ssh_pubkey "$SSH_PUBKEY"

    # Get disk size for validation
    local OS_DISK_SIZE_GIB
    OS_DISK_SIZE_GIB=$(disk_size_gib "$OS_DISK")
    local OS_DISK_SERIAL
    OS_DISK_SERIAL=$(disk_serial "$OS_DISK")

    echo
    echo "  How big should the Ubuntu root partition '/' be?"
    echo "    64 GiB is recommended (plenty for Ubuntu + packages + system logs)"
    echo "    Disk size: ${OS_DISK_SIZE_GIB} GiB available"
    echo "    The rest of the disk can be used for Mithril data later."
    echo ""
    prompt ROOT_SIZE_GIB "  Root (/) size in GiB [press Enter for recommended]" "64"
    [[ "$ROOT_SIZE_GIB" =~ ^[0-9]+$ ]] || die "Root size must be a number (GiB)."

    # Validate root size fits on disk (need ~2 GiB for EFI + some headroom)
    local MAX_ROOT_GIB=$((OS_DISK_SIZE_GIB - 2))
    if [[ "$ROOT_SIZE_GIB" -gt "$MAX_ROOT_GIB" ]]; then
        die "Root size ${ROOT_SIZE_GIB} GiB exceeds available disk space (~${MAX_ROOT_GIB} GiB usable on ${OS_DISK_SIZE_GIB} GiB disk)."
    fi

    # Minimum root size guard (prevent accidental tiny roots)
    if [[ "$ROOT_SIZE_GIB" -lt 30 ]]; then
        warn "Root partition under 30 GiB may be too small for OS + packages + logs."
        if ! yesno "  Continue with ${ROOT_SIZE_GIB} GiB root?" "n"; then
            die "Aborted. Re-run and choose a larger root size (50-80 GiB recommended)."
        fi
    fi

    # Swap file option (OOM prevention safety net)
    local CREATE_SWAP="no"
    local SWAP_SIZE_GIB="4"

    # Detect system RAM and calculate recommended swap
    local SYSTEM_RAM_GIB
    SYSTEM_RAM_GIB=$(awk '/MemTotal/ {printf "%.0f", $2/1024/1024}' /proc/meminfo 2>/dev/null || echo "16")

    # Swap recommendation: ~25% of RAM for systems with 16GB+, minimum 4GB
    local RECOMMENDED_SWAP
    if [[ "$SYSTEM_RAM_GIB" -ge 32 ]]; then
        RECOMMENDED_SWAP="8"
    elif [[ "$SYSTEM_RAM_GIB" -ge 16 ]]; then
        RECOMMENDED_SWAP="4"
    else
        # For smaller RAM systems, use ~50% of RAM
        RECOMMENDED_SWAP=$(( SYSTEM_RAM_GIB / 2 ))
        [[ "$RECOMMENDED_SWAP" -lt 2 ]] && RECOMMENDED_SWAP="2"
    fi
    SWAP_SIZE_GIB="$RECOMMENDED_SWAP"

    echo
    echo "  Create a swap file? (Recommended - prevents OOM crashes during memory spikes)"
    echo "    Detected RAM: ${SYSTEM_RAM_GIB} GiB"
    echo "    Swap provides a safety net when RAM is exhausted."
    echo "    Recommended: ${RECOMMENDED_SWAP} GiB for your system"
    echo "      (More RAM = less swap needed; less RAM = more swap helps)"
    echo ""
    if yesno "  Create swap file?" "y"; then
        CREATE_SWAP="yes"
        prompt SWAP_SIZE_GIB "  Swap size in GiB [press Enter for ${RECOMMENDED_SWAP} GiB]" "$RECOMMENDED_SWAP"
        [[ "$SWAP_SIZE_GIB" =~ ^[0-9]+$ ]] || die "Swap size must be a number (GiB)."
    fi

    # UFW firewall option
    echo
    echo "  Enable UFW firewall? (Recommended)"
    echo "    - Blocks unsolicited incoming connections"
    echo "    - Allows SSH (port 22) incoming"
    echo "    - Allows ALL outgoing connections (RPC, Lightbringer, etc.)"
    echo ""
    local ENABLE_UFW="yes"
    if yesno "  Enable UFW firewall?" "y"; then
        ENABLE_UFW="yes"
    else
        ENABLE_UFW="no"
        warn "UFW will be installed but not enabled. Enable later with: sudo ufw enable"
    fi

    # Root SSH access option (advanced)
    local ROOT_SSH_MODE="no"
    echo
    echo "  Root SSH access (advanced):"
    echo "    Most users should select 'N' (the default)."
    echo "    Selecting 'Y' allows root login with SSH key (break-glass access)."
    echo ""
    if yesno "  Allow root SSH login?" "n"; then
        ROOT_SSH_MODE="prohibit-password"
        warn "Root SSH will be allowed with key authentication only."
        echo "  You'll need to add your SSH key to /root/.ssh/authorized_keys after install."
    fi

    # Storage note - disk-setup.sh handles all Mithril storage configuration
    local MAKE_OSDATA="no"
    local OSDATA_FS=""
    local OSDATA_MP=""

    # Network interface detection + MAC address for reliable naming
    IFACE="$(ip route 2>/dev/null | awk '/default/ {print $5; exit}' || true)"
    IFACE="${IFACE:-eth0}"
    IFACE_MAC="$(get_mac "$IFACE")"

    echo
    echo "  Network configuration:"
    echo "    Detected interface: $IFACE"
    [[ -n "$IFACE_MAC" ]] && echo "    MAC address: $IFACE_MAC"
    echo
    echo "    DHCP = automatic network settings (works for most server providers)"
    echo "    Static = manually specify IP addresses (advanced)"
    echo
    echo "    Most users: Press Enter to use DHCP."
    echo
    echo "    1) DHCP (automatic - recommended)"
    echo "    2) Static IP (advanced)"
    read -r -p "  Choose [press Enter for DHCP]: " NET_MODE
    NET_MODE="${NET_MODE:-1}"

    local IP4_CIDR="" GW4="" IP6_CIDR="" GW6="" DHCP6="yes"
    # IPv6 is always enabled for DHCP (harmless if unsupported by provider)

    if [[ "$NET_MODE" == "2" ]]; then
        local DEF_IP4 DEF_GW4 DEF_IP6 DEF_GW6
        DEF_IP4="$(ip -4 -o addr show dev "$IFACE" 2>/dev/null | awk '{print $4}' | head -n1 || true)"
        DEF_GW4="$(ip route 2>/dev/null | awk '/default/ {print $3; exit}' || true)"
        DEF_IP6="$(ip -6 -o addr show dev "$IFACE" scope global 2>/dev/null | awk '{print $4}' | head -n1 || true)"
        DEF_GW6="$(ip -6 route 2>/dev/null | awk '/default/ {print $3; exit}' || true)"

        echo
        echo "  Detected from rescue environment:"
        echo "    IPv4: $DEF_IP4  Gateway: $DEF_GW4"
        echo "    IPv6: $DEF_IP6  Gateway: $DEF_GW6"
        echo
        prompt IP4_CIDR "  IPv4 CIDR" "$DEF_IP4"
        prompt GW4      "  IPv4 gateway" "$DEF_GW4"
        prompt IP6_CIDR "  IPv6 CIDR (blank to skip)" "$DEF_IP6"
        prompt GW6      "  IPv6 gateway (blank to skip)" "$DEF_GW6"
        [[ -n "$IP4_CIDR" && -n "$GW4" ]] || die "Static IPv4 requires IP CIDR + gateway."
        echo
        echo "  DNS servers (comma-separated):"
        echo "    Default: Cloudflare + Google (1.1.1.1, 8.8.8.8, etc.)"
        prompt DNS_SERVERS "  DNS servers" "1.1.1.1, 8.8.8.8, 2606:4700:4700::1111, 2001:4860:4860::8888"
    fi

    echo
    echo "  ┌─────────────────────────────────────────────────────────────────────────┐"
    echo "  │ INSTALL SUMMARY                                                         │"
    echo "  ├─────────────────────────────────────────────────────────────────────────┤"
    echo "  │ OS disk to ERASE: $OS_DISK (${OS_DISK_SIZE_GIB} GiB)"
    [[ -n "$OS_DISK_SERIAL" && "$OS_DISK_SERIAL" != "unknown" ]] && \
    echo "  │ Disk serial:      $OS_DISK_SERIAL"
    echo "  │ Root (/) size:    ${ROOT_SIZE_GIB} GiB"
    [[ "$CREATE_SWAP" == "yes" ]] && \
    echo "  │ Swap file:        ${SWAP_SIZE_GIB} GiB"
    [[ "$MAKE_OSDATA" == "yes" ]] && \
    echo "  │ Data partition:   remaining space -> $OSDATA_MP ($OSDATA_FS)"
    echo "  │ Hostname:         $HOSTNAME"
    echo "  │ Admin user:       $ADMIN_USER"
    echo "  │ Network:          $([ "$NET_MODE" == "1" ] && echo "DHCP" || echo "Static")"
    [[ -n "$IFACE_MAC" ]] && \
    echo "  │ Interface match:  MAC $IFACE_MAC"
    echo "  │ UFW firewall:     $ENABLE_UFW"
    echo "  │ Root SSH login:   $([ "$ROOT_SSH_MODE" == "no" ] && echo "disabled" || echo "key-only")"
    echo "  └─────────────────────────────────────────────────────────────────────────┘"
    echo

    # Include serial in confirmation if available for extra safety
    local CONFIRM_TEXT="INSTALL ERASE $OS_DISK"
    [[ -n "$OS_DISK_SERIAL" && "$OS_DISK_SERIAL" != "unknown" ]] && \
        CONFIRM_TEXT="INSTALL ERASE $OS_DISK ($OS_DISK_SERIAL)"
    confirm_phrase "$CONFIRM_TEXT"

    # Partition OS disk
    info "Partitioning OS disk..."
    wipefs -a "$OS_DISK"
    parted -s "$OS_DISK" mklabel gpt
    parted -s "$OS_DISK" mkpart ESP fat32 1MiB 1025MiB
    parted -s "$OS_DISK" set 1 esp on

    # Root partition
    ROOT_END_GIB="$((ROOT_SIZE_GIB + 1))"
    parted -s "$OS_DISK" mkpart ROOT ext4 1025MiB "${ROOT_END_GIB}GiB"

    if [[ "$MAKE_OSDATA" == "yes" ]]; then
        parted -s "$OS_DISK" mkpart DATA "${ROOT_END_GIB}GiB" 100%
    fi

    EFI_PART="$(part_path "$OS_DISK" 1)"
    ROOT_PART="$(part_path "$OS_DISK" 2)"

    info "Formatting partitions..."
    mkfs.fat -F32 "$EFI_PART"
    mkfs.ext4 -F -L rootfs "$ROOT_PART"

    local OSDATA_PART=""
    if [[ "$MAKE_OSDATA" == "yes" ]]; then
        OSDATA_PART="$(part_path "$OS_DISK" 3)"
        case "$OSDATA_FS" in
            ext4) mkfs.ext4 -F -L osdata "$OSDATA_PART" ;;
            xfs)  mkfs.xfs -f -L osdata "$OSDATA_PART" ;;
        esac
    fi

    # Mount and install
    info "Mounting target filesystem..."
    mount "$ROOT_PART" /mnt
    mkdir -p /mnt/boot/efi
    mount "$EFI_PART" /mnt/boot/efi

    if [[ "$MAKE_OSDATA" == "yes" ]]; then
        mkdir -p "/mnt$OSDATA_MP"
        mount "$OSDATA_PART" "/mnt$OSDATA_MP"
    fi

    info "Installing Ubuntu 26.04 (resolute) via debootstrap..."
    debootstrap --arch amd64 --include=ca-certificates resolute /mnt https://archive.ubuntu.com/ubuntu/

    mount --bind /dev  /mnt/dev
    mount --bind /dev/pts /mnt/dev/pts
    mount --bind /proc /mnt/proc
    mount --bind /sys  /mnt/sys

    # Build netplan config with MAC address matching (most robust)
    local NETPLAN_CONFIG=""
    if [[ "$NET_MODE" == "1" ]]; then
        # DHCP mode with MAC matching
        if [[ -n "$IFACE_MAC" ]]; then
            NETPLAN_CONFIG="network:
  version: 2
  renderer: networkd
  ethernets:
    mainif:
      match:
        macaddress: $IFACE_MAC
      set-name: eth0
      dhcp4: true
      dhcp6: $DHCP6"
        else
            NETPLAN_CONFIG="network:
  version: 2
  renderer: networkd
  ethernets:
    $IFACE:
      dhcp4: true
      dhcp6: $DHCP6"
        fi
    else
        # Static mode with MAC matching
        if [[ -n "$IFACE_MAC" ]]; then
            NETPLAN_CONFIG="network:
  version: 2
  renderer: networkd
  ethernets:
    mainif:
      match:
        macaddress: $IFACE_MAC
      set-name: eth0
      addresses:
        - ${IP4_CIDR}"
        else
            NETPLAN_CONFIG="network:
  version: 2
  renderer: networkd
  ethernets:
    $IFACE:
      addresses:
        - ${IP4_CIDR}"
        fi

        if [[ -n "${IP6_CIDR:-}" ]]; then
            NETPLAN_CONFIG+="
        - ${IP6_CIDR}"
        fi

        NETPLAN_CONFIG+="
      routes:
        - to: default
          via: ${GW4}"

        if [[ -n "${GW6:-}" && -n "${IP6_CIDR:-}" ]]; then
            NETPLAN_CONFIG+="
        - to: default
          via: ${GW6}"
        fi

        # Format DNS servers for netplan (remove spaces after commas)
        local DNS_LIST
        DNS_LIST=$(echo "$DNS_SERVERS" | tr -d ' ')
        NETPLAN_CONFIG+="
      nameservers:
        addresses: [${DNS_LIST}]"
    fi

    # Pass the public key as data, including any shell metacharacters in its comment.
    printf '%s\n' "$SSH_PUBKEY" > /mnt/tmp/mithril-admin.pub
    chmod 0600 /mnt/tmp/mithril-admin.pub

    # Configure installed system
    info "Configuring installed Ubuntu system..."
    chroot /mnt /bin/bash -euxo pipefail <<CHROOT
export DEBIAN_FRONTEND=noninteractive

# Add universe repository (needed for fail2ban and other packages)
# debootstrap only sets up main by default
cat > /etc/apt/sources.list <<'SOURCES'
deb https://archive.ubuntu.com/ubuntu resolute main restricted universe
deb https://archive.ubuntu.com/ubuntu resolute-updates main restricted universe
deb https://security.ubuntu.com/ubuntu resolute-security main restricted universe
SOURCES

apt-get update -qq
apt-get install -y -qq linux-generic grub-efi-amd64 openssh-server sudo \
                   fail2ban ufw unattended-upgrades netplan.io xfsprogs \
                   chrony git curl wget ca-certificates build-essential python3 \
                   htop iotop nload vim nano tmux

# Time synchronization (critical for blockchain nodes)
systemctl enable chrony >/dev/null 2>&1 || true

# Journald limits (prevent logs from filling disk)
mkdir -p /etc/systemd/journald.conf.d
cat > /etc/systemd/journald.conf.d/mithril.conf <<'EOF'
# Managed by server-setup.sh
# Limit journal storage to prevent disk fill
[Journal]
SystemMaxUse=2G
SystemKeepFree=1G
MaxFileSec=1week
EOF

# Hostname
echo "$HOSTNAME" > /etc/hostname
cat > /etc/hosts <<EOF
127.0.0.1   localhost
127.0.1.1   $HOSTNAME

# IPv6
::1         localhost ip6-localhost ip6-loopback
ff02::1     ip6-allnodes
ff02::2     ip6-allrouters
EOF

# Admin user + key
id -u "$ADMIN_USER" >/dev/null 2>&1 || adduser --disabled-password --gecos "" "$ADMIN_USER"
usermod -aG sudo "$ADMIN_USER" || true

# Passwordless sudo (no password set, so sudo group alone won't work)
echo "$ADMIN_USER ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/$ADMIN_USER
chmod 440 /etc/sudoers.d/$ADMIN_USER
visudo -cf /etc/sudoers.d/$ADMIN_USER

home="\$(getent passwd "$ADMIN_USER" | cut -d: -f6)"
install -d -m 0700 -o "$ADMIN_USER" -g "$ADMIN_USER" "\$home/.ssh"
touch "\$home/.ssh/authorized_keys"
chown "$ADMIN_USER:$ADMIN_USER" "\$home/.ssh/authorized_keys"
chmod 0600 "\$home/.ssh/authorized_keys"
pubkey="\$(cat /tmp/mithril-admin.pub)"
grep -qxF "\$pubkey" "\$home/.ssh/authorized_keys" || printf '%s\n' "\$pubkey" >> "\$home/.ssh/authorized_keys"
rm /tmp/mithril-admin.pub

# Netplan (with MAC address matching for reliable interface naming)
cat > /etc/netplan/01-netcfg.yaml <<'NETPLANEOF'
$NETPLAN_CONFIG
NETPLANEOF
chmod 0600 /etc/netplan/01-netcfg.yaml

# SSH hardening (safe - we just installed the key)
install -d -m 0755 /etc/ssh/sshd_config.d
cat > /etc/ssh/sshd_config.d/00-mithril-hardening.conf <<EOF
# Managed by server-setup.sh
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin $ROOT_SSH_MODE
EOF
mkdir -p /run/sshd
sshd -t
ssh_effective="\$(sshd -T)"
grep -qx 'passwordauthentication no' <<< "\$ssh_effective"
grep -qx 'kbdinteractiveauthentication no' <<< "\$ssh_effective"
# OpenSSH normalizes prohibit-password to the older without-password spelling.
grep -qxE 'permitrootlogin $( [[ "$ROOT_SSH_MODE" == no ]] && echo no || echo '(prohibit-password|without-password)' )' <<< "\$ssh_effective"

# fail2ban with systemd backend
install -d -m 0755 /etc/fail2ban/jail.d
cat > /etc/fail2ban/jail.d/sshd.local <<'EOF'
[sshd]
enabled = true
backend = systemd
bantime = 1h
findtime = 10m
maxretry = 5
EOF
systemctl enable fail2ban >/dev/null 2>&1 || true

# ufw with secure defaults
ufw default deny incoming >/dev/null 2>&1 || true
ufw default allow outgoing >/dev/null 2>&1 || true
ufw allow OpenSSH >/dev/null 2>&1 || ufw allow 22/tcp >/dev/null 2>&1 || true
if [[ "$ENABLE_UFW" == "yes" ]]; then
    ufw --force enable >/dev/null 2>&1 || true
fi

# unattended upgrades
dpkg-reconfigure -f noninteractive unattended-upgrades >/dev/null 2>&1 || true
systemctl enable unattended-upgrades >/dev/null 2>&1 || true

# fstab
ROOT_UUID=\$(blkid -s UUID -o value "$ROOT_PART")
EFI_UUID=\$(blkid -s UUID -o value "$EFI_PART")
cat > /etc/fstab <<EOF
UUID=\$ROOT_UUID  /          ext4  defaults,errors=remount-ro  0 1
UUID=\$EFI_UUID   /boot/efi  vfat  umask=0077                  0 1
EOF

# Swap file (if requested)
if [[ "$CREATE_SWAP" == "yes" ]]; then
    fallocate -l ${SWAP_SIZE_GIB}G /swapfile || dd if=/dev/zero of=/swapfile bs=1G count=${SWAP_SIZE_GIB}
    chmod 600 /swapfile
    mkswap /swapfile
    echo "/swapfile  none  swap  sw  0  0" >> /etc/fstab
fi
CHROOT

    # Append data mount to fstab (with noatime for better performance)
    if [[ "$MAKE_OSDATA" == "yes" ]]; then
        local uuid
        uuid="$(blkid -s UUID -o value "$OSDATA_PART")"
        echo "UUID=$uuid  $OSDATA_MP  $OSDATA_FS  defaults,noatime,nofail  0  2" >> /mnt/etc/fstab
    fi

    # Install GRUB
    info "Installing GRUB bootloader..."
    chroot /mnt /bin/bash -euxo pipefail <<CHROOT2
grub-install --target=x86_64-efi --efi-directory=/boot/efi --bootloader-id=ubuntu --recheck
update-grub
CHROOT2

    info "Unmounting..."
    sync
    if ! umount -R /mnt; then
        warn "Failed to cleanly unmount /mnt. Checking for busy processes..."
        lsof +D /mnt 2>/dev/null | head -20 || true
        die "Could not unmount /mnt. Processes may still be using it. Try: fuser -vm /mnt"
    fi
    success "Filesystems unmounted cleanly."

    echo
    echo "================================================================================"
    success "INSTALL COMPLETE!"
    echo
    echo "  ┌─────────────────────────────────────────────────────────────────────────┐"
    echo "  │ CONFIGURATION SUMMARY                                                   │"
    echo "  ├─────────────────────────────────────────────────────────────────────────┤"
    echo "  │ [✓] Ubuntu 26.04 LTS (resolute) installed"
    echo "  │ [✓] Hostname: $HOSTNAME"
    echo "  │ [✓] Admin user: $ADMIN_USER (with sudo access)"
    echo "  │ [✓] SSH key installed for $ADMIN_USER"
    echo "  │ [✓] SSH password login: disabled"
    echo "  │ [✓] SSH root login: $([ "$ROOT_SSH_MODE" == "no" ] && echo "disabled" || echo "key-only (prohibit-password)")"
    echo "  │ [✓] fail2ban: enabled (sshd jail)"
    echo "  │ [✓] UFW firewall: $([ "$ENABLE_UFW" == "yes" ] && echo "enabled (SSH allowed)" || echo "configured but NOT enabled")"
    echo "  │ [✓] Unattended security updates: enabled"
    echo "  │ [✓] Time sync (chrony): enabled"
    echo "  │ [✓] Journald limits: 2GB max"
    echo "  │ [✓] Root partition: ${ROOT_SIZE_GIB} GiB (errors=remount-ro)"
    [[ "$CREATE_SWAP" == "yes" ]] && \
    echo "  │ [✓] Swap file: ${SWAP_SIZE_GIB} GiB"
    [[ "$MAKE_OSDATA" == "yes" ]] && \
    echo "  │ [✓] Data partition: $OSDATA_MP ($OSDATA_FS)"
    echo "  │ [✓] Network: $([ "$NET_MODE" == "1" ] && echo "DHCP" || echo "Static") via MAC match"
    echo "  └─────────────────────────────────────────────────────────────────────────┘"
    echo
    echo "  ┌─────────────────────────────────────────────────────────────────────────┐"
    echo "  │ DISK LAYOUT                                                             │"
    echo "  └─────────────────────────────────────────────────────────────────────────┘"
    lsblk -o NAME,SIZE,TYPE,MOUNTPOINT "$OS_DISK" 2>/dev/null | sed 's/^/  /'
    echo
    echo "  ┌─────────────────────────────────────────────────────────────────────────┐"
    echo "  │ SSH KEY INSTALLED                                                       │"
    echo "  │   /home/$ADMIN_USER/.ssh/authorized_keys                                │"
    echo "  └─────────────────────────────────────────────────────────────────────────┘"
    echo
    echo "  Installed key (verify this matches yours):"
    echo -e "  ${GREEN}$(cat /mnt/home/$ADMIN_USER/.ssh/authorized_keys | head -1 | cut -c1-72)...${NC}"
    echo
    echo "  NEXT STEPS:"
    echo "    1. Verify the SSH key above matches yours"
    echo "       (To re-check: mount $ROOT_PART /mnt && cat /mnt/home/$ADMIN_USER/.ssh/authorized_keys)"
    echo
    echo "    2. Reboot into Ubuntu:"
    echo
    echo -e "       ${GREEN}reboot${NC}"
    echo
    echo "       (Hetzner rescue is one-boot-only. Other providers: disable rescue mode first.)"
    echo
    echo "    3. Clear old SSH host key (if you SSHed to rescue), then connect:"
    echo
    echo "       ssh-keygen -R YOUR_SERVER_IP"
    echo "       ssh -i ~/.ssh/YOUR_KEY $ADMIN_USER@YOUR_SERVER_IP"
    echo
    echo "    4. Clone repo and run remaining setup:"
    echo
    echo "       git clone https://github.com/Overclock-Validator/mithril.git"
    echo "       cd mithril"
    echo "       chmod +x scripts/*.sh"
    echo "       sudo ./scripts/disk-setup.sh --setup"
    echo "       sudo ./scripts/performance-tune.sh"
    echo
    echo -e "  ${YELLOW}Can't connect after reboot?${NC}"
    echo "    - Check provider firewall/security groups (port 22)"
    echo "    - Check provider console to verify server booted"
    echo "    - Use provider's serial/VNC console to debug boot issues"
    echo "================================================================================"
    echo
}

# ==============================================================================
# Main
# ==============================================================================

show_help() {
    cat <<'EOF'
server-setup.sh - Ubuntu server setup for Mithril

Usage:
  sudo ./server-setup.sh install   # Fresh Ubuntu 26.04 install (ERASES OS disk!)
  sudo ./server-setup.sh harden    # Safe: user + SSH key + security packages
  ./server-setup.sh status         # Show current security status

This script handles OS installation and security hardening.

For storage and performance tuning (run AFTER this script):
  sudo ./scripts/disk-setup.sh --setup       # Configure storage (offers benchmarks)
  sudo ./scripts/performance-tune.sh         # Kernel/IO optimizations
EOF
}

main() {
    local mode="${1:-}"
    case "$mode" in
        install) mode_install ;;
        harden)  mode_harden ;;
        status)  mode_status ;;
        --help|-h|help) show_help ;;
        "")      show_help; return 2 ;;
        *)       die "Unknown mode: $mode (use install|harden|status)" ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
