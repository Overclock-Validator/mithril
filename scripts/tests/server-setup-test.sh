#!/usr/bin/env bash
set -euo pipefail

# Exercise lockout prevention/rollback without modifying the host's SSH config.
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
source "$repo/scripts/server-setup.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/dropins"
config_dir="$work/dropins"
config="$work/sshd_config"
touch "$config"
ADMIN_USER=ubuntu
MOCK_EFFECTIVE='passwordauthentication no
kbdinteractiveauthentication no
permitrootlogin no'
MOCK_SYNTAX=0
MOCK_RELOAD=0
sshd() {
    case " $* " in
        *' -t '*) return "$MOCK_SYNTAX" ;;
        *' -T '*) printf '%s\n' "$MOCK_EFFECTIVE" ;;
        *) return 1 ;;
    esac
}
systemctl() { return "$MOCK_RELOAD"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
apply_sshd_dropin yes yes "$config_dir" "$config" || fail "valid config rejected"
grep -qx 'PasswordAuthentication no' "$config_dir/00-mithril-hardening.conf" || fail "password setting absent"
cp "$config_dir/00-mithril-hardening.conf" "$work/expected"

# A directive before Include (or a Match override) must cause rollback.
MOCK_EFFECTIVE='passwordauthentication yes
kbdinteractiveauthentication no
permitrootlogin no'
if apply_sshd_dropin yes yes "$config_dir" "$config"; then fail "ineffective config accepted"; fi
cmp "$work/expected" "$config_dir/00-mithril-hardening.conf" || fail "existing config lost"

MOCK_SYNTAX=1
if apply_sshd_dropin yes yes "$config_dir" "$config"; then fail "invalid syntax accepted"; fi
cmp "$work/expected" "$config_dir/00-mithril-hardening.conf" || fail "syntax rollback failed"
MOCK_SYNTAX=0
MOCK_EFFECTIVE='passwordauthentication no
kbdinteractiveauthentication no
permitrootlogin no'
MOCK_RELOAD=1
if apply_sshd_dropin yes yes "$config_dir" "$config"; then fail "failed reload accepted"; fi
cmp "$work/expected" "$config_dir/00-mithril-hardening.conf" || fail "reload rollback failed"
rm "$config_dir/00-mithril-hardening.conf"
if apply_sshd_dropin yes yes "$config_dir" "$config"; then fail "failed first install accepted"; fi
[[ ! -e "$config_dir/00-mithril-hardening.conf" ]] || fail "failed first install left drop-in"

# Check actual OpenSSH Include precedence when sshd is available (Ubuntu CI/server).
if [[ -x /usr/sbin/sshd ]]; then
    unset -f sshd
    PATH="/usr/sbin:$PATH"
    MOCK_RELOAD=0
    ssh-keygen -q -t ed25519 -N '' -f "$work/host_key"
    printf 'Include %s/*.conf\nHostKey %s\n' "$config_dir" "$work/host_key" > "$config"
    printf 'PasswordAuthentication yes\nPermitRootLogin yes\n' > "$config_dir/50-cloud-init.conf"
    apply_sshd_dropin yes yes "$config_dir" "$config" || fail "OpenSSH precedence hardening failed"
    cp "$config_dir/00-mithril-hardening.conf" "$work/expected"
    printf 'PasswordAuthentication yes\nInclude %s/*.conf\nHostKey %s\n' "$config_dir" "$work/host_key" > "$config"
    if apply_sshd_dropin yes yes "$config_dir" "$config"; then fail "pre-Include override accepted"; fi
    cmp "$work/expected" "$config_dir/00-mithril-hardening.conf" || fail "real OpenSSH rollback failed"
fi

# Root disks behind LVM/RAID must be protected, not treated as data disks.
findmnt() { echo /dev/mapper/root; }
lsblk() { printf '%s\n' /dev/mapper/root /dev/nvme0n1p2 /dev/nvme0n1; }
disk_contains_root /dev/nvme0n1 || fail "LVM root disk not protected"
if disk_contains_root /dev/nvme1n1; then fail "unrelated disk treated as root"; fi

source "$repo/scripts/disk-setup.sh"
lsblk() { printf '%s\n' '/dev/mapper/root lvm' '/dev/nvme0n1p2 part' '/dev/nvme0n1 disk'; }
[[ "$(get_root_disk)" == /dev/nvme0n1 ]] || fail "LVM root disk resolution failed"
lsblk() { printf '%s\n' '/dev/md0 raid1' '/dev/nvme0n1 disk' '/dev/nvme1n1 disk'; }
if (get_root_disk) >/dev/null 2>&1; then fail "RAID root accepted as a single disk"; fi
echo "Setup script regressions passed"
