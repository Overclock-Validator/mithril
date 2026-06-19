package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const gib = 1 << 30

// accountsDbBytes is the extracted AccountsDB size estimate by cluster.
// Generous to fail fast before the disk fills mid-build.
func accountsDbBytes(cluster string) uint64 {
	switch normCluster(cluster) {
	case "mainnet-beta", "mainnet":
		return 450 * gib // extracted AccountsDB
	case "testnet":
		return 100 * gib
	default: // devnet / unknown — small AccountsDB
		return 40 * gib
	}
}

func snapshotBytes(cluster string) uint64 {
	switch normCluster(cluster) {
	case "mainnet-beta", "mainnet":
		return 150 * gib // compressed full+incremental snapshot saved while streaming
	case "testnet":
		return 30 * gib
	default:
		return 15 * gib
	}
}

func normCluster(cluster string) string { return strings.ToLower(strings.TrimSpace(cluster)) }

// EstimatedAccountsDbBytes is the free space the AccountsDB needs.
func EstimatedAccountsDbBytes(cluster string) uint64 { return accountsDbBytes(cluster) }

// EstimatedSnapshotBytes is the free space the compressed snapshot needs.
func EstimatedSnapshotBytes(cluster string) uint64 { return snapshotBytes(cluster) }

// EstimatedBuildBytes is the peak free space a build needs when AccountsDB and
// snapshot share one disk (the common case, always true for safe-folders).
func EstimatedBuildBytes(cluster string) uint64 {
	return accountsDbBytes(cluster) + snapshotBytes(cluster)
}

// BuildSpaceCheck is the reclaim-aware verdict on whether a snapshot rebuild
// has room. Shared by the run-path fail-fast and the TUI confirm cards.
type BuildSpaceCheck struct {
	Determined bool   // false when free space can't be read (unsupported OS / stat error)
	OK         bool   // disks have room (only meaningful when Determined)
	SameDisk   bool   // accounts and snapshot share one filesystem
	UsableGB   uint64 // usable space on the accounts disk in GB (free + reclaimable old DB)
	NeedGB     uint64 // space needed on the accounts disk in GB
	Reason     string // shortfall message when !OK (empty otherwise)
}

// CheckBuildSpace reports whether a snapshot rebuild fits. The existing AccountsDB
// is reclaimable; a shared disk must hold both AccountsDB and snapshot at peak.
func CheckBuildSpace(cluster, accountsPath, snapshotDir string) BuildSpaceCheck {
	accFree, ok := FreeDiskBytes(accountsPath)
	if !ok {
		return BuildSpaceCheck{Determined: false}
	}
	accUsable := accFree
	if HasExistingAccountsDb(accountsPath) {
		accUsable += ReclaimableDirBytes(accountsPath)
	}
	accNeed := EstimatedAccountsDbBytes(cluster)
	snapNeed := EstimatedSnapshotBytes(cluster)

	same := snapshotDir != "" && SameDisk(accountsPath, snapshotDir)
	if same {
		total := accNeed + snapNeed
		c := BuildSpaceCheck{Determined: true, SameDisk: true, UsableGB: accUsable / gib, NeedGB: total / gib, OK: accUsable >= total}
		if !c.OK {
			c.Reason = fmt.Sprintf("not enough disk space: %s has ~%d GB usable but a %s build needs ~%d GB (AccountsDB + snapshot share this disk); free space or point storage at a larger disk",
				accountsPath, accUsable/gib, cluster, total/gib)
		}
		return c
	}

	c := BuildSpaceCheck{Determined: true, UsableGB: accUsable / gib, NeedGB: accNeed / gib, OK: accUsable >= accNeed}
	if !c.OK {
		c.Reason = fmt.Sprintf("not enough disk space: %s has ~%d GB usable but the %s AccountsDB needs ~%d GB; free space or point storage at a larger disk",
			accountsPath, accUsable/gib, cluster, accNeed/gib)
		return c
	}
	// Snapshot is on a separate disk — check it too.
	if snapshotDir != "" {
		if snapFree, sok := FreeDiskBytes(snapshotDir); sok && snapFree < snapNeed {
			c.OK = false
			c.Reason = fmt.Sprintf("not enough disk space for the snapshot: %s has ~%d GB free but ~%d GB is needed; free space or point snapshots at a larger disk",
				snapshotDir, snapFree/gib, snapNeed/gib)
		}
	}
	return c
}

// nearestExistingAncestor walks up to the first existing directory, so
// stat/statfs work even when path hasn't been created yet.
func nearestExistingAncestor(path string) string {
	p := strings.TrimSpace(path)
	for p != "" {
		if _, err := os.Stat(p); err == nil {
			return p
		}
		parent := filepath.Dir(p)
		if parent == p {
			return p
		}
		p = parent
	}
	return p
}

// FreeDiskBytes returns free bytes on the filesystem that would hold path. ok
// is false when undeterminable (unsupported platform or stat error).
func FreeDiskBytes(path string) (uint64, bool) {
	if strings.TrimSpace(path) == "" {
		return 0, false
	}
	return freeBytesForFS(nearestExistingAncestor(path))
}

// SameDisk reports whether a and b are on the same filesystem. Returns true when
// undeterminable (the conservative answer that never under-checks free space).
func SameDisk(a, b string) bool {
	da, oka := deviceID(nearestExistingAncestor(a))
	db, okb := deviceID(nearestExistingAncestor(b))
	if !oka || !okb {
		return true
	}
	return da == db
}

// WritableDir reports whether dir (or its nearest existing ancestor) is a
// writable directory, via access(2) (no create-probe; safe on the render path).
func WritableDir(dir string) bool {
	if strings.TrimSpace(dir) == "" {
		return false
	}
	p := nearestExistingAncestor(dir)
	info, err := os.Stat(p)
	return err == nil && info.IsDir() && writableFS(p)
}

// HasExistingAccountsDb reports whether dir already holds an AccountsDB, by
// checking for the manifest file every build writes.
func HasExistingAccountsDb(dir string) bool {
	if strings.TrimSpace(dir) == "" {
		return false
	}
	fi, err := os.Stat(filepath.Join(dir, "manifest"))
	return err == nil && fi.Size() > 0
}

// ReclaimableDirBytes sums regular-file bytes under dir (0 if missing); these
// bytes add to free space when judging whether a rebuild fits in place.
func ReclaimableDirBytes(dir string) uint64 {
	if strings.TrimSpace(dir) == "" {
		return 0
	}
	var total uint64
	_ = filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			if info, ierr := d.Info(); ierr == nil && info.Mode().IsRegular() {
				total += uint64(info.Size())
			}
		}
		return nil
	})
	return total
}
