package config

import (
	"os"
	"path/filepath"
)

// StoragePaths holds the four Mithril storage path defaults.
type StoragePaths struct {
	Accounts   string
	Snapshots  string
	Logs       string
	Shredstore string
}

// productionStoragePaths is the /mnt/mithril-* layout from scripts/disk-setup.sh.
func productionStoragePaths() StoragePaths {
	return StoragePaths{
		Accounts:   "/mnt/mithril-accounts",
		Snapshots:  "/mnt/mithril-ledger/snapshots",
		Logs:       "/mnt/mithril-logs",
		Shredstore: "/mnt/mithril-ledger/shredstore",
	}
}

// DefaultStoragePaths picks each /mnt/mithril-* path that's creatable, else the
// ~/.mithril/* fallback for that folder (production dirs live on different mounts).
func DefaultStoragePaths() StoragePaths {
	prod := productionStoragePaths()
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		home = "."
	}
	base := filepath.Join(home, ".mithril")

	pick := func(prodPath, name string) string {
		if isWritable(prodPath) {
			return prodPath
		}
		return filepath.Join(base, name)
	}
	return StoragePaths{
		Accounts:   pick(prod.Accounts, "accounts"),
		Snapshots:  pick(prod.Snapshots, "snapshots"),
		Logs:       pick(prod.Logs, "logs"),
		Shredstore: pick(prod.Shredstore, "shredstore"),
	}
}

// IsProductionLayout reports whether all four paths match productionStoragePaths().
// Returns false for mixed sets so user-edited configs are not mislabeled.
func IsProductionLayout(p StoragePaths) bool {
	prod := productionStoragePaths()
	return p == prod
}

// isWritable reports whether path (or its nearest existing parent) is writable.
func isWritable(path string) bool {
	p := path
	for {
		info, err := os.Stat(p)
		if err == nil {
			if !info.IsDir() {
				return false
			}
			tmp, err := os.CreateTemp(p, ".mithril-writecheck.*")
			if err != nil {
				return false
			}
			tmpName := tmp.Name()
			_ = tmp.Close()
			_ = os.Remove(tmpName)
			return true
		}
		parent := filepath.Dir(p)
		if parent == p {
			return false // reached filesystem root
		}
		p = parent
	}
}
