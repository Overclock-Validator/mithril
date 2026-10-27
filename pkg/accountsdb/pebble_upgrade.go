package accountsdb

import (
	"fmt"
	"os"

	pebblev1 "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	vfsv1 "github.com/cockroachdb/pebble/vfs"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// openUpgradingPebble opens a Pebble v2 store. A store written by Pebble v1
// below the oldest format v2 reads (FormatFlushableIngest) is first raised to
// that format with Pebble v1, so an AccountsDB built by an older Mithril opens
// without a snapshot rebuild. Pebble may compact SSTables during the upgrade;
// logical keys and values are unchanged. A store already at a v2-readable format, or
// a new one, opens exactly as before.
func openUpgradingPebble(dir string, opts *pebble.Options) (*pebble.DB, error) {
	version, ok, err := storedPebbleFormat(dir)
	if err != nil {
		return nil, err
	}
	if ok && version < uint64(pebble.FormatMinSupported) {
		mlog.Log.Infof("Pebble store %s has format version %d; upgrading it to %d before opening", dir, version, uint64(pebble.FormatMinSupported))
		if err := upgradeV1PebbleFormat(dir); err != nil {
			return nil, fmt.Errorf("upgrading Pebble store %s from format version %d: %w", dir, version, err)
		}
	}
	return pebble.Open(dir, opts)
}

// upgradeV1PebbleFormat raises a v1 store to the oldest format v2 reads.
func upgradeV1PebbleFormat(dir string) error {
	db, err := pebblev1.Open(dir, &pebblev1.Options{Logger: silentLogger{}})
	if err != nil {
		return err
	}
	// Recheck the persisted version under the database lock. A prior upgrade
	// may have completed since the read-only probe. Never request a downgrade.
	if db.FormatMajorVersion() >= pebblev1.FormatFlushableIngest {
		return db.Close()
	}
	if err := db.RatchetFormatMajorVersion(pebblev1.FormatFlushableIngest); err != nil {
		db.Close()
		return err
	}
	return db.Close()
}

// storedPebbleFormat reads Pebble's persisted format marker. OPTIONS records
// the requested open options and can remain stale after a format ratchet.
// Probe with v1 first so legacy formats remain readable; v2 handles newer
// formats beyond v1's supported range. Both probes are read-only.
func storedPebbleFormat(dir string) (version uint64, ok bool, err error) {
	legacy, err := pebblev1.Peek(dir, vfsv1.Default)
	if os.IsNotExist(err) {
		return 0, false, nil
	}
	if err == nil {
		return uint64(legacy.FormatMajorVersion), legacy.Exists, nil
	}
	current, currentErr := pebble.Peek(dir, vfs.Default)
	if currentErr != nil {
		return 0, false, fmt.Errorf("inspect Pebble format: v1: %v; v2: %w", err, currentErr)
	}
	return uint64(current.FormatMajorVersion), current.Exists, nil
}
