package accountsdb

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	pebblev1 "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/v2"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// openUpgradingPebble opens a Pebble v2 store. A store written by Pebble v1
// below the oldest format v2 reads (FormatFlushableIngest) is first raised to
// that format with Pebble v1, so an AccountsDB built by an older Mithril opens
// without a rebuild. Raising the format rewrites only Pebble's own metadata;
// keys and values are unchanged. A store already at a v2-readable format, or
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
	if err := db.RatchetFormatMajorVersion(pebblev1.FormatFlushableIngest); err != nil {
		db.Close()
		return err
	}
	return db.Close()
}

// storedPebbleFormat reads format_major_version from the store's newest
// OPTIONS file. ok is false when the directory holds no store yet.
func storedPebbleFormat(dir string) (version uint64, ok bool, err error) {
	paths, err := filepath.Glob(filepath.Join(dir, "OPTIONS-*"))
	if err != nil || len(paths) == 0 {
		return 0, false, err
	}
	// OPTIONS-<file number>: the highest number is the current one.
	sort.Slice(paths, func(i, j int) bool { return optionsFileNum(paths[i]) < optionsFileNum(paths[j]) })
	f, err := os.Open(paths[len(paths)-1])
	if err != nil {
		return 0, false, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, found := strings.Cut(strings.TrimSpace(sc.Text()), "=")
		if found && k == "format_major_version" {
			version, err := strconv.ParseUint(v, 10, 64)
			return version, err == nil, err
		}
	}
	return 0, false, sc.Err()
}

func optionsFileNum(path string) uint64 {
	n, _ := strconv.ParseUint(strings.TrimPrefix(filepath.Base(path), "OPTIONS-"), 10, 64)
	return n
}
