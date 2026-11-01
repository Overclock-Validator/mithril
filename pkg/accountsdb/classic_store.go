package accountsdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/pebble/v2"
)

// ErrClassicStore reports a store written by the per-slot classic replay path,
// which no longer exists. That path stored accounts in data files with no fold
// manifest, which fold recovery would delete as uncommitted writes, and it may
// have rewritten snapshot account files, so the store cannot resume from its
// snapshot either. The only safe action is to rebuild from a snapshot.
var ErrClassicStore = errors.New("accountsdb: store was written by classic replay, which this version removed; rebuild it from a snapshot (--bootstrap snapshot)")

// FoldHistory describes what a store's directory shows about how it was
// written, without changing anything.
type FoldHistory struct {
	// Folded is true once a fold or compaction has been recorded: fold meta,
	// any manifest (including rewound and temporary ones) or a rewind intent.
	// Rooted-durable replay folds at every graceful shutdown that replayed a
	// finalized slot, so only a store that never folded lacks all of these.
	Folded bool
	// KnownBootstrap is true when the snapshot build recorded the highest
	// bootstrap file id (bootstrap_high_file_id). Without it, files newer than
	// the snapshot cannot be told apart.
	KnownBootstrap bool
	// UnfoldedFiles counts data files above the bootstrap boundary that no
	// manifest references.
	UnfoldedFiles int
	// ClassicFiles counts the unfolded files whose slot has a bank hash row.
	// Classic replay recorded each slot's bank hash next to its account
	// stores; a fold records its bank hashes only after its manifest, so a
	// fold interrupted before the manifest leaves none.
	ClassicFiles int
}

// ClassicWrites reports whether h is the shape classic replay leaves: data
// files newer than the snapshot, with bank hashes recorded for their slots,
// while no fold was ever recorded. Unfolded files without bank hashes are a
// first fold interrupted before its manifest, which recovery removes as
// before.
func (h FoldHistory) ClassicWrites() bool {
	return !h.Folded && h.ClassicFiles > 0
}

// InspectFoldHistory reads the store's fold history without modifying it.
func (db *AccountsDb) InspectFoldHistory() (FoldHistory, error) {
	db.foldMu.LockAs("inspect_fold_history")
	defer db.foldMu.Unlock()
	return db.inspectFoldHistoryLocked()
}

func (db *AccountsDb) inspectFoldHistoryLocked() (FoldHistory, error) {
	var h FoldHistory
	_, haveMeta, err := db.readFoldMeta()
	if err != nil {
		return h, err
	}
	intent, err := db.readRewindIntentLocked()
	if err != nil {
		return h, err
	}
	referenced, err := listAllManifestFileIds(db.AcctsDir)
	if err != nil {
		return h, err
	}
	tmp, err := hasTmpManifest(db.AcctsDir)
	if err != nil {
		return h, err
	}
	h.Folded = haveMeta || intent != nil || len(referenced) > 0 || tmp
	bootstrapHigh := db.bootstrapHighFileId()
	h.KnownBootstrap = bootstrapHigh != ^uint64(0)
	if !h.KnownBootstrap {
		return h, nil
	}
	for _, dir := range db.AccountsDirs() {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return h, err
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			slot, fileId, ok := parseDataFileName(e.Name())
			if !ok || fileId <= bootstrapHigh || db.appendVecDir(fileId) != dir {
				continue
			}
			if _, ok := referenced[fileId]; ok {
				continue
			}
			h.UnfoldedFiles++
			recorded, err := db.hasBankHashRow(slot)
			if err != nil {
				return h, err
			}
			if recorded {
				h.ClassicFiles++
			}
		}
	}
	return h, nil
}

func (db *AccountsDb) hasBankHashRow(slot uint64) (bool, error) {
	var key [8]byte
	binary.LittleEndian.PutUint64(key[:], slot)
	_, closer, err := db.BankHashStore.Get(key[:])
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

// hasTmpManifest reports whether a manifest write was interrupted before its
// rename: evidence of a fold, even one that never committed.
func hasTmpManifest(acctsDir string) (bool, error) {
	entries, err := os.ReadDir(acctsDir)
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), segManifestTmpSuffix) {
			return true, nil
		}
	}
	return false, nil
}

func classicStoreError(h FoldHistory) error {
	return fmt.Errorf("%w: %d account file(s) newer than the snapshot have bank hashes but no fold manifest", ErrClassicStore, h.ClassicFiles)
}
