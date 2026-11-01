package accountsdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/util"
	"github.com/cockroachdb/pebble/v2"
)

// maxAccountDataLen is the runtime's account data limit
// (sealevel.MaxPermittedDataLength); a larger record length is a torn or
// foreign record.
const maxAccountDataLen = 10 * 1024 * 1024

// ErrClassicStore reports a store written by the per-slot classic replay path,
// which no longer exists. That path stored accounts in data files with no fold
// manifest, which fold recovery would delete as uncommitted writes, and it
// rewrote most updated accounts in place in older files, snapshot and fold
// files included, so the store can resume neither from its snapshot nor from
// its last fold. The only safe action is to rebuild from a snapshot.
//
// Classic replay on a store that had already folded leaves the same files,
// recognized when the index still points into one of them. A classic run
// whose every account update was in place leaves no such file; running
// classic replay on a folded store was never supported, and that case cannot
// be told apart from the store's files.
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
	// first fold interrupted before the manifest leaves none.
	ClassicFiles int
	// LiveFiles counts the unfolded files the index still points into, which
	// hold the only copy of those accounts. Folds and compactions write index
	// entries only after their manifest is durable, so an interrupted one
	// never leaves such a file; classic replay pointed the index at each new
	// per-slot file as it wrote it.
	LiveFiles int
}

// ClassicWrites reports whether h is a shape classic replay leaves: unfolded
// files the index points into, or, while no fold was ever recorded, unfolded
// files with bank hashes recorded for their slots. After a fold, an unfolded
// file with a bank hash row for its slot can be an interrupted compaction's
// output (named for its source file's slot), so only index liveness counts
// there. Recovery removes every other unfolded file as before.
func (h FoldHistory) ClassicWrites() bool {
	return h.LiveFiles > 0 || !h.Folded && h.ClassicFiles > 0
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
			live, err := db.indexPointsInto(filepath.Join(dir, e.Name()), fileId)
			if err != nil {
				return h, err
			}
			if live {
				h.LiveFiles++
			}
		}
	}
	return h, nil
}

// indexPointsInto reports whether the index locates any account recorded in
// the data file at path in that file. It reads record headers only and stops
// at the first live account, at the end of the file, or at a torn or
// unparseable record, which no complete writer left.
func (db *AccountsDb) indexPointsInto(path string, fileId uint64) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var hdr [hdrLen]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return false, nil
		}
		dataLen := binary.LittleEndian.Uint64(hdr[dataLenOffset:])
		if dataLen > maxAccountDataLen {
			return false, nil
		}
		val, closer, err := db.Index.Get(hdr[pubkeyOffset : pubkeyOffset+32])
		if err == nil {
			entry, uerr := UnmarshalAcctIdxEntryValue(val)
			closer.Close()
			if uerr == nil && entry.FileId == fileId {
				return true, nil
			}
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return false, err
		}
		if _, err := r.Discard(int(util.AlignUp(dataLen, 8))); err != nil {
			return false, nil
		}
	}
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
	if h.LiveFiles > 0 {
		return fmt.Errorf("%w: the index points into %d account file(s) newer than the snapshot that have no fold manifest", ErrClassicStore, h.LiveFiles)
	}
	return fmt.Errorf("%w: %d account file(s) newer than the snapshot have bank hashes but no fold manifest", ErrClassicStore, h.ClassicFiles)
}
