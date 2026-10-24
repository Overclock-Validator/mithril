package accountsdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

const sparseCompactionMinSize = int64(8 << 20)

type deadPayload struct{ offset, length int64 }

// sparsifyFile releases only obsolete payload pages in an out-of-horizon
// segment. File IDs, record headers, offsets, live bytes and undo bytes never
// change. This prevents one tiny undo record from pinning gigabytes of dead
// account data (testnet slots 452462795..452482738).
//
// Before the first hole is punched, atomically replace the retired fold's
// manifest with a compact-kind marker. Like other compact manifests it is NOT
// replayed as an index redo log. DataCRC=0 denotes an in-place sparse segment;
// its original whole-file checksum no longer describes obsolete payloads.
// The marker retains all record offsets and has its own normal manifest CRC.
// A crash before the marker leaves the original fold intact; a crash after it
// leaves some or all dead pages allocated. Either state is safe and retryable.
// Caller holds foldMu and has excluded every in-horizon/parked whole-file pin.
func (db *AccountsDb) sparsifyFile(c compactCandidate, undo map[uint64][32]byte, indexCache *sparseIndexCache) (reclaimed, scanned int64, deleted bool, err error) {
	m, err := ReadSegmentManifest(c.manifestPath)
	if err != nil {
		return 0, 0, false, err
	}
	info, err := os.Stat(c.manifestPath)
	if err != nil {
		return 0, 0, false, err
	}
	scanned = info.Size()
	if m.FileId != c.fileId || m.ThroughSlot != c.slot || m.DataLen != uint64(c.size) ||
		(m.Kind != ManifestKindFold && m.Kind != ManifestKindCompact) {
		return 0, scanned, false, fmt.Errorf("manifest does not describe source %s", c.name)
	}
	path := filepath.Join(db.AcctsDir, c.name)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return 0, scanned, false, err
	}
	defer f.Close()
	before, err := allocatedFileBytes(f)
	if err != nil {
		return 0, scanned, false, err
	}
	var dead []deadPayload
	retained, foundUndo := 0, 0
	var next uint64
	for _, r := range m.Records {
		if r.Offset != next || r.Offset > m.DataLen || m.DataLen-r.Offset < hdrLen {
			return 0, scanned, false, fmt.Errorf("invalid record offset %d in %s", r.Offset, c.name)
		}
		var h [hdrLen]byte
		if _, err := f.ReadAt(h[:], int64(r.Offset)); err != nil {
			return 0, scanned, false, err
		}
		scanned += hdrLen
		if [32]byte(h[16:48]) != r.Pubkey {
			return 0, scanned, false, fmt.Errorf("record key mismatch at offset %d", r.Offset)
		}
		length := binary.LittleEndian.Uint64(h[dataLenOffset : dataLenOffset+8])
		if length > m.DataLen-r.Offset-hdrLen {
			return 0, scanned, false, fmt.Errorf("record payload exceeds source at offset %d", r.Offset)
		}
		next = r.Offset + hdrLen + ((length + 7) &^ uint64(7))
		keep := false
		if key, pinned := undo[r.Offset]; pinned {
			if key != r.Pubkey {
				return 0, scanned, false, fmt.Errorf("undo key mismatch at offset %d", r.Offset)
			}
			foundUndo++
			keep = true
		}
		current, found, getErr := indexCache.lookup(db, r.Pubkey)
		if getErr != nil {
			return 0, scanned, false, getErr
		}
		if found && current.FileId == c.fileId {
			if current.Offset != r.Offset || current.Slot != c.slot {
				return 0, scanned, false, fmt.Errorf("index location disagrees with source for %x", r.Pubkey)
			}
			keep = true
		}
		if keep {
			retained++
			continue
		}
		// Retain headers and edge pages so old appendvec scanners remain valid.
		// Only complete 4 KiB pages wholly inside dead data may be discarded.
		start := (int64(r.Offset) + hdrLen + 4095) &^ int64(4095)
		end := (int64(r.Offset) + hdrLen + int64(length)) &^ int64(4095)
		if end > start {
			dead = append(dead, deadPayload{start, end - start})
		}
	}
	if next != m.DataLen || foundUndo != len(undo) {
		return 0, scanned, false, fmt.Errorf("manifest does not cover source or all undo records")
	}
	if retained > 0 && len(dead) == 0 {
		return 0, scanned, false, nil
	}
	if retained == 0 {
		db.appendVecReadMu.Lock()
		err := removeSourceFiles(db.AcctsDir, path, c.manifestPath)
		db.appendVecReadMu.Unlock()
		if err == nil {
			db.trackRemovedLocked(path, c.manifestPath)
		}
		return before, scanned, err == nil, err
	}
	if m.Kind != ManifestKindCompact || m.DataCRC != 0 {
		m.Kind, m.BatchSeq, m.DataCRC = ManifestKindCompact, 0, 0
		if err := WriteSegmentManifest(db.AcctsDir, m); err != nil {
			return 0, scanned, false, err
		}
	}
	if db.compactHooks.afterSparseMarker != nil {
		db.compactHooks.afterSparseMarker()
	}
	// Drain readers holding older index snapshots before punching any bytes.
	// foldMu prevents new index generations while the plan is applied.
	db.appendVecReadMu.Lock()
	defer db.appendVecReadMu.Unlock()
	for _, span := range dead {
		if err := punchDeadPayload(f, span.offset, span.length); err != nil {
			return 0, scanned, false, err
		}
		if db.compactHooks.afterSparsePunch != nil {
			db.compactHooks.afterSparsePunch()
		}
	}
	if err := f.Sync(); err != nil {
		return 0, scanned, false, err
	}
	after, err := allocatedFileBytes(f)
	if err != nil {
		return 0, scanned, false, err
	}
	db.trackAllocatedLocked(c.name, after)
	return max(int64(0), before-after), scanned, false, nil
}

type compactionTestHooks struct {
	afterHistoryFloor func()
	afterSparseMarker func()
	afterSparsePunch  func()
}
