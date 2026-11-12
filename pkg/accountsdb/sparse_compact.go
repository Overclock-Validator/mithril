package accountsdb

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

const sparseCompactionMinSize = int64(8 << 20)

type deadPayload struct{ offset, length int64 }

// sparsifyFile releases obsolete record pages in an out-of-horizon
// segment. File IDs and retained record headers, offsets and bytes never
// change. This prevents one tiny undo record from pinning gigabytes of dead
// account data (testnet slots 452462795..452482738).
//
// Before punching holes, publish a sparse-kind manifest containing exactly the
// retained records. Its offsets are authoritative: gaps may include obsolete
// headers as well as payloads. This reclaims runs of small dead accounts even
// when a few undo records prevent relocating the file. Index-based readers and
// rewind retain their original locations and byte-exact records.
//
// A crash before the marker leaves the old file intact. A crash after it leaves
// some dead gaps allocated; the next cycle derives those gaps from the retained
// offsets and can retry without reading discarded headers.
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
		(m.Kind != ManifestKindFold && m.Kind != ManifestKindCompact && m.Kind != ManifestKindSparse) {
		return 0, scanned, false, fmt.Errorf("manifest does not describe source %s", c.name)
	}
	path := c.path
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
	retained := make([]ManifestRecord, 0, len(m.Records))
	foundUndo := 0
	var next, retainedEnd uint64
	addDeadRange := func(start, end uint64) {
		// Keep boundary pages containing any part of a retained record.
		start = (start + 4095) &^ uint64(4095)
		end &= ^uint64(4095)
		if end > start {
			dead = append(dead, deadPayload{int64(start), int64(end - start)})
		}
	}
	for _, r := range m.Records {
		if r.Offset < next || (m.Kind != ManifestKindSparse && r.Offset != next) || r.Offset > m.DataLen || m.DataLen-r.Offset < hdrLen {
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
		if next > m.DataLen {
			return 0, scanned, false, fmt.Errorf("record padding exceeds source at offset %d", r.Offset)
		}
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
			addDeadRange(retainedEnd, r.Offset)
			retainedEnd = next
			// Retired manifests no longer provide fold redo/undo context.
			r.PrevValid, r.Prev = false, AccountIndexEntry{}
			retained = append(retained, r)
		}
	}
	if (m.Kind != ManifestKindSparse && next != m.DataLen) || foundUndo != len(undo) {
		return 0, scanned, false, fmt.Errorf("manifest does not cover source or all undo records")
	}
	addDeadRange(retainedEnd, m.DataLen)
	if len(retained) > 0 && len(dead) == 0 && len(retained) == len(m.Records) {
		return 0, scanned, false, nil
	}
	if len(retained) == 0 {
		db.appendVecReadMu.LockAs("compaction")
		err := removeSourceFiles(path, c.manifestPath)
		db.appendVecReadMu.Unlock()
		if err == nil {
			db.trackRemovedLocked(path, c.manifestPath)
		}
		return before, scanned, err == nil, err
	}
	if m.Kind != ManifestKindSparse || len(retained) != len(m.Records) {
		m.Kind, m.BatchSeq, m.DataCRC = ManifestKindSparse, 0, 0
		m.Records, m.Bankhashes, m.ResumeCtx = retained, nil, nil
		if err := WriteSegmentManifest(db.AcctsDir, m); err != nil {
			return 0, scanned, false, err
		}
	}
	// Publication is durable before either this notice or any hole punching.
	// A failed marker write must not announce a successful format conversion.
	db.warnSparseCompatibilityLocked(m.FileId)
	if db.compactHooks.afterSparseMarker != nil {
		db.compactHooks.afterSparseMarker()
	}
	// Drain readers holding older index snapshots before punching any bytes.
	// foldMu prevents new index generations while the plan is applied.
	db.appendVecReadMu.LockAs("compaction")
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

// warnSparseCompatibilityLocked also runs when compaction first encounters an
// existing kind-3 store. Caller holds foldMu; no additional persistent marker is
// needed, and repeated compaction cycles must not flood the operator's log.
func (db *AccountsDb) warnSparseCompatibilityLocked(fileID uint64) {
	if db.sparseCompatibilityWarned {
		return
	}
	db.sparseCompatibilityWarned = true
	mlog.Log.Warnf("accountsdb: %s uses sparse manifests (kind 3, file %d); older binaries cannot compact this store. Disabling compaction does not restore the old format; use a kind-3-capable binary or restore a pre-conversion copy for rollback. See docs/storage-sparse-reclamation.md", db.AcctsDir, fileID)
	fire(db.compactHooks.afterSparseCompatibilityWarning)
}

type compactionTestHooks struct {
	afterCompactManifest            func()
	afterCompactIndex               func()
	afterHistoryFloor               func()
	afterSparseMarker               func()
	afterSparsePunch                func()
	afterSparseCompatibilityWarning func()
}
