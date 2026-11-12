package accountsdb

import (
	"encoding/binary"
	"fmt"
	"os"
)

type sparseRelocation struct {
	moved, allocated, scanned int64
}

// compactSparseLive packs current records when their sparse source wastes
// physical pages. A few tiny undo records must not pin one page per live account
// indefinitely. The source remains intact until the index move is durable;
// sparsifyFile then keeps every undo record at its original offset.
//
// Only a sparse manifest can describe these files: discarded headers make the
// sequential appendvec parser unsafe. The caller holds foldMu and discards its
// index cache after a successful move, before sparsifying the source.
func (db *AccountsDb) compactSparseLive(c compactCandidate, undo map[uint64][32]byte, cache *sparseIndexCache, minDeadFraction float64, moveBudget int64) (result sparseRelocation, err error) {
	m, err := ReadSegmentManifest(c.manifestPath)
	if err != nil {
		return result, err
	}
	info, err := os.Stat(c.manifestPath)
	if err != nil {
		return result, err
	}
	result.scanned = info.Size()
	if m.Kind != ManifestKindSparse || m.FileId != c.fileId || m.ThroughSlot != c.slot || m.DataLen != uint64(c.size) {
		return result, fmt.Errorf("sparse manifest does not describe source %s", c.name)
	}
	f, err := os.Open(c.path)
	if err != nil {
		return result, err
	}
	defer f.Close()
	allocated, err := allocatedFileBytes(f)
	if err != nil {
		return result, err
	}
	type span struct{ offset, length uint64 }
	var spans []span
	var records []ManifestRecord
	var next, liveBytes, undoPages, undoPageEnd uint64
	foundUndo := 0
	for _, r := range m.Records {
		if r.Offset < next || r.Offset > m.DataLen || m.DataLen-r.Offset < hdrLen {
			return result, fmt.Errorf("invalid sparse record offset %d", r.Offset)
		}
		var h [hdrLen]byte
		if _, err := f.ReadAt(h[:], int64(r.Offset)); err != nil {
			return result, err
		}
		result.scanned += hdrLen
		if [32]byte(h[16:48]) != r.Pubkey {
			return result, fmt.Errorf("sparse record key mismatch at offset %d", r.Offset)
		}
		length := binary.LittleEndian.Uint64(h[dataLenOffset : dataLenOffset+8])
		if length > m.DataLen-r.Offset-hdrLen {
			return result, fmt.Errorf("sparse record payload exceeds source at offset %d", r.Offset)
		}
		next = r.Offset + hdrLen + ((length + 7) &^ uint64(7))
		if next > m.DataLen {
			return result, fmt.Errorf("sparse record padding exceeds source at offset %d", r.Offset)
		}
		if key, pinned := undo[r.Offset]; pinned {
			if key != r.Pubkey {
				return result, fmt.Errorf("sparse undo key mismatch at offset %d", r.Offset)
			}
			foundUndo++
			start, end := r.Offset&^uint64(4095), (next+4095)&^uint64(4095)
			if end > undoPageEnd {
				undoPages += end - max(start, undoPageEnd)
				undoPageEnd = end
			}
		}
		current, found, err := cache.lookup(db, r.Pubkey)
		if err != nil {
			return result, err
		}
		if !found || current.FileId != c.fileId {
			continue
		}
		if current.Offset != r.Offset || current.Slot != c.slot {
			return result, fmt.Errorf("index location disagrees with sparse source for %x", r.Pubkey)
		}
		spans = append(spans, span{r.Offset, next - r.Offset})
		records = append(records, ManifestRecord{Pubkey: r.Pubkey, Offset: liveBytes, OwnerSlot: r.OwnerSlot})
		liveBytes += next - r.Offset
	}
	if foundUndo != len(undo) {
		return result, fmt.Errorf("sparse manifest does not cover all undo records")
	}
	// Judge the saving against allocated pages, not the much larger logical
	// file. Include the undo pages that cannot be reclaimed after relocation.
	projected := undoPages + ((liveBytes + 4095) &^ uint64(4095))
	if liveBytes == 0 || moveBudget <= 0 || liveBytes > uint64(moveBudget) || allocated <= 0 ||
		1-float64(projected)/float64(allocated) < minDeadFraction {
		return result, nil
	}
	if db.MinFreeBytes > 0 {
		// Reserve space for data, manifest and index writes on every involved
		// filesystem. Low space skips relocation; in-place punching still runs.
		required := liveBytes + uint64(len(records))*(manifestRecordSize+128) + 1<<20
		dataDir := db.appendVecDir(c.fileId)
		for _, dir := range []string{dataDir, db.AcctsDir} {
			available, err := AvailableDiskBytes(dir)
			if err != nil {
				return result, err
			}
			if checkDiskBudget(available, required, db.MinFreeBytes) != nil {
				return result, nil
			}
			if dataDir == db.AcctsDir {
				break
			}
		}
	}
	data := make([]byte, int(liveBytes))
	for i, span := range spans {
		out := records[i].Offset
		if _, err := f.ReadAt(data[out:out+span.length], int64(span.offset)); err != nil {
			return result, err
		}
		result.scanned += int64(span.length)
	}
	result.allocated, err = db.publishCompactOutput(c, data, records, true)
	if err != nil {
		return result, err
	}
	result.moved = int64(liveBytes)
	return result, nil
}
