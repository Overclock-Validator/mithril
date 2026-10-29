package accountsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/pebble/v2"
	"hash/crc32"
)

// This frontier describes history deliberately expired by compaction, not the
// current replay head. Rewind must never roll it back and pretend that already
// reclaimed undo history exists again. The boundary itself stays pinned.
var metaKeyCompactionHistoryFloor = []byte("\x00mithril.meta.compaction_history_floor")

func encodeCompactionHistoryFloor(m foldMeta) []byte {
	b := append([]byte("MRHF0001"), encodeFoldMeta(m)...)
	return binary.LittleEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
}

func (db *AccountsDb) readCompactionHistoryFloor() (foldMeta, error) {
	data, closer, err := db.Index.Get(metaKeyCompactionHistoryFloor)
	if err == pebble.ErrNotFound {
		return foldMeta{}, nil
	}
	if err != nil {
		return foldMeta{}, err
	}
	defer closer.Close()
	if len(data) != 36 || !bytes.Equal(data[:8], []byte("MRHF0001")) || binary.LittleEndian.Uint32(data[32:]) != crc32.ChecksumIEEE(data[:32]) {
		return foldMeta{}, fmt.Errorf("accountsdb: invalid compaction history floor")
	}
	m, err := decodeFoldMeta(data[8:32])
	if err != nil || m.BatchSeq == 0 {
		return foldMeta{}, fmt.Errorf("accountsdb: invalid compaction history boundary")
	}
	// Bind the frontier to an intact retained boundary, never to a naked sequence
	// inferred from missing files. Its manifest remains pinned until superseded.
	manifest, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, m.ThroughSlot, m.FileId))
	if err != nil {
		return foldMeta{}, fmt.Errorf("accountsdb: compaction history boundary: %w", err)
	}
	if manifest.Kind != ManifestKindFold || manifest.BatchSeq != m.BatchSeq || manifest.ThroughSlot != m.ThroughSlot || manifest.FileId != m.FileId {
		return foldMeta{}, fmt.Errorf("accountsdb: compaction history boundary identity mismatch")
	}
	return m, nil
}

func (db *AccountsDb) compactionHistoryFloor(head, horizon uint64) (uint64, error) {
	retained, err := db.readCompactionHistoryFloor()
	if err != nil {
		return 0, err
	}
	if retained.BatchSeq > head {
		return 0, fmt.Errorf("accountsdb: compaction history boundary %d exceeds head %d", retained.BatchSeq, head)
	}
	floor := uint64(0)
	if head > horizon {
		floor = head - horizon
	}
	return max(floor, retained.BatchSeq), nil
}

// Caller holds foldMu and has validated EVERY manifest in [floor, head] with
// compactionPinSet. Commit the frontier durably BEFORE removing or sparsifying
// any older fold. WAL-off mode needs an explicit flush, just like fold/rewind.
func (db *AccountsDb) persistCompactionHistoryFloor(boundary foldMeta) error {
	old, err := db.readCompactionHistoryFloor()
	if err != nil {
		return err
	}
	if boundary.BatchSeq < old.BatchSeq {
		return fmt.Errorf("accountsdb: compaction history cannot move backward")
	}
	if boundary.BatchSeq == old.BatchSeq {
		if boundary != old {
			return fmt.Errorf("accountsdb: compaction history identity changed")
		}
		// Retry a prior successful WAL-off Set whose Flush reported an error.
		if db.IndexWALDisabled {
			return db.Index.Flush()
		}
		return nil
	}
	opts := pebble.Sync
	if db.IndexWALDisabled {
		opts = pebble.NoSync
	}
	if err := db.Index.Set(metaKeyCompactionHistoryFloor, encodeCompactionHistoryFloor(boundary), opts); err != nil {
		return err
	}
	if db.IndexWALDisabled {
		return db.Index.Flush()
	}
	return nil
}
