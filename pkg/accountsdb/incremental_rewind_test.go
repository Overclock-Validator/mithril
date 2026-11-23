package accountsdb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Compare both the protected whole files and individual undo locations with
// a fresh decode, including the file headers later used for reclamation.
func checkIncrementalPins(t *testing.T, db *AccountsDb, horizon uint64, path string) RewindScanStats {
	t.Helper()
	cfg := CompactionConfig{RewindHorizonBatches: horizon}
	stats, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Equal(t, path, stats.RewindScan.Path)
	requirePinsMatchUncached(t, db, horizon)
	return stats.RewindScan
}

func TestIncrementalRewindMatchesFull(t *testing.T) {

	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	checkIncrementalPins(t, db, 64, "full") // verified empty history can grow
	for _, slot := range []uint64{80, 90, 100, 110, 120} {
		// The successor at 100 sorts before 80 and 90 by filename.
		commitTestBatch(t, db, slot, foldAcct(1, slot, []byte("a")), foldAcct(2, slot, []byte("b")))
		state := checkIncrementalPins(t, db, 64, "append")
		require.Equal(t, int64(2), state.DecodedRecords)
		require.EqualValues(t, 1, state.DecodedManifests)
		state = checkIncrementalPins(t, db, 64, "hit")
		require.Zero(t, state.DecodedRecords)
	}
	commitTestBatch(t, db, 130, foldAcct(1, 130, []byte("a")))
	commitTestBatch(t, db, 140, foldAcct(1, 140, []byte("a")))
	checkIncrementalPins(t, db, 64, "full") // two unobserved successors
	commitTestBatch(t, db, 150, foldAcct(1, 150, []byte("a")))
	checkIncrementalPins(t, db, 2, "full") // horizon and validation boundary move
	_, err := db.RewindToBatchBoundary(140)
	require.NoError(t, err)
	checkIncrementalPins(t, db, 2, "full")
	commitTestBatch(t, db, 150, foldAcct(2, 7, []byte("replayed")))
	checkIncrementalPins(t, db, 2, "append")
}

func TestIncrementalRewindParkedAndChangedIdentity(t *testing.T) {

	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	first := commitTestBatch(t, db, 110, foldAcct(1, 1, []byte("a")))
	second := commitTestBatch(t, db, 120, foldAcct(1, 2, []byte("b")))
	path := segmentManifestPath(db.AcctsDir, 120, second.FileId)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	parked := path + ".rewound"
	require.NoError(t, os.WriteFile(parked, raw, 0600))
	checkIncrementalPins(t, db, 64, "full")
	commitTestBatch(t, db, 130, foldAcct(1, 3, []byte("third")))
	checkIncrementalPins(t, db, 64, "append")
	require.Contains(t, db.compactPinCache.pinned.whole, first.FileId)

	// An older parked file with a different valid body, identical size and
	// identical mtime must prevent extension of the previously verified map.
	info, err := os.Stat(parked)
	require.NoError(t, err)
	m, err := ReadSegmentManifest(parked)
	require.NoError(t, err)
	m.ResumeCtx[0] ^= 1
	require.NoError(t, os.WriteFile(parked, m.encode(), 0600))
	require.NoError(t, os.Chtimes(parked, info.ModTime(), info.ModTime()))
	commitTestBatch(t, db, 140, foldAcct(1, 4, []byte("fourth")))
	checkIncrementalPins(t, db, 64, "full")

	// A damaged parked manifest fails closed even alongside a proper successor.
	raw[0] ^= 1
	require.NoError(t, os.WriteFile(parked, raw, 0600))
	commitTestBatch(t, db, 150, foldAcct(1, 5, []byte("fifth")))
	_, err = db.CompactOnce(CompactionConfig{RewindHorizonBatches: 64})
	require.Error(t, err)
}

func TestIncrementalRewindConflictIsAtomic(t *testing.T) {

	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	first := commitTestBatch(t, db, 110, foldAcct(1, 1, []byte("aa")), foldAcct(2, 1, []byte("bb")))
	commitTestBatch(t, db, 120, foldAcct(1, 2, []byte("cc")))
	checkIncrementalPins(t, db, 64, "full")
	base := db.compactPinCache.pinned
	third := commitTestBatch(t, db, 130, foldAcct(1, 3, []byte("dd")), foldAcct(2, 3, []byte("ee")))
	m, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, 130, third.FileId))
	require.NoError(t, err)
	for i := range m.Records {
		r := &m.Records[i]
		if r.Pubkey[0] == 2 {
			r.Prev.FileId = first.FileId
			r.Prev.Offset = 0 // already pinned in the cache for key 1
		}
	}
	require.NoError(t, WriteSegmentManifest(db.AcctsDir, m))
	_, err = db.CompactOnce(CompactionConfig{RewindHorizonBatches: 64})
	require.ErrorContains(t, err, "conflicting undo pointers")
	require.Nil(t, db.compactPinCache)
	require.NotContains(t, base.whole, third.FileId)
	require.Len(t, base.records, 1)
	require.Len(t, base.records[first.FileId], 1)
	require.Equal(t, [32]byte{1}, base.records[first.FileId][0])

	// Cold full decode must reject the same conflicting reference.
	_, fullErr := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 64})
	require.EqualError(t, fullErr, err.Error())
}

func TestIncrementalRewindSelection(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	checkIncrementalPins(t, db, 64, "full")
	commitTestBatch(t, db, 100, foldAcct(1, 1, []byte("first")))
	mode := "append"
	state := checkIncrementalPins(t, db, 64, mode)
	require.EqualValues(t, 1, state.DecodedManifests)
}
