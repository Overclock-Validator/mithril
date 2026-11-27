package accountsdb

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestCommittedBankhashRecoveryPreservesIndex(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	first := commitTestBatch(t, db, 110, foldAcct(1, 100, nil))
	commitTestBatch(t, db, 120, foldAcct(1, 200, nil))
	commitTestBatch(t, db, 130, foldAcct(2, 300, nil))
	before := snapshotIndex(t, db)
	for _, slot := range []uint64{110, 120, 130} {
		var key [8]byte
		binary.LittleEndian.PutUint64(key[:], slot)
		require.NoError(t, db.BankHashStore.Delete(key[:], pebble.Sync))
	}
	// A committed historical segment need not survive account compaction.
	// Its CRC-valid retained manifest still carries the bankhash authority.
	require.NoError(t, os.Remove(filepath.Join(db.AcctsDir, SegmentDataName(110, first.FileId))))
	for range 2 {
		res, err := db.RecoverFoldState()
		require.NoError(t, err)
		require.Empty(t, res.ReplayedBatches)
		require.Equal(t, before, snapshotIndex(t, db))
		for _, slot := range []uint64{110, 120, 130} {
			got, err := db.GetBankHashForSlot(slot)
			require.NoError(t, err)
			want := bh(slot)
			require.Equal(t, want[:], got)
		}
	}
}

func TestCommittedBankhashRecoveryRejectsInvalidAuthority(t *testing.T) {
	for _, kind := range []string{"crc", "duplicate-sequence", "overlap", "unsorted-hashes", "above-frontier"} {
		t.Run(kind, func(t *testing.T) {
			db, _ := newFoldTestDb(t)
			defer db.CloseDb()
			commitTestBatch(t, db, 110)
			second := commitTestBatch(t, db, 120)
			path := segmentManifestPath(db.AcctsDir, 120, second.FileId)
			manifest, err := ReadSegmentManifest(path)
			require.NoError(t, err)
			switch kind {
			case "crc":
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				data[len(data)-1] ^= 1
				require.NoError(t, os.WriteFile(path, data, 0o600))
			case "duplicate-sequence":
				manifest.FileId += 100
				require.NoError(t, WriteSegmentManifest(db.AcctsDir, manifest))
			case "overlap":
				manifest.Bankhashes = []SlotBankhash{{Slot: 110, Bankhash: [32]byte{99}}}
				require.NoError(t, WriteSegmentManifest(db.AcctsDir, manifest))
			case "unsorted-hashes":
				manifest.Bankhashes = []SlotBankhash{{Slot: 120, Bankhash: bh(120)}, {Slot: 119, Bankhash: bh(119)}}
				require.NoError(t, WriteSegmentManifest(db.AcctsDir, manifest))
			case "above-frontier":
				manifest.Bankhashes = []SlotBankhash{{Slot: 121, Bankhash: bh(121)}}
				require.NoError(t, WriteSegmentManifest(db.AcctsDir, manifest))
			}
			before := snapshotIndex(t, db)
			_, err = db.RecoverFoldState()
			require.Error(t, err)
			require.Equal(t, before, snapshotIndex(t, db))
			require.FileExists(t, path)
		})
	}
}

func TestCommittedBankhashRecoveryIgnoresRewoundSuffix(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	commitTestBatch(t, db, 110)
	second := commitTestBatch(t, db, 120)
	_, err := db.RewindToBatchBoundary(110)
	require.NoError(t, err)
	parked := segmentManifestPath(db.AcctsDir, 120, second.FileId) + ".rewound"
	data, err := os.ReadFile(filepath.Join(db.AcctsDir, "rewound", filepath.Base(parked)))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(parked, data, 0o600))
	var key [8]byte
	binary.LittleEndian.PutUint64(key[:], 120)
	require.NoError(t, db.BankHashStore.Delete(key[:], pebble.Sync))
	res, err := db.RecoverFoldState()
	require.NoError(t, err)
	require.Equal(t, uint64(110), res.DurableThrough)
	require.True(t, res.RewindInProgress)
	_, err = db.GetBankHashForSlot(120)
	require.ErrorIs(t, err, pebble.ErrNotFound)
	require.FileExists(t, parked)
}

func TestCommittedBankhashRecoveryPropagatesWriteError(t *testing.T) {
	for _, tail := range []bool{false, true} {
		t.Run(map[bool]string{false: "committed", true: "replayed-tail"}[tail], func(t *testing.T) {
			db, dir := newFoldTestDb(t)
			defer db.CloseDb()
			batch := commitTestBatch(t, db, 110)
			if tail {
				require.NoError(t, db.Index.Delete(metaKeyLastBatch, pebble.Sync))
			}
			require.NoError(t, db.BankHashStore.Close())
			store, err := pebble.Open(filepath.Join(dir, "bankhash_db"), &pebble.Options{ReadOnly: true, Logger: silentLogger{}})
			require.NoError(t, err)
			db.BankHashStore = store
			_, err = db.RecoverFoldState()
			require.ErrorIs(t, err, pebble.ErrReadOnly)
			require.ErrorContains(t, err, "restore bankhash at slot 110")
			require.FileExists(t, segmentManifestPath(db.AcctsDir, 110, batch.FileId))
		})
	}
}

func TestCommittedBankhashRecoveryAcceptsSparseEmptyFolds(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	_, err := db.CommitBatch(nil, 9, map[uint64][32]byte{5: bh(5)}, nil)
	require.NoError(t, err)
	_, err = db.CommitBatch(nil, 1000, map[uint64][32]byte{500: bh(500), 1000: bh(1000)}, nil)
	require.NoError(t, err)
	res, err := db.RecoverFoldState()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), res.DurableThrough)
	for _, slot := range []uint64{5, 500, 1000} {
		got, err := db.GetBankHashForSlot(slot)
		require.NoError(t, err)
		want := bh(slot)
		require.Equal(t, want[:], got)
	}
}

func TestCompactionBankhashBarrierSkipsNoCandidate(t *testing.T) {
	db, dir := newFoldTestDb(t)
	defer db.CloseDb()
	commitTestBatch(t, db, 110)
	require.NoError(t, db.BankHashStore.Close())
	store, err := pebble.Open(filepath.Join(dir, "bankhash_db"), &pebble.Options{ReadOnly: true, Logger: silentLogger{}})
	require.NoError(t, err)
	db.BankHashStore = store
	stats, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1})
	require.NoError(t, err, "the pinned head needs no compaction WAL barrier")
	require.Zero(t, stats.CandidatesScanned)
}

func TestCompactionBankhashBarrierFailurePreservesManifests(t *testing.T) {
	db, dir := newFoldTestDb(t)
	defer db.CloseDb()
	for _, slot := range []uint64{110, 120, 130} {
		commitTestBatch(t, db, slot)
	}
	before := make(map[string][]byte)
	entries, err := os.ReadDir(db.AcctsDir)
	require.NoError(t, err)
	for _, entry := range entries {
		path := filepath.Join(db.AcctsDir, entry.Name())
		before[path], err = os.ReadFile(path)
		require.NoError(t, err)
	}
	require.NoError(t, db.BankHashStore.Close())
	store, err := pebble.Open(filepath.Join(dir, "bankhash_db"), &pebble.Options{ReadOnly: true, Logger: silentLogger{}})
	require.NoError(t, err)
	db.BankHashStore = store
	stats, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1})
	require.ErrorIs(t, err, pebble.ErrReadOnly)
	require.Zero(t, stats.CandidatesScanned)
	entries, err = os.ReadDir(db.AcctsDir)
	require.NoError(t, err)
	require.Len(t, entries, len(before))
	for path, want := range before {
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}
