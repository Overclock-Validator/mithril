package accountsdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoverFoldStateRejectsDuplicateBatchSequence(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	commitTestBatch(t, db, 110, foldAcct(1, 1, nil))
	head := commitTestBatch(t, db, 120, foldAcct(2, 2, nil))

	original := segmentManifestPath(db.AcctsDir, head.ThroughSlot, head.FileId)
	duplicateManifest, err := ReadSegmentManifest(original)
	require.NoError(t, err)
	duplicateManifest.ThroughSlot = 999
	duplicateManifest.FileId += 1000
	require.NoError(t, WriteSegmentManifest(db.AcctsDir, duplicateManifest))
	duplicate := segmentManifestPath(db.AcctsDir, duplicateManifest.ThroughSlot, duplicateManifest.FileId)

	_, err = db.RecoverFoldState()
	require.ErrorContains(t, err, "duplicate fold manifest sequence 2")
	assert.FileExists(t, original)
	assert.FileExists(t, duplicate)
}

func TestRecoverFoldStateRequiresAuthoritativeMetaManifestIdentity(t *testing.T) {
	t.Run("torn full manifest", func(t *testing.T) {
		db, _ := newFoldTestDb(t)
		defer db.CloseDb()
		head := commitTestBatch(t, db, 110, foldAcct(1, 1, nil))
		path := segmentManifestPath(db.AcctsDir, head.ThroughSlot, head.FileId)
		encoded, err := os.ReadFile(path)
		require.NoError(t, err)
		encoded[25] ^= 0xff // ThroughSlot in the fixed header; leave the tail CRC stale.
		require.NoError(t, os.WriteFile(path, encoded, 0o644))

		_, err = db.RecoverFoldState()
		require.ErrorIs(t, err, ErrTornManifest)
		assert.FileExists(t, path)
		assert.FileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(head.ThroughSlot, head.FileId)))
	})

	t.Run("CRC-valid forged sequence", func(t *testing.T) {
		db, _ := newFoldTestDb(t)
		defer db.CloseDb()
		head := commitTestBatch(t, db, 110, foldAcct(1, 1, nil))
		path := segmentManifestPath(db.AcctsDir, head.ThroughSlot, head.FileId)
		manifest, err := ReadSegmentManifest(path)
		require.NoError(t, err)
		manifest.BatchSeq++
		require.NoError(t, os.WriteFile(path, manifest.encode(), 0o644))

		_, err = db.RecoverFoldState()
		require.ErrorContains(t, err, "does not match authoritative meta")
	})
}

func TestRecoverFoldStateRejectsInvalidReplaySlotChainWithoutDeleting(t *testing.T) {
	db, dir := newFoldTestDb(t)
	first := commitTestBatch(t, db, 110, foldAcct(1, 1, nil))
	second := commitTestBatch(t, db, 120, foldAcct(2, 2, nil))
	secondPath := segmentManifestPath(db.AcctsDir, second.ThroughSlot, second.FileId)
	manifest, err := ReadSegmentManifest(secondPath)
	require.NoError(t, err)
	manifest.FromSlot = first.ThroughSlot - 1
	// Simulate on-disk semantic corruption in place. The production publisher
	// deliberately refuses to replace an existing durable decision.
	require.NoError(t, os.WriteFile(secondPath, manifest.encode(), 0o644))

	db = reopenFoldTestDbWithFreshMutableIndex(t, db, dir)
	defer db.CloseDb()
	recovered, err := db.RecoverFoldState()
	require.ErrorContains(t, err, "breaks replay slot chain")
	assert.Empty(t, recovered.OrphansRemoved)
	assert.FileExists(t, secondPath)
	assert.FileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(second.ThroughSlot, second.FileId)))
}

func TestRecoverFoldStateCorruptCompactManifestPreservesIndexLiveOutput(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	big := make([]byte, 2048)
	source := commitTestBatch(t, db, 110,
		foldAcct(1, 100, big),
		foldAcct(2, 200, []byte("index-live-output")),
	)
	commitTestBatch(t, db, 120, foldAcct(1, 101, big))
	commitTestBatch(t, db, 130, foldAcct(1, 102, big))
	stats, err := db.CompactOnce(CompactionConfig{
		RewindHorizonBatches: 1,
		MinDeadFraction:      0.5,
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.FilesCompacted)

	indexed, ok := db.Index.Lookup(solana.PublicKey{2})
	require.True(t, ok)
	require.NotEqual(t, source.FileId, indexed.Entry.FileId)
	dataPath := filepath.Join(db.AcctsDir, SegmentDataName(indexed.Entry.Slot, indexed.Entry.FileId))
	manifestPath := segmentManifestPath(db.AcctsDir, indexed.Entry.Slot, indexed.Entry.FileId)
	manifest, err := ReadSegmentManifest(manifestPath)
	require.NoError(t, err)
	require.Equal(t, ManifestKindCompact, manifest.Kind)

	encoded, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	encoded[0] ^= 0xff // corrupt the final compact manifest's fixed header
	require.NoError(t, os.WriteFile(manifestPath, encoded, 0o644))

	recovered, err := db.RecoverFoldState()
	require.ErrorIs(t, err, ErrTornManifest)
	assert.Empty(t, recovered.OrphansRemoved)
	assert.FileExists(t, manifestPath)
	assert.FileExists(t, dataPath, "orphan GC must fail closed around a malformed final manifest")
	assert.Equal(t, []byte("index-live-output"), mustColdRead(t, db, 130, solana.PublicKey{2}).Data)
}

func TestRecoverFoldStateSegmentVerificationFailurePreservesDurableFold(t *testing.T) {
	db, dir := newFoldTestDb(t)
	commitTestBatch(t, db, 110, foldAcct(1, 1, []byte("first")))
	second := commitTestBatch(t, db, 120, foldAcct(2, 2, []byte("second")))
	manifestPath := segmentManifestPath(db.AcctsDir, second.ThroughSlot, second.FileId)
	dataPath := filepath.Join(db.AcctsDir, SegmentDataName(second.ThroughSlot, second.FileId))
	data, err := os.ReadFile(dataPath)
	require.NoError(t, err)
	data[len(data)-1] ^= 0xff
	require.NoError(t, os.WriteFile(dataPath, data, 0o644))

	db = reopenFoldTestDbWithFreshMutableIndex(t, db, dir)
	defer db.CloseDb()
	recovered, err := db.RecoverFoldState()
	require.ErrorContains(t, err, "segment data crc mismatch")
	assert.Empty(t, recovered.OrphansRemoved)
	assert.FileExists(t, manifestPath)
	assert.FileExists(t, dataPath)
}

func TestManifestOnlyCommitPreservesReplaySlotChain(t *testing.T) {
	db, dir := newFoldTestDb(t)
	first := commitTestBatch(t, db, 110, foldAcct(1, 1, nil))
	second, err := db.CommitBatch(nil, 120, map[uint64][32]byte{120: bh(120)}, []byte("empty"))
	require.NoError(t, err)
	manifest, err := ReadSegmentManifest(
		segmentManifestPath(db.AcctsDir, second.ThroughSlot, second.FileId),
	)
	require.NoError(t, err)
	assert.Equal(t, first.ThroughSlot, manifest.FromSlot)

	db = reopenFoldTestDbWithFreshMutableIndex(t, db, dir)
	defer db.CloseDb()
	recovered, err := db.RecoverFoldState()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), recovered.BatchSeq)
	assert.Equal(t, second.ThroughSlot, recovered.DurableThrough)
	assert.Equal(t, []uint64{1, 2}, recovered.ReplayedBatches)
}

func TestRewindRejectsAmbiguousManifestsBeforeParking(t *testing.T) {
	t.Run("duplicate sequence", func(t *testing.T) {
		db, _ := newFoldTestDb(t)
		defer db.CloseDb()
		first := commitTestBatch(t, db, 110, foldAcct(1, 1, nil))
		second := commitTestBatch(t, db, 120, foldAcct(2, 2, nil))
		third := commitTestBatch(t, db, 130, foldAcct(3, 3, nil))
		original := segmentManifestPath(db.AcctsDir, second.ThroughSlot, second.FileId)
		manifest, err := ReadSegmentManifest(original)
		require.NoError(t, err)
		manifest.ThroughSlot = 999
		manifest.FileId += 1000
		require.NoError(t, WriteSegmentManifest(db.AcctsDir, manifest))

		_, err = db.RewindToBatchBoundary(first.ThroughSlot)
		require.ErrorContains(t, err, "duplicate fold manifest sequence")
		assertFoldSuffixNotParked(t, db.AcctsDir, second, third)
	})

	t.Run("duplicate through slot", func(t *testing.T) {
		db, _ := newFoldTestDb(t)
		defer db.CloseDb()
		first := commitTestBatch(t, db, 110, foldAcct(1, 1, nil))
		second := commitTestBatch(t, db, 120, foldAcct(2, 2, nil))
		third := commitTestBatch(t, db, 130, foldAcct(3, 3, nil))
		manifest, err := ReadSegmentManifest(
			segmentManifestPath(db.AcctsDir, second.ThroughSlot, second.FileId),
		)
		require.NoError(t, err)
		manifest.BatchSeq = 99
		manifest.FileId += 1000
		require.NoError(t, WriteSegmentManifest(db.AcctsDir, manifest))

		_, err = db.RewindToBatchBoundary(first.ThroughSlot)
		require.ErrorContains(t, err, "duplicate fold through slot")
		assertFoldSuffixNotParked(t, db.AcctsDir, second, third)
	})

	t.Run("corrupt header", func(t *testing.T) {
		db, _ := newFoldTestDb(t)
		defer db.CloseDb()
		first := commitTestBatch(t, db, 110, foldAcct(1, 1, nil))
		second := commitTestBatch(t, db, 120, foldAcct(2, 2, nil))
		third := commitTestBatch(t, db, 130, foldAcct(3, 3, nil))
		path := segmentManifestPath(db.AcctsDir, third.ThroughSlot, third.FileId)
		encoded, err := os.ReadFile(path)
		require.NoError(t, err)
		encoded[25] ^= 0xff
		require.NoError(t, os.WriteFile(path, encoded, 0o644))

		_, err = db.RewindToBatchBoundary(first.ThroughSlot)
		require.ErrorContains(t, err, "not fully CRC-valid")
		assertFoldSuffixNotParked(t, db.AcctsDir, second, third)
	})
}

func assertFoldSuffixNotParked(t *testing.T, acctsDir string, batches ...BatchCommitResult) {
	t.Helper()
	for _, batch := range batches {
		path := segmentManifestPath(acctsDir, batch.ThroughSlot, batch.FileId)
		assert.FileExists(t, path)
		assert.NoFileExists(t, path+".rewound")
	}
}
