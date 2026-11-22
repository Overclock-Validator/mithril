//go:build linux

package accountsdb

import (
	"fmt"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// The production directory contained over half a million bootstrap files.
// Sending a sparse segment back to that queue after its first successful
// reclamation delayed its next visit long after its undo records expired.
func TestSparseQueueRevisitsSmallSegmentsAheadOfBootstrapBacklog(t *testing.T) {
	const bootstrapFiles = 520_000
	items := make([]compactCandidate, 0, bootstrapFiles+3)
	for i := range bootstrapFiles {
		items = append(items, compactCandidate{
			name: fmt.Sprintf("466000000.%d", i), size: 1 << 20, allocated: 1 << 20,
		})
	}
	for i, allocated := range []int64{4 << 10, 16 << 20} {
		items = append(items, compactCandidate{
			name: fmt.Sprintf("467000000.%d", i), size: 256 << 20, allocated: allocated,
			manifestPath: "manifest", alreadySparse: true, sparseRecords: true,
			scanOrder: uint64(i + 1),
		})
	}
	items = append(items, compactCandidate{
		name: "467000001.1", size: 128 << 20, allocated: 128 << 20, manifestPath: "manifest",
	})
	q := newCompactCandidateQueue(items, "", "", true, false)
	for _, name := range []string{"467000000.0", "467000001.1", "467000000.1"} {
		c, ok := q.next()
		require.True(t, ok)
		require.Equal(t, name, c.name, "tiny and low-allocation sparse files must share prompt retry service with new folds")
	}
}

// Sparse files retain their retry ages even after allocation falls below
// 8 MiB, so every advancing head cannot put the same first filename ahead of
// all other sparse segments again.
func TestSparseQueuePreservesRetryAgeAcrossFolds(t *testing.T) {
	verifyEveryCycle(t)
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	r1 := commitTestBatch(t, db, 110, foldAcct(1, 10, []byte("first")))
	r2 := commitTestBatch(t, db, 120, foldAcct(2, 20, []byte("second")))
	commitTestBatch(t, db, 130, foldAcct(3, 30, []byte("boundary")))
	commitTestBatch(t, db, 140, foldAcct(4, 40, []byte("head")))
	for _, r := range []BatchCommitResult{r1, r2} {
		m, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, r.ThroughSlot, r.FileId))
		require.NoError(t, err)
		m.Kind, m.BatchSeq, m.DataCRC = ManifestKindSparse, 0, 0
		m.Bankhashes, m.ResumeCtx = nil, nil
		require.NoError(t, WriteSegmentManifest(db.AcctsDir, m))
	}
	cfg := CompactionConfig{RewindHorizonBatches: 1, MaxFilesPerCycle: 1}
	_, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	firstAge := db.compactLastScan[r1.FileId]
	require.Positive(t, firstAge)
	commitTestBatch(t, db, 150, foldAcct(5, 50, []byte("new head")))
	_, err = db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Equal(t, firstAge, db.compactLastScan[r1.FileId], "a small sparse file must retain its scan age across head changes")
	require.Greater(t, db.compactLastScan[r2.FileId], firstAge, "the other sparse file gets the next retry")
	require.Zero(t, db.compactInventoryDrift)
	require.Equal(t, []byte("first"), mustColdRead(t, db, 150, solana.PublicKey{1}).Data)
	require.Equal(t, []byte("second"), mustColdRead(t, db, 150, solana.PublicKey{2}).Data)
}

func TestSparseQueueServicesSmallSegmentWithUndoPin(t *testing.T) {
	verifyEveryCycle(t)
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	r1 := commitTestBatch(t, db, 110, foldAcct(1, 10, []byte("undo value")))
	commitTestBatch(t, db, 120, foldAcct(2, 20, []byte("boundary")))
	commitTestBatch(t, db, 130, foldAcct(1, 30, []byte("current value")))
	m, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, 110, r1.FileId))
	require.NoError(t, err)
	m.Kind, m.BatchSeq, m.DataCRC = ManifestKindSparse, 0, 0
	m.Bankhashes, m.ResumeCtx = nil, nil
	require.NoError(t, WriteSegmentManifest(db.AcctsDir, m))
	stats, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1, MaxFilesPerCycle: 1})
	require.NoError(t, err)
	require.Equal(t, 1, stats.CandidatesScanned, "a small sparse segment must remain eligible while its exact undo record is pinned")
	require.Positive(t, db.compactLastScan[r1.FileId])
	require.Zero(t, db.compactInventoryDrift)
	_, err = db.RewindToBatchBoundary(120)
	require.NoError(t, err)
	require.Equal(t, []byte("undo value"), mustColdRead(t, db, 120, solana.PublicKey{1}).Data)
}
