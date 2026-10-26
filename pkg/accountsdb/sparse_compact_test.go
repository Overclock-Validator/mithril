//go:build linux

package accountsdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestSparseCompactionRetainsExactLiveAndUndoRecords(t *testing.T) {
	for _, walOff := range []bool{false, true} {
		for _, crash := range []string{"none", "marker", "punch"} {
			t.Run(fmt.Sprintf("wal_off=%t/crash=%s", walOff, crash), func(t *testing.T) {
				previous := DisableIndexWAL
				DisableIndexWAL = walOff
				t.Cleanup(func() { DisableIndexWAL = previous })
				db, dir := newFoldTestDb(t)
				t.Cleanup(func() { db.CloseDb() })
				big := bytes.Repeat([]byte{0xa5}, 16<<20)
				undo := bytes.Repeat([]byte{0x71}, 12<<10)
				r1 := commitTestBatch(t, db, 110, foldAcct(1, 10, big), foldAcct(2, 20, undo), foldAcct(3, 30, []byte("live")))
				commitTestBatch(t, db, 120, foldAcct(1, 11, []byte("newer")))
				commitTestBatch(t, db, 130, foldAcct(1, 12, []byte("boundary")))
				commitTestBatch(t, db, 140, foldAcct(1, 13, []byte("head")), foldAcct(2, 21, []byte("updated")))
				indexBefore := snapshotIndex(t, db)
				source := filepath.Join(db.AcctsDir, SegmentDataName(110, r1.FileId))
				before, err := os.Stat(source)
				require.NoError(t, err)
				if crash == "marker" {
					db.compactHooks.afterSparseMarker = func() { panic("simulated crash") }
				} else if crash == "punch" {
					db.compactHooks.afterSparsePunch = func() { panic("simulated crash") }
				}
				cfg := CompactionConfig{RewindHorizonBatches: 1}
				if crash == "none" {
					stats, err := db.CompactOnce(cfg)
					require.NoError(t, err)
					require.Equal(t, 1, stats.FilesSparsified)
					require.Greater(t, stats.BytesReclaimed, int64(15<<20))
				} else {
					require.Panics(t, func() { _, _ = db.CompactOnce(cfg) })
				}
				db = reopenFoldTestDb(t, db, dir)
				recovered, err := db.RecoverFoldState()
				require.NoError(t, err)
				require.Equal(t, uint64(140), recovered.DurableThrough)
				require.Equal(t, bh(140), recovered.RootedBankhash)
				_, err = db.CompactOnce(cfg)
				require.NoError(t, err, "partial reclamation must be retryable")
				require.Equal(t, indexBefore, snapshotIndex(t, db), "sparse reclamation must not move account locations")
				after, err := os.Stat(source)
				require.NoError(t, err)
				require.Equal(t, before.Size(), after.Size(), "logical offsets and file length remain stable")
				require.Less(t, allocatedBytesFromInfo(after), allocatedBytesFromInfo(before)-(15<<20))
				require.Equal(t, []byte("live"), mustColdRead(t, db, 140, solana.PublicKey{3}).Data)
				require.Equal(t, []byte("head"), mustColdRead(t, db, 140, solana.PublicKey{1}).Data)
				marker, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, 110, r1.FileId))
				require.NoError(t, err)
				require.Equal(t, ManifestKindCompact, marker.Kind)
				require.Zero(t, marker.DataCRC)
				// Header-only parsing remains compatible with the old appendvec
				// reader; only obsolete payload bytes can differ.
				data, err := os.ReadFile(source)
				require.NoError(t, err)
				keys, _, _, err := BuildIndexEntriesFromAppendVecs(data, uint64(len(data)), 110, r1.FileId)
				require.NoError(t, err)
				require.Len(t, keys, 3)
				_, err = db.RewindToBatchBoundary(130)
				require.NoError(t, err)
				require.Equal(t, undo, mustColdRead(t, db, 130, solana.PublicKey{2}).Data, "undo record sharing the reclaimed file must be byte-exact")
				require.Equal(t, []byte("boundary"), mustColdRead(t, db, 130, solana.PublicKey{1}).Data)
			})
		}
	}
}

func TestSparseCompactionRefusesUnknownRewindPins(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	r1 := commitTestBatch(t, db, 110, foldAcct(1, 10, bytes.Repeat([]byte{1}, 16<<20)), foldAcct(2, 20, []byte("old")))
	commitTestBatch(t, db, 120, foldAcct(1, 11, nil))
	commitTestBatch(t, db, 130, foldAcct(1, 12, nil))
	r4 := commitTestBatch(t, db, 140, foldAcct(2, 21, []byte("new")))
	source := filepath.Join(db.AcctsDir, SegmentDataName(110, r1.FileId))
	before, err := os.Stat(source)
	require.NoError(t, err)
	manifestPath := segmentManifestPath(db.AcctsDir, 140, r4.FileId)
	m, err := ReadSegmentManifest(manifestPath)
	require.NoError(t, err)
	m.Records[0].Prev.Offset++ // valid manifest CRC, invalid undo pointer
	require.NoError(t, WriteSegmentManifest(db.AcctsDir, m))
	_, err = db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1})
	require.Error(t, err)
	after, err := os.Stat(source)
	require.NoError(t, err)
	require.Equal(t, allocatedBytesFromInfo(before), allocatedBytesFromInfo(after))
	// A header that makes a retained fold look retired must not hide its undo
	// pointers. The missing in-horizon sequence makes reclamation fail closed.
	m.Records[0].Prev.Offset--
	m.Kind, m.BatchSeq = ManifestKindCompact, 0
	require.NoError(t, WriteSegmentManifest(db.AcctsDir, m))
	_, err = db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1})
	require.ErrorContains(t, err, "missing fold manifest seq 4")
	// An unreadable in-horizon manifest must also abort, never guess at pins.
	require.NoError(t, os.WriteFile(manifestPath, []byte("torn"), 0600))
	_, err = db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1})
	require.ErrorContains(t, err, "cannot establish rewind pins")
}

func TestSparseCompactionWaitsForOlderIndexReaders(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	big := bytes.Repeat([]byte{0x5a}, 16<<20)
	r1 := commitTestBatch(t, db, 110, foldAcct(1, 10, big), foldAcct(2, 20, []byte("keep")))
	// Pin a reader from before the overwrite, as GetAccountsBatch does.
	db.appendVecReadMu.RLock()
	locked := true
	defer func() {
		if locked {
			db.appendVecReadMu.RUnlock()
		}
	}()
	old := db.Index.NewSnapshot()
	defer old.Close()
	commitTestBatch(t, db, 120, foldAcct(1, 11, nil))
	commitTestBatch(t, db, 130, foldAcct(1, 12, nil))
	commitTestBatch(t, db, 140, foldAcct(1, 13, nil))
	marker := make(chan struct{})
	db.compactHooks.afterSparseMarker = func() { close(marker) }
	done := make(chan error, 1)
	go func() { _, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1}); done <- err }()
	select {
	case <-marker:
	case <-time.After(10 * time.Second):
		t.Fatal("compactor did not reach marker")
	}
	select {
	case err := <-done:
		t.Fatalf("compaction passed a pinned reader: %v", err)
	default:
	}
	entry, closer, err := old.Get(solana.PublicKey{1}.Bytes())
	require.NoError(t, err)
	location, err := UnmarshalAcctIdxEntryValue(entry)
	closer.Close()
	require.NoError(t, err)
	require.Equal(t, r1.FileId, location.FileId)
	f, err := os.Open(filepath.Join(db.AcctsDir, SegmentDataName(110, r1.FileId)))
	require.NoError(t, err)
	read := make([]byte, len(big))
	_, err = f.ReadAt(read, int64(location.Offset)+hdrLen)
	f.Close()
	require.NoError(t, err)
	require.Equal(t, big, read, "older reader must finish before obsolete pages are freed")
	db.appendVecReadMu.RUnlock()
	locked = false
	require.NoError(t, <-done)
}

func TestSparseCompactionPrioritizesUnreclaimedSegments(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	big := bytes.Repeat([]byte{0x55}, 16<<20)
	r1 := commitTestBatch(t, db, 110, foldAcct(1, 10, big), foldAcct(2, 20, big))
	r2 := commitTestBatch(t, db, 120, foldAcct(1, 11, big), foldAcct(3, 30, []byte("keep")))
	commitTestBatch(t, db, 130, foldAcct(1, 12, nil))
	commitTestBatch(t, db, 140, foldAcct(1, 13, nil))
	cfg := CompactionConfig{RewindHorizonBatches: 1, MaxFilesPerCycle: 1}
	first, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Equal(t, 1, first.FilesSparsified)
	retained, err := os.Stat(filepath.Join(db.AcctsDir, SegmentDataName(110, r1.FileId)))
	require.NoError(t, err)
	require.Greater(t, allocatedBytesFromInfo(retained), sparseCompactionMinSize)
	// Even after restart resets the cursor, a partly reclaimed segment must
	// not consume the next cycle ahead of newly eligible high-volume data.
	db.sparseCursor = ""
	second, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Equal(t, 1, second.FilesSparsified)
	marker, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, 120, r2.FileId))
	require.NoError(t, err)
	require.Equal(t, ManifestKindCompact, marker.Kind)
	require.Equal(t, big, mustColdRead(t, db, 140, solana.PublicKey{2}).Data)
}

func TestSparseCompactionRevisitsExpiredUndoAheadOfNewSegments(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	big := bytes.Repeat([]byte{0x55}, 16<<20)
	r1 := commitTestBatch(t, db, 110, foldAcct(1, 10, big), foldAcct(2, 20, big), foldAcct(3, 30, []byte("live")))
	commitTestBatch(t, db, 120, foldAcct(1, 11, nil))
	commitTestBatch(t, db, 130, foldAcct(1, 12, nil))
	commitTestBatch(t, db, 140, foldAcct(2, 21, nil), foldAcct(9, 90, big))
	cfg := CompactionConfig{RewindHorizonBatches: 1, MaxFilesPerCycle: 1}
	first, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Greater(t, first.BytesReclaimed, int64(15<<20))
	source := filepath.Join(db.AcctsDir, SegmentDataName(110, r1.FileId))
	pinned, err := os.Stat(source)
	require.NoError(t, err)
	require.Greater(t, allocatedBytesFromInfo(pinned), int64(16<<20), "undo version is still pinned")

	// New large segments arrive continuously, while the old undo record ages
	// out. Even with one-file cycles, its second pass must get a turn.
	commitTestBatch(t, db, 150, foldAcct(9, 91, big))
	commitTestBatch(t, db, 160, foldAcct(9, 92, big))
	commitTestBatch(t, db, 170, foldAcct(9, 93, big))
	second, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Greater(t, second.BytesReclaimed, int64(15<<20))
	after, err := os.Stat(source)
	require.NoError(t, err)
	require.Less(t, allocatedBytesFromInfo(after), int64(1<<20), "expired undo payload must not be stranded behind fresh/bootstrap files")
	require.Equal(t, []byte("live"), mustColdRead(t, db, 170, solana.PublicKey{3}).Data)
	third, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Equal(t, 1, third.FilesSparsified, "newly eligible segments also get a turn")
	require.Greater(t, third.BytesReclaimed, int64(15<<20))
	_, err = db.RewindToBatchBoundary(160)
	require.NoError(t, err)
	require.Equal(t, big, mustColdRead(t, db, 160, solana.PublicKey{9}).Data)
	require.Equal(t, uint64(92), mustColdRead(t, db, 160, solana.PublicKey{9}).Lamports)
}

func TestSparseCompactionSkipsUnchangedHeadAndRechecksAfterFold(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	big := bytes.Repeat([]byte{0x55}, 16<<20)
	commitTestBatch(t, db, 110, foldAcct(1, 10, big), foldAcct(2, 20, big))
	commitTestBatch(t, db, 120, foldAcct(1, 11, nil))
	commitTestBatch(t, db, 130, foldAcct(1, 12, nil))
	cfg := CompactionConfig{RewindHorizonBatches: 1, MaxFilesPerCycle: 1}
	first, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Equal(t, 1, first.FilesSparsified)
	second, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Zero(t, second.CandidatesScanned, "a retained large live account does not require repeated idle scans")
	commitTestBatch(t, db, 140, foldAcct(2, 21, nil))
	commitTestBatch(t, db, 150, foldAcct(1, 13, nil))
	_, err = db.CompactOnce(cfg)
	require.NoError(t, err)
	commitTestBatch(t, db, 160, foldAcct(1, 14, nil))
	third, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	require.GreaterOrEqual(t, third.FilesDeleted, 1)
}

func TestSparseCompactionRecurringChurnStaysBounded(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	big := bytes.Repeat([]byte{0x37}, 8<<20)
	cfg := CompactionConfig{RewindHorizonBatches: 2, MaxFilesPerCycle: 1}
	for step := uint64(1); step <= 64; step++ {
		// One hot account and four less frequently updated accounts leave
		// partial files whose live/undo versions expire on later cycles.
		commitTestBatch(t, db, 100+step, foldAcct(1, step, big), foldAcct(byte(2+step%4), step, big))
		for range 3 {
			_, err := db.CompactOnce(cfg)
			require.NoError(t, err)
		}
		var allocated int64
		entries, err := os.ReadDir(db.AcctsDir)
		require.NoError(t, err)
		for _, e := range entries {
			if !isDataFileName(e.Name()) {
				continue
			}
			info, err := e.Info()
			require.NoError(t, err)
			allocated += allocatedBytesFromInfo(info)
		}
		require.Less(t, allocated, int64(256<<20), "retained disk bytes must stay bounded through repeated pin expiry (step %d)", step)
	}
	_, err := db.RewindToBatchBoundary(162)
	require.NoError(t, err)
	require.Equal(t, uint64(62), mustColdRead(t, db, 162, solana.PublicKey{1}).Lamports)
	require.Equal(t, big, mustColdRead(t, db, 162, solana.PublicKey{1}).Data)
}

func TestSparseCompactionAlsoServicesSmallFilesDuringLargeChurn(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	old := commitTestBatch(t, db, 110, foldAcct(200, 10, []byte("obsolete")))
	big := bytes.Repeat([]byte{0x37}, 8<<20)
	commitTestBatch(t, db, 120, foldAcct(200, 11, nil), foldAcct(1, 1, big))
	commitTestBatch(t, db, 130, foldAcct(1, 2, big))
	commitTestBatch(t, db, 140, foldAcct(1, 3, big))
	source := filepath.Join(db.AcctsDir, SegmentDataName(110, old.FileId))
	for step := uint64(1); step <= 8; step++ {
		commitTestBatch(t, db, 140+step, foldAcct(1, 3+step, big))
		_, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1, MaxFilesPerCycle: 1})
		require.NoError(t, err)
	}
	_, err := os.Stat(source)
	require.True(t, os.IsNotExist(err), "continuous large segments must not starve the small-file queue")
	require.Equal(t, big, mustColdRead(t, db, 148, solana.PublicKey{1}).Data)
}
