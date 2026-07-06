package accountsdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An interrupted RewindToBatchBoundary — suffix manifests parked and the index
// meta rolled back, but the state file not yet updated — is detected by
// recovery (RewindInProgress) and surfaces the rewound boundary's context, so
// startup completes the rewind instead of condemning the store as data loss.
func TestRecoveryDetectsInterruptedRewind(t *testing.T) {
	db, dir := newFoldTestDb(t)
	r1 := commitTestBatch(t, db, 110, foldAcct(1, 100, []byte("v1")))
	r2 := commitTestBatch(t, db, 120, foldAcct(1, 111, []byte("v2")))
	r3 := commitTestBatch(t, db, 130, foldAcct(1, 122, []byte("v3")))

	// Simulate the interruption: park the suffix manifests and roll the index
	// meta back to batch 1 (slot 110), as the rewind's atomic step would, but
	// die before moving segments / updating the state file.
	for _, r := range []BatchCommitResult{r2, r3} {
		p := segmentManifestPath(db.AcctsDir, r.ThroughSlot, r.FileId)
		require.NoError(t, os.Rename(p, p+".rewound"))
	}
	require.NoError(t, db.Index.Set(metaKeyLastBatch,
		encodeFoldMeta(foldMeta{BatchSeq: 1, ThroughSlot: 110, FileId: r1.FileId}), pebble.Sync))

	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	rec, err := db.RecoverFoldState()
	require.NoError(t, err)
	assert.True(t, rec.RewindInProgress, "parked .rewound manifests must be detected")
	assert.Equal(t, uint64(110), rec.DurableThrough, "store sits at the rewound boundary")
	assert.Equal(t, []byte("ctx-110"), rec.ResumeCtx, "the rewound boundary's context is available for reconcile")
	// The parked suffix segments are NOT orphan-GC'd (a re-run can still use them).
	assert.FileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(130, r3.FileId)))

	// Completion is idempotent: rewinding to the boundary the store already sits
	// at finalizes the parked leftovers, so RewindInProgress clears.
	res, err := db.RewindToBatchBoundary(110)
	require.NoError(t, err)
	assert.Equal(t, uint64(110), res.NewThrough)
	assert.Equal(t, []byte("ctx-110"), res.ResumeCtx)
	rec2, err := db.RecoverFoldState()
	require.NoError(t, err)
	assert.False(t, rec2.RewindInProgress, "early-return finalized the parked leftovers")
}

// Crash after Step 1 (park) but BEFORE Step 2 (the atomic meta rollback): the
// meta still names the head, so the store never moved — completing the rewind
// must run the full rollback to the target boundary and clear the parked state.
func TestRewindCompletesAfterParkOnlyInterruption(t *testing.T) {
	db, dir := newFoldTestDb(t)
	commitTestBatch(t, db, 110, foldAcct(1, 100, []byte("v1")))
	r2 := commitTestBatch(t, db, 120, foldAcct(1, 111, []byte("v2")))
	r3 := commitTestBatch(t, db, 130, foldAcct(1, 122, []byte("v3")))

	// Park the suffix manifests but leave the index meta at head (130).
	for _, r := range []BatchCommitResult{r2, r3} {
		p := segmentManifestPath(db.AcctsDir, r.ThroughSlot, r.FileId)
		require.NoError(t, os.Rename(p, p+".rewound"))
	}

	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	rec, err := db.RecoverFoldState()
	require.NoError(t, err)
	require.True(t, rec.RewindInProgress, "parked manifests detected even though meta was not rolled back")

	// Complete to boundary 110 (the highest non-parked boundary): full rollback.
	res, err := db.RewindToBatchBoundary(110)
	require.NoError(t, err)
	assert.Equal(t, uint64(110), res.NewThrough)
	assert.Equal(t, []byte("ctx-110"), res.ResumeCtx)
	assert.Equal(t, uint64(100), mustColdRead(t, db, 110, solana.PublicKey{1}).Lamports, "index rolled back to the boundary")

	rec2, err := db.RecoverFoldState()
	require.NoError(t, err)
	assert.False(t, rec2.RewindInProgress, "parked manifests cleared after completion")
	meta, ok, err := db.readFoldMeta()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint64(110), meta.ThroughSlot)
}

// snapshotIndex captures the full account index (pubkey -> entry), excluding
// the fold meta row, for bit-exact before/after comparisons.
func snapshotIndex(t *testing.T, db *AccountsDb) map[[32]byte]AccountIndexEntry {
	t.Helper()
	iter, err := db.Index.NewIter(nil)
	require.NoError(t, err)
	defer iter.Close()
	out := make(map[[32]byte]AccountIndexEntry)
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		if len(k) != 32 {
			continue // fold meta row
		}
		var key [32]byte
		copy(key[:], k)
		e, uerr := UnmarshalAcctIdxEntry(iter.Value())
		require.NoError(t, uerr)
		out[key] = *e
	}
	return out
}

func commitTestBatch(t *testing.T, db *AccountsDb, through uint64, delta ...*accounts.Account) BatchCommitResult {
	t.Helper()
	res, err := db.CommitBatch(
		foldDeltas(accounts.SlotDelta{Slot: through, Delta: delta}),
		through,
		map[uint64][32]byte{through: bh(through)},
		[]byte(fmt.Sprintf("ctx-%d", through)),
	)
	require.NoError(t, err)
	return res
}

// Rewind restores the index bit-exactly to each earlier batch boundary
// (compared against snapshots taken when the boundary was the head), returns
// the boundary's manifest context, and rolls the fold meta back.
func TestRewindRoundTripAgainstShadowSnapshots(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	type boundary struct {
		through uint64
		index   map[[32]byte]AccountIndexEntry
	}
	var boundaries []boundary
	snap := func(through uint64) {
		boundaries = append(boundaries, boundary{through, snapshotIndex(t, db)})
	}

	commitTestBatch(t, db, 110, foldAcct(1, 100, []byte("a1")), foldAcct(2, 200, []byte("b1")), foldAcct(3, 300, []byte("c1")))
	snap(110)
	commitTestBatch(t, db, 120, foldAcct(1, 111, []byte("a2-longer")), foldAcct(4, 400, nil))
	snap(120)
	commitTestBatch(t, db, 130, foldAcct(2, 222, []byte("b3")), foldAcct(4, 444, []byte("d3")))
	snap(130)
	commitTestBatch(t, db, 140, foldAcct(1, 133, []byte("a4")), foldAcct(5, 500, []byte("e4")))
	snap(140)

	// Boundary-by-boundary, newest-first.
	for i := len(boundaries) - 2; i >= 0; i-- {
		want := boundaries[i]
		res, err := db.RewindToBatchBoundary(want.through)
		require.NoError(t, err)
		assert.Equal(t, want.through, res.NewThrough)
		assert.Equal(t, []byte(fmt.Sprintf("ctx-%d", want.through)), res.ResumeCtx, "manifest context at the boundary")
		assert.Equal(t, want.index, snapshotIndex(t, db), "index must be bit-equal to the boundary snapshot")

		meta, ok, merr := db.readFoldMeta()
		require.NoError(t, merr)
		require.True(t, ok)
		assert.Equal(t, want.through, meta.ThroughSlot)
	}

	// Values as of the oldest boundary; keys born later are gone.
	assert.Equal(t, uint64(100), mustColdRead(t, db, 110, solana.PublicKey{1}).Lamports)
	assert.Equal(t, uint64(200), mustColdRead(t, db, 110, solana.PublicKey{2}).Lamports)
	_, err := db.GetAccount(110, solana.PublicKey{4})
	assert.ErrorIs(t, err, ErrNoAccount, "key first written after the boundary must vanish")

	// Undone segments were parked for forensics, and folding resumes cleanly
	// from the rewound boundary (fresh fileIds, contiguous seq).
	parked, err := os.ReadDir(filepath.Join(db.AcctsDir, "rewound"))
	require.NoError(t, err)
	assert.NotEmpty(t, parked)
	res := commitTestBatch(t, db, 118, foldAcct(1, 118, []byte("again")))
	assert.Equal(t, uint64(2), res.BatchSeq, "seq continues from the rewound boundary")
	assert.Equal(t, uint64(118), mustColdRead(t, db, 118, solana.PublicKey{1}).Lamports)
}

// A multi-batch rewind in ONE call lands on the same state as the incremental
// path: a key overwritten in several undone batches ends at its pre-suffix entry.
func TestRewindMultiBatchSingleCall(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	commitTestBatch(t, db, 110, foldAcct(1, 100, []byte("v1")), foldAcct(2, 200, []byte("w1")))
	want := snapshotIndex(t, db)
	commitTestBatch(t, db, 120, foldAcct(1, 111, []byte("v2")))
	commitTestBatch(t, db, 130, foldAcct(1, 122, []byte("v3")), foldAcct(2, 233, []byte("w3")))
	commitTestBatch(t, db, 140, foldAcct(1, 144, []byte("v4")))

	res, err := db.RewindToBatchBoundary(110)
	require.NoError(t, err)
	assert.Equal(t, 3, res.UndoneBatches)
	assert.Equal(t, want, snapshotIndex(t, db))
	assert.Equal(t, uint64(100), mustColdRead(t, db, 110, solana.PublicKey{1}).Lamports)
	assert.Equal(t, uint64(200), mustColdRead(t, db, 110, solana.PublicKey{2}).Lamports)
}

// Crash matrix C6: a rewind interrupted right after parking the suffix
// manifests (step 1) must (a) leave recovery unconfused — the committed head
// stays authoritative, nothing is orphan-deleted — and (b) complete when the
// rewind is re-issued, picking the parked manifests back up.
func TestRewindResumesAfterInterruptedParking(t *testing.T) {
	db, dir := newFoldTestDb(t)

	commitTestBatch(t, db, 110, foldAcct(1, 100, []byte("v1")))
	r2 := commitTestBatch(t, db, 120, foldAcct(1, 111, []byte("v2")))
	r3 := commitTestBatch(t, db, 130, foldAcct(1, 122, []byte("v3")))

	// Simulate the crash: park the suffix manifests exactly as step 1 does,
	// then "restart" (reopen + fold recovery), as node startup would.
	for _, r := range []BatchCommitResult{r2, r3} {
		p := segmentManifestPath(db.AcctsDir, r.ThroughSlot, r.FileId)
		require.NoError(t, os.Rename(p, p+".rewound"))
	}
	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	rec, err := db.RecoverFoldState()
	require.NoError(t, err)
	assert.Equal(t, uint64(130), rec.DurableThrough, "index meta stays authoritative after park-only crash")
	assert.FileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(120, r2.FileId)), "parked batches' segments must not be orphan-GC'd")
	assert.FileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(130, r3.FileId)))

	// Re-issued rewind resumes through the parked manifests.
	res, err := db.RewindToBatchBoundary(110)
	require.NoError(t, err)
	assert.Equal(t, uint64(110), res.NewThrough)
	assert.Equal(t, 2, res.UndoneBatches)
	assert.Equal(t, uint64(100), mustColdRead(t, db, 110, solana.PublicKey{1}).Lamports)

	// Idempotence of completion: rewinding to the same boundary is a no-op
	// that still reports the boundary's context.
	res2, err := db.RewindToBatchBoundary(110)
	require.NoError(t, err)
	assert.Equal(t, uint64(110), res2.NewThrough)
	assert.Equal(t, 0, res2.UndoneBatches)
	assert.Equal(t, []byte("ctx-110"), res2.ResumeCtx)
}

// Rewind fails closed: non-boundary targets, horizons broken by a missing
// intermediate manifest, and never-folded stores all refuse cleanly.
func TestRewindRefusalCases(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	commitTestBatch(t, db, 110, foldAcct(1, 100, []byte("v1")))
	r2 := commitTestBatch(t, db, 120, foldAcct(1, 111, []byte("v2")))
	commitTestBatch(t, db, 130, foldAcct(1, 122, []byte("v3")))

	_, err := db.RewindToBatchBoundary(115)
	require.ErrorContains(t, err, "no fold boundary", "mid-batch slot is not a boundary")

	require.NoError(t, os.Remove(segmentManifestPath(db.AcctsDir, 120, r2.FileId)))
	_, err = db.RewindToBatchBoundary(110)
	require.ErrorContains(t, err, "missing", "gap in the undo chain must refuse, not skip")

	fresh, _ := newFoldTestDb(t)
	defer fresh.CloseDb()
	_, err = fresh.RewindToBatchBoundary(100)
	require.ErrorContains(t, err, "no fold meta")
}

func TestListRewindPointsAscending(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	commitTestBatch(t, db, 110, foldAcct(1, 100, []byte("v1")))
	commitTestBatch(t, db, 120, foldAcct(1, 111, []byte("v2")))
	points, err := db.ListRewindPoints()
	require.NoError(t, err)
	require.Len(t, points, 2)
	assert.Equal(t, uint64(110), points[0].ThroughSlot)
	assert.Equal(t, uint64(120), points[1].ThroughSlot)
	assert.Equal(t, bh(110), points[0].Bankhash)
	assert.Equal(t, bh(120), points[1].Bankhash)
}

// Compaction: live records move to a fresh manifest-backed output and the
// source (plus its manifest) is deleted; fully-dead files fast-path delete;
// in-horizon segments and undo-pointer targets are pinned untouched; and a
// rewind within the horizon still works afterwards.
func TestCompactMovesLiveRespectsPinsAndRewind(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	big := make([]byte, 4096)
	// B1: keys 1,2 big (soon dead), key 3 small (stays live forever).
	r1 := commitTestBatch(t, db, 110, foldAcct(1, 100, big), foldAcct(2, 200, big), foldAcct(3, 300, []byte("keep")))
	// B2..B4 rewrite keys 1,2 (B2 ends up fully dead; B3 is B4's undo target).
	r2 := commitTestBatch(t, db, 120, foldAcct(1, 101, big), foldAcct(2, 201, big))
	r3 := commitTestBatch(t, db, 130, foldAcct(1, 102, big), foldAcct(2, 202, big))
	r4 := commitTestBatch(t, db, 140, foldAcct(1, 103, big), foldAcct(2, 203, big))

	// Horizon 1 pins B4 (in horizon) and B3 (named by B4's undo pointers).
	stats, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1, MinDeadFraction: 0.5})
	require.NoError(t, err)
	assert.Equal(t, 1, stats.FilesCompacted, "B1 has one live record to move")
	assert.Equal(t, 1, stats.FilesDeleted, "B2 is fully dead")
	assert.Positive(t, stats.BytesReclaimed)

	assert.NoFileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(110, r1.FileId)))
	assert.NoFileExists(t, segmentManifestPath(db.AcctsDir, 110, r1.FileId))
	assert.NoFileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(120, r2.FileId)))
	assert.NoFileExists(t, segmentManifestPath(db.AcctsDir, 120, r2.FileId))
	assert.FileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(130, r3.FileId)), "undo-pointer target is pinned")
	assert.FileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(140, r4.FileId)), "in-horizon head is pinned")

	// The moved key reads back cold from its new, compact-manifest-backed home.
	idx := snapshotIndex(t, db)
	e3, ok := idx[[32]byte(solana.PublicKey{3})]
	require.True(t, ok)
	assert.NotEqual(t, r1.FileId, e3.FileId, "index must name the compaction output")
	assert.Equal(t, uint64(110), e3.Slot, "filename slot component is preserved")
	assert.FileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(110, e3.FileId)))
	m, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, 110, e3.FileId))
	require.NoError(t, err)
	assert.Equal(t, ManifestKindCompact, m.Kind)
	got := mustColdRead(t, db, 140, solana.PublicKey{3})
	assert.Equal(t, uint64(300), got.Lamports)
	assert.Equal(t, []byte("keep"), got.Data)
	assert.Equal(t, uint64(103), mustColdRead(t, db, 140, solana.PublicKey{1}).Lamports, "head values untouched")

	// The pinned horizon kept every file a rewind needs.
	res, err := db.RewindToBatchBoundary(130)
	require.NoError(t, err)
	assert.Equal(t, uint64(130), res.NewThrough)
	assert.Equal(t, uint64(102), mustColdRead(t, db, 130, solana.PublicKey{1}).Lamports)
	assert.Equal(t, []byte("keep"), mustColdRead(t, db, 130, solana.PublicKey{3}).Data, "compacted key unaffected by rewind")
}

// Mostly-live files are left to decay; the move budget bounds work per cycle
// while fully-dead deletion stays free.
func TestCompactThresholdAndBudget(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	big := make([]byte, 2048)
	// B1: two live keys + one small dead one -> low dead fraction, untouched.
	commitTestBatch(t, db, 110, foldAcct(1, 100, big), foldAcct(2, 200, big), foldAcct(3, 300, []byte("x")))
	commitTestBatch(t, db, 120, foldAcct(3, 301, []byte("y")))

	stats, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1, MinDeadFraction: 0.5})
	require.NoError(t, err)
	assert.Zero(t, stats.FilesCompacted, "mostly-live file must not be rewritten")
	assert.Zero(t, stats.FilesDeleted)

	// Two compactable files, budget of 1 byte: only the first move lands this
	// cycle; the cursor resumes at the second next cycle.
	db2, _ := newFoldTestDb(t)
	defer db2.CloseDb()
	commitTestBatch(t, db2, 110, foldAcct(1, 100, big), foldAcct(4, 400, []byte("live-a")))
	commitTestBatch(t, db2, 120, foldAcct(1, 101, big), foldAcct(5, 500, []byte("live-b")))
	commitTestBatch(t, db2, 130, foldAcct(1, 102, big))
	commitTestBatch(t, db2, 140, foldAcct(1, 103, big))

	stats, err = db2.CompactOnce(CompactionConfig{RewindHorizonBatches: 1, MinDeadFraction: 0.5, MaxMoveBytesPerCycle: 1})
	require.NoError(t, err)
	assert.Equal(t, 1, stats.FilesCompacted, "move budget stops the cycle after one file")
	stats, err = db2.CompactOnce(CompactionConfig{RewindHorizonBatches: 1, MinDeadFraction: 0.5, MaxMoveBytesPerCycle: 1})
	require.NoError(t, err)
	assert.Equal(t, 1, stats.FilesCompacted, "next cycle picks up the remaining file")
	assert.Equal(t, uint64(400), mustColdRead(t, db2, 140, solana.PublicKey{4}).Lamports)
	assert.Equal(t, uint64(500), mustColdRead(t, db2, 140, solana.PublicKey{5}).Lamports)
}

// Bootstrap appendvecs (no manifest, fileId <= bootstrap high-water) compact
// like any other file; undecided orphans above the high-water mark are left
// for recovery.
func TestCompactBootstrapAppendVecAndOrphanSkip(t *testing.T) {
	db, _ := newFoldTestDb(t) // bootstrap_high_file_id = 0
	defer db.CloseDb()

	// Hand-build bootstrap file "50.0": three records, only key 2 indexed (live).
	accts := []*accounts.Account{
		foldAcct(1, 100, make([]byte, 1024)),
		foldAcct(2, 200, []byte("live")),
		foldAcct(3, 300, make([]byte, 1024)),
	}
	var buf bytes.Buffer
	offsets := make([]uint64, len(accts))
	for i, a := range accts {
		offsets[i] = uint64(buf.Len())
		ava := AppendVecAccount{DataLen: uint64(len(a.Data)), Pubkey: a.Key, Lamports: a.Lamports, RentEpoch: a.RentEpoch, Owner: a.Owner, Data: a.Data}
		_, err := ava.MarshalReturningLength(&buf)
		require.NoError(t, err)
	}
	require.NoError(t, os.WriteFile(filepath.Join(db.AcctsDir, "50.0"), buf.Bytes(), 0o644))
	var idxBuf [24]byte
	entry := AccountIndexEntry{Slot: 50, FileId: 0, Offset: offsets[1]}
	entry.Marshal(&idxBuf)
	require.NoError(t, db.Index.Set(accts[1].Key[:], idxBuf[:], nil))

	// An undecided orphan (fileId above bootstrap high-water, no manifest).
	require.NoError(t, os.WriteFile(filepath.Join(db.AcctsDir, "60.7"), []byte("junk"), 0o644))

	stats, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1, MinDeadFraction: 0.5})
	require.NoError(t, err)
	assert.Equal(t, 1, stats.FilesCompacted)

	assert.NoFileExists(t, filepath.Join(db.AcctsDir, "50.0"))
	assert.FileExists(t, filepath.Join(db.AcctsDir, "60.7"), "orphans are recovery's to delete")
	got := mustColdRead(t, db, 50, solana.PublicKey{2})
	assert.Equal(t, uint64(200), got.Lamports)
	assert.Equal(t, []byte("live"), got.Data)
}

// The read path errors (after one retry) instead of panicking when the index
// names a missing file or a record that holds a different pubkey.
func TestReadPathErrorsInsteadOfPanicking(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	// Dangling entry: file does not exist.
	dangling := solana.PublicKey{7}
	var idxBuf [24]byte
	(&AccountIndexEntry{Slot: 99, FileId: 424242, Offset: 0}).Marshal(&idxBuf)
	require.NoError(t, db.Index.Set(dangling[:], idxBuf[:], nil))
	_, err := db.GetAccount(99, dangling)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrNoAccount, "a broken read must not masquerade as account-absent")

	// Wrong-pubkey record: key 9's entry points at key 1's record.
	r := commitTestBatch(t, db, 110, foldAcct(1, 100, []byte("v1")))
	wrong := solana.PublicKey{9}
	(&AccountIndexEntry{Slot: 110, FileId: r.FileId, Offset: 0}).Marshal(&idxBuf)
	require.NoError(t, db.Index.Set(wrong[:], idxBuf[:], nil))
	_, err = db.GetAccount(110, wrong)
	require.Error(t, err)
	require.ErrorContains(t, err, "after retry")

	// Healthy reads are unaffected.
	assert.Equal(t, uint64(100), mustColdRead(t, db, 110, solana.PublicKey{1}).Lamports)
}
