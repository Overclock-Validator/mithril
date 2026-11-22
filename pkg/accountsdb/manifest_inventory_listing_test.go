package accountsdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/stretchr/testify/require"
)

// requireManifestListingMatchesDirectory compares the inventory-backed listing
// with a listing of the directory itself.
func requireManifestListingMatchesDirectory(t *testing.T, db *AccountsDb, when string) ([]ManifestHeader, []string) {
	t.Helper()
	wantFolds, wantParked, err := ListFoldAndParkedManifests(db.AcctsDir)
	require.NoError(t, err, when)
	folds, parked, err := db.FoldAndParkedManifests()
	require.NoError(t, err, when)
	require.Equal(t, wantFolds, folds, when)
	require.Equal(t, wantParked, parked, when)
	return folds, parked
}

// Through folds, relocation, in-place sparse rewrites, fully dead deletion,
// rewind and reopen, the listing served from the inventory equals a listing of
// the directory, and between invalidations it is served without a rescan.
func TestFoldAndParkedManifestsMatchesDirectory(t *testing.T) {
	previous := compactionInventoryVerifyCycles
	compactionInventoryVerifyCycles = 1 << 30
	t.Cleanup(func() { compactionInventoryVerifyCycles = previous })
	db, dir := newFoldTestDb(t)
	big := make([]byte, 3*4096)
	cfg := CompactionConfig{RewindHorizonBatches: 2, MinDeadFraction: 0.5}

	folds, parked := requireManifestListingMatchesDirectory(t, db, "empty store")
	require.Empty(t, folds)
	require.Empty(t, parked)
	require.NotNil(t, db.compactInventory, "the first call builds an inventory")
	require.True(t, db.compactInventory.namesOnly(), "without a stat per data file")
	var served *compactionInventory

	through := uint64(100)
	compacted, deleted, sparse := 0, 0, 0
	for round := 0; round < 12; round++ {
		through += 10
		delta := []*accounts.Account{foldAcct(1, through, big), foldAcct(2, through+1, big)}
		if round%2 == 0 {
			delta = append(delta, foldAcct(byte(10+round), 7, []byte("keep")))
		}
		commitTestBatch(t, db, through, delta...)
		folds, _ = requireManifestListingMatchesDirectory(t, db, "after fold")
		require.Equal(t, through, folds[len(folds)-1].ThroughSlot, "the fold just committed is listed")
		stats, err := db.CompactOnce(cfg)
		require.NoError(t, err)
		compacted += stats.FilesCompacted
		deleted += stats.FilesDeleted
		sparse += stats.FilesSparsified
		requireManifestListingMatchesDirectory(t, db, "after compaction")
		if round == 0 {
			served = db.compactInventory
			require.False(t, served.namesOnly(), "the first compaction cycle built the full inventory")
		}
		require.Same(t, served, db.compactInventory, "round %d was answered from memory", round)
	}
	for i := 0; i < 8; i++ {
		stats, err := db.CompactOnce(cfg)
		require.NoError(t, err)
		compacted += stats.FilesCompacted
		deleted += stats.FilesDeleted
		sparse += stats.FilesSparsified
		requireManifestListingMatchesDirectory(t, db, "after a later compaction cycle")
	}
	require.Same(t, served, db.compactInventory)
	require.Zero(t, db.compactInventoryDrift)
	require.Positive(t, compacted+deleted+sparse, "compaction changed manifests under tracking")

	// Rewind parks and moves manifests outside tracking and drops the view.
	_, err := db.RewindToBatchBoundary(through - 10)
	require.NoError(t, err)
	require.Nil(t, db.compactInventory)
	folds, _ = requireManifestListingMatchesDirectory(t, db, "after rewind")
	require.Equal(t, through-10, folds[len(folds)-1].ThroughSlot)
	require.NotNil(t, db.compactInventory)
	require.True(t, db.compactInventory.namesOnly(), "rebuilt for the listing alone")

	commitTestBatch(t, db, through+5, foldAcct(1, 1, big))
	requireManifestListingMatchesDirectory(t, db, "after a fold on the rewound store")

	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	_, err = db.RecoverFoldState()
	require.NoError(t, err)
	require.Nil(t, db.compactInventory)
	requireManifestListingMatchesDirectory(t, db, "after reopen")
	require.Zero(t, db.compactInventoryDrift)
}

// Parked manifests and unreadable headers are reported as the directory
// listing reports them, and a compact-kind manifest is left out.
func TestFoldAndParkedManifestsParkedAndUnreadable(t *testing.T) {
	previous := compactionInventoryVerifyCycles
	compactionInventoryVerifyCycles = 1 << 30
	t.Cleanup(func() { compactionInventoryVerifyCycles = previous })
	dir := t.TempDir()
	for _, m := range []*SegmentManifest{
		{Kind: ManifestKindFold, BatchSeq: 2, ThroughSlot: 20, FileId: 2},
		{Kind: ManifestKindFold, BatchSeq: 1, ThroughSlot: 10, FileId: 1},
		{Kind: ManifestKindCompact, ThroughSlot: 30, FileId: 3},
		{Kind: ManifestKindFold, BatchSeq: 3, ThroughSlot: 40, FileId: 4},
	} {
		require.NoError(t, WriteSegmentManifest(dir, m))
	}
	require.NoError(t, os.Rename(filepath.Join(dir, "40.4.manifest"), filepath.Join(dir, "40.4.manifest.rewound")))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.manifest"), []byte("invalid-header"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ignored.manifest.tmp"), nil, 0o600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "directory.manifest"), 0o700))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "directory.manifest.rewound"), 0o700))

	db := &AccountsDb{AcctsDir: dir}
	folds, parked := requireManifestListingMatchesDirectory(t, db, "mixed directory")
	require.Len(t, folds, 3)
	require.Equal(t, filepath.Join(dir, "bad.manifest"), folds[0].Path)
	require.Zero(t, folds[0].BatchSeq)
	require.Equal(t, uint64(1), folds[1].BatchSeq)
	require.Equal(t, uint64(2), folds[2].BatchSeq)
	require.Equal(t, []string{filepath.Join(dir, "40.4.manifest.rewound")}, parked)

	// The unreadable header is not a reason to rescan: it is still there.
	served := db.compactInventory
	requireManifestListingMatchesDirectory(t, db, "second call")
	require.Same(t, served, db.compactInventory)
	require.Zero(t, db.compactInventoryDrift)

	_, _, err := (&AccountsDb{AcctsDir: filepath.Join(dir, "missing")}).FoldAndParkedManifests()
	require.ErrorIs(t, err, os.ErrNotExist)
}

// A manifest removed behind the store's back is not reported from the stale
// view: the inventory is rebuilt and the answer is what the directory holds.
func TestFoldAndParkedManifestsRebuildsWhenListedManifestIsGone(t *testing.T) {
	for _, tc := range []struct {
		name, remove string
	}{
		{"fold", "20.2.manifest"},
		{"parked", "40.4.manifest.rewound"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := compactionInventoryVerifyCycles
			compactionInventoryVerifyCycles = 1 << 30
			t.Cleanup(func() { compactionInventoryVerifyCycles = previous })
			dir := t.TempDir()
			for _, m := range []*SegmentManifest{
				{Kind: ManifestKindFold, BatchSeq: 1, ThroughSlot: 10, FileId: 1},
				{Kind: ManifestKindFold, BatchSeq: 2, ThroughSlot: 20, FileId: 2},
				{Kind: ManifestKindFold, BatchSeq: 3, ThroughSlot: 40, FileId: 4},
			} {
				require.NoError(t, WriteSegmentManifest(dir, m))
			}
			require.NoError(t, os.Rename(filepath.Join(dir, "40.4.manifest"), filepath.Join(dir, "40.4.manifest.rewound")))
			db := &AccountsDb{AcctsDir: dir}
			folds, parked := requireManifestListingMatchesDirectory(t, db, "before removal")
			require.Len(t, folds, 2)
			require.Len(t, parked, 1)
			served := db.compactInventory

			require.NoError(t, os.Remove(filepath.Join(dir, tc.remove)))
			folds, parked = requireManifestListingMatchesDirectory(t, db, "after removal")
			require.Len(t, folds, 2-len(parked))
			for _, h := range folds {
				require.NotZero(t, h.BatchSeq, "the vanished manifest is not reported as unreadable")
			}
			require.NotSame(t, served, db.compactInventory, "the stale view was replaced")
			require.Equal(t, uint64(1), db.compactInventoryDrift)

			served = db.compactInventory
			requireManifestListingMatchesDirectory(t, db, "next call")
			require.Same(t, served, db.compactInventory, "one rebuild was enough")
			require.Equal(t, uint64(1), db.compactInventoryDrift)
		})
	}
}

// A fold that fails after its manifest is durable drops the inventory, so the
// manifest is still listed afterwards.
func TestFoldAndParkedManifestsAfterFailedFold(t *testing.T) {
	previous := compactionInventoryVerifyCycles
	compactionInventoryVerifyCycles = 1 << 30
	t.Cleanup(func() { compactionInventoryVerifyCycles = previous })
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	commitTestBatch(t, db, 110, foldAcct(1, 1, []byte("a")))
	requireManifestListingMatchesDirectory(t, db, "one fold")
	require.NotNil(t, db.compactInventory)

	db.foldHooks.afterManifestRename = func() { panic("crash after the manifest rename") }
	require.Panics(t, func() {
		_, _ = db.CommitBatch(
			foldDeltas(accounts.SlotDelta{Slot: 120, Delta: []*accounts.Account{foldAcct(1, 2, []byte("b"))}}),
			120, map[uint64][32]byte{120: bh(120)}, []byte("ctx-120"),
		)
	})
	db.foldHooks.afterManifestRename = nil
	require.Nil(t, db.compactInventory, "the interrupted fold dropped the view")
	folds, _ := requireManifestListingMatchesDirectory(t, db, "after the interrupted fold")
	require.Len(t, folds, 2)
	require.Equal(t, uint64(120), folds[1].ThroughSlot)
}

// A store that never compacts answers from a names-only inventory: built by
// one listing, kept across folds, and never holding the per-data-file view.
// A later compaction cycle replaces it with a full inventory and finds no
// drift in the manifest names it tracked.
func TestFoldAndParkedManifestsNamesOnlyInventory(t *testing.T) {
	previous := compactionInventoryVerifyCycles
	compactionInventoryVerifyCycles = 1 << 30
	t.Cleanup(func() { compactionInventoryVerifyCycles = previous })
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	big := make([]byte, 3*4096)

	requireManifestListingMatchesDirectory(t, db, "empty store")
	served := db.compactInventory
	require.True(t, served.namesOnly())
	through := uint64(100)
	for round := 0; round < 6; round++ {
		through += 10
		commitTestBatch(t, db, through, foldAcct(1, through, big), foldAcct(byte(10+round), 7, []byte("keep")))
		folds, _ := requireManifestListingMatchesDirectory(t, db, "after fold")
		require.Len(t, folds, round+1)
		require.Same(t, served, db.compactInventory, "round %d was answered from memory", round)
		require.True(t, db.compactInventory.namesOnly(), "a fold does not add the data view")
	}

	// The names-only view is re-verified on the same schedule, by one listing.
	compactionInventoryVerifyCycles = 0
	requireManifestListingMatchesDirectory(t, db, "at the periodic rebuild")
	require.NotSame(t, served, db.compactInventory)
	require.True(t, db.compactInventory.namesOnly())
	require.Zero(t, db.compactInventoryDrift)
	compactionInventoryVerifyCycles = 1 << 30

	strict := compactionInventoryStrict
	compactionInventoryStrict = true
	t.Cleanup(func() { compactionInventoryStrict = strict })
	_, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 2, MinDeadFraction: 0.5})
	require.NoError(t, err)
	require.False(t, db.compactInventory.namesOnly(), "compaction built the full inventory")
	require.Zero(t, db.compactInventoryDrift, "and every manifest name was already tracked")
	requireManifestListingMatchesDirectory(t, db, "after the upgrade")
}
