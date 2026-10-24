package accountsdb

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MITHRIL_TEST_INVENTORY_STRICT=1 makes every compaction cycle in this
// package's tests rescan the directory and fail on drift, which shows whether
// any directory writer escapes the inventory's tracking.
func init() {
	if os.Getenv("MITHRIL_TEST_INVENTORY_STRICT") == "1" {
		compactionInventoryVerifyCycles = 0
		compactionInventoryStrict = true
	}
}

// verifyEveryCycle rescans the directory on every cycle for the test.
func verifyEveryCycle(t *testing.T) {
	previous := compactionInventoryVerifyCycles
	compactionInventoryVerifyCycles = 0
	t.Cleanup(func() { compactionInventoryVerifyCycles = previous })
}

// allowDrift disables strict mode for a test that changes the directory
// behind the inventory on purpose.
func allowDrift(t *testing.T) {
	strict := compactionInventoryStrict
	compactionInventoryStrict = false
	t.Cleanup(func() { compactionInventoryStrict = strict })
}

// requirePinsMatchUncached compares the cached pin set with a fresh one
// computed from the directory.
func requirePinsMatchUncached(t *testing.T, db *AccountsDb, horizon uint64) {
	t.Helper()
	db.foldMu.Lock()
	defer db.foldMu.Unlock()
	meta, _, err := db.readFoldMeta()
	require.NoError(t, err)
	inv, err := scanCompactionInventory(db.AcctsDir)
	require.NoError(t, err)
	cached, cachedHeaders, err := db.compactionPinSet(meta.BatchSeq, horizon, inv)
	require.NoError(t, err)
	saved := db.compactPinCache
	db.compactPinCache = nil
	fresh, freshHeaders, err := db.compactionPinSet(meta.BatchSeq, horizon, inv)
	db.compactPinCache = saved
	require.NoError(t, err)
	require.Equal(t, fresh.whole, cached.whole)
	require.Equal(t, fresh.records, cached.records)
	require.Equal(t, freshHeaders, cachedHeaders)
}

// Folds, relocation, fully-dead deletion and rewind keep the tracked
// inventory identical to the directory, and cached pins identical to pins
// recomputed from the manifests.
func TestCompactionInventoryTracksFoldsCompactionAndRewind(t *testing.T) {
	verifyEveryCycle(t)
	db, dir := newFoldTestDb(t)
	big := make([]byte, 4096)
	cfg := CompactionConfig{RewindHorizonBatches: 2, MinDeadFraction: 0.5}
	through := uint64(100)
	compacted, deleted := 0, 0
	for round := 0; round < 12; round++ {
		through += 10
		delta := []*accounts.Account{foldAcct(1, through, big), foldAcct(2, through+1, big)}
		if round%2 == 0 {
			// Odd rounds hold only overwritten keys and later become fully dead.
			delta = append(delta, foldAcct(byte(10+round), 7, []byte("keep")))
		}
		commitTestBatch(t, db, through, delta...)
		stats, err := db.CompactOnce(cfg)
		require.NoError(t, err)
		compacted += stats.FilesCompacted
		deleted += stats.FilesDeleted
		require.Zero(t, db.compactInventoryDrift, "round %d", round)
		requirePinsMatchUncached(t, db, cfg.RewindHorizonBatches)
	}
	for i := 0; i < 8; i++ {
		stats, err := db.CompactOnce(cfg)
		require.NoError(t, err)
		compacted += stats.FilesCompacted
		deleted += stats.FilesDeleted
	}
	require.Zero(t, db.compactInventoryDrift)
	require.Positive(t, compacted, "relocation ran under tracking")
	require.Positive(t, deleted, "fully-dead deletion ran under tracking")

	// Rewind moves files outside compaction; the next cycle must not use the old view.
	_, err := db.RewindToBatchBoundary(through - 10)
	require.NoError(t, err)
	require.Nil(t, db.compactInventory, "rewind invalidates the inventory")
	_, err = db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Zero(t, db.compactInventoryDrift)
	requirePinsMatchUncached(t, db, cfg.RewindHorizonBatches)
	commitTestBatch(t, db, through+5, foldAcct(1, 1, big))
	_, err = db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Zero(t, db.compactInventoryDrift)

	// A reopened store starts without an inventory and rebuilds it.
	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	_, err = db.RecoverFoldState()
	require.NoError(t, err)
	_, err = db.CompactOnce(cfg)
	require.NoError(t, err)
	require.Zero(t, db.compactInventoryDrift)
	assert.Equal(t, uint64(1), mustColdRead(t, db, through+5, solana.PublicKey{1}).Lamports)
}

// A file the inventory does not know about (an untracked writer) is picked
// up by the periodic rescan and reported as drift; a file deleted behind its
// back is skipped and forces a rescan, never an error.
func TestCompactionInventoryDetectsUntrackedChanges(t *testing.T) {
	allowDrift(t)
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	cfg := CompactionConfig{RewindHorizonBatches: 1, MinDeadFraction: 0.5}
	commitTestBatch(t, db, 110, foldAcct(1, 1, []byte("a")))
	commitTestBatch(t, db, 120, foldAcct(1, 2, []byte("b")))
	_, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	require.NotNil(t, db.compactInventory)

	previous := compactionInventoryVerifyCycles
	compactionInventoryVerifyCycles = 1 << 30
	stray := filepath.Join(db.AcctsDir, SegmentDataName(5, 999))
	require.NoError(t, os.WriteFile(stray, []byte("not tracked"), 0o644))
	_, err = db.CompactOnce(cfg)
	require.NoError(t, err)
	_, known := db.compactInventory.data[filepath.Base(stray)]
	require.False(t, known, "between rescans an untracked file is not seen")

	compactionInventoryVerifyCycles = 0
	t.Cleanup(func() { compactionInventoryVerifyCycles = previous })
	_, err = db.CompactOnce(cfg)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), db.compactInventoryDrift, "the rescan reports the untracked file")
	_, known = db.compactInventory.data[filepath.Base(stray)]
	assert.True(t, known, "and adopts it")

}

// A candidate deleted behind the inventory's back is skipped, and the next
// cycle rescans instead of trusting the stale view.
func TestCompactionInventorySurvivesExternallyDeletedCandidate(t *testing.T) {
	allowDrift(t)
	previous := compactionInventoryVerifyCycles
	compactionInventoryVerifyCycles = 1 << 30
	t.Cleanup(func() { compactionInventoryVerifyCycles = previous })
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	cfg := CompactionConfig{RewindHorizonBatches: 1, MinDeadFraction: 0.5}
	big := make([]byte, 4096)
	r1 := commitTestBatch(t, db, 110, foldAcct(1, 1, big))
	commitTestBatch(t, db, 120, foldAcct(1, 2, big))
	commitTestBatch(t, db, 130, foldAcct(1, 3, big))
	commitTestBatch(t, db, 140, foldAcct(1, 4, big))
	db.foldMu.Lock()
	_, err := db.compactionInventoryLocked()
	db.foldMu.Unlock()
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(db.AcctsDir, SegmentDataName(110, r1.FileId))))

	_, err = db.CompactOnce(cfg)
	require.NoError(t, err, "a vanished candidate is not an error")
	require.Nil(t, db.compactInventory, "the vanished candidate was reached and dropped the stale view")
	verifyEveryCycle(t)
	_, err = db.CompactOnce(cfg)
	require.NoError(t, err)
	assert.Zero(t, db.compactInventoryDrift, "the stale view was dropped, not diffed")
}

// Pins are recomputed when a fold adds a retained manifest and reused while
// nothing retained changes.
func TestCompactionPinCacheRecomputesOnlyOnRetainedChange(t *testing.T) {
	verifyEveryCycle(t)
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	cfg := CompactionConfig{RewindHorizonBatches: 2, MinDeadFraction: 0.99}
	for through := uint64(110); through <= 140; through += 10 {
		commitTestBatch(t, db, through, foldAcct(1, through, bytes.Repeat([]byte{1}, 64)))
	}
	_, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	first := db.compactPinCache
	require.NotNil(t, first)
	_, err = db.CompactOnce(cfg)
	require.NoError(t, err)
	assert.Same(t, first, db.compactPinCache, "nothing retained changed: pins reused")

	commitTestBatch(t, db, 150, foldAcct(1, 150, []byte("x")))
	_, err = db.CompactOnce(cfg)
	require.NoError(t, err)
	assert.NotSame(t, first, db.compactPinCache, "a new fold changes the retained set")
	requirePinsMatchUncached(t, db, cfg.RewindHorizonBatches)
}

// A retained manifest damaged after the pins were cached must still make the
// cycle fail closed, exactly as when every cycle decoded the full manifests,
// including damage that keeps the file's size and mtime.
func TestCompactionPinCacheFailsClosedOnWarmManifestDamage(t *testing.T) {
	cases := map[string]func(raw []byte) []byte{
		"record tail byte":   func(raw []byte) []byte { raw[len(raw)-5] ^= 1; return raw },
		"crc trailer":        func(raw []byte) []byte { raw[len(raw)-1] ^= 1; return raw },
		"header batch seq":   func(raw []byte) []byte { raw[10] ^= 1; return raw },
		"middle of the body": func(raw []byte) []byte { raw[len(raw)/2] ^= 0x80; return raw },
		"truncated":          func(raw []byte) []byte { return raw[:len(raw)-9] },
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			db, _ := newFoldTestDb(t)
			defer db.CloseDb()
			commitTestBatch(t, db, 110, foldAcct(1, 1, []byte("first")))
			r := commitTestBatch(t, db, 120, foldAcct(1, 2, []byte("second")))
			commitTestBatch(t, db, 130, foldAcct(1, 3, []byte("third")))
			cfg := CompactionConfig{RewindHorizonBatches: 64}
			_, err := db.CompactOnce(cfg)
			require.NoError(t, err)
			cached := db.compactPinCache
			require.NotNil(t, cached)
			_, err = db.CompactOnce(cfg)
			require.NoError(t, err)
			require.Same(t, cached, db.compactPinCache, "an unchanged manifest set reuses the decoded pins")

			path := segmentManifestPath(db.AcctsDir, 120, r.FileId)
			info, err := os.Stat(path)
			require.NoError(t, err)
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, damage(raw), info.Mode()))
			require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
			_, err = db.CompactOnce(cfg)
			require.Error(t, err, "a damaged retained manifest fails the cycle")
			require.Nil(t, db.compactPinCache, "and drops the cached pins")
			_, err = db.CompactOnce(cfg)
			require.Error(t, err, "and keeps failing while it stays damaged")
		})
	}
}

// Parked (.rewound) manifests are retained too; damage to one fails closed.
func TestCompactionPinCacheFailsClosedOnDamagedParkedManifest(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	commitTestBatch(t, db, 110, foldAcct(1, 1, []byte("first")))
	r := commitTestBatch(t, db, 120, foldAcct(1, 2, []byte("second")))
	commitTestBatch(t, db, 130, foldAcct(1, 3, []byte("third")))
	cfg := CompactionConfig{RewindHorizonBatches: 64}
	_, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	active := segmentManifestPath(db.AcctsDir, 120, r.FileId)
	parked := active + ".rewound"
	raw, err := os.ReadFile(active)
	require.NoError(t, err)
	raw[len(raw)-5] ^= 1
	require.NoError(t, os.WriteFile(parked, raw, 0o644))
	db.foldMu.Lock()
	db.invalidateCompactionInventoryLocked() // an interrupted rewind leaves parked manifests; recovery invalidates
	db.foldMu.Unlock()
	_, err = db.CompactOnce(cfg)
	require.Error(t, err)
}

// A store whose pin setup fails (here a missing retained manifest) keeps
// failing closed every cycle without rescanning the directory each time:
// nothing changed on disk.
func TestCompactionInventoryKeptWhenPinSetupFails(t *testing.T) {
	previous := compactionInventoryVerifyCycles
	compactionInventoryVerifyCycles = 1 << 30 // strict mode would rescan every cycle on purpose
	t.Cleanup(func() { compactionInventoryVerifyCycles = previous })
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	commitTestBatch(t, db, 110, foldAcct(1, 1, []byte("first")))
	r := commitTestBatch(t, db, 120, foldAcct(1, 2, []byte("second")))
	commitTestBatch(t, db, 130, foldAcct(1, 3, []byte("third")))
	cfg := CompactionConfig{RewindHorizonBatches: 64}
	_, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	inv := db.compactInventory
	require.NotNil(t, inv)
	require.NoError(t, os.Remove(segmentManifestPath(db.AcctsDir, 120, r.FileId)))
	db.foldMu.Lock()
	delete(inv.manifests, filepath.Base(segmentManifestPath(db.AcctsDir, 120, r.FileId)))
	db.foldMu.Unlock()
	for i := 0; i < 3; i++ {
		_, err = db.CompactOnce(cfg)
		require.ErrorContains(t, err, "missing fold manifest")
		require.Same(t, inv, db.compactInventory, "a failed pin setup keeps the inventory")
	}
}
