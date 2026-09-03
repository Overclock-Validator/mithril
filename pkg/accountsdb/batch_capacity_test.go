package accountsdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommitBatchChunksOversizedSingleSlotWithoutWedge(t *testing.T) {
	fixture := tinyProductionAccountsDBFixture(t, 2)
	require.NoError(t, fixture.config.Validate())
	db := openProductionAccountsDBFixture(t, fixture)
	defer db.CloseDb()

	beforeHighWater := db.LargestFileId.Load()
	result, err := db.CommitBatch([]accounts.SlotDelta{{
		Slot: 101,
		Delta: []*accounts.Account{
			foldAcct(1, 1, nil),
			foldAcct(2, 2, nil),
			foldAcct(3, 3, nil),
		},
	}}, 101, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, 3, result.Keys)
	assert.Equal(t, beforeHighWater+1, db.LargestFileId.Load())

	manifests, listErr := ListFoldManifests(db.AcctsDir)
	require.NoError(t, listErr)
	require.Len(t, manifests, 1)
	_, statErr := os.Stat(filepath.Join(db.AcctsDir, SegmentDataName(101, beforeHighWater+1)))
	require.NoError(t, statErr)
	meta, ok, metaErr := db.readFoldMeta()
	require.NoError(t, metaErr)
	require.True(t, ok)
	assert.Equal(t, uint64(101), meta.ThroughSlot)
	stats := db.AccountIndexStats()
	assert.Equal(t, uint64(1), stats.FoldCommits)
	assert.Equal(t, uint64(2), stats.FoldWALFrames)
	assert.Equal(t, uint64(1), stats.OversizedFoldCommits)
	assert.Equal(t, uint64(3), stats.LargestFoldKeys)
}

func TestOversizedLiveFoldMasksPointReadsAndSerializesRangeScans(t *testing.T) {
	fixture := tinyProductionAccountsDBFixture(t, 2)
	db := openProductionAccountsDBFixture(t, fixture)
	defer db.CloseDb()

	accountsToFold := []*accounts.Account{
		foldAcct(1, 11, nil),
		foldAcct(2, 22, nil),
		foldAcct(3, 33, nil),
		foldAcct(4, 44, nil),
		foldAcct(5, 55, nil),
	}
	firstFrame := make(chan struct{})
	release := make(chan struct{})
	db.foldHooks.afterIndexTxnFrame = func(completed, total int) {
		if completed == 1 {
			assert.Equal(t, 3, total)
			close(firstFrame)
			<-release
		}
	}
	commitDone := make(chan error, 1)
	go func() {
		_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 101, Delta: accountsToFold}}, 101, nil, nil)
		commitDone <- err
	}()
	select {
	case <-firstFrame:
	case <-time.After(10 * time.Second):
		t.Fatal("multi-frame fold did not publish its first frame")
	}

	// Key 5 has not reached the physical index, but pendingFold is the complete
	// logical epoch and must already serve it.
	got, err := db.GetAccount(101, accountsToFold[4].Key)
	require.NoError(t, err)
	assert.Equal(t, uint64(55), got.Lamports)
	assert.False(t, db.accountIndexWriteMu.TryLock(), "logical transaction must own the writer fence across every frame")

	scanDone := make(chan error, 1)
	go func() {
		scanDone <- db.ScanKeysBetweenPrefixes(context.Background(), 0, ^uint64(0), func(solana.PublicKey) error {
			return nil
		})
	}()
	select {
	case scanErr := <-scanDone:
		t.Fatalf("range scan crossed an incomplete logical transaction: %v", scanErr)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-commitDone)
	select {
	case scanErr := <-scanDone:
		require.NoError(t, scanErr)
	case <-time.After(10 * time.Second):
		t.Fatal("range scan did not resume after logical transaction commit")
	}
}

func TestOversizedDecidedMutationTransactionIsChunkedAndRetryable(t *testing.T) {
	fixture := tinyProductionAccountsDBFixture(t, 2)
	db := openProductionAccountsDBFixture(t, fixture)

	mutations := make([]deltaIndexMutation, 5)
	for i := range mutations {
		mutations[i] = liveDeltaMutation(foldAcct(byte(i+1), uint64(i+1), nil).Key, AccountIndexEntry{
			Slot:   101,
			FileId: 9,
			Offset: uint64(i * 8),
		})
	}
	// Model a crash after the first bounded data frame but before the final
	// fold-meta marker. Recovery may replay the whole decided transaction.
	require.NoError(t, db.applyAccountIndexMutations(mutations[:2], nil))
	db.CloseDb()

	db = openProductionAccountsDBFixture(t, fixture)
	t.Cleanup(func() { require.NoError(t, db.CloseDb()) })
	wantMeta := foldMeta{BatchSeq: 7, ThroughSlot: 101, FileId: 9}
	require.NoError(t, db.applyAccountIndexMutationTransaction(mutations, &wantMeta))
	gotMeta, ok, err := db.readFoldMeta()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, wantMeta, gotMeta)
	for i := range mutations {
		entry, source, found, lookupErr := db.ProductionIndex.LookupCandidate(mutations[i].Key)
		require.NoError(t, lookupErr)
		require.True(t, found)
		assert.NotEqual(t, accountIndexSourceNone, source)
		assert.Equal(t, mutations[i].Value.Entry, entry)
	}
}

func TestDecidedFoldFailurePoisonsAndRecoversExactlyOnce(t *testing.T) {
	fixture := tinyProductionAccountsDBFixture(t, 2)
	db := openProductionAccountsDBFixture(t, fixture)
	account := foldAcct(9, 99, nil)
	var closeErr error
	db.foldHooks.beforeIndexCommit = func() {
		closeErr = db.ProductionIndex.Close()
	}

	_, err := db.CommitBatch([]accounts.SlotDelta{{
		Slot: 101, Delta: []*accounts.Account{account},
	}}, 101, nil, []byte("recover-me"))
	require.NoError(t, closeErr)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrFoldCommitDecided))
	_, retryErr := db.CommitBatch(nil, 102, nil, nil)
	require.Error(t, retryErr)
	assert.True(t, errors.Is(retryErr, ErrFoldCommitDecided))
	db.readCacheEpochMu.RLock()
	assert.Contains(t, db.pendingFold, [32]byte(account.Key))
	db.readCacheEpochMu.RUnlock()
	assert.ErrorIs(t, db.ProductionIndex.checkUsable(), ErrShardedMutableClosed)
	db.CloseDb()

	db = openProductionAccountsDBFixture(t, fixture)
	t.Cleanup(func() { require.NoError(t, db.CloseDb()) })
	recovery, recoveryErr := db.RecoverFoldState()
	require.NoError(t, recoveryErr)
	assert.Equal(t, []uint64{1}, recovery.ReplayedBatches)
	meta, ok, metaErr := db.readFoldMeta()
	require.NoError(t, metaErr)
	require.True(t, ok)
	assert.Equal(t, uint64(1), meta.BatchSeq)
	assert.Equal(t, uint64(101), meta.ThroughSlot)

	second, secondErr := db.RecoverFoldState()
	require.NoError(t, secondErr)
	assert.Empty(t, second.ReplayedBatches)
}

func TestSegmentManifestReportsPostRenameSyncAmbiguity(t *testing.T) {
	dir := t.TempDir()
	manifest := &SegmentManifest{
		Version:     segManifestVersion,
		Kind:        ManifestKindFold,
		BatchSeq:    1,
		ThroughSlot: 42,
		FileId:      7,
	}
	wantErr := errors.New("injected directory sync failure")
	err := writeSegmentManifest(dir, manifest, func(string) error { return wantErr })
	require.Error(t, err)
	assert.True(t, segmentManifestWasRenamed(err))
	assert.ErrorIs(t, err, wantErr)

	got, readErr := ReadSegmentManifest(segmentManifestPath(dir, 42, 7))
	require.NoError(t, readErr)
	assert.Equal(t, manifest.Version, got.Version)
	assert.Equal(t, manifest.Kind, got.Kind)
	assert.Equal(t, manifest.BatchSeq, got.BatchSeq)
	assert.Equal(t, manifest.ThroughSlot, got.ThroughSlot)
	assert.Equal(t, manifest.FileId, got.FileId)
}

func TestProductionRewindChunksUniqueUndoSet(t *testing.T) {
	fixture := tinyProductionAccountsDBFixture(t, 2)
	db := openProductionAccountsDBFixture(t, fixture)
	defer db.CloseDb()

	_, err := db.CommitBatch(nil, 101, nil, []byte("target"))
	require.NoError(t, err)
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 102, Delta: []*accounts.Account{
		foldAcct(1, 1, nil), foldAcct(2, 2, nil),
	}}}, 102, nil, []byte("two"))
	require.NoError(t, err)
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 103, Delta: []*accounts.Account{
		foldAcct(3, 3, nil), foldAcct(4, 4, nil),
	}}}, 103, nil, []byte("three"))
	require.NoError(t, err)

	result, err := db.RewindToBatchBoundary(101)
	require.NoError(t, err)
	assert.Equal(t, uint64(101), result.NewThrough)
	assert.Equal(t, 2, result.UndoneBatches)
	assert.Equal(t, 4, result.UndoneKeys)
	meta, ok, metaErr := db.readFoldMeta()
	require.NoError(t, metaErr)
	require.True(t, ok)
	assert.Equal(t, uint64(1), meta.BatchSeq)
	assert.Equal(t, uint64(101), meta.ThroughSlot)
	for key := byte(1); key <= 4; key++ {
		_, _, found, lookupErr := db.ProductionIndex.LookupCandidate(foldAcct(key, 0, nil).Key)
		require.NoError(t, lookupErr)
		assert.False(t, found)
	}
}

func tinyProductionAccountsDBFixture(t *testing.T, maxHotKeys uint64) productionAccountsDBFixture {
	t.Helper()
	fixture := newProductionAccountsDBFixture(t, nil)
	fixture.config.MaxHotKeys = maxHotKeys
	fixture.config.MaxHotBytes = maxHotKeys * DefaultShardedMutableBytesPerKey
	fixture.config.SealKeys = 1
	fixture.config.RebaseKeys = maxHotKeys
	fixture.config.MaxConcurrentSeals = 1
	require.NoError(t, fixture.config.Validate())
	return fixture
}
