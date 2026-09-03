package accountsdb

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func productionShutdownKeyForShard(
	router PersistentIndexShardRouter,
	shardID uint32,
	start uint64,
) solana.PublicKey {
	for ordinal := start; ; ordinal++ {
		var key solana.PublicKey
		binary.LittleEndian.PutUint64(key[24:], ordinal)
		if router.Shard(key) == shardID {
			return key
		}
	}
}

func TestAccountsDbShutdownTimeoutRetainsV2StoreLockAndCanResume(t *testing.T) {
	config := productionIndexTestConfig()
	root, records := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)

	pinned, err := index.view.Acquire()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pinned.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = index.Shutdown(ctx)
	})

	bankHashStore, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	db := &AccountsDb{ProductionIndex: index, BankHashStore: bankHashStore}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = db.Shutdown(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, db.productionIndexClosed)
	assert.False(t, index.shutdownComplete())
	assert.True(t, db.bankHashStoreClosed, "sidecars must close before the production index can release the store lock")
	require.Panics(t, func() {
		_ = bankHashStore.Set([]byte("closed"), []byte("closed"), pebble.Sync)
	})

	// Shutdown fences new work immediately, even though an old generation is
	// still pinned and teardown has not yet released the process-wide lock.
	_, _, _, err = index.LookupCandidate(records[0].key)
	require.ErrorIs(t, err, ErrShardedMutableClosed)
	secondOwner, err := acquireProductionAccountIndexStoreLock(root, true)
	require.Nil(t, secondOwner)
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)

	require.NoError(t, pinned.Close())
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer retryCancel()
	require.NoError(t, db.Shutdown(retryCtx))
	assert.True(t, db.productionIndexClosed)
	assert.True(t, index.shutdownComplete())
	// A completed shutdown is stable, even with an already-cancelled context.
	alreadyCancelled, cancelAlready := context.WithCancel(context.Background())
	cancelAlready()
	require.NoError(t, db.Shutdown(alreadyCancelled))

	secondOwner, err = acquireProductionAccountIndexStoreLock(root, true)
	require.NoError(t, err)
	require.NoError(t, secondOwner.Close())
}

func TestAccountsDbShutdownPropagatesGenerationCleanupError(t *testing.T) {
	wantErr := errors.New("injected unmap failure")
	resource := mustIndexResource(t, "shutdown-error", nil, func() error {
		return wantErr
	}, nil)
	manager, err := NewIndexViewManager(
		validRootIndexCatalogForTest(t, 2, 1),
		nil,
		[]*IndexGenerationResource{resource},
	)
	require.NoError(t, err)
	index := &ProductionAccountIndex{view: manager}
	db := &AccountsDb{ProductionIndex: index}

	err = db.Shutdown(context.Background())
	require.ErrorIs(t, err, wantErr)
	assert.True(t, db.productionIndexClosed)
	// Both convenience close and repeated bounded shutdown report the stable
	// teardown failure instead of silently discarding it.
	require.ErrorIs(t, db.CloseDb(), wantErr)
	require.ErrorIs(t, db.Shutdown(context.Background()), wantErr)
}

func TestProductionAccountIndexShutdownRejectsNilContextWithoutClosing(t *testing.T) {
	config := productionIndexTestConfig()
	root, records := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })

	require.Error(t, index.Shutdown(nil))
	_, _, found, err := index.LookupCandidate(records[0].key)
	require.NoError(t, err)
	assert.True(t, found)
}

func TestProductionShutdownOwnsRejectedGenerationCleanupUntilStoreUnlock(t *testing.T) {
	root := t.TempDir()
	storeLock, err := acquireProductionAccountIndexStoreLock(root, true)
	require.NoError(t, err)
	wantErr := errors.New("injected rejected-generation close failure")
	cleanupStarted := make(chan struct{})
	allowCleanup := make(chan struct{})
	resource := mustIndexResource(t, "rejected-candidate", nil, func() error {
		close(cleanupStarted)
		<-allowCleanup
		return wantErr
	}, nil)
	index := &ProductionAccountIndex{
		storeLock:    storeLock,
		shutdownDone: make(chan struct{}),
	}
	cleanupReleased := false
	defer func() {
		if !cleanupReleased {
			close(allowCleanup)
		}
		_ = index.Close()
	}()

	index.discardDerivedResources(nil, []*IndexGenerationResource{resource})
	select {
	case <-cleanupStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("rejected-generation cleanup did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = index.Shutdown(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, index.shutdownComplete())
	secondOwner, lockErr := acquireProductionAccountIndexStoreLock(root, true)
	require.Nil(t, secondOwner)
	require.ErrorIs(t, lockErr, ErrProductionAccountIndexInUse)

	close(allowCleanup)
	cleanupReleased = true
	require.ErrorIs(t, index.Shutdown(context.Background()), wantErr)
	assert.True(t, index.shutdownComplete())
	assert.ErrorIs(t, index.checkPoison(), ErrProductionAccountIndexPoisoned)

	secondOwner, err = acquireProductionAccountIndexStoreLock(root, true)
	require.NoError(t, err)
	require.NoError(t, secondOwner.Close())
}

func TestProductionShutdownCancelsMaintenanceBeforeWaitingForJournalWriter(t *testing.T) {
	config := productionIndexTestConfig()
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)

	index.mutable.writeMu.Lock()
	writerHeld := true
	defer func() {
		if writerHeld {
			index.mutable.writeMu.Unlock()
		}
		_ = index.Close()
	}()

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- index.Shutdown(context.Background()) }()
	select {
	case <-index.mutable.ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown waited for the journal writer before cancelling maintenance")
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown completed through a held journal writer: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	secondOwner, lockErr := acquireProductionAccountIndexStoreLock(root, true)
	require.Nil(t, secondOwner)
	require.ErrorIs(t, lockErr, ErrProductionAccountIndexInUse)

	index.mutable.writeMu.Unlock()
	writerHeld = false
	select {
	case err := <-shutdownDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish after releasing the journal writer")
	}
}

func TestProductionShutdownCancelsBackpressuredWriterBeforeReaderDrain(t *testing.T) {
	config := productionIndexTestConfig()
	config.MaxHotKeys = 1
	config.MaxHotBytes = DefaultShardedMutableBytesPerKey
	config.SealKeys = 1
	config.RebaseKeys = 1
	root, _ := initializeProductionIndexFixture(t, config)
	index, err := OpenProductionAccountIndex(root, config)
	require.NoError(t, err)
	pinned, err := index.view.Acquire()
	require.NoError(t, err)
	pinClosed := false
	defer func() {
		if !pinClosed {
			_ = pinned.Close()
		}
		_ = index.Close()
	}()

	shardID := uint32(0)
	keys := [4]solana.PublicKey{}
	for i := range keys {
		keys[i] = productionShutdownKeyForShard(index.mutable.router, shardID, uint64(1000+i*1000))
	}
	for i := 1; i < len(keys); i++ {
		require.NotEqual(t, keys[i-1], keys[i])
	}

	// First establish a reader-pinned obsolete base. The next same-shard rebase
	// then waits at the hard generation gate.
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(keys[0], AccountIndexEntry{Slot: 1000, FileId: 1001, Offset: 8}),
	}, nil, true))
	require.Eventually(t, func() bool {
		stats := index.RuntimeStats()
		return stats.RebaseCount == 1 && stats.ObsoleteBaseGenerationsPending == 1
	}, 10*time.Second, time.Millisecond)
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(keys[1], AccountIndexEntry{Slot: 2000, FileId: 2001, Offset: 16}),
	}, nil, true))
	require.Eventually(t, func() bool {
		stats := index.RuntimeStats()
		return stats.RebaseCount == 1 && stats.RebasesInProgress == 1
	}, 10*time.Second, time.Millisecond)

	// Fill the one-key active budget while that shard cannot seal, then start a
	// second distinct write which must hold applyMu while waiting for the rebase.
	require.NoError(t, index.Apply([]deltaIndexMutation{
		liveDeltaMutation(keys[2], AccountIndexEntry{Slot: 3000, FileId: 3001, Offset: 24}),
	}, nil, true))
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- index.Apply([]deltaIndexMutation{
			liveDeltaMutation(keys[3], AccountIndexEntry{Slot: 4000, FileId: 4001, Offset: 32}),
		}, nil, true)
	}()
	require.Eventually(t, func() bool {
		if index.applyMu.TryLock() {
			index.applyMu.Unlock()
			return false
		}
		return true
	}, 5*time.Second, time.Millisecond, "writer never entered its capacity wait")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err = index.Shutdown(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case err := <-writerDone:
		require.ErrorIs(t, err, ErrShardedMutableClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not cancel the backpressured writer")
	}

	secondOwner, lockErr := acquireProductionAccountIndexStoreLock(root, true)
	require.Nil(t, secondOwner)
	require.ErrorIs(t, lockErr, ErrProductionAccountIndexInUse)
	require.NoError(t, pinned.Close())
	pinClosed = true
	require.NoError(t, index.Shutdown(context.Background()))
	secondOwner, err = acquireProductionAccountIndexStoreLock(root, true)
	require.NoError(t, err)
	require.NoError(t, secondOwner.Close())
}
