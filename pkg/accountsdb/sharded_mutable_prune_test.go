package accountsdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardedMutablePruneWithoutEligibleRetirementsLeavesJournalUnchanged(t *testing.T) {
	for _, withRetirement := range []bool{false, true} {
		name := "no retirements"
		if withRetirement {
			name = "only newer retirements"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, InitializeShardedMutableAccountIndex(dir))
			config := shardedMutableTestConfig(t, dir, nil, nil)
			idx := openShardedMutableForTest(t, config)
			t.Cleanup(func() { _ = idx.Close() })
			key := shardedMutableTestKey(0, 1)
			require.NoError(t, idx.Apply([]deltaIndexMutation{
				liveDeltaMutation(key, AccountIndexEntry{Slot: 1, FileId: 1, Offset: 8}),
			}, nil, true))
			want := deltaIndexValue{Entry: AccountIndexEntry{Slot: 2, FileId: 2, Offset: 16}}
			mutations := []deltaIndexMutation{liveDeltaMutation(key, want.Entry)}
			if withRetirement {
				mutations = append(mutations, retireDeltaMutation(99, 100))
			}
			require.NoError(t, idx.Apply(mutations, nil, true))

			path := filepath.Join(dir, ShardedDeltaIndexJournalFileName)
			beforeInfo, err := os.Stat(path)
			require.NoError(t, err)
			beforeBytes, err := os.ReadFile(path)
			require.NoError(t, err)
			before := idx.Stats()
			// A retirement in frame 2 is newer than both requested watermarks.
			for _, cutoff := range []uint64{0, 1, 1} {
				require.NoError(t, idx.PruneRetiredThrough(t.Context(), cutoff))
			}
			afterInfo, err := os.Stat(path)
			require.NoError(t, err)
			afterBytes, err := os.ReadFile(path)
			require.NoError(t, err)
			after := idx.Stats()
			require.True(t, os.SameFile(beforeInfo, afterInfo), "no-op prune replaced the WAL")
			require.Equal(t, beforeBytes, afterBytes)
			require.Equal(t, before.JournalSequence, after.JournalSequence)
			require.Equal(t, before.RewriteCount, after.RewriteCount)
			requireShardedMutableValue(t, idx, key, want)
			require.Equal(t, withRetirement, idx.IsRetired(99, 100))

			require.NoError(t, idx.Close())
			reopened := openShardedMutableForTest(t, config)
			t.Cleanup(func() { _ = reopened.Close() })
			requireShardedMutableValue(t, reopened, key, want)
			require.Equal(t, withRetirement, reopened.IsRetired(99, 100))

			// Explicit compaction still removes superseded ordinary history even
			// when no retirement is eligible for pruning.
			before = reopened.Stats()
			require.NoError(t, reopened.CompactJournal(t.Context()))
			after = reopened.Stats()
			require.Equal(t, before.RewriteCount+1, after.RewriteCount)
			require.Less(t, after.JournalBytes, before.JournalBytes)
			requireShardedMutableValue(t, reopened, key, want)
			require.Equal(t, withRetirement, reopened.IsRetired(99, 100))
			if withRetirement {
				// Once coverage reaches the retirement, the same API must rewrite
				// the journal and remove the marker durably.
				require.NoError(t, reopened.PruneRetiredThrough(t.Context(), 2))
				require.False(t, reopened.IsRetired(99, 100))
				require.Equal(t, after.RewriteCount+1, reopened.Stats().RewriteCount)
				require.NoError(t, reopened.Close())
				recovered := openShardedMutableForTest(t, config)
				t.Cleanup(func() { _ = recovered.Close() })
				require.False(t, recovered.IsRetired(99, 100))
				requireShardedMutableValue(t, recovered, key, want)
			}
		})
	}
}

func TestShardedMutableNoOpPrunePreservesValidation(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, InitializeShardedMutableAccountIndex(dir))
	idx := openShardedMutableForTest(t, shardedMutableTestConfig(t, dir, nil, nil))
	t.Cleanup(func() { _ = idx.Close() })

	require.ErrorContains(t, idx.PruneRetiredThrough(nil, 0), "nil")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, idx.PruneRetiredThrough(ctx, 0), context.Canceled)
	require.ErrorContains(t, idx.PruneRetiredThrough(t.Context(), 1), "exceeds journal tail")

	// Cancel after the initial context check, while the prune is waiting for
	// the writer lock. The no-op path must still observe the cancellation.
	waiting, cancelWaiting := context.WithCancel(t.Context())
	defer cancelWaiting()
	checked := make(chan struct{}, 1)
	done := make(chan error, 1)
	idx.writeMu.Lock()
	go func() {
		done <- idx.PruneRetiredThrough(shardedPruneObservedContext{waiting, checked}, 0)
	}()
	<-checked
	cancelWaiting()
	idx.writeMu.Unlock()
	require.ErrorIs(t, <-done, context.Canceled)

	poison := errors.New("injected journal failure")
	idx.stateMu.Lock()
	idx.poison = poison
	idx.stateMu.Unlock()
	require.ErrorIs(t, idx.PruneRetiredThrough(t.Context(), 0), poison)
	_ = idx.Close()
	require.ErrorIs(t, idx.PruneRetiredThrough(t.Context(), 0), ErrShardedMutableClosed)
}

type shardedPruneObservedContext struct {
	context.Context
	checked chan struct{}
}

func (ctx shardedPruneObservedContext) Err() error {
	err := ctx.Context.Err()
	select {
	case ctx.checked <- struct{}{}:
	default:
	}
	return err
}
