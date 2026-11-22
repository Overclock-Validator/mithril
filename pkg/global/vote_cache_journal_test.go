package global

import (
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestVoteCacheJournalRestoresRetainedPrefixAndRepeatedSibling(t *testing.T) {
	ResetVoteCacheJournal()
	t.Cleanup(ResetVoteCacheJournal)
	key, newKey, deletedKey := solana.PublicKey{201}, solana.PublicKey{202}, solana.PublicKey{203}
	for _, key := range []solana.PublicKey{key, newKey, deletedKey} {
		old := VoteCacheItem(key)
		t.Cleanup(func() {
			if old == nil {
				DeleteVoteCacheItem(key)
			} else {
				PutVoteCacheItem(key, old)
			}
		})
	}
	root, retained, discarded, sibling := new(sealevel.VoteStateVersions), new(sealevel.VoteStateVersions), new(sealevel.VoteStateVersions), new(sealevel.VoteStateVersions)
	PutVoteCacheItem(key, root)
	PutVoteCacheItem(deletedKey, root)
	DeleteVoteCacheItem(newKey)
	PutVoteCacheItemForSlot(11, key, retained)
	PutVoteCacheItemForSlot(12, key, discarded)
	DeleteVoteCacheItemForSlot(12, key)
	PutVoteCacheItemForSlot(12, key, sibling) // first before-value must survive
	PutVoteCacheItemForSlot(12, newKey, discarded)
	DeleteVoteCacheItemForSlot(12, deletedKey)
	PutVoteCacheItemForSlot(13, key, discarded)
	RewindVoteCache(12)
	require.Same(t, retained, VoteCacheItem(key))
	require.Nil(t, VoteCacheItem(newKey))
	require.Same(t, root, VoteCacheItem(deletedKey))
	PutVoteCacheItemForSlot(12, key, sibling)
	RewindVoteCache(12)
	require.Same(t, retained, VoteCacheItem(key), "same-slot sibling records a new undo base")
	RewindVoteCache(11)
	require.Same(t, root, VoteCacheItem(key))
}

func TestVoteCacheJournalParallelWritesAndEpochRebuild(t *testing.T) {
	ResetVoteCacheJournal()
	t.Cleanup(ResetVoteCacheJournal)
	before := VoteCacheSnapshot()
	t.Cleanup(func() {
		for key := range VoteCacheSnapshot() {
			DeleteVoteCacheItem(key)
		}
		for key, state := range before {
			PutVoteCacheItem(key, state)
		}
	})
	WithVoteCacheJournalSlot(20, func() {
		var workers sync.WaitGroup
		for n := 0; n < 32; n++ {
			workers.Add(1)
			go func(n int) {
				defer workers.Done()
				key := solana.PublicKey{211, byte(n)}
				PutVoteCacheItem(key, new(sealevel.VoteStateVersions))
				DeleteVoteCacheItem(key)
				PutVoteCacheItem(key, new(sealevel.VoteStateVersions))
			}(n)
		}
		workers.Wait()
	})
	RewindVoteCache(20)
	require.Equal(t, before, VoteCacheSnapshot())
}

func TestVoteCacheJournalPrunesOnlyDurablePrefix(t *testing.T) {
	ResetVoteCacheJournal()
	t.Cleanup(ResetVoteCacheJournal)
	key := solana.PublicKey{215}
	old := VoteCacheItem(key)
	t.Cleanup(func() {
		if old == nil {
			DeleteVoteCacheItem(key)
		} else {
			PutVoteCacheItem(key, old)
		}
	})
	root, tip := new(sealevel.VoteStateVersions), new(sealevel.VoteStateVersions)
	PutVoteCacheItemForSlot(21, key, root)
	PutVoteCacheItemForSlot(22, key, tip)
	PruneVoteCacheJournal(21)
	require.NotContains(t, instance.voteCacheUndo, uint64(21))
	require.Contains(t, instance.voteCacheUndo, uint64(22))
	RewindVoteCache(22)
	require.Same(t, root, VoteCacheItem(key))
}
