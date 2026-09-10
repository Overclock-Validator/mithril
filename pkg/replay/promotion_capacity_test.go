package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type limitedBatchCommitter struct {
	*fakeCommitter
	limit uint64
}

func (committer *limitedBatchCommitter) MaxBatchAccountIndexMutations() uint64 {
	return committer.limit
}

func capacitySlot(slot uint64, keys ...byte) accounts.SlotDelta {
	delta := make([]*accounts.Account, 0, len(keys))
	for _, key := range keys {
		delta = append(delta, testAccount(key, slot))
	}
	return accounts.SlotDelta{Slot: slot, Delta: delta}
}

func TestSelectFoldChunkHonorsAtomicMutationLimit(t *testing.T) {
	prefix := []accounts.SlotDelta{
		capacitySlot(1, 1, 2),
		capacitySlot(2, 3, 4),
		capacitySlot(3, 5, 6),
		capacitySlot(4, 7),
	}

	chunk, ready, err := selectFoldChunk(prefix, 4, false, 5)
	require.NoError(t, err)
	require.True(t, ready)
	require.Len(t, chunk, 2)
	assert.Equal(t, uint64(2), chunk[len(chunk)-1].Slot)

	chunk, ready, err = selectFoldChunk(prefix[:2], 4, false, 5)
	require.NoError(t, err)
	assert.False(t, ready, "an under-limit trailing partial chunk remains in RAM")
	assert.Nil(t, chunk)

	chunk, ready, err = selectFoldChunk([]accounts.SlotDelta{
		capacitySlot(1, 1, 2),
		capacitySlot(2, 3, 4, 5),
	}, 4, false, 5)
	require.NoError(t, err)
	require.True(t, ready, "a partial chunk at the exact capacity must fold")
	require.Len(t, chunk, 2)
}

func TestSelectFoldChunkKeepsOneOversizedSlotWhole(t *testing.T) {
	chunk, ready, err := selectFoldChunk([]accounts.SlotDelta{
		capacitySlot(99, 1, 2, 3),
	}, 128, true, 2)
	require.NoError(t, err)
	require.True(t, ready)
	require.Len(t, chunk, 1)
	assert.Equal(t, uint64(99), chunk[0].Slot)
}

func TestPromoteRootedBatchedAdaptivelySplitsAtSlots(t *testing.T) {
	overlay := accounts.NewWorkingSet()
	contexts := make(map[uint64]*state.ResumeContext)
	for slot, keys := range [][]byte{{1, 2}, {3, 4}, {5, 6}, {7, 8}} {
		slotNumber := uint64(slot + 1)
		delta := capacitySlot(slotNumber, keys...)
		overlay.Add(slotNumber, delta.Delta)
		contexts[slotNumber] = &state.ResumeContext{Slot: slotNumber}
	}
	committer := &limitedBatchCommitter{
		fakeCommitter: &fakeCommitter{durable: accounts.NewMemAccounts()},
		limit:         5,
	}

	promoted, err := promoteRootedBatched(
		overlay, 4, nil, contexts, committer, 4, "", true,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), promoted)
	assert.Equal(t, []uint64{2, 4}, committer.throughs)
	assert.Zero(t, overlay.HeldSlots())
}

func TestBuildFoldJobUsesAdaptiveCapacity(t *testing.T) {
	committer := &limitedBatchCommitter{
		fakeCommitter: &fakeCommitter{durable: accounts.NewMemAccounts()},
		limit:         3,
	}
	tail := newUnrootedTail(&fakeDurable{}, committer, 16, 4, "")
	for slot, keys := range [][]byte{{1, 2}, {3, 4}, {5}} {
		slotNumber := uint64(slot + 1)
		tail.Add(slotNumber, capacitySlot(slotNumber, keys...).Delta, nil)
		tail.SetContext(slotNumber, &state.ResumeContext{Slot: slotNumber})
	}

	job, err := tail.buildFoldJob(3, false)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Len(t, job.chunk, 1)
	assert.Equal(t, uint64(1), job.through)
}
