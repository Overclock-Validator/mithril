package accounts

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkingSetRetainedVersionsReleaseOnPromotionAndUnwind(t *testing.T) {
	w := NewWorkingSet()
	a := wsAcct(1, 1)
	a.Data = make([]byte, 1, 8192) // charge capacity, not just visible length
	w.Add(1, []*Account{a, a, nil})
	first := w.RetainedBytes()
	require.GreaterOrEqual(t, first, uint64(8192))
	w.Add(1, []*Account{a})
	require.Equal(t, first, w.RetainedBytes(), "same-slot replacement must not double-charge")
	b := wsAcct(1, 2)
	b.Data = make([]byte, 16384)
	w.Add(2, []*Account{b})
	require.Greater(t, w.RetainedBytes(), first+16384, "older versions still occupy RAM")
	require.NoError(t, w.CheckInvariants())
	w.EvictFrom(2)
	require.Equal(t, first, w.RetainedBytes())
	got, ok := w.Lookup(wsKey(1))
	require.True(t, ok)
	require.Same(t, a, got)
	w.Add(2, []*Account{b})
	w.PromotePrefix(1)
	require.Equal(t, RetainedAccountBytes(b), w.RetainedBytes())
	require.NoError(t, w.CheckInvariants())
	w.PromotePrefix(2)
	require.Zero(t, w.RetainedBytes())
	require.NoError(t, w.CheckInvariants())
}

func TestWorkingSetByteBoundedChunksRespectSafePrefix(t *testing.T) {
	w := NewWorkingSet()
	for slot := uint64(1); slot <= 5; slot++ {
		a := wsAcct(1, slot)
		a.Data = make([]byte, 8192)
		w.Add(slot, []*Account{a})
	}
	require.Nil(t, w.PromotionChunkWithByteLimit(1, 128, 16384, false), "unverified suffix cannot satisfy the byte target")
	chunk := w.PromotionChunkWithByteLimit(5, 128, 16384, false)
	require.Len(t, chunk, 2)
	require.Equal(t, uint64(2), chunk[1].Slot)
	partial := w.PromotionChunkWithByteLimit(1, 128, 16384, true)
	require.Len(t, partial, 1)
	require.Equal(t, uint64(1), partial[0].Slot)
	require.Nil(t, w.PromotionChunkWithByteLimit(0, 128, 1, true))
	require.Len(t, w.PromotionChunkWithByteLimit(5, 128, 1, false), 1, "an oversized atomic slot must make progress")
	require.Equal(t, 5, w.HeldSlots(), "selection must not mutate retained state")
}
