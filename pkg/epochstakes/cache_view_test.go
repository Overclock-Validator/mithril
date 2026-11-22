package epochstakes

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestCacheViewRestoresParentAndInvalidatesSiblingGenerations(t *testing.T) {
	cache := NewEpochStakesCache()
	key := solana.PublicKey{17}
	cache.PutEpochWithSelection(3, map[solana.PublicKey]uint64{key: 10}, nil, 10, SelectionUnfiltered)
	parent := cache.CaptureView()
	parentGeneration := cache.Generation(3)
	cache.PutEpochWithSelection(3, map[solana.PublicKey]uint64{key: 90}, nil, 90, SelectionVATFiltered)
	cache.PutEpochWithSelection(4, map[solana.PublicKey]uint64{key: 80}, nil, 80, SelectionVATFiltered)
	abandoned := cache.CaptureView()
	abandonedGeneration := cache.Generation(4)
	cache.RestoreView(parent)
	require.Equal(t, uint64(10), cache.EpochStakes(3)[key])
	require.Equal(t, uint64(10), cache.TotalStake(3))
	restored, ok := cache.Snapshot(3)
	require.True(t, ok)
	require.Equal(t, SelectionUnfiltered, restored.Selection)
	require.False(t, cache.HasEpochStakes(4))
	require.NotContains(t, cache.selectionCache, uint64(4), "removed sibling provenance must not survive")
	require.Greater(t, cache.Generation(3), parentGeneration)
	require.Greater(t, cache.Generation(4), abandonedGeneration)
	// Neither restoring nor later publications mutate either retained view.
	cache.RestoreView(abandoned)
	restored, ok = cache.Snapshot(3)
	require.True(t, ok)
	require.Equal(t, SelectionVATFiltered, restored.Selection)
	require.Equal(t, uint64(90), cache.EpochStakes(3)[key])
	require.Equal(t, uint64(80), cache.EpochStakes(4)[key])
	cache.RestoreView(parent)
	restored, ok = cache.Snapshot(3)
	require.True(t, ok)
	require.Equal(t, SelectionUnfiltered, restored.Selection)
	require.NotContains(t, cache.selectionCache, uint64(4))
	require.Equal(t, uint64(10), cache.EpochStakes(3)[key])
}
