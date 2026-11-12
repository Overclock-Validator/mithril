package epochstakes

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestCacheViewRestoresParentAndInvalidatesSiblingGenerations(t *testing.T) {
	cache := NewEpochStakesCache()
	key := solana.PublicKey{17}
	cache.PutEpoch(3, map[solana.PublicKey]uint64{key: 10}, nil, 10)
	parent := cache.CaptureView()
	parentGeneration := cache.Generation(3)
	cache.PutEpoch(3, map[solana.PublicKey]uint64{key: 90}, nil, 90)
	cache.PutEpoch(4, map[solana.PublicKey]uint64{key: 80}, nil, 80)
	abandoned := cache.CaptureView()
	abandonedGeneration := cache.Generation(4)
	cache.RestoreView(parent)
	require.Equal(t, uint64(10), cache.EpochStakes(3)[key])
	require.Equal(t, uint64(10), cache.TotalStake(3))
	require.False(t, cache.HasEpochStakes(4))
	require.Greater(t, cache.Generation(3), parentGeneration)
	require.Greater(t, cache.Generation(4), abandonedGeneration)
	// Neither restoring nor later publications mutate either retained view.
	cache.RestoreView(abandoned)
	require.Equal(t, uint64(90), cache.EpochStakes(3)[key])
	require.Equal(t, uint64(80), cache.EpochStakes(4)[key])
	cache.RestoreView(parent)
	require.Equal(t, uint64(10), cache.EpochStakes(3)[key])
}
