package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/stretchr/testify/require"
)

func TestAlpenglowMetadataAddresses(t *testing.T) {
	require.Equal(t, features.AlpenglowFeatureGateAddress, AlpenglowFeatureGatePubkey)
	require.Equal(t, "A1pengvuM6JEcyNuTnMqepBKhwHE3N6PmUrdATGawhJS", alpenglowFeatureGatePubkey.String())
	require.Equal(t, "3vJzniWALu2qGFJ1ZY6JD3Y2ZmA5zWqmVdDX7yBXHgu7", VoteRewardAccountAddr().String())
	require.Equal(t, "GaGQ2vyb3xuUwjKiq9tQQWoqhzWYh7LbiLXvqG2sUzzG", NanosecondClockAccountAddr().String())
	require.Equal(t, "CHfmHwNcskfxg1ZnLatbHbmVvYbA6rLDz3Xok5a3Ku8i", RewardEpochDelegatedStakesAccountAddr().String())
}

func TestGenesisAlpenglowMetadataNamespace(t *testing.T) {
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.AlpenglowGenesisV1, 0)
	// Independently observed from Bank::update_clock_from_footer in the pinned
	// Agave oracle. No deployed feature account is synthesized for genesis.
	require.Equal(t, "A5HnwXe9U16naY6JUPLEfobUAmCch4yxhnxd2criXCdK", NanosecondClockAccountAddr(f).String())
	require.True(t, alpenglowClockFeatureActive(f))
	require.False(t, f.IsActive(features.Alpenglow))
	require.NotEqual(t, VoteRewardAccountAddr(), VoteRewardAccountAddr(f))
	require.NotEqual(t, RewardEpochDelegatedStakesAccountAddr(), RewardEpochDelegatedStakesAccountAddr(f))
	f.EnableFeature(features.Alpenglow, 1)
	require.Equal(t, NanosecondClockAccountAddr(), NanosecondClockAccountAddr(f))
	require.Equal(t, VoteRewardAccountAddr(), VoteRewardAccountAddr(f))
	require.Equal(t, RewardEpochDelegatedStakesAccountAddr(), RewardEpochDelegatedStakesAccountAddr(f))
}
