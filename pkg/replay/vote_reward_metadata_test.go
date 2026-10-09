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
