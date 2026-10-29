package accountsdb

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/stretchr/testify/require"
)

// The remembered feature match must never outlive a change to the map's
// activations, and must agree with maps.Equal for every map it is asked about.
func TestProgramCacheFeatureMatchTracksMutations(t *testing.T) {
	source := []byte{1, 2, 3, 4}
	bank := features.NewFeaturesDefault()
	bank.EnableFeature(features.Alpenglow, 10)
	entry := &ProgramCacheEntry{}
	entry.BindSource(source, bank)

	for i := 0; i < 3; i++ { // first call compares, later calls take the remembered match
		require.True(t, entry.MatchesSource(source, bank))
	}
	require.NotNil(t, entry.matchedFeatures.Load())

	// In-place activation on the same map object: no longer the bound features.
	bank.EnableFeature(features.RemoveAccountsDeltaHash, 20)
	require.False(t, entry.MatchesSource(source, bank))
	// Disabling records an explicit false entry, which still differs from absence.
	bank.DisableFeature(features.RemoveAccountsDeltaHash)
	require.False(t, entry.MatchesSource(source, bank))

	// A different map object with equal activations matches.
	equal := features.NewFeaturesDefault()
	equal.EnableFeature(features.Alpenglow, 10)
	require.True(t, entry.MatchesSource(source, equal))
	// Same activation, different slot: not equal.
	other := features.NewFeaturesDefault()
	other.EnableFeature(features.Alpenglow, 11)
	require.False(t, entry.MatchesSource(source, other))
	// The previously matched map still matches after asking about another one.
	require.True(t, entry.MatchesSource(source, equal))

	// Rebinding forgets the old match.
	entry.BindSource(source, other)
	require.Nil(t, entry.matchedFeatures.Load())
	require.False(t, entry.MatchesSource(source, equal))
	require.True(t, entry.MatchesSource(source, other))
}
