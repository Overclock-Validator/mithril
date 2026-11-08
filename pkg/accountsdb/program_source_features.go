package accountsdb

import (
	"github.com/Overclock-Validator/mithril/pkg/features"
)

// matchedFeatureSet records a feature set that equaled an entry's source
// features, and the feature mutation count when it did.
type matchedFeatureSet struct {
	activations any // f.Identity(); holding it keeps the set from being reused
	mutations   uint64
}

// sourceFeaturesMatch reports entry.sourceFeatures.Equal(f). Every cache hit
// asks this for the bank's feature set (hundreds of gates); the same set, with
// no Features mutation since it last matched, still matches.
func (entry *ProgramCacheEntry) sourceFeaturesMatch(f *features.Features) bool {
	activations := f.Identity()
	mutations := features.Mutations()
	if last := entry.matchedFeatures.Load(); last != nil && last.activations == activations && last.mutations == mutations {
		return true
	}
	if !entry.sourceFeatures.Equal(f) {
		return false
	}
	entry.matchedFeatures.Store(&matchedFeatureSet{activations: activations, mutations: mutations})
	return true
}
