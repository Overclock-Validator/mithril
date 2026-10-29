package accountsdb

import (
	"maps"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/features"
)

// matchedFeatureSet records a feature map that equaled an entry's source
// features, and the feature mutation count when it did.
type matchedFeatureSet struct {
	activations unsafe.Pointer // the map object; holding it keeps its address from being reused
	mutations   uint64
}

// sourceFeaturesMatch reports maps.Equal(entry.sourceFeatures, *f). Every cache
// hit asks this for the bank's feature map (hundreds of gates); the same map
// object, with no Features mutation since it last matched, still matches.
func (entry *ProgramCacheEntry) sourceFeaturesMatch(f *features.Features) bool {
	activations := *(*unsafe.Pointer)(unsafe.Pointer(f))
	mutations := features.Mutations()
	if last := entry.matchedFeatures.Load(); last != nil && last.activations == activations && last.mutations == mutations {
		return true
	}
	if !maps.Equal(entry.sourceFeatures, *f) {
		return false
	}
	entry.matchedFeatures.Store(&matchedFeatureSet{activations: activations, mutations: mutations})
	return true
}
