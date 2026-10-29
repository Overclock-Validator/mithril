package features

import (
	"fmt"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/base58"
)

type FeatureGate struct {
	Name    string
	Address [32]byte
}

type FeatureActivationInfo struct {
	Enabled        bool
	ActivationSlot uint64
}

type Features map[FeatureGate]FeatureActivationInfo

// mutations counts in-place changes to any Features map. A caller that compared
// a map earlier can tell, from the same map object and an unchanged count, that
// it still holds the same activations without walking it again.
var mutations atomic.Uint64

// Mutations returns the number of in-place Features changes made so far.
func Mutations() uint64 { return mutations.Load() }

func NewFeaturesDefault() *Features {
	newMap := make(Features, 0)
	return &newMap
}

// Clone returns an independent copy of the feature activation map.
func (f *Features) Clone() *Features {
	clone := NewFeaturesDefault()
	if f == nil {
		return clone
	}
	for gate, info := range *f {
		(*clone)[gate] = info
	}
	return clone
}

func (f *Features) EnableFeature(gate FeatureGate, activationSlot uint64) {
	(*f)[gate] = FeatureActivationInfo{Enabled: true, ActivationSlot: activationSlot}
	mutations.Add(1)
}

func (f *Features) DisableFeature(gate FeatureGate) {
	(*f)[gate] = FeatureActivationInfo{Enabled: false}
	mutations.Add(1)
}

func (f *Features) IsActive(gate FeatureGate) bool {
	if info, found := (*f)[gate]; found {
		return info.Enabled
	} else {
		return false
	}
}

func (f *Features) IsSbpfV3DeploymentAndExecutionActive() bool {
	return f.IsActive(EnableSbpfV3DeploymentAndExecution)
}

func (f *Features) ActivationSlot(gate FeatureGate) (uint64, bool) {
	if !f.IsActive(gate) {
		return 0, false
	}
	return (*f)[gate].ActivationSlot, true
}

func (f *Features) AllEnabled() []string {
	enabledFeatureStrs := make([]string, 0)
	for feat, enabled := range *f {
		if enabled.Enabled {
			enabledFeatureStrs = append(enabledFeatureStrs, fmt.Sprintf("feature %s (%s) enabled", feat.Name, base58.Encode(feat.Address[:])))
		}
	}
	return enabledFeatureStrs
}

func (f *Features) FullInflationFeaturesEnabled() []FeatureGate {
	var enabledFeatures []FeatureGate

	if f.IsActive(FullInflationVote) && f.IsActive(FullInflationEnable) {
		enabledFeatures = append(enabledFeatures, FullInflationEnable)
	}

	if f.IsActive(FullInflationDevnetAndTestnet) {
		enabledFeatures = append(enabledFeatures, FullInflationDevnetAndTestnet)
	}

	return enabledFeatures
}
