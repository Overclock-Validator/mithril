package features

import (
	"fmt"
	"iter"
	"maps"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/base58"
)

// FeatureGate identifies a feature. Gates declared in this package carry an
// index, so checking them is an array read instead of a map lookup; gates
// built elsewhere (tests) have index 0 and are looked up in the map.
type FeatureGate struct {
	Name    string
	Address [32]byte
	index   uint16
}

// maxFeatureGates bounds the number of gates newGate can register.
const maxFeatureGates = 512

var registeredGates uint16

// newGate registers a gate under the next index.
func newGate(name, address string) FeatureGate {
	if int(registeredGates)+1 >= maxFeatureGates {
		panic("features: too many feature gates")
	}
	registeredGates++
	return FeatureGate{Name: name, Address: base58.MustDecodeFromString(address), index: registeredGates}
}

type FeatureActivationInfo struct {
	Enabled        bool
	ActivationSlot uint64
}

// Features is a bank's feature activation set. Copies of a Features value share
// one set, as copies of the map it used to be did.
type Features struct {
	set *featureSet
}

type featureSet struct {
	gates  map[FeatureGate]FeatureActivationInfo
	active [maxFeatureGates]bool // gates[g].Enabled by g.index, for registered gates
}

// mutations counts in-place changes to any Features set. A caller that compared
// a set earlier can tell, from the same set (Identity) and an unchanged count,
// that it still holds the same activations without walking it again.
var mutations atomic.Uint64

// Mutations returns the number of in-place Features changes made so far.
func Mutations() uint64 { return mutations.Load() }

func NewFeaturesDefault() *Features {
	return &Features{set: &featureSet{gates: make(map[FeatureGate]FeatureActivationInfo)}}
}

// Clone returns an independent copy of the feature activation set.
func (f *Features) Clone() *Features {
	clone := NewFeaturesDefault()
	if f == nil || f.set == nil {
		return clone
	}
	maps.Copy(clone.set.gates, f.set.gates)
	clone.set.active = f.set.active
	return clone
}

// Identity returns a comparable value that is equal for two Features exactly
// when they share one set. Holding it keeps the set alive.
func (f *Features) Identity() any {
	if f == nil {
		return (*featureSet)(nil)
	}
	return f.set
}

// Equal reports whether f and other hold the same activations.
func (f *Features) Equal(other *Features) bool {
	return maps.Equal(f.gateMap(), other.gateMap())
}

// All iterates over every gate in the set, enabled or not, in no fixed order.
func (f *Features) All() iter.Seq2[FeatureGate, FeatureActivationInfo] {
	return maps.All(f.gateMap())
}

// Len returns the number of gates in the set, enabled or not.
func (f *Features) Len() int { return len(f.gateMap()) }

// Info returns the gate's entry in the set.
func (f *Features) Info(gate FeatureGate) (FeatureActivationInfo, bool) {
	info, ok := f.gateMap()[gate]
	return info, ok
}

func (f *Features) gateMap() map[FeatureGate]FeatureActivationInfo {
	if f == nil || f.set == nil {
		return nil
	}
	return f.set.gates
}

func (f *Features) setFeature(gate FeatureGate, info FeatureActivationInfo) {
	if f.set == nil {
		f.set = &featureSet{gates: make(map[FeatureGate]FeatureActivationInfo)}
	}
	f.set.gates[gate] = info
	if gate.index != 0 {
		f.set.active[gate.index] = info.Enabled
	}
	mutations.Add(1)
}

func (f *Features) EnableFeature(gate FeatureGate, activationSlot uint64) {
	f.setFeature(gate, FeatureActivationInfo{Enabled: true, ActivationSlot: activationSlot})
}

func (f *Features) DisableFeature(gate FeatureGate) {
	f.setFeature(gate, FeatureActivationInfo{Enabled: false})
}

func (f *Features) IsActive(gate FeatureGate) bool {
	if f == nil || f.set == nil {
		return false
	}
	if gate.index != 0 {
		return f.set.active[gate.index]
	}
	return f.set.gates[gate].Enabled
}

func (f *Features) IsSbpfV3DeploymentAndExecutionActive() bool {
	return f.IsActive(EnableSbpfV3DeploymentAndExecution)
}

func (f *Features) ActivationSlot(gate FeatureGate) (uint64, bool) {
	if !f.IsActive(gate) {
		return 0, false
	}
	return f.set.gates[gate].ActivationSlot, true
}

func (f *Features) AllEnabled() []string {
	enabledFeatureStrs := make([]string, 0)
	for feat, enabled := range f.gateMap() {
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
