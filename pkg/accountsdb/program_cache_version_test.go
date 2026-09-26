package accountsdb

import (
	"github.com/Overclock-Validator/mithril/pkg/features"
	"testing"
)

func TestProgramCacheSourceBinding(t *testing.T) {
	f := features.NewFeaturesDefault()
	source := []byte{1, 2, 3}
	entry := &ProgramCacheEntry{DeploymentSlot: 100}
	if entry.MatchesSource(source, f) {
		t.Fatal("unbound entry matched")
	}
	entry.BindSource(source, f)
	if !entry.MatchesSource(source, f) {
		t.Fatal("identical source missed")
	}
	source[0]++
	if entry.MatchesSource(source, f) {
		t.Fatal("same-slot fork source matched")
	}
	source[0]--
	f.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	if entry.MatchesSource(source, f) {
		t.Fatal("different feature environment matched")
	}
	if !entry.MatchesSource(source, features.NewFeaturesDefault()) {
		t.Fatal("binding retained mutable feature map")
	}
}
