package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

func applyAlpenglowRuntimeFeatureOverrides(f *features.Features, slot uint64) {
	if f == nil {
		return
	}

	var enabled []string
	for _, gate := range []features.FeatureGate{
		features.VoteStateV4,
		features.TimelyVoteCredits,
		features.DeprecateUnusedLegacyVotePlumbing,
	} {
		if f.IsActive(gate) {
			continue
		}
		f.EnableFeature(gate, slot)
		enabled = append(enabled, gate.Name)
	}
	if len(enabled) != 0 {
		mlog.Log.Infof("Alpenglow mode: forcing runtime vote feature(s) at slot %d: %v", slot, enabled)
	}
}
func alpenglowClockFeatureActive(f *features.Features) bool {
	if f == nil {
		return false
	}
	return f.IsActive(features.Alpenglow) || f.IsActive(features.AlpenglowDevContext)
}
