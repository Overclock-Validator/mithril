package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/duration"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"math"
	"testing"
)

func TestClockSlotElapsedFeatureBoundaries(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}
	for _, tc := range []struct {
		gate  features.FeatureGate
		nanos uint64
	}{
		{features.ReduceSlotTimeTo350ms, 350_000_000},
		{features.ReduceSlotTimeTo300ms, 300_000_000},
		{features.ReduceSlotTimeTo250ms, 250_000_000},
		{features.ReduceSlotTimeTo200ms, 200_000_000},
	} {
		t.Run(tc.gate.Name, func(t *testing.T) {
			f := features.NewFeaturesDefault()
			f.EnableFeature(tc.gate, 150)
			elapsed := clockSlotElapsed(schedule, f)
			for _, c := range []struct{ from, to, nanos uint64 }{
				{149, 150, 400_000_000}, {198, 199, 400_000_000},
				{199, 200, tc.nanos}, {198, 201, 400_000_000 + 2*tc.nanos},
				{200, 210, 10 * tc.nanos}, {210, 210, 0}, {211, 210, 0},
			} {
				if got, want := elapsed(c.from, c.to), duration.NewDurationFromNanos(c.nanos); got != want {
					t.Fatalf("(%d,%d]: got %+v want %+v", c.from, c.to, got, want)
				}
			}
		})
	}
}

func TestClockSlotElapsedOutOfOrderAndLegacy(t *testing.T) {
	s := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.ReduceSlotTimeTo200ms, 50)
	f.EnableFeature(features.ReduceSlotTimeTo350ms, 150)
	f.EnableFeature(features.ReduceSlotTimeTo300ms, 50)
	if got, want := clockSlotElapsed(s, f)(98, 202), duration.NewDurationFromNanos(400_000_000+103*200_000_000); got != want {
		t.Fatalf("out-of-order gates: got %+v want %+v", got, want)
	}
	if got, want := clockSlotElapsed(s, nil)(10, 20), duration.NewDurationFromSecs(4); got != want {
		t.Fatalf("legacy: got %+v want %+v", got, want)
	}
	if got := clockSlotElapsed(s, nil)(0, math.MaxUint64); got.Secs != 7378697629483820646 || got.Nanos != 0 {
		t.Fatalf("large range overflow: %+v", got)
	}
}

func TestClockTimestampMainnet200msRegression(t *testing.T) {
	s := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.ReduceSlotTimeTo200ms, 450000000)
	key := solana.PublicKey{1}
	votes := []*tsEntry{{pubkey: key, slot: 460151569, timestamp: 1792696778}}
	stakes := map[solana.PublicKey]uint64{key: 1}
	got, err := calculateStakeWeightedTimestampWithElapsed(votes, stakes, 460151570, clockSlotElapsed(s, f), 460080000, 1792681598)
	if err != nil || got != 1792696778 {
		t.Fatalf("got %d, %v; want unchanged timestamp 1792696778", got, err)
	}
	legacy, err := calculateStakeWeightedTimestamp(votes, stakes, 460151570, duration.NewDurationFromNanos(400_000_000), 460080000, 1792681598)
	if err != nil || legacy != 1792703069 {
		t.Fatalf("legacy reproduction: got %d, %v", legacy, err)
	}
}
