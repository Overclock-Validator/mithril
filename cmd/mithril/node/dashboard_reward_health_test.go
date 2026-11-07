package node

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/gagliardetto/solana-go"
)

func TestRewardHealthMeasurementPreservesBindingAndKnownZero(t *testing.T) {
	at := time.Now()
	p := replay.ProgressSnapshot{RewardHealth: replay.RewardHealthSummary{SampledAt: at, Identity: solana.PublicKey{1}, VoteAccount: solana.PublicKey{2},
		Through: 8, HasWindow: true, Included: 1, CoverageComplete: true, Health: replay.RewardHealthIncludedRecently,
		HasLatestResolved: true, HasLatestIncluded: true,
		LatestResolved: replay.RewardTargetObservation{Outcome: replay.RewardTargetIncluded},
		LatestIncluded: replay.RewardTargetObservation{Outcome: replay.RewardTargetIncluded}}}
	m := replayMeasurement(p).RewardHealth
	if m == nil || m.SampledAt.Location() != time.UTC || m.Identity != p.RewardHealth.Identity.String() || m.VoteAccount != p.RewardHealth.VoteAccount.String() ||
		m.WindowFirst == nil || *m.WindowFirst != 0 || m.WindowLast == nil || *m.WindowLast != 0 || m.LatestIncluded == nil || m.LatestIncluded.Slot != 0 ||
		m.LatestResolved == nil || m.LatestResolved.Outcome != "included" || m.Health != "included_recently" || m.Through != 8 || m.Included != 1 || !m.CoverageComplete {
		t.Fatalf("wrong reward-health mapping: %+v", m)
	}
	p.RewardHealth.LatestIncluded.TargetSlot = 99
	p.RewardHealth.WindowLast = 99
	if m.LatestIncluded.Slot != 0 || *m.WindowLast != 0 {
		t.Fatal("adapter retained source aliases")
	}
	p.RewardHealth.HasLatestResolved, p.RewardHealth.HasLatestIncluded, p.RewardHealth.HasWindow = false, false, false
	unknown := replayMeasurement(p).RewardHealth
	if unknown.LatestResolved != nil || unknown.LatestIncluded != nil || unknown.WindowFirst != nil || unknown.WindowLast != nil {
		t.Fatal("unavailable references became known from numeric defaults")
	}
	if replayMeasurement(replay.ProgressSnapshot{}).RewardHealth != nil {
		t.Fatal("old producer acquired invented reward health")
	}
}
