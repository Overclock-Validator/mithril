package dashboard

import (
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/gagliardetto/solana-go"
)

func TestPublicLeaderOutcomesRejectCountsOutsideWindow(t *testing.T) {
	now := time.Now().UTC()
	s := testSnapshot(now)
	s.Mode = "validator"
	s.IdentityKey = new("test-identity")
	through := uint64(10)
	s.Replay = &Replay{SampledAt: now, StartedAt: now.Add(-time.Minute), DurableSlot: &through,
		LeaderOutcomes: &LeaderOutcomes{SampledAt: now, Provenance: "selected_and_folded", Identity: *s.IdentityKey,
			HasWindow: true, WindowStart: 0, WindowEnd: through, HasCoveredRange: true, CoveredStart: 0, CoveredEnd: through,
			Produced: 3, SelectedForkSkipped: 1, ScheduledUnknown: 2, UnknownScheduleSlots: 1, UnknownAncestrySlots: 2, CoverageIncomplete: true}}
	s.Consensus = &Consensus{SampledAt: now, Voting: &Voting{SampledAt: now, Identity: *s.IdentityKey, VoteAccount: "test-vote"}}
	if p, _ := publicValue(t, &s, "fresh", now); p.Replay.LeaderOutcomes == nil {
		t.Fatal("valid leader outcome omitted")
	}
	s.Replay.LeaderOutcomes.Produced = 20 // Eleven slots cannot contain twenty assignments.
	if p, _ := publicValue(t, &s, "fresh", now); p.Replay.LeaderOutcomes != nil {
		t.Fatal("impossible leader counts were published")
	}
}

func TestLeaderWindowAcceptsProducerResults(t *testing.T) {
	identity, other := solana.PublicKey{1}, solana.PublicKey{2}
	anchor := replay.CertificateFoldEvidence{Slot: 0, BlockID: solana.Hash{1}, Bankhash: solana.Hash{2}, HasBlockID: true}
	child := replay.LeaderOutcomeCarrier{CertificateFoldEvidence: replay.CertificateFoldEvidence{Slot: 20, BlockID: solana.Hash{3}, Bankhash: solana.Hash{4}, HasBlockID: true},
		ParentSlot: 0, ExecutionParentSlot: 0, ParentBlockID: anchor.BlockID, HasParentBlockID: true, ExecutionParentBankhash: anchor.Bankhash}
	lookup := func(slot uint64) (solana.PublicKey, bool) {
		switch slot % 5 {
		case 0:
			return solana.PublicKey{}, false
		case 1, 2:
			return identity, true
		default:
			return other, true
		}
	}
	for _, tc := range []struct {
		through uint64
		rows    []replay.LeaderOutcomeCarrier
		parent  *replay.CertificateFoldEvidence
		lookup  func(uint64) (solana.PublicKey, bool)
	}{
		{0, []replay.LeaderOutcomeCarrier{{CertificateFoldEvidence: anchor}}, nil, lookup},
		{20, []replay.LeaderOutcomeCarrier{child}, &anchor, lookup},
		{20, []replay.LeaderOutcomeCarrier{child}, nil, lookup},
		{511, nil, nil, nil},
		{math.MaxUint64, nil, nil, lookup},
	} {
		s := replay.ComputeLeaderOutcomes(identity, tc.through, tc.rows, tc.parent, tc.lookup)
		v := LeaderOutcomes{HasWindow: s.HasWindow, WindowStart: s.WindowStart, WindowEnd: s.WindowEnd, HasCoveredRange: s.HasCoveredRange,
			CoveredStart: s.CoveredStart, CoveredEnd: s.CoveredEnd, Produced: s.Produced, SelectedForkSkipped: s.SelectedForkSkipped,
			ScheduledUnknown: s.ScheduledUnknown, UnknownScheduleSlots: s.UnknownScheduleSlots, UnknownAncestrySlots: s.UnknownAncestrySlots, CoverageIncomplete: s.CoverageIncomplete}
		if !validLeaderOutcomeWindow(&v, &tc.through) {
			t.Fatalf("producer result rejected at %d: %+v", tc.through, s)
		}
	}
}

func TestLeaderOutcomeWindowValidation(t *testing.T) {
	through := uint64(10)
	valid := LeaderOutcomes{HasWindow: true, WindowEnd: through, HasCoveredRange: true, CoveredEnd: through,
		Produced: 3, SelectedForkSkipped: 1, ScheduledUnknown: 2, UnknownScheduleSlots: 1, UnknownAncestrySlots: 2, CoverageIncomplete: true}
	if !validLeaderOutcomeWindow(&valid, &through) {
		t.Fatal("valid overlapping unknown counts rejected")
	}
	for name, change := range map[string]func(*LeaderOutcomes){
		"reversed window":     func(v *LeaderOutcomes) { v.WindowStart = 11 },
		"future window":       func(v *LeaderOutcomes) { v.WindowEnd = 11 },
		"overflow count":      func(v *LeaderOutcomes) { v.Produced = math.MaxUint64 },
		"unknown schedule":    func(v *LeaderOutcomes) { v.UnknownScheduleSlots = 12 },
		"unknown ancestry":    func(v *LeaderOutcomes) { v.UnknownAncestrySlots = 12 },
		"unknown assignment":  func(v *LeaderOutcomes) { v.ScheduledUnknown = 3 },
		"contradictory cover": func(v *LeaderOutcomes) { v.CoverageIncomplete = false },
		"missing range":       func(v *LeaderOutcomes) { v.HasCoveredRange = false },
		"bad range":           func(v *LeaderOutcomes) { v.CoveredStart = 8; v.CoveredEnd = 7 },
		"narrow range":        func(v *LeaderOutcomes) { v.CoveredStart = 9 },
	} {
		t.Run(name, func(t *testing.T) {
			v := valid
			change(&v)
			if validLeaderOutcomeWindow(&v, &through) {
				t.Fatal("invalid window accepted")
			}
		})
	}
	if validLeaderOutcomeWindow(&valid, nil) {
		t.Fatal("window without durable slot accepted")
	}
	wide := LeaderOutcomes{HasWindow: true, WindowEnd: 512, CoverageIncomplete: true}
	wideDurable := uint64(512)
	if validLeaderOutcomeWindow(&wide, &wideDurable) {
		t.Fatal("window longer than 512 slots accepted")
	}
	unknown := LeaderOutcomes{HasWindow: true, WindowEnd: through, ScheduledUnknown: 11, UnknownAncestrySlots: 11, CoverageIncomplete: true}
	if !validLeaderOutcomeWindow(&unknown, &through) || !validLeaderOutcomeWindow(&LeaderOutcomes{}, nil) {
		t.Fatal("unknown or absent window rejected")
	}
	// Anonymous counts from two earlier isolated-node observations, not live state.
	for _, observed := range []LeaderOutcomes{
		{HasWindow: true, WindowStart: 622, WindowEnd: 1133, HasCoveredRange: true, CoveredStart: 981, CoveredEnd: 1133,
			SelectedForkSkipped: 12, ScheduledUnknown: 28, UnknownScheduleSlots: 146, UnknownAncestrySlots: 359, CoverageIncomplete: true},
		{HasWindow: true, WindowStart: 770, WindowEnd: 1281, HasCoveredRange: true, CoveredStart: 981, CoveredEnd: 1281,
			Produced: 16, SelectedForkSkipped: 12, ScheduledUnknown: 28, UnknownAncestrySlots: 211, CoverageIncomplete: true},
	} {
		if !validLeaderOutcomeWindow(&observed, &observed.WindowEnd) {
			t.Fatalf("real observed window rejected: %+v", observed)
		}
	}
}
