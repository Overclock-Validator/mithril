package dashboard

import (
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/gagliardetto/solana-go"
)

func TestPublicCertificateOutcomesRejectImpossibleCounts(t *testing.T) {
	now := time.Now().UTC()
	s := testSnapshot(now)
	s.Mode = "validator"
	s.IdentityKey = new("test-identity")
	through := uint64(10)
	s.Replay = &Replay{SampledAt: now, StartedAt: now.Add(-time.Minute), DurableSlot: &through,
		CertificateOutcomes: &CertificateOutcomes{SampledAt: now, Provenance: "selected_and_folded", VoteAccount: "test-vote",
			HasWindow: true, FirstCarrier: 9, LastCarrier: 10, Carriers: 2, RewardIncluded: 1, RewardUnknown: 1, CoverageIncomplete: true}}
	s.Consensus = &Consensus{SampledAt: now, Voting: &Voting{SampledAt: now, Identity: *s.IdentityKey, VoteAccount: "test-vote"}}
	if p, _ := publicValue(t, &s, "fresh", now); p.Replay.CertificateOutcomes == nil {
		t.Fatal("valid certificate outcomes omitted")
	}
	s.Replay.CertificateOutcomes.RewardIncluded = 3
	if p, _ := publicValue(t, &s, "fresh", now); p.Replay.CertificateOutcomes != nil {
		t.Fatal("more reward results than carriers were published")
	}
}

func TestCertificateWindowAcceptsProducerResults(t *testing.T) {
	subject := solana.PublicKey{1}
	owner := replay.NewCertificateOutcomes(subject)
	carrier := func(slot uint64, id byte) replay.CertificateCarrier {
		return replay.CertificateCarrier{Slot: slot, BlockID: solana.Hash{id}, Bankhash: solana.Hash{id, 1},
			Observation: block.CertificateObservation{VoteAccount: subject, Reward: block.CertificateMembership{TargetSlot: slot - 8, AdmissionKnown: true, Eligible: true, Present: true, Included: true}}}
	}
	evidence := func(c replay.CertificateCarrier) replay.CertificateFoldEvidence {
		return replay.CertificateFoldEvidence{Slot: c.Slot, BlockID: c.BlockID, Bankhash: c.Bankhash, HasBlockID: true}
	}
	check := func(through uint64) {
		t.Helper()
		s := owner.Snapshot()
		v := CertificateOutcomes{HasWindow: s.HasWindow, FirstCarrier: s.FirstCarrier, LastCarrier: s.LastCarrier, Carriers: s.Carriers, Gaps: s.Gaps,
			SlotsWithoutCarrier: s.SlotsWithoutCarrier, Pending: s.Pending, Dropped: s.Dropped, CoverageIncomplete: s.CoverageIncomplete,
			RewardIncluded: s.RewardIncluded, RewardExcluded: s.RewardExcluded, RewardUnknown: s.RewardUnknown, RewardIneligible: s.RewardIneligible,
			FinalFast: s.FinalFast, FinalSlow: s.FinalSlow, FastMember: s.FastMember, FastNonmember: s.FastNonmember,
			FastMembershipUnknown: s.FastMembershipUnknown, FinalDuplicates: s.FinalDuplicates, FinalVariants: s.FinalVariants, FinalConflict: s.FinalConflict}
		if !validCertificateOutcomeWindow(&v, &through) {
			t.Fatalf("producer certificate result rejected: %+v", s)
		}
	}
	a, b := carrier(20, 1), carrier(30, 2)
	a.Observation.Final = block.CertificateMembership{TargetSlot: 2, TargetEpoch: 3, Present: true, AdmissionKnown: true, Eligible: true, Included: true}
	a.Observation.FinalBlockID, a.Observation.FinalFast = solana.Hash{8}, true
	b.Observation.Final, b.Observation.FinalBlockID = a.Observation.Final, a.Observation.FinalBlockID
	if !owner.Observe(a) || !owner.Observe(b) {
		t.Fatal("producer did not stage fixture")
	}
	owner.Settle([]replay.CertificateFoldEvidence{evidence(a)}, time.Now())
	check(a.Slot)
	owner.Settle([]replay.CertificateFoldEvidence{evidence(b)}, time.Now())
	check(b.Slot)
	c := carrier(31, 3)
	c.Observation.Final, c.Observation.FinalBlockID = a.Observation.Final, solana.Hash{9}
	if !owner.Observe(c) {
		t.Fatal("producer did not stage conflicting final")
	}
	owner.Settle([]replay.CertificateFoldEvidence{evidence(c)}, time.Now())
	check(c.Slot)
	conflict := evidence(a)
	conflict.Bankhash = solana.Hash{9}
	owner.Settle([]replay.CertificateFoldEvidence{conflict}, time.Now())
	check(c.Slot)
	owner.Settle(make([]replay.CertificateFoldEvidence, 1025), time.Now())
	check(c.Slot)
}

func TestCertificateOutcomeWindowValidation(t *testing.T) {
	through := uint64(10)
	valid := CertificateOutcomes{HasWindow: true, FirstCarrier: 9, LastCarrier: 10, Carriers: 2,
		RewardIncluded: 1, RewardUnknown: 1, CoverageIncomplete: true, FinalFast: 1, FastMember: 1}
	if !validCertificateOutcomeWindow(&valid, &through) {
		t.Fatal("valid partial certificate window rejected")
	}
	for name, mutate := range map[string]func(*CertificateOutcomes){
		"too many pending":           func(v *CertificateOutcomes) { v.Pending = 1025 },
		"too many carriers":          func(v *CertificateOutcomes) { v.Carriers = 513 },
		"reversed":                   func(v *CertificateOutcomes) { v.FirstCarrier = 11 },
		"future":                     func(v *CertificateOutcomes) { v.LastCarrier = 11 },
		"wrong gap count":            func(v *CertificateOutcomes) { v.SlotsWithoutCarrier = 1 },
		"missing reward":             func(v *CertificateOutcomes) { v.RewardUnknown = 0 },
		"excess reward":              func(v *CertificateOutcomes) { v.RewardIncluded = 2 },
		"gaps exceed unknown":        func(v *CertificateOutcomes) { v.Gaps = 2 },
		"finals exceed carriers":     func(v *CertificateOutcomes) { v.FinalSlow = 2 },
		"variants exceed duplicates": func(v *CertificateOutcomes) { v.FinalVariants = 1 },
		"fast membership mismatch":   func(v *CertificateOutcomes) { v.FastMember = 0 },
		"coverage mislabelled":       func(v *CertificateOutcomes) { v.CoverageIncomplete = false },
		"overflow":                   func(v *CertificateOutcomes) { v.RewardIncluded = math.MaxUint64 },
	} {
		t.Run(name, func(t *testing.T) {
			v := valid
			mutate(&v)
			if validCertificateOutcomeWindow(&v, &through) {
				t.Fatal("invalid certificate window accepted")
			}
		})
	}
	if validCertificateOutcomeWindow(&valid, nil) {
		t.Fatal("window without durable slot accepted")
	}
	empty := CertificateOutcomes{Pending: 1024, Dropped: math.MaxUint64, CoverageIncomplete: true}
	if !validCertificateOutcomeWindow(&empty, nil) {
		t.Fatal("absent window with pending evidence rejected")
	}
	// A sparse range may span the entire slot space without arithmetic overflow.
	max := uint64(math.MaxUint64)
	sparse := CertificateOutcomes{HasWindow: true, LastCarrier: max, Carriers: 2, SlotsWithoutCarrier: max - 1, RewardUnknown: 2, CoverageIncomplete: true}
	if !validCertificateOutcomeWindow(&sparse, &max) {
		t.Fatal("valid sparse window rejected")
	}
	// Anonymous counts from earlier isolated-node observations, not live state.
	for _, observed := range []CertificateOutcomes{
		{HasWindow: true, FirstCarrier: 981, LastCarrier: 1133, Carriers: 141, SlotsWithoutCarrier: 12, Pending: 1,
			RewardIncluded: 122, RewardExcluded: 19, FinalFast: 141, FastNonmember: 141, CoverageIncomplete: true},
		{HasWindow: true, FirstCarrier: 981, LastCarrier: 1281, Carriers: 289, SlotsWithoutCarrier: 12, Pending: 1,
			RewardIncluded: 270, RewardExcluded: 19, FinalFast: 273, FastMember: 3, FastNonmember: 270, CoverageIncomplete: true},
	} {
		if !validCertificateOutcomeWindow(&observed, &observed.LastCarrier) {
			t.Fatalf("real observed certificate window rejected: %+v", observed)
		}
	}
}
