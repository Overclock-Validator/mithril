package replay

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func rewardHealthCarrier(slot uint64, vote, identity solana.PublicKey, schedule *sealevel.SysvarEpochSchedule) CertificateCarrier {
	c := outcomeCarrier(slot, 1, vote)
	c.Observation.Reward = block.CertificateMembership{}
	if slot >= 8 {
		c.Observation.Reward = block.CertificateMembership{TargetSlot: slot - 8, TargetEpoch: schedule.GetEpoch(slot - 8),
			MaterialGeneration: 1, EpochIdentity: identity, AdmissionKnown: true, Eligible: true}
	}
	return c
}

func rewardHealthFixture(t *testing.T, first, last uint64, schedule *sealevel.SysvarEpochSchedule) (*CertificateOutcomes, solana.PublicKey) {
	t.Helper()
	vote, identity := solana.PublicKey{1}, solana.PublicKey{2}
	r := NewCertificateOutcomes(vote)
	var evidence []CertificateFoldEvidence
	for offset := uint64(0); offset <= last-first; offset++ {
		c := rewardHealthCarrier(first+offset, vote, identity, schedule)
		require.True(t, r.Observe(c))
		evidence = append(evidence, outcomeEvidence(c))
	}
	r.Settle(evidence, time.Unix(100, 0))
	return r, identity
}

func TestRewardHealthWindowAndReferences(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	r, identity := rewardHealthFixture(t, 8, 135, schedule)
	s := r.rewardHealthSnapshot(identity, 135, schedule)
	require.Equal(t, RewardHealthNoInclusion128, s.Health)
	require.Equal(t, uint64(128), s.Excluded)
	require.True(t, s.HasWindow && s.CoverageComplete && s.HasLatestResolved)
	require.Equal(t, uint64(0), s.WindowFirst)
	require.Equal(t, uint64(127), s.WindowLast)
	require.Equal(t, RewardTargetObservation{TargetSlot: 127, TargetEpoch: 3, Outcome: RewardTargetExcluded}, s.LatestResolved)
	require.False(t, s.HasLatestIncluded)
	require.Equal(t, time.Unix(100, 0).UTC(), s.SampledAt)
	require.Equal(t, "selected_and_folded", s.Provenance)
	require.Equal(t, r.subject, s.VoteAccount)
	require.Equal(t, identity, s.Identity)
	short := r.rewardHealthSnapshot(identity, 134, schedule)
	require.Equal(t, uint64(127), short.Excluded)
	require.True(t, short.CoverageComplete)
	require.Equal(t, RewardHealthUnknown, short.Health, "127 targets cannot prove a 128-target negative")
	r.window[0].carrier.Observation.Reward.Included, r.window[0].carrier.Observation.Reward.Present = true, true
	before := r.Snapshot()
	s = r.rewardHealthSnapshot(identity, 135, schedule)
	require.Equal(t, RewardHealthIncludedRecently, s.Health)
	require.True(t, s.HasLatestIncluded)
	require.Equal(t, RewardTargetObservation{TargetSlot: 0, TargetEpoch: 0, Outcome: RewardTargetIncluded}, s.LatestIncluded)
	require.Equal(t, before, r.Snapshot(), "derivation must not change the old aggregate")
	s = r.rewardHealthSnapshot(identity, 136, schedule)
	require.Equal(t, uint64(1), s.WindowFirst)
	require.Equal(t, uint64(128), s.WindowLast)
	require.Equal(t, uint64(1), s.Unobserved)
	require.Equal(t, uint64(127), s.Excluded)
	require.Equal(t, RewardHealthUnknown, s.Health, "older resolved evidence cannot move the anchor backwards")
	require.True(t, s.HasLatestIncluded, "older retained inclusion remains historical context")
	require.Zero(t, s.Included, "an inclusion just outside the window is not recent")
	require.Zero(t, testing.AllocsPerRun(100, func() { r.rewardHealthSnapshot(identity, 135, schedule) }))
}

func TestRewardHealthUnknownIsNotExclusion(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	for name, mutate := range map[string]func(*certificateWindowEntry){
		"target":       func(e *certificateWindowEntry) { e.carrier.Observation.Reward.TargetSlot-- },
		"epoch":        func(e *certificateWindowEntry) { e.carrier.Observation.Reward.TargetEpoch++ },
		"generation":   func(e *certificateWindowEntry) { e.carrier.Observation.Reward.MaterialGeneration = 0 },
		"admission":    func(e *certificateWindowEntry) { e.carrier.Observation.Reward.AdmissionKnown = false },
		"identity":     func(e *certificateWindowEntry) { e.carrier.Observation.Reward.EpochIdentity = solana.PublicKey{3} },
		"zeroIdentity": func(e *certificateWindowEntry) { e.carrier.Observation.Reward.EpochIdentity = solana.PublicKey{} },
		"absent":       func(e *certificateWindowEntry) { e.carrier.Observation.Reward.Included = true },
		"finalOnly": func(e *certificateWindowEntry) {
			e.carrier.Observation.Final = e.carrier.Observation.Reward
			e.carrier.Observation.Final.Present, e.carrier.Observation.Final.Included = true, true
			e.carrier.Observation.Reward.AdmissionKnown = false
		},
		"ineligibleInclusion": func(e *certificateWindowEntry) {
			e.carrier.Observation.Reward.Eligible = false
			e.carrier.Observation.Reward.Included, e.carrier.Observation.Reward.Present = true, true
		},
		"voteAccount": func(e *certificateWindowEntry) { e.carrier.Observation.VoteAccount = solana.PublicKey{3} },
		"bankhash":    func(e *certificateWindowEntry) { e.fold.Bankhash = solana.Hash{4} },
		"blockID":     func(e *certificateWindowEntry) { e.fold.BlockID = solana.Hash{4} },
		"missingID":   func(e *certificateWindowEntry) { e.fold.HasBlockID = false },
		"carrierSlot": func(e *certificateWindowEntry) { e.carrier.Slot-- },
		"gap":         func(e *certificateWindowEntry) { e.gap = true },
	} {
		t.Run(name, func(t *testing.T) {
			r, identity := rewardHealthFixture(t, 8, 135, schedule)
			r.window[0].carrier.Observation.Reward.Included, r.window[0].carrier.Observation.Reward.Present = true, true
			mutate(&r.window[127])
			s := r.rewardHealthSnapshot(identity, 135, schedule)
			require.Equal(t, uint64(1), s.Included)
			require.Equal(t, uint64(126), s.Excluded)
			require.Equal(t, uint64(1), s.Unknown)
			require.Equal(t, RewardHealthUnknown, s.Health, "unknown newest target overrides older inclusion")
			require.False(t, s.CoverageComplete)
			require.Equal(t, uint64(126), s.LatestResolved.TargetSlot)
		})
	}
	r, identity := rewardHealthFixture(t, 9, 135, schedule)
	s := r.rewardHealthSnapshot(identity, 135, schedule)
	require.Equal(t, uint64(1), s.Unobserved)
	require.Equal(t, RewardHealthUnknown, s.Health)
	r, identity = rewardHealthFixture(t, 8, 135, schedule)
	r.window[0].carrier.Observation.Reward.Eligible = false
	s = r.rewardHealthSnapshot(identity, 135, schedule)
	require.Equal(t, uint64(1), s.Ineligible)
	require.True(t, s.CoverageComplete)
	require.Equal(t, RewardHealthUnknown, s.Health, "ineligibility is not a missed opportunity")
	r.window[127].carrier.Observation.Reward.Eligible = false
	require.Equal(t, RewardHealthIneligible, r.rewardHealthSnapshot(identity, 135, schedule).Health)
	r, identity = rewardHealthFixture(t, 8, 135, schedule)
	r.coverageLost = true
	s = r.rewardHealthSnapshot(identity, 135, schedule)
	require.False(t, s.CoverageComplete)
	require.Equal(t, RewardHealthUnknown, s.Health)
	r.window[0].carrier.Observation.Reward.Included, r.window[0].carrier.Observation.Reward.Present = true, true
	require.Equal(t, RewardHealthIncludedRecently, r.rewardHealthSnapshot(identity, 135, schedule).Health, "coverage loss does not erase exact positive evidence")
	r, identity = rewardHealthFixture(t, 8, 136, schedule)
	r.window[0].gap = true // Outside targets 1..128.
	r.refresh(time.Unix(100, 0))
	require.True(t, r.Snapshot().CoverageIncomplete)
	require.Equal(t, RewardHealthNoInclusion128, r.rewardHealthSnapshot(identity, 136, schedule).Health)
}

func TestRewardHealthEpochRetentionAndBounds(t *testing.T) {
	warmup := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128, FirstNormalEpoch: 2, FirstNormalSlot: 96, Warmup: true}
	r, identity := rewardHealthFixture(t, 96, 104, warmup)
	require.Equal(t, uint64(1), r.rewardHealthSnapshot(identity, 103, warmup).LatestResolved.TargetEpoch)
	require.Equal(t, uint64(2), r.rewardHealthSnapshot(identity, 104, warmup).LatestResolved.TargetEpoch)
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	r, identity = rewardHealthFixture(t, 0, 8, schedule)
	s := r.rewardHealthSnapshot(identity, 7, schedule)
	require.False(t, s.HasWindow || s.HasLatestResolved || s.HasLatestIncluded)
	require.Equal(t, RewardHealthUnknown, s.Health)
	s = r.rewardHealthSnapshot(identity, 8, schedule)
	require.True(t, s.HasWindow && s.HasLatestResolved)
	require.Zero(t, s.LatestResolved.TargetSlot)
	r, identity = rewardHealthFixture(t, 8, 519, schedule)
	r.window[0].carrier.Observation.Reward.Included, r.window[0].carrier.Observation.Reward.Present = true, true
	s = r.rewardHealthSnapshot(identity, 519, schedule)
	require.True(t, s.HasLatestIncluded)
	require.Equal(t, RewardHealthNoInclusion128, s.Health)
	c := rewardHealthCarrier(520, r.subject, identity, schedule)
	require.True(t, r.Observe(c))
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, time.Unix(101, 0))
	require.False(t, r.rewardHealthSnapshot(identity, 520, schedule).HasLatestIncluded)
	require.True(t, s.HasLatestIncluded, "older result is detached from eviction")
	r, identity = rewardHealthFixture(t, math.MaxUint64-127, math.MaxUint64, schedule)
	s = r.rewardHealthSnapshot(identity, math.MaxUint64, schedule)
	require.Equal(t, uint64(math.MaxUint64-8), s.WindowLast)
	require.Equal(t, uint64(math.MaxUint64-135), s.WindowFirst)
	require.Equal(t, RewardHealthNoInclusion128, s.Health)
	for _, invalid := range []*sealevel.SysvarEpochSchedule{nil, {}, {SlotsPerEpoch: math.MaxUint64}, {SlotsPerEpoch: 32, FirstNormalEpoch: math.MaxUint64}} {
		require.Equal(t, RewardHealthSummary{}, r.rewardHealthSnapshot(identity, math.MaxUint64, invalid))
	}
	var absent *CertificateOutcomes
	require.Equal(t, RewardHealthSummary{}, absent.rewardHealthSnapshot(identity, 135, schedule))
	require.Equal(t, RewardHealthSummary{}, r.rewardHealthSnapshot(solana.PublicKey{}, 135, schedule))
	require.Equal(t, RewardHealthSummary{}, NewCertificateOutcomes(solana.PublicKey{}).rewardHealthSnapshot(identity, 135, schedule))
	require.Equal(t, RewardHealthSummary{}, NewCertificateOutcomes(r.subject).rewardHealthSnapshot(identity, 135, schedule))
}

func TestRewardHealthSettlementPublicationAndReset(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	r, identity := rewardHealthFixture(t, 8, 135, schedule)
	before := r.rewardHealthSnapshot(identity, 135, schedule)
	c := rewardHealthCarrier(136, r.subject, identity, schedule)
	c.Observation.Reward.Included, c.Observation.Reward.Present = true, true
	require.True(t, r.Observe(c))
	require.Equal(t, before, r.rewardHealthSnapshot(identity, 135, schedule), "pending inclusion is not settled")
	selected := NewCertificateOutcomes(r.subject)
	require.True(t, selected.Observe(c))
	selected.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, time.Unix(101, 0))
	included := selected.rewardHealthSnapshot(identity, 136, schedule)
	require.Equal(t, RewardHealthIncludedRecently, included.Health)
	require.True(t, included.HasLatestIncluded)
	require.Equal(t, uint64(128), included.LatestIncluded.TargetSlot)
	r.RewindFrom(136)
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, time.Unix(101, 0))
	s := r.rewardHealthSnapshot(identity, 136, schedule)
	require.Equal(t, RewardHealthUnknown, s.Health, "rewound observation cannot establish inclusion")
	require.Equal(t, uint64(1), s.Unknown)
	require.False(t, s.HasLatestIncluded)
	other := r.rewardHealthSnapshot(solana.PublicKey{9}, 136, schedule)
	require.False(t, other.HasLatestResolved || other.HasLatestIncluded)
	require.Equal(t, uint64(128), other.Unknown)
	var p Progress
	p.begin(8, nil)
	var readers sync.WaitGroup
	for range 2 {
		readers.Go(func() {
			for range 100 {
				v, _ := p.Snapshot()
				if !v.RewardHealth.SampledAt.IsZero() && (!v.HasDurableSlot || v.RewardHealth.Through != v.DurableSlot) {
					t.Error("health and durable frontier were not published together")
				}
			}
		})
	}
	for range 100 {
		p.settledOutcomes(136, r.Snapshot(), LeaderOutcomeSummary{}, SlotHistorySummary{}, ChainActivitySummary{}, EpochOutcomeSummary{}, s)
	}
	readers.Wait()
	published, _ := p.Snapshot()
	s.LatestResolved.TargetSlot = 0
	require.NotEqual(t, s.LatestResolved, published.RewardHealth.LatestResolved)
	p.rewound()
	rewound, _ := p.Snapshot()
	require.Equal(t, published.RewardHealth, rewound.RewardHealth, "speculative rewind preserves settled evidence and time")
	p.begin(137, nil)
	reset, _ := p.Snapshot()
	require.Equal(t, RewardHealthSummary{}, reset.RewardHealth)
	require.False(t, published.RewardHealth.SampledAt.IsZero())
}
