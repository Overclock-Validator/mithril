package replay

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func leaderSettlementSchedule(t *testing.T, identity solana.PublicKey, through uint64) *sealevel.SysvarEpochSchedule {
	t.Helper()
	global.SetLeaderSchedule(nil)
	t.Cleanup(func() { global.SetLeaderSchedule(nil) })
	slots := make([]uint64, through+1)
	for i := range slots {
		slots[i] = uint64(i)
	}
	global.SetLeaderSchedule(leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{identity: slots}, 0))
	return &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
}

func leaderSettlementCarrier(subject solana.PublicKey, c LeaderOutcomeCarrier) CertificateCarrier {
	return CertificateCarrier{Slot: c.Slot, BlockID: c.BlockID, Bankhash: c.Bankhash,
		ParentSlot: c.ParentSlot, ParentBlockID: c.ParentBlockID, HasParentBlockID: c.HasParentBlockID,
		ExecutionParentSlot: c.ExecutionParentSlot, ExecutionParentBankhash: c.ExecutionParentBankhash,
		Observation: block.CertificateObservation{VoteAccount: subject}}
}

func TestLeaderSettlementObserveFoldAndRewind(t *testing.T) {
	identity, subject := solana.PublicKey{1}, solana.PublicKey{9}
	schedule := leaderSettlementSchedule(t, identity, 20)
	r := NewCertificateOutcomes(subject)
	parent := leaderOutcomeChild(10, 1, CertificateFoldEvidence{})
	old := leaderOutcomeChild(20, 2, parent.CertificateFoldEvidence)
	require.True(t, r.Observe(leaderSettlementCarrier(subject, parent)))
	require.True(t, r.Observe(leaderSettlementCarrier(subject, old)))
	require.Equal(t, LeaderOutcomeSummary{}, r.leaderSnapshot(identity, 20, schedule), "staging or a failed fold cannot publish")
	r.Settle([]CertificateFoldEvidence{parent.CertificateFoldEvidence}, time.Unix(100, 0))
	first := r.leaderSnapshot(identity, 10, schedule)
	require.Equal(t, uint64(1), first.Produced)
	require.Zero(t, first.SelectedForkSkipped)
	require.Equal(t, uint64(10), first.ScheduledUnknown)
	r.RewindFrom(20)
	replacement := leaderOutcomeChild(20, 3, parent.CertificateFoldEvidence)
	require.True(t, r.Observe(leaderSettlementCarrier(subject, replacement)))
	r.Settle([]CertificateFoldEvidence{replacement.CertificateFoldEvidence}, time.Unix(101, 0))
	s := r.leaderSnapshot(identity, 20, schedule)
	require.Equal(t, uint64(2), s.Produced)
	require.Equal(t, uint64(9), s.SelectedForkSkipped)
	require.Equal(t, uint64(10), s.ScheduledUnknown)
	require.Equal(t, uint64(10), s.UnknownAncestrySlots)
	require.Zero(t, r.Snapshot().Gaps)
	require.Zero(t, r.Snapshot().Pending)
	require.Equal(t, "selected_and_folded", s.Provenance)
	require.Equal(t, time.Unix(101, 0).UTC(), s.SampledAt)
	require.Equal(t, uint64(1), first.Produced, "old detached summary is unchanged")
}

func TestLeaderSettlementMissingMetadataAndIDsAreBarriers(t *testing.T) {
	for _, missingID := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing_footer_metadata", true: "missing_fold_identity"}[missingID], func(t *testing.T) {
			identity, subject := solana.PublicKey{1}, solana.PublicKey{9}
			schedule := leaderSettlementSchedule(t, identity, 30)
			r := NewCertificateOutcomes(subject)
			a := leaderOutcomeChild(10, 1, CertificateFoldEvidence{})
			b := leaderOutcomeChild(20, 2, a.CertificateFoldEvidence)
			c := leaderOutcomeChild(30, 3, b.CertificateFoldEvidence)
			require.True(t, r.Observe(leaderSettlementCarrier(subject, a)))
			// No observation for B: its native receipt remains a produced point,
			// but its absent parent metadata cannot establish the preceding span.
			require.True(t, r.Observe(leaderSettlementCarrier(subject, c)))
			eb := b.CertificateFoldEvidence
			eb.HasBlockID = !missingID
			r.Settle([]CertificateFoldEvidence{a.CertificateFoldEvidence, eb, c.CertificateFoldEvidence}, time.Unix(100, 0))
			s := r.leaderSnapshot(identity, 30, schedule)
			require.Equal(t, uint64(1), r.Snapshot().Gaps)
			if missingID {
				require.Equal(t, uint64(2), s.Produced)
				require.Zero(t, s.SelectedForkSkipped)
				require.Equal(t, uint64(29), s.UnknownAncestrySlots)
			} else {
				require.Equal(t, uint64(3), s.Produced)
				require.Equal(t, uint64(9), s.SelectedForkSkipped)
				require.Equal(t, uint64(19), s.UnknownAncestrySlots)
			}
			require.True(t, s.CoverageIncomplete)
		})
	}
}

func TestLeaderSettlementEvictionPreservesExactPredecessor(t *testing.T) {
	identity, subject := solana.PublicKey{1}, solana.PublicKey{9}
	schedule := leaderSettlementSchedule(t, identity, 512)
	r := NewCertificateOutcomes(subject)
	var evidence []CertificateFoldEvidence
	var previous CertificateFoldEvidence
	for slot := uint64(0); slot <= 512; slot++ {
		c := leaderOutcomeChild(slot, 1, previous)
		require.True(t, r.Observe(leaderSettlementCarrier(subject, c)))
		evidence = append(evidence, c.CertificateFoldEvidence)
		previous = c.CertificateFoldEvidence
	}
	r.Settle(evidence, time.Unix(100, 0))
	require.Equal(t, certificateWindowLimit, r.count)
	require.True(t, r.hasPredecessor)
	require.Equal(t, evidence[0], r.predecessor)
	s := r.leaderSnapshot(identity, 512, schedule)
	require.Equal(t, uint64(1), s.WindowStart)
	require.Equal(t, uint64(512), s.Produced)
	require.Zero(t, s.SelectedForkSkipped)
	require.Zero(t, s.UnknownAncestrySlots)
	require.False(t, s.CoverageIncomplete)
	conflict := evidence[0]
	conflict.BlockID = solana.Hash{99}
	r.Settle([]CertificateFoldEvidence{conflict}, time.Unix(101, 0))
	require.False(t, r.predecessor.HasBlockID, "old conflicting evidence must poison the retained anchor")
	require.True(t, r.leaderSnapshot(identity, 512, schedule).CoverageIncomplete)
}

func TestLeaderSettlementMissingEpochAndFixedIdentity(t *testing.T) {
	global.SetLeaderSchedule(nil)
	t.Cleanup(func() { global.SetLeaderSchedule(nil) })
	a, b, subject := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{9}
	indices := make([]uint64, 32)
	for i := range indices {
		indices[i] = uint64(i)
	}
	global.SetLeaderScheduleForEpoch(0, leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{a: indices}, 0))
	global.SetLeaderScheduleForEpoch(1, leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{b: indices}, 32))
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	r := NewCertificateOutcomes(subject)
	parent := leaderOutcomeChild(0, 1, CertificateFoldEvidence{})
	child := leaderOutcomeChild(63, 2, parent.CertificateFoldEvidence)
	require.True(t, r.Observe(leaderSettlementCarrier(subject, parent)))
	require.True(t, r.Observe(leaderSettlementCarrier(subject, child)))
	r.Settle([]CertificateFoldEvidence{parent.CertificateFoldEvidence, child.CertificateFoldEvidence}, time.Unix(100, 0))
	oldIdentity := r.leaderSnapshot(a, 63, schedule)
	newIdentity := r.leaderSnapshot(b, 63, schedule)
	require.Equal(t, a, oldIdentity.Identity)
	require.Equal(t, b, newIdentity.Identity)
	require.Equal(t, uint64(1), oldIdentity.Produced)
	require.Equal(t, uint64(31), oldIdentity.SelectedForkSkipped)
	require.Equal(t, uint64(1), newIdentity.Produced)
	require.Equal(t, uint64(31), newIdentity.SelectedForkSkipped)
	global.SetLeaderScheduleForEpoch(1, nil)
	missing := r.leaderSnapshot(a, 63, schedule)
	require.Equal(t, uint64(32), missing.UnknownScheduleSlots)
	require.Zero(t, missing.UnknownAncestrySlots)
	require.Equal(t, uint64(31), missing.CoveredEnd)
	require.True(t, missing.CoverageIncomplete)
	require.False(t, oldIdentity.CoverageIncomplete, "a later schedule eviction cannot mutate an old summary")
	require.Equal(t, LeaderOutcomeSummary{}, r.leaderSnapshot(solana.PublicKey{}, 63, schedule))
	require.Equal(t, LeaderOutcomeSummary{}, r.leaderSnapshot(a, 63, nil))
}

func TestLeaderSettlementFoldLossUsesActualEndAndProgressResets(t *testing.T) {
	identity, subject := solana.PublicKey{1}, solana.PublicKey{9}
	schedule := leaderSettlementSchedule(t, identity, 1000)
	r := NewCertificateOutcomes(subject)
	c := leaderOutcomeChild(10, 1, CertificateFoldEvidence{})
	require.True(t, r.Observe(leaderSettlementCarrier(subject, c)))
	r.Settle([]CertificateFoldEvidence{c.CertificateFoldEvidence}, time.Unix(100, 0))
	// The actual committed fold extends to 1000, but one layer hash is missing.
	// Never reuse the last captured carrier (10) as the window's durable end.
	r.settleFold(map[uint64][32]byte{1000: testHash(1)}, map[uint64]solana.Hash{1000: {2}}, 2, time.Unix(101, 0))
	s := r.leaderSnapshot(identity, 1000, schedule)
	require.Equal(t, uint64(1000), s.WindowEnd)
	require.Equal(t, uint64(489), s.WindowStart)
	require.Equal(t, uint64(512), s.UnknownAncestrySlots)
	require.Equal(t, uint64(512), s.ScheduledUnknown)
	require.Zero(t, s.Produced)
	require.Zero(t, s.SelectedForkSkipped)
	require.True(t, s.CoverageIncomplete)
	var progress Progress
	progress.begin(10, nil)
	progress.settledOutcomes(1000, r.Snapshot(), s, r.slotSnapshot(1000), r.activitySnapshot(1000), r.epochSnapshot(identity, 1000, schedule), RewardHealthSummary{})
	old, ok := progress.Snapshot()
	require.True(t, ok)
	progress.rewound()
	current, _ := progress.Snapshot()
	require.Equal(t, old.LeaderOutcomes, current.LeaderOutcomes, "speculative rewind preserves the settled prefix")
	progress.begin(10, nil)
	reset, _ := progress.Snapshot()
	require.Equal(t, uint64(2), reset.Attempt)
	require.Equal(t, LeaderOutcomeSummary{}, reset.LeaderOutcomes)
	require.Equal(t, CertificateOutcomeSummary{}, reset.CertificateOutcomes)
	require.Equal(t, uint64(1000), old.LeaderOutcomes.WindowEnd)
	var absent *CertificateOutcomes
	require.Equal(t, LeaderOutcomeSummary{}, absent.leaderSnapshot(identity, 1000, schedule))
}

func TestLeaderSettlementUsesActualNativeFlush(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failed_fold"}[fail], func(t *testing.T) {
			identity, subject := solana.PublicKey{1}, solana.PublicKey{9}
			schedule := leaderSettlementSchedule(t, identity, 25)
			fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
			if fail {
				fc.failOn = 25
			}
			tail := asyncTestTail(fc, 15, 25)
			r := NewCertificateOutcomes(subject)
			parent := leaderOutcomeChild(15, 1, CertificateFoldEvidence{})
			parent.Bankhash = solana.Hash(tail.bankhashes[15])
			child := leaderOutcomeChild(25, 2, parent.CertificateFoldEvidence)
			child.Bankhash = solana.Hash(tail.bankhashes[25])
			require.True(t, r.Observe(leaderSettlementCarrier(subject, parent)))
			require.True(t, r.Observe(leaderSettlementCarrier(subject, child)))
			ids := map[uint64]solana.Hash{15: parent.BlockID, 25: child.BlockID}
			before := captureCertificateFlush(tail)
			through, ctx, err := tail.flush(25)
			if fail {
				require.Error(t, err)
				require.Equal(t, LeaderOutcomeSummary{}, r.leaderSnapshot(identity, 25, schedule))
				return
			}
			require.NoError(t, err)
			require.NotNil(t, ctx)
			r.settleFlush(before, tail, ids, through, time.Unix(100, 0))
			s := r.leaderSnapshot(identity, through, schedule)
			require.Equal(t, uint64(2), s.Produced)
			require.Equal(t, uint64(9), s.SelectedForkSkipped)
			require.Equal(t, uint64(15), s.UnknownAncestrySlots)
		})
	}
}

func BenchmarkLeaderSnapshotOwner512Records(b *testing.B) {
	global.SetLeaderSchedule(nil)
	b.Cleanup(func() { global.SetLeaderSchedule(nil) })
	identity, subject := solana.PublicKey{1}, solana.PublicKey{9}
	const slotsPerEpoch, through = uint64(54_000), uint64(54_255)
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: slotsPerEpoch}
	indices := make([]uint64, slotsPerEpoch)
	for i := range indices {
		indices[i] = uint64(i)
	}
	first := leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{identity: indices}, 0)
	second := leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{identity: indices}, slotsPerEpoch)
	global.SetLeaderScheduleForEpoch(0, first)
	r := NewCertificateOutcomes(subject)
	var evidence []CertificateFoldEvidence
	var previous CertificateFoldEvidence
	for slot := through - 512; slot <= through; slot++ {
		c := leaderOutcomeChild(slot, 1, previous)
		r.Observe(leaderSettlementCarrier(subject, c))
		evidence = append(evidence, c.CertificateFoldEvidence)
		previous = c.CertificateFoldEvidence
	}
	r.Settle(evidence, time.Unix(100, 0))
	for _, missing := range []bool{false, true} {
		name := "full_epoch_views"
		global.SetLeaderScheduleForEpoch(1, second)
		if missing {
			name = "missing_second_epoch"
			global.SetLeaderScheduleForEpoch(1, nil)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			var snapshot LeaderOutcomeSummary
			for range b.N {
				snapshot = r.leaderSnapshot(identity, through, schedule)
			}
			b.StopTimer()
			if missing {
				require.Equal(b, uint64(256), snapshot.UnknownScheduleSlots)
			} else {
				require.Equal(b, uint64(512), snapshot.Produced)
				require.False(b, snapshot.CoverageIncomplete)
			}
		})
	}
}
