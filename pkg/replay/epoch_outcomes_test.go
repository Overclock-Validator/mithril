package replay

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func epochOutcomeFixture(t *testing.T, schedule *sealevel.SysvarEpochSchedule, start, through uint64) (*CertificateOutcomes, solana.PublicKey) {
	t.Helper()
	identity, vote := solana.PublicKey{1}, solana.PublicKey{2}
	global.SetLeaderSchedule(nil)
	t.Cleanup(func() { global.SetLeaderSchedule(nil) })
	r := NewCertificateOutcomes(vote)
	var parent CertificateFoldEvidence
	byEpoch := make(map[uint64][]uint64)
	for slot := start; slot <= through; slot++ {
		child := leaderOutcomeChild(slot, 1, parent)
		c := leaderSettlementCarrier(vote, child)
		if slot >= rewardcerts.SlotsForReward {
			target := slot - rewardcerts.SlotsForReward
			c.Observation.Reward = block.CertificateMembership{TargetSlot: target, TargetEpoch: schedule.GetEpoch(target), AdmissionKnown: true, Eligible: true, Present: true, Included: true}
		}
		require.True(t, r.Observe(c))
		r.Settle([]CertificateFoldEvidence{child.CertificateFoldEvidence}, time.Unix(100, 0))
		parent = child.CertificateFoldEvidence
		epoch, index := schedule.GetEpochAndSlotIndex(slot)
		byEpoch[epoch] = append(byEpoch[epoch], index)
	}
	for epoch, slots := range byEpoch {
		global.SetLeaderScheduleForEpoch(epoch, leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{identity: slots}, schedule.Slot0(epoch)))
	}
	return r, identity
}

func TestEpochOutcomesBoundaryAndRewardDelay(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	r, identity := epochOutcomeFixture(t, schedule, 0, 39)
	s := r.epochSnapshot(identity, 39, schedule)
	require.Equal(t, 2, s.Count)
	current, previous := s.Epochs[0], s.Epochs[1]
	require.Equal(t, uint64(1), current.Epoch)
	require.Equal(t, uint64(8), current.Produced)
	require.False(t, current.LeaderEpochComplete)
	require.False(t, current.HasRewards, "current epoch targets have not matured")
	require.Equal(t, uint64(0), previous.Epoch)
	require.Equal(t, uint64(32), previous.Produced)
	require.True(t, previous.LeaderEpochComplete)
	require.Equal(t, uint64(32), previous.Included)
	require.True(t, previous.RewardEpochComplete, "boundary carriers reward the previous epoch")
	require.Equal(t, uint64(0), previous.RewardFirst)
	require.Equal(t, uint64(31), previous.RewardLast)
	// An uncommitted candidate and its rewind cannot change settled results.
	c := outcomeCarrier(40, 3, r.subject)
	require.True(t, r.Observe(c))
	require.Equal(t, s, r.epochSnapshot(identity, 39, schedule))
	r.RewindFrom(40)
	require.Equal(t, s, r.epochSnapshot(identity, 39, schedule))
	global.SetLeaderScheduleForEpoch(0, nil)
	missing := r.epochSnapshot(identity, 39, schedule).Epochs[1]
	require.Equal(t, uint64(32), missing.UnknownSchedule)
	require.False(t, missing.LeaderEpochComplete)
	require.Equal(t, uint64(32), previous.Produced, "prior value is detached")
	p := &Progress{}
	p.begin(0, nil)
	p.settledOutcomes(39, r.Snapshot(), LeaderOutcomeSummary{}, SlotHistorySummary{}, ChainActivitySummary{}, s, RewardHealthSummary{})
	p.begin(40, nil)
	reset, _ := p.Snapshot()
	require.Equal(t, EpochOutcomeSummary{}, reset.EpochOutcomes)
}

func TestEpochOutcomesUnknownAndIneligibleAreNotMisses(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	r, identity := epochOutcomeFixture(t, schedule, 10, 39)
	// Targets 0 and 1 have no retained carriers. Each other category is distinct.
	r.window[0].carrier.Observation.Reward.Eligible = false
	r.window[0].carrier.Observation.Reward.Included = false
	r.window[1].carrier.Observation.Reward.Included = false
	r.window[2].carrier.Observation.Reward.AdmissionKnown = false
	r.window[3].carrier.Observation.Reward.TargetEpoch = 99
	r.window[4].gap = true
	r.window[5].carrier.Observation.Reward.Eligible = false // contradictory inclusion
	e := r.epochSnapshot(identity, 39, schedule).Epochs[1]
	require.Equal(t, uint64(2), e.Unobserved)
	require.Equal(t, uint64(1), e.Ineligible)
	require.Equal(t, uint64(1), e.Excluded)
	require.Equal(t, uint64(4), e.Unknown)
	require.Equal(t, uint64(24), e.Included)
	require.False(t, e.RewardEpochComplete)
	require.False(t, e.LeaderEpochComplete)
	// Even complete per-target evidence cannot erase a lost fold interval.
	r.coverageLost = true
	require.False(t, r.epochSnapshot(identity, 39, schedule).Epochs[1].RewardEpochComplete)
}
