package replay

import (
	"math"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func leaderOutcomeEvidence(slot uint64, id byte) CertificateFoldEvidence {
	return CertificateFoldEvidence{Slot: slot, BlockID: solana.Hash{id}, Bankhash: solana.Hash{id, 1}, HasBlockID: true}
}

func leaderOutcomeChild(slot uint64, id byte, parent CertificateFoldEvidence) LeaderOutcomeCarrier {
	return LeaderOutcomeCarrier{CertificateFoldEvidence: leaderOutcomeEvidence(slot, id),
		ParentSlot: parent.Slot, ExecutionParentSlot: parent.Slot, ParentBlockID: parent.BlockID,
		HasParentBlockID: parent.HasBlockID, ExecutionParentBankhash: parent.Bankhash}
}

func TestLeaderOutcomesExactFoldedChainAndAssignments(t *testing.T) {
	identity, other := solana.PublicKey{1}, solana.PublicKey{2}
	anchor := leaderOutcomeEvidence(99, 1)
	a := leaderOutcomeChild(100, 2, anchor)
	b := leaderOutcomeChild(110, 3, a.CertificateFoldEvidence)
	c := leaderOutcomeChild(611, 4, b.CertificateFoldEvidence)
	assigned := map[uint64]bool{100: true, 105: true, 110: true, 111: true, 611: true}
	calls := 0
	s := ComputeLeaderOutcomes(identity, 611, []LeaderOutcomeCarrier{c, b, a, a}, &anchor, func(slot uint64) (solana.PublicKey, bool) {
		calls++
		if assigned[slot] {
			return identity, true
		}
		return other, true
	})
	require.Equal(t, 512, calls)
	require.Equal(t, uint64(100), s.WindowStart)
	require.Equal(t, uint64(611), s.WindowEnd)
	require.Equal(t, uint64(3), s.Produced)
	require.Equal(t, uint64(2), s.SelectedForkSkipped)
	require.Zero(t, s.ScheduledUnknown)
	require.Zero(t, s.UnknownAncestrySlots)
	require.Zero(t, s.UnknownScheduleSlots)
	require.True(t, s.HasCoveredRange)
	require.Equal(t, s.WindowStart, s.CoveredStart)
	require.Equal(t, s.WindowEnd, s.CoveredEnd)
	require.False(t, s.CoverageIncomplete)
}

func TestLeaderOutcomesNeverInferSkipFromUnprovenParent(t *testing.T) {
	identity, other := solana.PublicKey{1}, solana.PublicKey{2}
	for _, name := range []string{"missing_anchor", "parent_id", "parent_flag", "execution_slot", "execution_bankhash", "parent_not_older", "child_id", "child_bankhash", "conflicting_duplicate", "intervening_produced", "omitted_failed_or_rewound", "future_child"} {
		t.Run(name, func(t *testing.T) {
			anchor := leaderOutcomeEvidence(10, 1)
			child := leaderOutcomeChild(20, 2, anchor)
			predecessor := &anchor
			var extra []LeaderOutcomeCarrier
			switch name {
			case "missing_anchor":
				predecessor = nil
			case "parent_id":
				child.ParentBlockID = solana.Hash{9}
			case "parent_flag":
				child.HasParentBlockID = false
			case "execution_slot":
				child.ExecutionParentSlot = 11
			case "execution_bankhash":
				child.ExecutionParentBankhash = solana.Hash{9}
			case "parent_not_older":
				child.ParentSlot, child.ExecutionParentSlot = 20, 20
			case "child_id":
				child.HasBlockID = false
			case "child_bankhash":
				child.Bankhash = solana.Hash{}
			case "conflicting_duplicate":
				conflict := child
				conflict.BlockID = solana.Hash{9}
				extra = append(extra, conflict)
			case "intervening_produced":
				extra = append(extra, LeaderOutcomeCarrier{CertificateFoldEvidence: leaderOutcomeEvidence(15, 3)})
			case "future_child":
				child.Slot = 21
			}
			carriers := append(extra, child)
			if name == "omitted_failed_or_rewound" {
				carriers = nil
			}
			calls := 0
			s := ComputeLeaderOutcomes(identity, 20, carriers, predecessor, func(slot uint64) (solana.PublicKey, bool) {
				calls++
				if slot == 12 {
					return identity, true
				}
				return other, true
			})
			require.Equal(t, 21, calls)
			require.Zero(t, s.SelectedForkSkipped)
			require.Zero(t, s.Produced)
			require.Equal(t, uint64(1), s.ScheduledUnknown)
			require.Positive(t, s.UnknownAncestrySlots)
			require.True(t, s.CoverageIncomplete)
		})
	}
}

func TestLeaderOutcomesBoundedInvalidInputAndUnavailableSchedule(t *testing.T) {
	identity := solana.PublicKey{1}
	calls := 0
	s := ComputeLeaderOutcomes(identity, math.MaxUint64, make([]LeaderOutcomeCarrier, 513), nil, func(uint64) (solana.PublicKey, bool) {
		calls++
		return identity, true
	})
	require.Equal(t, 512, calls)
	require.Equal(t, uint64(512), s.ScheduledUnknown)
	require.Equal(t, uint64(512), s.UnknownAncestrySlots)
	require.Zero(t, s.Produced)
	require.Zero(t, s.SelectedForkSkipped)
	require.False(t, s.HasCoveredRange)
	require.True(t, s.CoverageIncomplete)
	s = ComputeLeaderOutcomes(identity, 511, nil, nil, nil)
	require.Equal(t, uint64(512), s.UnknownScheduleSlots)
	require.Equal(t, uint64(512), s.UnknownAncestrySlots)
	require.Zero(t, s.ScheduledUnknown, "unknown schedules cannot establish identity assignments")
	s = ComputeLeaderOutcomes(solana.PublicKey{}, 511, nil, nil, func(uint64) (solana.PublicKey, bool) { return identity, true })
	require.True(t, s.CoverageIncomplete)
	require.Zero(t, s.Produced)
	require.Zero(t, s.SelectedForkSkipped)
	// A zero leader is unavailable, not proof of no assignment.
	s = ComputeLeaderOutcomes(identity, 0, nil, nil, func(uint64) (solana.PublicKey, bool) { return solana.PublicKey{}, true })
	require.Equal(t, uint64(1), s.UnknownScheduleSlots)
}
