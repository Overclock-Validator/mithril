package leaderschedule

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestVoteKeyedSchedulePreservesSelectedVoteAccount(t *testing.T) {
	const (
		epoch  = uint64(0)
		length = uint64(16)
		repeat = uint64(4)
	)
	node := solana.PublicKey{9}
	voteA := solana.PublicKey{1}
	voteB := solana.PublicKey{2}
	voteAccounts := map[solana.PublicKey]*epochstakes.VoteAccount{
		voteA: {NodePubkey: node},
		voteB: {NodePubkey: node},
	}
	stakes := map[solana.PublicKey]uint64{
		voteA: 10,
		voteB: 20,
	}
	epochSchedule := &sealevel.SysvarEpochSchedule{
		SlotsPerEpoch:            32,
		LeaderScheduleSlotOffset: 32,
	}

	schedule := New(voteAccounts, stakes, epochSchedule, epoch, length, repeat)
	expectedVotes := stakeWeightedSlotLeaders(
		[]pubkeyAndStakePair{
			{pubkey: voteA, stake: stakes[voteA]},
			{pubkey: voteB, stake: stakes[voteB]},
		},
		epoch,
		length,
		repeat,
	)

	firstSlot := epochSchedule.FirstSlotInEpoch(epoch)
	for i, expectedVote := range expectedVotes {
		slot := firstSlot + uint64(i)
		gotNode, gotVote, ok := schedule.LeaderForSlotWithVoteAccount(slot)
		require.True(t, ok, "slot %d missing vote-keyed leader metadata", slot)
		require.Equal(t, node, gotNode)
		require.Equal(t, expectedVote, gotVote)
		if i%int(repeat) != 0 {
			require.Equal(t, expectedVotes[i-1], gotVote, "repeat group changed within slot %d", slot)
		}
	}
}

func TestNodeKeyedScheduleHasNoVoteAccountProvenance(t *testing.T) {
	node := solana.PublicKey{9}
	schedule := NewLeaderScheduleFromKeyedSlots(
		map[solana.PublicKey][]uint64{node: {0}},
		100,
	)

	gotNode, ok := schedule.LeaderForSlot(100)
	require.True(t, ok)
	require.Equal(t, node, gotNode)

	_, _, ok = schedule.LeaderForSlotWithVoteAccount(100)
	require.False(t, ok)
	_, _, ok = schedule.LeaderForSlotWithVoteAccount(101)
	require.False(t, ok)
}

func TestNextSlotForLeader(t *testing.T) {
	ours := solana.PublicKey{1}
	other := solana.PublicKey{2}
	schedule := NewLeaderScheduleFromKeyedSlots(
		map[solana.PublicKey][]uint64{
			ours:  {0, 1, 10},
			other: {2, 3},
		},
		100,
	)

	slot, ok := schedule.NextSlotForLeader(ours, 100)
	require.True(t, ok)
	require.Equal(t, uint64(100), slot)

	slot, ok = schedule.NextSlotForLeader(ours, 101)
	require.True(t, ok)
	require.Equal(t, uint64(101), slot)

	slot, ok = schedule.NextSlotForLeader(ours, 102)
	require.True(t, ok)
	require.Equal(t, uint64(110), slot)

	_, ok = schedule.NextSlotForLeader(ours, 111)
	require.False(t, ok)
}
