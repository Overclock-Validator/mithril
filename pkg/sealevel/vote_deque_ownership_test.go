package sealevel

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
	"github.com/gammazero/deque"
	"github.com/stretchr/testify/require"
)

func TestProcessNewVoteStateOwnsRetainedDeque(t *testing.T) {
	// Model the TowerSync scratch deque being returned to its pool and reused.
	scratch := new(deque.Deque[LandedVote])
	scratch.PushBack(LandedVote{Lockout: VoteLockout{Slot: 100, ConfirmationCount: 2}})
	scratch.PushBack(LandedVote{Lockout: VoteLockout{Slot: 101, ConfirmationCount: 1}})
	state := new(VoteState)
	require.NoError(t, processNewVoteState(state, scratch, nil, nil, 0, 101, features.Features{}))
	cached := newVoteState4FromCurrent(state, solana.PublicKey{})
	want := []LandedVote{state.Votes.At(0), state.Votes.At(1)}

	scratch.Clear()
	scratch.PushBack(LandedVote{Lockout: VoteLockout{Slot: 200, ConfirmationCount: 2}})
	scratch.PushBack(LandedVote{Lockout: VoteLockout{Slot: 201, ConfirmationCount: 1}})
	for i, vote := range want {
		require.Equal(t, vote, state.Votes.At(i))
		require.Equal(t, vote, cached.Votes.At(i))
	}
}
