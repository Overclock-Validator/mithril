package consensus

import (
	"crypto/ed25519"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func waitSlotTestVoter(t *testing.T, cutoff uint64) *alpenglowVoter {
	t.Helper()
	identity, authorized := voterTestKey(11), voterTestKey(12)
	voteAccount := solana.PublicKey(voterTestKey(13).Public().(ed25519.PublicKey))
	set := voterTestValidatorSet(t, identity, authorized, voteAccount)
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, err := NewEngine(Config{AlpenglowShredVersion: 0x1234, AlpenglowIdentity: identity})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	require.NoError(t, engine.SetAlpenglowValidatorSet(set))
	engine.SetAlpenglowRoot(root)
	voter, err := newAlpenglowVoterUnstarted(engine, VotingConfig{
		Identity: identity, AuthorizedVoter: authorized, VoteAccount: voteAccount,
		HistoryDir: t.TempDir(), EpochForSlot: func(uint64) uint64 { return set.Epoch },
		Peers:          func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil },
		WaitToVoteSlot: cutoff, ReadyToVote: func(uint64) bool { return true },
	}, root, []alpenglow.ValidatorSet{set})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, voter.close()) })
	return voter
}

func TestWaitToVoteSlotGatesEveryNewVoteType(t *testing.T) {
	voter := waitSlotTestVoter(t, 44)
	constructors := []func(uint64) alpenglow.Vote{
		func(slot uint64) alpenglow.Vote { return alpenglow.NewNotarizationVote(slot, solana.Hash{1}) },
		alpenglow.NewFinalizationVote,
		alpenglow.NewSkipVote,
		func(slot uint64) alpenglow.Vote { return alpenglow.NewNotarizationFallbackVote(slot, solana.Hash{1}) },
		alpenglow.NewSkipFallbackVote,
	}
	for _, started := range []bool{false, true} {
		voter.votingStarted = started
		for _, voteAt := range constructors {
			vote := voteAt(43)
			_, _, err := voter.sign(vote, true)
			require.ErrorIs(t, err, errVoterNotReady, "%s started=%t", vote.Type, started)
			for _, slot := range []uint64{44, 45} {
				message, _, err := voter.sign(voteAt(slot), true)
				require.NoError(t, err, "%s slot=%d started=%t", vote.Type, slot, started)
				require.Equal(t, voteAt(slot), message.Vote)
			}
		}
	}
}

func TestWaitToVoteSlotSplitsSkipWindowAndPersistsOnlyAllowedVotes(t *testing.T) {
	voter := waitSlotTestVoter(t, 42)
	require.NoError(t, voter.trySkipWindow(40))
	for _, slot := range []uint64{40, 41} {
		require.False(t, voter.history.VotedAt(slot))
	}
	for _, slot := range []uint64{42, 43} {
		require.True(t, voter.history.HasSkipped(slot))
	}
	restored, err := alpenglow.LoadVoteHistory(voter.historyDir, voter.node)
	require.NoError(t, err)
	require.Equal(t, voter.history.VotesCast, restored.VotesCast)
	require.EqualValues(t, 2, voter.engine.ensurePool().Snapshot().VerifiedVotes)
	// Joining live voting must not make older slots eligible afterward.
	require.True(t, voter.votingStarted)
	voted, err := voter.cast(alpenglow.NewSkipVote(41), false)
	require.NoError(t, err)
	require.False(t, voted)
	require.False(t, voter.history.VotedAt(41))
}

func TestWaitToVoteSlotPreservesAuthenticatedHistoryRestoration(t *testing.T) {
	voter := waitSlotTestVoter(t, 44)
	require.NoError(t, voter.history.AddVote(alpenglow.NewSkipVote(40)))
	require.NoError(t, voter.saveHistory())
	restored, err := alpenglow.LoadVoteHistory(voter.historyDir, voter.node)
	require.NoError(t, err)
	voter.history = restored
	require.NoError(t, voter.restoreVotesForEpoch(voter.epochForSlot(40)))
	require.EqualValues(t, 1, voter.engine.ensurePool().Snapshot().VerifiedVotes)
	require.False(t, voter.votingStarted, "restoring a recorded vote must not bypass startup readiness")
	require.ErrorIs(t, voter.votingGateError(41), errVoterNotReady)
}
