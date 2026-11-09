package consensus

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestVerifiedVoteHookReceivesOnlyAcceptedVotesWithParsedSignature(t *testing.T) {
	engine, err := NewEngine(Config{})
	require.NoError(t, err)
	set := testAlpenglowValidatorSet()
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	require.NoError(t, engine.SetAlpenglowValidatorSet(set))

	var received []alpenglow.VerifiedVote
	engine.SetVerifiedVoteHook(func(vote alpenglow.VerifiedVote) { received = append(received, vote) })
	legacyCalls := 0
	engine.SetVotorMessageHook(func(alpenglow.Message) { legacyCalls++ })
	signature := testAlpenglowCertificateSignature(t, alpenglow.Certificate{Type: alpenglow.CertificateSkip, Slot: 42})
	message := alpenglow.NewVoteMessage(alpenglow.NewSkipVote(42), signature, 0)
	engine.observeVotorMessage(message)
	require.Len(t, received, 1)
	require.Equal(t, 1, legacyCalls)
	_, ok := received[0].VerifiedSignature()
	require.True(t, ok, "accepted network vote lost its parsed signature")

	engine.observeVotorMessage(message)
	engine.observeVotorMessage(alpenglow.NewVoteMessage(alpenglow.NewSkipVote(43), signature, 0))
	require.Len(t, received, 1, "duplicate or wrong-payload signature reached the typed hook")
	require.Equal(t, 1, legacyCalls)

	// Even a cryptographically verified vote cannot bypass rooted pool admission.
	engine.pool = alpenglow.NewConsensusPool(alpenglow.ConsensusPoolConfig{
		RootBlock: alpenglow.BlockID{Slot: 50, Hash: solana.Hash{1}},
	})
	_, err = engine.acceptVerifiedVoteResult(received[0])
	require.NoError(t, err)
	require.Len(t, received, 1)
	require.Equal(t, 1, legacyCalls)
}
