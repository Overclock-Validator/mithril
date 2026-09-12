package consensus

import (
	"crypto/ed25519"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

const votorAdmissionTestEpochSlots = uint64(100_000)

func TestVotorPeerAdmissionTracksAgaveEpochWindow(t *testing.T) {
	engine, peers := newVotorAdmissionTestEngine(t, voterTestKey(70))

	// Four slots into epoch 1, root-8 still belongs to epoch 0. The upcoming
	// epoch is farther than Agave's 30,000-slot verification horizon.
	engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: votorAdmissionTestEpochSlots + 4})
	require.True(t, engine.admitVotorPeer(peers[0]), "trailing reward epoch")
	require.True(t, engine.admitVotorPeer(peers[1]), "root epoch")
	require.False(t, engine.admitVotorPeer(peers[2]), "far upcoming epoch")

	// Once the root is within 30,000 slots of the boundary, epoch 2 opens and
	// the now-distant epoch 0 closes.
	engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: 2*votorAdmissionTestEpochSlots - alpenglow.AgaveVoteVerificationWindow})
	require.False(t, engine.admitVotorPeer(peers[0]), "expired trailing epoch")
	require.True(t, engine.admitVotorPeer(peers[1]), "root epoch")
	require.True(t, engine.admitVotorPeer(peers[2]), "near upcoming epoch")
}

func TestVotorPeerAdmissionThresholdIsInclusive(t *testing.T) {
	engine, peers := newVotorAdmissionTestEngine(t, voterTestKey(70))
	boundary := 2*votorAdmissionTestEpochSlots - alpenglow.AgaveVoteVerificationWindow

	engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: boundary - 1})
	require.False(t, engine.admitVotorPeer(peers[2]))

	engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: boundary})
	require.True(t, engine.admitVotorPeer(peers[2]))
}

func TestVotorPeerRootDoesNotRegress(t *testing.T) {
	engine, peers := newVotorAdmissionTestEngine(t, voterTestKey(70))
	current := 2*votorAdmissionTestEpochSlots + 10
	engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: current})
	require.True(t, engine.admitVotorPeer(peers[2]))
	require.False(t, engine.admitVotorPeer(peers[1]))

	engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: votorAdmissionTestEpochSlots + 10})
	require.Equal(t, current, engine.votorPeerRoot.Load())
	require.True(t, engine.admitVotorPeer(peers[2]))
	require.False(t, engine.admitVotorPeer(peers[1]))
}

func TestVotorPeerRootSaturatesTrailingLookupAtZero(t *testing.T) {
	engine, peers := newVotorAdmissionTestEngine(t, voterTestKey(70))
	engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: 3})
	require.True(t, engine.admitVotorPeer(peers[0]))
}

func TestVotorPeerAdmissionRequiresLocalMembership(t *testing.T) {
	engine, peers := newVotorAdmissionTestEngine(t, voterTestKey(99))
	engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: votorAdmissionTestEpochSlots + 10})
	require.False(t, engine.admitVotorPeer(peers[1]))
}

func TestVotorRankComesFromAuthenticatedIdentityAndVoteEpoch(t *testing.T) {
	engine, peers := newVotorAdmissionTestEngine(t, voterTestKey(70))
	engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: 2*votorAdmissionTestEpochSlots - alpenglow.AgaveVoteVerificationWindow})

	rank, ok := engine.votorRankForPeer(2*votorAdmissionTestEpochSlots+1, peers[2])
	require.True(t, ok)
	require.Equal(t, uint16(1), rank)

	// Epoch-1's peer is transport-admitted in the merged boundary set, but it
	// cannot claim a rank for an epoch-2 vote.
	_, ok = engine.votorRankForPeer(2*votorAdmissionTestEpochSlots+1, peers[1])
	require.False(t, ok)
}

func TestVotorBroadcasterUsesMergedTransportEpochs(t *testing.T) {
	localIdentity := voterTestKey(70)
	engine, peers := newVotorAdmissionTestEngine(t, localIdentity)
	voter := &alpenglowVoter{
		engine: engine,
		node:   solana.PublicKey(localIdentity.Public().(ed25519.PublicKey)),
		sets:   make(map[uint64]alpenglow.ValidatorSet),
	}
	engine.validatorSetsMu.RLock()
	for epoch, set := range engine.validatorSets {
		voter.sets[epoch] = cloneValidatorSet(set)
	}
	engine.validatorSetsMu.RUnlock()

	engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: votorAdmissionTestEpochSlots + 4})
	identities := validatorIdentitySet(voter.votorTransportValidators())
	require.Contains(t, identities, peers[0])
	require.Contains(t, identities, peers[1])
	require.NotContains(t, identities, peers[2])

	engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: 2*votorAdmissionTestEpochSlots - alpenglow.AgaveVoteVerificationWindow})
	identities = validatorIdentitySet(voter.votorTransportValidators())
	require.NotContains(t, identities, peers[0])
	require.Contains(t, identities, peers[1])
	require.Contains(t, identities, peers[2])
}

func validatorIdentitySet(validators []alpenglow.ValidatorStake) map[solana.PublicKey]struct{} {
	identities := make(map[solana.PublicKey]struct{}, len(validators))
	for _, validator := range validators {
		identities[validator.NodePubkey] = struct{}{}
	}
	return identities
}

func newVotorAdmissionTestEngine(t *testing.T, configuredIdentity ed25519.PrivateKey) (*AlpenglowObserverEngine, map[uint64]solana.PublicKey) {
	t.Helper()
	localIdentity := voterTestKey(70)
	authorized := voterTestKey(71)
	voteAccount := solana.PublicKey(voterTestKey(72).Public().(ed25519.PublicKey))
	engine, err := NewEngine(Config{AlpenglowIdentity: configuredIdentity})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetAlpenglowEpochLookup(func(slot uint64) uint64 { return slot / votorAdmissionTestEpochSlots })

	peers := make(map[uint64]solana.PublicKey, 3)
	for epoch := uint64(0); epoch <= 2; epoch++ {
		set := voterTestValidatorSet(t, localIdentity, authorized, voteAccount)
		set.Epoch = epoch
		peer := solana.PublicKey(voterTestKey(byte(80 + epoch)).Public().(ed25519.PublicKey))
		set.Validators[1].NodePubkey = peer
		peers[epoch] = peer
		require.NoError(t, engine.SetAlpenglowValidatorSet(set))
	}
	return engine, peers
}
