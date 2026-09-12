package consensus

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestDeferredIntrawindowSafeToNotarDrainsAfterExactReplay(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	parent := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x40}}
	firstBlock := alpenglow.BlockID{Slot: 41, Hash: solana.Hash{0x41}}
	fallbackBlock := alpenglow.BlockID{Slot: 41, Hash: solana.Hash{0x42}}
	identity := voterTestKey(101)
	authorized := voterTestKey(102)
	voteAccount := solana.PublicKey(voterTestKey(103).Public().(ed25519.PublicKey))
	set := voterTestValidatorSet(t, identity, authorized, voteAccount)

	engine, err := NewEngine(Config{AlpenglowShredVersion: 0x1234, AlpenglowIdentity: identity})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	require.NoError(t, engine.SetAlpenglowValidatorSet(set))
	engine.SetAlpenglowRoot(root)

	historyDir := t.TempDir()
	node := solana.PublicKey(identity.Public().(ed25519.PublicKey))
	history := alpenglow.NewVoteHistory(node, root.Slot)
	require.NoError(t, history.AddVote(alpenglow.NewNotarizationVote(firstBlock.Slot, firstBlock.Hash)))
	require.NoError(t, alpenglow.SaveVoteHistory(historyDir, history, identity))
	require.NoError(t, engine.EnableVoting(VotingConfig{
		Identity:        identity,
		AuthorizedVoter: authorized,
		VoteAccount:     voteAccount,
		HistoryDir:      historyDir,
		EpochForSlot:    func(uint64) uint64 { return set.Epoch },
		Peers:           func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil },
		WaitToVoteSlot:  root.Slot + 1,
		ReadyToVote:     func(uint64) bool { return true },
	}))

	for rank, key := range []ed25519.PrivateKey{voterTestKey(21), voterTestKey(22)} {
		peer := signedVerifiedVoterPeerVote(t, engine, set, key, uint16(rank+1),
			alpenglow.NewNotarizationVote(fallbackBlock.Slot, fallbackBlock.Hash))
		admission, err := engine.acceptVerifiedVoteResult(peer)
		require.NoError(t, err)
		require.Equal(t, alpenglow.VoteAdmissionAccepted, admission)
	}

	pendingCandidate := func() bool {
		pending := engine.ensurePool().TakePendingSafeToNotar()
		for _, candidate := range pending {
			engine.ensurePool().RequeuePendingSafeToNotar(candidate)
		}
		return len(pending) == 1 && pending[0] == fallbackBlock
	}
	// The stake threshold is present, but neither exact execution nor the
	// replay parent's certificate is. The service must retain, not emit, it.
	require.Eventually(t, pendingCandidate, 2*time.Second, 10*time.Millisecond)
	restored, err := alpenglow.LoadVoteHistory(historyDir, node)
	require.NoError(t, err)
	require.False(t, restored.HasNotarFallback(fallbackBlock))

	replayBlock := &block.Block{
		Slot:                      fallbackBlock.Slot,
		ParentSlot:                parent.Slot,
		AlpenglowBlockID:          [32]byte(fallbackBlock.Hash),
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    [32]byte(parent.Hash),
		HasAlpenglowParentBlockID: true,
	}
	require.NoError(t, engine.ObserveBlock(context.Background(), BlockObservation{Block: replayBlock, Source: "test"}))
	require.Eventually(t, pendingCandidate, 2*time.Second, 10*time.Millisecond,
		"ObserveBlock alone must not become execution proof")
	require.NoError(t, engine.OnReplayResult(context.Background(), SlotReplayResult{Slot: fallbackBlock.Slot, Source: "test"}))
	require.Eventually(t, pendingCandidate, 2*time.Second, 10*time.Millisecond,
		"successful replay alone must not bypass parent certification")
	restored, err = alpenglow.LoadVoteHistory(historyDir, node)
	require.NoError(t, err)
	require.False(t, restored.HasNotarFallback(fallbackBlock))

	wrongParent := alpenglow.BlockID{Slot: parent.Slot, Hash: solana.Hash{0x44}}
	update, accepted := engine.acceptVerifiedCertificate(alpenglow.Certificate{
		Type:              alpenglow.CertificateNotarizeFallback,
		Slot:              wrongParent.Slot,
		BlockHash:         wrongParent.Hash,
		StakeVerified:     true,
		SignatureVerified: true,
	})
	require.True(t, accepted)
	require.Len(t, update.Certificates, 1)
	require.Eventually(t, pendingCandidate, 2*time.Second, 10*time.Millisecond,
		"a same-slot sibling certificate must not satisfy the exact replay parent")
	restored, err = alpenglow.LoadVoteHistory(historyDir, node)
	require.NoError(t, err)
	require.False(t, restored.HasNotarFallback(fallbackBlock))

	update, accepted = engine.acceptVerifiedCertificate(alpenglow.Certificate{
		Type:              alpenglow.CertificateNotarizeFallback,
		Slot:              parent.Slot,
		BlockHash:         parent.Hash,
		StakeVerified:     true,
		SignatureVerified: true,
	})
	require.True(t, accepted)
	require.Len(t, update.Certificates, 1)
	require.Eventually(t, func() bool {
		restored, loadErr := alpenglow.LoadVoteHistory(historyDir, node)
		return loadErr == nil && restored.HasNotarFallback(fallbackBlock)
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, engine.AlpenglowSafetyError())
}

func TestDeferredIntrawindowSafeToNotarDrainsAfterLateEnable(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x51}}
	parent := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x52}}
	firstBlock := alpenglow.BlockID{Slot: 41, Hash: solana.Hash{0x53}}
	fallbackBlock := alpenglow.BlockID{Slot: 41, Hash: solana.Hash{0x54}}
	identity := voterTestKey(104)
	authorized := voterTestKey(105)
	voteAccount := solana.PublicKey(voterTestKey(106).Public().(ed25519.PublicKey))
	set := voterTestValidatorSet(t, identity, authorized, voteAccount)
	engine, err := NewEngine(Config{AlpenglowShredVersion: 0x1234, AlpenglowIdentity: identity})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	require.NoError(t, engine.SetAlpenglowValidatorSet(set))
	engine.SetAlpenglowRoot(root)

	historyDir := t.TempDir()
	node := solana.PublicKey(identity.Public().(ed25519.PublicKey))
	history := alpenglow.NewVoteHistory(node, root.Slot)
	require.NoError(t, history.AddVote(alpenglow.NewNotarizationVote(firstBlock.Slot, firstBlock.Hash)))
	require.NoError(t, alpenglow.SaveVoteHistory(historyDir, history, identity))
	replayBlock := &block.Block{
		Slot: fallbackBlock.Slot, ParentSlot: parent.Slot,
		AlpenglowBlockID: [32]byte(fallbackBlock.Hash), HasAlpenglowBlockID: true,
		AlpenglowParentBlockID: [32]byte(parent.Hash), HasAlpenglowParentBlockID: true,
	}
	require.NoError(t, engine.ObserveBlock(context.Background(), BlockObservation{Block: replayBlock, Source: "test"}))
	require.NoError(t, engine.OnReplayResult(context.Background(), SlotReplayResult{Slot: fallbackBlock.Slot, Source: "test"}))
	_, accepted := engine.acceptVerifiedCertificate(alpenglow.Certificate{
		Type: alpenglow.CertificateNotarizeFallback, Slot: parent.Slot, BlockHash: parent.Hash,
		StakeVerified: true, SignatureVerified: true,
	})
	require.True(t, accepted)
	for rank, key := range []ed25519.PrivateKey{voterTestKey(21), voterTestKey(22)} {
		peer := signedVerifiedVoterPeerVote(t, engine, set, key, uint16(rank+1),
			alpenglow.NewNotarizationVote(fallbackBlock.Slot, fallbackBlock.Hash))
		admission, err := engine.acceptVerifiedVoteResult(peer)
		require.NoError(t, err)
		require.Equal(t, alpenglow.VoteAdmissionAccepted, admission)
	}
	restored, err := alpenglow.LoadVoteHistory(historyDir, node)
	require.NoError(t, err)
	require.False(t, restored.HasNotarFallback(fallbackBlock))

	require.NoError(t, engine.EnableVoting(VotingConfig{
		Identity: identity, AuthorizedVoter: authorized, VoteAccount: voteAccount, HistoryDir: historyDir,
		EpochForSlot:   func(uint64) uint64 { return set.Epoch },
		Peers:          func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil },
		WaitToVoteSlot: root.Slot + 1,
		ReadyToVote:    func(uint64) bool { return true },
	}))
	require.Eventually(t, func() bool {
		restored, loadErr := alpenglow.LoadVoteHistory(historyDir, node)
		return loadErr == nil && restored.HasNotarFallback(fallbackBlock)
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, engine.AlpenglowSafetyError())
}
