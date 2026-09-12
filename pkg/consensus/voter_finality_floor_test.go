package consensus

import (
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestAlpenglowVoterSkipsReplayBehindLiveFinality(t *testing.T) {
	const (
		durableRoot  = uint64(4_603_572)
		staleReplay  = uint64(4_603_708)
		liveFinality = uint64(4_604_090)
	)
	root := alpenglow.BlockID{Slot: durableRoot, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)
	voter.votingStarted = true

	parent := alpenglow.BlockID{Slot: staleReplay - 1, Hash: solana.Hash{0x70}}
	block := alpenglow.BlockID{Slot: staleReplay, Hash: solana.Hash{0x71}}
	require.True(t, voter.history.AddParentReady(staleReplay, parent))
	voter.pending[staleReplay] = []pendingVotorBlock{{block: block, parent: parent}}
	voter.receivedShred[staleReplay] = true
	voter.timeoutsSet[staleReplay] = true

	// This is the ordering from the incident: live Votor finality advances and
	// prunes the verified pool while replay is still hundreds of slots behind.
	engine.ensurePool().PruneBehindFinality(liveFinality)
	require.Greater(t, engine.ensurePool().Snapshot().RootSlot, staleReplay)
	require.NoError(t, voter.handle(voterEvent{
		kind: voterEventConsensus,
		consensus: alpenglow.ConsensusEvent{
			Kind:  alpenglow.ConsensusEventFinalized,
			Slot:  liveFinality,
			Block: alpenglow.BlockID{Slot: liveFinality, Hash: solana.Hash{0x72}},
		},
	}))
	require.Equal(t, liveFinality, voter.highestFinal)
	require.NotContains(t, voter.pending, staleReplay)
	require.NotContains(t, voter.receivedShred, staleReplay)
	require.NotContains(t, voter.timeoutsSet, staleReplay)

	observation := alpenglow.ReplayBlockObservation{
		Block:      block,
		ParentSlot: parent.Slot,
		ParentHash: parent.Hash,
	}
	require.NoError(t, voter.handle(voterEvent{kind: voterEventBlock, block: observation}))
	require.NoError(t, voter.handle(voterEvent{kind: voterEventBlockTimeout, slot: staleReplay}))
	require.False(t, voter.history.VotedAt(staleReplay))
	require.Empty(t, voter.history.VotesCast[staleReplay])
	require.Zero(t, engine.ensurePool().Snapshot().VerifiedVotes)
	require.NoError(t, engine.AlpenglowSafetyError())

	persisted, err := alpenglow.LoadVoteHistory(voter.historyDir, voter.node)
	require.NoError(t, err)
	require.False(t, persisted.VotedAt(staleReplay))
}

func TestAlpenglowVoterNeverVotesAtPoolRootBoundary(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)
	engine.ensurePool().SetRoot(alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x40}})

	require.Equal(t, uint64(40), voter.admissionFloor())
	require.NoError(t, voter.trySkipWindow(41))
	require.False(t, voter.history.HasSkipped(40), "the pool rejects votes at its root, not only below it")
	for slot := uint64(41); slot <= 43; slot++ {
		require.Truef(t, voter.history.HasSkipped(slot), "slot %d", slot)
	}
	require.Equal(t, uint64(3), engine.ensurePool().Snapshot().VerifiedVotes)
	require.NoError(t, engine.AlpenglowSafetyError())

	persisted, err := alpenglow.LoadVoteHistory(voter.historyDir, voter.node)
	require.NoError(t, err)
	require.False(t, persisted.HasSkipped(40))
	for slot := uint64(41); slot <= 43; slot++ {
		require.Truef(t, persisted.HasSkipped(slot), "persisted slot %d", slot)
	}
}

func TestAlpenglowVoterFinalityRaceAfterPersistenceIsBenign(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)

	hookCalled := false
	voter.beforeLocalVoteInject = func(vote alpenglow.Vote) {
		hookCalled = true
		require.Equal(t, uint64(40), vote.Slot)
		persisted, err := alpenglow.LoadVoteHistory(voter.historyDir, voter.node)
		require.NoError(t, err)
		require.True(t, persisted.HasSkipped(vote.Slot), "history must be durable before pool admission can publish a certificate")
		engine.ensurePool().SetRoot(alpenglow.BlockID{Slot: vote.Slot, Hash: solana.Hash{0x40}})
	}

	voted, err := voter.cast(alpenglow.NewSkipVote(40), false)
	require.NoError(t, err)
	require.False(t, voted)
	require.True(t, hookCalled)
	require.True(t, voter.history.HasSkipped(40), "retain the signed record conservatively for anti-equivocation")
	require.Zero(t, engine.ensurePool().Snapshot().VerifiedVotes)
	require.Zero(t, voter.snapshot().VotesCastThisRun)
	require.Zero(t, voter.snapshot().BroadcastMessagesQueued)
	require.NoError(t, engine.AlpenglowSafetyError())

	persisted, err := alpenglow.LoadVoteHistory(voter.historyDir, voter.node)
	require.NoError(t, err)
	require.True(t, persisted.HasSkipped(40))
}

func TestLocalVoteExactDuplicateAdmissionIsIdempotent(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)

	message, result, err := voter.sign(alpenglow.NewSkipVote(40), false)
	require.NoError(t, err)
	require.NoError(t, engine.injectLocalVote(message, result))
	require.NoError(t, engine.injectLocalVote(message, result))
	snapshot := engine.ensurePool().Snapshot()
	require.Equal(t, uint64(1), snapshot.VerifiedVotes)
	require.Equal(t, uint64(1), snapshot.DuplicateVotes)
	require.NoError(t, engine.AlpenglowSafetyError())
}

func TestAlpenglowVoterPersistenceFailurePublishesNothing(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)
	notDirectory := filepath.Join(t.TempDir(), "history-file")
	require.NoError(t, os.WriteFile(notDirectory, []byte("not a directory"), 0o600))
	voter.historyDir = notDirectory

	injectHookCalled := false
	voter.beforeLocalVoteInject = func(alpenglow.Vote) { injectHookCalled = true }
	voted, err := voter.cast(alpenglow.NewSkipVote(40), false)
	require.ErrorContains(t, err, "persist vote history before consensus publication")
	require.False(t, voted)
	require.False(t, injectHookCalled)
	snapshot := engine.ensurePool().Snapshot()
	require.Zero(t, snapshot.VerifiedVotes)
	require.Zero(t, snapshot.Certificates)
	require.Zero(t, voter.snapshot().BroadcastMessagesQueued)
	require.NoError(t, engine.AlpenglowSafetyError())
}

func TestAlpenglowVoterRestoresAndRebroadcastsSlotVoteOnce(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)
	require.NoError(t, voter.history.AddVote(alpenglow.NewSkipVote(40)))
	require.NoError(t, voter.saveHistory())

	require.NoError(t, voter.restoreVotesForEpoch(7))
	require.Equal(t, uint64(1), engine.ensurePool().Snapshot().VerifiedVotes)
	require.Equal(t, uint64(1), voter.snapshot().BroadcastMessagesQueued)
	require.Zero(t, voter.snapshot().VotesCastThisRun)
	require.False(t, voter.votingStarted, "restoring an old vote must not bypass the startup live-window gate")

	require.NoError(t, voter.restoreVotesForEpoch(7))
	require.Equal(t, uint64(1), voter.snapshot().BroadcastMessagesQueued, "each logical restored vote is rebroadcast once per run")
	require.NoError(t, engine.AlpenglowSafetyError())
}

func TestAlpenglowVoterDefersBlockVoteRestoreUntilExactReplay(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)
	block := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x40}}
	require.NoError(t, voter.history.AddVote(alpenglow.NewNotarizationVote(block.Slot, block.Hash)))
	voter.history.AddBlockNotarized(block)
	require.NoError(t, voter.history.AddVote(alpenglow.NewFinalizationVote(block.Slot)))
	require.NoError(t, voter.saveHistory())

	observation := alpenglow.ReplayBlockObservation{
		Block:      block,
		ParentSlot: root.Slot,
		ParentHash: root.Hash,
	}
	// ObserveBlock records the candidate before execution. A validator-set
	// event in this interval must not restore or publish its historical vote.
	engine.observeChainReplayBlock(observation)
	require.NoError(t, voter.restoreVotesForEpoch(7))
	require.Zero(t, engine.ensurePool().Snapshot().VerifiedVotes)
	require.Zero(t, voter.snapshot().BroadcastMessagesQueued)

	// voterEventBlock is emitted only by successful OnReplayResult.
	require.NoError(t, voter.handle(voterEvent{kind: voterEventBlock, block: observation}))
	require.Equal(t, uint64(2), engine.ensurePool().Snapshot().VerifiedVotes)
	require.Equal(t, uint64(2), voter.snapshot().BroadcastMessagesQueued)
	require.NoError(t, engine.AlpenglowSafetyError())
}

func TestAlpenglowVoterRetriesBlockRestoreWhenSetArrivesAfterReplay(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)
	block := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x40}}
	require.NoError(t, voter.history.AddVote(alpenglow.NewNotarizationVote(block.Slot, block.Hash)))
	require.NoError(t, voter.saveHistory())
	set := voter.sets[7]
	delete(voter.sets, 7)

	observation := alpenglow.ReplayBlockObservation{
		Block:      block,
		ParentSlot: root.Slot,
		ParentHash: root.Hash,
	}
	require.NoError(t, voter.handle(voterEvent{kind: voterEventBlock, block: observation}))
	require.True(t, voter.executedBlocks[block])
	require.Zero(t, engine.ensurePool().Snapshot().VerifiedVotes)
	require.Zero(t, voter.snapshot().BroadcastMessagesQueued)

	voter.sets[7] = set
	require.NoError(t, voter.restoreVotesForEpoch(7))
	require.Equal(t, uint64(1), engine.ensurePool().Snapshot().VerifiedVotes)
	require.Equal(t, uint64(1), voter.snapshot().BroadcastMessagesQueued)
}

func TestAlpenglowVoterNeverRestoresKnownInvalidBlockVote(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)
	block := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x40}}
	require.NoError(t, voter.history.AddVote(alpenglow.NewNotarizationVote(block.Slot, block.Hash)))
	require.NoError(t, voter.saveHistory())
	require.NoError(t, engine.ObserveObjectivelyInvalidAlpenglowBlock(block, "test-invalid body"))

	require.NoError(t, voter.restoreVotesForEpoch(7))
	observation := alpenglow.ReplayBlockObservation{
		Block:      block,
		ParentSlot: root.Slot,
		ParentHash: root.Hash,
	}
	require.NoError(t, voter.handle(voterEvent{kind: voterEventBlock, block: observation}))
	require.Zero(t, engine.ensurePool().Snapshot().VerifiedVotes)
	require.Zero(t, engine.ensurePool().Snapshot().Certificates)
	require.Zero(t, voter.snapshot().BroadcastMessagesQueued)
	require.NoError(t, engine.AlpenglowSafetyError())
	require.True(t, voter.history.VotedAt(block.Slot), "invalidity suppresses publication, not the anti-equivocation record")
}

func TestAlpenglowVoterRestoresFallbackWithoutRebroadcastingUnexecutedFirstBlock(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)
	firstBlock := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x40}}
	fallbackBlock := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x41}}
	first := alpenglow.NewNotarizationVote(firstBlock.Slot, firstBlock.Hash)
	fallback := alpenglow.NewNotarizationFallbackVote(fallbackBlock.Slot, fallbackBlock.Hash)
	require.NoError(t, voter.history.AddVote(first))
	require.NoError(t, voter.history.AddVote(fallback))
	require.NoError(t, voter.saveHistory())
	voter.executedBlocks[fallbackBlock] = true
	voter.pending[fallbackBlock.Slot] = []pendingVotorBlock{{block: fallbackBlock, parent: root}}

	set := voter.sets[7]
	fallbackMessage, fallbackResult, err := voter.sign(fallback, false)
	require.NoError(t, err)
	update, err := engine.ensurePool().AddVerifiedVote(alpenglow.VerifiedVote{Message: fallbackMessage, Result: fallbackResult})
	require.NoError(t, err)
	require.True(t, update.VoteAccepted)
	require.NoError(t, voter.handle(voterEvent{kind: voterEventValidatorSet, set: set}))
	require.NotContains(t, voter.pending, fallbackBlock.Slot)
	require.Equal(t, uint64(1), engine.ensurePool().Snapshot().VerifiedVotes)
	require.Equal(t, uint64(1), engine.ensurePool().Snapshot().DuplicateVotes)
	require.Equal(t, uint64(1), voter.snapshot().BroadcastMessagesQueued)

	firstMessage, _, err := voter.sign(first, false)
	require.NoError(t, err)
	require.False(t, engine.ensurePool().HasVerifiedVote(firstMessage), "execution-unproven base block vote must remain unpublished")
	require.True(t, engine.ensurePool().HasVerifiedVote(fallbackMessage))

	// Two peer skips cross the 40% safety threshold. The resulting SafeToSkip
	// proves that durable Notarize(firstBlock), not the restored fallback, is
	// still the pool's local round-one provenance.
	for rank, key := range []ed25519.PrivateKey{voterTestKey(21), voterTestKey(22)} {
		peer := signedVerifiedVoterPeerVote(t, engine, set, key, uint16(rank+1), alpenglow.NewSkipVote(40))
		update, err = engine.ensurePool().AddVerifiedVote(peer)
		require.NoError(t, err)
	}
	require.True(t, consensusUpdateHasEvent(update, alpenglow.ConsensusEventSafeToSkip, 40))
	for _, event := range update.Events {
		if event.Kind == alpenglow.ConsensusEventSafeToSkip {
			require.NoError(t, voter.handleConsensus(event))
		}
	}
	require.True(t, voter.history.HasSkipFallback(40))
	require.Equal(t, uint64(5), voter.snapshot().BroadcastMessagesQueued, "restored fallback plus three window skips and one skip-fallback")

	// The base identity may execute later than its already-restored fallback.
	// It must still restore once, without conflicting with the same-rank
	// fallback vote for a different block.
	require.NoError(t, voter.handle(voterEvent{kind: voterEventBlock, block: alpenglow.ReplayBlockObservation{
		Block: firstBlock, ParentSlot: root.Slot, ParentHash: root.Hash,
	}}))
	require.True(t, engine.ensurePool().HasVerifiedVote(firstMessage))
	require.Equal(t, uint64(6), voter.snapshot().BroadcastMessagesQueued)
	require.NoError(t, engine.AlpenglowSafetyError())
}

func TestAlpenglowVoterStandstillNeverRefreshesUnexecutedOrInvalidBlockVote(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)
	blockID := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x40}}
	require.NoError(t, voter.history.AddVote(alpenglow.NewNotarizationVote(blockID.Slot, blockID.Hash)))
	voter.votingStarted = true
	voter.latestLiveSlot = blockID.Slot
	voter.highestFinal = root.Slot
	voter.lastFinalizedAt = time.Now().Add(-votorStandstillTimeout - time.Second)

	require.Empty(t, voter.buildRefreshQueue(), "ObserveBlock without successful replay is not refresh authority")
	voter.executedBlocks[blockID] = true
	voter.refreshQueue = voter.buildRefreshQueue()
	require.Len(t, voter.refreshQueue, 1)
	standstill := root.Slot
	voter.standstillSlot = &standstill
	require.NoError(t, engine.ObserveObjectivelyInvalidAlpenglowBlock(blockID, "test invalid after refresh queue build"))
	require.NoError(t, voter.handleStandstill())
	require.Empty(t, voter.refreshQueue, "send-time tombstone recheck must remove a stale queued refresh")
	require.Zero(t, voter.snapshot().BroadcastMessagesQueued)
}

func TestAlpenglowVoterStandstillDropsRefreshThatBecameRooted(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)
	require.NoError(t, voter.history.AddVote(alpenglow.NewSkipVote(40)))
	voter.votingStarted = true
	voter.latestLiveSlot = 40
	voter.highestFinal = root.Slot
	voter.lastFinalizedAt = time.Now().Add(-votorStandstillTimeout - time.Second)
	voter.refreshQueue = voter.buildRefreshQueue()
	require.Len(t, voter.refreshQueue, 1)
	standstill := root.Slot
	voter.standstillSlot = &standstill
	newRoot := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x40}}
	engine.ensurePool().SetRoot(newRoot)
	require.NoError(t, voter.handle(voterEvent{kind: voterEventRoot, root: newRoot}))
	require.NoError(t, voter.handleStandstill())
	require.Empty(t, voter.refreshQueue)
	require.Zero(t, voter.snapshot().BroadcastMessagesQueued)
}

func TestAlpenglowVoterStandstillRetainsLatestFinalityCertificate(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)
	certificate := alpenglow.Certificate{
		Type:              alpenglow.CertificateFinalize,
		Slot:              40,
		SignatureVerified: true,
		StakeVerified:     true,
	}
	update, err := engine.ensurePool().AddVerifiedCertificate(certificate)
	require.NoError(t, err)
	require.Len(t, update.Certificates, 1)
	voter.votingStarted = true
	voter.latestLiveSlot = certificate.Slot
	voter.highestFinal = certificate.Slot
	voter.lastFinalizedAt = time.Now().Add(-votorStandstillTimeout - time.Second)
	voter.refreshQueue = voter.buildRefreshQueue()
	require.Len(t, voter.refreshQueue, 1, "latest-finality proof is intentionally refreshable at the action floor")
	standstill := certificate.Slot
	voter.standstillSlot = &standstill
	require.NoError(t, voter.handleStandstill())
	require.Equal(t, uint64(1), voter.snapshot().BroadcastMessagesQueued)
}

func TestAlpenglowVotingEnabledAfterReplayRestoresAllExecutionProvenForkVotes(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	identity := voterTestKey(91)
	authorized := voterTestKey(92)
	voteAccount := solana.PublicKey(voterTestKey(93).Public().(ed25519.PublicKey))
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
	firstBlock := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x40}}
	fallbackBlock := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x41}}
	require.NoError(t, history.AddVote(alpenglow.NewNotarizationVote(firstBlock.Slot, firstBlock.Hash)))
	require.NoError(t, history.AddVote(alpenglow.NewNotarizationFallbackVote(fallbackBlock.Slot, fallbackBlock.Hash)))
	require.NoError(t, alpenglow.SaveVoteHistory(historyDir, history, identity))
	for _, blockID := range []alpenglow.BlockID{firstBlock, fallbackBlock} {
		replayBlock := &block.Block{
			Slot:                      blockID.Slot,
			ParentSlot:                root.Slot,
			AlpenglowBlockID:          [32]byte(blockID.Hash),
			HasAlpenglowBlockID:       true,
			AlpenglowParentBlockID:    [32]byte(root.Hash),
			HasAlpenglowParentBlockID: true,
		}
		require.NoError(t, engine.ObserveBlock(context.Background(), BlockObservation{Block: replayBlock, Source: "test"}))
		require.NoError(t, engine.OnReplayResult(context.Background(), SlotReplayResult{Slot: blockID.Slot, Source: "test"}))
	}
	require.NoError(t, engine.EnableVoting(VotingConfig{
		Identity:        identity,
		AuthorizedVoter: authorized,
		VoteAccount:     voteAccount,
		HistoryDir:      historyDir,
		EpochForSlot:    func(uint64) uint64 { return set.Epoch },
		Peers:           func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil },
		WaitToVoteSlot:  firstBlock.Slot,
		ReadyToVote:     func(uint64) bool { return true },
	}))
	require.Eventually(t, func() bool {
		stats := engine.Snapshot().Voting
		return engine.ensurePool().Snapshot().VerifiedVotes == 2 && stats != nil && stats.BroadcastMessagesQueued == 2
	}, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, engine.Snapshot().Voting.VotesCastThisRun, "restart restoration is not a new vote")
	require.NoError(t, engine.AlpenglowSafetyError())
}

func TestAlpenglowValidatorSetEpochIsImmutable(t *testing.T) {
	engine, err := NewEngine(Config{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	set := voterTestValidatorSet(t, voterTestKey(94), voterTestKey(95), solana.PublicKey(voterTestKey(96).Public().(ed25519.PublicKey)))
	require.NoError(t, engine.SetAlpenglowValidatorSet(set))
	require.NoError(t, engine.SetAlpenglowValidatorSet(cloneValidatorSet(set)))
	changed := cloneValidatorSet(set)
	changed.Validators[0].Stake++
	changed.TotalStake++
	require.ErrorContains(t, engine.SetAlpenglowValidatorSet(changed), "immutable")
	installed, ok := engine.ensureVerifier().ValidatorSetForEpoch(set.Epoch)
	require.True(t, ok)
	require.True(t, validatorSetsEqual(set, installed))
}

func TestAlpenglowVoterLandingDedupeSurvivesLiveFinality(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)
	block := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x40}}
	require.NoError(t, voter.history.AddVote(alpenglow.NewNotarizationVote(block.Slot, block.Hash)))
	bitmap, err := alpenglow.EncodeSignerStoreBitmap(alpenglow.SignerBitmap{
		Encoding: alpenglow.SignerBitmapBase2,
		Length:   3,
		Base:     []bool{true, false, false},
	})
	require.NoError(t, err)
	certificate := alpenglow.Certificate{
		Type:      alpenglow.CertificateNotarize,
		Slot:      block.Slot,
		BlockHash: block.Hash,
		Bitmap:    bitmap,
	}

	voter.recordNetworkCertificate(certificate)
	require.Equal(t, uint64(1), voter.snapshot().NetworkLandedVotes)
	require.NoError(t, voter.handleConsensus(alpenglow.ConsensusEvent{
		Kind:  alpenglow.ConsensusEventFinalized,
		Slot:  50,
		Block: alpenglow.BlockID{Slot: 50, Hash: solana.Hash{0x50}},
	}))
	require.Len(t, voter.landed, 1, "live finality must not forget a durable vote's unique landing proof")
	voter.recordNetworkCertificate(certificate)
	require.Equal(t, uint64(1), voter.snapshot().NetworkLandedVotes)

	engine.ensurePool().SetRoot(alpenglow.BlockID{Slot: 41, Hash: solana.Hash{0x41}})
	require.NoError(t, voter.handle(voterEvent{
		kind: voterEventRoot,
		root: alpenglow.BlockID{Slot: 41, Hash: solana.Hash{0x41}},
	}))
	require.Empty(t, voter.landed, "durable root retirement may release landing dedupe state")
	voter.recordNetworkCertificate(certificate)
	require.Equal(t, uint64(1), voter.snapshot().NetworkLandedVotes)
}

func TestLocalVoteCapacityRejectionRemainsHardError(t *testing.T) {
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	engine, voter := newStoppedFloorTestVoter(t, root)
	cfg := alpenglow.DefaultConsensusPoolConfig()
	cfg.RootBlock = root
	cfg.MaxVerifiedVotes = 1
	engine.pool = alpenglow.NewConsensusPool(cfg)

	voted, err := voter.cast(alpenglow.NewSkipVote(40), false)
	require.NoError(t, err)
	require.True(t, voted)

	voted, err = voter.cast(alpenglow.NewSkipVote(41), false)
	require.Error(t, err)
	require.False(t, voted)
	require.False(t, errors.Is(err, errLocalVoteStale))
	require.True(t, strings.Contains(err.Error(), string(alpenglow.VoteAdmissionVoteCapacity)), err.Error())
	require.Equal(t, uint64(1), engine.ensurePool().Snapshot().VerifiedVotes)
	persisted, loadErr := alpenglow.LoadVoteHistory(voter.historyDir, voter.node)
	require.NoError(t, loadErr)
	require.True(t, persisted.HasSkipped(41), "hard admission failure retains the signed anti-equivocation record")
	require.Equal(t, uint64(1), voter.snapshot().BroadcastMessagesQueued, "the rejected vote itself is never broadcast")
}

func newStoppedFloorTestVoter(t *testing.T, root alpenglow.BlockID) (*AlpenglowObserverEngine, *alpenglowVoter) {
	t.Helper()
	identity := voterTestKey(81)
	authorized := voterTestKey(82)
	voteAccount := solana.PublicKey(voterTestKey(83).Public().(ed25519.PublicKey))
	set := voterTestValidatorSet(t, identity, authorized, voteAccount)

	engine, err := NewEngine(Config{AlpenglowShredVersion: 0x1234, AlpenglowIdentity: identity})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	require.NoError(t, engine.SetAlpenglowValidatorSet(set))
	engine.SetAlpenglowRoot(root)

	voter, err := newAlpenglowVoter(engine, VotingConfig{
		Identity:        identity,
		AuthorizedVoter: authorized,
		VoteAccount:     voteAccount,
		HistoryDir:      t.TempDir(),
		EpochForSlot:    func(uint64) uint64 { return set.Epoch },
		Peers:           func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil },
		WaitToVoteSlot:  root.Slot + 1,
		ReadyToVote:     func(uint64) bool { return true },
	}, root)
	require.NoError(t, err)
	voter.closeOnce.Do(func() { close(voter.done) })
	voter.wg.Wait()
	t.Cleanup(func() { require.NoError(t, voter.broadcaster.Close()) })
	voter.sets[set.Epoch] = cloneValidatorSet(set)
	return engine, voter
}

func signedVerifiedVoterPeerVote(t *testing.T, engine *AlpenglowObserverEngine, set alpenglow.ValidatorSet, key ed25519.PrivateKey, rank uint16, vote alpenglow.Vote) alpenglow.VerifiedVote {
	t.Helper()
	signer, err := alpenglow.DeriveBLSSigner(key)
	require.NoError(t, err)
	payload, err := alpenglow.EncodeVotePayloadToSign(vote, engine.shredVersion)
	require.NoError(t, err)
	signature, err := signer.Sign(payload)
	require.NoError(t, err)
	message := alpenglow.VoteMessage{Vote: vote, Signature: signature, Rank: rank}
	result, err := engine.ensureVerifier().VerifyVoteMessageForEpoch(set.Epoch, message)
	require.NoError(t, err)
	return alpenglow.VerifiedVote{Message: message, Result: result}
}

func consensusUpdateHasEvent(update alpenglow.ConsensusUpdate, kind alpenglow.ConsensusEventKind, slot uint64) bool {
	for _, event := range update.Events {
		if event.Kind == kind && event.Slot == slot {
			return true
		}
	}
	return false
}
