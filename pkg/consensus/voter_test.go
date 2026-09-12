package consensus

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestAlpenglowVoterLoopFailureClosesAdmission(t *testing.T) {
	engine, err := NewEngine(Config{})
	require.NoError(t, err)
	voter := &alpenglowVoter{
		engine:  engine,
		history: alpenglow.NewVoteHistory(solana.PublicKey{}, 0),
		events:  make(chan voterEvent, 64),
		done:    make(chan struct{}),
	}
	voter.start()
	require.NoError(t, voter.enqueue(voterEvent{
		kind: voterEventConsensus,
		consensus: alpenglow.ConsensusEvent{
			Kind:   alpenglow.ConsensusEventConflict,
			Slot:   42,
			Reason: "test safety fault",
		},
	}))

	select {
	case <-voter.done:
	case <-time.After(time.Second):
		t.Fatal("voter loop exited on a safety fault without closing admission")
	}
	voter.wg.Wait()
	require.Error(t, engine.AlpenglowSafetyError())
	for range 32 {
		require.ErrorContains(t, voter.enqueue(voterEvent{kind: voterEventFirstShred, slot: 43}), "closed")
	}
	require.Empty(t, voter.events, "closed voter retained events without a consumer")
}

func TestEnableVotingRejectsDifferentTransportIdentity(t *testing.T) {
	transportIdentity := voterTestKey(31)
	votingIdentity := voterTestKey(32)
	engine, err := NewEngine(Config{AlpenglowIdentity: transportIdentity})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	err = engine.EnableVoting(VotingConfig{
		Identity:        votingIdentity,
		AuthorizedVoter: voterTestKey(33),
		VoteAccount:     solana.PublicKey(voterTestKey(34).Public().(ed25519.PublicKey)),
		HistoryDir:      t.TempDir(),
		EpochForSlot:    func(uint64) uint64 { return 0 },
		Peers:           func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil },
	})
	require.ErrorContains(t, err, "does not match consensus transport identity")
}

func TestEnableVotingSeedsTransportPeersBeforeBroadcasterStarts(t *testing.T) {
	identity := voterTestKey(35)
	authorized := voterTestKey(36)
	voteAccount := solana.PublicKey(voterTestKey(37).Public().(ed25519.PublicKey))
	set := voterTestValidatorSet(t, identity, authorized, voteAccount)
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}

	engine, err := NewEngine(Config{AlpenglowShredVersion: 0x1234, AlpenglowIdentity: identity})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	require.NoError(t, engine.SetAlpenglowValidatorSet(set))
	engine.SetAlpenglowRoot(root)

	firstPeers := make(chan []alpenglow.ValidatorStake, 1)
	require.NoError(t, engine.EnableVoting(VotingConfig{
		Identity:        identity,
		AuthorizedVoter: authorized,
		VoteAccount:     voteAccount,
		HistoryDir:      t.TempDir(),
		EpochForSlot:    func(uint64) uint64 { return set.Epoch },
		Peers: func(validators []alpenglow.ValidatorStake) []alpenglow.VotorPeer {
			select {
			case firstPeers <- append([]alpenglow.ValidatorStake(nil), validators...):
			default:
			}
			return nil
		},
	}))

	select {
	case validators := <-firstPeers:
		require.Equal(t, set.Validators, validators)
	default:
		t.Fatal("initial broadcaster reconciliation ran before validator sets were installed")
	}
}

func TestNetworkCertificateLandingIgnoredAfterSafetyFault(t *testing.T) {
	engine, err := NewEngine(Config{})
	require.NoError(t, err)
	voter := &alpenglowVoter{
		events: make(chan voterEvent, 1),
		done:   make(chan struct{}),
	}
	engine.voterMu.Lock()
	engine.voter = voter
	engine.voterMu.Unlock()
	t.Cleanup(func() {
		engine.voterMu.Lock()
		engine.voter = nil
		engine.voterMu.Unlock()
	})

	engine.latchSafetyError(errors.New("test safety fault"))
	engine.noteNetworkCertificate(alpenglow.Certificate{
		Type:      alpenglow.CertificateSkip,
		Slot:      42,
		Signature: []byte{1},
		Bitmap:    []byte{2},
	})

	require.Empty(t, voter.events)
	engine.networkProofsMu.Lock()
	require.Empty(t, engine.networkProofs)
	require.Empty(t, engine.networkProofOrder)
	engine.networkProofsMu.Unlock()
}

func TestEngineVotingWiringCastsOnlyAfterSuccessfulReplay(t *testing.T) {
	identity := voterTestKey(51)
	authorized := voterTestKey(52)
	voteAccount := solana.PublicKey(voterTestKey(53).Public().(ed25519.PublicKey))
	set := voterTestValidatorSet(t, identity, authorized, voteAccount)
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
	historyDir := t.TempDir()

	engine, err := NewEngine(Config{AlpenglowShredVersion: 0x1234, AlpenglowIdentity: identity})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	require.NoError(t, engine.SetAlpenglowValidatorSet(set))
	engine.SetAlpenglowRoot(root)
	require.NoError(t, engine.EnableVoting(VotingConfig{
		Identity:        identity,
		AuthorizedVoter: authorized,
		VoteAccount:     voteAccount,
		HistoryDir:      historyDir,
		EpochForSlot:    func(uint64) uint64 { return set.Epoch },
		Peers:           func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil },
		ReadyToVote:     func(uint64) bool { return true },
	}))

	blockID := solana.Hash{0x40}
	parentID := root.Hash
	replayBlock := &block.Block{
		Slot:                      40,
		ParentSlot:                root.Slot,
		AlpenglowBlockID:          [32]byte(blockID),
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    [32]byte(parentID),
		HasAlpenglowParentBlockID: true,
	}
	require.NoError(t, engine.ObserveBlock(context.Background(), BlockObservation{Block: replayBlock, Source: "test"}))
	// Observation alone is not execution proof and must not trigger a vote.
	time.Sleep(20 * time.Millisecond)
	history, err := alpenglow.LoadVoteHistory(historyDir, solana.PublicKey(identity.Public().(ed25519.PublicKey)))
	require.NoError(t, err)
	require.False(t, history.VotedAt(40))

	require.NoError(t, engine.OnReplayResult(context.Background(), SlotReplayResult{Slot: 40, Source: "test"}))
	require.Eventually(t, func() bool {
		history, err := alpenglow.LoadVoteHistory(historyDir, solana.PublicKey(identity.Public().(ed25519.PublicKey)))
		return err == nil && history.VotedNotar[40] == blockID
	}, 2*time.Second, 10*time.Millisecond)
}

func TestLocalVoteSelfVerificationUsesVoteSlotEpoch(t *testing.T) {
	const voteSlot = uint64(3677108)
	identity := voterTestKey(61)
	authorized := voterTestKey(62)
	voteAccount := solana.PublicKey(voterTestKey(63).Public().(ed25519.PublicKey))

	current := voterTestValidatorSet(t, identity, authorized, voteAccount)
	current.Epoch = 68
	current.TotalStake = 6384321979102293
	current.Validators[0].Stake = 99999995434240
	future := cloneValidatorSet(current)
	future.Epoch = 69
	future.TotalStake = 6385864915298246

	engine, err := NewEngine(Config{AlpenglowShredVersion: 0x1234, AlpenglowIdentity: identity})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetAlpenglowEpochLookup(func(slot uint64) uint64 {
		if slot <= voteSlot {
			return current.Epoch
		}
		return future.Epoch
	})
	require.NoError(t, engine.SetAlpenglowValidatorSet(current))
	require.NoError(t, engine.SetAlpenglowValidatorSet(future))
	require.Equal(t, future.Epoch, engine.ensureVerifier().LatestEpoch(), "future stakes should be preloaded")

	vote := alpenglow.NewSkipVote(voteSlot)
	payload, err := alpenglow.EncodeVotePayloadToSign(vote, engine.shredVersion)
	require.NoError(t, err)
	signer, err := alpenglow.DeriveBLSSigner(authorized)
	require.NoError(t, err)
	signature, err := signer.Sign(payload)
	require.NoError(t, err)
	message := alpenglow.VoteMessage{Vote: vote, Signature: signature, Rank: current.Validators[0].Rank}
	result := alpenglow.VoteVerifyResult{
		Epoch:      current.Epoch,
		Rank:       current.Validators[0].Rank,
		Stake:      current.Validators[0].Stake,
		TotalStake: current.TotalStake,
	}

	require.NoError(t, engine.injectLocalVote(message, result))
	require.True(t, engine.ensurePool().HasVerifiedVote(message))
	require.NoError(t, engine.AlpenglowSafetyError())
}

func TestAlpenglowVoterWaitsForLiveWindowThenVotesAndPersists(t *testing.T) {
	identity := voterTestKey(11)
	authorized := voterTestKey(12)
	voteAccount := solana.PublicKey(voterTestKey(13).Public().(ed25519.PublicKey))
	set := voterTestValidatorSet(t, identity, authorized, voteAccount)
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}

	engine, err := NewEngine(Config{AlpenglowShredVersion: 0x1234, AlpenglowIdentity: identity})
	require.NoError(t, err)
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	require.NoError(t, engine.SetAlpenglowValidatorSet(set))
	engine.SetAlpenglowRoot(root)

	ready := false
	voter, err := newAlpenglowVoter(engine, VotingConfig{
		Identity:        identity,
		AuthorizedVoter: authorized,
		VoteAccount:     voteAccount,
		HistoryDir:      t.TempDir(),
		EpochForSlot:    func(uint64) uint64 { return set.Epoch },
		Peers:           func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil },
		WaitToVoteSlot:  40,
		ReadyToVote:     func(uint64) bool { return ready },
	}, root)
	require.NoError(t, err)
	// Drive this focused state-machine test synchronously while leaving its
	// outbound worker alive for the cast path.
	voter.closeOnce.Do(func() { close(voter.done) })
	voter.wg.Wait()
	t.Cleanup(func() { require.NoError(t, voter.broadcaster.Close()) })
	voter.sets[set.Epoch] = cloneValidatorSet(set)
	require.True(t, voter.history.AddParentReady(40, root))
	require.ErrorIs(t, voter.votingGateError(40), errVoterNotReady)

	block := alpenglow.BlockID{Slot: 40, Hash: solana.Hash{0x40}}
	observation := alpenglow.ReplayBlockObservation{Block: block, ParentSlot: root.Slot, ParentHash: root.Hash}
	require.NoError(t, voter.handle(voterEvent{kind: voterEventBlock, block: observation}))
	require.False(t, voter.history.VotedAt(block.Slot))
	require.Empty(t, voter.pending, "catch-up blocks must not be retained for backfill")

	ready = true
	require.NoError(t, voter.handle(voterEvent{kind: voterEventBlock, block: observation}))
	require.Equal(t, block.Hash, voter.history.VotedNotar[block.Slot])
	require.True(t, engine.ensurePool().Snapshot().VerifiedVotes > 0)
	require.True(t, voter.votingStarted)

	// The wall-clock estimate may drift after the one-time live join. It must
	// not disable votes driven by verified Votor/replay events thereafter.
	ready = false
	voter.history.AddBlockNotarized(block)
	voted, err := voter.tryFinal(block)
	require.NoError(t, err)
	require.True(t, voted)
	require.True(t, voter.history.IsOver(block.Slot))

	require.NoError(t, voter.trySkipWindow(44))
	for slot := uint64(44); slot <= 47; slot++ {
		require.Truef(t, voter.history.HasSkipped(slot), "slot %d", slot)
	}
	require.NoError(t, voter.handle(voterEvent{kind: voterEventFirstShred, slot: 48}))
	require.NoError(t, voter.handle(voterEvent{kind: voterEventCrashedLeaderTimeout, slot: 48}))
	require.False(t, voter.history.VotedAt(48), "first shred suppresses only the early crashed-leader timeout")
	require.NoError(t, voter.handle(voterEvent{kind: voterEventBlockTimeout, slot: 48}))
	for slot := uint64(48); slot <= 51; slot++ {
		require.Truef(t, voter.history.HasSkipped(slot), "slot %d", slot)
	}

	restored, err := alpenglow.LoadVoteHistory(voter.historyDir, voter.node)
	require.NoError(t, err)
	require.Equal(t, block.Hash, restored.VotedNotar[block.Slot])
	require.True(t, restored.IsOver(block.Slot))
	require.True(t, restored.HasSkipped(51))
}

func TestAlpenglowVoterSuccessfulVoteCompletesLiveWindowJoin(t *testing.T) {
	identity := voterTestKey(71)
	authorized := voterTestKey(72)
	voteAccount := solana.PublicKey(voterTestKey(73).Public().(ed25519.PublicKey))
	set := voterTestValidatorSet(t, identity, authorized, voteAccount)
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}

	engine, err := NewEngine(Config{AlpenglowShredVersion: 0x1234, AlpenglowIdentity: identity})
	require.NoError(t, err)
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	require.NoError(t, engine.SetAlpenglowValidatorSet(set))
	engine.SetAlpenglowRoot(root)

	ready := true
	voter, err := newAlpenglowVoter(engine, VotingConfig{
		Identity:        identity,
		AuthorizedVoter: authorized,
		VoteAccount:     voteAccount,
		HistoryDir:      t.TempDir(),
		EpochForSlot:    func(uint64) uint64 { return set.Epoch },
		Peers:           func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil },
		WaitToVoteSlot:  40,
		ReadyToVote:     func(uint64) bool { return ready },
	}, root)
	require.NoError(t, err)
	voter.closeOnce.Do(func() { close(voter.done) })
	voter.wg.Wait()
	t.Cleanup(func() { require.NoError(t, voter.broadcaster.Close()) })
	voter.sets[set.Epoch] = cloneValidatorSet(set)

	voted, err := voter.cast(alpenglow.NewSkipVote(40), false)
	require.NoError(t, err)
	require.True(t, voted)
	require.True(t, voter.votingStarted)

	ready = false
	voted, err = voter.cast(alpenglow.NewSkipVote(41), false)
	require.NoError(t, err)
	require.True(t, voted, "a drifting wall-clock callback must not stop voting after the first successful live vote")
}

func TestNetworkVoteLandingRequiresExactVotorCertificateProof(t *testing.T) {
	t.Run("fresh network certificate confirms persisted vote", func(t *testing.T) {
		engine, cert := networkLandingTestEngine(t)
		engine.observeVotorMessage(alpenglow.NewCertificateMessage(cert))
		require.Eventually(t, func() bool {
			stats := engine.Snapshot().Voting
			return stats != nil && stats.NetworkLandedVotes == 1 && stats.LastNetworkLandedSlot == cert.Slot
		}, 2*time.Second, 10*time.Millisecond)
		engine.observeVotorMessage(alpenglow.NewCertificateMessage(cert))
		require.Never(t, func() bool {
			return engine.Snapshot().Voting.NetworkLandedVotes != 1
		}, 100*time.Millisecond, 10*time.Millisecond)
	})

	t.Run("footer alone cannot confirm but exact later network proof can", func(t *testing.T) {
		engine, cert := networkLandingTestEngine(t)
		engine.ObserveFooterCertificates([]alpenglow.Certificate{cert})
		require.Zero(t, engine.Snapshot().Voting.NetworkLandedVotes)

		// The logical certificate is cached from the footer. Receiving the exact
		// same signature+bitmap over Votor is still valid network-origin proof.
		engine.observeVotorMessage(alpenglow.NewCertificateMessage(cert))
		require.Eventually(t, func() bool {
			return engine.Snapshot().Voting.NetworkLandedVotes == 1
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("different cached signer set is never substituted", func(t *testing.T) {
		engine, certWithUs := networkLandingTestEngine(t)
		withoutUs := voterTestAggregateCertificate(t, certWithUs.Slot, certWithUs.BlockHash, []int{1, 2}, 0x1234)
		engine.ObserveFooterCertificates([]alpenglow.Certificate{withoutUs})

		// Both certificates have the same logical key, but only the wire proof has
		// our rank. The cached footer aggregate must not manufacture a landing.
		engine.observeVotorMessage(alpenglow.NewCertificateMessage(certWithUs))
		require.Never(t, func() bool {
			return engine.Snapshot().Voting.NetworkLandedVotes != 0
		}, 200*time.Millisecond, 10*time.Millisecond)
	})

	t.Run("invalid network certificate cannot confirm", func(t *testing.T) {
		engine, cert := networkLandingTestEngine(t)
		cert.Signature = append([]byte(nil), cert.Signature...)
		cert.Signature[0] ^= 0xff
		engine.observeVotorMessage(alpenglow.NewCertificateMessage(cert))
		require.Never(t, func() bool {
			return engine.Snapshot().Voting.NetworkLandedVotes != 0
		}, 200*time.Millisecond, 10*time.Millisecond)
	})

	t.Run("valid certificate cannot claim a vote absent from durable history", func(t *testing.T) {
		engine, _ := networkLandingTestEngine(t)
		uncast := voterTestAggregateCertificate(t, 41, solana.Hash{0x41}, []int{0, 1}, 0x1234)
		engine.observeVotorMessage(alpenglow.NewCertificateMessage(uncast))
		require.Never(t, func() bool {
			return engine.Snapshot().Voting.NetworkLandedVotes != 0
		}, 200*time.Millisecond, 10*time.Millisecond)
	})

	t.Run("network origin survives deferred epoch verification", func(t *testing.T) {
		identity := voterTestKey(51)
		authorized := voterTestKey(52)
		voteAccount := solana.PublicKey(voterTestKey(53).Public().(ed25519.PublicKey))
		set := voterTestValidatorSet(t, identity, authorized, voteAccount)
		root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}
		blockID := solana.Hash{0x40}
		historyDir := t.TempDir()
		history := alpenglow.NewVoteHistory(solana.PublicKey(identity.Public().(ed25519.PublicKey)), root.Slot)
		require.NoError(t, history.AddVote(alpenglow.NewNotarizationVote(40, blockID)))
		require.NoError(t, alpenglow.SaveVoteHistory(historyDir, history, identity))

		engine, err := NewEngine(Config{AlpenglowShredVersion: 0x1234, AlpenglowIdentity: identity})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, engine.Close()) })
		engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
		engine.SetAlpenglowRoot(root)
		require.NoError(t, engine.EnableVoting(VotingConfig{
			Identity:        identity,
			AuthorizedVoter: authorized,
			VoteAccount:     voteAccount,
			HistoryDir:      historyDir,
			EpochForSlot:    func(uint64) uint64 { return set.Epoch },
			Peers:           func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil },
			ReadyToVote:     func(uint64) bool { return true },
		}))

		cert := voterTestAggregateCertificate(t, 40, blockID, []int{0, 1}, 0x1234)
		engine.observeVotorMessage(alpenglow.NewCertificateMessage(cert))
		require.Zero(t, engine.Snapshot().Voting.NetworkLandedVotes)
		require.NoError(t, engine.SetAlpenglowValidatorSet(set))
		require.Eventually(t, func() bool {
			return engine.Snapshot().Voting.NetworkLandedVotes == 1
		}, 2*time.Second, 10*time.Millisecond)
	})
}

func networkLandingTestEngine(t *testing.T) (*AlpenglowObserverEngine, alpenglow.Certificate) {
	t.Helper()
	identity := voterTestKey(51)
	authorized := voterTestKey(52)
	voteAccount := solana.PublicKey(voterTestKey(53).Public().(ed25519.PublicKey))
	set := voterTestValidatorSet(t, identity, authorized, voteAccount)
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}}

	engine, err := NewEngine(Config{AlpenglowShredVersion: 0x1234, AlpenglowIdentity: identity})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	require.NoError(t, engine.SetAlpenglowValidatorSet(set))
	engine.SetAlpenglowRoot(root)
	require.NoError(t, engine.EnableVoting(VotingConfig{
		Identity:        identity,
		AuthorizedVoter: authorized,
		VoteAccount:     voteAccount,
		HistoryDir:      t.TempDir(),
		EpochForSlot:    func(uint64) uint64 { return set.Epoch },
		Peers:           func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil },
		ReadyToVote:     func(uint64) bool { return true },
	}))

	blockID := solana.Hash{0x40}
	replayBlock := &block.Block{
		Slot:                      40,
		ParentSlot:                root.Slot,
		AlpenglowBlockID:          [32]byte(blockID),
		HasAlpenglowBlockID:       true,
		AlpenglowParentBlockID:    [32]byte(root.Hash),
		HasAlpenglowParentBlockID: true,
	}
	require.NoError(t, engine.ObserveBlock(context.Background(), BlockObservation{Block: replayBlock, Source: "test"}))
	require.NoError(t, engine.OnReplayResult(context.Background(), SlotReplayResult{Slot: 40, Source: "test"}))
	require.Eventually(t, func() bool {
		stats := engine.Snapshot().Voting
		return stats != nil && stats.VotesCastThisRun > 0
	}, 2*time.Second, 10*time.Millisecond)

	return engine, voterTestAggregateCertificate(t, 40, blockID, []int{0, 1}, 0x1234)
}

func voterTestAggregateCertificate(t *testing.T, slot uint64, blockHash solana.Hash, ranks []int, shredVersion uint16) alpenglow.Certificate {
	t.Helper()
	cert := alpenglow.Certificate{Type: alpenglow.CertificateNotarize, Slot: slot, BlockHash: blockHash}
	base := make([]bool, 3)
	for _, rank := range ranks {
		base[rank] = true
	}
	bitmap, err := alpenglow.EncodeSignerStoreBitmap(alpenglow.SignerBitmap{
		Encoding: alpenglow.SignerBitmapBase2,
		Length:   len(base),
		Base:     base,
	})
	require.NoError(t, err)
	cert.Bitmap = bitmap

	vote := alpenglow.NewNotarizationVote(slot, blockHash)
	payload, err := alpenglow.EncodeVotePayloadToSign(vote, shredVersion)
	require.NoError(t, err)
	keys := []ed25519.PrivateKey{voterTestKey(52), voterTestKey(21), voterTestKey(22)}
	var aggregate bls12381.G2Affine
	aggregate.SetInfinity()
	for _, rank := range ranks {
		signer, err := alpenglow.DeriveBLSSigner(keys[rank])
		require.NoError(t, err)
		signature, err := signer.Sign(payload)
		require.NoError(t, err)
		var point bls12381.G2Affine
		_, err = point.SetBytes(signature)
		require.NoError(t, err)
		aggregate.Add(&aggregate, &point)
	}
	raw := aggregate.RawBytes()
	cert.Signature = append([]byte(nil), raw[:]...)
	return cert
}

func voterTestValidatorSet(t *testing.T, identity, authorized ed25519.PrivateKey, voteAccount solana.PublicKey) alpenglow.ValidatorSet {
	t.Helper()
	validators := make([]alpenglow.ValidatorStake, 3)
	keys := []ed25519.PrivateKey{authorized, voterTestKey(21), voterTestKey(22)}
	for rank, key := range keys {
		signer, err := alpenglow.DeriveBLSSigner(key)
		require.NoError(t, err)
		validators[rank] = alpenglow.ValidatorStake{
			Rank:                uint16(rank),
			VoteAccount:         solana.PublicKey(voterTestKey(byte(30 + rank)).Public().(ed25519.PublicKey)),
			NodePubkey:          solana.PublicKey(voterTestKey(byte(40 + rank)).Public().(ed25519.PublicKey)),
			BlsPubkeyCompressed: signer.PublicKeyCompressed(),
			Stake:               30,
		}
	}
	validators[0].VoteAccount = voteAccount
	validators[0].NodePubkey = solana.PublicKey(identity.Public().(ed25519.PublicKey))
	return alpenglow.ValidatorSet{Epoch: 7, Validators: validators, TotalStake: 100}
}

func voterTestKey(value byte) ed25519.PrivateKey {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = value
	}
	return ed25519.NewKeyFromSeed(seed)
}
