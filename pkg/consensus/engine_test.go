package consensus

import (
	"context"
	"errors"
	"math/big"
	"testing"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

func TestNormalizeMode(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want Mode
	}{
		{name: "empty defaults classic", raw: "", want: ModeClassic},
		{name: "classic", raw: "classic", want: ModeClassic},
		{name: "legacy alias", raw: "legacy", want: ModeClassic},
		{name: "observer", raw: "alpenglow-observer", want: ModeAlpenglowObserver},
		{name: "alpenglow", raw: "alpenglow", want: ModeAlpenglow},
		{name: "trim lowercase", raw: "  CLASSIC  ", want: ModeClassic},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeMode(tt.raw)
			if err != nil {
				t.Fatalf("NormalizeMode(%q) returned error: %v", tt.raw, err)
			}
			if got != tt.want {
				t.Fatalf("NormalizeMode(%q)=%q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestNormalizeModeRejectsUnknown(t *testing.T) {
	if _, err := NormalizeMode("tower"); err == nil {
		t.Fatalf("expected invalid mode error")
	}
}

func TestAlpenglowVotingModeFailsFast(t *testing.T) {
	engine, err := NewEngine(ModeAlpenglow)
	if err != nil {
		t.Fatalf("NewEngine returned error: %v", err)
	}
	if err := engine.Start(context.Background()); !errors.Is(err, ErrAlpenglowVotingNotImplemented) {
		t.Fatalf("Start error = %v, want %v", err, ErrAlpenglowVotingNotImplemented)
	}
}

func TestAlpenglowObserverTracksReplayInSnapshot(t *testing.T) {
	engine, err := NewEngine(ModeAlpenglowObserver)
	if err != nil {
		t.Fatalf("NewEngine returned error: %v", err)
	}
	if err := engine.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	var blockhash [32]byte
	blockhash[0] = 7
	var alpenglowBlockID [32]byte
	alpenglowBlockID[0] = 9
	if err := engine.ObserveBlock(context.Background(), BlockObservation{
		Block: &block.Block{
			Slot:                99,
			Blockhash:           blockhash,
			AlpenglowBlockID:    alpenglowBlockID,
			HasAlpenglowBlockID: true,
		},
		Source: "turbine",
	}); err != nil {
		t.Fatalf("ObserveBlock returned error: %v", err)
	}
	if err := engine.OnReplayResult(context.Background(), SlotReplayResult{
		Slot:     99,
		Bankhash: blockhash,
		Source:   "turbine",
	}); err != nil {
		t.Fatalf("OnReplayResult returned error: %v", err)
	}

	snapshot := engine.Snapshot()
	if snapshot.Alpenglow == nil {
		t.Fatalf("expected alpenglow snapshot")
	}
	if snapshot.Alpenglow.ReplayBlocksObserved != 1 || snapshot.Alpenglow.ReplayResultsObserved != 1 {
		t.Fatalf("alpenglow snapshot = %+v", snapshot.Alpenglow)
	}
	if snapshot.Alpenglow.LatestReplayBlockSlot != 99 || snapshot.Alpenglow.LatestReplayResultSlot != 99 {
		t.Fatalf("alpenglow latest slots = %+v", snapshot.Alpenglow)
	}
	if [32]byte(snapshot.Alpenglow.LatestReplayBlock.Hash) != alpenglowBlockID {
		t.Fatalf("latest replay block hash = %s, want %x", snapshot.Alpenglow.LatestReplayBlock.Hash, alpenglowBlockID)
	}
}

func TestAlpenglowObserverFeedsCertifiedDecisionResolver(t *testing.T) {
	engine, err := NewEngine(ModeAlpenglowObserver)
	if err != nil {
		t.Fatalf("NewEngine returned error: %v", err)
	}
	observer := engine.(*AlpenglowObserverEngine)
	if err := observer.SetAlpenglowValidatorSet(testAlpenglowValidatorSet()); err != nil {
		t.Fatalf("SetAlpenglowValidatorSet returned error: %v", err)
	}

	cert := alpenglow.Certificate{
		Type:   alpenglow.CertificateSkip,
		Slot:   42,
		Bitmap: testAlpenglowSignerBitmap(),
	}
	cert.Signature = testAlpenglowCertificateSignature(t, cert)
	observer.observeVotorMessage(alpenglow.NewCertificateMessage(cert))

	decision, ok := observer.NextAlpenglowDecision(41)
	if !ok {
		t.Fatalf("expected certified skip decision")
	}
	if decision.Kind != alpenglow.ChainDecisionKindSkip || decision.Slot != 42 {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	snapshot := observer.Snapshot()
	if snapshot.AlpenglowChain == nil {
		t.Fatalf("expected chain snapshot")
	}
	if snapshot.AlpenglowChain.CertificatesAccepted != 1 || snapshot.AlpenglowChain.CertifiedSkips != 1 {
		t.Fatalf("unexpected chain snapshot: %+v", snapshot.AlpenglowChain)
	}
}

func TestPoolRejectedCertificateDoesNotReachChainOrConsumers(t *testing.T) {
	observer := &AlpenglowObserverEngine{
		pool: alpenglow.NewConsensusPool(alpenglow.ConsensusPoolConfig{
			RootBlock: alpenglow.BlockID{Slot: 10, Hash: solana.Hash{1}},
		}),
	}
	var delivered int
	observer.SetVotorMessageHook(func(alpenglow.Message) { delivered++ })
	observer.acceptVerifiedCertificate(alpenglow.Certificate{
		Type:              alpenglow.CertificateSkip,
		Slot:              9,
		SignatureVerified: true,
		StakeVerified:     true,
	})
	if snapshot := observer.ensureChain().Snapshot(); snapshot.CertificatesObserved != 0 {
		t.Fatalf("pool-rejected certificate reached chain tracker: %+v", snapshot)
	}
	if delivered != 0 {
		t.Fatalf("pool-rejected certificate reached verified consumer hook")
	}
}

func TestVerifiedCertificatePoolInvariantFailureHaltsEngine(t *testing.T) {
	observer := &AlpenglowObserverEngine{
		pool: alpenglow.NewConsensusPool(alpenglow.DefaultConsensusPoolConfig()),
	}
	for seed := byte(1); seed <= 8; seed++ {
		observer.acceptVerifiedCertificate(alpenglow.Certificate{
			Type:              alpenglow.CertificateNotarizeFallback,
			Slot:              11,
			BlockHash:         solana.Hash{seed},
			SignatureVerified: true,
			StakeVerified:     true,
		})
	}
	decision, ok := observer.NextAlpenglowDecision(10)
	if !ok || decision.Kind != alpenglow.ChainDecisionKindConflict {
		t.Fatalf("verified pool invariant failure did not halt decisions: %+v (ok=%v)", decision, ok)
	}
	if err := observer.OnReplayResult(context.Background(), SlotReplayResult{Slot: 11}); err == nil {
		t.Fatal("replay continued after verified pool invariant failure")
	}
}

func TestAcceptedStrongCertificateConflictHaltsEngine(t *testing.T) {
	observer := &AlpenglowObserverEngine{
		pool: alpenglow.NewConsensusPool(alpenglow.DefaultConsensusPoolConfig()),
	}
	for seed := byte(1); seed <= 2; seed++ {
		observer.acceptVerifiedCertificate(alpenglow.Certificate{
			Type:              alpenglow.CertificateNotarize,
			Slot:              11,
			BlockHash:         solana.Hash{seed},
			SignatureVerified: true,
			StakeVerified:     true,
		})
	}
	decision, ok := observer.NextAlpenglowDecision(10)
	if !ok || decision.Kind != alpenglow.ChainDecisionKindConflict {
		t.Fatalf("strong-certificate conflict did not halt decisions: %+v (ok=%v)", decision, ok)
	}
	if err := observer.OnReplayResult(context.Background(), SlotReplayResult{Slot: 11}); err == nil {
		t.Fatal("replay continued after strong-certificate conflict")
	}
}

func TestAlpenglowObserverSafetyFaultFailsClosed(t *testing.T) {
	observer := &AlpenglowObserverEngine{}
	observer.latchSafetyError(errors.New("injected downstream rejection"))
	decision, ok := observer.NextAlpenglowDecision(41)
	if !ok || decision.Kind != alpenglow.ChainDecisionKindConflict || decision.Slot != 42 {
		t.Fatalf("safety decision = %+v (ok=%v)", decision, ok)
	}
	if err := observer.OnReplayResult(context.Background(), SlotReplayResult{Slot: 41}); err == nil {
		t.Fatal("replay continued after consensus safety fault")
	}
	if parent := observer.AlpenglowBlockProductionParent(44); parent.Kind != alpenglow.BlockProductionParentNotReady {
		t.Fatalf("block production parent after safety fault = %+v", parent)
	}
}

func TestAlpenglowObserverHaltsOnConflictingReplayParentLink(t *testing.T) {
	engine := &AlpenglowObserverEngine{
		chain: newAlpenglowObserverChainTracker(),
		pool:  alpenglow.NewConsensusPool(alpenglow.DefaultConsensusPoolConfig()),
	}
	block := alpenglow.BlockID{Slot: 12, Hash: solana.Hash{12}}
	engine.ObserveAlpenglowCandidateBlock(alpenglow.ReplayBlockObservation{
		Block: block, ParentSlot: 11, ParentHash: solana.Hash{11},
	})
	engine.ObserveAlpenglowCandidateBlock(alpenglow.ReplayBlockObservation{
		Block: block, ParentSlot: 11, ParentHash: solana.Hash{10},
	})

	if err := engine.safetyError(); err == nil {
		t.Fatal("conflicting parent linkage did not latch engine safety fault")
	}
	decision, ok := engine.NextAlpenglowDecision(11)
	if !ok || decision.Kind != alpenglow.ChainDecisionKindConflict {
		t.Fatalf("safety-fault decision = %+v (ok=%v)", decision, ok)
	}
}

func TestAlpenglowObserverCandidateBlockEnablesIndirectSkipDecision(t *testing.T) {
	engine, err := NewEngine(ModeAlpenglowObserver)
	if err != nil {
		t.Fatalf("NewEngine returned error: %v", err)
	}
	observer := engine.(*AlpenglowObserverEngine)
	if err := observer.SetAlpenglowValidatorSet(testAlpenglowValidatorSet()); err != nil {
		t.Fatalf("SetAlpenglowValidatorSet returned error: %v", err)
	}

	var blockID solana.Hash
	blockID[0] = 15
	cert := alpenglow.Certificate{
		Type:      alpenglow.CertificateFinalizeFast,
		Slot:      15,
		BlockHash: blockID,
		Bitmap:    testAlpenglowSignerBitmap(),
	}
	cert.Signature = testAlpenglowCertificateSignature(t, cert)
	observer.observeVotorMessage(alpenglow.NewCertificateMessage(cert))
	observer.ObserveAlpenglowCandidateBlock(alpenglow.ReplayBlockObservation{
		Block:      alpenglow.BlockID{Slot: 15, Hash: blockID},
		ParentSlot: 12,
	})

	decision, ok := observer.NextAlpenglowDecision(12)
	if !ok {
		t.Fatalf("expected indirect skip decision")
	}
	if decision.Kind != alpenglow.ChainDecisionKindSkip || decision.Slot != 13 || !decision.Indirect {
		t.Fatalf("unexpected indirect skip decision: %+v", decision)
	}
}

func TestAlpenglowObserverPublishesCertificateBlockIDsOnly(t *testing.T) {
	observer := &AlpenglowObserverEngine{recentBlockIDs: make(map[uint64]solana.Hash)}

	var published []alpenglow.BlockID
	observer.SetAlpenglowBlockIDSink(func(slot uint64, blockID solana.Hash) {
		published = append(published, alpenglow.BlockID{Slot: slot, Hash: blockID})
	})

	var voteBlockID solana.Hash
	voteBlockID[0] = 1
	observer.observeVotorBlockID(alpenglow.NewVoteMessage(alpenglow.NewNotarizationVote(10, voteBlockID), []byte{1}, 0))
	if len(published) != 0 {
		t.Fatalf("vote block ID was published as a certified hint: %+v", published)
	}
	var fallbackBlockID solana.Hash
	fallbackBlockID[0] = 3
	fallback := alpenglow.Certificate{
		Type: alpenglow.CertificateNotarizeFallback, Slot: 9, BlockHash: fallbackBlockID,
		SignatureVerified: true, StakeVerified: true,
	}
	if _, err := observer.ensureChain().ObserveCertificate(fallback); err != nil {
		t.Fatal(err)
	}
	observer.observeVotorBlockID(alpenglow.NewCertificateMessage(fallback))
	if len(published) != 0 {
		t.Fatalf("fallback-only block ID became an assembler filter: %+v", published)
	}

	var certBlockID solana.Hash
	certBlockID[0] = 2
	cert := alpenglow.Certificate{
		Type:              alpenglow.CertificateNotarize,
		Slot:              10,
		BlockHash:         certBlockID,
		SignatureVerified: true,
		StakeVerified:     true,
	}
	if _, err := observer.ensureChain().ObserveCertificate(cert); err != nil {
		t.Fatal(err)
	}
	observer.observeVotorBlockID(alpenglow.NewCertificateMessage(cert))
	if len(published) != 1 || published[0] != (alpenglow.BlockID{Slot: 10, Hash: certBlockID}) {
		t.Fatalf("published block IDs = %+v, want certified block ID", published)
	}
}

func TestAlpenglowObserverDoesNotInferParentFromFallbackHint(t *testing.T) {
	observer := &AlpenglowObserverEngine{recentBlockIDs: make(map[uint64]solana.Hash)}
	parent := alpenglow.BlockID{Slot: 10, Hash: solana.Hash{1}}
	if _, err := observer.ensureChain().ObserveCertificate(alpenglow.Certificate{
		Type:              alpenglow.CertificateNotarizeFallback,
		Slot:              parent.Slot,
		BlockHash:         parent.Hash,
		SignatureVerified: true,
		StakeVerified:     true,
	}); err != nil {
		t.Fatalf("observe fallback parent: %v", err)
	}
	observer.rememberRecentAlpenglowBlockID(parent.Slot, parent.Hash)

	obs := alpenglow.ReplayBlockObservation{Block: alpenglow.BlockID{Slot: 11, Hash: solana.Hash{2}}, ParentSlot: parent.Slot}
	observer.enrichReplayBlockObservation(&obs)
	if obs.ParentHash != (solana.Hash{}) {
		t.Fatalf("arrival-order fallback hint was used as parent: %s", obs.ParentHash)
	}

	if _, err := observer.ensureChain().ObserveCertificate(alpenglow.Certificate{
		Type:              alpenglow.CertificateNotarize,
		Slot:              parent.Slot,
		BlockHash:         parent.Hash,
		SignatureVerified: true,
		StakeVerified:     true,
	}); err != nil {
		t.Fatalf("observe decisive parent: %v", err)
	}
	observer.enrichReplayBlockObservation(&obs)
	if obs.ParentHash != parent.Hash {
		t.Fatalf("decisive parent was not inferred: %s", obs.ParentHash)
	}
}

func TestAlpenglowObserverDeliversOnlyVerifiedVotesToConsumers(t *testing.T) {
	engine, err := NewEngine(ModeAlpenglowObserver)
	if err != nil {
		t.Fatal(err)
	}
	observer := engine.(*AlpenglowObserverEngine)
	if err := observer.SetAlpenglowValidatorSet(testAlpenglowValidatorSet()); err != nil {
		t.Fatal(err)
	}
	observer.SetAlpenglowEpochLookup(func(uint64) uint64 { return 1 })
	var delivered int
	observer.SetVotorMessageHook(func(msg alpenglow.Message) {
		if msg.Vote != nil {
			delivered++
		}
	})

	vote := alpenglow.NewSkipVote(50)
	valid := alpenglow.VoteMessage{Vote: vote, Signature: testAlpenglowVoteSignature(t, vote), Rank: 0}
	observer.observeVotorMessage(alpenglow.Message{Vote: &valid})
	invalid := valid
	invalid.Vote = alpenglow.NewSkipVote(51)
	observer.observeVotorMessage(alpenglow.Message{Vote: &invalid})

	if delivered != 1 {
		t.Fatalf("verified hook deliveries = %d, want 1", delivered)
	}
	snapshot := observer.Snapshot()
	if snapshot.Alpenglow == nil || snapshot.Alpenglow.VotesObserved != 1 {
		t.Fatalf("verified observer snapshot = %+v", snapshot.Alpenglow)
	}
	if snapshot.AlpenglowPool == nil || snapshot.AlpenglowPool.VerifiedVotes != 1 {
		t.Fatalf("consensus pool snapshot = %+v", snapshot.AlpenglowPool)
	}
}

func TestAlpenglowObserverDoesNotDeliverDuplicateVoteToConsumers(t *testing.T) {
	engine, err := NewEngine(ModeAlpenglowObserver)
	if err != nil {
		t.Fatal(err)
	}
	observer := engine.(*AlpenglowObserverEngine)
	if err := observer.SetAlpenglowValidatorSet(testAlpenglowValidatorSet()); err != nil {
		t.Fatal(err)
	}
	observer.SetAlpenglowEpochLookup(func(uint64) uint64 { return 1 })
	var delivered int
	observer.SetVotorMessageHook(func(msg alpenglow.Message) {
		if msg.Vote != nil {
			delivered++
		}
	})

	vote := alpenglow.NewSkipVote(50)
	message := alpenglow.VoteMessage{Vote: vote, Signature: testAlpenglowVoteSignature(t, vote), Rank: 0}
	observer.observeVotorMessage(alpenglow.Message{Vote: &message})
	observer.observeVotorMessage(alpenglow.Message{Vote: &message})
	if delivered != 1 {
		t.Fatalf("consumer deliveries = %d, want one accepted vote", delivered)
	}
}

func TestAlpenglowObserverEmitsParentReadyFromVerifiedCertificates(t *testing.T) {
	engine, err := NewEngine(ModeAlpenglowObserver)
	if err != nil {
		t.Fatal(err)
	}
	observer := engine.(*AlpenglowObserverEngine)
	if err := observer.SetAlpenglowValidatorSet(testAlpenglowValidatorSet()); err != nil {
		t.Fatal(err)
	}
	observer.SetAlpenglowEpochLookup(func(uint64) uint64 { return 1 })
	var events []alpenglow.ConsensusEvent
	observer.SetAlpenglowEventSink(func(event alpenglow.ConsensusEvent) { events = append(events, event) })

	var blockHash solana.Hash
	blockHash[0] = 8
	certs := []alpenglow.Certificate{
		{Type: alpenglow.CertificateNotarize, Slot: 1, BlockHash: blockHash, Bitmap: testAlpenglowSignerBitmap()},
		{Type: alpenglow.CertificateSkip, Slot: 2, Bitmap: testAlpenglowSignerBitmap()},
		{Type: alpenglow.CertificateSkip, Slot: 3, Bitmap: testAlpenglowSignerBitmap()},
	}
	for i := range certs {
		certs[i].Signature = testAlpenglowCertificateSignature(t, certs[i])
		observer.observeVotorMessage(alpenglow.NewCertificateMessage(certs[i]))
	}
	for _, event := range events {
		if event.Kind == alpenglow.ConsensusEventParentReady && event.Slot == 4 && event.Block == (alpenglow.BlockID{Slot: 1, Hash: blockHash}) {
			return
		}
	}
	t.Fatalf("missing parent-ready event: %+v", events)
}

func TestSetAlpenglowRootRestoresStartupParentReady(t *testing.T) {
	engine := &AlpenglowObserverEngine{pool: alpenglow.NewConsensusPool(alpenglow.DefaultConsensusPoolConfig())}
	root := alpenglow.BlockID{Slot: 208, Hash: solana.Hash{7}}
	engine.SetAlpenglowRoot(root)
	parent := engine.AlpenglowBlockProductionParent(209)
	if parent.Kind != alpenglow.BlockProductionParentReady || parent.Parent != root {
		t.Fatalf("startup ParentReady = %+v, want root %+v", parent, root)
	}
}

func TestChainHistoryPrunesAtDurableNotConsensusRoot(t *testing.T) {
	engine := &AlpenglowObserverEngine{}
	if _, err := engine.ensureChain().ObserveCertificate(alpenglow.Certificate{
		Type: alpenglow.CertificateSkip, Slot: 10, SignatureVerified: true, StakeVerified: true,
	}); err != nil {
		t.Fatal(err)
	}
	engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: 20, Hash: solana.Hash{2}})
	if got := engine.ensureChain().Snapshot().CertificatesObserved; got != 1 {
		t.Fatalf("consensus root pruned decision history: certificates=%d", got)
	}
	engine.SetAlpenglowDurableRoot(20)
	if got := engine.ensureChain().Snapshot().CertifiedSkips; got != 0 {
		t.Fatalf("durable root did not prune old decisions: skips=%d", got)
	}
}

func testAlpenglowValidatorSet() alpenglow.ValidatorSet {
	key := testAlpenglowBLSKey()
	var pubkey bls12381.G1Affine
	pubkey.ScalarMultiplicationBase(key)
	compressed := pubkey.Bytes()
	uncompressed := pubkey.RawBytes()
	return alpenglow.ValidatorSet{
		Epoch: 1,
		Validators: []alpenglow.ValidatorStake{
			{
				Rank:                  0,
				Stake:                 100,
				BlsPubkeyCompressed:   compressed,
				BlsPubkeyUncompressed: uncompressed,
			},
		},
		TotalStake: 100,
	}
}

func testAlpenglowSignerBitmap() []byte {
	return []byte{0, 1, 0, 1}
}

func testAlpenglowBLSKey() *big.Int {
	return big.NewInt(7)
}

func testAlpenglowCertificateSignature(t *testing.T, cert alpenglow.Certificate) []byte {
	t.Helper()
	vote := testAlpenglowCertificateVote(t, cert)
	// Match CertificateVerifier's default shred version (0) and signed payload layout.
	payload, err := alpenglow.EncodeVotePayloadToSign(vote, 0)
	if err != nil {
		t.Fatalf("encode certificate vote: %v", err)
	}
	message, err := bls12381.HashToG2(payload, []byte("BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_"))
	if err != nil {
		t.Fatalf("hash certificate vote: %v", err)
	}
	var signature bls12381.G2Affine
	signature.ScalarMultiplication(&message, testAlpenglowBLSKey())
	raw := signature.RawBytes()
	return raw[:]
}

func testAlpenglowVoteSignature(t *testing.T, vote alpenglow.Vote) []byte {
	t.Helper()
	payload, err := alpenglow.EncodeVotePayloadToSign(vote, 0)
	if err != nil {
		t.Fatal(err)
	}
	message, err := bls12381.HashToG2(payload, []byte("BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_"))
	if err != nil {
		t.Fatal(err)
	}
	var signature bls12381.G2Affine
	signature.ScalarMultiplication(&message, testAlpenglowBLSKey())
	raw := signature.RawBytes()
	return raw[:]
}

func testAlpenglowCertificateVote(t *testing.T, cert alpenglow.Certificate) alpenglow.Vote {
	t.Helper()
	switch cert.Type {
	case alpenglow.CertificateFinalizeFast, alpenglow.CertificateNotarize:
		return alpenglow.NewNotarizationVote(cert.Slot, cert.BlockHash)
	case alpenglow.CertificateFinalize:
		return alpenglow.NewFinalizationVote(cert.Slot)
	case alpenglow.CertificateSkip:
		return alpenglow.NewSkipVote(cert.Slot)
	case alpenglow.CertificateGenesis:
		return alpenglow.NewGenesisVote(cert.Slot, cert.BlockHash)
	case alpenglow.CertificateNotarizeFallback:
		return alpenglow.NewNotarizationVote(cert.Slot, cert.BlockHash)
	default:
		t.Fatalf("unsupported certificate type %q", cert.Type)
		return alpenglow.Vote{}
	}
}
