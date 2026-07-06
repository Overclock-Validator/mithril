package consensus

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

func TestAlpenglowObserverTracksReplayInSnapshot(t *testing.T) {
	engine, err := NewEngine(Config{})
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

// A certificate that arrives before its epoch's validator set is installed must be
// deferred and replayed once the stakes land — otherwise the cert is lost and that
// slot's decision stalls.
func TestAlpenglowDefersCertUntilStakesInstall(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatalf("NewEngine returned error: %v", err)
	}
	observer := engine
	set := testAlpenglowValidatorSet()
	observer.SetAlpenglowEpochLookup(func(slot uint64) uint64 { return set.Epoch })

	cert := alpenglow.Certificate{
		Type:   alpenglow.CertificateSkip,
		Slot:   42,
		Bitmap: testAlpenglowSignerBitmap(),
	}
	cert.Signature = testAlpenglowCertificateSignature(t, cert)

	// Arrives before stakes install → deferred, no decision yet.
	observer.observeVotorMessage(alpenglow.NewCertificateMessage(cert))
	if _, ok := observer.NextAlpenglowDecision(41); ok {
		t.Fatal("cert should be deferred until stakes install, not decided")
	}

	// Stakes install → the deferred cert replays and applies.
	if err := observer.SetAlpenglowValidatorSet(set); err != nil {
		t.Fatalf("SetAlpenglowValidatorSet returned error: %v", err)
	}
	decision, ok := observer.NextAlpenglowDecision(41)
	if !ok || decision.Kind != alpenglow.ChainDecisionKindSkip || decision.Slot != 42 {
		t.Fatalf("deferred cert not applied after stakes install: decision=%+v ok=%v", decision, ok)
	}
}

// Deferred certs carry network-controlled slots, so the pending buffer must bound the
// number of distinct epoch buckets (an attacker could otherwise feed far-future slots).
func TestAlpenglowPendingCertEpochCap(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	observer := engine
	observer.SetAlpenglowEpochLookup(func(slot uint64) uint64 { return slot }) // slot == epoch

	noSet := fmt.Errorf("alpenglow verifier: no validator set for epoch")
	for epoch := uint64(1); epoch <= uint64(alpenglowPendingEpochCap)+2; epoch++ {
		observer.deferCertIfStakesMissing(alpenglow.Certificate{Slot: epoch}, noSet)
	}

	observer.pendingCertsMu.Lock()
	n := len(observer.pendingCerts)
	_, lowestKept := observer.pendingCerts[1]
	observer.pendingCertsMu.Unlock()
	if n > alpenglowPendingEpochCap {
		t.Fatalf("pending epoch buckets = %d, want <= %d", n, alpenglowPendingEpochCap)
	}
	if !lowestKept {
		t.Fatal("genuine near epoch (1) must survive; far-future garbage epochs are evicted")
	}
}

// ObserveFooterCertificates must return the blocks the verified certs finalize:
// the FinalizeFast block on the fast path, the notarized block paired with the
// Finalize cert on the slow path — the replay loop captures these for the
// promotion gate.
func TestObserveFooterCertificatesReturnsFinalizedBlocks(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	observer := engine
	if err := observer.SetAlpenglowValidatorSet(testAlpenglowValidatorSet()); err != nil {
		t.Fatalf("SetAlpenglowValidatorSet: %v", err)
	}

	fastHash := solana.Hash{0xFA}
	fast := alpenglow.Certificate{
		Type: alpenglow.CertificateFinalizeFast, Slot: 42, BlockHash: fastHash,
		Bitmap: testAlpenglowSignerBitmap(),
	}
	fast.Signature = testAlpenglowCertificateSignature(t, fast)
	finalized := observer.ObserveFooterCertificates([]alpenglow.Certificate{fast})
	if len(finalized) != 1 || finalized[0] != (alpenglow.BlockID{Slot: 42, Hash: fastHash}) {
		t.Fatalf("fast path: want [{42 %s}], got %+v", fastHash, finalized)
	}

	slowHash := solana.Hash{0x51}
	notar := alpenglow.Certificate{
		Type: alpenglow.CertificateNotarize, Slot: 43, BlockHash: slowHash,
		Bitmap: testAlpenglowSignerBitmap(),
	}
	notar.Signature = testAlpenglowCertificateSignature(t, notar)
	fin := alpenglow.Certificate{
		Type: alpenglow.CertificateFinalize, Slot: 43,
		Bitmap: testAlpenglowSignerBitmap(),
	}
	fin.Signature = testAlpenglowCertificateSignature(t, fin)
	finalized = observer.ObserveFooterCertificates([]alpenglow.Certificate{notar, fin})
	if len(finalized) != 1 || finalized[0] != (alpenglow.BlockID{Slot: 43, Hash: slowHash}) {
		t.Fatalf("slow path: want [{43 %s}], got %+v", slowHash, finalized)
	}
}

// Once a validator set is installed, deferral only accepts epochs near it — a cert
// with a far-off epoch can never verify soon and must not occupy buckets.
func TestAlpenglowDeferRejectsFarOffEpochs(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	observer := engine
	if err := observer.SetAlpenglowValidatorSet(testAlpenglowValidatorSet()); err != nil { // epoch 1
		t.Fatalf("SetAlpenglowValidatorSet: %v", err)
	}
	observer.SetAlpenglowEpochLookup(func(slot uint64) uint64 { return slot }) // slot == epoch

	noSet := fmt.Errorf("alpenglow verifier: no validator set for epoch")
	observer.deferCertIfStakesMissing(alpenglow.Certificate{Slot: 500}, noSet) // far future
	observer.deferCertIfStakesMissing(alpenglow.Certificate{Slot: 2}, noSet)   // latest+1: in window

	observer.pendingCertsMu.Lock()
	defer observer.pendingCertsMu.Unlock()
	if _, ok := observer.pendingCerts[500]; ok {
		t.Fatal("far-future epoch must not be deferred")
	}
	if _, ok := observer.pendingCerts[2]; !ok {
		t.Fatal("near epoch must be deferred")
	}
}

func TestAlpenglowObserverFeedsCertifiedDecisionResolver(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatalf("NewEngine returned error: %v", err)
	}
	observer := engine
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

func TestAlpenglowObserverCandidateBlockEnablesIndirectSkipDecision(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatalf("NewEngine returned error: %v", err)
	}
	observer := engine
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

	var certBlockID solana.Hash
	certBlockID[0] = 2
	observer.observeVotorBlockID(alpenglow.NewCertificateMessage(alpenglow.Certificate{
		Type:      alpenglow.CertificateNotarize,
		Slot:      10,
		BlockHash: certBlockID,
	}))
	if len(published) != 1 || published[0] != (alpenglow.BlockID{Slot: 10, Hash: certBlockID}) {
		t.Fatalf("published block IDs = %+v, want certified block ID", published)
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
	payload, err := alpenglow.EncodeVote(vote)
	if err != nil {
		t.Fatalf("encode certificate vote: %v", err)
	}
	message, err := bls12381.HashToG2(payload, []byte(alpenglow.DefaultHashToPointDST))
	if err != nil {
		t.Fatalf("hash certificate vote: %v", err)
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

// The Alpenglow-only build has exactly one engine: NewEngine must always
// yield the observer engine regardless of configuration.
func TestNewEngineYieldsObserver(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatalf("NewEngine returned error: %v", err)
	}
	if engine == nil {
		t.Fatal("NewEngine returned nil engine")
	}
	if got := engine.Name(); got != "alpenglow-observer" {
		t.Fatalf("engine.Name()=%q, want %q", got, "alpenglow-observer")
	}
}
