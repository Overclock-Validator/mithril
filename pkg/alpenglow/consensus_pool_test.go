package alpenglow

import (
	"math/big"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestConsensusPoolBuildsAgaveCertificatesFromVerifiedVotes(t *testing.T) {
	set, keys := testBLSValidatorSet(100, 40, 35, 25)
	pool := NewConsensusPool(DefaultConsensusPoolConfig())
	pool.NoteLiveSlot(10)
	blockHash := parentReadyHash(7)

	update, err := pool.AddVerifiedVote(poolVote(t, set, keys, 0, NewNotarizationVote(11, blockHash), false))
	if err != nil {
		t.Fatal(err)
	}
	if len(update.Certificates) != 0 {
		t.Fatalf("40%% unexpectedly formed certificate: %+v", update.Certificates)
	}
	update, err = pool.AddVerifiedVote(poolVote(t, set, keys, 1, NewNotarizationVote(11, blockHash), false))
	if err != nil {
		t.Fatal(err)
	}
	assertCertificateTypes(t, update.Certificates, CertificateNotarize, CertificateNotarizeFallback)
	for _, cert := range update.Certificates {
		if _, _, err := verifyCertificateWithSet(set, cert, true, testBLSVerificationShredVersion); err != nil {
			t.Fatalf("locally assembled %s certificate failed verification: %v", cert.Type, err)
		}
	}
	update, err = pool.AddVerifiedVote(poolVote(t, set, keys, 2, NewNotarizationVote(11, blockHash), false))
	if err != nil {
		t.Fatal(err)
	}
	assertCertificateTypes(t, update.Certificates, CertificateFinalizeFast)
	if len(update.Events) == 0 || !hasConsensusEvent(update.Events, ConsensusEventFinalized, 11, blockHash) {
		t.Fatalf("missing fast-finalized event: %+v", update.Events)
	}
}

func TestConsensusPoolSlowFinalizationNeedsNotarizeAndFinalize(t *testing.T) {
	pool := NewConsensusPool(DefaultConsensusPoolConfig())
	hash := parentReadyHash(5)
	finalize := Certificate{Type: CertificateFinalize, Slot: 20, StakeVerified: true, SignatureVerified: true}
	update, err := pool.AddVerifiedCertificate(finalize)
	if err != nil {
		t.Fatal(err)
	}
	if hasConsensusEvent(update.Events, ConsensusEventFinalized, 20, hash) {
		t.Fatal("finalize certificate alone selected a block")
	}
	notarize := Certificate{Type: CertificateNotarize, Slot: 20, BlockHash: hash, StakeVerified: true, SignatureVerified: true}
	update, err = pool.AddVerifiedCertificate(notarize)
	if err != nil {
		t.Fatal(err)
	}
	if !hasConsensusEvent(update.Events, ConsensusEventFinalized, 20, hash) {
		t.Fatalf("missing slow-finalized event: %+v", update.Events)
	}
}

func TestConsensusPoolRejectsVerifiedConflictingVotes(t *testing.T) {
	set, keys := testBLSValidatorSet(100, 100)
	pool := NewConsensusPool(DefaultConsensusPoolConfig())
	pool.NoteLiveSlot(30)
	if _, err := pool.AddVerifiedVote(poolVote(t, set, keys, 0, NewSkipVote(31), false)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.AddVerifiedVote(poolVote(t, set, keys, 0, NewNotarizationVote(31, parentReadyHash(1)), false)); err != nil {
		t.Fatal(err)
	}
	if len(pool.Evidence()) != 1 {
		t.Fatalf("evidence = %+v", pool.Evidence())
	}
	if got := pool.Snapshot().ConflictingVotes; got != 1 {
		t.Fatalf("conflicting votes = %d, want 1", got)
	}
}

func TestConsensusPoolSafeEventsRequireLocalVote(t *testing.T) {
	set, keys := testBLSValidatorSet(100, 10, 40, 50)
	pool := NewConsensusPool(DefaultConsensusPoolConfig())
	pool.NoteLiveSlot(40)
	target := parentReadyHash(8)
	if _, err := pool.AddVerifiedVote(poolVote(t, set, keys, 0, NewSkipVote(41), true)); err != nil {
		t.Fatal(err)
	}
	update, err := pool.AddVerifiedVote(poolVote(t, set, keys, 1, NewNotarizationVote(41, target), false))
	if err != nil {
		t.Fatal(err)
	}
	if !hasConsensusEvent(update.Events, ConsensusEventSafeToNotar, 41, target) {
		t.Fatalf("missing safe-to-notar event: %+v", update.Events)
	}
}

func TestEncodeSignerStoreBitmapRoundTrip(t *testing.T) {
	bitmap := SignerBitmap{
		Encoding: SignerBitmapBase3,
		Length:   7,
		Base:     []bool{true, false, false, true, false, false, false},
		Fallback: []bool{false, true, false, false, false, true, false},
	}
	encoded, err := EncodeSignerStoreBitmap(bitmap)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSignerStoreBitmap(encoded, 7)
	if err != nil {
		t.Fatal(err)
	}
	for rank := 0; rank < bitmap.Length; rank++ {
		if decoded.Base[rank] != bitmap.Base[rank] || decoded.Fallback[rank] != bitmap.Fallback[rank] {
			t.Fatalf("rank %d round trip mismatch: %+v", rank, decoded)
		}
	}
}

func poolVote(t *testing.T, set ValidatorSet, keys []*big.Int, rank uint16, vote Vote, local bool) VerifiedVote {
	t.Helper()
	message := VoteMessage{
		Vote:      vote,
		Signature: testBLSSignature(t, []testBLSVoteSignature{{Vote: vote, Key: keys[int(rank)]}}),
		Rank:      rank,
	}
	result, err := verifyVoteMessageWithSet(set, message, testBLSVerificationShredVersion)
	if err != nil {
		t.Fatalf("verify test vote: %v", err)
	}
	return VerifiedVote{Message: message, Result: result, Local: local}
}

func assertCertificateTypes(t *testing.T, certs []Certificate, want ...CertificateType) {
	t.Helper()
	got := make(map[CertificateType]bool)
	for _, cert := range certs {
		got[cert.Type] = true
	}
	if len(got) != len(want) {
		t.Fatalf("certificate types = %v, want %v", got, want)
	}
	for _, certType := range want {
		if !got[certType] {
			t.Fatalf("missing certificate type %s in %v", certType, got)
		}
	}
}

func hasConsensusEvent(events []ConsensusEvent, kind ConsensusEventKind, slot uint64, hash solana.Hash) bool {
	for _, event := range events {
		if event.Kind == kind && event.Slot == slot && (hash == (solana.Hash{}) || event.Block.Hash == hash) {
			return true
		}
	}
	return false
}
