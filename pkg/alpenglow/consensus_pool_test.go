package alpenglow

import (
	"math/big"
	"reflect"
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

func TestConsensusPoolSlowFinalizationFailsClosedOnTwinNotarizeCertificates(t *testing.T) {
	pool := NewConsensusPool(DefaultConsensusPoolConfig())
	for _, hash := range []solana.Hash{parentReadyHash(5), parentReadyHash(6)} {
		_, err := pool.AddVerifiedCertificate(Certificate{
			Type: CertificateNotarize, Slot: 20, BlockHash: hash,
			StakeVerified: true, SignatureVerified: true,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	update, err := pool.AddVerifiedCertificate(Certificate{
		Type: CertificateFinalize, Slot: 20,
		StakeVerified: true, SignatureVerified: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasConsensusEvent(update.Events, ConsensusEventConflict, 20, solana.Hash{}) {
		t.Fatalf("missing conflict event: %+v", update.Events)
	}
	if got := pool.Snapshot().FinalizedBlocks; got != 0 {
		t.Fatalf("finalized blocks = %d, want 0", got)
	}
	if got := pool.Snapshot().ConflictingSlots; got != 1 {
		t.Fatalf("conflicting slots = %d, want 1", got)
	}
}

func TestConsensusPoolDetectsLateTwinAfterSlowFinalization(t *testing.T) {
	pool := NewConsensusPool(DefaultConsensusPoolConfig())
	first := Certificate{
		Type: CertificateNotarize, Slot: 20, BlockHash: parentReadyHash(5),
		StakeVerified: true, SignatureVerified: true,
	}
	finalize := Certificate{Type: CertificateFinalize, Slot: 20, StakeVerified: true, SignatureVerified: true}
	if _, err := pool.AddVerifiedCertificate(finalize); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.AddVerifiedCertificate(first); err != nil {
		t.Fatal(err)
	}
	update, err := pool.AddVerifiedCertificate(Certificate{
		Type: CertificateNotarize, Slot: 20, BlockHash: parentReadyHash(6),
		StakeVerified: true, SignatureVerified: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasConsensusEvent(update.Events, ConsensusEventConflict, 20, solana.Hash{}) {
		t.Fatalf("late twin did not produce conflict: %+v", update.Events)
	}
	if got := pool.Snapshot().FinalizedBlocks; got != 0 {
		t.Fatalf("late twin left %d finalized blocks", got)
	}
}

func TestConsensusPoolRejectedFallbackCertificateIsNotRetained(t *testing.T) {
	pool := NewConsensusPool(DefaultConsensusPoolConfig())
	for i := byte(1); i <= maxNotarFallbackBlocks; i++ {
		update, err := pool.AddVerifiedCertificate(Certificate{
			Type: CertificateNotarizeFallback, Slot: 20, BlockHash: parentReadyHash(i),
			StakeVerified: true, SignatureVerified: true,
		})
		if err != nil {
			t.Fatalf("insert fallback %d: %v", i, err)
		}
		if len(update.Certificates) != 1 {
			t.Fatalf("fallback %d accepted certificates = %d", i, len(update.Certificates))
		}
	}
	_, err := pool.AddVerifiedCertificate(Certificate{
		Type: CertificateNotarizeFallback, Slot: 20, BlockHash: parentReadyHash(99),
		StakeVerified: true, SignatureVerified: true,
	})
	if err == nil {
		t.Fatal("eighth fallback certificate was accepted")
	}
	if got := pool.Snapshot().Certificates; got != maxNotarFallbackBlocks {
		t.Fatalf("retained certificates = %d, want %d", got, maxNotarFallbackBlocks)
	}
}

func TestConsensusPoolDoesNotRegrowPrunedCertificates(t *testing.T) {
	pool := NewConsensusPool(DefaultConsensusPoolConfig())
	pool.SetRoot(BlockID{Slot: 20, Hash: parentReadyHash(20)})
	update, err := pool.AddVerifiedCertificate(Certificate{
		Type: CertificateSkip, Slot: 10, StakeVerified: true, SignatureVerified: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(update.Events) != 0 || pool.Snapshot().Certificates != 0 {
		t.Fatalf("pruned certificate regrew pool state: update=%+v snapshot=%+v", update, pool.Snapshot())
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

func TestConflictingVoteTypesMatchAgave(t *testing.T) {
	tests := map[VoteType][]VoteType{
		VoteTypeFinalize:         {VoteTypeNotarizeFallback, VoteTypeSkip, VoteTypeSkipFallback, VoteTypeGenesis},
		VoteTypeNotarize:         {VoteTypeSkip, VoteTypeNotarizeFallback, VoteTypeGenesis},
		VoteTypeNotarizeFallback: {VoteTypeFinalize, VoteTypeNotarize, VoteTypeGenesis},
		VoteTypeSkip:             {VoteTypeFinalize, VoteTypeNotarize, VoteTypeSkipFallback, VoteTypeGenesis},
		VoteTypeSkipFallback:     {VoteTypeSkip, VoteTypeFinalize, VoteTypeGenesis},
		VoteTypeGenesis:          {VoteTypeFinalize, VoteTypeNotarize, VoteTypeNotarizeFallback, VoteTypeSkip, VoteTypeSkipFallback},
	}
	for voteType, want := range tests {
		if got := conflictingVoteTypes(voteType); !reflect.DeepEqual(got, want) {
			t.Fatalf("conflictingVoteTypes(%s) = %v, want %v", voteType, got, want)
		}
	}
}

func TestConsensusPoolSafeEventsRequireLocalVote(t *testing.T) {
	set, keys := testBLSValidatorSet(100, 10, 40, 50)
	pool := NewConsensusPool(DefaultConsensusPoolConfig())
	pool.NoteLiveSlot(39)
	target := parentReadyHash(8)
	if _, err := pool.AddVerifiedVote(poolVote(t, set, keys, 0, NewSkipVote(40), true)); err != nil {
		t.Fatal(err)
	}
	update, err := pool.AddVerifiedVote(poolVote(t, set, keys, 1, NewNotarizationVote(40, target), false))
	if err != nil {
		t.Fatal(err)
	}
	if !hasConsensusEvent(update.Events, ConsensusEventSafeToNotar, 40, target) {
		t.Fatalf("missing safe-to-notar event: %+v", update.Events)
	}
}

func TestConsensusPoolDefersIntrawindowSafeToNotar(t *testing.T) {
	set, keys := testBLSValidatorSet(100, 10, 40, 50)
	pool := NewConsensusPool(DefaultConsensusPoolConfig())
	pool.NoteLiveSlot(40)
	target := parentReadyHash(9)
	if _, err := pool.AddVerifiedVote(poolVote(t, set, keys, 0, NewSkipVote(41), true)); err != nil {
		t.Fatal(err)
	}
	update, err := pool.AddVerifiedVote(poolVote(t, set, keys, 1, NewNotarizationVote(41, target), false))
	if err != nil {
		t.Fatal(err)
	}
	if hasConsensusEvent(update.Events, ConsensusEventSafeToNotar, 41, target) {
		t.Fatalf("intrawindow SafeToNotar emitted before parent checks: %+v", update.Events)
	}
	pending := pool.TakePendingSafeToNotar()
	if len(pending) != 1 || pending[0] != (BlockID{Slot: 41, Hash: target}) {
		t.Fatalf("pending SafeToNotar = %+v", pending)
	}
	if pending = pool.TakePendingSafeToNotar(); len(pending) != 0 {
		t.Fatalf("pending queue was not drained: %+v", pending)
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

func TestSignerStoreBitmapAcceptsAgaveWireCapacity(t *testing.T) {
	base := make([]bool, CertificateBitmapCapacity)
	base[0] = true
	encoded, err := EncodeSignerStoreBitmap(SignerBitmap{
		Encoding: SignerBitmapBase2,
		Length:   CertificateBitmapCapacity,
		Base:     base,
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeSignerStoreBitmap(encoded, CertificateBitmapCapacity)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Length != CertificateBitmapCapacity || !decoded.Base[0] {
		t.Fatalf("decoded bitmap = length %d first=%v", decoded.Length, decoded.Base[0])
	}
	if _, err := DecodeSignerStoreBitmap(encoded, MaximumVATValidators); err == nil {
		t.Fatal("wire bitmap unexpectedly fit the smaller VAT active-set cap")
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
