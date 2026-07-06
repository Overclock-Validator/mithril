package alpenglow

import (
	"math/big"
	"testing"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/gagliardetto/solana-go"
)

// signTestVote produces one rank's 192-byte uncompressed BLS signature.
func signTestVote(t *testing.T, vote Vote, key *big.Int) []byte {
	t.Helper()
	payload, err := EncodeVote(vote)
	if err != nil {
		t.Fatalf("encode vote: %v", err)
	}
	message, err := bls12381.HashToG2(payload, []byte(blsHashToPointDST))
	if err != nil {
		t.Fatalf("hash to g2: %v", err)
	}
	var signed bls12381.G2Affine
	signed.ScalarMultiplication(&message, key)
	raw := signed.RawBytes()
	return raw[:]
}

// newTestPool builds a pool over a 5-validator set (stakes 40/30/15/10/5 of
// 100) with an emit-capture callback.
func newTestPool(t *testing.T) (*CertPool, ValidatorSet, []*big.Int, *[]Certificate) {
	t.Helper()
	set, keys := testBLSValidatorSet(100, 40, 30, 15, 10, 5)
	verifier := NewCertificateVerifier()
	if err := verifier.SetValidatorSet(set); err != nil {
		t.Fatalf("set validator set: %v", err)
	}
	var emitted []Certificate
	pool := NewCertPool(DefaultCertPoolConfig(), verifier, func(c Certificate) { emitted = append(emitted, c) })
	pool.SetEpochLookup(func(uint64) uint64 { return set.Epoch })
	// Open the trusted vote window past every slot the tests use (the window is
	// anchored to replay-observed progress, never to raw vote slots).
	pool.NoteLiveSlot(2000)
	return pool, set, keys, &emitted
}

func addVote(t *testing.T, pool *CertPool, vote Vote, rank uint16, key *big.Int) {
	t.Helper()
	pool.AddVote(VoteMessage{Vote: vote, Rank: rank, Signature: signTestVote(t, vote, key)})
}

// 60% of notarize stake assembles a notarize certificate that passes the full
// production verifier — the strongest pool invariant.
func TestCertPoolAssemblesNotarizeCert(t *testing.T) {
	pool, set, keys, emitted := newTestPool(t)
	var blockHash solana.Hash
	blockHash[0] = 0xAB
	vote := NewNotarizationVote(500, blockHash)

	addVote(t, pool, vote, 0, keys[0]) // 40% — below 60%
	if len(*emitted) != 0 {
		t.Fatalf("no cert expected at 40%%, got %d", len(*emitted))
	}
	addVote(t, pool, vote, 1, keys[1]) // 70% — notarize crosses

	var notarize *Certificate
	for i := range *emitted {
		if (*emitted)[i].Type == CertificateNotarize {
			notarize = &(*emitted)[i]
		}
	}
	if notarize == nil {
		t.Fatalf("expected a notarize certificate, emitted: %+v", *emitted)
	}
	if notarize.Slot != 500 || notarize.BlockHash != blockHash {
		t.Fatalf("wrong cert identity: %+v", notarize)
	}
	if _, _, err := verifyCertificateWithSet(set, *notarize, true); err != nil {
		t.Fatalf("pool-assembled cert failed the production verifier: %v", err)
	}
}

// 80% of notarize stake additionally assembles a finalize-fast certificate.
func TestCertPoolAssemblesFinalizeFastAtEightyPercent(t *testing.T) {
	pool, set, keys, emitted := newTestPool(t)
	var blockHash solana.Hash
	blockHash[0] = 0xCD
	vote := NewNotarizationVote(600, blockHash)

	addVote(t, pool, vote, 0, keys[0]) // 40
	addVote(t, pool, vote, 1, keys[1]) // 70 -> notarize emits
	hasFast := func() bool {
		for _, c := range *emitted {
			if c.Type == CertificateFinalizeFast {
				return true
			}
		}
		return false
	}
	if hasFast() {
		t.Fatal("finalize-fast must not emit below 80%")
	}
	addVote(t, pool, vote, 2, keys[2]) // 85 -> fast crosses
	if !hasFast() {
		t.Fatalf("expected finalize-fast at 85%%, emitted: %+v", *emitted)
	}
	for _, c := range *emitted {
		if _, _, err := verifyCertificateWithSet(set, c, true); err != nil {
			t.Fatalf("%s cert failed verification: %v", c.Type, err)
		}
	}
}

// A contested slot can produce ONLY fallback votes for a block (everyone who
// cast plain notarize voted for a sibling). Agave assembles the NotarizeFallback
// cert from fallback votes alone — base3 with an empty base bitmap — and so
// must the pool.
func TestCertPoolAssemblesFallbackOnlyNotarizeFallbackCert(t *testing.T) {
	pool, set, keys, emitted := newTestPool(t)
	var blockHash solana.Hash
	blockHash[0] = 0x44
	fb := NewNotarizationFallbackVote(650, blockHash)

	addVote(t, pool, fb, 0, keys[0]) // 40% — below 60
	if len(*emitted) != 0 {
		t.Fatalf("no cert at 40%%, got %+v", *emitted)
	}
	addVote(t, pool, fb, 1, keys[1]) // 70% — crosses with ZERO plain notarize votes

	var cert *Certificate
	for i := range *emitted {
		if (*emitted)[i].Type == CertificateNotarizeFallback {
			cert = &(*emitted)[i]
		}
	}
	if cert == nil {
		t.Fatalf("expected a fallback-only notarize-fallback certificate, emitted: %+v", *emitted)
	}
	bitmap, err := DecodeSignerStoreBitmap(cert.Bitmap, len(set.Validators))
	if err != nil {
		t.Fatalf("decode bitmap: %v", err)
	}
	if bitmap.Encoding != SignerBitmapBase3 {
		t.Fatalf("fallback-only cert must use base3 (empty base group), got %s", bitmap.Encoding)
	}
	for i, b := range bitmap.Base {
		if b {
			t.Fatalf("base group must be empty for a fallback-only cert (rank %d set)", i)
		}
	}
	if _, _, err := verifyCertificateWithSet(set, *cert, true); err != nil {
		t.Fatalf("fallback-only cert failed the production verifier: %v", err)
	}
}

// Same for skip: skip-fallback votes alone assemble the skip certificate.
func TestCertPoolAssemblesFallbackOnlySkipCert(t *testing.T) {
	pool, set, keys, emitted := newTestPool(t)
	skipFB := NewSkipFallbackVote(660)

	addVote(t, pool, skipFB, 0, keys[0]) // 40
	addVote(t, pool, skipFB, 1, keys[1]) // 70 — crosses with ZERO plain skip votes

	var cert *Certificate
	for i := range *emitted {
		if (*emitted)[i].Type == CertificateSkip {
			cert = &(*emitted)[i]
		}
	}
	if cert == nil {
		t.Fatalf("expected a fallback-only skip certificate, emitted: %+v", *emitted)
	}
	if _, _, err := verifyCertificateWithSet(set, *cert, true); err != nil {
		t.Fatalf("fallback-only skip cert failed the production verifier: %v", err)
	}
}

// Skip + skip-fallback assemble a base3 union certificate.
func TestCertPoolAssemblesBase3SkipCert(t *testing.T) {
	pool, set, keys, emitted := newTestPool(t)
	skip := NewSkipVote(700)
	skipFB := NewSkipFallbackVote(700)

	addVote(t, pool, skip, 0, keys[0])   // 40 base
	addVote(t, pool, skipFB, 1, keys[1]) // +30 fallback = 70 union

	var cert *Certificate
	for i := range *emitted {
		if (*emitted)[i].Type == CertificateSkip {
			cert = &(*emitted)[i]
		}
	}
	if cert == nil {
		t.Fatalf("expected a skip certificate, emitted: %+v", *emitted)
	}
	bitmap, err := DecodeSignerStoreBitmap(cert.Bitmap, len(set.Validators))
	if err != nil {
		t.Fatalf("decode bitmap: %v", err)
	}
	if bitmap.Encoding != SignerBitmapBase3 {
		t.Fatalf("expected base3 bitmap, got %s", bitmap.Encoding)
	}
	if err := bitmap.CheckDisjoint(); err != nil {
		t.Fatalf("bitmap not disjoint: %v", err)
	}
	if _, _, err := verifyCertificateWithSet(set, *cert, true); err != nil {
		t.Fatalf("pool-assembled base3 cert failed verification: %v", err)
	}
}

// A corrupted signature in the batch is bisected out; the cert still emits
// once enough honest stake arrives.
func TestCertPoolBisectsBadSignature(t *testing.T) {
	pool, set, keys, emitted := newTestPool(t)
	var blockHash solana.Hash
	blockHash[0] = 0xEE
	vote := NewNotarizationVote(800, blockHash)

	// Rank 2 (15%) signs the WRONG vote (valid point, wrong payload).
	wrong := NewNotarizationVote(801, blockHash)
	pool.AddVote(VoteMessage{Vote: vote, Rank: 2, Signature: signTestVote(t, wrong, keys[2])})
	addVote(t, pool, vote, 0, keys[0]) // 40 good
	addVote(t, pool, vote, 3, keys[3]) // +10 good; candidate 65 crosses, verified only 50

	if len(*emitted) != 0 {
		t.Fatalf("cert must not emit on 50%% verified stake, got %+v", *emitted)
	}
	if pool.Snapshot().BadSignatures == 0 {
		t.Fatal("bad signature must be detected and counted")
	}

	addVote(t, pool, vote, 1, keys[1]) // +30 good -> verified 80
	var notarize *Certificate
	for i := range *emitted {
		if (*emitted)[i].Type == CertificateNotarize {
			notarize = &(*emitted)[i]
		}
	}
	if notarize == nil {
		t.Fatalf("expected notarize cert after honest quorum, emitted: %+v", *emitted)
	}
	if _, _, err := verifyCertificateWithSet(set, *notarize, true); err != nil {
		t.Fatalf("cert with bisected-out bad vote failed verification: %v", err)
	}
	// The bad rank must not be in the bitmap.
	bitmap, err := DecodeSignerStoreBitmap(notarize.Bitmap, len(set.Validators))
	if err != nil {
		t.Fatalf("decode bitmap: %v", err)
	}
	if bitmap.Base[2] {
		t.Fatal("rank with bad signature must be excluded from the certificate")
	}
}

// Conflicting same-type votes are equivocation evidence recorded at VERIFIED
// time (both signatures check out), and the second block is not counted. The
// evidence is detected once both blocks' tallies fold — the security-relevant
// case where an equivocator's stake actually matters.
func TestCertPoolEquivocationEvidence(t *testing.T) {
	pool, _, keys, _ := newTestPool(t)
	var h1, h2 solana.Hash
	h1[0], h2[0] = 1, 2

	// Rank 0 (40%) validly signs BOTH blocks. Both tallies must fold for the
	// equivocation to be observed: h1 via ranks 0+1 (70%), h2 via ranks 0+2+3.
	addVote(t, pool, NewNotarizationVote(900, h1), 0, keys[0]) // h1: 40 pending
	addVote(t, pool, NewNotarizationVote(900, h2), 0, keys[0]) // h2: 40 pending
	addVote(t, pool, NewNotarizationVote(900, h1), 1, keys[1]) // h1: 70 -> folds; rank0 verified for h1
	addVote(t, pool, NewNotarizationVote(900, h2), 2, keys[2]) // h2: 55 pending
	addVote(t, pool, NewNotarizationVote(900, h2), 3, keys[3]) // h2: 65 -> folds; rank0 equivocation

	ev := pool.EquivocationEvidence()
	if len(ev) != 1 {
		t.Fatalf("expected 1 evidence entry, got %d: %+v", len(ev), ev)
	}
	if ev[0].Rank != 0 || ev[0].Slot != 900 || ev[0].First != h1 || ev[0].Second != h2 {
		t.Fatalf("wrong evidence: %+v", ev[0])
	}
	if pool.Snapshot().VotesEquivocated != 1 {
		t.Fatal("equivocation counter must increment")
	}
}

// A bogus vote for a victim rank must NOT suppress that validator's real vote
// (dedupe-poisoning). The bogus vote never verifies, so it never touches the
// equivocation/dedupe ledger; the real vote still assembles its certificate.
func TestCertPoolBogusVoteDoesNotPoisonRealVote(t *testing.T) {
	pool, set, keys, emitted := newTestPool(t)
	var real, bogus solana.Hash
	real[0], bogus[0] = 0xA0, 0xB0

	// Attacker forges a vote for rank 0 on a bogus block, signed with the WRONG
	// key (rank 4's key) — a bad signature for rank 0.
	pool.AddVote(VoteMessage{Vote: NewNotarizationVote(950, bogus), Rank: 0, Signature: signTestVote(t, NewNotarizationVote(950, bogus), keys[4])})

	// Rank 0's REAL vote for the real block, then rank 1 — 70% crosses.
	addVote(t, pool, NewNotarizationVote(950, real), 0, keys[0])
	addVote(t, pool, NewNotarizationVote(950, real), 1, keys[1])

	var notarize *Certificate
	for i := range *emitted {
		if (*emitted)[i].Type == CertificateNotarize && (*emitted)[i].BlockHash == real {
			notarize = &(*emitted)[i]
		}
	}
	if notarize == nil {
		t.Fatalf("real vote must still assemble despite the bogus vote; emitted: %+v", *emitted)
	}
	if _, _, err := verifyCertificateWithSet(set, *notarize, true); err != nil {
		t.Fatalf("assembled cert failed verification: %v", err)
	}
	// No equivocation was recorded against the honest rank 0.
	if len(pool.EquivocationEvidence()) != 0 {
		t.Fatalf("bogus vote must not forge equivocation, got %+v", pool.EquivocationEvidence())
	}
	// Rank 0 is in the certificate (its real vote counted).
	bitmap, err := DecodeSignerStoreBitmap(notarize.Bitmap, len(set.Validators))
	if err != nil {
		t.Fatal(err)
	}
	if !bitmap.Base[0] {
		t.Fatal("honest rank 0's real vote must be counted")
	}
}

// DoS bounds: votes outside the slot window and past the pending cap reject.
func TestCertPoolIngestBounds(t *testing.T) {
	set, keys := testBLSValidatorSet(100, 40, 30, 15, 10, 5)
	verifier := NewCertificateVerifier()
	if err := verifier.SetValidatorSet(set); err != nil {
		t.Fatal(err)
	}
	pool := NewCertPool(CertPoolConfig{MaxSlotsAhead: 10, MaxPendingVotesPerSlot: 2, EquivocationCap: 4}, verifier, nil)
	pool.SetEpochLookup(func(uint64) uint64 { return set.Epoch })
	pool.NoteLiveSlot(100) // trusted window anchor at slot 100

	// Below-threshold votes stay PENDING (nothing folds them), so the cap is
	// observable: ranks 2+3 hold 25%% of stake, well under any threshold.
	addVote(t, pool, NewFinalizationVote(100), 2, keys[2])
	pool.AddVote(VoteMessage{Vote: NewSkipVote(500), Rank: 1, Signature: signTestVote(t, NewSkipVote(500), keys[1])}) // beyond anchor(100)+10
	if pool.Snapshot().VotesRejected == 0 {
		t.Fatal("far-future vote must reject")
	}

	// Pending cap: third distinct pending vote on the slot rejects.
	addVote(t, pool, NewSkipVote(100), 3, keys[3])
	before := pool.Snapshot().VotesRejected
	pool.AddVote(VoteMessage{Vote: NewSkipFallbackVote(100), Rank: 4, Signature: signTestVote(t, NewSkipFallbackVote(100), keys[4])})
	if pool.Snapshot().VotesRejected != before+1 {
		t.Fatal("pending cap must reject the overflow vote")
	}

	// Floor: pruned slots reject.
	pool.ObserveFloor(150)
	before = pool.Snapshot().VotesRejected
	addVote(t, pool, NewSkipVote(120), 0, keys[0])
	if pool.Snapshot().VotesRejected != before+1 {
		t.Fatal("vote at or below the floor must reject")
	}
	if pool.Snapshot().Slots != 0 {
		t.Fatal("floor must prune retained slots")
	}
}

// Votes buffered before the epoch's validator set installs assemble
// retroactively when it lands.
func TestCertPoolDeferredEpochAssembly(t *testing.T) {
	set, keys := testBLSValidatorSet(100, 40, 30, 15, 10, 5)
	verifier := NewCertificateVerifier()
	var emitted []Certificate
	pool := NewCertPool(DefaultCertPoolConfig(), verifier, func(c Certificate) { emitted = append(emitted, c) })
	pool.SetEpochLookup(func(uint64) uint64 { return set.Epoch })
	pool.NoteLiveSlot(1000)

	var blockHash solana.Hash
	blockHash[0] = 0x77
	vote := NewNotarizationVote(1000, blockHash)
	addVote(t, pool, vote, 0, keys[0])
	addVote(t, pool, vote, 1, keys[1])
	if len(emitted) != 0 {
		t.Fatal("nothing can assemble before the validator set installs")
	}

	if err := verifier.SetValidatorSet(set); err != nil {
		t.Fatal(err)
	}
	pool.OnValidatorSetInstalled(set.Epoch)
	if len(emitted) == 0 {
		t.Fatal("buffered votes must assemble once the set installs")
	}
}

// Without a real slot→epoch lookup the pool must NOT assemble (it must never
// guess an epoch / validator set), but it must assemble retroactively once the
// lookup is wired — fail-safe, not fail-broken.
func TestCertPoolMissingEpochLookupDoesNotAssemble(t *testing.T) {
	set, keys := testBLSValidatorSet(100, 40, 30, 15, 10, 5)
	verifier := NewCertificateVerifier()
	if err := verifier.SetValidatorSet(set); err != nil {
		t.Fatal(err)
	}
	var emitted []Certificate
	pool := NewCertPool(DefaultCertPoolConfig(), verifier, func(c Certificate) { emitted = append(emitted, c) })
	// No SetEpochLookup, but seed the window directly.
	pool.NoteLiveSlot(1200)

	var blockHash solana.Hash
	blockHash[0] = 0x33
	vote := NewNotarizationVote(1200, blockHash)
	addVote(t, pool, vote, 0, keys[0])
	addVote(t, pool, vote, 1, keys[1]) // 70% — would cross, but no epoch map
	if len(emitted) != 0 {
		t.Fatalf("must not assemble without a slot→epoch lookup, got %+v", emitted)
	}
	// A retry with the epoch still unresolved also assembles nothing.
	pool.OnValidatorSetInstalled(set.Epoch)
	if len(emitted) != 0 {
		t.Fatal("OnValidatorSetInstalled must not guess slot epochs")
	}
	// Wire the lookup and retry: the buffered quorum now assembles.
	pool.SetEpochLookup(func(uint64) uint64 { return set.Epoch })
	pool.OnValidatorSetInstalled(set.Epoch)
	if len(emitted) == 0 {
		t.Fatal("buffered quorum must assemble once the epoch lookup is installed")
	}
}

// Future-slot spam cannot grow the pool without bound: distinct future slots
// are capped by MaxLiveSlots and total buffered votes by MaxPendingVotesTotal.
func TestCertPoolGlobalMemoryBounds(t *testing.T) {
	set, keys := testBLSValidatorSet(100, 40, 30, 15, 10, 5)
	verifier := NewCertificateVerifier()
	if err := verifier.SetValidatorSet(set); err != nil {
		t.Fatal(err)
	}
	pool := NewCertPool(CertPoolConfig{
		MaxSlotsAhead: 1000, MaxPendingVotesPerSlot: 10, MaxLiveSlots: 3, MaxPendingVotesTotal: 4,
	}, verifier, nil)
	pool.SetEpochLookup(func(uint64) uint64 { return set.Epoch })
	pool.NoteLiveSlot(2000)

	// Spam distinct future slots: only MaxLiveSlots (3) may be retained, and the
	// global pending cap (4) stops buffering regardless of the per-slot cap.
	for slot := uint64(2001); slot <= 2010; slot++ {
		addVote(t, pool, NewSkipVote(slot), 0, keys[0])
	}
	snap := pool.Snapshot()
	if snap.Slots > 3 {
		t.Fatalf("MaxLiveSlots must cap retained slots, got %d", snap.Slots)
	}
	if snap.PendingTotal > 4 {
		t.Fatalf("MaxPendingVotesTotal must cap buffered votes, got %d", snap.PendingTotal)
	}
	if snap.VotesRejected == 0 {
		t.Fatal("spam beyond the bounds must be rejected")
	}
}

// Votor trigger freshness: SafeToNotar's MIXED condition (notar(b) >= 20% AND
// notar(b)+skip >= 60%) crosses on CANDIDATE stake -> both involved tallies
// fold immediately, so a voting engine reading VerifiedVotorStakes sees the
// predicate pass at the same moment an eager-verification node would — even
// though NO certificate threshold was reached. This is the cross-tally case a
// per-tally fold floor would miss.
func TestCertPoolFoldsForSafeToNotarMixedCondition(t *testing.T) {
	pool, _, keys, emitted := newTestPool(t)
	var b solana.Hash
	b[0] = 0x21
	// notar(b) = ranks 2+3 = 25% (>=20, <40); skip = rank 0 = 40% (<60 alone).
	addVote(t, pool, NewNotarizationVote(300, b), 2, keys[2])
	addVote(t, pool, NewNotarizationVote(300, b), 3, keys[3])

	stakes, ok := pool.VerifiedVotorStakes(300)
	if !ok {
		t.Fatal("validator set must be resolvable")
	}
	if stakes.Notarize[b] != 0 {
		t.Fatalf("below every trigger, nothing should be verified yet (lazy), got %d", stakes.Notarize[b])
	}

	// The skip arrival makes the mixed condition cross: 25+40 = 65 >= 60.
	addVote(t, pool, NewSkipVote(300), 0, keys[0])

	stakes, ok = pool.VerifiedVotorStakes(300)
	if !ok {
		t.Fatal("validator set must be resolvable")
	}
	if stakes.Notarize[b] != 25 || stakes.Skip != 40 {
		t.Fatalf("mixed-condition crossing must fold both tallies: notar=%d skip=%d", stakes.Notarize[b], stakes.Skip)
	}
	if len(*emitted) != 0 {
		t.Fatalf("no certificate threshold was reached; emitted %+v", *emitted)
	}
}

// SafeToSkip (skip + notarTotal - topNotar >= 40%) involves EVERY notarize
// tally; its crossing folds them all — including a sibling too small to
// trigger anything on its own.
func TestCertPoolFoldsForSafeToSkip(t *testing.T) {
	pool, _, keys, _ := newTestPool(t)
	var a, b solana.Hash
	a[0], b[0] = 0xA1, 0xB1
	addVote(t, pool, NewNotarizationVote(310, a), 1, keys[1]) // 30%
	addVote(t, pool, NewNotarizationVote(310, b), 2, keys[2]) // 15% — fails every per-block trigger
	// skip 40%: SafeToSkip = 40 + (45-30) = 55 >= 40 -> fold skip + ALL notar.
	addVote(t, pool, NewSkipVote(310), 0, keys[0])

	stakes, ok := pool.VerifiedVotorStakes(310)
	if !ok {
		t.Fatal("validator set must be resolvable")
	}
	if stakes.Notarize[a] != 30 || stakes.Notarize[b] != 15 || stakes.Skip != 40 {
		t.Fatalf("SafeToSkip crossing must fold all trigger tallies: a=%d b=%d skip=%d",
			stakes.Notarize[a], stakes.Notarize[b], stakes.Skip)
	}
	if stakes.NotarizeTotal != 45 || stakes.TopNotarize != 30 {
		t.Fatalf("aggregates wrong: total=%d top=%d", stakes.NotarizeTotal, stakes.TopNotarize)
	}
}

// Below every trigger, verification stays lazy: nothing folds, votes buffer as
// candidate stake only.
func TestCertPoolStaysLazyBelowTriggers(t *testing.T) {
	pool, _, keys, _ := newTestPool(t)
	var c solana.Hash
	c[0] = 0xC1
	addVote(t, pool, NewNotarizationVote(320, c), 4, keys[4]) // 5%
	addVote(t, pool, NewSkipVote(320), 3, keys[3])            // 10%; SafeToSkip = 10+0 < 40

	stakes, ok := pool.VerifiedVotorStakes(320)
	if !ok {
		t.Fatal("validator set must be resolvable")
	}
	if stakes.Notarize[c] != 0 || stakes.Skip != 0 {
		t.Fatalf("sub-trigger tallies must stay unverified (lazy): %+v", stakes)
	}
	if pool.Snapshot().PendingTotal != 2 {
		t.Fatalf("votes must remain buffered as candidates, pending=%d", pool.Snapshot().PendingTotal)
	}
}

// Bitmap encode/decode round-trips exactly for both encodings.
func TestEncodeSignerStoreBitmapRoundTrip(t *testing.T) {
	b2 := SignerBitmap{Encoding: SignerBitmapBase2, Length: 11, Base: make([]bool, 11)}
	b2.Base[0], b2.Base[3], b2.Base[10] = true, true, true
	enc, err := EncodeSignerStoreBitmap(b2)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := DecodeSignerStoreBitmap(enc, 11)
	if err != nil {
		t.Fatal(err)
	}
	for i := range b2.Base {
		if dec.Base[i] != b2.Base[i] {
			t.Fatalf("base2 bit %d mismatch", i)
		}
	}

	b3 := SignerBitmap{Encoding: SignerBitmapBase3, Length: 7, Base: make([]bool, 7), Fallback: make([]bool, 7)}
	b3.Base[1], b3.Base[6] = true, true
	b3.Fallback[0], b3.Fallback[4] = true, true
	enc, err = EncodeSignerStoreBitmap(b3)
	if err != nil {
		t.Fatal(err)
	}
	dec, err = DecodeSignerStoreBitmap(enc, 7)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		if dec.Base[i] != b3.Base[i] || dec.Fallback[i] != b3.Fallback[i] {
			t.Fatalf("base3 symbol %d mismatch", i)
		}
	}

	// Non-disjoint base3 must refuse to encode.
	bad := SignerBitmap{Encoding: SignerBitmapBase3, Length: 2, Base: []bool{true, false}, Fallback: []bool{true, false}}
	if _, err := EncodeSignerStoreBitmap(bad); err == nil {
		t.Fatal("non-disjoint base3 must fail to encode")
	}
}

// Duplicate certs are emitted once; identical re-votes are deduped.
func TestCertPoolEmitsOnce(t *testing.T) {
	pool, _, keys, emitted := newTestPool(t)
	var blockHash solana.Hash
	blockHash[0] = 0x55
	vote := NewNotarizationVote(1100, blockHash)

	addVote(t, pool, vote, 0, keys[0])
	addVote(t, pool, vote, 1, keys[1])
	n := len(*emitted)
	addVote(t, pool, vote, 1, keys[1]) // exact duplicate
	addVote(t, pool, vote, 4, keys[4]) // more stake, same certs already out (fast needs 80: 40+30+5=75, no)
	if len(*emitted) != n {
		t.Fatalf("no new certs expected, went from %d to %d", n, len(*emitted))
	}
}
