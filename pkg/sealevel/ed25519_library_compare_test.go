package sealevel

import (
	stdlibed25519 "crypto/ed25519"
	"fmt"
	"testing"

	oasised25519 "github.com/oasisprotocol/curve25519-voi/primitives/ed25519"
)

var (
	oasisDalekStrictOptions = oasised25519.Options{
		Verify: &oasised25519.VerifyOptions{
			AllowSmallOrderA:   false,
			AllowSmallOrderR:   false,
			AllowNonCanonicalA: true,
			AllowNonCanonicalR: false,
			CofactorlessVerify: true,
		},
	}
	// This benchmark-only profile preserves the strict input-admission flags
	// but selects Voi's cofactored ABGLSV-Pornin equation. It is not equivalent
	// to Mithril's DalekStrict consensus predicate.
	oasisCofactoredStrictBenchmarkOptions = oasised25519.Options{
		Verify: &oasised25519.VerifyOptions{
			AllowNonCanonicalA: true,
		},
	}
	ed25519LibraryCompareSink bool
)

type ed25519LibraryCompareFixture struct {
	stdlibPublicKey stdlibed25519.PublicKey
	oasisPublicKey  oasised25519.PublicKey
	expandedKey     *oasised25519.ExpandedPublicKey
	message         []byte
	signature       []byte
}

func newEd25519LibraryCompareFixture(tb testing.TB, messageSize int) ed25519LibraryCompareFixture {
	tb.Helper()

	var seed [stdlibed25519.SeedSize]byte
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	privateKey := stdlibed25519.NewKeyFromSeed(seed[:])
	stdlibPublicKey := privateKey.Public().(stdlibed25519.PublicKey)

	message := make([]byte, messageSize)
	for i := range message {
		message[i] = byte(i*31 + messageSize)
	}
	signature := stdlibed25519.Sign(privateKey, message)
	oasisPublicKey := oasised25519.PublicKey(stdlibPublicKey)
	expandedKey, err := oasised25519.NewExpandedPublicKey(oasisPublicKey)
	if err != nil {
		tb.Fatalf("expand honest Ed25519 public key: %v", err)
	}

	return ed25519LibraryCompareFixture{
		stdlibPublicKey: stdlibPublicKey,
		oasisPublicKey:  oasisPublicKey,
		expandedKey:     expandedKey,
		message:         message,
		signature:       signature,
	}
}

func TestEd25519LibraryCompareHonestSignatures(t *testing.T) {
	for _, messageSize := range []int{64, 200, 1232} {
		t.Run(fmt.Sprintf("msg=%d", messageSize), func(t *testing.T) {
			fixture := newEd25519LibraryCompareFixture(t, messageSize)

			if !stdlibed25519.Verify(fixture.stdlibPublicKey, fixture.message, fixture.signature) {
				t.Fatal("crypto/ed25519 rejected an honest signature")
			}
			if !oasised25519.VerifyWithOptions(fixture.oasisPublicKey, fixture.message, fixture.signature, &oasisDalekStrictOptions) {
				t.Fatal("Oasis cold verification rejected an honest signature")
			}
			if !oasised25519.VerifyExpandedWithOptions(fixture.expandedKey, fixture.message, fixture.signature, &oasisDalekStrictOptions) {
				t.Fatal("Oasis expanded verification rejected an honest signature")
			}
		})
	}
}

// Oasis's aggregate VerifyBatchOnly path deliberately returns false when any
// entry requests cofactorless verification. BatchVerifier.Verify can fall back
// to serial verification, but that is not a cofactorless batch acceleration.
func TestOasisDalekStrictBatchVerificationUnsupported(t *testing.T) {
	fixture := newEd25519LibraryCompareFixture(t, 200)
	verifier := oasised25519.NewBatchVerifier()
	verifier.AddWithOptions(fixture.oasisPublicKey, fixture.message, fixture.signature, &oasisDalekStrictOptions)
	if verifier.VerifyBatchOnly(nil) {
		t.Fatal("Oasis unexpectedly batch-verified a cofactorless entry")
	}
}

func BenchmarkEd25519LibraryCompare(b *testing.B) {
	for _, messageSize := range []int{64, 200, 1232} {
		fixture := newEd25519LibraryCompareFixture(b, messageSize)

		b.Run(fmt.Sprintf("impl=stdlib/msg=%d", messageSize), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(messageSize))
			b.ResetTimer()

			var ok bool
			for i := 0; i < b.N; i++ {
				ok = stdlibed25519.Verify(fixture.stdlibPublicKey, fixture.message, fixture.signature)
			}
			ed25519LibraryCompareSink = ok
		})

		b.Run(fmt.Sprintf("impl=oasis-cold/msg=%d", messageSize), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(messageSize))
			b.ResetTimer()

			var ok bool
			for i := 0; i < b.N; i++ {
				ok = oasised25519.VerifyWithOptions(fixture.oasisPublicKey, fixture.message, fixture.signature, &oasisDalekStrictOptions)
			}
			ed25519LibraryCompareSink = ok
		})

		b.Run(fmt.Sprintf("impl=oasis-expanded/msg=%d", messageSize), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(messageSize))
			b.ResetTimer()

			var ok bool
			for i := 0; i < b.N; i++ {
				ok = oasised25519.VerifyExpandedWithOptions(fixture.expandedKey, fixture.message, fixture.signature, &oasisDalekStrictOptions)
			}
			ed25519LibraryCompareSink = ok
		})

		b.Run(fmt.Sprintf("impl=oasis-cofactored-pornin-cold/msg=%d", messageSize), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(messageSize))
			b.ResetTimer()

			var ok bool
			for i := 0; i < b.N; i++ {
				ok = oasised25519.VerifyWithOptions(fixture.oasisPublicKey, fixture.message, fixture.signature, &oasisCofactoredStrictBenchmarkOptions)
			}
			ed25519LibraryCompareSink = ok
		})

		b.Run(fmt.Sprintf("impl=oasis-cofactored-pornin-expanded/msg=%d", messageSize), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(messageSize))
			b.ResetTimer()

			var ok bool
			for i := 0; i < b.N; i++ {
				ok = oasised25519.VerifyExpandedWithOptions(fixture.expandedKey, fixture.message, fixture.signature, &oasisCofactoredStrictBenchmarkOptions)
			}
			ed25519LibraryCompareSink = ok
		})
	}
}

func BenchmarkEd25519LibraryExpand(b *testing.B) {
	fixture := newEd25519LibraryCompareFixture(b, 200)
	b.ReportAllocs()
	b.ResetTimer()

	var expanded *oasised25519.ExpandedPublicKey
	for i := 0; i < b.N; i++ {
		var err error
		expanded, err = oasised25519.NewExpandedPublicKey(fixture.oasisPublicKey)
		if err != nil {
			b.Fatal(err)
		}
	}
	if expanded == nil {
		b.Fatal("Oasis returned a nil expanded public key")
	}
}
