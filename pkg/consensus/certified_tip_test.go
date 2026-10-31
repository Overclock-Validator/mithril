package consensus

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
)

func signedTestCertificate(t *testing.T, cert alpenglow.Certificate) alpenglow.Certificate {
	t.Helper()
	cert.Bitmap = testAlpenglowSignerBitmap()
	cert.Signature = testAlpenglowCertificateSignature(t, cert)
	return cert
}

// RPC health uses the certified tip as the network's slot, so only verified
// certificates that arrived over Votor may move it: footer certificates are old
// during catch-up, and unverified ones are attacker-controlled.
func TestAlpenglowCertifiedTipFollowsVerifiedVotorCertificates(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	set := testAlpenglowValidatorSet()
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	if err := engine.SetAlpenglowValidatorSet(set); err != nil {
		t.Fatalf("SetAlpenglowValidatorSet: %v", err)
	}
	if _, _, ok := engine.AlpenglowCertifiedTip(); ok {
		t.Fatal("certified tip set before any certificate")
	}

	engine.ObserveFooterCertificates([]alpenglow.Certificate{signedTestCertificate(t, alpenglow.Certificate{
		Type: alpenglow.CertificateFinalizeFast, Slot: 40, BlockHash: solana.Hash{0x40},
	})})
	if slot, _, ok := engine.AlpenglowCertifiedTip(); ok {
		t.Fatalf("footer certificate moved the certified tip to %d", slot)
	}

	engine.observeVotorMessage(alpenglow.NewCertificateMessage(signedTestCertificate(t, alpenglow.Certificate{
		Type: alpenglow.CertificateSkip, Slot: 50,
	})))
	slot, at, ok := engine.AlpenglowCertifiedTip()
	if !ok || slot != 50 || at.IsZero() {
		t.Fatalf("certified tip after Votor certificate = %d, %v, %v; want 50", slot, at, ok)
	}

	forged := signedTestCertificate(t, alpenglow.Certificate{Type: alpenglow.CertificateSkip, Slot: 900})
	forged.Signature[0] ^= 0xff
	engine.observeVotorMessage(alpenglow.NewCertificateMessage(forged))
	if slot, _, _ := engine.AlpenglowCertifiedTip(); slot != 50 {
		t.Fatalf("certificate with a bad signature moved the certified tip to %d", slot)
	}

	engine.observeVotorMessage(alpenglow.NewCertificateMessage(signedTestCertificate(t, alpenglow.Certificate{
		Type: alpenglow.CertificateSkip, Slot: 45,
	})))
	if slot, _, _ := engine.AlpenglowCertifiedTip(); slot != 50 {
		t.Fatalf("older certificate moved the certified tip back to %d", slot)
	}
}

func TestAlpenglowCertifiedTipCountsDeferredVotorCertificates(t *testing.T) {
	engine, err := NewEngine(Config{})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	set := testAlpenglowValidatorSet()
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	engine.observeVotorMessage(alpenglow.NewCertificateMessage(signedTestCertificate(t, alpenglow.Certificate{
		Type: alpenglow.CertificateSkip, Slot: 42,
	})))
	if _, _, ok := engine.AlpenglowCertifiedTip(); ok {
		t.Fatal("certificate counted before its stakes were installed")
	}
	if err := engine.SetAlpenglowValidatorSet(set); err != nil {
		t.Fatalf("SetAlpenglowValidatorSet: %v", err)
	}
	if slot, _, ok := engine.AlpenglowCertifiedTip(); !ok || slot != 42 {
		t.Fatalf("deferred Votor certificate gave certified tip %d, %v; want 42", slot, ok)
	}
}
