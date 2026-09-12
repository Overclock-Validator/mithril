package gossip

import (
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	narya "github.com/Overclock-Validator/narya-ed25519/ed25519"
)

// A canonical small-order point encoding. Mainnet (ed25519-dalek
// verify_strict) rejects a signature whose public key or R point is
// small-order; Go's crypto/ed25519 does not. These gossip Verify
// methods must reject it — a crafted contact-info or ping/pong with a
// small-order key must not be accepted where mainnet would reject it.
const smallOrderPubHex = "0100000000000000000000000000000000000000000000000000000000000000"

func smallOrderPubkey(t *testing.T) Pubkey {
	t.Helper()
	raw, err := hex.DecodeString(smallOrderPubHex)
	if err != nil {
		t.Fatal(err)
	}
	var p Pubkey
	copy(p[:], raw)
	return p
}

// TestPingVerifyStrict covers the happy path (an honest ping still
// verifies), small-order rejection (the fix), and exact agreement with
// narya.VerifyStrict across a spread of inputs (the wiring).
func TestPingVerifyStrict(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	honest, err := newPing(priv)
	if err != nil {
		t.Fatal(err)
	}
	if !honest.Verify() {
		t.Fatal("honest ping failed to verify")
	}

	// Small-order From: rejected regardless of signature.
	bad := honest
	bad.From = smallOrderPubkey(t)
	if bad.Verify() {
		t.Fatal("ping with small-order public key was accepted")
	}

	// The method must equal narya.VerifyStrict on every input.
	for _, p := range []Ping{honest, bad} {
		if p.Verify() != narya.VerifyStrict(p.From[:], p.Token[:], p.Signature[:]) {
			t.Fatal("Ping.Verify diverged from narya.VerifyStrict")
		}
	}
}

// TestPongVerifyStrict mirrors TestPingVerifyStrict for pong.
func TestPongVerifyStrict(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	ping, err := newPing(priv)
	if err != nil {
		t.Fatal(err)
	}
	honest, err := newPong(ping, priv)
	if err != nil {
		t.Fatal(err)
	}
	if !honest.Verify() {
		t.Fatal("honest pong failed to verify")
	}

	bad := honest
	bad.From = smallOrderPubkey(t)
	if bad.Verify() {
		t.Fatal("pong with small-order public key was accepted")
	}
	for _, p := range []Pong{honest, bad} {
		if p.Verify() != narya.VerifyStrict(p.From[:], p.Hash[:], p.Signature[:]) {
			t.Fatal("Pong.Verify diverged from narya.VerifyStrict")
		}
	}
}
