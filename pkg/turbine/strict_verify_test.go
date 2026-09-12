package turbine

import (
	"encoding/hex"
	"testing"

	narya "github.com/Overclock-Validator/narya-ed25519/ed25519"
	"github.com/gagliardetto/solana-go"
)

// TestShredVerifyStrict confirms shred signature verification is routed
// through strict semantics: an honestly signed shred still verifies,
// and a small-order leader key is rejected (mainnet's verify_strict
// rejects small-order A). A crafted shred attributed to a small-order
// leader must not be admitted where mainnet would reject it.
func TestShredVerifyStrict(t *testing.T) {
	shred, leader := buildSignedTestShred(t, 10, 0x42)
	if err := shred.VerifySignature(leader); err != nil {
		t.Fatalf("honest shred failed strict verification: %v", err)
	}

	raw, err := hex.DecodeString("0100000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	var smallOrderLeader solana.PublicKey
	copy(smallOrderLeader[:], raw)
	if err := shred.VerifySignature(smallOrderLeader); err == nil {
		t.Fatal("shred with small-order leader key was accepted")
	}

	// The reference and cached paths must agree with narya.VerifyStrict.
	root, err := shred.MerkleRoot()
	if err != nil {
		t.Fatal(err)
	}
	for _, leaderKey := range []solana.PublicKey{leader, smallOrderLeader} {
		want := narya.VerifyStrict(leaderKey[:], root[:], shred.Signature[:])
		got := shred.VerifySignature(leaderKey) == nil
		if got != want {
			t.Fatalf("VerifySignature diverged from narya.VerifyStrict for leader %x", leaderKey)
		}
		cache := &shredSigCache{}
		if (cache.verifyShred(shred, leaderKey) == nil) != want {
			t.Fatalf("verifyShred diverged from narya.VerifyStrict for leader %x", leaderKey)
		}
	}
}

// TestShredSigCacheDoesNotCacheStrictRejection ensures a strict
// rejection is never memoized as valid: re-verifying a small-order
// leader must keep failing.
func TestShredSigCacheDoesNotCacheStrictRejection(t *testing.T) {
	shred, _ := buildSignedTestShred(t, 11, 0x7)
	raw, _ := hex.DecodeString("0100000000000000000000000000000000000000000000000000000000000000")
	var smallOrderLeader solana.PublicKey
	copy(smallOrderLeader[:], raw)

	cache := &shredSigCache{}
	for i := 0; i < 3; i++ {
		if cache.verifyShred(shred, smallOrderLeader) == nil {
			t.Fatalf("iteration %d: cache accepted a small-order leader", i)
		}
	}
}
