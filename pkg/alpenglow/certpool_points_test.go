package alpenglow

import (
	"bytes"
	"testing"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
)

func TestCertPoolVerifiedPointsMatchIndividualVerification(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		name := "valid"
		if mixed {
			name = "mixed-invalid"
		}
		t.Run(name, func(t *testing.T) {
			pool, set, keys, _ := newTestPool(t)
			vote := NewSkipVote(500)
			var batch []VoteMessage
			for i, key := range keys {
				signedVote := vote
				if mixed && i%2 == 1 {
					signedVote = NewSkipVote(501) // Valid point, wrong payload, in both halves.
				}
				batch = append(batch, VoteMessage{Vote: vote, Rank: uint16(i), Signature: signTestVote(t, signedVote, key)})
			}
			if mixed {
				var infinity bls12381.G2Affine
				infinity.SetInfinity()
				raw := infinity.RawBytes()
				batch = append(batch,
					VoteMessage{Vote: vote, Rank: 0, Signature: []byte{0xff}},
					VoteMessage{Vote: vote, Rank: 1, Signature: raw[:]},
					VoteMessage{Vote: vote, Rank: uint16(len(keys)), Signature: batch[0].Signature},
				)
			}
			var expected []VoteMessage
			for _, msg := range batch {
				if _, err := verifyVoteMessageWithSet(set, msg); err == nil {
					expected = append(expected, msg)
				}
			}
			verified := pool.verifyBatch(batch, &set)
			if len(verified) != len(expected) {
				t.Fatalf("verified %d members, want %d", len(verified), len(expected))
			}
			for i, member := range verified {
				if member.message.Rank != expected[i].Rank || member.message.Vote != expected[i].Vote || !bytes.Equal(member.message.Signature, expected[i].Signature) {
					t.Fatalf("member %d does not match its individually verified message", i)
				}
				raw := member.sig.RawBytes()
				if !bytes.Equal(raw[:], expected[i].Signature) {
					t.Fatalf("member %d retained the wrong signature point", i)
				}
				pub, err := validatorBLSPubkey(set, int(expected[i].Rank))
				if err != nil || !member.pubkey.Equal(&pub) {
					t.Fatalf("member %d retained the wrong public key", i)
				}
			}
			if mixed && pool.Snapshot().BatchesVerified <= 1 {
				t.Fatal("mixed batch did not exercise recursive verification")
			}
		})
	}
}

// Entropy failure uses this individual-verification path instead of accepting
// an unweighted aggregate. Reusing points must preserve its filtering too.
func TestIndividuallyVerifiedBatchRetainsOnlyValidPoints(t *testing.T) {
	_, set, keys, _ := newTestPool(t)
	vote := NewSkipVote(502)
	payload, err := EncodeVotePayloadToSign(vote, 0)
	if err != nil {
		t.Fatal(err)
	}
	var members []parsedBatchVote
	for i, key := range keys {
		signedVote := vote
		if i%2 == 1 {
			signedVote = NewSkipVote(503)
		}
		msg := VoteMessage{Vote: vote, Rank: uint16(i), Signature: signTestVote(t, signedVote, key)}
		pub, err := validatorBLSPubkey(set, i)
		if err != nil {
			t.Fatal(err)
		}
		var sig bls12381.G2Affine
		if _, err := sig.SetBytes(msg.Signature); err != nil {
			t.Fatal(err)
		}
		members = append(members, parsedBatchVote{message: msg, pubkey: pub, sig: sig})
	}
	verified := individuallyVerifiedBatch(members, payload)
	if len(verified) != 3 {
		t.Fatalf("verified %d, want 3", len(verified))
	}
	for i, member := range verified {
		if member.message.Rank != uint16(i*2) || !member.sig.Equal(&members[i*2].sig) || !member.pubkey.Equal(&members[i*2].pubkey) {
			t.Fatalf("wrong verified member at %d", i)
		}
	}
}
