package alpenglow

import (
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestVerifiedSignatureBoundToVerifiedMessageAndResult(t *testing.T) {
	set, keys := testBLSValidatorSet(100, 100)
	const shredVersion = uint16(37)
	message := VoteMessage{Vote: NewNotarizationVote(42, solana.Hash{4}), Rank: 0}
	message.Signature = signTestVoteWithShredVersion(t, message.Vote, keys[0], shredVersion)
	result, err := verifyVoteMessageWithSetAndShredVersion(set, message, shredVersion)
	require.NoError(t, err)
	verified := VerifiedVote{Message: message, Result: result}
	point, ok := verified.VerifiedSignature()
	require.True(t, ok)
	want, err := decodeBLSSignature(message.Signature)
	require.NoError(t, err)
	require.Equal(t, want, point)

	for _, tt := range []struct {
		name   string
		mutate func(*VerifiedVote)
	}{
		{"vote type", func(v *VerifiedVote) { v.Message.Vote.Type = VoteTypeSkip }},
		{"slot", func(v *VerifiedVote) { v.Message.Vote.Slot++ }},
		{"block", func(v *VerifiedVote) { v.Message.Vote.BlockHash[0]++ }},
		{"message rank", func(v *VerifiedVote) { v.Message.Rank++ }},
		{"signature bytes", func(v *VerifiedVote) { v.Message.Signature[0] ^= 1 }},
		{"signature length", func(v *VerifiedVote) { v.Message.Signature = v.Message.Signature[:96] }},
		{"epoch", func(v *VerifiedVote) { v.Result.Epoch++ }},
		{"result rank", func(v *VerifiedVote) { v.Result.Rank++ }},
		{"stake", func(v *VerifiedVote) { v.Result.Stake++ }},
		{"total stake", func(v *VerifiedVote) { v.Result.TotalStake++ }},
		{"caller-constructed result", func(v *VerifiedVote) {
			v.Result = VoteVerifyResult{Epoch: result.Epoch, Rank: result.Rank, Stake: result.Stake, TotalStake: result.TotalStake}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			modified := verified
			modified.Message.Signature = append([]byte(nil), message.Signature...)
			tt.mutate(&modified)
			_, ok := modified.VerifiedSignature()
			require.False(t, ok, "modified vote reused a trusted point")
		})
	}

	// Returned points cannot mutate the verifier's cache; unsigned transport
	// metadata can change without invalidating the exact signed payload.
	point.SetInfinity()
	verified.Message.At = time.Now()
	verified.Local = true
	point, ok = verified.VerifiedSignature()
	require.True(t, ok)
	require.Equal(t, want, point)
	_, err = verifyVoteMessageWithSetAndShredVersion(set, message, shredVersion+1)
	require.Error(t, err, "the cache must not bypass verification under another shred version")

	// The cache owns a digest, never an alias to mutable signature storage.
	message.Signature[0] ^= 1
	_, ok = verified.VerifiedSignature()
	require.False(t, ok)
}

func TestCertPoolPublishesVerifiedSignaturePoint(t *testing.T) {
	pool, _, keys, _ := newTestPool(t)
	var verified []VerifiedVote
	pool.SetVerifiedVoteSink(func(vote VerifiedVote) { verified = append(verified, vote) })
	vote := NewSkipVote(501)
	addVote(t, pool, vote, 0, keys[0])
	require.Len(t, verified, 1)
	point, ok := verified[0].VerifiedSignature()
	require.True(t, ok)
	want, err := decodeBLSSignature(verified[0].Message.Signature)
	require.NoError(t, err)
	require.Equal(t, want, point)

	// Valid curve bytes signed for a different slot must still fail the pairing
	// check and must never acquire signature provenance or reach the consumer.
	pool.AddVote(VoteMessage{Vote: NewSkipVote(502), Rank: 0, Signature: verified[0].Message.Signature})
	require.Len(t, verified, 1)
}

func TestCertPoolVerifiedSignatureDiscardedOnShredVersionChange(t *testing.T) {
	pool, _, keys, emitted := newTestPool(t)
	seen := make(chan VerifiedVote, 8)
	pool.SetVerifiedVoteSink(func(vote VerifiedVote) { seen <- vote })
	vote := NewSkipVote(500)
	message := VoteMessage{Vote: vote, Rank: 0, Signature: signTestVote(t, vote, keys[0])}
	release, owner := parkCertPoolVerification(t, pool, message)
	pool.verifier.SetShredVersion(17)
	release()
	waitCertPoolCall(t, owner)
	require.Empty(t, seen, "work spanning a shred-version change acquired trusted provenance")
	require.Empty(t, *emitted)
	require.Zero(t, pool.Snapshot().PendingTotal)

	message.Signature = signTestVoteWithShredVersion(t, vote, keys[0], 17)
	pool.AddVote(message)
	require.Len(t, seen, 1)
	verified := <-seen
	_, ok := verified.VerifiedSignature()
	require.True(t, ok, "fresh verification under the new shred version lost provenance")
}
