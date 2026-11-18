package rewardcerts

import (
	"bytes"
	"sync"
	"testing"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartialCertIncrementalAggregateMatchesRawAggregation(t *testing.T) {
	const count = 12
	keys := testBLSKeys(t, count)
	partial := newPartialCert(count)
	var votes []alpenglow.VoteMessage
	var raw [][]byte
	for rank := range count {
		vote := testSignedVote(t, alpenglow.NewSkipVote(123), rank, keys[rank])
		if rank%2 == 0 {
			point, err := decodeVoteSignature(vote.Signature)
			require.NoError(t, err)
			compressed := point.Bytes()
			vote.Signature = compressed[:]
		}
		require.NoError(t, partial.addVote(vote.Rank, vote.Signature))
		votes = append(votes, vote)
		raw = append(raw, vote.Signature)
		built, err := partial.build()
		require.NoError(t, err)
		want, err := aggregateVoteSignatures(raw)
		require.NoError(t, err)
		assert.Equal(t, want, built.Signature)
		assertAggregateMatchesVotes(t, built.Signature[:], votes)
	}
}

func TestMalformedRewardVotesDoNotOccupyRanks(t *testing.T) {
	var nonSubgroup bls12381.G2Affine
	for x := byte(1); x < 100; x++ {
		encoded := make([]byte, bls12381.SizeOfG2AffineCompressed)
		encoded[0], encoded[len(encoded)-1] = 0x80, x
		// Construct a curve-valid but subgroup-invalid fixture deliberately.
		decoder := bls12381.NewDecoder(bytes.NewReader(encoded), bls12381.NoSubgroupChecks())
		if decoder.Decode(&nonSubgroup) == nil && nonSubgroup.IsOnCurve() && !nonSubgroup.IsInSubGroup() {
			break
		}
	}
	require.True(t, nonSubgroup.IsOnCurve())
	require.False(t, nonSubgroup.IsInSubGroup())
	nonSubgroupRaw := nonSubgroup.RawBytes()
	var infinity bls12381.G2Affine
	infinity.SetInfinity()
	infinityRaw := infinity.RawBytes()
	badSignatures := map[string][]byte{
		"empty":        nil,
		"length":       {1, 2, 3},
		"encoding":     bytes.Repeat([]byte{0xff}, bls12381.SizeOfG2AffineUncompressed),
		"infinity":     infinityRaw[:],
		"not subgroup": nonSubgroupRaw[:],
	}
	var blockID solana.Hash
	blockID[0] = 9
	for kind, vote := range map[string]alpenglow.Vote{
		"skip": alpenglow.NewSkipVote(123), "notar": alpenglow.NewNotarizationVote(123, blockID),
	} {
		for name, badSignature := range badSignatures {
			t.Run(kind+"/"+name, func(t *testing.T) {
				builder := NewBuilder(BuilderConfig{MaxValidators: 1})
				valid := testSignedVote(t, vote, 0, testBLSKeys(t, 1)[0])
				bad := valid
				bad.Signature = badSignature
				builder.AddVote(bad)
				require.Empty(t, builder.votes)
				// An unavailable/forged provenance must also decode normally.
				builder.AddVerifiedVote(alpenglow.VerifiedVote{Message: bad})
				require.Empty(t, builder.votes)
				builder.AddVote(valid)
				certs := builder.BuildForLeaderSlot(123 + SlotsForReward)
				if kind == "skip" {
					cert, err := DecodeSkipRewardCertificate(certs.Skip)
					require.NoError(t, err)
					assertAggregateMatchesVotes(t, cert.Signature[:], []alpenglow.VoteMessage{valid})
				} else {
					cert, err := DecodeNotarRewardCertificate(certs.Notar)
					require.NoError(t, err)
					assertAggregateMatchesVotes(t, cert.Signature[:], []alpenglow.VoteMessage{valid})
				}
			})
		}
	}
}

func TestRewardAggregateOwnsSignatureAndBuiltResult(t *testing.T) {
	keys := testBLSKeys(t, 2)
	vote := testSignedVote(t, alpenglow.NewSkipVote(123), 0, keys[0])
	original := vote
	original.Signature = append([]byte(nil), vote.Signature...)
	partial := newPartialCert(2)
	require.NoError(t, partial.addVote(vote.Rank, vote.Signature))
	clear(vote.Signature)
	first, err := partial.build()
	require.NoError(t, err)
	firstBytes := first.Signature
	firstBitmap := append([]byte(nil), first.Bitmap...)
	assertAggregateMatchesVotes(t, first.Signature[:], []alpenglow.VoteMessage{original})

	secondVote := testSignedVote(t, alpenglow.NewSkipVote(123), 1, keys[1])
	point, err := decodeVoteSignature(secondVote.Signature)
	require.NoError(t, err)
	require.NoError(t, partial.addVerifiedSignature(secondVote.Rank, point))
	point.SetInfinity()
	second, err := partial.build()
	require.NoError(t, err)
	assertAggregateMatchesVotes(t, second.Signature[:], []alpenglow.VoteMessage{original, secondVote})
	assert.Equal(t, firstBytes, first.Signature)
	assert.Equal(t, firstBitmap, first.Bitmap)
}

func TestBuilderVerifiedVoteMatchesRawPath(t *testing.T) {
	const count = 8
	keys := testBLSKeys(t, count)
	votes := make([]alpenglow.VoteMessage, count)
	for rank := range count {
		votes[rank] = testSignedVote(t, alpenglow.NewSkipVote(123), rank, keys[rank])
	}
	set, _ := testValidatorSetForVotes(t, votes)
	verifier := alpenglow.NewCertificateVerifier()
	require.NoError(t, verifier.SetValidatorSet(set))
	verifier.SetShredVersion(0)
	rawBuilder := NewBuilder(BuilderConfig{MaxValidators: count})
	verifiedBuilder := NewBuilder(BuilderConfig{MaxValidators: count})
	for _, vote := range votes {
		result, err := verifier.VerifyVoteMessageForEpoch(set.Epoch, vote)
		require.NoError(t, err)
		verified := alpenglow.VerifiedVote{Message: vote, Result: result}
		_, ok := verified.VerifiedSignature()
		require.True(t, ok)
		rawBuilder.AddVote(vote)
		verifiedBuilder.AddVerifiedVote(verified)
		verifiedBuilder.AddVerifiedVote(verified) // Rank deduplication remains exact.
	}
	assert.Equal(t, rawBuilder.BuildForLeaderSlot(123+SlotsForReward), verifiedBuilder.BuildForLeaderSlot(123+SlotsForReward))
}

func TestBuilderConcurrentAdmissionDeduplicatesIncrementalAggregate(t *testing.T) {
	const count = 32
	keys := testBLSKeys(t, count)
	votes := make([]alpenglow.VoteMessage, count)
	for rank := range count {
		votes[rank] = testSignedVote(t, alpenglow.NewSkipVote(123), rank, keys[rank])
	}
	builder := NewBuilder(BuilderConfig{MaxValidators: count})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for _, vote := range votes {
				builder.AddVote(vote)
			}
		})
	}
	wg.Wait()
	cert, err := DecodeSkipRewardCertificate(builder.BuildForLeaderSlot(123 + SlotsForReward).Skip)
	require.NoError(t, err)
	assertAggregateMatchesVotes(t, cert.Signature[:], votes)
}

func TestNotarTieKeepsOneValidBlockAndFirstVotePerRank(t *testing.T) {
	keys := testBLSKeys(t, 2)
	var blocks [2]solana.Hash
	blocks[0][0], blocks[1][0] = 1, 2
	builder := NewBuilder(BuilderConfig{MaxValidators: 2})
	votes := make([]alpenglow.VoteMessage, 2)
	for rank, block := range blocks {
		votes[rank] = testSignedVote(t, alpenglow.NewNotarizationVote(123, block), rank, keys[rank])
		builder.AddVote(votes[rank])
	}
	// A conflicting later notarization may not let one rank boost another block.
	builder.AddVote(testSignedVote(t, alpenglow.NewNotarizationVote(123, blocks[1]), 0, keys[0]))
	cert, err := DecodeNotarRewardCertificate(builder.BuildForLeaderSlot(123 + SlotsForReward).Notar)
	require.NoError(t, err)
	for rank, block := range blocks {
		if cert.BlockID == block {
			assertBitmapRanks(t, cert.Bitmap, 2, rank)
			assertAggregateMatchesVotes(t, cert.Signature[:], []alpenglow.VoteMessage{votes[rank]})
			return
		}
	}
	t.Fatalf("unexpected tied notarization block %s", cert.BlockID)
}
