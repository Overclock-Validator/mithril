package rewardcerts

import (
	"testing"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
)

// BenchmarkBuilderClose660 isolates the work that used to run after the
// producer stopped executing transactions. Admission/setup is done once, and
// each iteration consumes the same immutable vote entry.
func BenchmarkBuilderClose660(b *testing.B) {
	const rewardSlot = uint64(123)
	builder := NewBuilder(BuilderConfig{MaxValidators: 660})
	vote := alpenglow.NewSkipVote(rewardSlot)
	payload, err := alpenglow.EncodeVotePayloadToSign(vote, 0)
	if err != nil {
		b.Fatal(err)
	}
	point, err := bls12381.HashToG2(payload, []byte(testBLSHashDST))
	if err != nil {
		b.Fatal(err)
	}
	var signature bls12381.G2Affine
	signature.SetInfinity()
	for rank := uint16(0); rank < 660; rank++ {
		signature.Add(&signature, &point)
		raw := signature.RawBytes()
		builder.AddVote(alpenglow.VoteMessage{Vote: vote, Rank: rank, Signature: raw[:]})
	}
	entry := builder.votes[rewardSlot]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.votes[rewardSlot] = entry
		if result := builder.BuildForLeaderSlot(rewardSlot + SlotsForReward); len(result.Skip) == 0 {
			b.Fatal("missing reward certificate")
		}
	}
}

// BenchmarkBuilderClose660SkipAndNotar compares the former synchronous work
// with an entry whose signatures were accumulated as verified votes arrived.
// Both paths build complete, encoded skip and notarization reward certificates.
func BenchmarkBuilderClose660SkipAndNotar(b *testing.B) {
	const rewardSlot = uint64(123)
	var blockID solana.Hash
	blockID[0] = 9
	builder := NewBuilder(BuilderConfig{MaxValidators: 660})
	var rawSignatures [2][][]byte
	for kind, vote := range []alpenglow.Vote{alpenglow.NewSkipVote(rewardSlot), alpenglow.NewNotarizationVote(rewardSlot, blockID)} {
		payload, err := alpenglow.EncodeVotePayloadToSign(vote, 0)
		if err != nil {
			b.Fatal(err)
		}
		point, err := bls12381.HashToG2(payload, []byte(testBLSHashDST))
		if err != nil {
			b.Fatal(err)
		}
		var signature bls12381.G2Affine
		signature.SetInfinity()
		for rank := uint16(0); rank < 660; rank++ {
			signature.Add(&signature, &point)
			raw := signature.RawBytes()
			rawSignatures[kind] = append(rawSignatures[kind], raw[:])
			builder.AddVote(alpenglow.VoteMessage{Vote: vote, Rank: rank, Signature: raw[:]})
		}
	}
	entry := builder.votes[rewardSlot]
	b.Run("legacy_close", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			skipSig, err := aggregateVoteSignatures(rawSignatures[0])
			if err != nil {
				b.Fatal(err)
			}
			notarSig, err := aggregateVoteSignatures(rawSignatures[1])
			if err != nil {
				b.Fatal(err)
			}
			skipBitmap, err := encodeSignerStoreBase2(entry.skip.bits)
			if err != nil {
				b.Fatal(err)
			}
			notarBitmap, err := encodeSignerStoreBase2(entry.notar.partials[blockID].bits)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := EncodeSkipRewardCertificate(SkipRewardCertificate{Slot: rewardSlot, Signature: skipSig, Bitmap: skipBitmap}); err != nil {
				b.Fatal(err)
			}
			if _, err := EncodeNotarRewardCertificate(NotarRewardCertificate{Slot: rewardSlot, BlockID: blockID, Signature: notarSig, Bitmap: notarBitmap}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("incremental_close", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder.votes[rewardSlot] = entry
			result := builder.BuildForLeaderSlot(rewardSlot + SlotsForReward)
			if len(result.Skip) == 0 || len(result.Notar) == 0 {
				b.Fatal("missing reward certificates")
			}
		}
	})
}
