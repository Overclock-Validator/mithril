package rewardcerts

import (
	"bytes"
	"encoding/binary"
	"math/big"
	"testing"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestDecodeFinalCertificateRoundTrip(t *testing.T) {
	raw := buildTestFinalCertWire(t)
	fc, err := DecodeFinalCertificate(raw)
	require.NoError(t, err)
	require.Equal(t, uint64(1234567890), fc.Slot)
	require.Equal(t, bytes.Repeat([]byte{1}, 32), fc.BlockID[:])
	require.Len(t, fc.FinalAggregate.Bitmap, 64)
	require.Nil(t, fc.NotarAggregate)

	_, err = DecodeFinalCertificate(raw[:len(raw)-1])
	require.Error(t, err)
}

func buildTestFinalCertWire(t *testing.T) []byte {
	t.Helper()
	var out []byte
	var slotBuf [8]byte
	binary.LittleEndian.PutUint64(slotBuf[:], 1234567890)
	out = append(out, slotBuf[:]...)
	out = append(out, bytes.Repeat([]byte{1}, 32)...)
	out = append(out, make([]byte, 96)...)
	bitmap := bytes.Repeat([]byte{42}, 64)
	var bitmapLen [2]byte
	binary.LittleEndian.PutUint16(bitmapLen[:], uint16(len(bitmap)))
	out = append(out, bitmapLen[:]...)
	out = append(out, bitmap...)
	out = append(out, 0) // notar_aggregate None
	return out
}

func TestValidateSlowFinalCertificateMatchesAgaveBeta1FinalizeSigners(t *testing.T) {
	keys := []*big.Int{big.NewInt(3), big.NewInt(4), big.NewInt(5)}
	validators := make([]alpenglow.ValidatorStake, len(keys))
	for rank, key := range keys {
		var public bls12381.G1Affine
		public.ScalarMultiplicationBase(key)
		var voteAccount solana.PublicKey
		voteAccount[0] = byte(rank + 1)
		validators[rank] = alpenglow.ValidatorStake{
			Rank:                  uint16(rank),
			VoteAccount:           voteAccount,
			BlsPubkeyCompressed:   public.Bytes(),
			BlsPubkeyUncompressed: public.RawBytes(),
			Stake:                 100,
		}
	}
	set := alpenglow.ValidatorSet{Epoch: 7, Validators: validators, TotalStake: 300}
	var blockID solana.Hash
	blockID[0] = 9
	finalizeVote := alpenglow.NewFinalizationVote(77)
	notarizeVote := alpenglow.NewNotarizationVote(77, blockID)

	finalAggregate := compressedAggregateSignature(t, finalizeVote, keys[1], keys[2])
	notarAggregate := compressedAggregateSignature(t, notarizeVote, keys[0], keys[1])
	finalBitmap := mustSignerBitmapBase2(t, 3, 1, 2)
	notarBitmap := mustSignerBitmapBase2(t, 3, 0, 1)

	var raw []byte
	var slot [8]byte
	binary.LittleEndian.PutUint64(slot[:], 77)
	raw = append(raw, slot[:]...)
	raw = append(raw, blockID[:]...)
	raw = appendVotesAggregateWire(raw, finalAggregate, finalBitmap)
	raw = append(raw, 1)
	raw = appendVotesAggregateWire(raw, notarAggregate, notarBitmap)

	validated, err := ValidateBlockFinalCertificate(raw, set, 0)
	require.NoError(t, err)
	require.Len(t, validated.Signers, 2)
	require.NotContains(t, validated.Signers, validators[0].VoteAccount)
	require.Contains(t, validated.Signers, validators[1].VoteAccount)
	require.Contains(t, validated.Signers, validators[2].VoteAccount)

	union, err := ValidateBlockFinalCertificateWithPolicy(raw, set, 0, SlowFinalSignersUnion)
	require.NoError(t, err)
	require.Len(t, union.Signers, 3)
}

func compressedAggregateSignature(t *testing.T, vote alpenglow.Vote, keys ...*big.Int) [96]byte {
	t.Helper()
	payload, err := alpenglow.EncodeVotePayloadToSign(vote, 0)
	require.NoError(t, err)
	message, err := bls12381.HashToG2(payload, []byte(testBLSHashDST))
	require.NoError(t, err)
	var aggregate bls12381.G2Affine
	aggregate.SetInfinity()
	for _, key := range keys {
		var signature bls12381.G2Affine
		signature.ScalarMultiplication(&message, key)
		aggregate.Add(&aggregate, &signature)
	}
	return aggregate.Bytes()
}

func appendVotesAggregateWire(out []byte, signature [96]byte, bitmap []byte) []byte {
	out = append(out, signature[:]...)
	var size [2]byte
	binary.LittleEndian.PutUint16(size[:], uint16(len(bitmap)))
	out = append(out, size[:]...)
	return append(out, bitmap...)
}
