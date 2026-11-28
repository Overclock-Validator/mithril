package alpenglow

import (
	"crypto/sha256"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
)

// verifiedVoteSignature owns a point already admitted by the single or batch
// verifier. Nothing outside this package can manufacture or mutate this binding.
// Keep a digest rather than an alias to the message's mutable signature bytes.
type verifiedVoteSignature struct {
	point      bls12381.G2Affine
	message    VoteMessageKey
	digest     [sha256.Size]byte
	epoch      uint64
	rank       uint16
	stake      uint64
	totalStake uint64
}

// bindVerifiedSignature must only be called after successful verification of
// this exact message under the result's installed epoch and shred-version domain.
func (r *VoteVerifyResult) bindVerifiedSignature(msg VoteMessage, point bls12381.G2Affine) {
	r.signature = &verifiedVoteSignature{
		point:      point,
		message:    msg.Key(),
		digest:     sha256.Sum256(msg.Signature),
		epoch:      r.Epoch,
		rank:       r.Rank,
		stake:      r.Stake,
		totalStake: r.TotalStake,
	}
}

// VerifiedSignature returns an owned copy of the signature point verified for
// this exact message and result. It is unavailable for caller-constructed
// results, or if any signed identity, signature byte, or result binding changed.
// Arrival time and Local are transport metadata and do not change the signature.
// The caller still needs normal consensus admission and reward-slot/epoch rules;
// this cache replaces parsing only, not verification of a different message.
func (v VerifiedVote) VerifiedSignature() (bls12381.G2Affine, bool) {
	proof := v.Result.signature
	if proof == nil || proof.message != v.Message.Key() ||
		proof.epoch != v.Result.Epoch || proof.rank != v.Result.Rank ||
		proof.stake != v.Result.Stake || proof.totalStake != v.Result.TotalStake ||
		proof.digest != sha256.Sum256(v.Message.Signature) {
		return bls12381.G2Affine{}, false
	}
	return proof.point, true
}
