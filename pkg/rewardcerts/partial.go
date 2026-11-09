package rewardcerts

import (
	"fmt"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
)

type partialCert struct {
	aggregate bls12381.G2Jac
	bits      []bool
	count     int
}

func newPartialCert(maxValidators int) partialCert {
	return partialCert{bits: make([]bool, maxValidators)}
}

func (p *partialCert) wantsVote(rank uint16) bool {
	i := int(rank)
	return i >= 0 && i < len(p.bits) && !p.bits[i]
}

func (p *partialCert) addVote(rank uint16, signature []byte) error {
	i := int(rank)
	if i < 0 || i >= len(p.bits) {
		return fmt.Errorf("rewardcerts: invalid vote rank %d", rank)
	}
	if p.bits[i] {
		return fmt.Errorf("rewardcerts: duplicate vote rank %d", rank)
	}
	point, err := decodeVoteSignature(signature)
	if err != nil {
		return err
	}
	return p.addVerifiedSignature(rank, point)
}

// addVerifiedSignature accepts a point that has already passed decoding and
// subgroup checks. Keep the aggregate in Jacobian coordinates while votes
// arrive, so building the footer needs only one affine conversion.
func (p *partialCert) addVerifiedSignature(rank uint16, signature bls12381.G2Affine) error {
	i := int(rank)
	if i < 0 || i >= len(p.bits) {
		return fmt.Errorf("rewardcerts: invalid vote rank %d", rank)
	}
	if p.bits[i] {
		return fmt.Errorf("rewardcerts: duplicate vote rank %d", rank)
	}
	if signature.IsInfinity() {
		return fmt.Errorf("rewardcerts: invalid BLS signature infinity point")
	}
	p.aggregate.AddMixed(&signature)
	p.bits[i] = true
	p.count++
	return nil
}

func (p *partialCert) build() (SkipRewardCertificate, error) {
	if p.count == 0 {
		return SkipRewardCertificate{}, errEmptyBitmap
	}
	bitmap, err := encodeSignerStoreBase2(p.bits)
	if err != nil {
		return SkipRewardCertificate{}, err
	}
	var signature bls12381.G2Affine
	signature.FromJacobian(&p.aggregate)
	if signature.IsInfinity() {
		return SkipRewardCertificate{}, errEmptyBitmap
	}
	return SkipRewardCertificate{Signature: signature.Bytes(), Bitmap: bitmap}, nil
}

func (p *partialCert) votesSeen() int {
	return p.count
}
