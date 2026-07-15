package rewardcerts

import (
	"fmt"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
)

// ValidatedRewardCert is a BLS-verified union of validators from skip/notar reward certs.
type ValidatedRewardCert struct {
	RewardSlot uint64
	Validators map[solana.PublicKey]struct{}
}

// ValidateRewardCertificates verifies footer reward certs and returns participating vote accounts.
func ValidateRewardCertificates(currentSlot uint64, skipRaw, notarRaw []byte, validatorSet alpenglow.ValidatorSet, shredVersion uint16) (*ValidatedRewardCert, error) {
	var skipCert *SkipRewardCertificate
	var notarCert *NotarRewardCertificate

	if len(skipRaw) > 0 {
		decoded, err := DecodeSkipRewardCertificate(skipRaw)
		if err != nil {
			return nil, fmt.Errorf("decode skip reward cert: %w", err)
		}
		skipCert = &decoded
	}
	if len(notarRaw) > 0 {
		decoded, err := DecodeNotarRewardCertificate(notarRaw)
		if err != nil {
			return nil, fmt.Errorf("decode notar reward cert: %w", err)
		}
		notarCert = &decoded
	}

	rewardSlot, err := extractRewardSlot(currentSlot, skipCert, notarCert)
	if err != nil {
		return nil, err
	}
	if rewardSlot == nil {
		return nil, nil
	}

	validators := make(map[solana.PublicKey]struct{})
	verifier := alpenglow.NewCertificateVerifier()
	if err := verifier.SetValidatorSet(validatorSet); err != nil {
		return nil, fmt.Errorf("configure validator set: %w", err)
	}
	verifier.SetShredVersion(shredVersion)

	if skipCert != nil {
		if err := verifyAndCollectRewardCert(verifier, validatorSet, skipCertToCertificate(*skipCert), validators); err != nil {
			return nil, fmt.Errorf("verify skip reward cert: %w", err)
		}
	}
	if notarCert != nil {
		if err := verifyAndCollectRewardCert(verifier, validatorSet, notarCertToCertificate(*notarCert), validators); err != nil {
			return nil, fmt.Errorf("verify notar reward cert: %w", err)
		}
	}
	if len(validators) == 0 {
		return nil, nil
	}
	return &ValidatedRewardCert{
		RewardSlot: *rewardSlot,
		Validators: validators,
	}, nil
}

func extractRewardSlot(currentSlot uint64, skip *SkipRewardCertificate, notar *NotarRewardCertificate) (*uint64, error) {
	if skip == nil && notar == nil {
		return nil, nil
	}
	var slot uint64
	switch {
	case skip != nil && notar != nil:
		if skip.Slot != notar.Slot {
			return nil, fmt.Errorf("rewardcerts: skip slot %d != notar slot %d at current slot %d", skip.Slot, notar.Slot, currentSlot)
		}
		slot = skip.Slot
	case skip != nil:
		slot = skip.Slot
	default:
		slot = notar.Slot
	}
	if slot+SlotsForReward != currentSlot {
		return nil, fmt.Errorf("rewardcerts: invalid reward slot %d for current slot %d", slot, currentSlot)
	}
	return &slot, nil
}

func skipCertToCertificate(cert SkipRewardCertificate) alpenglow.Certificate {
	sig, _ := decompressRewardCertSignature(cert.Signature)
	return alpenglow.Certificate{
		Type:      alpenglow.CertificateSkip,
		Slot:      cert.Slot,
		Signature: sig,
		Bitmap:    cert.Bitmap,
	}
}

func notarCertToCertificate(cert NotarRewardCertificate) alpenglow.Certificate {
	sig, _ := decompressRewardCertSignature(cert.Signature)
	return alpenglow.Certificate{
		Type:      alpenglow.CertificateNotarize,
		Slot:      cert.Slot,
		BlockHash: cert.BlockID,
		Signature: sig,
		Bitmap:    cert.Bitmap,
	}
}

func verifyAndCollectRewardCert(verifier *alpenglow.CertificateVerifier, set alpenglow.ValidatorSet, cert alpenglow.Certificate, validators map[solana.PublicKey]struct{}) error {
	if err := verifier.VerifyRewardCertificateForEpoch(set.Epoch, cert); err != nil {
		return err
	}
	bitmap, err := alpenglow.DecodeSignerStoreBitmap(cert.Bitmap, alpenglow.CertificateBitmapCapacity)
	if err != nil {
		return err
	}
	for rank, included := range bitmap.Union() {
		if !included || rank >= len(set.Validators) {
			continue
		}
		validators[set.Validators[rank].VoteAccount] = struct{}{}
	}
	return nil
}

func decompressRewardCertSignature(compressed [blsSignatureCompressedSize]byte) ([]byte, error) {
	var sig bls12381.G2Affine
	if err := sig.Unmarshal(compressed[:]); err != nil {
		return nil, fmt.Errorf("decompress BLS signature: %w", err)
	}
	raw := sig.RawBytes()
	return raw[:], nil
}
