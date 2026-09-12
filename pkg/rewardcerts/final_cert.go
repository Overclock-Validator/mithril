package rewardcerts

import (
	"encoding/binary"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
)

// FinalCertificate is the Alpenglow block footer finalization certificate wire format.
type FinalCertificate struct {
	Slot           uint64
	BlockID        solana.Hash
	FinalAggregate VotesAggregateWire
	NotarAggregate *VotesAggregateWire
}

type VotesAggregateWire struct {
	Signature [blsSignatureCompressedSize]byte
	Bitmap    []byte
}

// ValidatedFinalCert holds BLS-verified finalization signers from a footer final cert.
type ValidatedFinalCert struct {
	FinalSlot uint64
	Signers   map[solana.PublicKey]struct{}
}

func DecodeFinalCertificate(raw []byte) (FinalCertificate, error) {
	if len(raw) < 8+32+blsSignatureCompressedSize+2+1 {
		return FinalCertificate{}, fmt.Errorf("decode final certificate: too short (%d bytes)", len(raw))
	}
	pos := 0
	var fc FinalCertificate
	fc.Slot = binary.LittleEndian.Uint64(raw[pos : pos+8])
	pos += 8
	copy(fc.BlockID[:], raw[pos:pos+32])
	pos += 32

	finalAgg, n, err := decodeVotesAggregate(raw[pos:])
	if err != nil {
		return FinalCertificate{}, fmt.Errorf("decode final aggregate: %w", err)
	}
	fc.FinalAggregate = finalAgg
	pos += n

	if pos >= len(raw) {
		return FinalCertificate{}, fmt.Errorf("decode final certificate: missing notar option")
	}
	switch raw[pos] {
	case 0:
		pos++
	case 1:
		pos++
		notarAgg, n, err := decodeVotesAggregate(raw[pos:])
		if err != nil {
			return FinalCertificate{}, fmt.Errorf("decode notar aggregate: %w", err)
		}
		fc.NotarAggregate = &notarAgg
		pos += n
	default:
		return FinalCertificate{}, fmt.Errorf("decode final certificate: invalid notar option tag %d", raw[pos])
	}
	if pos != len(raw) {
		return FinalCertificate{}, fmt.Errorf("decode final certificate: trailing bytes (%d)", len(raw)-pos)
	}
	return fc, nil
}

func decodeVotesAggregate(raw []byte) (VotesAggregateWire, int, error) {
	if len(raw) < blsSignatureCompressedSize+2 {
		return VotesAggregateWire{}, 0, fmt.Errorf("short votes aggregate")
	}
	var agg VotesAggregateWire
	copy(agg.Signature[:], raw[:blsSignatureCompressedSize])
	bitmapLen := int(binary.LittleEndian.Uint16(raw[blsSignatureCompressedSize : blsSignatureCompressedSize+2]))
	total := blsSignatureCompressedSize + 2 + bitmapLen
	if total > len(raw) {
		return VotesAggregateWire{}, 0, fmt.Errorf("votes aggregate overflow")
	}
	agg.Bitmap = append([]byte(nil), raw[blsSignatureCompressedSize+2:total]...)
	return agg, total, nil
}

// ValidateBlockFinalCertificate verifies a footer final cert and returns participating vote accounts.
func ValidateBlockFinalCertificate(
	finalCertRaw []byte,
	validatorSet alpenglow.ValidatorSet,
	shredVersion uint16,
) (*ValidatedFinalCert, error) {
	if len(finalCertRaw) == 0 {
		return nil, nil
	}
	fc, err := DecodeFinalCertificate(finalCertRaw)
	if err != nil {
		return nil, err
	}

	verifier := alpenglow.NewCertificateVerifier()
	if err := verifier.SetValidatorSet(validatorSet); err != nil {
		return nil, fmt.Errorf("configure validator set: %w", err)
	}
	verifier.SetShredVersion(shredVersion)
	return validateDecodedBlockFinalCertificate(fc, validatorSet, verifier)
}

// ValidateDecodedBlockFinalCertificateWithVerifier verifies an already
// strictly-decoded footer certificate with immutable validator material
// installed in verifier. Exact aggregate-signature verification is retained.
func ValidateDecodedBlockFinalCertificateWithVerifier(
	fc FinalCertificate,
	epoch uint64,
	verifier *alpenglow.CertificateVerifier,
) (*ValidatedFinalCert, error) {
	if verifier == nil {
		return nil, fmt.Errorf("missing certificate verifier")
	}
	validatorSet, ok := verifier.ValidatorSetForEpoch(epoch)
	if !ok {
		return nil, fmt.Errorf("certificate verifier has no validator set for epoch %d", epoch)
	}
	return validateDecodedBlockFinalCertificate(fc, validatorSet, verifier)
}

func validateDecodedBlockFinalCertificate(
	fc FinalCertificate,
	validatorSet alpenglow.ValidatorSet,
	verifier *alpenglow.CertificateVerifier,
) (*ValidatedFinalCert, error) {
	signers := make(map[solana.PublicKey]struct{})
	if fc.NotarAggregate != nil {
		notarSig, err := decompressRewardCertSignature(fc.NotarAggregate.Signature)
		if err != nil {
			return nil, fmt.Errorf("decompress notar signature: %w", err)
		}
		finalizeSig, err := decompressRewardCertSignature(fc.FinalAggregate.Signature)
		if err != nil {
			return nil, fmt.Errorf("decompress finalize signature: %w", err)
		}
		notarCert := alpenglow.Certificate{
			Type:      alpenglow.CertificateNotarize,
			Slot:      fc.Slot,
			BlockHash: fc.BlockID,
			Signature: notarSig,
			Bitmap:    fc.NotarAggregate.Bitmap,
		}
		finalizeCert := alpenglow.Certificate{
			Type:      alpenglow.CertificateFinalize,
			Slot:      fc.Slot,
			Signature: finalizeSig,
			Bitmap:    fc.FinalAggregate.Bitmap,
		}
		if _, _, err := verifier.VerifyCertificateForEpoch(validatorSet.Epoch, notarCert); err != nil {
			return nil, fmt.Errorf("verify notarize final cert: %w", err)
		}
		if _, _, err := verifier.VerifyCertificateForEpoch(validatorSet.Epoch, finalizeCert); err != nil {
			return nil, fmt.Errorf("verify finalize final cert: %w", err)
		}
		if err := collectFinalCertSigners(validatorSet, notarCert, signers); err != nil {
			return nil, err
		}
		if err := collectFinalCertSigners(validatorSet, finalizeCert, signers); err != nil {
			return nil, err
		}
	} else {
		finalizeSig, err := decompressRewardCertSignature(fc.FinalAggregate.Signature)
		if err != nil {
			return nil, fmt.Errorf("decompress fast finalize signature: %w", err)
		}
		fastCert := alpenglow.Certificate{
			Type:      alpenglow.CertificateFinalizeFast,
			Slot:      fc.Slot,
			BlockHash: fc.BlockID,
			Signature: finalizeSig,
			Bitmap:    fc.FinalAggregate.Bitmap,
		}
		if _, _, err := verifier.VerifyCertificateForEpoch(validatorSet.Epoch, fastCert); err != nil {
			return nil, fmt.Errorf("verify fast finalize final cert: %w", err)
		}
		if err := collectFinalCertSigners(validatorSet, fastCert, signers); err != nil {
			return nil, err
		}
	}
	if len(signers) == 0 {
		return nil, fmt.Errorf("final certificate for slot %d has no signers", fc.Slot)
	}
	return &ValidatedFinalCert{
		FinalSlot: fc.Slot,
		Signers:   signers,
	}, nil
}

func collectFinalCertSigners(set alpenglow.ValidatorSet, cert alpenglow.Certificate, signers map[solana.PublicKey]struct{}) error {
	bitmap, err := alpenglow.DecodeSignerStoreBitmap(cert.Bitmap, alpenglow.CertificateBitmapCapacity)
	if err != nil {
		return err
	}
	if bitmap.Encoding == alpenglow.SignerBitmapBase3 {
		return fmt.Errorf("final certificate %s for slot %d uses unsupported base3 bitmap", cert.Type, cert.Slot)
	}
	for rank, included := range bitmap.Union() {
		if !included || rank >= len(set.Validators) {
			continue
		}
		signers[set.Validators[rank].VoteAccount] = struct{}{}
	}
	return nil
}
