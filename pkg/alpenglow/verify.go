package alpenglow

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/gagliardetto/solana-go"
)

const (
	// CertificateBitmapCapacity is Agave's signer-bitmap wire bound. It is
	// deliberately larger than the active VAT cap: certificates with a legal
	// zero tail must still decode compatibly.
	CertificateBitmapCapacity = 4096
	// MaximumVATValidators is SIMD-0357's active validator-set cap.
	MaximumVATValidators    = 2000
	signerStoreVersionBase2 = byte(0)
	signerStoreVersionBase3 = byte(1)
	signerStoreHeaderLen    = 3
	base3SymbolsPerByte     = 5
	blsHashToPointDST       = "BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_"
)

type ValidatorStake struct {
	Rank                  uint16
	VoteAccount           solana.PublicKey
	NodePubkey            solana.PublicKey
	BlsPubkeyCompressed   [48]byte
	BlsPubkeyUncompressed [96]byte
	Stake                 uint64
}

type ValidatorSet struct {
	Epoch      uint64
	Validators []ValidatorStake
	TotalStake uint64
}

type SignerBitmapEncoding string

const (
	SignerBitmapBase2 SignerBitmapEncoding = "base2"
	SignerBitmapBase3 SignerBitmapEncoding = "base3"
)

type SignerBitmap struct {
	Encoding SignerBitmapEncoding
	Length   int
	Base     []bool
	Fallback []bool
}

type CertificateVerifyResult struct {
	Epoch             uint64
	SignerCount       int
	IncludedStake     uint64
	TotalStake        uint64
	StakeVerified     bool
	SignatureVerified bool
}

type VoteVerifyResult struct {
	Epoch      uint64
	Rank       uint16
	Stake      uint64
	TotalStake uint64
}

type VoteSignatureDiagnostics struct {
	Epoch             uint64
	ValidatorCount    int
	AdvertisedRank    uint16
	AdvertisedSigner  *SignerSample
	PayloadLen        int
	PayloadHex        string
	SignatureLen      int
	SignatureHex      string
	MatchCount        int
	MatchSamples      []SignerSample
	DiagnosticError   string
	AdvertisedRankErr string
	Epochs            []EpochVoteSignatureDiagnostics
}

type EpochVoteSignatureDiagnostics struct {
	Epoch             uint64
	ValidatorCount    int
	AdvertisedSigner  *SignerSample
	AdvertisedRankErr string
	MatchCount        int
	MatchSamples      []SignerSample
}

type SignerSample struct {
	Rank            uint16
	Stake           uint64
	VoteAccount     solana.PublicKey
	NodePubkey      solana.PublicKey
	BLSPubkeyPrefix string
	BLSPubkeyHex    string
}

type CertificateDiagnostics struct {
	Epoch               uint64
	ValidatorCount      int
	BitmapEncoding      SignerBitmapEncoding
	BitmapLength        int
	BitmapBytes         int
	BitmapError         string
	SignerCount         int
	BaseSignerCount     int
	FallbackSignerCount int
	IncludedStake       uint64
	TotalStake          uint64
	PrimaryVote         Vote
	FallbackVote        Vote
	HasFallbackVote     bool
	PrimaryPayloadLen   int
	FallbackPayloadLen  int
	BaseRanks           []uint16
	FallbackRanks       []uint16
	SignerSamples       []SignerSample
}

type CertificateVerifier struct {
	mu                  sync.RWMutex
	sets                map[uint64]ValidatorSet
	latestEpoch         uint64
	maxValidators       int
	clusterShredVersion uint16
}

func NewCertificateVerifier() *CertificateVerifier {
	return &CertificateVerifier{
		sets:          make(map[uint64]ValidatorSet),
		maxValidators: MaximumVATValidators,
	}
}

// SetShredVersion configures the cluster shred version used when deriving BLS vote
// payloads for signature verification. Agave signs VotePayloadToSign with the
// local shred version; verifiers must use the same value.
func (v *CertificateVerifier) SetShredVersion(version uint16) {
	v.mu.Lock()
	v.clusterShredVersion = version
	v.mu.Unlock()
}

func (v *CertificateVerifier) shredVersion() uint16 {
	v.mu.RLock()
	version := v.clusterShredVersion
	v.mu.RUnlock()
	return version
}

func (v *CertificateVerifier) ShredVersion() uint16 { return v.shredVersion() }

func (v *CertificateVerifier) ValidatorSetForEpoch(epoch uint64) (ValidatorSet, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	set, ok := v.sets[epoch]
	return set, ok
}

func votePayloadBytes(shredVersion uint16, vote Vote) ([]byte, error) {
	return EncodeVotePayloadToSign(vote, shredVersion)
}

func BuildValidatorSet(epoch uint64, stakes map[solana.PublicKey]uint64, voteAccounts map[solana.PublicKey]*epochstakes.VoteAccount, totalStake uint64) (ValidatorSet, error) {
	if len(stakes) == 0 {
		return ValidatorSet{}, fmt.Errorf("alpenglow verifier: no epoch stakes for epoch %d", epoch)
	}
	if totalStake == 0 {
		for _, stake := range stakes {
			totalStake += stake
		}
	}
	if totalStake == 0 {
		return ValidatorSet{}, fmt.Errorf("alpenglow verifier: total stake is zero for epoch %d", epoch)
	}

	entries := make([]ValidatorStake, 0, len(stakes))
	for voteAcct, stake := range stakes {
		if stake == 0 {
			continue
		}
		voteMeta := voteAccounts[voteAcct]
		if voteMeta == nil || voteMeta.BlsPubkeyCompressed == nil {
			continue
		}
		blsRaw, err := decompressBLSPubkey(voteMeta.BlsPubkeyCompressed[:])
		if err != nil {
			continue
		}
		entries = append(entries, ValidatorStake{
			VoteAccount:           voteAcct,
			NodePubkey:            voteMeta.NodePubkey,
			BlsPubkeyCompressed:   *voteMeta.BlsPubkeyCompressed,
			BlsPubkeyUncompressed: blsRaw,
			Stake:                 stake,
		})
	}
	if len(entries) == 0 {
		return ValidatorSet{}, fmt.Errorf("alpenglow verifier: no BLS-ranked validators for epoch %d", epoch)
	}
	entries = capValidatorStakeEntries(entries, MaximumVATValidators)
	// cavey TODO: do we really need this?
	if len(entries) == 0 {
		return ValidatorSet{}, fmt.Errorf("alpenglow verifier: no validators remain after VAT stake cap for epoch %d", epoch)
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Stake != entries[j].Stake {
			return entries[i].Stake > entries[j].Stake
		}
		return bytes.Compare(entries[i].BlsPubkeyCompressed[:], entries[j].BlsPubkeyCompressed[:]) < 0
	})
	for i := range entries {
		entries[i].Rank = uint16(i)
	}
	return ValidatorSet{Epoch: epoch, Validators: entries, TotalStake: totalStake}, nil
}

func capValidatorStakeEntries(entries []ValidatorStake, maxValidators int) []ValidatorStake {
	if len(entries) <= maxValidators {
		return entries
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Stake != entries[j].Stake {
			return entries[i].Stake > entries[j].Stake
		}
		return bytes.Compare(entries[i].BlsPubkeyCompressed[:], entries[j].BlsPubkeyCompressed[:]) < 0
	})
	cutoffStake := entries[maxValidators].Stake
	capped := make([]ValidatorStake, 0, maxValidators)
	for _, entry := range entries {
		if entry.Stake > cutoffStake {
			capped = append(capped, entry)
		}
	}
	return capped
}

func (v *CertificateVerifier) SetValidatorSet(set ValidatorSet) error {
	if len(set.Validators) == 0 {
		return fmt.Errorf("alpenglow verifier: validator set for epoch %d is empty", set.Epoch)
	}
	if len(set.Validators) > v.maxValidators {
		return fmt.Errorf("alpenglow verifier: validator set for epoch %d has %d validators, max %d", set.Epoch, len(set.Validators), v.maxValidators)
	}
	if set.TotalStake == 0 {
		return fmt.Errorf("alpenglow verifier: validator set for epoch %d has zero total stake", set.Epoch)
	}

	copied := ValidatorSet{
		Epoch:      set.Epoch,
		Validators: append([]ValidatorStake(nil), set.Validators...),
		TotalStake: set.TotalStake,
	}
	for rank, validator := range copied.Validators {
		if validator.Rank != uint16(rank) {
			return fmt.Errorf("alpenglow verifier: validator set epoch %d has rank mismatch at index %d: %d", set.Epoch, rank, validator.Rank)
		}
		if validator.Stake == 0 {
			return fmt.Errorf("alpenglow verifier: validator set epoch %d has zero stake at rank %d", set.Epoch, rank)
		}
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	v.sets[copied.Epoch] = copied
	if copied.Epoch >= v.latestEpoch {
		v.latestEpoch = copied.Epoch
	}
	return nil
}

func (v *CertificateVerifier) VerifyCertificateStake(cert Certificate) (Certificate, CertificateVerifyResult, error) {
	v.mu.RLock()
	epoch := v.latestEpoch
	_, ok := v.sets[epoch]
	v.mu.RUnlock()
	if !ok {
		return cert, CertificateVerifyResult{}, fmt.Errorf("alpenglow verifier: no validator set configured")
	}
	return v.VerifyCertificateStakeForEpoch(epoch, cert)
}

func (v *CertificateVerifier) VerifyCertificate(cert Certificate) (Certificate, CertificateVerifyResult, error) {
	v.mu.RLock()
	epoch := v.latestEpoch
	_, ok := v.sets[epoch]
	v.mu.RUnlock()
	if !ok {
		return cert, CertificateVerifyResult{}, fmt.Errorf("alpenglow verifier: no validator set configured")
	}
	return v.VerifyCertificateForEpoch(epoch, cert)
}

// VerifyRewardCertificateForEpoch verifies a footer reward certificate the way Agave
// ValidatedRewardCert::try_new does: BLS signature + bitmap only, without the 60%
// consensus stake threshold. Reward certs aggregate skip/notar voters for payout, not
// a supermajority certificate.
func (v *CertificateVerifier) VerifyRewardCertificateForEpoch(epoch uint64, cert Certificate) error {
	v.mu.RLock()
	set, ok := v.sets[epoch]
	v.mu.RUnlock()
	if !ok {
		return fmt.Errorf("alpenglow verifier: no validator set for epoch %d", epoch)
	}
	return verifyRewardCertificateWithSet(set, cert, v.shredVersion())
}

func verifyRewardCertificateWithSet(set ValidatorSet, cert Certificate, shredVersion uint16) error {
	if err := validateChainCertificateForVerifier(cert); err != nil {
		return err
	}
	if len(cert.Signature) != BLSSignatureSize {
		return fmt.Errorf("alpenglow verifier: %s reward certificate for slot %d has invalid signature length %d", cert.Type, cert.Slot, len(cert.Signature))
	}
	bitmap, err := DecodeSignerStoreBitmap(cert.Bitmap, CertificateBitmapCapacity)
	if err != nil {
		return err
	}
	return verifyCertificateSignatureWithSet(set, cert, bitmap, shredVersion)
}

func (v *CertificateVerifier) VerifyCertificateStakeForEpoch(epoch uint64, cert Certificate) (Certificate, CertificateVerifyResult, error) {
	v.mu.RLock()
	set, ok := v.sets[epoch]
	v.mu.RUnlock()
	if !ok {
		return cert, CertificateVerifyResult{}, fmt.Errorf("alpenglow verifier: no validator set for epoch %d", epoch)
	}
	return verifyCertificateWithSet(set, cert, false, v.shredVersion())
}

func (v *CertificateVerifier) VerifyCertificateForEpoch(epoch uint64, cert Certificate) (Certificate, CertificateVerifyResult, error) {
	v.mu.RLock()
	set, ok := v.sets[epoch]
	v.mu.RUnlock()
	if !ok {
		return cert, CertificateVerifyResult{}, fmt.Errorf("alpenglow verifier: no validator set for epoch %d", epoch)
	}
	return verifyCertificateWithSet(set, cert, true, v.shredVersion())
}

func (v *CertificateVerifier) VerifyVoteMessage(msg VoteMessage) (VoteVerifyResult, error) {
	if err := msg.ValidateBasic(); err != nil {
		return VoteVerifyResult{}, err
	}
	if len(msg.Signature) != BLSSignatureSize {
		return VoteVerifyResult{}, fmt.Errorf("alpenglow verifier: %s vote for slot %d rank %d has invalid signature length %d", msg.Vote.Type, msg.Vote.Slot, msg.Rank, len(msg.Signature))
	}

	v.mu.RLock()
	epoch := v.latestEpoch
	set, ok := v.sets[epoch]
	v.mu.RUnlock()
	if !ok {
		return VoteVerifyResult{}, fmt.Errorf("alpenglow verifier: no validator set configured")
	}
	return verifyVoteMessageWithSet(set, msg, v.shredVersion())
}

func (v *CertificateVerifier) VerifyVoteMessageForEpoch(epoch uint64, msg VoteMessage) (VoteVerifyResult, error) {
	if err := msg.ValidateBasic(); err != nil {
		return VoteVerifyResult{}, err
	}
	if len(msg.Signature) != BLSSignatureSize {
		return VoteVerifyResult{}, fmt.Errorf("alpenglow verifier: %s vote for slot %d rank %d has invalid signature length %d", msg.Vote.Type, msg.Vote.Slot, msg.Rank, len(msg.Signature))
	}

	v.mu.RLock()
	set, ok := v.sets[epoch]
	v.mu.RUnlock()
	if !ok {
		return VoteVerifyResult{}, fmt.Errorf("alpenglow verifier: no validator set for epoch %d", epoch)
	}
	return verifyVoteMessageWithSet(set, msg, v.shredVersion())
}

func (v *CertificateVerifier) DiagnoseCertificate(cert Certificate, maxSamples int) CertificateDiagnostics {
	if maxSamples < 0 {
		maxSamples = 0
	}

	v.mu.RLock()
	epoch := v.latestEpoch
	set, ok := v.sets[epoch]
	v.mu.RUnlock()
	if !ok {
		return CertificateDiagnostics{Epoch: epoch, BitmapError: "no validator set configured"}
	}
	return diagnoseCertificateWithSet(set, cert, maxSamples, v.shredVersion())
}

func (v *CertificateVerifier) DiagnoseCertificateForEpoch(epoch uint64, cert Certificate, maxSamples int) CertificateDiagnostics {
	if maxSamples < 0 {
		maxSamples = 0
	}

	v.mu.RLock()
	set, ok := v.sets[epoch]
	v.mu.RUnlock()
	if !ok {
		return CertificateDiagnostics{Epoch: epoch, BitmapError: fmt.Sprintf("no validator set for epoch %d", epoch)}
	}
	return diagnoseCertificateWithSet(set, cert, maxSamples, v.shredVersion())
}

func (v *CertificateVerifier) DiagnoseVoteMessage(msg VoteMessage, maxMatches int) VoteSignatureDiagnostics {
	return v.diagnoseVoteMessageForEpoch(nil, msg, maxMatches)
}

func (v *CertificateVerifier) DiagnoseVoteMessageForEpoch(epoch uint64, msg VoteMessage, maxMatches int) VoteSignatureDiagnostics {
	return v.diagnoseVoteMessageForEpoch(&epoch, msg, maxMatches)
}

func (v *CertificateVerifier) diagnoseVoteMessageForEpoch(focusEpoch *uint64, msg VoteMessage, maxMatches int) VoteSignatureDiagnostics {
	if maxMatches < 0 {
		maxMatches = 0
	}
	diag := VoteSignatureDiagnostics{
		AdvertisedRank: msg.Rank,
		SignatureLen:   len(msg.Signature),
	}
	if err := msg.ValidateBasic(); err != nil {
		diag.DiagnosticError = err.Error()
		return diag
	}

	v.mu.RLock()
	focusedEpoch := v.latestEpoch
	if focusEpoch != nil {
		focusedEpoch = *focusEpoch
	}
	set, ok := v.sets[focusedEpoch]
	sets := make([]ValidatorSet, 0, len(v.sets))
	for _, epochSet := range v.sets {
		sets = append(sets, epochSet)
	}
	v.mu.RUnlock()
	if !ok {
		diag.Epoch = focusedEpoch
		if focusEpoch != nil {
			diag.DiagnosticError = fmt.Sprintf("no validator set for epoch %d", focusedEpoch)
		} else {
			diag.DiagnosticError = "no validator set configured"
		}
		return diag
	}
	diag.Epoch = set.Epoch
	diag.ValidatorCount = len(set.Validators)
	if int(msg.Rank) < len(set.Validators) {
		sample := validatorSignerSample(set.Validators[msg.Rank])
		diag.AdvertisedSigner = &sample
	} else {
		diag.AdvertisedRankErr = fmt.Sprintf("rank %d exceeds validator set len %d", msg.Rank, len(set.Validators))
	}

	payload, err := votePayloadBytes(v.shredVersion(), msg.Vote)
	if err != nil {
		diag.DiagnosticError = err.Error()
		return diag
	}
	diag.PayloadLen = len(payload)
	diag.PayloadHex = hex.EncodeToString(payload)
	diag.SignatureHex = hex.EncodeToString(msg.Signature)

	signature, err := decodeBLSSignature(msg.Signature)
	if err != nil {
		diag.DiagnosticError = err.Error()
		return diag
	}

	for _, epochSet := range sortedValidatorSets(sets) {
		epochDiag := diagnoseVoteMessageWithSet(epochSet, msg, payload, signature, maxMatches)
		diag.Epochs = append(diag.Epochs, epochDiag)
		if epochSet.Epoch == set.Epoch {
			diag.MatchCount = epochDiag.MatchCount
			diag.MatchSamples = epochDiag.MatchSamples
		}
	}
	return diag
}

func sortedValidatorSets(sets []ValidatorSet) []ValidatorSet {
	sort.Slice(sets, func(i, j int) bool {
		return sets[i].Epoch < sets[j].Epoch
	})
	return sets
}

func diagnoseVoteMessageWithSet(set ValidatorSet, msg VoteMessage, payload []byte, signature bls12381.G2Affine, maxMatches int) EpochVoteSignatureDiagnostics {
	diag := EpochVoteSignatureDiagnostics{
		Epoch:          set.Epoch,
		ValidatorCount: len(set.Validators),
	}
	if int(msg.Rank) < len(set.Validators) {
		sample := validatorSignerSample(set.Validators[msg.Rank])
		diag.AdvertisedSigner = &sample
	} else {
		diag.AdvertisedRankErr = fmt.Sprintf("rank %d exceeds validator set len %d", msg.Rank, len(set.Validators))
	}

	for rank, validator := range set.Validators {
		if validator.Stake == 0 {
			continue
		}
		var pubkey bls12381.G1Affine
		if _, err := pubkey.SetBytes(validator.BlsPubkeyCompressed[:]); err != nil {
			continue
		}
		if pubkey.IsInfinity() {
			continue
		}
		if err := verifyBLSSignature(pubkey, payload, signature); err != nil {
			continue
		}
		diag.MatchCount++
		if len(diag.MatchSamples) < maxMatches {
			sample := validatorSignerSample(validator)
			sample.Rank = uint16(rank)
			diag.MatchSamples = append(diag.MatchSamples, sample)
		}
	}
	return diag
}

func verifyCertificateStakeWithSet(set ValidatorSet, cert Certificate, shredVersion uint16) (Certificate, CertificateVerifyResult, error) {
	return verifyCertificateWithSet(set, cert, false, shredVersion)
}

func verifyCertificateWithSet(set ValidatorSet, cert Certificate, verifySignature bool, shredVersion uint16) (Certificate, CertificateVerifyResult, error) {
	if err := validateChainCertificateForVerifier(cert); err != nil {
		return cert, CertificateVerifyResult{}, err
	}
	if verifySignature && len(cert.Signature) != BLSSignatureSize {
		return cert, CertificateVerifyResult{}, fmt.Errorf("alpenglow verifier: %s certificate for slot %d has invalid signature length %d", cert.Type, cert.Slot, len(cert.Signature))
	}
	bitmap, err := DecodeSignerStoreBitmap(cert.Bitmap, CertificateBitmapCapacity)
	if err != nil {
		return cert, CertificateVerifyResult{}, err
	}

	includedRanks := bitmap.Union()
	var includedStake uint64
	for rank, included := range includedRanks {
		if !included {
			continue
		}
		if rank >= len(set.Validators) {
			return cert, CertificateVerifyResult{}, fmt.Errorf("alpenglow verifier: signer rank %d exceeds validator set len %d", rank, len(set.Validators))
		}
		includedStake += set.Validators[rank].Stake
	}

	cert.IncludedStake = includedStake
	cert.TotalStake = set.TotalStake
	cert.StakeVerified = true
	if err := cert.ValidateBasic(); err != nil {
		cert.StakeVerified = false
		return cert, CertificateVerifyResult{
			Epoch:             set.Epoch,
			SignerCount:       countBool(includedRanks),
			IncludedStake:     includedStake,
			TotalStake:        set.TotalStake,
			SignatureVerified: cert.SignatureVerified,
		}, err
	}
	if verifySignature {
		if err := verifyCertificateSignatureWithSet(set, cert, bitmap, shredVersion); err != nil {
			return cert, CertificateVerifyResult{
				Epoch:         set.Epoch,
				SignerCount:   countBool(includedRanks),
				IncludedStake: includedStake,
				TotalStake:    set.TotalStake,
				StakeVerified: true,
			}, err
		}
		cert.SignatureVerified = true
	}

	return cert, CertificateVerifyResult{
		Epoch:             set.Epoch,
		SignerCount:       countBool(includedRanks),
		IncludedStake:     includedStake,
		TotalStake:        set.TotalStake,
		StakeVerified:     true,
		SignatureVerified: cert.SignatureVerified,
	}, nil
}

func verifyVoteMessageWithSet(set ValidatorSet, msg VoteMessage, shredVersion uint16) (VoteVerifyResult, error) {
	if int(msg.Rank) >= len(set.Validators) {
		return VoteVerifyResult{}, fmt.Errorf("alpenglow verifier: %s vote for slot %d rank %d exceeds validator set len %d", msg.Vote.Type, msg.Vote.Slot, msg.Rank, len(set.Validators))
	}
	validator := set.Validators[msg.Rank]
	if validator.Stake == 0 {
		return VoteVerifyResult{}, fmt.Errorf("alpenglow verifier: %s vote for slot %d rank %d has zero stake", msg.Vote.Type, msg.Vote.Slot, msg.Rank)
	}
	payload, err := votePayloadBytes(shredVersion, msg.Vote)
	if err != nil {
		return VoteVerifyResult{}, err
	}
	var pubkey bls12381.G1Affine
	if _, err := pubkey.SetBytes(validator.BlsPubkeyCompressed[:]); err != nil {
		return VoteVerifyResult{}, fmt.Errorf("alpenglow verifier: invalid BLS pubkey at rank %d: %w", msg.Rank, err)
	}
	if pubkey.IsInfinity() {
		return VoteVerifyResult{}, fmt.Errorf("alpenglow verifier: invalid BLS pubkey infinity point at rank %d", msg.Rank)
	}
	signature, err := decodeBLSSignature(msg.Signature)
	if err != nil {
		return VoteVerifyResult{}, err
	}
	if err := verifyBLSSignature(pubkey, payload, signature); err != nil {
		return VoteVerifyResult{}, fmt.Errorf("alpenglow verifier: %s vote for slot %d rank %d failed BLS signature verification: %w", msg.Vote.Type, msg.Vote.Slot, msg.Rank, err)
	}
	return VoteVerifyResult{
		Epoch:      set.Epoch,
		Rank:       msg.Rank,
		Stake:      validator.Stake,
		TotalStake: set.TotalStake,
	}, nil
}

type blsVerifyTerm struct {
	pubkey  bls12381.G1Affine
	message bls12381.G2Affine
}

func verifyCertificateSignatureWithSet(set ValidatorSet, cert Certificate, bitmap SignerBitmap, shredVersion uint16) error {
	primaryVote, fallbackVote, hasFallback, err := certificateVotePayloads(cert)
	if err != nil {
		return err
	}
	primaryPayload, err := votePayloadBytes(shredVersion, primaryVote)
	if err != nil {
		return err
	}

	signature, err := decodeBLSSignature(cert.Signature)
	if err != nil {
		return err
	}

	switch bitmap.Encoding {
	case SignerBitmapBase2:
		term, ok, err := aggregateVerificationTerm(set, bitmap.Base, primaryPayload)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("alpenglow verifier: %s certificate for slot %d has no signers", cert.Type, cert.Slot)
		}
		return verifyAggregateSignature(cert, []blsVerifyTerm{term}, signature)
	case SignerBitmapBase3:
		if !hasFallback {
			return fmt.Errorf("alpenglow verifier: %s certificate for slot %d uses base3 signer bitmap without fallback vote", cert.Type, cert.Slot)
		}
		if err := bitmap.CheckDisjoint(); err != nil {
			return err
		}
		fallbackPayload, err := votePayloadBytes(shredVersion, fallbackVote)
		if err != nil {
			return err
		}
		terms := make([]blsVerifyTerm, 0, 2)
		if term, ok, err := aggregateVerificationTerm(set, bitmap.Base, primaryPayload); err != nil {
			return err
		} else if ok {
			terms = append(terms, term)
		}
		if term, ok, err := aggregateVerificationTerm(set, bitmap.Fallback, fallbackPayload); err != nil {
			return err
		} else if ok {
			terms = append(terms, term)
		}
		if len(terms) == 0 {
			return fmt.Errorf("alpenglow verifier: %s certificate for slot %d has no signers", cert.Type, cert.Slot)
		}
		return verifyAggregateSignature(cert, terms, signature)
	default:
		return fmt.Errorf("alpenglow verifier: unsupported signer bitmap encoding %q", bitmap.Encoding)
	}
}

func certificateVotePayloads(cert Certificate) (Vote, Vote, bool, error) {
	switch cert.Type {
	case CertificateNotarize, CertificateFinalizeFast:
		return NewNotarizationVote(cert.Slot, cert.BlockHash), Vote{}, false, nil
	case CertificateFinalize:
		return NewFinalizationVote(cert.Slot), Vote{}, false, nil
	case CertificateGenesis:
		return NewGenesisVote(cert.Slot, cert.BlockHash), Vote{}, false, nil
	case CertificateNotarizeFallback:
		return NewNotarizationVote(cert.Slot, cert.BlockHash), NewNotarizationFallbackVote(cert.Slot, cert.BlockHash), true, nil
	case CertificateSkip:
		return NewSkipVote(cert.Slot), NewSkipFallbackVote(cert.Slot), true, nil
	default:
		return Vote{}, Vote{}, false, fmt.Errorf("alpenglow verifier: invalid certificate type %q", cert.Type)
	}
}

func decodeBLSSignature(data []byte) (bls12381.G2Affine, error) {
	var signature bls12381.G2Affine
	if len(data) != BLSSignatureSize {
		return signature, fmt.Errorf("alpenglow verifier: invalid BLS signature length %d", len(data))
	}
	if _, err := signature.SetBytes(data); err != nil {
		return signature, fmt.Errorf("alpenglow verifier: invalid BLS signature: %w", err)
	}
	if signature.IsInfinity() {
		return signature, fmt.Errorf("alpenglow verifier: invalid BLS signature infinity point")
	}
	return signature, nil
}

func verifyBLSSignature(pubkey bls12381.G1Affine, payload []byte, signature bls12381.G2Affine) error {
	message, err := bls12381.HashToG2(payload, []byte(blsHashToPointDST))
	if err != nil {
		return fmt.Errorf("hash vote payload to BLS G2: %w", err)
	}
	_, _, g1Generator, _ := bls12381.Generators()
	var negGenerator bls12381.G1Affine
	negGenerator.Neg(&g1Generator)
	ok, err := bls12381.PairingCheck(
		[]bls12381.G1Affine{pubkey, negGenerator},
		[]bls12381.G2Affine{message, signature},
	)
	if err != nil {
		return fmt.Errorf("BLS pairing check failed: %w", err)
	}
	if !ok {
		return fmt.Errorf("pairing mismatch")
	}
	return nil
}

func aggregateVerificationTerm(set ValidatorSet, ranks []bool, payload []byte) (blsVerifyTerm, bool, error) {
	var aggregate bls12381.G1Affine
	aggregate.SetInfinity()
	var signerCount int
	for rank, included := range ranks {
		if !included {
			continue
		}
		if rank >= len(set.Validators) {
			return blsVerifyTerm{}, false, fmt.Errorf("alpenglow verifier: signer rank %d exceeds validator set len %d", rank, len(set.Validators))
		}
		var pubkey bls12381.G1Affine
		if _, err := pubkey.SetBytes(set.Validators[rank].BlsPubkeyCompressed[:]); err != nil {
			return blsVerifyTerm{}, false, fmt.Errorf("alpenglow verifier: invalid BLS pubkey at rank %d: %w", rank, err)
		}
		if pubkey.IsInfinity() {
			return blsVerifyTerm{}, false, fmt.Errorf("alpenglow verifier: invalid BLS pubkey infinity point at rank %d", rank)
		}
		aggregate.Add(&aggregate, &pubkey)
		signerCount++
	}
	if signerCount == 0 {
		return blsVerifyTerm{}, false, nil
	}
	message, err := bls12381.HashToG2(payload, []byte(blsHashToPointDST))
	if err != nil {
		return blsVerifyTerm{}, false, fmt.Errorf("alpenglow verifier: hash vote payload to BLS G2: %w", err)
	}
	return blsVerifyTerm{pubkey: aggregate, message: message}, true, nil
}

func verifyAggregateSignature(cert Certificate, terms []blsVerifyTerm, signature bls12381.G2Affine) error {
	if len(terms) == 0 {
		return fmt.Errorf("alpenglow verifier: %s certificate for slot %d has no aggregate verification terms", cert.Type, cert.Slot)
	}
	g1Points := make([]bls12381.G1Affine, 0, len(terms)+1)
	g2Points := make([]bls12381.G2Affine, 0, len(terms)+1)
	for _, term := range terms {
		if term.pubkey.IsInfinity() {
			continue
		}
		g1Points = append(g1Points, term.pubkey)
		g2Points = append(g2Points, term.message)
	}
	if len(g1Points) == 0 {
		return fmt.Errorf("alpenglow verifier: %s certificate for slot %d has no non-empty aggregate public key", cert.Type, cert.Slot)
	}
	_, _, g1Generator, _ := bls12381.Generators()
	var negGenerator bls12381.G1Affine
	negGenerator.Neg(&g1Generator)
	g1Points = append(g1Points, negGenerator)
	g2Points = append(g2Points, signature)

	ok, err := bls12381.PairingCheck(g1Points, g2Points)
	if err != nil {
		return fmt.Errorf("alpenglow verifier: BLS aggregate pairing check failed: %w", err)
	}
	if !ok {
		return fmt.Errorf("alpenglow verifier: %s certificate for slot %d failed aggregate BLS signature verification", cert.Type, cert.Slot)
	}
	return nil
}

func diagnoseCertificateWithSet(set ValidatorSet, cert Certificate, maxSamples int, shredVersion uint16) CertificateDiagnostics {
	diag := CertificateDiagnostics{
		Epoch:          set.Epoch,
		ValidatorCount: len(set.Validators),
		BitmapBytes:    len(cert.Bitmap),
		TotalStake:     set.TotalStake,
	}
	bitmap, err := DecodeSignerStoreBitmap(cert.Bitmap, CertificateBitmapCapacity)
	if err != nil {
		diag.BitmapError = err.Error()
		return diag
	}
	diag.BitmapEncoding = bitmap.Encoding
	diag.BitmapLength = bitmap.Length

	primaryVote, fallbackVote, hasFallback, err := certificateVotePayloads(cert)
	if err == nil {
		diag.PrimaryVote = primaryVote
		diag.FallbackVote = fallbackVote
		diag.HasFallbackVote = hasFallback
		if payload, err := votePayloadBytes(shredVersion, primaryVote); err == nil {
			diag.PrimaryPayloadLen = len(payload)
		}
		if hasFallback {
			if payload, err := votePayloadBytes(shredVersion, fallbackVote); err == nil {
				diag.FallbackPayloadLen = len(payload)
			}
		}
	}

	baseRanks, baseStake := signerRankSamples(set, bitmap.Base, maxSamples)
	fallbackRanks, fallbackStake := signerRankSamples(set, bitmap.Fallback, maxSamples)
	diag.BaseRanks = baseRanks
	diag.FallbackRanks = fallbackRanks
	diag.BaseSignerCount = countBool(bitmap.Base)
	diag.FallbackSignerCount = countBool(bitmap.Fallback)

	union := bitmap.Union()
	diag.SignerCount = countBool(union)
	for rank, included := range union {
		if !included || rank >= len(set.Validators) {
			continue
		}
		validator := set.Validators[rank]
		diag.IncludedStake += validator.Stake
		if len(diag.SignerSamples) < maxSamples {
			sample := validatorSignerSample(validator)
			sample.Rank = uint16(rank)
			diag.SignerSamples = append(diag.SignerSamples, sample)
		}
	}
	if diag.IncludedStake == 0 {
		diag.IncludedStake = baseStake + fallbackStake
	}
	return diag
}

func validatorSignerSample(validator ValidatorStake) SignerSample {
	return SignerSample{
		Rank:            validator.Rank,
		Stake:           validator.Stake,
		VoteAccount:     validator.VoteAccount,
		NodePubkey:      validator.NodePubkey,
		BLSPubkeyPrefix: fmt.Sprintf("%x", validator.BlsPubkeyCompressed[:6]),
		BLSPubkeyHex:    fmt.Sprintf("%x", validator.BlsPubkeyCompressed[:]),
	}
}

func signerRankSamples(set ValidatorSet, ranks []bool, maxSamples int) ([]uint16, uint64) {
	samples := make([]uint16, 0, min(maxSamples, len(ranks)))
	var stake uint64
	for rank, included := range ranks {
		if !included || rank >= len(set.Validators) {
			continue
		}
		stake += set.Validators[rank].Stake
		if len(samples) < maxSamples {
			samples = append(samples, uint16(rank))
		}
	}
	return samples, stake
}

func validateChainCertificateForVerifier(cert Certificate) error {
	cert.IncludedStake = 0
	cert.TotalStake = 0
	if err := validateChainCertificate(cert); err != nil {
		return err
	}
	if len(cert.Bitmap) == 0 {
		return fmt.Errorf("alpenglow verifier: %s certificate for slot %d has empty bitmap", cert.Type, cert.Slot)
	}
	if len(cert.Signature) != 0 && len(cert.Signature) != BLSSignatureSize {
		return fmt.Errorf("alpenglow verifier: %s certificate for slot %d has invalid signature length %d", cert.Type, cert.Slot, len(cert.Signature))
	}
	return nil
}

func DecodeSignerStoreBitmap(data []byte, maxLen int) (SignerBitmap, error) {
	if len(data) < signerStoreHeaderLen {
		return SignerBitmap{}, fmt.Errorf("alpenglow verifier: signer bitmap too short")
	}
	if maxLen < 0 || maxLen > CertificateBitmapCapacity {
		return SignerBitmap{}, fmt.Errorf("alpenglow verifier: invalid max bitmap len %d", maxLen)
	}
	totalBits := int(binary.LittleEndian.Uint16(data[1:3]))
	if totalBits > maxLen {
		return SignerBitmap{}, fmt.Errorf("alpenglow verifier: signer bitmap len %d exceeds max %d", totalBits, maxLen)
	}
	payload := data[signerStoreHeaderLen:]
	switch data[0] {
	case signerStoreVersionBase2:
		return decodeSignerStoreBase2(payload, totalBits)
	case signerStoreVersionBase3:
		return decodeSignerStoreBase3(payload, totalBits)
	default:
		return SignerBitmap{}, fmt.Errorf("alpenglow verifier: unsupported signer bitmap version %d", data[0])
	}
}

func decodeSignerStoreBase2(payload []byte, totalBits int) (SignerBitmap, error) {
	expectedLen := (totalBits + 7) / 8
	if len(payload) != expectedLen {
		return SignerBitmap{}, fmt.Errorf("alpenglow verifier: corrupt base2 signer bitmap payload len %d, expected %d", len(payload), expectedLen)
	}
	base := make([]bool, totalBits)
	for i := 0; i < totalBits; i++ {
		base[i] = payload[i/8]&(1<<uint(i%8)) != 0
	}
	return SignerBitmap{Encoding: SignerBitmapBase2, Length: totalBits, Base: base}, nil
}

func decodeSignerStoreBase3(payload []byte, totalBits int) (SignerBitmap, error) {
	expectedLen := (totalBits + base3SymbolsPerByte - 1) / base3SymbolsPerByte
	if len(payload) != expectedLen {
		return SignerBitmap{}, fmt.Errorf("alpenglow verifier: corrupt base3 signer bitmap payload len %d, expected %d", len(payload), expectedLen)
	}
	base := make([]bool, totalBits)
	fallback := make([]bool, totalBits)
	for chunkIndex, blockByte := range payload {
		blockNum := blockByte
		startBit := chunkIndex * base3SymbolsPerByte
		endBit := min(startBit+base3SymbolsPerByte, totalBits)
		for bitIndex := startBit; bitIndex < endBit; bitIndex++ {
			remainder := blockNum % 3
			blockNum /= 3
			switch remainder {
			case 0:
			case 1:
				base[bitIndex] = true
			case 2:
				fallback[bitIndex] = true
			default:
				panic("unreachable base3 remainder")
			}
		}
	}
	return SignerBitmap{Encoding: SignerBitmapBase3, Length: totalBits, Base: base, Fallback: fallback}, nil
}

func (b SignerBitmap) Union() []bool {
	union := make([]bool, b.Length)
	for i, ok := range b.Base {
		if ok {
			union[i] = true
		}
	}
	for i, ok := range b.Fallback {
		if ok {
			union[i] = true
		}
	}
	return union
}

func (b SignerBitmap) CheckDisjoint() error {
	for i := 0; i < b.Length; i++ {
		if i < len(b.Base) && i < len(b.Fallback) && b.Base[i] && b.Fallback[i] {
			return fmt.Errorf("alpenglow verifier: base3 signer bitmap has rank %d in primary and fallback sets", i)
		}
	}
	return nil
}

func countBool(values []bool) int {
	var count int
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func decompressBLSPubkey(compressed []byte) ([96]byte, error) {
	var out [96]byte
	if len(compressed) != 48 {
		return out, fmt.Errorf("invalid compressed BLS pubkey len %d", len(compressed))
	}
	var point bls12381.G1Affine
	if _, err := point.SetBytes(compressed); err != nil {
		return out, err
	}
	out = point.RawBytes()
	return out, nil
}
