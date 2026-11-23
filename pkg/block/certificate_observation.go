package block

import "github.com/gagliardetto/solana-go"

// CertificateMembership describes one fixed vote account in an exact target
// epoch's admitted validator set. Unknown admission is not exclusion.
type CertificateMembership struct {
	TargetSlot, TargetEpoch, MaterialGeneration uint64
	EpochIdentity                               solana.PublicKey
	Rank                                        uint16
	AdmissionKnown, Eligible, Present, Included bool
}

// CertificateObservation is compact, already-validated footer membership, not
// proof that this process signed, received a payment, or settled the carrier.
type CertificateObservation struct {
	VoteAccount  solana.PublicKey
	Reward       CertificateMembership
	Final        CertificateMembership
	FinalBlockID solana.Hash
	FinalFast    bool
}

type certificateObservationContext struct {
	subject solana.PublicKey
	value   CertificateObservation
	ready   bool
}

// EnableCertificateObservation starts a private, nonserialized request before
// execution. The block's execution owner is the only writer until handoff.
// A zero subject disables capture without allocating.
func (b *Block) EnableCertificateObservation(subject solana.PublicKey) {
	if b == nil {
		return
	}
	if subject == (solana.PublicKey{}) {
		b.certificateObservation = nil
		return
	}
	b.certificateObservation = &certificateObservationContext{subject: subject}
}

// CertificateObservationSubject returns the immutable requested vote account.
func (b *Block) CertificateObservationSubject() (solana.PublicKey, bool) {
	if b != nil && b.certificateObservation != nil {
		return b.certificateObservation.subject, true
	}
	return solana.PublicKey{}, false
}

// ClearCertificateObservation drops any prior result before another apply,
// retaining the fixed request without allocating.
func (b *Block) ClearCertificateObservation() {
	if b != nil && b.certificateObservation != nil {
		b.certificateObservation.value, b.certificateObservation.ready = CertificateObservation{}, false
	}
}

// RecordCertificateObservation stores a successful apply's detached values.
// It does not publish them: replay must still succeed and accept the carrier.
func (b *Block) RecordCertificateObservation(value CertificateObservation) {
	if b != nil && b.certificateObservation != nil && value.VoteAccount == b.certificateObservation.subject {
		b.certificateObservation.value, b.certificateObservation.ready = value, true
	}
}

// CertificateObservation returns a value copy of the private apply result.
func (b *Block) CertificateObservation() (CertificateObservation, bool) {
	if b != nil && b.certificateObservation != nil && b.certificateObservation.ready {
		return b.certificateObservation.value, true
	}
	return CertificateObservation{}, false
}
