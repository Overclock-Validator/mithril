package replay

import (
	"errors"
	"fmt"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

// InvalidRewardCertificateError identifies objectively invalid footer contents.
// Missing or inconsistent local validator material must not produce this error:
// it cannot establish that a network block is invalid.
type InvalidRewardCertificateError struct {
	Slot uint64
	Err  error
}

func (err *InvalidRewardCertificateError) Error() string {
	return fmt.Sprintf("slot %d invalid reward certificates: %v", err.Slot, err.Err)
}

func (err *InvalidRewardCertificateError) Unwrap() error { return err.Err }

func IsInvalidRewardCertificateError(err error) bool {
	var invalid *InvalidRewardCertificateError
	return errors.As(err, &invalid)
}

// validatePreConsensusRewardCertificates checks a candidate before consensus
// observation or account execution. Successful validation is reused when
// ApplyAlpenglowVoteRewards consumes the same footer with the same epoch keys.
func validatePreConsensusRewardCertificates(
	block *b.Block,
	epochSchedule *sealevel.SysvarEpochSchedule,
	shredVersion uint16,
) error {
	if block == nil {
		return fmt.Errorf("reward certificate preflight: missing block")
	}
	if block.IsSkipped || (len(block.SkipRewardCert) == 0 && len(block.NotarRewardCert) == 0) {
		return nil
	}
	start := time.Now()
	defer metrics.GlobalBlockReplay.RewardCertificatePreflight.AddTimingSince(start)
	if epochSchedule == nil {
		return fmt.Errorf("slot %d reward certificate preflight: missing epoch schedule", block.Slot)
	}
	rewardSlot, ok := rewardcerts.RewardSlotForLeader(block.Slot)
	if !ok {
		return &InvalidRewardCertificateError{Slot: block.Slot, Err: fmt.Errorf("invalid reward slot offset")}
	}
	// Derive the validator epoch from the containing block, never from a slot
	// claimed by unvalidated certificate bytes.
	rewardEpoch := epochSchedule.GetEpoch(rewardSlot)
	details := &metrics.GlobalBlockReplay.VoteRewardDetails
	material, err := loadVoteRewardVerifierMaterial(rewardEpoch, shredVersion, details)
	if err != nil {
		return fmt.Errorf("slot %d reward certificate preflight: %w", block.Slot, err)
	}
	_, timings, err := material.validateRewardCertificates(block.Slot, block.SkipRewardCert, block.NotarRewardCert, true)
	if timings.Skip > 0 {
		details.SkipCertificateValidation.AddTiming(timings.Skip)
	}
	if timings.Notar > 0 {
		details.NotarCertificateValidation.AddTiming(timings.Notar)
	}
	if err != nil {
		return &InvalidRewardCertificateError{Slot: block.Slot, Err: err}
	}
	return nil
}
