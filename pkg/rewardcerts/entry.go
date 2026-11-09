package rewardcerts

import (
	"fmt"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
)

type slotEntry struct {
	skip          partialCert
	notar         notarEntry
	maxValidators int
}

func newSlotEntry(maxValidators int) slotEntry {
	return slotEntry{
		skip:          newPartialCert(maxValidators),
		notar:         newNotarEntry(maxValidators),
		maxValidators: maxValidators,
	}
}

func (e *slotEntry) wantsVote(msg alpenglow.VoteMessage) bool {
	switch msg.Vote.Type {
	case alpenglow.VoteTypeSkip:
		return e.skip.wantsVote(msg.Rank)
	case alpenglow.VoteTypeNotarize:
		return e.notar.wantsVote(msg.Rank)
	default:
		return false
	}
}

func (e *slotEntry) addVote(msg alpenglow.VoteMessage) error {
	switch msg.Vote.Type {
	case alpenglow.VoteTypeSkip:
		return e.skip.addVote(msg.Rank, msg.Signature)
	case alpenglow.VoteTypeNotarize:
		block, ok := msg.Vote.Block()
		if !ok {
			return fmt.Errorf("rewardcerts: notarize vote missing block id")
		}
		return e.notar.addVote(msg.Rank, msg.Signature, block.Hash, e.maxValidators)
	default:
		return nil
	}
}

func (e *slotEntry) addVerifiedSignature(msg alpenglow.VoteMessage, signature bls12381.G2Affine) error {
	switch msg.Vote.Type {
	case alpenglow.VoteTypeSkip:
		return e.skip.addVerifiedSignature(msg.Rank, signature)
	case alpenglow.VoteTypeNotarize:
		block, ok := msg.Vote.Block()
		if !ok {
			return fmt.Errorf("rewardcerts: notarize vote missing block id")
		}
		return e.notar.addVerifiedSignature(msg.Rank, signature, block.Hash, e.maxValidators)
	default:
		return nil
	}
}

func (e *slotEntry) build(rewardSlot uint64) (RewardCertificates, error) {
	var out RewardCertificates

	if skipCert, err := e.skip.build(); err == nil {
		skipCert.Slot = rewardSlot
		encoded, err := EncodeSkipRewardCertificate(skipCert)
		if err != nil {
			return out, err
		}
		out.Skip = encoded
	} else if err != errEmptyBitmap {
		return out, err
	}

	if notarCert, err := e.notar.buildCert(rewardSlot); err == nil {
		encoded, err := EncodeNotarRewardCertificate(notarCert)
		if err != nil {
			return out, err
		}
		out.Notar = encoded
	} else if err != errEmptyBitmap {
		return out, err
	}

	return out, nil
}
