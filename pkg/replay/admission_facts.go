package replay

import (
	"math"
	"math/bits"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

// AdmissionFundingFacts applies the sampled bank's rules at AccountFacts.Slot.
// It is not the floor used to select a historical epoch set, proof of membership,
// or permission to vote. MeetsFloor is only a numerical balance comparison;
// a false VAT gate must not be interpreted as an underfunded admission failure.
type AdmissionFundingFacts struct {
	HasRules             bool
	VATFilteringActive   bool
	VATChargeGateActive  bool
	RentMinimumLamports  uint64
	BurnLamports         uint64
	FloorLamports        uint64
	HasFundingComparison bool
	MeetsFloor           bool
}

func accountFactAdmissionFunding(bank *sealevel.SlotCtx, schedule *sealevel.SysvarEpochSchedule, balance uint64, hasBalance bool) AdmissionFundingFacts {
	if bank == nil || bank.Features == nil || !admissionScheduleValid(schedule) {
		return AdmissionFundingFacts{}
	}
	sysvars := bank.BankSysvars()
	if sysvars == nil || sysvars.Slot() != bank.Slot {
		return AdmissionFundingFacts{}
	}
	epoch, _, ok := admissionEpochEnd(schedule, bank.Slot)
	if !ok || epoch != bank.Epoch {
		return AdmissionFundingFacts{}
	}
	// Validate the same next-epoch boundaries used by alpenglowVATBurnPerEpoch
	// before it performs saturating arithmetic. Saturation is not a real boundary.
	if alpenglowClockFeatureActive(bank.Features) {
		for _, gate := range [...]features.FeatureGate{features.ReduceSlotTimeTo350ms, features.ReduceSlotTimeTo300ms, features.ReduceSlotTimeTo250ms, features.ReduceSlotTimeTo200ms} {
			if slot, active := bank.Features.ActivationSlot(gate); active {
				activationEpoch, next, valid := admissionEpochEnd(schedule, slot)
				if !valid || activationEpoch == math.MaxUint64 || schedule.FirstSlotInEpoch(activationEpoch+1) != next {
					return AdmissionFundingFacts{}
				}
			}
		}
	}
	rent, known := sysvars.Rent()
	if !known || math.IsNaN(rent.ExemptionThreshold) || math.IsInf(rent.ExemptionThreshold, 0) || rent.ExemptionThreshold < 0 || rent.BurnPercent > 100 {
		return AdmissionFundingFacts{}
	}
	// MinimumBalance adds the protocol's 128-byte account storage overhead.
	// Reject an unrepresentable input/result rather than depend on float-to-uint
	// conversion at 2^64, or disguise a saturated intermediate as a valid rent.
	annual, err := safemath.CheckedMulU64(sealevel.VoteStateV4Size+128, rent.LamportsPerUint8Year)
	minimum := float64(annual) * rent.ExemptionThreshold
	if err != nil || math.IsNaN(minimum) || math.IsInf(minimum, 0) || minimum >= math.Exp2(64) {
		return AdmissionFundingFacts{}
	}
	facts := AdmissionFundingFacts{HasRules: true, VATFilteringActive: bank.Features.IsActive(features.ValidatorAdmissionTicket),
		RentMinimumLamports: rent.MinimumBalance(sealevel.VoteStateV4Size), BurnLamports: alpenglowVATBurnPerEpoch(bank.Features, schedule, bank.Slot)}
	partitionedRewards := bank.Features.IsActive(features.EnablePartitionedEpochReward) || bank.Features.IsActive(features.EnablePartitionedEpochRewardsSuperfeature)
	facts.VATChargeGateActive = facts.VATFilteringActive && bank.Features.IsActive(features.Alpenglow) && partitionedRewards
	facts.FloorLamports = safemath.SaturatingAddU64(facts.RentMinimumLamports, facts.BurnLamports)
	facts.HasFundingComparison, facts.MeetsFloor = hasBalance, hasBalance && balance >= facts.FloorLamports
	return facts
}

// admissionScheduleValid checks the finite warmup geometry before the runtime
// epoch helpers, which assume a valid schedule. Leader offsets do not enter VAT.
func admissionScheduleValid(schedule *sealevel.SysvarEpochSchedule) bool {
	if schedule == nil || schedule.SlotsPerEpoch == 0 {
		return false
	}
	if !schedule.Warmup {
		return schedule.FirstNormalEpoch == 0 && schedule.FirstNormalSlot == 0
	}
	if schedule.SlotsPerEpoch < sealevel.MinimumSlotsPerEpoch || schedule.SlotsPerEpoch > 1<<63 {
		return false
	}
	epoch := uint64(bits.Len64(schedule.SlotsPerEpoch-1) - bits.TrailingZeros64(sealevel.MinimumSlotsPerEpoch))
	return schedule.FirstNormalEpoch == epoch && schedule.FirstNormalSlot == ((uint64(1)<<epoch)-1)*sealevel.MinimumSlotsPerEpoch
}

func admissionEpochEnd(schedule *sealevel.SysvarEpochSchedule, slot uint64) (epoch, next uint64, ok bool) {
	epoch, index := schedule.GetEpochAndSlotIndex(slot)
	length := schedule.SlotsInEpoch(epoch)
	if index > slot || length == 0 || index >= length || (slot >= schedule.FirstNormalSlot && epoch < schedule.FirstNormalEpoch) {
		return 0, 0, false
	}
	next, err := safemath.CheckedAddU64(slot-index, length)
	return epoch, next, err == nil
}
