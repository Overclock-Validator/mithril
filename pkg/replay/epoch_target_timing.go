package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

// EpochTargetTiming is the target implied by a completed bank's active features
// for its current and immediately following epoch. Zero is unknown. These are
// nanoseconds per slot, not measured arrival/execution times or time.Duration.
// The containing CompletedBlock supplies the original bank binding and date.
type EpochTargetTiming struct {
	CurrentNanoseconds uint64
	NextNanoseconds    uint64
}

// epochTargetSeed is copied once per replay attempt. Its already-effective
// snapshot target supports only forward lookups, not pre-snapshot history.
type epochTargetSeed struct {
	slot        uint64
	bankhash    solana.Hash
	schedule    sealevel.SysvarEpochSchedule
	nanoseconds uint64
}

func newEpochTargetSeed(s *state.MithrilState) epochTargetSeed {
	if s == nil || s.ManifestNsPerSlot == 0 || len(s.ManifestParentBankhash) > 44 {
		return epochTargetSeed{}
	}
	schedule, ok := bankEpochScheduleFromState(s)
	if !ok || !admissionScheduleValid(schedule) {
		return epochTargetSeed{}
	}
	hash, err := solana.HashFromBase58(s.ManifestParentBankhash)
	if err != nil || hash.IsZero() || hash.String() != s.ManifestParentBankhash {
		return epochTargetSeed{}
	}
	return epochTargetSeed{slot: s.ManifestParentSlot, bankhash: hash, schedule: *schedule, nanoseconds: s.ManifestNsPerSlot}
}

func (seed epochTargetSeed) observe(bank *sealevel.SlotCtx, completed CompletedBlock) EpochTargetTiming {
	// Identity distinguishes an uninitialized set from an initialized empty one.
	if seed.nanoseconds == 0 || bank == nil || bank.Features == nil || bank.Features.Identity() == (*features.Features)(nil).Identity() ||
		completed.Bankhash.IsZero() || completed.Slot < seed.slot || bank.Slot != completed.Slot || bank.Epoch != completed.Epoch ||
		len(bank.FinalBankhash) != len(completed.Bankhash) || solana.HashFromBytes(bank.FinalBankhash) != completed.Bankhash ||
		(completed.Slot == seed.slot && completed.Bankhash != seed.bankhash) {
		return EpochTargetTiming{}
	}
	schedule, ok := completed.CheckedEpochSchedule()
	if !ok || schedule != seed.schedule {
		return EpochTargetTiming{}
	}
	_, nextSlot, hasNext := admissionEpochEnd(&schedule, completed.Slot)
	timing := EpochTargetTiming{CurrentNanoseconds: seed.nanoseconds}
	if hasNext {
		timing.NextNanoseconds = seed.nanoseconds
	}
	// Four fixed lookups, only on successful monitored completion. Recompute
	// from the immutable seed so a discarded faster fork cannot poison a retry.
	for _, transition := range [...]struct {
		gate        features.FeatureGate
		nanoseconds uint64
	}{
		{features.ReduceSlotTimeTo350ms, 350_000_000},
		{features.ReduceSlotTimeTo300ms, 300_000_000},
		{features.ReduceSlotTimeTo250ms, 250_000_000},
		{features.ReduceSlotTimeTo200ms, 200_000_000},
	} {
		activationSlot, active := bank.Features.ActivationSlot(transition.gate)
		if !active {
			continue
		}
		_, effectiveSlot, valid := admissionEpochEnd(&schedule, activationSlot)
		if activationSlot > completed.Slot || !valid ||
			(effectiveSlot <= seed.slot && transition.nanoseconds < seed.nanoseconds) {
			// An effective target shorter than the saved snapshot contradicts
			// that baseline. Do not silently repair it or saturate a boundary.
			return EpochTargetTiming{}
		}
		if effectiveSlot <= completed.Slot {
			timing.CurrentNanoseconds = min(timing.CurrentNanoseconds, transition.nanoseconds)
		}
		if hasNext && effectiveSlot <= nextSlot {
			timing.NextNanoseconds = min(timing.NextNanoseconds, transition.nanoseconds)
		}
	}
	return timing
}
