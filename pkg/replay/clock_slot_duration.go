package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/duration"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"sort"
)

// clockSlotElapsed mirrors Agave's SlotParamsArchive for Clock timestamps.
// Reductions take effect in the epoch AFTER activation. Elapsed time from a
// parent to a child includes slots (parent, child], including skipped slots.
func clockSlotElapsed(schedule *sealevel.SysvarEpochSchedule, f *features.Features) func(uint64, uint64) duration.Duration {
	type transition struct{ slot, nanos uint64 }
	var transitions []transition
	if f != nil {
		for _, item := range []struct {
			gate  features.FeatureGate
			nanos uint64
		}{
			{features.ReduceSlotTimeTo350ms, 350_000_000},
			{features.ReduceSlotTimeTo300ms, 300_000_000},
			{features.ReduceSlotTimeTo250ms, 250_000_000},
			{features.ReduceSlotTimeTo200ms, 200_000_000},
		} {
			if slot, ok := f.ActivationSlot(item.gate); ok {
				epoch := safemath.SaturatingAddU64(schedule.GetEpoch(slot), 1)
				transitions = append(transitions, transition{schedule.FirstSlotInEpoch(epoch), item.nanos})
			}
		}
	}
	sort.Slice(transitions, func(i, j int) bool { return transitions[i].slot < transitions[j].slot })
	return func(from, to uint64) duration.Duration {
		if from >= to {
			return duration.Duration{}
		}
		cursor, nanos := from+1, uint64(nsPerSlot)
		var total duration.Duration
		for _, next := range transitions {
			if next.slot > to {
				break
			}
			if next.slot > cursor {
				total = total.SaturatingAdd(clockSlotsDuration(next.slot-cursor, nanos))
				cursor = next.slot
			}
			// A later longer-slot gate cannot undo an earlier reduction.
			nanos = min(nanos, next.nanos)
		}
		return total.SaturatingAdd(clockSlotsDuration(to-cursor+1, nanos))
	}
}

func clockSlotsDuration(slots, nanos uint64) duration.Duration {
	// Split before multiplying to preserve u128-style range without overflowing
	// uint64 nanoseconds. All supported slot durations are below one second.
	const billion = 1_000_000_000
	remainder := (slots % billion) * nanos
	return duration.NewDuration((slots/billion)*nanos+remainder/billion, uint32(remainder%billion))
}
