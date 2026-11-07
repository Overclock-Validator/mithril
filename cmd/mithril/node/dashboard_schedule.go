package node

import (
	"math"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/replay"
)

// heldScheduleMeasurement copies at most 32 O(1) lookups from one immutable
// epoch schedule pointer. It never scans schedules to filter by identity,
// reads mutable sysvars, or crosses the completed bank's epoch boundary.
func heldScheduleMeasurement(p replay.ProgressSnapshot) *dashboard.HeldScheduleWindow {
	if !p.HasCompleted {
		return nil
	}
	b := p.Completed
	count := dashboard.HeldScheduleCount(b.Slot, b.SlotIndex, b.SlotsInEpoch)
	if count == 0 || b.At.IsZero() || b.Bankhash.IsZero() {
		return nil
	}
	w := &dashboard.HeldScheduleWindow{Scope: "held_schedule_at_capture", ReferenceSlot: b.Slot,
		ReferenceBankhash: b.Bankhash.String(), ReferenceCompletedAt: b.At.UTC(), Epoch: b.Epoch,
		SlotIndex: b.SlotIndex, SlotsInEpoch: b.SlotsInEpoch, Slots: make([]dashboard.HeldScheduleSlot, count)}
	schedule := global.LeaderSchedulesForEpochs([]uint64{b.Epoch})[0]
	for i := range w.Slots {
		entry := &w.Slots[i]
		entry.Slot = b.Slot + uint64(i)
		if schedule != nil {
			if key, ok := schedule.LeaderForSlot(entry.Slot); ok && !key.IsZero() {
				value := key.String()
				entry.Leader = &value
			}
		}
	}
	identity := p.NextKnownAssignment.Identity
	if !identity.IsZero() && !p.NextKnownAssignment.SampledAt.IsZero() {
		end := b.Slot + min(b.SlotsInEpoch-b.SlotIndex-1, math.MaxUint64-b.Slot)
		if own, ok := schedule.CachedAssignmentsForLeader(identity, b.Slot-b.SlotIndex, b.Slot, end); ok {
			w.OwnAssignments = &dashboard.HeldOwnAssignments{Identity: identity.String(), Total: own.Total, Remaining: own.Remaining,
				Upcoming: make([]dashboard.HeldOwnSlot, own.Count)}
			for i := range w.OwnAssignments.Upcoming {
				w.OwnAssignments.Upcoming[i].Slot = own.Upcoming[i]
			}
		}
	}
	return w
}
