package blockstream

import (
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
)

// TurbinePrewarmRepairObservation is a detached, non-resetting repair-client
// counter sample taken before collection stops. LateResponses are recognized
// matches after expiration and overlap Timeouts; these are not disjoint outcomes.
type TurbinePrewarmRepairObservation struct {
	SampledAt     time.Time
	Requests      uint64
	Responses     uint64
	LateResponses uint64
	Timeouts      uint64
}

// TurbinePrewarmHandoverObservation describes the first returned in-memory spool,
// not a contiguous slot range, repair-only completion, disk inventory or catch-up
// milestone. FirstSlot and LastSlot are known only when ReturnedBlocks is positive.
// OverflowEvictions counts capacity evictions, not unique lost blocks. The optional
// repair sample has its own date and is not an exact handover counter snapshot.
type TurbinePrewarmHandoverObservation struct {
	HandedOverAt      time.Time
	FloorSlot         uint64
	Capacity          uint64
	ReturnedBlocks    uint64
	OverflowEvictions uint64
	FirstSlot         uint64
	LastSlot          uint64
	RepairKnown       bool
	Repair            TurbinePrewarmRepairObservation
}

// handoverObservationLocked consumes the first handover even if its signed
// source counts are invalid, so a later empty handover cannot replace it.
func (pw *TurbinePrewarm) handoverObservationLocked(blocks []*b.Block, repair TurbinePrewarmRepairObservation, repairKnown bool) (TurbinePrewarmHandoverObservation, bool) {
	if pw.handoverObserved {
		return TurbinePrewarmHandoverObservation{}, false
	}
	pw.handoverObserved = true
	if pw.capacity <= 0 || pw.dropped < 0 || len(blocks) > pw.capacity {
		return TurbinePrewarmHandoverObservation{}, false
	}
	o := TurbinePrewarmHandoverObservation{
		HandedOverAt: time.Now(), FloorSlot: pw.floor, Capacity: uint64(pw.capacity),
		ReturnedBlocks: uint64(len(blocks)), OverflowEvictions: uint64(pw.dropped),
		RepairKnown: repairKnown, Repair: repair,
	}
	if len(blocks) > 0 {
		o.FirstSlot, o.LastSlot = blocks[0].Slot, blocks[len(blocks)-1].Slot
	}
	return o, true
}
