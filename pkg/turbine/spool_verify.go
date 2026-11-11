package turbine

import "fmt"

// VerifySpooledShreds makes the receiver check each spooled shred's leader
// signature when it hydrates a slot from disk. A live node verifies shreds as
// they arrive, so spooled shreds are trusted by default. A backtest replaying
// a capture it did not record can ask for the check; it costs one signature
// verification per FEC set (results are cached per Merkle root).
var VerifySpooledShreds bool

// OnSpooledShredRejected, when set, hears about every spooled shred that fails
// the check. The shred is not assembled, so its slot cannot complete.
var OnSpooledShredRejected func(slot uint64, err error)

// verifySpooledShred checks one hydrated shred against its slot leader.
func (r *UDPReceiver) verifySpooledShred(shred *Shred) error {
	if r.leaderForSlot == nil {
		return fmt.Errorf("slot %d: no leader schedule to verify spooled shreds against", shred.Slot)
	}
	leader, ok := r.leaderForSlot(shred.Slot)
	if !ok {
		return fmt.Errorf("slot %d: no leader known for spooled shred %d", shred.Slot, shred.Index)
	}
	if _, err := r.sigCache.verifyShredRoot(shred, leader); err != nil {
		return fmt.Errorf("slot %d shred %d (%s): %w", shred.Slot, shred.Index, shredTypeName(shred.Type), err)
	}
	return nil
}

func shredTypeName(t ShredType) string {
	if t == ShredTypeCode {
		return "code"
	}
	return "data"
}
