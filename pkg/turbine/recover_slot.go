package turbine

import (
	"context"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/block"
)

// RecoverSlotDataShreds assembles one slot from its spooled packets (data and
// coding shreds, in any order, duplicates allowed) and returns its data
// shreds 0..last as canonical payloads. Data shreds that were never received
// are recovered from coding shreds and authenticated against the FEC set's
// Merkle root exactly as replay does, so the result is what replay executes.
// Transaction signatures are not checked. It fails when the slot cannot be
// completed.
func RecoverSlotDataShreds(slot uint64, packets [][]byte) ([][]byte, error) {
	a := NewSlotAssembler()
	a.verifyTransactions = func(context.Context, *block.Block) error { return nil }
	var out [][]byte
	a.completedShreds = func(state *slotState) {
		if state.slot != slot {
			return
		}
		out = make([][]byte, 0, state.lastIndex+1)
		for idx := uint32(0); idx <= state.lastIndex; idx++ {
			shred := state.shreds[idx]
			if shred == nil {
				out = nil
				return
			}
			out = append(out, append([]byte(nil), shred.Payload...))
		}
	}
	// Data shreds go in first, so recovery only rebuilds the ones never
	// received. Held shreds then come back byte-identical, including the
	// retransmitter signature that resigned variants carry outside the
	// Merkle-committed region (a recovered shred takes it from a coding shred).
	var lastErr error
	for _, wantType := range []ShredType{ShredTypeData, ShredTypeCode} {
		for _, packet := range packets {
			shred, err := ParseShred(packet)
			if err != nil || shred.Slot != slot || shred.Type != wantType {
				continue
			}
			if _, err := a.AddShred(shred); err != nil {
				lastErr = err
			}
			if out != nil {
				return out, nil
			}
		}
	}
	if lastErr != nil {
		return nil, fmt.Errorf("slot %d does not assemble: %w", slot, lastErr)
	}
	return nil, fmt.Errorf("slot %d does not assemble from its %d packets", slot, len(packets))
}
