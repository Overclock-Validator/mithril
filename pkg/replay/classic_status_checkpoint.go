package replay

import (
	"fmt"
	"github.com/Overclock-Validator/mithril/pkg/state"
)

// Called only after the classic account writer has drained, before publishing
// the matching state file. The immutable sidecar is durable before its reference.
func saveClassicTransactionStatuses(dir string, st *state.MithrilState, cache *TransactionStatusCache, slot uint64) error {
	if slot == 0 {
		return nil
	}
	payload, err := cache.SnapshotThrough(slot)
	if err != nil {
		return fmt.Errorf("capture classic transaction statuses: %w", err)
	}
	ref, err := PrepareTransactionStatusCheckpoint(dir, slot, payload)
	if err != nil {
		return fmt.Errorf("persist classic transaction statuses: %w", err)
	}
	st.LastTransactionStatusCheckpoint = ref
	return nil
}
