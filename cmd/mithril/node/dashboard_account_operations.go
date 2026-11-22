package node

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

type accountOperationsPoint struct {
	value accountsdb.AccountOperationsObservation
	at    time.Time
}

// accountOperationsWindow runs under d.mu for the attached DB owner. It uses
// original monotonic capture clocks and independent counter deltas, not a scan.
func (m *storageDashboardMonitor) accountOperationsWindow(value *accountsdb.AccountOperationsObservation, started, completed time.Time) *dashboard.AccountOperationsWindow {
	if m.operationsRetired || value == nil || value.StartedAt.IsZero() || started.IsZero() || completed.IsZero() ||
		started == started.Round(0) || completed == completed.Round(0) || completed.Before(started) || completed.Sub(started) > time.Second ||
		value.StartedAt.Before(m.binding.DBOpenedAt) || value.StartedAt.After(started) {
		return nil
	}
	previous := m.operationsPrevious
	m.operationsPrevious = &accountOperationsPoint{value: *value, at: completed}
	if previous == nil {
		return nil
	}
	if !previous.value.StartedAt.Equal(value.StartedAt) || value.RequestedKeys < previous.value.RequestedKeys || value.CommittedRecords < previous.value.CommittedRecords {
		m.operationsRetired = true
		m.operationsPrevious = nil
		return nil
	}
	elapsed := completed.Sub(previous.at)
	wallElapsed := completed.Round(0).Sub(previous.at.Round(0))
	if elapsed <= 0 || elapsed > dashboard.StorageDetailFreshFor || wallElapsed-elapsed > 250*time.Millisecond || elapsed-wallElapsed > 250*time.Millisecond {
		return nil
	}
	return &dashboard.AccountOperationsWindow{Scope: "runtime_db_requested_keys_and_committed_records", RecordingStartedAt: value.StartedAt.UTC(),
		StartedAt: previous.at.UTC(), EndedAt: completed.UTC(), DurationNanoseconds: uint64(elapsed),
		RequestedKeys: value.RequestedKeys - previous.value.RequestedKeys, CommittedRecords: value.CommittedRecords - previous.value.CommittedRecords}
}
