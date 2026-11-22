package dashboard

import (
	"math"
	"time"
)

// AccountOperationsWindow reports independent owner-counter deltas. Requested
// keys include misses, duplicates and errors. Committed records are deduplicated
// successful folds (including tombstones), not overlay writes or physical I/O.
type AccountOperationsWindow struct {
	Scope               string    `json:"scope"`
	RecordingStartedAt  time.Time `json:"recording_started_at"`
	StartedAt           time.Time `json:"started_at"`
	EndedAt             time.Time `json:"ended_at"`
	DurationNanoseconds uint64    `json:"duration_nanoseconds,string"`
	RequestedKeys       uint64    `json:"requested_keys,string"`
	CommittedRecords    uint64    `json:"committed_records,string"`
}

func validAccountOperationsWindow(w *AccountOperationsWindow, opened, captured time.Time) bool {
	if w == nil {
		return true
	}
	if w.Scope != "runtime_db_requested_keys_and_committed_records" || !validReplayDetailTime(w.RecordingStartedAt) ||
		!validReplayDetailTime(w.StartedAt) || !validReplayDetailTime(w.EndedAt) || w.RecordingStartedAt.Before(opened) ||
		w.StartedAt.Before(w.RecordingStartedAt) || w.EndedAt.After(captured) || captured.Sub(w.EndedAt) > time.Second || !w.EndedAt.After(w.StartedAt) ||
		w.DurationNanoseconds == 0 || w.DurationNanoseconds > uint64(StorageDetailFreshFor) ||
		w.RequestedKeys == math.MaxUint64 || w.CommittedRecords == math.MaxUint64 {
		return false
	}
	wall := w.EndedAt.Sub(w.StartedAt)
	elapsed := time.Duration(w.DurationNanoseconds)
	return wall-elapsed <= 250*time.Millisecond && elapsed-wall <= 250*time.Millisecond
}
