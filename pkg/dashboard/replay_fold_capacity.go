package dashboard

import (
	"math"
	"time"
)

// ReplayFoldCapacity is one latest replay capacity-branch observation, not
// evidence of disk failure, finality blockage or catch-up readiness.
type ReplayFoldCapacity struct {
	Scope               string     `json:"scope"`
	State               string     `json:"state"`
	Slot                uint64     `json:"slot,string"`
	StartedAt           time.Time  `json:"started_at"`
	HadInFlightFold     bool       `json:"had_in_flight_fold"`
	Outcome             string     `json:"outcome"`
	ReturnedAt          *time.Time `json:"returned_at"`
	DurationNanoseconds *uint64    `json:"duration_nanoseconds,string"`
}

func validReplayFoldCapacity(o *ReplayFoldCapacity) bool {
	if o == nil {
		return true
	}
	if o.Scope != "replay_fold_capacity" || o.Slot == 0 || !validReplayDetailTime(o.StartedAt) {
		return false
	}
	switch o.State {
	case "active":
		return o.Outcome == "" && o.ReturnedAt == nil && o.DurationNanoseconds == nil
	case "returned":
		return (o.Outcome == "relieved" || o.Outcome == "still_over_capacity" || o.Outcome == "failed") &&
			o.ReturnedAt != nil && validReplayDetailTime(*o.ReturnedAt) && (o.DurationNanoseconds == nil || *o.DurationNanoseconds < math.MaxInt64)
	default:
		return false
	}
}

func cloneReplayFoldCapacity(o *ReplayFoldCapacity) *ReplayFoldCapacity {
	if o == nil {
		return nil
	}
	v := *o
	if v.ReturnedAt != nil {
		at := *v.ReturnedAt
		v.ReturnedAt = &at
	}
	if v.DurationNanoseconds != nil {
		n := *v.DurationNanoseconds
		v.DurationNanoseconds = &n
	}
	return &v
}
