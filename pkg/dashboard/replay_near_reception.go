package dashboard

import "time"

// ReplayNearReception is a historical observation of a newly completed replay
// row near recent authenticated input, not current synchronization or readiness.
type ReplayNearReception struct {
	Scope               string    `json:"scope"`
	Slot                uint64    `json:"slot,string"`
	Epoch               uint64    `json:"epoch,string"`
	Bankhash            string    `json:"bankhash"`
	CompletedAt         time.Time `json:"completed_at"`
	ObservedAt          time.Time `json:"observed_at"`
	SourceStartedAt     time.Time `json:"source_started_at"`
	ReceiverStartedAt   time.Time `json:"receiver_started_at"`
	ReceiverSession     uint64    `json:"receiver_session,string"`
	ReceptionSlot       uint64    `json:"reception_slot,string"`
	ReceptionSampledAt  time.Time `json:"reception_sampled_at"`
	ReceptionObservedAt time.Time `json:"reception_observed_at"`
	RetainedSlots       int       `json:"retained_slots"`
}

func validReplayNearReception(o *ReplayNearReception) bool {
	if o == nil {
		return true
	}
	if o.Scope != "first_completed_replay_near_recent_input" || !validReplayDetailHash(o.Bankhash, true) || o.ReceiverSession == 0 ||
		o.RetainedSlots < 1 || o.RetainedSlots > 64 {
		return false
	}
	for _, at := range [...]time.Time{o.CompletedAt, o.ObservedAt, o.SourceStartedAt, o.ReceiverStartedAt, o.ReceptionSampledAt, o.ReceptionObservedAt} {
		if !validReplayDetailTime(at) {
			return false
		}
	}
	if o.Slot >= o.ReceptionSlot {
		return o.Slot-o.ReceptionSlot <= 4
	}
	return o.ReceptionSlot-o.Slot <= 4
}

func cloneReplayNearReception(o *ReplayNearReception) *ReplayNearReception {
	if o == nil {
		return nil
	}
	value := *o
	return &value
}
