package alpenglow

import (
	"math"
	"sync/atomic"
	"time"
)

// ApplicationPayloadObservation is one owner's detached QUIC application-byte
// counter. Receiver copies and locally accepted send-queue copies are separate
// scopes; neither is encrypted wire traffic or delivery evidence.
type ApplicationPayloadObservation struct {
	StartedAt time.Time `json:"started_at"`
	SampledAt time.Time `json:"sampled_at"`
	Bytes     uint64    `json:"bytes,string"`
	Known     bool      `json:"known"`
}

func saturatingApplicationBytes(total uint64, n int) uint64 {
	if n <= 0 {
		return total
	}
	if uint64(n) > math.MaxUint64-total {
		return math.MaxUint64
	}
	return total + uint64(n)
}

func addApplicationBytes(counter *atomic.Uint64, n int) {
	for {
		before := counter.Load()
		after := saturatingApplicationBytes(before, n)
		if after == before || counter.CompareAndSwap(before, after) {
			return
		}
	}
}

func applicationPayloadObservation(start time.Time, total uint64) ApplicationPayloadObservation {
	o := ApplicationPayloadObservation{StartedAt: start, SampledAt: time.Now(), Known: total != math.MaxUint64}
	if o.Known {
		o.Bytes = total
	}
	return o
}

// ApplicationPayloadObservation reads only the opted-in receiver counter, not
// transport queues. Retirement removes availability. Call from diagnostics.
func (r *Receiver) ApplicationPayloadObservation() (ApplicationPayloadObservation, bool) {
	if r == nil {
		return ApplicationPayloadObservation{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.payloadStartedAt.IsZero() {
		return ApplicationPayloadObservation{}, false
	}
	return applicationPayloadObservation(r.payloadStartedAt, r.payloadBytes), true
}

// recordApplicationPayloadLocked reuses the existing datagram-counter lock.
func (r *Receiver) recordApplicationPayloadLocked(n int) {
	if r.cfg.RecordApplicationPayload {
		r.payloadBytes = saturatingApplicationBytes(r.payloadBytes, n)
	}
}
