package quicserver

import (
	"math"
	"sync/atomic"
	"time"
)

type ServerStats struct {
	TotalConnections       atomic.Uint64
	TotalNewConnections    atomic.Uint64
	ActiveStreams          atomic.Uint64
	TotalNewStreams        atomic.Uint64
	InvalidStreamSize      atomic.Uint64
	TotalStreamReadErrors  atomic.Uint64
	TotalStreamReadTimeout atomic.Uint64
	ConnectionAddFailed    atomic.Uint64
	ConnectionRemoved      atomic.Uint64
	ConnectionSetupError   atomic.Uint64
	ConnectionSetupTimeout atomic.Uint64
	RefusedTooManyOpen     atomic.Uint64
	RefusedPerIP           atomic.Uint64
	OpenConnections        atomic.Uint64
	QuicEndpointsCount     atomic.Uint64
	streamPayload          *streamPayloadRecorder // Immutable after Spawn.
}

// StreamReadPayload is a detached observation for one ServerStats lifetime.
// Bytes counts readUniStream's returned application prefix, including prefixes
// returned with errors. It excludes the overflow probe, encrypted framing and
// streams refused before reading; it is not accepted transaction traffic.
type StreamReadPayload struct {
	StartedAt, SampledAt time.Time
	Bytes                uint64
	Known                bool
}

type streamPayloadRecorder struct {
	startedAt time.Time
	bytes     atomic.Uint64
	closed    atomic.Bool
}

func (r *streamPayloadRecorder) add(n int) {
	if n <= 0 {
		return
	}
	for {
		before := r.bytes.Load()
		if before == math.MaxUint64 {
			return
		}
		after := uint64(math.MaxUint64)
		if uint64(n) < math.MaxUint64-before {
			after = before + uint64(n)
		}
		if r.bytes.CompareAndSwap(before, after) {
			return
		}
	}
}

// ObserveStreamReadPayload samples enabled, still-running server counters for a
// low-rate owner. Callers bind the result to their existing server generation;
// replacement ServerStats never shares this recorder. No transport I/O occurs.
func (s *ServerStats) ObserveStreamReadPayload() (StreamReadPayload, bool) {
	if s == nil || s.streamPayload == nil || s.streamPayload.closed.Load() {
		return StreamReadPayload{}, false
	}
	r := s.streamPayload
	n := r.bytes.Load()
	v := StreamReadPayload{StartedAt: r.startedAt, SampledAt: time.Now(), Known: n != math.MaxUint64}
	if v.Known {
		v.Bytes = n
	}
	if r.closed.Load() {
		return StreamReadPayload{}, false
	}
	return v, true
}
