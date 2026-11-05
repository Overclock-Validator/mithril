package gossip

import (
	"math"
	"net"
	"sync/atomic"
	"time"
)

// UDPPayloadObservation counts copied/written payload bytes from one Run, not
// complete datagrams, authenticated traffic or wire bandwidth. Directions are
// independent atomic reads; a saturated total is unknown, never wrapped zero.
type UDPPayloadObservation struct {
	Generation               uint64
	StartedAt, SampledAt     time.Time
	ReceivedBytes, SentBytes uint64
	ReceivedKnown, SentKnown bool
	Window                   UDPPayloadWindow
}

// UDPPayloadWindow is a same-Run difference between original owner samples.
// Duration retains monotonic elapsed time. Directions are independently known;
// this is neither full wire bandwidth nor proof of authentication or delivery.
type UDPPayloadWindow struct {
	StartedAt, SampledAt     time.Time
	Duration                 time.Duration
	ReceivedBytes, SentBytes uint64
	ReceivedKnown, SentKnown bool
}

func observedUDPPayloadWindow(before, after UDPPayloadObservation) UDPPayloadWindow {
	elapsed := after.SampledAt.Sub(before.SampledAt)
	if before.Generation == 0 || before.Generation != after.Generation || before.StartedAt.IsZero() ||
		!before.StartedAt.Equal(after.StartedAt) || before.SampledAt.IsZero() || after.SampledAt.IsZero() ||
		before.SampledAt.Before(before.StartedAt) || elapsed <= 0 || elapsed > 30*time.Second {
		return UDPPayloadWindow{}
	}
	w := UDPPayloadWindow{StartedAt: before.SampledAt, SampledAt: after.SampledAt, Duration: elapsed}
	if before.ReceivedKnown && after.ReceivedKnown && before.ReceivedBytes != math.MaxUint64 &&
		after.ReceivedBytes != math.MaxUint64 && after.ReceivedBytes >= before.ReceivedBytes {
		w.ReceivedKnown, w.ReceivedBytes = true, after.ReceivedBytes-before.ReceivedBytes
	}
	if before.SentKnown && after.SentKnown && before.SentBytes != math.MaxUint64 &&
		after.SentBytes != math.MaxUint64 && after.SentBytes >= before.SentBytes {
		w.SentKnown, w.SentBytes = true, after.SentBytes-before.SentBytes
	}
	if !w.ReceivedKnown && !w.SentKnown {
		return UDPPayloadWindow{}
	}
	return w
}

// udpPayloadRecorder is pinned with the actual socket through each Run's
// helpers. Late work from a retired recvLoop cannot update its successor.
type udpPayloadRecorder struct {
	conn       *net.UDPConn
	generation uint64
	startedAt  time.Time
	received   atomic.Uint64
	sent       atomic.Uint64
}

func addUDPPayloadBytes(counter *atomic.Uint64, n, limit int) {
	if n <= 0 || n > limit {
		return
	}
	for {
		before := counter.Load()
		if before == math.MaxUint64 {
			return
		}
		after := uint64(math.MaxUint64)
		if uint64(n) < math.MaxUint64-before {
			after = before + uint64(n)
		}
		if counter.CompareAndSwap(before, after) {
			return
		}
	}
}

func (r *udpPayloadRecorder) recordReceived(conn *net.UDPConn, n, limit int) {
	if r != nil && r.conn != nil && r.conn == conn {
		addUDPPayloadBytes(&r.received, n, limit)
	}
}

func (r *udpPayloadRecorder) recordSent(conn *net.UDPConn, n, limit int) {
	if r != nil && r.conn != nil && r.conn == conn {
		addUDPPayloadBytes(&r.sent, n, limit)
	}
}

func (r *udpPayloadRecorder) snapshot() UDPPayloadObservation {
	received, sent := r.received.Load(), r.sent.Load()
	o := UDPPayloadObservation{Generation: r.generation, StartedAt: r.startedAt, SampledAt: time.Now(),
		ReceivedKnown: received != math.MaxUint64, SentKnown: sent != math.MaxUint64}
	if o.ReceivedKnown {
		o.ReceivedBytes = received
	}
	if o.SentKnown {
		o.SentBytes = sent
	}
	return o
}

// EnableUDPPayloadMonitoring selects recording before the next Run. Enabling
// it during Run does not claim coverage of that already-started socket.
func (c *Client) EnableUDPPayloadMonitoring() {
	if c != nil {
		c.udpPayloadEnabled.Store(true)
	}
}
