package turbine

import (
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

func TestReceiverSocketMemoryOwnerLifecycle(t *testing.T) {
	r := NewUDPReceiver("")
	r.socketMemory.observe = func(*net.UDPConn) metrics.SocketObservation {
		t.Fatal("observer called without an owner")
		return metrics.SocketObservation{}
	}
	if got, ok := r.ObserveSocketMemory(); ok || got != (ReceiverSocketMemory{}) {
		t.Fatalf("ownerless observation = %+v, %t", got, ok)
	}
	live, repair := &net.UDPConn{}, &net.UDPConn{}
	calls := map[*net.UDPConn]int{}
	r.socketMemory.observe = func(conn *net.UDPConn) metrics.SocketObservation {
		calls[conn]++
		return knownSocketMemory(time.Now(), uint32(len(calls)))
	}
	generation := r.attachSocketMemoryOwners(live, repair)
	if got, ok := r.ObserveSocketMemory(); !ok || got.Generation != generation || !got.RepairEnabled || calls[live] != 1 || calls[repair] != 1 {
		t.Fatalf("distinct owner observation = %+v, %t, calls=%v", got, ok, calls)
	}
	r.detachSocketMemoryOwners(generation)

	for _, tc := range []struct {
		name   string
		change func(*UDPReceiver, uint64, *net.UDPConn)
	}{
		{"replacement", func(r *UDPReceiver, _ uint64, next *net.UDPConn) { r.attachSocketMemoryOwners(next, nil) }},
		{"close", func(r *UDPReceiver, generation uint64, _ *net.UDPConn) { r.detachSocketMemoryOwners(generation) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewUDPReceiver("")
			current, next := &net.UDPConn{}, &net.UDPConn{}
			started, release := make(chan struct{}), make(chan struct{})
			r.socketMemory.observe = func(*net.UDPConn) metrics.SocketObservation {
				close(started)
				<-release
				return knownSocketMemory(time.Now(), 1)
			}
			generation := r.attachSocketMemoryOwners(current, nil)
			result := make(chan bool, 1)
			go func() {
				_, ok := r.ObserveSocketMemory()
				result <- ok
			}()
			<-started
			tc.change(r, generation, next)
			close(release)
			if <-result {
				t.Fatal("published an observation after owner generation changed")
			}
			if tc.name == "replacement" {
				r.socketMemory.mu.Lock()
				r.socketMemory.observe = func(*net.UDPConn) metrics.SocketObservation { return knownSocketMemory(time.Now(), 100) }
				r.socketMemory.mu.Unlock()
				if got, ok := r.ObserveSocketMemory(); !ok || got.Live.DropDelta.Known {
					t.Fatalf("replacement inherited the prior baseline: %+v, %t", got, ok)
				}
			}
		})
	}
}

func TestReceiverSocketMemoryDropWindowsAndFailureReset(t *testing.T) {
	r := NewUDPReceiver("")
	live := &net.UDPConn{}
	start := time.Now()
	observations := []metrics.SocketObservation{
		knownSocketMemory(start, 10),
		knownSocketMemory(start.Add(socketMemoryFreshness), 15),
		{StartedAt: start.Add(31 * time.Second), SampledAt: start.Add(31 * time.Second), Reason: metrics.SocketReadFailed},
		knownSocketMemory(start.Add(32*time.Second), 2),
		knownSocketMemory(start.Add(33*time.Second), 4),
		knownSocketMemory(start.Add(34*time.Second), 4),
		knownSocketMemory(start.Add(35*time.Second), 3),
		knownSocketMemory(start.Add(36*time.Second), 5),
		knownSocketMemory(start.Add(67*time.Second), 8),
	}
	r.socketMemory.observe = func(conn *net.UDPConn) metrics.SocketObservation {
		if conn != live {
			t.Fatalf("observed unowned socket %p", conn)
		}
		got := observations[0]
		observations = observations[1:]
		return got
	}
	generation := r.attachSocketMemoryOwners(live, nil)
	read := func() ReceiverSocketMemory {
		t.Helper()
		got, ok := r.ObserveSocketMemory()
		if !ok || got.Generation != generation || got.RepairEnabled || got.Repair.Observation.Reason != metrics.SocketNotProvided {
			t.Fatalf("observation = %+v, %t", got, ok)
		}
		return got
	}

	if got := read(); got.Live.DropDelta.Known {
		t.Fatal("first sample had a drop delta")
	}
	if got := read(); !got.Live.DropDelta.Known || got.Live.DropDelta.Drops != 5 || got.Live.DropDelta.StartedAt != start || got.Live.DropDelta.SampledAt != start.Add(socketMemoryFreshness) {
		t.Fatalf("exact-boundary delta = %+v", got.Live.DropDelta)
	}
	if got := read(); got.Live.Observation.Known || got.Live.DropDelta.Known {
		t.Fatalf("failed read retained known state: %+v", got.Live)
	}
	if got := read(); got.Live.DropDelta.Known {
		t.Fatal("recovery sample reused the failed baseline")
	}
	if got := read(); !got.Live.DropDelta.Known || got.Live.DropDelta.Drops != 2 {
		t.Fatalf("recovery delta = %+v", got.Live.DropDelta)
	}
	if got := read(); !got.Live.DropDelta.Known || got.Live.DropDelta.Drops != 0 {
		t.Fatalf("known zero delta = %+v", got.Live.DropDelta)
	}
	if got := read(); got.Live.DropDelta.Known {
		t.Fatal("counter decrease was treated as a wrap")
	}
	if got := read(); !got.Live.DropDelta.Known || got.Live.DropDelta.Drops != 2 {
		t.Fatalf("post-reset delta = %+v", got.Live.DropDelta)
	}
	if got := read(); got.Live.DropDelta.Known {
		t.Fatal("over-30-second interval produced a delta")
	}

	sample := SocketMemorySample{Observation: knownSocketMemory(start, 0)}
	if !sample.Fresh(start.Add(socketMemoryFreshness)) || sample.Fresh(start.Add(socketMemoryFreshness+time.Nanosecond)) || sample.Fresh(start.Add(-time.Nanosecond)) {
		t.Fatal("socket freshness boundary is not exact")
	}
}

func knownSocketMemory(at time.Time, drops uint32) metrics.SocketObservation {
	return metrics.SocketObservation{StartedAt: at, SampledAt: at, Known: true, Reason: metrics.SocketAvailable,
		Memory: metrics.SocketMemory{Drops: drops}}
}
