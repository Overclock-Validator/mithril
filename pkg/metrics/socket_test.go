package metrics

import (
	"math"
	"net"
	"runtime"
	"testing"
	"time"
)

func TestObservedSocketDrops(t *testing.T) {
	at := time.Now()
	before := SocketObservation{Known: true, Reason: SocketAvailable, StartedAt: at.Add(-time.Millisecond), SampledAt: at, Memory: SocketMemory{Drops: 4}}
	after := before
	after.StartedAt, after.SampledAt, after.Memory.Drops = at.Add(time.Second-time.Millisecond), at.Add(time.Second), 9
	got := ObservedSocketDrops(before, after)
	if !got.Known || got.Drops != 5 || got.StartedAt != before.SampledAt || got.SampledAt != after.SampledAt {
		t.Fatalf("observed drops = %+v", got)
	}
	for name, change := range map[string]func(*SocketObservation, *SocketObservation){
		"unknown_before": func(b, _ *SocketObservation) { b.Known = false },
		"unknown_after":  func(_, a *SocketObservation) { a.Known = false },
		"unsupported":    func(_, a *SocketObservation) { a.Reason = SocketUnsupported },
		"missing_start":  func(b, _ *SocketObservation) { b.StartedAt = time.Time{} },
		"missing_end":    func(_, a *SocketObservation) { a.SampledAt = time.Time{} },
		"reversed_read":  func(_, a *SocketObservation) { a.StartedAt = a.SampledAt.Add(time.Nanosecond) },
		"slow_read":      func(b, _ *SocketObservation) { b.StartedAt = b.SampledAt.Add(-time.Second - time.Nanosecond) },
		"same_endpoint":  func(b, a *SocketObservation) { *a = *b },
		"long_gap": func(b, a *SocketObservation) {
			a.SampledAt = b.SampledAt.Add(30*time.Second + time.Nanosecond)
			a.StartedAt = a.SampledAt
		},
		"counter_reset": func(b, a *SocketObservation) { a.Memory.Drops = b.Memory.Drops - 1 },
	} {
		t.Run(name, func(t *testing.T) {
			b, a := before, after
			change(&b, &a)
			if ObservedSocketDrops(b, a) != (SocketDropDelta{}) {
				t.Fatal("invalid interval acquired coverage")
			}
		})
	}
	after.Memory.Drops = before.Memory.Drops
	if zero := ObservedSocketDrops(before, after); !zero.Known || zero.Drops != 0 {
		t.Fatal("known-zero delta became unknown")
	}
	after.StartedAt, after.SampledAt = at.Add(30*time.Second), at.Add(30*time.Second)
	if !ObservedSocketDrops(before, after).Known {
		t.Fatal("exact freshness boundary was lost")
	}
	if allocations := testing.AllocsPerRun(100, func() { ObservedSocketDrops(before, after) }); allocations != 0 {
		t.Fatalf("pure drop interval allocated %v times", allocations)
	}
}

func TestSocketObservationNilAndUnsupported(t *testing.T) {
	want := SocketObservation{Reason: SocketNotProvided}
	if got := ObserveUDPSocketMemory(nil); got != want {
		t.Fatalf("nil observation = %+v, want %+v", got, want)
	}
	if allocations := testing.AllocsPerRun(100, func() { ObserveUDPSocketMemory(nil) }); allocations != 0 {
		t.Fatalf("nil observation allocated %v times", allocations)
	}
	if runtime.GOOS != "linux" {
		want = SocketObservation{Reason: SocketUnsupported}
		if got := ObserveUDPSocketMemory(&net.UDPConn{}); got != want {
			t.Fatalf("unsupported observation = %+v, want %+v", got, want)
		}
	}
}

func TestSocketObservationFinalization(t *testing.T) {
	start := time.Now()
	values := [9]uint32{0, 1, 2, 3, 4, 5, 6, 7, math.MaxUint32}
	want := SocketMemory{
		ReceiveAllocated: 0, ReceiveLimit: 1, SendAllocated: 2, SendLimit: 3,
		ForwardAllocated: 4, SendQueued: 5, OptionsAllocated: 6, Backlog: 7, Drops: math.MaxUint32,
	}
	for _, elapsed := range []time.Duration{0, time.Nanosecond, socketCaptureBudget} {
		got := finishSocketObservation(start, start.Add(elapsed), values, 36, SocketAvailable)
		if !got.Known || got.Reason != SocketAvailable || got.Memory != want || got.StartedAt != start || got.SampledAt != start.Add(elapsed) {
			t.Fatalf("valid capture (%v) = %+v", elapsed, got)
		}
	}
	zero := finishSocketObservation(start, start, [9]uint32{}, 36, SocketAvailable)
	if !zero.Known || zero.Memory != (SocketMemory{}) {
		t.Fatalf("known zero = %+v", zero)
	}
	for i := range values {
		values[i] = math.MaxUint32
	}
	max := finishSocketObservation(start, start, values, 36, SocketAvailable)
	wantMax := SocketMemory{
		ReceiveAllocated: math.MaxUint32, ReceiveLimit: math.MaxUint32,
		SendAllocated: math.MaxUint32, SendLimit: math.MaxUint32,
		ForwardAllocated: math.MaxUint32, SendQueued: math.MaxUint32,
		OptionsAllocated: math.MaxUint32, Backlog: math.MaxUint32, Drops: math.MaxUint32,
	}
	if !max.Known || max.Memory != wantMax {
		t.Fatalf("maximum values = %+v", max)
	}
	values[0] = 0
	if max.Memory.ReceiveAllocated != math.MaxUint32 {
		t.Fatal("observation aliases the input array")
	}
	for _, size := range []uint32{0, 1, 32, 35, 37, 40, math.MaxUint32} {
		got := finishSocketObservation(start, start, values, size, SocketAvailable)
		if got.Known || got.Reason != SocketInvalidResponse || got.Memory != (SocketMemory{}) {
			t.Fatalf("invalid size %d = %+v", size, got)
		}
	}
	for _, reason := range []SocketReason{SocketUnsupported, SocketClosed, SocketReadFailed} {
		got := finishSocketObservation(start, start, values, 36, reason)
		if got.Known || got.Reason != reason || got.Memory != (SocketMemory{}) {
			t.Fatalf("failed capture %d = %+v", reason, got)
		}
	}
	for _, tc := range []struct {
		name   string
		start  time.Time
		end    time.Time
		reason SocketReason
	}{
		{"expired", start, start.Add(socketCaptureBudget + time.Nanosecond), SocketCaptureExpired},
		{"reversed", start, start.Add(-time.Nanosecond), SocketClockInvalid},
		{"missing_start", time.Time{}, start, SocketClockInvalid},
		{"missing_end", start, time.Time{}, SocketClockInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := finishSocketObservation(tc.start, tc.end, values, 36, SocketAvailable)
			if got.Known || got.Reason != tc.reason || got.Memory != (SocketMemory{}) || got.StartedAt != tc.start || got.SampledAt != tc.end {
				t.Fatalf("invalid capture = %+v", got)
			}
		})
	}
	for _, reason := range []SocketReason{SocketNotProvided, SocketInvalidResponse, SocketCaptureExpired, SocketClockInvalid, 255} {
		got := finishSocketObservation(start, start, values, 36, reason)
		if got.Known || got.Reason != SocketReadFailed || got.Memory != (SocketMemory{}) {
			t.Fatalf("unexpected read outcome %d = %+v", reason, got)
		}
	}
}
