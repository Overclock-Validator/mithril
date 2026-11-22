package node

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func TestPayloadWindowSameOwnerAndMonotonicBounds(t *testing.T) {
	start := time.Now()
	before := payloadSample{startedAt: start, sampledAt: start.Add(time.Second), bytes: 10, known: true}
	after := payloadSample{startedAt: start, sampledAt: start.Add(3 * time.Second), bytes: 16, known: true}
	w := payloadWindow(before, after, nil)
	if w == nil || w.Bytes != 6 || w.DurationNanoseconds != uint64(2*time.Second) || !w.SampledAt.Equal(after.sampledAt) {
		t.Fatalf("wrong measured payload interval: %+v", w)
	}
	zero := after
	zero.bytes = before.bytes
	if w := payloadWindow(before, zero, nil); w == nil || w.Bytes != 0 {
		t.Fatal("known zero interval was lost")
	}
	if repeated := payloadWindow(after, after, w); repeated != w {
		t.Fatal("identical cached source did not retain its original window")
	}
	changed := after
	changed.bytes++
	if payloadWindow(after, changed, w) != nil {
		t.Fatal("a changed counter with the same sample time retained an interval")
	}
	for name, change := range map[string]func(*payloadSample){
		"unknown":         func(p *payloadSample) { p.known = false },
		"new owner":       func(p *payloadSample) { p.startedAt = p.startedAt.Add(time.Second) },
		"counter reset":   func(p *payloadSample) { p.bytes = before.bytes - 1 },
		"no elapsed time": func(p *payloadSample) { p.sampledAt = before.sampledAt },
		"long gap": func(p *payloadSample) {
			p.sampledAt = before.sampledAt.Add(dashboard.NetworkDetailFreshFor + time.Nanosecond)
		},
		"wall clock only": func(p *payloadSample) { p.sampledAt = p.sampledAt.Round(0) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := after
			change(&bad)
			if payloadWindow(before, bad, nil) != nil {
				t.Fatal("invalid interval became a rate")
			}
		})
	}
}
