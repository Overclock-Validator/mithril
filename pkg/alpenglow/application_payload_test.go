package alpenglow

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestApplicationPayloadSaturationAndDisabled(t *testing.T) {
	for _, tc := range []struct {
		total uint64
		n     int
		want  uint64
	}{
		{0, 0, 0}, {0, -1, 0}, {0, 3, 3}, {math.MaxUint64 - 2, 2, math.MaxUint64},
		{math.MaxUint64 - 2, 3, math.MaxUint64}, {math.MaxUint64, 3, math.MaxUint64},
	} {
		if got := saturatingApplicationBytes(tc.total, tc.n); got != tc.want {
			t.Fatalf("add(%d,%d)=%d, want %d", tc.total, tc.n, got, tc.want)
		}
	}
	var total atomic.Uint64
	var writers sync.WaitGroup
	for range 4 {
		writers.Go(func() {
			for range 100 {
				addApplicationBytes(&total, 3)
			}
		})
	}
	writers.Wait()
	if total.Load() != 1200 {
		t.Fatal("concurrent accepted payloads were lost")
	}
	var absent *Receiver
	if _, ok := absent.ApplicationPayloadObservation(); ok {
		t.Fatal("nil receiver reported data")
	}
	r := &Receiver{}
	r.recordDatagram(5)
	r.recordRateLimitedDatagram(7)
	if _, ok := r.ApplicationPayloadObservation(); ok || r.payloadBytes != 0 {
		t.Fatal("ordinary receiver enabled byte accounting")
	}
	r.cfg.RecordApplicationPayload = true
	r.payloadStartedAt = time.Now()
	r.recordDatagram(5)
	r.recordRateLimitedDatagram(7)
	o, ok := r.ApplicationPayloadObservation()
	if !ok || !o.Known || o.Bytes != 12 || o.StartedAt != r.payloadStartedAt || o.SampledAt.Before(o.StartedAt) || r.Stats().DatagramsReceived != 2 || r.Stats().RateLimitedDatagrams != 2 {
		t.Fatal("app-byte accounting changed old counters or its independent lifetime")
	}
	r.payloadBytes = math.MaxUint64
	if o, ok := r.ApplicationPayloadObservation(); !ok || o.Known || o.Bytes != 0 {
		t.Fatal("saturation became a known total")
	}
	r.closed = true
	if _, ok := r.ApplicationPayloadObservation(); ok {
		t.Fatal("closed receiver retained availability")
	}
}
