package consensus

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestLiveClockObservationMatchesProtocolWithoutMutation(t *testing.T) {
	e := &AlpenglowObserverEngine{monitoringEnabled: true}
	for _, age := range []time.Duration{0, 250 * time.Millisecond, 750 * time.Millisecond, 10 * time.Second} {
		e.liveClockSlot, e.liveClockAt = 100, time.Now().Add(-age)
		anchor, at := e.liveClockSlot, e.liveClockAt
		before, known := e.AlpenglowLiveSlot()
		if !known {
			t.Fatal("fixture has no protocol clock")
		}
		o, known := e.ObserveLiveClock()
		after, protocolKnown := e.AlpenglowLiveSlot()
		if !known || !protocolKnown || o.EstimatedSlot < before || o.EstimatedSlot > after ||
			o.AnchorSlot != anchor || o.AnchorAt != at || o.SampledAt.Before(at) || o.AnchorAge != o.SampledAt.Sub(at) ||
			o.AddedSlots > 4 || o.EstimatedSlot != o.AnchorSlot+o.AddedSlots || e.liveClockSlot != anchor || e.liveClockAt != at {
			t.Fatalf("diagnostic diverged from protocol bracket or changed anchor: %+v", o)
		}
		o.AnchorSlot, o.AnchorAt = 0, time.Time{}
		if e.liveClockSlot != anchor || e.liveClockAt != at {
			t.Fatal("returned value aliases anchor")
		}
	}
	e.liveClockSlot, e.liveClockAt = 100, time.Now().Add(-time.Hour)
	o, ok := e.ObserveLiveClock()
	if !ok || o.EstimatedSlot != 104 || o.AnchorAge < time.Hour {
		t.Fatal("old anchor did not remain capped and dated")
	}
	oldAt := o.AnchorAt
	e.noteAlpenglowLiveSlot(101)
	o, ok = e.ObserveLiveClock()
	if !ok || o.AnchorSlot != 101 || o.AddedSlots != min(uint64(o.AnchorAge/alpenglowLiveClockSlotDuration), uint64(4)) ||
		!o.AnchorAt.After(oldAt) {
		t.Fatal("new event did not correct extrapolation")
	}
	e.liveClockSlot, e.liveClockAt = math.MaxUint64-1, time.Now().Add(-time.Hour)
	o, ok = e.ObserveLiveClock()
	if !ok || o.EstimatedSlot != math.MaxUint64 || o.AddedSlots != 1 {
		t.Fatal("uint64 saturation wrapped or overstated actual delta")
	}
}

func TestLiveClockObservationUnknownAndDefault(t *testing.T) {
	for _, e := range []*AlpenglowObserverEngine{nil, {}, {liveClockSlot: 100, liveClockAt: time.Now()},
		{monitoringEnabled: true}, {monitoringEnabled: true, liveClockSlot: 100},
		{monitoringEnabled: true, liveClockSlot: 100, liveClockAt: time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC)},
		{monitoringEnabled: true, liveClockSlot: 100, liveClockAt: time.Now().Add(time.Hour)}} {
		if o, ok := e.ObserveLiveClock(); ok || o != (LiveClockObservation{}) {
			t.Fatal("disabled or invalid clock acquired a measurement")
		}
	}
	e := &AlpenglowObserverEngine{}
	if n := testing.AllocsPerRun(1000, func() { e.ObserveLiveClock() }); n != 0 {
		t.Fatalf("disabled observer allocated: %f", n)
	}
}

func TestLiveClockObservationConcurrentAnchorUpdates(t *testing.T) {
	e := &AlpenglowObserverEngine{monitoringEnabled: true}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for slot := uint64(1); slot <= 1000; slot++ {
			e.noteAlpenglowLiveSlot(slot)
		}
	}()
	for i := 0; i < 1000; i++ {
		if o, ok := e.ObserveLiveClock(); ok && (o.AnchorSlot == 0 || o.EstimatedSlot < o.AnchorSlot || o.EstimatedSlot-o.AnchorSlot != o.AddedSlots || o.AddedSlots > 4) {
			t.Fatal("concurrent anchor produced mixed fields")
		}
	}
	wg.Wait()
}
