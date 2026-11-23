package dashboard

import (
	"strings"
	"testing"
	"time"
)

func receptionFixture(at time.Time) Snapshot {
	s := publicTestSnapshot(at)
	s.BlockSource.StartedAt = at.Add(-10 * time.Second)
	slot, observed := uint64(0), at.Add(-4*time.Second)
	s.BlockSource.Reception = &Reception{SampledAt: at, StartedAt: at.Add(-9 * time.Second), Session: 2,
		WindowMilliseconds: 4800, Slot: &slot, ObservedAt: &observed, RetainedSlots: 1}
	return s
}

func TestPublicReceptionExpiresIndependentlyOfHeartbeat(t *testing.T) {
	at := time.Now().UTC()
	s := receptionFixture(at)
	p, _ := publicValue(t, &s, "fresh", at)
	if r := p.Input.Reception; r == nil || r.Slot == nil || *r.Slot != 0 || r.State != "fresh" || r.Session != 2 || r.WindowMilliseconds != 4800 {
		t.Fatalf("known zero or reception binding lost: %+v", r)
	}
	for _, tc := range []struct {
		now   time.Time
		known bool
	}{
		{at.Add(800*time.Millisecond - time.Nanosecond), true},
		{at.Add(800 * time.Millisecond), false},
	} {
		p, _ = publicValue(t, &s, "fresh", tc.now)
		r := p.Input.Reception
		if r == nil || r.State != "fresh" || (r.Slot != nil) != tc.known || (r.ObservedAt != nil) != tc.known {
			t.Fatalf("reference TTL at %s: %+v, want known=%t", tc.now, r, tc.known)
		}
	}
	// The reference can still be younger than 4.8 seconds when its independently
	// sampled one-second producer has stopped updating for over three seconds.
	observed := at
	s.BlockSource.Reception.ObservedAt = &observed
	p, _ = publicValue(t, &s, "fresh", at.Add(3*time.Second+time.Nanosecond))
	if r := p.Input.Reception; r == nil || r.State != "stale" || r.Slot != nil || r.ObservedAt != nil {
		t.Fatalf("stalled reception sample appeared current: %+v", r)
	}
	p, _ = publicValue(t, &s, "stale", at)
	if r := p.Input.Reception; r == nil || r.State != "stale" || r.Slot != nil {
		t.Fatal("stale feed retained a current reference")
	}
}

func TestPublicReceptionLimitsLifecycleAndDetachedCopy(t *testing.T) {
	at := time.Now().UTC()
	for _, mutate := range []func(*Snapshot){
		func(s *Snapshot) { s.BlockSource.LiveConnected = false },
		func(s *Snapshot) { s.BlockSource.Reception.Session = 0 },
		func(s *Snapshot) { s.BlockSource.Reception.WindowMilliseconds = 0 },
		func(s *Snapshot) { s.BlockSource.Reception.WindowMilliseconds = 60001 },
		func(s *Snapshot) { s.BlockSource.Reception.RetainedSlots = -1 },
		func(s *Snapshot) { s.BlockSource.Reception.RetainedSlots = 65 },
		func(s *Snapshot) { s.BlockSource.Reception.RetainedSlots = 0 },
		func(s *Snapshot) { s.BlockSource.Reception.CoverageLimited = true },
		func(s *Snapshot) { s.BlockSource.Reception.ObservedAt = nil },
		func(s *Snapshot) { s.BlockSource.Reception.Slot = nil },
		func(s *Snapshot) { s.BlockSource.StartedAt = s.StartedAt.Add(-time.Second) },
		func(s *Snapshot) { s.BlockSource.Reception.StartedAt = s.StartedAt.Add(-time.Second) },
		func(s *Snapshot) { s.BlockSource.Reception.StartedAt = at.Add(time.Second) },
		func(s *Snapshot) { s.BlockSource.Reception.SampledAt = at.Add(2 * time.Second) },
		func(s *Snapshot) { s.BlockSource.Reception.SampledAt = s.StartedAt },
		func(s *Snapshot) { value := at.Add(time.Nanosecond); s.BlockSource.Reception.ObservedAt = &value },
		func(s *Snapshot) {
			value := at.Add(-4800 * time.Millisecond)
			s.BlockSource.Reception.ObservedAt = &value
		},
	} {
		s := receptionFixture(at)
		mutate(&s)
		p, _ := publicValue(t, &s, "fresh", at)
		if p.Input.Reception != nil {
			t.Fatalf("invalid reception survived: %+v", s.BlockSource.Reception)
		}
	}
	s := receptionFixture(at)
	max := ^uint64(0)
	s.BlockSource.Reception.Slot = &max
	p, raw := publicValue(t, &s, "fresh", at)
	if !strings.Contains(string(raw), `"slot":"18446744073709551615"`) {
		t.Fatal("recent slot lost uint64 precision")
	}
	max = 3
	if *p.Input.Reception.Slot != ^uint64(0) {
		t.Fatal("projection aliases the producer slot")
	}
	s.BlockSource.Reception.Slot, s.BlockSource.Reception.ObservedAt = nil, nil
	s.BlockSource.Reception.CoverageLimited, s.BlockSource.Reception.RetainedSlots = true, 64
	p, _ = publicValue(t, &s, "fresh", at)
	if r := p.Input.Reception; r == nil || !r.CoverageLimited || r.Slot != nil || r.ObservedAt != nil || r.RetainedSlots != 64 {
		t.Fatal("capacity loss fabricated a maximum or erased its coverage status")
	}
	s.BlockSource.Reception = nil
	p, _ = publicValue(t, &s, "fresh", at)
	if p.Input.Reception != nil {
		t.Fatal("old producer acquired a fabricated reception object")
	}
}
