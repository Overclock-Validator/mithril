package dashboard

import (
	"testing"
	"time"
)

func TestStatusLeaseContinuityRequiresBothClockBounds(t *testing.T) {
	for _, tc := range []struct {
		monotonic, wall time.Duration
		want            bool
	}{
		{time.Second, time.Second, true},
		{time.Second, -time.Millisecond, false},
		{time.Second, 0, false},
		{-time.Millisecond, time.Second, false},
		{0, time.Second, false},
	} {
		if got := statusLeaseContinuous(tc.monotonic, tc.wall); got != tc.want {
			t.Fatalf("monotonic=%v wall=%v: got %v, want %v", tc.monotonic, tc.wall, got, tc.want)
		}
	}
}

func TestStatusBindingChanges(t *testing.T) {
	now := time.Now()
	s := testSnapshot(now.UTC())
	identity, genesis := "identity", "genesis"
	s.IdentityKey, s.GenesisHash = &identity, &genesis
	if !sameStatusBinding(&s, &s) || !sameStatusBinding(nil, nil) || sameStatusBinding(&s, nil) {
		t.Fatal("incorrect equality for identical or nil bindings")
	}
	for _, mutate := range []func(*Snapshot){
		func(s *Snapshot) { s.RunID = "different" },
		func(s *Snapshot) { s.StartedAt = s.StartedAt.Add(time.Second) },
		func(s *Snapshot) { s.IdentityKey = nil },
		func(s *Snapshot) { s.GenesisHash = nil },
		func(s *Snapshot) { value := "other"; s.IdentityKey = &value },
		func(s *Snapshot) { value := "other"; s.GenesisHash = &value },
	} {
		changed := s
		mutate(&changed)
		if sameStatusBinding(&s, &changed) {
			t.Fatal("binding change not detected")
		}
	}
}
