package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"
)

func maximumHarmonic(now time.Time) *Harmonic {
	// False occupies more JSON bytes than true. All five gauges remain known.
	return &Harmonic{Enabled: true, Strategy: "mrev", Observation: &HarmonicObservation{StartedAt: now, SampledAt: now}}
}

func harmonicFixture(now time.Time) Snapshot {
	s := testSnapshot(now)
	s.Mode = "validator"
	s.Harmonic = maximumHarmonic(now)
	return s
}

func TestHarmonicProjectionStatesAndDetachedValues(t *testing.T) {
	now := sizingTime(time.Now())
	s := harmonicFixture(now)
	p := publicHarmonic(&s, "fresh", now)
	if p == nil || p.State != "fresh" || p.Strategy == nil || *p.Strategy != "mrev" || p.BlockEngineHealthy == nil || *p.BlockEngineHealthy {
		t.Fatal("harmonic projection changed known values")
	}
	s.Harmonic.Observation.BlockEngineHealthy = true
	if *p.BlockEngineHealthy {
		t.Fatal("projection retained mutable source values")
	}
}

func TestHarmonicProjectionFreshnessAndBinding(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name   string
		change func(*Snapshot)
	}{
		{"verifying", func(s *Snapshot) { s.Mode = "verifying" }},
		{"classic", func(s *Snapshot) { s.Alpenglow = false }},
		{"old producer", func(s *Snapshot) { s.Harmonic = nil }},
		{"disabled strategy", func(s *Snapshot) { s.Harmonic.Enabled = false; s.Harmonic.Observation = nil }},
		{"disabled observation", func(s *Snapshot) { s.Harmonic.Enabled = false; s.Harmonic.Strategy = "" }},
		{"unknown strategy", func(s *Snapshot) { s.Harmonic.Strategy = "https://secret.invalid/token" }},
		{"missing node start", func(s *Snapshot) { s.StartedAt = time.Time{} }},
		{"missing service start", func(s *Snapshot) { s.Harmonic.Observation.StartedAt = time.Time{} }},
		{"previous node", func(s *Snapshot) { s.Harmonic.Observation.StartedAt = s.StartedAt.Add(-time.Nanosecond) }},
		{"sample before service", func(s *Snapshot) {
			s.Harmonic.Observation.SampledAt = s.Harmonic.Observation.StartedAt.Add(-time.Nanosecond)
		}},
		{"sample beyond node", func(s *Snapshot) { s.SampledAt = now.Add(-time.Second - time.Nanosecond) }},
		{"sample beyond collector", func(s *Snapshot) {
			s.SampledAt = now.Add(time.Second)
			s.Harmonic.Observation.SampledAt = now.Add(time.Second + time.Nanosecond)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := harmonicFixture(now)
			tc.change(&s)
			if publicHarmonic(&s, "fresh", now) != nil {
				t.Fatal("invalid optional group survived")
			}
		})
	}
	s := harmonicFixture(now)
	for _, state := range []string{"unavailable", "healthy", ""} {
		if publicHarmonic(&s, state, now) != nil {
			t.Fatal("invalid parent state survived")
		}
	}
	if publicHarmonic(nil, "fresh", now) != nil {
		t.Fatal("absent source became enabled")
	}
	for _, tc := range []struct {
		parent string
		age    time.Duration
		want   string
	}{
		{"fresh", 5 * time.Second, "fresh"},
		{"fresh", 5*time.Second + time.Nanosecond, "stale"},
		{"stale", 0, "stale"},
		{"fresh", -time.Second, "fresh"},
	} {
		at := now.Add(-tc.age)
		s.Harmonic = maximumHarmonic(at)
		s.StartedAt = now.Add(-time.Minute)
		p := publicHarmonic(&s, tc.parent, now)
		if p == nil || p.State != tc.want || p.SampledAt == nil || !p.SampledAt.Equal(at) || p.BlockEngineHealthy == nil {
			t.Fatalf("age %s parent %s: %+v", tc.age, tc.parent, p)
		}
	}
}

func TestHarmonicDefaultOffAndPrivateFrames(t *testing.T) {
	now := time.Now().UTC()
	s := harmonicFixture(now)
	c := newCollector("/unused.sock")
	defer c.client.CloseIdleConnections()
	c.public = true
	for _, options := range []PublicOptions{{}, {Resources: true}, {Harmonic: true}} {
		c.publicOptions = options
		with, err := c.encode(&s, "fresh", now)
		if err != nil || bytes.Contains(with.publicJSON, []byte(`"harmonic"`)) {
			t.Fatal("Harmonic escaped its disclosure options")
		}
		absent := s
		absent.Harmonic = nil
		without, err := c.encode(&absent, "fresh", now)
		if err != nil || !bytes.Equal(with.publicJSON, without.publicJSON) {
			t.Fatal("default-off option changed existing public bytes")
		}
	}
	c.publicOptions = PublicOptions{Resources: true, Harmonic: true}
	set, err := c.encode(&s, "fresh", now)
	var p PublicSnapshot
	if err != nil || json.Unmarshal(set.advancedPublicJSON, &p) != nil || p.Harmonic == nil || p.Harmonic.State != "fresh" {
		t.Fatal("enabled summary missing")
	}
	private, err := frames(&s, "fresh", now)
	if err != nil || decodedFrames(t, &frameSet{messages: private})["mithril.harmonic"] == nil {
		t.Fatal("private summary missing")
	}
	s.Harmonic = nil
	private, err = frames(&s, "fresh", now)
	if err != nil || len(private) != 16 || decodedFrames(t, &frameSet{messages: private})["mithril.harmonic"] != nil {
		t.Fatal("old private protocol changed")
	}
}

func TestHarmonicRequiresPublicProfileAndResources(t *testing.T) {
	for _, tc := range []struct {
		origin string
		opts   PublicOptions
	}{
		{"", PublicOptions{Harmonic: true}},
		{"", PublicOptions{Resources: true, Harmonic: true}},
		{"https://stats.example", PublicOptions{Harmonic: true}},
	} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if err := serveProfile(context.Background(), "/unused.sock", listener, tc.origin, tc.opts); err == nil || !strings.Contains(err.Error(), "resource disclosure") {
			t.Fatalf("invalid disclosure options accepted: %+v, %v", tc, err)
		}
	}
}

func TestHarmonicRealFeedCollectorDoesNotRedateSource(t *testing.T) {
	now := time.Now().UTC()
	s := harmonicFixture(now)
	path := socketPath(t)
	feed, err := ListenFeed(context.Background(), path, func() Snapshot { return s })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	c := newCollector(path)
	defer c.client.CloseIdleConnections()
	c.public = true
	c.publicOptions = PublicOptions{Resources: true, Harmonic: true}
	c.poll(context.Background(), now)
	decode := func() PublicSnapshot {
		t.Helper()
		set := c.latest.Load()
		var p PublicSnapshot
		if set == nil || json.Unmarshal(set.advancedPublicJSON, &p) != nil {
			t.Fatal("actual collector response missing")
		}
		return p
	}
	p := decode()
	if p.Harmonic == nil || p.Harmonic.State != "fresh" || !p.Harmonic.SampledAt.Equal(now) {
		t.Fatal("real feed lost source sample")
	}
	feed.Close()
	c.poll(context.Background(), now.Add(6*time.Second))
	p = decode()
	if p.SourceState != "stale" || p.Harmonic == nil || p.Harmonic.State != "stale" || !p.Harmonic.SampledAt.Equal(now) || p.Harmonic.BlockEngineHealthy == nil || *p.Harmonic.BlockEngineHealthy {
		t.Fatal("feed loss redated source or changed a known false")
	}
}
