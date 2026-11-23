package dashboard

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"
)

func maximumSnapshotArchive(now time.Time) *SnapshotArchive {
	total := uint64(math.MaxUint64)
	return &SnapshotArchive{Attempt: math.MaxUint64, StartedAt: now, SampledAt: now,
		Phase: "interrupted", FullSnapshotSlot: math.MaxUint64, InputKind: "remote",
		CompressedInputBytes: math.MaxUint64, CompressedReportedTotal: &total, AccountPayloadBytes: math.MaxUint64}
}

func TestSnapshotManifestProjection(t *testing.T) {
	now := time.Now().UTC()
	s := testSnapshot(now)
	s.SnapshotManifests = &SnapshotManifests{Attempt: 1,
		Full:        &SnapshotManifestParse{Slot: math.MaxUint64, ParsedAt: now},
		Incremental: &SnapshotManifestParse{Slot: 0, ParsedAt: now},
		ReadyMarker: &SnapshotReadyMarker{Slot: 0, ObservedAt: now}}
	p := publicStartup(&s, "fresh", now)
	if p.SnapshotManifests == nil || p.SnapshotManifests.Full.Slot != math.MaxUint64 || p.SnapshotManifests.Incremental.Slot != 0 {
		t.Fatalf("manifest projection: %+v", p.SnapshotManifests)
	}
	if p.SnapshotManifests.ReadyMarker == nil || p.SnapshotManifests.ReadyMarker.Slot != 0 {
		t.Fatal("selected snapshot marker missing")
	}
	p.SnapshotManifests.ReadyMarker.Slot = 7
	if s.SnapshotManifests.ReadyMarker.Slot != 0 {
		t.Fatal("public marker aliases source")
	}
	s.SnapshotManifests.ReadyMarker.Slot = 1
	if got := publicStartup(&s, "fresh", now).SnapshotManifests; got == nil || got.ReadyMarker != nil {
		t.Fatal("mismatched marker was published")
	}
	s.SnapshotManifests.ReadyMarker.Slot = 0
	s.SnapshotManifests.ReadyMarker.ObservedAt = now.Add(-time.Nanosecond)
	if got := publicStartup(&s, "fresh", now).SnapshotManifests; got == nil || got.ReadyMarker != nil {
		t.Fatal("marker preceding the selected parse was published")
	}
	p.SnapshotManifests.Full.Slot = 1
	if s.SnapshotManifests.Full.Slot != math.MaxUint64 {
		t.Fatal("public value aliases source")
	}
	s.SnapshotManifests.Full.ParsedAt = s.StartedAt.Add(-time.Second)
	p = publicStartup(&s, "fresh", now)
	if p.SnapshotManifests != nil {
		t.Fatal("published out-of-run parse")
	}
}

func TestPublicSnapshotManifestFieldBoundary(t *testing.T) {
	field, _ := reflect.TypeOf(PublicStartup{}).FieldByName("SnapshotManifests")
	if field.Type != reflect.TypeOf((*PublicSnapshotManifests)(nil)) {
		t.Fatal("public startup must use a separate manifest type")
	}
	manifest := reflect.TypeOf(PublicSnapshotManifests{})
	for _, name := range []string{"Full", "Incremental"} {
		field, _ := manifest.FieldByName(name)
		if field.Type != reflect.TypeOf((*PublicSnapshotManifestParse)(nil)) {
			t.Fatalf("%s must use a separate public type", name)
		}
	}
	field, _ = manifest.FieldByName("ReadyMarker")
	if field.Type != reflect.TypeOf((*PublicSnapshotReadyMarker)(nil)) {
		t.Fatal("ready marker must use a separate public type")
	}
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	p := PublicSnapshotManifests{Attempt: 1, Full: &PublicSnapshotManifestParse{Slot: 42, ParsedAt: at},
		Incremental: &PublicSnapshotManifestParse{Slot: 43, ParsedAt: at}, ReadyMarker: &PublicSnapshotReadyMarker{Slot: 43, ObservedAt: at}}
	got, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"attempt":"1","full":{"slot":"42","parsed_at":"2026-01-02T03:04:05Z"},"incremental":{"slot":"43","parsed_at":"2026-01-02T03:04:05Z"},"ready_marker":{"slot":"43","observed_at":"2026-01-02T03:04:05Z"}}`
	if string(got) != want {
		t.Fatalf("unexpected public manifest JSON: got %s, want %s", got, want)
	}
}

func TestSnapshotArchiveProjection(t *testing.T) {
	now := time.Now().UTC()
	s := testSnapshot(now)
	s.SnapshotArchive = maximumSnapshotArchive(now)
	for _, phase := range []string{"active", "complete", "interrupted"} {
		s.SnapshotArchive.Phase = phase
		for _, kind := range []string{"local", "remote"} {
			s.SnapshotArchive.InputKind = kind
			p, _ := publicValue(t, &s, "fresh", now)
			a := p.Startup.SnapshotArchive
			if a == nil || a.State != "fresh" || a.Phase != phase || a.InputKind != kind ||
				a.CompressedReportedTotal == nil || *a.CompressedReportedTotal != math.MaxUint64 {
				t.Fatalf("archive projection: %+v", a)
			}
			*a.CompressedReportedTotal = 1
			if *s.SnapshotArchive.CompressedReportedTotal != math.MaxUint64 {
				t.Fatal("public archive aliases source total")
			}
		}
	}
	s.SnapshotArchive.CompressedReportedTotal = nil
	s.SnapshotArchive.FullSnapshotSlot, s.SnapshotArchive.CompressedInputBytes, s.SnapshotArchive.AccountPayloadBytes = 0, 0, 0
	p, _ := publicValue(t, &s, "fresh", now)
	if a := p.Startup.SnapshotArchive; a == nil || a.CompressedReportedTotal != nil || a.FullSnapshotSlot != 0 || a.CompressedInputBytes != 0 {
		t.Fatalf("zero/unknown archive facts lost: %+v", a)
	}
	total := uint64(1)
	s.SnapshotArchive.CompressedReportedTotal, s.SnapshotArchive.CompressedInputBytes = &total, 2
	p, _ = publicValue(t, &s, "fresh", now)
	if a := p.Startup.SnapshotArchive; a == nil || a.CompressedInputBytes != 2 || *a.CompressedReportedTotal != 1 {
		t.Fatal("underreported total changed raw bytes")
	}
	// A new feed heartbeat cannot renew the archive owner's older date.
	s.SampledAt = now.Add(6 * time.Second)
	p, _ = publicValue(t, &s, "fresh", s.SampledAt)
	if a := p.Startup.SnapshotArchive; a == nil || a.State != "stale" || !a.SampledAt.Equal(now) || a.Phase != "interrupted" {
		t.Fatalf("terminal archive date was renewed: %+v", a)
	}
	p, _ = publicValue(t, &s, "stale", now)
	if p.Startup.SnapshotArchive.State != "stale" {
		t.Fatal("stale outer source became fresh archive")
	}
	p, _ = publicValue(t, &s, "unavailable", now)
	if p.Startup != nil {
		t.Fatal("unavailable source retained startup")
	}
}

func TestSnapshotArchiveInvalidAndOldProducer(t *testing.T) {
	now := time.Now().UTC()
	for name, mutate := range map[string]func(*Snapshot){
		"attempt":     func(s *Snapshot) { s.SnapshotArchive.Attempt = 0 },
		"start_zero":  func(s *Snapshot) { s.SnapshotArchive.StartedAt = time.Time{} },
		"sample_zero": func(s *Snapshot) { s.SnapshotArchive.SampledAt = time.Time{} },
		"before_run":  func(s *Snapshot) { s.SnapshotArchive.StartedAt = s.StartedAt.Add(-time.Second) },
		"reversed":    func(s *Snapshot) { s.SnapshotArchive.StartedAt = now.Add(time.Nanosecond) },
		"future":      func(s *Snapshot) { s.SnapshotArchive.SampledAt = now.Add(2 * time.Second) },
		"after_feed":  func(s *Snapshot) { s.SampledAt = now.Add(-2 * time.Second) },
		"kind":        func(s *Snapshot) { s.SnapshotArchive.InputKind = "https://secret.invalid/archive" },
		"phase":       func(s *Snapshot) { s.SnapshotArchive.Phase = "ready" },
		"total_zero":  func(s *Snapshot) { zero := uint64(0); s.SnapshotArchive.CompressedReportedTotal = &zero },
	} {
		t.Run(name, func(t *testing.T) {
			s := testSnapshot(now)
			s.SnapshotArchive = maximumSnapshotArchive(now)
			mutate(&s)
			p, _ := publicValue(t, &s, "fresh", now)
			if p.Startup.SnapshotArchive != nil {
				t.Fatal("invalid archive became public")
			}
		})
	}
	s := testSnapshot(now)
	p, _ := publicValue(t, &s, "fresh", now)
	if p.Startup.SnapshotArchive != nil {
		t.Fatal("old producer acquired archive observation")
	}
	f, err := frames(&s, "fresh", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := decodedFrames(t, &frameSet{messages: f})["mithril.snapshot_archive"]; found {
		t.Fatal("old producer acquired archive frame")
	}
}
