package node

import (
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/progress"
)

func TestDashboardSnapshotManifestAttempts(t *testing.T) {
	genesis := "test-genesis"
	d := &nodeDashboard{state: dashboard.Snapshot{RunID: "manifest-run", StartedAt: time.Now().Add(-time.Second).UTC(), GenesisHash: &genesis}}
	first := d.newSnapshotProgress(100, "local")
	if d.snapshot().SnapshotManifests != nil {
		t.Fatal("unparsed manifest reported")
	}
	first.ManifestParsed(false, 100)
	first.ManifestParsed(true, 110)
	got := d.snapshot().SnapshotManifests
	if got == nil || got.Attempt != 1 || got.Full == nil || got.Full.Slot != 100 || got.Incremental == nil || got.Incremental.Slot != 110 || got.Full.ParsedAt.IsZero() || got.Incremental.ParsedAt.IsZero() {
		t.Fatalf("manifest parse observations: %+v", got)
	}
	second := d.newSnapshotProgress(200, "remote")
	first.ManifestParsed(false, 101)
	if d.snapshot().SnapshotManifests != nil {
		t.Fatal("retired bootstrap republished a manifest")
	}
	second.ManifestParsed(false, 200)
	if got = d.snapshot().SnapshotManifests; got == nil || got.Attempt != 2 || got.Full.Slot != 200 || got.Incremental != nil {
		t.Fatalf("replacement attempt: %+v", got)
	}
	d.mu.Lock()
	d.state.Phase = "stopping"
	d.mu.Unlock()
	second.ManifestParsed(true, 210)
	if d.snapshot().SnapshotManifests.Incremental != nil {
		t.Fatal("stopping bootstrap accepted a late parse")
	}
}

func TestDashboardSnapshotReadyMarker(t *testing.T) {
	genesis := "test-genesis"
	d := &nodeDashboard{state: dashboard.Snapshot{RunID: "manifest-run", StartedAt: time.Now().Add(-time.Second).UTC(), GenesisHash: &genesis, Phase: "bootstrap"}}
	first := d.newSnapshotProgress(100, "local")
	d.snapshotReadyMarker(100)
	if d.snapshot().SnapshotManifests != nil {
		t.Fatal("ready marker without a parsed snapshot")
	}
	first.ManifestParsed(false, 100)
	first.ManifestParsed(true, 110)
	d.snapshotReadyMarker(100)
	if d.snapshot().SnapshotManifests.ReadyMarker != nil {
		t.Fatal("accepted the full slot instead of the selected incremental slot")
	}
	d.snapshotReadyMarker(110)
	old := d.snapshot().SnapshotManifests
	if old.ReadyMarker == nil || old.ReadyMarker.Slot != 110 || old.ReadyMarker.ObservedAt.Before(old.Incremental.ParsedAt) {
		t.Fatalf("ready marker: %+v", old)
	}
	d.snapshotReadyMarker(110)
	if !d.snapshot().SnapshotManifests.ReadyMarker.ObservedAt.Equal(old.ReadyMarker.ObservedAt) {
		t.Fatal("re-observation renewed the marker")
	}
	second := d.newSnapshotProgress(200, "local")
	second.ManifestParsed(false, 200)
	if d.snapshot().SnapshotManifests.ReadyMarker != nil || old.ReadyMarker.Slot != 110 {
		t.Fatal("replacement attempt retained the marker or changed an earlier snapshot")
	}
	d.mu.Lock()
	d.state.Phase = "stopping"
	d.mu.Unlock()
	d.snapshotReadyMarker(200)
	if d.snapshot().SnapshotManifests.ReadyMarker != nil {
		t.Fatal("stopping node published a ready marker")
	}
}

func TestDashboardSnapshotArchiveOwner(t *testing.T) {
	genesis := "test-genesis"
	d := &nodeDashboard{state: dashboard.Snapshot{RunID: "archive-run", StartedAt: time.Now().Add(-time.Second).UTC(), GenesisHash: &genesis}}
	first := d.newSnapshotProgress(0, "local")
	if d.snapshot().SnapshotArchive != nil {
		t.Fatal("unstarted owner fabricated archive progress")
	}
	first.Download.Add(10)
	first.Extract.Add(20)
	first.Start()
	t.Cleanup(first.Stop)
	a := d.snapshot().SnapshotArchive
	if a == nil || a.Attempt != 1 || a.Phase != "active" || a.FullSnapshotSlot != 0 || a.InputKind != "local" ||
		a.CompressedReportedTotal != nil || a.CompressedInputBytes != 10 || a.AccountPayloadBytes != 20 {
		t.Fatalf("initial owner observation: %+v", a)
	}
	second := d.newSnapshotProgress(math.MaxUint64, "remote")
	if d.snapshot().SnapshotArchive != nil {
		t.Fatal("new unstarted attempt retained old progress")
	}
	first.Stop()
	if d.snapshot().SnapshotArchive != nil {
		t.Fatal("retired owner republished terminal history")
	}
	second.SetDownloadTotal(1)
	second.Download.Add(2)
	second.Extract.Add(3)
	second.Start()
	t.Cleanup(second.Stop)
	second.Stop()
	a = d.snapshot().SnapshotArchive
	if a == nil || a.Attempt != 2 || a.Phase != "complete" || a.InputKind != "remote" || a.FullSnapshotSlot != math.MaxUint64 ||
		a.CompressedInputBytes != 2 || a.AccountPayloadBytes != 3 || a.CompressedReportedTotal == nil || *a.CompressedReportedTotal != 1 {
		t.Fatalf("new attempt observation: %+v", a)
	}
	date := a.SampledAt
	second.Download.Add(9)
	if later := d.snapshot().SnapshotArchive; later.SampledAt != date || later.CompressedInputBytes != 2 {
		t.Fatal("viewer renewed terminal values")
	}
	for name, mutate := range map[string]func(){
		"run":     func() { d.state.RunID = "new-run" },
		"start":   func() { d.state.StartedAt = d.state.StartedAt.Add(time.Second) },
		"genesis": func() { next := "next-genesis"; d.state.GenesisHash = &next },
	} {
		t.Run(name, func(t *testing.T) {
			d.mu.Lock()
			saved := d.state
			mutate()
			d.mu.Unlock()
			if d.snapshot().SnapshotArchive != nil {
				t.Fatal("archive was relabeled with a different node binding")
			}
			d.mu.Lock()
			d.state = saved
			d.mu.Unlock()
		})
	}
	d.snapshotAttempt = math.MaxUint64
	_ = d.newSnapshotProgress(0, "local")
	if d.snapshotAttempt != math.MaxUint64 || d.snapshot().SnapshotArchive != nil {
		t.Fatal("exhausted archive attempt wrapped or kept old values")
	}
}

func TestDashboardExplicitSnapshotInputKind(t *testing.T) {
	for _, tc := range []struct {
		name, path, want string
	}{
		{"local", "/var/lib/mithril/snapshot.tar.zst", "local"},
		{"relative", "snapshot.tar.zst", "local"},
		{"http", "http://example.test/snapshot.tar.zst", "remote"},
		{"https", "https://example.test/snapshot.tar.zst", "remote"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			genesis := "test-genesis"
			d := &nodeDashboard{state: dashboard.Snapshot{RunID: "archive-run", StartedAt: time.Now().Add(-time.Second).UTC(), GenesisHash: &genesis}}
			p := d.newExplicitSnapshotProgress(123, tc.path)
			p.Start()
			t.Cleanup(p.Stop)
			if got := d.snapshot().SnapshotArchive; got == nil || got.InputKind != tc.want {
				t.Fatalf("snapshot input kind: got %+v, want %s", got, tc.want)
			}
		})
	}
}

func TestDashboardSnapshotArchiveMeasurement(t *testing.T) {
	now := time.Now()
	o := progress.DualProgressObservation{StartedAt: now, CapturedAt: now, State: "full_archive_interrupted",
		CompressedInputBytes: math.MaxUint64, AccountPayloadBytes: math.MaxUint64, CompressedTotalKnown: true, CompressedReportedTotal: 1}
	a := snapshotArchiveMeasurement(1, 0, "local", now.Add(-time.Second), o)
	if a == nil || a.Phase != "interrupted" || a.CompressedInputBytes != math.MaxUint64 || a.AccountPayloadBytes != math.MaxUint64 ||
		a.CompressedReportedTotal == nil || *a.CompressedReportedTotal != 1 {
		t.Fatalf("raw interrupted archive facts lost: %+v", a)
	}
	o.CompressedTotalKnown, o.CompressedReportedTotal = false, 99
	if a := snapshotArchiveMeasurement(1, 0, "local", now, o); a == nil || a.CompressedReportedTotal != nil {
		t.Fatal("unknown total became known")
	}
	for name, mutate := range map[string]func(*progress.DualProgressObservation){
		"start":      func(o *progress.DualProgressObservation) { o.StartedAt = time.Time{} },
		"sample":     func(o *progress.DualProgressObservation) { o.CapturedAt = time.Time{} },
		"reversed":   func(o *progress.DualProgressObservation) { o.CapturedAt = o.StartedAt.Add(-time.Nanosecond) },
		"before_run": func(o *progress.DualProgressObservation) { o.StartedAt = now.Add(-time.Second) },
		"state":      func(o *progress.DualProgressObservation) { o.State = "ready" },
		"zero_total": func(o *progress.DualProgressObservation) { o.CompressedTotalKnown, o.CompressedReportedTotal = true, 0 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := o
			mutate(&bad)
			if snapshotArchiveMeasurement(1, 0, "local", now, bad) != nil {
				t.Fatal("accepted invalid archive observation")
			}
		})
	}
	if snapshotArchiveMeasurement(0, 0, "local", now, o) != nil || snapshotArchiveMeasurement(1, 0, "/private/archive", now, o) != nil {
		t.Fatal("accepted missing attempt or arbitrary input kind")
	}
	var disabled *nodeDashboard
	if disabled.newSnapshotProgress(0, "local") == nil {
		t.Fatal("disabled dashboard removed ordinary progress owner")
	}
}
