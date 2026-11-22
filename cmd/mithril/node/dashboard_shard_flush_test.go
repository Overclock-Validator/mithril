package node

import (
	"errors"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func TestDashboardShardFlushLatestArchiveAndReplacement(t *testing.T) {
	genesis := "shard-test-genesis"
	d := &nodeDashboard{state: dashboard.Snapshot{RunID: "shard-run", StartedAt: time.Now().Add(-time.Second).UTC(), GenesisHash: &genesis}}
	full := d.newSnapshotProgress(100, "local")
	full.Start()
	t.Cleanup(full.Stop)
	full.Stop()
	incremental := full.NewIncrementalProgress(120, "remote")
	incremental.Start()
	t.Cleanup(incremental.Stop)
	early := full.NewIndexingProgress()
	early.Start(2)
	if d.snapshot().SnapshotShardFlush != nil {
		t.Fatal("full completion hid an unfinished incremental archive")
	}
	incremental.Stop()
	p := full.NewIndexingProgress()
	p.Start(4)
	p.Update(3, 4)
	p.Update(1, 4)
	s := d.snapshot()
	a := s.SnapshotShardFlush
	if a == nil || a.Attempt != 3 || a.FullSnapshotSlot != 100 || a.IncrementalSlot == nil || *a.IncrementalSlot != 120 ||
		a.CompletedShards != 3 || a.TotalShards != 4 || a.Phase != "active" || s.SnapshotArchive != nil || s.IncrementalArchive != nil {
		t.Fatal("shard stage lost its latest archive lineage or observed maximum")
	}
	p.RecordResult(errors.New("fixture failure"))
	a = d.snapshot().SnapshotShardFlush
	if a.Phase != "error" || a.CompletedShards != 3 {
		t.Fatal("aggregate error result became readiness or fabricated completion")
	}
	terminal := a.SampledAt
	p.RecordResult(nil)
	if d.snapshot().SnapshotShardFlush.SampledAt != terminal {
		t.Fatal("terminal stage was redated")
	}
	p.RecordSSTIngestReturn()
	a = d.snapshot().SnapshotShardFlush
	if a.SSTIngestReturnedAt == nil || a.SSTIngestReturnedAt.Before(terminal) || a.SampledAt != terminal || a.Phase != "error" || a.CompletedShards != 3 {
		t.Fatal("ingestion event changed the independently recorded flush outcome")
	}
	ingested := *a.SSTIngestReturnedAt
	p.RecordSSTIngestReturn()
	if *d.snapshot().SnapshotShardFlush.SSTIngestReturnedAt != ingested {
		t.Fatal("duplicate ingestion callback redated the event")
	}
	replacement := d.newSnapshotProgress(200, "local")
	replacement.Start()
	t.Cleanup(replacement.Stop)
	p.Update(4, 4)
	p.RecordResult(nil)
	p.RecordSSTIngestReturn()
	stale := full.NewIndexingProgress()
	stale.Start(1)
	stale.Update(1, 1)
	if s := d.snapshot(); s.SnapshotShardFlush != nil || s.SnapshotArchive == nil || s.SnapshotArchive.FullSnapshotSlot != 200 {
		t.Fatal("retired root changed replacement bootstrap facts")
	}
	replacement.Stop()
	last := replacement.NewIndexingProgress()
	last.Start(1)
	d.stopping()
	last.Update(1, 1)
	last.RecordResult(nil)
	last.RecordSSTIngestReturn()
	if d.snapshot().SnapshotShardFlush != nil {
		t.Fatal("late callback survived node stopping")
	}
}

func TestDashboardShardFlushNodeBindingFencesCallbacks(t *testing.T) {
	for name, change := range map[string]func(*nodeDashboard){
		"run":     func(d *nodeDashboard) { d.state.RunID = "another-run" },
		"start":   func(d *nodeDashboard) { d.state.StartedAt = d.state.StartedAt.Add(time.Second) },
		"genesis": func(d *nodeDashboard) { next := "another-genesis"; d.state.GenesisHash = &next },
		"attempt": func(d *nodeDashboard) { d.snapshotAttempt++ },
	} {
		t.Run(name, func(t *testing.T) {
			genesis := "shard-test-genesis"
			d := &nodeDashboard{state: dashboard.Snapshot{RunID: "shard-run", StartedAt: time.Now().Add(-time.Second).UTC(), GenesisHash: &genesis}}
			full := d.newSnapshotProgress(100, "local")
			full.Start()
			t.Cleanup(full.Stop)
			full.Stop()
			p := full.NewIndexingProgress()
			p.Start(1)
			before := d.snapshotShardFlush.value
			d.mu.Lock()
			change(d)
			d.mu.Unlock()
			p.Update(1, 1)
			p.RecordResult(nil)
			p.RecordSSTIngestReturn()
			if d.snapshotShardFlush.value != before || d.snapshot().SnapshotShardFlush != nil {
				t.Fatal("late callback crossed a changed node/attempt binding")
			}
		})
	}
}
