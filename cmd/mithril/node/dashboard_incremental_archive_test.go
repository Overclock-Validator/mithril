package node

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func TestDashboardIncrementalArchiveAttemptsAndLineage(t *testing.T) {
	genesis := "archive-test-genesis"
	d := &nodeDashboard{state: dashboard.Snapshot{RunID: "archive-run", StartedAt: time.Now().Add(-time.Second).UTC(), GenesisHash: &genesis}}
	full := d.newSnapshotProgress(100, "local")
	full.Start()
	t.Cleanup(full.Stop)
	full.Stop()
	if s := d.snapshot(); s.SnapshotArchive == nil || s.SnapshotArchive.Phase != "complete" || s.IncrementalArchive != nil {
		t.Fatal("full archive history lost before incremental")
	}
	first := full.NewIncrementalProgress(120, "remote")
	if first == nil {
		t.Fatal("completed full owner could not construct incremental")
	}
	if s := d.snapshot(); s.SnapshotArchive != nil || s.IncrementalArchive != nil {
		t.Fatal("unstarted incremental retained full history or fabricated progress")
	}
	first.SetDownloadTotal(1)
	first.Download.Add(2)
	first.Extract.Add(3)
	first.Start()
	t.Cleanup(first.Stop)
	a := d.snapshot().IncrementalArchive
	if a == nil || a.Attempt != 2 || a.FullSnapshotSlot != 100 || a.IncrementalSlot != 120 || a.InputKind != "remote" ||
		a.CompressedInputBytes != 2 || a.AccountPayloadBytes != 3 || a.CompressedReportedTotal == nil || *a.CompressedReportedTotal != 1 {
		t.Fatalf("incremental facts not independently bound: %+v", a)
	}
	first.Interrupt()
	a = d.snapshot().IncrementalArchive
	if a.Phase != "interrupted" || d.snapshot().SnapshotArchive != nil {
		t.Fatal("failed incremental relabelled as full or complete")
	}
	terminalAt := a.SampledAt
	first.Download.Add(99)
	if next := d.snapshot().IncrementalArchive; !next.SampledAt.Equal(terminalAt) || next.CompressedInputBytes != 2 {
		t.Fatal("outer sampling redated terminal incremental")
	}
	second := full.NewIncrementalProgress(121, "local")
	if second == nil || second == first {
		t.Fatal("retry did not receive a fresh archive owner")
	}
	second.Start()
	t.Cleanup(second.Stop)
	second.Stop()
	a = d.snapshot().IncrementalArchive
	if a == nil || a.Attempt != 3 || a.IncrementalSlot != 121 || a.Phase != "complete" ||
		a.CompressedInputBytes != 0 || a.AccountPayloadBytes != 0 || a.CompressedReportedTotal != nil {
		t.Fatalf("retry inherited old data: %+v", a)
	}
	replacement := d.newSnapshotProgress(200, "local")
	replacement.Start()
	t.Cleanup(replacement.Stop)
	if full.NewIncrementalProgress(122, "remote") != nil {
		t.Fatal("retired bootstrap constructed an incremental in its replacement")
	}
	if s := d.snapshot(); s.SnapshotArchive == nil || s.SnapshotArchive.FullSnapshotSlot != 200 || s.IncrementalArchive != nil {
		t.Fatal("retired constructor changed the replacement observation")
	}
	replacement.Stop()
	if replacement.NewIncrementalProgress(200, "local") != nil {
		t.Fatal("incremental end did not advance beyond its base")
	}
	d.mu.Lock()
	d.state.RunID = "replacement-run"
	d.mu.Unlock()
	if replacement.NewIncrementalProgress(201, "local") != nil {
		t.Fatal("constructor crossed a node-run binding")
	}
}
