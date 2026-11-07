package progress

import (
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestIndexingProgressObservationOrderingAndResult(t *testing.T) {
	p := NewIndexingProgress("shards")
	p.output = io.Discard
	var observations []IndexingProgressObservation
	if !p.SetObserver(func(o IndexingProgressObservation) { observations = append(observations, o) }) {
		t.Fatal("unstarted owner refused an observer")
	}
	p.RecordResult(nil)
	p.RecordSSTIngestReturn()
	p.Start(4)
	p.RecordSSTIngestReturn()
	if p.SetObserver(nil) || len(observations) != 1 || observations[0].State != "active" || observations[0].Completed != 0 || observations[0].Total != 4 {
		t.Fatal("initial observation or startup-only contract changed")
	}
	p.Update(3, 4)
	p.Update(1, 4)
	p.Update(3, 4)
	if len(observations) != 2 || observations[1].Completed != 3 {
		t.Fatal("reordered or duplicate callbacks regressed or redated progress")
	}
	p.Update(2, 5)
	p.RecordResult(errors.New("fixture failure"))
	terminal := observations[len(observations)-1]
	if terminal.State != "error" || terminal.Completed != 3 || terminal.Total != 4 || terminal.StartedAt != observations[0].StartedAt {
		t.Fatal("actual error result lost pinned total or observed high-water")
	}
	p.RecordResult(nil)
	p.Update(4, 4)
	p.Start(10)
	if len(observations) != 3 || !reflect.DeepEqual(observations[2], terminal) {
		t.Fatal("terminal observation was replaced or redated")
	}
	p.RecordSSTIngestReturn()
	ingested := observations[len(observations)-1]
	if len(observations) != 4 || ingested.SSTIngestReturnedAt.IsZero() || ingested.SSTIngestReturnedAt.Before(terminal.CapturedAt) ||
		ingested.CapturedAt != terminal.CapturedAt || ingested.State != "error" || ingested.Completed != terminal.Completed || ingested.Total != terminal.Total {
		t.Fatal("later ingestion return changed the original flush error/result clock")
	}
	p.RecordSSTIngestReturn()
	if len(observations) != 4 {
		t.Fatal("duplicate ingestion return redated a historical event")
	}
	for _, counts := range [][2]int{{-1, 4}, {5, 4}, {0, 0}, {0, 1001}} {
		p := NewIndexingProgress("shards")
		p.started = true
		p.observer = func(IndexingProgressObservation) { t.Fatal("invalid source counts were observed") }
		if callback, _ := p.observationLocked(counts[0], counts[1], "active"); callback != nil {
			t.Fatal("out-of-contract shard counts accepted")
		}
	}
}

func TestIndexingProgressReturnedDoesNotInventCompletedCount(t *testing.T) {
	p := NewIndexingProgress("shards")
	p.output = io.Discard
	var last IndexingProgressObservation
	p.SetObserver(func(o IndexingProgressObservation) { last = o })
	p.Start(4)
	p.Update(2, 4)
	p.RecordResult(nil)
	if last.State != "returned" || last.Completed != 2 || last.Total != 4 {
		t.Fatal("nil return fabricated missing shard callbacks")
	}
	ordinary := NewIndexingProgress("ordinary")
	ordinary.output = io.Discard
	ordinary.Start(1)
	ordinary.Update(1, 1)
	ordinary.RecordResult(nil)
	ordinary.RecordSSTIngestReturn()
	if ordinary.observation != nil {
		t.Fatal("ordinary terminal display enabled observation")
	}
}

func TestIndexingProgressFreshConstructorAfterArchive(t *testing.T) {
	var absent *DualProgress
	if absent.NewIndexingProgress() == nil {
		t.Fatal("nil archive lost the ordinary display")
	}
	d := NewDualProgress()
	d.output = io.Discard
	created := 0
	d.SetIndexingProgress(func() *IndexingProgress { created++; return NewIndexingProgress("shards") })
	_ = d.NewIndexingProgress()
	if created != 0 {
		t.Fatal("unstarted archive attached an indexing owner")
	}
	d.Start()
	if d.SetIndexingProgress(nil) {
		t.Fatal("started root changed its constructor")
	}
	d.Stop()
	first, second := d.NewIndexingProgress(), d.NewIndexingProgress()
	if first == nil || second == nil || first == second || created != 2 {
		t.Fatal("successful joined archive did not construct fresh stage owners")
	}
	first.output = io.Discard
	first.Start(1)
	d.mu.Lock()
	d.indexingProgress = func() *IndexingProgress { return first }
	d.mu.Unlock()
	if d.NewIndexingProgress() == first {
		t.Fatal("constructor reused an already-started stage owner")
	}
	failed := NewDualProgress()
	failed.output = io.Discard
	failed.SetIndexingProgress(func() *IndexingProgress { t.Fatal("interrupted root attached an owner"); return nil })
	failed.Start()
	failed.Interrupt()
	_ = failed.NewIndexingProgress()
}
