package progress

import (
	"io"
	"testing"
)

func TestIncrementalProgressUsesFreshJoinedOwners(t *testing.T) {
	var absent *DualProgress
	if absent.NewIncrementalProgress(1, "local") != nil || NewDualProgress().NewIncrementalProgress(1, "local") != nil {
		t.Fatal("default path created an incremental owner")
	}
	full, fullEvents := observedProgress(t)
	var slots []uint64
	var kinds []string
	var events []<-chan DualProgressObservation
	if !full.SetIncrementalProgress(func(slot uint64, kind string) *DualProgress {
		full.IsInterrupted() // Constructors must run outside the parent mutex.
		d, output := observedProgress(t)
		d.output = io.Discard
		slots, kinds, events = append(slots, slot), append(kinds, kind), append(events, output)
		return d
	}) {
		t.Fatal("constructor rejected before Start")
	}
	if full.NewIncrementalProgress(1, "local") != nil {
		t.Fatal("incremental owner created before full completion")
	}
	full.Start()
	progressEvent(t, fullEvents)
	if full.SetIncrementalProgress(nil) || full.NewIncrementalProgress(1, "local") != nil {
		t.Fatal("active full owner reconfigured or overlapped")
	}
	full.Stop()
	if got := progressEvent(t, fullEvents); got.State != "full_archive_complete" {
		t.Fatal("full-archive terminal changed")
	}
	if full.NewIncrementalProgress(1, "https://private.invalid/path") != nil {
		t.Fatal("constructor accepted a raw source instead of a classification")
	}
	first := full.NewIncrementalProgress(20, "remote")
	if first == nil || first == full {
		t.Fatal("incremental owner reused terminal full state")
	}
	first.Start()
	if got := progressEvent(t, events[0]); got.State != "active" || got.CompressedInputBytes != 0 {
		t.Fatal("incremental counters did not start fresh")
	}
	first.Download.Add(8)
	first.Interrupt()
	if got := progressEvent(t, events[0]); got.State != "incremental_archive_interrupted" || got.CompressedInputBytes != 8 {
		t.Fatal("incremental failure was labelled full or complete")
	}
	second := full.NewIncrementalProgress(21, "local")
	if second == nil || second == first || len(slots) != 2 || slots[0] != 20 || slots[1] != 21 || kinds[0] != "remote" || kinds[1] != "local" {
		t.Fatal("retry did not create a separately bound owner")
	}
	second.Start()
	if got := progressEvent(t, events[1]); got.State != "active" || got.CompressedInputBytes != 0 {
		t.Fatal("retry inherited failed-attempt bytes")
	}
	second.Stop()
	if got := progressEvent(t, events[1]); got.State != "incremental_archive_complete" {
		t.Fatal("incremental terminal classification changed")
	}
}
