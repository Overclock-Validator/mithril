package turbine

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type gatedSpoolJournal struct {
	*os.File
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *gatedSpoolJournal) Write(p []byte) (int, error) {
	f.once.Do(func() { close(f.started); <-f.release })
	return f.File.Write(p)
}
func gateSpoolJournal(t *testing.T, s *ShredSpool) *gatedSpoolJournal {
	t.Helper()
	s.journal.close()
	file, err := os.OpenFile(filepath.Join(s.dir, spoolJournalName), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	gate := &gatedSpoolJournal{File: file, started: make(chan struct{}), release: make(chan struct{})}
	s.journal = newSpoolCompletionJournal(gate)
	return gate
}
func waitSpoolTest(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("spool operation did not complete")
	}
}

func TestShredSpoolCompletionDoesNotWaitForJournalAndCloseDrainsOverflow(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Seed real slot files before blocking only the completeness writer.
	for slot := uint64(1); slot <= spoolJournalQueueSize+8; slot++ {
		s.Append(slot, []byte("packet"))
	}
	gate := gateSpoolJournal(t, s)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(gate.release) }); s.Close() })
	s.MarkComplete(1, 0, 1)
	waitSpoolTest(t, gate.started)
	done := make(chan struct{})
	go func() {
		for slot := uint64(2); slot <= spoolJournalQueueSize+8; slot++ {
			s.MarkComplete(slot, 0, 1)
		}
		close(done)
	}()
	waitSpoolTest(t, done)
	if !s.journalOverflow {
		t.Fatal("test did not overflow the bounded queue")
	}
	if s.CompleteSlots() != spoolJournalQueueSize+8 {
		t.Fatal("overflow lost live completeness")
	}
	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned before the blocked writer drained")
	default:
	}
	release.Do(func() { close(gate.release) })
	waitSpoolTest(t, closed)
	s.Close() // idempotent; no closed-channel send
	s.MarkComplete(9999, 0, 1)
	if _, ok := s.IsComplete(9999); ok {
		t.Fatal("post-Close completion was accepted")
	}
	reopened, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.CompleteSlots() != spoolJournalQueueSize+8 {
		t.Fatal("clean handoff lost overflowed completion hints")
	}
}

func TestShredSpoolInvalidationWaitsBeforeReplacement(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.Append(800, []byte("old-file"))
	if _, err := s.ReadSlot(800); err != nil {
		t.Fatal(err)
	}
	gate := gateSpoolJournal(t, s)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(gate.release) }); s.Close() })
	s.MarkComplete(800, 0, 1)
	waitSpoolTest(t, gate.started)
	done := make(chan struct{})
	go func() { s.DiscardSlot(800); s.Append(800, []byte("replacement-partial")); close(done) }()
	// While the earlier completion write is stalled, replacement must wait.
	select {
	case <-done:
		t.Fatal("replacement passed an undrained invalidation")
	case <-time.After(20 * time.Millisecond):
	}
	data, err := os.ReadFile(s.pathFor(800))
	if err != nil || !bytes.Contains(data, []byte("old-file")) {
		t.Fatalf("old file mutated before invalidation: %v", err)
	}
	release.Do(func() { close(gate.release) })
	waitSpoolTest(t, done)
	s.Close()
	reopened, err := OpenShredSpool(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, ok := reopened.IsComplete(800); ok {
		t.Fatal("older completion resurrected for replacement")
	}
	packets, err := reopened.ReadSlot(800)
	if err != nil || len(packets) != 1 || string(packets[0]) != "replacement-partial" {
		t.Fatalf("replacement: %q %v", packets, err)
	}
}

type faultySpoolJournal struct {
	*os.File
	failTruncate bool // only changed while worker is fenced by invalidate's reply
}

func (f *faultySpoolJournal) Write(p []byte) (int, error) { return f.File.Write(p[:len(p)/2]) }
func (f *faultySpoolJournal) Truncate(n int64) error {
	if f.failTruncate {
		return errors.New("injected truncate failure")
	}
	return f.File.Truncate(n)
}

func TestShredSpoolJournalShortWriteDisablesHintsAndFencesMutations(t *testing.T) {
	for _, failTruncate := range []bool{false, true} {
		dir := t.TempDir()
		s, err := OpenShredSpool(dir, 0)
		if err != nil {
			t.Fatal(err)
		}
		s.Append(100, []byte("old-file"))
		s.MarkComplete(100, 0, 1)
		s.Close()
		s, err = OpenShredSpool(dir, 0)
		if err != nil {
			t.Fatal(err)
		}
		s.journal.close()
		file, err := os.OpenFile(filepath.Join(dir, spoolJournalName), os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		faulty := &faultySpoolJournal{File: file, failTruncate: failTruncate}
		s.journal = newSpoolCompletionJournal(faulty)
		s.mu.Lock()
		dropped := s.dropSlotLocked(100)
		s.mu.Unlock()
		if dropped == failTruncate {
			t.Fatalf("drop=%v with truncate failure=%v", dropped, failTruncate)
		}
		if failTruncate {
			data, err := os.ReadFile(s.pathFor(100))
			if err != nil || !bytes.Contains(data, []byte("old-file")) {
				t.Fatal("failed journal fence mutated slot file")
			}
			faulty.failTruncate = false
			s.DiscardSlot(100) // retry can now invalidate all old hints
		}
		s.Append(100, []byte("partial"))
		s.MarkComplete(101, 0, 1)
		s.Close()
		data, err := os.ReadFile(filepath.Join(dir, spoolJournalName))
		if err != nil || len(data) != 0 {
			t.Fatalf("failed journal must stay empty, got %d bytes: %v", len(data), err)
		}
		reopened, err := OpenShredSpool(dir, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := reopened.IsComplete(100); ok {
			t.Fatal("failed journal resurrected completion")
		}
		reopened.Close()
	}
}
