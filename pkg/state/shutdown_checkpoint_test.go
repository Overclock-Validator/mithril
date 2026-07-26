package state

import (
	"testing"
)

// The reference must track LastSlot unconditionally. LastSlot advances even
// when a shutdown carries no checkpoint, and a leftover reference from an
// earlier run would then name a slot the state file no longer claims — which
// is exactly what makes startup cleanup retain the wrong sidecar and delete
// the one resume needs.
func TestUpdateOnShutdownTracksCheckpointReferenceWithLastSlot(t *testing.T) {
	dir := t.TempDir()
	s := &MithrilState{StateSchemaVersion: CurrentStateSchemaVersion, SnapshotSlot: 50}

	first := &TransactionStatusCheckpointRef{
		Version: 1, Root: 100, File: "checkpoint-100-aa.bin", Size: 10, SHA256: "aa",
	}
	if err := s.UpdateOnShutdown(dir, 100, bh(0x01), &ShutdownContext{
		RunID: "run-1", TransactionStatusCheckpoint: first,
	}); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
	if s.LastTransactionStatusCheckpoint == nil || s.LastTransactionStatusCheckpoint.Root != 100 {
		t.Fatalf("checkpoint reference not recorded: %+v", s.LastTransactionStatusCheckpoint)
	}
	// The stored reference must be a copy, not the caller's pointer.
	if s.LastTransactionStatusCheckpoint == first {
		t.Fatal("state aliased the caller's checkpoint reference")
	}

	// A later shutdown that produced no checkpoint must clear the old one
	// rather than leave it describing slot 100 while LastSlot says 200.
	if err := s.UpdateOnShutdown(dir, 200, bh(0x02), &ShutdownContext{RunID: "run-2"}); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	if s.LastSlot != 200 {
		t.Fatalf("LastSlot = %d, want 200", s.LastSlot)
	}
	if s.LastTransactionStatusCheckpoint != nil {
		t.Fatalf("stale checkpoint reference survived a checkpointless shutdown: %+v",
			s.LastTransactionStatusCheckpoint)
	}

	// A shutdown with no context at all advances LastSlot too, so it must also
	// leave no reference behind.
	s.LastTransactionStatusCheckpoint = first
	if err := s.UpdateOnShutdown(dir, 300, bh(0x03), nil); err != nil {
		t.Fatalf("third shutdown: %v", err)
	}
	if s.LastSlot != 300 {
		t.Fatalf("LastSlot = %d, want 300", s.LastSlot)
	}
	if s.LastTransactionStatusCheckpoint != nil {
		t.Fatalf("stale checkpoint reference survived a contextless shutdown: %+v",
			s.LastTransactionStatusCheckpoint)
	}

	// The cleared state must be what a restart actually reads back.
	loaded, err := LoadState(dir)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if loaded.LastTransactionStatusCheckpoint != nil {
		t.Fatalf("persisted state still names a checkpoint: %+v",
			loaded.LastTransactionStatusCheckpoint)
	}
	if loaded.LastSlot != 300 {
		t.Fatalf("persisted LastSlot = %d, want 300", loaded.LastSlot)
	}
}

// A contextless shutdown advances LastSlot but leaves the resume context at the
// previous run's slot. HasResumeData must refuse that pairing, or replay would
// restart from LastSlot+1 on accounts captured many slots earlier.
func TestContextlessShutdownDoesNotOfferStaleResumeContext(t *testing.T) {
	dir := t.TempDir()
	s := &MithrilState{StateSchemaVersion: CurrentStateSchemaVersion, SnapshotSlot: 50}

	// Run A: clean shutdown at slot 100 with a full context.
	if err := s.UpdateOnShutdown(dir, 100, bh(0x01), &ShutdownContext{
		RunID:       "run-A",
		AcctsLtHash: "bHRoYXNoLWFzLW9mLXNsb3QtMTAw",
		BlockHeight: 100,
	}); err != nil {
		t.Fatalf("run A shutdown: %v", err)
	}
	ltHashAt100 := s.LastAcctsLtHash
	if ltHashAt100 == "" {
		t.Fatal("run A did not record an lt hash")
	}

	// Run B: replays to 180, then ProcessBlock fails. lastSlotCtx is nil, so the
	// caller passes no context and only the slot advances.
	if err := s.UpdateOnShutdown(dir, 180, bh(0x02), nil); err != nil {
		t.Fatalf("run B shutdown: %v", err)
	}

	loaded, err := LoadState(dir)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if loaded.LastSlot != 180 {
		t.Fatalf("LastSlot = %d, want 180", loaded.LastSlot)
	}

	// The failure: the file claims slot 180 while its resume context is slot
	// 100's, and it still advertises itself as resumable.
	if loaded.LastAcctsLtHash == ltHashAt100 && loaded.HasResumeData() {
		t.Fatalf("state offers resume at slot %d using the context captured at slot 100; "+
			"replay would restart on accounts %d slots stale",
			loaded.LastSlot, loaded.LastSlot-100)
	}
}
