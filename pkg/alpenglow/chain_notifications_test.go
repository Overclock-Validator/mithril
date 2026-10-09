package alpenglow

import (
	"testing"
	"time"
)

func TestChainTrackerDecisionChangesBetweenVersionCheckAndWait(t *testing.T) {
	tracker := NewChainTracker()
	changes := tracker.DecisionChanges()
	before := tracker.DecisionVersion()
	cert := Certificate{Type: CertificateSkip, Slot: 42}
	if _, err := tracker.ObserveCertificate(cert); err != nil {
		t.Fatalf("observe untrusted certificate: %v", err)
	}
	if after := tracker.DecisionVersion(); after != before {
		t.Fatalf("untrusted certificate changed version: %d -> %d", before, after)
	}
	assertNoChainDecisionChange(t, changes)

	// The decision arrives after replay's version check but before it waits.
	// The notification must remain available without an active receiver.
	specObserve(t, tracker, cert)
	assertChainDecisionChange(t, changes)
	if after := tracker.DecisionVersion(); after <= before || !tracker.SkipCertifiedAt(cert.Slot) {
		t.Fatalf("notification did not expose accepted skip: version %d -> %d", before, after)
	}

	before = tracker.DecisionVersion()
	specObserve(t, tracker, cert)
	if after := tracker.DecisionVersion(); after != before {
		t.Fatalf("duplicate certificate changed version: %d -> %d", before, after)
	}
	assertNoChainDecisionChange(t, changes)
}

func TestChainTrackerDecisionChangesOnFinalizationAndLateAncestry(t *testing.T) {
	tracker := NewChainTracker()
	changes := tracker.DecisionChanges()
	child := BlockID{Slot: 15, Hash: chainTestHash(15)}
	parent := BlockID{Slot: 12, Hash: chainTestHash(12)}
	tracker.ObserveReplayBlock(ReplayBlockObservation{
		Block: child, ParentSlot: parent.Slot, ParentHash: parent.Hash,
	})
	specObserve(t, tracker, Certificate{Type: CertificateFinalizeFast, Slot: child.Slot, BlockHash: child.Hash})
	assertChainDecisionChange(t, changes)
	before := tracker.DecisionVersion()

	specFinalize(t, tracker, child, CertificateFinalizeFast)
	assertChainDecisionChange(t, changes)
	if after := tracker.DecisionVersion(); after <= before {
		t.Fatalf("finalization notification did not advance version: %d -> %d", before, after)
	}
	if via, ok := tracker.FinalizedSkipAt(13); !ok || via != child {
		t.Fatalf("finalization did not publish omitted slot: via=%+v ok=%v", via, ok)
	}
	specFinalize(t, tracker, child, CertificateFinalizeFast)
	assertNoChainDecisionChange(t, changes)

	// The parent's header extends finalized ancestry and derives more skips
	// without any additional certificate or finalization event.
	before = tracker.DecisionVersion()
	certificatesBefore := tracker.Snapshot().CertificatesAccepted
	tracker.ObserveReplayBlock(ReplayBlockObservation{
		Block: parent, ParentSlot: 9, ParentHash: chainTestHash(9),
	})
	assertChainDecisionChange(t, changes)
	if after := tracker.DecisionVersion(); after <= before {
		t.Fatalf("ancestry notification did not advance version: %d -> %d", before, after)
	}
	if after := tracker.Snapshot().CertificatesAccepted; after != certificatesBefore {
		t.Fatalf("ancestry changed certificate count: %d -> %d", certificatesBefore, after)
	}
	if via, ok := tracker.FinalizedSkipAt(10); !ok || via != parent {
		t.Fatalf("late ancestry did not publish omitted slot: via=%+v ok=%v", via, ok)
	}
}

func TestChainTrackerDecisionChangesCoalesceWithoutBlocking(t *testing.T) {
	tracker := NewChainTracker()
	changes := tracker.DecisionChanges()
	if changes == nil || changes != tracker.DecisionChanges() || cap(changes) != 1 {
		t.Fatal("decision changes must return a stable capacity-one channel")
	}
	const count = 100
	done := make(chan error, 1)
	go func() {
		for slot := uint64(1); slot <= count; slot++ {
			if _, err := tracker.ObserveCertificate(Certificate{
				Type: CertificateSkip, Slot: slot, SignatureVerified: true,
			}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("observe certificates: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unread decision notifications blocked certificate processing")
	}
	assertChainDecisionChange(t, changes)
	if version := tracker.DecisionVersion(); version != count {
		t.Fatalf("coalesced notification lost decisions: version=%d want=%d", version, count)
	}
	assertNoChainDecisionChange(t, changes)

	// Once consumed, the same channel must be ready for the next change.
	specObserve(t, tracker, Certificate{Type: CertificateSkip, Slot: count + 1})
	assertChainDecisionChange(t, changes)
}

func assertChainDecisionChange(t *testing.T, changes <-chan struct{}) {
	t.Helper()
	select {
	case _, ok := <-changes:
		if !ok {
			t.Fatal("decision changes channel closed")
		}
	default:
		t.Fatal("decision change did not leave a notification")
	}
}

func assertNoChainDecisionChange(t *testing.T, changes <-chan struct{}) {
	t.Helper()
	select {
	case <-changes:
		t.Fatal("unexpected decision change notification")
	default:
	}
}
