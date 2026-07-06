package alpenglow

import "testing"

// CertifiedBlockAt / SkipCertifiedAt are the execute-on-receipt switch
// sweep's decision oracles: the sweep unwinds an executed block exactly when
// CertifiedBlockAt names a different sibling, and marks a slot skipped exactly
// when SkipCertifiedAt says so. These tests pin the scenario matrix the sweep
// depends on: certified sibling, fallback-only ambiguity, explicit skip,
// indirect (derived) skip, finalized-by-ancestry, and the two-decisive-blocks
// conflict handoff.

// A notarize cert makes its block THE decisive block for the slot — the
// certified-sibling-switch signal.
func TestCertifiedBlockAtDecisiveNotarize(t *testing.T) {
	tracker := NewChainTracker()
	sibling := BlockID{Slot: 40, Hash: chainTestHash(2)}
	specObserve(t, tracker, Certificate{Type: CertificateNotarize, Slot: 40, BlockHash: sibling.Hash})

	got, certType, ok := tracker.CertifiedBlockAt(40)
	if !ok || got != sibling || certType != CertificateNotarize {
		t.Fatalf("notarize cert must be decisive: got %+v %s ok=%v", got, certType, ok)
	}
}

// Fallback certs are ambiguous (up to 7 can legally coexist) — never a switch
// signal. Codex item: "fallback-only = ambiguous, do not choose".
func TestCertifiedBlockAtIgnoresFallbackOnly(t *testing.T) {
	tracker := NewChainTracker()
	specObserve(t, tracker, Certificate{Type: CertificateNotarizeFallback, Slot: 41, BlockHash: chainTestHash(1)})
	specObserve(t, tracker, Certificate{Type: CertificateNotarizeFallback, Slot: 41, BlockHash: chainTestHash(2)})

	if _, _, ok := tracker.CertifiedBlockAt(41); ok {
		t.Fatalf("fallback-only slot must have NO decisive block")
	}
	if tracker.SkipCertifiedAt(41) {
		t.Fatalf("fallback certs are not skip evidence")
	}
}

// An explicit skip cert answers SkipCertifiedAt; the slot has no decisive block.
func TestSkipCertifiedAtExplicit(t *testing.T) {
	tracker := NewChainTracker()
	specObserve(t, tracker, Certificate{Type: CertificateSkip, Slot: 42})

	if !tracker.SkipCertifiedAt(42) {
		t.Fatalf("explicit skip cert must report skip-certified")
	}
	if _, _, ok := tracker.CertifiedBlockAt(42); ok {
		t.Fatalf("a skip-certified slot has no decisive block")
	}
}

// Indirect skips: a finalized block whose parent link jumps slots derives the
// omitted slots as skipped — with no skip certificate anywhere. This is how
// the sweep learns to mark leader-skipped slots during catchup and after
// fork switches.
func TestSkipCertifiedAtIndirect(t *testing.T) {
	tracker := NewChainTracker()
	finalized := BlockID{Slot: 15, Hash: chainTestHash(15)}
	specObserve(t, tracker, Certificate{Type: CertificateFinalizeFast, Slot: 15, BlockHash: finalized.Hash})
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: finalized, ParentSlot: 12, ParentHash: chainTestHash(12)})

	for _, slot := range []uint64{13, 14} {
		if !tracker.SkipCertifiedAt(slot) {
			t.Fatalf("slot %d omitted between finalized ancestors must be indirectly skip-certified", slot)
		}
	}
	if tracker.SkipCertifiedAt(12) {
		t.Fatalf("the finalized parent slot itself is not skipped")
	}
	// And the finalized block is decisive at its own slot.
	if got, _, ok := tracker.CertifiedBlockAt(15); !ok || got != finalized {
		t.Fatalf("finalized block must be decisive at its slot: %+v ok=%v", got, ok)
	}
}

// Finalization by ancestry upgrades a FALLBACK-cert'd parent to decisive: a
// notar-fallback cert alone is ambiguous (never a switch signal), but once a
// finalized descendant chains to it the ambiguity is resolved and the sweep
// may act on it.
func TestCertifiedBlockAtFinalizedByAncestry(t *testing.T) {
	tracker := NewChainTracker()
	parent := BlockID{Slot: 12, Hash: chainTestHash(12)}
	child := BlockID{Slot: 15, Hash: chainTestHash(15)}

	// Replay executed both blocks (execute-on-receipt), and the parent picked
	// up only a notar-fallback cert — ambiguous by itself.
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: parent, ParentSlot: 11, ParentHash: chainTestHash(11)})
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: child, ParentSlot: parent.Slot, ParentHash: parent.Hash})
	specObserve(t, tracker, Certificate{Type: CertificateNotarizeFallback, Slot: 12, BlockHash: parent.Hash})
	if _, _, ok := tracker.CertifiedBlockAt(12); ok {
		t.Fatalf("a fallback-only parent must not be decisive before ancestry finalization")
	}

	specObserve(t, tracker, Certificate{Type: CertificateFinalizeFast, Slot: 15, BlockHash: child.Hash})

	got, _, ok := tracker.CertifiedBlockAt(12)
	if !ok || got != parent {
		t.Fatalf("ancestry-finalized fallback parent must be decisive at slot 12: %+v ok=%v", got, ok)
	}
}

// Two decisive blocks in one slot is Byzantine evidence: CertifiedBlockAt
// reports NO decisive block (the conflict machinery owns the halt) rather
// than arbitrarily picking one. Codex item: "multiple decisive blocks =
// conflict/evidence, fail closed".
func TestCertifiedBlockAtTwoDecisiveIsNoDecision(t *testing.T) {
	tracker := NewChainTracker()
	a := BlockID{Slot: 44, Hash: chainTestHash(1)}
	b := BlockID{Slot: 44, Hash: chainTestHash(2)}
	// Byzantine: two "unique-strength" certs for one slot (impossible under
	// honest-majority thresholds, exactly what evidence must catch).
	specObserve(t, tracker, Certificate{Type: CertificateNotarize, Slot: 44, BlockHash: a.Hash})
	specObserve(t, tracker, Certificate{Type: CertificateFinalizeFast, Slot: 44, BlockHash: b.Hash})

	if _, _, ok := tracker.CertifiedBlockAt(44); ok {
		t.Fatalf("two decisive blocks must yield NO switch decision — the conflict path owns it")
	}
	if decision, ok := tracker.NextDecision(43); !ok || decision.Kind != ChainDecisionKindConflict {
		t.Fatalf("conflict machinery must report the Byzantine slot, got %+v ok=%v", decision, ok)
	}
}

// The cert-less variant: an ancestry-finalized parent that never received ANY
// certificate is still decisive — surfaced via the per-slot finalized index,
// since the cert-only blockSlots scan cannot see it. This is the adversarial
// corner where the fix matters: replay executed an equivocation twin at the
// parent slot, no cert for the true parent ever arrived, and the finalized
// descendant is the only evidence. Without the index the sweep would take the
// expensive rooted re-replay; with it, the cheap in-RAM switch fires.
func TestCertifiedBlockAtCertlessAncestryFinalized(t *testing.T) {
	tracker := NewChainTracker()
	parent := BlockID{Slot: 12, Hash: chainTestHash(12)}
	child := BlockID{Slot: 15, Hash: chainTestHash(15)}

	// The true parent was replay-observed (or hash-linked) but NEVER certified.
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: parent, ParentSlot: 11, ParentHash: chainTestHash(11)})
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: child, ParentSlot: parent.Slot, ParentHash: parent.Hash})
	if _, _, ok := tracker.CertifiedBlockAt(12); ok {
		t.Fatalf("nothing decisive before finality")
	}

	specObserve(t, tracker, Certificate{Type: CertificateFinalizeFast, Slot: 15, BlockHash: child.Hash})

	got, _, ok := tracker.CertifiedBlockAt(12)
	if !ok || got != parent {
		t.Fatalf("cert-less ancestry-finalized parent must be decisive: %+v ok=%v", got, ok)
	}

	// Two-decisive still fails closed: a decisive cert for a DIFFERENT sibling
	// at the finalized slot is Byzantine — no switch decision, conflict owns it.
	specObserve(t, tracker, Certificate{Type: CertificateNotarize, Slot: 12, BlockHash: chainTestHash(99)})
	if _, _, ok := tracker.CertifiedBlockAt(12); ok {
		t.Fatalf("finalized block + different decisive cert must yield NO switch decision")
	}
}

// A cert-less finalized block replay has NOT observed is a repair target: the
// wanted-blocks feed must name it (Finalized=true) even though its slot has no
// certified blocks at all — otherwise cert-driven repair could never fetch the
// one block the chain provably needs.
func TestWantedBlocksIncludesCertlessFinalized(t *testing.T) {
	tracker := NewChainTracker()
	parent := BlockID{Slot: 12, Hash: chainTestHash(12)}
	child := BlockID{Slot: 15, Hash: chainTestHash(15)}

	// Only the CHILD was observed by replay; the parent is known solely via
	// the child's parent link + ancestry finalization.
	tracker.ObserveReplayBlock(ReplayBlockObservation{Block: child, ParentSlot: parent.Slot, ParentHash: parent.Hash})
	specObserve(t, tracker, Certificate{Type: CertificateFinalizeFast, Slot: 15, BlockHash: child.Hash})

	wanted := tracker.WantedBlocks(0, 16)
	for _, w := range wanted {
		if w.Block == parent {
			if !w.Finalized {
				t.Fatalf("cert-less finalized target must carry Finalized=true: %+v", w)
			}
			return
		}
	}
	t.Fatalf("unobserved cert-less finalized parent must be a wanted block, got %+v", wanted)
}
