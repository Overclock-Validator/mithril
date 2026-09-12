package replay

import (
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

type stubFinalityEngine struct {
	consensusengine.Engine
	finalized      map[uint64]alpenglow.BlockID
	finalizedSkips map[uint64]alpenglow.BlockID
	conflicts      map[uint64]bool
}

func (s *stubFinalityEngine) FinalizedBlockAt(slot uint64) (alpenglow.BlockID, bool) {
	id, ok := s.finalized[slot]
	return id, ok
}

func (s *stubFinalityEngine) FinalityConflictAt(slot uint64) bool {
	return s.conflicts[slot]
}

func (s *stubFinalityEngine) FinalizedSkipAt(slot uint64) (alpenglow.BlockID, bool) {
	via, ok := s.finalizedSkips[slot]
	return via, ok
}

func TestPromotionGatePassesMatchingSlots(t *testing.T) {
	blockA := solana.Hash{0xA}
	through, err := alpenglowPromotionGate(&stubFinalityEngine{},
		map[uint64]solana.Hash{5: blockA},
		map[uint64]solana.Hash{5: blockA},
		nil,
		4, 6, 6, nil)
	if err != nil || through != 6 {
		t.Fatalf("matching slot must pass: through=%d err=%v", through, err)
	}
}

func TestPromotionGateStopsOnMismatch(t *testing.T) {
	through, err := alpenglowPromotionGate(&stubFinalityEngine{},
		map[uint64]solana.Hash{5: {0xA}},
		map[uint64]solana.Hash{5: {0xB}},
		nil,
		4, 8, 8, nil)
	if through != 4 {
		t.Fatalf("mismatch at 5 must cap promotion at 4, got %d", through)
	}
	var mismatch *AlpenglowFinalityMismatch
	if !errors.As(err, &mismatch) || mismatch.Slot != 5 {
		t.Fatalf("want AlpenglowFinalityMismatch at slot 5, got %v", err)
	}
}

// Prefix-stop: slots after a mismatch must not promote even if they match —
// they build on the unpromoted block's state.
func TestPromotionGatePrefixStop(t *testing.T) {
	blockA, blockB := solana.Hash{0xA}, solana.Hash{0xB}
	through, err := alpenglowPromotionGate(&stubFinalityEngine{},
		map[uint64]solana.Hash{5: blockA, 6: blockB},
		map[uint64]solana.Hash{5: {0xEE}, 6: blockB},
		nil,
		4, 6, 6, nil)
	if through != 4 || err == nil {
		t.Fatalf("mismatch at 5 must stop 6 too (prefix-stop), got through=%d err=%v", through, err)
	}
}

// Slots with no executed alpenglow block-id (RPC fallback) or no known finality
// pass under the documented delegated-trust regime.
func TestPromotionGatePassesUnknownSides(t *testing.T) {
	through, err := alpenglowPromotionGate(&stubFinalityEngine{},
		map[uint64]solana.Hash{5: {0xA}}, // finality known, nothing executed (RPC block)
		map[uint64]solana.Hash{6: {0xB}}, // executed known, no finality data
		nil,
		4, 7, 7, nil)
	if err != nil || through != 7 {
		t.Fatalf("unknown sides must pass: through=%d err=%v", through, err)
	}
}

// The engine's finality index backs up the replay-local capture (near-tip QUIC path).
func TestPromotionGateFallsBackToEngineIndex(t *testing.T) {
	engine := &stubFinalityEngine{finalized: map[uint64]alpenglow.BlockID{
		5: {Slot: 5, Hash: solana.Hash{0xA}},
	}}
	through, err := alpenglowPromotionGate(engine,
		nil,
		map[uint64]solana.Hash{5: {0xB}},
		nil,
		4, 6, 6, nil)
	if through != 4 || err == nil {
		t.Fatalf("engine-index mismatch must stop: through=%d err=%v", through, err)
	}
}

// Catchup: the watermark can sit millions of slots past local execution — the walk
// must stay bounded by the executed tip while the promotion bound passes unclamped.
func TestPromotionGateWalkBoundedByExecutedTip(t *testing.T) {
	engine := &stubFinalityEngine{conflicts: map[uint64]bool{5_000_000: true}}
	through, err := alpenglowPromotionGate(engine,
		nil, map[uint64]solana.Hash{100: {0xA}},
		nil,
		99, 9_000_000, 100, nil) // executed tip 100; conflict far beyond never walked
	if err != nil || through != 9_000_000 {
		t.Fatalf("walk past executed tip must be skipped, bound unclamped: through=%d err=%v", through, err)
	}
}

// A recorded safety conflict at a slot fails closed — never promote through it.
func TestPromotionGateStopsOnConflict(t *testing.T) {
	engine := &stubFinalityEngine{conflicts: map[uint64]bool{5: true}}
	through, err := alpenglowPromotionGate(engine, nil, nil, nil, 4, 8, 8, nil)
	if through != 4 || err == nil {
		t.Fatalf("conflicted slot must fail closed: through=%d err=%v", through, err)
	}
}

func TestPromotionGateStopsOnFinalizedSkipMismatch(t *testing.T) {
	via := alpenglow.BlockID{Slot: 8, Hash: solana.Hash{0x8}}
	engine := &stubFinalityEngine{finalizedSkips: map[uint64]alpenglow.BlockID{
		5: via,
		6: via,
	}}
	through, err := alpenglowPromotionGate(engine, nil,
		map[uint64]solana.Hash{5: {0xA}, 6: {}}, nil, 4, 8, 8, nil)
	if through != 4 {
		t.Fatalf("executed block at finalized-skipped slot 5 must cap promotion at 4, got %d", through)
	}
	var mismatch *AlpenglowFinalityMismatch
	if !errors.As(err, &mismatch) || mismatch.Slot != 5 || !mismatch.Skip || mismatch.Conflict {
		t.Fatalf("want finalized-skip mismatch at slot 5, got %#v (%v)", mismatch, err)
	}

	through, err = alpenglowPromotionGate(engine, nil,
		map[uint64]solana.Hash{5: {}, 6: {}}, nil, 4, 8, 8, nil)
	if err != nil || through != 8 {
		t.Fatalf("local skips must match finalized skipped ancestry: through=%d err=%v", through, err)
	}
}

func TestFinalizedSkipEvidenceIsDistinctFromConflict(t *testing.T) {
	mithrilState := &state.MithrilState{}
	recordAlpenglowEvidence(mithrilState, &AlpenglowFinalityMismatch{
		Slot: 5, Executed: solana.Hash{0xA}, Skip: true,
	})
	if len(mithrilState.AlpenglowEvidence) != 1 {
		t.Fatalf("evidence count = %d, want 1", len(mithrilState.AlpenglowEvidence))
	}
	ev := mithrilState.AlpenglowEvidence[0]
	if !ev.Skip || ev.Conflict {
		t.Fatalf("finalized skip evidence lost outcome distinction: %+v", ev)
	}
	if ev.Finalized != "0000000000000000000000000000000000000000000000000000000000000000" {
		t.Fatalf("finalized skip encoding = %q", ev.Finalized)
	}
}

// Fresh bootstrap: lastRooted starts at 0 while the executed tip is in the millions —
// the walk must be floored at the tail depth, not span the whole chain.
func TestPromotionGateWalkFlooredAtTailDepth(t *testing.T) {
	stats := &alpenglowGateStats{}
	through, err := alpenglowPromotionGate(&stubFinalityEngine{}, nil, nil, nil,
		0, 5_000_000, 5_000_000, stats)
	if err != nil || through != 5_000_000 {
		t.Fatalf("fresh-bootstrap walk must pass: through=%d err=%v", through, err)
	}
	if stats.checked > unrootedTailHaltCap {
		t.Fatalf("walk checked %d slots, must be floored at tail depth %d", stats.checked, unrootedTailHaltCap)
	}
}

// Persisted evidence: a previously-disputed slot promotes only on an exact executed
// match — the delegated-trust pass (missing sides) no longer applies to it.
func TestPromotionGateForcedEvidence(t *testing.T) {
	forced := map[uint64]alpenglowFinalityExpectation{5: {Block: solana.Hash{0xA}}}

	// No executed identity (RPC block): would pass under delegated trust, must now stop.
	through, err := alpenglowPromotionGate(&stubFinalityEngine{}, nil, nil, forced, 4, 8, 8, nil)
	if through != 4 || err == nil {
		t.Fatalf("disputed slot without exact match must stop: through=%d err=%v", through, err)
	}
	// Wrong executed identity: stop.
	through, err = alpenglowPromotionGate(&stubFinalityEngine{}, nil,
		map[uint64]solana.Hash{5: {0xB}}, forced, 4, 8, 8, nil)
	if through != 4 || err == nil {
		t.Fatalf("disputed slot with wrong identity must stop: through=%d err=%v", through, err)
	}
	// Exact match (repair fetched the certified block): promotes.
	through, err = alpenglowPromotionGate(&stubFinalityEngine{}, nil,
		map[uint64]solana.Hash{5: {0xA}}, forced, 4, 8, 8, nil)
	if err != nil || through != 8 {
		t.Fatalf("disputed slot with exact match must promote: through=%d err=%v", through, err)
	}
	// Conflict evidence (zero hash): never promotes.
	through, err = alpenglowPromotionGate(&stubFinalityEngine{}, nil,
		map[uint64]solana.Hash{5: {0xA}}, map[uint64]alpenglowFinalityExpectation{5: {Conflict: true}}, 4, 8, 8, nil)
	if through != 4 || err == nil {
		t.Fatalf("conflict evidence must block promotion: through=%d err=%v", through, err)
	}
	var mismatch *AlpenglowFinalityMismatch
	if !errors.As(err, &mismatch) || !mismatch.Conflict {
		t.Fatalf("conflict evidence must surface as Conflict, got %v", err)
	}
	// A persisted finalized skip is a valid zero local outcome, not a
	// conflict sentinel.
	through, err = alpenglowPromotionGate(&stubFinalityEngine{}, nil,
		map[uint64]solana.Hash{5: {}}, map[uint64]alpenglowFinalityExpectation{5: {Skip: true}}, 4, 8, 8, nil)
	if err != nil || through != 8 {
		t.Fatalf("persisted finalized skip must accept the local skip: through=%d err=%v", through, err)
	}
}
