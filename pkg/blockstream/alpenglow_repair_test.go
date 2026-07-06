package blockstream

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

func newRepairTestSource(t *testing.T, wanted func(uint64, int) []alpenglow.WantedBlock, skip func(uint64) bool) *BlockSource {
	t.Helper()
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType:                   BlockSourceTurbine,
		TurbineBindAddr:              "127.0.0.1:0",
		TurbineAlpenglowBlockIDHints: true,
		StartSlot:                    151,
		EndSlot:                      200,
		AlpenglowWantedBlocks:        wanted,
		AlpenglowSkipCertified:       skip,
	})
	bs.isNearTip.Store(true)
	return bs
}

func wantedOne(slot uint64, hash solana.Hash) func(uint64, int) []alpenglow.WantedBlock {
	return func(afterSlot uint64, max int) []alpenglow.WantedBlock {
		if slot <= afterSlot || max < 1 {
			return nil
		}
		return []alpenglow.WantedBlock{{Block: alpenglow.BlockID{Slot: slot, Hash: hash}, Strongest: alpenglow.CertificateNotarize}}
	}
}

// A certified-but-unobserved block pins the assembler to the certified id;
// re-nudges are rate limited to one per slot per second.
func TestRepairLoopNudgesAndRateLimits(t *testing.T) {
	certified := solana.Hash{0xC1}
	bs := newRepairTestSource(t, wantedOne(152, certified), nil)

	nudged := make(map[uint64]time.Time)
	t0 := time.Now()
	bs.serviceAlpenglowWantedBlocks(nudged, t0)
	if got := bs.knownAlpenglowBlockIDs[152]; got != certified {
		t.Fatalf("known block id = %s, want %s", got, certified)
	}

	// Within the pause the slot is not re-nudged (hint removed to observe it)...
	delete(bs.knownAlpenglowBlockIDs, 152)
	bs.serviceAlpenglowWantedBlocks(nudged, t0.Add(200*time.Millisecond))
	if _, ok := bs.knownAlpenglowBlockIDs[152]; ok {
		t.Fatalf("expected no re-nudge within the rate-limit pause")
	}
	// ...and after the pause it is.
	bs.serviceAlpenglowWantedBlocks(nudged, t0.Add(1100*time.Millisecond))
	if got := bs.knownAlpenglowBlockIDs[152]; got != certified {
		t.Fatalf("expected re-nudge after the pause, known = %s", got)
	}
}

// A buffered pre-emission candidate with a different block id is provably
// non-canonical: discarded, slot state cleared for re-fetch, assembler pinned
// to the certified id — and no RPC retry is enqueued.
func TestRepairLoopDiscardsMismatchedBufferedCandidate(t *testing.T) {
	certified := solana.Hash{0xC1}
	other := solana.Hash{0xC2}
	bs := newRepairTestSource(t, wantedOne(152, certified), nil)
	bs.reorderBuffer[152] = &b.Block{
		Slot:                152,
		HasAlpenglowBlockID: true,
		AlpenglowBlockID:    [32]byte(other),
	}
	bs.slotState[152] = slotDone

	bs.serviceAlpenglowWantedBlocks(make(map[uint64]time.Time), time.Now())

	if bs.reorderBuffer[152] != nil {
		t.Fatalf("expected mismatched candidate to be discarded")
	}
	if _, exists := bs.slotState[152]; exists {
		t.Fatalf("expected slot state cleared so the slot re-fetches")
	}
	if got := bs.knownAlpenglowBlockIDs[152]; got != certified {
		t.Fatalf("known block id = %s, want %s", got, certified)
	}
	if len(bs.retrySlots) != 0 {
		t.Fatalf("cert-driven repair must not enqueue RPC retries, got %+v", bs.retrySlots)
	}
}

// A buffered candidate that already carries the certified id needs no repair:
// no nudge, no hint, no discard.
func TestRepairLoopLeavesMatchingBufferedCandidate(t *testing.T) {
	certified := solana.Hash{0xC1}
	bs := newRepairTestSource(t, wantedOne(152, certified), nil)
	bs.reorderBuffer[152] = &b.Block{
		Slot:                152,
		HasAlpenglowBlockID: true,
		AlpenglowBlockID:    [32]byte(certified),
	}

	nudged := make(map[uint64]time.Time)
	bs.serviceAlpenglowWantedBlocks(nudged, time.Now())

	if bs.reorderBuffer[152] == nil {
		t.Fatalf("matching candidate must stay buffered")
	}
	if len(nudged) != 0 {
		t.Fatalf("matching candidate must not consume a nudge, got %+v", nudged)
	}
	if _, ok := bs.knownAlpenglowBlockIDs[152]; ok {
		t.Fatalf("matching candidate needs no hint")
	}
}

// Skip-cancel marks certificate-skipped slots (bounded window, rate limited)
// and survives having no active receiver; the limiter is GC'd as the emission
// frontier advances past entries.
func TestRepairLoopSkipCancelAndLimiterGC(t *testing.T) {
	skipCertified := func(slot uint64) bool { return slot == 153 }
	bs := newRepairTestSource(t, func(uint64, int) []alpenglow.WantedBlock { return nil }, skipCertified)

	nudged := map[uint64]time.Time{149: time.Now().Add(-time.Hour)} // below the frontier: must be GC'd
	bs.serviceAlpenglowWantedBlocks(nudged, time.Now())

	if _, ok := nudged[149]; ok {
		t.Fatalf("limiter entries below the frontier must be pruned")
	}
	if _, ok := nudged[153]; !ok {
		t.Fatalf("skip-certified slot must be skip-cancelled (limiter records the reset)")
	}
}

// Outside near-tip the loop is inert: catch-up repairs arrive via backfill.
func TestRepairLoopGatedOnNearTip(t *testing.T) {
	bs := newRepairTestSource(t, wantedOne(152, solana.Hash{0xC1}), nil)
	bs.isNearTip.Store(false)

	nudged := make(map[uint64]time.Time)
	bs.serviceAlpenglowWantedBlocks(nudged, time.Now())

	if len(nudged) != 0 || len(bs.knownAlpenglowBlockIDs) != 0 {
		t.Fatalf("repair loop must be inert outside near-tip")
	}
}
