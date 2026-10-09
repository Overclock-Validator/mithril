package turbine

import (
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func sweepForTest(a *SlotAssembler) {
	a.mu.Lock()
	a.pruneOldSlotsLocked()
	a.mu.Unlock()
}

func TestRetentionSweepFloorMovesWithoutNewShreds(t *testing.T) {
	a := NewSlotAssembler()
	a.maxObservedSlot = 10000
	a.SetRetentionFloor(1000)
	a.slots[1000] = &slotState{slot: 1000}
	a.slots[1001] = &slotState{slot: 1001}
	a.completedSlots[1001] = struct{}{}
	a.SetKnownAlpenglowBlockID(1001, solana.Hash{1})
	a.PrioritizeRepairSlot(1000)
	sweepForTest(a)
	require.Contains(t, a.slots, uint64(1000))
	a.SetRetentionFloor(1001)
	sweepForTest(a)
	require.NotContains(t, a.slots, uint64(1000))
	require.NotContains(t, a.priorityRepairSlots, uint64(1000))
	require.Contains(t, a.slots, uint64(1001))
	a.SetRetentionFloor(0)
	sweepForTest(a)
	require.Empty(t, a.slots)
	require.Empty(t, a.completedSlots)
	require.Empty(t, a.knownBlockIDs)

	// Lowering the floor must permit new old repair state again.
	a.SetRetentionFloor(1000)
	a.slotState(1000, 1)
	sweepForTest(a)
	require.Contains(t, a.slots, uint64(1000))
}

func TestRetentionSweepReleasesCompletingProtectionAtFixedEdge(t *testing.T) {
	for _, outcome := range []string{"abort", "cancel", "error", "complete", "reset"} {
		t.Run(outcome, func(t *testing.T) {
			a := NewSlotAssembler()
			a.maxObservedSlot = 10000
			s := &slotState{slot: 1000, parentSlot: 999, completing: true, shreds: map[uint32]*Shred{0: {}}}
			a.slots[s.slot] = s
			a.SetKnownAlpenglowBlockID(999, solana.Hash{1})
			a.SetKnownAlpenglowBlockID(1000, solana.Hash{2})
			a.RejectAlpenglowBlockID(1000, solana.Hash{3})
			sweepForTest(a)
			require.Contains(t, a.slots, uint64(1000))
			require.Contains(t, a.knownBlockIDs, uint64(999))
			require.Contains(t, a.rejectedBlockIDs, uint64(1000))
			work := &slotCompletionWork{state: s}
			switch outcome {
			case "abort":
				a.abortCompletion(work)
			case "cancel":
				_, err := a.finalizeCompletion(work, processedSlotCompletion{canceled: true})
				require.NoError(t, err)
			case "error":
				_, err := a.finalizeCompletion(work, processedSlotCompletion{err: errors.New("decode failure")})
				require.Error(t, err)
			case "complete":
				_, err := a.finalizeCompletion(work, processedSlotCompletion{block: &block.Block{Slot: 1000}})
				require.NoError(t, err)
			case "reset":
				a.ResetSlot(1000)
			}
			sweepForTest(a)
			require.Empty(t, a.slots)
			require.Empty(t, a.completedSlots)
			require.Empty(t, a.knownBlockIDs)
			require.Empty(t, a.rejectedBlockIDs)
			require.Empty(t, a.partialShredObs)
		})
	}
}

func TestRetentionSweepOldHintsAddedAtFixedEdge(t *testing.T) {
	a := NewSlotAssembler()
	a.maxObservedSlot = 10000
	sweepForTest(a)
	a.SetKnownAlpenglowBlockID(1000, solana.Hash{1})
	a.RejectAlpenglowBlockID(1001, solana.Hash{2})
	sweepForTest(a)
	require.Empty(t, a.knownBlockIDs)
	require.Empty(t, a.rejectedBlockIDs)
	a.mu.Lock()
	a.trackBlockIDLocked(&block.Block{Slot: 1002, HasAlpenglowBlockID: true, AlpenglowBlockID: solana.Hash{3}})
	a.mu.Unlock()
	sweepForTest(a)
	require.Empty(t, a.knownBlockIDs)
}

func TestRetentionSweepCapacityAtFixedEdge(t *testing.T) {
	a := NewSlotAssembler()
	a.maxObservedSlot = 10000
	a.SetRetentionFloor(1000)
	a.PrioritizeRepairSlot(1001)
	sweepForTest(a)
	for i := 0; i < maxRetainedIncompleteSlotCap+2; i++ {
		a.slotState(1000+uint64(i), 1)
	}
	sweepForTest(a)
	require.Len(t, a.slots, maxRetainedIncompleteSlotCap)
	require.Contains(t, a.slots, uint64(1000))
	require.Contains(t, a.slots, uint64(1001))
	require.NotContains(t, a.slots, uint64(1000+maxRetainedIncompleteSlotCap+1))
}

func BenchmarkRetentionRepeatedCompletedShred(b *testing.B) {
	a := NewSlotAssembler()
	a.maxObservedSlot = 10000
	for slot := uint64(9488); slot <= 10000; slot++ {
		a.completedSlots[slot] = struct{}{}
		a.knownBlockIDs[slot] = solana.Hash{1}
		a.rejectedBlockIDs[slot] = map[solana.Hash]struct{}{{2}: {}}
		a.partialShredObs[slot] = PartialShredObservation{DataShreds: 1}
	}
	sh := &Shred{Slot: 10000, Type: ShredTypeData}
	// The public ingestion path still acquires the lock and performs its
	// ordinary completed-slot rejection on every packet.
	_, _ = a.AddShred(sh)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = a.AddShred(sh)
	}
}
