package blockstream

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestAlpenglowRewindDiagnosticThresholdAndThrottle(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	var d alpenglowRewindDiagnostic
	_, due := d.takeNotice(start)
	require.False(t, due)
	id := solana.Hash{1}
	d.begin(100, id, start)
	for _, elapsed := range []time.Duration{-time.Second, 0, alpenglowRewindWaitWarnAfter - time.Nanosecond} {
		_, due = d.takeNotice(start.Add(elapsed))
		require.False(t, due, "elapsed=%s", elapsed)
	}
	notice, due := d.takeNotice(start.Add(alpenglowRewindWaitWarnAfter))
	require.True(t, due)
	require.Equal(t, uint64(100), notice.slot)
	require.Equal(t, id, notice.certified)
	require.Equal(t, alpenglowRewindWaitWarnAfter, notice.elapsed)
	for _, elapsed := range []time.Duration{alpenglowRewindWaitWarnAfter, 10 * time.Second, 35*time.Second - time.Nanosecond} {
		_, due = d.takeNotice(start.Add(elapsed))
		require.False(t, due, "elapsed=%s", elapsed)
	}
	notice, due = d.takeNotice(start.Add(35 * time.Second))
	require.True(t, due)
	require.Equal(t, 35*time.Second, notice.elapsed)
	_, due = d.takeNotice(start.Add(65 * time.Second))
	require.True(t, due)
}

func TestAlpenglowRewindDiagnosticRequiresExactNewEmission(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	id := solana.Hash{1}
	var d alpenglowRewindDiagnostic
	preRewindGeneration := d.generation()
	d.begin(100, id, start)
	matching := &b.Block{Slot: 100, HasAlpenglowBlockID: true, AlpenglowBlockID: id}
	d.served(preRewindGeneration, matching)
	require.Equal(t, uint64(100), d.slot, "a pre-rewind send cannot satisfy the request")
	for _, blk := range []*b.Block{
		nil,
		{Slot: 101, HasAlpenglowBlockID: true, AlpenglowBlockID: id},
		{Slot: 99, HasAlpenglowBlockID: true, AlpenglowBlockID: id},
		{Slot: 100, IsSkipped: true},
		{Slot: 100},
		{Slot: 100, HasAlpenglowBlockID: true, AlpenglowBlockID: solana.Hash{2}},
	} {
		d.served(d.generation(), blk)
		require.Equal(t, uint64(100), d.slot)
	}
	_, due := d.takeNotice(start.Add(5 * time.Second))
	require.True(t, due)
	d.served(d.generation(), matching)
	_, due = d.takeNotice(start.Add(time.Hour))
	require.False(t, due)
}

func TestAlpenglowRewindDiagnosticReplacementAndClear(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	id := solana.Hash{1}
	var d alpenglowRewindDiagnostic
	d.begin(100, id, start)
	oldGeneration := d.generation()
	_, due := d.takeNotice(start.Add(5 * time.Second))
	require.True(t, due)
	// Repeating even the same requested identity is a fresh rewind. Its wait
	// and warning interval restart, and old send acknowledgements stay stale.
	d.begin(100, id, start.Add(6*time.Second))
	d.served(oldGeneration, &b.Block{Slot: 100, HasAlpenglowBlockID: true, AlpenglowBlockID: id})
	_, due = d.takeNotice(start.Add(10 * time.Second))
	require.False(t, due)
	notice, due := d.takeNotice(start.Add(11 * time.Second))
	require.True(t, due)
	require.Equal(t, 5*time.Second, notice.elapsed)
	d.begin(96, solana.Hash{2}, start.Add(12*time.Second))
	notice, due = d.takeNotice(start.Add(17 * time.Second))
	require.True(t, due)
	require.Equal(t, uint64(96), notice.slot)
	require.Equal(t, solana.Hash{2}, notice.certified)
	d.clear() // Superseding speculative rewind, quarantine, or shutdown.
	_, due = d.takeNotice(start.Add(time.Hour))
	require.False(t, due)
}

func TestAlpenglowRewindDiagnosticWithoutIdentityAcceptsSlotOutcome(t *testing.T) {
	for _, skipped := range []bool{false, true} {
		var d alpenglowRewindDiagnostic
		d.begin(100, solana.Hash{}, time.Unix(1_700_000_000, 0))
		d.served(d.generation(), &b.Block{Slot: 100, IsSkipped: skipped})
		require.Zero(t, d.slot)
	}
}

func TestAlpenglowRewindDiagnosticSupersededDecision(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	id := solana.Hash{1}
	for _, replacement := range []solana.Hash{{2}, {}} {
		var d alpenglowRewindDiagnostic
		d.begin(100, id, start)
		d.superseded(101, replacement)
		require.Equal(t, uint64(100), d.slot, "another slot's decision cannot supersede the wait")
		d.superseded(100, id)
		require.Equal(t, uint64(100), d.slot, "the same decision still awaits delivery")
		d.superseded(100, replacement)
		_, due := d.takeNotice(start.Add(time.Hour))
		require.False(t, due)
	}
	var unpinned alpenglowRewindDiagnostic
	unpinned.begin(100, solana.Hash{}, start)
	unpinned.superseded(100, id)
	require.Equal(t, uint64(100), unpinned.slot)
	unpinned.superseded(100, solana.Hash{})
	require.Equal(t, uint64(100), unpinned.slot)
}

func TestAlpenglowRewindDiagnosticShutdownClearsPendingWait(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	bs := &BlockSource{stopChan: make(chan struct{})}
	bs.alpenglowRewindWait.begin(100, solana.Hash{1}, start)
	bs.Stop()
	require.Zero(t, bs.alpenglowRewindWait.slot)
	bs.maybeLogAlpenglowRewindWait(start.Add(time.Hour))
	require.Zero(t, bs.alpenglowRewindWait.slot)
}

func TestAlpenglowRewindDiagnosticRechecksSupersededEarlierSlot(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	id := solana.Hash{1}
	for _, tc := range []struct {
		name     string
		decision alpenglow.ChainDecision
		clear    bool
	}{
		{"changed block", alpenglow.ChainDecision{Slot: 100, Kind: alpenglow.ChainDecisionKindBlock, Block: alpenglow.BlockID{Slot: 100, Hash: solana.Hash{2}}}, true},
		{"certified skip", alpenglow.ChainDecision{Slot: 100, Kind: alpenglow.ChainDecisionKindSkip}, true},
		{"same block", alpenglow.ChainDecision{Slot: 100, Kind: alpenglow.ChainDecisionKindBlock, Block: alpenglow.BlockID{Slot: 100, Hash: id}}, false},
		{"other slot", alpenglow.ChainDecision{Slot: 101, Kind: alpenglow.ChainDecisionKindSkip}, false},
		{"conflict", alpenglow.ChainDecision{Slot: 100, Kind: alpenglow.ChainDecisionKindConflict}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			bs := &BlockSource{nextSlotToSend: 104}
			bs.alpenglowDecisionSource = func(after uint64) (alpenglow.ChainDecision, bool) {
				require.Equal(t, uint64(99), after, "recheck the pending slot, not the advanced source frontier")
				calls++
				return tc.decision, true
			}
			bs.alpenglowRewindWait.begin(100, id, start)
			bs.maybeLogAlpenglowRewindWait(start.Add(4 * time.Second))
			require.Zero(t, calls)
			bs.maybeLogAlpenglowRewindWait(start.Add(5 * time.Second))
			require.Equal(t, 1, calls)
			require.Equal(t, tc.clear, bs.alpenglowRewindWait.slot == 0)
			bs.maybeLogAlpenglowRewindWait(start.Add(6 * time.Second))
			require.Equal(t, 1, calls, "revalidation follows warning throttling")
		})
	}
}
