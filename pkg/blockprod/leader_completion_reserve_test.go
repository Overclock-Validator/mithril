package blockprod

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfiguredCompletionReserveKeepsProtocolDeadlines(t *testing.T) {
	ready := time.Unix(1_700_000_000, 0)
	now := ready
	loop := NewLeaderLoop(LeaderLoopConfig{
		SlotDuration: AlpenglowSlotDuration, CompletionReserve: 60 * time.Millisecond,
		Now: func() time.Time { return now },
	})
	loop.productionWindow = leaderProductionWindow{
		active: true, startSlot: 212, endSlot: 215, nextSlot: 212, readyAt: ready,
	}
	for offset := uint64(0); offset < 4; offset++ {
		slot := 212 + offset
		deadline := ready.Add(time.Duration(offset+1) * 200 * time.Millisecond)
		require.Equal(t, deadline, loop.productionWindowProtocolDeadlineLocked(slot))
		require.Equal(t, deadline.Add(-60*time.Millisecond), loop.productionWindowDeadlineLocked(slot))
		// The override admits starts during the additional 15ms, but still
		// rejects at its own cutoff instead of extending the protocol deadline.
		now = deadline.Add(-70 * time.Millisecond)
		require.NoError(t, loop.productionStartCutoffErrorLocked(slot))
		now = deadline.Add(-60 * time.Millisecond)
		require.ErrorIs(t, loop.productionStartCutoffErrorLocked(slot), errProductionStartCutoffElapsed)
	}
}
