package replay

import (
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestRecentExecutedTailIsBoundedAndResets(t *testing.T) {
	disabled := &Progress{}
	disabled.begin(1, nil)
	disabled.completed(CompletedBlock{At: time.Now(), Slot: 1}, 0, false)
	disabledSnapshot, _ := disabled.Snapshot()
	require.Empty(t, disabledSnapshot.RecentExecuted())
	legacy := &Progress{}
	require.NoError(t, legacy.EnableReplayDiagnostics("legacy"))
	legacy.begin(1, nil)
	legacy.completed(CompletedBlock{At: time.Now(), Slot: 1}, 0, false)
	legacySnapshot, _ := legacy.Snapshot()
	require.Empty(t, legacySnapshot.RecentExecuted(), "legacy replay diagnostics must not enable the fast tail")
	require.Error(t, legacy.EnableFastExecuted(), "fast capture cannot be enabled after replay starts")
	p := &Progress{}
	require.Error(t, p.EnableFastExecuted())
	require.NoError(t, p.EnableReplayDiagnostics("test-run"))
	require.NoError(t, p.EnableFastExecuted())
	p.begin(1, nil)
	started, _ := p.Snapshot()
	for i := uint64(1); i <= 50; i++ {
		p.completed(CompletedBlock{At: started.StartedAt.Add(time.Duration(i) * time.Millisecond),
			Slot: i, Bankhash: solana.Hash{1}, BlockID: solana.Hash{byte(i)}, HasBlockID: true, Transactions: i,
			hasExecutionParent: true, executionParentSlot: i - 1, executionParentHash: solana.Hash{1}}, 0, false)
	}
	s, ok := p.Snapshot()
	require.True(t, ok)
	tail := s.RecentExecuted()
	require.Len(t, tail, 48)
	require.Equal(t, uint64(3), tail[0].Slot)
	require.Equal(t, uint64(50), tail[47].Slot)
	require.True(t, tail[0].HasBlockID)
	require.Equal(t, solana.Hash{3}, tail[0].BlockID)
	require.Equal(t, solana.Hash{50}, tail[47].BlockID)
	tail[0].Slot = 999
	tail[0].BlockID[0] = 99
	require.Equal(t, uint64(3), s.RecentExecuted()[0].Slot)
	require.Equal(t, solana.Hash{3}, s.RecentExecuted()[0].BlockID)
	p.rewound()
	s, _ = p.Snapshot()
	require.Empty(t, s.RecentExecuted())
	clearAt, clearSequence := s.ExecutionAt, s.ExecutionSequence
	p.skipped(29, 0, false)
	s, _ = p.Snapshot()
	require.Equal(t, clearAt, s.ExecutionAt)
	require.Equal(t, clearSequence, s.ExecutionSequence)
	p.completed(CompletedBlock{At: time.Now(), Slot: 30, Bankhash: solana.Hash{2}, HasBlockID: true}, 0, false)
	s, _ = p.Snapshot()
	require.Len(t, s.RecentExecuted(), 1)
	require.False(t, s.RecentExecuted()[0].HasBlockID, "a flagged zero ID must stay unknown without losing the valid tail")
	p.completed(CompletedBlock{At: time.Now(), Slot: 20, Bankhash: solana.Hash{3}}, 0, false)
	s, _ = p.Snapshot()
	require.Len(t, s.RecentExecuted(), 1)
	require.Equal(t, uint64(20), s.RecentExecuted()[0].Slot)
	p.completed(CompletedBlock{At: time.Now(), Slot: 21, Bankhash: solana.Hash{4},
		hasExecutionParent: true, executionParentSlot: 20, executionParentHash: solana.Hash{9}}, 0, false)
	s, _ = p.Snapshot()
	require.Len(t, s.RecentExecuted(), 1, "a mismatching execution parent cannot join the prior tail")
	p.begin(1, nil)
	s, _ = p.Snapshot()
	require.Empty(t, s.RecentExecuted())
}
