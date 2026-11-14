package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVoteEventsDrainAndBound(t *testing.T) {
	DrainVoteEvents()
	at := time.Unix(0, 1000)
	RecordVoteEvent(7, "notarize", VoteQueued, 0, at)
	RecordVoteEvent(7, "notarize", VoteFanoutDone, 5, at.Add(time.Millisecond))
	got, dropped := DrainVoteEvents()
	require.Zero(t, dropped)
	require.Equal(t, []VoteEvent{
		{Slot: 7, Type: "notarize", Stage: VoteQueued, AtNanos: 1000},
		{Slot: 7, Type: "notarize", Stage: VoteFanoutDone, Peers: 5, AtNanos: 1000 + int64(time.Millisecond)},
	}, got)
	got, _ = DrainVoteEvents()
	require.Empty(t, got)

	for i := range maxPendingVoteEvents + 3 {
		RecordVoteEvent(uint64(i), "notarize", VoteQueued, 0, at)
	}
	got, dropped = DrainVoteEvents()
	require.Len(t, got, maxPendingVoteEvents)
	require.Equal(t, uint64(3), dropped)
	require.Equal(t, uint64(3), got[0].Slot, "the oldest are dropped")
}
