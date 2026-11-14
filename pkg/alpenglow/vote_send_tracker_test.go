package alpenglow

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/stretchr/testify/require"
)

func voteMessage(slot uint64) Message {
	return Message{Vote: &VoteMessage{Vote: Vote{Type: VoteTypeNotarize, Slot: slot}}}
}

func TestVoteSendTrackerRecordsFirstSendAndFanout(t *testing.T) {
	metrics.DrainVoteEvents()
	tr := newVoteSendTracker(voteMessage(9), 3)
	tr.done(false) // one peer's copy dropped
	tr.done(true)
	tr.done(true)
	got, _ := metrics.DrainVoteEvents()
	require.Len(t, got, 2)
	require.Equal(t, metrics.VoteFirstSend, got[0].Stage)
	require.Equal(t, uint64(9), got[0].Slot)
	require.Equal(t, "notarize", got[0].Type)
	require.Equal(t, metrics.VoteFanoutDone, got[1].Stage)
	require.Equal(t, 2, got[1].Peers, "two copies sent")
	require.GreaterOrEqual(t, got[1].AtNanos, got[0].AtNanos)
}

func TestVoteSendTrackerWithoutPeersOrVote(t *testing.T) {
	metrics.DrainVoteEvents()
	require.Nil(t, newVoteSendTracker(voteMessage(4), 0))
	got, _ := metrics.DrainVoteEvents()
	require.Len(t, got, 1)
	require.Equal(t, metrics.VoteFanoutDone, got[0].Stage)
	require.Zero(t, got[0].Peers, "no connected peer: the vote went nowhere")

	require.Nil(t, newVoteSendTracker(Message{Certificate: &Certificate{}}, 3), "certificates are not tracked")
	var none *voteSendTracker
	none.done(true) // nil-safe
	got, _ = metrics.DrainVoteEvents()
	require.Empty(t, got)
}
