package node

import (
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/require"
)

func TestReplayShutdownReasonPreservesRootingFailure(t *testing.T) {
	message := "rooted-durable: speculative state reached capacity at slot 462672569; rooting stalled (durable=462671999); halting"
	require.Equal(t, state.ShutdownReasonError+": "+message,
		replayShutdownReason(&replay.ReplayResult{Error: errors.New(message)}))
	require.Equal(t, state.ShutdownReasonStall,
		replayShutdownReason(&replay.ReplayResult{Error: errors.New("block fetch stalled - no progress for 5m0s")}))
	require.Equal(t, state.ShutdownReasonCompleted, replayShutdownReason(&replay.ReplayResult{}))
	require.Equal(t, state.ShutdownReasonNormal,
		replayShutdownReason(&replay.ReplayResult{WasCancelled: true, Error: errors.New(message)}))
}
