package node

import (
	"context"
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

func TestRunCommandReportsReplayFailure(t *testing.T) {
	// RunE is required for Cobra to return a replay failure to main, where it
	// produces a nonzero exit and activates systemd's Restart=on-failure policy.
	require.Nil(t, Run.Run)
	require.NotNil(t, Run.RunE)

	divergence := &replay.FooterBankhashMismatch{Slot: 470788792}
	err := replayCommandError(&replay.ReplayResult{Error: divergence})
	require.ErrorIs(t, err, divergence)
	var mismatch *replay.FooterBankhashMismatch
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, uint64(470788792), mismatch.Slot)

	stall := errors.New("block fetch stalled - no progress for 5m0s")
	require.ErrorIs(t, replayCommandError(&replay.ReplayResult{Error: stall}), stall)
	require.NoError(t, replayCommandError(&replay.ReplayResult{}))
	require.NoError(t, replayCommandError(&replay.ReplayResult{WasCancelled: true}))
	require.NoError(t, replayCommandError(&replay.ReplayResult{WasCancelled: true, Error: context.Canceled}))
	// Cancellation still drains the last durable fold. Its storage or safety
	// failure must remain visible even though replay was asked to stop.
	require.ErrorIs(t, replayCommandError(&replay.ReplayResult{WasCancelled: true, Error: divergence}), divergence)
	storageFailure := errors.New("rooted-durable: storage write failed")
	require.ErrorIs(t, replayCommandError(&replay.ReplayResult{WasCancelled: true, Error: storageFailure}), storageFailure)
}
