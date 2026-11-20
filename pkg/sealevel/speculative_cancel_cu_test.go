package sealevel

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// A private replay attempt may be cancelled before its first SBF instruction.
// It must not be charged as an on-chain VM fault when the loader depletes CUs.
func TestSpeculativeCancellationPreservesComputeMeter(t *testing.T) {
	ctx := agaveSBFContext(t, agaveSBFMapping{name: "legacy_flat"}, nil)
	ctx.Speculative = true
	var cancelled atomic.Bool
	cancelled.Store(true)
	ctx.SpeculativeCancel = &cancelled
	initial := ctx.ComputeMeter.Remaining()

	err := executeLoadedProgram(ctx, agaveSBFProgram(t), executionSyscalls(ctx))
	require.ErrorIs(t, err, InstrErrProgramFailedToComplete)
	require.Equal(t, initial, ctx.ComputeMeter.Remaining())
}
