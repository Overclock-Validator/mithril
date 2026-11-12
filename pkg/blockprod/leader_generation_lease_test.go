package blockprod

import (
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/stretchr/testify/require"
)

func TestLeaderReleasesGenerationLeaseBeforeHandoff(t *testing.T) {
	loop, _, _, _ := newFinishingLeaderLoop(t, 106, &recordingHooks{}, func(*ParentContext, replay.CommitLeaderInput) error { return nil })
	held, acquired, released, delivered := false, 0, 0, false
	loop.parentGenerationLease = func(generation uint64) (func(), bool) {
		require.Equal(t, loop.activeParentGeneration, generation)
		held = true
		acquired++
		return func() { held = false; released++ }, true
	}
	loop.onBlock = func(*b.Block) {
		require.False(t, held, "replay handoff must not hold the parent lease")
		require.Equal(t, 1, released)
		delivered = true
	}
	loop.finishActiveSlotLocked()
	require.True(t, delivered)
	require.Equal(t, 1, acquired)
	require.Equal(t, 1, released, "release must be exactly once")
}

func TestLeaderRejectsGenerationChangedAfterFinalCanonicalCheck(t *testing.T) {
	hooks := &recordingHooks{}
	loop, _, _, delivered := newFinishingLeaderLoop(t, 106, hooks, func(*ParentContext, replay.CommitLeaderInput) error { return nil })
	acquired := false
	loop.parentGenerationLease = func(uint64) (func(), bool) { acquired = true; return nil, false }
	loop.finishActiveSlotLocked()
	require.True(t, acquired)
	require.False(t, *delivered)
	require.Nil(t, loop.activeBank)
	require.Equal(t, []string{"ending", "aborted"}, hooks.kinds())
}
