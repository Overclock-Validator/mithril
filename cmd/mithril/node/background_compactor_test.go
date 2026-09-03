package node

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackgroundCompactorStopJoinsCycleBeforeAccountsDBTeardown(t *testing.T) {
	started := make(chan struct{})
	cancelObserved := make(chan struct{})
	releaseCycle := make(chan struct{})
	var calls atomic.Uint64
	var compacting atomic.Bool

	worker := startBackgroundCompactor(t.Context(), time.Millisecond, func(ctx context.Context) {
		compacting.Store(true)
		defer compacting.Store(false)
		if calls.Add(1) == 1 {
			close(started)
		}
		<-ctx.Done()
		close(cancelObserved)
		<-releaseCycle
	})

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("compaction cycle did not start")
	}

	stopped := make(chan struct{})
	go func() {
		worker.Stop()
		close(stopped)
	}()
	select {
	case <-cancelObserved:
	case <-time.After(2 * time.Second):
		t.Fatal("active compaction did not observe cancellation")
	}
	select {
	case <-stopped:
		t.Fatal("Stop returned while CompactOnce callback was still active")
	default:
	}

	close(releaseCycle)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not join the completed compaction cycle")
	}
	// AccountsDB teardown would begin here. No compaction callback may still be
	// active or begin after this point.
	assert.False(t, compacting.Load())
	require.Eventually(t, func() bool { return calls.Load() == 1 }, time.Second, time.Millisecond)
	time.Sleep(3 * time.Millisecond)
	assert.Equal(t, uint64(1), calls.Load())
	worker.Stop() // idempotent join
}
