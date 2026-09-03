package node

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJoinedStopperWaitsForLeaderAccountsDBUseBeforeTeardown(t *testing.T) {
	stopSignal := make(chan struct{})
	finishStarted := make(chan struct{})
	releaseFinish := make(chan struct{})
	done := make(chan struct{})
	var accountsDBUseActive atomic.Bool
	go func() {
		<-stopSignal
		accountsDBUseActive.Store(true)
		close(finishStarted)
		<-releaseFinish
		accountsDBUseActive.Store(false)
		close(done)
	}()

	stopper := newJoinedStopper(func() { close(stopSignal) }, done)
	stopped := make(chan struct{})
	go func() {
		stopper.Stop()
		close(stopped)
	}()
	select {
	case <-finishStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("leader finalization did not start")
	}
	select {
	case <-stopped:
		t.Fatal("leader stop returned while its AccountsDB use was active")
	default:
	}
	assert.True(t, accountsDBUseActive.Load())

	close(releaseFinish)
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("leader stop did not join finalization")
	}
	assert.False(t, accountsDBUseActive.Load(), "AccountsDB teardown may begin only after leader join")
	stopper.Stop() // idempotent
}
