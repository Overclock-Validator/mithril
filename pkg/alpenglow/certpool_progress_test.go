package alpenglow

import (
	"sync"
	"testing"
	"time"
)

func TestCertPoolProgressDoesNotWaitForVerificationLock(t *testing.T) {
	pool := NewCertPool(DefaultCertPoolConfig(), NewCertificateVerifier(), nil)
	pool.mu.Lock() // The same lock held while verifying incoming BLS votes.
	done := make(chan struct{})
	go func() {
		pool.NoteLiveSlot(123)
		close(done)
	}()
	completed := false
	select {
	case <-done:
		completed = true
	case <-time.After(time.Second):
	}
	pool.mu.Unlock()
	<-done
	if !completed {
		t.Fatal("trusted replay progress waited for the verification lock")
	}
	if got := pool.Snapshot().LiveSlot; got != 123 {
		t.Fatalf("live slot = %d, want 123", got)
	}
}

func TestCertPoolConcurrentProgressRemainsMonotonic(t *testing.T) {
	pool := NewCertPool(DefaultCertPoolConfig(), NewCertificateVerifier(), nil)
	const workers, updates = 16, 256
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			<-start
			for n := range updates {
				slot := uint64(n*workers + worker + 1)
				pool.NoteLiveSlot(slot)
				pool.NoteLiveSlot(slot / 2) // Stale updates race newer progress.
			}
		})
	}
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	close(start)
	var previous uint64
	for {
		live := pool.Snapshot().LiveSlot
		pool.mu.Lock()
		anchor := pool.windowAnchorLocked()
		pool.mu.Unlock()
		if live < previous || anchor < live {
			t.Errorf("progress regressed: previous=%d snapshot=%d anchor=%d", previous, live, anchor)
		}
		previous = anchor
		select {
		case <-finished:
			pool.NoteLiveSlot(0)
			pool.NoteLiveSlot(1)
			if got := pool.Snapshot().LiveSlot; got != workers*updates {
				t.Fatalf("live slot = %d, want %d", got, workers*updates)
			}
			return
		default:
		}
	}
}

func TestCertPoolProgressPreservesTrustedVoteWindow(t *testing.T) {
	set, keys := testBLSValidatorSet(100, 40, 30, 15, 10, 5)
	verifier := NewCertificateVerifier()
	if err := verifier.SetValidatorSet(set); err != nil {
		t.Fatal(err)
	}
	pool := NewCertPool(CertPoolConfig{MaxSlotsAhead: 10}, verifier, nil)
	pool.SetEpochLookup(func(uint64) uint64 { return set.Epoch })
	pool.NoteLiveSlot(100)
	checkVote := func(slot uint64, rejected bool, live uint64) {
		t.Helper()
		before := pool.Snapshot()
		addVote(t, pool, NewSkipVote(slot), 4, keys[4])
		after := pool.Snapshot()
		wantRejected := before.VotesRejected
		if rejected {
			wantRejected++
		}
		if after.VotesRejected != wantRejected {
			t.Fatalf("slot %d: rejected=%d, want %d", slot, after.VotesRejected, wantRejected)
		}
		if after.LiveSlot != live {
			t.Fatalf("raw vote at %d moved trusted progress to %d, want %d", slot, after.LiveSlot, live)
		}
	}
	checkVote(110, false, 100) // Inclusive upper edge.
	checkVote(111, true, 100)  // Accepted raw votes cannot slide the window.
	pool.NoteLiveSlot(90)
	checkVote(111, true, 100)
	pool.NoteLiveSlot(101)
	checkVote(111, false, 101)
	pool.ObserveFloor(120)
	checkVote(120, true, 101)  // Finalized floor still rejects old votes.
	checkVote(130, false, 101) // Floor can anchor the window above replay.
	checkVote(131, true, 101)
}
