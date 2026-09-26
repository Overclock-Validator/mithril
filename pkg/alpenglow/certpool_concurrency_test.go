package alpenglow

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func waitCertPoolCall(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("certificate-pool operation did not finish")
	}
}

func certPoolAsync(fn func()) <-chan struct{} {
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	return done
}

// Park a real owner at the production verification gate. No cryptography is
// stubbed; releasing the gate runs the normal randomized verification path.
func parkCertPoolVerification(t *testing.T, pool *CertPool, msg VoteMessage) (func(), <-chan struct{}) {
	t.Helper()
	pool.verificationMu.Lock()
	var once sync.Once
	release := func() { once.Do(pool.verificationMu.Unlock) }
	t.Cleanup(release)
	done := certPoolAsync(func() { pool.AddVote(msg) })
	ready := certPoolAsync(func() {
		for pool.Snapshot().PendingTotal == 0 {
			time.Sleep(time.Millisecond)
		}
	})
	waitCertPoolCall(t, ready) // Also proves Snapshot can acquire mu during work.
	return release, done
}

func TestCertPoolOffLockAdmissionAndRewardFlush(t *testing.T) {
	pool, set, keys, emitted := newTestPool(t)
	seen := make(chan VerifiedVote, 8)
	pool.SetVerifiedVoteSink(func(v VerifiedVote) { seen <- v })
	vote := NewSkipVote(500)
	first := VoteMessage{Vote: vote, Rank: 0, Signature: signTestVote(t, vote, keys[0])}
	second := VoteMessage{Vote: vote, Rank: 1, Signature: signTestVote(t, vote, keys[1])}
	release, owner := parkCertPoolVerification(t, pool, first)
	waitCertPoolCall(t, certPoolAsync(func() { pool.AddVote(second) }))
	if got := pool.Snapshot().PendingTotal; got != 2 {
		t.Fatalf("pending = %d; in-flight and newly buffered votes must both count", got)
	}
	flush := certPoolAsync(func() { pool.FlushRewardVotes(vote.Slot) })
	select {
	case <-flush:
		t.Fatal("flush escaped in-flight verification")
	case <-time.After(20 * time.Millisecond):
	}
	if len(seen) != 0 {
		t.Fatal("unverified vote reached sink")
	}
	release()
	waitCertPoolCall(t, owner)
	waitCertPoolCall(t, flush)
	if len(seen) != 2 || pool.Snapshot().PendingTotal != 0 {
		t.Fatalf("flush lost or duplicated work: published=%d snapshot=%+v", len(seen), pool.Snapshot())
	}
	if len(*emitted) == 0 {
		t.Fatal("buffered arrivals did not drive certificate assembly")
	}
	for _, cert := range *emitted {
		if _, _, err := verifyCertificateWithSet(set, cert, true); err != nil {
			t.Fatalf("concurrently assembled certificate failed verification: %v", err)
		}
	}
}

func TestCertPoolOffLockPruningDiscardsInFlightResults(t *testing.T) {
	pool, _, keys, emitted := newTestPool(t)
	seen := make(chan VerifiedVote, 8)
	pool.SetVerifiedVoteSink(func(v VerifiedVote) { seen <- v })
	vote := NewSkipVote(500)
	release, owner := parkCertPoolVerification(t, pool, VoteMessage{Vote: vote, Rank: 0, Signature: signTestVote(t, vote, keys[0])})
	waitCertPoolCall(t, certPoolAsync(func() { pool.ObserveFloor(vote.Slot) }))
	if got := pool.Snapshot(); got.PendingTotal != 0 || got.Slots != 0 {
		t.Fatalf("pruning retained in-flight accounting: %+v", got)
	}
	release()
	waitCertPoolCall(t, owner)
	if len(seen) != 0 || len(*emitted) != 0 || pool.Snapshot().PendingTotal != 0 {
		t.Fatal("pruned work was published or decremented accounting twice")
	}
	addVote(t, pool, NewSkipVote(501), 0, keys[0])
	if len(seen) != 1 {
		t.Fatal("subsequent live vote was lost")
	}
}

func TestCertPoolOffLockBindingChangeDiscardsResults(t *testing.T) {
	for _, kind := range []string{"validator-set", "epoch-lookup"} {
		t.Run(kind, func(t *testing.T) {
			pool, set, keys, emitted := newTestPool(t)
			seen := make(chan VerifiedVote, 8)
			pool.SetVerifiedVoteSink(func(v VerifiedVote) { seen <- v })
			vote := NewSkipVote(500)
			msg := VoteMessage{Vote: vote, Rank: 0, Signature: signTestVote(t, vote, keys[0])}
			release, owner := parkCertPoolVerification(t, pool, msg)
			if kind == "validator-set" {
				if err := pool.verifier.SetValidatorSet(set); err != nil {
					t.Fatal(err)
				}
			} else {
				pool.SetEpochLookup(func(uint64) uint64 { return set.Epoch })
			}
			release()
			waitCertPoolCall(t, owner)
			if got := pool.Snapshot(); len(seen) != 0 || len(*emitted) != 0 || got.PendingTotal != 0 || got.Slots != 0 {
				t.Fatalf("stale binding published or retained state: %+v", got)
			}
			pool.AddVote(msg)
			if len(seen) != 1 {
				t.Fatal("vote could not be retried under the current binding")
			}
		})
	}
}

func TestCertPoolOffLockPendingBoundAndDuplicates(t *testing.T) {
	pool, _, keys, _ := newTestPool(t)
	pool.cfg.MaxPendingVotesPerSlot = 2
	pool.cfg.MaxPendingVotesTotal = 2
	vote := NewSkipVote(500)
	msgs := make([]VoteMessage, 3)
	for i := range msgs {
		msgs[i] = VoteMessage{Vote: vote, Rank: uint16(i), Signature: signTestVote(t, vote, keys[i])}
	}
	release, owner := parkCertPoolVerification(t, pool, msgs[0])
	waitCertPoolCall(t, certPoolAsync(func() { pool.AddVote(msgs[1]) }))
	waitCertPoolCall(t, certPoolAsync(func() { pool.AddVote(msgs[0]) }))
	third := certPoolAsync(func() { pool.AddVote(msgs[2]) })
	select {
	case <-third:
		t.Fatal("capacity-pressure admission did not wait for authentication")
	case <-time.After(20 * time.Millisecond):
	}
	if got := pool.Snapshot().PendingTotal; got != 2 {
		t.Fatalf("in-flight votes escaped bounds or duplicate consumed capacity: %d", got)
	}
	release()
	waitCertPoolCall(t, owner)
	waitCertPoolCall(t, third)
	pool.FlushRewardVotes(vote.Slot)
	if got := pool.Snapshot(); got.PendingTotal != 0 || got.VotesAccepted != 3 {
		t.Fatalf("capacity wakeup lost or duplicated votes: %+v", got)
	}
}

func TestCertPoolOffLockEvictionDoesNotResurrectSlot(t *testing.T) {
	set, keys := testBLSValidatorSet(100, 40, 30, 15, 10, 5)
	verifier := NewCertificateVerifier()
	if err := verifier.SetValidatorSet(set); err != nil {
		t.Fatal(err)
	}
	pool := NewCertPool(CertPoolConfig{MaxLiveSlots: 1}, verifier, nil)
	pool.SetEpochLookup(func(uint64) uint64 { return set.Epoch })
	pool.NoteLiveSlot(100)
	seen := make(chan VerifiedVote, 8)
	pool.SetVerifiedVoteSink(func(v VerifiedVote) { seen <- v })
	future := NewSkipVote(110)
	release, owner := parkCertPoolVerification(t, pool, VoteMessage{Vote: future, Rank: 0, Signature: signTestVote(t, future, keys[0])})
	near := NewSkipVote(101)
	msg := VoteMessage{Vote: near, Rank: 4, Signature: signTestVote(t, near, keys[4])}
	waitCertPoolCall(t, certPoolAsync(func() { pool.AddVote(msg) }))
	if got := pool.Snapshot(); got.PendingTotal != 1 || got.Slots != 1 {
		t.Fatalf("eviction accounting mismatch: %+v", got)
	}
	release()
	waitCertPoolCall(t, owner)
	pool.FlushRewardVotes(near.Slot)
	if len(seen) != 1 || (<-seen).Message.Vote.Slot != near.Slot || pool.Snapshot().PendingTotal != 0 {
		t.Fatal("evicted work displaced or corrupted the nearer slot")
	}
}

func TestCertPoolRewardFlushPriority(t *testing.T) {
	for _, prune := range []bool{false, true} {
		t.Run(fmt.Sprintf("prune=%t", prune), func(t *testing.T) {
			pool, _, keys, _ := newTestPool(t)
			vote := NewSkipVote(500)
			// A below-threshold vote must be published by the reward flush.
			pool.AddVote(VoteMessage{Vote: vote, Rank: 4, Signature: signTestVote(t, vote, keys[4])})
			pool.mu.Lock()
			ps := pool.slots[vote.Slot]
			ps.processing = true // Hold ownership until both flushes are queued.
			pool.mu.Unlock()
			flush1 := certPoolAsync(func() { pool.FlushRewardVotes(vote.Slot) })
			flush2 := certPoolAsync(func() { pool.FlushRewardVotes(vote.Slot) })
			deadline := time.Now().Add(2 * time.Second)
			for {
				pool.mu.Lock()
				waiting := ps.flushWaiting
				pool.mu.Unlock()
				if waiting == 2 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("flushes did not register")
				}
				time.Sleep(time.Millisecond)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			pool.SetVerifiedVoteSink(func(v VerifiedVote) {
				if v.Message.Rank == 4 {
					close(entered)
					<-release
				}
			})
			msg := VoteMessage{Vote: vote, Rank: 0, Signature: signTestVote(t, vote, keys[0])}
			arrival := certPoolAsync(func() { pool.AddVote(msg) })
			if prune {
				pool.ObserveFloor(vote.Slot)
			} else {
				pool.mu.Lock()
				ps.processing = false
				pool.workCond.Broadcast()
				pool.mu.Unlock()
				waitCertPoolCall(t, entered)
				if got := pool.Snapshot().VotesAccepted; got != 1 {
					t.Fatalf("new arrival overtook reward flush: accepted=%d", got)
				}
				select {
				case <-arrival:
					t.Fatal("arrival escaped active flush")
				default:
				}
			}
			once.Do(func() { close(release) })
			waitCertPoolCall(t, flush1)
			waitCertPoolCall(t, flush2)
			waitCertPoolCall(t, arrival)
			pool.mu.Lock()
			defer pool.mu.Unlock()
			if ps.flushWaiting != 0 {
				t.Fatalf("leaked flush waiters: %d", ps.flushWaiting)
			}
		})
	}
}
