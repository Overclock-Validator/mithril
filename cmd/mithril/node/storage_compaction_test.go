package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
)

type blockedCompactor struct{ started, release chan struct{} }

func (c *blockedCompactor) CompactOnce(accountsdb.CompactionConfig) (accountsdb.CompactStats, error) {
	close(c.started)
	<-c.release
	return accountsdb.CompactStats{}, nil
}

type compactionReply struct {
	stats accountsdb.CompactStats
	err   error
}

type observedCompactor struct {
	started chan struct{}
	replies chan compactionReply
}

func (c *observedCompactor) CompactOnce(accountsdb.CompactionConfig) (accountsdb.CompactStats, error) {
	c.started <- struct{}{}
	r := <-c.replies
	return r.stats, r.err
}

func awaitCompaction(t *testing.T, c *observedCompactor) {
	t.Helper()
	select {
	case <-c.started:
	case <-time.After(5 * time.Second):
		t.Fatal("compaction did not start")
	}
}

func awaitCompactionObservation(t *testing.T, m *compactionMonitor, after time.Time) compactionObservation {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if v, ok := m.snapshot(); ok && v.CompletedAt.After(after) {
			return v
		}
		select {
		case <-deadline.C:
			t.Fatal("completed compaction observation was not published")
		case <-tick.C:
		}
	}
}

func TestCompactionObservationTracksReturnedCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &observedCompactor{started: make(chan struct{}, 1), replies: make(chan compactionReply)}
	var monitor compactionMonitor
	stop := startAccountsCompactor(ctx, c, accountsdb.CompactionConfig{}, time.Millisecond, &monitor)
	t.Cleanup(func() { cancel(); close(c.replies); stop() })
	awaitCompaction(t, c)
	if _, ok := monitor.snapshot(); ok {
		t.Fatal("in-flight first cycle was published as completed")
	}
	c.replies <- compactionReply{}
	zero := awaitCompactionObservation(t, &monitor, time.Time{})
	if zero.Failed || zero.Stats != (accountsdb.CompactStats{}) || zero.SessionStartedAt.IsZero() ||
		zero.StartedAt.Before(zero.SessionStartedAt) || zero.Duration < 0 || zero.Duration != zero.CompletedAt.Sub(zero.StartedAt) {
		t.Fatalf("zero-work cycle lost its known-zero or timing context: %+v", zero)
	}
	awaitCompaction(t, c)
	for range 50 {
		current, ok := monitor.snapshot()
		if !ok || current != zero {
			t.Fatal("snapshot reads or an in-flight cycle changed the completed observation")
		}
	}
	partial := accountsdb.CompactStats{CandidatesScanned: 7, FilesCompacted: 2, FilesDeleted: 3,
		FilesSparsified: 1, LiveBytesMoved: 1234, BytesReclaimed: 5678}
	c.replies <- compactionReply{partial, errors.New("private error /do/not/publish")}
	failed := awaitCompactionObservation(t, &monitor, zero.CompletedAt)
	if !failed.Failed || failed.Stats != partial || failed.SessionStartedAt != zero.SessionStartedAt {
		t.Fatalf("partial failed cycle: got %+v, want stats %+v in the same session", failed, partial)
	}
	// Returned values own no mutable references to the retained observation.
	failed.Stats.CandidatesScanned = 999
	if current, _ := monitor.snapshot(); current.Stats != partial {
		t.Fatal("caller mutated the retained compaction stats")
	}
}

func TestCompactionObservationReplacementStartsUnknown(t *testing.T) {
	var monitor compactionMonitor
	old := time.Now().Add(-time.Hour)
	monitor.completed(old, old, old.Add(time.Second), accountsdb.CompactStats{FilesDeleted: 4}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	c := &observedCompactor{started: make(chan struct{}, 1), replies: make(chan compactionReply)}
	stop := startAccountsCompactor(ctx, c, accountsdb.CompactionConfig{}, time.Hour, &monitor)
	t.Cleanup(func() { cancel(); close(c.replies); stop() })
	awaitCompaction(t, c)
	if _, ok := monitor.snapshot(); ok {
		t.Fatal("new worker retained the previous worker's completed cycle")
	}
	c.replies <- compactionReply{}
	v := awaitCompactionObservation(t, &monitor, old)
	if !v.SessionStartedAt.After(old.Add(time.Second)) || v.Stats != (accountsdb.CompactStats{}) {
		t.Fatalf("new worker did not bind its own zero-work observation: %+v", v)
	}
}

func TestCompactionObservationDisabledAndConcurrentCopies(t *testing.T) {
	var disabled *compactionMonitor
	if _, ok := disabled.snapshot(); ok {
		t.Fatal("disabled observer has a completed cycle")
	}
	now := time.Now()
	if n := testing.AllocsPerRun(100, func() {
		disabled.completed(now, now, now, accountsdb.CompactStats{}, nil)
	}); n != 0 {
		t.Fatalf("disabled publication allocated %g objects, want 0", n)
	}
	var monitor compactionMonitor
	var readers sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range 1000 {
				v, ok := monitor.snapshot()
				if ok && (int64(v.Stats.CandidatesScanned) != v.Stats.LiveBytesMoved || v.Duration != v.CompletedAt.Sub(v.StartedAt)) {
					errs <- fmt.Errorf("incoherent completed observation: %+v", v)
					return
				}
			}
		}()
	}
	for i := range 1000 {
		monitor.completed(now, now, now.Add(time.Duration(i)), accountsdb.CompactStats{CandidatesScanned: i, LiveBytesMoved: int64(i)}, nil)
	}
	readers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestAccountsCompactorShutdownJoinsInFlightCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &blockedCompactor{started: make(chan struct{}), release: make(chan struct{})}
	stop := startAccountsCompactor(ctx, c, accountsdb.CompactionConfig{}, time.Hour, nil)
	select {
	case <-c.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first compaction never started")
	}
	cancel()
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
		t.Fatal("shutdown returned with compactor still using the DB")
	case <-time.After(20 * time.Millisecond):
	}
	close(c.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not join completed compaction")
	}
}
