package replay

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/require"
)

type blockedCapacityCommitter struct {
	fakeCommitter
	entered, release chan struct{}
}

func (c *blockedCapacityCommitter) CommitBatch(d []accounts.SlotDelta, through uint64, hashes map[uint64][32]byte, context []byte) (accountsdb.BatchCommitResult, error) {
	close(c.entered)
	<-c.release
	return c.fakeCommitter.CommitBatch(d, through, hashes, context)
}

func TestReplayFoldCapacityVisibleWhileActualDrainIsBlocked(t *testing.T) {
	fc := &blockedCapacityCommitter{fakeCommitter: fakeCommitter{durable: accounts.NewMemAccounts()}, entered: make(chan struct{}), release: make(chan struct{})}
	tail := newUnrootedTail(&fakeDurable{}, fc, 2, 2, "")
	for slot := uint64(1); slot <= 2; slot++ {
		tail.Add(slot, []*accounts.Account{testAccount(byte(slot), slot)}, testHashBytes(byte(slot)))
		tail.SetContext(slot, &state.ResumeContext{Slot: slot})
	}
	job, err := tail.buildFoldJob(2, false)
	require.NoError(t, err)
	tail.Add(3, []*accounts.Account{testAccount(3, 3)}, testHashBytes(3))
	tail.SetContext(3, &state.ResumeContext{Slot: 3})
	promoter := newAsyncPromoter(fc)
	var release sync.Once
	var done chan struct{}
	t.Cleanup(func() {
		release.Do(func() { close(fc.release) })
		if done != nil {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("capacity drain did not join")
				return
			}
		}
		promoter.stop()
	})
	promoter.enqueue(job)
	select {
	case <-fc.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("fold worker did not enter commit")
	}
	p := diagnosticProgress(t)
	before, _ := p.Snapshot()
	started := time.Now()
	ticket := p.beginFoldCapacityObservation(3, true, started)
	done = make(chan struct{})
	var stillOver bool
	go func() {
		stillOver = settleFoldsAtCapacity(tail, promoter, 3, func(r *foldResult) {
			if r != nil && r.err == nil {
				tail.applyFoldJob(r.job)
			}
		}, nil)
		ticket.finish(stillOver, promoter.failure != nil)
		close(done)
	}()
	s, ok := p.ReplayDiagnosticsSnapshot()
	require.True(t, ok)
	require.Empty(t, s.Records, "a new completed row was not required")
	require.Equal(t, ReplayDiagnosticFoldCapacity{State: "active", Slot: 3, StartedAt: started, HadInFlightFold: true}, s.FoldCapacity)
	select {
	case <-done:
		t.Fatal("capacity drain returned before the worker was released")
	default:
	}
	after, _ := p.Snapshot()
	require.Equal(t, before, after, "diagnostics changed replay phase, readiness or progress")
	release.Do(func() { close(fc.release) })
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("capacity drain did not finish")
	}
	require.False(t, stillOver)
	s, _ = p.ReplayDiagnosticsSnapshot()
	require.Equal(t, "returned", s.FoldCapacity.State)
	require.Equal(t, "relieved", s.FoldCapacity.Outcome)
	require.True(t, s.FoldCapacity.DurationKnown)
	require.False(t, s.FoldCapacity.ReturnedAt.IsZero())
	require.Equal(t, uint64(s.FoldCapacity.ReturnedAt.Sub(started)), s.FoldCapacity.DurationNanoseconds)
	s.FoldCapacity.Slot = 0
	next, _ := p.ReplayDiagnosticsSnapshot()
	require.Equal(t, uint64(3), next.FoldCapacity.Slot)
}

func TestReplayFoldCapacityOutcomesResetAndDisabled(t *testing.T) {
	var absent *Progress
	var disabled Progress
	started := time.Now()
	require.Zero(t, testing.AllocsPerRun(1000, func() {
		absent.beginFoldCapacityObservation(3, true, started).finish(false, false)
		disabled.beginFoldCapacityObservation(3, true, started).finish(false, false)
	}))
	for _, tc := range []struct {
		over, failed bool
		outcome      string
	}{{false, false, "relieved"}, {true, false, "still_over_capacity"}, {true, true, "failed"}, {false, true, "failed"}} {
		p := diagnosticProgress(t)
		o := p.beginFoldCapacityObservation(3, false, started)
		o.finish(tc.over, tc.failed)
		s, _ := p.ReplayDiagnosticsSnapshot()
		require.Equal(t, tc.outcome, s.FoldCapacity.Outcome)
		first := s.FoldCapacity
		o.finish(!tc.over, !tc.failed)
		s, _ = p.ReplayDiagnosticsSnapshot()
		require.Equal(t, first, s.FoldCapacity, "duplicate finish redated the original event")
		old := p.beginFoldCapacityObservation(4, true, started)
		newer := p.beginFoldCapacityObservation(5, false, started)
		old.finish(false, false)
		s, _ = p.ReplayDiagnosticsSnapshot()
		require.Equal(t, uint64(5), s.FoldCapacity.Slot)
		require.Equal(t, "active", s.FoldCapacity.State)
		p.rewound()
		newer.finish(false, false)
		s, _ = p.ReplayDiagnosticsSnapshot()
		require.Zero(t, s.FoldCapacity)
		p.diagnostics.Load().foldCapacitySequence = math.MaxUint64
		saturated := p.beginFoldCapacityObservation(6, false, started)
		require.Nil(t, saturated.owner)
		s, _ = p.ReplayDiagnosticsSnapshot()
		require.Zero(t, s.FoldCapacity)
		p.begin(2, nil)
		future := p.beginFoldCapacityObservation(7, false, time.Now().Add(time.Hour))
		future.finish(false, false)
		s, _ = p.ReplayDiagnosticsSnapshot()
		require.Equal(t, "returned", s.FoldCapacity.State)
		require.False(t, s.FoldCapacity.DurationKnown, "invalid elapsed time became measured zero")
	}
}
