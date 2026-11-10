package blockprod

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestLeaderFinalizesWhileFinalEntriesDrain(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "entry_failure"}[fail], func(t *testing.T) {
			committed := make(chan struct{})
			hooks := &recordingHooks{}
			loop, _, broadcaster, delivered := newFinishingLeaderLoop(t, 106, hooks,
				func(*ParentContext, replay.CommitLeaderInput) error { close(committed); return nil })
			started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			loop.activeSink.mu.Lock()
			loop.activeSink.broadcast = func([]turbine.Entry) error {
				close(started)
				<-release
				if fail {
					return errors.New("entry transmission failed")
				}
				return nil
			}
			loop.activeSink.mu.Unlock()
			loop.activeSink.OnEntryBatch([]turbine.Entry{{}}, 0)
			<-started
			go func() { loop.finishActiveSlot(); close(finished) }()
			select {
			case <-committed:
			case <-time.After(time.Second):
				t.Fatal("bank finalization waited for entry broadcast")
			}
			require.Zero(t, broadcaster.count(), "footer must wait for entry drain")
			select {
			case <-finished:
				t.Fatal("completed before the entry worker joined")
			default:
			}
			unblock()
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("completion failed to resume after entry drain")
			}
			require.Equal(t, !fail, *delivered)
			if fail {
				require.Zero(t, broadcaster.count(), "failed entry must never get a footer")
				require.Equal(t, leaderReasonEntryBroadcastFailed, hooks.events[len(hooks.events)-1].reason)
			} else {
				require.Greater(t, broadcaster.count(), 0)
			}
		})
	}
}

type deadlineHoldingHooks struct{ *holdingHooks }

func (*deadlineHoldingHooks) DrainStatus(uint64) SlotDrainStatus {
	return SlotDrainStatus{Outstanding: 2}
}

func TestLeaderBoundsDeadlineAwareHold(t *testing.T) {
	ready := time.Unix(1_700_000_000, 0)
	for _, reserve := range []time.Duration{40 * time.Millisecond, 10 * time.Millisecond} {
		hooks := &recordingHooks{}
		loop, _, _, delivered := newFinishingLeaderLoop(t, 120, hooks,
			func(*ParentContext, replay.CommitLeaderInput) error { return nil })
		loop.completionReserve = reserve
		loop.holder = &deadlineHoldingHooks{&holdingHooks{recordingHooks: hooks, hold: true}}
		loop.currentSlot = func() uint64 { return 120 }
		loop.leaderForSlot = func(uint64) (solana.PublicKey, bool) { return solana.PublicKey{}, false }
		loop.productionWindow = leaderProductionWindow{active: true, startSlot: 120, endSlot: 123, nextSlot: 120, readyAt: ready}
		cutoff := ready.Add(200*time.Millisecond - reserve)
		drainEnd := cutoff.Add(min(reserve/4, 5*time.Millisecond))
		now := cutoff
		loop.now = func() time.Time { return now }
		require.Equal(t, drainEnd, loop.productionWindowDrainDeadlineLocked(120))
		loop.tick()
		require.NotNil(t, loop.activeBank, "a busy holder gets finite grace")
		require.Equal(t, drainEnd, loop.nextCompletionWakeLocked())
		now = drainEnd.Add(-time.Nanosecond)
		loop.tick()
		require.NotNil(t, loop.activeBank)
		now = drainEnd
		loop.tick()
		require.True(t, *delivered, "drain boundary must end the hold")
		require.Nil(t, loop.activeBank)
	}
}

func TestLeaderDeadlineTimerDoesNotWaitForPoll(t *testing.T) {
	for _, busyRelease := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "busy_release"}[busyRelease], func(t *testing.T) {
			committed := make(chan time.Time, 1)
			hooks := &recordingHooks{}
			loop, _, _, _ := newFinishingLeaderLoop(t, 120, hooks,
				func(*ParentContext, replay.CommitLeaderInput) error { committed <- time.Now(); return nil })
			loop.pollInterval = time.Second
			loop.currentSlot = func() uint64 { return 120 }
			loop.leaderForSlot = func(uint64) (solana.PublicKey, bool) { return solana.PublicKey{}, false }
			ready := time.Now().Add(-75 * time.Millisecond)
			loop.productionWindow = leaderProductionWindow{active: true, startSlot: 120, endSlot: 123, nextSlot: 120, readyAt: ready}
			cutoff := loop.productionWindowDeadlineLocked(120)
			if busyRelease {
				released := make(chan struct{})
				close(released)
				loop.holder = &holdingHooks{recordingHooks: hooks, released: released}
			}
			stop, done := make(chan struct{}), make(chan struct{})
			go func() { loop.Run(stop); close(done) }()
			defer func() { close(stop); <-done }()
			select {
			case at := <-committed:
				require.False(t, at.Before(cutoff), "timer must not freeze early")
			case <-time.After(500 * time.Millisecond):
				t.Fatal("completion waited for the one-second poll interval")
			}
		})
	}
}
