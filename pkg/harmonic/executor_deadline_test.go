package harmonic

import (
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestExecutorBoundedDrainKeepsCommittedBundles(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	h := startExecutorWith(t, ExecutorConfig{Workers: 1}, true, func(seq uint64) {
		if seq == 2 {
			close(entered)
			<-release
		}
	})
	clock, now := h.useClock()
	env := gatedBank(t, costmodel.Limits{})
	end, limit := now.Add(time.Hour), now.Add(time.Hour+5*time.Millisecond)
	h.exec.SlotStarted(blockprod.LeaderSlotInfo{Slot: 10, Bank: env.Bank, Start: now, End: end, DrainDeadline: limit})
	first := transfer(t, 1)
	h.send(t, bundleEvent(10, 1, false, first))
	h.sync(t)
	// A whole revert-protected bundle is in scratch, and a conflicting one
	// waits behind it. Neither may partially land after the drain deadline.
	h.send(t, bundleEvent(10, 2, true, transfer(t, 2), transfer(t, 3)))
	<-entered
	h.send(t, bundleEvent(10, 3, false, transfer(t, 4)))
	require.Eventually(t, func() bool { return h.exec.pending.Load() == 2 }, time.Second, time.Millisecond)
	clock.set(limit)
	h.send(t, bundleEvent(10, 4, false, transfer(t, 5)))
	require.Eventually(t, func() bool { return h.exec.pending.Load() == 1 }, time.Second, time.Millisecond)
	require.False(t, h.exec.HoldSlot(10), "expired work must not consume the rest of the reserve")
	unblock()
	h.sync(t)
	require.Equal(t, []*solana.Transaction{first}, env.Bank.ForgedTransactions())
	report := h.end(t, 10)
	require.Equal(t, uint64(1), report.Executed)
	require.Equal(t, uint64(3), report.Rejected)
	require.Equal(t, uint64(1), report.DeadlineQueued)
	require.Equal(t, uint64(1), report.DeadlineRunning)
	require.Equal(t, uint64(1), report.DeadlineDiscarded)
	require.Equal(t, uint64(1), report.DeadlineRejected)
	require.Equal(t, FlagDeadline, report.Flags)
}

func TestExecutorDrainBoundary(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(map[bool]string{true: "before", false: "at"}[before], func(t *testing.T) {
			h := startExecutor(t, true, nil)
			clock, now := h.useClock()
			env := gatedBank(t, costmodel.Limits{})
			end, limit := now.Add(time.Hour), now.Add(time.Hour+5*time.Millisecond)
			h.exec.SlotStarted(blockprod.LeaderSlotInfo{Slot: 10, Bank: env.Bank, Start: now, End: end, DrainDeadline: limit})
			h.send(t, bundleEvent(10, 1, false, transfer(t, 1)))
			h.sync(t)
			at := limit
			if before {
				at = at.Add(-time.Nanosecond)
			}
			clock.set(at)
			h.send(t, bundleEvent(10, 2, false, transfer(t, 2)))
			h.sync(t)
			executed := 1
			if before {
				executed++
			}
			require.Len(t, env.Bank.ForgedTransactions(), executed)
			report := h.end(t, 10)
			require.Equal(t, uint64(executed), report.Executed)
		})
	}
}

func TestExecutorOldScratchReleaseDispatchesNewSlot(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	h := startExecutorWith(t, ExecutorConfig{Workers: 1}, true, func(seq uint64) {
		if seq == 2 {
			close(entered)
			<-release
		}
	})
	clock, now := h.useClock()
	oldBank, newBank := gatedBank(t, costmodel.Limits{}), gatedBank(t, costmodel.Limits{})
	limit := now.Add(time.Hour + 5*time.Millisecond)
	h.exec.SlotStarted(blockprod.LeaderSlotInfo{Slot: 10, Bank: oldBank.Bank, Start: now, End: now.Add(time.Hour), DrainDeadline: limit})
	h.send(t, bundleEvent(10, 1, false, transfer(t, 1)))
	h.sync(t)
	h.send(t, bundleEvent(10, 2, false, transfer(t, 2)))
	<-entered
	clock.set(limit)
	h.exec.SlotEnding(10)
	h.exec.SlotStarted(blockprod.LeaderSlotInfo{Slot: 11, Bank: newBank.Bank, Start: limit, End: limit.Add(time.Hour), DrainDeadline: limit.Add(time.Hour + 5*time.Millisecond)})
	h.send(t, bundleEvent(11, 1, false, transfer(t, 3)))
	// An old event cannot admit into the new bank or consume its sequence.
	h.send(t, bundleEvent(10, 3, false, transfer(t, 4)))
	require.Eventually(t, func() bool { return h.exec.pending.Load() == 2 }, time.Second, time.Millisecond)
	unblock()
	h.sync(t)
	require.Len(t, oldBank.Bank.ForgedTransactions(), 1)
	require.Len(t, newBank.Bank.ForgedTransactions(), 1)
	select {
	case report := <-h.reports:
		require.Equal(t, uint64(10), report.Slot)
		require.Equal(t, uint64(1), report.DeadlineDiscarded)
	case <-time.After(time.Second):
		t.Fatal("old slot report never completed")
	}
	require.Equal(t, uint64(1), h.end(t, 11).Executed)
}
