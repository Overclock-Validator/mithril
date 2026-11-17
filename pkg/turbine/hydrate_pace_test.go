package turbine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHydratePaceDelay(t *testing.T) {
	const lo = 1000
	// Near replay: never delayed, whatever the budget.
	for slot := uint64(lo); slot <= lo+hydratePaceNear; slot++ {
		require.Zero(t, hydratePaceDelay(slot, lo, 10, time.Second))
	}
	// Far: an even share of what is left.
	require.Equal(t, 100*time.Millisecond, hydratePaceDelay(lo+hydratePaceNear+1, lo, 5, 500*time.Millisecond))
	require.Equal(t, 500*time.Millisecond, hydratePaceDelay(lo+200, lo, 1, 500*time.Millisecond))
	// Budget spent or nothing left: no delay.
	require.Zero(t, hydratePaceDelay(lo+200, lo, 5, 0))
	require.Zero(t, hydratePaceDelay(lo+200, lo, 5, -time.Millisecond))
	require.Zero(t, hydratePaceDelay(lo+200, lo, 0, time.Second))
	// The shares of n far loads add up to at most the budget.
	budget := 125 * time.Millisecond
	var total time.Duration
	for n := 7; n > 0; n-- {
		d := hydratePaceDelay(lo+100, lo, n, budget-total)
		total += d
	}
	require.LessOrEqual(t, total, budget)
}

// pacedSpoolReceiver spools an incomplete FEC set for each slot in
// [lo, lo+count): each slot is cheap to hydrate and never completes, so
// passes exercise loading and pacing only.
func pacedSpoolReceiver(t *testing.T, lo uint64, count int) (*UDPReceiver, *[]time.Time, *[]uint64) {
	t.Helper()
	spool, err := OpenShredSpool(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(spool.Close)
	for slot := lo; slot < lo+uint64(count); slot++ {
		gen := ShredGenerator{Slot: slot, ParentSlot: slot - 1, Version: 1}
		packets, _, _, _, err := gen.MakeShredsFromData(benchmarkLeaderKey(), benchmarkPayload(2000), false, [32]byte{}, 0, 0)
		require.NoError(t, err)
		spool.Append(slot, packets[0])
	}
	r := NewUDPReceiver("127.0.0.1:0")
	r.SetShredSpool(spool)
	var mu sync.Mutex
	times, slots := &[]time.Time{}, &[]uint64{}
	r.hydrateObserved = func(slot uint64) {
		mu.Lock()
		*times = append(*times, time.Now())
		*slots = append(*slots, slot)
		mu.Unlock()
	}
	return r, times, slots
}

// With pacing on, every slot within hydratePaceNear of replay loads before
// any pause, in ascending order, and the paced far slots finish within the
// budget: replay never waits on the hydrator for slots the spool holds.
func TestHydratePassNeverDelaysSlotsNearReplay(t *testing.T) {
	const lo, count = 5000, 48
	r, times, slots := pacedSpoolReceiver(t, lo, count)
	budget := 300 * time.Millisecond
	r.SetHydrationPacing(budget)
	r.hydrLo.Store(lo)
	r.hydrHi.Store(lo + 256)
	start := time.Now()
	stop, rerun := r.hydratePass(context.Background(), lo, lo+256, map[uint64]bool{})
	elapsed := time.Since(start)
	require.False(t, stop)
	require.False(t, rerun)
	require.Len(t, *slots, count)
	for i, slot := range *slots {
		require.Equal(t, uint64(lo+i), slot, "loads must be in ascending slot order")
	}
	near := hydratePaceNear + 1
	// Near slots load back to back: no pacing delay before any of them.
	require.Less(t, (*times)[near-1].Sub(start), 50*time.Millisecond, "near slots were delayed")
	// Far slots are spread out, and the pass ends within the budget.
	require.Greater(t, (*times)[count-1].Sub((*times)[near-1]), budget/2, "far slots were not paced")
	require.Less(t, elapsed, budget+100*time.Millisecond, "pass overran its budget")
}

// When replay advances during a paced wait, the pass stops at once so the
// next pass serves the new near region first.
func TestHydratePassRestartsWhenWindowMoves(t *testing.T) {
	const lo, count = 7000, 48
	r, times, _ := pacedSpoolReceiver(t, lo, count)
	r.SetHydrationPacing(10 * time.Second)
	r.hydrLo.Store(lo)
	r.hydrHi.Store(lo + 256)
	go func() {
		time.Sleep(100 * time.Millisecond)
		r.SetHydrationWindow(lo+10, lo+266)
	}()
	start := time.Now()
	stop, rerun := r.hydratePass(context.Background(), lo, lo+256, map[uint64]bool{})
	require.False(t, stop)
	require.True(t, rerun)
	require.Less(t, time.Since(start), time.Second, "paced wait ignored the window move")
	require.LessOrEqual(t, len(*times), hydratePaceNear+2)
}

// Without a pacing budget, the pass loads everything back to back as before.
func TestHydratePassUnpacedByDefault(t *testing.T) {
	const lo, count = 9000, 48
	r, times, _ := pacedSpoolReceiver(t, lo, count)
	start := time.Now()
	stop, rerun := r.hydratePass(context.Background(), lo, lo+256, map[uint64]bool{})
	require.False(t, stop)
	require.False(t, rerun)
	require.Len(t, *times, count)
	require.Less(t, time.Since(start), 200*time.Millisecond)
}
