package turbine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestReceiverLiveSpoolRetention(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	require.NoError(t, err)
	defer spool.Close()
	r := NewUDPReceiver("127.0.0.1:0")
	r.SetSharedShredSpool(spool)
	var replayed atomic.Uint64
	r.SetReplaySlotSource(replayed.Load)
	for _, slot := range []uint64{100, 488, 900, 1001} {
		spool.Append(slot, []byte("retained"))
	}
	r.pruneReplayedShreds()
	require.True(t, spool.HasSlot(100), "unknown replay frontier must not prune")
	replayed.Store(512)
	r.pruneReplayedShreds()
	require.True(t, spool.HasSlot(100), "early slots must not underflow")

	r.SetHydrationWindow(100, 110)
	replayed.Store(1000)
	r.pruneReplayedShreds()
	require.True(t, spool.HasSlot(100), "catchup must retain its waiting slot")
	r.SetHydrationWindow(0, 0)
	r.pruneReplayedShreds()
	require.False(t, spool.HasSlot(100))
	require.True(t, spool.HasSlot(488), "retention boundary is inclusive")
	require.True(t, spool.HasSlot(900), "recent replay history must survive")
	require.True(t, spool.HasSlot(1001), "future producer data must survive")

	replayed.Store(700)
	r.pruneReplayedShreds()
	require.True(t, spool.HasSlot(488), "rewind must not advance retention")
	replacement := NewUDPReceiver("127.0.0.1:0")
	replacement.SetSharedShredSpool(spool)
	spool.Append(20, []byte("repaired after restart"))
	replayed.Store(10000)
	r.pruneReplayedShreds()
	require.True(t, spool.HasSlot(20), "previous receiver must not prune replacement data")
	require.True(t, spool.HasSlot(1001), "previous receiver must not prune producer data")
}

func TestReceiverLiveSpoolCapacityPrefersRecentSlots(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 80)
	require.NoError(t, err)
	defer spool.Close()
	r := NewUDPReceiver("127.0.0.1:0")
	r.SetSharedShredSpool(spool)
	r.SetReplaySlotSource(func() uint64 { return 1000 })
	packet := make([]byte, 20)
	spool.Append(500, packet)
	spool.Append(501, packet)
	r.pruneReplayedShreds()
	spool.Append(502, packet)
	require.False(t, spool.HasSlot(500), "live capacity must retire the oldest slot")
	require.True(t, spool.HasSlot(501))
	require.True(t, spool.HasSlot(502), "live capacity must accept newer producer data")
	spool.Append(500, packet)
	require.False(t, spool.HasSlot(500), "late old packets must not displace newer slots")
	_, size := spool.Stats()
	require.LessOrEqual(t, size, int64(80))

	r.SetHydrationWindow(600, 610)
	spool.Append(600, packet)
	spool.Append(601, packet)
	spool.Append(602, packet)
	require.False(t, spool.HasSlot(602), "catchup must keep its low-end priority")
	require.True(t, spool.HasSlot(600))
	require.True(t, spool.HasSlot(601))
}

func runSpoolReceiver(t *testing.T, r *UDPReceiver) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(cancel)
	select {
	case err := <-r.Ready():
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not start")
	}
	return func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("receiver did not stop")
		}
	}
}

func TestReceiverSharedSpoolSurvivesRestart(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	require.NoError(t, err)
	defer spool.Close()
	old := NewUDPReceiver("127.0.0.1:0")
	old.SetSharedShredSpool(spool)
	stop := runSpoolReceiver(t, old)
	old.SetHydrationWindow(100, 110)
	stop()

	spool.Append(120, []byte("producer after receiver stop"))
	require.True(t, spool.HasSlot(120), "receiver closed the node-owned spool")
	old.SetHydrationWindow(200, 210)
	old.ResetSlotAndDiscardSpool(120)
	require.True(t, spool.HasSlot(120), "stopped receiver deleted data during restart backoff")

	replacement := NewUDPReceiver("127.0.0.1:0")
	replacement.SetSharedShredSpool(spool)
	stop = runSpoolReceiver(t, replacement)
	defer stop()
	spool.Append(20, []byte("repaired after replay rewind"))
	require.True(t, spool.HasSlot(20), "old floor rejected the rewound slot")

	// A caller may have retained the old receiver before the handoff.
	old.SetHydrationWindow(200, 210)
	old.ResetSlotAndDiscardSpool(20)
	old.RejectAlpenglowBlockIDAndDiscardSlot(120, solana.Hash{1})
	require.True(t, spool.HasSlot(20), "stale receiver removed repaired data")
	require.True(t, spool.HasSlot(120), "stale receiver removed producer data")

	replacement.SetHydrationWindow(40, 50)
	require.False(t, spool.HasSlot(20), "new receiver did not advance retention")
	replacement.ResetSlotAndDiscardSpool(120)
	require.False(t, spool.HasSlot(120), "new receiver could not discard rejected data")
}

func TestReceiverOwnedSpoolClosesOnExit(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	require.NoError(t, err)
	defer spool.Close()
	r := NewUDPReceiver("127.0.0.1:0")
	r.SetShredSpool(spool)
	stop := runSpoolReceiver(t, r)
	stop()
	spool.Append(100, []byte("not retained"))
	require.False(t, spool.HasSlot(100), "receiver-owned spool stayed open")
}
