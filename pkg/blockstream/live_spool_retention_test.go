package blockstream

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

func TestTurbineSpoolRetiresCatchupDataAfterLiveHandoff(t *testing.T) {
	const capacity = 1 << 20
	spool, err := turbine.OpenShredSpool(t.TempDir(), capacity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(spool.Close)
	spool.Append(100, make([]byte, capacity-16))
	leader := solana.PrivateKey(ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)))
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceTurbine, TurbineBindAddr: "127.0.0.1:0",
		StartSlot: 100, EndSlot: 2000, DisableRPCBlockFetch: true, ShredSpool: spool,
		LeaderForSlot: func(uint64) (solana.PublicKey, bool) { return leader.PublicKey(), true },
	})
	bs.maybeStartLightbringerStream()
	t.Cleanup(func() {
		bs.Stop()
		bs.liveStreamWg.Wait()
	})
	waitForBlockSourceCondition(t, bs.liveStreamConnected.Load)
	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver == nil {
		t.Fatal("connected stream has no receiver")
	}
	receiver.SetHydrationWindow(100, 110)
	bs.confirmedTip.Store(1000)
	bs.SetLastExecutedSlot(1000)
	time.Sleep(700 * time.Millisecond)
	if !spool.HasSlot(100) {
		t.Fatal("active catchup data was pruned")
	}
	bs.deactivateRepairCatchup(receiver)
	deadline := time.Now().Add(2 * time.Second)
	for spool.HasSlot(100) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if spool.HasSlot(100) {
		t.Fatal("obsolete catchup data still fills the live producer cache")
	}
	session := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
		Leader: leader, Slot: 1001, ParentSlot: 1000,
		Broadcaster: &turbine.UDPBroadcaster{}, ShredSpool: spool,
	})
	if err := session.BroadcastHeader(solana.Hash{1}); err != nil {
		t.Fatal(err)
	}
	if err := session.BroadcastFooter(solana.Hash{2}, 0, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := session.BroadcastEndingTickLast(solana.Hash{3}); err != nil {
		t.Fatal(err)
	}
	if packets, err := spool.ReadSlot(1001); err != nil || len(packets) == 0 {
		t.Fatalf("new producer block not retained: packets=%d err=%v", len(packets), err)
	}
}
