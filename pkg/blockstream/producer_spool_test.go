package blockstream

import (
	"crypto/ed25519"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

func TestTurbineStreamSharedSpoolSurvivesRestart(t *testing.T) {
	spool, err := turbine.OpenShredSpool(t.TempDir(), ShredSpoolMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(spool.Close)
	seed, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	addr := seed.LocalAddr().String()
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	unusedDir := filepath.Join(t.TempDir(), "unused")
	leader := solana.PrivateKey(ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)))
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceTurbine, TurbineBindAddr: addr,
		StartSlot: 100, EndSlot: 1000, DisableRPCBlockFetch: true,
		ShredSpool: spool, ShredSpoolDir: unusedDir,
		LeaderForSlot: func(uint64) (solana.PublicKey, bool) { return leader.PublicKey(), true },
	})
	if bs.shredSpool != spool {
		t.Fatal("constructor did not retain the shared spool")
	}
	bs.liveStreamWg.Add(1)
	done := make(chan struct{})
	go func() {
		bs.runTurbineStream()
		close(done)
	}()
	stop := func() {
		bs.Stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("turbine stream did not stop")
		}
	}
	t.Cleanup(stop)
	waitForBlockSourceCondition(t, bs.liveStreamConnected.Load)
	bs.alpenglowMu.Lock()
	first := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if first == nil {
		t.Fatal("connected turbine stream has no receiver")
	}
	if _, err := os.Stat(unusedDir); !os.IsNotExist(err) {
		t.Fatalf("shared spool did not take precedence over directory: %v", err)
	}
	store := func(slot uint64) [][]byte {
		t.Helper()
		session := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
			Leader: leader, Slot: slot, ParentSlot: slot - 1,
			Broadcaster: &turbine.UDPBroadcaster{}, ShredSpool: spool,
		})
		if err := session.BroadcastHeader(solana.Hash{1}); err != nil {
			t.Fatal(err)
		}
		packets, err := spool.ReadSlot(slot)
		if err != nil || len(packets) == 0 {
			t.Fatalf("produced slot %d is not retained: packets=%d err=%v", slot, len(packets), err)
		}
		return packets
	}
	store(200)
	first.SetHydrationWindow(300, 400)
	if spool.HasSlot(200) {
		t.Fatal("active receiver did not advance the shared retention floor")
	}
	if !bs.requestLiveStreamReconnect("test shared spool handover") {
		t.Fatal("could not request receiver restart")
	}
	waitForBlockSourceCondition(t, func() bool { return !bs.liveStreamConnected.Load() })
	store(400)
	first.SetHydrationWindow(500, 600)
	first.ResetSlotAndDiscardSpool(400)
	if !spool.HasSlot(400) {
		t.Fatal("stopped receiver changed the producer cache during restart backoff")
	}
	deadline := time.Now().Add(5 * time.Second)
	var second *turbine.UDPReceiver
	for time.Now().Before(deadline) {
		bs.alpenglowMu.Lock()
		second = bs.activeTurbineReceiver
		bs.alpenglowMu.Unlock()
		if second != first && bs.liveStreamConnected.Load() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if second == nil || second == first || !bs.liveStreamConnected.Load() {
		t.Fatal("replacement turbine receiver did not start")
	}
	packets := store(150)
	first.SetHydrationWindow(500, 600)
	first.RejectAlpenglowBlockIDAndDiscardSlot(150, solana.Hash{2})
	if !spool.HasSlot(150) {
		t.Fatal("previous receiver changed the replacement receiver's cache")
	}
	conn, err := net.DialUDP("udp", nil, mustResolveUDPAddr(t, addr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write(packets[0]); err != nil {
		t.Fatal(err)
	}
	waitForBlockSourceCondition(t, func() bool { return second.Stats().Packets > 0 })
	stop()
	store(151)
}
