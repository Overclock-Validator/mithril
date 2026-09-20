package blockstream

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/repair"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

func TestRepairCatchupStartsWithoutLiveShreds(t *testing.T) {
	for _, tc := range []struct {
		name        string
		peer        bool
		tip         uint64
		rpcFallback bool
		wantRequest bool
	}{
		{name: "shreds_only", peer: true, tip: 1200, wantRequest: true},
		{name: "no_peer", tip: 1200},
		{name: "no_tip", peer: true},
		{name: "rpc_fallback", peer: true, tip: 1200, rpcFallback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			identity := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
			receiver := turbine.NewUDPReceiver("127.0.0.1:0")
			if err := receiver.SetRepairPeerSource(identity, func() []gossip.RepairPeer {
				if !tc.peer {
					return nil
				}
				return []gossip.RepairPeer{{Pubkey: gossip.Pubkey{1}, Addr: peer.LocalAddr().(*net.UDPAddr)}}
			}); err != nil {
				t.Fatal(err)
			}
			bs := NewBlockSource(&BlockSourceOpts{
				SourceType: BlockSourceTurbine, TurbineBindAddr: "127.0.0.1:0",
				StartSlot: 1000, RepairCatchupMaxGapSlots: 1024,
				DisableRPCBlockFetch: !tc.rpcFallback,
			})
			bs.confirmedTip.Store(tc.tip)
			ctx, cancel := context.WithCancel(context.Background())
			receiverDone, monitorDone := make(chan struct{}), make(chan struct{})
			go func() { defer close(receiverDone); _ = receiver.Run(ctx) }()
			go func() { defer close(monitorDone); bs.runRepairCatchup(ctx, receiver) }()
			t.Cleanup(func() {
				cancel()
				bs.Stop()
				for _, done := range []chan struct{}{receiverDone, monitorDone} {
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Error("repair startup worker did not stop")
					}
				}
			})
			select {
			case err := <-receiver.Ready():
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("receiver did not start")
			}
			wait := time.Second
			if tc.wantRequest {
				wait = 3 * time.Second
			}
			if err := peer.SetReadDeadline(time.Now().Add(wait)); err != nil {
				t.Fatal(err)
			}
			packet := make([]byte, 2048)
			n, _, err := peer.ReadFromUDP(packet)
			if !tc.wantRequest {
				if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
					t.Fatalf("unexpected repair request: bytes=%d err=%v", n, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("no initial repair request without live shreds: %v", err)
			}
			if n != 160 || binary.LittleEndian.Uint32(packet[:4]) != 9 || binary.LittleEndian.Uint64(packet[144:152]) != 1000 {
				t.Fatalf("unexpected initial repair request: %x", packet[:n])
			}
			if !repair.VerifySignedRequest(packet[:n], gossip.Pubkey(identity.Public().(ed25519.PublicKey))) {
				t.Fatal("initial repair request is not authenticated")
			}
			if edge, _ := receiver.ShredEdges(); edge != 0 {
				t.Fatalf("test received an unexpected live shred at slot %d", edge)
			}
		})
	}
}
