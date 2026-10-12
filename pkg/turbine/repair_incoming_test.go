package turbine

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	repairproto "github.com/Overclock-Validator/mithril/pkg/repair"
	"github.com/gagliardetto/solana-go"
)

// This uses the real UDP read/parse/version/leader-signature/assembly path.
// The nonce gate selects the interleaving; no cancellation or accounting helper
// is called by the test. Synthetic test-leader keys never leave loopback.
func TestRepairIncomingAdmissionReservation(t *testing.T) {
	for _, mode := range []string{"broadcast", "late-repair", "bad-signature", "wrong-version", "missing-leader"} {
		t.Run(mode, func(t *testing.T) {
			listen := func() *net.UDPConn {
				t.Helper()
				conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
						t.Error(err)
					}
				})
				return conn
			}
			conn, peerSocket := listen(), listen()
			leader := testShredLeader(t)
			gen := ShredGenerator{Slot: 93, ParentSlot: 92, Version: 7}
			packets, _, _, _, err := gen.MakeShredsFromData(leader, []byte("signed incoming repair-race fixture"), false, solana.Hash{4}, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			packet := append([]byte(nil), packets[0]...)
			peer := gossip.RepairPeer{Pubkey: gossip.Pubkey(leader.PublicKey()), Addr: peerSocket.LocalAddr().(*net.UDPAddr)}
			peers := []gossip.RepairPeer{peer}
			r := NewUDPReceiver(conn.LocalAddr().String())
			r.SetShredVersion(7)
			r.SetLeaderForSlot(func(slot uint64) (solana.PublicKey, bool) {
				return leader.PublicKey(), slot == 93 && mode != "missing-leader"
			})
			if err := r.SetRepairPeerSource(nil, func() []gossip.RepairPeer { return peers }); err != nil {
				t.Fatal(err)
			}
			c := r.repairClient
			send := func(budget int) int {
				return c.sendTier(conn, peers, []SlotRepairRequest{{Slot: 93, MissingDataShreds: []uint32{0}}}, budget, nil, repairMinRequestTimeout)
			}
			if mode == "late-repair" {
				// Capture a genuinely sent request, expire it through the normal
				// API, then return its nonce with the signed data over UDP while a
				// replacement request is paused before registration.
				if send(1) != 1 {
					t.Fatal("initial repair request did not send")
				}
				if err := peerSocket.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				wire := make([]byte, 256)
				n, _, err := peerSocket.ReadFromUDP(wire)
				if err != nil {
					t.Fatal(err)
				}
				sender := gossip.Pubkey(c.identity.Public().(ed25519.PublicKey))
				if n != 160 || !repairproto.VerifySignedRequest(wire[:n], sender) {
					t.Fatal("initial request was not an authenticated repair packet")
				}
				// Signature(64), sender(32), recipient(32), timestamp(8) follow
				// the four-byte request variant; the nonce starts at byte 140.
				packet = binary.LittleEndian.AppendUint32(packet, binary.LittleEndian.Uint32(wire[140:144]))
				c.expireOutstanding(time.Now().Add(2 * repairMaxRequestTimeout))
			}
			if mode == "bad-signature" {
				packet[0] ^= 1
			}
			if mode == "wrong-version" {
				r.SetShredVersion(8)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- r.readPackets(ctx, conn, mode == "late-repair") }()
			t.Cleanup(func() {
				cancel()
				if err := conn.Close(); err != nil {
					t.Error(err)
				}
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(time.Second):
					t.Error("receiver reader did not join")
				}
			})
			accepted := mode == "broadcast" || mode == "late-repair"
			sent := cancelDuringRepairSend(t, func() int { return send(3) }, func() {
				if _, err := peerSocket.WriteToUDP(packet, conn.LocalAddr().(*net.UDPAddr)); err != nil {
					t.Error(err)
					return
				}
				deadline := time.Now().Add(time.Second)
				for time.Now().Before(deadline) {
					detail, held := r.HeadShredDetail(93)
					stats := r.Stats()
					if accepted && held && detail.DataShreds == 1 {
						return // Assembly follows the actual admission cancellation.
					}
					if !accepted && stats.SignatureErrors+stats.ShredVersionMismatch+stats.MissingLeaders == 1 {
						return
					}
					time.Sleep(time.Millisecond)
				}
				t.Error("incoming packet did not finish the expected admission/rejection path")
			})
			c.expireOutstanding(time.Now().Add(2 * repairMaxRequestTimeout))
			stats := r.Stats()
			detail, held := r.HeadShredDetail(93)
			if accepted {
				if !held || detail.DataShreds != 1 || stats.SigVerifies != 1 || stats.SignatureErrors != 0 || stats.AssemblyErrors != 0 {
					t.Fatalf("valid shred was not verified and assembled: held=%v detail=%+v stats=%+v", held, detail, stats)
				}
				if mode == "late-repair" && (stats.Repair.LateResponses != 1 || stats.UsefulRepairShreds != 1 || stats.RepairSocketUnmatched != 0) {
					t.Fatalf("late response was not matched and admitted: %+v", stats)
				}
			} else if held || sent != 1 {
				t.Fatalf("rejected incoming shred admitted or retired valid repair: held=%v sent=%d", held, sent)
			}
			c.mu.Lock()
			charges := 0
			for _, record := range c.perPeer {
				charges += record.inflight
			}
			outstanding, inflight := len(c.outstanding), len(c.inflight)
			c.mu.Unlock()
			t.Logf("mode=%s accepted=%v signature_verifies=%d sent_during_gate=%d outstanding=%d inflight=%d peer_charges=%d", mode, accepted, stats.SigVerifies, sent, outstanding, inflight, charges)
			if charges != 0 || outstanding != 0 || inflight != 0 || (accepted && sent != 0) {
				t.Fatalf("incoming admission resurrected cancelled repair: sent=%d outstanding=%d inflight=%d peer_charges=%d", sent, outstanding, inflight, charges)
			}
		})
	}
}
