package turbine

import (
	"crypto/rand"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

type repairNonceGate struct {
	reader  io.Reader
	started chan struct{}
	release chan struct{}
}

func (g *repairNonceGate) Read(p []byte) (int, error) {
	if g.started != nil {
		started := g.started
		g.started = nil
		close(started)
		<-g.release
	}
	return g.reader.Read(p)
}

// The nonce read is after capacity reservation but before registration. Pause
// there to deterministically reproduce a broadcast/reset arriving while signing.
// These tests must not run in parallel because crypto/rand.Reader is global.
func cancelDuringRepairSend(t *testing.T, send func() int, cancel func()) int {
	t.Helper()
	started, release := make(chan struct{}), make(chan struct{})
	previous := rand.Reader
	rand.Reader = &repairNonceGate{reader: previous, started: started, release: release}
	defer func() { rand.Reader = previous }()
	done := make(chan int, 1)
	go func() { done <- send() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("sender did not reach nonce generation")
	}
	cancel()
	close(release)
	return <-done
}

func TestRepairReservationCancellation(t *testing.T) {
	for _, when := range []string{"before", "during-shred", "during-slot", "during-replaced", "during-highest", "after", "timeout", "send-error"} {
		t.Run(when, func(t *testing.T) {
			conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			c := newPacingTestClient(t)
			peers := []gossip.RepairPeer{{Addr: conn.LocalAddr().(*net.UDPAddr)}}
			cancel := func() { c.satisfyDataShred(&Shred{Slot: 50, Index: 7, Type: ShredTypeData}) }
			send := func() int {
				if when == "during-highest" {
					return c.sendTier(conn, peers, []SlotRepairRequest{{Slot: 50, NeedHighestDataShred: true}}, 3, nil, repairMinRequestTimeout)
				}
				return c.sendTier(conn, peers, []SlotRepairRequest{{Slot: 50, MissingDataShreds: []uint32{7}}}, 3, nil, repairMinRequestTimeout)
			}
			var sent int
			switch when {
			case "before":
				cancel()
				sent = send()
			case "during-shred":
				sent = cancelDuringRepairSend(t, send, cancel)
			case "during-slot", "during-highest":
				sent = cancelDuringRepairSend(t, send, func() { c.cancelSlotRequests(50) })
			case "during-replaced":
				sent = cancelDuringRepairSend(t, send, func() {
					cancel()
					if send() != 1 {
						t.Error("replacement request did not send")
					}
				})
			case "after":
				sent = send()
				cancel()
			case "timeout":
				sent = send()
			case "send-error":
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
				sent = send()
			}
			c.expireOutstanding(time.Now().Add(2 * repairMaxRequestTimeout))
			if len(c.outstanding) != 0 || len(c.inflight) != 0 {
				t.Fatal("requests remained after cancellation/expiry")
			}
			for _, peer := range c.perPeer {
				if peer.inflight != 0 {
					t.Fatalf("sent=%d outstanding=0 but leaked peer capacity=%d", sent, peer.inflight)
				}
			}
			if (when == "during-shred" || when == "during-slot" || when == "during-replaced" || when == "during-highest") && sent != 0 {
				t.Fatalf("cancelled reservation still sent %d requests", sent)
			}
			if when == "send-error" && (sent != 0 || c.errors.Load() != 1) {
				t.Fatal("send error was not accounted exactly once")
			}
		})
	}
}

func TestRepairCancellationDoesNotStallAfterRateBackoff(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	peers := []gossip.RepairPeer{{Addr: conn.LocalAddr().(*net.UDPAddr)}}
	c, err := newRepairClient(nil, func() []gossip.RepairPeer { return peers })
	if err != nil {
		t.Fatal(err)
	}
	// Small rates make the same adaptive-cap transition cheap to reproduce:
	// one peer's cap falls from 31 to 16 after the 40 -> 20 requests/s backoff.
	c.setRateLimit(40)
	for i := 0; i < 16; i++ {
		cancelDuringRepairSend(t, func() int {
			return c.sendTier(conn, peers, []SlotRepairRequest{{Slot: 50, MissingDataShreds: []uint32{7}}}, 3, nil, repairMinRequestTimeout)
		}, func() { c.satisfyDataShred(&Shred{Slot: 50, Index: 7, Type: ShredTypeData}) })
		c.expireOutstanding(time.Now().Add(2 * repairMaxRequestTimeout))
	}
	c.setRateLimit(20)
	a := NewSlotAssembler()
	a.PrioritizeRepairSlot(51)
	previous := c.requests.Load()
	c.repairOnce(conn, a)
	if c.requests.Load() == previous {
		t.Fatalf("repair stopped after rate backoff: requests=%d outstanding=%d", previous, len(c.outstanding))
	}
	packet := make([]byte, 256)
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := conn.ReadFromUDP(packet); err != nil {
		t.Fatalf("no repair UDP packet after backoff: %v", err)
	}
}
