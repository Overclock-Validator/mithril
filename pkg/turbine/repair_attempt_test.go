package turbine

import (
	"crypto/rand"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

// Production retry/accounting durations run on synctest's virtual clock.
// Requests still traverse the real sender and loopback UDP; no request or
// capacity maps are fabricated or edited by the fixture.
func sendLongLivedRepairForTest(t *testing.T, target int) (*repairClient, *net.UDPConn, []gossip.RepairPeer) {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	peers := []gossip.RepairPeer{{Addr: conn.LocalAddr().(*net.UDPAddr)}}
	c, err := newRepairClient(nil, func() []gossip.RepairPeer { return peers })
	if err != nil {
		t.Fatal(err)
	}
	peers[0].Pubkey = gossip.Pubkey(c.identity.PublicKey())
	pol := headPolicy(1)
	start := time.Now()
	for c.requests.Load() < uint64(target) && time.Since(start) < 5*time.Minute {
		c.expireOutstanding(time.Now())
		if !c.sendShredAttempt(conn, peers, repairRequestWindowIndex, 50, 7, pol, c.accountingTimeout()) {
			time.Sleep(repairScanInterval)
		}
		if len(c.byResponse) != len(c.outstanding) {
			t.Fatal("nonce collision invalidates the wrap fixture")
		}
	}
	sk := shredKey{kind: repairRequestWindowIndex, slot: 50, index: 7}
	inf := c.inflight[sk]
	if c.requests.Load() != uint64(target) || inf == nil || uint8(inf.nextAttempt) != uint8(target) {
		t.Fatalf("long-lived generation premise: requests=%d inflight=%+v target=%d", c.requests.Load(), inf, target)
	}
	t.Logf("real sends=%d virtual elapsed=%s outstanding=%d nextAttempt=%d", target, time.Since(start), len(c.outstanding), inf.nextAttempt)
	return c, conn, peers
}

type repairNoncePause struct {
	reader           io.Reader
	used             atomic.Bool
	started, release chan struct{}
}

func (g *repairNoncePause) Read(p []byte) (int, error) {
	if g.used.CompareAndSwap(false, true) {
		close(g.started)
		<-g.release
	}
	return g.reader.Read(p)
}

func TestRepairAttemptReuseDuringSigning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		peers[0].Pubkey = gossip.Pubkey(c.identity.PublicKey())
		previous := rand.Reader
		gate := &repairNoncePause{reader: previous, started: make(chan struct{}), release: make(chan struct{})}
		rand.Reader = gate
		defer func() { rand.Reader = previous }()
		done := make(chan bool, 1)
		// Response-driven followup uses bulk policy, concurrently with the
		// periodic head sender. Hold that first reservation before registration.
		go func() {
			done <- c.sendShredAttempt(conn, peers, repairRequestWindowIndex, 50, 7, bulkPolicy(), c.accountingTimeout())
		}()
		<-gate.started
		start := time.Now()
		for c.requests.Load() < 256 && time.Since(start) < 5*time.Minute {
			c.expireOutstanding(time.Now())
			if !c.sendShredAttempt(conn, peers, repairRequestWindowIndex, 50, 7, headPolicy(1), c.accountingTimeout()) {
				time.Sleep(repairScanInterval)
			}
		}
		// Release and join before any assertion can terminate the bubble.
		close(gate.release)
		sent := <-done
		if !sent || c.requests.Load() != 257 {
			t.Fatalf("pending-reservation premise: sent=%v requests=%d", sent, c.requests.Load())
		}
		t.Logf("pending signer released after %s; registered=%d peer_charges=%d", time.Since(start), len(c.outstanding), repairPeerCharges(c))
		time.Sleep(2 * repairMaxRequestTimeout)
		c.expireOutstanding(time.Now())
		if len(c.outstanding) != 0 || repairPeerCharges(c) != 0 || len(c.byResponse) != 0 {
			t.Errorf("byte attempt key reused while old reservation remained: outstanding=%d peer_inflight=%d response_keys=%d",
				len(c.outstanding), repairPeerCharges(c), len(c.byResponse))
		}
	})
}

func repairPeerCharges(c *repairClient) int {
	total := 0
	for _, rec := range c.perPeer {
		total += rec.inflight
	}
	return total
}

func TestRepairAttemptWrapCancellation(t *testing.T) {
	for _, target := range []int{255, 256, 257, 512, 513} {
		synctest.Test(t, func(t *testing.T) {
			c, _, _ := sendLongLivedRepairForTest(t, target)
			c.satisfyDataShred(&Shred{Type: ShredTypeData, Slot: 50, Index: 7})
			if len(c.outstanding) != 0 || repairPeerCharges(c) != 0 || len(c.byResponse) != 0 {
				t.Errorf("after admission-cancellation function: target=%d outstanding=%d peer_inflight=%d response_keys=%d; want all zero",
					target, len(c.outstanding), repairPeerCharges(c), len(c.byResponse))
			}
		})
	}
}

func TestRepairAttemptWrapPoisonsReplacement(t *testing.T) {
	for _, target := range []int{255, 256} {
		synctest.Test(t, func(t *testing.T) {
			c, conn, peers := sendLongLivedRepairForTest(t, target)
			c.satisfyDataShred(&Shred{Type: ShredTypeData, Slot: 50, Index: 7})
			// Exercise same-shred sends after admission cancellation. A complete
			// receiver ResetSlot also cancels all slot requests and is NOT this
			// interleaving. Observe deadlines without changing bookkeeping.
			deadline := time.Now().Add(repairMaxRequestTimeout)
			for _, req := range c.outstanding {
				if req.accountAt.Before(deadline) {
					deadline = req.accountAt
				}
			}
			if !c.sendShredAttempt(conn, peers, repairRequestWindowIndex, 50, 7, headPolicy(1), c.accountingTimeout()) {
				t.Fatal("replacement did not send")
			}
			time.Sleep(deadline.Sub(time.Now()))
			c.expireOutstanding(time.Now())
			c.sendShredAttempt(conn, peers, repairRequestWindowIndex, 50, 7, headPolicy(1), c.accountingTimeout())
			time.Sleep(2 * repairMaxRequestTimeout)
			c.expireOutstanding(time.Now())
			if len(c.outstanding) != 0 || repairPeerCharges(c) != 0 || len(c.byResponse) != 0 {
				t.Errorf("all accounting deadlines elapsed: target=%d outstanding=%d peer_inflight=%d response_keys=%d; want all zero",
					target, len(c.outstanding), repairPeerCharges(c), len(c.byResponse))
			}
		})
	}
}
