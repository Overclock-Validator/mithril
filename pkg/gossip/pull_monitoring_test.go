package gossip

import (
	"math"
	"net"
	"testing"
	"time"
)

func payloadTestSocket(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestPullRequestWriteObservation(t *testing.T) {
	peer, conn := payloadTestSocket(t), payloadTestSocket(t)
	c := &Client{}
	addr := peer.LocalAddr().(*net.UDPAddr)
	c.recordPullRequestWrite(sendUDP(conn, []byte{1}, addr, nil))
	if got := c.Stats(); got.TxPullRequests != 1 || got.TxPackets != 1 || got.TxErrors != 0 {
		t.Fatalf("successful local write: %+v", got)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	c.recordPullRequestWrite(sendUDP(conn, []byte{1}, addr, nil))
	if got := c.Stats(); got.TxPullRequests != 1 || got.TxPackets != 1 || got.TxErrors != 1 {
		t.Fatalf("failed local write: %+v", got)
	}
	c.txPullRequests.Store(math.MaxUint64 - 1)
	c.recordPullRequestWrite(nil)
	c.recordPullRequestWrite(nil)
	if got := c.Stats().TxPullRequests; got != math.MaxUint64 {
		t.Fatalf("counter wrapped after saturation: %d", got)
	}
}

func TestPullRequestPacketLoopCountsActualWrites(t *testing.T) {
	peer, conn := payloadTestSocket(t), payloadTestSocket(t)
	addr := peer.LocalAddr().(*net.UDPAddr)
	c := &Client{}
	peers := []*net.UDPAddr{addr, nil, addr}
	c.sendPullRequestPacket(conn, []byte{1}, peers, 0, nil)
	if got := c.Stats(); got.TxPullRequests != 2 || got.TxPackets != 2 || got.TxErrors != 1 {
		t.Fatalf("mixed writes were miscounted: %+v", got)
	}
	many := make([]*net.UDPAddr, maxPullRequestsPerTick+2)
	receivers := make([]*net.UDPConn, len(many))
	for i := range many {
		receivers[i] = payloadTestSocket(t)
		many[i] = receivers[i].LocalAddr().(*net.UDPAddr)
	}
	c.sendPullRequestPacket(conn, []byte{2}, many, 9, nil)
	c.sendPullRequestPacket(conn, []byte{3}, nil, 0, nil)
	if got := c.Stats(); got.TxPullRequests != 10 || got.TxPackets != 10 || got.TxErrors != 1 {
		t.Fatalf("bounded or empty peer window was miscounted: %+v", got)
	}
	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var buf [4]byte
	for i := 0; i < 2; i++ {
		if n, _, err := peer.ReadFromUDP(buf[:]); err != nil || n != 1 {
			t.Fatalf("received %d/%d: %v", i, n, err)
		}
	}
	for i, receiver := range receivers {
		if err := receiver.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		n, _, err := receiver.ReadFromUDP(buf[:])
		if i == 7 || i == 8 {
			if e, ok := err.(net.Error); !ok || !e.Timeout() {
				t.Fatalf("out-of-window peer %d: bytes=%d err=%v", i, n, err)
			}
		} else if err != nil || n != 1 || buf[0] != 2 {
			t.Fatalf("window peer %d received %d bytes: %v", i, n, err)
		}
	}
}

func TestPullRequestUninitializedContactDoesNotCountWrite(t *testing.T) {
	c := &Client{}
	if err := c.sendPullRequests(nil, nil); err == nil {
		t.Fatal("uninitialized contact was accepted")
	}
	if got := c.Stats(); got.TxPullRequests != 0 || got.TxPackets != 0 || got.TxErrors != 0 {
		t.Fatalf("pre-send failure changed write counters: %+v", got)
	}
}
