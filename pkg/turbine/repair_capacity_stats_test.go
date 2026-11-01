package turbine

import (
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

func TestRepairCapacityStats(t *testing.T) {
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
	c.setRateLimit(40)
	check := func(outstanding, charged, perPeerCap int) {
		t.Helper()
		st := c.stats()
		if st.Outstanding != outstanding || st.PeerInflight != charged || st.PerPeerCap != perPeerCap || st.Peers != 1 {
			t.Errorf("stats=%+v want outstanding=%d peer_inflight=%d per_peer_cap=%d peers=1", st, outstanding, charged, perPeerCap)
		}
		t.Logf("repair_outstanding=%d repair_peer_inflight=%d repair_per_peer_cap=%d", st.Outstanding, st.PeerInflight, st.PerPeerCap)
	}
	send := func() int {
		return c.sendTier(conn, peers, []SlotRepairRequest{{Slot: 50, MissingDataShreds: []uint32{7}}}, 1, nil, repairMinRequestTimeout)
	}
	check(0, 0, 31)
	if sent := cancelDuringRepairSend(t, send, func() {
		check(0, 1, 31) // Signing reserves capacity before registration.
		c.satisfyDataShred(&Shred{Slot: 50, Index: 7, Type: ShredTypeData})
	}); sent != 0 {
		t.Fatal("cancelled reservation sent a request")
	}
	check(0, 0, 31)
	if send() != 1 {
		t.Fatal("fresh request did not send")
	}
	check(1, 1, 31)
	c.expireOutstanding(time.Now().Add(2 * repairMaxRequestTimeout))
	check(0, 0, 31)
	c.setRateLimit(20)
	check(0, 0, 16)
}
