package gossip

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRepairPeerForAddr(t *testing.T) {
	now := time.Now()
	ipv4 := &net.UDPAddr{IP: net.IP{192, 0, 2, 7}, Port: 8008}
	ipv6 := &net.UDPAddr{IP: net.ParseIP("fe80::7"), Port: 8008, Zone: "eth0"}
	c := &Client{repairPeers: map[Pubkey]RepairPeer{
		{1}: {Pubkey: Pubkey{1}, Addr: ipv4, LastSeen: now},
		{2}: {Pubkey: Pubkey{2}, Addr: ipv4, LastSeen: now}, // duplicate endpoints remain valid
		{3}: {Pubkey: Pubkey{3}, Addr: ipv4, LastSeen: now.Add(-2 * peerExpirationWindow)},
		{4}: {Pubkey: Pubkey{4}, Addr: ipv6, LastSeen: now},
		{5}: {Pubkey: Pubkey{5}, Addr: nil, LastSeen: now},
	}}
	for i := 0; i < 20; i++ {
		key, ok := c.RepairPeerForAddr(&net.UDPAddr{IP: net.ParseIP("192.0.2.7"), Port: 8008})
		require.True(t, ok)
		require.Contains(t, []Pubkey{{1}, {2}}, key)
	}
	require.NotContains(t, c.repairPeers, Pubkey{3}, "expired matches are pruned")
	key, ok := c.RepairPeerForAddr(ipv6)
	require.True(t, ok)
	require.Equal(t, Pubkey{4}, key)
	for _, addr := range []*net.UDPAddr{
		nil,
		{IP: ipv4.IP, Port: 8009},
		{IP: ipv4.IP, Port: 8008, Zone: "eth0"},
		{IP: ipv6.IP, Port: 8008, Zone: "eth1"},
		{IP: net.ParseIP("192.0.2.8"), Port: 8008},
	} {
		_, ok := c.RepairPeerForAddr(addr)
		require.False(t, ok, "%v", addr)
	}
	delete(c.repairPeers, Pubkey{1})
	delete(c.repairPeers, Pubkey{2})
	_, ok = c.RepairPeerForAddr(ipv4)
	require.False(t, ok, "expired peer must not become a fallback")
}

func BenchmarkRepairSourceLookup(b *testing.B) {
	c := &Client{repairPeers: make(map[Pubkey]RepairPeer)}
	for i := 0; i < 4096; i++ {
		var key Pubkey
		binary.LittleEndian.PutUint64(key[:8], uint64(i))
		c.repairPeers[key] = RepairPeer{Pubkey: key, LastSeen: time.Now(), Addr: &net.UDPAddr{
			IP: net.IP{10, byte(i >> 8), byte(i), 1}, Port: 8008,
		}}
	}
	addr := &net.UDPAddr{IP: net.IP{192, 0, 2, 1}, Port: 8008}
	b.Run("snapshot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for _, peer := range c.RepairPeers() {
				if peer.Addr.Port == addr.Port && peer.Addr.Zone == addr.Zone && peer.Addr.IP.Equal(addr.IP) {
					b.Fatal("unexpected match")
				}
			}
		}
	})
	b.Run("lookup", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, ok := c.RepairPeerForAddr(addr); ok {
				b.Fatal("unexpected match")
			}
		}
	})
}
