package turbine

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type mutableTVUPeers struct {
	peers []gossip.TVUPeer
}

func (m *mutableTVUPeers) TVUPeers() []gossip.TVUPeer { return m.peers }
func (m *mutableTVUPeers) SelfTVUAddr() *net.UDPAddr  { return nil }
func (m *mutableTVUPeers) LookupTVU(pubkey solana.PublicKey) (*net.UDPAddr, bool) {
	for _, peer := range m.peers {
		if solana.PublicKey(peer.Pubkey) == pubkey {
			return peer.TVUAddr, true
		}
	}
	return nil, false
}

func TestShredIDSeedMatchesAgaveLayout(t *testing.T) {
	leader := solana.PublicKey{1}
	id := ShredID{Slot: 42, Index: 7, Type: ShredTypeData}
	seed := id.Seed(leader)
	require.NotEqual(t, [32]byte{}, seed)

	id2 := ShredID{Slot: 42, Index: 7, Type: ShredTypeCode}
	require.NotEqual(t, id.Seed(leader), id2.Seed(leader))
}

func TestBroadcastClusterNodesDeterministicRoot(t *testing.T) {
	self := solana.MustPublicKeyFromBase58("11111111111111111111111111111112")
	peerA := solana.MustPublicKeyFromBase58("11111111111111111111111111111113")
	peerB := solana.MustPublicKeyFromBase58("11111111111111111111111111111114")

	nodes := NewBroadcastClusterNodes(ClusterNodesConfig{
		Self: self,
		TVUPeers: []gossip.TVUPeer{
			{Pubkey: gossip.Pubkey(peerA), TVUAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 8001}},
			{Pubkey: gossip.Pubkey(peerB), TVUAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.2"), Port: 8001}},
		},
		Stakes: map[solana.PublicKey]uint64{
			peerA: 100,
			peerB: 50,
		},
		UseChaCha8: true,
	})

	shred := ShredID{Slot: 100, Index: 3, Type: ShredTypeData}
	addr1, ok := nodes.BroadcastPeer(shred)
	require.True(t, ok)
	require.NotNil(t, addr1)

	addr2, ok := nodes.BroadcastPeer(shred)
	require.True(t, ok)
	require.Equal(t, addr1.String(), addr2.String())
}

func TestBroadcastClusterNodesEqualStakePubkeysDescendLikeAgave(t *testing.T) {
	self := solana.PublicKey{1}
	lowPubkey := solana.PublicKey{2}
	highPubkey := solana.PublicKey{3}
	peers := []gossip.TVUPeer{
		{Pubkey: gossip.Pubkey(lowPubkey), TVUAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 8001}},
		{Pubkey: gossip.Pubkey(highPubkey), TVUAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.2"), Port: 8001}},
	}

	nodes := NewBroadcastClusterNodes(ClusterNodesConfig{
		Self:       self,
		TVUPeers:   peers,
		Stakes:     map[solana.PublicKey]uint64{lowPubkey: 100, highPubkey: 100},
		UseChaCha8: true,
	})

	require.Len(t, nodes.nodes, 3)
	require.Equal(t, highPubkey, nodes.nodes[0].pubkey)
	require.Equal(t, lowPubkey, nodes.nodes[1].pubkey)
	require.Equal(t, self, nodes.nodes[2].pubkey)
}

func TestTurbineBroadcasterHonorsDedupAddrs(t *testing.T) {
	self := solana.PublicKey{1}
	peerA := solana.PublicKey{2}
	peerB := solana.PublicKey{3}
	peers := &mutableTVUPeers{peers: []gossip.TVUPeer{
		{Pubkey: gossip.Pubkey(peerA), TVUAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 8001}},
		{Pubkey: gossip.Pubkey(peerB), TVUAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 8002}},
	}}
	stakes := func(uint64) map[solana.PublicKey]uint64 {
		return map[solana.PublicKey]uint64{peerA: 100, peerB: 50}
	}
	countRoutable := func(nodes *ClusterNodes) int {
		count := 0
		for _, node := range nodes.nodes {
			if node.tvuAddr != nil {
				count++
			}
		}
		return count
	}

	development, err := NewTurbineBroadcaster(TurbineBroadcasterConfig{
		Self: func() solana.PublicKey { return self }, Peers: peers, Stakes: stakes, DedupAddrs: false,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, development.Close()) })
	require.Equal(t, 2, countRoutable(development.clusterNodesForSlot(10)))

	publicCluster, err := NewTurbineBroadcaster(TurbineBroadcasterConfig{
		Self: func() solana.PublicKey { return self }, Peers: peers, Stakes: stakes, DedupAddrs: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, publicCluster.Close()) })
	require.Equal(t, 1, countRoutable(publicCluster.clusterNodesForSlot(10)))
}

func TestTurbineBroadcasterRefreshesPeersEachSlot(t *testing.T) {
	self := solana.PublicKey{1}
	peer := solana.PublicKey{2}
	source := &mutableTVUPeers{}
	broadcaster, err := NewTurbineBroadcaster(TurbineBroadcasterConfig{
		Self:  func() solana.PublicKey { return self },
		Peers: source,
		Stakes: func(uint64) map[solana.PublicKey]uint64 {
			return map[solana.PublicKey]uint64{peer: 1}
		},
	})
	require.NoError(t, err)
	defer broadcaster.Close()

	first := broadcaster.clusterNodesForSlot(10)
	_, ok := first.BroadcastPeer(ShredID{Slot: 10, Type: ShredTypeData})
	require.False(t, ok)

	source.peers = []gossip.TVUPeer{{
		Pubkey:  gossip.Pubkey(peer),
		TVUAddr: &net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 8001},
	}}
	// The current slot stays stable, while the next slot refreshes gossip.
	require.Same(t, first, broadcaster.clusterNodesForSlot(10))
	second := broadcaster.clusterNodesForSlot(11)
	addr, ok := second.BroadcastPeer(ShredID{Slot: 11, Type: ShredTypeData})
	require.True(t, ok, "nodes=%#v", second.nodes)
	require.Equal(t, "203.0.113.1:8001", addr.String())
}

func TestTurbineBroadcasterFailsWithoutRoute(t *testing.T) {
	broadcaster, err := NewTurbineBroadcaster(TurbineBroadcasterConfig{Self: func() solana.PublicKey { return solana.PublicKey{1} }})
	require.NoError(t, err)
	defer broadcaster.Close()
	packet := make([]byte, shredIndexOffset+5)
	packet[shredVariantOffset] = legacyDataVariant
	binary.LittleEndian.PutUint64(packet[shredSlotOffset:], 12)
	err = broadcaster.broadcastPacket(packet)
	require.ErrorContains(t, err, "no routable TVU destination")
}
