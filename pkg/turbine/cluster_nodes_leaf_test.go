package turbine

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// referenceRetransmitPeers is retransmitPeersInto as it was before the leaf
// shortcuts: it always walks the shuffle until it finds this node.
func referenceRetransmitPeers(c *ClusterNodes, leader solana.PublicKey, shred ShredID, fanout int, dst []*net.UDPAddr) (uint8, []*net.UDPAddr, error) {
	if c == nil || fanout <= 0 {
		return maxTurbineHops - 1, nil, nil
	}
	if leader == c.selfPubkey {
		return 0, nil, ErrRetransmitLoopback
	}
	shuffle := c.borrowRetransmitShuffle(leader)
	defer c.releaseRetransmitShuffle(shuffle)
	rng := newTurbineRNG(shred.Seed(leader), c.useChaCha8)
	selfPos := -1
	for position := 0; ; position++ {
		index, ok := shuffle.Next(rng)
		if !ok {
			break
		}
		if c.nodes[index].pubkey == c.selfPubkey {
			selfPos = position
			break
		}
	}
	if selfPos < 0 {
		return maxTurbineHops - 1, nil, nil
	}
	offset := 0
	if selfPos > 0 {
		offset = (selfPos - 1) % fanout
	}
	anchor := selfPos - offset
	step := fanout
	if selfPos == 0 {
		step = 1
	}
	position := anchor*fanout + offset + 1
	peers := dst[:0]
	if dst == nil {
		peers = make([]*net.UDPAddr, 0, fanout)
	}
	shufflePosition := selfPos
	for range fanout {
		var index int
		for shufflePosition < position {
			var ok bool
			index, ok = shuffle.Next(rng)
			if !ok {
				return turbineRootDistance(selfPos, fanout), peers, nil
			}
			shufflePosition++
		}
		node := c.nodes[index]
		if node.hasContact {
			if addr, ok := broadcastTVUUDP(node.tvuAddr); ok {
				peers = append(peers, addr)
			}
		}
		position += step
	}
	return turbineRootDistance(selfPos, fanout), peers, nil
}

func leafTestKey(i int) solana.PublicKey {
	var key solana.PublicKey
	binary.LittleEndian.PutUint64(key[:8], uint64(i)+1)
	key[31] = 0x5a
	return key
}

// leafTestCluster builds a snapshot of total nodes, the first staked of which
// have stake. Node 0 is this node when selfStaked, else node staked (the first
// unstaked one), so self is unstaked whenever staked < total.
func leafTestCluster(total, staked int, selfStaked, broadcast bool, rng *rand.Rand) (*ClusterNodes, []solana.PublicKey) {
	keys := make([]solana.PublicKey, total)
	stakes := make(map[solana.PublicKey]uint64)
	peers := make([]gossip.TVUPeer, 0, total)
	for i := range keys {
		keys[i] = leafTestKey(i)
		if i < staked {
			stakes[keys[i]] = uint64(1 + rng.Intn(1_000_000))
		}
		// One staked node in eight is known by stake only, with no contact.
		if i < staked && i%8 == 7 {
			continue
		}
		peers = append(peers, gossip.TVUPeer{Pubkey: gossip.Pubkey(keys[i]), TVUAddr: &net.UDPAddr{IP: net.IPv4(10, byte(i>>16), byte(i>>8), byte(i)), Port: 8001}})
	}
	self := keys[0]
	if !selfStaked && staked < total {
		self = keys[staked]
	}
	cfg := ClusterNodesConfig{Self: self, SelfTVU: &net.UDPAddr{IP: net.IPv4(10, 255, 255, 254), Port: 8001}, TVUPeers: peers, Stakes: stakes, UseChaCha8: true}
	return newClusterNodesWithLeaf(cfg, broadcast, true), keys
}

func TestTurbineLeafBound(t *testing.T) {
	for _, fanout := range []int{1, 2, 3, 7, 200} {
		for nodes := 1; nodes <= 1300; nodes++ {
			leafFrom, distance, known := turbineLeafBound(nodes, fanout)
			require.Less(t, leafFrom, nodes)
			for position := 0; position < nodes; position++ {
				hasChild := turbineFirstChild(position, fanout) < nodes
				require.Equal(t, position < leafFrom, hasChild, "nodes %d fanout %d position %d", nodes, fanout, position)
				if known && position >= leafFrom {
					require.Equal(t, distance, turbineRootDistance(position, fanout), "nodes %d fanout %d position %d", nodes, fanout, position)
				}
			}
		}
	}
	// Mainnet shape: 200-wide tree, a few thousand nodes. Only the root and its
	// 200 children forward; everyone else is two hops out.
	leafFrom, distance, known := turbineLeafBound(3800, dataPlaneFanout)
	require.Equal(t, 201, leafFrom)
	require.Equal(t, uint8(2), distance)
	require.True(t, known)
}

func TestRetransmitPeersMatchReferenceWithLeafShortcuts(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	cases := []struct{ total, staked int }{
		{1, 0}, {1, 1}, {2, 0}, {2, 1}, {2, 2}, {5, 0}, {5, 2}, {9, 9}, {40, 3}, {203, 1}, {203, 2}, {204, 3},
		{450, 30}, {450, 0}, {450, 450}, {700, 250}, {1500, 665},
	}
	for _, tc := range cases {
		for _, fanout := range []int{1, 2, 3, 200} {
			for _, selfStaked := range []bool{false, true} {
				for _, broadcast := range []bool{false, true} {
					nodes, keys := leafTestCluster(tc.total, tc.staked, selfStaked, broadcast, rng)
					leaders := []solana.PublicKey{keys[0], keys[len(keys)-1], keys[len(keys)/2], leafTestKey(1 << 20), nodes.selfPubkey}
					name := fmt.Sprintf("total %d staked %d fanout %d selfStaked %v broadcast %v", tc.total, tc.staked, fanout, selfStaked, broadcast)
					for _, leader := range leaders {
						for i := range 40 {
							shred := ShredID{Slot: uint64(1000 + i/8), Index: uint32(rng.Intn(4096)), Type: []ShredType{ShredTypeData, ShredTypeCode}[i%2]}
							wantDistance, wantPeers, wantErr := referenceRetransmitPeers(nodes, leader, shred, fanout, nil)
							gotDistance, gotPeers, gotErr := nodes.RetransmitPeers(leader, shred, fanout)
							require.Equal(t, wantErr, gotErr, name)
							require.Equal(t, wantDistance, gotDistance, name)
							require.Equal(t, wantPeers == nil, gotPeers == nil, name)
							require.Equal(t, len(wantPeers), len(gotPeers), name)
							for j := range wantPeers {
								require.Same(t, wantPeers[j], gotPeers[j], name)
							}
							// Caller-owned storage: same result, in that storage.
							scratch := make([]*net.UDPAddr, 0, fanout)
							gotDistance, gotPeers, gotErr = nodes.retransmitPeersInto(leader, shred, fanout, scratch)
							require.Equal(t, wantErr, gotErr, name)
							require.Equal(t, wantDistance, gotDistance, name)
							require.Equal(t, len(wantPeers), len(gotPeers), name)
						}
					}
				}
			}
		}
	}
}

func TestUnstakedRetransmitNodeSkipsShuffle(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	nodes, keys := leafTestCluster(3800, 665, false, false, rng)
	require.Equal(t, 664, nodes.selfMinPosition)
	leafFrom, _, known := turbineLeafBound(len(nodes.nodes), dataPlaneFanout)
	require.True(t, known)
	require.GreaterOrEqual(t, nodes.selfMinPosition, leafFrom)
	shred := ShredID{Slot: 77, Index: 12, Type: ShredTypeData}
	scratch := make([]*net.UDPAddr, 0, dataPlaneFanout)
	allocs := testing.AllocsPerRun(100, func() {
		distance, peers, err := nodes.retransmitPeersInto(keys[3], shred, dataPlaneFanout, scratch)
		if err != nil || distance != 2 || len(peers) != 0 {
			t.Fatalf("distance %d peers %d err %v", distance, len(peers), err)
		}
	})
	require.Zero(t, allocs)
	// A staked node in the same cluster is not skipped up front.
	staked, _ := leafTestCluster(3800, 665, true, false, rng)
	require.Zero(t, staked.selfMinPosition)
}

func BenchmarkRetransmitPeersUnstaked(b *testing.B) {
	rng := rand.New(rand.NewSource(3))
	nodes, keys := leafTestCluster(3800, 665, false, false, rng)
	scratch := make([]*net.UDPAddr, 0, dataPlaneFanout)
	b.Run("shortcut", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			_, _, _ = nodes.retransmitPeersInto(keys[i%600], ShredID{Slot: 9, Index: uint32(i), Type: ShredTypeData}, dataPlaneFanout, scratch)
		}
	})
	b.Run("reference", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			_, _, _ = referenceRetransmitPeers(nodes, keys[i%600], ShredID{Slot: 9, Index: uint32(i), Type: ShredTypeData}, dataPlaneFanout, scratch)
		}
	})
}

func BenchmarkRetransmitPeersSmallStake(b *testing.B) {
	rng := rand.New(rand.NewSource(3))
	nodes, keys := leafTestCluster(3800, 665, true, false, rng)
	scratch := make([]*net.UDPAddr, 0, dataPlaneFanout)
	b.Run("shortcut", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			_, _, _ = nodes.retransmitPeersInto(keys[1+i%600], ShredID{Slot: 9, Index: uint32(i), Type: ShredTypeData}, dataPlaneFanout, scratch)
		}
	})
	b.Run("reference", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			_, _, _ = referenceRetransmitPeers(nodes, keys[1+i%600], ShredID{Slot: 9, Index: uint32(i), Type: ShredTypeData}, dataPlaneFanout, scratch)
		}
	})
}
