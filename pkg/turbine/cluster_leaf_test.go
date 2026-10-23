package turbine

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func leafTestConfig(positive, zero int, chacha8 bool) ClusterNodesConfig {
	cfg := ClusterNodesConfig{Self: solana.PublicKey{0xff, 0xff}, Stakes: make(map[solana.PublicKey]uint64), UseChaCha8: chacha8}
	for i := range positive + zero {
		var key solana.PublicKey
		binary.LittleEndian.PutUint64(key[:], uint64(i+1))
		if i < positive {
			cfg.Stakes[key] = uint64(i + 1)
		}
		cfg.TVUPeers = append(cfg.TVUPeers, gossip.TVUPeer{Pubkey: gossip.Pubkey(key), TVUAddr: &net.UDPAddr{IP: net.IPv4(127, 1, byte(i/250), byte(i%250+1)), Port: 8001}})
	}
	return cfg
}

func TestUnstakedLeafMatchesShuffle(t *testing.T) {
	for _, chacha8 := range []bool{false, true} {
		for _, count := range []int{201, 202, 1000} {
			cfg := leafTestConfig(count, 100, chacha8)
			fast := NewRetransmitClusterNodes(cfg)
			reference := NewRetransmitClusterNodes(cfg)
			reference.unstakedLeaf = false
			require.Equal(t, count >= 202, fast.unstakedLeaf)
			leaders := []solana.PublicKey{{1}, {250}, {0xee, 0xee}} // positive, zero/positive, absent
			for i := range 500 {
				leader := leaders[i%len(leaders)]
				id := ShredID{Slot: uint64(i / 32), Index: uint32(i), Type: ShredTypeData}
				if i%2 != 0 {
					id.Type = ShredTypeCode
				}
				for _, fanout := range []int{dataPlaneFanout, 16} {
					wantDistance, wantPeers, wantErr := reference.RetransmitPeers(leader, id, fanout)
					distance, peers, err := fast.RetransmitPeers(leader, id, fanout)
					require.Equal(t, wantErr, err)
					require.Equal(t, wantDistance, distance)
					require.Equal(t, wantPeers, peers)
				}
			}
			_, _, err := fast.RetransmitPeers(cfg.Self, ShredID{}, dataPlaneFanout)
			require.ErrorIs(t, err, ErrRetransmitLoopback)
		}
	}
}

func TestUnstakedLeafEligibility(t *testing.T) {
	for _, tc := range []struct {
		name             string
		positive, zero   int
		selfStake, stake uint64
		want             bool
	}{
		{"boundary_below", 201, 3, 0, 0, false},
		{"boundary_at", 202, 3, 0, 0, true},
		{"staked", 202, 3, 1, 0, false},
		{"all_zero", 0, 1000, 0, 0, false},
		{"cluster_at", 202, 39997, 0, 0, true},
		{"cluster_above", 202, 39998, 0, 0, false},
		{"weight_overflow", 202, 3, 0, math.MaxUint64, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := leafTestConfig(tc.positive, tc.zero, true)
			cfg.Stakes[cfg.Self] = tc.selfStake
			if tc.stake != 0 {
				for key := range cfg.Stakes {
					if key != cfg.Self {
						cfg.Stakes[key] = tc.stake
					}
				}
			}
			require.Equal(t, tc.want, NewRetransmitClusterNodes(cfg).unstakedLeaf)
			require.False(t, NewBroadcastClusterNodes(cfg).unstakedLeaf)
		})
	}
}

func BenchmarkUnstakedLeafSelection(b *testing.B) {
	for _, count := range []int{202, 1000} {
		for _, shortcut := range []bool{false, true} {
			b.Run(fmt.Sprintf("positive_%d/shortcut_%v", count, shortcut), func(b *testing.B) {
				nodes := NewRetransmitClusterNodes(leafTestConfig(count, 1000, true))
				nodes.unstakedLeaf = shortcut
				storage := make([]*net.UDPAddr, 0, dataPlaneFanout)
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					distance, peers, err := nodes.retransmitPeersInto(solana.PublicKey{1}, ShredID{Slot: 100, Index: uint32(i), Type: ShredTypeData}, dataPlaneFanout, storage)
					if err != nil || distance != 2 || len(peers) != 0 {
						b.Fatal("leaf selection changed")
					}
				}
			})
		}
	}
}
