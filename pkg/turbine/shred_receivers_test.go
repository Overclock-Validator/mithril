package turbine

import (
	"context"
	"crypto/ed25519"
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func listenUDP(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func readPacket(t *testing.T, conn *net.UDPConn) []byte {
	t.Helper()
	buf := make([]byte, packetDataSize)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	n, _, err := conn.ReadFromUDP(buf)
	require.NoError(t, err)
	return buf[:n]
}

func TestTurbineBroadcasterSendsToShredReceivers(t *testing.T) {
	root, receiver := listenUDP(t), listenUDP(t)
	leaderPub, leaderKey := testIdentity(t)
	rootPub, _ := testIdentity(t)
	b, err := NewTurbineBroadcaster(TurbineBroadcasterConfig{
		Self:  leaderPub,
		Peers: &mutableTVUPeers{peers: []gossip.TVUPeer{{Pubkey: gossip.Pubkey(rootPub), TVUAddr: root.LocalAddr().(*net.UDPAddr)}}},
		Stakes: func(uint64) map[solana.PublicKey]uint64 {
			return map[solana.PublicKey]uint64{leaderPub: 1, rootPub: 1}
		},
		ShredReceivers: []*net.UDPAddr{receiver.LocalAddr().(*net.UDPAddr)},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })

	gen := ShredGenerator{Slot: 90, ParentSlot: 89, Version: 44, ReferenceTick: 63}
	packets, _, _, _, err := gen.MakeShredsFromData(solana.PrivateKey(leaderKey), []byte("leader block"), true, solana.Hash{9}, 0, 0)
	require.NoError(t, err)
	require.NoError(t, b.Broadcast(packets[:1]))
	assert.Equal(t, packets[0], readPacket(t, root), "the Turbine root")
	assert.Equal(t, packets[0], readPacket(t, receiver), "and the shred receiver")
}

// A relay sends each shred it retransmits to the shred receivers too, after
// its Turbine children, and a leaf with no children still does.
func TestRetransmitterSendsToShredReceivers(t *testing.T) {
	for name, position := range map[string]int{"relay": 1, "leaf": dataPlaneFanout + 1} {
		t.Run(name, func(t *testing.T) {
			leaderPub, leaderKey := testIdentity(t)
			const identityCount = dataPlaneFanout + 5
			identities := make(map[solana.PublicKey]ed25519.PrivateKey)
			allPeers := make([]gossip.TVUPeer, identityCount)
			stakes := map[solana.PublicKey]uint64{leaderPub: 1_000}
			for i := range allPeers {
				pubkey, private := testIdentity(t)
				identities[pubkey] = private
				allPeers[i] = gossip.TVUPeer{
					Pubkey:  gossip.Pubkey(pubkey),
					TVUAddr: &net.UDPAddr{IP: net.IPv4(127, 2, byte(i/250), byte(i%250+1)), Port: 9000 + i},
				}
				stakes[pubkey] = uint64(identityCount - i)
			}
			gen := ShredGenerator{Slot: 90, ParentSlot: 89, Version: 44, ReferenceTick: 63}
			packets, _, _, _, err := gen.MakeShredsFromData(solana.PrivateKey(leaderKey), []byte("relayed block"), true, solana.Hash{9}, 0, 0)
			require.NoError(t, err)
			packet := packets[0]
			shred, err := ParseShred(packet)
			require.NoError(t, err)
			id := ShredID{Slot: shred.Slot, Index: shred.Index, Type: shred.Type}

			nodes := NewRetransmitClusterNodes(ClusterNodesConfig{Self: solana.PublicKey(allPeers[0].Pubkey), TVUPeers: allPeers, Stakes: stakes, UseChaCha8: true})
			shuffle := nodes.retransmitShuffle(leaderPub, id)
			self := nodes.nodes[shuffle[position]].pubkey
			nodes.selfPubkey, nodes.selfIndex = self, shuffle[position]
			parent, ok, err := nodes.RetransmitParent(leaderPub, id, dataPlaneFanout)
			require.NoError(t, err)
			require.True(t, ok)
			if parent != leaderPub {
				merkleRoot, err := shred.MerkleRoot()
				require.NoError(t, err)
				offset, err := shred.retransmitterSignatureOffset()
				require.NoError(t, err)
				copy(packet[offset:offset+ed25519.SignatureSize], ed25519.Sign(identities[parent], merkleRoot[:]))
				shred, err = ParseShred(packet)
				require.NoError(t, err)
			}

			receiver := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8003}
			sender := newCaptureBatchSender()
			retransmitter, err := newRetransmitterWithSenders(RetransmitConfig{
				Identity:       identities[self],
				Peers:          &mutableTVUPeers{peers: allPeers},
				Stakes:         func(uint64) map[solana.PublicKey]uint64 { return stakes },
				UseChaCha8:     true,
				ShredReceivers: []*net.UDPAddr{receiver},
			}, []packetBatchSender{sender})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				retransmitter.Run(ctx)
				close(done)
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})

			require.NoError(t, retransmitter.Submit(packet, shred, leaderPub, false))
			require.Eventually(t, func() bool { return retransmitter.Stats().ShredReceiverSent == 1 }, time.Second, time.Millisecond)
			sent := sender.packets()
			last := sent[len(sent)-1]
			assert.Equal(t, []*net.UDPAddr{receiver}, last.peers, "the shred receiver comes after the children")
			if name == "relay" {
				require.Len(t, sent, 2)
				assert.NotEmpty(t, sent[0].peers)
				assert.Equal(t, sent[0].packet, last.packet, "the receiver gets what the children get")
			} else {
				assert.Len(t, sent, 1, "a leaf has no children")
			}

			// A duplicate is not forwarded again.
			require.NoError(t, retransmitter.Submit(packet, shred, leaderPub, false))
			time.Sleep(20 * time.Millisecond)
			assert.Equal(t, uint64(1), retransmitter.Stats().ShredReceiverSent)
		})
	}
}
