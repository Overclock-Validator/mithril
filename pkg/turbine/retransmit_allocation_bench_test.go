package turbine

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
)

type discardRelaySender struct{}

func (*discardRelaySender) Send(_ []byte, peers []*net.UDPAddr) (int, error) { return len(peers), nil }
func (*discardRelaySender) Close() error                                     { return nil }

// Includes Submit's dedupe/copy, channel handoff, routing and sender dispatch.
// The sender performs no syscalls; this isolates relay CPU/allocations rather
// than claiming a network-throughput improvement. Authentication before Submit
// and resigned-shred crypto are covered by functional tests, not this fixture.
func BenchmarkRetransmitPipeline(b *testing.B) {
	for _, count := range []int{90, 512} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			nodes, leader, key := relayAllocationNodes(count, true)
			r, err := newRetransmitterWithSenders(RetransmitConfig{Identity: key, Peers: &mutableTVUPeers{}, Stakes: func(uint64) map[solana.PublicKey]uint64 { return nil }, QueueDepth: 256}, []packetBatchSender{&discardRelaySender{}})
			if err != nil {
				b.Fatal(err)
			}
			r.cache[10] = cachedRetransmitNodes{asof: time.Now().Add(time.Hour), nodes: nodes}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { r.Run(ctx); close(done) }()
			packet := make([]byte, dataPayloadSize)
			shred := &Shred{Slot: 10, Type: ShredTypeData, Payload: packet}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Exactly one producer; leave capacity before submitting so every
				// iteration measures actual forwarding rather than dropped work.
				for len(r.queue) == cap(r.queue) {
					runtime.Gosched()
				}
				binary.LittleEndian.PutUint64(packet[:8], uint64(i))
				shred.Index = uint32(i)
				if err := r.Submit(packet, shred, leader, false); err != nil {
					b.Fatal(err)
				}
			}
			for {
				var n uint64
				for i := range r.rootDistance {
					n += r.rootDistance[i].Load()
				}
				if n >= uint64(b.N) {
					break
				}
				runtime.Gosched()
			}
			cancel()
			<-done
			b.StopTimer()
			if r.queueDrops.Load() != 0 {
				b.Fatal("unexpected queue drop")
			}
		})
	}
}

func relayAllocationNodes(count int, chacha8 bool) (*ClusterNodes, solana.PublicKey, ed25519.PrivateKey) {
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	var self solana.PublicKey
	copy(self[:], key.Public().(ed25519.PublicKey))
	leader := solana.PublicKey{255}
	peers := make([]gossip.TVUPeer, 0, count)
	stakes := map[solana.PublicKey]uint64{self: 100, leader: 50}
	for i := 0; i < count; i++ {
		var pub solana.PublicKey
		binary.LittleEndian.PutUint64(pub[:], uint64(i+1))
		addr := &net.UDPAddr{IP: net.IPv4(127, 1, byte(i/250), byte(i%250+1)), Port: 8001}
		peers = append(peers, gossip.TVUPeer{Pubkey: gossip.Pubkey(pub), TVUAddr: addr})
		stakes[pub] = uint64(i % 101)
	}
	return NewRetransmitClusterNodes(ClusterNodesConfig{Self: self, TVUPeers: peers, Stakes: stakes, UseChaCha8: chacha8}), leader, key
}
