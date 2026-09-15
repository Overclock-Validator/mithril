package turbine

import (
	"bytes"
	"context"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Derive expected children from the full weighted permutation, independently
// of the streaming child-selection loop and its caller-owned output storage.
func expectedRelayPeers(nodes *ClusterNodes, leader solana.PublicKey, id ShredID, fanout int) (uint8, []*net.UDPAddr) {
	order := nodes.retransmitShuffle(leader, id)
	self := -1
	for i, index := range order {
		if nodes.nodes[index].pubkey == nodes.selfPubkey {
			self = i
			break
		}
	}
	if self < 0 {
		return maxTurbineHops - 1, nil
	}
	offset := 0
	if self > 0 {
		offset = (self - 1) % fanout
	}
	step := fanout
	if self == 0 {
		step = 1
	}
	var peers []*net.UDPAddr
	for position, n := (self-offset)*fanout+offset+1, 0; position < len(order) && n < fanout; position, n = position+step, n+1 {
		node := nodes.nodes[order[position]]
		if node.hasContact {
			if addr, ok := broadcastTVUUDP(node.tvuAddr); ok {
				peers = append(peers, addr)
			}
		}
	}
	return turbineRootDistance(self, fanout), peers
}

func TestRetransmitScratchMatchesPermutation(t *testing.T) {
	for _, chacha8 := range []bool{false, true} {
		for _, count := range []int{0, 31, 90, 512} {
			nodes, leader, _ := relayAllocationNodes(count, chacha8)
			// Exercise absent contacts and unroutable peers without changing stake order.
			for i := range nodes.nodes {
				if i%13 == 0 {
					nodes.nodes[i].hasContact = false
				}
				if i%17 == 0 {
					nodes.nodes[i].tvuAddr = &net.UDPAddr{IP: net.ParseIP("::1"), Port: 8001}
				}
			}
			for _, fanout := range []int{1, 3, 200} {
				var scratch [dataPlaneFanout]*net.UDPAddr
				for index := uint32(0); index < 40; index++ {
					id := ShredID{Slot: uint64(10 + index/4), Index: index, Type: ShredType(index % 2)}
					wantDistance, want := expectedRelayPeers(nodes, leader, id, fanout)
					distance, got, err := nodes.retransmitPeersInto(leader, id, fanout, scratch[:0])
					require.NoError(t, err)
					require.Equal(t, wantDistance, distance)
					require.Equal(t, len(want), len(got))
					for i := range want {
						require.Same(t, want[i], got[i])
					}
					_, owned, err := nodes.RetransmitPeers(leader, id, fanout)
					require.NoError(t, err)
					copyOfOwned := append([]*net.UDPAddr(nil), owned...)
					clear(scratch[:])
					require.Equal(t, copyOfOwned, append([]*net.UDPAddr(nil), owned...))
				}
			}
		}
	}
}

func TestRetransmitScratchConcurrent(t *testing.T) {
	nodes, leader, _ := relayAllocationNodes(300, true)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			var scratch [dataPlaneFanout]*net.UDPAddr
			for index := uint32(0); index < 50; index++ {
				id := ShredID{Slot: 10, Index: index, Type: ShredTypeData}
				wd, wp := expectedRelayPeers(nodes, leader, id, 3)
				d, p, err := nodes.retransmitPeersInto(leader, id, 3, scratch[:0])
				if err != nil || d != wd || len(p) != len(wp) {
					t.Error("concurrent routing mismatch")
					return
				}
				for i := range p {
					if p[i] != wp[i] {
						t.Error("concurrent peer mismatch")
						return
					}
				}
			}
		})
	}
	wg.Wait()
}

func TestRetransmitPacketPoolOwnership(t *testing.T) {
	r := &Retransmitter{}
	input := bytes.Repeat([]byte{7}, packetDataSize)
	a, ownerA := r.copyPacket(input)
	b, ownerB := r.copyPacket(input)
	require.NotSame(t, ownerA, ownerB)
	clear(input)
	require.Equal(t, byte(7), a[0])
	require.Equal(t, byte(7), b[0])
	r.releasePacket(ownerA)
	for range 50 {
		p, owner := r.copyPacket(bytes.Repeat([]byte{9}, 1203))
		clear(p)
		r.releasePacket(owner)
	}
	require.Equal(t, bytes.Repeat([]byte{7}, packetDataSize), b)
	r.releasePacket(ownerB)
	oversized := bytes.Repeat([]byte{4}, packetDataSize+1)
	p, owner := r.copyPacket(oversized)
	require.Nil(t, owner)
	clear(oversized)
	require.Equal(t, byte(4), p[0])
}

func TestRetransmitQueuedPacketOwnsInput(t *testing.T) {
	nodes, leader, key := relayAllocationNodes(90, true)
	r, err := newRetransmitterWithSenders(RetransmitConfig{Identity: key, Peers: &mutableTVUPeers{}, Stakes: func(uint64) map[solana.PublicKey]uint64 { return nil }, QueueDepth: 1}, []packetBatchSender{newCaptureBatchSender()})
	require.NoError(t, err)
	r.cache[10] = cachedRetransmitNodes{asof: time.Now(), nodes: nodes}
	packet := bytes.Repeat([]byte{3}, dataPayloadSize)
	packet[0] = 1
	shred := &Shred{Slot: 10, Index: 1, Type: ShredTypeData, Payload: packet}
	require.NoError(t, r.Submit(packet, shred, leader, false))
	want := append([]byte(nil), packet...)
	for i := 2; i < 20; i++ {
		packet[0] = byte(i)
		shred.Index = uint32(i)
		require.NoError(t, r.Submit(packet, shred, leader, false))
	}
	require.Equal(t, uint64(18), r.queueDrops.Load())
	clear(packet)
	work := <-r.queue
	require.Equal(t, want, work.packet)
	// Cancellation leaves no owned copies queued after workers finish.
	r.queue <- work
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.Run(ctx)
	require.Empty(t, r.queue)
	packet[0] = 99
	shred.Index = 99
	require.NoError(t, r.Submit(packet, shred, leader, false))
	require.Empty(t, r.queue, "late submit retained storage after workers stopped")
}

type borrowedRelaySender struct {
	t       *testing.T
	want    []byte
	entered chan struct{}
	resume  chan struct{}
	calls   int
}

func (s *borrowedRelaySender) Send(packet []byte, peers []*net.UDPAddr) (int, error) {
	s.calls++
	if s.calls == 1 {
		close(s.entered)
		<-s.resume
	}
	require.Equal(s.t, s.want, packet)
	if s.calls == 1 {
		return 0, syscall.EAGAIN
	}
	return len(peers), nil
}
func (*borrowedRelaySender) Close() error { return nil }

func TestRetransmitPoolLeaseSurvivesSendRetries(t *testing.T) {
	nodes, leader, key := relayAllocationNodes(31, true)
	id := ShredID{Slot: 10, Type: ShredTypeData}
	// Select a shred for which this validator is the root and has children.
	for ; id.Index < 10000; id.Index++ {
		d, p, err := nodes.RetransmitPeers(leader, id, 200)
		require.NoError(t, err)
		if d == 0 && len(p) > 1 {
			break
		}
	}
	require.Less(t, id.Index, uint32(10000))
	want := bytes.Repeat([]byte{7}, dataPayloadSize)
	sender := &borrowedRelaySender{t: t, want: want, entered: make(chan struct{}), resume: make(chan struct{})}
	r, err := newRetransmitterWithSenders(RetransmitConfig{Identity: key, Peers: &mutableTVUPeers{}, Stakes: func(uint64) map[solana.PublicKey]uint64 { return nil }}, []packetBatchSender{sender})
	require.NoError(t, err)
	r.cache[10] = cachedRetransmitNodes{asof: time.Now(), nodes: nodes}
	packet, owner := r.copyPacket(want)
	var scratch [dataPlaneFanout]*net.UDPAddr
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.send(retransmitWork{packet: packet, storage: owner, shred: id, leader: leader}, sender, scratch[:0])
	}()
	select {
	case <-sender.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("send did not start")
	}
	for range 100 {
		p, owned := r.copyPacket(want)
		clear(p)
		r.releasePacket(owned)
	}
	close(sender.resume)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("send did not finish")
	}
	require.Equal(t, 2, sender.calls)
	for _, addr := range scratch {
		require.Nil(t, addr, "scratch pinned prior snapshot")
	}
}

func TestRetransmitConcurrentSubmitAndStop(t *testing.T) {
	nodes, leader, key := relayAllocationNodes(90, true)
	r, err := newRetransmitterWithSenders(RetransmitConfig{Identity: key, Peers: &mutableTVUPeers{}, Stakes: func(uint64) map[solana.PublicKey]uint64 { return nil }, QueueDepth: 8}, []packetBatchSender{&discardRelaySender{}, &discardRelaySender{}})
	require.NoError(t, err)
	r.cache[10] = cachedRetransmitNodes{asof: time.Now(), nodes: nodes}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Go(func() {
			packet := make([]byte, dataPayloadSize)
			for i := 0; i < 100; i++ {
				packet[0], packet[1] = byte(worker), byte(i)
				shred := &Shred{Slot: 10, Index: uint32(worker*100 + i), Type: ShredTypeData, Payload: packet}
				if err := r.Submit(packet, shred, leader, false); err != nil {
					t.Error(err)
				}
				if i == 50 {
					cancel()
				}
			}
		})
	}
	wg.Wait()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown blocked")
	}
	require.Empty(t, r.queue)
}
