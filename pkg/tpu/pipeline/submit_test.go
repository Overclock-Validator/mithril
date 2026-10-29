package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/stretchr/testify/require"
)

// sourceSink records the source of every packet that reaches the sink. A
// non-nil hold blocks Receive until it is closed.
type sourceSink struct {
	mu      sync.Mutex
	sources []packet.Source
	hold    chan struct{}
}

func (s *sourceSink) Receive(pkt packet.Packet) {
	if s.hold != nil {
		<-s.hold
	}
	s.mu.Lock()
	s.sources = append(s.sources, pkt.Source())
	s.mu.Unlock()
	pkt.Release()
}

func (s *sourceSink) received() []packet.Source {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]packet.Source(nil), s.sources...)
}

func TestSubmitDeliversPacketWithItsSource(t *testing.T) {
	recv := &sourceSink{}
	p, _ := Start(context.Background(), Config{SigverifyWorkers: 1, Sink: recv})
	defer p.Stop()

	require.True(t, p.Submit(packet.OwnedFrom(mustValidTestWire(t), packet.SourceRelayer)))
	require.Eventually(t, func() bool { return len(recv.received()) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []packet.Source{packet.SourceRelayer}, recv.received(), "the source survives dedup and sigverify")

	stats := p.Stats()
	require.Equal(t, SubmitStats{Enqueued: 1}, stats.Submitted[packet.SourceRelayer])
	require.Equal(t, SubmitStats{}, stats.Submitted[packet.SourceQUIC])
	require.Equal(t, uint64(1), stats.Ingress.EnqueuedPackets)
}

func TestSubmitAfterStopReleasesAndReportsDrop(t *testing.T) {
	p, _ := Start(context.Background(), Config{SigverifyWorkers: 1})
	p.Stop()

	pool := packet.NewPool(1)
	buf, slot, ok := pool.Acquire()
	require.True(t, ok)
	copy(buf, mustValidTestWire(t))
	require.False(t, p.Submit(packet.FromPool(pool, buf, 1, slot)))
	_, _, ok = pool.Acquire()
	require.True(t, ok, "a dropped packet is released")
	require.Equal(t, uint64(1), p.Stats().Submitted[packet.SourceQUIC].DroppedClosed)
}

func TestSubmitConcurrentWithStopNeverPanics(t *testing.T) {
	wire := mustValidTestWire(t)
	for range 20 {
		p, _ := Start(context.Background(), Config{SigverifyWorkers: 1})
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 200 {
					p.Submit(packet.OwnedFrom(wire, packet.SourceRelayer))
				}
			}()
		}
		p.Stop()
		wg.Wait()
		stats := p.Stats().Submitted[packet.SourceRelayer]
		require.Equal(t, uint64(4*200), stats.Enqueued+stats.DroppedFull+stats.DroppedClosed)
	}
}

func TestSubmitDropsWhenIngressIsFull(t *testing.T) {
	recv := &sourceSink{hold: make(chan struct{})}
	p, _ := Start(context.Background(), Config{SigverifyWorkers: 1, IngressCap: 1, DedupOutCap: 1, VerifiedCap: 1, Sink: recv})
	var dropped bool
	// Distinct transactions pass dedup and back up behind the held sink until
	// ingress itself is full.
	for seq := range uint64(64) {
		if !p.Submit(packet.OwnedFrom(txfixture.MustSignedTransferWire(seq), packet.SourceRelayer)) {
			dropped = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(recv.hold)
	p.Stop()
	require.True(t, dropped, "a full ingress must refuse without blocking")
	stats := p.Stats()
	require.NotZero(t, stats.Submitted[packet.SourceRelayer].DroppedFull)
	require.NotZero(t, stats.Ingress.DroppedFull)
}
