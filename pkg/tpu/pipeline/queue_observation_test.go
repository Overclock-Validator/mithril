package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
)

func TestQueuePressureNativeBounds(t *testing.T) {
	var absent *Pipeline
	if _, ok := absent.ObserveQueues(); ok {
		t.Fatal("nil pipeline was observed")
	}

	t.Run("disabled", func(t *testing.T) {
		p, _ := Start(context.Background(), Config{SigverifyWorkers: 1, DedupCacheCapacity: 16})
		defer p.Stop()
		if v, ok := p.ObserveQueues(); ok || v != (QueueObservation{}) || p.dedupOut != nil || p.verified != nil || !p.queueStartedAt.IsZero() {
			t.Fatal("default-off observation retained channels or a clock")
		}
		if !p.Submit(packet.Owned(mustValidTestWire(t))) {
			t.Fatal("disabled pipeline refused valid packet")
		}
		p.Stop()
		if p.Stats().Sink.InPackets != 1 {
			t.Fatal("disabled pipeline did not deliver packet")
		}
	})

	t.Run("existing_channels_full_drain_stop", func(t *testing.T) {
		recv := &sourceSink{hold: make(chan struct{})}
		p, ingress := Start(context.Background(), Config{ObserveQueues: true, SigverifyWorkers: 1,
			IngressCap: 2, DedupOutCap: 3, VerifiedCap: 4, DedupCacheCapacity: 256, Sink: recv})
		released := false
		defer func() {
			if !released {
				close(recv.hold)
			}
			p.Stop()
		}()
		zero, ok := p.ObserveQueues()
		if !ok || ingress != p.ingress || zero.Ingress != (QueueGauge{Capacity: 2}) ||
			zero.DedupOutput != (QueueGauge{Capacity: 3}) || zero.Verified != (QueueGauge{Capacity: 4}) || zero.StartedAt.IsZero() {
			t.Fatalf("original channel capacities/zero: %+v", zero)
		}
		accepted := 0
		var full QueueObservation
		for seq := range uint64(128) {
			if p.Submit(packet.Owned(txfixture.MustSignedTransferWire(seq))) {
				accepted++
			}
			time.Sleep(time.Millisecond)
			v, ok := p.ObserveQueues()
			if !ok {
				t.Fatal("running pipeline lost observation")
			}
			if v.Ingress.Length == 2 && v.DedupOutput.Length == 3 && v.Verified.Length == 4 {
				full = v
				break
			}
		}
		if full.Ingress != (QueueGauge{Length: 2, Capacity: 2}) || full.DedupOutput != (QueueGauge{Length: 3, Capacity: 3}) ||
			full.Verified != (QueueGauge{Length: 4, Capacity: 4}) || !full.StartedAt.Equal(zero.StartedAt) || full.SampledAt.Before(full.StartedAt) {
			t.Fatalf("actual held stage queues did not fill: %+v", full)
		}
		// With every downstream stage blocked, observation cannot consume work.
		again, ok := p.ObserveQueues()
		if !ok || again.Ingress != full.Ingress || again.DedupOutput != full.DedupOutput || again.Verified != full.Verified {
			t.Fatal("observing queues changed their contents")
		}
		close(recv.hold)
		released = true
		deadline := time.Now().Add(2 * time.Second)
		for len(recv.received()) != accepted && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		drained, ok := p.ObserveQueues()
		if !ok || len(recv.received()) != accepted || drained.Ingress.Length != 0 || drained.DedupOutput.Length != 0 || drained.Verified.Length != 0 {
			t.Fatalf("admitted packets did not drain unchanged: %+v, admitted %d, delivered %d", drained, accepted, len(recv.received()))
		}
		p.Stop()
		if _, ok := p.ObserveQueues(); ok {
			t.Fatal("stopped pipeline reported fresh gauges")
		}
		next, _ := Start(context.Background(), Config{ObserveQueues: true, SigverifyWorkers: 1, DedupCacheCapacity: 16})
		defer next.Stop()
		v, ok := next.ObserveQueues()
		if !ok || v.Ingress != (QueueGauge{Capacity: 4096}) || v.DedupOutput != (QueueGauge{Capacity: 4096}) || v.Verified != (QueueGauge{Capacity: 4096}) {
			t.Fatalf("replacement inherited old queue state: %+v", v)
		}
	})
}
