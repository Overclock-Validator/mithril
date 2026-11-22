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

func TestSigverifyInFlightBatches(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		p, _ := Start(context.Background(), Config{SigverifyWorkers: 1, DedupCacheCapacity: 16})
		defer p.Stop()
		if p.sigverifyInFlight != nil {
			t.Fatal("disabled pipeline allocated a diagnostic counter")
		}
		if !p.Submit(packet.Owned(mustValidTestWire(t))) {
			t.Fatal("disabled pipeline refused a valid packet")
		}
		p.Stop()
		if p.Stats().Sink.InPackets != 1 {
			t.Fatal("disabled instrumentation changed delivery")
		}
	})
	t.Run("blocked_handoff_drain_and_replacement", func(t *testing.T) {
		recv := &sourceSink{hold: make(chan struct{})}
		p, _ := Start(context.Background(), Config{ObserveQueues: true, ObserveWorkerPasses: true, SigverifyWorkers: 1,
			IngressCap: 16, DedupOutCap: 16, VerifiedCap: 1, DedupCacheCapacity: 16, Sink: recv})
		released := false
		defer func() {
			if !released {
				close(recv.hold)
			}
			p.Stop()
		}()
		zero, ok := p.ObserveQueues()
		if !ok || zero.SigverifyWorkers != 1 || zero.SigverifyInFlight != 0 {
			t.Fatalf("idle worker gauge: %+v", zero)
		}
		for seq := range uint64(4) {
			if !p.Submit(packet.Owned(txfixture.MustSignedTransferWire(seq))) {
				t.Fatal("spacious test ingress refused a packet")
			}
		}
		deadline := time.Now().Add(2 * time.Second)
		for p.Stats().Sigverify.VerifiedPackets < 3 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		held, ok := p.ObserveQueues()
		if !ok || p.Stats().Sigverify.VerifiedPackets < 3 || held.Verified.Length != 1 || held.SigverifyInFlight != 1 || held.WorkerPassCount != 1 || held.WorkerPasses[0].Active <= 0 {
			t.Fatalf("worker blocked on output was not in flight: %+v", held)
		}
		again, ok := p.ObserveQueues()
		if !ok || again.SigverifyInFlight != 1 || again.Verified != held.Verified || again.WorkerPassCount != 1 || again.WorkerPasses[0].Active < held.WorkerPasses[0].Active {
			t.Fatal("sampling changed blocked work")
		}
		close(recv.hold)
		released = true
		deadline = time.Now().Add(2 * time.Second)
		for (len(recv.received()) != 4 || p.sigverifyInFlight.Load() != 0) && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		drained, ok := p.ObserveQueues()
		if !ok || len(recv.received()) != 4 || drained.SigverifyInFlight != 0 || drained.Verified.Length != 0 {
			t.Fatalf("drained worker gauge/delivery: %+v", drained)
		}
		p.Stop()
		if _, ok := p.ObserveQueues(); ok {
			t.Fatal("stopped pipeline reported a worker sample")
		}
		next, _ := Start(context.Background(), Config{ObserveQueues: true, SigverifyWorkers: 2, DedupCacheCapacity: 16})
		defer next.Stop()
		fresh, ok := next.ObserveQueues()
		if !ok || fresh.SigverifyWorkers != 2 || fresh.SigverifyInFlight != 0 || fresh.StartedAt.Equal(zero.StartedAt) {
			t.Fatalf("replacement carried old worker state: %+v", fresh)
		}
	})
}

func TestWorkerPassObservationOptInAndReplacement(t *testing.T) {
	off, _ := Start(context.Background(), Config{ObserveQueues: true, SigverifyWorkers: 1, DedupCacheCapacity: 16})
	if v, ok := off.ObserveQueues(); !ok || v.WorkerPassCount != 0 || off.workerPasses != nil {
		t.Fatal("worker timing must be separately opted in")
	}
	off.Stop()
	p, _ := Start(context.Background(), Config{ObserveQueues: true, ObserveWorkerPasses: true, SigverifyWorkers: 1, DedupCacheCapacity: 16})
	v, ok := p.ObserveQueues()
	if !ok || v.WorkerPassCount != 1 || v.WorkerPasses[0].Index != 0 || v.WorkerPasses[0].Elapsed <= 0 || v.WorkerPasses[0].Active > v.WorkerPasses[0].Elapsed {
		t.Fatalf("invalid worker observation: %+v", v)
	}
	p.Stop()
	if _, ok := p.ObserveQueues(); ok {
		t.Fatal("stopped pipeline remained observable")
	}
	next, _ := Start(context.Background(), Config{ObserveQueues: true, ObserveWorkerPasses: true, SigverifyWorkers: 1, DedupCacheCapacity: 16})
	defer next.Stop()
	nv, ok := next.ObserveQueues()
	if !ok || nv.WorkerPassCount != 1 || !nv.StartedAt.After(v.StartedAt) {
		t.Fatal("replacement did not obtain its own lifetime")
	}
}
