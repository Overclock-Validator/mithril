package pipeline

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/sigverifytelemetry"
	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/sink"
	"github.com/Overclock-Validator/mithril/pkg/tpu/wire"
)

func TestWireSanitizeAcceptsValidFixture(t *testing.T) {
	wireTx := mustValidTestWire(t)
	if _, err := wire.Sanitize(wireTx); err != nil {
		t.Fatalf("sanitize valid fixture: %v", err)
	}
}

func TestPipelineEndToEnd(t *testing.T) {
	wireTx := mustValidTestWire(t)
	ctx := context.Background()
	noop := &sink.Noop{}
	p, ingress := Start(ctx, Config{SigverifyWorkers: 2, Sink: noop})
	defer p.Stop()

	requireEnqueue(t, ingress, packet.Owned(wireTx))

	deadline := time.Now().Add(2 * time.Second)
	for noop.Snapshot().InPackets == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for sink, stats=%+v", p.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPipelineTelemetryPreservesFlow(t *testing.T) {
	wireTx := mustValidTestWire(t)
	sigverifytelemetry.Enable(16)
	t.Cleanup(sigverifytelemetry.Disable)
	noop := &sink.Noop{}
	p, ingress := Start(context.Background(), Config{SigverifyWorkers: 1, Sink: noop})
	defer p.Stop()

	requireEnqueue(t, ingress, packet.Owned(wireTx))
	deadline := time.Now().Add(2 * time.Second)
	for noop.Snapshot().InPackets == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for sink, stats=%+v", p.Stats())
		}
		time.Sleep(time.Millisecond)
	}

	s := sigverifytelemetry.Current()
	if s.Mode != sigverifytelemetry.CollectionPassive || s.Transactions != 1 || s.VerificationAttempts != 1 || s.QueueSamples != 1 || s.BatchSamples != 0 {
		t.Fatalf("telemetry snapshot = %+v", s)
	}
	if len(s.VerificationTrace) != 1 || s.VerificationTrace[0].Outcome != sigverifytelemetry.VerificationOutcomeValid {
		t.Fatalf("telemetry trace = %+v", s.VerificationTrace)
	}
	if len(s.DispatchTrace) != 1 || s.DispatchTrace[0].Mode != sigverifytelemetry.CollectionPassive || s.DispatchTrace[0].SignatureLanes != 1 || len(s.DispatchTrace[0].JobSignatures) != 1 || s.DispatchTrace[0].JobSignatures[0] != 1 {
		t.Fatalf("dispatch trace = %+v", s.DispatchTrace)
	}
	if got := s.VerificationTrace[0]; got.DispatchID != s.DispatchTrace[0].DispatchID || got.JobIndex != 0 || got.LaneIndex != 0 {
		t.Fatalf("verification correlation = %+v", got)
	}
}

func TestCollectSigverifyTelemetryBatchRecordsPacketBoundaries(t *testing.T) {
	wireTx := mustValidTestWire(t)
	first := packet.Owned(append([]byte(nil), wireTx...))
	in := make(chan packet.Packet, 2)
	in <- packet.Owned([]byte{0xff})
	in <- packet.Owned(append([]byte(nil), wireTx...))
	close(in)

	packets, queuedBefore, queuedAfter, inputClosed := collectSigverifyTelemetryBatch(first, in)
	defer func() {
		for _, pkt := range packets {
			pkt.Release()
		}
	}()
	if len(packets) != 3 || queuedBefore != 2 || queuedAfter != 0 || !inputClosed {
		t.Fatalf("batch shape: packets=%d queued=%d->%d closed=%v", len(packets), queuedBefore, queuedAfter, inputClosed)
	}
	counts := make([]uint16, len(packets))
	for i, pkt := range packets {
		counts[i] = prepareTelemetrySigverifyPacket(pkt).signatures
	}
	want := []uint16{1, 0, 1}
	for i := range want {
		if counts[i] != want[i] {
			t.Fatalf("signature counts = %v, want %v", counts, want)
		}
	}
}

func TestSchedulingSimulationCorrelatesClaimedTPUGroup(t *testing.T) {
	wireTx := mustValidTestWire(t)
	sigverifytelemetry.EnableSchedulingSimulation(32)
	t.Cleanup(sigverifytelemetry.Disable)
	in := make(chan packet.Packet, 3)
	out := make(chan packet.Packet, 3)
	for i := 0; i < 3; i++ {
		in <- packet.Owned(append([]byte(nil), wireTx...))
	}
	close(in)
	var stats SigverifyStats
	runSigverifyWorker(in, out, &stats)
	close(out)
	for pkt := range out {
		pkt.Release()
	}

	s := sigverifytelemetry.Current()
	if s.Mode != sigverifytelemetry.CollectionSchedulingSimulation || len(s.DispatchTrace) != 1 || len(s.VerificationTrace) != 3 {
		t.Fatalf("simulation trace = %+v", s)
	}
	dispatch := s.DispatchTrace[0]
	if dispatch.Mode != sigverifytelemetry.CollectionSchedulingSimulation || dispatch.ClaimEventSequence == 0 || dispatch.ReadyEventSequence <= dispatch.ClaimEventSequence || dispatch.SignatureLanes != 3 {
		t.Fatalf("simulation dispatch = %+v", dispatch)
	}
	for i, attempt := range s.VerificationTrace {
		if attempt.DispatchID != dispatch.DispatchID || attempt.JobIndex != uint32(i) || attempt.LaneIndex != 0 || attempt.BeginEventSequence <= dispatch.ReadyEventSequence {
			t.Fatalf("attempt[%d] correlation = %+v", i, attempt)
		}
	}
}

func requireEnqueue(t *testing.T, ingress chan<- packet.Packet, pkt packet.Packet) {
	t.Helper()
	var stats IngressStats
	deadline := time.Now().Add(time.Second)
	for !TryEnqueue(ingress, pkt, &stats) {
		if time.Now().After(deadline) {
			t.Fatal("ingress channel full")
		}
		runtime.Gosched()
	}
}
