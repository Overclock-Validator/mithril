package pipeline

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/sink"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/tpu/wire"
	"github.com/stretchr/testify/require"
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

func TestPipelineV1FeatureAdmission(t *testing.T) {
	for _, tc := range []struct {
		name       string
		enabled    func() bool
		wantAtSink bool
	}{
		{name: "nil_fails_closed", enabled: nil, wantAtSink: false},
		{name: "inactive", enabled: func() bool { return false }, wantAtSink: false},
		{name: "active", enabled: func() bool { return true }, wantAtSink: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			noop := &sink.Noop{}
			p, ingress := Start(context.Background(), Config{
				SigverifyWorkers: 1,
				TxV1Enabled:      tc.enabled,
				Sink:             noop,
			})
			defer p.Stop()

			wireTx := txfixture.MustSignedV1Wire(11, 32)
			requireEnqueue(t, ingress, packet.Owned(wireTx))
			require.Eventually(t, func() bool {
				stats := p.Stats()
				return stats.Dedup.DroppedUnsupportedVersion+
					stats.Sigverify.VerifiedPackets+stats.Sigverify.DroppedSigverify == 1
			}, 2*time.Second, 5*time.Millisecond)
			require.Equal(t, tc.wantAtSink, noop.Snapshot().InPackets == 1)
		})
	}
}

func TestPipelineV1ActivationDoesNotPoisonDedup(t *testing.T) {
	var enabled atomic.Bool
	noop := &sink.Noop{}
	p, ingress := Start(context.Background(), Config{
		SigverifyWorkers: 1,
		TxV1Enabled:      enabled.Load,
		Sink:             noop,
	})
	defer p.Stop()

	wireTx := txfixture.MustSignedV1Wire(12, 32)
	requireEnqueue(t, ingress, packet.Owned(wireTx))
	require.Eventually(t, func() bool {
		return p.Stats().Dedup.DroppedUnsupportedVersion == 1
	}, 2*time.Second, 5*time.Millisecond)
	require.Zero(t, p.Stats().Dedup.DroppedDedup)

	enabled.Store(true)
	requireEnqueue(t, ingress, packet.Owned(wireTx))
	require.Eventually(t, func() bool {
		return noop.Snapshot().InPackets == 1
	}, 2*time.Second, 5*time.Millisecond)
	require.Zero(t, p.Stats().Dedup.DroppedDedup)
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

// Every admitted packet must reach the sink, whatever the count. Sigverify
// workers group packets, and a count that divides badly into groups must not
// leave a remainder sitting in a worker's scratch waiting for company that
// never arrives.
func TestPipelineDeliversAwkwardPacketCounts(t *testing.T) {
	for _, count := range []int{1, 3, 7, 8, 9, 65} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			noop := &sink.Noop{}
			p, ingress := Start(context.Background(), Config{SigverifyWorkers: 4, Sink: noop})
			defer p.Stop()

			for i := 0; i < count; i++ {
				// Distinct payloads: identical ones are dropped by the dedup
				// stage before sigverify ever sees them.
				requireEnqueue(t, ingress, packet.Owned(txfixture.MustSignedTransferWire(uint64(i))))
			}

			deadline := time.Now().Add(10 * time.Second)
			for noop.Snapshot().InPackets < uint64(count) {
				if time.Now().After(deadline) {
					t.Fatalf("count=%d: only %d packets reached the sink, stats=%+v",
						count, noop.Snapshot().InPackets, p.Stats())
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
}
