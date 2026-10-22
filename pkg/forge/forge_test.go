package forge

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/pipeline"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/stretchr/testify/assert"
)

func TestSinkDropsWithoutWorkingBank(t *testing.T) {
	sink := NewSink(blockprod.NewController())
	sink.Receive(packet.Owned(txfixture.MustSignedTransferWire(0)))
	assert.Equal(t, uint64(1), sink.Stats().DroppedNoBank)
}

func TestSinkAcceptsTransfer(t *testing.T) {
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{})
	defer env.Close()

	sink := NewSink(env.Controller)
	sink.Receive(packet.Owned(txfixture.MustSignedTransferWire(0)))

	stats := sink.Stats()
	assert.Equal(t, uint64(1), stats.Accepted)
	assert.Equal(t, uint64(1), stats.InPackets)
}

func TestSinkDropsWhileBankIsTPUGated(t *testing.T) {
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{TPUGated: true})
	defer env.Close()

	sink := NewSink(env.Controller)
	sink.Receive(packet.Owned(txfixture.MustSignedTransferWire(0)))
	assert.Equal(t, uint64(1), sink.Stats().DroppedNoBank)
	assert.Empty(t, env.Bank.ForgedTransactions())

	env.Bank.AllowTPU()
	sink.Receive(packet.Owned(txfixture.MustSignedTransferWire(0)))
	assert.Equal(t, uint64(1), sink.Stats().Accepted)
	assert.Len(t, env.Bank.ForgedTransactions(), 1)
}

func TestSinkCountsAlreadyProcessed(t *testing.T) {
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{})
	defer env.Close()

	sink := NewSink(env.Controller)
	wire := txfixture.MustSignedTransferWire(0)
	sink.Receive(packet.Owned(wire))
	sink.Receive(packet.Owned(wire))

	stats := sink.Stats()
	assert.Equal(t, uint64(2), stats.InPackets)
	assert.Equal(t, uint64(1), stats.Accepted)
	assert.Equal(t, uint64(1), stats.DroppedAlreadyProcessed)
}

func TestSinkDropsOnBlockCostLimit(t *testing.T) {
	limits := costmodel.DefaultLimits()
	limits.BlockCost = 1
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{Limits: limits})
	defer env.Close()

	sink := NewSink(env.Controller)
	sink.Receive(packet.Owned(txfixture.MustSignedTransferWire(0)))

	stats := sink.Stats()
	assert.Equal(t, uint64(1), stats.DroppedCost)
	assert.Equal(t, uint64(1), stats.DroppedBlockCost)
}

func TestPipelineForgeIntegration(t *testing.T) {
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{})
	defer env.Close()

	forgeSink := NewSink(env.Controller)
	ctx := context.Background()
	p, ingress := pipeline.Start(ctx, pipeline.Config{
		SigverifyWorkers: 2,
		Sink:             forgeSink,
	})
	defer p.Stop()

	wire := txfixture.MustSignedTransferWire(1)
	requireEnqueue(t, ingress, packet.Owned(wire))

	deadline := time.Now().Add(2 * time.Second)
	for forgeSink.Stats().Accepted == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for forge sink, stats=%+v pipeline=%+v", forgeSink.Stats(), p.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}

	assert.Equal(t, uint64(1), forgeSink.Stats().Accepted)
	assert.Equal(t, uint64(1), p.Stats().Sigverify.VerifiedPackets)
}

func TestPipelineForgeDropsWithoutBank(t *testing.T) {
	forgeSink := NewSink(blockprod.NewController())
	ctx := context.Background()
	p, ingress := pipeline.Start(ctx, pipeline.Config{SigverifyWorkers: 1, Sink: forgeSink})
	defer p.Stop()

	requireEnqueue(t, ingress, packet.Owned(txfixture.MustSignedTransferWire(0)))

	deadline := time.Now().Add(2 * time.Second)
	for forgeSink.Stats().DroppedNoBank == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out, stats=%+v", forgeSink.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(t, uint64(1), forgeSink.Stats().DroppedNoBank)
}

func requireEnqueue(t *testing.T, ingress chan<- packet.Packet, pkt packet.Packet) {
	t.Helper()
	var stats pipeline.IngressStats
	deadline := time.Now().Add(time.Second)
	for !pipeline.TryEnqueue(ingress, pkt, &stats) {
		if time.Now().After(deadline) {
			t.Fatal("ingress channel full")
		}
		runtime.Gosched()
	}
}
