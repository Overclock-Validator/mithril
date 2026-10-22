package tpu

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/pipeline"
	"github.com/Overclock-Validator/mithril/pkg/tpu/sink"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
)

func TestTPUStartStop(t *testing.T) {
	_, identity, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	svc, err := Start(context.Background(), Config{
		Name:       "test-tpu",
		ListenAddr: "127.0.0.1:0",
		Identity:   identity,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	addr, err := svc.AdvertisedQUICAddr()
	if err != nil {
		t.Fatalf("AdvertisedQUICAddr: %v", err)
	}
	if addr.Port == 0 {
		t.Fatalf("expected ephemeral TPU port")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestTPUSubmitFeedsPipelineUntilStopped(t *testing.T) {
	_, identity, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	recv := &sink.Noop{}
	svc, err := Start(context.Background(), Config{
		Name:       "test-tpu",
		ListenAddr: "127.0.0.1:0",
		Identity:   identity,
		Pipeline:   pipeline.Config{SigverifyWorkers: 1, Sink: recv},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !svc.Submit(txfixture.MustSignedTransferWire(0), packet.SourceRelayer) {
		t.Fatalf("Submit refused a packet on a running TPU")
	}
	deadline := time.Now().Add(2 * time.Second)
	for recv.Snapshot().InPackets == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("submitted packet never reached the sink: %+v", svc.Stats().Pipeline)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := svc.Stats().Pipeline.Submitted[packet.SourceRelayer].Enqueued; got != 1 {
		t.Fatalf("relayer enqueued = %d", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if svc.Submit(txfixture.MustSignedTransferWire(1), packet.SourceRelayer) {
		t.Fatalf("Submit accepted a packet after Stop")
	}
	var stopped *TPU
	if stopped.Submit(txfixture.MustSignedTransferWire(2), packet.SourceRelayer) {
		t.Fatalf("Submit on a nil TPU must refuse")
	}
}
