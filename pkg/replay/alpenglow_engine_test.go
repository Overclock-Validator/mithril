package replay

import (
	"context"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
)

// fakeEngine returns a controlled Snapshot so we can drive the alpenglow finality
// watermark path without a live cluster.
type fakeEngine struct {
	snap consensusengine.Snapshot
}

func (f *fakeEngine) Name() string                { return "alpenglow-observer" }
func (f *fakeEngine) Start(context.Context) error { return nil }
func (f *fakeEngine) ObserveBlock(context.Context, consensusengine.BlockObservation) error {
	return nil
}
func (f *fakeEngine) OnReplayResult(context.Context, consensusengine.SlotReplayResult) error {
	return nil
}
func (f *fakeEngine) Snapshot() consensusengine.Snapshot { return f.snap }
func (f *fakeEngine) Close() error                       { return nil }

// alpenglowRootedSlot is the finality watermark that drives promotion in
// alpenglow mode. It must: report the cert-finalized slot, gate on slot>0, and be
// nil-safe (no engine / no chain).
func TestAlpenglowRootedSlot(t *testing.T) {
	// nil engine
	if slot, ok := alpenglowRootedSlot(nil); ok || slot != 0 {
		t.Fatalf("nil engine: got (%d,%v), want (0,false)", slot, ok)
	}

	// engine with no alpenglow chain yet
	noChain := &fakeEngine{snap: consensusengine.Snapshot{Mode: "alpenglow-observer"}}
	if slot, ok := alpenglowRootedSlot(noChain); ok || slot != 0 {
		t.Fatalf("nil chain: got (%d,%v), want (0,false)", slot, ok)
	}

	// chain present but nothing finalized yet (slot 0) → not ok
	zero := &fakeEngine{snap: consensusengine.Snapshot{
		AlpenglowChain: &alpenglow.ChainSnapshot{LatestDirectFinalizedBlock: alpenglow.BlockID{Slot: 0}},
	}}
	if slot, ok := alpenglowRootedSlot(zero); ok || slot != 0 {
		t.Fatalf("slot 0: got (%d,%v), want (0,false)", slot, ok)
	}

	// finalized slot present → reports it
	fin := &fakeEngine{snap: consensusengine.Snapshot{
		AlpenglowChain: &alpenglow.ChainSnapshot{LatestDirectFinalizedBlock: alpenglow.BlockID{Slot: 430276100}},
	}}
	if slot, ok := alpenglowRootedSlot(fin); !ok || slot != 430276100 {
		t.Fatalf("finalized: got (%d,%v), want (430276100,true)", slot, ok)
	}
}
