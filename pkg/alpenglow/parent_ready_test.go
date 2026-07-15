package alpenglow

import (
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestParentReadyConnectsNotarizedBlockAcrossSkips(t *testing.T) {
	root := BlockID{Slot: 0, Hash: parentReadyHash(1)}
	tracker := NewParentReadyTracker(root)
	block := BlockID{Slot: 1, Hash: parentReadyHash(2)}

	events, err := tracker.AddNotarFallbackOrStronger(block)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("unexpected event before leader-window boundary: %+v", events)
	}
	if events := tracker.AddSkip(2); len(events) != 0 {
		t.Fatalf("unexpected event at slot 3: %+v", events)
	}
	events = tracker.AddSkip(3)
	if len(events) != 1 || events[0].Kind != ConsensusEventParentReady || events[0].Slot != 4 || events[0].Block != block {
		t.Fatalf("parent-ready event = %+v, want block %v for slot 4", events, block)
	}
	parent := tracker.BlockProductionParent(4)
	if parent.Kind != BlockProductionParentReady || parent.Parent != block {
		t.Fatalf("block production parent = %+v", parent)
	}
}

func TestParentReadyChoosesLowestBlockLikeAgave(t *testing.T) {
	root := BlockID{Slot: 0, Hash: parentReadyHash(9)}
	tracker := NewParentReadyTracker(root)
	block := BlockID{Slot: 1, Hash: parentReadyHash(2)}
	if _, err := tracker.AddNotarFallbackOrStronger(block); err != nil {
		t.Fatal(err)
	}
	tracker.AddSkip(1)
	tracker.AddSkip(2)
	tracker.AddSkip(3)

	parent := tracker.BlockProductionParent(4)
	if parent.Kind != BlockProductionParentReady || parent.Parent != root {
		t.Fatalf("parent = %+v, want lowest block %v", parent, root)
	}
}

func TestParentReadyGenesisAllowedAtRoot(t *testing.T) {
	root := BlockID{Slot: 100, Hash: parentReadyHash(1)}
	tracker := NewParentReadyTracker(root)
	genesis := BlockID{Slot: 100, Hash: parentReadyHash(3)}
	if _, err := tracker.AddNotarFallbackOrStronger(genesis); err != nil {
		t.Fatal(err)
	}
	if tracker.HasNotarFallbackOrStronger(genesis) {
		t.Fatal("ordinary certificate at root should be ignored")
	}
	if _, err := tracker.AddGenesis(genesis); err != nil {
		t.Fatal(err)
	}
	if !tracker.HasNotarFallbackOrStronger(genesis) {
		t.Fatal("genesis certificate at root was not retained")
	}
}

func parentReadyHash(value byte) solana.Hash {
	var hash solana.Hash
	hash[0] = value
	return hash
}
