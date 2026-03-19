package forkchoice

import (
	"errors"

	"github.com/gagliardetto/solana-go"
)

var (
	ErrNeedWait        = errors.New("consensus: vote landing window not reached, need to wait")
	ErrNoSupermajority = errors.New("consensus: no hash reached supermajority for target slot")
)

// SlotDecision represents the resolved action for a single slot.
type SlotDecision struct {
	Slot     uint64
	UseBlock bool // true = use the block, false = slot is empty/skipped
}

// ConsensusCoordinator bridges the ForkChoiceService (vote accumulation) and
// the SkipPath solver to verify bankhash-chain consistency for a slot range.
//
// For a given range [startSlot, endSlot], it:
//  1. Queries forkchoice for the vote-confirmed bankhash at endSlot
//  2. Runs the skipPath solver to find a valid chain from prevBankhash to that hash
//  3. Returns per-slot decisions (use block or skip)
type ConsensusCoordinator struct {
	forkChoice *ForkChoiceService
	maxDepth   int
	policy     string // "halt" = return error on unresolved, "warn" = log and continue
}

// NewConsensusCoordinator creates a coordinator with the given forkchoice service,
// maximum skipPath solver depth, and unresolved policy.
func NewConsensusCoordinator(fc *ForkChoiceService, maxDepth int, policy string) *ConsensusCoordinator {
	return &ConsensusCoordinator{
		forkChoice: fc,
		maxDepth:   maxDepth,
		policy:     policy,
	}
}

// ResolveRange attempts to determine which slots in [startSlot, endSlot] should
// use blocks vs be treated as skipped.
//
// prevBankhash is the parent bankhash at startSlot-1 (the last executed block's bankhash).
// candidates are executed slot candidates carrying computed bankhash + parent bankhash.
//
// Returns slot decisions or an error:
//   - ErrNeedWait: votes haven't landed yet, caller should retry later
//   - ErrNoSupermajority: no hash reached 2/3 threshold for endSlot
//   - ErrNoPath: solver couldn't find a valid chain to the target hash
//   - ErrDepthExceeded: range exceeds maxDepth
func (cc *ConsensusCoordinator) ResolveRange(
	startSlot, endSlot uint64,
	prevBankhash solana.Hash,
	candidates map[uint64]*SlotCandidate,
) ([]SlotDecision, error) {
	// Query forkchoice for the vote-confirmed hash at the end slot.
	targetHash, status := cc.forkChoice.GetSupermajorityHash(endSlot)

	switch status {
	case BankhashNeedWait:
		return nil, ErrNeedWait
	case BankhashNoSupermajority:
		return nil, ErrNoSupermajority
	case BankhashHasSupermajority:
		// Continue to solve
	}

	// Run the skipPath solver to find a valid chain.
	result, err := SkipPath(startSlot, endSlot, prevBankhash, candidates, targetHash, cc.maxDepth)
	if err != nil {
		return nil, err
	}

	// Convert []bool path to []SlotDecision.
	decisions := make([]SlotDecision, len(result.Path))
	for i, useBlock := range result.Path {
		decisions[i] = SlotDecision{
			Slot:     startSlot + uint64(i),
			UseBlock: useBlock,
		}
	}

	return decisions, nil
}

// Policy returns the coordinator's unresolved policy ("halt" or "warn").
func (cc *ConsensusCoordinator) Policy() string {
	return cc.policy
}
