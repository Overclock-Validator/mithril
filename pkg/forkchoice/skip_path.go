package forkchoice

import (
	"errors"
	"fmt"

	"github.com/gagliardetto/solana-go"
)

var (
	ErrDepthExceeded = errors.New("skip path: depth exceeded max allowed")
	ErrNoPath        = errors.New("skip path: no valid path found to target hash")
)

// SlotCandidate represents a candidate block at a specific slot.
type SlotCandidate struct {
	Slot          uint64
	HasBlock      bool
	Blockhash     solana.Hash // computed bankhash after executing this slot's block
	LastBlockhash solana.Hash // expected parent bankhash (block.LastBlockhash)
}

// SolveResult contains the resolved skip path.
// Path[i] corresponds to slot (startSlot + i):
//
//	true  → use the block at that slot
//	false → slot is empty/skipped
type SolveResult struct {
	Path        []bool
	MatchedHash solana.Hash
}

// candidate represents one possible bankhash state and the decisions that led there.
type candidate struct {
	hash solana.Hash
	path []bool
}

// SkipPath finds a sequence of empty/block decisions from startSlot to endSlot
// such that, starting from prevBankhash (the parent bankhash at startSlot-1),
// the bankhash state at endSlot equals targetHash.
//
// This solver operates over executed candidate results, not raw shreds.
// A skipped slot leaves the bankhash state unchanged in the current verifier
// model. A block candidate can only advance the state if its LastBlockhash
// matches the current bankhash.
//
// The solver uses BFS with hash-dedup at each depth to prevent state explosion.
// Since empty slots are identity on bankhash state, consecutive skips collapse
// to a single state.
func SkipPath(
	startSlot uint64,
	endSlot uint64,
	prevBankhash solana.Hash,
	candidates map[uint64]*SlotCandidate,
	targetHash solana.Hash,
	maxDepth int,
) (*SolveResult, error) {
	if endSlot < startSlot {
		return nil, fmt.Errorf("skip path: endSlot %d < startSlot %d", endSlot, startSlot)
	}

	depth := endSlot - startSlot + 1
	if int(depth) > maxDepth {
		return nil, ErrDepthExceeded
	}

	// Start with a single candidate at prevBankhash with no decisions.
	current := []candidate{
		{hash: prevBankhash, path: nil},
	}

	for slot := startSlot; slot <= endSlot; slot++ {
		cand := candidates[slot]

		// Deduplicate next states by bankhash. Two paths reaching the same
		// bankhash state at the same slot produce identical results going forward.
		seen := make(map[solana.Hash]struct{})
		var next []candidate

		for _, c := range current {
			// Branch 1: slot is empty (skipped). Bankhash state unchanged.
			emptyHash := c.hash
			if _, dup := seen[emptyHash]; !dup {
				seen[emptyHash] = struct{}{}
				newPath := make([]bool, len(c.path)+1)
				copy(newPath, c.path)
				newPath[len(c.path)] = false
				next = append(next, candidate{hash: emptyHash, path: newPath})
			}

			// Branch 2: slot has a block AND block's parent bankhash matches our state.
			if cand != nil && cand.HasBlock && cand.LastBlockhash == c.hash {
				blockHash := cand.Blockhash
				if _, dup := seen[blockHash]; !dup {
					seen[blockHash] = struct{}{}
					newPath := make([]bool, len(c.path)+1)
					copy(newPath, c.path)
					newPath[len(c.path)] = true
					next = append(next, candidate{hash: blockHash, path: newPath})
				}
			}
		}

		current = next
	}

	// Look for a candidate whose final bankhash equals targetHash.
	for _, c := range current {
		if c.hash == targetHash {
			return &SolveResult{
				Path:        c.path,
				MatchedHash: targetHash,
			}, nil
		}
	}

	return nil, ErrNoPath
}
