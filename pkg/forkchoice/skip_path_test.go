package forkchoice

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hash(b byte) solana.Hash {
	return solana.Hash{b}
}

func TestSkipPathSingleBlock(t *testing.T) {
	// prevBankhash → executed slot 10 candidate → targetHash
	prevBankhash := hash(0x01)
	blockHash := hash(0x02)

	candidates := map[uint64]*SlotCandidate{
		10: {Slot: 10, HasBlock: true, Blockhash: blockHash, LastBlockhash: prevBankhash},
	}

	result, err := SkipPath(10, 10, prevBankhash, candidates, blockHash, 64)
	require.NoError(t, err)
	assert.Equal(t, []bool{true}, result.Path)
	assert.Equal(t, blockHash, result.MatchedHash)
}

func TestSkipPathMultipleBlocks(t *testing.T) {
	// prevBankhash → executed slot 10 → executed slot 12 → targetHash
	// Slots 11 is empty (skipped)
	prevBankhash := hash(0x01)
	block10Hash := hash(0x02)
	block12Hash := hash(0x03)

	candidates := map[uint64]*SlotCandidate{
		10: {Slot: 10, HasBlock: true, Blockhash: block10Hash, LastBlockhash: prevBankhash},
		12: {Slot: 12, HasBlock: true, Blockhash: block12Hash, LastBlockhash: block10Hash},
	}

	result, err := SkipPath(10, 12, prevBankhash, candidates, block12Hash, 64)
	require.NoError(t, err)
	assert.Equal(t, []bool{true, false, true}, result.Path)
	assert.Equal(t, block12Hash, result.MatchedHash)
}

func TestSkipPathNoPathExists(t *testing.T) {
	prevBankhash := hash(0x01)
	wrongTarget := hash(0xFF)

	candidates := map[uint64]*SlotCandidate{
		10: {Slot: 10, HasBlock: true, Blockhash: hash(0x02), LastBlockhash: prevBankhash},
	}

	_, err := SkipPath(10, 10, prevBankhash, candidates, wrongTarget, 64)
	assert.ErrorIs(t, err, ErrNoPath)
}

func TestSkipPathDepthExceeded(t *testing.T) {
	prevBankhash := hash(0x01)
	target := hash(0x02)

	_, err := SkipPath(0, 100, prevBankhash, nil, target, 64)
	assert.ErrorIs(t, err, ErrDepthExceeded)
}

func TestSkipPathEmptyOnlyPath(t *testing.T) {
	// All slots skipped: target == prevBankhash
	prevBankhash := hash(0x01)

	result, err := SkipPath(10, 13, prevBankhash, nil, prevBankhash, 64)
	require.NoError(t, err)
	assert.Equal(t, []bool{false, false, false, false}, result.Path)
}

func TestSkipPathMixedEmptyAndBlock(t *testing.T) {
	// slots: 10(empty), 11(block), 12(empty), 13(block)
	prevBankhash := hash(0x01)
	block11Hash := hash(0x02)
	block13Hash := hash(0x03)

	candidates := map[uint64]*SlotCandidate{
		11: {Slot: 11, HasBlock: true, Blockhash: block11Hash, LastBlockhash: prevBankhash},
		13: {Slot: 13, HasBlock: true, Blockhash: block13Hash, LastBlockhash: block11Hash},
	}

	result, err := SkipPath(10, 13, prevBankhash, candidates, block13Hash, 64)
	require.NoError(t, err)
	assert.Equal(t, []bool{false, true, false, true}, result.Path)
}

func TestSkipPathBlockLastBlockhashMismatch(t *testing.T) {
	// Block exists but its LastBlockhash doesn't match prevBankhash — can't use it
	prevBankhash := hash(0x01)
	wrongParent := hash(0xFF)

	candidates := map[uint64]*SlotCandidate{
		10: {Slot: 10, HasBlock: true, Blockhash: hash(0x02), LastBlockhash: wrongParent},
	}

	// Target is prevBankhash (all empty), which should work
	result, err := SkipPath(10, 10, prevBankhash, candidates, prevBankhash, 64)
	require.NoError(t, err)
	assert.Equal(t, []bool{false}, result.Path, "should skip the block with mismatched parent")
}

func TestSkipPathDedupCollapsesStates(t *testing.T) {
	// Multiple consecutive empty slots should not cause state explosion.
	// 60 slots, all empty, target = prevBankhash
	prevBankhash := hash(0x01)

	result, err := SkipPath(0, 59, prevBankhash, nil, prevBankhash, 64)
	require.NoError(t, err)
	assert.Len(t, result.Path, 60)
	for _, decision := range result.Path {
		assert.False(t, decision)
	}
}

func TestSkipPathEndSlotBeforeStartSlot(t *testing.T) {
	_, err := SkipPath(10, 5, hash(0x01), nil, hash(0x01), 64)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "endSlot 5 < startSlot 10")
}

func TestSkipPathChooseBlockOverEmpty(t *testing.T) {
	// When both paths work (block.Blockhash == prevBankhash would mean empty
	// and block both reach same state), the solver should still find a valid path.
	// This tests deterministic behavior.
	prevBankhash := hash(0x01)
	target := hash(0x02)

	candidates := map[uint64]*SlotCandidate{
		10: {Slot: 10, HasBlock: true, Blockhash: target, LastBlockhash: prevBankhash},
	}

	result, err := SkipPath(10, 10, prevBankhash, candidates, target, 64)
	require.NoError(t, err)
	assert.Equal(t, []bool{true}, result.Path)
}
