package forkchoice

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makePubkey(id byte) solana.PublicKey {
	var pk [32]byte
	pk[0] = id
	return solana.PublicKeyFromBytes(pk[:])
}

func TestComputeThresholdStake(t *testing.T) {
	// Match Agave's formula: uint64(float64(total) * 2.0/3.0)
	assert.Equal(t, uint64(6), computeThresholdStake(10))
	assert.Equal(t, uint64(66), computeThresholdStake(100))
	assert.Equal(t, uint64(0), computeThresholdStake(0))
	assert.Equal(t, uint64(0), computeThresholdStake(1))
	// total=3: int(3 * 0.666...) = int(2.0) = 2
	assert.Equal(t, uint64(2), computeThresholdStake(3))
}

// TestThresholdCrossingSmallTotal mirrors Agave's test_add_vote_pubkey:
// total=10, 10 voters each with stake=1, threshold crosses at i=6 (stake=7 > threshold=6).
func TestThresholdCrossingSmallTotal(t *testing.T) {
	acc := newSlotVoteAccumulator(10, 42)
	hash := solana.Hash{1}

	for i := 0; i < 10; i++ {
		pubkey := makePubkey(byte(i + 1))
		crossed, isNew := acc.addVote(hash, pubkey, 1)
		assert.True(t, isNew, "vote %d should be new", i)

		// Agave: at i=6, stake goes from 6 to 7, crossing threshold of 6
		if i == 6 {
			assert.True(t, crossed, "threshold should cross at i=6 (stake=7)")
		} else {
			assert.False(t, crossed, "threshold should NOT cross at i=%d", i)
		}
	}

	assert.True(t, acc.hasSupermajority())
	assert.True(t, acc.hashHasSupermajority(hash))
	assert.Equal(t, uint64(10), acc.stakeForHash(hash))
}

func TestDuplicateVotePubkeyDoesNotIncreaseStake(t *testing.T) {
	acc := newSlotVoteAccumulator(10, 42)
	hash := solana.Hash{1}
	pubkey := makePubkey(1)

	_, isNew1 := acc.addVote(hash, pubkey, 5)
	assert.True(t, isNew1)
	assert.Equal(t, uint64(5), acc.stakeForHash(hash))

	crossed2, isNew2 := acc.addVote(hash, pubkey, 5)
	assert.False(t, isNew2, "duplicate vote should not be new")
	assert.False(t, crossed2, "duplicate vote should not cross threshold")
	assert.Equal(t, uint64(5), acc.stakeForHash(hash), "stake should not increase on duplicate")
}

func TestTwoHashesSameSlot(t *testing.T) {
	acc := newSlotVoteAccumulator(10, 42)
	hashA := solana.Hash{0xAA}
	hashB := solana.Hash{0xBB}

	// 7 voters for hashA
	for i := 0; i < 7; i++ {
		acc.addVote(hashA, makePubkey(byte(i+1)), 1)
	}
	// 3 voters for hashB
	for i := 0; i < 3; i++ {
		acc.addVote(hashB, makePubkey(byte(i+100)), 1)
	}

	assert.True(t, acc.hashHasSupermajority(hashA), "hashA should have supermajority")
	assert.False(t, acc.hashHasSupermajority(hashB), "hashB should NOT have supermajority")

	winning, ok := acc.winningHash()
	require.True(t, ok)
	assert.Equal(t, hashA, winning)
}

func TestNoThresholdReached(t *testing.T) {
	acc := newSlotVoteAccumulator(10, 42)
	hash := solana.Hash{1}

	// Only 3 stake — not enough for threshold of 6
	for i := 0; i < 3; i++ {
		acc.addVote(hash, makePubkey(byte(i+1)), 1)
	}

	assert.False(t, acc.hasSupermajority())
	assert.False(t, acc.hashHasSupermajority(hash))
	_, ok := acc.winningHash()
	assert.False(t, ok)
}

// TestDedupAcrossDifferentHashes verifies that dedup is per (slot, hash, pubkey),
// matching Agave behavior. Same pubkey voting for different hashes should count in both.
func TestDedupAcrossDifferentHashes(t *testing.T) {
	acc := newSlotVoteAccumulator(10, 42)
	hashA := solana.Hash{0xAA}
	hashB := solana.Hash{0xBB}
	pubkey := makePubkey(1)

	_, isNew1 := acc.addVote(hashA, pubkey, 5)
	_, isNew2 := acc.addVote(hashB, pubkey, 5)

	assert.True(t, isNew1)
	assert.True(t, isNew2, "same pubkey voting for different hash should be new")
	assert.Equal(t, uint64(5), acc.stakeForHash(hashA))
	assert.Equal(t, uint64(5), acc.stakeForHash(hashB))
}

func TestStakeForNonexistentHash(t *testing.T) {
	acc := newSlotVoteAccumulator(10, 42)
	assert.Equal(t, uint64(0), acc.stakeForHash(solana.Hash{0xFF}))
}

func TestHashHasSupermajorityNonexistentHash(t *testing.T) {
	acc := newSlotVoteAccumulator(10, 42)
	assert.False(t, acc.hashHasSupermajority(solana.Hash{0xFF}))
}
