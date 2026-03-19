package forkchoice

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestForkChoiceState creates a minimal forkChoiceState for testing.
// Does NOT start the service or worker pool — only the state is used.
func newTestForkChoiceState(totalStake uint64) *ForkChoiceService {
	state := &forkChoiceState{
		voteStakeTotals: make(map[uint64]*slotVoteAccumulator),
		totalEpochStake: totalStake,
	}
	return &ForkChoiceService{state: state}
}

// injectSupermajority simulates a slot reaching supermajority by directly
// manipulating the accumulator state. This isolates coordinator tests from
// vote parsing and block submission internals.
func injectSupermajority(fc *ForkChoiceService, slot uint64, winningHash solana.Hash, stake uint64) {
	acc := newSlotVoteAccumulator(fc.state.totalEpochStake, slot)
	tracker := &voteStakeTracker{
		voted: make(map[solana.PublicKey]struct{}),
		stake: stake,
	}
	acc.trackers[winningHash] = tracker
	acc.confirmed = true
	acc.confirmedHash = winningHash
	fc.state.voteStakeTotals[slot] = acc
}

func TestResolveRangeSuccess(t *testing.T) {
	fc := newTestForkChoiceState(100)

	prevBankhash := hash(0x01)
	blockHash := hash(0x02)

	// Simulate: slot 10 has a block, supermajority confirms blockHash.
	injectSupermajority(fc, 10, blockHash, 70)
	fc.state.latestSlotIngested = 10 + VoteConfirmationTimeoutSlots

	candidates := map[uint64]*SlotCandidate{
		10: {Slot: 10, HasBlock: true, Blockhash: blockHash, LastBlockhash: prevBankhash},
	}

	cc := NewConsensusCoordinator(fc, 64, "halt")
	decisions, err := cc.ResolveRange(10, 10, prevBankhash, candidates)
	require.NoError(t, err)
	require.Len(t, decisions, 1)
	assert.Equal(t, uint64(10), decisions[0].Slot)
	assert.True(t, decisions[0].UseBlock)
}

func TestResolveRangeNeedWait(t *testing.T) {
	fc := newTestForkChoiceState(100)

	// latestSlotIngested is 0 — way before landing window for slot 10.
	fc.state.latestSlotIngested = 0

	cc := NewConsensusCoordinator(fc, 64, "halt")
	_, err := cc.ResolveRange(10, 10, hash(0x01), nil)
	assert.ErrorIs(t, err, ErrNeedWait)
}

func TestResolveRangeNoSupermajority(t *testing.T) {
	fc := newTestForkChoiceState(100)

	// Landing window passed but no votes at all for slot 10.
	fc.state.latestSlotIngested = 10 + VoteConfirmationTimeoutSlots

	cc := NewConsensusCoordinator(fc, 64, "halt")
	_, err := cc.ResolveRange(10, 10, hash(0x01), nil)
	assert.ErrorIs(t, err, ErrNoSupermajority)
}

func TestResolveRangeNoPath(t *testing.T) {
	fc := newTestForkChoiceState(100)

	prevBankhash := hash(0x01)
	targetHash := hash(0xFF)

	// Supermajority says targetHash won, but our candidates can't chain to it.
	injectSupermajority(fc, 10, targetHash, 70)
	fc.state.latestSlotIngested = 10 + VoteConfirmationTimeoutSlots

	candidates := map[uint64]*SlotCandidate{
		10: {Slot: 10, HasBlock: true, Blockhash: hash(0x02), LastBlockhash: prevBankhash},
	}

	cc := NewConsensusCoordinator(fc, 64, "halt")
	_, err := cc.ResolveRange(10, 10, prevBankhash, candidates)
	assert.ErrorIs(t, err, ErrNoPath)
}

func TestResolveRangeMultiSlotMixed(t *testing.T) {
	fc := newTestForkChoiceState(100)

	// Slots 10-13: 10(empty), 11(block), 12(empty), 13(block)
	prevBankhash := hash(0x01)
	block11Hash := hash(0x02)
	block13Hash := hash(0x03)

	injectSupermajority(fc, 13, block13Hash, 70)
	fc.state.latestSlotIngested = 13 + VoteConfirmationTimeoutSlots

	candidates := map[uint64]*SlotCandidate{
		11: {Slot: 11, HasBlock: true, Blockhash: block11Hash, LastBlockhash: prevBankhash},
		13: {Slot: 13, HasBlock: true, Blockhash: block13Hash, LastBlockhash: block11Hash},
	}

	cc := NewConsensusCoordinator(fc, 64, "halt")
	decisions, err := cc.ResolveRange(10, 13, prevBankhash, candidates)
	require.NoError(t, err)
	require.Len(t, decisions, 4)

	assert.Equal(t, SlotDecision{Slot: 10, UseBlock: false}, decisions[0])
	assert.Equal(t, SlotDecision{Slot: 11, UseBlock: true}, decisions[1])
	assert.Equal(t, SlotDecision{Slot: 12, UseBlock: false}, decisions[2])
	assert.Equal(t, SlotDecision{Slot: 13, UseBlock: true}, decisions[3])
}

func TestResolveRangeDepthExceeded(t *testing.T) {
	fc := newTestForkChoiceState(100)

	injectSupermajority(fc, 100, hash(0xAA), 70)
	fc.state.latestSlotIngested = 100 + VoteConfirmationTimeoutSlots

	// Range is 0..100 = 101 slots, but maxDepth is 64.
	cc := NewConsensusCoordinator(fc, 64, "halt")
	_, err := cc.ResolveRange(0, 100, hash(0x01), nil)
	assert.ErrorIs(t, err, ErrDepthExceeded)
}

func TestResolveRangeAllEmpty(t *testing.T) {
	fc := newTestForkChoiceState(100)

	prevBankhash := hash(0x01)

	// Target hash equals prevBankhash — all slots skipped.
	injectSupermajority(fc, 12, prevBankhash, 70)
	fc.state.latestSlotIngested = 12 + VoteConfirmationTimeoutSlots

	cc := NewConsensusCoordinator(fc, 64, "halt")
	decisions, err := cc.ResolveRange(10, 12, prevBankhash, nil)
	require.NoError(t, err)
	require.Len(t, decisions, 3)
	for _, d := range decisions {
		assert.False(t, d.UseBlock)
	}
}

func TestCoordinatorPolicy(t *testing.T) {
	fc := newTestForkChoiceState(100)
	cc := NewConsensusCoordinator(fc, 64, "halt")
	assert.Equal(t, "halt", cc.Policy())

	cc2 := NewConsensusCoordinator(fc, 64, "warn")
	assert.Equal(t, "warn", cc2.Policy())
}
