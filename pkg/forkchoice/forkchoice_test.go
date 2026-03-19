package forkchoice

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
)

func TestNeedWaitWhenNoWinnerBeforeTimeout(t *testing.T) {
	epochAuth := epochstakes.NewEpochAuthorizedVotersCache()
	epochStakes := map[solana.PublicKey]uint64{}
	service := NewForkChoiceService(0, epochStakes, 100, epochAuth)


	// No blocks ingested yet, no winner, within timeout → NeedWait.
	result := service.IsBankhashCorrect(10, solana.Hash{1})
	assert.Equal(t, BankhashNeedWait, result.Status)
	assert.Equal(t, uint64(100), result.TotalEpochStake)
}

func TestHasSupermajorityAfterEnoughVotes(t *testing.T) {
	epochAuth := epochstakes.NewEpochAuthorizedVotersCache()
	epochStakes := map[solana.PublicKey]uint64{}
	totalStake := uint64(100)
	service := NewForkChoiceService(0, epochStakes, totalStake, epochAuth)


	slot := uint64(10)
	hash := solana.Hash{0xAA}

	// Manually populate state to test query path
	service.state.mu.Lock()
	acc := newSlotVoteAccumulator(totalStake, slot)
	for i := 0; i < 67; i++ {
		var pk [32]byte
		pk[0] = byte(i + 1)
		pk[1] = byte((i + 1) >> 8)
		acc.addVote(hash, solana.PublicKeyFromBytes(pk[:]), 1)
	}
	service.state.voteStakeTotals[slot] = acc
	service.state.latestSlotIngested = slot + VoteConfirmationTimeoutSlots
	service.state.mu.Unlock()

	result := service.IsBankhashCorrect(slot, hash)
	assert.Equal(t, BankhashHasSupermajority, result.Status)
	assert.Equal(t, hash, result.WinningHash)
	assert.Equal(t, uint64(67), result.StakeForHash)
	assert.Equal(t, totalStake, result.TotalEpochStake)
	assert.Equal(t, computeThresholdStake(totalStake), result.ThresholdStake)
}

func TestNoSupermajorityAfterLandingWindow(t *testing.T) {
	epochAuth := epochstakes.NewEpochAuthorizedVotersCache()
	epochStakes := map[solana.PublicKey]uint64{}
	totalStake := uint64(100)
	service := NewForkChoiceService(0, epochStakes, totalStake, epochAuth)


	slot := uint64(10)
	hash := solana.Hash{0xAA}

	service.state.mu.Lock()
	acc := newSlotVoteAccumulator(totalStake, slot)
	// Only 30 stake — not enough for threshold of 66
	for i := 0; i < 30; i++ {
		var pk [32]byte
		pk[0] = byte(i + 1)
		acc.addVote(hash, solana.PublicKeyFromBytes(pk[:]), 1)
	}
	service.state.voteStakeTotals[slot] = acc
	service.state.latestSlotIngested = slot + VoteConfirmationTimeoutSlots
	service.state.mu.Unlock()

	result := service.IsBankhashCorrect(slot, hash)
	assert.Equal(t, BankhashNoSupermajority, result.Status)
	assert.Equal(t, uint64(30), result.StakeForHash)
	assert.Equal(t, totalStake, result.TotalEpochStake)
	assert.Equal(t, computeThresholdStake(totalStake), result.ThresholdStake)
}

func TestNoVotesSeenAfterLandingWindow(t *testing.T) {
	epochAuth := epochstakes.NewEpochAuthorizedVotersCache()
	epochStakes := map[solana.PublicKey]uint64{}
	totalStake := uint64(100)
	service := NewForkChoiceService(0, epochStakes, totalStake, epochAuth)


	// Landing window passed but no accumulator for this slot
	service.state.mu.Lock()
	service.state.latestSlotIngested = 50 + VoteConfirmationTimeoutSlots
	service.state.mu.Unlock()

	result := service.IsBankhashCorrect(50, solana.Hash{0xAA})
	assert.Equal(t, BankhashNoSupermajority, result.Status)
}

// TestEarlyConfirmationBeforeTimeout verifies that a slot with observed
// supermajority is confirmed immediately, even before the timeout window.
func TestEarlyConfirmationBeforeTimeout(t *testing.T) {
	epochAuth := epochstakes.NewEpochAuthorizedVotersCache()
	epochStakes := map[solana.PublicKey]uint64{}
	totalStake := uint64(100)
	service := NewForkChoiceService(0, epochStakes, totalStake, epochAuth)


	slot := uint64(10)
	hash := solana.Hash{0xAA}

	// Inject supermajority but keep latestSlotIngested BELOW timeout.
	service.state.mu.Lock()
	acc := newSlotVoteAccumulator(totalStake, slot)
	for i := 0; i < 67; i++ {
		var pk [32]byte
		pk[0] = byte(i + 1)
		pk[1] = byte((i + 1) >> 8)
		acc.addVote(hash, solana.PublicKeyFromBytes(pk[:]), 1)
	}
	service.state.voteStakeTotals[slot] = acc
	service.state.latestSlotIngested = slot + 5 // Well below slot + 32
	service.state.mu.Unlock()

	result := service.IsBankhashCorrect(slot, hash)
	assert.Equal(t, BankhashHasSupermajority, result.Status, "should confirm immediately when winner exists")
	assert.Equal(t, hash, result.WinningHash)
	assert.Equal(t, uint64(67), result.StakeForHash)
	assert.Equal(t, totalStake, result.TotalEpochStake)
	assert.Equal(t, computeThresholdStake(totalStake), result.ThresholdStake)
}

// TestNeedWaitPartialVotesBeforeTimeout verifies NeedWait when there are
// partial votes (below threshold) and the timeout hasn't expired.
func TestNeedWaitPartialVotesBeforeTimeout(t *testing.T) {
	epochAuth := epochstakes.NewEpochAuthorizedVotersCache()
	epochStakes := map[solana.PublicKey]uint64{}
	totalStake := uint64(100)
	service := NewForkChoiceService(0, epochStakes, totalStake, epochAuth)


	slot := uint64(10)
	hash := solana.Hash{0xAA}

	service.state.mu.Lock()
	acc := newSlotVoteAccumulator(totalStake, slot)
	// Only 30 stake — not enough for threshold of 66
	for i := 0; i < 30; i++ {
		var pk [32]byte
		pk[0] = byte(i + 1)
		acc.addVote(hash, solana.PublicKeyFromBytes(pk[:]), 1)
	}
	service.state.voteStakeTotals[slot] = acc
	service.state.latestSlotIngested = slot + 10 // Below timeout
	service.state.mu.Unlock()

	result := service.IsBankhashCorrect(slot, hash)
	assert.Equal(t, BankhashNeedWait, result.Status, "no winner + before timeout = NeedWait")
}

// TestNoSupermajorityPartialVotesAfterTimeout verifies NoSupermajority when
// partial votes exist but the timeout has expired without a winner.
func TestNoSupermajorityPartialVotesAfterTimeout(t *testing.T) {
	epochAuth := epochstakes.NewEpochAuthorizedVotersCache()
	epochStakes := map[solana.PublicKey]uint64{}
	totalStake := uint64(100)
	service := NewForkChoiceService(0, epochStakes, totalStake, epochAuth)


	slot := uint64(10)
	hash := solana.Hash{0xAA}

	service.state.mu.Lock()
	acc := newSlotVoteAccumulator(totalStake, slot)
	for i := 0; i < 30; i++ {
		var pk [32]byte
		pk[0] = byte(i + 1)
		acc.addVote(hash, solana.PublicKeyFromBytes(pk[:]), 1)
	}
	service.state.voteStakeTotals[slot] = acc
	service.state.latestSlotIngested = slot + VoteConfirmationTimeoutSlots
	service.state.mu.Unlock()

	result := service.IsBankhashCorrect(slot, hash)
	assert.Equal(t, BankhashNoSupermajority, result.Status, "no winner + after timeout = NoSupermajority")
	assert.Equal(t, uint64(30), result.StakeForHash)
	assert.Equal(t, computeThresholdStake(totalStake), result.ThresholdStake)
}

// TestEarlyMismatchBeforeTimeout verifies that a bankhash mismatch is surfaced
// immediately when a different hash wins supermajority, even before timeout.
func TestEarlyMismatchBeforeTimeout(t *testing.T) {
	epochAuth := epochstakes.NewEpochAuthorizedVotersCache()
	epochStakes := map[solana.PublicKey]uint64{}
	totalStake := uint64(100)
	service := NewForkChoiceService(0, epochStakes, totalStake, epochAuth)


	slot := uint64(10)
	winnerHash := solana.Hash{0xBB}
	ourHash := solana.Hash{0xAA}

	// Inject supermajority for winnerHash, also add some stake for ourHash.
	service.state.mu.Lock()
	acc := newSlotVoteAccumulator(totalStake, slot)
	for i := 0; i < 67; i++ {
		var pk [32]byte
		pk[0] = byte(i + 1)
		pk[1] = byte((i + 1) >> 8)
		acc.addVote(winnerHash, solana.PublicKeyFromBytes(pk[:]), 1)
	}
	// Add 10 stake for our hash (below threshold).
	for i := 0; i < 10; i++ {
		var pk [32]byte
		pk[0] = byte(i + 100)
		acc.addVote(ourHash, solana.PublicKeyFromBytes(pk[:]), 1)
	}
	service.state.voteStakeTotals[slot] = acc
	service.state.latestSlotIngested = slot + 5 // Well below timeout
	service.state.mu.Unlock()

	result := service.IsBankhashCorrect(slot, ourHash)
	assert.Equal(t, BankhashNoSupermajority, result.Status, "mismatch surfaced immediately")
	assert.Equal(t, winnerHash, result.WinningHash, "winning hash should be the other hash")
	assert.Equal(t, uint64(10), result.StakeForHash, "our hash stake")
	assert.Equal(t, uint64(67), result.WinningStake, "winner stake")
}

// TestGetSupermajorityHashEarlyConfirmation verifies that GetSupermajorityHash
// returns the winner immediately, before the timeout window.
func TestGetSupermajorityHashEarlyConfirmation(t *testing.T) {
	epochAuth := epochstakes.NewEpochAuthorizedVotersCache()
	epochStakes := map[solana.PublicKey]uint64{}
	totalStake := uint64(100)
	service := NewForkChoiceService(0, epochStakes, totalStake, epochAuth)


	slot := uint64(10)
	winnerHash := solana.Hash{0xAA}

	service.state.mu.Lock()
	acc := newSlotVoteAccumulator(totalStake, slot)
	for i := 0; i < 67; i++ {
		var pk [32]byte
		pk[0] = byte(i + 1)
		pk[1] = byte((i + 1) >> 8)
		acc.addVote(winnerHash, solana.PublicKeyFromBytes(pk[:]), 1)
	}
	service.state.voteStakeTotals[slot] = acc
	service.state.latestSlotIngested = slot + 5 // Below timeout
	service.state.mu.Unlock()

	hash, status := service.GetSupermajorityHash(slot)
	assert.Equal(t, BankhashHasSupermajority, status, "should return winner immediately")
	assert.Equal(t, winnerHash, hash)
}

// TestGetSupermajorityHashNeedWaitBeforeTimeout verifies that GetSupermajorityHash
// returns NeedWait when no winner exists and the timeout hasn't expired.
func TestGetSupermajorityHashNeedWaitBeforeTimeout(t *testing.T) {
	epochAuth := epochstakes.NewEpochAuthorizedVotersCache()
	epochStakes := map[solana.PublicKey]uint64{}
	totalStake := uint64(100)
	service := NewForkChoiceService(0, epochStakes, totalStake, epochAuth)


	slot := uint64(10)

	// Partial votes, no winner.
	service.state.mu.Lock()
	acc := newSlotVoteAccumulator(totalStake, slot)
	var pk [32]byte
	pk[0] = 1
	acc.addVote(solana.Hash{0xAA}, solana.PublicKeyFromBytes(pk[:]), 30)
	service.state.voteStakeTotals[slot] = acc
	service.state.latestSlotIngested = slot + 10 // Below timeout
	service.state.mu.Unlock()

	hash, status := service.GetSupermajorityHash(slot)
	assert.Equal(t, BankhashNeedWait, status, "no winner + before timeout = NeedWait")
	assert.Equal(t, solana.Hash{}, hash)
}

// TestGetSupermajorityHashNoSupermajorityAfterTimeout verifies that
// GetSupermajorityHash returns NoSupermajority when partial votes exist
// but the timeout has expired without a winner.
func TestGetSupermajorityHashNoSupermajorityAfterTimeout(t *testing.T) {
	epochAuth := epochstakes.NewEpochAuthorizedVotersCache()
	epochStakes := map[solana.PublicKey]uint64{}
	totalStake := uint64(100)
	service := NewForkChoiceService(0, epochStakes, totalStake, epochAuth)

	slot := uint64(10)

	// Partial votes, no winner, timeout expired.
	service.state.mu.Lock()
	acc := newSlotVoteAccumulator(totalStake, slot)
	var pk [32]byte
	pk[0] = 1
	acc.addVote(solana.Hash{0xAA}, solana.PublicKeyFromBytes(pk[:]), 30)
	service.state.voteStakeTotals[slot] = acc
	service.state.latestSlotIngested = slot + VoteConfirmationTimeoutSlots
	service.state.mu.Unlock()

	hash, status := service.GetSupermajorityHash(slot)
	assert.Equal(t, BankhashNoSupermajority, status, "no winner + after timeout = NoSupermajority")
	assert.Equal(t, solana.Hash{}, hash)
}

// TestSubmitBlockCapturesEpochDataBeforeUpdateEpoch verifies that a block
// submitted before UpdateEpoch is processed with the epoch data that was
// current at submission time, not the post-update data. This prevents
// pre-boundary blocks queued before an epoch transition from being parsed
// and weighted with post-boundary stakes/voters.
func TestSubmitBlockCapturesEpochDataBeforeUpdateEpoch(t *testing.T) {
	voterKey := solana.PublicKey{1}
	voteAcct := solana.PublicKey{2}

	// Epoch 0: voterKey authorized for voteAcct, stake=50, total=100.
	epoch0Auth := epochstakes.NewEpochAuthorizedVotersCache()
	epoch0Auth.PutEntry(voteAcct, voterKey)
	epoch0Stakes := map[solana.PublicKey]uint64{voteAcct: 50}
	epoch0Total := uint64(100)

	// Epoch 1: voterKey NOT authorized, different total.
	epoch1Auth := epochstakes.NewEpochAuthorizedVotersCache()
	epoch1Stakes := map[solana.PublicKey]uint64{voteAcct: 75}
	epoch1Total := uint64(200)

	service := NewForkChoiceService(0, epoch0Stakes, epoch0Total, epoch0Auth)


	// Build a vote tx: voterKey votes for slot 50 with hash {0xBB}.
	votedSlot := uint64(50)
	votedHash := solana.Hash{0xBB}
	voteTx := buildTestVoteTx(voteAcct, voterKey, votedSlot, votedHash)

	// Submit the block — captures epoch 0 data in the job.
	// Service is NOT started, so the job sits in the channel.
	service.SubmitBlock(100, []*solana.Transaction{voteTx})

	// Update to epoch 1 BEFORE the job is processed.
	service.UpdateEpoch(1, epoch1Stakes, epoch1Total, epoch1Auth)

	// Drain the job and verify it carries epoch 0 data.
	job := <-service.jobChan
	assert.Equal(t, epoch0Total, job.totalEpochStake, "job should carry epoch 0 total stake")

	// Process the job — should use epoch 0 data from the job.
	service.processBlock(job)

	// The vote should have been accepted (voterKey authorized in epoch 0),
	// and the accumulator should use epoch 0's total stake for threshold.
	service.state.mu.Lock()
	acc, exists := service.state.voteStakeTotals[votedSlot]
	service.state.mu.Unlock()

	assert.True(t, exists, "accumulator should exist — vote authorized in epoch 0")
	assert.Equal(t, epoch0Total, acc.totalEpochStake, "accumulator threshold should use epoch 0 total")
	assert.Equal(t, computeThresholdStake(epoch0Total), acc.thresholdStake)
	assert.Equal(t, uint64(50), acc.stakeForHash(votedHash), "vote weighted with epoch 0 stake")

	// Verify the converse: if the job had used epoch 1 data, the vote would
	// have been rejected (voterKey not in epoch1Auth) and no accumulator created.
	// The existence of the accumulator with epoch 0 weights proves the snapshot.
}

// TestSequentialProcessingDeterminesWinner verifies that block processing order
// determines winner selection when two competing hashes can both independently
// cross the 2/3 threshold. This is a regression test for the ordering fix that
// replaced the concurrent ants pool with sequential inline processing.
//
// The test exercises the full service path: Start() → SubmitBlock() → run() →
// processBlock() → Stop(). Channel FIFO ordering guarantees block 100 is
// processed before block 101, so hashX always crosses the threshold first.
//
// With the old ants pool, thread scheduling determined which block's votes
// applied first, making the winner non-deterministic.
func TestSequentialProcessingDeterminesWinner(t *testing.T) {
	// Two voters, each with enough stake to independently cross the threshold.
	voterA := solana.PublicKey{1}
	voteAcctA := solana.PublicKey{2}
	voterB := solana.PublicKey{3}
	voteAcctB := solana.PublicKey{4}

	totalStake := uint64(100) // threshold = uint64(100 * 2/3) = 66

	epochAuth := epochstakes.NewEpochAuthorizedVotersCache()
	epochAuth.PutEntry(voteAcctA, voterA)
	epochAuth.PutEntry(voteAcctB, voterB)

	epochStakes := map[solana.PublicKey]uint64{
		voteAcctA: 67, // > threshold of 66
		voteAcctB: 67,
	}

	service := NewForkChoiceService(0, epochStakes, totalStake, epochAuth)
	service.Start()

	// Both voters vote for the same slot but different hashes.
	votedSlot := uint64(50)
	hashX := solana.Hash{0xAA}
	hashY := solana.Hash{0xBB}

	// Block at slot 100: voterA votes for (slot 50, hashX) — submitted first.
	txA := buildTestVoteTx(voteAcctA, voterA, votedSlot, hashX)
	service.SubmitBlock(100, []*solana.Transaction{txA})

	// Block at slot 101: voterB votes for (slot 50, hashY) — submitted second.
	txB := buildTestVoteTx(voteAcctB, voterB, votedSlot, hashY)
	service.SubmitBlock(101, []*solana.Transaction{txB})

	// Stop drains all queued jobs before returning.
	service.Stop()

	// latestSlotIngested should have been advanced by processBlock (not manual).
	service.state.mu.Lock()
	latestIngested := service.state.latestSlotIngested
	service.state.mu.Unlock()
	assert.Equal(t, uint64(101), latestIngested, "watermark should advance to 101")

	// hashX must win because block 100 was processed first (channel FIFO).
	resultX := service.IsBankhashCorrect(votedSlot, hashX)
	assert.Equal(t, BankhashHasSupermajority, resultX.Status, "hashX should have supermajority")
	assert.Equal(t, hashX, resultX.WinningHash)

	// hashY loses — a different hash already won supermajority.
	resultY := service.IsBankhashCorrect(votedSlot, hashY)
	assert.Equal(t, BankhashNoSupermajority, resultY.Status, "hashY should be NoSupermajority (mismatch)")
	assert.Equal(t, hashX, resultY.WinningHash, "winning hash should still be hashX")
}

// buildTestVoteTx constructs a minimal valid vote transaction for testing.
// The tx passes IsVote(), IsSigner(authority), and parseAndValidateVoteTx().
func buildTestVoteTx(voteAcct, voteAuthority solana.PublicKey, slot uint64, hash solana.Hash) *solana.Transaction {
	// Encode VoteProgramInstrTypeVote (type=2):
	//   [type:4][num_slots:8][slot:8][hash:32][timestamp_opt:1] = 53 bytes
	data := make([]byte, 53)
	binary.LittleEndian.PutUint32(data[0:4], 2)  // VoteProgramInstrTypeVote
	binary.LittleEndian.PutUint64(data[4:12], 1)  // 1 slot (Rust Vec len is u64)
	binary.LittleEndian.PutUint64(data[12:20], slot)
	copy(data[20:52], hash[:])
	data[52] = 0 // No timestamp

	return &solana.Transaction{
		Message: solana.Message{
			Header: solana.MessageHeader{
				NumRequiredSignatures: 2,
			},
			AccountKeys: []solana.PublicKey{voteAcct, voteAuthority, solana.VoteProgramID},
			Instructions: []solana.CompiledInstruction{
				{
					ProgramIDIndex: 2,
					Accounts:       []uint16{0, 1},
					Data:           solana.Base58(data),
				},
			},
		},
		Signatures: []solana.Signature{{}, {}},
	}
}
