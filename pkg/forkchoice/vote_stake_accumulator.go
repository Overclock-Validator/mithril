package forkchoice

import (
	"github.com/gagliardetto/solana-go"
)

// VoteThresholdSize matches Agave's VOTE_THRESHOLD_SIZE = 2f64 / 3f64.
// See: agave/runtime/src/commitment.rs:9
const VoteThresholdSize = 2.0 / 3.0

// computeThresholdStake computes the threshold using Agave's exact formula:
//
//	threshold_stake = (total_stake as f64 * threshold) as u64
//
// Supermajority is reached when accumulated stake exceeds this value.
// See: agave/core/src/consensus/vote_stake_tracker.rs:30
func computeThresholdStake(totalStake uint64) uint64 {
	return uint64(float64(totalStake) * VoteThresholdSize)
}

// voteStakeTracker tracks per-pubkey vote stake for a single (slot, hash) pair.
// Equivalent to Agave's VoteStakeTracker.
// See: agave/core/src/consensus/vote_stake_tracker.rs
type voteStakeTracker struct {
	voted map[solana.PublicKey]struct{}
	stake uint64
}

// slotVoteAccumulator tracks all hash trackers for a single slot.
// Equivalent to Agave's SlotVoteTracker.optimistic_votes_tracker.
// See: agave/core/src/cluster_info_vote_listener.rs:75
type slotVoteAccumulator struct {
	trackers        map[solana.Hash]*voteStakeTracker
	totalEpochStake uint64
	thresholdStake  uint64
	slot            uint64
	confirmed       bool
	confirmedHash   solana.Hash
}

func newSlotVoteAccumulator(totalEpochStake uint64, slot uint64) *slotVoteAccumulator {
	return &slotVoteAccumulator{
		trackers:        make(map[solana.Hash]*voteStakeTracker),
		totalEpochStake: totalEpochStake,
		thresholdStake:  computeThresholdStake(totalEpochStake),
		slot:            slot,
	}
}

// addVote records a vote for the given hash by votePubkey with the given stake.
// Returns (thresholdCrossed, isNew).
//
// thresholdCrossed is true only on the exact vote that causes the hash to cross
// the 2/3 threshold. Uses Agave crossing semantics:
//
//	old_stake <= threshold_stake && threshold_stake < new_stake
//
// isNew is true if this pubkey had not previously voted for this (slot, hash).
// Dedup is per (slot, hash, pubkey) — same pubkey can vote for different hashes.
//
// See: agave/core/src/consensus/vote_stake_tracker.rs:14-37
func (acc *slotVoteAccumulator) addVote(hash solana.Hash, votePubkey solana.PublicKey, stake uint64) (thresholdCrossed bool, isNew bool) {
	tracker, exists := acc.trackers[hash]
	if !exists {
		tracker = &voteStakeTracker{
			voted: make(map[solana.PublicKey]struct{}),
		}
		acc.trackers[hash] = tracker
	}

	if _, alreadyVoted := tracker.voted[votePubkey]; alreadyVoted {
		return false, false
	}

	tracker.voted[votePubkey] = struct{}{}
	oldStake := tracker.stake
	newStake := oldStake + stake
	tracker.stake = newStake

	crossed := oldStake <= acc.thresholdStake && acc.thresholdStake < newStake
	if crossed && !acc.confirmed {
		acc.confirmed = true
		acc.confirmedHash = hash
	}

	return crossed, true
}

// hasSupermajority returns true if any hash for this slot has reached 2/3 supermajority.
func (acc *slotVoteAccumulator) hasSupermajority() bool {
	return acc.confirmed
}

// hashHasSupermajority returns true if the given specific hash has crossed the 2/3 threshold.
func (acc *slotVoteAccumulator) hashHasSupermajority(hash solana.Hash) bool {
	tracker, exists := acc.trackers[hash]
	if !exists {
		return false
	}
	return tracker.stake > acc.thresholdStake
}

// winningHash returns the hash that crossed the threshold, if any.
func (acc *slotVoteAccumulator) winningHash() (solana.Hash, bool) {
	if !acc.confirmed {
		return solana.Hash{}, false
	}
	return acc.confirmedHash, true
}

// stakeForHash returns the accumulated stake for a specific hash.
func (acc *slotVoteAccumulator) stakeForHash(hash solana.Hash) uint64 {
	tracker, exists := acc.trackers[hash]
	if !exists {
		return 0
	}
	return tracker.stake
}
