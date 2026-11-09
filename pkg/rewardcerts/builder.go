package rewardcerts

import (
	"sync"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
)

// rewardVoteFutureWindow bounds unactionable, network-controlled vote slots.
// A live producer only needs the N-SlotsForReward window; the larger cushion
// tolerates modest replay/receiver skew without allowing an unbounded map.
const rewardVoteFutureWindow = 512

type BuilderConfig struct {
	MaxValidators int
	RootSlot      func() uint64
	// BeforeBuild publishes any lazily verified votes for rewardSlot. It runs
	// before Builder takes its lock, so the callback may synchronously AddVote.
	BeforeBuild func(rewardSlot uint64)
}

func DefaultBuilderConfig() BuilderConfig {
	return BuilderConfig{MaxValidators: alpenglow.MaximumVATValidators}
}

// Builder accumulates skip/notar votes and produces footer reward certificates.
type Builder struct {
	mu            sync.Mutex
	maxValidators int
	rootSlotFn    func() uint64
	beforeBuild   func(uint64)
	rootSlot      uint64
	votes         map[uint64]slotEntry
}

func NewBuilder(cfg BuilderConfig) *Builder {
	maxValidators := cfg.MaxValidators
	if maxValidators <= 0 {
		maxValidators = alpenglow.MaximumVATValidators
	}
	return &Builder{
		maxValidators: maxValidators,
		rootSlotFn:    cfg.RootSlot,
		beforeBuild:   cfg.BeforeBuild,
		votes:         make(map[uint64]slotEntry),
	}
}

func (b *Builder) pruneForRootLocked(rootSlot uint64) {
	if rootSlot == b.rootSlot {
		return
	}
	b.rootSlot = rootSlot
	cutoff := rootSlot
	if cutoff > SlotsForReward {
		cutoff -= SlotsForReward
	} else {
		cutoff = 0
	}
	for slot := range b.votes {
		if slot < cutoff {
			delete(b.votes, slot)
		}
	}
}

// SetRootSlot drops vote state older than the reward window.
func (b *Builder) SetRootSlot(rootSlot uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pruneForRootLocked(rootSlot)
}

// AddVote records an observed Alpenglow skip or notarization vote.
func (b *Builder) AddVote(msg alpenglow.VoteMessage) {
	b.addVote(msg, nil)
}

// AddVerifiedVote consumes a vote admitted by consensus. When verification
// retained the signature point, reuse that copied point instead of decoding
// and subgroup-checking the same signature again. Votes without that provenance
// take the full decoding path; footer verification is unchanged.
func (b *Builder) AddVerifiedVote(vote alpenglow.VerifiedVote) {
	if signature, ok := vote.VerifiedSignature(); ok {
		b.addVote(vote.Message, &signature)
		return
	}
	b.AddVote(vote.Message)
}

func (b *Builder) addVote(msg alpenglow.VoteMessage, signature *bls12381.G2Affine) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rootSlotFn != nil {
		root := b.rootSlotFn()
		b.pruneForRootLocked(root)
		if msg.Vote.Slot > root && msg.Vote.Slot-root > rewardVoteFutureWindow {
			return
		}
	}
	slot := msg.Vote.Slot
	entry, ok := b.votes[slot]
	if !ok {
		entry = newSlotEntry(b.maxValidators)
	}
	if !entry.wantsVote(msg) {
		return
	}
	var err error
	if signature == nil {
		err = entry.addVote(msg)
	} else {
		err = entry.addVerifiedSignature(msg, *signature)
	}
	if err != nil {
		return
	}
	b.votes[slot] = entry
}

// BuildForLeaderSlot returns wincode-encoded footer certs for the slot reward
// window ending at leaderSlot.
func (b *Builder) BuildForLeaderSlot(leaderSlot uint64) RewardCertificates {
	rewardSlot, ok := rewardSlotForLeader(leaderSlot)
	if !ok {
		return RewardCertificates{}
	}
	if b.beforeBuild != nil {
		b.beforeBuild(rewardSlot)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.dropBefore(rewardSlot)

	entry, ok := b.votes[rewardSlot]
	if !ok {
		return RewardCertificates{}
	}
	delete(b.votes, rewardSlot)
	certs, err := entry.build(rewardSlot)
	if err != nil {
		return RewardCertificates{}
	}
	return certs
}

func (b *Builder) dropBefore(slot uint64) {
	for key := range b.votes {
		if key < slot {
			delete(b.votes, key)
		}
	}
}

func rewardSlotForLeader(leaderSlot uint64) (uint64, bool) {
	if leaderSlot < SlotsForReward {
		return 0, false
	}
	return leaderSlot - SlotsForReward, true
}

// RewardSlotForLeader returns the vote slot whose skip/notar cert a leader embeds.
func RewardSlotForLeader(leaderSlot uint64) (uint64, bool) {
	return rewardSlotForLeader(leaderSlot)
}
