package rewardcerts

import (
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
)

type BuilderConfig struct {
	MaxValidators int
	RootSlot      func() uint64
}

func DefaultBuilderConfig() BuilderConfig {
	return BuilderConfig{MaxValidators: alpenglow.CertificateBitmapCapacity}
}

// Builder accumulates skip/notar votes and produces footer reward certificates.
type Builder struct {
	mu            sync.Mutex
	maxValidators int
	rootSlotFn    func() uint64
	rootSlot      uint64
	votes         map[uint64]slotEntry
}

func NewBuilder(cfg BuilderConfig) *Builder {
	maxValidators := cfg.MaxValidators
	if maxValidators <= 0 {
		maxValidators = alpenglow.CertificateBitmapCapacity
	}
	return &Builder{
		maxValidators: maxValidators,
		rootSlotFn:    cfg.RootSlot,
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
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rootSlotFn != nil {
		b.pruneForRootLocked(b.rootSlotFn())
	}
	slot := msg.Vote.Slot
	entry, ok := b.votes[slot]
	if !ok {
		entry = newSlotEntry(b.maxValidators)
	} else if !entry.wantsVote(msg) {
		return
	}
	if err := entry.addVote(msg); err != nil {
		return
	}
	b.votes[slot] = entry
}

// BuildForLeaderSlot returns wincode-encoded footer certs for the slot reward
// window ending at leaderSlot.
func (b *Builder) BuildForLeaderSlot(leaderSlot uint64) RewardCertificates {
	b.mu.Lock()
	defer b.mu.Unlock()

	rewardSlot, ok := rewardSlotForLeader(leaderSlot)
	if !ok {
		return RewardCertificates{}
	}
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
