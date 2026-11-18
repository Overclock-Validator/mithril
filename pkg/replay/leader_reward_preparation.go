package replay

import (
	"bytes"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

// LeaderRewardValidationStatus reports whether finalization reused a successful
// background validation. It does not change certificate or account semantics.
type LeaderRewardValidationStatus struct {
	Prepared       bool
	LookupDuration time.Duration
}

const leaderRewardPreparationSlots = 16

type preparedLeaderRewardValidation struct {
	skip, notar []byte
	// Material is immutable and identifies the exact reward epoch, stake
	// generation and shred version. Reloaded material must validate again.
	material  *voteRewardVerifierMaterial
	validated *rewardcerts.ValidatedRewardCert
}

// This cache is separate from replay's most-recent-certificate cache: replay
// continues advancing while the leader prepares a future footer. Only successful
// full validations enter it, and no cryptography runs under its mutex.
type leaderRewardValidationCache struct {
	mu         sync.Mutex
	latestSlot uint64
	entries    map[uint64]preparedLeaderRewardValidation
}

var preparedLeaderRewardValidations leaderRewardValidationCache

func (c *leaderRewardValidationCache) advanceLocked(slot uint64) bool {
	if slot > c.latestSlot {
		c.latestSlot = slot
	}
	if c.latestSlot >= leaderRewardPreparationSlots {
		retiredThrough := c.latestSlot - leaderRewardPreparationSlots
		for pending := range c.entries {
			if pending <= retiredThrough {
				delete(c.entries, pending)
			}
		}
		return slot > retiredThrough
	}
	return true
}

func (c *leaderRewardValidationCache) advance(slot uint64) {
	c.mu.Lock()
	c.advanceLocked(slot)
	c.mu.Unlock()
}

func (c *leaderRewardValidationCache) put(slot uint64, skip, notar []byte, material *voteRewardVerifierMaterial, validated *rewardcerts.ValidatedRewardCert) {
	entry := preparedLeaderRewardValidation{
		skip: bytes.Clone(skip), notar: bytes.Clone(notar),
		material: material, validated: cloneValidatedRewardCert(validated),
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.advanceLocked(slot) {
		return
	}
	if c.entries == nil {
		c.entries = make(map[uint64]preparedLeaderRewardValidation)
	}
	c.entries[slot] = entry
}

func (c *leaderRewardValidationCache) take(slot uint64, skip, notar []byte, material *voteRewardVerifierMaterial) (*rewardcerts.ValidatedRewardCert, bool) {
	c.mu.Lock()
	entry, ok := c.entries[slot]
	if !ok || material == nil || entry.material != material ||
		!bytes.Equal(entry.skip, skip) || !bytes.Equal(entry.notar, notar) {
		c.mu.Unlock()
		return nil, false
	}
	delete(c.entries, slot)
	c.mu.Unlock()
	// Ownership transfers to this one consumer. Neither caller-owned certificate
	// bytes nor previously returned signer maps can mutate a retained entry.
	return entry.validated, true
}

// LeaderRewardCertPreparer adds full certificate verification to the existing
// background builder. Place it inside rewardcerts.Pipeline so the leader never
// waits here. On any preparation error the original bytes are returned, and the
// ordinary finalization verifier remains responsible for accepting/rejecting
// them; preparation cannot silently remove an invalid certificate.
type LeaderRewardCertPreparer struct {
	builder       rewardcerts.CertificateBuilder
	epochSchedule sealevel.SysvarEpochSchedule
	shredVersion  uint16
	cache         *leaderRewardValidationCache
}

func NewLeaderRewardCertPreparer(builder rewardcerts.CertificateBuilder, epochSchedule *sealevel.SysvarEpochSchedule, shredVersion uint16) *LeaderRewardCertPreparer {
	p := &LeaderRewardCertPreparer{builder: builder, shredVersion: shredVersion, cache: &preparedLeaderRewardValidations}
	if epochSchedule != nil {
		p.epochSchedule = *epochSchedule
	}
	return p
}

func (p *LeaderRewardCertPreparer) BuildForLeaderSlot(slot uint64) rewardcerts.RewardCertificates {
	p.cache.advance(slot)
	if p.builder == nil {
		return rewardcerts.RewardCertificates{}
	}
	certs := p.builder.BuildForLeaderSlot(slot)
	// Retain owned bytes even when the builder reuses its output buffers.
	certs.Skip, certs.Notar = bytes.Clone(certs.Skip), bytes.Clone(certs.Notar)
	if (len(certs.Skip) == 0 && len(certs.Notar) == 0) || p.epochSchedule.SlotsPerEpoch == 0 {
		return certs
	}
	rewardSlot, ok := rewardcerts.RewardSlotForLeader(slot)
	if !ok {
		return certs
	}
	epoch := p.epochSchedule.GetEpoch(rewardSlot)
	material, err := loadVoteRewardVerifierMaterial(epoch, p.shredVersion, nil)
	if err != nil {
		return certs
	}
	validated, _, err := rewardcerts.ValidateRewardCertificatesWithVerifier(slot, certs.Skip, certs.Notar, epoch, material.verifier, false)
	if err == nil {
		p.cache.put(slot, certs.Skip, certs.Notar, material, validated)
	}
	return certs
}
