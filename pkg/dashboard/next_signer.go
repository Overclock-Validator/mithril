package dashboard

import (
	"math"
	"time"

	"github.com/gagliardetto/solana-go"
)

// NextEpochSigner observes an installed engine set, not bank admission or
// permission to sign. Its root remains known when the current set is absent.
type NextEpochSigner struct {
	SampledAt          time.Time `json:"sampled_at"`
	RootSlot           uint64    `json:"root_slot,string"`
	RootEpoch          uint64    `json:"root_epoch,string"`
	Epoch              uint64    `json:"epoch,string"`
	InstalledSet       bool      `json:"installed_set"`
	KeyEligible        *bool     `json:"key_eligible"`
	Rank               *uint16   `json:"rank"`
	StakeLamports      *uint64   `json:"stake_lamports,string"`
	TotalStakeLamports *uint64   `json:"total_stake_lamports,string"`
}

// PublicNextEpochSigner retains raw installed stake and total. The installer
// does not validate their sum or ordering; this observation derives no ratio.
type PublicNextEpochSigner struct {
	PublicObservation
	Provenance         string  `json:"provenance"`
	RootSlot           uint64  `json:"root_slot,string"`
	RootEpoch          uint64  `json:"root_epoch,string"`
	Epoch              uint64  `json:"epoch,string"`
	InstalledSet       bool    `json:"installed_set"`
	KeyEligible        *bool   `json:"key_eligible"`
	Rank               *uint16 `json:"rank"`
	StakeLamports      *uint64 `json:"stake_lamports,string"`
	TotalStakeLamports *uint64 `json:"total_stake_lamports,string"`
}

func publicNextEpochSigner(s *Snapshot, state string, now time.Time) *PublicNextEpochSigner {
	if s.Consensus == nil || !s.Consensus.VotingEnabled || s.Consensus.Voting == nil ||
		s.Consensus.Voting.NextEpochSigner == nil || s.Mode != "validator" || !s.Alpenglow || s.IdentityKey == nil || s.RunID == "" {
		return nil
	}
	c, v := s.Consensus, s.Consensus.Voting
	f := v.NextEpochSigner
	if v.Identity != *s.IdentityKey || s.StartedAt.IsZero() || !validPublicTime(f.SampledAt, now) ||
		!f.SampledAt.Equal(v.SampledAt) || f.SampledAt.Before(s.StartedAt) || f.SampledAt.After(c.SampledAt) ||
		f.SampledAt.After(s.SampledAt.Add(time.Second)) || f.RootEpoch == math.MaxUint64 || f.Epoch != f.RootEpoch+1 ||
		v.RootSlot != nil && *v.RootSlot != f.RootSlot || v.Epoch != nil && *v.Epoch != f.RootEpoch {
		return nil
	}
	for _, key := range []string{v.Identity, v.VoteAccount} {
		parsed, err := solana.PublicKeyFromBase58(key)
		if err != nil || parsed.IsZero() || parsed.String() != key {
			return nil
		}
	}
	if !f.InstalledSet {
		if f.KeyEligible != nil || f.Rank != nil || f.StakeLamports != nil || f.TotalStakeLamports != nil {
			return nil
		}
	} else if f.KeyEligible == nil || f.TotalStakeLamports == nil || *f.TotalStakeLamports == 0 ||
		*f.KeyEligible && (f.Rank == nil || f.StakeLamports == nil || *f.StakeLamports == 0) ||
		!*f.KeyEligible && (f.Rank != nil || f.StakeLamports != nil) {
		return nil
	}
	p := &PublicNextEpochSigner{PublicObservation: publicObservation(f.SampledAt.UTC(), state, now), Provenance: "installed_engine_set",
		RootSlot: f.RootSlot, RootEpoch: f.RootEpoch, Epoch: f.Epoch, InstalledSet: f.InstalledSet}
	if f.InstalledSet {
		eligible, total := *f.KeyEligible, *f.TotalStakeLamports
		p.KeyEligible, p.TotalStakeLamports = &eligible, &total
		if eligible {
			rank, stake := *f.Rank, *f.StakeLamports
			p.Rank, p.StakeLamports = &rank, &stake
		}
	}
	return p
}
