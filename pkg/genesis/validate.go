package genesis

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// ValidateProfile accepts the pinned v1 genesis shape, including its exact
// feature accounts and zero-epoch validator states. Rebuilding is also a
// canonicality check on padding, account owners, sysvars and all parameters.
func ValidateProfile(ctx context.Context, g *Genesis) (Config, error) {
	var c Config
	if g == nil {
		return c, fmt.Errorf("nil genesis")
	}
	c.Profile = Profile
	c.CreationTime = g.CreationTime.UTC().Format(time.RFC3339)
	fixed, err := fixedProfile()
	if err != nil {
		return c, err
	}
	if g.EpochSchedule.SlotPerEpoch != fixed.EpochSchedule.SlotPerEpoch {
		c.TestSlotsPerEpoch = g.EpochSchedule.SlotPerEpoch
	}
	reserved := map[solana.PublicKey]bool{}
	for _, a := range fixed.Accounts {
		reserved[a.Pubkey] = true
	}
	rows := map[solana.PublicKey]AccountEntry{}
	stakes := map[solana.PublicKey]AccountEntry{}
	for _, a := range g.Accounts {
		if err := ctx.Err(); err != nil {
			return c, err
		}
		if _, ok := rows[a.Pubkey]; ok {
			return c, fmt.Errorf("duplicate genesis address")
		}
		rows[a.Pubkey] = a
		if reserved[a.Pubkey] {
			continue
		}
		if a.Owner == solana.StakeProgramID {
			s, err := sealevel.UnmarshalStakeState(a.Data)
			if err != nil || s.Status != sealevel.StakeStateV2StatusStake {
				return c, fmt.Errorf("invalid genesis stake account %s", a.Key)
			}
			vote := s.Stake.Stake.Delegation.VoterPubkey
			if _, ok := stakes[vote]; ok {
				return c, fmt.Errorf("v1 requires exactly one bootstrap stake account per validator")
			}
			stakes[vote] = a
		}
	}
	used := map[solana.PublicKey]bool{}
	for _, a := range g.Accounts {
		if reserved[a.Pubkey] || a.Owner != solana.VoteProgramID {
			continue
		}
		vote, err := sealevel.UnmarshalVersionedVoteState(a.Data)
		if err != nil || vote.Type != sealevel.VoteStateVersionV4 || vote.V4.BlsPubkeyCompressed == nil {
			return c, fmt.Errorf("invalid genesis vote account %s", a.Key)
		}
		vs := vote.V4
		authorized, ok := vs.AuthorizedVoters.AuthorizedVoters.Get(0)
		if !ok {
			return c, fmt.Errorf("missing authorized voter at epoch 0")
		}
		stake, ok := stakes[a.Pubkey]
		if !ok {
			return c, fmt.Errorf("vote account %s has no bootstrap stake", a.Key)
		}
		ss, err := sealevel.UnmarshalStakeState(stake.Data)
		if err != nil {
			return c, err
		}
		identity, ok := rows[vs.NodePubkey]
		if !ok {
			return c, fmt.Errorf("missing validator identity account")
		}
		c.Validators = append(c.Validators, Validator{Identity: vs.NodePubkey.String(), VoteAccount: solana.PublicKey(a.Pubkey).String(), StakeAccount: solana.PublicKey(stake.Pubkey).String(), AuthorizedVoter: authorized.String(), AuthorizedWithdrawer: vs.AuthorizedWithdrawer.String(), StakeStaker: ss.Stake.Meta.Authorized.Staker.String(), StakeWithdrawer: ss.Stake.Meta.Authorized.Withdrawer.String(), InflationRewardsCollector: vs.InflationRewardsCollector.String(), BlockRevenueCollector: vs.BlockRevenueCollector.String(), BLSPublicKey: hex.EncodeToString(vs.BlsPubkeyCompressed[:]), IdentityLamports: identity.Lamports, VoteLamports: a.Lamports, StakeLamports: stake.Lamports, InflationCommissionBPS: vs.InflationRewardsCommissionBps, BlockCommissionBPS: vs.BlockRevenueCommissionBps})
		used[identity.Pubkey] = true
		used[a.Pubkey] = true
		used[stake.Pubkey] = true
	}
	for _, a := range g.Accounts {
		if !used[a.Pubkey] && !reserved[a.Pubkey] {
			if a.Owner != solana.SystemProgramID {
				return c, fmt.Errorf("unsupported account %s outside pinned profile", a.Key)
			}
			c.Accounts = append(c.Accounts, FundedAccount{Address: solana.PublicKey(a.Pubkey).String(), Lamports: a.Lamports})
		}
	}
	rebuilt, resolved, err := Build(ctx, c)
	if err != nil {
		return c, err
	}
	actual, err := Encode(g)
	if err != nil {
		return c, err
	}
	want, err := Encode(rebuilt)
	if err != nil {
		return c, err
	}
	if !bytes.Equal(actual, want) {
		return c, fmt.Errorf("genesis does not match the exact %s profile or canonical initial account states", Profile)
	}
	return resolved, nil
}
