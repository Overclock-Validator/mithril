package rewards

import (
	"math"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/wide"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestCalculateAlpenglowStakePointsUsesDelegatedStakeFraction(t *testing.T) {
	votePubkey := solana.NewWallet().PublicKey()
	voteState := &sealevel.VoteStateVersions{
		Type: sealevel.VoteStateVersionV4,
		V4: sealevel.VoteState4{EpochCredits: []sealevel.EpochCredits{{
			Epoch: 70, Credits: 2_000, PrevCredits: 1_000,
		}}},
	}
	delegation := &sealevel.Delegation{
		VoterPubkey:       votePubkey,
		StakeLamports:     25,
		ActivationEpoch:   math.MaxUint64,
		DeactivationEpoch: math.MaxUint64,
		CreditsObserved:   0,
	}
	mode := RewardCalculationMode{
		FullAlpenglow: true,
		RewardEpochDelegatedStakes: map[solana.PublicKey]uint64{
			votePubkey: 100,
		},
	}

	got := calculateStakePointsAndCredits(
		solana.PublicKey{}, &sealevel.SysvarStakeHistory{}, delegation,
		voteState, nil, 70, mode,
	)

	// 1,000 lamport-denominated credits * 25 / 100 delegated stake.
	require.True(t, got.Points.Eq(wide.Uint128FromUint64(250)))
	require.Equal(t, uint64(2_000), got.NewCreditsObserved)
	require.False(t, got.ForceCreditsUpdateWithSkippedReward)
}

func TestCalculateAlpenglowStakePointsRejectsWrongRewardEpoch(t *testing.T) {
	votePubkey := solana.NewWallet().PublicKey()
	voteState := &sealevel.VoteStateVersions{
		Type: sealevel.VoteStateVersionV4,
		V4: sealevel.VoteState4{EpochCredits: []sealevel.EpochCredits{{
			Epoch: 69, Credits: 2_000, PrevCredits: 1_000,
		}}},
	}
	delegation := &sealevel.Delegation{
		VoterPubkey:       votePubkey,
		StakeLamports:     25,
		ActivationEpoch:   math.MaxUint64,
		DeactivationEpoch: math.MaxUint64,
	}
	got := calculateStakePointsAndCredits(
		solana.PublicKey{}, &sealevel.SysvarStakeHistory{}, delegation,
		voteState, nil, 70, RewardCalculationMode{
			FullAlpenglow: true,
			RewardEpochDelegatedStakes: map[solana.PublicKey]uint64{
				votePubkey: 100,
			},
		},
	)
	require.True(t, got.Points.Eq(wide.Uint128{}))
	require.Equal(t, uint64(0), got.NewCreditsObserved)
}

func TestAlpenglowCommissionSplitPreservesFractionalLamport(t *testing.T) {
	voteState := &sealevel.VoteStateVersions{
		Type: sealevel.VoteStateVersionV4,
		V4:   sealevel.VoteState4{InflationRewardsCommissionBps: 1_234},
	}

	alpenglow := voteCommissionSplit(voteState, 1_000, true, true)
	require.Equal(t, CommissionSplit{
		VoterPortion: 124, StakerPortion: 876, IsSplit: true,
	}, alpenglow)

	tower := voteCommissionSplit(voteState, 1_000, false, true)
	require.Equal(t, CommissionSplit{
		VoterPortion: 123, StakerPortion: 876, IsSplit: true,
	}, tower)
}

func TestZeroCommissionRetainsVotingRewardEntry(t *testing.T) {
	votePubkey := solana.NewWallet().PublicKey()
	voteState := &sealevel.VoteStateVersions{
		Type: sealevel.VoteStateVersionV4,
		V4:   sealevel.VoteState4{InflationRewardsCommissionBps: 0},
	}
	split := voteCommissionSplit(voteState, 1_000, true, true)
	require.Zero(t, split.VoterPortion)

	rewards := make(map[solana.PublicKey]*atomic.Uint64)
	accumulateVotingReward(rewards, votePubkey, voteState, false, split.VoterPortion)
	require.Contains(t, rewards, votePubkey)
	require.Zero(t, rewards[votePubkey].Load())
}

func TestV4CommissionKeepsBasisPointPrecision(t *testing.T) {
	voteState := &sealevel.VoteStateVersions{
		Type: sealevel.VoteStateVersionV4,
		V4:   sealevel.VoteState4{InflationRewardsCommissionBps: 1_234},
	}
	require.Equal(t, uint16(1_234), voteInflationCommissionBPS(voteState, true))
	require.Equal(t, uint16(1_200), voteInflationCommissionBPS(voteState, false))
}

func TestAlpenglowEarnedPointsAreNotCreditsOnly(t *testing.T) {
	creditsObserved := uint64(1_000)
	newCreditsObserved := uint64(2_000)
	mode := RewardCalculationMode{FullAlpenglow: true}

	require.False(t, shouldForceCreditsOnly(
		CalculatedStakePoints{
			Points:             wide.Uint128FromUint64(250),
			NewCreditsObserved: newCreditsObserved,
		},
		1, math.MaxUint64, 70, creditsObserved, mode,
	))

	require.True(t, shouldForceCreditsOnly(
		CalculatedStakePoints{NewCreditsObserved: newCreditsObserved},
		1, math.MaxUint64, 70, creditsObserved, mode,
	))
}

func TestAlpenglowSkippedRewardCreditsRespectStakeActivation(t *testing.T) {
	votePubkey := solana.PublicKey{1}
	voteState := &sealevel.VoteStateVersions{
		Type: sealevel.VoteStateVersionV4,
		V4: sealevel.VoteState4{EpochCredits: []sealevel.EpochCredits{{
			Epoch: 115, Credits: 2_000, PrevCredits: 1_000,
		}}},
	}
	mode := RewardCalculationMode{
		FullAlpenglow:              true,
		RewardEpochDelegatedStakes: map[solana.PublicKey]uint64{votePubkey: 1_000_000},
	}
	for _, tc := range []struct {
		name                     string
		activation, deactivation uint64
		advance                  bool
	}{
		{"fully cooled in rewarded epoch", 103, 114, false},
		{"not yet activating", 116, math.MaxUint64, false},
		{"activating in rewarded epoch", 115, math.MaxUint64, true},
		{"effective fractional reward", 103, math.MaxUint64, true},
		{"still cooling in rewarded epoch", 103, 115, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delegation := &sealevel.Delegation{
				VoterPubkey: votePubkey, StakeLamports: 1,
				ActivationEpoch: tc.activation, DeactivationEpoch: tc.deactivation,
				CreditsObserved: 1_000,
			}
			pcs := calculateStakePointsAndCredits(solana.PublicKey{}, &sealevel.SysvarStakeHistory{},
				delegation, voteState, nil, 115, mode)
			require.True(t, pcs.Points.Eq(wide.Uint128{}))
			require.Equal(t, uint64(2_000), pcs.NewCreditsObserved)
			require.Equal(t, tc.advance, shouldForceCreditsOnly(pcs, 1, tc.activation, 115, 1_000, mode))
		})
	}
}

func TestInactiveStakePreservesExplicitCreditUpdates(t *testing.T) {
	pcs := CalculatedStakePoints{NewCreditsObserved: 2_000, Inactive: true}
	mode := RewardCalculationMode{FullAlpenglow: true}
	require.False(t, shouldForceCreditsOnly(pcs, 1, 103, 115, 1_000, mode))
	require.True(t, shouldForceCreditsOnly(pcs, 0, 103, 115, 1_000, mode), "disabled inflation")
	require.True(t, shouldForceCreditsOnly(pcs, 1, 115, 115, 1_000, mode), "activation epoch")
	pcs.ForceCreditsUpdateWithSkippedReward = true
	require.True(t, shouldForceCreditsOnly(pcs, 1, 103, 115, 3_000, mode), "vote credit rewind")

	// Tower does not inherit Alpenglow's automatic skipped-reward advance.
	pcs.ForceCreditsUpdateWithSkippedReward = false
	pcs.Inactive = false
	require.False(t, shouldForceCreditsOnly(pcs, 1, 103, 115, 1_000, RewardCalculationMode{}))
}

func TestInflationRewardsUseHistoricalSlotTimeTransitions(t *testing.T) {
	schedule := &sealevel.SysvarEpochSchedule{
		SlotsPerEpoch:            54_000,
		LeaderScheduleSlotOffset: 54_000,
	}
	f := features.Features{}
	f.EnableFeature(features.FullInflationVote, 0)
	f.EnableFeature(features.FullInflationEnable, 0)
	f.EnableFeature(features.ReduceSlotTimeTo350ms, 216_000)
	f.EnableFeature(features.ReduceSlotTimeTo300ms, 270_000)
	f.EnableFeature(features.ReduceSlotTimeTo250ms, 378_000)
	f.EnableFeature(features.ReduceSlotTimeTo200ms, 432_000)
	inflation := Inflation{
		Initial: 0.08, Terminal: 0.015, Taper: 0.15,
		FoundationVal: 0.05, FoundationTerm: 7,
	}

	// This is the epoch-70/71 value recorded by Agave on the Alpenglow
	// cluster. Using the snapshot's current 200 ms value for all historical
	// slots instead produces the crashed run's incorrect 13,011,524,481,614.
	require.Equal(t, uint64(13_006_459_537_024), CalculatePreviousEpochInflationRewards(
		schedule, &inflation, 502_227_596_106_177_093,
		71, 70, 157_784_629.968, &f,
	))
}
