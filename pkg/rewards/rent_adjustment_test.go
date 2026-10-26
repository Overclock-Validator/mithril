package rewards

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/bankhash"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// The fixture holds every other slot effect constant. Both hashes are from
// the captured mainnet incident at slot 460944096.
func TestEpoch1067RentAdjustmentBankHash(t *testing.T) {
	var fixture struct {
		Slot                      uint64              `json:"slot"`
		ParentBankhash            string              `json:"parent_bankhash"`
		Blockhash                 string              `json:"blockhash"`
		ExpectedBankhash          string              `json:"expected_bankhash"`
		OriginalBankhash          string              `json:"original_bankhash"`
		BaseLtHash                []byte              `json:"base_accounts_lt_hash"`
		Account                   *accounts.Account   `json:"account"`
		Rent                      sealevel.SysvarRent `json:"rent"`
		RewardedEpoch             uint64              `json:"rewarded_epoch"`
		CreditsObserved           uint64              `json:"credits_observed"`
		OriginalUpdatedDataSHA256 string              `json:"original_updated_data_sha256"`
	}
	raw, err := os.ReadFile("testdata/epoch1067/rent-adjustment.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &fixture))
	parent := fixture.Account
	stake, err := sealevel.UnmarshalStakeState(parent.Data)
	require.NoError(t, err)
	require.Equal(t, uint64(1_666_240), fixture.Rent.MinimumBalance(uint64(len(parent.Data))))
	require.Equal(t, uint64(2_282_880), stake.Stake.Meta.RentExemptReserve)
	require.Equal(t, uint64(1_764_988_030), stake.Stake.Stake.Delegation.StakeLamports)

	parents := accounts.NewMemAccounts()
	require.NoError(t, parents.SetAccountWithoutLock(parent.Key, parent))
	f := &features.Features{}
	f.EnableFeature(features.AccountsLtHash, 0)
	f.EnableFeature(features.RemoveAccountsDeltaHash, 0)
	for _, adjust := range []bool{false, true} {
		t.Run(fmt.Sprintf("adjust=%t", adjust), func(t *testing.T) {
			spool := NewPartitionedSpoolWriters(t.TempDir(), 460944000, 1)
			require.NoError(t, spool.WriteRecord(&SpoolRecord{
				StakePubkey: parent.Key, StakeLamports: stake.Stake.Stake.Delegation.StakeLamports,
				CreditsObserved: fixture.CreditsObserved,
			}))
			require.NoError(t, spool.Close())
			updated, _, paid, burned, _ := DistributeStakingRewardsFromSpool(
				&accountsdb.AccountsDb{}, spool.SpoolDir(), spool.Slot(), 0, fixture.Slot,
				func(solana.PublicKey) (*accounts.Account, error) { return parent, nil }, nil,
				StakeRewardDistributionConfig{AdjustDelegationsForRent: adjust, Rent: fixture.Rent, RewardedEpoch: fixture.RewardedEpoch})
			require.Zero(t, paid)
			require.Zero(t, burned)
			require.Len(t, updated, 1)
			require.Equal(t, parent.Lamports, updated[0].Lamports)
			ctx := &sealevel.SlotCtx{Features: f, ParentAccts: parents, AcctsLtHash: new(lthash.LtHash).InitWithHash(fixture.BaseLtHash)}
			hash := bankhash.CalculateBankHash(ctx, nil, updated, solana.MustHashFromBase58(fixture.ParentBankhash), 0, solana.MustHashFromBase58(fixture.Blockhash))
			if adjust {
				require.Equal(t, fixture.ExpectedBankhash, solana.HashFromBytes(hash).String())
			} else {
				require.Equal(t, fixture.OriginalUpdatedDataSHA256, fmt.Sprintf("%x", sha256.Sum256(updated[0].Data)))
				require.Equal(t, fixture.OriginalBankhash, solana.HashFromBytes(hash).String())
			}
		})
	}
}

func TestDistributeStakingRewardsAdjustsDelegationForRent(t *testing.T) {
	// Mainnet's current minimum for a 200-byte stake is 1,666,240. The
	// metadata's historical 2,282,880 reserve must not determine this cap.
	rent := sealevel.SysvarRent{LamportsPerUint8Year: 5080, ExemptionThreshold: 1}
	const minimum = uint64(1_666_240)
	for _, tc := range []struct {
		name                        string
		balance, delegation, reward uint64
		wantStake, wantDeactivation uint64
		disabled, overflow          bool
	}{
		{name: "mainnet credits only", balance: 1_546_390_650, delegation: 1_764_988_030, wantStake: 1_544_724_410, wantDeactivation: math.MaxUint64},
		{name: "gate inactive", balance: 1_546_390_650, delegation: 1_764_988_030, wantStake: 1_764_988_030, wantDeactivation: math.MaxUint64, disabled: true},
		{name: "reward included before cap", balance: minimum + 100, delegation: 200, reward: 50, wantStake: 150, wantDeactivation: math.MaxUint64},
		{name: "fully funded", balance: minimum + 100, delegation: 100, reward: 50, wantStake: 150, wantDeactivation: math.MaxUint64},
		{name: "excess balance stays undelegated", balance: minimum + 1000, delegation: 100, reward: 50, wantStake: 150, wantDeactivation: math.MaxUint64},
		{name: "below rent", balance: minimum - 100, delegation: 200, wantStake: 0, wantDeactivation: 1066},
		{name: "reward below rent", balance: minimum - 100, delegation: 200, reward: 50, wantStake: 0, wantDeactivation: 1066},
		{name: "reward reaches rent", balance: minimum - 100, delegation: 200, reward: 100, wantStake: 0, wantDeactivation: 1066},
		{name: "reward exceeds rent", balance: minimum - 100, delegation: 200, reward: 150, wantStake: 50, wantDeactivation: math.MaxUint64},
		{name: "already zero preserves deactivation", balance: minimum - 1, delegation: 0, wantStake: 0, wantDeactivation: math.MaxUint64},
		{name: "overflow burns reward", balance: math.MaxUint64, delegation: 100, reward: 1, overflow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := solana.PublicKey{41}
			stake := &sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake}
			stake.Stake.Meta.RentExemptReserve = 2_282_880
			stake.Stake.Stake.Delegation = sealevel.Delegation{
				StakeLamports: tc.delegation, ActivationEpoch: 1066, DeactivationEpoch: math.MaxUint64,
			}
			stake.Stake.Stake.CreditsObserved = 7
			stake.Stake.StakeFlags.Bits = 1
			parent := &accounts.Account{Key: key, Owner: addresses.StakeProgramAddr, Lamports: tc.balance, Data: make([]byte, 200)}
			require.NoError(t, sealevel.MarshalStakeStakeInto(stake, parent.Data))
			original := parent.Clone()
			spool := NewPartitionedSpoolWriters(t.TempDir(), 460944000, 1)
			require.NoError(t, spool.WriteRecord(&SpoolRecord{
				StakePubkey: key, StakeLamports: tc.delegation, RewardLamports: tc.reward, CreditsObserved: 9,
			}))
			require.NoError(t, spool.Close())
			updated, parents, distributed, burned, _ := DistributeStakingRewardsFromSpool(
				&accountsdb.AccountsDb{}, spool.SpoolDir(), spool.Slot(), 0, 460944096,
				func(pubkey solana.PublicKey) (*accounts.Account, error) {
					require.Equal(t, key, pubkey)
					return parent, nil
				}, nil, StakeRewardDistributionConfig{AdjustDelegationsForRent: !tc.disabled, Rent: rent, RewardedEpoch: 1066})
			require.Equal(t, original, parent, "distribution must not mutate the retained parent")
			require.Equal(t, original, parents[0])
			if tc.overflow {
				require.Nil(t, updated[0])
				require.Zero(t, distributed)
				require.Equal(t, tc.reward, burned)
				return
			}
			require.Zero(t, burned)
			require.Equal(t, tc.reward, distributed)
			require.Equal(t, tc.balance+tc.reward, updated[0].Lamports)
			got, err := sealevel.UnmarshalStakeState(updated[0].Data)
			require.NoError(t, err)
			require.Equal(t, tc.wantStake, got.Stake.Stake.Delegation.StakeLamports)
			require.Equal(t, tc.wantDeactivation, got.Stake.Stake.Delegation.DeactivationEpoch)
			require.Equal(t, uint64(9), got.Stake.Stake.CreditsObserved)
			require.Equal(t, stake.Stake.Meta, got.Stake.Meta)
			require.Equal(t, stake.Stake.StakeFlags, got.Stake.StakeFlags)
		})
	}
}
