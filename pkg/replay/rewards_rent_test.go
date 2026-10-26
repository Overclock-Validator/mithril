package replay

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestRewardDistributionUsesBankRentAndFeatureGate(t *testing.T) {
	for _, tc := range []struct {
		name           string
		active, staged bool
		balance, want  uint64
	}{
		{name: "parent bank rent", active: true, balance: 2000, want: 1344},
		{name: "staged rent overrides parent", active: true, staged: true, balance: 2000, want: 1016},
		{name: "zero delegation deactivates in rewarded epoch", active: true, balance: 500},
		{name: "inactive gate needs no rent", balance: 2000, want: 5000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldCache := sealevel.SysvarCache
			t.Cleanup(func() { sealevel.SysvarCache = oldCache })
			staleRent := sealevel.NewDefaultRentSysvar()
			sealevel.SysvarCache.Rent.Sysvar = &staleRent
			f := &features.Features{}
			if tc.active {
				f.EnableFeature(features.RelaxPostExecMinBalanceCheck, 0)
			}
			key := solana.PublicKey{49}
			stake := &sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake}
			stake.Stake.Stake.Delegation.StakeLamports = 5000
			stake.Stake.Stake.Delegation.DeactivationEpoch = math.MaxUint64
			account := &accounts.Account{Key: key, Lamports: tc.balance, Data: make([]byte, 200)}
			require.NoError(t, sealevel.MarshalStakeStakeInto(stake, account.Data))
			epochRewards := &accounts.Account{Key: sealevel.SysvarEpochRewardsAddr, Lamports: 1, Data: encodeEpochRewardsForTest(t, sealevel.SysvarEpochRewards{
				Active: true, DistributionStartingBlockHeight: 100, NumPartitions: 1,
			})}
			rent := sealevel.SysvarRent{LamportsPerUint8Year: 2, ExemptionThreshold: 1}
			bankAccounts := []*accounts.Account{epochRewards}
			if tc.active {
				bankAccounts = append(bankAccounts, &accounts.Account{Key: sealevel.SysvarRentAddr, Lamports: 1, Data: rent.MustMarshal()})
			}
			bank, err := sealevel.NewBankSysvars(460944095, bankAccounts...)
			require.NoError(t, err)
			parent := &sealevel.SlotCtx{Slot: 460944095}
			require.NoError(t, parent.PublishBankSysvars(bank))
			staged := []*accounts.Account{account}
			if tc.staged {
				rent.LamportsPerUint8Year = 3
				staged = append(staged, &accounts.Account{Key: sealevel.SysvarRentAddr, Lamports: 1, Data: rent.MustMarshal()})
			}
			spool := rewards.NewPartitionedSpoolWriters(t.TempDir(), 460944000, 1)
			require.NoError(t, spool.WriteRecord(&rewards.SpoolRecord{StakePubkey: key, StakeLamports: 5000, CreditsObserved: 9}))
			require.NoError(t, spool.Close())
			info := &rewards.PartitionedRewardDistributionInfo{SpoolDir: spool.SpoolDir(), SpoolSlot: spool.Slot(), NumRewardPartitionsRemaining: 1}
			ctx := &ReplayCtx{CurrentFeatures: f, Capitalization: 12345}
			updated, _, _ := distributePartitionedEpochRewardsForSlot(&accountsdb.AccountsDb{RootedDurable: true}, parent, staged, ctx, info, 460944096, 100, 1067)
			require.Len(t, updated, 2)
			got, err := sealevel.UnmarshalStakeState(updated[0].Data)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Stake.Stake.Delegation.StakeLamports)
			if tc.active && tc.want == 0 {
				require.Equal(t, uint64(1066), got.Stake.Stake.Delegation.DeactivationEpoch)
			}
			require.Equal(t, tc.balance, updated[0].Lamports)
			require.Equal(t, uint64(12345), ctx.Capitalization)
			require.False(t, decodeEpochRewardsForTest(t, updated[1].Data).Active)
		})
	}
}
