package replay

import (
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestRecordBlockFeeRewardUsesExecutedBankResult(t *testing.T) {
	global.SetManageLeaderSchedule(true)
	defer global.SetManageLeaderSchedule(false)
	leader := solana.PublicKey{1}
	mem := accounts.NewMemAccounts()
	require.NoError(t, mem.SetAccountWithoutLock(leader, &accounts.Account{Key: leader, Lamports: 1_025}))
	slotCtx := &sealevel.SlotCtx{Slot: 7, Accounts: mem, LamportsBurnt: 25}
	block := &b.Block{Slot: 7, Leader: leader, Features: features.NewFeaturesDefault(), Transactions: []*solana.Transaction{{}}}

	require.NoError(t, recordBlockFeeReward(block, slotCtx, &fees.TxFeeInfoAccumulator{TotalFees: 50}))
	require.Len(t, block.Rewards, 1)
	require.Equal(t, rpc.RewardTypeFee, block.Rewards[0].RewardType)
	require.Equal(t, int64(25), block.Rewards[0].Lamports)
	require.Equal(t, uint64(1_025), block.Rewards[0].PostBalance)
	require.Equal(t, uint64(25), block.BlockReward.Lamports)
}

func TestRecordBlockFeeRewardIgnoresBlockWithoutLeader(t *testing.T) {
	require.NoError(t, recordBlockFeeReward(
		&b.Block{Slot: 8, Features: features.NewFeaturesDefault(), Transactions: []*solana.Transaction{{}}},
		&sealevel.SlotCtx{Slot: 8},
		&fees.TxFeeInfoAccumulator{TotalFees: 50},
	))
}

func TestRecordBlockFeeRewardUsesCustomCollector(t *testing.T) {
	for _, tc := range []struct {
		name           string
		identityLoaded bool
		burn           bool
	}{
		{"identity loaded", true, false},
		{"identity not loaded", false, false},
		{"invalid collector burns fees", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader, vote, collector := solana.PublicKey{201}, solana.PublicKey{202}, solana.PublicKey{203}
			global.SetLeaderSchedule(nil)
			global.PutEpochStakes(987, map[solana.PublicKey]uint64{vote: 1}, map[solana.PublicKey]*epochstakes.VoteAccount{vote: {NodePubkey: leader, BlockRevenueCollector: &collector}}, 1)
			t.Cleanup(func() { global.ClearEpochStakes(987); global.SetLeaderSchedule(nil) })
			f := features.NewFeaturesDefault()
			f.EnableFeature(features.Alpenglow, 0)
			f.EnableFeature(features.CustomCommissionCollector, 0)
			f.EnableFeature(features.RewardFullPriorityFee, 0)
			mem := accounts.NewMemAccounts()
			account := &accounts.Account{Key: collector, Lamports: 10_000_000}
			if tc.burn {
				account.Owner = solana.PublicKey{204}
			}
			require.NoError(t, mem.SetAccountWithoutLock(collector, account))
			if tc.identityLoaded {
				require.NoError(t, mem.SetAccountWithoutLock(leader, &accounts.Account{Key: leader, Lamports: 1_000_000}))
			}
			ctx := &sealevel.SlotCtx{Slot: 9870, Epoch: 987, Features: f, Accounts: mem, ParentAccts: accounts.NewMemAccounts(), AcctMapsMu: new(sync.Mutex), ModifiedAccts: map[solana.PublicKey]bool{}, WritableAccts: map[solana.PublicKey]bool{}}
			rent := sealevel.NewDefaultRentSysvar()
			sysvars, err := sealevel.NewBankSysvars(ctx.Slot, &accounts.Account{Key: sealevel.SysvarRentAddr, Lamports: 1, Data: rent.MustMarshal()})
			require.NoError(t, err)
			require.NoError(t, ctx.PublishBankSysvars(sysvars))
			block := &b.Block{Slot: ctx.Slot, Epoch: ctx.Epoch, Leader: leader, Features: f, Transactions: []*solana.Transaction{{}}}
			accumulated := &fees.TxFeeInfoAccumulator{TotalFees: 50, PriorityFees: 50}
			require.NoError(t, distributeBlockTxFees(ctx, block, accumulated))
			paid, err := ctx.GetAccount(collector)
			require.NoError(t, err)
			wantBalance := uint64(10_000_050)
			if tc.burn {
				wantBalance = 10_000_000
				require.Equal(t, uint64(50), ctx.LamportsBurnt)
			}
			require.Equal(t, wantBalance, paid.Lamports)
			require.NoError(t, recordBlockFeeReward(block, ctx, accumulated))
			if tc.burn {
				require.Empty(t, block.Rewards)
				return
			}
			require.Equal(t, []rpc.BlockReward{{Pubkey: collector, Lamports: 50, PostBalance: paid.Lamports, RewardType: rpc.RewardTypeFee}}, block.Rewards)
			require.Equal(t, collector, block.BlockReward.Leader)
		})
	}
}
