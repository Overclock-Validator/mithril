package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
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
	block := &b.Block{Slot: 7, Leader: leader, Transactions: []*solana.Transaction{{}}}

	require.NoError(t, recordBlockFeeReward(block, slotCtx, &fees.TxFeeInfoAccumulator{TotalFees: 50}))
	require.Len(t, block.Rewards, 1)
	require.Equal(t, rpc.RewardTypeFee, block.Rewards[0].RewardType)
	require.Equal(t, int64(25), block.Rewards[0].Lamports)
	require.Equal(t, uint64(1_025), block.Rewards[0].PostBalance)
	require.Equal(t, uint64(25), block.BlockReward.Lamports)
}

func TestRecordBlockFeeRewardIgnoresBlockWithoutLeader(t *testing.T) {
	require.NoError(t, recordBlockFeeReward(
		&b.Block{Slot: 8, Transactions: []*solana.Transaction{{}}},
		&sealevel.SlotCtx{Slot: 8},
		&fees.TxFeeInfoAccumulator{TotalFees: 50},
	))
}
