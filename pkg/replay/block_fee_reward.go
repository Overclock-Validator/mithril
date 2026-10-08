package replay

import (
	"fmt"
	"math"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

func recordBlockFeeReward(block *b.Block, slotCtx *sealevel.SlotCtx, accumulated *fees.TxFeeInfoAccumulator) error {
	if block == nil || slotCtx == nil || accumulated == nil || len(block.Transactions) == 0 || accumulated.TotalFees == 0 {
		return nil
	}
	leader := block.Leader
	if !global.ManageLeaderSchedule() && block.BlockReward != nil {
		leader = block.BlockReward.Leader
	}
	if leader == (solana.PublicKey{}) {
		return nil
	}
	if slotCtx.LamportsBurnt > accumulated.TotalFees {
		return fmt.Errorf("record fee reward at slot %d: burnt fees %d exceed total fees %d", block.Slot, slotCtx.LamportsBurnt, accumulated.TotalFees)
	}
	lamports := accumulated.TotalFees - slotCtx.LamportsBurnt
	if lamports > math.MaxInt64 {
		return fmt.Errorf("record fee reward at slot %d: reward %d overflows int64", block.Slot, lamports)
	}
	leaderAccount, err := slotCtx.GetAccount(leader)
	if err != nil {
		return fmt.Errorf("record fee reward at slot %d: read leader %s: %w", block.Slot, leader, err)
	}
	reward := rpc.BlockReward{
		Pubkey:      leader,
		Lamports:    int64(lamports),
		PostBalance: leaderAccount.Lamports,
		RewardType:  rpc.RewardTypeFee,
	}
	block.BlockReward = &b.BlockRewardsInfo{Leader: leader, Lamports: lamports, PostBalance: leaderAccount.Lamports}
	for i := range block.Rewards {
		if block.Rewards[i].RewardType == rpc.RewardTypeFee {
			block.Rewards[i] = reward
			return nil
		}
	}
	block.Rewards = append(block.Rewards, reward)
	return nil
}
