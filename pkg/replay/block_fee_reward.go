package replay

import (
	"fmt"
	"math"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

func recordBlockFeeReward(block *b.Block, slotCtx *sealevel.SlotCtx, accumulated *fees.TxFeeInfoAccumulator) error {
	if block == nil || slotCtx == nil || accumulated == nil || len(block.Transactions) == 0 || accumulated.TotalFees == 0 {
		return nil
	}
	if slotCtx.LamportsBurnt > accumulated.TotalFees {
		return fmt.Errorf("record fee reward at slot %d: burnt fees %d exceed total fees %d", block.Slot, slotCtx.LamportsBurnt, accumulated.TotalFees)
	}
	lamports := accumulated.TotalFees - slotCtx.LamportsBurnt
	if lamports == 0 {
		return nil
	}
	if lamports > math.MaxInt64 {
		return fmt.Errorf("record fee reward at slot %d: reward %d overflows int64", block.Slot, lamports)
	}
	collector, _, err := transactionFeeCollector(block)
	if err != nil {
		return fmt.Errorf("record fee reward at slot %d: resolve collector: %w", block.Slot, err)
	}
	if collector == (solana.PublicKey{}) {
		return nil
	}
	collectorAccount, err := slotCtx.GetAccount(collector)
	if err != nil {
		return fmt.Errorf("record fee reward at slot %d: read collector %s: %w", block.Slot, collector, err)
	}
	reward := rpc.BlockReward{
		Pubkey:      collector,
		Lamports:    int64(lamports),
		PostBalance: collectorAccount.Lamports,
		RewardType:  rpc.RewardTypeFee,
	}
	block.BlockReward = &b.BlockRewardsInfo{Leader: collector, Lamports: lamports, PostBalance: collectorAccount.Lamports}
	for i := range block.Rewards {
		if block.Rewards[i].RewardType == rpc.RewardTypeFee {
			block.Rewards[i] = reward
			return nil
		}
	}
	block.Rewards = append(block.Rewards, reward)
	return nil
}
