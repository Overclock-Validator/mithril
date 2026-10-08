package replay

import (
	"fmt"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// transactionFeeCollector uses the same historical vote account as the leader
// schedule. Current vote state (including in-block collector changes) takes
// effect only when it is captured for a future schedule.
// Agave v4.4.0-alpha.5 runtime/src/bank/fee_distribution.rs:153-182;
// Firedancer src/flamenco/runtime/fd_runtime.c:fd_runtime_deposit_or_burn_fee.
func transactionFeeCollector(block *b.Block) (collector, vote solana.PublicKey, err error) {
	collector = block.Leader
	if !block.Features.IsActive(features.CustomCommissionCollector) {
		if !global.ManageLeaderSchedule() && block.BlockReward != nil {
			collector = block.BlockReward.Leader
		}
		return collector, vote, nil
	}
	voteAccounts := global.EpochStakesVoteAccts(block.Epoch)
	vote, err = leaderVotePubkey(block.Slot, voteAccounts, block.Leader)
	if err != nil {
		return collector, vote, err
	}
	account := voteAccounts[vote]
	if account == nil || account.BlockRevenueCollector == nil {
		return collector, vote, fmt.Errorf("missing historical block revenue collector for vote account %s in leader-schedule epoch %d; restore complete epoch stakes from a snapshot or retained historical state", vote, block.Epoch)
	}
	return *account.BlockRevenueCollector, vote, nil
}

func distributeBlockTxFees(slotCtx *sealevel.SlotCtx, block *b.Block, accumulated *fees.TxFeeInfoAccumulator) error {
	if accumulated.TotalFees == 0 {
		return nil
	}
	collector, vote, err := transactionFeeCollector(block)
	if err != nil {
		return fmt.Errorf("resolve fee collector at slot %d: %w", block.Slot, err)
	}
	slotCtx.LamportsBurnt, err = fees.DistributeTxFees(slotCtx, collector, vote, accumulated)
	return err
}
