package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/bankview"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// newBankView captures a completed bank for RPC: scalars and the shared
// feature set only, with no per-account work and nothing that keeps the slot's
// execution state alive.
func newBankView(block *b.Block, slotCtx *sealevel.SlotCtx, transactionCount uint64) *bankview.View {
	v := &bankview.View{
		Slot:             block.Slot,
		ParentSlot:       block.ParentSlot,
		BlockHeight:      block.BlockHeight,
		TransactionCount: transactionCount,
		BlockID:          solana.Hash(block.AlpenglowBlockID),
		HasBlockID:       block.HasAlpenglowBlockID,
		Blockhash:        solana.Hash(slotCtx.Blockhash),
		Features:         slotCtx.Features,
	}
	if len(slotCtx.FinalBankhash) == 32 {
		v.Bankhash = solana.HashFromBytes(slotCtx.FinalBankhash)
	}
	if slotCtx.FeeRateGovernor != nil {
		v.LamportsPerSignature = slotCtx.FeeRateGovernor.LamportsPerSignature
		v.FeeRateGovernor = *slotCtx.FeeRateGovernor
	}
	v.LatestEvictedBlockhash = solana.Hash(slotCtx.LatestEvictedBlockhash)
	v.EpochStakes = slotCtx.VoteAccts
	v.TotalEpochStake = slotCtx.TotalEpochStake
	return v
}
