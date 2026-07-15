package blockprod

import (
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// LeaderBlockInput captures forged leader state for AccountsDB commit.
type LeaderBlockInput struct {
	Bank             *WorkingBank
	EpochSchedule    *sealevel.SysvarEpochSchedule
	ParentBankhash   solana.Hash
	PrevNumSigs      uint64
	PrevFeeGovernor  *sealevel.FeeRateGovernor
	EntryBlockhash   solana.Hash
	ParentBlockID    solana.Hash
	TxFeeAccumulator fees.TxFeeInfoAccumulator
}

func BuildLeaderBlock(in LeaderBlockInput) *b.Block {
	bank := in.Bank
	slot := bank.Slot()
	parentSlot := bank.SlotCtx().ParentSlot

	block := &b.Block{
		Slot:                slot,
		ParentSlot:          parentSlot,
		SourceParentSlot:    parentSlot,
		Leader:              bank.Leader(),
		Transactions:        bank.ForgedTransactions(),
		Epoch:               in.EpochSchedule.GetEpoch(slot),
		Features:            bank.SlotCtx().Features,
		NumSignatures:       in.PrevNumSigs + bank.NumSignatures(),
		PrevNumSignatures:   in.PrevNumSigs,
		PrevFeeRateGovernor: in.PrevFeeGovernor,
		LastBlockhash:       global.LatestBlockHash(),
		Blockhash:           in.EntryBlockhash,
	}
	if in.ParentBlockID != (solana.Hash{}) {
		block.HasAlpenglowParentBlockID = true
		block.AlpenglowParentBlockID = in.ParentBlockID
	}
	copy(block.ParentBankhash[:], in.ParentBankhash[:])
	block.FeeRateGovernor = sealevel.NewFeeRateGovernorDerived(block.PrevFeeRateGovernor, block.PrevNumSignatures)
	if block.FeeRateGovernor.PrevLamportsPerSignature == 0 {
		block.FeeRateGovernor.PrevLamportsPerSignature = 5000
	}
	block.BlockHeight = global.BlockHeight() + 1
	return block
}
