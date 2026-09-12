package blockprod

import (
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// LeaderBlockInput captures forged leader state for AccountsDB commit.
type LeaderBlockInput struct {
	Bank                *WorkingBank
	ParentSlot          uint64
	ParentBankhash      solana.Hash
	ParentLastBlockhash solana.Hash
	ParentBlockHeight   uint64
	PrevNumSigs         uint64
	PrevFeeGovernor     *sealevel.FeeRateGovernor
	EntryBlockhash      solana.Hash
	TxFeeAccumulator    fees.TxFeeInfoAccumulator
}

func BuildLeaderBlock(in LeaderBlockInput) *b.Block {
	bank := in.Bank
	slot := bank.Slot()
	block := &b.Block{
		Slot:         slot,
		ParentSlot:   in.ParentSlot,
		Leader:       bank.Leader(),
		Transactions: bank.ForgedTransactions(),
		Epoch:        bank.SlotCtx().Epoch,
		Features:     bank.SlotCtx().Features,
		// Agave resets Bank.signature_count for every child bank.  The parent
		// count is used only to derive this slot's fee governor; it is not part
		// of this slot's bank-hash signature count.
		NumSignatures:       bank.NumSignatures(),
		PrevNumSignatures:   in.PrevNumSigs,
		PrevFeeRateGovernor: in.PrevFeeGovernor,
		LastBlockhash:       in.ParentLastBlockhash,
		Blockhash:           in.EntryBlockhash,
		BlockHeight:         in.ParentBlockHeight + 1,
	}
	copy(block.ParentBankhash[:], in.ParentBankhash[:])
	block.FeeRateGovernor = sealevel.NewFeeRateGovernorDerived(block.PrevFeeRateGovernor, block.PrevNumSignatures)
	if block.FeeRateGovernor.PrevLamportsPerSignature == 0 {
		block.FeeRateGovernor.PrevLamportsPerSignature = 5000
	}
	return block
}
