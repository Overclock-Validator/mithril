package scheduler

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

func scoreTransaction(tx *solana.Transaction, feats *features.Features) (reward uint64, messageHash [32]byte, err error) {
	if tx == nil {
		return 0, messageHash, fmt.Errorf("nil transaction")
	}
	messageHash, err = replay.TransactionMessageHash(tx)
	if err != nil {
		return 0, messageHash, err
	}
	instrs, err := instructionsForScoring(tx)
	if err != nil {
		return 0, messageHash, err
	}
	limits, err := sealevel.ComputeBudgetLimitsForTransaction(tx, instrs, feats)
	if err != nil {
		return 0, messageHash, err
	}
	feeInfo := fees.CalculateTxFees(tx, instrs, limits, feats)
	return fees.LeaderReward(feeInfo), messageHash, nil
}

func instructionsForScoring(tx *solana.Transaction) ([]sealevel.Instruction, error) {
	out := make([]sealevel.Instruction, 0, len(tx.Message.Instructions))
	for _, compiled := range tx.Message.Instructions {
		programID, err := tx.ResolveProgramIDIndex(compiled.ProgramIDIndex)
		if err != nil {
			return nil, err
		}
		out = append(out, sealevel.Instruction{
			ProgramId: programID,
			Data:      compiled.Data,
		})
	}
	return out, nil
}
