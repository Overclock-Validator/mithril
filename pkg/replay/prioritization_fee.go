package replay

import (
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// TransactionPrioritizationFee returns what a replayed transaction contributes
// to recent prioritization fees: its compute unit price and the accounts it
// write-locks. Like Agave's PrioritizationFeeCache, it leaves out simple vote
// transactions, transactions whose compute budget does not sanitize, and
// transactions that request a zero compute unit limit (ok is false).
//
// tx must be a replayed transaction, so its lookup tables are resolved and its
// account locks are valid. writable is appended to and returned.
func TransactionPrioritizationFee(tx *solana.Transaction, f *features.Features, writable []solana.PublicKey) (price uint64, _ []solana.PublicKey, ok bool) {
	if tx == nil || isSimpleVoteTransaction(tx) {
		return 0, writable, false
	}
	instrs, _, metas, err := instrsAndAcctMetasFromTx(tx, f)
	if err != nil {
		return 0, writable, false
	}
	limits, err := sealevel.ComputeBudgetLimitsForTransaction(tx, instrs, f)
	if err != nil || limits.ComputeUnitLimit == 0 {
		return 0, writable, false
	}
	// Program IDs are demoted to read-only unless the upgradeable loader is
	// present, as in instrsAndAcctMetasFromTx.
	demotePrograms := true
	for _, key := range tx.Message.AccountKeys {
		if key == a.BpfLoaderUpgradeableAddr {
			demotePrograms = false
			break
		}
	}
	for _, am := range metas {
		if !isWritable(am, f) || demotePrograms && isProgramID(tx, am.PublicKey) {
			continue
		}
		writable = append(writable, am.PublicKey)
	}
	return limits.ComputeUnitPrice, writable, true
}

func isProgramID(tx *solana.Transaction, key solana.PublicKey) bool {
	for _, ix := range tx.Message.Instructions {
		if int(ix.ProgramIDIndex) < len(tx.Message.AccountKeys) && tx.Message.AccountKeys[ix.ProgramIDIndex] == key {
			return true
		}
	}
	return false
}

// isSimpleVoteTransaction matches Agave: a legacy message with at most two
// signatures and a single vote program instruction.
func isSimpleVoteTransaction(tx *solana.Transaction) bool {
	msg := &tx.Message
	if len(tx.Signatures) >= 3 || msg.GetVersion() != solana.MessageVersionLegacy || len(msg.Instructions) != 1 {
		return false
	}
	idx := int(msg.Instructions[0].ProgramIDIndex)
	return idx < len(msg.AccountKeys) && msg.AccountKeys[idx] == a.VoteProgramAddr
}
