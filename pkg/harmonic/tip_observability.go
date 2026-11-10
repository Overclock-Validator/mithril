package harmonic

import (
	"encoding/binary"

	"github.com/gagliardetto/solana-go"
)

// TipIntentObservation describes decoded ordered-stream transactions, not tip
// payments. It recognizes only top-level System Program Transfer instructions
// whose destination is a configured tip account in the static account keys.
// CPI transfers and destinations loaded through lookup tables are not covered.
// Recorded observations mean the transaction entered the local bank: a failed
// standalone transaction can be recorded without paying its intended tip, and
// the bank itself may later be abandoned. Finalized RPC balances remain the
// source of truth for landed payments.
type TipIntentObservation struct {
	Transactions       uint64
	DirectTipIntents   uint64
	LookupTransactions uint64
}

func (o *TipIntentObservation) add(other TipIntentObservation) {
	o.Transactions += other.Transactions
	o.DirectTipIntents += other.DirectTipIntents
	o.LookupTransactions += other.LookupTransactions
}

// observeTipIntents reads existing decoded data only. It does not resolve
// lookups, fetch accounts, execute instructions, or retain transaction data.
func observeTipIntents(txs []*solana.Transaction, tips [tipAccounts]solana.PublicKey) TipIntentObservation {
	var o TipIntentObservation
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		o.Transactions++
		m := &tx.Message
		staticKeys := len(m.AccountKeys)
		if m.NumLookups() > 0 {
			o.LookupTransactions++
			if m.IsResolved() {
				staticKeys -= m.NumLookups()
			}
		}
		for _, ix := range m.Instructions {
			// System Program Transfer = u32(2), u64(lamports). An intent is
			// counted once per transaction, irrespective of the number of tips.
			if int(ix.ProgramIDIndex) >= staticKeys ||
				m.AccountKeys[ix.ProgramIDIndex] != solana.SystemProgramID ||
				len(ix.Accounts) < 2 || int(ix.Accounts[1]) >= staticKeys ||
				len(ix.Data) != 12 || binary.LittleEndian.Uint32(ix.Data[:4]) != 2 ||
				binary.LittleEndian.Uint64(ix.Data[4:]) == 0 {
				continue
			}
			destination := m.AccountKeys[ix.Accounts[1]]
			if isObservedTipAccount(destination, tips) {
				o.DirectTipIntents++
				break
			}
		}
	}
	return o
}

func isObservedTipAccount(key solana.PublicKey, tips [tipAccounts]solana.PublicKey) bool {
	for _, tip := range tips {
		if !tip.IsZero() && key == tip {
			return true
		}
	}
	return false
}
