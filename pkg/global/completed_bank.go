package global

import "sync/atomic"

// CompletedBank is the last fully replayed bank: its slot, block height and the
// cumulative transaction count including its transactions. Replay publishes
// the three together because Slot, BlockHeight and TransactionCount change at
// different points of a block, so reading them separately mid-block mixes
// two banks.
type CompletedBank struct {
	Slot             uint64
	BlockHeight      uint64
	TransactionCount uint64
}

var completedBank atomic.Pointer[CompletedBank]

func SetCompletedBank(bank CompletedBank) {
	completedBank.Store(&bank)
}

// LastCompletedBank returns false until replay completes or rewinds to a bank.
func LastCompletedBank() (CompletedBank, bool) {
	bank := completedBank.Load()
	if bank == nil {
		return CompletedBank{}, false
	}
	return *bank, true
}

// ResetCompletedBank forgets the published bank (tests).
func ResetCompletedBank() {
	completedBank.Store(nil)
}
