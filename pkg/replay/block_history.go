package replay

import (
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

// blockForHistory supplies metadata from the executed parent without changing
// the block's execution context. Unknown non-genesis parents remain unavailable.
func blockForHistory(block *b.Block, parentBlockhash solana.Hash) *b.Block {
	if block.LastBlockhash != ([32]byte{}) || block.Slot == 0 {
		return block
	}
	if parentBlockhash == (solana.Hash{}) {
		return nil
	}
	historyBlock := *block
	historyBlock.LastBlockhash = parentBlockhash
	return &historyBlock
}
