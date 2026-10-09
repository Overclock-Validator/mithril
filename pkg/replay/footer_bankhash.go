package replay

import (
	"bytes"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/base58"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

// requireAlpenglowBlockFooter mirrors Agave MigrationStatus::should_allow_block_markers
// plus BlockComponentProcessor::on_final requiring has_footer for Alpenglow blocks.
// Locally constructed blocks and synthetic skipped slots are exempt.
func requireAlpenglowBlockFooter(block *b.Block, slotCtx *sealevel.SlotCtx, alpenglowClock bool) bool {
	if !alpenglowClock || block == nil || block.IsSkipped || !block.FromLiveStream {
		return false
	}
	if slotCtx == nil || slotCtx.Features == nil {
		return false
	}
	return alpenglowClockFeatureActive(slotCtx.Features)
}

// verifyAlpenglowBlockFooter enforces footer presence on Alpenglow turbine blocks and
// compares the footer bank hash to the locally computed hash when present.
func verifyAlpenglowBlockFooter(slotCtx *sealevel.SlotCtx, block *b.Block, alpenglowClock bool) error {
	if requireAlpenglowBlockFooter(block, slotCtx, alpenglowClock) {
		if !block.HasAlpenglowFooter {
			return fmt.Errorf("slot %d missing block footer", block.Slot)
		}
		// Validator mode relies on local execution matching the bank hash in
		// the footer committed by the certified double-Merkle block id. A
		// present-but-zero footer must not silently bypass that check.
		if !block.HasExpectedBankhash {
			return fmt.Errorf("slot %d block footer is missing its bank hash", block.Slot)
		}
	}
	return verifyFooterBankHash(slotCtx, block)
}

// verifyFooterBankHash compares the leader footer bank hash against the locally computed hash.
// When block.HasExpectedBankhash is set, a mismatch fails the slot (Agave freeze_and_verify_bank_hash).
func verifyFooterBankHash(slotCtx *sealevel.SlotCtx, block *b.Block) error {
	if block == nil || !block.HasExpectedBankhash {
		return nil
	}
	if slotCtx == nil {
		return fmt.Errorf("slot %d footer bank hash verification: missing slot context", block.Slot)
	}
	if bytes.Equal(block.ExpectedBankhash[:], slotCtx.FinalBankhash) {
		return nil
	}
	return fmt.Errorf(
		"slot %d footer bank hash mismatch: footer=%s computed=%s",
		block.Slot,
		base58.Encode(block.ExpectedBankhash[:]),
		base58.Encode(slotCtx.FinalBankhash),
	)
}
