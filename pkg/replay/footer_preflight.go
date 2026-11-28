package replay

import (
	"fmt"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// quarantineMissingAlpenglowFooter checks a complete network candidate, never a
// streaming header/shell. Alpenglow requires a footer even if the leader ended
// the shred stream without one. Reject that exact candidate before ObserveBlock
// or bank finalization, so a later skip or alternate branch can resolve the slot.
// Any speculative execution must be undone before invalidating the source.
// A quarantine failure is fatal: continuing would leave the invalid candidate
// eligible for redelivery or ignore a contradiction with certified state.
func quarantineMissingAlpenglowFooter(block *b.Block, streamer *streamingExecutor, quarantine func(*b.Block) error) (bool, error) {
	if block == nil || block.IsSkipped || !block.FromLiveStream || block.FromLocalProduction || block.HasAlpenglowFooter {
		return false, nil
	}
	streamer.discardSlot(block.Slot, "missing_footer")
	if err := quarantine(block); err != nil {
		return true, fmt.Errorf("slot %d missing block footer; could not quarantine Alpenglow candidate: %w", block.Slot, err)
	}
	mlog.Log.Warnf("replay: slot %d missing block footer; exact Alpenglow candidate quarantined before ObserveBlock/voting", block.Slot)
	return true, nil
}
