package replay

import (
	"context"
	"fmt"
	"path/filepath"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

// spooledDecodeAhead is how many decoded blocks may wait for the replay.
// Decoding a block is several times cheaper than replaying it, so one decoder
// a few blocks ahead keeps the replay from ever waiting.
const spooledDecodeAhead = 4

// SpooledBlock is one decoded block, or the reason it could not be decoded.
type SpooledBlock struct {
	Block *b.Block
	Err   error
}

// DecodeSpooledBlocksAhead decodes the given slots' blocks from this node's
// spooled shred files in dir, in order, while the caller replays earlier
// ones: at most ahead decoded blocks wait unread. The channel yields one
// entry per slot and is closed after the last, after the first error, or when
// ctx is cancelled. The blocks come from the node's own spool, so shred and
// transaction signatures are not checked again.
func DecodeSpooledBlocksAhead(ctx context.Context, dir string, slots []uint64, ahead int) <-chan SpooledBlock {
	out := make(chan SpooledBlock, max(1, ahead))
	go func() {
		defer close(out)
		for _, slot := range slots {
			var next SpooledBlock
			packets, err := turbine.ReadSpoolFile(filepath.Join(dir, turbine.SpoolFileName(slot)))
			if err == nil {
				next.Block, err = turbine.BlockFromSpooledPackets(slot, packets)
			}
			if err != nil {
				next = SpooledBlock{Err: fmt.Errorf("decode spooled slot %d: %w", slot, err)}
			}
			select {
			case out <- next:
			case <-ctx.Done():
				return
			}
			if next.Err != nil {
				return
			}
		}
	}()
	return out
}
