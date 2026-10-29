package accountsdb

import (
	"fmt"
	"runtime"

	"github.com/cockroachdb/pebble/v2"
	"golang.org/x/sync/errgroup"
)

// undoEntry is the index entry a key had before a fold overwrites it.
type undoEntry struct {
	entry AccountIndexEntry
	valid bool // false: the key was absent from the index
}

// undoCaptureMinChunk keeps each worker's range long enough to amortize it.
const undoCaptureMinChunk = 2048

// captureUndoEntries returns every key's current index entry, in key order.
// keys must be sorted. A few workers each look up one contiguous range with
// point reads: unlike iterator seeks these use the bloom filters, so keys the
// fold creates stay cheap, and sorted ranges keep each worker's reads local.
// (Ordered iterators measured slower on a mainnet-sized index.)
//
// It reads the same state as at any moment before the fold's index commit:
// the caller holds foldMu, and nothing else writes the index.
func (db *AccountsDb) captureUndoEntries(keys [][32]byte) ([]undoEntry, error) {
	out := make([]undoEntry, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	workers := max(1, min(max(4, runtime.NumCPU()/2), (len(keys)+undoCaptureMinChunk-1)/undoCaptureMinChunk))
	chunk := (len(keys) + workers - 1) / workers
	var g errgroup.Group
	for start := 0; start < len(keys); start += chunk {
		end := min(start+chunk, len(keys))
		g.Go(func() error {
			for i := start; i < end; i++ {
				val, closer, err := db.Index.Get(keys[i][:])
				if err == pebble.ErrNotFound {
					continue
				}
				if err != nil {
					return err
				}
				entry, err := UnmarshalAcctIdxEntryValue(val)
				closer.Close()
				if err != nil {
					return fmt.Errorf("index entry for %x: %w", keys[i], err)
				}
				out[i] = undoEntry{entry: entry, valid: true}
			}
			return nil
		})
	}
	return out, g.Wait()
}
