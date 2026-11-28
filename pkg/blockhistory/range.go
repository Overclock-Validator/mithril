package blockhistory

import (
	"context"
	"fmt"
)

// MaxBlocksRange is the maximum difference between inclusive range endpoints.
const MaxBlocksRange = 500_000

// Blocks returns produced slots only when every slot has retained, rooted
// coverage. On ErrNotAvailable, unavailable is the first unknown slot, or start
// when the expected root changed. It does not certify transaction payloads.
func (s *Store) Blocks(ctx context.Context, start, end, rooted uint64) (slots []uint64, unavailable uint64, err error) {
	if err := ctx.Err(); err != nil {
		return nil, start, err
	}
	if end < start {
		return []uint64{}, 0, nil
	}
	if end-start > MaxBlocksRange {
		return nil, start, fmt.Errorf("slot range exceeds %d", MaxBlocksRange)
	}
	// ponytail: at most 500001 map probes under one read lock; replace with an
	// ordered index only if measured range traffic delays summary publication.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.rooted.Load() != rooted || end > rooted {
		return nil, start, ErrNotAvailable
	}
	slots = make([]uint64, 0)
	for slot := start; ; slot++ {
		if err := ctx.Err(); err != nil {
			return nil, slot, err
		}
		record, ok := s.persisted[slot]
		if !ok || s.sourceBatch[slot] > rooted {
			return nil, slot, ErrNotAvailable
		}
		if !record.Skipped {
			slots = append(slots, slot)
		}
		if slot == end {
			break
		}
	}
	// setRooted publishes its atomic root before acquiring mu. The read lock
	// protects the maps, but cannot alone freeze the selected root.
	if s.rooted.Load() != rooted {
		return nil, start, ErrNotAvailable
	}
	return slots, 0, nil
}
