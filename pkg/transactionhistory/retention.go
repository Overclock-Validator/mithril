package transactionhistory

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// ErrRetention is retryable resource pressure. It does not poison the store;
// a bounded maintenance step may have removed old candidates before refusal.
var ErrRetention = errors.New("transaction history retention or disk reserve limit")

// Retention bounds live logical entries, not engine disk usage. Slots is a
// window anchored to the newest retained or incoming candidate, not finality.
// MinFreeBytes is checked with one maximum batch of headroom; it is admission
// protection, not a reservation against compaction or other disk users.
type Retention struct {
	Slots        uint64
	MaxBytes     uint64
	MinFreeBytes uint64
}

func (r Retention) validate(limits Limits) error {
	if r.Slots == 0 || r.MaxBytes < limits.MaxBatchBytes || r.MinFreeBytes == 0 || r.MinFreeBytes > math.MaxUint64-limits.MaxBatchBytes {
		return errors.New("invalid transaction history retention")
	}
	return nil
}

// OpenWithRetention enables bounded admission. An existing store over its new
// quota opens with truthful stats; writes must first make room in bounded steps.
// A low physical reserve refuses Open before creating files. Policy validation
// also precedes filesystem creation. Open does not apply a retention policy.
func OpenWithRetention(dir string, genesis [32]byte, limits Limits, retention Retention) (*Store, error) {
	return openStoreWithRetention(dir, genesis, limits, vfs.Default, &retention)
}

// Stats reports live completion declarations, not continuous coverage,
// canonical eligibility, payload integrity, or physical disk consumption.
// FirstSlot and LastSlot are zero when Blocks is zero; slot zero is also valid.
type Stats struct {
	Blocks       uint64
	LogicalBytes uint64
	FirstSlot    uint64
	LastSlot     uint64
}

// Stats uses acknowledged counters and at most two completion-key seeks.
func (s *Store) Stats() (Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return Stats{}, err
	}
	return s.statsLocked()
}

// completionBytes counts the live body/index/completion keys and values. It
// excludes engine framing, WAL, obsolete files and snapshot-pinned versions.
func completionBytes(c completion) uint64 {
	return c.bytes + uint64(c.count)*(45+109+64) + 41 + completionSize
}

func completionAt(iter *pebble.Iterator, limits Limits) (completion, error) {
	key := iter.Key()
	if len(key) != 41 || key[0] != 'c' {
		return completion{}, ErrUnavailable
	}
	id := BlockID{Slot: binary.BigEndian.Uint64(key[1:9])}
	copy(id.Blockhash[:], key[9:])
	return decodeCompletion(iter.Value(), id, limits)
}

func (s *Store) rebuildStats() error {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{'c'}, UpperBound: []byte{'d'}})
	if err != nil {
		return err
	}
	for valid := iter.First(); valid; valid = iter.Next() {
		c, err := completionAt(iter, s.limits)
		if err != nil || s.blocks == math.MaxUint64 || completionBytes(c) > math.MaxUint64-s.bytes {
			return errors.Join(ErrUnavailable, err, iter.Close())
		}
		s.blocks++
		s.bytes += completionBytes(c)
	}
	return errors.Join(iter.Error(), iter.Close())
}

func (s *Store) statsLocked() (Stats, error) {
	stats := Stats{Blocks: s.blocks, LogicalBytes: s.bytes}
	if s.blocks == 0 {
		return stats, nil
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{'c'}, UpperBound: []byte{'d'}})
	if err != nil {
		return Stats{}, err
	}
	for i := range 2 {
		var valid bool
		if i == 0 {
			valid = iter.First()
		} else {
			valid = iter.Last()
		}
		if !valid {
			return Stats{}, errors.Join(ErrUnavailable, iter.Error(), iter.Close())
		}
		c, err := completionAt(iter, s.limits)
		if err != nil {
			return Stats{}, errors.Join(err, iter.Close())
		}
		if i == 0 {
			stats.FirstSlot = c.block.ID.Slot
		} else {
			stats.LastSlot = c.block.ID.Slot
		}
	}
	return stats, errors.Join(iter.Error(), iter.Close())
}

func (s *Store) makeRoomLocked(c completion) error {
	charge := completionBytes(c)
	if s.blocks == math.MaxUint64 || charge > math.MaxUint64-s.bytes {
		return ErrLimit
	}
	if s.retention == nil {
		return nil
	}
	r := s.retention
	if charge > r.MaxBytes {
		return ErrRetention
	}
	stats, err := s.statsLocked()
	if err != nil {
		return err
	}
	newest := max(c.block.ID.Slot, stats.LastSlot)
	var cutoff uint64
	if newest >= r.Slots {
		cutoff = newest - (r.Slots - 1)
	}
	// Deep rewinds behind a retained orphan's window are explicit gaps. This
	// policy does not choose a canonical fork or persist another high-water mark.
	if c.block.ID.Slot < cutoff {
		return ErrRetention
	}
	for removed := 0; ; removed++ {
		if (stats.Blocks == 0 || stats.FirstSlot >= cutoff) && stats.LogicalBytes <= r.MaxBytes-charge {
			return s.checkDiskSpace()
		}
		if removed == 64 {
			return ErrRetention
		}
		// 'd' includes every 'c' key, even candidates at MaxUint64.
		done, err := s.pruneFirstLocked([]byte{'d'})
		if err != nil {
			return err
		}
		if !done {
			return ErrUnavailable
		}
		stats, err = s.statsLocked()
		if err != nil {
			return err
		}
	}
}

func (s *Store) checkDiskSpace() error {
	if s.retention == nil {
		return nil
	}
	return checkHistoryDiskSpace(s.fs, s.dir, s.retention.MinFreeBytes, s.limits.MaxBatchBytes)
}

func checkHistoryDiskSpace(fs vfs.FS, dir string, reserve, headroom uint64) error {
	for {
		usage, err := fs.GetDiskUsage(dir)
		if err == nil {
			if usage.AvailBytes < reserve || usage.AvailBytes-reserve < headroom {
				return ErrRetention
			}
			return nil
		}
		parent := fs.PathDir(dir)
		if !os.IsNotExist(err) || parent == dir {
			return fmt.Errorf("inspect transaction history free space: %w", err)
		}
		dir = parent
	}
}
