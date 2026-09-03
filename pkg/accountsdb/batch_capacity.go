package accountsdb

import (
	"errors"
	"fmt"
	"sync/atomic"
)

var ErrAccountIndexMutationCapacity = errors.New("accountsdb: account-index cannot encode a bounded mutation transaction")

// ErrFoldCommitDecided means the segment manifest is durable and recovery
// must finish this batch. The running process must stop serving/folding rather
// than treating the operation like an undecided retry: some advisory or index
// side effects may already have landed.
var ErrFoldCommitDecided = errors.New("accountsdb: fold manifest is durable; restart recovery is required")

func foldCommitDecidedError(batchSeq, throughSlot uint64, stage string, cause error) error {
	return fmt.Errorf(
		"%w: batch=%d through_slot=%d stage=%s: %w",
		ErrFoldCommitDecided,
		batchSeq,
		throughSlot,
		stage,
		cause,
	)
}

// MaxBatchAccountIndexMutations returns the preferred number of distinct
// account keys in one physical index frame. Zero means that the selected
// (legacy/test) index does not advertise a tighter bound.
//
// Replay uses this target to split ordinary rooted prefixes at slot boundaries.
// CommitBatch remains authoritative and encodes an indivisible oversized slot
// as several bounded frames under one logical pending-fold publication.
func (db *AccountsDb) MaxBatchAccountIndexMutations() uint64 {
	if db == nil || db.ProductionIndex == nil || db.ProductionIndex.mutable == nil {
		return 0
	}
	return db.ProductionIndex.mutable.maxAccountFrameMutations()
}

func (idx *ShardedMutableAccountIndex) maxAccountFrameMutations() uint64 {
	if idx == nil || idx.config.BytesPerKey == 0 {
		return 0
	}
	return min(
		idx.maxFrameMutations(),
		idx.config.MaxHotKeys,
		idx.config.MaxHotBytes/idx.config.BytesPerKey,
	)
}

func (db *AccountsDb) validateBatchAccountIndexMutations(count uint64) error {
	limit := db.MaxBatchAccountIndexMutations()
	if db != nil && db.ProductionIndex != nil && limit == 0 && count != 0 {
		return fmt.Errorf(
			"%w: production index cannot encode any mutations",
			ErrAccountIndexMutationCapacity,
		)
	}
	return nil
}

func (db *AccountsDb) accountIndexMutationFrameCount(count uint64) uint64 {
	limit := db.MaxBatchAccountIndexMutations()
	if count == 0 || limit == 0 {
		return 1
	}
	return 1 + (count-1)/limit
}

func updateAtomicMaximum(value *atomic.Uint64, candidate uint64) {
	for current := value.Load(); candidate > current; current = value.Load() {
		if value.CompareAndSwap(current, candidate) {
			return
		}
	}
}

// applyAccountIndexMutationTransaction durably writes an arbitrarily large
// logical mutation transaction as bounded WAL frames. Only the final frame
// carries meta, which is its recovery commit marker. Replaying the full
// transaction after a crash is idempotent because every mutation is an exact
// newest-wins assignment.
//
// Replay plans ordinary CommitBatch calls to one frame, but an indivisible
// oversized slot may use this path live. pendingFold masks its changed keys;
// range-snapshot capture, recovery and rewind serialize on
// accountIndexWriteMu. The range's disk scan and visitor run after releasing
// that fence. Retrying the whole decided operation after a crash is safe
// because assignments are exact and idempotent.
func (db *AccountsDb) applyAccountIndexMutationTransaction(
	mutations []deltaIndexMutation,
	meta *foldMeta,
) error {
	if db == nil {
		return errors.New("accountsdb: nil database")
	}
	db.accountIndexWriteMu.Lock()
	defer db.accountIndexWriteMu.Unlock()
	limit := db.MaxBatchAccountIndexMutations()
	if limit == 0 || uint64(len(mutations)) <= limit {
		return db.applyAccountIndexMutationsLocked(mutations, meta)
	}
	if limit > uint64(maxInt) {
		limit = uint64(maxInt)
	}
	chunkSize := int(limit)
	if chunkSize == 0 {
		return fmt.Errorf("%w: selected index advertises a zero mutation limit", ErrAccountIndexMutationCapacity)
	}
	totalFrames := (len(mutations) + chunkSize - 1) / chunkSize
	completedFrames := 0
	for start := 0; start < len(mutations); start += chunkSize {
		end := min(start+chunkSize, len(mutations))
		var frameMeta *foldMeta
		if end == len(mutations) {
			frameMeta = meta
		}
		if err := db.applyAccountIndexMutationsLocked(mutations[start:end], frameMeta); err != nil {
			return fmt.Errorf(
				"accountsdb: apply account-index transaction frame %d-%d of %d: %w",
				start,
				end,
				len(mutations),
				err,
			)
		}
		completedFrames++
		if hook := db.foldHooks.afterIndexTxnFrame; hook != nil {
			hook(completedFrames, totalFrames)
		}
	}
	return nil
}
