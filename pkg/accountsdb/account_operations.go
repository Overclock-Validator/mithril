package accountsdb

import (
	"math"
	"sync/atomic"
	"time"
)

// AccountOperationsObservation counts requested DB keys and successful folded
// records. Requests include misses, duplicates and errors, not overlay accesses.
// Commits include deduplicated tombstones, not failed folds or recovery replay.
// Its independent atomic totals are approximate under concurrent operations.
type AccountOperationsObservation struct {
	StartedAt                       time.Time
	RequestedKeys, CommittedRecords uint64
}

type accountOperationsRecorder struct {
	startedAt                       time.Time
	requestedKeys, committedRecords atomic.Uint64
}

// EnableAccountOperations opts this runtime DB owner into recording once. It
// never resets a recorder, including on rewind or cache reconstruction.
func (db *AccountsDb) EnableAccountOperations() bool {
	if db == nil || db.storageOpenedAt.IsZero() || db.storageClosing.Load() {
		return false
	}
	if db.accountOperations.Load() == nil {
		db.accountOperations.CompareAndSwap(nil, &accountOperationsRecorder{startedAt: time.Now()})
	}
	return !db.storageClosing.Load()
}

// accountOperationAdd reserves MaxUint64 as terminal unknown instead of wrap.
func accountOperationAdd(counter *atomic.Uint64, count uint64) {
	for {
		before := counter.Load()
		if before == math.MaxUint64 || count == 0 {
			return
		}
		after := uint64(math.MaxUint64)
		if count < math.MaxUint64-before {
			after = before + count
		}
		if counter.CompareAndSwap(before, after) {
			return
		}
	}
}

func (db *AccountsDb) recordAccountRequests(count uint64) {
	if db != nil {
		if r := db.accountOperations.Load(); r != nil {
			accountOperationAdd(&r.requestedKeys, count)
		}
	}
}

func (db *AccountsDb) recordCommittedAccounts(count uint64) {
	if r := db.accountOperations.Load(); r != nil {
		accountOperationAdd(&r.committedRecords, count)
	}
}

func (db *AccountsDb) accountOperationsObservation() *AccountOperationsObservation {
	if r := db.accountOperations.Load(); r != nil {
		reads, writes := r.requestedKeys.Load(), r.committedRecords.Load()
		if reads != math.MaxUint64 && writes != math.MaxUint64 {
			return &AccountOperationsObservation{StartedAt: r.startedAt, RequestedKeys: reads, CommittedRecords: writes}
		}
	}
	return nil
}
