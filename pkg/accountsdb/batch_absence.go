package accountsdb

import "github.com/gagliardetto/solana-go"

// Bound retained negative entries independently of the byte-weighted account
// cache. Eviction changes lookup cost only; it never establishes absence.
const batchAbsentLimit = 8192

// batchAccountAbsentLocked requires readCacheEpochMu. Positive/pending-store
// views must be checked first. Single-account APIs retain ErrNoAccount semantics
// and do not consume these synthetic batch placeholders.
func (db *AccountsDb) batchAccountAbsentLocked(key solana.PublicKey) bool {
	if db.batchAbsentEpoch != db.readCacheEpoch {
		return false
	}
	_, ok := db.batchAbsent[key]
	return ok
}

// cacheBatchAbsences accepts only exact misses from the batch's Pebble snapshot,
// never read/decode errors or zero-lamport records. Folds and legacy writes
// advance readCacheEpoch; rewind also clears caches. A pending fold may expose
// a newer account than that snapshot, so those keys cannot be admitted.
// Epoch capture, cache probing and snapshot creation must share the epoch read
// lock. Writers must publish pending-fold state and advance the epoch before
// committing index changes; admission rechecks both under the epoch write lock.
func (db *AccountsDb) cacheBatchAbsences(keys []solana.PublicKey, cold []int, found, admit []bool, epoch uint64) {
	eligible := false
	for i, idx := range cold {
		if !found[i] && admit[idx] {
			eligible = true
			if hook := db.batchHooks.beforeCacheAdmission; hook != nil {
				hook(keys[idx])
			}
		}
	}
	if !eligible {
		return
	}
	db.readCacheEpochMu.Lock()
	defer db.readCacheEpochMu.Unlock()
	if epoch != db.readCacheEpoch {
		return
	}
	if db.batchAbsentEpoch != epoch {
		clear(db.batchAbsent)
		db.batchAbsentEpoch = epoch
	}
	for i, idx := range cold {
		if found[i] || !admit[idx] || db.pendingFoldContainsLocked(keys[idx]) {
			continue
		}
		if db.batchAbsent == nil {
			db.batchAbsent = make(map[solana.PublicKey]struct{})
		}
		if len(db.batchAbsent) >= batchAbsentLimit {
			if _, exists := db.batchAbsent[keys[idx]]; !exists {
				clear(db.batchAbsent)
			}
		}
		db.batchAbsent[keys[idx]] = struct{}{}
	}
}
