package accountsdb

import (
	"os"

	"github.com/gagliardetto/solana-go"
)

// batchAbsentCarry: MITHRIL_BATCH_ABSENT_CARRY=0 restores the earlier
// behaviour, in which every fold invalidated all negative entries (for A/B
// measurement and as a fallback).
var batchAbsentCarry = os.Getenv("MITHRIL_BATCH_ABSENT_CARRY") != "0"

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

// carryBatchAbsencesLocked keeps the negative entries valid across the fold
// that is publishing changed. An entry says its key was absent from the durable
// index as of the epoch it was admitted at. A fold changes the durable state of
// exactly the keys in changed, so every other entry is still true after it;
// only the changed keys are dropped (deletions included, which costs one index
// read later and nothing else). Without this every fold emptied the set, and
// the blocks loading during the fold repeated a cold index read for each key
// they had already learned was absent.
//
// Requires the readCacheEpochMu write lock, held across the fold's own epoch
// advance: the entries must be valid at exactly the epoch before it. Entries
// that are already stale stay stale and are cleared by the next admission.
// Rewind does not call this, so it still invalidates everything.
func (db *AccountsDb) carryBatchAbsencesLocked(changed map[[32]byte]dedupedVersion) {
	if !batchAbsentCarry || db.batchAbsentEpoch+1 != db.readCacheEpoch {
		return
	}
	if len(changed) < len(db.batchAbsent) {
		for key := range changed {
			delete(db.batchAbsent, solana.PublicKey(key))
		}
	} else {
		for key := range db.batchAbsent {
			if _, ok := changed[[32]byte(key)]; ok {
				delete(db.batchAbsent, key)
			}
		}
	}
	db.batchAbsentEpoch = db.readCacheEpoch
}

// cacheBatchAbsences accepts only exact misses from the batch's Pebble snapshot,
// never read/decode errors or zero-lamport records. Folds advance
// readCacheEpoch and carry the entries their changed keys leave true
// (carryBatchAbsencesLocked); rewind advances it and clears caches. A pending
// fold may expose a newer account than that snapshot, so those keys cannot be
// admitted.
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
