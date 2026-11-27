package accountsdb

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/history"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

// historyState feeds an optional history store from folds. History is a record
// of the past for RPC: a failure to record it is logged and disables it, and
// never fails or delays the fold beyond the time to encode one chunk.
type historyState struct {
	mu     sync.Mutex // serializes recording and guards stats
	store  atomic.Pointer[history.Store]
	failed atomic.Bool
	stats  HistoryStats
}

// HistoryStats count the history work done at folds.
type HistoryStats struct {
	Chunks      uint64
	Slots       uint64
	Accounts    uint64
	AppendNanos uint64
	PrevNanos   uint64
	LastThrough uint64
	Resets      uint64
	Truncations uint64
}

// SetHistory attaches a history store; every later fold records its slots.
func (db *AccountsDb) SetHistory(s *history.Store) {
	db.history.mu.Lock()
	defer db.history.mu.Unlock()
	db.history.store.Store(s)
	db.history.failed.Store(false)
}

// History returns the attached history store, or nil.
func (db *AccountsDb) History() *history.Store {
	if db.history.failed.Load() {
		return nil
	}
	return db.history.store.Load()
}

// HistoryStats returns a snapshot of the history counters.
func (db *AccountsDb) HistoryStats() HistoryStats {
	db.history.mu.Lock()
	defer db.history.mu.Unlock()
	return db.history.stats
}

// HistoryVersion converts an account to a history version. Zero-lamport
// accounts do not exist as far as RPC is concerned.
func HistoryVersion(a *accounts.Account) *history.Version {
	if a == nil || a.Lamports == 0 {
		return &history.Version{Absent: true}
	}
	return &history.Version{Lamports: a.Lamports, Owner: a.Owner, Executable: a.Executable, RentEpoch: a.RentEpoch, Data: a.Data}
}

// recordHistory runs at the start of CommitBatch, before the fold changes the
// store, so the store still holds every key's version from before the batch.
func (db *AccountsDb) recordHistory(deltas []accounts.SlotDelta, throughSlot uint64) {
	db.history.mu.Lock()
	defer db.history.mu.Unlock()
	s := db.history.store.Load()
	if s == nil || db.history.failed.Load() {
		return
	}
	start := time.Now()
	from := db.DurableThrough() + 1
	if through, ok := s.Through(); ok && through+1 != from {
		var err error
		if through >= from {
			// A retried fold or a rewound store: drop what will be re-recorded.
			err = s.TruncateFrom(from)
			db.history.stats.Truncations++
		}
		if err == nil {
			if through2, ok2 := s.Through(); ok2 && through2+1 != from {
				// A gap (history enabled later, or chunks lost): start over,
				// since reads cannot span missing slots.
				mlog.Log.Warnf("history: recorded through %d but the fold starts at %d; starting history again", through2, from)
				err = s.Reset()
				db.history.stats.Resets++
			}
		}
		if err != nil {
			db.disableHistory(err)
			return
		}
	}
	slots := make([]history.SlotWrites, 0, len(deltas))
	for _, sd := range deltas {
		sw := history.SlotWrites{Slot: sd.Slot}
		for _, a := range sd.Delta {
			if a == nil || a.IsDummy {
				continue
			}
			sw.Keys = append(sw.Keys, [32]byte(a.Key))
			sw.Versions = append(sw.Versions, HistoryVersion(a))
		}
		slots = append(slots, sw)
	}
	var prevNanos time.Duration
	prev := func(keys [][32]byte) ([]*history.Version, error) {
		t := time.Now()
		defer func() { prevNanos = time.Since(t) }()
		pks := make([]solana.PublicKey, len(keys))
		for i, k := range keys {
			pks[i] = solana.PublicKey(k)
		}
		accts, err := db.GetAccountsBatch(context.Background(), from-1, pks)
		if err != nil {
			return nil, err
		}
		out := make([]*history.Version, len(keys))
		for i := range keys {
			var a *accounts.Account
			if i < len(accts) {
				a = accts[i]
			}
			out[i] = HistoryVersion(a)
		}
		return out, nil
	}
	if err := s.Append(from, throughSlot, slots, prev); err != nil {
		db.disableHistory(err)
		return
	}
	st := &db.history.stats
	st.Chunks++
	st.Slots += throughSlot - from + 1
	for _, sw := range slots {
		st.Accounts += uint64(len(sw.Keys))
	}
	st.AppendNanos += uint64(time.Since(start))
	st.PrevNanos += uint64(prevNanos)
	st.LastThrough = throughSlot
}

func (db *AccountsDb) disableHistory(err error) {
	db.history.failed.Store(true)
	mlog.Log.Errorf("history: recording stopped: %v", err)
}
