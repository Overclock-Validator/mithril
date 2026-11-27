package accountsdb

import (
	"context"
	"fmt"
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
	keys, err := db.historyKeys(deltas, from)
	if err != nil {
		db.disableHistory(err)
		return
	}
	benchHistorySample(keys.keys)
	if err := s.AppendKeys(from, throughSlot, keys.keys); err != nil {
		db.disableHistory(err)
		return
	}
	prevNanos := keys.prevNanos
	accountsWritten := keys.versions
	st := &db.history.stats
	st.Chunks++
	st.Slots += throughSlot - from + 1
	st.Accounts += uint64(accountsWritten)
	st.AppendNanos += uint64(time.Since(start))
	st.PrevNanos += uint64(prevNanos)
	st.LastThrough = throughSlot
}

func (db *AccountsDb) disableHistory(err error) {
	db.history.failed.Store(true)
	mlog.Log.Errorf("history: recording stopped: %v", err)
}

type historyBatch struct {
	keys      []history.KeyHistory
	versions  int
	prevNanos time.Duration
}

// historyKeys turns a fold batch into per-key differences one key at a time,
// so a batch is never materialized whole. Compacted versions (differences
// already) are rebuilt by walking each key newest to oldest; where both sides
// of a change exist, the working set's difference is exactly the one history
// stores and is reused.
func (db *AccountsDb) historyKeys(deltas []accounts.SlotDelta, from uint64) (historyBatch, error) {
	type entry struct {
		slot uint64
		full *accounts.Account
		diff []byte
	}
	byKey := make(map[[32]byte][]entry)
	var order [][32]byte
	add := func(k [32]byte, e entry) {
		if _, ok := byKey[k]; !ok {
			order = append(order, k)
		}
		byKey[k] = append(byKey[k], e)
	}
	var out historyBatch
	for _, sd := range deltas {
		for _, a := range sd.Delta {
			if a == nil || a.IsDummy {
				continue
			}
			add([32]byte(a.Key), entry{slot: sd.Slot, full: a})
			out.versions++
		}
		for _, c := range sd.Compacted {
			add(c.Key, entry{slot: sd.Slot, diff: c.Diff})
			out.versions++
		}
	}
	// Versions before the fold, read in bounded batches.
	t := time.Now()
	prev := make([]*history.Version, len(order))
	const readBatch = 8192
	for lo := 0; lo < len(order); lo += readBatch {
		hi := min(lo+readBatch, len(order))
		pks := make([]solana.PublicKey, hi-lo)
		for i, k := range order[lo:hi] {
			pks[i] = solana.PublicKey(k)
		}
		accts, err := db.GetAccountsBatch(PassiveRead(context.Background()), from-1, pks)
		if err != nil {
			return out, err
		}
		for i := range pks {
			var a *accounts.Account
			if i < len(accts) {
				a = accts[i]
			}
			prev[lo+i] = HistoryVersion(a)
		}
	}
	out.prevNanos = time.Since(t)

	raw := func(a *accounts.Account) *history.Version {
		return &history.Version{Lamports: a.Lamports, Owner: a.Owner, Executable: a.Executable, RentEpoch: a.RentEpoch, Data: a.Data}
	}
	semantic := func(v *history.Version) *history.Version {
		if v.Lamports == 0 {
			return &history.Version{Absent: true}
		}
		return v
	}
	out.keys = make([]history.KeyHistory, 0, len(order))
	for ki, k := range order {
		es := byKey[k]
		n := len(es)
		if es[n-1].full == nil {
			return out, fmt.Errorf("history: newest version of %x in the batch is not full", k[:4])
		}
		// Walk newest to oldest. cur is the raw version at es[i].
		cur := raw(es[n-1].full)
		var diffs []history.Diff // newest first
		for i := n - 1; i >= 1; i-- {
			var older *history.Version
			reuse := false
			if es[i-1].full != nil {
				older = raw(es[i-1].full)
			} else {
				older = cur.Clone()
				if err := history.ApplyXor(older, es[i-1].diff, false); err != nil {
					return out, fmt.Errorf("history: rebuild %x at slot %d: %w", k[:4], es[i-1].slot, err)
				}
				reuse = older.Lamports != 0 && cur.Lamports != 0
			}
			hOld, hCur := semantic(older), semantic(cur)
			if !history.Same(hOld, hCur) {
				d := es[i-1].diff
				if !reuse {
					d = history.AppendXor(nil, hOld, hCur)
				}
				diffs = append(diffs, history.Diff{Slot: es[i].slot, Bytes: d})
			}
			cur = older
		}
		if first := semantic(cur); !history.Same(prev[ki], first) {
			diffs = append(diffs, history.Diff{Slot: es[0].slot, Bytes: history.AppendXor(nil, prev[ki], first)})
		}
		for l, r := 0, len(diffs)-1; l < r; l, r = l+1, r-1 {
			diffs[l], diffs[r] = diffs[r], diffs[l]
		}
		out.keys = append(out.keys, history.KeyHistory{Key: k, Prev: prev[ki], Diffs: diffs})
	}
	return out, nil
}
