package accountsdb

import (
	"bytes"
	"context"
	"fmt"
	"sort"
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
	bank   *history.BankData // staged by the fold job for its next CommitBatch
}

// HistoryEnabled reports whether folds record history.
func (db *AccountsDb) HistoryEnabled() bool { return db.History() != nil }

// StageHistoryBank hands the next CommitBatch the bank context of its slots.
// The fold worker calls it immediately before CommitBatch.
func (db *AccountsDb) StageHistoryBank(b *history.BankData) {
	db.history.mu.Lock()
	db.history.bank = b
	db.history.mu.Unlock()
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
// History recorded past the durable store (a fold that failed after recording,
// or a store rewound or rebuilt since) is dropped first, and history that no
// longer reaches the durable store is cleared, so the two agree from the
// start. A previously attached store is closed.
func (db *AccountsDb) SetHistory(s *history.Store) error {
	db.history.mu.Lock()
	defer db.history.mu.Unlock()
	if s != nil {
		if err := reconcileHistory(s, db.DurableThrough()); err != nil {
			return err
		}
	}
	if old := db.history.store.Swap(s); old != nil && old != s {
		old.Close()
	}
	db.history.failed.Store(false)
	return nil
}

func reconcileHistory(s *history.Store, durable uint64) error {
	through, ok := s.Through()
	if !ok {
		return nil
	}
	if through > durable {
		if err := s.TruncateFrom(durable + 1); err != nil {
			return err
		}
		through, ok = s.Through()
	}
	if ok && through != durable {
		return s.Reset()
	}
	return nil
}

// CloseHistory detaches and closes the history store.
func (db *AccountsDb) CloseHistory() {
	db.history.mu.Lock()
	defer db.history.mu.Unlock()
	if old := db.history.store.Swap(nil); old != nil {
		old.Close()
	}
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
	bank := db.history.bank
	db.history.bank = nil
	if s == nil || db.history.failed.Load() {
		return
	}
	start := time.Now()
	from := db.DurableThrough() + 1
	if bank != nil && !bankWithin(bank, from, throughSlot) {
		mlog.Log.Warnf("history: bank context staged for another fold; recording %d..%d without it", from, throughSlot)
		bank = nil
	}
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
	buildStart := time.Now()
	keys, err := db.historyKeys(deltas, from)
	buildTime := time.Since(buildStart)
	if err != nil {
		db.disableHistory(err)
		return
	}
	benchHistorySample(keys.keys)
	writeStart := time.Now()
	if err := s.AppendKeys(from, throughSlot, keys.keys, bank); err != nil {
		db.disableHistory(err)
		return
	}
	mlog.Log.FileOnlyf("history: recorded %d..%d: %d keys, %d versions in %s (previous versions %s, differences %s, write %s)",
		from, throughSlot, len(keys.keys), keys.versions, time.Since(start).Round(time.Millisecond),
		keys.prevNanos.Round(time.Millisecond), (buildTime - keys.prevNanos).Round(time.Millisecond), time.Since(writeStart).Round(time.Millisecond))
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

func bankWithin(b *history.BankData, from, to uint64) bool {
	for i, r := range b.Slots {
		if r.Slot < from || r.Slot > to || (i > 0 && r.Slot <= b.Slots[i-1].Slot) {
			return false
		}
	}
	for i, r := range b.Signatures {
		if r.Slot < from || r.Slot > to || (i > 0 && r.Slot <= b.Signatures[i-1].Slot) {
			return false
		}
	}
	return b.Top == nil || (b.Top.Slot >= from && b.Top.Slot <= to)
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
	// Key order: the store writes records sorted, and sorted reads walk the
	// index and files in order.
	sort.Slice(order, func(i, j int) bool { return bytes.Compare(order[i][:], order[j][:]) < 0 })
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

	setRaw := func(v *history.Version, a *accounts.Account) {
		*v = history.Version{Lamports: a.Lamports, Owner: a.Owner, Executable: a.Executable, RentEpoch: a.RentEpoch, Data: a.Data}
	}
	absent := &history.Version{Absent: true} // shared, never modified
	semantic := func(v *history.Version) *history.Version {
		if v.Lamports == 0 {
			return absent
		}
		return v
	}
	// Each difference is computed into one reused scratch buffer, then its
	// exact bytes are copied into a shared slab: one allocation per 512 KiB of
	// differences, and no space reserved for the worst case.
	var scratch, slab []byte
	appendDiff := func(old, cur *history.Version) ([]byte, bool) {
		var changed bool
		scratch, changed = history.AppendXorChanged(scratch[:0], old, cur)
		if !changed {
			return nil, false
		}
		if cap(slab)-len(slab) < len(scratch) {
			slab = make([]byte, 0, max(512<<10, len(scratch)))
		}
		at := len(slab)
		slab = append(slab, scratch...)
		return slab[at:len(slab):len(slab)], true
	}
	var bufA, bufB history.Version // the two versions compared, reused per key
	out.keys = make([]history.KeyHistory, 0, len(order))
	for ki, k := range order {
		es := byKey[k]
		n := len(es)
		if es[n-1].full == nil {
			return out, fmt.Errorf("history: newest version of %x in the batch is not full", k[:4])
		}
		// Walk newest to oldest. cur is the raw version at es[i].
		cur, older := &bufA, &bufB
		setRaw(cur, es[n-1].full)
		var diffs []history.Diff // newest first
		for i := n - 1; i >= 1; i-- {
			reuse := false
			if es[i-1].full != nil {
				setRaw(older, es[i-1].full)
			} else {
				*older = *cur
				older.Data = bytes.Clone(cur.Data) // cur's data may be a stored account's
				if err := history.ApplyXor(older, es[i-1].diff, false); err != nil {
					return out, fmt.Errorf("history: rebuild %x at slot %d: %w", k[:4], es[i-1].slot, err)
				}
				reuse = older.Lamports != 0 && cur.Lamports != 0
			}
			hOld, hCur := semantic(older), semantic(cur)
			if reuse {
				if !history.Same(hOld, hCur) {
					diffs = append(diffs, history.Diff{Slot: es[i].slot, Bytes: es[i-1].diff})
				}
			} else if d, changed := appendDiff(hOld, hCur); changed {
				diffs = append(diffs, history.Diff{Slot: es[i].slot, Bytes: d})
			}
			cur, older = older, cur
		}
		if d, changed := appendDiff(prev[ki], semantic(cur)); changed {
			diffs = append(diffs, history.Diff{Slot: es[0].slot, Bytes: d})
		}
		for l, r := 0, len(diffs)-1; l < r; l, r = l+1, r-1 {
			diffs[l], diffs[r] = diffs[r], diffs[l]
		}
		out.keys = append(out.keys, history.KeyHistory{Key: k, Prev: prev[ki], Diffs: diffs})
	}
	return out, nil
}
