package accounts

import (
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/history"
	"github.com/gagliardetto/solana-go"
)

// WorkingSet is the canonical timeline's mutable suffix: confirmed-but-unrooted
// slot writes buffered in RAM over the durable rooted store. Reads hit the
// flat map (newest unrooted value per key, O(1)); the per-slot undo journal
// makes suffix eviction — the execute-on-receipt fork switch — O(evicted
// writes) instead of a full rescan. Siblings are never materialized as state:
// a switch unwinds this suffix and re-executes the certified block.
type WorkingSet struct {
	mu            sync.RWMutex
	bySlot        map[uint64]*slotLayer
	order         []uint64               // held slots, ascending
	flat          map[[32]byte]flatEntry // newest unrooted value per key
	retainedBytes uint64                 // account capacities plus estimated per-version bookkeeping
	revision      uint64                 // changes under mu with every visible account-view mutation
	// generation is odd while EvictFrom rewrites the suffix and even otherwise.
	// Add and PromotePrefix leave it unchanged: they never alter the value a
	// key had at an already held slot. Readers outside the lock use it as a
	// seqlock to detect that a fork switch replaced slots they may have read.
	generation atomic.Uint64

	// Compaction: once a key has two finalized versions, the older one is kept
	// as the XOR difference from the next newer version instead of a full
	// copy. Values are unchanged (readers rebuild them), so revision and
	// generation do not move. compactedThrough is the highest slot compacted;
	// lastFinal maps each key to the newest compacted-range layer holding it
	// in full.
	compactedThrough uint64
	lastFinal        map[[32]byte]uint64
}

type flatEntry struct {
	slot uint64 // owner slot of acct (load-bearing for undo application)
	acct *Account
}

type slotLayer struct {
	slot          uint64
	writes        map[[32]byte]*Account
	compacted     map[[32]byte]compactedVersion // versions kept as differences; disjoint from writes
	retainedBytes uint64
	// undo records, one per FIRST write of a key in this slot: what the flat
	// entry pointed at before this slot overwrote it. Applying a suffix's
	// undos newest-layer-first restores the flat map exactly.
	undo []undoPtr
}

// Include account structs, map entries and undo records as well as the backing
// array capacity. This is a conservative budget, not a measurement of the heap;
// shared account values in different slots are deliberately counted per version.
const workingSetVersionOverhead = 512

// RetainedAccountBytes estimates the cost of one immutable working-set version.
func RetainedAccountBytes(a *Account) uint64 {
	return uint64(cap(a.Data)) + workingSetVersionOverhead
}

// compactedVersion is an account version stored as the XOR difference that
// turns the next newer held version of the same key back into it.
type compactedVersion struct {
	diff []byte
	slot uint64 // the account's Slot field
}

// compactedVersionOverhead budgets the map entry and slice header of one
// compacted version.
const compactedVersionOverhead = 128

func compactedBytes(c compactedVersion) uint64 { return uint64(cap(c.diff)) + compactedVersionOverhead }

func historyVersion(a *Account) *history.Version {
	return &history.Version{Lamports: a.Lamports, Owner: a.Owner, Executable: a.Executable, RentEpoch: a.RentEpoch, Data: a.Data}
}

type undoPtr struct {
	key      [32]byte
	prevSlot uint64 // meaningful when existed
	existed  bool   // false: the key had no unrooted value before this slot
}

// NewWorkingSet creates an empty suffix; reads compose Lookup over the durable
// store externally (see pkg/replay).
func NewWorkingSet() *WorkingSet {
	return &WorkingSet{
		bySlot: make(map[uint64]*slotLayer),
		flat:   make(map[[32]byte]flatEntry),
	}
}

// Add appends slot's account writes at the tip, capturing one undo record per
// first-written key. Slots must arrive in ascending order (one confirmed
// chain), so an added slot is always >= every held slot.
func (w *WorkingSet) Add(slot uint64, delta []*Account) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.revision++

	layer, ok := w.bySlot[slot]
	if !ok {
		layer = &slotLayer{slot: slot, writes: make(map[[32]byte]*Account, len(delta))}
		w.bySlot[slot] = layer
		w.order = append(w.order, slot)
	}
	for _, a := range delta {
		if a == nil {
			continue
		}
		key := [32]byte(a.Key)
		if previous, exists := layer.writes[key]; exists {
			cost := RetainedAccountBytes(previous)
			layer.retainedBytes -= cost
			w.retainedBytes -= cost
		}
		cost := RetainedAccountBytes(a)
		layer.retainedBytes += cost
		w.retainedBytes += cost
		if _, again := layer.writes[key]; !again {
			// First write of this key in this slot: journal what flat held.
			if e, exists := w.flat[key]; exists && e.slot != slot {
				layer.undo = append(layer.undo, undoPtr{key: key, prevSlot: e.slot, existed: true})
			} else if !exists {
				layer.undo = append(layer.undo, undoPtr{key: key})
			}
		}
		layer.writes[key] = a
		// Newest wins. Guard on owner slot so an out-of-order add can never
		// install an older value over a newer one.
		if e, exists := w.flat[key]; !exists || slot >= e.slot {
			w.flat[key] = flatEntry{slot: slot, acct: a}
		}
	}
}

// Revision identifies one account view for this WorkingSet's lifetime. Capture
// it when publishing a completed bank, then check it around request-local reads.
func (w *WorkingSet) Revision() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.revision
}

// HeldSlots reports the number of buffered unrooted slots (for RAM bounding).
func (w *WorkingSet) HeldSlots() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.order)
}

// RetainedBytes includes every buffered slot version, not only newest keys.
func (w *WorkingSet) RetainedBytes() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.retainedBytes
}

// Lookup returns the newest unrooted value for pubkey (nil, false if none held).
// No fall-through to durable; the newest held value is the correct pre-root value.
func (w *WorkingSet) Lookup(pubkey [32]byte) (*Account, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if e, ok := w.flat[pubkey]; ok {
		return e.acct, true
	}
	return nil, false
}

// LookupAt returns the newest unrooted value for pubkey written at or below
// maxSlot, so a reader can resolve a key as of an older replayed bank while
// replay keeps adding slots. A miss means no held slot at or below maxSlot
// wrote the key and the durable store owns the value.
func (w *WorkingSet) LookupAt(pubkey [32]byte, maxSlot uint64) (*Account, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	e, ok := w.flat[pubkey]
	if !ok {
		return nil, false // flat is the fold of every held layer
	}
	if e.slot <= maxSlot {
		return e.acct, true
	}
	for i := len(w.order) - 1; i >= 0; i-- {
		slot := w.order[i]
		if slot > maxSlot {
			continue
		}
		layer := w.bySlot[slot]
		if a, ok := layer.writes[pubkey]; ok {
			return a, true
		}
		if _, ok := layer.compacted[pubkey]; ok {
			return w.rebuildLocked(i, pubkey), true
		}
	}
	return nil, false
}

// rebuildLocked returns the version of key held compacted in the layer at
// order[i], by applying differences backward from the next full version.
func (w *WorkingSet) rebuildLocked(i int, key [32]byte) *Account {
	var chain []compactedVersion
	var base *Account
	for j := i; j < len(w.order) && base == nil; j++ {
		layer := w.bySlot[w.order[j]]
		if a, ok := layer.writes[key]; ok {
			base = a
		} else if c, ok := layer.compacted[key]; ok {
			chain = append(chain, c)
		}
	}
	if base == nil {
		panic("working set: compacted version without a newer full version")
	}
	v := historyVersion(base).Clone()
	for k := len(chain) - 1; k >= 0; k-- {
		if err := history.ApplyXor(v, chain[k].diff, false); err != nil {
			panic("working set: corrupt compacted version: " + err.Error())
		}
	}
	return &Account{Key: solana.PublicKey(key), Lamports: v.Lamports, Data: v.Data, Owner: v.Owner, Executable: v.Executable, RentEpoch: v.RentEpoch, Slot: chain[0].slot}
}

// layerVersionsLocked returns every version held in the layers order[lo:hi],
// rebuilding compacted ones in a single newest-to-oldest pass per key.
func (w *WorkingSet) layerVersionsLocked(lo, hi int) [][]*Account {
	out := make([][]*Account, hi-lo)
	need := make(map[[32]byte]bool)
	for i := lo; i < hi; i++ {
		layer := w.bySlot[w.order[i]]
		out[i-lo] = make([]*Account, 0, len(layer.writes)+len(layer.compacted))
		for _, a := range layer.writes {
			out[i-lo] = append(out[i-lo], a)
		}
		for k := range layer.compacted {
			need[k] = true
		}
	}
	if len(need) == 0 {
		return out
	}
	cur := make(map[[32]byte]*history.Version, len(need))
	for i := len(w.order) - 1; i >= lo; i-- {
		layer := w.bySlot[w.order[i]]
		for k := range need {
			if a, ok := layer.writes[k]; ok {
				cur[k] = historyVersion(a)
				continue
			}
			c, ok := layer.compacted[k]
			if !ok {
				continue
			}
			v := cur[k].Clone()
			if err := history.ApplyXor(v, c.diff, false); err != nil {
				panic("working set: corrupt compacted version: " + err.Error())
			}
			cur[k] = v
			if i < hi {
				out[i-lo] = append(out[i-lo], &Account{Key: solana.PublicKey(k), Lamports: v.Lamports, Data: v.Data, Owner: v.Owner, Executable: v.Executable, RentEpoch: v.RentEpoch, Slot: c.slot})
			}
		}
	}
	return out
}

// Generation returns the eviction seqlock counter (odd while an eviction is
// in progress).
func (w *WorkingSet) Generation() uint64 {
	return w.generation.Load()
}

// RangeVersions calls fn for every held version of every account, oldest slot
// first. It holds the read lock throughout, so replay's next Add waits for it:
// keep fn to a cheap filter. fn must not retain or modify a.
func (w *WorkingSet) RangeVersions(fn func(slot uint64, a *Account)) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for i, versions := range w.layerVersionsLocked(0, len(w.order)) {
		for _, a := range versions {
			fn(w.order[i], a)
		}
	}
}

// LookupBatch fills out with the newest unrooted value for every matching
// pubkey. Misses are left nil. The returned accounts are retained immutable
// WorkingSet values; callers must copy-on-write before mutation.
//
// A block may contain tens of thousands of unique keys. Holding one read lock
// for the whole lookup avoids two atomic lock operations per key while still
// giving the caller a coherent view of the speculative suffix.
func (w *WorkingSet) LookupBatch(pubkeys []solana.PublicKey, out []*Account) {
	if len(pubkeys) != len(out) {
		panic("working set batch lookup length mismatch")
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	for i, pubkey := range pubkeys {
		if e, ok := w.flat[[32]byte(pubkey)]; ok {
			out[i] = e.acct
		}
	}
}

// SlotDelta is one held slot's account writes, returned for durable promotion.
type SlotDelta struct {
	Slot  uint64
	Delta []*Account
}

// PromotionPrefix returns held slots <= through (ascending) with their writes,
// to durably commit before PromotePrefix(through). Values reference the stored
// accounts.
func (w *WorkingSet) PromotionPrefix(through uint64) []SlotDelta {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.promotionChunkLocked(through, len(w.order), 0, true)
}

// PromotionChunk returns at most maxSlots oldest held slots through the caller's
// verified promotion bound. Unless allowPartial is set, an incomplete chunk
// returns nil before allocating or collecting account writes. Selection and
// collection share one read lock, so pruning cannot change the selected prefix.
// This only prepares borrowed account pointers; it does not commit, prune, or
// advance durability. Callers still own finality checks and durable commit order.
func (w *WorkingSet) PromotionChunk(through uint64, maxSlots int, allowPartial bool) []SlotDelta {
	return w.PromotionChunkWithByteLimit(through, maxSlots, 0, allowPartial)
}

// PromotionChunkWithByteLimit admits a shorter chunk once maxBytes is reached.
// Slots are atomic: the last slot may overshoot the byte target. The caller's
// verified through bound is always respected, including under memory pressure.
// Zero maxBytes preserves slot-only batching.
func (w *WorkingSet) PromotionChunkWithByteLimit(through uint64, maxSlots int, maxBytes uint64, allowPartial bool) []SlotDelta {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.promotionChunkLocked(through, maxSlots, maxBytes, allowPartial)
}

func (w *WorkingSet) promotionChunkLocked(through uint64, maxSlots int, maxBytes uint64, allowPartial bool) []SlotDelta {
	count := 0
	var bytes uint64
	for count < len(w.order) && count < maxSlots && w.order[count] <= through {
		bytes += w.bySlot[w.order[count]].retainedBytes
		count++
		if maxBytes > 0 && bytes >= maxBytes {
			break
		}
	}
	if count == 0 || (!allowPartial && count < maxSlots && (maxBytes == 0 || bytes < maxBytes)) {
		return nil
	}
	batch := make([]SlotDelta, count)
	for i, versions := range w.layerVersionsLocked(0, count) {
		batch[i] = SlotDelta{Slot: w.order[i], Delta: versions}
	}
	return batch
}

// PromotePrefix drops the rooted prefix (held slots <= through); keys with no
// newer held writer fall through to durable. Caller MUST make them durable
// BEFORE this.
func (w *WorkingSet) PromotePrefix(through uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.order) == 0 || w.order[0] > through {
		return
	}
	w.revision++

	kept := make([]uint64, 0, len(w.order))
	for _, slot := range w.order {
		if slot > through {
			kept = append(kept, slot)
			continue
		}
		for key := range w.bySlot[slot].writes {
			if e, ok := w.flat[key]; ok && e.slot <= through {
				delete(w.flat, key) // no surviving held writer -> durable
			}
		}
		w.retainedBytes -= w.bySlot[slot].retainedBytes
		delete(w.bySlot, slot)
	}
	w.order = kept
	for k, s := range w.lastFinal {
		if s <= through {
			delete(w.lastFinal, k)
		}
	}
	// Undo records pointing into the promoted prefix now dangle; surviving
	// layers' undos are rewritten to "restore = fall through to durable",
	// which is exactly right: the promoted value IS the durable value.
	for _, slot := range w.order {
		layer := w.bySlot[slot]
		for i := range layer.undo {
			if layer.undo[i].existed && layer.undo[i].prevSlot <= through {
				layer.undo[i].existed = false
			}
		}
	}
}

// EvictFrom drops the abandoned suffix (held slots >= slot) — the fork-switch
// unwind. Undo journals apply newest-layer-first, restoring each affected key
// to its newest surviving value (or removing it so reads fall through to
// durable). Cost is O(evicted writes).
func (w *WorkingSet) EvictFrom(slot uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Identify the suffix (order is ascending).
	cut := len(w.order)
	for i, s := range w.order {
		if s >= slot {
			cut = i
			break
		}
	}
	suffix := w.order[cut:]
	if len(suffix) == 0 {
		return
	}
	w.revision++
	w.generation.Add(1) // odd: the suffix is being rewritten
	defer w.generation.Add(1)
	if slot <= w.compactedThrough {
		// Compaction covers only finalized slots, so this is unexpected: keep
		// it correct by restoring every compacted version in full first.
		w.expandLocked()
	}

	// Apply undos newest layer first: for keys written in several evicted
	// layers, the OLDEST layer's undo applies last and wins — restoring the
	// pre-suffix state exactly.
	for i := len(suffix) - 1; i >= 0; i-- {
		layer := w.bySlot[suffix[i]]
		for _, u := range layer.undo {
			if !u.existed {
				delete(w.flat, u.key)
				continue
			}
			if prev, held := w.bySlot[u.prevSlot]; held && u.prevSlot < slot {
				if acct, ok := prev.writes[u.key]; ok {
					w.flat[u.key] = flatEntry{slot: u.prevSlot, acct: acct}
					continue
				}
			}
			// Previous writer already promoted (or missing): durable owns it.
			delete(w.flat, u.key)
		}
	}
	for _, s := range suffix {
		w.retainedBytes -= w.bySlot[s].retainedBytes
		delete(w.bySlot, s)
	}
	w.order = w.order[:cut]
}

// expandLocked turns every compacted version back into a full one.
func (w *WorkingSet) expandLocked() {
	versions := w.layerVersionsLocked(0, len(w.order))
	for i, slot := range w.order {
		layer := w.bySlot[slot]
		if len(layer.compacted) == 0 {
			continue
		}
		for _, a := range versions[i] {
			k := [32]byte(a.Key)
			c, ok := layer.compacted[k]
			if !ok {
				continue
			}
			layer.writes[k] = a
			cost := RetainedAccountBytes(a)
			old := compactedBytes(c)
			layer.retainedBytes += cost - old
			w.retainedBytes += cost - old
		}
		layer.compacted = nil
	}
	w.compactedThrough = 0
	w.lastFinal = nil
}

// CompactThrough keeps older versions of finalized slots (<= through) as
// differences from the next newer finalized version. Finalized slots are never
// evicted, so differences never point into a suffix a fork switch removes.
// Differences are computed under the read lock; the swap takes the write lock
// briefly. It returns the number of versions compacted.
func (w *WorkingSet) CompactThrough(through uint64) int {
	type swap struct {
		slot uint64
		key  [32]byte
		full *Account
		c    compactedVersion
	}
	w.mu.RLock()
	from := w.compactedThrough
	gen := w.generation.Load()
	if through <= from || gen&1 == 1 {
		w.mu.RUnlock()
		return 0
	}
	last := make(map[[32]byte]uint64)
	lookupLast := func(k [32]byte) (uint64, bool) {
		if s, ok := last[k]; ok {
			return s, true
		}
		s, ok := w.lastFinal[k]
		return s, ok
	}
	var swaps []swap
	for _, slot := range w.order {
		if slot <= from {
			continue
		}
		if slot > through {
			break
		}
		layer := w.bySlot[slot]
		for k, cur := range layer.writes {
			if cur.IsDummy {
				continue
			}
			if p, ok := lookupLast(k); ok {
				if prevLayer := w.bySlot[p]; prevLayer != nil {
					if full, ok := prevLayer.writes[k]; ok && !full.IsDummy {
						d := history.AppendXor(nil, historyVersion(full), historyVersion(cur))
						swaps = append(swaps, swap{slot: p, key: k, full: full, c: compactedVersion{diff: d, slot: full.Slot}})
					}
				}
			}
			last[k] = slot
		}
	}
	w.mu.RUnlock()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.compactedThrough != from || w.generation.Load() != gen {
		return 0 // a fork switch or another compaction ran; try again later
	}
	done := 0
	for _, sw := range swaps {
		layer := w.bySlot[sw.slot]
		if layer == nil || layer.writes[sw.key] != sw.full {
			continue // promoted meanwhile
		}
		delete(layer.writes, sw.key)
		if layer.compacted == nil {
			layer.compacted = make(map[[32]byte]compactedVersion)
		}
		layer.compacted[sw.key] = sw.c
		freed := RetainedAccountBytes(sw.full)
		added := compactedBytes(sw.c)
		layer.retainedBytes = layer.retainedBytes - freed + added
		w.retainedBytes = w.retainedBytes - freed + added
		done++
	}
	if w.lastFinal == nil {
		w.lastFinal = make(map[[32]byte]uint64, len(last))
	}
	for k, s := range last {
		if _, held := w.bySlot[s]; held {
			w.lastFinal[k] = s
		}
	}
	w.compactedThrough = through
	return done
}

// CompactedThrough returns the highest slot compaction has covered.
func (w *WorkingSet) CompactedThrough() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.compactedThrough
}

// CheckInvariants verifies flat ≡ fold(bySlot in order) — test hook (I6).
func (w *WorkingSet) CheckInvariants() error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	want := make(map[[32]byte]flatEntry)
	var retainedBytes uint64
	for _, slot := range w.order {
		var layerBytes uint64
		for key, acct := range w.bySlot[slot].writes {
			want[key] = flatEntry{slot: slot, acct: acct}
			layerBytes += RetainedAccountBytes(acct)
		}
		for key, c := range w.bySlot[slot].compacted {
			if _, both := w.bySlot[slot].writes[key]; both {
				return errInvariant("version both full and compacted", key, slot)
			}
			delete(want, key) // a newer layer must hold it in full
			layerBytes += compactedBytes(c)
		}
		if layerBytes != w.bySlot[slot].retainedBytes {
			return errInvariant("slot retained bytes", w.bySlot[slot].retainedBytes, layerBytes)
		}
		retainedBytes += layerBytes
	}
	if retainedBytes != w.retainedBytes {
		return errInvariant("retained bytes", w.retainedBytes, retainedBytes)
	}
	if len(want) != len(w.flat) {
		return errInvariant("flat size", len(w.flat), len(want))
	}
	for key, e := range want {
		got, ok := w.flat[key]
		if !ok || got.slot != e.slot || got.acct != e.acct {
			return errInvariant("flat entry", got, e)
		}
	}
	return nil
}

type invariantError struct {
	what string
	got  any
	want any
}

func (e *invariantError) Error() string {
	return "working set invariant violated: " + e.what
}

func errInvariant(what string, got, want any) error {
	return &invariantError{what: what, got: got, want: want}
}
