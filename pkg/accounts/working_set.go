package accounts

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gagliardetto/solana-go"
)

// ErrWorkingSetCapacity means publishing a complete slot would exceed the
// configured retained-byte budget. The rejected slot is never made visible.
var ErrWorkingSetCapacity = errors.New("working set retained-byte budget exceeded")

const (
	// These are deliberately conservative charges rather than a claim about
	// Go's exact heap layout. Each retained account is reachable from both the
	// per-slot map and the newest-value map and may need an undo entry; the
	// charge includes all of those map/slice/object costs plus the full capacity
	// of the retained data backing array. A slot charge covers its layer, order
	// entry, and small-map minimum allocation.
	workingSetAccountFixedBytes = uint64(512)
	workingSetSlotFixedBytes    = uint64(4 << 10)
)

// WorkingSetCapacityError reports the atomic admission decision for a rejected
// slot. It unwraps to ErrWorkingSetCapacity for errors.Is checks.
type WorkingSetCapacityError struct {
	Slot           uint64
	CurrentBytes   uint64
	RequestedBytes uint64
	MaximumBytes   uint64
}

func (e *WorkingSetCapacityError) Error() string {
	return fmt.Sprintf(
		"working set slot %d requires %d retained bytes with %d already held (maximum %d): %v",
		e.Slot, e.RequestedBytes, e.CurrentBytes, e.MaximumBytes, ErrWorkingSetCapacity,
	)
}

func (e *WorkingSetCapacityError) Unwrap() error { return ErrWorkingSetCapacity }

// WorkingSetStats is a race-free snapshot of the deterministic retained-byte
// accounting. RetainedBytes is conservative charged memory, not process RSS.
type WorkingSetStats struct {
	HeldSlots        uint64
	RetainedBytes    uint64
	HighWaterBytes   uint64
	MaximumBytes     uint64
	LargestSlotBytes uint64
	// HighWaterOverageBytes is retained for metrics compatibility. Strict
	// admission keeps it at zero whenever MaximumBytes is configured.
	HighWaterOverageBytes uint64
}

// WorkingSet is the canonical timeline's mutable suffix: confirmed-but-unrooted
// slot writes buffered in RAM over the durable rooted store. Reads hit the
// flat map (newest unrooted value per key, O(1)); the per-slot undo journal
// makes suffix eviction — the execute-on-receipt fork switch — O(evicted
// writes) instead of a full rescan. Siblings are never materialized as state:
// a switch unwinds this suffix and re-executes the certified block.
type WorkingSet struct {
	mu                    sync.RWMutex
	bySlot                map[uint64]*slotLayer
	order                 []uint64               // held slots, ascending
	flat                  map[[32]byte]flatEntry // newest unrooted value per key
	maximumBytes          uint64                 // zero means unlimited (tests/legacy callers only)
	retainedBytes         uint64
	highWaterBytes        uint64
	largestSlotBytes      uint64
	highWaterOverageBytes uint64
}

type flatEntry struct {
	slot uint64 // owner slot of acct (load-bearing for undo application)
	acct *Account
}

type slotLayer struct {
	slot          uint64
	writes        map[[32]byte]*Account
	retainedBytes uint64
	// undo records, one per FIRST write of a key in this slot: what the flat
	// entry pointed at before this slot overwrote it. Applying a suffix's
	// undos newest-layer-first restores the flat map exactly.
	undo []undoPtr
}

type undoPtr struct {
	key [32]byte
	// Store an indirection, not *Account: promotion can release the complete
	// prior layer even while a surviving undo record names its old slot.
	prevSlot uint64 // meaningful when existed
	existed  bool   // false: the key had no unrooted value before this slot
}

// NewWorkingSet creates an empty suffix; reads compose Lookup over the durable
// store externally (see pkg/replay).
func NewWorkingSet() *WorkingSet {
	return NewWorkingSetWithMaxRetainedBytes(0)
}

// NewWorkingSetWithMaxRetainedBytes creates an empty suffix with an atomic
// publication budget. A zero maximum retains the historical unlimited
// behaviour and is intended for focused tests; production supplies a limit.
func NewWorkingSetWithMaxRetainedBytes(maximumBytes uint64) *WorkingSet {
	return &WorkingSet{
		bySlot:       make(map[uint64]*slotLayer),
		flat:         make(map[[32]byte]flatEntry),
		maximumBytes: maximumBytes,
	}
}

// Add appends slot's account writes at the tip, capturing one undo record per
// first-written key. Admission is all-or-nothing: when the complete incoming
// charge would exceed the configured maximum, Add returns
// ErrWorkingSetCapacity before creating a layer or changing lookup-visible
// state. This deliberately rejects even one individually oversized slot; a
// configured maximum is a hard retained-byte bound, not a soft watermark.
// Slots must arrive in ascending order (one confirmed chain), so an added slot
// is always >= every held slot.
func (w *WorkingSet) Add(slot uint64, delta []*Account) error {
	requestedBytes, nonNilAccounts := workingSetAdmissionCharge(delta)

	w.mu.Lock()
	defer w.mu.Unlock()

	layer, ok := w.bySlot[slot]
	if !ok {
		requestedBytes = saturatingAddWorkingSetBytes(requestedBytes, workingSetSlotFixedBytes)
	}
	projectedBytes := saturatingAddWorkingSetBytes(w.retainedBytes, requestedBytes)
	if w.maximumBytes != 0 && projectedBytes > w.maximumBytes {
		return &WorkingSetCapacityError{
			Slot:           slot,
			CurrentBytes:   w.retainedBytes,
			RequestedBytes: requestedBytes,
			MaximumBytes:   w.maximumBytes,
		}
	}

	if !ok {
		layer = &slotLayer{slot: slot, writes: make(map[[32]byte]*Account, nonNilAccounts)}
		w.bySlot[slot] = layer
		w.order = append(w.order, slot)
	}
	layer.retainedBytes = saturatingAddWorkingSetBytes(layer.retainedBytes, requestedBytes)
	w.retainedBytes = projectedBytes
	if w.retainedBytes > w.highWaterBytes {
		w.highWaterBytes = w.retainedBytes
	}
	if layer.retainedBytes > w.largestSlotBytes {
		w.largestSlotBytes = layer.retainedBytes
	}
	for _, a := range delta {
		if a == nil {
			continue
		}
		key := [32]byte(a.Key)
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
	return nil
}

func workingSetAdmissionCharge(delta []*Account) (uint64, int) {
	var charge uint64
	nonNil := 0
	for _, acct := range delta {
		if acct == nil {
			continue
		}
		nonNil++
		accountBytes := saturatingAddWorkingSetBytes(workingSetAccountFixedBytes, uint64(cap(acct.Data)))
		charge = saturatingAddWorkingSetBytes(charge, accountBytes)
	}
	return charge, nonNil
}

func saturatingAddWorkingSetBytes(a, b uint64) uint64 {
	if ^uint64(0)-a < b {
		return ^uint64(0)
	}
	return a + b
}

// Stats reports the current, historical high-water, and configured maximum
// retained-byte charges.
func (w *WorkingSet) Stats() WorkingSetStats {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return WorkingSetStats{
		HeldSlots:             uint64(len(w.order)),
		RetainedBytes:         w.retainedBytes,
		HighWaterBytes:        w.highWaterBytes,
		MaximumBytes:          w.maximumBytes,
		LargestSlotBytes:      w.largestSlotBytes,
		HighWaterOverageBytes: w.highWaterOverageBytes,
	}
}

// HeldSlots reports the number of buffered unrooted slots (for RAM bounding).
func (w *WorkingSet) HeldSlots() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.order)
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
	batch, _ := w.PromotionPrefixBounded(through, 0, 0)
	return batch
}

// PromotionPrefixBounded snapshots the first durable-promotion chunk of held
// slots <= through. A zero slot or mutation limit is unlimited. Slot boundaries
// are never split: if the first slot alone exceeds maximumMutations, that one
// complete slot is returned. The boolean reports that a configured bound was
// reached; when false, the returned chunk is the trailing partial rooted
// prefix, which a caller may leave buffered unless it is force-flushing.
//
// Mutation count is the conservative sum of each layer's unique writes. Keys
// repeated across slots can make this an overestimate, never an unsafe
// underestimate. Only returned layers are copied, bounding transient pointer
// storage independently of the total rooted backlog.
func (w *WorkingSet) PromotionPrefixBounded(
	through uint64,
	maximumSlots int,
	maximumMutations uint64,
) ([]SlotDelta, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var batch []SlotDelta
	var mutations uint64
	for _, slot := range w.order { // ascending
		if slot > through {
			break
		}
		layer := w.bySlot[slot]
		slotMutations := uint64(len(layer.writes))
		if maximumMutations != 0 && len(batch) != 0 && slotMutations > maximumMutations-mutations {
			return batch, true
		}
		delta := make([]*Account, 0, len(layer.writes))
		for _, a := range layer.writes {
			delta = append(delta, a)
		}
		batch = append(batch, SlotDelta{Slot: slot, Delta: delta})
		if maximumSlots > 0 && len(batch) == maximumSlots {
			return batch, true
		}
		if maximumMutations != 0 {
			if slotMutations > maximumMutations {
				return batch, true
			}
			mutations += slotMutations
			if mutations == maximumMutations {
				return batch, true
			}
		}
	}
	return batch, false
}

// PromotePrefix drops the rooted prefix (held slots <= through); keys with no
// newer held writer fall through to durable. Caller MUST make them durable
// BEFORE this.
func (w *WorkingSet) PromotePrefix(through uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	cut := 0
	for cut < len(w.order) && w.order[cut] <= through {
		slot := w.order[cut]
		for key := range w.bySlot[slot].writes {
			if e, ok := w.flat[key]; ok && e.slot <= through {
				delete(w.flat, key) // no surviving held writer -> durable
			}
		}
		w.retainedBytes -= w.bySlot[slot].retainedBytes
		delete(w.bySlot, slot)
		cut++
	}
	if cut != 0 {
		copy(w.order, w.order[cut:])
		clear(w.order[len(w.order)-cut:])
		w.order = w.order[:len(w.order)-cut]
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
			// undoPtr deliberately stores only its slot number, never an account
			// pointer, so a dangling prevSlot cannot retain promoted account data.
			delete(w.flat, u.key)
		}
	}
	for _, s := range suffix {
		w.retainedBytes -= w.bySlot[s].retainedBytes
		delete(w.bySlot, s)
	}
	w.order = w.order[:cut]
}

// CheckInvariants verifies flat ≡ fold(bySlot in order) — test hook (I6).
func (w *WorkingSet) CheckInvariants() error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	want := make(map[[32]byte]flatEntry)
	var retainedBytes uint64
	for _, slot := range w.order {
		retainedBytes = saturatingAddWorkingSetBytes(retainedBytes, w.bySlot[slot].retainedBytes)
		for key, acct := range w.bySlot[slot].writes {
			want[key] = flatEntry{slot: slot, acct: acct}
		}
	}
	if retainedBytes != w.retainedBytes {
		return errInvariant("retained bytes", w.retainedBytes, retainedBytes)
	}
	if w.maximumBytes != 0 && w.retainedBytes > w.maximumBytes {
		return errInvariant("retained byte maximum", w.retainedBytes, w.maximumBytes)
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
