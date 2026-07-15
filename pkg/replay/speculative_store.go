package replay

import (
	"fmt"
	"sort"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

const maxSpeculativeLayers = 512

// SpeculativeLayer holds post-slot account state for keys modified during speculative execution.
type SpeculativeLayer struct {
	Slot       uint64
	ParentSlot uint64
	Deltas     map[solana.PublicKey]*accounts.Account
	undo       []speculativeUndo
}

type speculativeUndo struct {
	key      solana.PublicKey
	prevSlot uint64
	existed  bool
}

type speculativeFlatEntry struct {
	slot uint64
	acct *accounts.Account
}

// SpeculativeStore resolves account state by walking parent layers back to the finalized slot.
type SpeculativeStore struct {
	mu            sync.RWMutex
	finalizedSlot uint64
	layers        map[uint64]*SpeculativeLayer
	order         []uint64
	flat          map[solana.PublicKey]speculativeFlatEntry
}

func newSpeculativeStore() *SpeculativeStore {
	return &SpeculativeStore{
		layers: make(map[uint64]*SpeculativeLayer),
		flat:   make(map[solana.PublicKey]speculativeFlatEntry),
	}
}

func (st *SpeculativeStore) FinalizedSlot() uint64 {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.finalizedSlot
}

func (st *SpeculativeStore) SetFinalizedSlot(slot uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.finalizedSlot = slot
}

func (st *SpeculativeStore) UseStoreForParent(parentSlot uint64) bool {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return parentSlot > st.finalizedSlot
}

func (st *SpeculativeStore) LayerCount() int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.layers)
}

// PromotionPrefix snapshots the oldest active layers through the finalized
// watermark. Account pointers are immutable layer-owned clones; the caller
// must settle the fold before pruning or unwinding these layers.
func (st *SpeculativeStore) PromotionPrefix(through uint64, maxSlots int) []accounts.SlotDelta {
	st.mu.RLock()
	defer st.mu.RUnlock()
	out := make([]accounts.SlotDelta, 0, min(len(st.order), maxSlots))
	for _, slot := range st.order {
		if slot > through || (maxSlots > 0 && len(out) >= maxSlots) {
			break
		}
		layer := st.layers[slot]
		if layer == nil {
			break
		}
		delta := make([]*accounts.Account, 0, len(layer.Deltas))
		for _, acct := range layer.Deltas {
			delta = append(delta, acct)
		}
		out = append(out, accounts.SlotDelta{Slot: slot, Delta: delta})
	}
	return out
}

// Resolve returns account state at endSlot. The active tip normally resolves
// from flat in O(1); historical views fall back to the bounded parent walk.
func (st *SpeculativeStore) Resolve(endSlot uint64, pk solana.PublicKey, db *accountsdb.AccountsDb) (*accounts.Account, error) {
	st.mu.RLock()
	finalized := st.finalizedSlot
	if endSlot > finalized {
		if entry, ok := st.flat[pk]; ok && entry.slot <= endSlot {
			acct := entry.acct.Clone()
			st.mu.RUnlock()
			return acct, nil
		}
		// Replay callers sometimes name the slot being built, and skipped slots
		// have no layer of their own. Resolve both against the newest executed
		// ancestor at or before endSlot instead of requiring an exact layer.
		slot := endSlot
		if _, exact := st.layers[slot]; !exact {
			index := sort.Search(len(st.order), func(i int) bool { return st.order[i] > endSlot }) - 1
			if index >= 0 {
				slot = st.order[index]
			} else {
				slot = finalized
			}
		}
		if slot <= finalized {
			st.mu.RUnlock()
			if db == nil {
				return nil, fmt.Errorf("speculative store: nil accounts db")
			}
			acct, err := db.GetAccountDurable(finalized, pk)
			if err != nil {
				return nil, err
			}
			return acct.Clone(), nil
		}
		for slot > finalized {
			layer, ok := st.layers[slot]
			if !ok {
				st.mu.RUnlock()
				return nil, fmt.Errorf("speculative store: missing ancestor layer for slot %d while resolving %s", slot, pk)
			}
			if acct, ok := layer.Deltas[pk]; ok {
				acct = acct.Clone()
				st.mu.RUnlock()
				return acct, nil
			}
			slot = layer.ParentSlot
		}
	}
	st.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("speculative store: nil accounts db")
	}
	acct, err := db.GetAccountDurable(finalized, pk)
	if err != nil {
		return nil, err
	}
	return acct.Clone(), nil
}

// RecordLayer stores post-slot account state for deferred replay. snapshotAccts is the
// end-of-slot bank-hash account set (includes sysvars updated outside ModifiedAccts).
func (st *SpeculativeStore) RecordLayer(slot, parentSlot uint64, slotCtx *sealevel.SlotCtx, snapshotAccts []*accounts.Account) error {
	if slotCtx == nil {
		return fmt.Errorf("speculative store: nil slot context for slot %d", slot)
	}
	if slotCtx.Slot != slot {
		return fmt.Errorf("speculative store: slot context slot %d != layer slot %d", slotCtx.Slot, slot)
	}

	layer := &SpeculativeLayer{
		Slot:       slot,
		ParentSlot: parentSlot,
		Deltas:     make(map[solana.PublicKey]*accounts.Account),
	}

	for _, acct := range snapshotAccts {
		if acct == nil {
			continue
		}
		layer.Deltas[acct.Key] = acct.Clone()
	}

	slotCtx.AcctMapsMu.Lock()
	modified := make([]solana.PublicKey, 0, len(slotCtx.ModifiedAccts))
	for pk := range slotCtx.ModifiedAccts {
		if _, ok := layer.Deltas[pk]; ok {
			continue
		}
		modified = append(modified, pk)
	}
	slotCtx.AcctMapsMu.Unlock()

	for _, pk := range modified {
		acct, err := slotCtx.GetAccount(pk)
		if err != nil {
			return fmt.Errorf("speculative store: record layer slot %d: %w", slot, err)
		}
		layer.Deltas[pk] = acct.Clone()
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if _, exists := st.layers[slot]; exists {
		return fmt.Errorf("speculative store: duplicate layer for slot %d", slot)
	}
	if len(st.layers) >= maxSpeculativeLayers {
		return fmt.Errorf("speculative store: layer limit %d exceeded", maxSpeculativeLayers)
	}
	if len(st.order) > 0 && slot <= st.order[len(st.order)-1] {
		return fmt.Errorf("speculative store: out-of-order layer %d after %d", slot, st.order[len(st.order)-1])
	}
	expectedParent := st.finalizedSlot
	if len(st.order) > 0 {
		expectedParent = st.order[len(st.order)-1]
	}
	if parentSlot != expectedParent {
		return fmt.Errorf("speculative store: slot %d parent %d does not extend active tip %d", slot, parentSlot, expectedParent)
	}
	for key, acct := range layer.Deltas {
		if previous, exists := st.flat[key]; exists {
			layer.undo = append(layer.undo, speculativeUndo{key: key, prevSlot: previous.slot, existed: true})
		} else {
			layer.undo = append(layer.undo, speculativeUndo{key: key})
		}
		st.flat[key] = speculativeFlatEntry{slot: slot, acct: acct}
	}
	st.layers[slot] = layer
	st.order = append(st.order, slot)
	return nil
}

func (st *SpeculativeStore) PruneLayersAbove(anchorSlot uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	cut := len(st.order)
	for index, slot := range st.order {
		if slot > anchorSlot {
			cut = index
			break
		}
	}
	suffix := st.order[cut:]
	for index := len(suffix) - 1; index >= 0; index-- {
		layer := st.layers[suffix[index]]
		for _, undo := range layer.undo {
			if !undo.existed {
				delete(st.flat, undo.key)
				continue
			}
			if previous := st.layers[undo.prevSlot]; previous != nil && undo.prevSlot <= anchorSlot {
				if acct, exists := previous.Deltas[undo.key]; exists {
					st.flat[undo.key] = speculativeFlatEntry{slot: undo.prevSlot, acct: acct}
					continue
				}
			}
			delete(st.flat, undo.key)
		}
	}
	for _, slot := range suffix {
		delete(st.layers, slot)
	}
	st.order = st.order[:cut]
}

func (st *SpeculativeStore) PruneLayersThrough(committedSlot uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	kept := st.order[:0]
	for _, slot := range st.order {
		if slot > committedSlot {
			kept = append(kept, slot)
			continue
		}
		for key := range st.layers[slot].Deltas {
			if entry, exists := st.flat[key]; exists && entry.slot <= committedSlot {
				delete(st.flat, key)
			}
		}
		delete(st.layers, slot)
	}
	st.order = kept
	for _, slot := range st.order {
		for index := range st.layers[slot].undo {
			undo := &st.layers[slot].undo[index]
			if undo.existed && undo.prevSlot <= committedSlot {
				undo.existed = false
				undo.prevSlot = 0
			}
		}
	}
}

func (st *SpeculativeStore) Clear() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.layers = make(map[uint64]*SpeculativeLayer)
	st.order = nil
	st.flat = make(map[solana.PublicKey]speculativeFlatEntry)
}

func (st *SpeculativeStore) CheckInvariants() error {
	st.mu.RLock()
	defer st.mu.RUnlock()
	want := make(map[solana.PublicKey]speculativeFlatEntry)
	for _, slot := range st.order {
		layer := st.layers[slot]
		if layer == nil {
			return fmt.Errorf("speculative store: ordered slot %d has no layer", slot)
		}
		for key, acct := range layer.Deltas {
			want[key] = speculativeFlatEntry{slot: slot, acct: acct}
		}
	}
	if len(want) != len(st.flat) {
		return fmt.Errorf("speculative store: flat size %d, want %d", len(st.flat), len(want))
	}
	for key, expected := range want {
		actual, exists := st.flat[key]
		if !exists || actual.slot != expected.slot || actual.acct != expected.acct {
			return fmt.Errorf("speculative store: flat entry mismatch for %s", key)
		}
	}
	return nil
}
