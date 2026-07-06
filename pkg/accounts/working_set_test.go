package accounts

import (
	"math/rand"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func wsAcct(key byte, lamports uint64) *Account {
	return &Account{Key: solana.PublicKey{key}, Lamports: lamports}
}

func wsKey(b byte) [32]byte { return [32]byte{b} }

// EvictFrom restores the exact prior value via the undo journal.
func TestWorkingSetUndoRestoresExactPriorValue(t *testing.T) {
	w := NewWorkingSet()
	w.Add(5, []*Account{wsAcct(1, 100)})
	w.Add(7, []*Account{wsAcct(1, 700), wsAcct(2, 200)})
	w.Add(9, []*Account{wsAcct(1, 900)})

	w.EvictFrom(9)
	require.NoError(t, w.CheckInvariants())
	a, ok := w.Lookup(wsKey(1))
	require.True(t, ok)
	assert.Equal(t, uint64(700), a.Lamports, "slot-7 value restored")

	w.EvictFrom(7)
	require.NoError(t, w.CheckInvariants())
	a, ok = w.Lookup(wsKey(1))
	require.True(t, ok)
	assert.Equal(t, uint64(100), a.Lamports, "slot-5 value restored")
	_, ok = w.Lookup(wsKey(2))
	assert.False(t, ok, "key 2's only writer evicted -> falls through to durable")
}

// Evicting a suffix spanning several layers restores the pre-suffix state
// (the oldest evicted layer's undo wins).
func TestWorkingSetEvictMultiLayerSuffix(t *testing.T) {
	w := NewWorkingSet()
	w.Add(5, []*Account{wsAcct(1, 100)})
	w.Add(7, []*Account{wsAcct(1, 700)})
	w.Add(8, []*Account{wsAcct(1, 800), wsAcct(3, 300)})
	w.Add(9, []*Account{wsAcct(1, 900), wsAcct(3, 390)})

	w.EvictFrom(7) // evicts 7, 8, 9 in one call
	require.NoError(t, w.CheckInvariants())
	a, ok := w.Lookup(wsKey(1))
	require.True(t, ok)
	assert.Equal(t, uint64(100), a.Lamports)
	_, ok = w.Lookup(wsKey(3))
	assert.False(t, ok)
	assert.Equal(t, 1, w.HeldSlots())
}

// A key whose previous writer was already promoted falls through to durable
// on eviction (the promoted value IS the durable value).
func TestWorkingSetEvictAcrossPromotedPrevSlot(t *testing.T) {
	w := NewWorkingSet()
	w.Add(5, []*Account{wsAcct(1, 100)})
	w.Add(7, []*Account{wsAcct(1, 700)})

	w.PromotePrefix(5) // slot 5 now durable
	require.NoError(t, w.CheckInvariants())

	w.EvictFrom(7)
	require.NoError(t, w.CheckInvariants())
	_, ok := w.Lookup(wsKey(1))
	assert.False(t, ok, "prev writer promoted -> durable owns the value")
	assert.Equal(t, 0, w.HeldSlots())
}

// Evict-then-re-add behaves like the slots never existed.
func TestWorkingSetEvictThenReAdd(t *testing.T) {
	w := NewWorkingSet()
	w.Add(5, []*Account{wsAcct(1, 100)})
	w.Add(7, []*Account{wsAcct(1, 700)})
	w.EvictFrom(7)

	w.Add(7, []*Account{wsAcct(1, 777), wsAcct(4, 40)})
	require.NoError(t, w.CheckInvariants())
	a, _ := w.Lookup(wsKey(1))
	assert.Equal(t, uint64(777), a.Lamports)
	a, _ = w.Lookup(wsKey(4))
	assert.Equal(t, uint64(40), a.Lamports)

	// And the re-added slot unwinds cleanly again.
	w.EvictFrom(7)
	require.NoError(t, w.CheckInvariants())
	a, _ = w.Lookup(wsKey(1))
	assert.Equal(t, uint64(100), a.Lamports)
	_, ok := w.Lookup(wsKey(4))
	assert.False(t, ok)
}

// shadowSet is the naive reference implementation: layers only, lookups scan
// newest-first. The WorkingSet must agree with it under random operations.
type shadowSet struct {
	order  []uint64
	layers map[uint64]map[[32]byte]*Account
}

func newShadow() *shadowSet {
	return &shadowSet{layers: make(map[uint64]map[[32]byte]*Account)}
}

func (s *shadowSet) add(slot uint64, delta []*Account) {
	layer, ok := s.layers[slot]
	if !ok {
		layer = make(map[[32]byte]*Account)
		s.layers[slot] = layer
		s.order = append(s.order, slot)
	}
	for _, a := range delta {
		layer[[32]byte(a.Key)] = a
	}
}

func (s *shadowSet) promote(through uint64) {
	kept := s.order[:0]
	for _, slot := range s.order {
		if slot <= through {
			delete(s.layers, slot)
			continue
		}
		kept = append(kept, slot)
	}
	s.order = kept
}

func (s *shadowSet) evictFrom(slot uint64) {
	kept := s.order[:0]
	for _, held := range s.order {
		if held >= slot {
			delete(s.layers, held)
			continue
		}
		kept = append(kept, held)
	}
	s.order = kept
}

func (s *shadowSet) lookup(key [32]byte) (*Account, bool) {
	for i := len(s.order) - 1; i >= 0; i-- {
		if a, ok := s.layers[s.order[i]][key]; ok {
			return a, true
		}
	}
	return nil, false
}

// Model/fuzz: random Add/PromotePrefix/EvictFrom sequences must keep the
// WorkingSet observably identical to the naive shadow.
func TestWorkingSetModelAgainstShadow(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for round := 0; round < 50; round++ {
		w := NewWorkingSet()
		sh := newShadow()
		slot := uint64(100)
		heldLow := slot

		for op := 0; op < 200; op++ {
			switch rng.Intn(10) {
			case 0, 1, 2, 3, 4, 5: // add next slot
				slot++
				n := rng.Intn(4)
				delta := make([]*Account, 0, n)
				for i := 0; i < n; i++ {
					delta = append(delta, wsAcct(byte(rng.Intn(12)), uint64(rng.Intn(100000))))
				}
				w.Add(slot, delta)
				sh.add(slot, delta)
			case 6, 7: // promote a prefix
				if slot > heldLow {
					through := heldLow + uint64(rng.Intn(int(slot-heldLow)))
					w.PromotePrefix(through)
					sh.promote(through)
					heldLow = through + 1
				}
			case 8, 9: // evict a suffix
				if slot > heldLow {
					from := heldLow + 1 + uint64(rng.Intn(int(slot-heldLow)))
					w.EvictFrom(from)
					sh.evictFrom(from)
					slot = from - 1
				}
			}

			if err := w.CheckInvariants(); err != nil {
				t.Fatalf("round %d op %d: %v", round, op, err)
			}
			for k := 0; k < 12; k++ {
				key := wsKey(byte(k))
				got, gok := w.Lookup(key)
				want, wok := sh.lookup(key)
				if gok != wok || (gok && got != want) {
					t.Fatalf("round %d op %d key %d: workingset=(%v,%v) shadow=(%v,%v)", round, op, k, got, gok, want, wok)
				}
			}
		}
	}
}
