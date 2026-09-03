package accounts

import (
	"errors"
	"math/rand"
	"sync"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func wsAcct(key byte, lamports uint64) *Account {
	return &Account{Key: solana.PublicKey{key}, Lamports: lamports}
}

func wsKey(b byte) [32]byte { return [32]byte{b} }

func TestWorkingSetRetainedByteThresholdPublishesWholeSlotOrNothing(t *testing.T) {
	const dataBytes = 2 << 20
	oneSlotBytes := workingSetSlotFixedBytes + workingSetAccountFixedBytes + dataBytes
	w := NewWorkingSetWithMaxRetainedBytes(oneSlotBytes + oneSlotBytes/2)

	first := &Account{Key: solana.PublicKey{1}, Lamports: 1, Data: make([]byte, dataBytes)}
	second := &Account{Key: solana.PublicKey{1}, Lamports: 2, Data: make([]byte, dataBytes)}
	third := &Account{Key: solana.PublicKey{1}, Lamports: 3, Data: make([]byte, dataBytes)}
	require.NoError(t, w.Add(1, []*Account{first}))
	err := w.Add(2, []*Account{second})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrWorkingSetCapacity))

	stats := w.Stats()
	assert.Equal(t, uint64(1), stats.HeldSlots)
	assert.Equal(t, oneSlotBytes, stats.RetainedBytes)
	assert.Equal(t, oneSlotBytes, stats.HighWaterBytes)
	assert.Equal(t, oneSlotBytes, stats.LargestSlotBytes)
	assert.Zero(t, stats.HighWaterOverageBytes)

	err = w.Add(3, []*Account{third})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrWorkingSetCapacity))
	assert.Equal(t, uint64(1), w.Stats().HeldSlots, "rejected slot must not publish a layer")
	latest, ok := w.Lookup(wsKey(1))
	require.True(t, ok)
	assert.Same(t, first, latest, "rejected slot must not change the flat lookup view")
	require.NoError(t, w.CheckInvariants())

	// A fold releases its exact layer charge and admission can proceed again.
	w.PromotePrefix(1)
	require.NoError(t, w.Add(2, []*Account{second}))
	latest, ok = w.Lookup(wsKey(1))
	require.True(t, ok)
	assert.Same(t, second, latest)
	require.ErrorIs(t, w.Add(3, []*Account{third}), ErrWorkingSetCapacity)
	latest, ok = w.Lookup(wsKey(1))
	require.True(t, ok)
	assert.Same(t, second, latest)
	require.NoError(t, w.CheckInvariants())
}

func TestWorkingSetRejectsIndividuallyOversizedSlotWithoutState(t *testing.T) {
	const maximum = uint64(1 << 20)
	w := NewWorkingSetWithMaxRetainedBytes(maximum)
	oversized := &Account{Key: solana.PublicKey{7}, Lamports: 7, Data: make([]byte, maximum)}

	err := w.Add(7, []*Account{oversized})
	var capacityErr *WorkingSetCapacityError
	require.ErrorAs(t, err, &capacityErr)
	assert.Equal(t, uint64(7), capacityErr.Slot)
	assert.Zero(t, capacityErr.CurrentBytes)
	assert.Greater(t, capacityErr.RequestedBytes, maximum)
	assert.Equal(t, maximum, capacityErr.MaximumBytes)
	assert.Equal(t, WorkingSetStats{MaximumBytes: maximum}, w.Stats())
	_, ok := w.Lookup(wsKey(7))
	assert.False(t, ok)
	require.NoError(t, w.CheckInvariants())
}

func TestWorkingSetConcurrentFoldAndMultiMiBAdmissionStayAtomic(t *testing.T) {
	const dataBytes = 2 << 20
	oneSlotBytes := workingSetSlotFixedBytes + workingSetAccountFixedBytes + dataBytes
	// Exactly one slot fits. A concurrent fold may win before admission
	// (success) or after its atomic check (clean rejection and retry), but a
	// partially visible slot is never an outcome.
	w := NewWorkingSetWithMaxRetainedBytes(oneSlotBytes)
	current := &Account{Key: solana.PublicKey{9}, Lamports: 1, Data: make([]byte, dataBytes)}
	require.NoError(t, w.Add(1, []*Account{current}))

	for slot := uint64(2); slot <= 16; slot++ {
		next := &Account{Key: solana.PublicKey{9}, Lamports: slot, Data: make([]byte, dataBytes)}
		start := make(chan struct{})
		addResult := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			addResult <- w.Add(slot, []*Account{next})
		}()
		go func() {
			defer wg.Done()
			<-start
			w.PromotePrefix(slot - 1)
		}()
		close(start)
		wg.Wait()
		if err := <-addResult; err != nil {
			require.ErrorIs(t, err, ErrWorkingSetCapacity)
			_, published := w.Lookup(wsKey(9))
			assert.False(t, published, "rejected layer must not leak through lookup")
			require.NoError(t, w.Add(slot, []*Account{next}))
		}
		current = next
		got, ok := w.Lookup(wsKey(9))
		require.True(t, ok)
		assert.Same(t, current, got)
		require.NoError(t, w.CheckInvariants())
		stats := w.Stats()
		assert.LessOrEqual(t, stats.RetainedBytes, stats.MaximumBytes)
	}
}

func TestWorkingSetPromotionPrefixBounded(t *testing.T) {
	w := NewWorkingSet()
	for slot, count := range []int{2, 3, 4, 1} {
		delta := make([]*Account, count)
		for i := range delta {
			delta[i] = wsAcct(byte(slot*16+i+1), uint64(slot+1))
		}
		require.NoError(t, w.Add(uint64(slot+1), delta))
	}

	chunk, limitReached := w.PromotionPrefixBounded(4, 3, 6)
	require.True(t, limitReached, "adding slot 3 would cross the mutation bound")
	require.Len(t, chunk, 2)
	assert.Equal(t, []uint64{1, 2}, []uint64{chunk[0].Slot, chunk[1].Slot})
	assert.Len(t, chunk[0].Delta, 2)
	assert.Len(t, chunk[1].Delta, 3)

	chunk, limitReached = w.PromotionPrefixBounded(4, 3, 0)
	require.True(t, limitReached)
	require.Len(t, chunk, 3)
	assert.Equal(t, uint64(3), chunk[2].Slot)

	chunk, limitReached = w.PromotionPrefixBounded(4, 8, 20)
	assert.False(t, limitReached, "all eligible slots form a trailing partial chunk")
	require.Len(t, chunk, 4)

	chunk, limitReached = w.PromotionPrefixBounded(1, 8, 1)
	require.True(t, limitReached, "one semantic slot is returned even above the mutation limit")
	require.Len(t, chunk, 1)
	assert.Len(t, chunk[0].Delta, 2)
}

func BenchmarkWorkingSetPromotionPrefixBounded(b *testing.B) {
	const (
		slotCount       = 512
		accountsPerSlot = 64
	)
	w := NewWorkingSet()
	for slot := 0; slot < slotCount; slot++ {
		delta := make([]*Account, accountsPerSlot)
		for i := range delta {
			keyNumber := slot*accountsPerSlot + i
			key := solana.PublicKey{
				byte(keyNumber), byte(keyNumber >> 8), byte(keyNumber >> 16), byte(keyNumber >> 24),
			}
			delta[i] = &Account{Key: key, Lamports: uint64(slot + 1)}
		}
		if err := w.Add(uint64(slot+1), delta); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("full-rooted-prefix", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			batch := w.PromotionPrefix(slotCount)
			if len(batch) != slotCount {
				b.Fatal(len(batch))
			}
		}
	})
	b.Run("bounded-16-slot-chunk", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			batch, ready := w.PromotionPrefixBounded(slotCount, 16, 0)
			if len(batch) != 16 || !ready {
				b.Fatalf("len=%d ready=%t", len(batch), ready)
			}
		}
	})
}

func TestWorkingSetLookupBatchPreservesOrderAndMisses(t *testing.T) {
	w := NewWorkingSet()
	first := wsAcct(1, 100)
	latest := wsAcct(1, 700)
	second := wsAcct(2, 200)
	w.Add(5, []*Account{first, second})
	w.Add(7, []*Account{latest})

	keys := []solana.PublicKey{{1}, {9}, {2}, {1}}
	out := make([]*Account, len(keys))
	w.LookupBatch(keys, out)

	assert.Same(t, latest, out[0])
	assert.Nil(t, out[1])
	assert.Same(t, second, out[2])
	assert.Same(t, latest, out[3])
}

func BenchmarkWorkingSetLookupBlock(b *testing.B) {
	const keyCount = 30_000
	w := NewWorkingSet()
	keys := make([]solana.PublicKey, keyCount)
	delta := make([]*Account, keyCount/2)
	for i := range keys {
		keys[i][0] = byte(i)
		keys[i][1] = byte(i >> 8)
		keys[i][2] = byte(i >> 16)
		if i < len(delta) {
			delta[i] = &Account{Key: keys[i], Lamports: uint64(i + 1)}
		}
	}
	w.Add(1, delta)
	out := make([]*Account, len(keys))

	b.Run("per-key-lock", func(b *testing.B) {
		for range b.N {
			for _, key := range keys {
				_, _ = w.Lookup([32]byte(key))
			}
		}
	})
	b.Run("single-batch-lock", func(b *testing.B) {
		for range b.N {
			clear(out)
			w.LookupBatch(keys, out)
		}
	})
}

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

func TestWorkingSetPromotedLargeValueIsNotChargedThroughSurvivingUndo(t *testing.T) {
	const largeDataBytes = 2 << 20
	w := NewWorkingSetWithMaxRetainedBytes(3 << 20)
	large := &Account{
		Key:      solana.PublicKey{1},
		Lamports: 100,
		Data:     make([]byte, largeDataBytes),
	}
	newer := wsAcct(1, 700)
	require.NoError(t, w.Add(5, []*Account{large}))
	require.NoError(t, w.Add(7, []*Account{newer}))

	w.PromotePrefix(5)
	wantRetained := workingSetSlotFixedBytes + workingSetAccountFixedBytes
	stats := w.Stats()
	assert.Equal(t, uint64(1), stats.HeldSlots)
	assert.Equal(t, wantRetained, stats.RetainedBytes,
		"the surviving undo names a slot only and must not retain the promoted data charge")
	require.NoError(t, w.CheckInvariants())

	w.EvictFrom(7)
	_, ok := w.Lookup(wsKey(1))
	assert.False(t, ok, "a missing previous layer means the promoted durable value owns the key")
	assert.Zero(t, w.Stats().RetainedBytes)
	require.NoError(t, w.CheckInvariants())
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
