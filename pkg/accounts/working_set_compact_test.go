package accounts

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func sameAccount(a, b *Account) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Key == b.Key && a.Lamports == b.Lamports && a.Owner == b.Owner && a.Executable == b.Executable &&
		a.RentEpoch == b.RentEpoch && a.Slot == b.Slot && bytes.Equal(a.Data, b.Data)
}

// Compaction must not change any value a reader, the fold or an eviction sees.
func TestWorkingSetCompactionPreservesVersions(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	keys := make([][32]byte, 25)
	for i := range keys {
		rng.Read(keys[i][:])
	}
	w := NewWorkingSet()
	ref := NewWorkingSet() // the same writes, never compacted
	written := map[uint64]map[[32]byte]*Account{}
	newAcct := func(k [32]byte, slot uint64, prev *Account) *Account {
		a := &Account{Key: solana.PublicKey(k), Lamports: uint64(rng.Intn(1e6)), Slot: slot, RentEpoch: 3}
		if prev != nil {
			a.Data = bytes.Clone(prev.Data)
			if len(a.Data) > 0 {
				a.Data[rng.Intn(len(a.Data))] ^= 0x5a
			}
			if rng.Intn(6) == 0 {
				a.Data = append(a.Data, 1, 2, 3)
			}
			a.Owner = prev.Owner
		} else {
			a.Data = make([]byte, 500+rng.Intn(400))
			rng.Read(a.Data[:20])
		}
		return a
	}
	latest := map[[32]byte]*Account{}
	slot := uint64(1000)
	finalized := uint64(0)
	for step := 0; step < 120; step++ {
		var delta []*Account
		for _, k := range keys {
			if rng.Intn(3) == 0 {
				a := newAcct(k, slot, latest[k])
				delta = append(delta, a)
				latest[k] = a
			}
		}
		w.Add(slot, delta)
		ref.Add(slot, delta)
		written[slot] = map[[32]byte]*Account{}
		for _, a := range delta {
			written[slot][[32]byte(a.Key)] = a
		}
		slot++
		// finality trails the tip by a few slots
		if slot-1000 > 5 {
			finalized = slot - 5
			w.CompactThrough(finalized)
		}
		switch {
		case step%17 == 16 && finalized > 1000:
			// a fork switch above finality
			cut := finalized + 1 + uint64(rng.Intn(3))
			w.EvictFrom(cut)
			ref.EvictFrom(cut)
			for s := cut; s < slot; s++ {
				delete(written, s)
			}
			slot = cut
			latest = map[[32]byte]*Account{}
			for _, k := range keys {
				if a, ok := ref.Lookup(k); ok {
					latest[k] = a
				}
			}
		case step%23 == 22:
			through := finalized - 10
			require.Equal(t, len(ref.PromotionPrefix(through)), len(w.PromotionPrefix(through)))
			w.PromotePrefix(through)
			ref.PromotePrefix(through)
		}
		require.NoError(t, w.CheckInvariants(), "step %d", step)
		// Every held version, at every held slot, reads the same.
		ref.mu.RLock()
		order := append([]uint64(nil), ref.order...)
		ref.mu.RUnlock()
		for _, s := range order {
			for _, k := range keys {
				want, okw := ref.LookupAt(k, s)
				got, okg := w.LookupAt(k, s)
				require.Equal(t, okw, okg)
				require.True(t, sameAccount(want, got), "step %d slot %d", step, s)
			}
		}
	}
	require.Positive(t, w.CompactedThrough())
	require.Less(t, w.RetainedBytes(), ref.RetainedBytes(), "compaction must save memory")

	// The fold sees every per-slot version exactly, and each key's newest
	// batch version in full.
	resolved := func(batch []SlotDelta) map[uint64][]*Account {
		out := map[uint64][]*Account{}
		ResolveVersions(batch, func(s uint64, a *Account) { out[s] = append(out[s], a) })
		return out
	}
	gotBatch := w.PromotionPrefix(slot)
	want := resolved(ref.PromotionPrefix(slot))
	got := resolved(gotBatch)
	require.Equal(t, len(want), len(got))
	for s, wv := range want {
		gv := got[s]
		sortDelta(wv)
		sortDelta(gv)
		require.Equal(t, len(wv), len(gv), "slot %d", s)
		for j := range wv {
			require.True(t, sameAccount(wv[j], gv[j]), "slot %d", s)
		}
	}
	seen := map[[32]byte]bool{}
	for i := len(gotBatch) - 1; i >= 0; i-- {
		for _, a := range gotBatch[i].Delta {
			seen[[32]byte(a.Key)] = true
		}
		for _, c := range gotBatch[i].Compacted {
			require.True(t, seen[c.Key], "a compacted version must have a newer full one in the batch")
		}
	}
	// A partial batch (the oldest few slots) also resolves exactly.
	part := w.PromotionChunk(slot, 7, true)
	refPart := resolved(ref.PromotionChunk(slot, 7, true))
	for s, gv := range resolved(part) {
		require.Equal(t, len(refPart[s]), len(gv), "partial slot %d", s)
	}

	// RangeVersions visits the same versions.
	count := func(ws *WorkingSet) int {
		n := 0
		ws.RangeVersions(func(uint64, *Account) { n++ })
		return n
	}
	require.Equal(t, count(ref), count(w))

	// An eviction reaching into compacted slots restores full versions first.
	cut := w.CompactedThrough()
	w.EvictFrom(cut)
	ref.EvictFrom(cut)
	require.NoError(t, w.CheckInvariants())
	for _, k := range keys {
		want, okw := ref.Lookup(k)
		got, okg := w.Lookup(k)
		require.Equal(t, okw, okg)
		require.True(t, sameAccount(want, got))
	}
}

func sortDelta(d []*Account) {
	sort.Slice(d, func(i, j int) bool { return bytes.Compare(d[i].Key[:], d[j].Key[:]) < 0 })
}

// Finality can be reported before the tail holds those slots; compaction must
// not mark slots it has not seen as covered.
func TestWorkingSetCompactionFinalityAhead(t *testing.T) {
	w := NewWorkingSet()
	k := solana.PublicKey{1}
	w.Add(10, []*Account{{Key: k, Lamports: 1, Data: make([]byte, 600)}})
	require.Equal(t, 0, w.CompactThrough(20)) // 11..20 not held yet
	require.EqualValues(t, 10, w.CompactedThrough())
	w.Add(11, []*Account{{Key: k, Lamports: 2, Data: make([]byte, 600)}})
	require.Equal(t, 1, w.CompactThrough(20))
	require.EqualValues(t, 11, w.CompactedThrough())
	a, ok := w.LookupAt([32]byte(k), 10)
	require.True(t, ok)
	require.EqualValues(t, 1, a.Lamports)
}

// A dummy placeholder between two real versions must end the compaction
// chain, so rebuilding the older version never starts from the dummy.
func TestCompactionStopsAtDummy(t *testing.T) {
	var k [32]byte
	k[0] = 7
	mk := func(slot uint64, b byte) *Account {
		a := &Account{Key: solana.PublicKey(k), Lamports: 5, Slot: slot, Data: make([]byte, 700)}
		a.Data[3] = b
		return a
	}
	w := NewWorkingSet()
	a10 := mk(10, 1)
	w.Add(10, []*Account{a10})
	w.Add(11, []*Account{{Key: solana.PublicKey(k), IsDummy: true, Slot: 11}})
	w.Add(12, []*Account{mk(12, 2)})
	w.Add(13, []*Account{mk(13, 3)})
	w.CompactThrough(13)
	got, ok := w.LookupAt(k, 10)
	require.True(t, ok)
	require.True(t, sameAccount(got, a10))
	got, ok = w.LookupAt(k, 12)
	require.True(t, ok)
	require.EqualValues(t, 2, got.Data[3])
	w.RangeVersions(func(uint64, *Account) {})
}
