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
			a.Data = make([]byte, 50+rng.Intn(200))
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

	// The fold sees every per-slot version exactly.
	got := w.PromotionPrefix(slot)
	want := ref.PromotionPrefix(slot)
	require.Equal(t, len(want), len(got))
	for i := range want {
		require.Equal(t, want[i].Slot, got[i].Slot)
		sortDelta(want[i].Delta)
		sortDelta(got[i].Delta)
		require.Equal(t, len(want[i].Delta), len(got[i].Delta), "slot %d", want[i].Slot)
		for j := range want[i].Delta {
			require.True(t, sameAccount(want[i].Delta[j], got[i].Delta[j]), "slot %d", want[i].Slot)
		}
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
