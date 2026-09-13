package scheduler

import (
	"container/heap"
	"encoding/binary"
	"math/rand"
	"sort"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestMaxHeapRemovalMatchesSortedOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(417))
	for _, count := range []int{1, 2, 3, 4, 7, 8, 9, 127, 128, 129, 4095, 4096, 4097} {
		var h maxHeap
		want := make([]*entry, count)
		for i := range want {
			want[i] = &entry{seq: uint64(i), reward: uint64(rng.Intn(17))}
			heap.Push(&h, want[i])
		}
		sort.Slice(want, func(i, j int) bool {
			if want[i].reward == want[j].reward {
				return want[i].seq < want[j].seq
			}
			return want[i].reward > want[j].reward
		})
		backing := h[:cap(h)]
		for i, expected := range want {
			require.Same(t, expected, h.popEntry(), "count=%d pop=%d", count, i)
			require.Nil(t, backing[len(h)], "removed pointer retained")
		}
	}
}

// Compare interleaved insertion, eviction, cleanup and removal with a small
// unsorted reference model. Reusing hashes after removal also exercises stale
// nodes left in the other heap without reusing the entry objects themselves.
func TestBufferMixedOperationsMatchReference(t *testing.T) {
	const capacity = 64
	rng := rand.New(rand.NewSource(418))
	b := NewBuffer(capacity)
	model := make(map[[32]byte]*entry)
	best := func(high bool) *entry {
		var found *entry
		for _, e := range model {
			if found == nil || (high && (e.reward > found.reward || e.reward == found.reward && e.seq < found.seq)) ||
				(!high && (e.reward < found.reward || e.reward == found.reward && e.seq > found.seq)) {
				found = e
			}
		}
		return found
	}
	for step := 0; step < 10000; step++ {
		switch action := rng.Intn(10); {
		case action < 7:
			e := &entry{tx: &solana.Transaction{}, seq: uint64(step), reward: uint64(rng.Intn(16))}
			binary.LittleEndian.PutUint64(e.messageHash[:], uint64(rng.Intn(256)))
			wantResult := InsertAccepted
			var wantEvicted *entry
			if _, duplicate := model[e.messageHash]; duplicate {
				wantResult = InsertDuplicate
			} else if len(model) == capacity {
				lowest := best(false)
				if e.reward <= lowest.reward {
					wantResult = InsertRejectedCapacity
				} else {
					wantEvicted = lowest
					delete(model, lowest.messageHash)
				}
			}
			if wantResult == InsertAccepted {
				model[e.messageHash] = e
			}
			got, evicted := b.Insert(e)
			require.Equal(t, wantResult, got, "step=%d", step)
			require.True(t, wantEvicted == evicted, "eviction differs at step=%d", step)
		case action < 9:
			want := best(true)
			got := b.PopMax()
			require.True(t, want == got, "selection differs at step=%d", step)
			if want != nil {
				delete(model, want.messageHash)
			}
		default:
			mod := uint64(rng.Intn(11))
			want := 0
			for hash, e := range model {
				if e.seq%11 == mod {
					delete(model, hash)
					want++
				}
			}
			require.Equal(t, want, b.Cleanup(func(e *entry) bool { return e.seq%11 == mod }))
		}
		require.Equal(t, len(model), b.Len(), "step=%d", step)
	}
	for len(model) > 0 {
		want := best(true)
		require.Same(t, want, b.PopMax())
		delete(model, want.messageHash)
	}
	require.Nil(t, b.PopMax())
}
