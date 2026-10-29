package leaderschedule

import (
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func nextLeaderByScan(ls *LeaderSchedule, identity solana.PublicKey, from uint64) (uint64, bool) {
	var best uint64
	found := false
	for slot, leader := range ls.lsMap {
		if leader.nodePubkey == identity && slot >= from && (!found || slot < best) {
			best, found = slot, true
		}
	}
	return best, found
}

func TestNextLeaderCacheMatchesScan(t *testing.T) {
	r := rand.New(rand.NewSource(47))
	ls := &LeaderSchedule{lsMap: make(map[uint64]slotLeader)}
	for i := 0; i < 2000; i++ {
		ls.lsMap[uint64(r.Intn(10000))] = slotLeader{nodePubkey: solana.PublicKey{byte(r.Intn(17))}}
	}
	ls.lsMap[math.MaxUint64] = slotLeader{nodePubkey: solana.PublicKey{3}}
	for i := 0; i < 400; i++ {
		id := solana.PublicKey{byte(r.Intn(20))}
		from := uint64(r.Intn(11000))
		want, ok := nextLeaderByScan(ls, id, from)
		got, found := ls.NextSlotForLeader(id, from)
		require.Equal(t, ok, found)
		require.Equal(t, want, got)
	}
	for _, from := range []uint64{0, math.MaxUint64 - 1, math.MaxUint64, 1, 0} {
		want, ok := nextLeaderByScan(ls, solana.PublicKey{3}, from)
		got, found := ls.NextSlotForLeader(solana.PublicKey{3}, from)
		require.Equal(t, ok, found)
		require.Equal(t, want, got)
	}
}

func TestNextLeaderCacheRetainsAbsentIdentityAndOutOfOrderQueries(t *testing.T) {
	ours, other := solana.PublicKey{1}, solana.PublicKey{2}
	ls := NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{ours: {7, 0, 5}, other: {1}}, 100)
	absent := solana.PublicKey{9}
	_, ok := ls.NextSlotForLeader(absent, 100)
	require.False(t, ok)
	cached := ls.nextLeader.Load()
	require.NotNil(t, cached)
	require.Empty(t, cached.slots)
	for _, from := range []uint64{101, 0, math.MaxUint64, 100} {
		_, ok = ls.NextSlotForLeader(absent, from)
		require.False(t, ok)
		require.Same(t, cached, ls.nextLeader.Load())
	}
	for _, from := range []uint64{108, 105, 100, 107, 0} {
		want, ok := nextLeaderByScan(ls, ours, from)
		got, found := ls.NextSlotForLeader(ours, from)
		require.Equal(t, ok, found)
		require.Equal(t, want, got)
	}
	require.Equal(t, ours, ls.nextLeader.Load().identity)
	var nilSchedule *LeaderSchedule
	_, ok = nilSchedule.NextSlotForLeader(ours, 0)
	require.False(t, ok)
	empty := &LeaderSchedule{}
	_, ok = empty.NextSlotForLeader(ours, 0)
	require.False(t, ok)
}

func TestNextLeaderCacheConcurrentIdentityChanges(t *testing.T) {
	ls := NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{{1}: {0, 2, 4}, {2}: {1, 3, 5}}, 100)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id solana.PublicKey) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				from := uint64(j % 109)
				want, ok := nextLeaderByScan(ls, id, from)
				got, found := ls.NextSlotForLeader(id, from)
				if got != want || found != ok {
					t.Errorf("identity %v from %d: got %d,%t want %d,%t", id, from, got, found, want, ok)
					return
				}
			}
		}(solana.PublicKey{byte(i%3 + 1)})
	}
	wg.Wait()
}

func BenchmarkNextLeaderTelemetryLookup(b *testing.B) {
	ls := &LeaderSchedule{lsMap: make(map[uint64]slotLeader, 432000)}
	for s := uint64(0); s < 432000; s++ {
		ls.lsMap[s] = slotLeader{nodePubkey: solana.PublicKey{byte(s%200 + 1)}}
	}
	for _, present := range []bool{false, true} {
		id := solana.PublicKey{255}
		label := "absent"
		if present {
			id = solana.PublicKey{1}
			label = "present"
		}
		b.Run(label+"/scan", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				nextLeaderByScan(ls, id, uint64(i%432000))
			}
		})
		b.Run(label+"/cached", func(b *testing.B) {
			ls.NextSlotForLeader(id, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ls.NextSlotForLeader(id, uint64(i%432000))
			}
		})
	}
}
