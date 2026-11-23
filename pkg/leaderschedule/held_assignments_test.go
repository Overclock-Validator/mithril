package leaderschedule

import (
	"math"
	"sync"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestCachedAssignmentsForLeader(t *testing.T) {
	a, b := solana.PublicKey{1}, solana.PublicKey{2}
	indices := make([]uint64, 40)
	for i := range indices {
		indices[i] = uint64(i)
	}
	ls := NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{a: indices, b: {40}}, 0)
	if _, ok := ls.CachedAssignmentsForLeader(a, 0, 0, 40); ok || ls.nextLeader.Load() != nil {
		t.Fatal("dashboard query warmed a cold identity index")
	}
	ls.NextSlotForLeader(a, 0)
	s, ok := ls.CachedAssignmentsForLeader(a, 10, 16, 30)
	if !ok || s.Total != 21 || s.Remaining != 14 || s.Count != 14 || s.Upcoming[0] != 17 || s.Upcoming[13] != 30 {
		t.Fatalf("partial range summary: %+v known=%v", s, ok)
	}
	s, ok = ls.CachedAssignmentsForLeader(a, 0, 0, 40)
	if !ok || s.Total != 40 || s.Remaining != 39 || s.Count != 16 || s.Upcoming[15] != 16 {
		t.Fatalf("bounded preview: %+v known=%v", s, ok)
	}
	s.Upcoming[0] = 999
	if current, _ := ls.CachedAssignmentsForLeader(a, 0, 0, 40); current.Upcoming[0] != 1 {
		t.Fatal("returned summary aliases the immutable identity index")
	}
	for _, bounds := range [][3]uint64{{1, 0, 2}, {0, 3, 2}} {
		if _, ok := ls.CachedAssignmentsForLeader(a, bounds[0], bounds[1], bounds[2]); ok {
			t.Fatal("invalid range accepted")
		}
	}
	if n := testing.AllocsPerRun(1000, func() { ls.CachedAssignmentsForLeader(a, 0, 0, 40) }); n != 0 {
		t.Fatalf("cached summary allocated: %v", n)
	}
	ls.NextSlotForLeader(b, 0)
	if _, ok := ls.CachedAssignmentsForLeader(a, 0, 0, 40); ok {
		t.Fatal("different cached identity treated as known")
	}
	zero := solana.PublicKey{3}
	ls.NextSlotForLeader(zero, 0)
	if s, ok := ls.CachedAssignmentsForLeader(zero, 0, 0, math.MaxUint64); !ok || s.Total != 0 || s.Remaining != 0 || s.Count != 0 {
		t.Fatal("warmed known-empty identity became unknown")
	}
	last := NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{a: {math.MaxUint64}}, 0)
	last.NextSlotForLeader(a, 0)
	if s, ok := last.CachedAssignmentsForLeader(a, 0, math.MaxUint64-1, math.MaxUint64); !ok || s.Remaining != 1 || s.Upcoming[0] != math.MaxUint64 {
		t.Fatal("last representable assignment lost")
	}
	if s, ok := last.CachedAssignmentsForLeader(a, 0, math.MaxUint64, math.MaxUint64); !ok || s.Remaining != 0 || s.Count != 0 {
		t.Fatal("strict-after boundary wrapped")
	}
	var nilSchedule *LeaderSchedule
	if _, ok := nilSchedule.CachedAssignmentsForLeader(a, 0, 0, 1); ok {
		t.Fatal("nil schedule is known")
	}
	var workers sync.WaitGroup
	for _, key := range []solana.PublicKey{a, b} {
		workers.Go(func() {
			for range 100 {
				ls.NextSlotForLeader(key, 0)
				if s, ok := ls.CachedAssignmentsForLeader(key, 0, 0, 40); ok && (key == a && s.Total != 40 || key == b && s.Total != 1) {
					t.Error("cached summary mixed identity generations")
				}
			}
		})
	}
	workers.Wait()
}
