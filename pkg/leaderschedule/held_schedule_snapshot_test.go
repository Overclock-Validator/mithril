package leaderschedule

import (
	"encoding/binary"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func heldScheduleKey(n uint64) solana.PublicKey {
	var key solana.PublicKey
	binary.LittleEndian.PutUint64(key[:8], n+1)
	return key
}

func TestCaptureHeldScheduleExactRangesAndGrouping(t *testing.T) {
	a, b := heldScheduleKey(0), heldScheduleKey(1)
	for _, tc := range []struct {
		name   string
		first  uint64
		values []solana.PublicKey
		stride uint8
	}{
		{"grouped", 32, []solana.PublicKey{a, a, a, a, b, b, b, b}, 4},
		{"partial tail", 99, []solana.PublicKey{a, a, a, a, b, b}, 4},
		{"different member", 0, []solana.PublicKey{a, a, b, a, b}, 1},
		{"later group differs", 0, []solana.PublicKey{a, a, a, a, b, b, a, b}, 1},
		{"later tail differs", 0, []solana.PublicKey{a, a, a, a, b, a}, 1},
		{"unknown group", 0, []solana.PublicKey{{}, {}, {}, {}, a, a, a, a}, 4},
		{"partial known", 0, []solana.PublicKey{a, {}, a, a}, 1},
		{"empty held range", 8, []solana.PublicKey{{}, {}, {}}, 4},
		{"maximum slot", math.MaxUint64 - 1, []solana.PublicKey{a, a}, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls := &LeaderSchedule{lsMap: map[uint64]slotLeader{}}
			for i, key := range tc.values {
				if !key.IsZero() {
					ls.lsMap[tc.first+uint64(i)] = slotLeader{nodePubkey: key}
				}
			}
			ls.lsMap[tc.first-1] = slotLeader{nodePubkey: heldScheduleKey(2)}
			// First0's outside sentinel wraps to MaxUint64; never an in-range row.
			ls.NextSlotForLeader(a, tc.first)
			identityCache := ls.nextLeader.Load()
			got, err := ls.CaptureHeldSchedule(tc.first, uint64(len(tc.values)))
			if err != nil || got.Stride != tc.stride {
				t.Fatalf("capture stride %d, want %d: %v", got.Stride, tc.stride, err)
			}
			if ls.nextLeader.Load() != identityCache {
				t.Fatal("diagnostic copy replaced the identity cache")
			}
			var known uint64
			for i, want := range tc.values {
				key, ok := got.LeaderForSlot(tc.first + uint64(i))
				if key != want || ok != !want.IsZero() {
					t.Fatalf("offset%d: %s/%t, want%s/%t", i, key, ok, want, !want.IsZero())
				}
				if ok {
					known++
				}
			}
			if got.KnownSlots != known {
				t.Fatalf("known %d, want %d", got.KnownSlots, known)
			}
			again, err := ls.CaptureHeldSchedule(tc.first, uint64(len(tc.values)))
			if err != nil || !reflect.DeepEqual(got, again) {
				t.Fatal("copy is not deterministic")
			}
			clear(ls.lsMap)
			for i, want := range tc.values {
				key, _ := got.LeaderForSlot(tc.first + uint64(i))
				if key != want {
					t.Fatal("copy borrowed schedule data")
				}
			}
		})
	}
}

func TestCaptureHeldScheduleLimitsAndConcurrentReaders(t *testing.T) {
	var absent *LeaderSchedule
	if _, err := absent.CaptureHeldSchedule(0, 1); err == nil {
		t.Fatal("nil schedule accepted")
	}
	ls := &LeaderSchedule{lsMap: map[uint64]slotLeader{}}
	for _, pair := range [][2]uint64{{0, 0}, {0, MaxHeldScheduleSlots + 1}, {math.MaxUint64, 2}} {
		if _, err := ls.CaptureHeldSchedule(pair[0], pair[1]); err == nil {
			t.Fatal("invalid range accepted")
		}
	}
	for i := uint64(0); i <= MaxHeldScheduleLeaders; i++ {
		ls.lsMap[i] = slotLeader{nodePubkey: heldScheduleKey(i)}
	}
	if _, err := ls.CaptureHeldSchedule(0, MaxHeldScheduleLeaders+1); err == nil {
		t.Fatal("identity limit bypassed")
	}
	delete(ls.lsMap, MaxHeldScheduleLeaders)
	got, err := ls.CaptureHeldSchedule(0, MaxHeldScheduleSlots)
	if err != nil || len(got.Leaders) != MaxHeldScheduleLeaders || got.KnownSlots != MaxHeldScheduleLeaders || got.Stride != 1 || len(got.Indices) != 2*MaxHeldScheduleSlots {
		t.Fatalf("maximum snapshot: leaders%d known%d stride%d bytes%d: %v", len(got.Leaders), got.KnownSlots, got.Stride, len(got.Indices), err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Go(func() {
			for i := uint64(0); i < MaxHeldScheduleLeaders; i++ {
				key, ok := got.LeaderForSlot(i)
				if !ok || key != heldScheduleKey(i) {
					t.Error("concurrent copy lookup mismatch")
					return
				}
			}
		})
	}
	wg.Wait()
	if n := testing.AllocsPerRun(100, func() { got.LeaderForSlot(100) }); n != 0 {
		t.Fatalf("lookup allocates: %f", n)
	}
}
