package global

import (
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/gagliardetto/solana-go"
)

func testLeader(firstByte byte) solana.PublicKey {
	var leader solana.PublicKey
	leader[0] = firstByte
	return leader
}

func testLeaderSchedule(leader solana.PublicKey, start uint64) *leaderschedule.LeaderSchedule {
	return leaderschedule.NewLeaderScheduleFromKeyedSlots(
		map[solana.PublicKey][]uint64{leader: {0, 1}},
		start,
	)
}

func TestEpochLeaderSchedulesCoexist(t *testing.T) {
	SetLeaderSchedule(nil)
	t.Cleanup(func() { SetLeaderSchedule(nil) })

	leader58 := testLeader(58)
	leader59 := testLeader(59)
	SetLeaderScheduleForEpoch(58, testLeaderSchedule(leader58, 100))
	SetLeaderScheduleForEpoch(59, testLeaderSchedule(leader59, 200))

	if got, ok := LeaderForSlot(100); !ok || got != leader58 {
		t.Fatalf("epoch 58 schedule missing after epoch 59 install: got %s, ok=%t", got, ok)
	}
	if got, ok := LeaderForSlot(200); !ok || got != leader59 {
		t.Fatalf("epoch 59 schedule missing: got %s, ok=%t", got, ok)
	}
	if _, ok := LeaderForSlot(300); ok {
		t.Fatalf("unexpected leader outside installed schedules")
	}
}

func TestEpochLeaderScheduleConcurrentReadsAndReplacement(t *testing.T) {
	SetLeaderSchedule(nil)
	t.Cleanup(func() { SetLeaderSchedule(nil) })

	const start = uint64(1_000)
	SetLeaderScheduleForEpoch(1, testLeaderSchedule(testLeader(1), start))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1_000; j++ {
				LeaderForSlot(start)
			}
		}()
	}
	for i := 0; i < 1_000; i++ {
		SetLeaderScheduleForEpoch(1, testLeaderSchedule(testLeader(byte(i%250+1)), start))
	}
	wg.Wait()

	if _, ok := LeaderForSlot(start); !ok {
		t.Fatalf("replacement lost the epoch schedule")
	}
}

func TestNextSlotForLeaderAcrossEpochs(t *testing.T) {
	SetLeaderSchedule(nil)
	t.Cleanup(func() { SetLeaderSchedule(nil) })

	ours := testLeader(7)
	other := testLeader(8)
	SetLeaderScheduleForEpoch(1, testLeaderSchedule(ours, 100))
	SetLeaderScheduleForEpoch(2, leaderschedule.NewLeaderScheduleFromKeyedSlots(
		map[solana.PublicKey][]uint64{ours: {0}, other: {1}},
		200,
	))

	if slot, ok := NextSlotForLeader(ours, 100); !ok || slot != 100 {
		t.Fatalf("got slot=%d ok=%t, want 100", slot, ok)
	}
	if slot, ok := NextSlotForLeader(ours, 102); !ok || slot != 200 {
		t.Fatalf("got slot=%d ok=%t, want 200", slot, ok)
	}
	if _, ok := NextSlotForLeader(ours, 201); ok {
		t.Fatalf("expected no later slot for ours")
	}
}
