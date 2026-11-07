package global

import (
	"sync"
	"testing"
)

func TestLeaderSchedulesForEpochsSnapshot(t *testing.T) {
	SetLeaderSchedule(nil)
	t.Cleanup(func() { SetLeaderSchedule(nil) })
	a := testLeaderSchedule(testLeader(1), 100)
	b := testLeaderSchedule(testLeader(2), 100)
	SetLeaderScheduleForEpoch(1, a)
	old := LeaderSchedulesForEpochs([]uint64{1, 2, 1})
	if old[0] != a || old[1] != nil || old[2] != a {
		t.Fatalf("unexpected initial snapshot: %v", old)
	}
	SetLeaderScheduleForEpoch(1, b)
	SetLeaderScheduleForEpoch(1, nil)
	if got, ok := old[0].LeaderForSlot(100); !ok || got != testLeader(1) {
		t.Fatalf("old snapshot changed: %v, %v", got, ok)
	}
	SetLeaderSchedule(a)
	legacy := LeaderSchedulesForEpochs([]uint64{1, 2})
	if legacy[0] != a || legacy[1] != a {
		t.Fatalf("unexpected legacy snapshot: %v", legacy)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			SetLeaderSchedule(a)
			SetLeaderScheduleForEpoch(1, b)
			SetLeaderScheduleForEpoch(1, nil)
		}
	}()
	for i := 0; i < 1000; i++ {
		s := LeaderSchedulesForEpochs([]uint64{1, 2, 1})
		if s[0] != s[2] || (s[1] != nil && (s[0] != a || s[1] != a)) {
			t.Errorf("incoherent snapshot: %v", s)
			break
		}
	}
	wg.Wait()
}
