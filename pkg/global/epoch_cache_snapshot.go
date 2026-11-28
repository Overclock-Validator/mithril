package global

import (
	"maps"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
)

// EpochCacheSnapshot retains only small outer maps. Epoch stake payloads and
// completed leader schedules are immutable and shared across all banks until
// a transition publishes a replacement. Capture/restore is replay-loop owned.
type EpochCacheSnapshot struct {
	stakes epochstakes.CacheView
	LeaderScheduleSnapshot
}

// LeaderScheduleSnapshot pins immutable schedules without holding a lock during a scan.
type LeaderScheduleSnapshot struct {
	generation     uint64
	schedules      map[uint64]*leaderschedule.LeaderSchedule
	legacySchedule *leaderschedule.LeaderSchedule
}

func CaptureEpochCaches() *EpochCacheSnapshot {
	return &EpochCacheSnapshot{stakes: instance.epochStakes.CaptureView(), LeaderScheduleSnapshot: CaptureLeaderSchedules()}
}

func CaptureLeaderSchedules() LeaderScheduleSnapshot {
	instance.leaderScheduleMutex.RLock()
	defer instance.leaderScheduleMutex.RUnlock()
	return LeaderScheduleSnapshot{instance.leaderScheduleGeneration, maps.Clone(instance.leaderSchedules), instance.leaderSchedule}
}

func (s LeaderScheduleSnapshot) ForEpoch(epoch uint64) *leaderschedule.LeaderSchedule {
	if s.legacySchedule != nil {
		return s.legacySchedule
	}
	return s.schedules[epoch]
}

// Current checks that a separately read bank was sampled under the same schedules.
func (s LeaderScheduleSnapshot) Current() bool {
	instance.leaderScheduleMutex.RLock()
	defer instance.leaderScheduleMutex.RUnlock()
	return s.generation == instance.leaderScheduleGeneration
}

func RestoreEpochCaches(view *EpochCacheSnapshot) {
	if view == nil {
		return
	}
	instance.epochStakes.RestoreView(view.stakes)
	instance.leaderScheduleMutex.Lock()
	instance.leaderScheduleGeneration++
	instance.leaderSchedules = maps.Clone(view.schedules)
	instance.leaderSchedule = view.legacySchedule
	instance.leaderScheduleMutex.Unlock()
}
