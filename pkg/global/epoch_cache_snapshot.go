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
	stakes         epochstakes.CacheView
	schedules      map[uint64]*leaderschedule.LeaderSchedule
	legacySchedule *leaderschedule.LeaderSchedule
}

func CaptureEpochCaches() *EpochCacheSnapshot {
	view := &EpochCacheSnapshot{stakes: instance.epochStakes.CaptureView()}
	instance.leaderScheduleMutex.RLock()
	view.schedules = maps.Clone(instance.leaderSchedules)
	view.legacySchedule = instance.leaderSchedule
	instance.leaderScheduleMutex.RUnlock()
	return view
}

func RestoreEpochCaches(view *EpochCacheSnapshot) {
	if view == nil {
		return
	}
	instance.epochStakes.RestoreView(view.stakes)
	instance.leaderScheduleMutex.Lock()
	instance.leaderSchedules = maps.Clone(view.schedules)
	instance.leaderSchedule = view.legacySchedule
	instance.leaderScheduleMutex.Unlock()
}
