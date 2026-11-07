package node

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

type cacheActivityEndpoint struct {
	value accountsdb.CacheActivityObservation
	at    time.Time
}

// Seven existing ten-second samples bound this observed interval to one minute.
// It may be shorter after jitter, reset, a gap or faster-than-usual sampling.
type cacheActivityWindow struct {
	points [7]cacheActivityEndpoint
	count  int
}

func cacheActivityCountersMonotone(before, after accountsdb.CacheLookupActivity) bool {
	return after.Hits >= before.Hits && after.Misses >= before.Misses && after.Evictions >= before.Evictions && after.EvictionWeight >= before.EvictionWeight
}

func (w *cacheActivityWindow) observe(value accountsdb.CacheActivityObservation, known bool, started, completed time.Time) *dashboard.CacheActivityWindow {
	if !known || started.IsZero() || completed.IsZero() || completed.Before(started) || completed.Sub(started) > time.Second ||
		value.Instance == 0 || value.Instance == ^uint64(0) || value.Generation == 0 || value.Generation == ^uint64(0) || value.Instance > value.Generation {
		*w = cacheActivityWindow{}
		return nil
	}
	if w.count > 0 {
		before := w.points[w.count-1]
		elapsed := completed.Sub(before.at)
		if before.value.Generation != value.Generation || before.value.Instance != value.Instance || elapsed <= 0 || elapsed > time.Minute ||
			!cacheActivityCountersMonotone(before.value.Vote, value.Vote) || !cacheActivityCountersMonotone(before.value.Program, value.Program) ||
			!cacheActivityCountersMonotone(before.value.CommonLarge, value.CommonLarge) {
			*w = cacheActivityWindow{}
		}
	}
	for w.count > 0 && completed.Sub(w.points[0].at) > time.Minute {
		copy(w.points[:], w.points[1:w.count])
		w.count--
	}
	if w.count == len(w.points) {
		copy(w.points[:], w.points[1:])
		w.count--
	}
	w.points[w.count] = cacheActivityEndpoint{value: value, at: completed}
	w.count++
	if w.count < 2 {
		return nil
	}
	first := w.points[0]
	delta := func(before, after accountsdb.CacheLookupActivity) dashboard.CacheLookupDelta {
		return dashboard.CacheLookupDelta{Hits: after.Hits - before.Hits, Misses: after.Misses - before.Misses}
	}
	return &dashboard.CacheActivityWindow{StartedAt: first.at.UTC(), EndedAt: completed.UTC(), DurationNanoseconds: uint64(completed.Sub(first.at)),
		Vote: delta(first.value.Vote, value.Vote), Program: delta(first.value.Program, value.Program), CommonLarge: delta(first.value.CommonLarge, value.CommonLarge)}
}
