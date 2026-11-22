package dashboard

import (
	"math"
	"time"
)

type statusFamily uint8

const (
	statusCache statusFamily = iota
	statusReplay
	statusNetwork
	statusNetworkNode
	statusProduction
	statusStorage
	statusPeers
	statusMachine
	statusReception
	statusDisplay
	statusFamilyCount
)

// statusStamp is internal to the reader. Zero is uninitialized and MaxUint64
// retires a counter permanently rather than allowing an old epoch to recur.
type statusStamp struct {
	nodeEpoch   uint64
	familyEpoch uint64
}

type statusFence struct {
	nodeEpoch uint64
	families  [statusFamilyCount]uint64
}

func (s statusStamp) valid() bool {
	return s.nodeEpoch != 0 && s.nodeEpoch != math.MaxUint64 &&
		s.familyEpoch != 0 && s.familyEpoch != math.MaxUint64
}

func (f *frameSet) statusStamp(family statusFamily) statusStamp {
	if f == nil || family >= statusFamilyCount {
		return statusStamp{}
	}
	return statusStamp{nodeEpoch: f.fence.nodeEpoch, familyEpoch: f.fence.families[family]}
}

// admitLiveDetail checks the selected family against one current status frame
// and its gate. A concurrent ordinary refresh may replace the frame without
// changing its context, so retry a bounded number of times.
func (c *collector) admitLiveDetail(family statusFamily, selected statusStamp, readGate func() (statusStamp, bool), now time.Time) (time.Duration, bool) {
	if !selected.valid() || readGate == nil {
		return 0, false
	}
	for range 2 {
		frame := c.latest.Load()
		if frame == nil || frame.freshUntil.IsZero() {
			return 0, false
		}
		gate, eligible := readGate()
		if c.latest.Load() != frame {
			continue
		}
		secondGate, secondEligible := readGate()
		if c.latest.Load() != frame || gate != secondGate || eligible != secondEligible {
			continue
		}
		if !eligible || selected != gate || selected != frame.statusStamp(family) {
			return 0, false
		}
		if current := time.Now(); current.After(now) {
			now = current
		}
		remaining, valid := frame.remainingStatus(0, now)
		return remaining, valid && remaining.Milliseconds() > 0
	}
	return 0, false
}

func sameStatusBinding(a, b *Snapshot) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	sameOptional := func(x, y *string) bool {
		return (x == nil && y == nil) || (x != nil && y != nil && *x == *y)
	}
	return a.RunID == b.RunID && a.StartedAt.Equal(b.StartedAt) &&
		sameOptional(a.GenesisHash, b.GenesisHash) && sameOptional(a.IdentityKey, b.IdentityKey)
}

// Both bounds matter: a small wall-clock step may expire status before its
// monotonic deadline without exceeding the reader's clock-skew tolerance.
func statusLeaseContinuous(monotonicRemaining, wallRemaining time.Duration) bool {
	return monotonicRemaining > 0 && wallRemaining > 0
}

// nextNodeEpoch measures continuity between source receipts, not the ordering
// of HTTP responses. The previous epoch is owned even when no frame is cached.
func nextNodeEpoch(previous uint64, old, next *Snapshot, previousFreshUntil, previousWallUntil time.Time, nextState string, receivedAt time.Time) (uint64, bool) {
	if previous == math.MaxUint64 || (nextState != "fresh" && nextState != "stale" && nextState != "unavailable") {
		return math.MaxUint64, false
	}
	if nextState == "fresh" && (next == nil || receivedAt.IsZero() || receivedAt == receivedAt.Round(0) ||
		(!previousFreshUntil.IsZero() && previousFreshUntil == previousFreshUntil.Round(0)) ||
		previousFreshUntil.IsZero() != previousWallUntil.IsZero()) {
		return math.MaxUint64, false
	}
	if previous == 0 {
		return 1, true
	}
	wasFresh, isFresh := !previousFreshUntil.IsZero(), nextState == "fresh"
	changed := !sameStatusBinding(old, next) || wasFresh != isFresh ||
		(wasFresh && isFresh && !statusLeaseContinuous(previousFreshUntil.Sub(receivedAt),
			previousWallUntil.Sub(receivedAt.Round(0))))
	if !changed {
		return previous, true
	}
	if previous >= math.MaxUint64-1 {
		return math.MaxUint64, false
	}
	return previous + 1, true
}

func advanceStatusStamp(prev statusStamp, nodeEpoch uint64, sameFamilyContext bool) (statusStamp, bool) {
	if nodeEpoch == 0 || nodeEpoch == math.MaxUint64 || prev.familyEpoch == math.MaxUint64 {
		return statusStamp{nodeEpoch: nodeEpoch, familyEpoch: math.MaxUint64}, false
	}
	if prev.familyEpoch == 0 {
		return statusStamp{nodeEpoch: nodeEpoch, familyEpoch: 1}, true
	}
	if prev.nodeEpoch == nodeEpoch && sameFamilyContext {
		return prev, true
	}
	if prev.familyEpoch >= math.MaxUint64-1 {
		return statusStamp{nodeEpoch: nodeEpoch, familyEpoch: math.MaxUint64}, false
	}
	return statusStamp{nodeEpoch: nodeEpoch, familyEpoch: prev.familyEpoch + 1}, true
}
