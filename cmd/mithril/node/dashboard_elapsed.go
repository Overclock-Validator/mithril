package node

import (
	"math"
	"time"
)

// sampledRunElapsed requires the original monotonic clocks. It never infers
// uptime from UTC dates, extrapolates after disconnect, or wraps saturation.
func sampledRunElapsed(started, sampled time.Time) *uint64 {
	if started.IsZero() || started == started.Round(0) || sampled == sampled.Round(0) {
		return nil
	}
	elapsed := sampled.Sub(started)
	if elapsed < 0 || elapsed == time.Duration(math.MaxInt64) {
		return nil
	}
	value := uint64(elapsed)
	return &value
}
