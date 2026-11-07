package dashboard

import "time"

const clusterClockTolerance = time.Second

func clusterClockConsistentAges(monotonic, wall time.Duration) bool {
	if monotonic < 0 || wall < 0 {
		return false
	}
	if monotonic > wall {
		return monotonic-wall <= clusterClockTolerance
	}
	return wall-monotonic <= clusterClockTolerance
}

func clusterClockConsistent(start, now time.Time) bool {
	if start.IsZero() || start == start.Round(0) || now == now.Round(0) {
		return false
	}
	return clusterClockConsistentAges(now.Sub(start), now.Round(0).Sub(start.Round(0)))
}
