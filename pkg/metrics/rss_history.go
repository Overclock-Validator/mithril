package metrics

import (
	"time"
	"unicode/utf8"
)

const rssHourSamples = 400

// RSSHistoryBinding identifies the node run and verified chain of an RSS sample.
// Binding a previously unknown genesis starts new history.
type RSSHistoryBinding struct {
	RunID       string
	StartedAt   time.Time
	GenesisHash [32]byte
}

// RSSHistoryPoint is one actual RSS observation. At must retain its original
// monotonic clock; a ticker date or a UTC reconstruction is not a read timestamp.
type RSSHistoryPoint struct {
	At    time.Time
	Bytes uint64
	Known bool
}

// RSSHourObservation compares two original samples approximately one hour apart.
// Negative and Magnitude avoid int64 overflow for a decreasing uint64 gauge.
// Baseline, elapsed and delta are unavailable unless Known is true.
type RSSHourObservation struct {
	Current, Baseline RSSHistoryPoint
	Elapsed           time.Duration
	Magnitude         uint64
	Negative, Known   bool
	Reason            string
}

// RSSHourHistory belongs to the existing diagnostics goroutine. Observe is its
// only operation; callers publish the returned value, never the live ring.
// It reads no clock or operating-system data and starts no worker.
type RSSHourHistory struct {
	binding RSSHistoryBinding
	points  [rssHourSamples]RSSHistoryPoint
	head    int
	count   int
}

// NewRSSHourHistory enables bounded history without sampling anything.
func NewRSSHourHistory(enabled bool) *RSSHourHistory {
	if !enabled {
		return nil
	}
	return &RSSHourHistory{}
}

// Observe consumes the diagnostics owner's original read result. A failed read
// remains a continuity barrier; later samples never fill its missing value.
func (h *RSSHourHistory) Observe(binding RSSHistoryBinding, point RSSHistoryPoint) RSSHourObservation {
	if h == nil {
		return RSSHourObservation{}
	}
	if binding.RunID == "" || len(binding.RunID) > 128 || !utf8.ValidString(binding.RunID) ||
		binding.StartedAt.IsZero() || binding.StartedAt.Year() < 1 || binding.StartedAt.Year() > 9999 || binding.GenesisHash == ([32]byte{}) {
		*h = RSSHourHistory{}
		return RSSHourObservation{Reason: "binding_unavailable"}
	}
	if h.binding != binding {
		*h = RSSHourHistory{binding: binding}
	}
	if point.At.IsZero() || point.At.Year() < 1 || point.At.Year() > 9999 || point.At.Before(binding.StartedAt) || point.At == point.At.Round(0) {
		*h = RSSHourHistory{binding: binding}
		return RSSHourObservation{Reason: "clock_unavailable"}
	}
	regressed := h.count > 0 && !point.At.After(h.at(h.count-1).At)
	if regressed {
		*h = RSSHourHistory{binding: binding}
	}
	if h.count == len(h.points) {
		h.points[h.head] = point
		h.head = (h.head + 1) % len(h.points)
	} else {
		h.points[(h.head+h.count)%len(h.points)] = point
		h.count++
	}
	o := RSSHourObservation{Current: point, Reason: "insufficient_history"}
	if regressed {
		o.Reason = "clock_discontinuity"
		return o
	}
	if !point.Known {
		o.Reason = "sample_unavailable"
		return o
	}
	best, distance := -1, 16*time.Second
	for i := 0; i+1 < h.count; i++ {
		baseline := h.at(i)
		age := point.At.Sub(baseline.At)
		if !baseline.Known || age < time.Hour-15*time.Second || age > time.Hour+15*time.Second {
			continue
		}
		deviation := age - time.Hour
		if deviation < 0 {
			deviation = -deviation
		}
		// Equal-distance ties retain the earlier original observation.
		if deviation < distance {
			best, distance = i, deviation
		}
	}
	if best < 0 {
		return o
	}
	for i := best + 1; i < h.count; i++ {
		previous, current := h.at(i-1), h.at(i)
		elapsed := current.At.Sub(previous.At)
		if !current.Known || elapsed <= 0 || elapsed > 30*time.Second {
			o.Reason = "coverage_gap"
			return o
		}
	}
	o.Baseline = h.at(best)
	o.Elapsed = point.At.Sub(o.Baseline.At)
	o.Known, o.Reason = true, "known"
	if point.Bytes < o.Baseline.Bytes {
		o.Negative, o.Magnitude = true, o.Baseline.Bytes-point.Bytes
	} else {
		o.Magnitude = point.Bytes - o.Baseline.Bytes
	}
	return o
}

func (h *RSSHourHistory) at(index int) RSSHistoryPoint {
	return h.points[(h.head+index)%len(h.points)]
}
