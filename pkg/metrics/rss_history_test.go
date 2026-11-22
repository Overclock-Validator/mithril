package metrics

import (
	"math"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

func rssHistoryBinding(at time.Time) RSSHistoryBinding {
	return RSSHistoryBinding{RunID: "node-run", StartedAt: at, GenesisHash: [32]byte{1}}
}

func TestRSSHourCoverage(t *testing.T) {
	at := time.Now()
	binding := rssHistoryBinding(at)
	for _, tc := range []struct {
		first, last uint64
		negative    bool
		magnitude   uint64
	}{
		{7, 7, false, 0}, {7, 17, false, 10}, {17, 7, true, 10},
		{0, math.MaxUint64, false, math.MaxUint64}, {math.MaxUint64, 0, true, math.MaxUint64},
	} {
		h := NewRSSHourHistory(true)
		var got RSSHourObservation
		for i := 0; i <= 360; i++ {
			bytes := tc.first
			if i == 360 {
				bytes = tc.last
			}
			got = h.Observe(binding, RSSHistoryPoint{At: at.Add(time.Duration(i) * 10 * time.Second), Bytes: bytes, Known: true})
			if i < 359 && got.Known {
				t.Fatal("short startup history acquired an hour delta")
			}
		}
		if !got.Known || got.Reason != "known" || got.Elapsed != time.Hour || got.Magnitude != tc.magnitude || got.Negative != tc.negative ||
			got.Baseline.At != at || got.Baseline.Bytes != tc.first || got.Current.At != at.Add(time.Hour) || got.Current.Bytes != tc.last {
			t.Fatalf("RSS %d to %d: got %+v", tc.first, tc.last, got)
		}
		// Returned values cannot change when the live fixed ring wraps.
		before := got
		for i := 361; i < 900; i++ {
			h.Observe(binding, RSSHistoryPoint{At: at.Add(time.Duration(i) * 10 * time.Second), Bytes: uint64(i), Known: true})
		}
		if !reflect.DeepEqual(got, before) || h.count != rssHourSamples {
			t.Fatal("replacement changed a returned observation or exceeded retention")
		}
	}
}

func TestRSSHourCoverageGapsAndRecovery(t *testing.T) {
	at := time.Now()
	binding := rssHistoryBinding(at)
	for _, missing := range []bool{true, false} {
		h := NewRSSHourHistory(true)
		var got RSSHourObservation
		for i := 0; i <= 360; i++ {
			if !missing && i >= 170 && i <= 173 {
				continue // A 50-second gap cannot be a continuous hour.
			}
			got = h.Observe(binding, RSSHistoryPoint{At: at.Add(time.Duration(i) * 10 * time.Second), Bytes: uint64(i), Known: !missing || i != 170})
		}
		if got.Known || got.Reason != "coverage_gap" || !got.Baseline.At.IsZero() || got.Elapsed != 0 || got.Magnitude != 0 {
			t.Fatalf("missing=%v fabricated complete history: %+v", missing, got)
		}
		for i := 361; i <= 540; i++ {
			got = h.Observe(binding, RSSHistoryPoint{At: at.Add(time.Duration(i) * 10 * time.Second), Bytes: uint64(i), Known: true})
		}
		if !got.Known || got.Baseline.At != at.Add(1800*time.Second) || got.Magnitude != 360 {
			t.Fatalf("history did not recover after a complete later hour: %+v", got)
		}
	}
}

func TestRSSHourBindingClockAndMissing(t *testing.T) {
	at := time.Now()
	binding := rssHistoryBinding(at)
	for _, disabled := range []*RSSHourHistory{nil, NewRSSHourHistory(false)} {
		if got := disabled.Observe(binding, RSSHistoryPoint{At: at, Known: true}); !reflect.DeepEqual(got, RSSHourObservation{}) {
			t.Fatal("disabled history fabricated an observation")
		}
	}
	for _, change := range []func(*RSSHistoryBinding){
		func(b *RSSHistoryBinding) { b.RunID = "new-run" },
		func(b *RSSHistoryBinding) { b.StartedAt = b.StartedAt.Add(time.Second) },
		func(b *RSSHistoryBinding) { b.GenesisHash[0]++ },
	} {
		h := NewRSSHourHistory(true)
		for i := 0; i <= 360; i++ {
			h.Observe(binding, RSSHistoryPoint{At: at.Add(time.Duration(i) * 10 * time.Second), Bytes: 1, Known: true})
		}
		next := binding
		change(&next)
		got := h.Observe(next, RSSHistoryPoint{At: at.Add(3610 * time.Second), Bytes: 2, Known: true})
		if got.Known || got.Reason != "insufficient_history" || h.count != 1 {
			t.Fatal("new context retained an old hour baseline")
		}
	}
	h := NewRSSHourHistory(true)
	h.Observe(binding, RSSHistoryPoint{At: at, Bytes: 1, Known: true})
	if got := h.Observe(binding, RSSHistoryPoint{At: at.Add(10 * time.Second)}); got.Reason != "sample_unavailable" || got.Known {
		t.Fatal("missing RSS became measured zero")
	}
	if got := h.Observe(binding, RSSHistoryPoint{At: at.Add(5 * time.Second), Known: true}); got.Reason != "clock_discontinuity" || got.Known || h.count != 1 {
		t.Fatal("regressing read clock retained continuity")
	}
	if got := h.Observe(binding, RSSHistoryPoint{At: at.UTC(), Known: true}); got.Reason != "clock_unavailable" || got.Known || h.count != 0 {
		t.Fatal("wall-only timestamp established elapsed coverage")
	}
	if got := h.Observe(RSSHistoryBinding{}, RSSHistoryPoint{At: at, Known: true}); got.Reason != "binding_unavailable" || h.count != 0 {
		t.Fatal("unknown chain/run retained history")
	}
	if size := unsafe.Sizeof(*h); size > 32<<10 {
		t.Fatalf("fixed history owns %d bytes, limit 32 KiB", size)
	}
}
