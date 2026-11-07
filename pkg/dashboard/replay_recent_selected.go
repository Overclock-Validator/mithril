package dashboard

import (
	"bytes"
	"encoding/json"
	"time"
)

// ReplayRecentSelectedCapacity bounds this display subset, not source retention.
const ReplayRecentSelectedCapacity = 8

// ReplaySelectedRecord preserves one selected-and-folded record's optional facts.
// A gap means unavailable replay facts, not a skipped slot.
type ReplaySelectedRecord struct {
	Slot     uint64               `json:"slot,string"`
	Bankhash *string              `json:"bankhash"`
	BlockID  *string              `json:"block_id"`
	Gap      bool                 `json:"gap"`
	Facts    *ReplaySelectedFacts `json:"facts"`
}

// ReplayRecentSelected contains the newest bounded subset of one cached source
// window, oldest first. ObservedAt dates the window, not each record's fold.
// CoverageIncomplete describes missing source facts, not complete epoch coverage.
type ReplayRecentSelected struct {
	Scope              string                 `json:"scope"`
	ObservedAt         time.Time              `json:"observed_at"`
	WindowEntries      uint64                 `json:"window_entries,string"`
	SourceRecords      uint64                 `json:"source_records,string"`
	CoverageIncomplete bool                   `json:"coverage_incomplete"`
	Records            []ReplaySelectedRecord `json:"records"`
}

func selectedRecordValue(r ReplaySelectedRecord, at time.Time) ReplayLatestSelected {
	return ReplayLatestSelected{Scope: "latest_selected_and_folded", ObservedAt: at,
		Slot: r.Slot, Bankhash: r.Bankhash, BlockID: r.BlockID, Gap: r.Gap, Facts: r.Facts}
}

func validReplayRecentSelected(s *ReplayRecentSelected) bool {
	if s == nil {
		return true
	}
	if s.Scope != "recent_selected_and_folded" || !validReplayDetailTime(s.ObservedAt) || s.WindowEntries > 512 ||
		s.SourceRecords != min(s.WindowEntries, 256) || s.Records == nil ||
		len(s.Records) != int(min(s.SourceRecords, ReplayRecentSelectedCapacity)) {
		return false
	}
	for i, r := range s.Records {
		v := selectedRecordValue(r, s.ObservedAt)
		// Settle sorts new evidence and updates, rather than appends, old slots.
		if !validReplayLatestSelected(&v) || r.Gap && !s.CoverageIncomplete || i > 0 && r.Slot <= s.Records[i-1].Slot {
			return false
		}
	}
	return true
}

func validReplaySelectedMetadata(latest *ReplayLatestSelected, recent *ReplayRecentSelected) bool {
	if !validReplayLatestSelected(latest) || !validReplayRecentSelected(recent) {
		return false
	}
	if recent == nil {
		return true
	}
	if len(recent.Records) == 0 {
		return latest == nil
	}
	if latest == nil {
		return false
	}
	newest := selectedRecordValue(recent.Records[len(recent.Records)-1], recent.ObservedAt)
	// Compare the bounded canonical values, including every nullable fact and
	// date, without treating time.Time's location or monotonic bits as wire data.
	a, err := json.Marshal(latest)
	if err != nil {
		return false
	}
	b, err := json.Marshal(newest)
	return err == nil && bytes.Equal(a, b)
}

func cloneReplayRecentSelected(s *ReplayRecentSelected) *ReplayRecentSelected {
	if s == nil {
		return nil
	}
	copy := *s
	copy.Records = make([]ReplaySelectedRecord, len(s.Records))
	for i, r := range s.Records {
		v := selectedRecordValue(r, s.ObservedAt)
		c := cloneReplayLatestSelected(&v)
		copy.Records[i] = ReplaySelectedRecord{Slot: c.Slot, Bankhash: c.Bankhash, BlockID: c.BlockID, Gap: c.Gap, Facts: c.Facts}
	}
	return &copy
}
