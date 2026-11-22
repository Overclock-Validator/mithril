package dashboard

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"
)

const (
	activityHistoryCapacity = 32
	activityHistoryMaxBytes = 16 << 10
)

// ActivityHistory contains recent, non-overlapping selected-chain intervals.
type ActivityHistory struct {
	SchemaVersion int                       `json:"schema_version"`
	RunID         string                    `json:"run_id"`
	GenesisHash   string                    `json:"genesis_hash"`
	NodeStartedAt time.Time                 `json:"node_started_at"`
	Attempt       uint64                    `json:"attempt,string"`
	Rewinds       uint64                    `json:"rewinds,string"`
	SampledAt     time.Time                 `json:"sampled_at"`
	SettledAt     time.Time                 `json:"settled_at"`
	Frontier      uint64                    `json:"frontier,string"`
	Binding       ReplayDetailBinding       `json:"binding"`
	CapturedAt    time.Time                 `json:"captured_at"`
	ObservedAt    time.Time                 `json:"observed_at"`
	Rows          []ActivityHistoryInterval `json:"rows"`
}

func (h *ActivityHistory) setContext() {
	h.RunID, h.NodeStartedAt, h.Attempt, h.Rewinds = h.Binding.RunID, h.Binding.NodeStartedAt, h.Binding.Attempt, h.Binding.Rewinds
	if h.Binding.GenesisHash != nil {
		h.GenesisHash = *h.Binding.GenesisHash
	}
	h.SampledAt, h.SettledAt = h.CapturedAt, h.ObservedAt
	if len(h.Rows) > 0 {
		h.Frontier = h.Rows[0].LastSlot
	}
}

// ActivityHistoryInterval describes (AnchorSlot, LastSlot], not replay speed.
type ActivityHistoryInterval struct {
	AnchorSlot    uint64 `json:"anchor_slot,string"`
	LastSlot      uint64 `json:"last_slot,string"`
	StartTime     int64  `json:"start_time,string"`
	EndTime       int64  `json:"end_time,string"`
	Blocks        uint64 `json:"blocks,string"`
	Transactions  uint64 `json:"transactions,string"`
	NonVote       uint64 `json:"non_vote_transactions,string"`
	Failed        uint64 `json:"failed_transactions,string"`
	FailedNonVote uint64 `json:"failed_non_vote_transactions,string"`
}

func validActivityHistory(h ActivityHistory) bool {
	if h.SchemaVersion != 1 || !validReplayDetailBinding(h.Binding) || h.Binding.GenesisHash == nil ||
		h.RunID != h.Binding.RunID || h.GenesisHash != *h.Binding.GenesisHash || !h.NodeStartedAt.Equal(h.Binding.NodeStartedAt) ||
		h.Attempt != h.Binding.Attempt || h.Rewinds != h.Binding.Rewinds || !h.SampledAt.Equal(h.CapturedAt) || !h.SettledAt.Equal(h.ObservedAt) ||
		!validReplayDetailTime(h.CapturedAt) || !validReplayDetailTime(h.ObservedAt) ||
		h.ObservedAt.After(h.CapturedAt) || h.ObservedAt.Before(h.Binding.NodeStartedAt) ||
		h.ObservedAt.Before(h.Binding.AttemptStartedAt) || h.ObservedAt.Before(h.Binding.ResetAt) ||
		len(h.Rows) == 0 || len(h.Rows) > activityHistoryCapacity || h.Frontier != h.Rows[0].LastSlot {
		return false
	}
	for i, row := range h.Rows {
		if row.LastSlot <= row.AnchorSlot || row.StartTime <= 0 || row.EndTime <= row.StartTime ||
			row.EndTime > h.ObservedAt.Unix()+1 || row.Blocks == 0 || row.Blocks > 511 ||
			row.Blocks > row.LastSlot-row.AnchorSlot || row.Failed > row.Transactions || row.NonVote > row.Transactions ||
			row.FailedNonVote > row.NonVote || row.FailedNonVote > row.Failed || row.Failed-row.FailedNonVote > row.Transactions-row.NonVote ||
			i > 0 && (h.Rows[i-1].AnchorSlot != row.LastSlot || h.Rows[i-1].StartTime != row.EndTime) {
			return false
		}
	}
	return true
}

// activityHistoryRegressed compares publications within an already-matched core replay context.
func activityHistoryRegressed(binding ReplayDetailBinding, capturedAt, observedAt time.Time, frontier uint64, h ActivityHistory) bool {
	return h.Binding.SourceGeneration < binding.SourceGeneration || h.Binding.ResetAt.Before(binding.ResetAt) ||
		h.CapturedAt.Before(capturedAt) || h.ObservedAt.Before(observedAt) ||
		h.Binding.SourceGeneration == binding.SourceGeneration && (!h.Binding.ResetAt.Equal(binding.ResetAt) || h.Frontier < frontier)
}

func decodeActivityHistory(body []byte) (ActivityHistory, error) {
	var h ActivityHistory
	if len(body) == 0 || len(body) > activityHistoryMaxBytes || json.Unmarshal(body, &h) != nil || !validActivityHistory(h) {
		return ActivityHistory{}, errors.New("invalid activity history")
	}
	canonical, err := json.Marshal(h)
	if err != nil || !bytes.Equal(canonical, body) {
		return ActivityHistory{}, errors.New("invalid activity history")
	}
	return h, nil
}
