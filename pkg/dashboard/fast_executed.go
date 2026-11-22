package dashboard

import (
	"time"
	"unicode/utf8"
)

// FastExecutedBinding identifies the local replay attempt that produced a tail.
type FastExecutedBinding struct {
	RunID            string    `json:"run_id"`
	GenesisHash      *string   `json:"genesis_hash"`
	NodeStartedAt    time.Time `json:"node_started_at"`
	Attempt          uint64    `json:"attempt,string"`
	Rewinds          uint64    `json:"rewinds,string"`
	AttemptStartedAt time.Time `json:"attempt_started_at"`
}

// FastExecuted is a provisional replacement view, never a canonical slot feed.
type FastExecuted struct {
	SchemaVersion int                 `json:"schema_version"`
	Family        string              `json:"family"`
	Scope         string              `json:"scope"`
	State         string              `json:"state"`
	Binding       FastExecutedBinding `json:"binding"`
	Sequence      uint64              `json:"sequence,string"`
	SampledAt     time.Time           `json:"sampled_at"`
	Records       []ExecutedSlot      `json:"records"`
}

func sameFastBinding(a, b FastExecutedBinding) bool {
	return a.RunID == b.RunID && a.NodeStartedAt.Equal(b.NodeStartedAt) &&
		(a.GenesisHash == nil) == (b.GenesisHash == nil) &&
		(a.GenesisHash == nil || *a.GenesisHash == *b.GenesisHash) &&
		a.Attempt == b.Attempt && a.Rewinds == b.Rewinds && a.AttemptStartedAt.Equal(b.AttemptStartedAt)
}

func validFastExecuted(v FastExecuted) bool {
	b := v.Binding
	if v.SchemaVersion != 1 || v.Family != "recent_executed" || v.Scope != "locally_executed" ||
		b.RunID == "" || len(b.RunID) > 128 || !utf8.ValidString(b.RunID) || b.Attempt == 0 ||
		!validReplayDetailTime(b.NodeStartedAt) || !validReplayDetailTime(b.AttemptStartedAt) ||
		b.AttemptStartedAt.Before(b.NodeStartedAt) ||
		(b.GenesisHash != nil && !validReplayDetailHash(*b.GenesisHash, false)) ||
		v.Sequence == 0 || !validReplayDetailTime(v.SampledAt) || v.SampledAt.Before(b.AttemptStartedAt) ||
		v.Records == nil || len(v.Records) > 48 {
		return false
	}
	if v.State == "unavailable" {
		return len(v.Records) == 0
	}
	if v.State != "fresh" || len(v.Records) == 0 {
		return false
	}
	for i, row := range v.Records {
		if !validReplayDetailTime(row.At) || row.At.Before(b.AttemptStartedAt) || row.At.After(v.SampledAt) ||
			!validReplayDetailHash(row.Bankhash, false) || row.BlockID != nil && !validReplayDetailHash(*row.BlockID, true) ||
			i > 0 && row.Slot <= v.Records[i-1].Slot {
			return false
		}
	}
	return true
}
