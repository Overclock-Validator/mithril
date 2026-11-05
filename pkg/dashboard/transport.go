package dashboard

import "time"

// PublicTurbineDiagnostics contains receiver-session observations, not a
// recent network tip or a count of executed/selected blocks. The containing
// Turbine observation supplies the sample time, session and freshness state.
type PublicTurbineDiagnostics struct {
	ObservedHeaderSlot     *uint64                 `json:"observed_header_slot,string"`
	ReconstructedSlot      *uint64                 `json:"reconstructed_slot,string"`
	BlockEmissionAttempts  uint64                  `json:"block_emission_attempts,string"`
	RecoveredDataShreds    uint64                  `json:"recovered_data_shreds,string"`
	ParseErrors            uint64                  `json:"parse_errors,string"`
	AssemblyErrors         uint64                  `json:"assembly_errors,string"`
	ActiveSlots            int                     `json:"active_slots"`
	EvictedSlots           uint64                  `json:"evicted_slots,string"`
	IgnoredOldShreds       uint64                  `json:"ignored_old_shreds,string"`
	SignatureVerifications uint64                  `json:"signature_verifications,string"`
	SignatureCacheHits     uint64                  `json:"signature_cache_hits,string"`
	PeerPayloadBytes       *uint64                 `json:"peer_payload_bytes,string,omitempty"`
	Admission              *PublicTurbineAdmission `json:"admission,omitempty"`
	UsefulRepairShreds     uint64                  `json:"useful_repair_shreds,string"`
	SpooledRepairShreds    uint64                  `json:"spooled_repair_shreds,string"`
	Repair                 *PublicRepair           `json:"repair"`
}

// PublicTurbineAdmission contains the allowlisted shred admission counters.
type PublicTurbineAdmission struct {
	MissingLeaders       uint64 `json:"missing_leaders,string"`
	ShredVersionMismatch uint64 `json:"shred_version_mismatch,string"`
}

// PublicRepair counts matched responses independently from requests/timeouts.
// Late responses can already have been counted as timeouts. Outstanding and
// peer counts are gauges, not cumulative request or packet counters. Responding
// peers matched at least one response in the owner's last sixty seconds.
type PublicRepair struct {
	Requests        uint64 `json:"requests,string"`
	Responses       uint64 `json:"responses,string"`
	LateResponses   uint64 `json:"late_responses,string"`
	Timeouts        uint64 `json:"timeouts,string"`
	Errors          uint64 `json:"errors,string"`
	Outstanding     int    `json:"outstanding"`
	Peers           int    `json:"peers"`
	RespondingPeers int    `json:"responding_peers"`
}

func validTurbineObservation(s *Snapshot, b *BlockSource, now time.Time) bool {
	t := b.Turbine
	return t != nil && t.Session != 0 && !b.StartedAt.IsZero() && !b.StartedAt.Before(s.StartedAt) &&
		!b.SampledAt.Before(b.StartedAt) && !b.SampledAt.After(s.SampledAt.Add(time.Second)) &&
		!t.StartedAt.Before(b.StartedAt) && !t.SampledAt.Before(t.StartedAt) &&
		!t.SampledAt.After(b.SampledAt.Add(time.Second)) && validPublicTime(t.SampledAt, now)
}

func publicTurbineDiagnostics(t *Turbine) *PublicTurbineDiagnostics {
	d := t.Diagnostics
	if d == nil || d.ActiveSlots < 0 || d.ActiveSlots > 1<<31-1 ||
		d.ReconstructedSlot != nil && (d.ObservedHeaderSlot == nil || *d.ReconstructedSlot > *d.ObservedHeaderSlot) {
		return nil
	}
	p := &PublicTurbineDiagnostics{
		BlockEmissionAttempts: d.BlockEmissionAttempts, RecoveredDataShreds: d.RecoveredDataShreds,
		ParseErrors: t.ParseErrors, AssemblyErrors: d.AssemblyErrors, ActiveSlots: d.ActiveSlots,
		EvictedSlots: d.EvictedSlots, IgnoredOldShreds: d.IgnoredOldShreds,
		SignatureVerifications: d.SignatureVerifications, SignatureCacheHits: d.SignatureCacheHits,
		UsefulRepairShreds: t.UsefulRepairShreds, SpooledRepairShreds: t.SpooledRepairShreds,
	}
	if d.ObservedHeaderSlot != nil {
		slot := *d.ObservedHeaderSlot
		p.ObservedHeaderSlot = &slot
	}
	if d.ReconstructedSlot != nil {
		slot := *d.ReconstructedSlot
		p.ReconstructedSlot = &slot
	}
	if d.PeerPayloadBytes != nil {
		bytes := *d.PeerPayloadBytes
		p.PeerPayloadBytes = &bytes
	}
	if d.Admission != nil {
		p.Admission = &PublicTurbineAdmission{MissingLeaders: d.Admission.MissingLeaders, ShredVersionMismatch: d.Admission.ShredVersionMismatch}
	}
	if r := t.Repair; r != nil && d.RepairOutstanding >= 0 && d.RepairOutstanding <= 1<<31-1 &&
		d.RepairPeers >= 0 && d.RepairPeers <= 1<<31-1 && d.RepairRespondingPeers >= 0 && d.RepairRespondingPeers <= 1<<31-1 {
		p.Repair = &PublicRepair{Requests: r.Requests, Responses: r.Responses, LateResponses: r.LateResponses,
			Timeouts: r.Timeouts, Errors: r.Errors, Outstanding: d.RepairOutstanding, Peers: d.RepairPeers, RespondingPeers: d.RepairRespondingPeers}
	}
	return p
}
