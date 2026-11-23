package dashboard

import "time"

// Pipeline is a bounded local TPU/scheduler observation, not a transaction
// trace or chain inclusion report. Counters span the StartedAt service instance;
// fields are sampled independently, so stages need not balance in one sample.
type Pipeline struct {
	SampledAt time.Time         `json:"sampled_at"`
	StartedAt time.Time         `json:"started_at"`
	Ingress   PipelineIngress   `json:"ingress"`
	Dedup     PipelineDedup     `json:"dedup"`
	Sigverify PipelineSigverify `json:"sigverify"`
	QUIC      *PipelineQUIC     `json:"quic"`
	Scheduler PipelineScheduler `json:"scheduler"`
}

// PipelineIngress counts all TPU ingress sources, not just QUIC packets.
type PipelineIngress struct {
	EnqueuedPackets uint64 `json:"enqueued_packets,string"`
	EnqueuedBytes   uint64 `json:"enqueued_bytes,string"`
	DroppedFull     uint64 `json:"dropped_full,string"`
}

// PipelineDedup reports wire sanitation and deduplication before sigverify.
type PipelineDedup struct {
	InPackets                 uint64 `json:"in_packets,string"`
	InBytes                   uint64 `json:"in_bytes,string"`
	OutPackets                uint64 `json:"out_packets,string"`
	OutBytes                  uint64 `json:"out_bytes,string"`
	DroppedSanitize           uint64 `json:"dropped_sanitize,string"`
	DroppedUnsupportedVersion uint64 `json:"dropped_unsupported_version,string"`
	DroppedDedup              uint64 `json:"dropped_dedup,string"`
}

// PipelineSigverify reports packet verification, not execution success.
type PipelineSigverify struct {
	InPackets        uint64 `json:"in_packets,string"`
	InBytes          uint64 `json:"in_bytes,string"`
	VerifiedPackets  uint64 `json:"verified_packets,string"`
	VerifiedBytes    uint64 `json:"verified_bytes,string"`
	DroppedSigverify uint64 `json:"dropped_sigverify,string"`
}

// PipelineQUIC copies atomic counters without retaining their live pointers.
// OpenConnections and ActiveStreams are gauges. AcceptErrors includes both
// listener and stream-accept errors; StreamBufferDrops is pool exhaustion after
// accepting a stream, not a refused connection or ingress queue rejection.
type PipelineQUIC struct {
	AcceptedConnections uint64 `json:"accepted_connections,string"`
	OpenConnections     uint64 `json:"open_connections,string"`
	AcceptedStreams     uint64 `json:"accepted_streams,string"`
	ActiveStreams       uint64 `json:"active_streams,string"`
	InvalidStreamSize   uint64 `json:"invalid_stream_size,string"`
	ReadErrors          uint64 `json:"read_errors,string"`
	ReadTimeouts        uint64 `json:"read_timeouts,string"`
	AcceptErrors        uint64 `json:"accept_errors,string"`
	RefusedTooManyOpen  uint64 `json:"refused_too_many_open,string"`
	RefusedPerIP        uint64 `json:"refused_per_ip,string"`
	StreamBufferDrops   uint64 `json:"stream_buffer_drops,string"`
}

// PipelineScheduler separates cumulative accepted insertions (BufferedTotal)
// from current queued entries (QueueDepth, nil if unobserved). Accepted counts
// local forge admissions, not finalized transactions. Cost rejections can be
// retried, so these counters do not form a disjoint transaction waterfall.
// EvictedCapacity counts Receive-admission replacements, not rebuffer evictions.
// DroppedExpired counts Cleanup's bank classification. Both pointers may be
// absent in older producers; measured zero stays distinguishable from absence.
type PipelineScheduler struct {
	InPackets               uint64  `json:"in_packets,string"`
	InBytes                 uint64  `json:"in_bytes,string"`
	BufferedTotal           uint64  `json:"buffered_total,string"`
	QueueDepth              *uint64 `json:"queue_depth,string"`
	Accepted                uint64  `json:"accepted,string"`
	DroppedParse            uint64  `json:"dropped_parse,string"`
	DroppedVote             uint64  `json:"dropped_vote,string"`
	DroppedDuplicate        uint64  `json:"dropped_duplicate,string"`
	DroppedCapacity         uint64  `json:"dropped_capacity,string"`
	EvictedCapacity         *uint64 `json:"evicted_capacity,string,omitempty"`
	DroppedExpired          *uint64 `json:"dropped_expired,string,omitempty"`
	DroppedAlreadyProcessed uint64  `json:"dropped_already_processed,string"`
	DroppedAccountLocks     uint64  `json:"dropped_account_locks,string"`
	DroppedLookup           uint64  `json:"dropped_lookup,string"`
	DroppedNoBank           uint64  `json:"dropped_no_bank,string"`
	DroppedCost             uint64  `json:"dropped_cost,string"`
	DroppedExecution        uint64  `json:"dropped_execution,string"`
}
