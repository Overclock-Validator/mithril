// Package dashboard exposes read-only node measurements. It does not operate
// the validator or serve the Solana JSON-RPC API.
package dashboard

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"
)

const (
	// SchemaVersion identifies the Mithril status payload format.
	SchemaVersion = 1
	// MaxSnapshotBytes bounds the public status payload.
	MaxSnapshotBytes = 32 << 10
	// MaxPrivateSnapshotBytes bounds the node-to-reader snapshot separately.
	MaxPrivateSnapshotBytes = 33 << 10
	// SampleInterval limits sampling independently of viewer count.
	SampleInterval = time.Second
	// StaleAfter is the maximum age of a successfully read node sample.
	StaleAfter = 5 * time.Second
)

// Snapshot is the allowlisted private feed. Never add private keys, paths,
// arbitrary configuration or raw errors: the collector forwards these fields.
// The frontier includes skips and can move backwards during recovery; it is
// neither a bank slot nor verified finality.
type Snapshot struct {
	SchemaVersion      int                 `json:"schema_version"`
	RunID              string              `json:"run_id"`
	SampledAt          time.Time           `json:"sampled_at"`
	StartedAt          time.Time           `json:"started_at"`
	ElapsedNanoseconds *uint64             `json:"elapsed_nanoseconds,string,omitempty"`
	Version            string              `json:"version"`
	Commit             string              `json:"commit"`
	Network            string              `json:"network"`
	GenesisHash        *string             `json:"genesis_hash"`
	IdentityKey        *string             `json:"identity_key"`
	Alpenglow          bool                `json:"alpenglow"`
	Mode               string              `json:"mode"`
	RPCConfigured      bool                `json:"rpc_configured"`
	Phase              string              `json:"phase"`
	ReplayFrontier     *uint64             `json:"replay_frontier"`
	Runtime            *Runtime            `json:"runtime"`
	Resources          *Resources          `json:"resources"`
	Replay             *Replay             `json:"replay"`
	Consensus          *Consensus          `json:"consensus"`
	RPC                *RPC                `json:"rpc"`
	Recovery           *Recovery           `json:"recovery"`
	BlockSource        *BlockSource        `json:"block_source"`
	Pipeline           *Pipeline           `json:"pipeline"`
	SnapshotManifests  *SnapshotManifests  `json:"snapshot_manifests,omitempty"`
	SnapshotArchive    *SnapshotArchive    `json:"snapshot_archive,omitempty"`
	IncrementalArchive *IncrementalArchive `json:"incremental_archive,omitempty"`
}

// BlockSource describes local input progress, not completed replay or tip lag.
type BlockSource struct {
	SampledAt            time.Time  `json:"sampled_at"`
	StartedAt            time.Time  `json:"started_at"`
	ConfiguredSource     string     `json:"configured_source"`
	NextSlot             uint64     `json:"next_slot,string"`
	LiveConnected        bool       `json:"live_connected"`
	LiveActive           bool       `json:"live_active"`
	RPCBlockFetchAllowed bool       `json:"rpc_block_fetch_allowed"`
	RepairCatchupPending bool       `json:"repair_catchup_pending"`
	RepairCatchupFrom    *uint64    `json:"repair_catchup_from,string"`
	RepairCatchupUntil   *uint64    `json:"repair_catchup_until,string"`
	Turbine              *Turbine   `json:"turbine"`
	Reception            *Reception `json:"reception,omitempty"`
	Gossip               *Gossip    `json:"gossip"`
}

// Gossip counts one client instance, which can span stream or identity changes.
// Contact gauges are locally known peers, not verified cluster membership.
type Gossip struct {
	SampledAt        time.Time  `json:"sampled_at"`
	StartedAt        time.Time  `json:"started_at"`
	ReceivedPackets  uint64     `json:"received_packets,string"`
	SentPackets      uint64     `json:"sent_packets,string"`
	DecodeErrors     uint64     `json:"decode_errors,string"`
	SendErrors       uint64     `json:"send_errors,string"`
	AcceptedContacts uint64     `json:"accepted_contacts,string"`
	KnownPeers       int        `json:"known_peers"`
	RepairPeers      int        `json:"repair_peers"`
	AlpenglowPeers   int        `json:"alpenglow_peers"`
	LastReceivedAt   *time.Time `json:"last_received_at"`
	LastSentAt       *time.Time `json:"last_sent_at"`
	RecentPeers      []Peer     `json:"recent_peers,omitempty"`
}

// Peer is a recent signed contact's advertised metadata, without endpoints.
type Peer struct {
	Identity     string    `json:"identity"`
	Version      string    `json:"version"`
	Client       uint16    `json:"client"`
	ShredVersion uint16    `json:"shred_version"`
	ReceivedAt   time.Time `json:"received_at"`
	Repair       bool      `json:"repair"`
	TVU          bool      `json:"tvu"`
	Alpenglow    bool      `json:"alpenglow"`
}

// Turbine counters span one receiver session. Parsed shreds are not proof of
// verified blocks. Samples reuse the receiver's existing diagnostics cycle.
type Turbine struct {
	SampledAt           time.Time           `json:"sampled_at"`
	StartedAt           time.Time           `json:"started_at"`
	Session             uint64              `json:"session,string"`
	Packets             uint64              `json:"packets,string"`
	DataShreds          uint64              `json:"data_shreds,string"`
	CodingShreds        uint64              `json:"coding_shreds,string"`
	ParseErrors         uint64              `json:"parse_errors,string"`
	SignatureErrors     uint64              `json:"signature_errors,string"`
	UsefulRepairShreds  uint64              `json:"useful_repair_shreds,string"`
	SpooledRepairShreds uint64              `json:"spooled_repair_shreds,string"`
	LastPacketAt        *time.Time          `json:"last_packet_at"`
	Repair              *Repair             `json:"repair"`
	Diagnostics         *TurbineDiagnostics `json:"diagnostics,omitempty"`
}

// TurbineDiagnostics reuses receiver-owner counters and assembler high-water
// positions. Reconstructable data may still fail decoding or chain validation.
type TurbineDiagnostics struct {
	ObservedHeaderSlot     *uint64           `json:"observed_header_slot,string"`
	ReconstructedSlot      *uint64           `json:"reconstructed_slot,string"`
	BlockEmissionAttempts  uint64            `json:"block_emission_attempts,string"`
	RecoveredDataShreds    uint64            `json:"recovered_data_shreds,string"`
	AssemblyErrors         uint64            `json:"assembly_errors,string"`
	ActiveSlots            int               `json:"active_slots"`
	EvictedSlots           uint64            `json:"evicted_slots,string"`
	IgnoredOldShreds       uint64            `json:"ignored_old_shreds,string"`
	SignatureVerifications uint64            `json:"signature_verifications,string"`
	SignatureCacheHits     uint64            `json:"signature_cache_hits,string"`
	PeerPayloadBytes       *uint64           `json:"peer_payload_bytes,string,omitempty"`
	Admission              *TurbineAdmission `json:"admission,omitempty"`
	RepairOutstanding      int               `json:"repair_outstanding"`
	RepairPeers            int               `json:"repair_peers"`
	RepairRespondingPeers  int               `json:"repair_responding_peers"`
}

// TurbineAdmission counts receiver-session rejections before shred authentication.
type TurbineAdmission struct {
	MissingLeaders       uint64 `json:"missing_leaders,string"`
	ShredVersionMismatch uint64 `json:"shred_version_mismatch,string"`
}

type Repair struct {
	Requests      uint64 `json:"requests,string"`
	Responses     uint64 `json:"responses,string"`
	LateResponses uint64 `json:"late_responses,string"`
	Timeouts      uint64 `json:"timeouts,string"`
	Errors        uint64 `json:"errors,string"`
}

// Recovery is the reconciled startup selection, not a continuously read state
// file. Replay reports the later attempt and post-initialization ready time.
type Recovery struct {
	SelectedAt     time.Time `json:"selected_at"`
	SnapshotSlot   uint64    `json:"snapshot_slot,string"`
	StoredSlot     *uint64   `json:"stored_slot,string"`
	StoredBankhash *string   `json:"stored_bankhash"`
	RootedSlot     *uint64   `json:"rooted_slot,string"`
	RootedBankhash *string   `json:"rooted_bankhash"`
}

// RPC reports listener activity and public method invocations, not readiness.
type RPC struct {
	SampledAt              time.Time   `json:"sampled_at"`
	MonitoringStartedAt    time.Time   `json:"monitoring_started_at"`
	Serving                bool        `json:"serving"`
	PublishedSlot          *uint64     `json:"published_slot,string"`
	PublishedAt            *time.Time  `json:"published_at"`
	HistoryRecoveryPending bool        `json:"history_recovery_pending"`
	Methods                []RPCMethod `json:"methods"`
}

// RPCMethod uses six non-cumulative latency bins, documented in dashboard-api.md.
type RPCMethod struct {
	Name            string    `json:"name"`
	Completed       uint64    `json:"completed,string"`
	Errors          uint64    `json:"errors,string"`
	Active          uint64    `json:"active,string"`
	DurationBuckets [6]string `json:"duration_buckets"`
}

// Consensus distinguishes verified certificate evidence from local casting.
// Neither counter is a reward-inclusion or FAST percentage.
type Consensus struct {
	SampledAt            time.Time `json:"sampled_at"`
	ObservedBlocks       uint64    `json:"observed_blocks,string"`
	ReplayedSlots        uint64    `json:"replayed_slots,string"`
	SafetyFault          *bool     `json:"safety_fault"`
	CertificatesAccepted uint64    `json:"certificates_accepted,string"`
	NotarizedSlot        *uint64   `json:"notarized_slot,string"`
	FinalizedSlot        *uint64   `json:"direct_finalized_slot,string"`
	FinalizedBlockID     *string   `json:"direct_finalized_block_id"`
	Receiver             *Receiver `json:"receiver"`
	VotingEnabled        bool      `json:"voting_enabled"`
	Voting               *Voting   `json:"voting"`
}

// Receiver reports Votor ingress; decoded messages are not verified votes.
type Receiver struct {
	DatagramsReceived    uint64     `json:"datagrams_received,string"`
	MessagesDecoded      uint64     `json:"messages_decoded,string"`
	DecodeErrors         uint64     `json:"decode_errors,string"`
	ReadErrors           uint64     `json:"read_errors,string"`
	ConnectionsRejected  uint64     `json:"connections_rejected,string"`
	RateLimitedDatagrams uint64     `json:"rate_limited_datagrams,string"`
	LastMessageAt        *time.Time `json:"last_message_at"`
}

// Voting separates key eligibility, local casting and verified network evidence.
// Counters span the voter instance, including identity changes within that run.
type Voting struct {
	SampledAt             time.Time  `json:"sampled_at"`
	Identity              string     `json:"identity"`
	VoteAccount           string     `json:"vote_account"`
	RootSlot              *uint64    `json:"root_slot,string"`
	Epoch                 *uint64    `json:"epoch,string"`
	KeyEligible           *bool      `json:"key_eligible"`
	Rank                  *uint16    `json:"rank"`
	AdmittedStake         *uint64    `json:"admitted_stake_lamports,string"`
	TotalAdmittedStake    *uint64    `json:"total_admitted_stake_lamports,string"`
	JoinedLive            *bool      `json:"joined_live,omitempty"`
	VotesCast             uint64     `json:"votes_cast_this_run,string"`
	NetworkLanded         uint64     `json:"network_landed_votes,string"`
	LastNetworkLandedSlot *uint64    `json:"last_network_landed_slot,string"`
	LastNetworkLandedAt   *time.Time `json:"last_network_landed_at"`
	MessagesQueued        uint64     `json:"messages_queued,string"`
	MessagesDropped       uint64     `json:"messages_dropped,string"`
	PeerSendErrors        uint64     `json:"peer_send_errors,string"`
	PeerQueueDrops        uint64     `json:"peer_queue_drops,string"`
	SendTimeouts          uint64     `json:"send_timeouts,string"`
	DesiredPeers          int        `json:"desired_peers"`
	Connections           int        `json:"connections"`
}

// Replay is local loop activity. Counts include work later discarded by a
// fork switch and reset at each attempt; they are not chain-wide totals.
type Replay struct {
	Attempt               uint64               `json:"attempt,string"`
	StartedAt             time.Time            `json:"started_at"`
	ReadyAt               *time.Time           `json:"ready_at"`
	SampledAt             time.Time            `json:"sampled_at"`
	Phase                 string               `json:"phase"`
	StartSlot             uint64               `json:"start_slot,string"`
	ResumeParent          *uint64              `json:"resume_parent,string"`
	ExecutedBlocks        uint64               `json:"executed_blocks,string"`
	ExecutedTransactions  uint64               `json:"executed_transactions,string"`
	ExecutedComputeUnits  uint64               `json:"executed_compute_units,string"`
	WorkCountersSaturated bool                 `json:"work_counters_saturated"`
	SkippedSlots          uint64               `json:"skipped_slots,string"`
	Rewinds               uint64               `json:"rewinds,string"`
	DurableSlot           *uint64              `json:"durable_slot,string"`
	Completed             *CompletedBlock      `json:"completed"`
	CertificateOutcomes   *CertificateOutcomes `json:"certificate_outcomes"`
	LeaderOutcomes        *LeaderOutcomes      `json:"leader_outcomes"`
	NextKnownAssignment   *NextKnownAssignment `json:"next_known_assignment"`
	SlotHistory           *SlotHistory         `json:"slot_history,omitempty"`
	ChainActivity         *ChainActivity       `json:"chain_activity,omitempty"`
	EpochOutcomes         *EpochOutcomes       `json:"epoch_outcomes,omitempty"`
	RewardHealth          *RewardHealth        `json:"reward_health,omitempty"`
	AccountFacts          *AccountFacts        `json:"account_facts,omitempty"`
}

// NextKnownAssignment is a partial local schedule search, not proof of the next
// actual assignment. A nil slot means unknown, never no future assignment.
type NextKnownAssignment struct {
	SampledAt       time.Time `json:"sampled_at"`
	Identity        string    `json:"identity"`
	ReferenceSlot   uint64    `json:"reference_slot,string"`
	SearchedThrough uint64    `json:"searched_through_slot,string"`
	NextKnownSlot   *uint64   `json:"next_known_slot,string"`
}

// LeaderOutcomes describes bounded selected-fork schedule observations.
type LeaderOutcomes struct {
	SampledAt            time.Time `json:"sampled_at"`
	Provenance           string    `json:"provenance"`
	Identity             string    `json:"identity"`
	HasWindow            bool      `json:"has_window"`
	WindowStart          uint64    `json:"window_start,string"`
	WindowEnd            uint64    `json:"window_end,string"`
	HasCoveredRange      bool      `json:"has_covered_range"`
	CoveredStart         uint64    `json:"covered_start,string"`
	CoveredEnd           uint64    `json:"covered_end,string"`
	Produced             uint64    `json:"produced,string"`
	SelectedForkSkipped  uint64    `json:"selected_fork_skipped,string"`
	ScheduledUnknown     uint64    `json:"scheduled_unknown,string"`
	UnknownScheduleSlots uint64    `json:"unknown_schedule_slots,string"`
	UnknownAncestrySlots uint64    `json:"unknown_ancestry_slots,string"`
	CoverageIncomplete   bool      `json:"coverage_incomplete"`
}

// CertificateOutcomes describes a fixed vote account's selected-and-folded
// carrier window. Dropped is cumulative for the current replay attempt.
type CertificateOutcomes struct {
	SampledAt             time.Time `json:"sampled_at"`
	Provenance            string    `json:"provenance"`
	VoteAccount           string    `json:"vote_account"`
	CoverageIncomplete    bool      `json:"coverage_incomplete"`
	HasWindow             bool      `json:"has_window"`
	FirstCarrier          uint64    `json:"first_carrier,string"`
	LastCarrier           uint64    `json:"last_carrier,string"`
	Carriers              uint64    `json:"carriers,string"`
	Gaps                  uint64    `json:"gaps,string"`
	SlotsWithoutCarrier   uint64    `json:"slots_without_carrier,string"`
	Pending               uint64    `json:"pending,string"`
	Dropped               uint64    `json:"dropped_this_attempt,string"`
	RewardIncluded        uint64    `json:"reward_included,string"`
	RewardExcluded        uint64    `json:"reward_excluded,string"`
	RewardUnknown         uint64    `json:"reward_unknown,string"`
	RewardIneligible      uint64    `json:"reward_ineligible,string"`
	FinalFast             uint64    `json:"final_fast,string"`
	FinalSlow             uint64    `json:"final_slow,string"`
	FastMember            uint64    `json:"fast_member,string"`
	FastNonmember         uint64    `json:"fast_nonmember,string"`
	FastMembershipUnknown uint64    `json:"fast_membership_unknown,string"`
	FinalDuplicates       uint64    `json:"final_duplicates,string"`
	FinalVariants         uint64    `json:"final_variants,string"`
	FinalConflict         uint64    `json:"final_conflict,string"`
}

// CompletedBlock describes the latest locally executed bank, not finality.
type CompletedBlock struct {
	At               time.Time `json:"at"`
	Slot             uint64    `json:"slot,string"`
	Bankhash         string    `json:"bankhash"`
	BlockHeight      uint64    `json:"block_height,string"`
	Epoch            uint64    `json:"epoch,string"`
	SlotIndex        uint64    `json:"slot_index,string"`
	SlotsInEpoch     uint64    `json:"slots_in_epoch,string"`
	Transactions     uint64    `json:"transactions,string"`
	ComputeUnits     uint64    `json:"compute_units,string"`
	ExecutionSeconds float64   `json:"execution_seconds"`
	WaitSeconds      float64   `json:"wait_seconds"`
}

// Runtime comes from the node's existing ten-second diagnostics sampler.
// GoBytes excludes released heap pages; it is not process RSS.
type Runtime struct {
	SampledAt        time.Time        `json:"sampled_at"`
	HeapBytes        uint64           `json:"heap_bytes,string"`
	GoBytes          uint64           `json:"go_bytes,string"`
	NextGCBytes      uint64           `json:"next_gc_bytes,string"`
	GCCount          uint64           `json:"gc_count,string"`
	MemoryLimitBytes int64            `json:"memory_limit_bytes,string"`
	Goroutines       int              `json:"goroutines"`
	Pressure         *RuntimePressure `json:"pressure"`
}

// RuntimePressure describes the existing minute-long diagnostic window.
// GC CPU is a share of non-idle Go CPU, not host or process utilization.
type RuntimePressure struct {
	SampledAt                 time.Time `json:"sampled_at"`
	WindowSeconds             float64   `json:"window_seconds"`
	SchedulerP99Seconds       float64   `json:"scheduler_p99_seconds"`
	SchedulerMaxSeconds       float64   `json:"scheduler_max_seconds"`
	GCCPUFraction             float64   `json:"gc_cpu_fraction"`
	GCPauseMaxSeconds         float64   `json:"gc_pause_max_seconds"`
	AllocBytesPerSecond       float64   `json:"alloc_bytes_per_second"`
	MutexWaitSecondsPerSecond float64   `json:"mutex_wait_seconds_per_second"`
}

// Resources is sampled by the node, never by the reader process. Filesystems
// are named by role, not path; two roles may refer to the same filesystem.
type Resources struct {
	SampledAt  time.Time        `json:"sampled_at"`
	RSSBytes   *uint64          `json:"rss_bytes,string"`
	CPUSeconds *float64         `json:"cpu_seconds"`
	Root       *Filesystem      `json:"root"`
	Accounts   *Filesystem      `json:"accounts"`
	Ledger     *Filesystem      `json:"ledger"`
	Host       *HostResources   `json:"host,omitempty"`
	Cgroup     *CgroupResources `json:"cgroup,omitempty"`
}

// Filesystem reports total and unprivileged-available space for one path's mount.
type Filesystem struct {
	TotalBytes     uint64 `json:"total_bytes,string"`
	AvailableBytes uint64 `json:"available_bytes,string"`
	Inodes         uint64 `json:"inodes,string"`
	FreeInodes     uint64 `json:"free_inodes,string"`
}

func (s Snapshot) validate() error {
	if a := s.IncrementalArchive; a != nil && (s.SnapshotArchive != nil || a.IncrementalSlot <= a.FullSnapshotSlot) {
		return errors.New("invalid dashboard incremental archive binding")
	}
	if s.ElapsedNanoseconds != nil && *s.ElapsedNanoseconds >= math.MaxInt64 {
		return errors.New("invalid dashboard elapsed duration")
	}
	if s.SchemaVersion != SchemaVersion || s.RunID == "" || len(s.RunID) > 128 || s.StartedAt.IsZero() || s.SampledAt.Before(s.StartedAt) {
		return errors.New("invalid dashboard snapshot identity or version")
	}
	if s.Mode != "verifying" && s.Mode != "validator" {
		return errors.New("invalid dashboard node mode")
	}
	switch s.Phase {
	case "starting", "bootstrap", "replaying", "stopping":
	default:
		return errors.New("invalid dashboard lifecycle phase")
	}
	return nil
}

type message struct {
	Topic string  `json:"topic"`
	Key   string  `json:"key"`
	ID    *uint64 `json:"id,omitempty"`
	Value any     `json:"value"`
}

// frames uses the FD envelope for equivalent fields only. The Mithril topic
// carries our schema, source freshness and semantics without pretending to
// implement FD's complete dashboard protocol.
func frames(s *Snapshot, state string, now time.Time) ([][]byte, error) {
	var version, commit, identity, genesis, network, mode, phase, run, started, sampled, frontier any
	var alpenglow, rpc any
	var runtime *Runtime
	var resources *Resources
	var replay *Replay
	var consensus *Consensus
	var rpcStatus *RPC
	var recovery *Recovery
	var blockSource *BlockSource
	var pipeline *Pipeline
	cluster := "unknown"
	if s != nil {
		version, commit = s.Version, s.Commit
		identity, genesis = s.IdentityKey, s.GenesisHash
		network, mode, phase, run = s.Network, s.Mode, s.Phase, s.RunID
		started, sampled = strconv.FormatInt(s.StartedAt.UnixNano(), 10), s.SampledAt.Format(time.RFC3339Nano)
		alpenglow, rpc = s.Alpenglow, s.RPCConfigured
		runtime, resources = s.Runtime, s.Resources
		replay = s.Replay
		consensus = s.Consensus
		rpcStatus = s.RPC
		recovery = s.Recovery
		blockSource = s.BlockSource
		pipeline = s.Pipeline
		if s.ReplayFrontier != nil {
			frontier = strconv.FormatUint(*s.ReplayFrontier, 10)
		}
		// Match known genesis hashes, never a caller-supplied network label.
		if s.GenesisHash != nil {
			switch *s.GenesisHash {
			case "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d":
				cluster = "mainnet-beta"
			case "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG":
				cluster = "devnet"
			case "4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY":
				cluster = "testnet"
			}
		}
	}
	values := []message{
		{Topic: "mithril", Key: "status", Value: map[string]any{
			"schema_version": SchemaVersion, "source_state": state, "run_id": run,
			"sampled_at": sampled, "collector_at": now.Format(time.RFC3339Nano),
			"phase": phase, "network": network, "genesis_hash": genesis,
			"mode": mode, "rpc_configured": rpc, "replay_frontier": frontier,
		}},
		{Topic: "mithril", Key: "runtime", Value: runtime},
		{Topic: "mithril", Key: "resources", Value: resources},
		{Topic: "mithril", Key: "replay", Value: replay},
		{Topic: "mithril", Key: "consensus", Value: consensus},
		{Topic: "mithril", Key: "rpc", Value: rpcStatus},
		{Topic: "mithril", Key: "recovery", Value: recovery},
		{Topic: "mithril", Key: "block_source", Value: blockSource},
		{Topic: "mithril", Key: "pipeline", Value: pipeline},
		{Topic: "summary", Key: "version", Value: version},
		{Topic: "summary", Key: "commit_hash", Value: commit},
		{Topic: "summary", Key: "identity_key", Value: identity},
		{Topic: "summary", Key: "is_alpenglow", Value: alpenglow},
		{Topic: "summary", Key: "cluster", Value: cluster},
		{Topic: "summary", Key: "startup_time_nanos", Value: started},
		{Topic: "summary", Key: "server_time_nanos", Value: strconv.FormatInt(now.UnixNano(), 10)},
	}
	if s != nil && s.SnapshotArchive != nil {
		values = append(values, message{Topic: "mithril", Key: "snapshot_archive", Value: s.SnapshotArchive})
	}
	if s != nil && s.IncrementalArchive != nil {
		values = append(values, message{Topic: "mithril", Key: "incremental_archive", Value: s.IncrementalArchive})
	}
	out := make([][]byte, 0, len(values))
	for _, value := range values {
		b, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}
