package dashboard

import (
	"encoding/json"
	"errors"
	"time"
)

// PublicSnapshot is the deliberate disclosure boundary for anonymous viewers.
// Private feed types must not be embedded here: adding a diagnostic must not
// silently publish it. These observations do not establish node readiness.
type PublicSnapshot struct {
	SchemaVersion int               `json:"schema_version"`
	SourceState   string            `json:"source_state"`
	SampledAt     *time.Time        `json:"sampled_at"`
	CollectorAt   time.Time         `json:"collector_at"`
	Node          *PublicNode       `json:"node"`
	Replay        *PublicReplay     `json:"replay"`
	Consensus     *PublicConsensus  `json:"consensus"`
	Voting        *PublicVoting     `json:"voting"`
	RPC           *PublicRPC        `json:"rpc"`
	Input         *PublicInput      `json:"input"`
	Throughput    *PublicThroughput `json:"throughput"`
	Pipeline      *PublicPipeline   `json:"pipeline"`
	Resources     *PublicResources  `json:"resources,omitempty"`
	Startup       *PublicStartup    `json:"startup,omitempty"`
	Peers         *PublicPeers      `json:"peers,omitempty"`
}

// PublicNode identifies the linked node and its configured capabilities.
// Configured voting or RPC does not mean that either service is working.
type PublicNode struct {
	RunID              string  `json:"run_id"`
	Identity           *string `json:"identity"`
	GenesisHash        *string `json:"genesis_hash"`
	Version            string  `json:"version"`
	Commit             string  `json:"commit"`
	Network            string  `json:"configured_network"`
	Mode               string  `json:"configured_mode"`
	Alpenglow          bool    `json:"alpenglow"`
	RPCConfigured      bool    `json:"rpc_configured"`
	Phase              string  `json:"phase"`
	ElapsedNanoseconds *uint64 `json:"elapsed_nanoseconds,string,omitempty"`
}

// PublicObservation dates a producer, independently of the feed heartbeat.
type PublicObservation struct {
	State     string    `json:"state"`
	SampledAt time.Time `json:"sampled_at"`
}

// PublicReplay reports local execution, not a cluster transaction rate.
type PublicReplay struct {
	PublicObservation
	Attempt               uint64                     `json:"attempt,string"`
	StartedAt             time.Time                  `json:"started_at"`
	Phase                 string                     `json:"phase"`
	Frontier              *uint64                    `json:"frontier,string"`
	Executed              uint64                     `json:"executed_blocks,string"`
	ExecutedTransactions  uint64                     `json:"executed_transactions,string"`
	ExecutedComputeUnits  uint64                     `json:"executed_compute_units,string"`
	WorkCountersSaturated bool                       `json:"work_counters_saturated"`
	Rewinds               uint64                     `json:"rewinds,string"`
	DurableSlot           *uint64                    `json:"durable_slot,string"`
	Completed             *PublicBlock               `json:"completed"`
	CertificateOutcomes   *PublicCertificateOutcomes `json:"certificate_outcomes"`
	LeaderOutcomes        *PublicLeaderOutcomes      `json:"leader_outcomes"`
	NextKnownAssignment   *PublicNextKnownAssignment `json:"next_known_assignment"`
	SlotHistory           *PublicSlotHistory         `json:"slot_history,omitempty"`
	ChainActivity         *PublicChainActivity       `json:"chain_activity,omitempty"`
	EpochOutcomes         *PublicEpochOutcomes       `json:"epoch_outcomes,omitempty"`
	RewardHealth          *PublicRewardHealth        `json:"reward_health,omitempty"`
	AccountFacts          *PublicAccountFacts        `json:"account_facts,omitempty"`
}

// PublicNextKnownAssignment searches only captured local schedules. A missing
// hit is unknown; a hit need not be the next assignment across missing slots.
type PublicNextKnownAssignment struct {
	PublicObservation
	Identity        string  `json:"identity"`
	ReferenceSlot   uint64  `json:"reference_slot,string"`
	SearchedThrough uint64  `json:"searched_through_slot,string"`
	NextKnownSlot   *uint64 `json:"next_known_slot,string"`
}

// PublicLeaderOutcomes reports selected-fork schedule observations, not local forging.
type PublicLeaderOutcomes struct {
	PublicObservation
	Provenance           string `json:"provenance"`
	Identity             string `json:"identity"`
	HasWindow            bool   `json:"has_window"`
	WindowStart          uint64 `json:"window_start,string"`
	WindowEnd            uint64 `json:"window_end,string"`
	HasCoveredRange      bool   `json:"has_covered_range"`
	CoveredStart         uint64 `json:"covered_start,string"`
	CoveredEnd           uint64 `json:"covered_end,string"`
	Produced             uint64 `json:"produced,string"`
	SelectedForkSkipped  uint64 `json:"selected_fork_skipped,string"`
	ScheduledUnknown     uint64 `json:"scheduled_unknown,string"`
	UnknownScheduleSlots uint64 `json:"unknown_schedule_slots,string"`
	UnknownAncestrySlots uint64 `json:"unknown_ancestry_slots,string"`
	CoverageIncomplete   bool   `json:"coverage_incomplete"`
}

// PublicCertificateOutcomes contains fixed-account counts for the retained
// selected-and-folded carrier window, not a FAST score or payment report.
type PublicCertificateOutcomes struct {
	PublicObservation
	Provenance            string `json:"provenance"`
	VoteAccount           string `json:"vote_account"`
	CoverageIncomplete    bool   `json:"coverage_incomplete"`
	HasWindow             bool   `json:"has_window"`
	FirstCarrier          uint64 `json:"first_carrier,string"`
	LastCarrier           uint64 `json:"last_carrier,string"`
	Carriers              uint64 `json:"carriers,string"`
	Gaps                  uint64 `json:"gaps,string"`
	SlotsWithoutCarrier   uint64 `json:"slots_without_carrier,string"`
	Pending               uint64 `json:"pending,string"`
	Dropped               uint64 `json:"dropped_this_attempt,string"`
	RewardIncluded        uint64 `json:"reward_included,string"`
	RewardExcluded        uint64 `json:"reward_excluded,string"`
	RewardUnknown         uint64 `json:"reward_unknown,string"`
	RewardIneligible      uint64 `json:"reward_ineligible,string"`
	FinalFast             uint64 `json:"final_fast,string"`
	FinalSlow             uint64 `json:"final_slow,string"`
	FastMember            uint64 `json:"fast_member,string"`
	FastNonmember         uint64 `json:"fast_nonmember,string"`
	FastMembershipUnknown uint64 `json:"fast_membership_unknown,string"`
	FinalDuplicates       uint64 `json:"final_duplicates,string"`
	FinalVariants         uint64 `json:"final_variants,string"`
	FinalConflict         uint64 `json:"final_conflict,string"`
}

// PublicBlock is the last successful local execution, which may be rewound.
type PublicBlock struct {
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

// PublicConsensus separates verified progress from unverified receiver activity.
type PublicConsensus struct {
	PublicObservation
	NotarizedSlot    *uint64         `json:"notarized_slot,string"`
	FinalizedSlot    *uint64         `json:"direct_finalized_slot,string"`
	FinalizedBlockID *string         `json:"direct_finalized_block_id"`
	VotingEnabled    bool            `json:"voting_enabled"`
	SafetyFault      *bool           `json:"safety_fault,omitempty"`
	Receiver         *PublicReceiver `json:"receiver,omitempty"`
}

// PublicReceiver contains the allowlisted receiver counters.
type PublicReceiver struct {
	DatagramsReceived    uint64     `json:"datagrams_received,string"`
	MessagesDecoded      uint64     `json:"messages_decoded,string"`
	DecodeErrors         uint64     `json:"decode_errors,string"`
	ReadErrors           uint64     `json:"read_errors,string"`
	ConnectionsRejected  uint64     `json:"connections_rejected,string"`
	RateLimitedDatagrams uint64     `json:"rate_limited_datagrams,string"`
	LastMessageAt        *time.Time `json:"last_message_at"`
}

// PublicVoting reports a bound voter observation, not reward or FAST scores.
// Activity counters span the voter instance, including any identity changes.
type PublicVoting struct {
	PublicObservation
	ActivityScope      string     `json:"activity_scope"`
	Identity           string     `json:"identity"`
	VoteAccount        string     `json:"vote_account"`
	Epoch              *uint64    `json:"epoch,string"`
	RootSlot           *uint64    `json:"root_slot,string"`
	KeyEligible        *bool      `json:"key_eligible"`
	AdmittedStake      *uint64    `json:"admitted_stake_lamports,string"`
	TotalAdmittedStake *uint64    `json:"total_admitted_stake_lamports,string"`
	VotesCast          uint64     `json:"votes_cast_this_run,string"`
	NetworkLanded      uint64     `json:"network_landed_votes,string"`
	LastNetworkSlot    *uint64    `json:"last_network_landed_slot,string"`
	LastNetworkAt      *time.Time `json:"last_network_landed_at,omitempty"`
	JoinedLive         *bool      `json:"joined_live,omitempty"`
}

// PublicRPC reports configured service observations, not request contents.
type PublicRPC struct {
	PublicObservation
	Serving       bool               `json:"serving"`
	PublishedSlot *uint64            `json:"published_slot,string"`
	PublishedAt   *time.Time         `json:"published_at"`
	Activity      *PublicRPCActivity `json:"activity,omitempty"`
}

// PublicInput reports source activity without publishing endpoints or config.
type PublicInput struct {
	PublicObservation
	StartedAt     time.Time        `json:"started_at"`
	NextSlot      uint64           `json:"next_slot,string"`
	LiveConnected bool             `json:"live_connected"`
	LiveActive    bool             `json:"live_active"`
	Turbine       *PublicTurbine   `json:"turbine"`
	Reception     *PublicReception `json:"reception,omitempty"`
	Gossip        *PublicGossip    `json:"gossip"`
}

// PublicGossip reports locally known contacts and client-instance counters.
// It is neither a directory of peer endpoints nor a cluster population count.
type PublicGossip struct {
	PublicObservation
	StartedAt        time.Time `json:"started_at"`
	ReceivedPackets  uint64    `json:"received_packets,string"`
	SentPackets      uint64    `json:"sent_packets,string"`
	DecodeErrors     uint64    `json:"decode_errors,string"`
	SendErrors       uint64    `json:"send_errors,string"`
	AcceptedContacts uint64    `json:"accepted_contacts,string"`
	KnownPeers       int       `json:"known_peers"`
	RepairPeers      int       `json:"repair_peers"`
	AlpenglowPeers   int       `json:"alpenglow_peers"`
}

// PublicPipeline reports local service activity, not chain inclusion. The
// independently sampled counters are not a disjoint transaction waterfall:
// one transaction can be reconsidered by the scheduler more than once.
type PublicPipeline struct {
	PublicObservation
	StartedAt                 time.Time `json:"started_at"`
	IngressPackets            uint64    `json:"ingress_packets,string"`
	IngressBytes              uint64    `json:"ingress_bytes,string"`
	IngressFullDrops          uint64    `json:"ingress_full_drops,string"`
	PassedDedupPackets        uint64    `json:"passed_dedup_packets,string"`
	InvalidWireDrops          uint64    `json:"invalid_wire_drops,string"`
	UnsupportedVersionDrops   uint64    `json:"unsupported_version_drops,string"`
	DuplicateDrops            uint64    `json:"duplicate_drops,string"`
	VerifiedPackets           uint64    `json:"verified_packets,string"`
	InvalidSignatureDrops     uint64    `json:"invalid_signature_drops,string"`
	SchedulerBufferedTotal    uint64    `json:"scheduler_buffered_total,string"`
	SchedulerQueueDepth       *uint64   `json:"scheduler_queue_depth,string"`
	BankAdmissions            uint64    `json:"bank_admissions,string"`
	BufferCapacityDrops       uint64    `json:"buffer_capacity_drops,string"`
	CostRejections            uint64    `json:"cost_rejections,string"`
	ExecutionRejections       uint64    `json:"execution_rejections,string"`
	QUICOpenConnections       *uint64   `json:"quic_open_connections,string"`
	QUICActiveStreams         *uint64   `json:"quic_active_streams,string"`
	QUICStreamBufferDrops     *uint64   `json:"quic_stream_buffer_drops,string"`
	SchedulerExpiredRemovals  *uint64   `json:"scheduler_expired_removals,string,omitempty"`
	SchedulerReceiveEvictions *uint64   `json:"scheduler_receive_evictions,string,omitempty"`
}

// PublicTurbine counts this receiver session, not cluster-wide traffic.
type PublicTurbine struct {
	PublicObservation
	Session         uint64                    `json:"session,string"`
	StartedAt       time.Time                 `json:"started_at"`
	Packets         uint64                    `json:"packets,string"`
	DataShreds      uint64                    `json:"data_shreds,string"`
	CodingShreds    uint64                    `json:"coding_shreds,string"`
	SignatureErrors uint64                    `json:"signature_errors,string"`
	LastPacketAt    *time.Time                `json:"last_packet_at"`
	Diagnostics     *PublicTurbineDiagnostics `json:"diagnostics,omitempty"`
}

func publicObservation(at time.Time, sourceState string, now time.Time) PublicObservation {
	state := sourceState
	if at.IsZero() || at.After(now.Add(time.Second)) {
		state = "unavailable"
	} else if state == "fresh" && now.Sub(at) > 30*time.Second {
		state = "stale"
	}
	return PublicObservation{State: state, SampledAt: at}
}

func validPublicTime(at, now time.Time) bool {
	return !at.IsZero() && !at.After(now.Add(time.Second))
}

// Leader outcomes count assignments within one bounded selected-fork window.
// Unknown schedules and ancestry may overlap, so do not add them together.
func validLeaderOutcomeWindow(l *LeaderOutcomes, durable *uint64) bool {
	if !l.HasWindow {
		return !l.HasCoveredRange && l.WindowStart == 0 && l.WindowEnd == 0 && l.CoveredStart == 0 && l.CoveredEnd == 0 &&
			l.Produced == 0 && l.SelectedForkSkipped == 0 && l.ScheduledUnknown == 0 && l.UnknownScheduleSlots == 0 && l.UnknownAncestrySlots == 0
	}
	if durable == nil || l.WindowEnd > *durable || l.WindowEnd < l.WindowStart || l.WindowEnd-l.WindowStart >= 512 {
		return false
	}
	n := l.WindowEnd - l.WindowStart + 1
	if l.Produced > n || l.SelectedForkSkipped > n || l.ScheduledUnknown > n || l.UnknownScheduleSlots > n || l.UnknownAncestrySlots > n {
		return false
	}
	known := l.Produced + l.SelectedForkSkipped
	if known+l.ScheduledUnknown > n-l.UnknownScheduleSlots || known > n-l.UnknownAncestrySlots || l.ScheduledUnknown > l.UnknownAncestrySlots ||
		!l.CoverageIncomplete && (l.UnknownScheduleSlots != 0 || l.UnknownAncestrySlots != 0) {
		return false
	}
	if !l.HasCoveredRange {
		return known == 0 && l.CoveredStart == 0 && l.CoveredEnd == 0
	}
	return l.CoveredStart >= l.WindowStart && l.CoveredEnd <= l.WindowEnd && l.CoveredEnd >= l.CoveredStart && known <= l.CoveredEnd-l.CoveredStart+1
}

// Certificate counts describe at most 512 retained carriers; pending and
// dropped observations have separate lifetimes and must not be added to them.
func validCertificateOutcomeWindow(c *CertificateOutcomes, durable *uint64) bool {
	if c.Pending > 1024 {
		return false
	}
	for _, v := range [...]uint64{c.Carriers, c.Gaps, c.RewardIncluded, c.RewardExcluded, c.RewardUnknown, c.RewardIneligible,
		c.FinalFast, c.FinalSlow, c.FastMember, c.FastNonmember, c.FastMembershipUnknown, c.FinalDuplicates, c.FinalVariants, c.FinalConflict} {
		if v > 512 {
			return false
		}
	}
	if !c.HasWindow {
		return c.FirstCarrier == 0 && c.LastCarrier == 0 && c.Carriers == 0 && c.Gaps == 0 && c.SlotsWithoutCarrier == 0 &&
			c.RewardIncluded == 0 && c.RewardExcluded == 0 && c.RewardUnknown == 0 && c.RewardIneligible == 0 &&
			c.FinalFast == 0 && c.FinalSlow == 0 && c.FastMember == 0 && c.FastNonmember == 0 && c.FastMembershipUnknown == 0 &&
			c.FinalDuplicates == 0 && c.FinalVariants == 0 && c.FinalConflict == 0
	}
	if durable == nil || c.Carriers == 0 || c.LastCarrier > *durable || c.LastCarrier < c.FirstCarrier ||
		c.LastCarrier-c.FirstCarrier < c.Carriers-1 || c.SlotsWithoutCarrier != c.LastCarrier-c.FirstCarrier-(c.Carriers-1) ||
		c.Gaps > c.Carriers || c.Gaps > c.RewardUnknown {
		return false
	}
	if c.RewardIncluded+c.RewardExcluded+c.RewardUnknown+c.RewardIneligible != c.Carriers ||
		c.FinalFast+c.FinalSlow+c.FinalDuplicates+c.FinalConflict > c.Carriers-c.Gaps || c.FinalVariants > c.FinalDuplicates ||
		c.FastMember+c.FastNonmember+c.FastMembershipUnknown != c.FinalFast {
		return false
	}
	return c.CoverageIncomplete || c.Gaps == 0 && c.SlotsWithoutCarrier == 0 && c.RewardUnknown == 0 && c.FastMembershipUnknown == 0 && c.FinalConflict == 0
}

// encodePublicSnapshot explicitly copies permitted fields. Optional resource
// aggregates are already projected; raw recovery hashes and diagnostics stay private.
func encodePublicSnapshot(s *Snapshot, state string, now time.Time, throughput *PublicThroughput, resources *PublicResources, peers *PublicPeers) ([]byte, error) {
	return encodePublicSnapshotProfile(s, state, now, throughput, resources, peers, 0)
}

// The default profile stays readable by existing strict v1 clients. New
// optional fields are sent only to clients that explicitly request them.
func encodePublicSnapshotProfile(s *Snapshot, state string, now time.Time, throughput *PublicThroughput, resources *PublicResources, peers *PublicPeers, manifestProfile int) ([]byte, error) {
	if resources != nil && manifestProfile != 4 {
		legacy := *resources
		legacy.Cgroup = nil
		resources = &legacy
	}
	p := PublicSnapshot{SchemaVersion: 1, SourceState: state, CollectorAt: now, Resources: resources, Peers: peers}
	if state != "fresh" && state != "stale" && state != "unavailable" {
		return nil, errors.New("invalid public source state")
	}
	if s == nil {
		p.SourceState = "unavailable"
	}
	if s != nil && state != "unavailable" {
		p.Throughput = throughput
		p.Startup = publicStartup(s, state, now)
		if manifestProfile == 0 {
			p.Startup.SnapshotManifests = nil
		} else if manifestProfile == 1 && p.Startup.SnapshotManifests != nil {
			p.Startup.SnapshotManifests.ReadyMarker = nil
		}
		network := "unknown"
		switch s.Network {
		case "mainnet", "mainnet-beta", "devnet", "testnet", "development", "alpenglow":
			network = s.Network
		}
		p.SampledAt = &s.SampledAt
		p.Node = &PublicNode{RunID: s.RunID, Identity: s.IdentityKey, GenesisHash: s.GenesisHash,
			Version: s.Version, Commit: s.Commit, Network: network, Mode: s.Mode,
			Alpenglow: s.Alpenglow, RPCConfigured: s.RPCConfigured, Phase: s.Phase, ElapsedNanoseconds: s.ElapsedNanoseconds}
		if r := s.Replay; r != nil && validPublicTime(r.SampledAt, now) {
			p.Replay = &PublicReplay{PublicObservation: publicObservation(r.SampledAt, state, now),
				Attempt: r.Attempt, StartedAt: r.StartedAt, Phase: r.Phase, Frontier: s.ReplayFrontier,
				Executed: r.ExecutedBlocks, Rewinds: r.Rewinds, DurableSlot: r.DurableSlot,
				ExecutedTransactions: r.ExecutedTransactions, ExecutedComputeUnits: r.ExecutedComputeUnits,
				WorkCountersSaturated: r.WorkCountersSaturated, SlotHistory: publicSlotHistory(s, state, now), ChainActivity: publicChainActivity(s, state, now)}
			if b := r.Completed; b != nil && validPublicTime(b.At, now) &&
				!r.StartedAt.IsZero() && !r.StartedAt.Before(s.StartedAt) &&
				!b.At.Before(r.StartedAt) && !b.At.After(r.SampledAt) {
				p.Replay.Completed = &PublicBlock{At: b.At, Slot: b.Slot, Bankhash: b.Bankhash,
					BlockHeight: b.BlockHeight, Epoch: b.Epoch, SlotIndex: b.SlotIndex, SlotsInEpoch: b.SlotsInEpoch,
					Transactions: b.Transactions, ComputeUnits: b.ComputeUnits,
					ExecutionSeconds: b.ExecutionSeconds, WaitSeconds: b.WaitSeconds}
			}
		}
		if c := s.Consensus; c != nil && s.Alpenglow && validPublicTime(c.SampledAt, now) {
			p.Consensus = &PublicConsensus{PublicObservation: publicObservation(c.SampledAt, state, now),
				NotarizedSlot: c.NotarizedSlot,
				FinalizedSlot: c.FinalizedSlot, FinalizedBlockID: c.FinalizedBlockID, VotingEnabled: c.VotingEnabled}
			if (manifestProfile == 3 || manifestProfile == 4) && c.SafetyFault != nil {
				fault := *c.SafetyFault
				p.Consensus.SafetyFault = &fault
			}
			if r := c.Receiver; r != nil {
				p.Consensus.Receiver = &PublicReceiver{DatagramsReceived: r.DatagramsReceived, MessagesDecoded: r.MessagesDecoded,
					DecodeErrors: r.DecodeErrors, ReadErrors: r.ReadErrors, ConnectionsRejected: r.ConnectionsRejected,
					RateLimitedDatagrams: r.RateLimitedDatagrams}
				if r.LastMessageAt != nil && validPublicTime(*r.LastMessageAt, now) {
					at := *r.LastMessageAt
					p.Consensus.Receiver.LastMessageAt = &at
				}
			}
			// A previous identity's observation must not label the current node.
			if v := c.Voting; v != nil && s.Mode == "validator" && s.IdentityKey != nil && v.Identity == *s.IdentityKey && validPublicTime(v.SampledAt, now) {
				p.Voting = &PublicVoting{PublicObservation: publicObservation(v.SampledAt, state, now),
					ActivityScope: "voter_instance",
					Identity:      v.Identity, VoteAccount: v.VoteAccount, Epoch: v.Epoch, RootSlot: v.RootSlot,
					KeyEligible: v.KeyEligible, AdmittedStake: v.AdmittedStake, TotalAdmittedStake: v.TotalAdmittedStake,
					VotesCast: v.VotesCast, NetworkLanded: v.NetworkLanded, LastNetworkSlot: v.LastNetworkLandedSlot}
				if at := v.LastNetworkLandedAt; at != nil && v.LastNetworkLandedSlot != nil && v.NetworkLanded > 0 &&
					validPublicTime(*at, now) && !at.Before(s.StartedAt) && !at.After(v.SampledAt.Add(time.Second)) {
					observed := at.UTC()
					p.Voting.LastNetworkAt = &observed
				}
				if v.JoinedLive != nil {
					joined := *v.JoinedLive
					p.Voting.JoinedLive = &joined
				}
			}
		}
		if p.Replay != nil {
			p.Replay.EpochOutcomes = publicEpochOutcomes(s, p.Voting, state, now)
			p.Replay.RewardHealth = publicRewardHealth(s, p.Voting, state, now)
			p.Replay.AccountFacts = publicAccountFacts(s, state, now)
		}
		if p.Replay != nil && p.Voting != nil && s.Replay.CertificateOutcomes != nil {
			c := s.Replay.CertificateOutcomes
			if c.Provenance == "selected_and_folded" && c.VoteAccount == p.Voting.VoteAccount && validPublicTime(c.SampledAt, now) &&
				!c.SampledAt.Before(s.Replay.StartedAt) && !c.SampledAt.After(s.SampledAt.Add(time.Second)) && validCertificateOutcomeWindow(c, s.Replay.DurableSlot) {
				observation := publicObservation(c.SampledAt, state, now)
				if p.Voting.State != "fresh" {
					observation.State = p.Voting.State
				}
				p.Replay.CertificateOutcomes = &PublicCertificateOutcomes{
					PublicObservation: observation, Provenance: c.Provenance, VoteAccount: c.VoteAccount,
					CoverageIncomplete: c.CoverageIncomplete, HasWindow: c.HasWindow, FirstCarrier: c.FirstCarrier, LastCarrier: c.LastCarrier,
					Carriers: c.Carriers, Gaps: c.Gaps, SlotsWithoutCarrier: c.SlotsWithoutCarrier, Pending: c.Pending, Dropped: c.Dropped,
					RewardIncluded: c.RewardIncluded, RewardExcluded: c.RewardExcluded, RewardUnknown: c.RewardUnknown, RewardIneligible: c.RewardIneligible,
					FinalFast: c.FinalFast, FinalSlow: c.FinalSlow, FastMember: c.FastMember, FastNonmember: c.FastNonmember,
					FastMembershipUnknown: c.FastMembershipUnknown, FinalDuplicates: c.FinalDuplicates, FinalVariants: c.FinalVariants, FinalConflict: c.FinalConflict,
				}
			}
		}
		if p.Replay != nil && s.IdentityKey != nil && s.Replay.NextKnownAssignment != nil {
			a := s.Replay.NextKnownAssignment
			if a.Identity == *s.IdentityKey && validPublicTime(a.SampledAt, now) && !a.SampledAt.Before(s.Replay.StartedAt) &&
				!a.SampledAt.After(s.SampledAt.Add(time.Second)) && a.SearchedThrough >= a.ReferenceSlot &&
				(a.NextKnownSlot == nil || *a.NextKnownSlot >= a.ReferenceSlot && *a.NextKnownSlot <= a.SearchedThrough) {
				p.Replay.NextKnownAssignment = &PublicNextKnownAssignment{PublicObservation: publicObservation(a.SampledAt, state, now),
					Identity: a.Identity, ReferenceSlot: a.ReferenceSlot, SearchedThrough: a.SearchedThrough, NextKnownSlot: a.NextKnownSlot}
			}
		}
		if p.Replay != nil && p.Voting != nil && s.Replay.LeaderOutcomes != nil {
			l := s.Replay.LeaderOutcomes
			if l.Provenance == "selected_and_folded" && l.Identity == p.Voting.Identity && validPublicTime(l.SampledAt, now) &&
				!l.SampledAt.Before(s.Replay.StartedAt) && !l.SampledAt.After(s.SampledAt.Add(time.Second)) && validLeaderOutcomeWindow(l, s.Replay.DurableSlot) {
				observation := publicObservation(l.SampledAt, state, now)
				if p.Voting.State != "fresh" {
					observation.State = p.Voting.State
				}
				p.Replay.LeaderOutcomes = &PublicLeaderOutcomes{PublicObservation: observation, Provenance: l.Provenance, Identity: l.Identity,
					HasWindow: l.HasWindow, WindowStart: l.WindowStart, WindowEnd: l.WindowEnd,
					HasCoveredRange: l.HasCoveredRange, CoveredStart: l.CoveredStart, CoveredEnd: l.CoveredEnd,
					Produced: l.Produced, SelectedForkSkipped: l.SelectedForkSkipped, ScheduledUnknown: l.ScheduledUnknown,
					UnknownScheduleSlots: l.UnknownScheduleSlots, UnknownAncestrySlots: l.UnknownAncestrySlots, CoverageIncomplete: l.CoverageIncomplete}
			}
		}
		if r := s.RPC; r != nil && s.RPCConfigured && validPublicTime(r.SampledAt, now) {
			p.RPC = &PublicRPC{PublicObservation: publicObservation(r.SampledAt, state, now),
				Serving: r.Serving, Activity: publicRPCActivity(r, s.StartedAt)}
			if r.PublishedSlot != nil && r.PublishedAt != nil && validPublicTime(*r.PublishedAt, now) &&
				!r.PublishedAt.Before(s.StartedAt) && !r.PublishedAt.After(r.SampledAt) {
				p.RPC.PublishedSlot, p.RPC.PublishedAt = r.PublishedSlot, r.PublishedAt
			}
		}
		if v := s.Pipeline; v != nil && s.Mode == "validator" && validPublicTime(v.SampledAt, now) {
			p.Pipeline = &PublicPipeline{PublicObservation: publicObservation(v.SampledAt, state, now), StartedAt: v.StartedAt,
				IngressPackets: v.Ingress.EnqueuedPackets, IngressBytes: v.Ingress.EnqueuedBytes, IngressFullDrops: v.Ingress.DroppedFull,
				PassedDedupPackets: v.Dedup.OutPackets, InvalidWireDrops: v.Dedup.DroppedSanitize,
				UnsupportedVersionDrops: v.Dedup.DroppedUnsupportedVersion, DuplicateDrops: v.Dedup.DroppedDedup,
				VerifiedPackets: v.Sigverify.VerifiedPackets, InvalidSignatureDrops: v.Sigverify.DroppedSigverify,
				SchedulerBufferedTotal: v.Scheduler.BufferedTotal, SchedulerQueueDepth: v.Scheduler.QueueDepth,
				BankAdmissions: v.Scheduler.Accepted, BufferCapacityDrops: v.Scheduler.DroppedCapacity,
				CostRejections: v.Scheduler.DroppedCost, ExecutionRejections: v.Scheduler.DroppedExecution}
			if v.Scheduler.DroppedExpired != nil && v.Scheduler.EvictedCapacity != nil {
				expired, evicted := *v.Scheduler.DroppedExpired, *v.Scheduler.EvictedCapacity
				p.Pipeline.SchedulerExpiredRemovals, p.Pipeline.SchedulerReceiveEvictions = &expired, &evicted
			}
			if q := v.QUIC; q != nil {
				p.Pipeline.QUICOpenConnections, p.Pipeline.QUICActiveStreams, p.Pipeline.QUICStreamBufferDrops = &q.OpenConnections, &q.ActiveStreams, &q.StreamBufferDrops
			}
		}
		if b := s.BlockSource; b != nil && validPublicTime(b.SampledAt, now) {
			p.Input = &PublicInput{PublicObservation: publicObservation(b.SampledAt, state, now),
				StartedAt: b.StartedAt, NextSlot: b.NextSlot, LiveConnected: b.LiveConnected, LiveActive: b.LiveActive}
			p.Input.Reception = publicReception(s, b, state, now)
			if t := b.Turbine; t != nil && b.LiveConnected && validTurbineObservation(s, b, now) {
				p.Input.Turbine = &PublicTurbine{PublicObservation: publicObservation(t.SampledAt, state, now),
					Session: t.Session, StartedAt: t.StartedAt, Packets: t.Packets, DataShreds: t.DataShreds,
					CodingShreds: t.CodingShreds, SignatureErrors: t.SignatureErrors, LastPacketAt: t.LastPacketAt}
				p.Input.Turbine.Diagnostics = publicTurbineDiagnostics(t)
			}
			if g := b.Gossip; g != nil && b.LiveConnected && validPublicTime(g.SampledAt, now) {
				p.Input.Gossip = &PublicGossip{PublicObservation: publicObservation(g.SampledAt, state, now), StartedAt: g.StartedAt,
					ReceivedPackets: g.ReceivedPackets, SentPackets: g.SentPackets, DecodeErrors: g.DecodeErrors,
					SendErrors: g.SendErrors, AcceptedContacts: g.AcceptedContacts, KnownPeers: g.KnownPeers,
					RepairPeers: g.RepairPeers, AlpenglowPeers: g.AlpenglowPeers}
			}
		}
	}
	b, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	if len(b) > MaxSnapshotBytes {
		return nil, errors.New("public snapshot exceeds size limit")
	}
	return b, nil
}
