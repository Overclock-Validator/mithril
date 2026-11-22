package dashboard

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"
	"unicode/utf8"
)

// NodeNetworkBinding identifies a run, independently of receiver or DB lifetime.
type NodeNetworkBinding struct {
	RunID         string    `json:"run_id"`
	NodeStartedAt time.Time `json:"node_started_at"`
	GenesisHash   *string   `json:"genesis_hash"`
}

// InterfaceTrafficDetail contains raw counters for a bounded visible-interface
// sample. Interface identities are not lifetime tokens; rates are not implied.
type InterfaceTrafficDetail struct {
	Scope            string                    `json:"scope"`
	State            string                    `json:"state"`
	CaptureStartedAt time.Time                 `json:"capture_started_at"`
	SampledAt        time.Time                 `json:"sampled_at"`
	TotalInterfaces  *uint32                   `json:"total_interfaces,string"`
	Interfaces       []InterfaceTrafficCounter `json:"interfaces"`
}

// InterfaceTrafficCounter omits interface names, addresses, MACs and paths.
// Counters can overlap between physical, virtual, tunnel and loopback devices.
// MTU is the sampled interface setting, not measured path MTU or bandwidth.
type InterfaceTrafficCounter struct {
	Index           uint32                    `json:"index,string"`
	Loopback        bool                      `json:"loopback"`
	ReceiveBytes    uint64                    `json:"receive_bytes,string"`
	TransmitBytes   uint64                    `json:"transmit_bytes,string"`
	ReceivePackets  uint64                    `json:"receive_packets,string"`
	TransmitPackets uint64                    `json:"transmit_packets,string"`
	Failures        *InterfaceTrafficFailures `json:"failures,omitempty"`
	MTU             uint32                    `json:"mtu,string,omitempty"`
}

// InterfaceTrafficFailures are /proc/net/dev interface totals, not Mithril
// socket losses. Linux folds missed receive errors into its receive drop column.
type InterfaceTrafficFailures struct {
	ReceiveErrors   uint64 `json:"receive_errors,string"`
	ReceiveDropped  uint64 `json:"receive_dropped,string"`
	TransmitErrors  uint64 `json:"transmit_errors,string"`
	TransmitDropped uint64 `json:"transmit_dropped,string"`
}

// GossipOperationDetail copies native client-lifetime operation counters.
// Categories overlap and do not establish acceptance or remote delivery.
type GossipOperationDetail struct {
	Scope             string               `json:"scope"`
	ClientGeneration  uint64               `json:"client_generation,string"`
	StartedAt         time.Time            `json:"started_at"`
	SampledAt         time.Time            `json:"sampled_at"`
	RxPings           uint64               `json:"rx_pings,string"`
	RxPongs           uint64               `json:"rx_pongs,string"`
	RxContacts        uint64               `json:"rx_contacts,string"`
	RxPullRequests    uint64               `json:"rx_pull_requests,string"`
	TxPushMessages    uint64               `json:"tx_push_messages,string"`
	TxPingMessages    uint64               `json:"tx_ping_messages,string"`
	TxPongMessages    uint64               `json:"tx_pong_messages,string"`
	TxPullResponses   uint64               `json:"tx_pull_responses,string"`
	TxPullRequests    *uint64              `json:"tx_pull_requests,string,omitempty"`
	Socket            *GossipSocketDetail  `json:"socket,omitempty"`
	LocalShredVersion *uint16              `json:"local_shred_version,string,omitempty"`
	Payload           *GossipPayloadDetail `json:"payload,omitempty"`
	Decode            *GossipDecodeDetail  `json:"decode,omitempty"`
}

// GossipDecodeDetail retains one opted-in Run's original diagnostic date.
// SampledAt is inherited from its enclosing GossipOperationDetail after source
// equality is checked. WireKinds is fixed protocol order: pull request, pull
// response, push, prune, ping, pong, unknown variant, malformed header. Entries
// are nullable canonical decimal strings; they count headers, not valid bodies.
// Contact outcomes count visited values only, including already-applied prefixes
// before an unsupported/malformed tail. UnparsedSuffix excludes the failing value.
// Independently sampled counters need not form an atomic arithmetic partition.
type GossipDecodeDetail struct {
	Scope              string     `json:"scope"`
	SocketGeneration   uint64     `json:"socket_generation,string"`
	StartedAt          time.Time  `json:"started_at"`
	WireKinds          [8]*string `json:"wire_kinds"`
	AttemptedContactV2 *uint64    `json:"attempted_contact_v2,string"`
	SignatureValid     *uint64    `json:"signature_valid,string"`
	SignatureInvalid   *uint64    `json:"signature_invalid,string"`
	UnsupportedType    *uint64    `json:"unsupported_type,string"`
	MalformedValue     *uint64    `json:"malformed_value,string"`
	UnparsedSuffix     *uint64    `json:"unparsed_suffix,string"`
}

// GossipPayloadDetail counts copied/written UDP payload bytes in one Run,
// independently of authenticated messages, TCP echo and kernel accounting.
// A null direction is saturated; omission means recording was not observed.
type GossipPayloadDetail struct {
	Scope            string                 `json:"scope"`
	SocketGeneration uint64                 `json:"socket_generation,string"`
	StartedAt        time.Time              `json:"started_at"`
	SampledAt        time.Time              `json:"sampled_at"`
	ReceivedBytes    *uint64                `json:"received_bytes,string"`
	SentBytes        *uint64                `json:"sent_bytes,string"`
	Window           *GossipPayloadInterval `json:"window,omitempty"`
}

// GossipPayloadInterval preserves a same-Run observed interval, not wire
// bandwidth, delivery, or a rolling average. Null directions are unknown.
type GossipPayloadInterval struct {
	StartedAt           time.Time `json:"started_at"`
	SampledAt           time.Time `json:"sampled_at"`
	DurationNanoseconds uint64    `json:"duration_nanoseconds,string"`
	ReceivedBytes       *uint64   `json:"received_bytes,string"`
	SentBytes           *uint64   `json:"sent_bytes,string"`
}

// GossipSocketDetail binds raw socket accounting to one Client.Run socket.
// Client and socket generations differ; identity swaps change neither.
type GossipSocketDetail struct {
	Scope       string       `json:"scope"`
	Generation  uint64       `json:"generation,string"`
	Observation SocketDetail `json:"observation"`
}

// TPUSocketDetail is independently dated accounting for one TPU instance's
// retained UDP socket. It is not a listener-health or transaction-delivery fact.
type TPUSocketDetail struct {
	Scope       string       `json:"scope"`
	Generation  uint64       `json:"generation,string"`
	SampledAt   time.Time    `json:"sampled_at"`
	Observation SocketDetail `json:"observation"`
}

// VotorSocketDetail is accounting for the attached engine's inbound listener,
// not outbound broadcaster sockets or evidence of vote delivery.
type VotorSocketDetail struct {
	Scope       string       `json:"scope"`
	Generation  uint64       `json:"generation,string"`
	SampledAt   time.Time    `json:"sampled_at"`
	Observation SocketDetail `json:"observation"`
}

// ApplicationPayloadCounter is one receiver or broadcaster instance total.
// Bytes is null after saturation; an absent counter has no observed owner.
type ApplicationPayloadCounter struct {
	StartedAt time.Time                   `json:"started_at"`
	SampledAt time.Time                   `json:"sampled_at"`
	Bytes     *uint64                     `json:"bytes,string"`
	Window    *ApplicationPayloadInterval `json:"window,omitempty"`
}

// ApplicationPayloadInterval is one same-owner measured counter difference,
// not encrypted wire traffic, remote delivery, or a rolling-rate estimate.
type ApplicationPayloadInterval struct {
	StartedAt           time.Time `json:"started_at"`
	SampledAt           time.Time `json:"sampled_at"`
	DurationNanoseconds uint64    `json:"duration_nanoseconds,string"`
	Bytes               uint64    `json:"bytes,string"`
}

// VotorPayloadDetail separates received application bytes from locally queued
// per-peer send copies. Neither counter measures encrypted traffic or delivery.
type VotorPayloadDetail struct {
	Scope        string                     `json:"scope"`
	Generation   uint64                     `json:"generation,string"`
	Received     *ApplicationPayloadCounter `json:"received"`
	SendAccepted *ApplicationPayloadCounter `json:"send_accepted"`
}

// TPUPayloadDetail counts application prefixes actually returned by TPU stream
// reads, including failed reads. It excludes overflow probes, encrypted framing,
// streams refused before reading, transaction acceptance and remote delivery.
type TPUPayloadDetail struct {
	Scope      string                     `json:"scope"`
	Generation uint64                     `json:"generation,string"`
	Received   *ApplicationPayloadCounter `json:"received"`
}

// TPUQueueGauge counts buffered packets, excluding worker-local and sink work.
type TPUQueueGauge struct {
	Length   uint64 `json:"length,string"`
	Capacity uint64 `json:"capacity,string"`
}

// TPUQueuesDetail preserves independent point samples from the attached TPU's
// original channels. It is neither an atomic backlog nor time spent full.
type TPUQueuesDetail struct {
	Scope       string        `json:"scope"`
	Generation  uint64        `json:"generation,string"`
	StartedAt   time.Time     `json:"started_at"`
	SampledAt   time.Time     `json:"sampled_at"`
	Ingress     TPUQueueGauge `json:"ingress"`
	DedupOutput TPUQueueGauge `json:"dedup_output"`
	Verified    TPUQueueGauge `json:"verified"`
}

// ConsensusClockDetail is the existing bounded local production-clock estimate,
// not a live-network, replay or accepted-consensus frontier. AnchorAge is the
// source's elapsed time; wall-date subtraction must not reconstruct it.
type ConsensusClockDetail struct {
	Scope                string    `json:"scope"`
	Generation           uint64    `json:"generation,string"`
	SampledAt            time.Time `json:"sampled_at"`
	AnchorAt             time.Time `json:"anchor_at"`
	AnchorSlot           uint64    `json:"anchor_slot,string"`
	AnchorAgeNanoseconds uint64    `json:"anchor_age_nanoseconds,string"`
	EstimatedSlot        uint64    `json:"estimated_slot,string"`
	AddedSlots           uint64    `json:"added_slots,string"`
}

// PrewarmHandoverDetail preserves one completed prewarm-to-replay handover.
// Returned blocks are not proof of subsequent replay or canonical inclusion.
type PrewarmHandoverDetail struct {
	Generation        uint64               `json:"generation,string"`
	HandedOverAt      time.Time            `json:"handed_over_at"`
	FloorSlot         uint64               `json:"floor_slot,string"`
	Capacity          uint64               `json:"capacity,string"`
	ReturnedBlocks    uint64               `json:"returned_blocks,string"`
	OverflowEvictions uint64               `json:"overflow_evictions,string"`
	FirstSlot         *uint64              `json:"first_slot,string"`
	LastSlot          *uint64              `json:"last_slot,string"`
	Repair            *PrewarmRepairDetail `json:"repair"`
}

// PrewarmRepairDetail contains counters scoped to the prewarm repair origin.
// Categories overlap: a late response can also have an earlier timeout.
type PrewarmRepairDetail struct {
	SampledAt     time.Time `json:"sampled_at"`
	Requests      uint64    `json:"requests,string"`
	Responses     uint64    `json:"responses,string"`
	LateResponses uint64    `json:"late_responses,string"`
	Timeouts      uint64    `json:"timeouts,string"`
}

// RepairGateDetail is a sampled local catch-up gate, not proof of repair
// traffic, completed work, or node readiness. Its lifetime follows Receiver.
type RepairGateDetail struct {
	SampledAt time.Time `json:"sampled_at"`
	Armed     bool      `json:"armed"`
}

// NodeNetworkDetail is a fixed v2 envelope for independently owned observations.
// Receiver retains the v1 receiver binding and never supplies host counters.
type NodeNetworkDetail struct {
	SchemaVersion    int                     `json:"schema_version"`
	Family           string                  `json:"family"`
	Scope            string                  `json:"scope"`
	Binding          NodeNetworkBinding      `json:"binding"`
	CapturedAt       time.Time               `json:"captured_at"`
	Receiver         *NetworkDetail          `json:"receiver"`
	ReceiverReason   string                  `json:"receiver_reason"`
	Interfaces       *InterfaceTrafficDetail `json:"interfaces"`
	InterfacesReason string                  `json:"interfaces_reason"`
	Gossip           *GossipOperationDetail  `json:"gossip,omitempty"`
	TPU              *TPUSocketDetail        `json:"tpu,omitempty"`
	Votor            *VotorSocketDetail      `json:"votor,omitempty"`
	VotorPayload     *VotorPayloadDetail     `json:"votor_payload,omitempty"`
	LiveClock        *ConsensusClockDetail   `json:"live_clock,omitempty"`
	PrewarmHandover  *PrewarmHandoverDetail  `json:"prewarm_handover,omitempty"`
	TPUPayload       *TPUPayloadDetail       `json:"tpu_payload,omitempty"`
	TPUQueues        *TPUQueuesDetail        `json:"tpu_queues,omitempty"`
	RepairGate       *RepairGateDetail       `json:"repair_gate,omitempty"`
}

// NodeNetworkCapture carries the original conservative monotonic deadline.
// Live observations bound the lease; historical handover dates do not renew it.
type NodeNetworkCapture struct {
	Detail     NodeNetworkDetail
	FreshUntil time.Time
}

// NodeNetworkSampler copies cached observations without performing I/O.
// It is an alternative to the v1 receiver sampler on the same private route.
type NodeNetworkSampler func(now time.Time) (NodeNetworkCapture, error)

func validateInterfaceTraffic(d InterfaceTrafficDetail) bool {
	if d.Scope != "visible_interface_counters" || !validReplayDetailTime(d.CaptureStartedAt) || !validReplayDetailTime(d.SampledAt) ||
		d.Interfaces == nil || len(d.Interfaces) > 8 {
		return false
	}
	if d.State != "available" {
		switch d.State {
		case "unsupported", "read_failed", "invalid_record", "binding_changed", "capture_expired", "clock_invalid":
			return d.TotalInterfaces == nil && len(d.Interfaces) == 0
		default:
			return false
		}
	}
	if d.SampledAt.Before(d.CaptureStartedAt) || d.SampledAt.Sub(d.CaptureStartedAt) > time.Second ||
		d.TotalInterfaces == nil || *d.TotalInterfaces > 256 || len(d.Interfaces) != min(int(*d.TotalInterfaces), 8) {
		return false
	}
	for i, value := range d.Interfaces {
		if value.Index == 0 || i > 0 && value.Index <= d.Interfaces[i-1].Index {
			return false
		}
	}
	return true
}

func validNodeNetworkBinding(b NodeNetworkBinding) bool {
	return b.RunID != "" && len(b.RunID) <= 128 && utf8.ValidString(b.RunID) && validReplayDetailTime(b.NodeStartedAt) &&
		(b.GenesisHash == nil || validReplayDetailHash(*b.GenesisHash, false))
}

func validApplicationPayloadCounter(c *ApplicationPayloadCounter, startedAt, capturedAt time.Time) bool {
	if c == nil || !validReplayDetailTime(c.StartedAt) || !validReplayDetailTime(c.SampledAt) ||
		c.StartedAt.Before(startedAt) || c.SampledAt.Before(c.StartedAt) || c.SampledAt.After(capturedAt.Add(time.Second)) ||
		c.Bytes != nil && *c.Bytes == math.MaxUint64 {
		return false
	}
	if w := c.Window; w != nil {
		return c.Bytes != nil && validReplayDetailTime(w.StartedAt) && validReplayDetailTime(w.SampledAt) &&
			w.SampledAt.Equal(c.SampledAt) && w.DurationNanoseconds > 0 &&
			w.DurationNanoseconds <= uint64(NetworkDetailFreshFor) && w.Bytes <= *c.Bytes
	}
	return true
}

func validApplicationPayloadLease(c *ApplicationPayloadCounter, now, until time.Time) bool {
	return c == nil || (!c.SampledAt.After(now.Add(time.Second)) && now.Sub(c.SampledAt) < NetworkDetailFreshFor &&
		!until.After(c.SampledAt.Add(NetworkDetailFreshFor)))
}

func validVotorPayloadLease(p *VotorPayloadDetail, now, until time.Time) bool {
	return p == nil || validApplicationPayloadLease(p.Received, now, until) && validApplicationPayloadLease(p.SendAccepted, now, until)
}

func validPrewarmHandover(p *PrewarmHandoverDetail, at time.Time) bool {
	if p == nil {
		return true
	}
	if p.Generation == 0 || p.Generation == math.MaxUint64 || p.Capacity == 0 || p.ReturnedBlocks > p.Capacity ||
		!validReplayDetailTime(p.HandedOverAt) || p.HandedOverAt.After(at.Add(time.Second)) ||
		p.Repair != nil && (!validReplayDetailTime(p.Repair.SampledAt) || p.Repair.SampledAt.After(at.Add(time.Second))) {
		return false
	}
	if p.ReturnedBlocks == 0 {
		return p.FirstSlot == nil && p.LastSlot == nil
	}
	return p.FirstSlot != nil && p.LastSlot != nil && *p.FirstSlot >= p.FloorSlot && *p.LastSlot >= *p.FirstSlot &&
		*p.LastSlot-*p.FirstSlot >= p.ReturnedBlocks-1 && (p.ReturnedBlocks != 1 || *p.FirstSlot == *p.LastSlot)
}

func validateNodeNetworkDetail(d NodeNetworkDetail) error {
	if d.SchemaVersion != 2 || d.Family != "network" || d.Scope != "node_network" ||
		!validNodeNetworkBinding(d.Binding) || !validReplayDetailTime(d.CapturedAt) {
		return errors.New("source_invalid")
	}
	if d.Receiver == nil {
		if !networkDetailSourceReason(d.ReceiverReason) {
			return errors.New("source_invalid")
		}
	} else {
		r := d.Receiver
		if d.ReceiverReason != "" || validateNetworkDetail(*r) != nil ||
			!sameNodeNetworkBinding(d.Binding, NodeNetworkBinding{RunID: r.Binding.RunID, NodeStartedAt: r.Binding.NodeStartedAt, GenesisHash: r.Binding.GenesisHash}) ||
			r.UpdatedAt.After(d.CapturedAt.Add(time.Second)) || r.Reconstruction != nil && r.Reconstruction.ObservedAt.After(d.CapturedAt.Add(time.Second)) {
			return errors.New("source_invalid")
		}
	}
	if g := d.RepairGate; g != nil && (d.Receiver == nil || !validReplayDetailTime(g.SampledAt) ||
		g.SampledAt.Before(d.Receiver.Binding.SourceStartedAt) || g.SampledAt.Before(d.Receiver.Binding.ReceiverStartedAt) ||
		g.SampledAt.After(d.CapturedAt.Add(time.Second))) {
		return errors.New("source_invalid")
	}
	if d.Interfaces == nil {
		if d.InterfacesReason != "not_provided" && !networkDetailSourceReason(d.InterfacesReason) {
			return errors.New("source_invalid")
		}
	} else if d.InterfacesReason != "" || !validateInterfaceTraffic(*d.Interfaces) || d.Interfaces.SampledAt.After(d.CapturedAt.Add(time.Second)) ||
		d.Interfaces.CaptureStartedAt.After(d.CapturedAt.Add(time.Second)) {
		return errors.New("source_invalid")
	}
	if g := d.Gossip; g != nil && (g.Scope != "gossip_client_totals" || g.ClientGeneration == 0 || g.TxPullRequests != nil && *g.TxPullRequests == math.MaxUint64 || !validReplayDetailTime(g.StartedAt) ||
		!validReplayDetailTime(g.SampledAt) || g.SampledAt.After(d.CapturedAt.Add(time.Second))) {
		return errors.New("source_invalid")
	}
	if d.Gossip != nil && d.Gossip.Socket != nil {
		s := d.Gossip.Socket
		if s.Scope != "gossip_udp_socket" || s.Generation == 0 || !s.Observation.Enabled ||
			validateSocketDetail(s.Observation) != nil || s.Observation.ReadBytes != nil ||
			s.Observation.SampledAt != nil && s.Observation.SampledAt.After(d.CapturedAt.Add(time.Second)) {
			return errors.New("source_invalid")
		}
	}
	if g := d.Gossip; g != nil && g.Payload != nil {
		p := g.Payload
		if p.Scope != "gossip_run_udp_payload" || p.SocketGeneration == 0 || !validReplayDetailTime(p.StartedAt) || !validReplayDetailTime(p.SampledAt) ||
			p.StartedAt.Before(g.StartedAt) || p.SampledAt.Before(p.StartedAt) || p.SampledAt.After(d.CapturedAt.Add(time.Second)) ||
			g.Socket != nil && p.SocketGeneration != g.Socket.Generation ||
			p.ReceivedBytes != nil && *p.ReceivedBytes == ^uint64(0) || p.SentBytes != nil && *p.SentBytes == ^uint64(0) {
			return errors.New("source_invalid")
		}
		if w := p.Window; w != nil {
			if !validReplayDetailTime(w.StartedAt) || !validReplayDetailTime(w.SampledAt) || !w.SampledAt.Equal(p.SampledAt) ||
				w.DurationNanoseconds == 0 || w.DurationNanoseconds > uint64(NetworkDetailFreshFor) ||
				w.ReceivedBytes == nil && w.SentBytes == nil ||
				w.ReceivedBytes != nil && (p.ReceivedBytes == nil || *w.ReceivedBytes > *p.ReceivedBytes) ||
				w.SentBytes != nil && (p.SentBytes == nil || *w.SentBytes > *p.SentBytes) {
				return errors.New("source_invalid")
			}
		}
	}
	if g := d.Gossip; g != nil && g.Decode != nil {
		p := g.Decode
		if p.Scope != "gossip_run_decode" || p.SocketGeneration == 0 || !validReplayDetailTime(p.StartedAt) ||
			p.StartedAt.Before(g.StartedAt) || g.SampledAt.Before(p.StartedAt) || g.Socket != nil && p.SocketGeneration != g.Socket.Generation ||
			g.Payload != nil && p.SocketGeneration != g.Payload.SocketGeneration {
			return errors.New("source_invalid")
		}
		for _, count := range p.WireKinds {
			if count != nil {
				value, err := strconv.ParseUint(*count, 10, 64)
				if err != nil || value == math.MaxUint64 || strconv.FormatUint(value, 10) != *count {
					return errors.New("source_invalid")
				}
			}
		}
		for _, count := range [...]*uint64{p.AttemptedContactV2, p.SignatureValid, p.SignatureInvalid, p.UnsupportedType, p.MalformedValue, p.UnparsedSuffix} {
			if count != nil && *count == math.MaxUint64 {
				return errors.New("source_invalid")
			}
		}
	}
	if s := d.TPU; s != nil && (s.Scope != "tpu_udp_socket" || s.Generation == 0 || !validReplayDetailTime(s.SampledAt) ||
		s.SampledAt.After(d.CapturedAt.Add(time.Second)) || !s.Observation.Enabled || validateSocketDetail(s.Observation) != nil ||
		s.Observation.ReadBytes != nil ||
		s.Observation.SampledAt != nil && s.Observation.SampledAt.After(s.SampledAt.Add(time.Second))) {
		return errors.New("source_invalid")
	}
	if s := d.Votor; s != nil && (s.Scope != "votor_inbound_udp_socket" || s.Generation == 0 || !validReplayDetailTime(s.SampledAt) ||
		s.SampledAt.After(d.CapturedAt.Add(time.Second)) || !s.Observation.Enabled || validateSocketDetail(s.Observation) != nil ||
		s.Observation.ReadBytes != nil || s.Observation.SampledAt != nil && s.Observation.SampledAt.After(s.SampledAt.Add(time.Second))) {
		return errors.New("source_invalid")
	}
	if p := d.VotorPayload; p != nil {
		if p.Scope != "votor_instance_application_payload" || p.Generation == 0 || p.Received == nil && p.SendAccepted == nil ||
			d.Votor != nil && p.Generation != d.Votor.Generation {
			return errors.New("source_invalid")
		}
		for _, c := range []*ApplicationPayloadCounter{p.Received, p.SendAccepted} {
			if c != nil && !validApplicationPayloadCounter(c, d.Binding.NodeStartedAt, d.CapturedAt) {
				return errors.New("source_invalid")
			}
		}
	}
	if p := d.TPUPayload; p != nil && (p.Scope != "tpu_server_stream_read_prefix" || p.Generation == 0 ||
		!validApplicationPayloadCounter(p.Received, d.Binding.NodeStartedAt, d.CapturedAt) ||
		d.TPU != nil && p.Generation != d.TPU.Generation) {
		return errors.New("source_invalid")
	}
	if q := d.TPUQueues; q != nil {
		if q.Scope != "tpu_pipeline_queue_samples" || q.Generation == 0 || !validReplayDetailTime(q.StartedAt) || !validReplayDetailTime(q.SampledAt) ||
			q.StartedAt.Before(d.Binding.NodeStartedAt) || q.SampledAt.Before(q.StartedAt) || q.SampledAt.After(d.CapturedAt.Add(time.Second)) ||
			d.CapturedAt.Sub(q.SampledAt) >= NetworkDetailFreshFor ||
			d.TPU != nil && q.Generation != d.TPU.Generation || d.TPUPayload != nil && q.Generation != d.TPUPayload.Generation {
			return errors.New("source_invalid")
		}
		for _, gauge := range [...]TPUQueueGauge{q.Ingress, q.DedupOutput, q.Verified} {
			if gauge.Capacity == 0 || gauge.Length > gauge.Capacity {
				return errors.New("source_invalid")
			}
		}
	}
	if c := d.LiveClock; c != nil {
		if c.Scope != "consensus_bounded_live_clock" || c.Generation == 0 || !validReplayDetailTime(c.SampledAt) || !validReplayDetailTime(c.AnchorAt) ||
			c.SampledAt.After(d.CapturedAt.Add(time.Second)) || c.AnchorAt.After(d.CapturedAt.Add(time.Second)) ||
			d.CapturedAt.Sub(c.SampledAt) >= NetworkDetailFreshFor || c.AnchorSlot == 0 || c.AnchorAgeNanoseconds >= math.MaxInt64 ||
			d.Votor != nil && c.Generation != d.Votor.Generation || d.VotorPayload != nil && c.Generation != d.VotorPayload.Generation {
			return errors.New("source_invalid")
		}
		added := min(c.AnchorAgeNanoseconds/200000000, uint64(4), ^uint64(0)-c.AnchorSlot)
		if c.AddedSlots != added || c.EstimatedSlot != c.AnchorSlot+added {
			return errors.New("source_invalid")
		}
	}
	if d.PrewarmHandover != nil && (d.Binding.GenesisHash == nil || !validReplayDetailHash(*d.Binding.GenesisHash, true) ||
		!validPrewarmHandover(d.PrewarmHandover, d.CapturedAt)) {
		return errors.New("source_invalid")
	}
	return nil
}

func sameNodeNetworkBinding(a, b NodeNetworkBinding) bool {
	return a.RunID == b.RunID && a.NodeStartedAt.Equal(b.NodeStartedAt) &&
		(a.GenesisHash == nil) == (b.GenesisHash == nil) && (a.GenesisHash == nil || *a.GenesisHash == *b.GenesisHash)
}

func decodeNodeNetworkDetail(body []byte) (NodeNetworkDetail, error) {
	var d NodeNetworkDetail
	if len(body) > MaxNetworkDetailBytes || json.Unmarshal(body, &d) != nil || validateNodeNetworkDetail(d) != nil {
		return NodeNetworkDetail{}, errors.New("source_invalid")
	}
	canonical, err := json.Marshal(d)
	if err != nil || !bytes.Equal(body, canonical) {
		return NodeNetworkDetail{}, errors.New("source_invalid")
	}
	return d, nil
}
