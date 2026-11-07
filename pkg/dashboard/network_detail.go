package dashboard

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// MaxNetworkDetailBytes bounds one complete network document.
const MaxNetworkDetailBytes = 16 << 10

// NetworkDetailFreshFor is the maximum age of an owner socket observation.
const NetworkDetailFreshFor = 30 * time.Second

// NetworkDetailBinding identifies the node, block source and owned socket pair.
type NetworkDetailBinding struct {
	RunID             string    `json:"run_id"`
	NodeStartedAt     time.Time `json:"node_started_at"`
	GenesisHash       *string   `json:"genesis_hash"`
	SourceStartedAt   time.Time `json:"source_started_at"`
	ReceiverStartedAt time.Time `json:"receiver_started_at"`
	ReceiverSession   uint64    `json:"receiver_session,string"`
	SocketGeneration  uint64    `json:"socket_generation,string"`
}

// NetworkDetail is a private atomic capture, not a host-wide network summary.
type NetworkDetail struct {
	SchemaVersion  int                   `json:"schema_version"`
	Family         string                `json:"family"`
	Scope          string                `json:"scope"`
	State          string                `json:"state"`
	Reason         string                `json:"reason"`
	Binding        NetworkDetailBinding  `json:"binding"`
	UpdatedAt      time.Time             `json:"updated_at"`
	Live           SocketDetail          `json:"live"`
	Repair         SocketDetail          `json:"repair"`
	Reconstruction *ReconstructionDetail `json:"reconstruction,omitempty"`
	ArrivalTiming  *ArrivalTimingDetail  `json:"arrival_timing,omitempty"`
	// RepairRequests counts locally accepted request-write prefixes, not RX,
	// pongs, framing, remote delivery or useful repair results.
	RepairRequests *ApplicationPayloadCounter `json:"repair_requests,omitempty"`
}

// ArrivalTimingDetail describes retained adjacent numeric-slot TVU arrivals,
// not block duration, fork agreement or cluster-tip accuracy.
type ArrivalTimingDetail struct {
	Scope                    string    `json:"scope"`
	SampledAt                time.Time `json:"sampled_at"`
	LatestReceiptAt          time.Time `json:"latest_receipt_at"`
	RetainedSlots            int       `json:"retained_slots"`
	AdjacentPairs            int       `json:"adjacent_pairs"`
	ReorderedPairs           int       `json:"reordered_pairs"`
	TotalIntervalNanoseconds uint64    `json:"total_interval_nanoseconds,string"`
}

// ReconstructionDetail is a historical all-data-full assembler high-water,
// before block decoding. Mixed live/repair/FEC/spool input can contribute.
type ReconstructionDetail struct {
	Scope      string    `json:"scope"`
	Slot       uint64    `json:"slot,string"`
	ObservedAt time.Time `json:"observed_at"`
}

// SocketDetail distinguishes a known zero from an unavailable kernel read.
type SocketDetail struct {
	Enabled    bool              `json:"enabled"`
	State      string            `json:"state"`
	StartedAt  *time.Time        `json:"started_at"`
	SampledAt  *time.Time        `json:"sampled_at"`
	Memory     *SocketMemory     `json:"memory"`
	DropWindow *SocketDropWindow `json:"drop_window"`
	ReadBytes  *SocketReadBytes  `json:"read_bytes,omitempty"`
}

// SocketReadBytes counts only UDP payload bytes copied to this socket's readers,
// including invalid data and truncated prefixes. It is not wire bandwidth,
// authenticated/useful payload, spool/FEC output or remote delivery.
type SocketReadBytes struct {
	StartedAt time.Time           `json:"started_at"`
	SampledAt time.Time           `json:"sampled_at"`
	Bytes     uint64              `json:"bytes,string"`
	Window    *SocketReadInterval `json:"window,omitempty"`
}

type SocketReadInterval struct {
	StartedAt           time.Time `json:"started_at"`
	SampledAt           time.Time `json:"sampled_at"`
	DurationNanoseconds uint64    `json:"duration_nanoseconds,string"`
	Bytes               uint64    `json:"bytes,string"`
}

// SocketMemory preserves all nine independent Linux SO_MEMINFO uint32 values.
// Allocations may exceed limits; neither sums nor payload depth are implied.
type SocketMemory struct {
	ReceiveAllocatedBytes uint32 `json:"receive_allocated_bytes,string"`
	ReceiveLimitBytes     uint32 `json:"receive_limit_bytes,string"`
	SendAllocatedBytes    uint32 `json:"send_allocated_bytes,string"`
	SendLimitBytes        uint32 `json:"send_limit_bytes,string"`
	ForwardAllocatedBytes uint32 `json:"forward_allocated_bytes,string"`
	SendQueuedBytes       uint32 `json:"send_queued_bytes,string"`
	OptionsAllocatedBytes uint32 `json:"options_allocated_bytes,string"`
	BacklogBytes          uint32 `json:"backlog_bytes,string"`
	Drops                 uint32 `json:"drops,string"`
}

// SocketDropWindow is a same-socket delta with an owner-measured duration.
type SocketDropWindow struct {
	StartedAt            time.Time `json:"started_at"`
	SampledAt            time.Time `json:"sampled_at"`
	DurationMilliseconds uint64    `json:"duration_milliseconds,string"`
	Drops                uint64    `json:"drops,string"`
}

// NetworkDetailCapture retains the original monotonic freshness deadline.
// FreshUntil is never serialized or reconstructed from the document's dates.
type NetworkDetailCapture struct {
	Detail     NetworkDetail
	FreshUntil time.Time
}

// NetworkDetailSampler must return a bounded detached cache copy without I/O.
// It runs on the existing Feed sampling cycle, never from an HTTP request.
type NetworkDetailSampler func(now time.Time) (NetworkDetailCapture, error)

func validateNetworkDetail(d NetworkDetail) error {
	b := d.Binding
	if d.SchemaVersion != 1 || d.Family != "network" || d.Scope != "turbine_receiver_session" || d.State != "fresh" || d.Reason != "" ||
		!utf8.ValidString(b.RunID) || b.RunID == "" || len(b.RunID) > 128 || b.ReceiverSession == 0 || b.SocketGeneration == 0 ||
		!validReplayDetailTime(b.NodeStartedAt) || !validReplayDetailTime(b.SourceStartedAt) || !validReplayDetailTime(b.ReceiverStartedAt) ||
		!validReplayDetailTime(d.UpdatedAt) || b.GenesisHash != nil && !validReplayDetailHash(*b.GenesisHash, false) || !d.Live.Enabled {
		return errors.New("source_invalid")
	}
	if r := d.Reconstruction; r != nil && (r.Scope != "assembler_session_high_water" || !validReplayDetailTime(r.ObservedAt) || r.ObservedAt.After(d.UpdatedAt.Add(time.Second))) {
		return errors.New("source_invalid")
	}
	if a := d.ArrivalTiming; a != nil && (a.Scope != "retained_adjacent_tvu_arrivals" || !validReplayDetailTime(a.SampledAt) || !validReplayDetailTime(a.LatestReceiptAt) ||
		a.RetainedSlots < 2 || a.RetainedSlots > 750 || a.AdjacentPairs < 1 || a.AdjacentPairs >= a.RetainedSlots ||
		a.ReorderedPairs < 0 || a.ReorderedPairs > a.AdjacentPairs || a.TotalIntervalNanoseconds > 1<<63-1 ||
		a.ReorderedPairs == a.AdjacentPairs && a.TotalIntervalNanoseconds != 0) {
		return errors.New("source_invalid")
	}
	for _, s := range []SocketDetail{d.Live, d.Repair} {
		if err := validateSocketDetail(s); err != nil {
			return err
		}
	}
	if d.RepairRequests != nil && (!d.Repair.Enabled || !validApplicationPayloadCounter(d.RepairRequests, b.ReceiverStartedAt, d.UpdatedAt)) {
		return errors.New("source_invalid")
	}
	return nil
}

func validateSocketDetail(s SocketDetail) error {
	if !s.Enabled {
		if s.State != "not_provided" || s.StartedAt != nil || s.SampledAt != nil || s.Memory != nil || s.DropWindow != nil || s.ReadBytes != nil {
			return errors.New("source_invalid")
		}
		return nil
	}
	switch s.State {
	case "available", "unsupported", "closed", "read_failed", "invalid_response", "capture_expired", "clock_invalid":
	default:
		return errors.New("source_invalid")
	}
	if (s.Memory != nil) != (s.State == "available") || (s.StartedAt == nil) != (s.SampledAt == nil) ||
		s.StartedAt == nil && s.State != "unsupported" || s.StartedAt != nil && (!validReplayDetailTime(*s.StartedAt) || !validReplayDetailTime(*s.SampledAt)) {
		return errors.New("source_invalid")
	}
	if w := s.DropWindow; w != nil {
		if s.Memory == nil || !validReplayDetailTime(w.StartedAt) || !validReplayDetailTime(w.SampledAt) ||
			!w.SampledAt.Equal(*s.SampledAt) || w.DurationMilliseconds > uint64(NetworkDetailFreshFor/time.Millisecond) || w.Drops > uint64(s.Memory.Drops) {
			return errors.New("source_invalid")
		}
	}
	if r := s.ReadBytes; r != nil && (!validReplayDetailTime(r.StartedAt) || !validReplayDetailTime(r.SampledAt)) {
		return errors.New("source_invalid")
	}
	if r := s.ReadBytes; r != nil && r.Window != nil {
		w := r.Window
		if !validReplayDetailTime(w.StartedAt) || !validReplayDetailTime(w.SampledAt) || !w.SampledAt.Equal(r.SampledAt) ||
			w.DurationNanoseconds == 0 || w.DurationNanoseconds > uint64(NetworkDetailFreshFor) || w.Bytes > r.Bytes {
			return errors.New("source_invalid")
		}
	}
	return nil
}

// decodeNetworkDetail accepts only this producer's canonical typed JSON.
func decodeNetworkDetail(body []byte) (NetworkDetail, error) {
	var d NetworkDetail
	if len(body) > MaxNetworkDetailBytes || json.Unmarshal(body, &d) != nil || validateNetworkDetail(d) != nil {
		return d, errors.New("source_invalid")
	}
	canonical, err := json.Marshal(d)
	if err != nil || !bytes.Equal(body, canonical) {
		return NetworkDetail{}, errors.New("source_invalid")
	}
	return d, nil
}

type networkDetailGate struct {
	binding    NetworkDetailBinding
	observedAt time.Time
	reason     string
	nodeReason string
	stamp      statusStamp
	nodeStamp  statusStamp
}

func networkGate(s Snapshot, now time.Time) networkDetailGate {
	g := networkDetailGate{binding: NetworkDetailBinding{RunID: s.RunID, NodeStartedAt: s.StartedAt}, observedAt: now}
	if s.GenesisHash != nil {
		value := *s.GenesisHash
		g.binding.GenesisHash = &value
	}
	n := s.BlockSource
	if n == nil {
		g.reason = "waiting_source"
	} else if n.ConfiguredSource != "turbine" {
		g.reason = "unsupported_source"
	} else if !n.LiveConnected || n.Turbine == nil {
		g.reason = "source_unavailable"
	} else {
		g.binding.SourceStartedAt, g.binding.ReceiverStartedAt, g.binding.ReceiverSession = n.StartedAt, n.Turbine.StartedAt, n.Turbine.Session
	}
	return g
}

func (g networkDetailGate) failure(now time.Time) string {
	if reason := g.nodeFailure(now); reason != "" {
		return reason
	}
	return g.reason
}

func (g networkDetailGate) nodeFailure(now time.Time) string {
	if g.observedAt.IsZero() || now.Before(g.observedAt) {
		return "source_time_invalid"
	}
	if now.Sub(g.observedAt) > StaleAfter {
		return "status_stale"
	}
	return g.nodeReason
}

func (g networkDetailGate) nodeBinding() NodeNetworkBinding {
	return NodeNetworkBinding{RunID: g.binding.RunID, NodeStartedAt: g.binding.NodeStartedAt, GenesisHash: g.binding.GenesisHash}
}

func (g networkDetailGate) matches(b NetworkDetailBinding) bool {
	a := g.binding
	return a.RunID == b.RunID && a.NodeStartedAt.Equal(b.NodeStartedAt) &&
		(a.GenesisHash == nil) == (b.GenesisHash == nil) && (a.GenesisHash == nil || *a.GenesisHash == *b.GenesisHash) &&
		a.SourceStartedAt.Equal(b.SourceStartedAt) && a.ReceiverStartedAt.Equal(b.ReceiverStartedAt) && a.ReceiverSession == b.ReceiverSession
}

type networkDetailPublication struct {
	body                    string
	extendedBody            string
	latestBody              string
	gateBody                string
	freshUntil              time.Time
	gate                    networkDetailGate
	reason                  string
	independentOnly         string
	extendedIndependentOnly string
	latestIndependentOnly   string
	receiver                *NetworkDetailBinding
}

const networkPullRequestsHeader = "Mithril-Network-Pull-Requests"
const networkMTUHeader = "Mithril-Network-Mtu"
const networkRepairGateHeader = "Mithril-Network-Repair-Gate"

type networkDetailSource struct {
	sample     NetworkDetailSampler
	nodeSample NodeNetworkSampler
	gate       atomic.Pointer[networkDetailGate]
	latest     atomic.Pointer[networkDetailPublication]
}

// update is called serially after a valid ordinary status publication. It
// replaces failures rather than retaining an older socket's known values.
func (s *networkDetailSource) update(snapshot Snapshot, now time.Time) {
	if s.nodeSample != nil {
		s.updateNode(snapshot, now)
		return
	}
	g := networkGate(snapshot, now)
	s.gate.Store(&g)
	p := &networkDetailPublication{gate: g, reason: g.failure(now)}
	defer func() { s.latest.Store(p) }()
	if p.reason != "" {
		return
	}
	capture, err := s.sample(now)
	completedAt := time.Now()
	if err != nil {
		p.reason = networkDetailReason(err.Error())
		return
	}
	if !g.matches(capture.Detail.Binding) {
		p.reason = "binding_changed"
		return
	}
	if validateNetworkDetail(capture.Detail) != nil {
		p.reason = "source_invalid"
		return
	}
	// The owner may publish while its cache is being copied. Check against
	// completion, but retain the owner's deadline rather than redating it.
	if capture.FreshUntil.IsZero() || !capture.FreshUntil.After(completedAt) || capture.FreshUntil.After(completedAt.Add(NetworkDetailFreshFor)) ||
		!validApplicationPayloadLease(capture.Detail.RepairRequests, completedAt, capture.FreshUntil) ||
		capture.Detail.Reconstruction != nil && capture.Detail.Reconstruction.ObservedAt.After(completedAt.Add(time.Second)) {
		p.reason = "source_time_invalid"
		return
	}
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		p.reason = "source_invalid"
		return
	}
	if len(body) > MaxNetworkDetailBytes {
		p.reason = "response_limit"
		return
	}
	p.body, p.freshUntil = string(body), capture.FreshUntil
}

func networkDetailReason(reason string) string {
	switch reason {
	case "waiting_source", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid", "unsupported_source", "binding_changed", "response_limit", "invalid_request":
		return reason
	default:
		return "source_unavailable"
	}
}

func (s *networkDetailSource) serveHTTP(w http.ResponseWriter, r *http.Request) {
	status, reason, body := http.StatusServiceUnavailable, "waiting_source", ""
	var remaining int64
	if r.Method != http.MethodGet || r.URL.Path != "/details/network" || r.URL.EscapedPath() != "/details/network" ||
		r.URL.RawQuery != "" || r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		status, reason = http.StatusBadRequest, "invalid_request"
	} else if g := s.gate.Load(); g != nil {
		now := time.Now()
		reason = g.failure(now)
		if s.nodeSample != nil {
			reason = g.nodeFailure(now)
		}
		if reason == "" {
			p := s.latest.Load()
			switch {
			case p == nil:
				reason = "waiting_source"
			case s.nodeSample == nil && !g.matches(p.gate.binding) || s.nodeSample != nil && !sameNodeNetworkBinding(g.nodeBinding(), p.gate.nodeBinding()):
				reason = "binding_changed"
			case p.reason != "":
				reason = p.reason
			case !p.freshUntil.After(now):
				reason = "status_stale"
			default:
				candidate := p.body
				if r.Header.Get(networkPullRequestsHeader) == "1" && p.extendedBody != "" {
					candidate = p.extendedBody
				}
				if r.Header.Get(networkPullRequestsHeader) == "1" && r.Header.Get(networkMTUHeader) == "1" && p.latestBody != "" {
					candidate = p.latestBody
				}
				if r.Header.Get(networkPullRequestsHeader) == "1" && r.Header.Get(networkMTUHeader) == "1" && r.Header.Get(networkRepairGateHeader) == "1" && p.gateBody != "" {
					candidate = p.gateBody
				}
				if s.nodeSample != nil && p.receiver != nil && (g.failure(now) != "" || !g.matches(*p.receiver)) {
					candidate = p.independentOnly
					if r.Header.Get(networkPullRequestsHeader) == "1" && p.extendedIndependentOnly != "" {
						candidate = p.extendedIndependentOnly
					}
					if r.Header.Get(networkPullRequestsHeader) == "1" && r.Header.Get(networkMTUHeader) == "1" && p.latestIndependentOnly != "" {
						candidate = p.latestIndependentOnly
					}
				}
				remaining = p.freshUntil.Sub(now).Milliseconds()
				if candidate == "" {
					reason = "binding_changed"
				} else if remaining <= 0 {
					reason = "status_stale"
				} else {
					status, body = http.StatusOK, candidate
				}
			}
		}
	}
	if body == "" {
		body = `{"schema_version":1,"family":"network","state":"unavailable","reason":"` + reason + `"}`
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if status == http.StatusOK {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining, 10))
	}
	w.WriteHeader(status)
	if _, err := io.WriteString(w, body); err != nil {
		return
	}
}
