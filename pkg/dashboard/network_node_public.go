package dashboard

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// PublicNodeNetworkDetail preserves independently scoped observations and adds
// collector state/date. A receiver can disappear without hiding interface data.
type PublicNodeNetworkDetail struct {
	SchemaVersion    int                     `json:"schema_version"`
	Family           string                  `json:"family"`
	Scope            string                  `json:"scope"`
	State            string                  `json:"state"`
	Reason           string                  `json:"reason"`
	CollectorAt      time.Time               `json:"collector_at"`
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

type nodeNetworkPublicFamily struct {
	binding                 NodeNetworkBinding
	receiver                *NetworkDetailBinding
	bodies                  [len(networkDetailPublicReasons)]string
	tail                    string
	independentOnly         string
	extendedTail            string
	extendedIndependentOnly string
	latestTail              string
	latestIndependentOnly   string
	gateTail                string
}

// preparePublicNodeNetwork shares the canonical observation tail across the
// finite state prefixes. Viewers write the two strings without encoding or
// concatenation. The receiver-cleared body remains separately precomputed.
func preparePublicNodeNetwork(d NodeNetworkDetail, collectedAt, freshUntil time.Time) (*nodeNetworkPublicFamily, error) {
	if validateNodeNetworkDetail(d) != nil || !validReplayDetailTime(collectedAt.UTC()) || !freshUntil.After(collectedAt) ||
		d.CapturedAt.After(collectedAt.Add(time.Second)) {
		return nil, errors.New("source_invalid")
	}
	// Preserve the previous wire shapes for strict older clients.
	fullGate := d.RepairGate
	d.RepairGate = nil
	fullInterfaces := d.Interfaces
	hasMTU := false
	if d.Interfaces != nil {
		interfaces := *d.Interfaces
		interfaces.Interfaces = append([]InterfaceTrafficCounter{}, interfaces.Interfaces...)
		for i := range interfaces.Interfaces {
			hasMTU = hasMTU || interfaces.Interfaces[i].MTU != 0
			interfaces.Interfaces[i].MTU = 0
		}
		d.Interfaces = &interfaces
	}
	if d.Receiver != nil && !validApplicationPayloadLease(d.Receiver.RepairRequests, collectedAt, freshUntil) ||
		!validVotorPayloadLease(d.VotorPayload, collectedAt, freshUntil) {
		return nil, errors.New("source_time_invalid")
	}
	if c := d.LiveClock; c != nil && (collectedAt.Sub(c.SampledAt) >= NetworkDetailFreshFor || freshUntil.After(c.SampledAt.Add(NetworkDetailFreshFor)) ||
		c.SampledAt.After(collectedAt.Add(time.Second)) || c.AnchorAt.After(collectedAt.Add(time.Second))) {
		return nil, errors.New("source_time_invalid")
	}
	if g := d.Gossip; g != nil && g.Decode != nil && (collectedAt.Sub(g.SampledAt) >= NetworkDetailFreshFor ||
		freshUntil.After(g.SampledAt.Add(NetworkDetailFreshFor)) || g.SampledAt.After(collectedAt.Add(time.Second))) {
		return nil, errors.New("source_time_invalid")
	}
	if p := d.TPUPayload; p != nil && (collectedAt.Sub(p.Received.SampledAt) >= NetworkDetailFreshFor ||
		freshUntil.After(p.Received.SampledAt.Add(NetworkDetailFreshFor)) || p.Received.SampledAt.After(collectedAt.Add(time.Second))) {
		return nil, errors.New("source_time_invalid")
	}
	if q := d.TPUQueues; q != nil && (collectedAt.Sub(q.SampledAt) >= NetworkDetailFreshFor ||
		freshUntil.After(q.SampledAt.Add(NetworkDetailFreshFor)) || q.SampledAt.After(collectedAt.Add(time.Second))) {
		return nil, errors.New("source_time_invalid")
	}
	if !validPrewarmHandover(d.PrewarmHandover, collectedAt) {
		return nil, errors.New("source_time_invalid")
	}
	if fullGate != nil && (collectedAt.Sub(fullGate.SampledAt) >= NetworkDetailFreshFor || freshUntil.After(fullGate.SampledAt.Add(NetworkDetailFreshFor)) || fullGate.SampledAt.After(collectedAt.Add(time.Second))) {
		return nil, errors.New("source_time_invalid")
	}
	binding := d.Binding
	if binding.GenesisHash != nil {
		value := *binding.GenesisHash
		binding.GenesisHash = &value
	}
	family := &nodeNetworkPublicFamily{binding: binding}
	if d.Receiver != nil {
		value := d.Receiver.Binding
		value.GenesisHash = binding.GenesisHash
		family.receiver = &value
	}
	public := PublicNodeNetworkDetail{SchemaVersion: 2, Family: "network", Scope: "node_network", State: "fresh", CollectorAt: collectedAt.UTC(),
		Binding: binding, CapturedAt: d.CapturedAt, Receiver: d.Receiver, ReceiverReason: d.ReceiverReason,
		Interfaces: d.Interfaces, InterfacesReason: d.InterfacesReason, Gossip: d.Gossip, TPU: d.TPU, Votor: d.Votor, VotorPayload: d.VotorPayload, LiveClock: d.LiveClock,
		PrewarmHandover: d.PrewarmHandover, TPUPayload: d.TPUPayload, TPUQueues: d.TPUQueues}
	total := 0
	encode := func() (string, error) {
		body, err := json.Marshal(public)
		if err != nil || len(body) > MaxNetworkDetailBytes {
			return "", errors.New("response_limit")
		}
		return string(body), nil
	}
	for i, reason := range networkDetailPublicReasons {
		public.State, public.Reason = "stale", reason
		if reason == "" {
			public.State = "fresh"
		}
		body, err := encode()
		if err != nil {
			return nil, err
		}
		prefix, tail, ok := strings.Cut(body, `,"binding":`)
		if !ok {
			return nil, errors.New("source_invalid")
		}
		if i == 0 {
			family.tail = strings.Clone(tail)
			total += len(family.tail)
		} else if tail != family.tail {
			return nil, errors.New("source_invalid")
		}
		// Concatenation keeps only the short prefix, not the full body's backing string.
		family.bodies[i] = prefix + `,"binding":`
		total += len(family.bodies[i])
	}
	if d.Receiver != nil && (d.Interfaces != nil || d.Gossip != nil || d.TPU != nil || d.Votor != nil || d.VotorPayload != nil || d.LiveClock != nil || d.PrewarmHandover != nil || d.TPUPayload != nil || d.TPUQueues != nil) {
		public.State, public.Reason, public.Receiver, public.ReceiverReason = "fresh", "", nil, "binding_changed"
		body, err := encode()
		if err != nil {
			return nil, err
		}
		family.independentOnly = body
		total += len(family.independentOnly)
	}
	if d.Gossip != nil && d.Gossip.TxPullRequests != nil {
		// Keep both projections cached; old clients never receive new fields.
		extendedTail, extendedIndependent := family.tail, family.independentOnly
		legacy := *d.Gossip
		legacy.TxPullRequests = nil
		public.Gossip, public.State, public.Reason, public.Receiver, public.ReceiverReason = &legacy, "fresh", "", d.Receiver, d.ReceiverReason
		body, err := encode()
		if err != nil {
			return nil, err
		}
		_, legacyTail, ok := strings.Cut(body, `,"binding":`)
		if !ok {
			return nil, errors.New("source_invalid")
		}
		family.tail = strings.Clone(legacyTail)
		if extendedIndependent != "" {
			public.Receiver, public.ReceiverReason = nil, "binding_changed"
			family.independentOnly, err = encode()
			if err != nil {
				return nil, err
			}
		}
		legacyTotal := total - len(extendedTail) - len(extendedIndependent) + len(family.tail) + len(family.independentOnly)
		if legacyTotal+len(extendedTail)+len(extendedIndependent) <= networkDetailFamilyBytes {
			family.extendedTail, family.extendedIndependentOnly = extendedTail, extendedIndependent
			total = legacyTotal + len(extendedTail) + len(extendedIndependent)
		} else {
			total = legacyTotal
		}
	}
	if hasMTU {
		public.Interfaces, public.Gossip = fullInterfaces, d.Gossip
		public.State, public.Reason, public.Receiver, public.ReceiverReason = "fresh", "", d.Receiver, d.ReceiverReason
		body, err := encode()
		if err != nil {
			return nil, err
		}
		_, tail, ok := strings.Cut(body, `,"binding":`)
		if !ok {
			return nil, errors.New("source_invalid")
		}
		independent := ""
		if family.independentOnly != "" {
			public.Receiver, public.ReceiverReason = nil, "binding_changed"
			independent, err = encode()
			if err != nil {
				return nil, err
			}
		}
		if total+len(tail)+len(independent) <= networkDetailFamilyBytes {
			family.latestTail, family.latestIndependentOnly = strings.Clone(tail), independent
			total += len(tail) + len(independent)
		}
	}
	if fullGate != nil {
		public.Interfaces, public.Gossip = fullInterfaces, d.Gossip
		public.State, public.Reason, public.Receiver, public.ReceiverReason, public.RepairGate = "fresh", "", d.Receiver, d.ReceiverReason, fullGate
		body, err := encode()
		if err == nil {
			_, tail, ok := strings.Cut(body, `,"binding":`)
			if !ok {
				return nil, errors.New("source_invalid")
			}
			if total+len(tail) <= networkDetailFamilyBytes {
				family.gateTail = strings.Clone(tail)
				total += len(tail)
			}
		}
	}
	if total > networkDetailFamilyBytes {
		return nil, errors.New("response_limit")
	}
	return family, nil
}
