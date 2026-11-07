package dashboard

import (
	"encoding/json"
	"time"
)

// updateNode retains the existing receiver gate for its child, while interface
// availability depends only on the node run and the cached observation's lease.
func (s *networkDetailSource) updateNode(snapshot Snapshot, now time.Time) {
	g := networkGate(snapshot, now)
	s.gate.Store(&g)
	p := &networkDetailPublication{gate: g, reason: g.nodeFailure(now)}
	defer func() { s.latest.Store(p) }()
	if p.reason != "" {
		return
	}
	capture, err := s.nodeSample(now)
	completedAt := time.Now()
	if err != nil {
		p.reason = networkDetailReason(err.Error())
		return
	}
	d := capture.Detail
	if !sameNodeNetworkBinding(g.nodeBinding(), d.Binding) {
		p.reason = "binding_changed"
		return
	}
	if validateNodeNetworkDetail(d) != nil {
		p.reason = "source_invalid"
		return
	}
	if capture.FreshUntil.IsZero() || !capture.FreshUntil.After(completedAt) || capture.FreshUntil.After(completedAt.Add(NetworkDetailFreshFor)) ||
		d.CapturedAt.After(completedAt.Add(time.Second)) ||
		d.RepairGate != nil && (capture.FreshUntil.After(d.RepairGate.SampledAt.Add(NetworkDetailFreshFor)) || d.RepairGate.SampledAt.After(completedAt.Add(time.Second))) ||
		d.Receiver != nil && !validApplicationPayloadLease(d.Receiver.RepairRequests, completedAt, capture.FreshUntil) ||
		!validVotorPayloadLease(d.VotorPayload, completedAt, capture.FreshUntil) ||
		!validPrewarmHandover(d.PrewarmHandover, completedAt) ||
		d.Gossip != nil && d.Gossip.Decode != nil && (capture.FreshUntil.After(d.Gossip.SampledAt.Add(NetworkDetailFreshFor)) ||
			d.Gossip.SampledAt.After(completedAt.Add(time.Second)) || completedAt.Sub(d.Gossip.SampledAt) >= NetworkDetailFreshFor) ||
		d.TPUPayload != nil && (capture.FreshUntil.After(d.TPUPayload.Received.SampledAt.Add(NetworkDetailFreshFor)) ||
			d.TPUPayload.Received.SampledAt.After(completedAt.Add(time.Second))) ||
		d.TPUQueues != nil && (capture.FreshUntil.After(d.TPUQueues.SampledAt.Add(NetworkDetailFreshFor)) ||
			d.TPUQueues.SampledAt.After(completedAt.Add(time.Second)) || completedAt.Sub(d.TPUQueues.SampledAt) >= NetworkDetailFreshFor) ||
		d.LiveClock != nil && (capture.FreshUntil.After(d.LiveClock.SampledAt.Add(NetworkDetailFreshFor)) ||
			d.LiveClock.SampledAt.After(completedAt.Add(time.Second)) || d.LiveClock.AnchorAt.After(completedAt.Add(time.Second))) {
		p.reason = "source_time_invalid"
		return
	}
	if d.Receiver != nil && (g.failure(completedAt) != "" || !g.matches(d.Receiver.Binding)) {
		d.Receiver, d.ReceiverReason = nil, "binding_changed"
		d.RepairGate = nil
	}
	// Encode negotiated variants on the sampling path, never on a viewer request.
	// The original v2 shape remains the default for strict older readers.
	encode := func(detail NodeNetworkDetail) (string, string, string, string, error) {
		var gate []byte
		if detail.RepairGate != nil {
			var err error
			gate, err = json.Marshal(detail)
			if err != nil {
				return "", "", "", "", err
			}
			detail.RepairGate = nil
		}
		latest, err := json.Marshal(detail)
		if err != nil || len(latest) > MaxNetworkDetailBytes {
			return "", "", "", "", err
		}
		latestBody := string(latest)
		gateBody := latestBody
		if gate != nil && len(gate) <= MaxNetworkDetailBytes {
			gateBody = string(gate)
		}
		if detail.Interfaces != nil {
			interfaces := *detail.Interfaces
			interfaces.Interfaces = append([]InterfaceTrafficCounter{}, interfaces.Interfaces...)
			for i := range interfaces.Interfaces {
				interfaces.Interfaces[i].MTU = 0
			}
			detail.Interfaces = &interfaces
		}
		extended, err := json.Marshal(detail)
		if err != nil || len(extended) > MaxNetworkDetailBytes {
			return "", "", "", "", err
		}
		if detail.Gossip != nil && detail.Gossip.TxPullRequests != nil {
			legacy := *detail.Gossip
			legacy.TxPullRequests = nil
			detail.Gossip = &legacy
		}
		body, err := json.Marshal(detail)
		if err != nil || len(body) > MaxNetworkDetailBytes {
			return "", "", "", "", err
		}
		return string(body), string(extended), latestBody, gateBody, nil
	}
	body, extended, latest, gate, err := encode(d)
	if err != nil || len(body) > MaxNetworkDetailBytes {
		p.reason = "response_limit"
		return
	}
	if body == "" || extended == "" || latest == "" || gate == "" {
		p.reason = "response_limit"
		return
	}
	p.body, p.extendedBody, p.latestBody, p.freshUntil = body, extended, latest, capture.FreshUntil
	p.gateBody = gate
	if d.Receiver != nil {
		binding := d.Receiver.Binding
		binding.GenesisHash = g.binding.GenesisHash
		p.receiver = &binding
	}
	if d.Interfaces != nil || d.Gossip != nil || d.TPU != nil || d.Votor != nil || d.VotorPayload != nil || d.LiveClock != nil || d.PrewarmHandover != nil || d.TPUPayload != nil || d.TPUQueues != nil {
		if d.Receiver == nil {
			p.independentOnly = p.body
			p.extendedIndependentOnly = p.extendedBody
			p.latestIndependentOnly = p.latestBody
		} else {
			d.Receiver, d.ReceiverReason = nil, "binding_changed"
			d.RepairGate = nil
			body, extended, latest, _, err := encode(d)
			if err != nil || len(body) > MaxNetworkDetailBytes {
				p.body, p.reason = "", "response_limit"
				return
			}
			if body == "" || extended == "" || latest == "" {
				p.body, p.reason = "", "response_limit"
				return
			}
			p.independentOnly, p.extendedIndependentOnly, p.latestIndependentOnly = body, extended, latest
		}
	}
}
