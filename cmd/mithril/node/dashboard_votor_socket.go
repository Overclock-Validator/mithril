package node

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

// The node starts each engine once, then attaches it after successful Start.
// This owner follows that inbound receiver lifetime, not the signing identity.
type votorSocketOwner struct {
	engine        *consensus.AlpenglowObserverEngine
	binding       dashboard.NodeNetworkBinding
	generation    uint64
	latest        *votorSocketCapture
	payloadLatest *votorPayloadCapture
}

type votorPayloadCapture struct {
	detail     dashboard.VotorPayloadDetail
	freshUntil time.Time
	received   payloadSample
	sent       payloadSample
}

func votorPayloadMeasurement(o consensus.VotorApplicationPayloadObservation, binding dashboard.NodeNetworkBinding, generation uint64, started, completed time.Time) *votorPayloadCapture {
	if completed.Before(started) || completed.Sub(started) > time.Second {
		return nil
	}
	until := started.Add(dashboard.NetworkDetailFreshFor)
	d := dashboard.VotorPayloadDetail{Scope: "votor_instance_application_payload", Generation: generation}
	copyCounter := func(raw alpenglow.ApplicationPayloadObservation, present bool) *dashboard.ApplicationPayloadCounter {
		if !present || raw.StartedAt.Before(binding.NodeStartedAt) || raw.SampledAt.Before(raw.StartedAt) ||
			raw.SampledAt.After(completed) || !raw.SampledAt.Add(dashboard.NetworkDetailFreshFor).After(completed) || raw.Known && raw.Bytes == ^uint64(0) {
			return nil
		}
		if deadline := raw.SampledAt.Add(dashboard.NetworkDetailFreshFor); deadline.Before(until) {
			until = deadline
		}
		c := &dashboard.ApplicationPayloadCounter{StartedAt: raw.StartedAt.UTC(), SampledAt: raw.SampledAt.UTC()}
		if raw.Known {
			value := raw.Bytes
			c.Bytes = &value
		}
		return c
	}
	d.Received, d.SendAccepted = copyCounter(o.Received, o.HasReceived), copyCounter(o.SendAccepted, o.HasSendAccepted)
	if d.Received == nil && d.SendAccepted == nil {
		return nil
	}
	capture := &votorPayloadCapture{detail: d, freshUntil: until}
	if d.Received != nil {
		capture.received = payloadSample{startedAt: o.Received.StartedAt, sampledAt: o.Received.SampledAt, bytes: o.Received.Bytes, known: o.Received.Known}
	}
	if d.SendAccepted != nil {
		capture.sent = payloadSample{startedAt: o.SendAccepted.StartedAt, sampledAt: o.SendAccepted.SampledAt, bytes: o.SendAccepted.Bytes, known: o.SendAccepted.Known}
	}
	return capture
}

type votorSocketCapture struct {
	raw        metrics.SocketObservation
	binding    dashboard.NodeNetworkBinding
	detail     dashboard.VotorSocketDetail
	freshUntil time.Time
}

func (d *nodeDashboard) observeVotorSocket() {
	if d == nil {
		return
	}
	d.mu.Lock()
	owner := d.votorSocket
	if !d.networkDetails || d.networkRetired || d.state.Phase == "stopping" || owner == nil || owner.engine != d.consensus {
		d.mu.Unlock()
		return
	}
	binding := owner.binding
	if d.state.RunID != binding.RunID || !d.state.StartedAt.Equal(binding.NodeStartedAt) || !sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash) {
		d.mu.Unlock()
		return
	}
	d.mu.Unlock()
	started := time.Now()
	o := owner.engine.ObserveReceiverSocketMemory()
	payload := owner.engine.ObserveVotorApplicationPayload()
	completed := time.Now()
	payloadCapture := votorPayloadMeasurement(payload, binding, owner.generation, started, completed)
	detail, until, err := socketDetailMeasurement(turbine.SocketMemorySample{Observation: o}, true, started, completed)
	if limit := started.Add(dashboard.NetworkDetailFreshFor); until.After(limit) {
		until = limit
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.votorSocket != owner || d.consensus != owner.engine || d.networkRetired || d.state.Phase == "stopping" ||
		d.state.RunID != binding.RunID || !d.state.StartedAt.Equal(binding.NodeStartedAt) || !sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash) {
		return
	}
	if payloadCapture != nil && owner.payloadLatest != nil {
		if payloadCapture.detail.Received != nil {
			var retained *dashboard.ApplicationPayloadInterval
			if previous := owner.payloadLatest.detail.Received; previous != nil {
				retained = previous.Window
			}
			payloadCapture.detail.Received.Window = payloadWindow(owner.payloadLatest.received, payloadCapture.received, retained)
		}
		if payloadCapture.detail.SendAccepted != nil {
			var retained *dashboard.ApplicationPayloadInterval
			if previous := owner.payloadLatest.detail.SendAccepted; previous != nil {
				retained = previous.Window
			}
			payloadCapture.detail.SendAccepted.Window = payloadWindow(owner.payloadLatest.sent, payloadCapture.sent, retained)
		}
	}
	owner.payloadLatest = payloadCapture
	if err != nil || completed.Before(started) || completed.Sub(started) > time.Second {
		owner.latest = nil
		return
	}
	if before := owner.latest; before != nil {
		if drops := metrics.ObservedSocketDrops(before.raw, o); drops.Known {
			detail.DropWindow = &dashboard.SocketDropWindow{StartedAt: drops.StartedAt.UTC(), SampledAt: drops.SampledAt.UTC(),
				DurationMilliseconds: uint64(drops.SampledAt.Sub(drops.StartedAt) / time.Millisecond), Drops: drops.Drops}
		}
	}
	owner.latest = &votorSocketCapture{raw: o, binding: binding, freshUntil: until,
		detail: dashboard.VotorSocketDetail{Scope: "votor_inbound_udp_socket", Generation: owner.generation, SampledAt: completed.UTC(), Observation: detail}}
}

func (d *nodeDashboard) captureVotorPayload(binding dashboard.NodeNetworkBinding, now time.Time) (*dashboard.VotorPayloadDetail, time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	owner := d.votorSocket
	if d.networkRetired || d.state.Phase == "stopping" || owner == nil || owner.engine != d.consensus || owner.payloadLatest == nil ||
		d.state.RunID != binding.RunID || !d.state.StartedAt.Equal(binding.NodeStartedAt) || !sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash) ||
		owner.binding.RunID != binding.RunID || !owner.binding.NodeStartedAt.Equal(binding.NodeStartedAt) || !sameReplayDetailGenesis(owner.binding.GenesisHash, binding.GenesisHash) {
		return nil, time.Time{}
	}
	c := owner.payloadLatest
	if !c.freshUntil.After(now) {
		return nil, time.Time{}
	}
	detail := c.detail
	for _, field := range []**dashboard.ApplicationPayloadCounter{&detail.Received, &detail.SendAccepted} {
		if *field != nil {
			v := **field
			if now.Before(v.SampledAt) {
				return nil, time.Time{}
			}
			if v.Bytes != nil {
				value := *v.Bytes
				v.Bytes = &value
			}
			if v.Window != nil {
				window := *v.Window
				v.Window = &window
			}
			*field = &v
		}
	}
	return &detail, c.freshUntil
}

func (d *nodeDashboard) captureVotorSocket(binding dashboard.NodeNetworkBinding, now time.Time) (*dashboard.VotorSocketDetail, time.Time) {
	d.mu.Lock()
	owner := d.votorSocket
	if d.networkRetired || d.state.Phase == "stopping" || owner == nil || owner.engine != d.consensus || owner.latest == nil {
		d.mu.Unlock()
		return nil, time.Time{}
	}
	c := owner.latest
	d.mu.Unlock()
	if c.binding.RunID != binding.RunID || !c.binding.NodeStartedAt.Equal(binding.NodeStartedAt) || !sameReplayDetailGenesis(c.binding.GenesisHash, binding.GenesisHash) ||
		now.Before(c.detail.SampledAt) || !c.freshUntil.After(now) {
		return nil, time.Time{}
	}
	v := c.detail
	for _, field := range []**time.Time{&v.Observation.StartedAt, &v.Observation.SampledAt} {
		if *field != nil {
			at := **field
			*field = &at
		}
	}
	if v.Observation.Memory != nil {
		memory := *v.Observation.Memory
		v.Observation.Memory = &memory
	}
	if v.Observation.DropWindow != nil {
		window := *v.Observation.DropWindow
		v.Observation.DropWindow = &window
	}
	d.mu.Lock()
	unchanged := d.votorSocket == owner && d.consensus == owner.engine && !d.networkRetired && d.state.Phase != "stopping" &&
		d.state.RunID == binding.RunID && d.state.StartedAt.Equal(binding.NodeStartedAt) && sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash)
	d.mu.Unlock()
	if !unchanged {
		return nil, time.Time{}
	}
	return &v, c.freshUntil
}
