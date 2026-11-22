package node

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

type interfaceTrafficObservation struct {
	binding    dashboard.NodeNetworkBinding
	detail     dashboard.InterfaceTrafficDetail
	freshUntil time.Time
}

// interfaceTrafficMonitor is sampled only by the existing diagnostics owner.
// Its original lease is independent of replay, receiver and database lifetimes.
type interfaceTrafficMonitor struct {
	sampler *metrics.InterfaceTrafficSampler
	latest  atomic.Pointer[interfaceTrafficObservation]
	retired atomic.Bool
}

func (m *interfaceTrafficMonitor) observe(binding dashboard.NodeNetworkBinding) {
	if m == nil || m.retired.Load() {
		return
	}
	started := time.Now()
	o, supported := m.sampler.Sample()
	if !supported {
		o = metrics.InterfaceTrafficObservation{StartedAt: started, SampledAt: time.Now(), State: "unsupported"}
	}
	detail, err := interfaceTrafficMeasurement(o)
	if err != nil || m.retired.Load() {
		m.latest.Store(nil)
		return
	}
	if binding.GenesisHash != nil {
		value := *binding.GenesisHash
		binding.GenesisHash = &value
	}
	m.latest.Store(&interfaceTrafficObservation{binding: binding, detail: detail, freshUntil: o.StartedAt.Add(dashboard.NetworkDetailFreshFor)})
}

func (m *interfaceTrafficMonitor) stop() {
	if m != nil {
		m.retired.Store(true)
		m.latest.Store(nil)
	}
}

func interfaceTrafficMeasurement(o metrics.InterfaceTrafficObservation) (dashboard.InterfaceTrafficDetail, error) {
	d := dashboard.InterfaceTrafficDetail{Scope: "visible_interface_counters", State: o.State,
		CaptureStartedAt: o.StartedAt.UTC(), SampledAt: o.SampledAt.UTC(), Interfaces: []dashboard.InterfaceTrafficCounter{}}
	if o.StartedAt.IsZero() || o.SampledAt.IsZero() || o.Count < 0 || o.Count > metrics.MaxInterfaceTraffic {
		return d, errors.New("source_invalid")
	}
	if o.State != "available" {
		if o.Count != 0 || o.TotalInterfaces != 0 || o.Interfaces != ([metrics.MaxInterfaceTraffic]metrics.InterfaceTrafficCounters{}) {
			return d, errors.New("source_invalid")
		}
		switch o.State {
		case "unsupported", "read_failed", "invalid_record", "binding_changed", "capture_expired", "clock_invalid":
			return d, nil
		default:
			return d, errors.New("source_invalid")
		}
	}
	if o.SampledAt.Before(o.StartedAt) || o.SampledAt.Sub(o.StartedAt) > time.Second || o.TotalInterfaces > 256 || o.Count != min(int(o.TotalInterfaces), metrics.MaxInterfaceTraffic) {
		return d, errors.New("source_invalid")
	}
	total := o.TotalInterfaces
	d.TotalInterfaces = &total
	for i := range o.Count {
		v := o.Interfaces[i]
		if v.Index == 0 || i > 0 && v.Index <= o.Interfaces[i-1].Index {
			return dashboard.InterfaceTrafficDetail{}, errors.New("source_invalid")
		}
		row := dashboard.InterfaceTrafficCounter{Index: v.Index, Loopback: v.Loopback, MTU: v.MTU,
			ReceiveBytes: v.ReceiveBytes, TransmitBytes: v.TransmitBytes, ReceivePackets: v.ReceivePackets, TransmitPackets: v.TransmitPackets}
		if v.FailuresAvailable {
			row.Failures = &dashboard.InterfaceTrafficFailures{ReceiveErrors: v.ReceiveErrors, ReceiveDropped: v.ReceiveDropped,
				TransmitErrors: v.TransmitErrors, TransmitDropped: v.TransmitDropped}
		}
		d.Interfaces = append(d.Interfaces, row)
	}
	return d, nil
}

func (m *interfaceTrafficMonitor) capture(binding dashboard.NodeNetworkBinding, now time.Time) (*dashboard.InterfaceTrafficDetail, time.Time, string) {
	if m == nil || m.retired.Load() {
		return nil, time.Time{}, "source_unavailable"
	}
	o := m.latest.Load()
	if o == nil {
		return nil, time.Time{}, "waiting_source"
	}
	if o.binding.RunID != binding.RunID || !o.binding.NodeStartedAt.Equal(binding.NodeStartedAt) || !sameReplayDetailGenesis(o.binding.GenesisHash, binding.GenesisHash) {
		return nil, time.Time{}, "binding_changed"
	}
	if !o.freshUntil.After(now) || now.Before(o.detail.SampledAt) {
		return nil, time.Time{}, "source_time_invalid"
	}
	d := o.detail
	d.Interfaces = append([]dashboard.InterfaceTrafficCounter{}, d.Interfaces...)
	for i := range d.Interfaces {
		if d.Interfaces[i].Failures != nil {
			value := *d.Interfaces[i].Failures
			d.Interfaces[i].Failures = &value
		}
	}
	if d.TotalInterfaces != nil {
		value := *d.TotalInterfaces
		d.TotalInterfaces = &value
	}
	if m.retired.Load() {
		return nil, time.Time{}, "source_unavailable"
	}
	return &d, o.freshUntil, ""
}

// sampleNodeNetwork only copies the diagnostics and receiver caches. The final
// ownership check fences replacement while independently owned children are copied.
func (d *nodeDashboard) sampleNodeNetwork(now time.Time) (dashboard.NodeNetworkCapture, error) {
	if d == nil {
		return dashboard.NodeNetworkCapture{}, errors.New("source_unavailable")
	}
	d.mu.Lock()
	binding := dashboard.NodeNetworkBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt}
	if d.state.GenesisHash != nil {
		value := *d.state.GenesisHash
		binding.GenesisHash = &value
	}
	traffic, stopping := d.interfaces, d.networkRetired || d.state.Phase == "stopping"
	prewarm := d.prewarmHandover
	d.mu.Unlock()
	if stopping {
		return dashboard.NodeNetworkCapture{}, errors.New("source_unavailable")
	}
	v := dashboard.NodeNetworkDetail{SchemaVersion: 2, Family: "network", Scope: "node_network", Binding: binding, InterfacesReason: "not_provided"}
	var deadline time.Time
	if receiver, gate, err := d.sampleNetworkDetail(now); err == nil {
		v.Receiver, deadline = &receiver.Detail, receiver.FreshUntil
		if gate != nil {
			v.RepairGate = &dashboard.RepairGateDetail{SampledAt: gate.SampledAt, Armed: gate.Armed}
			if until := gate.SampledAt.Add(dashboard.NetworkDetailFreshFor); until.Before(deadline) {
				deadline = until
			}
		}
	} else {
		v.ReceiverReason = err.Error() // sampleNetworkDetail returns fixed categories.
	}
	if traffic != nil {
		interfaces, until, reason := traffic.capture(binding, time.Now())
		v.Interfaces, v.InterfacesReason = interfaces, reason
		if interfaces != nil && (deadline.IsZero() || until.Before(deadline)) {
			deadline = until
		}
	}
	if gossip, until := d.captureGossipOperations(binding, time.Now()); gossip != nil {
		v.Gossip = gossip
		if deadline.IsZero() || until.Before(deadline) {
			deadline = until
		}
	}
	if socket, until := d.captureTPUSocket(binding, time.Now()); socket != nil {
		v.TPU = socket
		if deadline.IsZero() || until.Before(deadline) {
			deadline = until
		}
	}
	if payload, until := d.captureTPUPayload(binding, time.Now()); payload != nil {
		v.TPUPayload = payload
		if deadline.IsZero() || until.Before(deadline) {
			deadline = until
		}
	}
	if queues, until := d.captureTPUQueues(binding, time.Now()); queues != nil {
		v.TPUQueues = queues
		if deadline.IsZero() || until.Before(deadline) {
			deadline = until
		}
	}
	if socket, until := d.captureVotorSocket(binding, time.Now()); socket != nil {
		v.Votor = socket
		if deadline.IsZero() || until.Before(deadline) {
			deadline = until
		}
	}
	if payload, until := d.captureVotorPayload(binding, time.Now()); payload != nil {
		v.VotorPayload = payload
		if deadline.IsZero() || until.Before(deadline) {
			deadline = until
		}
	}
	if clock, until := d.captureLiveClock(binding); clock != nil {
		v.LiveClock = clock
		if deadline.IsZero() || until.Before(deadline) {
			deadline = until
		}
	}
	v.PrewarmHandover = prewarm.capture(binding)
	if v.PrewarmHandover != nil && deadline.IsZero() {
		// Revalidate the current node owner, not the age of the stopped receiver.
		deadline = time.Now().Add(dashboard.NetworkDetailFreshFor)
	}
	if v.Receiver == nil && v.Interfaces == nil && v.Gossip == nil && v.TPU == nil && v.TPUPayload == nil && v.TPUQueues == nil && v.Votor == nil && v.VotorPayload == nil && v.LiveClock == nil && v.PrewarmHandover == nil {
		reason := v.InterfacesReason
		if reason == "not_provided" {
			reason = v.ReceiverReason
		}
		return dashboard.NodeNetworkCapture{}, errors.New(reason)
	}
	v.CapturedAt = time.Now().UTC()
	d.mu.Lock()
	unchanged := d.state.RunID == binding.RunID && d.state.StartedAt.Equal(binding.NodeStartedAt) &&
		sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash) && d.interfaces == traffic &&
		d.prewarmHandover == prewarm && !d.networkRetired && d.state.Phase != "stopping" &&
		(v.Gossip == nil || d.gossipClient != nil && d.gossipGeneration == v.Gossip.ClientGeneration) &&
		(v.TPU == nil || d.tpuSocket != nil && d.tpuSocket.service == d.tpu && d.tpuSocket.generation == v.TPU.Generation) &&
		(v.TPUPayload == nil || d.tpuSocket != nil && d.tpuSocket.service == d.tpu && d.tpuSocket.generation == v.TPUPayload.Generation) &&
		(v.TPUQueues == nil || d.tpuSocket != nil && d.tpuSocket.service == d.tpu && d.tpuSocket.generation == v.TPUQueues.Generation) &&
		(v.Votor == nil || d.votorSocket != nil && d.votorSocket.engine == d.consensus && d.votorSocket.generation == v.Votor.Generation) &&
		(v.VotorPayload == nil || d.votorSocket != nil && d.votorSocket.engine == d.consensus && d.votorSocket.generation == v.VotorPayload.Generation) &&
		(v.LiveClock == nil || d.votorSocket != nil && d.votorSocket.engine == d.consensus && d.votorSocket.generation == v.LiveClock.Generation)
	d.mu.Unlock()
	if !unchanged {
		return dashboard.NodeNetworkCapture{}, errors.New("binding_changed")
	}
	return dashboard.NodeNetworkCapture{Detail: v, FreshUntil: deadline}, nil
}
