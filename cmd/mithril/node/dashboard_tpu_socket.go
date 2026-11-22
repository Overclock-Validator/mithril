package node

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/tpu"
	"github.com/Overclock-Validator/mithril/pkg/tpu/pipeline"
	"github.com/Overclock-Validator/mithril/pkg/tpu/quicserver"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

// tpuSocketOwner and its immutable capture are protected by nodeDashboard.mu.
// The service already owns its socket; this object neither creates nor closes it.
type tpuSocketOwner struct {
	service       *tpu.TPU
	generation    uint64
	latest        *tpuSocketCapture
	payloadLatest *tpuPayloadCapture
	queuesLatest  *tpuQueuesCapture
}

type tpuQueuesCapture struct {
	binding    dashboard.NodeNetworkBinding
	detail     dashboard.TPUQueuesDetail
	freshUntil time.Time
}

func tpuQueuesMeasurement(raw pipeline.QueueObservation, observed bool, binding dashboard.NodeNetworkBinding, generation uint64, started, completed time.Time) *tpuQueuesCapture {
	if !observed || generation == 0 || completed.Before(started) || completed.Sub(started) > time.Second || raw.StartedAt.IsZero() || raw.SampledAt.IsZero() ||
		raw.StartedAt.Before(binding.NodeStartedAt) || raw.SampledAt.Before(raw.StartedAt) || raw.SampledAt.After(completed) ||
		!raw.SampledAt.Add(dashboard.NetworkDetailFreshFor).After(completed) {
		return nil
	}
	for _, q := range [...]pipeline.QueueGauge{raw.Ingress, raw.DedupOutput, raw.Verified} {
		if q.Length < 0 || q.Capacity <= 0 || q.Length > q.Capacity {
			return nil
		}
	}
	until := raw.SampledAt.Add(dashboard.NetworkDetailFreshFor)
	if cap := started.Add(dashboard.NetworkDetailFreshFor); until.After(cap) {
		until = cap
	}
	return &tpuQueuesCapture{binding: binding, freshUntil: until, detail: dashboard.TPUQueuesDetail{
		Scope: "tpu_pipeline_queue_samples", Generation: generation, StartedAt: raw.StartedAt.UTC(), SampledAt: raw.SampledAt.UTC(),
		Ingress:     dashboard.TPUQueueGauge{Length: uint64(raw.Ingress.Length), Capacity: uint64(raw.Ingress.Capacity)},
		DedupOutput: dashboard.TPUQueueGauge{Length: uint64(raw.DedupOutput.Length), Capacity: uint64(raw.DedupOutput.Capacity)},
		Verified:    dashboard.TPUQueueGauge{Length: uint64(raw.Verified.Length), Capacity: uint64(raw.Verified.Capacity)}}}
}

type tpuPayloadCapture struct {
	binding    dashboard.NodeNetworkBinding
	detail     dashboard.TPUPayloadDetail
	freshUntil time.Time
	point      payloadSample
}

func tpuPayloadMeasurement(raw quicserver.StreamReadPayload, observed bool, binding dashboard.NodeNetworkBinding, generation uint64, started, completed time.Time) *tpuPayloadCapture {
	if !observed || completed.Before(started) || completed.Sub(started) > time.Second || raw.StartedAt.IsZero() || raw.SampledAt.IsZero() ||
		raw.StartedAt.Before(binding.NodeStartedAt) || raw.SampledAt.Before(raw.StartedAt) || raw.SampledAt.After(completed) ||
		!raw.SampledAt.Add(dashboard.NetworkDetailFreshFor).After(completed) || !raw.Known && raw.Bytes != 0 || raw.Known && raw.Bytes == ^uint64(0) {
		return nil
	}
	until := raw.SampledAt.Add(dashboard.NetworkDetailFreshFor)
	if cap := started.Add(dashboard.NetworkDetailFreshFor); until.After(cap) {
		until = cap
	}
	counter := &dashboard.ApplicationPayloadCounter{StartedAt: raw.StartedAt.UTC(), SampledAt: raw.SampledAt.UTC()}
	if raw.Known {
		value := raw.Bytes
		counter.Bytes = &value
	}
	return &tpuPayloadCapture{binding: binding, freshUntil: until,
		point:  payloadSample{startedAt: raw.StartedAt, sampledAt: raw.SampledAt, bytes: raw.Bytes, known: raw.Known},
		detail: dashboard.TPUPayloadDetail{Scope: "tpu_server_stream_read_prefix", Generation: generation, Received: counter}}
}

type tpuSocketCapture struct {
	raw        metrics.SocketObservation
	binding    dashboard.NodeNetworkBinding
	detail     dashboard.TPUSocketDetail
	freshUntil time.Time
}

func (d *nodeDashboard) observeTPUSocket() {
	if d == nil {
		return
	}
	d.mu.Lock()
	owner := d.tpuSocket
	if !d.networkDetails || d.networkRetired || d.state.Phase == "stopping" || owner == nil || owner.service != d.tpu {
		d.mu.Unlock()
		return
	}
	binding := dashboard.NodeNetworkBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt}
	if d.state.GenesisHash != nil {
		value := *d.state.GenesisHash
		binding.GenesisHash = &value
	}
	d.mu.Unlock()
	started := time.Now()
	o := owner.service.ObserveSocketMemory()
	payload, observedPayload := owner.service.ObserveStreamReadPayload()
	queues, observedQueues := owner.service.ObserveQueues()
	completed := time.Now()
	payloadCapture := tpuPayloadMeasurement(payload, observedPayload, binding, owner.generation, started, completed)
	queuesCapture := tpuQueuesMeasurement(queues, observedQueues, binding, owner.generation, started, completed)
	detail, until, err := socketDetailMeasurement(turbine.SocketMemorySample{Observation: o}, true, started, completed)
	if cap := started.Add(dashboard.NetworkDetailFreshFor); until.After(cap) {
		until = cap
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.tpuSocket != owner || d.tpu != owner.service || d.networkRetired || d.state.Phase == "stopping" ||
		d.state.RunID != binding.RunID || !d.state.StartedAt.Equal(binding.NodeStartedAt) || !sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash) {
		return
	}
	if payloadCapture != nil && owner.payloadLatest != nil {
		payloadCapture.detail.Received.Window = payloadWindow(owner.payloadLatest.point, payloadCapture.point, owner.payloadLatest.detail.Received.Window)
	}
	owner.payloadLatest = payloadCapture
	owner.queuesLatest = queuesCapture
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
	owner.latest = &tpuSocketCapture{raw: o, binding: binding, freshUntil: until,
		detail: dashboard.TPUSocketDetail{Scope: "tpu_udp_socket", Generation: owner.generation, SampledAt: completed.UTC(), Observation: detail}}
}

func (d *nodeDashboard) captureTPUQueues(binding dashboard.NodeNetworkBinding, now time.Time) (*dashboard.TPUQueuesDetail, time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	owner := d.tpuSocket
	if d.networkRetired || d.state.Phase == "stopping" || owner == nil || owner.service != d.tpu || owner.queuesLatest == nil ||
		d.state.RunID != binding.RunID || !d.state.StartedAt.Equal(binding.NodeStartedAt) || !sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash) {
		return nil, time.Time{}
	}
	c := owner.queuesLatest
	if c.binding.RunID != binding.RunID || !c.binding.NodeStartedAt.Equal(binding.NodeStartedAt) || !sameReplayDetailGenesis(c.binding.GenesisHash, binding.GenesisHash) ||
		!c.freshUntil.After(now) || now.Before(c.detail.SampledAt) {
		return nil, time.Time{}
	}
	detail := c.detail
	return &detail, c.freshUntil
}

func (d *nodeDashboard) captureTPUPayload(binding dashboard.NodeNetworkBinding, now time.Time) (*dashboard.TPUPayloadDetail, time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	owner := d.tpuSocket
	if d.networkRetired || d.state.Phase == "stopping" || owner == nil || owner.service != d.tpu || owner.payloadLatest == nil ||
		d.state.RunID != binding.RunID || !d.state.StartedAt.Equal(binding.NodeStartedAt) || !sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash) {
		return nil, time.Time{}
	}
	c := owner.payloadLatest
	if c.binding.RunID != binding.RunID || !c.binding.NodeStartedAt.Equal(binding.NodeStartedAt) || !sameReplayDetailGenesis(c.binding.GenesisHash, binding.GenesisHash) ||
		!c.freshUntil.After(now) || now.Before(c.detail.Received.SampledAt) {
		return nil, time.Time{}
	}
	detail := c.detail
	counter := *detail.Received
	if counter.Bytes != nil {
		value := *counter.Bytes
		counter.Bytes = &value
	}
	if counter.Window != nil {
		window := *counter.Window
		counter.Window = &window
	}
	detail.Received = &counter
	return &detail, c.freshUntil
}

func (d *nodeDashboard) captureTPUSocket(binding dashboard.NodeNetworkBinding, now time.Time) (*dashboard.TPUSocketDetail, time.Time) {
	d.mu.Lock()
	owner := d.tpuSocket
	if d.networkRetired || d.state.Phase == "stopping" || owner == nil || owner.service != d.tpu || owner.latest == nil {
		d.mu.Unlock()
		return nil, time.Time{}
	}
	c := owner.latest
	d.mu.Unlock()
	if c.binding.RunID != binding.RunID || !c.binding.NodeStartedAt.Equal(binding.NodeStartedAt) || !sameReplayDetailGenesis(c.binding.GenesisHash, binding.GenesisHash) ||
		now.Before(c.detail.SampledAt) || !c.freshUntil.After(now) {
		return nil, time.Time{}
	}
	value := c.detail
	for _, target := range []**time.Time{&value.Observation.StartedAt, &value.Observation.SampledAt} {
		if *target != nil {
			at := **target
			*target = &at
		}
	}
	if value.Observation.Memory != nil {
		memory := *value.Observation.Memory
		value.Observation.Memory = &memory
	}
	if value.Observation.DropWindow != nil {
		window := *value.Observation.DropWindow
		value.Observation.DropWindow = &window
	}
	d.mu.Lock()
	unchanged := d.tpuSocket == owner && d.tpu == owner.service && !d.networkRetired && d.state.Phase != "stopping" &&
		d.state.RunID == binding.RunID && d.state.StartedAt.Equal(binding.NodeStartedAt) && sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash)
	d.mu.Unlock()
	if !unchanged {
		return nil, time.Time{}
	}
	return &value, c.freshUntil
}
