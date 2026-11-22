package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/Overclock-Validator/mithril/pkg/tpu"
	"github.com/Overclock-Validator/mithril/pkg/tpu/pipeline"
)

func TestDashboardTPUQueuesIndependentSample(t *testing.T) {
	now := time.Now()
	service := &tpu.TPU{}
	d := &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{RunID: "tpu-queues", StartedAt: now.Add(-time.Minute).UTC()}}
	d.pipelineStarted(service, nil)
	binding := dashboard.NodeNetworkBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt}
	raw := pipeline.QueueObservation{StartedAt: now.Add(-time.Second), SampledAt: now,
		Ingress: pipeline.QueueGauge{Capacity: 2}, DedupOutput: pipeline.QueueGauge{Length: 3, Capacity: 3}, Verified: pipeline.QueueGauge{Length: 4, Capacity: 9}}
	c := tpuQueuesMeasurement(raw, true, binding, d.tpuSocket.generation, now.Add(-time.Millisecond), now)
	if c == nil || c.freshUntil.After(raw.SampledAt.Add(dashboard.NetworkDetailFreshFor)) {
		t.Fatal("queue sample lost original deadline")
	}
	d.tpuSocket.queuesLatest = c // No kernel socket or stream-payload capture.
	combined, err := d.sampleNodeNetwork(now)
	if err != nil || combined.Detail.TPUQueues == nil || combined.Detail.TPU != nil || combined.Detail.TPUPayload != nil ||
		combined.Detail.TPUQueues.DedupOutput.Length != 3 || combined.FreshUntil != c.freshUntil {
		t.Fatalf("independent queue capture: %+v %v", combined, err)
	}
	combined.Detail.TPUQueues.DedupOutput.Length = 0
	again, _ := d.captureTPUQueues(binding, now)
	if again == nil || again.DedupOutput.Length != 3 {
		t.Fatal("queue result aliases owner capture")
	}
	if expired, _ := d.captureTPUQueues(binding, c.freshUntil); expired != nil {
		t.Fatal("queue sample renewed at expiry")
	}
	for _, q := range []pipeline.QueueGauge{{Length: -1, Capacity: 3}, {Length: 4, Capacity: 3}, {Capacity: 0}} {
		bad := raw
		bad.Ingress = q
		if tpuQueuesMeasurement(bad, true, binding, 1, now, now) != nil {
			t.Fatal("invalid native queue became unsigned wire value")
		}
	}
	if tpuQueuesMeasurement(raw, false, binding, 1, now, now) != nil {
		t.Fatal("disabled queue became measured zero")
	}
	d.state.RunID = "replacement-run"
	if old, _ := d.captureTPUQueues(binding, now); old != nil {
		t.Fatal("old queue crossed run binding")
	}
	d.state.RunID = binding.RunID
	d.pipelineStarted(&tpu.TPU{}, nil)
	if old, _ := d.captureTPUQueues(binding, now); old != nil {
		t.Fatal("old queue crossed TPU replacement")
	}
	d.pipelineStopped()
	if old, _ := d.captureTPUQueues(binding, now); old != nil {
		t.Fatal("stopped TPU exposed live queues")
	}
}

func TestDashboardTPUQueuesActualOwner(t *testing.T) {
	holder, err := identity.NewEphemeralHolder()
	if err != nil {
		t.Fatal(err)
	}
	service, err := tpu.Start(context.Background(), tpu.Config{Name: "dashboard-queues", ListenAddr: "127.0.0.1:0", Identity: holder,
		Pipeline: pipeline.Config{ObserveQueues: true, SigverifyWorkers: 1, DedupCacheCapacity: 16, IngressCap: 2, DedupOutCap: 3, VerifiedCap: 4}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Stop(nil) })
	d := &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{RunID: "actual-queues", StartedAt: time.Now().Add(-time.Minute).UTC()}}
	d.pipelineStarted(service, nil)
	d.observeTPUSocket()
	binding := dashboard.NodeNetworkBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt}
	q, until := d.captureTPUQueues(binding, time.Now())
	if q == nil || q.Ingress != (dashboard.TPUQueueGauge{Capacity: 2}) || q.DedupOutput != (dashboard.TPUQueueGauge{Capacity: 3}) ||
		q.Verified != (dashboard.TPUQueueGauge{Capacity: 4}) || !until.After(time.Now()) {
		t.Fatalf("actual queue source: %+v", q)
	}
	d.pipelineStarted(service, nil)
	same, deadline := d.captureTPUQueues(binding, time.Now())
	if same == nil || same.SampledAt != q.SampledAt || deadline != until {
		t.Fatal("same attachment redated queue sample")
	}
}

func TestDashboardTPUSocketOwnerAndActualRead(t *testing.T) {
	holder, err := identity.NewEphemeralHolder()
	if err != nil {
		t.Fatal(err)
	}
	service, err := tpu.Start(context.Background(), tpu.Config{Name: "dashboard-tpu", ListenAddr: "127.0.0.1:0", Identity: holder})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Stop(nil) })
	disabled := &nodeDashboard{}
	disabled.pipelineStarted(service, nil)
	disabled.observeTPUSocket()
	if disabled.tpuSocket != nil || disabled.tpuSocketGeneration != 0 {
		t.Fatal("ordinary pipeline monitoring enabled kernel accounting")
	}
	d := &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{RunID: "tpu-run", StartedAt: time.Now().Add(-time.Minute).UTC()}}
	d.pipelineStarted(service, nil)
	if d.tpuSocket == nil || d.tpuSocket.latest != nil {
		t.Fatal("attachment fabricated an unsampled capture")
	}
	d.observeTPUSocket()
	binding := dashboard.NodeNetworkBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt}
	v, until := d.captureTPUSocket(binding, time.Now())
	if v == nil || v.Scope != "tpu_udp_socket" || v.Generation != 1 || v.SampledAt.IsZero() || !until.After(time.Now()) {
		t.Fatal("actual TPU socket did not publish its original owner scope/lease")
	}
	d.pipelineStarted(service, nil)
	if same, deadline := d.captureTPUSocket(binding, time.Now()); same == nil || same.Generation != 1 || deadline != until || same.SampledAt != v.SampledAt {
		t.Fatal("same instance attachment reset or redated socket evidence")
	}
	combined, err := d.sampleNodeNetwork(time.Now())
	if err != nil || combined.Detail.TPU == nil || combined.Detail.Receiver != nil || combined.Detail.Gossip != nil || combined.Detail.Interfaces != nil || combined.FreshUntil != until {
		t.Fatal("TPU-only capture depended on receiver, replay or host observations")
	}
	if expired, _ := d.captureTPUSocket(binding, until); expired != nil {
		t.Fatal("TPU capture outlived its original deadline")
	}
	owner := d.tpuSocket
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Swap(key); err != nil {
		t.Fatal(err)
	}
	if d.tpuSocket != owner || d.tpuSocket.generation != 1 {
		t.Fatal("identity swap changed the existing socket lifetime")
	}
	d.observeTPUSocket()
	current, _ := d.captureTPUSocket(binding, time.Now())
	if current == nil || current.Generation != 1 || current.Observation.State == "available" &&
		(current.Observation.DropWindow == nil || !current.Observation.DropWindow.StartedAt.Equal(*v.Observation.SampledAt) ||
			!current.Observation.DropWindow.SampledAt.Equal(*current.Observation.SampledAt)) {
		t.Fatal("same actual socket lost original drop endpoints across an identity swap")
	}
	d.pipelineStopped()
	if old, _ := d.captureTPUSocket(binding, time.Now()); old != nil {
		t.Fatal("detached TPU retained old accounting")
	}
	d.pipelineStarted(service, nil)
	if d.tpuSocket == nil || d.tpuSocket.generation != 2 || d.tpuSocket.latest != nil {
		t.Fatal("reattachment reused a retired capture lifetime")
	}
	d.tpuSocketGeneration = math.MaxUint64
	d.pipelineStopped()
	d.pipelineStarted(service, nil)
	if d.tpuSocket != nil || d.tpuSocketGeneration != math.MaxUint64 {
		t.Fatal("generation exhaustion wrapped or reused a label")
	}
}

func TestDashboardTPUSocketDetachedCopyAndBinding(t *testing.T) {
	now := time.Now()
	service := &tpu.TPU{}
	d := &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{RunID: "tpu-run", StartedAt: now.Add(-time.Minute).UTC()}}
	d.pipelineStarted(service, nil)
	binding := dashboard.NodeNetworkBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt}
	started, sampled := now.Add(-time.Millisecond).UTC(), now.UTC()
	d.tpuSocket.latest = &tpuSocketCapture{binding: binding, freshUntil: now.Add(time.Second), detail: dashboard.TPUSocketDetail{
		Scope: "tpu_udp_socket", Generation: 1, SampledAt: sampled,
		Observation: dashboard.SocketDetail{Enabled: true, State: "available", StartedAt: &started, SampledAt: &sampled, Memory: &dashboard.SocketMemory{Drops: math.MaxUint32},
			DropWindow: &dashboard.SocketDropWindow{StartedAt: now.Add(-time.Second).UTC(), SampledAt: sampled, DurationMilliseconds: 1000, Drops: 7}}}}
	value, _ := d.captureTPUSocket(binding, now)
	if value == nil || value.Observation.Memory.Drops != math.MaxUint32 {
		t.Fatal("bounded fixture lost an exact raw drop value")
	}
	value.Observation.Memory.Drops = 0
	value.Observation.DropWindow.Drops = 100
	*value.Observation.StartedAt = time.Time{}
	if current, _ := d.captureTPUSocket(binding, now); current == nil || current.Observation.Memory.Drops != math.MaxUint32 || current.Observation.StartedAt.IsZero() || current.Observation.DropWindow.Drops != 7 {
		t.Fatal("returned observation aliases the cached owner")
	}
	d.state.RunID = "another-run"
	if current, _ := d.captureTPUSocket(binding, now); current != nil {
		t.Fatal("TPU capture crossed the current node binding")
	}
	d.state.RunID = binding.RunID
	d.stopping()
	if current, _ := d.captureTPUSocket(binding, now); current != nil {
		t.Fatal("retired node retained socket evidence")
	}
}
