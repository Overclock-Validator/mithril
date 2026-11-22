package node

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func TestDashboardVotorSocketActualEngineAndLifetime(t *testing.T) {
	e, err := consensus.NewEngine(consensus.Config{AlpenglowObserverBindAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); _ = e.Close() })
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	disabled := &nodeDashboard{}
	disabled.consensusStarted(e)
	disabled.observeVotorSocket()
	if disabled.votorSocket != nil || disabled.votorSocketGeneration != 0 {
		t.Fatal("ordinary consensus monitoring enabled socket accounting")
	}
	withoutReceiver := &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{RunID: "no-votor", StartedAt: time.Now().Add(-time.Minute).UTC()}}
	withoutReceiver.consensusStarted(&consensus.AlpenglowObserverEngine{})
	withoutReceiver.observeVotorSocket()
	if withoutReceiver.votorSocket == nil || withoutReceiver.votorSocket.latest != nil {
		t.Fatal("unconfigured receiver fabricated a socket capture")
	}
	d := &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{RunID: "votor-run", StartedAt: time.Now().Add(-time.Minute).UTC()}}
	d.consensusStarted(e)
	if d.votorSocket == nil || d.votorSocket.latest != nil {
		t.Fatal("attachment fabricated an unsampled Votor capture")
	}
	d.observeVotorSocket()
	binding := dashboard.NodeNetworkBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt}
	if captured, _ := d.captureVotorSocket(binding, time.Now()); captured != nil {
		t.Fatal("unavailable receiver socket fabricated accounting")
	}
	d.consensusStarted(e)
	if d.votorSocket == nil || d.votorSocket.generation != 1 || d.votorSocket.latest != nil {
		t.Fatal("idempotent attachment fabricated a socket observation")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	d.observeVotorSocket()
	if closed, _ := d.captureVotorSocket(binding, time.Now()); closed != nil {
		t.Fatal("closed receiver retained a current socket observation")
	}
	d.consensusStarted(nil)
	d.votorSocketGeneration = math.MaxUint64
	d.consensusStarted(e)
	if d.votorSocket != nil || d.votorSocketGeneration != math.MaxUint64 {
		t.Fatal("Votor generation exhaustion wrapped or reused an old lifetime")
	}
}

func TestDashboardVotorPayloadIndependentCounterLeaseAndDetachment(t *testing.T) {
	now := time.Now()
	d := &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{RunID: "payload-copy", StartedAt: now.Add(-time.Minute).UTC()}}
	d.consensusStarted(&consensus.AlpenglowObserverEngine{})
	binding := d.votorSocket.binding
	o := consensus.VotorApplicationPayloadObservation{HasSendAccepted: true, SendAccepted: alpenglow.ApplicationPayloadObservation{
		StartedAt: now.Add(-20 * time.Second), SampledAt: now.Add(-10 * time.Second), Known: true, Bytes: 9007199254740993}}
	c := votorPayloadMeasurement(o, binding, 1, now, now)
	if c == nil || c.detail.Received != nil || c.detail.SendAccepted == nil || c.freshUntil != o.SendAccepted.SampledAt.Add(dashboard.NetworkDetailFreshFor) {
		t.Fatal("sender-only measurement fabricated RX or renewed the cached TX clock")
	}
	d.votorSocket.payloadLatest = c
	v, until := d.captureVotorPayload(binding, now)
	if v == nil || *v.SendAccepted.Bytes != o.SendAccepted.Bytes {
		t.Fatal("payload-only owner unavailable")
	}
	*v.SendAccepted.Bytes = 0
	v.SendAccepted.SampledAt = time.Time{}
	current, _ := d.captureVotorPayload(binding, now)
	if current == nil || *current.SendAccepted.Bytes != o.SendAccepted.Bytes || current.SendAccepted.SampledAt.IsZero() {
		t.Fatal("payload detail aliases its owner")
	}
	combined, err := d.sampleNodeNetwork(now)
	if err != nil || combined.Detail.VotorPayload == nil || combined.Detail.Votor != nil || combined.Detail.Receiver != nil || combined.FreshUntil != until {
		t.Fatal("payload availability depended on an inbound kernel socket or replay")
	}
	if v, _ := d.captureVotorPayload(binding, until); v != nil {
		t.Fatal("payload survived its original deadline")
	}
	o.SendAccepted.Known = false
	if c := votorPayloadMeasurement(o, binding, 1, now, now); c == nil || c.detail.SendAccepted.Bytes != nil {
		t.Fatal("saturation became a measured zero")
	}
	o.SendAccepted.Known = true
	for _, mutate := range []func(*consensus.VotorApplicationPayloadObservation){
		func(o *consensus.VotorApplicationPayloadObservation) {
			o.SendAccepted.SampledAt = now.Add(-31 * time.Second)
		},
		func(o *consensus.VotorApplicationPayloadObservation) { o.SendAccepted.SampledAt = now.Add(time.Second) },
		func(o *consensus.VotorApplicationPayloadObservation) {
			o.SendAccepted.StartedAt = binding.NodeStartedAt.Add(-time.Second)
		},
		func(o *consensus.VotorApplicationPayloadObservation) { o.SendAccepted.Bytes = math.MaxUint64 },
	} {
		bad := o
		mutate(&bad)
		if votorPayloadMeasurement(bad, binding, 1, now, now) != nil {
			t.Fatal("invalid raw counter remained available")
		}
	}
	if votorPayloadMeasurement(o, binding, 1, now, now.Add(2*time.Second)) != nil {
		t.Fatal("slow acquisition renewed payload counters")
	}
	d.consensusStarted(&consensus.AlpenglowObserverEngine{})
	if v, _ := d.captureVotorPayload(binding, now); v != nil {
		t.Fatal("replacement inherited old payload counters")
	}
}

func TestDashboardVotorPayloadActualPassiveReceiver(t *testing.T) {
	started := time.Now().Add(-time.Second).UTC()
	e, err := consensus.NewEngine(consensus.Config{AlpenglowObserverBindAddr: "127.0.0.1:0", VotorPayloadMonitoringEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); _ = e.Close() })
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	d := &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{RunID: "passive-payload", StartedAt: started}}
	d.consensusStarted(e)
	d.observeVotorSocket()
	binding := d.votorSocket.binding
	v, _ := d.captureVotorPayload(binding, time.Now())
	if v == nil || v.Received == nil || v.SendAccepted != nil || v.Received.Bytes == nil || *v.Received.Bytes != 0 {
		t.Fatal("actual passive receiver lost known zero or fabricated a broadcaster")
	}
	if socket, _ := d.captureVotorSocket(binding, time.Now()); socket != nil {
		t.Fatal("unavailable socket accounting appeared beside receiver payload")
	}
	d.observeVotorSocket()
	v, _ = d.captureVotorPayload(binding, time.Now())
	if v == nil || v.Received == nil || v.Received.Window == nil || v.Received.Window.Bytes != 0 || v.SendAccepted != nil {
		t.Fatal("actual passive receiver did not retain a measured zero interval")
	}
	v.Received.Window.Bytes = 1
	if again, _ := d.captureVotorPayload(binding, time.Now()); again == nil || again.Received.Window == nil || again.Received.Window.Bytes != 0 {
		t.Fatal("returned payload interval aliases the owner")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	d.observeVotorSocket()
	if v, _ := d.captureVotorPayload(binding, time.Now()); v != nil {
		t.Fatal("retired receiver remained available")
	}
}

func TestDashboardVotorSocketDetachedCopyAndBinding(t *testing.T) {
	now := time.Now()
	e := &consensus.AlpenglowObserverEngine{}
	d := &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{RunID: "votor-copy", StartedAt: now.Add(-time.Minute).UTC()}}
	d.consensusStarted(e)
	binding := d.votorSocket.binding
	started, sampled := now.Add(-time.Millisecond).UTC(), now.UTC()
	d.votorSocket.latest = &votorSocketCapture{binding: binding, freshUntil: now.Add(time.Second), detail: dashboard.VotorSocketDetail{
		Scope: "votor_inbound_udp_socket", Generation: 1, SampledAt: sampled, Observation: dashboard.SocketDetail{Enabled: true, State: "available",
			StartedAt: &started, SampledAt: &sampled, Memory: &dashboard.SocketMemory{Drops: 9},
			DropWindow: &dashboard.SocketDropWindow{StartedAt: now.Add(-time.Second).UTC(), SampledAt: sampled, DurationMilliseconds: 1000, Drops: 7}}}}
	v, _ := d.captureVotorSocket(binding, now)
	if v == nil {
		t.Fatal("control bounded Votor projection unavailable")
	}
	v.Observation.Memory.Drops, v.Observation.DropWindow.Drops = 0, 0
	*v.Observation.StartedAt = time.Time{}
	if current, _ := d.captureVotorSocket(binding, now); current == nil || current.Observation.Memory.Drops != 9 || current.Observation.DropWindow.Drops != 7 || current.Observation.StartedAt.IsZero() {
		t.Fatal("returned socket detail aliases its owner")
	}
	d.state.RunID = "new-run"
	d.observeVotorSocket()
	if foreign, _ := d.captureVotorSocket(binding, now); foreign != nil || d.votorSocket.latest.detail.SampledAt != sampled {
		t.Fatal("old engine source crossed a new node binding")
	}
	d.state.RunID = binding.RunID
	d.consensusStarted(&consensus.AlpenglowObserverEngine{})
	if d.votorSocket == nil || d.votorSocket.generation != 2 || d.votorSocket.latest != nil {
		t.Fatal("replacement engine inherited an old capture")
	}
	d.stopping()
	if d.votorSocket != nil {
		t.Fatal("node stopping retained the Votor source")
	}
}
