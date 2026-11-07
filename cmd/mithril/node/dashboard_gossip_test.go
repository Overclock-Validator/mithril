package node

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

func TestDashboardGossipOperationsMappingAndAttachment(t *testing.T) {
	at := time.Now()
	s := gossip.MonitoringSnapshot{StartedAt: at.Add(-time.Minute).UTC(), SampledAt: at, Stats: gossip.Stats{
		RxPings: math.MaxUint64, RxPongs: math.MaxUint64, RxContacts: math.MaxUint64, RxPullRequests: math.MaxUint64,
		TxPushMessages: math.MaxUint64, TxPingMessages: math.MaxUint64, TxPongMessages: math.MaxUint64, TxPullResponses: math.MaxUint64}}
	v := gossipOperationMeasurement(s, 1)
	if v == nil || v.Scope != "gossip_client_totals" || v.ClientGeneration != 1 || v.SampledAt != at.UTC() ||
		v.RxPings != math.MaxUint64 || v.RxPongs != math.MaxUint64 || v.RxContacts != math.MaxUint64 || v.RxPullRequests != math.MaxUint64 ||
		v.TxPushMessages != math.MaxUint64 || v.TxPingMessages != math.MaxUint64 || v.TxPongMessages != math.MaxUint64 || v.TxPullResponses != math.MaxUint64 {
		t.Fatal("native operation totals or original dates changed")
	}
	if gossipOperationMeasurement(s, 0) != nil || gossipOperationMeasurement(gossip.MonitoringSnapshot{}, 1) != nil {
		t.Fatal("unknown source became zero counters")
	}
	if v.LocalShredVersion != nil {
		t.Fatal("older source fabricated a local shred version")
	}
	if v.TxPullRequests == nil || *v.TxPullRequests != 0 {
		t.Fatal("known zero pull-request count omitted")
	}
	s.Stats.TxPullRequests = math.MaxUint64 - 1
	exact := gossipOperationMeasurement(s, 1)
	if exact.TxPullRequests == nil || *exact.TxPullRequests != math.MaxUint64-1 {
		t.Fatal("pull-request count lost precision")
	}
	*exact.TxPullRequests = 0
	if s.Stats.TxPullRequests != math.MaxUint64-1 {
		t.Fatal("projection aliases source")
	}
	s.Stats.TxPullRequests = math.MaxUint64
	if gossipOperationMeasurement(s, 1).TxPullRequests != nil {
		t.Fatal("saturated count presented as known")
	}
	s.LocalShredVersion, s.HasLocalShredVersion = math.MaxUint16, true
	version := gossipOperationMeasurement(s, 1)
	if version.LocalShredVersion == nil || *version.LocalShredVersion != math.MaxUint16 {
		t.Fatal("maximum local shred version was omitted or changed")
	}
	*version.LocalShredVersion = 0
	if s.LocalShredVersion != math.MaxUint16 {
		t.Fatal("projection aliases the cached local version")
	}
	var absent *nodeDashboard
	if absent.gossipObserver() != nil {
		t.Fatal("nil dashboard enabled an observer")
	}
	absent.observeGossipClient(nil)
	first, second := &gossip.Client{}, &gossip.Client{}
	d := &nodeDashboard{}
	d.observeGossipClient(first)
	if d.gossipClient != nil || d.gossipGeneration != 0 || d.gossipObserver() != nil {
		t.Fatal("default-off dashboard attached a client")
	}
	d.networkDetails = true
	d.observeGossipClient(first)
	d.observeGossipClient(first)
	d.observeGossipClient(nil)
	if d.gossipClient != first || d.gossipGeneration != 1 {
		t.Fatal("same owner or nil cleanup changed the counter lifetime")
	}
	d.observeGossipClient(second)
	if d.gossipClient != second || d.gossipGeneration != 2 {
		t.Fatal("replacement did not create a new counter lifetime")
	}
	d.stopping()
	d.observeGossipClient(first)
	if d.gossipClient != nil || d.gossipGeneration != 2 {
		t.Fatal("late attachment survived node retirement")
	}
	d = &nodeDashboard{networkDetails: true, gossipGeneration: math.MaxUint64}
	d.observeGossipClient(first)
	if d.gossipClient != nil || d.gossipGeneration != math.MaxUint64 {
		t.Fatal("generation exhaustion wrapped or reused an owner label")
	}
}

func TestDashboardGossipOperationsActualClientBeforeReplay(t *testing.T) {
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	config := gossip.Config{Entrypoint: peer.LocalAddr().String(), BindAddr: "127.0.0.1:0", TVUAddr: peer.LocalAddr().String(), AdvertisedIP: "127.0.0.1", ShredVersion: 42}
	client, err := gossip.NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	d := &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{RunID: "gossip-run", StartedAt: time.Now().Add(-time.Minute).UTC()}}
	d.observeGossipClient(client)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("loopback client shutdown: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("loopback client did not stop")
		}
	})
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	var sample gossip.MonitoringSnapshot
	for {
		if value, known := client.MonitoringSnapshot(); known {
			sample = value
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("real diagnostic cycle did not publish")
		case <-poll.C:
		}
	}
	if sample.Stats.TxPushMessages == 0 {
		t.Fatal("actual locally sent contact push was not observed")
	}
	if !sample.HasLocalShredVersion || sample.LocalShredVersion != 42 {
		t.Fatal("real client diagnostic did not retain its effective local shred version")
	}
	capture, err := d.sampleNodeNetwork(time.Now())
	if err != nil || capture.Detail.Receiver != nil || capture.Detail.Interfaces != nil || capture.Detail.Gossip == nil ||
		capture.Detail.Gossip.ClientGeneration != 1 || capture.Detail.Gossip.TxPushMessages != sample.Stats.TxPushMessages ||
		capture.Detail.Gossip.Socket == nil || capture.Detail.Gossip.Socket.Generation != sample.SocketGeneration ||
		capture.Detail.Gossip.LocalShredVersion == nil || *capture.Detail.Gossip.LocalShredVersion != 42 ||
		!sample.PayloadObserved || capture.Detail.Gossip.Payload == nil || capture.Detail.Gossip.Payload.SocketGeneration != sample.SocketGeneration ||
		capture.Detail.Gossip.Payload.SentBytes == nil || *capture.Detail.Gossip.Payload.SentBytes != sample.Payload.SentBytes || sample.Payload.SentBytes == 0 ||
		!sample.DecodeObserved || capture.Detail.Gossip.Decode == nil || capture.Detail.Gossip.Decode.SocketGeneration != sample.SocketGeneration ||
		!capture.Detail.Gossip.SampledAt.Equal(sample.SampledAt) || capture.Detail.Gossip.Decode.WireKinds[2] == nil ||
		capture.FreshUntil != sample.SampledAt.Add(dashboard.NetworkDetailFreshFor) {
		t.Fatal("pre-replay Gossip projection lost its owner, raw count or original lease")
	}
	binding := dashboard.NodeNetworkBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt}
	if v, _ := d.captureGossipOperations(binding, sample.SampledAt.Add(dashboard.NetworkDetailFreshFor)); v != nil {
		t.Fatal("expired Gossip capture remained available")
	}
	replacement, err := gossip.NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	d.observeGossipClient(replacement)
	if v, _ := d.captureGossipOperations(binding, time.Now()); v != nil {
		t.Fatal("unsampled replacement inherited the previous client's counters")
	}
	if d.gossipGeneration != 2 {
		t.Fatal("replacement client did not advance its independent generation")
	}
}
