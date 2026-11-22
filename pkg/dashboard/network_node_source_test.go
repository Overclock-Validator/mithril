package dashboard

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func nodeNetworkSourceFixture(now time.Time) (Snapshot, NodeNetworkCapture) {
	s, receiver := networkDetailFixture(now)
	d := NodeNetworkDetail{SchemaVersion: 2, Family: "network", Scope: "node_network", CapturedAt: now.UTC(),
		Binding:  NodeNetworkBinding{RunID: s.RunID, NodeStartedAt: s.StartedAt, GenesisHash: s.GenesisHash},
		Receiver: &receiver.Detail, Interfaces: maximumInterfaceTraffic(now)}
	return s, NodeNetworkCapture{Detail: d, FreshUntil: receiver.FreshUntil}
}

func TestNodeNetworkPullRequestCounterOptIn(t *testing.T) {
	now := time.Now()
	snapshot, capture := nodeNetworkSourceFixture(now)
	capture.Detail.Interfaces.Interfaces[0].MTU = 1500
	capture.Detail.RepairGate = &RepairGateDetail{SampledAt: now.UTC(), Armed: true}
	count := uint64(7)
	capture.Detail.Gossip = &GossipOperationDetail{Scope: "gossip_client_totals", ClientGeneration: 1,
		StartedAt: snapshot.StartedAt, SampledAt: now.UTC(), TxPullRequests: &count}
	source := &networkDetailSource{nodeSample: func(time.Time) (NodeNetworkCapture, error) { return capture, nil }}
	source.update(snapshot, now)
	read := func(optIn bool) NodeNetworkDetail {
		req := httptest.NewRequest("GET", "/details/network", nil)
		if optIn {
			req.Header.Set(networkPullRequestsHeader, "1")
		}
		w := httptest.NewRecorder()
		source.serveHTTP(w, req)
		d, err := decodeNodeNetworkDetail(w.Body.Bytes())
		if w.Code != 200 || err != nil || d.Gossip == nil {
			t.Fatalf("invalid capture: status=%d err=%v", w.Code, err)
		}
		if d.Interfaces.Interfaces[0].MTU != 0 {
			t.Fatal("unnegotiated MTU escaped into a strict-reader response")
		}
		if d.RepairGate != nil {
			t.Fatal("unnegotiated repair gate escaped")
		}
		return d
	}
	if d := read(false); d.Gossip.TxPullRequests != nil {
		t.Fatal("legacy response changed")
	}
	if d := read(true); d.Gossip.TxPullRequests == nil || *d.Gossip.TxPullRequests != count {
		t.Fatal("opt-in omitted the exact counter")
	}
	readMTU := func() NodeNetworkDetail {
		req := httptest.NewRequest("GET", "/details/network", nil)
		req.Header.Set(networkPullRequestsHeader, "1")
		req.Header.Set(networkMTUHeader, "1")
		w := httptest.NewRecorder()
		source.serveHTTP(w, req)
		d, err := decodeNodeNetworkDetail(w.Body.Bytes())
		if w.Code != 200 || err != nil || d.Interfaces == nil || d.Interfaces.Interfaces[0].MTU != 1500 || d.Gossip == nil || d.Gossip.TxPullRequests == nil || d.RepairGate != nil {
			t.Fatalf("negotiated capture lost MTU or pull counter: status=%d err=%v", w.Code, err)
		}
		return d
	}
	readMTU()
	readGate := func() NodeNetworkDetail {
		req := httptest.NewRequest("GET", "/details/network", nil)
		req.Header.Set(networkPullRequestsHeader, "1")
		req.Header.Set(networkMTUHeader, "1")
		req.Header.Set(networkRepairGateHeader, "1")
		w := httptest.NewRecorder()
		source.serveHTTP(w, req)
		d, err := decodeNodeNetworkDetail(w.Body.Bytes())
		if w.Code != 200 || err != nil {
			t.Fatalf("gate response: %d %v", w.Code, err)
		}
		return d
	}
	if d := readGate(); d.RepairGate == nil || !d.RepairGate.Armed {
		t.Fatal("negotiated gate missing")
	}
	lost := snapshot
	lost.BlockSource = nil
	g := networkGate(lost, time.Now())
	source.gate.Store(&g)
	if d := read(false); d.Receiver != nil || d.Gossip.TxPullRequests != nil {
		t.Fatal("legacy receiver-loss response changed")
	}
	if d := read(true); d.Receiver != nil || d.Gossip.TxPullRequests == nil || *d.Gossip.TxPullRequests != count {
		t.Fatal("receiver loss changed extended counter")
	}
	if d := readMTU(); d.Receiver != nil {
		t.Fatal("receiver loss retained receiver")
	}
	if d := readGate(); d.Receiver != nil || d.RepairGate != nil {
		t.Fatal("receiver loss retained repair gate")
	}
	if capture.Detail.Interfaces.Interfaces[0].MTU != 1500 || capture.Detail.Gossip.TxPullRequests == nil || *capture.Detail.Gossip.TxPullRequests != count {
		t.Fatal("legacy projection mutated source")
	}
}

func TestNodeNetworkPrivatePayloadLease(t *testing.T) {
	now := time.Now()
	snapshot, capture := nodeNetworkSourceFixture(now)
	count := uint64(4)
	counter := &ApplicationPayloadCounter{StartedAt: snapshot.StartedAt, SampledAt: now.UTC(), Bytes: &count}
	capture.Detail.Receiver.RepairRequests = counter
	capture.Detail.VotorPayload = &VotorPayloadDetail{Scope: "votor_instance_application_payload", Generation: 1, Received: counter}
	source := &networkDetailSource{nodeSample: func(time.Time) (NodeNetworkCapture, error) { return capture, nil }}
	source.update(snapshot, now)
	if got := source.latest.Load(); got.reason != "" || got.body == "" {
		t.Fatalf("valid payload lease rejected: %+v", got)
	}
	for _, owner := range []string{"repair", "votor"} {
		stale := *counter
		stale.SampledAt = now.Add(-NetworkDetailFreshFor).UTC()
		if owner == "repair" {
			capture.Detail.Receiver.RepairRequests = &stale
		} else {
			capture.Detail.Receiver.RepairRequests = counter
			capture.Detail.VotorPayload.Received = &stale
		}
		source.update(snapshot, now)
		if got := source.latest.Load(); got.reason != "source_time_invalid" || got.body != "" {
			t.Fatalf("expired %s payload escaped: %+v", owner, got)
		}
	}
}

func TestNodeNetworkPrivateFeedWithoutReceiver(t *testing.T) {
	now := time.Now()
	snapshot, capture := nodeNetworkSourceFixture(now)
	snapshot.BlockSource = nil
	capture.Detail.Receiver, capture.Detail.ReceiverReason = nil, "waiting_source"
	var samples, statuses atomic.Int64
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot {
		statuses.Add(1)
		return snapshot
	}, DetailSamplers{NodeNetwork: func(time.Time) (NodeNetworkCapture, error) { samples.Add(1); return capture, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	client := newCollector(path).client
	t.Cleanup(client.CloseIdleConnections)
	for range 10 {
		status, body, headers := replayDetailHTTP(t, client, "GET", "http://node/details/network", "")
		d, err := decodeNodeNetworkDetail([]byte(body))
		if status != 200 || err != nil || d.Receiver != nil || d.Interfaces == nil || d.Interfaces.Interfaces[0].ReceiveBytes != ^uint64(0) || headers.Get(replayDetailRemainingHeader) == "" {
			t.Fatalf("interface-only Unix feed: status=%d error=%v", status, err)
		}
	}
	feed.Close()
	if samples.Load() != statuses.Load() {
		t.Fatal("viewer caused source sampling or another worker")
	}
	_, err = ListenFeedWithDetails(context.Background(), socketPath(t), "", func() Snapshot { return snapshot }, DetailSamplers{
		Network:     func(time.Time) (NetworkDetailCapture, error) { return NetworkDetailCapture{}, nil },
		NodeNetwork: func(time.Time) (NodeNetworkCapture, error) { return capture, nil },
	})
	if err == nil {
		t.Fatal("two samplers competed for one network route")
	}
}

func TestNodeNetworkPrivateReceiverLossAndRunFence(t *testing.T) {
	now := time.Now()
	snapshot, capture := nodeNetworkSourceFixture(now)
	source := &networkDetailSource{nodeSample: func(time.Time) (NodeNetworkCapture, error) { return capture, nil }}
	read := func() (int, string) {
		w := httptest.NewRecorder()
		source.serveHTTP(w, httptest.NewRequest("GET", "/details/network", nil))
		return w.Code, w.Body.String()
	}
	source.update(snapshot, now)
	status, body := read()
	initial, err := decodeNodeNetworkDetail([]byte(body))
	if status != 200 || err != nil || initial.Receiver == nil || initial.Interfaces == nil {
		t.Fatal("initial child missing")
	}
	lost := snapshot
	lost.BlockSource = nil
	g := networkGate(lost, time.Now())
	// Exercise the gap between replacing the status gate and its publication.
	source.gate.Store(&g)
	status, body = read()
	cleared, err := decodeNodeNetworkDetail([]byte(body))
	if status != 200 || err != nil || cleared.Receiver != nil || cleared.ReceiverReason != "binding_changed" || cleared.Interfaces == nil ||
		cleared.Interfaces.Interfaces[0].ReceiveBytes != initial.Interfaces.Interfaces[0].ReceiveBytes || !cleared.Interfaces.SampledAt.Equal(initial.Interfaces.SampledAt) {
		t.Fatal("receiver loss retained foreign sockets, cleared host data or redated it")
	}
	deadline := source.latest.Load().freshUntil
	source.update(lost, time.Now())
	status, body = read()
	cleared, err = decodeNodeNetworkDetail([]byte(body))
	if status != 200 || err != nil || cleared.Receiver != nil || !source.latest.Load().freshUntil.Equal(deadline) {
		t.Fatal("copy renewed the source lease or rebound old receiver data")
	}
	g.binding.RunID = "new-node-run"
	source.gate.Store(&g)
	status, body = read()
	if status != 503 || !strings.Contains(body, "binding_changed") || strings.Contains(body, "receive_bytes") {
		t.Fatal("new run retained old interface counters")
	}
}
