package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestNetworkSplitPrivateFeedAndReader(t *testing.T) {
	now := time.Now()
	snapshot, capture := networkDetailFixture(now)
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot },
		DetailSamplers{Network: func(time.Time) (NetworkDetailCapture, error) { return capture, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	reader := networkDetailTestReader(t, path, snapshot, now)
	reader.collect(context.Background())
	if reply := reader.lookup(time.Now()); reply.status != http.StatusOK || reply.body == "" || reply.remainingMilliseconds <= 0 {
		t.Fatalf("fresh network feed: status=%d, remaining=%d", reply.status, reply.remainingMilliseconds)
	}
	feed.Close()
}

func TestNetworkSplitPublicRoute(t *testing.T) {
	now := time.Now()
	snapshot, capture := networkDetailFixture(now)
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot },
		DetailSamplers{Network: func(time.Time) (NetworkDetailCapture, error) { return capture, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, NetworkDetails: true})
	t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, _, headers := replayDetailPublicRequest(t, address, "/api/v1/details/network", "stats.example", "")
		if status == http.StatusOK && headers.Get(statusRemainingHeader) != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("network route did not become fresh: status=%d", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	status, _, _ := replayDetailPublicRequest(t, address, "/api/v1/details/network", "other.example", "")
	if status != http.StatusForbidden {
		t.Fatalf("wrong host: got %d, want %d", status, http.StatusForbidden)
	}
	for _, tc := range []struct {
		path     string
		advanced bool
	}{
		{"/api/v1/features", false},
		{"/api/v1/features?" + advancedProfileQuery, true},
	} {
		status, body, _ := replayDetailPublicRequest(t, address, tc.path, "stats.example", "")
		var result map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &result) != nil {
			t.Fatalf("feature discovery %s: status=%d", tc.path, status)
		}
		value, present := result["network_details"]
		if present != tc.advanced || present && value != true {
			t.Fatalf("feature discovery %s: network_details=%v, present=%t", tc.path, value, present)
		}
	}
	status, _, _ = replayDetailPublicRequest(t, address, "/api/v1/status?"+advancedProfileQuery, "stats.example", "")
	if status != http.StatusOK || snapshotProfile(advancedProfileQuery) != 4 {
		t.Fatalf("advanced status profile: status=%d", status)
	}
	dialer := websocket.Dialer{HandshakeTimeout: time.Second, NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(address, "http://"))
	}}
	conn, _, err := dialer.Dial("ws://stats.example/websocket?"+advancedProfileQuery, http.Header{"Origin": {"https://stats.example"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	_, frame, err := conn.ReadMessage()
	if err != nil || !json.Valid(frame) {
		t.Fatalf("advanced websocket frame: %v", err)
	}
}

func TestNetworkSplitRequiresPublicResources(t *testing.T) {
	for _, tc := range []struct {
		origin    string
		resources bool
	}{
		{"", true},
		{"https://stats.example", false},
	} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		err = serveProfile(context.Background(), "/unused", listener, tc.origin, PublicOptions{Resources: tc.resources, NetworkDetails: true})
		if err == nil {
			t.Fatalf("network details accepted origin=%q resources=%t", tc.origin, tc.resources)
		}
	}
}

func networkDetailFixture(now time.Time) (Snapshot, NetworkDetailCapture) {
	start := now.Add(-time.Hour).UTC()
	at := now.UTC()
	m := SocketMemory{ReceiveAllocatedBytes: math.MaxUint32, ReceiveLimitBytes: 1, SendAllocatedBytes: 2, SendLimitBytes: 3,
		ForwardAllocatedBytes: 4, SendQueuedBytes: 5, OptionsAllocatedBytes: 6, BacklogBytes: 7, Drops: 8}
	w := SocketDropWindow{StartedAt: now.Add(-time.Second).UTC(), SampledAt: at, DurationMilliseconds: 1000, Drops: 8}
	d := NetworkDetail{SchemaVersion: 1, Family: "network", Scope: "turbine_receiver_session", State: "fresh",
		Binding: NetworkDetailBinding{RunID: "network-run", NodeStartedAt: start, SourceStartedAt: start, ReceiverStartedAt: start,
			ReceiverSession: math.MaxUint64, SocketGeneration: math.MaxUint64}, UpdatedAt: at,
		Live:   SocketDetail{Enabled: true, State: "available", StartedAt: &at, SampledAt: &at, Memory: &m, DropWindow: &w},
		Repair: SocketDetail{Enabled: true, State: "available", StartedAt: &at, SampledAt: &at, Memory: &SocketMemory{}}}
	s := testSnapshot(at)
	s.RunID, s.StartedAt, s.GenesisHash = d.Binding.RunID, start, nil
	s.BlockSource = &BlockSource{SampledAt: at, StartedAt: start, ConfiguredSource: "turbine", LiveConnected: true,
		Turbine: &Turbine{SampledAt: at, StartedAt: start, Session: math.MaxUint64}}
	return s, NetworkDetailCapture{Detail: d, FreshUntil: now.Add(NetworkDetailFreshFor)}
}

func networkDetailTestReader(t *testing.T, path string, snapshot Snapshot, now time.Time) *networkDetailReader {
	t.Helper()
	var gate atomic.Pointer[networkDetailGate]
	g := networkGate(snapshot, now)
	g.stamp, g.nodeStamp = statusStamp{1, 1}, statusStamp{1, 1}
	gate.Store(&g)
	reader, err := newNetworkDetailReader(newCollector(path).client, &gate)
	if err != nil {
		t.Fatal(err)
	}
	reader.admit = func(networkDetailReply) (time.Duration, bool) { return time.Second, true }
	t.Cleanup(reader.client.CloseIdleConnections)
	return reader
}

func maximumInterfaceTraffic(now time.Time) *InterfaceTrafficDetail {
	total := uint32(256)
	d := &InterfaceTrafficDetail{Scope: "visible_interface_counters", State: "available", CaptureStartedAt: now.UTC(), SampledAt: now.UTC(),
		TotalInterfaces: &total, Interfaces: make([]InterfaceTrafficCounter, 8)}
	for i := range d.Interfaces {
		d.Interfaces[i] = InterfaceTrafficCounter{Index: math.MaxUint32 - 8 + uint32(i), ReceiveBytes: math.MaxUint64, TransmitBytes: math.MaxUint64,
			ReceivePackets: math.MaxUint64, TransmitPackets: math.MaxUint64,
			Failures: &InterfaceTrafficFailures{ReceiveErrors: math.MaxUint64, ReceiveDropped: math.MaxUint64, TransmitErrors: math.MaxUint64, TransmitDropped: math.MaxUint64}}
	}
	return d
}

func nodeNetworkSourceFixture(now time.Time) (Snapshot, NodeNetworkCapture) {
	s, receiver := networkDetailFixture(now)
	d := NodeNetworkDetail{SchemaVersion: 2, Family: "network", Scope: "node_network", CapturedAt: now.UTC(),
		Binding:  NodeNetworkBinding{RunID: s.RunID, NodeStartedAt: s.StartedAt, GenesisHash: s.GenesisHash},
		Receiver: &receiver.Detail, Interfaces: maximumInterfaceTraffic(now)}
	return s, NodeNetworkCapture{Detail: d, FreshUntil: receiver.FreshUntil}
}
