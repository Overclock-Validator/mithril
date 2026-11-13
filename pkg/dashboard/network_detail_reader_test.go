package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

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

func networkDetailTestObserveStatus(t *testing.T, reader *networkDetailReader, snapshot Snapshot) networkDetailGate {
	t.Helper()
	previous := reader.gate.Load()
	if previous == nil || !previous.nodeStamp.valid() {
		t.Fatal("missing initial network status stamp")
	}
	c := &collector{last: &snapshot}
	c.networkGate.Store(previous)
	c.observeNetworkDetailStatus("fresh", "", time.Now(), previous.nodeStamp.nodeEpoch)
	current := c.networkGate.Load()
	if current == nil || current.nodeStamp != previous.nodeStamp || !current.stamp.valid() || current.stamp == previous.stamp {
		t.Fatal("receiver transition did not preserve the node stamp and advance the receiver stamp")
	}
	reader.gate.Store(current)
	return *current
}

func decodePublicNetworkDetail(t *testing.T, reply networkDetailReply) PublicNetworkDetail {
	t.Helper()
	var detail PublicNetworkDetail
	if reply.status != http.StatusOK || len(reply.body) > MaxNetworkDetailBytes || json.Unmarshal([]byte(reply.body), &detail) != nil {
		t.Fatalf("public network detail: status=%d body=%s", reply.status, reply.body)
	}
	return detail
}

func TestNetworkDetailReaderConstructor(t *testing.T) {
	var gate atomic.Pointer[networkDetailGate]
	for _, client := range []*http.Client{nil, {}, {Transport: (*http.Transport)(nil)}} {
		if _, err := newNetworkDetailReader(client, &gate); err == nil {
			t.Fatal("invalid client accepted")
		}
	}
	original := newCollector("/unused").client
	if _, err := newNetworkDetailReader(original, nil); err == nil {
		t.Fatal("nil gate accepted")
	}
	reader, err := newNetworkDetailReader(original, &gate)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.client.CloseIdleConnections()
	transport := reader.client.Transport.(*http.Transport)
	if transport == original.Transport || transport.Proxy != nil || transport.MaxResponseHeaderBytes != 4096 ||
		transport.MaxConnsPerHost != 1 || transport.MaxIdleConnsPerHost != 1 || transport.MaxIdleConns != 1 ||
		!transport.DisableKeepAlives || reader.client.Timeout != time.Second || reader.client.Jar != nil ||
		reader.client.CheckRedirect(nil, nil) != http.ErrUseLastResponse || original.Transport.(*http.Transport).MaxConnsPerHost != 0 ||
		reader.limit.Limit() != 16 || reader.limit.Burst() != 32 || networkDetailCollectInterval != 10*time.Second {
		t.Fatal("reader did not own the exact bounded transport")
	}
}

func TestNetworkDetailReaderRealFeedAndCachedViewer(t *testing.T) {
	now := time.Now()
	snapshot, capture := networkDetailFixture(now)
	var samples atomic.Int64
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{
		Network: func(time.Time) (NetworkDetailCapture, error) {
			samples.Add(1)
			return capture, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	reader := networkDetailTestReader(t, path, snapshot, time.Now())
	reader.collect(context.Background())
	reply := reader.lookup(time.Now())
	got := decodePublicNetworkDetail(t, reply)
	if got.State != "fresh" || got.Reason != "" || got.CollectorAt.IsZero() || got.Binding != capture.Detail.Binding ||
		!reflect.DeepEqual(got.Live, capture.Detail.Live) || !reflect.DeepEqual(got.Repair, capture.Detail.Repair) ||
		got.Live.Memory.ReceiveAllocatedBytes != ^uint32(0) || got.Live.Memory.Drops != 8 || got.Repair.Memory == nil || *got.Repair.Memory != (SocketMemory{}) ||
		reply.remainingMilliseconds < 1 || reply.remainingMilliseconds > 30000 {
		t.Fatalf("public document changed source fields: %+v", got)
	}
	family := reader.latest.Load()
	if family == nil {
		t.Fatal("reader did not install one atomic family")
	}
	total := 0
	for _, body := range family.bodies {
		if len(body) > MaxNetworkDetailBytes {
			t.Fatal("public variant exceeded body bound")
		}
		total += len(body)
	}
	if total > networkDetailFamilyBytes {
		t.Fatal("public variants exceeded family bound")
	}
	feed.Close()
	before := samples.Load()
	for range 20 {
		response := httptest.NewRecorder()
		reader.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/network", nil))
		remaining, err := parseNetworkDetailRemaining(response.Header().Get(replayDetailRemainingHeader))
		if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" || err != nil || remaining <= 0 {
			t.Fatal("cached public document unavailable")
		}
	}
	if samples.Load() != before {
		t.Fatal("public viewers invoked the owner sampler")
	}
	for _, path := range []string{"/api/v1/details/network?", "/api/v1/details/network?role=live", "/api/v1/details/%6eetwork", "/api/v1/details/network/"} {
		response := httptest.NewRecorder()
		reader.serveHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusBadRequest || len(response.Body.Bytes()) > 512 {
			t.Fatalf("strict path accepted: %s status=%d", path, response.Code)
		}
	}
	reader.limit = rate.NewLimiter(0, 0)
	response := httptest.NewRecorder()
	reader.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/network", nil))
	if response.Code != http.StatusTooManyRequests || !strings.Contains(response.Body.String(), `"reason":"rate_limited"`) {
		t.Fatal("public rate limit did not fail closed")
	}
}

func TestNetworkPayloadLeaseNeverExtendsOriginalCounter(t *testing.T) {
	now := time.Now()
	_, receiver := networkDetailFixture(now)
	count := uint64(1)
	counter := &ApplicationPayloadCounter{StartedAt: receiver.Detail.Binding.ReceiverStartedAt, SampledAt: now.UTC(), Bytes: &count}
	receiver.Detail.RepairRequests = counter
	if _, err := preparePublicNetworkDetail(receiver.Detail, now, now.Add(time.Second)); err != nil {
		t.Fatalf("valid original lease rejected: %v", err)
	}
	if _, err := preparePublicNetworkDetail(receiver.Detail, now, now.Add(NetworkDetailFreshFor+time.Nanosecond)); err == nil {
		t.Fatal("repair counter acquired a longer lease")
	}
	stale := *counter
	stale.SampledAt = now.Add(-NetworkDetailFreshFor).UTC()
	receiver.Detail.RepairRequests = &stale
	if _, err := preparePublicNetworkDetail(receiver.Detail, now, now.Add(time.Second)); err == nil {
		t.Fatal("expired repair counter appeared fresh")
	}
	for _, direction := range []string{"received", "sent"} {
		_, capture := nodeNetworkSourceFixture(now)
		capture.Detail.Receiver, capture.Detail.ReceiverReason = nil, "waiting_source"
		p := &VotorPayloadDetail{Scope: "votor_instance_application_payload", Generation: 1}
		if direction == "received" {
			p.Received = &stale
		} else {
			p.SendAccepted = &stale
		}
		capture.Detail.VotorPayload = p
		if _, err := preparePublicNodeNetwork(capture.Detail, now, now.Add(time.Second)); err == nil {
			t.Fatalf("expired Votor %s counter appeared fresh", direction)
		}
	}
}

func TestNetworkDetailReaderFailureReplacementAndContext(t *testing.T) {
	now := time.Now()
	snapshot, capture := networkDetailFixture(now)
	type sourceReply struct {
		status    int
		body      string
		remaining string
		change    func()
	}
	var current atomic.Pointer[sourceReply]
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		reply := current.Load()
		if reply.change != nil {
			reply.change()
		}
		if reply.remaining != "" {
			w.Header().Set(replayDetailRemainingHeader, reply.remaining)
		}
		w.WriteHeader(reply.status)
		_, _ = io.WriteString(w, reply.body)
	})
	body, _ := json.Marshal(capture.Detail)
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "30000"})
	reader := networkDetailTestReader(t, path, snapshot, now)
	reader.collect(context.Background())
	first := reader.latest.Load()
	if first == nil {
		t.Fatal("control source did not install")
	}
	firstDeadline := first.sourceFreshUntil
	failure := networkDetailFailure("source_unavailable")
	current.Store(&sourceReply{status: failure.status, body: failure.body})
	reader.collect(context.Background())
	stale := decodePublicNetworkDetail(t, reader.lookup(time.Now()))
	if reader.latest.Load() != first || stale.State != "stale" || stale.Reason != "source_unavailable" || stale.Live.Memory == nil {
		t.Fatal("collector failure replaced or kept old values fresh")
	}
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "30000"})
	reader.collect(context.Background())
	if reader.latest.Load() != first || reader.failure.Load() != nil || !reader.latest.Load().sourceFreshUntil.Equal(firstDeadline) {
		t.Fatal("an unchanged source capture renewed its collector time or deadline")
	}
	closed := capture.Detail
	closed.Binding.SocketGeneration = 1
	closed.UpdatedAt = time.Now().UTC()
	at := closed.UpdatedAt
	closed.Live = SocketDetail{Enabled: true, State: "closed", StartedAt: &at, SampledAt: &at}
	body, _ = json.Marshal(closed)
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "30000"})
	reader.collect(context.Background())
	replaced := decodePublicNetworkDetail(t, reader.lookup(time.Now()))
	if replaced.State != "fresh" || replaced.Binding.SocketGeneration != closed.Binding.SocketGeneration || replaced.Live.State != "closed" || replaced.Live.Memory != nil {
		t.Fatalf("current closed observation did not clear old memory: got=%+v failure=%+v", replaced, reader.failure.Load())
	}
	g := *reader.gate.Load()
	g.binding.RunID = "replacement-run"
	reader.gate.Store(&g)
	blocked := reader.lookup(time.Now())
	if blocked.status != http.StatusServiceUnavailable || !strings.Contains(blocked.body, `"reason":"binding_changed"`) || strings.Contains(blocked.body, "receive_allocated_bytes") {
		t.Fatal("new run inherited an old socket document")
	}
	g.binding.RunID = capture.Detail.Binding.RunID
	g.reason = "source_unavailable"
	reader.gate.Store(&g)
	blocked = reader.lookup(time.Now())
	if blocked.status != http.StatusServiceUnavailable || !strings.Contains(blocked.body, `"reason":"source_unavailable"`) {
		t.Fatal("current status failure did not dominate retained details")
	}
	g.reason = ""
	reader.gate.Store(&g)
	current.Store(&sourceReply{status: 200, body: string(body), remaining: "30000", change: func() {
		next := *reader.gate.Load()
		next.binding.ReceiverSession++
		reader.gate.Store(&next)
	}})
	before := reader.latest.Load()
	reader.collect(context.Background())
	if reader.latest.Load() != before || reader.failure.Load() == nil || reader.failure.Load().reason != "binding_changed" {
		t.Fatal("status change during transfer partly installed a document")
	}
}

func TestNetworkDetailReaderRequestStartAndCancellation(t *testing.T) {
	now := time.Now()
	snapshot, capture := networkDetailFixture(now)
	body, _ := json.Marshal(capture.Detail)
	var headerAt atomic.Pointer[time.Time]
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(80 * time.Millisecond)
		at := time.Now()
		headerAt.Store(&at)
		w.Header().Set(replayDetailRemainingHeader, "1000")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		time.Sleep(80 * time.Millisecond)
		_, _ = w.Write(body)
	})
	reader := networkDetailTestReader(t, path, snapshot, now)
	started := time.Now()
	reader.collect(context.Background())
	family := reader.latest.Load()
	if family == nil || headerAt.Load() == nil || family.sourceFreshUntil.Before(started.Add(time.Second)) ||
		!family.sourceFreshUntil.Before(headerAt.Load().Add(950*time.Millisecond)) || !family.publishedAt.After(*headerAt.Load()) {
		t.Fatal("response delay extended the source freshness deadline")
	}
	expiredReply := reader.lookup(family.sourceFreshUntil)
	expired := decodePublicNetworkDetail(t, expiredReply)
	if expired.State != "stale" || expired.Reason != "status_stale" || expiredReply.remainingMilliseconds != 0 {
		t.Fatal("exact source deadline remained fresh")
	}
	family.sourceFreshUntil = time.Now().Add(-time.Second)
	response := httptest.NewRecorder()
	reader.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/network", nil))
	if response.Code != http.StatusOK || response.Header().Get(replayDetailRemainingHeader) != "" || !strings.Contains(response.Body.String(), `"state":"stale"`) {
		t.Fatal("stale public document carried a freshness header")
	}
	for _, value := range []string{"", "0", "-1", "030000", "30001", "1.0"} {
		if _, err := parseNetworkDetailRemaining(value); err == nil {
			t.Fatalf("invalid remaining time accepted: %q", value)
		}
	}

	entered := make(chan struct{})
	var once sync.Once
	blockedPath := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set(replayDetailRemainingHeader, "30000")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		once.Do(func() { close(entered) })
		<-request.Context().Done()
	})
	blocked := networkDetailTestReader(t, blockedPath, snapshot, time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	go blocked.run(ctx)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("slow body was not reached")
	}
	cancel()
	select {
	case <-blocked.done:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not join the network worker")
	}
}

func TestNetworkDetailReaderRejectsBadTransfers(t *testing.T) {
	now := time.Now()
	snapshot, capture := networkDetailFixture(now)
	valid, _ := json.Marshal(capture.Detail)
	failure := networkDetailFailure("source_unavailable")
	for _, mode := range []string{"duplicate", "unknown", "missing", "oversize", "bad header", "duplicate header", "redirect", "wrong status", "error header"} {
		t.Run(mode, func(t *testing.T) {
			path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
				body := valid
				status := http.StatusOK
				switch mode {
				case "duplicate":
					body = []byte(strings.Replace(string(valid), `"family":"network"`, `"family":"network","family":"network"`, 1))
				case "unknown":
					body = []byte(strings.Replace(string(valid), `"family":"network"`, `"family":"network","unknown":0`, 1))
				case "missing":
					body = []byte(strings.Replace(string(valid), `"reason":"",`, "", 1))
				case "oversize":
					body = []byte(strings.Repeat(" ", MaxNetworkDetailBytes+1))
				case "bad header":
					w.Header().Set(replayDetailRemainingHeader, "030000")
				case "duplicate header":
					w.Header().Add(replayDetailRemainingHeader, "30000")
					w.Header().Add(replayDetailRemainingHeader, "30000")
				case "redirect":
					http.Redirect(w, request, "/snapshot", http.StatusFound)
					return
				case "wrong status":
					status, body = http.StatusInternalServerError, []byte(failure.body)
				case "error header":
					status, body = http.StatusServiceUnavailable, []byte(failure.body)
					w.Header().Set(replayDetailRemainingHeader, "30000")
				}
				if status == http.StatusOK && len(w.Header().Values(replayDetailRemainingHeader)) == 0 {
					w.Header().Set(replayDetailRemainingHeader, "30000")
				}
				w.WriteHeader(status)
				_, _ = w.Write(body)
			})
			reader := networkDetailTestReader(t, path, snapshot, now)
			reader.collect(context.Background())
			if reader.latest.Load() != nil || reader.failure.Load() == nil {
				t.Fatal("invalid transfer installed a partial document")
			}
			reply := reader.lookup(time.Now())
			if reply.status != http.StatusServiceUnavailable || strings.Contains(reply.body, "/snapshot") || len(reply.body) > 512 {
				t.Fatal("invalid transfer escaped raw input")
			}
		})
	}
	for _, reason := range []string{"invalid_request", "rate_limited", "waiting_source", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid", "unsupported_source", "binding_changed", "response_limit", "not_allowed"} {
		reply := networkDetailFailure(reason)
		if len(reply.body) > 512 || !json.Valid([]byte(reply.body)) || strings.Contains(reply.body, "not_allowed") {
			t.Fatalf("unbounded or raw public failure: %q", reply.body)
		}
	}
}

func TestNetworkDetailPublicOptInAndRealRoute(t *testing.T) {
	for _, tc := range []struct {
		origin string
		opts   PublicOptions
	}{
		{"", PublicOptions{NetworkDetails: true}},
		{"", PublicOptions{Resources: true, NetworkDetails: true}},
		{"https://stats.example", PublicOptions{NetworkDetails: true}},
	} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if err := serveProfile(context.Background(), "/unused.sock", listener, tc.origin, tc.opts); err == nil || !strings.Contains(err.Error(), "resource disclosure") {
			t.Fatalf("network disclosure accepted: origin=%q options=%+v error=%v", tc.origin, tc.opts, err)
		}
	}

	now := time.Now()
	snapshot, capture := networkDetailFixture(now)
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{
		Network: func(time.Time) (NetworkDetailCapture, error) { return capture, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, NetworkDetails: true})
	t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
	var public PublicNetworkDetail
	var publicHeader http.Header
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, body, header := replayDetailPublicRequest(t, address, "/api/v1/details/network", "stats.example", "")
		if status == http.StatusOK && json.Unmarshal(body, &public) == nil {
			publicHeader = header
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("public reader did not install complete document: %d %s", status, body)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !reflect.DeepEqual(public.Live, capture.Detail.Live) || !reflect.DeepEqual(public.Repair, capture.Detail.Repair) || public.Binding != capture.Detail.Binding {
		t.Fatal("public route lost source fields")
	}
	if _, err := parseNetworkDetailRemaining(publicHeader.Get(replayDetailRemainingHeader)); err != nil {
		t.Fatal("public route lost its conservative freshness handoff")
	}
	status, features, _ := replayDetailPublicRequest(t, address, "/api/v1/features?include=advanced_status", "stats.example", "")
	if status != http.StatusOK || !strings.Contains(string(features), `"network_details":true`) {
		t.Fatal("network feature discovery missing")
	}
	status, ordinary, _ := replayDetailPublicRequest(t, address, "/api/v1/status", "stats.example", "")
	if status != http.StatusOK || strings.Contains(string(ordinary), "socket_generation") || len(ordinary) > MaxSnapshotBytes {
		t.Fatal("network detail changed the status contract")
	}
	for _, request := range []struct {
		path, host, origin string
		want               int
	}{
		{"/api/v1/details/network", "attacker.example", "", 403},
		{"/api/v1/details/network", "stats.example", "https://attacker.example", 403},
		{"/api/v1/details/network?", "stats.example", "", 400},
		{"/api/v1/details/network?role=live", "stats.example", "", 400},
		{"/api/v1/details/%6eetwork", "stats.example", "", 400},
	} {
		status, _, _ := replayDetailPublicRequest(t, address, request.path, request.host, request.origin)
		if status != request.want {
			t.Fatalf("%s: got %d, want %d", request.path, status, request.want)
		}
	}
}
