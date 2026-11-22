package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gagliardetto/solana-go"

	"golang.org/x/time/rate"
)

func fastExecutedFixture(now time.Time) (FastExecuted, replayDetailGate) {
	started := now.Add(-time.Hour).UTC()
	genesis := "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
	value := FastExecuted{SchemaVersion: 1, Family: "recent_executed", Scope: "locally_executed",
		State: "fresh", Sequence: 1, SampledAt: now.UTC(),
		Binding: FastExecutedBinding{RunID: "test", GenesisHash: &genesis, NodeStartedAt: started, Attempt: 1, AttemptStartedAt: started},
		Records: []ExecutedSlot{{At: now.UTC(), Slot: 3, Bankhash: "11111111111111111111111111111111"}}}
	gate := replayDetailGate{RunID: "test", NodeStartedAt: started, HasGenesis: true, GenesisHash: genesis,
		Attempt: 1, AttemptStartedAt: started, ObservedAt: now, State: "fresh", revision: 1, stamp: statusStamp{1, 1}}
	return value, gate
}

type fastRecorder struct{ *httptest.ResponseRecorder }

func (w fastRecorder) SetWriteDeadline(time.Time) error { return nil }

type fastNoopListener struct{}

func (fastNoopListener) Accept() (net.Conn, error) { return nil, errors.New("not used") }
func (fastNoopListener) Close() error              { return nil }
func (fastNoopListener) Addr() net.Addr            { return &net.TCPAddr{} }

func TestFastExecutedRequiresReplayDetails(t *testing.T) {
	err := serveProfile(context.Background(), "/unused", fastNoopListener{}, "https://example.com",
		PublicOptions{Resources: true, FastExecuted: true})
	if err == nil {
		t.Fatal("fast execution was accepted without replay details")
	}
}

func TestFastExecutedPrivateFeedFencesAndLease(t *testing.T) {
	now := time.Now()
	value, gate := fastExecutedFixture(now)
	var shared atomic.Pointer[replayDetailGate]
	shared.Store(&gate)
	feed := &fastExecutedFeed{sample: func() (FastExecuted, bool) { return value, true }, gate: &shared,
		limit: rate.NewLimiter(100, 100), active: make(chan struct{}, 1)}
	feed.update(now)
	first := feed.latest.Load()
	if first == nil || !first.freshUntil.After(now) {
		t.Fatal("initial source was not admitted")
	}
	feed.update(now.Add(time.Second))
	if feed.latest.Load() != first {
		t.Fatal("same sequence renewed the source lease")
	}
	response := httptest.NewRecorder()
	feed.serveHTTP(fastRecorder{response}, httptest.NewRequest(http.MethodGet, "/details/recent-executed", nil))
	if response.Code != http.StatusOK || response.Header().Get(replayDetailRemainingHeader) == "" {
		t.Fatal("fresh source was not served with its lease")
	}
	changed := gate
	changed.Rewinds++
	shared.Store(&changed)
	feed.update(now.Add(time.Second))
	if feed.latest.Load() != nil {
		t.Fatal("old replay attempt survived rewind")
	}
	missingGenesis := gate
	missingGenesis.HasGenesis, missingGenesis.GenesisHash = false, ""
	shared.Store(&missingGenesis)
	feed.update(now.Add(time.Second))
	if feed.latest.Load() != nil {
		t.Fatal("missing genesis was admitted")
	}
}

func TestFastExecutedPrivateRequestSamplesLatest(t *testing.T) {
	value, gate := fastExecutedFixture(time.Now())
	var shared atomic.Pointer[replayDetailGate]
	shared.Store(&gate)
	feed := &fastExecutedFeed{sample: func() (FastExecuted, bool) { return value, true }, gate: &shared,
		limit: rate.NewLimiter(100, 100), active: make(chan struct{}, 1)}
	serve := func(sequence, slot uint64) *fastExecutedSample {
		t.Helper()
		response := httptest.NewRecorder()
		feed.serveHTTP(fastRecorder{response}, httptest.NewRequest(http.MethodGet, "/details/recent-executed", nil))
		var got FastExecuted
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &got) != nil ||
			got.Sequence != sequence || len(got.Records) != 1 || got.Records[0].Slot != slot {
			t.Fatalf("private request did not sample sequence%d/slot%d: status%d", sequence, slot, response.Code)
		}
		return feed.latest.Load()
	}
	feed.update(time.Now()) // Ordinary startup status sampling primes the cache; no fast ticker runs.
	first := serve(1, 3)
	value.Sequence++
	value.SampledAt = time.Now().UTC()
	value.Records = []ExecutedSlot{value.Records[0]}
	value.Records[0].Slot++
	value.Records[0].At = value.SampledAt
	second := serve(2, 4)
	if second == first {
		t.Fatal("request served the previous cached publication")
	}
	deadline := second.freshUntil
	if serve(2, 4) != second || !second.freshUntil.Equal(deadline) {
		t.Fatal("repeated source sequence renewed the original lease")
	}
}

func TestFastExecutedBindingAndBounds(t *testing.T) {
	value, _ := fastExecutedFixture(time.Now())
	if !validFastExecuted(value) {
		t.Fatal("valid tail rejected")
	}
	value.Records = make([]ExecutedSlot, 49)
	if validFastExecuted(value) {
		t.Fatal("unbounded tail accepted")
	}
	value, _ = fastExecutedFixture(time.Now())
	value.State, value.Records = "unavailable", []ExecutedSlot{}
	if !validFastExecuted(value) {
		t.Fatal("bound explicit clear rejected")
	}
	value.Sequence = 0
	if validFastExecuted(value) {
		t.Fatal("zero sequence accepted")
	}
}

func TestFastExecutedOptionalBlockID(t *testing.T) {
	now := time.Now()
	value, gate := fastExecutedFixture(now)
	var shared atomic.Pointer[replayDetailGate]
	shared.Store(&gate)
	var body atomic.Pointer[[]byte]
	encode := func(v FastExecuted) {
		encoded, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		body.Store(&encoded)
	}
	encode(value)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(replayDetailRemainingHeader, "4000")
		_, _ = w.Write(*body.Load())
	}))
	defer server.Close()
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	r := newFastExecutedReader(&http.Client{Transport: transport}, &shared)
	defer r.client.CloseIdleConnections()
	r.collect(context.Background())
	if p := r.latest.Load(); p == nil || p.value.Records[0].BlockID != nil || bytes.Contains(p.body, []byte(`"block_id"`)) {
		t.Fatal("older records without an ID were rejected or assigned one")
	}
	id := (solana.Hash{2}).String()
	value.Sequence++
	value.Records[0].BlockID = &id
	encode(value)
	r.collect(context.Background())
	publication := r.latest.Load()
	if publication == nil || publication.value.Records[0].BlockID == nil || *publication.value.Records[0].BlockID != id ||
		id == publication.value.Records[0].Bankhash || !bytes.Contains(publication.body, []byte(`"block_id":"`+id+`"`)) {
		t.Fatal("the reader lost or confused the exact optional block identity")
	}
	t.Logf("BLOCK_ID_CONTRACT_JSON %s", publication.body)
	other := (solana.Hash{3}).String()
	value.Records[0].BlockID = &other
	encode(value)
	r.collect(context.Background())
	if r.latest.Load() != nil {
		t.Fatal("the same sequence changed its block ID")
	}
	for _, invalid := range []string{"", (solana.Hash{}).String(), "0bad", strings.Repeat("1", 45), strings.Repeat("2", 32)} {
		value.Sequence++
		value.Records[0].BlockID = &invalid
		if validFastExecuted(value) {
			t.Fatalf("accepted malformed optional block ID %q", invalid)
		}
		encode(value)
		r.collect(context.Background())
		if r.latest.Load() != nil {
			t.Fatal("the reader admitted an invalid block identity")
		}
	}
	value.Sequence++
	value.Records[0].BlockID = nil
	encode(value)
	withNull := bytes.Replace(*body.Load(), []byte(`"bankhash":`), []byte(`"block_id":null,"bankhash":`), 1)
	body.Store(&withNull)
	r.collect(context.Background())
	if p := r.latest.Load(); p == nil || p.value.Records[0].BlockID != nil {
		t.Fatal("an explicit unknown ID was invented or rejected")
	}
	changed := gate
	changed.Rewinds++
	shared.Store(&changed)
	r.collect(context.Background())
	if r.latest.Load() != nil {
		t.Fatal("optional identity bypassed the replay binding fence")
	}
}

func TestFastExecutedBlockIDFitsExistingBounds(t *testing.T) {
	value, _ := fastExecutedFixture(time.Date(2026, 11, 15, 0, 0, 0, 999999999, time.UTC))
	var hash solana.Hash
	for i := range hash {
		hash[i] = 255
	}
	id := hash.String()
	value.Binding.RunID = strings.Repeat("\x00", 128) // The existing UTF-8 limit permits JSON-escaped bytes.
	value.Sequence, value.Binding.Attempt, value.Binding.Rewinds = math.MaxUint64, math.MaxUint64, math.MaxUint64
	value.Records = make([]ExecutedSlot, 48)
	for i := range value.Records {
		value.Records[i] = ExecutedSlot{At: value.SampledAt, Slot: math.MaxUint64 - 47 + uint64(i),
			Bankhash: id, BlockID: &id, Transactions: math.MaxUint64, ComputeUnits: math.MaxUint64}
	}
	if !validFastExecuted(value) {
		t.Fatal("bounded full-width records were rejected")
	}
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > 16<<10 {
		t.Fatalf("optional identity exceeded the unchanged16KiB source limit: bytes=%d err=%v", len(encoded), err)
	}
	t.Logf("48 full-width optional-ID records: %d bytes (limit %d)", len(encoded), 16<<10)
	value.Records = append(value.Records, value.Records[0])
	if validFastExecuted(value) {
		t.Fatal("optional identity widened the48-record limit")
	}
}

func TestFastExecutedReaderClearCannotReviveOldSequence(t *testing.T) {
	now := time.Now()
	value, gate := fastExecutedFixture(now)
	var shared atomic.Pointer[replayDetailGate]
	shared.Store(&gate)
	available := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !available {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set(replayDetailRemainingHeader, "4000")
		_ = json.NewEncoder(w).Encode(value)
	}))
	defer server.Close()
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	}
	r := newFastExecutedReader(&http.Client{Transport: transport}, &shared)
	defer r.client.CloseIdleConnections()
	r.collect(context.Background())
	first := r.latest.Load()
	if first == nil {
		t.Fatal("fresh source was not accepted")
	}
	r.collect(context.Background())
	if r.latest.Load() != first {
		t.Fatal("repeated source renewed the lease")
	}
	available = false
	r.collect(context.Background())
	if r.latest.Load() != nil {
		t.Fatal("failed source did not clear")
	}
	select {
	case <-first.invalidated:
	default:
		t.Fatal("clear did not invalidate the prior publication")
	}
	available = true
	r.collect(context.Background())
	if r.latest.Load() != nil {
		t.Fatal("old sequence revived after clear")
	}
	value.Sequence++
	r.collect(context.Background())
	if r.latest.Load() == nil {
		t.Fatal("new sequence did not recover")
	}
	a := value
	alternate := gate
	alternate.Rewinds, alternate.revision, alternate.stamp = 1, 2, statusStamp{1, 2}
	shared.Store(&alternate)
	value.Binding.Rewinds, value.Sequence = 1, 1
	r.collect(context.Background())
	if r.latest.Load() == nil {
		t.Fatal("alternate binding was not admitted")
	}
	returned := gate
	returned.revision, returned.stamp = 3, statusStamp{1, 3}
	shared.Store(&returned)
	value = a
	r.collect(context.Background())
	if r.latest.Load() != nil {
		t.Fatal("old A sequence revived after A to B to A")
	}
	changed := gate
	changed.Rewinds++
	shared.Store(&changed)
	r.collect(context.Background())
	if r.latest.Load() != nil {
		t.Fatal("old binding survived rewind")
	}
}

func TestFastExecutedReaderTimeoutKeepsOnlyLivePublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		value, gate := fastExecutedFixture(time.Now())
		var shared atomic.Pointer[replayDetailGate]
		shared.Store(&gate)
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		reader := newFastExecutedReader(&http.Client{Transport: &http.Transport{}}, &shared)
		var transportErr error
		var reject bool
		var onTimeout func()
		reader.client.Transport = selectedEpochTransport(func(request *http.Request) (*http.Response, error) {
			if transportErr != nil {
				if onTimeout != nil {
					onTimeout()
				}
				return nil, transportErr
			}
			if reject {
				return &http.Response{StatusCode: http.StatusServiceUnavailable,
					Body: io.NopCloser(strings.NewReader("unavailable"))}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, ContentLength: int64(len(body)),
				Header: http.Header{http.CanonicalHeaderKey(replayDetailRemainingHeader): {"4000"}},
				Body:   io.NopCloser(bytes.NewReader(body)), Request: request}, nil
		})

		reader.collect(t.Context())
		first := reader.latest.Load()
		if first == nil {
			t.Fatal("fresh source was not admitted")
		}
		time.Sleep(time.Second)
		transportErr = context.DeadlineExceeded
		reader.collect(t.Context())
		if reader.latest.Load() != first {
			t.Fatal("transport timeout withdrew an eligible publication")
		}
		transportErr = nil
		reader.collect(t.Context())
		if reader.latest.Load() != first {
			t.Fatal("identical response changed the publication or renewed its lease")
		}

		reject = true
		reader.collect(t.Context())
		if reader.latest.Load() != nil {
			t.Fatal("authoritative source rejection failed to clear the publication")
		}
		reject = false
		reader.collect(t.Context())
		if reader.latest.Load() != nil {
			t.Fatal("authoritatively withdrawn bytes were revived")
		}
		next := func() *fastExecutedPublication {
			t.Helper()
			transportErr = nil
			value.Sequence++
			body, err = json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			reader.collect(t.Context())
			newest := reader.latest.Load()
			if newest == nil {
				t.Fatal("new sequence did not recover")
			}
			return newest
		}

		next()
		rewound := gate
		rewound.Rewinds++
		rewound.revision++
		rewound.stamp = statusStamp{1, 2}
		onTimeout = func() { shared.Store(&rewound) }
		transportErr = context.DeadlineExceeded
		reader.collect(t.Context())
		if reader.latest.Load() != nil {
			t.Fatal("transport timeout carried a publication across a rewind")
		}

		shared.Store(&gate)
		onTimeout = nil
		unexpired := next()
		time.Sleep(time.Until(unexpired.freshUntil))
		transportErr = context.DeadlineExceeded
		reader.collect(t.Context())
		if reader.latest.Load() != nil {
			t.Fatal("transport timeout retained a publication at its original expiry")
		}
	})
}

func TestFastExecutedHTTPAdmissionAndBounds(t *testing.T) {
	now := time.Now()
	value, gate := fastExecutedFixture(now)
	c := &collector{clockOrigin: now}
	c.detailGate.Store(&gate)
	frame := &frameSet{freshUntil: now.Add(2 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
	frame.fence.nodeEpoch, frame.fence.families[statusReplay] = 1, 1
	c.latest.Store(frame)
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	r := &fastExecutedReader{gate: &c.detailGate, limit: rate.NewLimiter(100, 100), active: make(chan struct{}, 1)}
	r.latest.Store(&fastExecutedPublication{value: value, body: body, stamp: gate.stamp,
		freshUntil: now.Add(time.Second), invalidated: make(chan struct{})})
	request := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.serveHTTP(fastRecorder{w}, httptest.NewRequest(http.MethodGet, path, nil), c)
		return w
	}
	response := request("/api/v1/details/recent-executed")
	if response.Code != http.StatusOK || response.Header().Get(statusRemainingHeader) == "" ||
		response.Header().Get(replayDetailRemainingHeader) == "" {
		t.Fatal("bound fast response lacked status or detail lease")
	}
	if request("/api/v1/details/recent-executed?x=1").Code != http.StatusBadRequest {
		t.Fatal("query unexpectedly accepted")
	}
	changed := gate
	changed.Rewinds++
	c.detailGate.Store(&changed)
	if request("/api/v1/details/recent-executed").Code != http.StatusServiceUnavailable {
		t.Fatal("old replay binding survived rewind")
	}
}

type blockedFastWriter struct {
	header  http.Header
	started chan struct{}
	release chan struct{}
	body    []byte
}

func (w *blockedFastWriter) Header() http.Header              { return w.header }
func (w *blockedFastWriter) WriteHeader(int)                  { close(w.started); <-w.release }
func (w *blockedFastWriter) SetWriteDeadline(time.Time) error { return nil }
func (w *blockedFastWriter) Write(b []byte) (int, error) {
	w.body = append(w.body, b...)
	return len(b), nil
}

func TestFastExecutedHTTPInvalidatedBeforeBody(t *testing.T) {
	now := time.Now()
	value, gate := fastExecutedFixture(now)
	c := &collector{clockOrigin: now}
	c.detailGate.Store(&gate)
	frame := &frameSet{freshUntil: now.Add(2 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
	frame.fence.nodeEpoch, frame.fence.families[statusReplay] = 1, 1
	c.latest.Store(frame)
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	r := &fastExecutedReader{gate: &c.detailGate, limit: rate.NewLimiter(100, 100), active: make(chan struct{}, 1)}
	r.changed.Store(&fastExecutedChange{changed: make(chan struct{})})
	r.latest.Store(&fastExecutedPublication{value: value, body: body, stamp: gate.stamp,
		freshUntil: now.Add(time.Second), invalidated: make(chan struct{})})
	w := &blockedFastWriter{header: make(http.Header), started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		r.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/details/recent-executed", nil), c)
		close(done)
	}()
	<-w.started
	r.replace(nil)
	close(w.release)
	<-done
	if len(w.body) != 0 {
		t.Fatal("invalidated fast publication wrote its body")
	}
}

func TestFastExecutedPrivateHTTPInvalidatedBeforeBody(t *testing.T) {
	now := time.Now()
	value, gate := fastExecutedFixture(now)
	var shared atomic.Pointer[replayDetailGate]
	shared.Store(&gate)
	feed := &fastExecutedFeed{sample: func() (FastExecuted, bool) { return value, true }, gate: &shared,
		limit: rate.NewLimiter(100, 100), active: make(chan struct{}, 1)}
	feed.update(now)
	w := &blockedFastWriter{header: make(http.Header), started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		feed.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/details/recent-executed", nil))
		close(done)
	}()
	<-w.started
	feed.replace(nil)
	close(w.release)
	<-done
	if len(w.body) != 0 {
		t.Fatal("invalidated private publication wrote its body")
	}
}

type blockedFastBodyWriter struct {
	header  http.Header
	started chan struct{}
	expired chan struct{}
	once    sync.Once
}

func (w *blockedFastBodyWriter) Header() http.Header { return w.header }
func (w *blockedFastBodyWriter) WriteHeader(int)     {}
func (w *blockedFastBodyWriter) Write([]byte) (int, error) {
	close(w.started)
	<-w.expired
	return 0, errors.New("write deadline")
}
func (w *blockedFastBodyWriter) SetWriteDeadline(at time.Time) error {
	if !at.After(time.Now()) {
		w.once.Do(func() { close(w.expired) })
	}
	return nil
}

func TestFastExecutedHTTPInvalidationInterruptsBlockedBody(t *testing.T) {
	now := time.Now()
	value, gate := fastExecutedFixture(now)
	c := &collector{clockOrigin: now}
	c.detailGate.Store(&gate)
	frame := &frameSet{freshUntil: now.Add(2 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
	frame.fence.nodeEpoch, frame.fence.families[statusReplay] = 1, 1
	c.latest.Store(frame)
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	r := &fastExecutedReader{gate: &c.detailGate, limit: rate.NewLimiter(100, 100), active: make(chan struct{}, 1)}
	r.changed.Store(&fastExecutedChange{changed: make(chan struct{})})
	r.latest.Store(&fastExecutedPublication{value: value, body: body, stamp: gate.stamp,
		freshUntil: now.Add(time.Second), invalidated: make(chan struct{})})
	w := &blockedFastBodyWriter{header: make(http.Header), started: make(chan struct{}), expired: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		r.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/details/recent-executed", nil), c)
		close(done)
	}()
	<-w.started
	r.replace(nil)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked HTTP body was not interrupted")
	}
}

func TestFastExecutedReaderStartPacing(t *testing.T) {
	for _, test := range []struct {
		name                                  string
		fetch                                 time.Duration
		status                                int
		malformed, repeat, clear, cancelFetch bool
		count                                 int
	}{
		{name: "fast rate ceiling", status: 200, count: 11},
		{name: "80ms fetch", fetch: 80 * time.Millisecond, status: 200, count: 3},
		{name: "slow cooldown", fetch: 150 * time.Millisecond, status: 200, count: 3},
		{name: "malformed", fetch: 80 * time.Millisecond, status: 200, malformed: true, count: 3},
		{name: "429", fetch: 80 * time.Millisecond, status: 429, count: 3},
		{name: "503", fetch: 80 * time.Millisecond, status: 503, count: 3},
		{name: "unsupported", fetch: 80 * time.Millisecond, status: 404, count: 3},
		{name: "explicit clear", fetch: 80 * time.Millisecond, status: 200, clear: true, count: 3},
		{name: "duplicate lease", fetch: 80 * time.Millisecond, status: 200, repeat: true, count: 3},
		{name: "cancel fetch", fetch: 150 * time.Millisecond, status: 200, cancelFetch: true, count: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				value, gate := fastExecutedFixture(time.Now())
				var shared atomic.Pointer[replayDetailGate]
				shared.Store(&gate)
				started := make(chan time.Time, 16)
				var calls, active, maximum int
				var firstBody []byte
				client := &http.Client{Transport: selectedEpochTransport(func(request *http.Request) (*http.Response, error) {
					calls++
					active++
					maximum = max(maximum, active)
					defer func() { active-- }()
					started <- time.Now()
					select {
					case <-time.After(test.fetch):
					case <-request.Context().Done():
						return nil, request.Context().Err()
					}
					value.Sequence = uint64(calls)
					value.SampledAt, value.Records[0].At = time.Now().UTC(), time.Now().UTC()
					body, err := json.Marshal(value)
					if err != nil {
						return nil, err
					}
					code := http.StatusOK
					if calls == 1 {
						firstBody = body
					} else {
						code = test.status
						if code != http.StatusOK {
							body = []byte("unavailable")
						}
						if test.malformed {
							body = []byte(`{"schema_version":`)
						}
						if test.repeat {
							body = firstBody
						}
						if test.clear {
							next := value
							next.State, next.Records = "unavailable", []ExecutedSlot{}
							body, _ = json.Marshal(next)
						}
					}
					return &http.Response{StatusCode: code, Header: http.Header{replayDetailRemainingHeader: {"4000"}}, Body: io.NopCloser(bytes.NewReader(body)), Request: request}, nil
				})}
				r := &fastExecutedReader{client: client, gate: &shared, done: make(chan struct{})}
				r.changed.Store(&fastExecutedChange{changed: make(chan struct{})})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go r.run(ctx)
				var starts []time.Time
				var original *fastExecutedPublication
				var originalUntil time.Time
				for i := 0; i < test.count; i++ {
					starts = append(starts, <-started)
					if test.cancelFetch {
						break
					}
					time.Sleep(test.fetch)
					synctest.Wait()
					if i == 0 {
						original = r.latest.Load()
						if original == nil {
							t.Fatal("initial valid fetch not admitted")
						}
						originalUntil = original.freshUntil
					}
				}
				if test.repeat && (r.latest.Load() != original || !original.freshUntil.Equal(originalUntil)) {
					t.Fatal("duplicate renewed the original lease")
				}
				cancel()
				<-r.done
				if calls != test.count || maximum != 1 || active != 0 {
					t.Fatalf("calls/overlap after cancellation: %d/%d/%d", calls, maximum, active)
				}
				for i := 1; i < len(starts); i++ {
					want := max(100*time.Millisecond, test.fetch+10*time.Millisecond)
					if i > 1 && (test.malformed || test.status != 200 || test.clear) {
						want = test.fetch + 100*time.Millisecond
						if test.status == 404 {
							want = test.fetch + 5*time.Second
						}
					}
					if got := starts[i].Sub(starts[i-1]); got != want {
						t.Fatalf("start spacing %s, want %s", got, want)
					}
					count := 0
					for _, at := range starts {
						if !at.Before(starts[i-1]) && at.Before(starts[i-1].Add(time.Second)) {
							count++
						}
					}
					if count > 10 {
						t.Fatal("more than10 starts in a half-open second")
					}
				}
			})
		})
	}
}
