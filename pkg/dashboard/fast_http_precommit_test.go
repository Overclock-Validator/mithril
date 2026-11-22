package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/time/rate"
)

// Header() does not commit. WriteHeader() is already logical commitment,
// even while a wrapper is holding its underlying wire write.
type fastCommitBarrier struct {
	http.ResponseWriter
	stage             string
	entered           chan int
	release           [2]<-chan struct{}
	expired           chan struct{}
	attempts, headers atomic.Int32
	pastDeadlines     atomic.Int32
	once              [2]sync.Once
}

func (w *fastCommitBarrier) Header() http.Header {
	if w.stage != "postcommit" {
		n := int(w.attempts.Load())
		if n > 0 && n <= 2 && (n == 1 || w.stage == "churn") {
			w.once[n-1].Do(func() { w.entered <- n; <-w.release[n-1] })
		}
	}
	return w.ResponseWriter.Header()
}
func (w *fastCommitBarrier) WriteHeader(code int) {
	w.headers.Add(1)
	if w.stage == "postcommit" && code == http.StatusOK {
		w.once[0].Do(func() { w.entered <- 1; <-w.release[0] })
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *fastCommitBarrier) SetWriteDeadline(at time.Time) error {
	err := http.NewResponseController(w.ResponseWriter).SetWriteDeadline(at)
	if err == nil {
		if at.After(time.Now()) {
			w.attempts.Add(1)
		} else {
			w.pastDeadlines.Add(1)
			select {
			case w.expired <- struct{}{}:
			default:
			}
		}
	}
	return err
}
func (w *fastCommitBarrier) FlushError() error {
	return http.NewResponseController(w.ResponseWriter).Flush()
}

type fastCommitTransport struct {
	*http.Transport
	path     string
	response *http.Response
	err      error
}

func (t *fastCommitTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	request := r.Clone(r.Context())
	u := *r.URL
	u.Path = t.path
	request.URL = &u
	t.response, t.err = t.Transport.RoundTrip(request)
	return t.response, t.err
}

func TestFastHTTPPrecommitReplacement(t *testing.T) {
	for _, kind := range []string{"private", "public"} {
		for _, scenario := range []string{"newer", "budget clipped", "budget expired", "canceled", "same sequence", "older sequence", "binding", "gate revision", "gate pointer", "gate ABA", "stamp", "expired", "churn", "postcommit"} {
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				if kind == "public" && (scenario == "gate pointer" || scenario == "gate ABA") {
					t.Skip("private zero-fence pointer controls")
				}
				now := time.Now()
				value, gate := fastExecutedFixture(now)
				firstUntil := now.Add(3 * time.Second)
				if scenario == "budget clipped" {
					firstUntil = now.Add(500 * time.Millisecond)
				}
				if scenario == "budget expired" {
					firstUntil = now.Add(40 * time.Millisecond)
				}
				if kind == "private" {
					gate.stamp = statusStamp{}
					gate.revision = 0
				}
				var shared atomic.Pointer[replayDetailGate]
				shared.Store(&gate)
				var serve func(http.ResponseWriter, *http.Request)
				var replace func(FastExecuted, replayDetailGate, time.Time)
				path := "/details/recent-executed"
				if kind == "private" {
					feed := &fastExecutedFeed{sample: func() (FastExecuted, bool) { return value, true }, gate: &shared, limit: rate.NewLimiter(100, 100), active: make(chan struct{}, 1)}
					body, err := json.Marshal(value)
					if err != nil {
						t.Fatal(err)
					}
					feed.latest.Store(&fastExecutedSample{value: value, body: body, freshUntil: firstUntil, invalidated: make(chan struct{})})
					serve = feed.serveHTTP
					replace = func(v FastExecuted, g replayDetailGate, until time.Time) {
						body, err := json.Marshal(v)
						if err != nil {
							t.Fatal(err)
						}
						if scenario == "gate ABA" {
							middle := g
							middle.Rewinds++
							shared.Store(&middle)
						}
						if scenario == "binding" || scenario == "gate revision" || scenario == "gate pointer" || scenario == "gate ABA" || scenario == "stamp" {
							shared.Store(&g)
						}
						feed.replace(&fastExecutedSample{value: v, body: body, freshUntil: until, invalidated: make(chan struct{})})
					}
				} else {
					c := &collector{clockOrigin: now}
					c.detailGate.Store(&gate)
					frame := &frameSet{freshUntil: firstUntil, clockOrigin: now, clockUncertain: &c.clockUncertain, lease: newStatusFrameLease(now.Add(3 * time.Second))}
					frame.fence.nodeEpoch, frame.fence.families[statusReplay] = 1, 1
					c.publishStatusFrame(frame)
					reader := &fastExecutedReader{gate: &c.detailGate, limit: rate.NewLimiter(100, 100), active: make(chan struct{}, 1)}
					reader.changed.Store(&fastExecutedChange{changed: make(chan struct{})})
					body, err := json.Marshal(value)
					if err != nil {
						t.Fatal(err)
					}
					reader.latest.Store(&fastExecutedPublication{value: value, body: body, stamp: gate.stamp, freshUntil: firstUntil, invalidated: make(chan struct{})})
					serve = func(w http.ResponseWriter, r *http.Request) { reader.serveHTTP(w, r, c) }
					replace = func(v FastExecuted, g replayDetailGate, until time.Time) {
						body, err := json.Marshal(v)
						if err != nil {
							t.Fatal(err)
						}
						c.detailGate.Store(&g)
						if g.stamp != gate.stamp {
							next := *frame
							next.fence.nodeEpoch, next.fence.families[statusReplay] = g.stamp.nodeEpoch, g.stamp.familyEpoch
							c.publishStatusFrame(&next)
						}
						reader.replace(&fastExecutedPublication{value: v, body: body, stamp: g.stamp, freshUntil: until, invalidated: make(chan struct{})})
					}
					path = "/api/v1/details/recent-executed"
				}
				dir, err := os.MkdirTemp("", "fpc-")
				if err != nil {
					t.Fatal(err)
				}
				defer os.RemoveAll(dir)
				socket := filepath.Join(dir, "s")
				listener, err := net.Listen("unix", socket)
				if err != nil {
					t.Fatal(err)
				}
				releases := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
				var releaseOnce [2]sync.Once
				release := func(n int) { releaseOnce[n-1].Do(func() { close(releases[n-1]) }) }
				defer release(1)
				defer release(2)
				barrier := &fastCommitBarrier{stage: scenario, entered: make(chan int, 2), release: [2]<-chan struct{}{releases[0], releases[1]}, expired: make(chan struct{}, 2)}
				handled := make(chan http.Header, 1)
				serverCancel := make(chan context.CancelFunc, 1)
				server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer func() { handled <- w.Header().Clone() }()
					barrier.ResponseWriter = w
					ctx, cancel := context.WithCancel(r.Context())
					defer cancel()
					serverCancel <- cancel
					serve(barrier, r.WithContext(ctx))
				})}
				defer server.Close()
				go server.Serve(listener)
				transport := &fastCommitTransport{Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", socket)
				}}, path: path}
				defer transport.CloseIdleConnections()
				type result struct {
					body      []byte
					remaining time.Duration
					reason    string
				}
				done := make(chan result, 1)
				started := time.Now()
				go func() {
					body, remaining, _, reason := fetchDetail(context.Background(), &http.Client{Transport: transport}, "recent-executed", 16<<10, nil, func(v string) (time.Duration, error) { return parseDetailRemaining(v, StaleAfter) }, func([]byte) string { return "source_unavailable" })
					done <- result{body, remaining, reason}
				}()
				waitStep := func(n int) {
					t.Helper()
					select {
					case got := <-barrier.entered:
						if got != n {
							t.Fatalf("attempt%d, want%d", got, n)
						}
					case <-time.After(time.Second):
						t.Fatal("response did not reach barrier")
					}
				}
				waitExpiry := func() {
					t.Helper()
					select {
					case <-barrier.expired:
					case <-time.After(time.Second):
						t.Fatal("invalidation did not expire actual write deadline")
					}
				}
				waitStep(1)
				if scenario != "postcommit" && barrier.headers.Load() != 0 {
					t.Fatal("precommit barrier observed committed headers")
				}
				next := value
				next.Sequence++
				next.Records = []ExecutedSlot{value.Records[0]}
				next.Records[0].Slot++
				nextGate, until := gate, now.Add(2*time.Second)
				switch scenario {
				case "same sequence":
					next.Sequence = value.Sequence
				case "older sequence":
					next.Sequence = value.Sequence - 1
				case "binding":
					next.Binding.Rewinds++
					nextGate.Rewinds++
				case "gate revision":
					nextGate.revision++
				case "stamp":
					nextGate.stamp.familyEpoch++
				case "expired":
					until = now.Add(-time.Millisecond)
				}
				if scenario == "budget expired" {
					timer := time.NewTimer(time.Until(firstUntil) + time.Millisecond)
					<-timer.C
				}
				if scenario == "canceled" {
					(<-serverCancel)()
				}
				replace(next, nextGate, until)
				if scenario == "postcommit" {
					waitExpiry()
				}
				release(1)
				if scenario == "churn" {
					waitStep(2)
					next.Sequence++
					next.Records = append([]ExecutedSlot(nil), next.Records...)
					next.Records[0].Slot++
					replace(next, nextGate, until)
					release(2)
				}
				select {
				case got := <-done:
					if scenario == "newer" || scenario == "budget clipped" {
						var decoded FastExecuted
						if got.reason != "" || json.Unmarshal(got.body, &decoded) != nil || decoded.Sequence != next.Sequence || transport.response == nil || transport.response.StatusCode != 200 {
							t.Fatalf("fresh precommit replacement was not served: reason=%s bytes=%d response=%v", got.reason, len(got.body), transport.response != nil)
						}
						if barrier.attempts.Load() != 2 || barrier.headers.Load() != 1 {
							t.Fatalf("wrong bounded commit: attempts=%d headers=%d", barrier.attempts.Load(), barrier.headers.Load())
						}
						if started.Add(got.remaining).After(until) || started.Add(got.remaining).After(firstUntil) || got.remaining <= 0 {
							t.Fatal("replacement renewed beyond its original deadline")
						}
						if transport.response.Header.Get("Content-Length") != strconv.Itoa(len(got.body)) {
							t.Fatal("lost exact framing")
						}
					} else {
						if got.body != nil || got.remaining != 0 || got.reason != "source_unavailable" || transport.response != nil || !errors.Is(transport.err, io.EOF) {
							t.Fatalf("unsafe response after%s: reason=%s bytes=%d response=%v", scenario, got.reason, len(got.body), transport.response != nil)
						}
						want := int32(1)
						if scenario == "churn" {
							want = 2
						}
						if barrier.attempts.Load() != want {
							t.Fatalf("unexpected retry: attempts%d want%d", barrier.attempts.Load(), want)
						}
						if scenario == "postcommit" {
							if barrier.headers.Load() != 1 {
								t.Fatal("postcommit case never entered WriteHeader")
							}
						} else if barrier.headers.Load() != 0 {
							t.Fatal("invalid precommit candidate committed headers")
						}
					}
				case <-time.After(2 * time.Second):
					t.Fatal("bounded HTTP request did not finish")
				}
				select {
				case headers := <-handled:
					if scenario == "postcommit" {
						if barrier.pastDeadlines.Load() == 0 {
							t.Fatal("committed invalidation did not expire socket write deadline")
						}
					} else if barrier.pastDeadlines.Load() != 0 {
						t.Fatal("precommit invalidation poisoned socket write deadline")
					}
					if scenario != "newer" && scenario != "budget clipped" && scenario != "postcommit" &&
						(headers.Get("Content-Length") != "" || headers.Get(replayDetailRemainingHeader) != "" || headers.Get(statusRemainingHeader) != "") {
						t.Fatal("uncommitted failure leaked length or freshness headers")
					}
				case <-time.After(time.Second):
					t.Fatal("handler did not finish")
				}
			})
		}
	}
}

// A stale true validity result must not outrun a watcher that already marked
// the uncommitted response invalid. The existing callback is the only seam.
type fastCommitBoundaryWriter struct {
	header                 http.Header
	headers, pastDeadlines atomic.Int32
}

func (w *fastCommitBoundaryWriter) Header() http.Header         { return w.header }
func (w *fastCommitBoundaryWriter) WriteHeader(int)             { w.headers.Add(1) }
func (w *fastCommitBoundaryWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *fastCommitBoundaryWriter) FlushError() error           { return nil }
func (w *fastCommitBoundaryWriter) SetWriteDeadline(at time.Time) error {
	if !at.After(time.Now()) {
		w.pastDeadlines.Add(1)
	}
	return nil
}

func TestFastHTTPCommitReservationHonorsPrecommitInvalidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := &fastCommitBoundaryWriter{header: make(http.Header)}
		current := atomic.Bool{}
		current.Store(true)
		checked, release := make(chan struct{}), make(chan struct{})
		invalidated, done := make(chan struct{}), make(chan bool, 1)
		request, _ := http.NewRequest(http.MethodGet, "http://node/details/recent-executed", nil)
		calls := 0
		go func() {
			done <- writeFastResponse(w, request, []byte(`{"ok":true}`), time.Time{}, time.Now().Add(time.Second), invalidated, nil, func() bool {
				calls++
				valid := current.Load()
				if calls == 3 {
					close(checked)
					<-release
				}
				return valid
			})
		}()
		<-checked
		current.Store(false)
		invalidated <- struct{}{}
		synctest.Wait() // Watcher finishes; writer remains blocked with its earlier true result.
		close(release)
		if <-done {
			t.Fatal("precommit invalidation slipped through commit reservation")
		}
		if w.headers.Load() != 0 || w.pastDeadlines.Load() != 0 {
			t.Fatal("uncommitted invalidation committed headers or expired socket")
		}
		if w.header.Get("Content-Length") != "" || w.header.Get(replayDetailRemainingHeader) != "" || w.header.Get(statusRemainingHeader) != "" {
			t.Fatal("uncommitted failure retained freshness headers")
		}
	})
}
