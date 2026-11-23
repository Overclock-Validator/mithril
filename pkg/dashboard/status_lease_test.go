package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

type statusLeaseFixtureValue struct {
	sample  Snapshot
	headers []string
	delay   time.Duration
}

func statusLeaseFixture(t *testing.T, value statusLeaseFixtureValue) (*collector, *atomic.Pointer[statusLeaseFixtureValue]) {
	c, current, _ := statusLeaseFixturePath(t, value)
	return c, current
}

func statusLeaseFixturePath(t *testing.T, value statusLeaseFixtureValue) (*collector, *atomic.Pointer[statusLeaseFixtureValue], string) {
	t.Helper()
	path := socketPath(t)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	current := new(atomic.Pointer[statusLeaseFixtureValue])
	current.Store(&value)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := current.Load()
		for _, header := range v.headers {
			w.Header().Add(statusRemainingHeader, header)
		}
		if v.delay > 0 {
			time.Sleep(v.delay)
		}
		if err := json.NewEncoder(w).Encode(v.sample); err != nil {
			t.Error(err)
		}
	})}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	c := newCollector(path)
	t.Cleanup(func() { c.client.CloseIdleConnections(); _ = server.Close(); _ = listener.Close(); <-done })
	return c, current, path
}

func TestCollectorDoesNotRefreshExpiredPrivateStatus(t *testing.T) {
	now := time.Now().UTC()
	c, _ := statusLeaseFixture(t, statusLeaseFixtureValue{sample: testSnapshot(now), headers: []string{"0"}})
	c.poll(context.Background(), now)
	if got := statusFields(t, c.latest.Load())["source_state"]; got != "stale" {
		t.Fatalf("expired private status reported %v, want stale", got)
	}
}

func TestStatusLeaseHeader(t *testing.T) {
	for _, tc := range []struct {
		values    []string
		remaining time.Duration
		present   bool
		valid     bool
	}{
		{nil, 0, false, true}, {[]string{"0"}, 0, true, true},
		{[]string{"1"}, time.Millisecond, true, true}, {[]string{"5000"}, 5 * time.Second, true, true},
		{[]string{"5001"}, 0, true, false}, {[]string{"01"}, 0, true, false},
		{[]string{"+1"}, 0, true, false}, {[]string{"-1"}, 0, true, false},
		{[]string{" 1"}, 0, true, false}, {[]string{"1, 2"}, 0, true, false},
		{[]string{"1", "1"}, 0, true, false},
	} {
		h := make(http.Header)
		for _, v := range tc.values {
			h.Add(statusRemainingHeader, v)
		}
		remaining, present, valid := statusLease(h)
		if remaining != tc.remaining || present != tc.present || valid != tc.valid {
			t.Errorf("header %q: got %v, %v, %v", tc.values, remaining, present, valid)
		}
	}
}

func TestCollectorStatusLeaseRepeatsAndLegacyReplacement(t *testing.T) {
	now := time.Now().UTC()
	value := statusLeaseFixtureValue{sample: testSnapshot(now), headers: []string{"5000"}}
	c, current := statusLeaseFixture(t, value)
	update := func(v statusLeaseFixtureValue) { current.Store(&v) }
	poll := func(want string) {
		t.Helper()
		c.poll(context.Background(), now)
		if got := statusFields(t, c.latest.Load())["source_state"]; got != want {
			t.Fatalf("source state: got %v, want %s", got, want)
		}
	}
	poll("fresh")
	value.headers = []string{"0"}
	update(value)
	poll("stale")
	value.headers = []string{"5000"}
	update(value)
	poll("stale") // Re-reading the same sample must not extend its lease.
	value.headers = nil
	update(value)
	poll("stale") // A leased run cannot silently become a legacy source.
	value.sample.RunID = "replacement-run"
	update(value)
	poll("fresh") // A different legacy run remains supported.
}

func TestCollectorRejectsChangedStatusAtSameSampleTime(t *testing.T) {
	for _, headers := range [][]string{nil, {"5000"}} {
		t.Run(fmt.Sprintf("leased=%t", headers != nil), func(t *testing.T) {
			now := time.Now().UTC()
			value := statusLeaseFixtureValue{sample: testSnapshot(now), headers: headers}
			c, current := statusLeaseFixture(t, value)
			c.poll(context.Background(), now)
			if c.last == nil || c.last.Phase != "replaying" {
				t.Fatal("initial status not accepted")
			}
			value.sample.Phase = "stopping"
			current.Store(&value)
			c.poll(context.Background(), now)
			if c.last == nil || c.last.Phase != "replaying" || statusFields(t, c.latest.Load())["source_state"] != "stale" {
				t.Fatal("changed status at the same sample time was accepted as fresh")
			}
			value.sample.SampledAt = now.Add(time.Millisecond)
			current.Store(&value)
			c.poll(context.Background(), value.sample.SampledAt)
			if c.last == nil || c.last.Phase != "stopping" || statusFields(t, c.latest.Load())["source_state"] != "fresh" {
				t.Fatal("newer valid status did not restore freshness")
			}
		})
	}
}

func TestPrivateFeedRejectsChangedCachedSampleAtSameTime(t *testing.T) {
	path := socketPath(t)
	var sample atomic.Pointer[Snapshot]
	good := testSnapshot(time.Now().UTC())
	sample.Store(&good)
	feed, err := ListenFeed(context.Background(), path, func() Snapshot { return *sample.Load() })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	bad := good
	bad.Mode = "invalid"
	sample.Store(&bad) // Keep later ticker samples from replacing the test body.
	original := feed.latest.Load()
	var changed Snapshot
	if err := json.Unmarshal(original.body, &changed); err != nil {
		t.Fatal(err)
	}
	c := newCollector(path)
	t.Cleanup(c.client.CloseIdleConnections)
	c.poll(context.Background(), time.Now())
	if c.last == nil || c.last.Phase != "replaying" {
		t.Fatal("initial private-feed sample not accepted")
	}
	changed.Phase = "stopping"
	body, err := json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	feed.latest.Store(&feedStatus{body: body, freshUntil: original.freshUntil})
	c.poll(context.Background(), time.Now())
	if c.last == nil || c.last.Phase != "replaying" || statusFields(t, c.latest.Load())["source_state"] != "stale" {
		t.Fatal("changed cached body was accepted under the earlier sample time")
	}
}

func TestCollectorStatusLeaseIncludesResponseDelay(t *testing.T) {
	now := time.Now().UTC()
	c, _ := statusLeaseFixture(t, statusLeaseFixtureValue{sample: testSnapshot(now), headers: []string{"1"}, delay: 10 * time.Millisecond})
	c.poll(context.Background(), now)
	if got := statusFields(t, c.latest.Load())["source_state"]; got != "stale" {
		t.Fatalf("delayed response reported %v, want stale", got)
	}
}

func TestPrivateFeedPublishesOriginalStatusLease(t *testing.T) {
	path := socketPath(t)
	var sample atomic.Pointer[Snapshot]
	good := testSnapshot(time.Now().UTC())
	sample.Store(&good)
	feed, err := ListenFeed(context.Background(), path, func() Snapshot { return *sample.Load() })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	bad := good
	bad.Mode = "invalid"
	sample.Store(&bad)
	c := newCollector(path)
	t.Cleanup(c.client.CloseIdleConnections)
	response, err := c.client.Get("http://node/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	remaining, present, valid := statusLease(response.Header)
	if !present || !valid || remaining <= 0 || remaining > StaleAfter {
		t.Fatalf("live feed lease: remaining %v, present %v, valid %v", remaining, present, valid)
	}
	accepted := feed.latest.Load()
	feed.latest.Store(&feedStatus{body: accepted.body, freshUntil: time.Now().Add(-time.Second)})
	response, err = c.client.Get("http://node/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if got := response.Header.Get(statusRemainingHeader); got != "0" {
		t.Fatalf("expired feed lease: got %q, want 0", got)
	}
	c.poll(context.Background(), time.Now().UTC())
	if got := statusFields(t, c.latest.Load())["source_state"]; got != "stale" {
		t.Fatalf("expired feed reported %v, want stale", got)
	}
}
