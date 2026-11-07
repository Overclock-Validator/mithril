package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard/cluster"
	"github.com/mr-tron/base58"
	"golang.org/x/time/rate"
)

func clusterTestSnapshot(now time.Time, rows int) cluster.Snapshot {
	epoch := cluster.Count("239")
	s := cluster.Snapshot{SchemaVersion: 1, Scope: "independent_cluster", Profile: "alpenglow", Network: "alpenglow-community", GenesisHash: cluster.GenesisHash,
		CapturedAt: now, Epoch: &epoch, FastState: "unavailable", Validators: []cluster.Validator{}}
	for _, p := range []struct{ id, url, binding, kind string }{
		{"rpc", "https://rpc.ag.validblocks.com/", "rpc_genesis_before_after", "response"},
		{"solstat", "https://solstat.noders.services/api/validators?network=alpenglow", "rpc_profile_epoch_checked", "provider"},
		{"titan", "https://observatory-origin.titananalytics.io/analytics/api/stream", "payload_genesis_epoch_checked", "provider"},
		{"lantern", "https://alpenglow.lantern.one/", "rpc_gossip_ip_matched", "receipt"},
		{"puffin", "https://puffin-validator.xyz/alpenglow", "", ""},
	} {
		status := cluster.ProviderStatus{ID: p.id, SourceURL: p.url, CapturedAt: now, RowCount: cluster.Count(fmt.Sprint(rows)), State: "fresh", SourceAt: &now,
			SourceTimeKind: p.kind, Epoch: &epoch, GenesisHash: cluster.GenesisHash, Binding: p.binding}
		if p.id == "puffin" {
			status.State, status.Reason, status.SourceAt, status.Epoch, status.RowCount = "retired", "provider_retired", nil, nil, "0"
			status.GenesisHash = ""
		}
		s.Providers = append(s.Providers, status)
	}
	for i := 0; i < rows; i++ {
		key := make([]byte, 32)
		key[0], key[1] = byte(i/255+1), byte(i%255)
		identity := base58.Encode(key)
		key[31] = 1
		stake := fmt.Sprintf("%d.123456789", rows-i)
		s.Validators = append(s.Validators, cluster.Validator{Identity: identity, VoteAccount: base58.Encode(key), DirectorySource: "solstat", Name: fmt.Sprintf("Node %03d", i), StakeSOL: &stake,
			CountryCode: "DE", CountrySource: "lantern_dbip", Inclusion: []cluster.Inclusion{}})
	}
	return s
}

func testClusterReader(view *clusterView) *clusterReader {
	c := &clusterReader{limit: rate.NewLimiter(rate.Inf, 1), clockOrigin: time.Now()}
	c.latest.Store(view)
	return c
}

func TestClusterProjectionLeaseHeader(t *testing.T) {
	const header = "Mithril-Cluster-Remaining-Milliseconds"
	now := time.Now()
	for _, tc := range []struct {
		name  string
		start time.Time
		valid bool
	}{
		{"recent", now.Add(-14 * time.Second), true},
		{"expired", now.Add(-cluster.MaxAge), false},
		{"future", now.Add(time.Minute), false},
		{"wall clock only", now.UTC(), false},
		{"missing", time.Time{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := clusterTestSnapshot(now.UTC(), 1)
			c := testClusterReader(projectCluster(&s, "generation-one", "fresh", "", tc.start))
			for _, route := range []clusterRoute{clusterDirectoryRoute, clusterMapRoute, clusterVersionsRoute, clusterPerformanceHistoryRoute} {
				before := time.Now()
				w := httptest.NewRecorder()
				c.handler(func(*http.Request) bool { return true }, route)(w, httptest.NewRequest("GET", "http://local/api/v1/cluster", nil))
				after := time.Now()
				values := w.Header().Values(header)
				if w.Code != http.StatusOK || len(values) != 1 {
					t.Fatalf("route %d: status=%d headers=%q", route, w.Code, values)
				}
				got, err := strconv.ParseInt(values[0], 10, 64)
				if err != nil || got < 0 || got > cluster.MaxAge.Milliseconds() {
					t.Fatalf("route %d: invalid lease %q", route, values[0])
				}
				if tc.valid {
					min := (cluster.MaxAge - time.Second - after.Sub(tc.start)).Milliseconds()
					max := (cluster.MaxAge - time.Second - before.Sub(tc.start)).Milliseconds()
					if got < min || got > max {
						t.Fatalf("route %d: lease=%d want [%d,%d]", route, got, min, max)
					}
				} else if got != 0 {
					t.Fatalf("route %d: anomalous lease=%d", route, got)
				}
				if !strings.Contains(w.Body.String(), `"generation":"generation-one"`) {
					t.Fatalf("route %d: generation changed: %s", route, w.Body.String())
				}
			}
		})
	}
}

func TestClusterPollLeasePreservesPollStart(t *testing.T) {
	const header = "Mithril-Cluster-Remaining-Milliseconds"
	start := time.Now().Add(-14 * time.Second)
	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := cluster.WriteFile(path, clusterTestSnapshot(start.UTC(), 1)); err != nil {
		t.Fatal(err)
	}
	c := testClusterReader(nil)
	c.clockOrigin = start
	c.poll(path, start)
	w := httptest.NewRecorder()
	c.handler(func(*http.Request) bool { return true }, clusterDirectoryRoute)(w, httptest.NewRequest("GET", "http://local/api/v1/cluster", nil))
	remaining, err := strconv.ParseInt(w.Header().Get(header), 10, 64)
	if err != nil || remaining <= 0 || remaining > (cluster.MaxAge-14*time.Second).Milliseconds() {
		t.Fatalf("poll start not preserved: status=%d lease=%q", w.Code, w.Header().Get(header))
	}
}

func TestClusterReaderStartupLease(t *testing.T) {
	const header = "Mithril-Cluster-Remaining-Milliseconds"
	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := cluster.WriteFile(path, clusterTestSnapshot(time.Now().UTC(), 1)); err != nil {
		t.Fatal(err)
	}
	c, err := newClusterReader(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	deadline := time.After(3 * time.Second)
	for c.latest.Load().metadata.State != "fresh" {
		select {
		case <-deadline:
			t.Fatal("reader did not load the fixture")
		case <-time.After(time.Millisecond):
		}
	}
	w := httptest.NewRecorder()
	c.handler(func(*http.Request) bool { return true }, clusterDirectoryRoute)(w, httptest.NewRequest("GET", "http://local/api/v1/cluster", nil))
	remaining, err := strconv.ParseInt(w.Header().Get(header), 10, 64)
	if err != nil || remaining <= 0 || remaining > cluster.MaxAge.Milliseconds() {
		t.Fatalf("startup lease: status=%d lease=%q", w.Code, w.Header().Get(header))
	}
}

func TestClusterLeaseIncludesSourceAge(t *testing.T) {
	for _, tc := range []struct {
		name           string
		ageSnapshot    bool
		ageProvider    bool
		ageCapture     bool
		ageWindow      bool
		agePerformance bool
		ageHistory     bool
	}{
		{"snapshot", true, false, false, false, false, false},
		{"provider", false, true, false, false, false, false},
		{"provider capture", false, false, true, false, false, false},
		{"inclusion window", false, false, false, true, false, false},
		{"performance receipt", false, false, false, false, true, false},
		{"history receipt", false, false, false, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			s := clusterTestSnapshot(now.UTC(), 1)
			old := now.Add(-110 * time.Second).UTC()
			if tc.ageSnapshot {
				s = clusterTestSnapshot(old, 1)
			}
			if tc.ageProvider {
				s.Providers[1].SourceAt = &old
			}
			if tc.ageCapture {
				s.Providers[1].CapturedAt = old
				source := old.Add(3 * time.Second)
				s.Providers[1].SourceAt = &source
			}
			if tc.ageWindow {
				s.Validators[0].Inclusion = []cluster.Inclusion{{Window: "1000", Epoch: *s.Epoch,
					Opportunities: "0", Included: "0", Missed: "0", CohortSize: "1", Observations: "1",
					RankPolicy: cluster.RankPolicy, FirstSlot: "1", LastSlot: "1", FirstObservedAt: old, LastObservedAt: old}}
			}
			if tc.agePerformance || tc.ageHistory {
				s.Reference = &cluster.RPCReference{Slot: "1", EpochStart: "0", SlotsInEpoch: "100", ObservedAt: now.UTC()}
				sample := cluster.RPCPerformance{Slot: "1", Transactions: "0", Slots: "1", PeriodSeconds: 1, ObservedAt: old}
				if tc.agePerformance {
					s.Performance = &sample
				}
				if tc.ageHistory {
					s.PerformanceHistory = []cluster.RPCPerformance{sample}
				}
			}
			path := filepath.Join(t.TempDir(), "cluster.json")
			if err := cluster.WriteFile(path, s); err != nil {
				t.Fatal(err)
			}
			c := testClusterReader(nil)
			c.clockOrigin = now
			c.poll(path, now)
			for _, route := range []clusterRoute{clusterDirectoryRoute, clusterMapRoute, clusterVersionsRoute, clusterPerformanceHistoryRoute} {
				w := httptest.NewRecorder()
				c.handler(func(*http.Request) bool { return true }, route)(w, httptest.NewRequest("GET", "http://local/api/v1/cluster", nil))
				remaining, err := strconv.ParseInt(w.Header().Get(clusterRemainingHeader), 10, 64)
				if err != nil || w.Code != http.StatusOK || remaining <= 0 || remaining > 10000 {
					t.Fatalf("route %d: status=%d remaining=%q", route, w.Code, w.Header().Get(clusterRemainingHeader))
				}
			}
			first := c.latest.Load().leaseStarted
			c.poll(path, now.Add(time.Second))
			if c.latest.Load().leaseStarted.After(first) {
				t.Fatal("same generation extended its source lease")
			}
			if remaining := clusterProjectionRemaining(c.latest.Load().leaseStarted, now.Add(11*time.Second)); remaining != 0 {
				t.Fatalf("lease remained valid after source expiry: %d", remaining)
			}
			if tc.ageProvider {
				c.poll(path, now.Add(11*time.Second))
				if c.latest.Load().metadata.Providers[1].State != "stale" {
					t.Fatal("expired provider was not marked stale")
				}
			}
		})
	}
}

func TestClusterLeaseStaleAndFutureSource(t *testing.T) {
	now := time.Now()
	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := cluster.WriteFile(path, clusterTestSnapshot(now.Add(4*time.Second).UTC(), 1)); err != nil {
		t.Fatal(err)
	}
	c := testClusterReader(nil)
	c.clockOrigin = now
	c.poll(path, now)
	if c.latest.Load().metadata.State != "fresh" {
		t.Fatal("tolerated future source was not fresh")
	}
	if remaining := clusterProjectionRemaining(c.latest.Load().leaseStarted, now); remaining > (cluster.MaxAge - clusterClockTolerance).Milliseconds() {
		t.Fatalf("future source extended lease: %d", remaining)
	}
	if err := cluster.WriteFile(path, clusterTestSnapshot(now.Add(6*time.Second).UTC(), 1)); err != nil {
		t.Fatal(err)
	}
	c.poll(path, now)
	if c.latest.Load().metadata.Reason != "source_time_invalid" {
		t.Fatal("excessively future source was not rejected")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c.poll(path, time.Now())
	if c.latest.Load().metadata.State != "stale" {
		t.Fatal("missing source did not become stale")
	}
	for _, route := range []clusterRoute{clusterDirectoryRoute, clusterMapRoute, clusterVersionsRoute, clusterPerformanceHistoryRoute} {
		w := httptest.NewRecorder()
		c.handler(func(*http.Request) bool { return true }, route)(w, httptest.NewRequest("GET", "http://local/api/v1/cluster", nil))
		if got := w.Header().Get(clusterRemainingHeader); got != "0" {
			t.Fatalf("route %d: stale lease = %q", route, got)
		}
	}
}

func TestClusterLeaseRetainsCaptureAnchor(t *testing.T) {
	const header = "Mithril-Cluster-Remaining-Milliseconds"
	firstPoll := time.Now().Add(-65 * time.Second)
	path := filepath.Join(t.TempDir(), "cluster.json")
	s := clusterTestSnapshot(firstPoll.UTC(), 1)
	olderSource := firstPoll.UTC().Add(-60 * time.Second)
	s.Providers[1].SourceAt = &olderSource
	if err := cluster.WriteFile(path, s); err != nil {
		t.Fatal(err)
	}
	c := testClusterReader(nil)
	c.clockOrigin = firstPoll
	c.poll(path, firstPoll)
	first := c.latest.Load()
	c.poll(path, time.Now())
	second := c.latest.Load()
	if second.metadata.Generation != first.metadata.Generation || !second.metadata.CollectorAt.Equal(first.metadata.CollectorAt) ||
		second.pollStarted != first.pollStarted || second.metadata.State != "fresh" || second.metadata.Providers[1].State != "stale" {
		t.Fatal("same capture changed its anchor or retained stale provider freshness")
	}
	w := httptest.NewRecorder()
	c.handler(func(*http.Request) bool { return true }, clusterDirectoryRoute)(w, httptest.NewRequest("GET", "http://local/api/v1/cluster", nil))
	remaining, err := strconv.ParseInt(w.Header().Get(header), 10, 64)
	if err != nil || remaining <= 0 || remaining > (cluster.MaxAge-65*time.Second).Milliseconds() {
		t.Fatalf("same capture renewed lease: %q", w.Header().Get(header))
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c.poll(path, time.Now())
	failed := c.latest.Load()
	if failed.metadata.State != "stale" || failed.pollStarted != first.pollStarted || !failed.metadata.CollectorAt.Equal(first.metadata.CollectorAt) {
		t.Fatal("source failure changed the capture anchor")
	}
	if err := cluster.WriteFile(path, s); err != nil {
		t.Fatal(err)
	}
	c.poll(path, time.Now())
	recovered := c.latest.Load()
	if recovered.metadata.State != "fresh" || recovered.pollStarted != first.pollStarted || !recovered.metadata.CollectorAt.Equal(first.metadata.CollectorAt) {
		t.Fatal("same-capture recovery changed its anchor")
	}
	if err := cluster.WriteFile(path, clusterTestSnapshot(time.Now().UTC(), 1)); err != nil {
		t.Fatal(err)
	}
	c.poll(path, time.Now())
	replacement := c.latest.Load()
	if replacement.metadata.State != "fresh" || replacement.metadata.Generation == first.metadata.Generation || !replacement.pollStarted.After(first.pollStarted) ||
		!replacement.metadata.CollectorAt.Equal(replacement.pollStarted.UTC()) {
		t.Fatal("new capture did not get a new anchor")
	}
}

func TestClusterLeaseClockDiscontinuity(t *testing.T) {
	for _, tc := range []struct {
		name string
		mono time.Duration
		wall time.Duration
		want bool
	}{
		{"consistent", 30 * time.Second, 30 * time.Second, true},
		{"small drift", 30 * time.Second, 30*time.Second + time.Second, true},
		{"suspend", 30 * time.Second, 32 * time.Second, false},
		{"rollback", 30 * time.Second, 28 * time.Second, false},
		{"negative monotonic", -time.Second, 0, false},
		{"negative wall", 0, -time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := clusterClockConsistentAges(tc.mono, tc.wall); got != tc.want {
				t.Fatalf("monotonic=%s wall=%s: consistent=%v want=%v", tc.mono, tc.wall, got, tc.want)
			}
		})
	}
}

func TestClusterClockFailureStaysStaleAcrossCaptures(t *testing.T) {
	const header = "Mithril-Cluster-Remaining-Milliseconds"
	now := time.Now()
	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := cluster.WriteFile(path, clusterTestSnapshot(now.UTC(), 1)); err != nil {
		t.Fatal(err)
	}
	c := testClusterReader(nil)
	c.clockOrigin = now.Add(time.Second)
	c.poll(path, now)
	first := c.latest.Load()
	if first.metadata.State != "stale" {
		t.Fatal("clock anomaly advertised fresh")
	}
	c.clockOrigin = now // Simulate the clock subsequently returning to a consistent reading.
	if err := cluster.WriteFile(path, clusterTestSnapshot(now.Add(time.Second).UTC(), 1)); err != nil {
		t.Fatal(err)
	}
	c.poll(path, now.Add(2*time.Second))
	second := c.latest.Load()
	if second.metadata.Generation == first.metadata.Generation || second.metadata.State != "stale" {
		t.Fatal("a new capture cleared the clock failure")
	}
	w := httptest.NewRecorder()
	c.handler(func(*http.Request) bool { return true }, clusterDirectoryRoute)(w, httptest.NewRequest("GET", "http://local/api/v1/cluster", nil))
	if got := w.Header().Get(header); got != "0" {
		t.Fatalf("clock failure granted lease %q", got)
	}
}

func TestClusterClockFailureDetectedAtResponse(t *testing.T) {
	const header = "Mithril-Cluster-Remaining-Milliseconds"
	now := time.Now()
	s := clusterTestSnapshot(now.UTC(), 1)
	c := testClusterReader(projectCluster(&s, "generation-one", "fresh", "", now))
	c.clockOrigin = now.Add(time.Second)
	request := func() string {
		w := httptest.NewRecorder()
		c.handler(func(*http.Request) bool { return true }, clusterDirectoryRoute)(w, httptest.NewRequest("GET", "http://local/api/v1/cluster", nil))
		return w.Header().Get(header)
	}
	if got := request(); got != "0" {
		t.Fatalf("clock anomaly granted lease %q", got)
	}
	c.clockOrigin = now
	if got := request(); got != "0" {
		t.Fatalf("clock recovery cleared response failure: %q", got)
	}
}

func TestClusterReaderProjectsLegacyPuffinWithoutMutatingSnapshot(t *testing.T) {
	now := time.Now().UTC()
	s := clusterTestSnapshot(now, 0)
	view := projectCluster(&s, "generation", "fresh", "", now)
	for i, p := range view.metadata.Providers {
		if p.ID == "puffin" {
			if p.State != "unavailable" || p.Reason != "not_collected" {
				t.Fatalf("incorrect projected state: %s/%s", p.State, p.Reason)
			}
			if s.Providers[i].State != "retired" || s.Providers[i].Reason != "provider_retired" {
				t.Fatal("source snapshot mutated")
			}
			return
		}
	}
	t.Fatal("missing Puffin metadata")
}

func TestClusterReaderPreservesSmallStake(t *testing.T) {
	now := time.Now().UTC()
	s := clusterTestSnapshot(now, 1)
	stake := "0.000000001"
	s.Validators[0].StakeSOL = &stake
	if err := cluster.Validate(s); err != nil {
		t.Fatal(err)
	}
	c := testClusterReader(projectCluster(&s, "generation-one", "fresh", "", now))
	w := httptest.NewRecorder()
	c.handler(func(*http.Request) bool { return true }, clusterDirectoryRoute)(w, httptest.NewRequest("GET", "https://stats.example/api/v1/cluster", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"stake_sol":"0.000000001"`) {
		t.Fatalf("small stake response: %d %s", w.Code, w.Body.String())
	}
}

func TestClusterPerformanceHistoryProjection(t *testing.T) {
	now := time.Now().UTC()
	s := clusterTestSnapshot(now, 1)
	s.Reference = &cluster.RPCReference{Slot: "115", EpochStart: "100", SlotsInEpoch: "100", ObservedAt: now}
	s.PerformanceHistory = []cluster.RPCPerformance{{Slot: "114", Transactions: "120", Slots: "4", PeriodSeconds: 60, ObservedAt: now}}
	if err := cluster.Validate(s); err != nil {
		t.Fatal(err)
	}
	c := testClusterReader(projectCluster(&s, "generation-one", "fresh", "", now))
	h := c.handler(func(r *http.Request) bool { return r.Host == "stats.example" }, clusterPerformanceHistoryRoute)
	for _, tc := range []struct {
		target string
		status int
	}{
		{"https://stats.example/api/v1/cluster-performance-history?generation=generation-one", http.StatusOK},
		{"https://stats.example/api/v1/cluster-performance-history?generation=old", http.StatusConflict},
		{"https://stats.example/api/v1/cluster-performance-history?offset=1", http.StatusBadRequest},
		{"https://wrong.example/api/v1/cluster-performance-history", http.StatusForbidden},
	} {
		w := httptest.NewRecorder()
		h(w, httptest.NewRequest("GET", tc.target, nil))
		if w.Code != tc.status {
			t.Fatalf("%s: status=%d want=%d", tc.target, w.Code, tc.status)
		}
		if tc.status == http.StatusOK && !strings.Contains(w.Body.String(), `"rpc_performance_history":[{"slot":"114"`) {
			t.Fatalf("missing history: %s", w.Body.String())
		}
	}
}

func TestClusterReaderPaginationProjectionAndQueries(t *testing.T) {
	now := time.Now().UTC()
	s := clusterTestSnapshot(now, 13)
	s.Reference = &cluster.RPCReference{Slot: "115", EpochStart: "100", SlotsInEpoch: "100", ObservedAt: now}
	nonVote := cluster.Count("100")
	s.Performance = &cluster.RPCPerformance{Slot: "114", Transactions: "120", NonVoteTransactions: &nonVote, Slots: "4", PeriodSeconds: 60, ObservedAt: now}
	s.SolstatSummary = &cluster.SolstatSummary{Epoch: *s.Epoch, TotalValidators: "13", ActiveValidators: "12", DelinquentValidators: "1", TotalStakeSOL: "13.5"}
	s.Validators[1].Name = "<script>not markup</script>"
	s.Validators[1].CountryCode = "US"
	view := projectCluster(&s, "generation-one", "fresh", "", now)
	c := testClusterReader(view)
	allowed := func(r *http.Request) bool { return r.Host == "stats.example" && r.Header.Get("Origin") != "null" }
	request := func(query string, countries bool) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		route := clusterDirectoryRoute
		if countries {
			route = clusterMapRoute
		}
		c.handler(allowed, route)(w, httptest.NewRequest("GET", "https://stats.example/api/v1/cluster"+query, nil))
		return w
	}
	w := request("?order=stake", false)
	var page clusterDirectory
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &page) != nil || page.Total != 13 || len(page.Validators) != 9 || page.NextOffset == nil || *page.NextOffset != 9 || page.Validators[0].Name != "Node 000" || page.Reference == nil || page.Reference.Slot != "115" || !page.Reference.ObservedAt.Equal(now) || page.Performance == nil || page.Performance.Transactions != "120" || page.Performance.NonVoteTransactions == nil || *page.Performance.NonVoteTransactions != "100" || page.SolstatSummary == nil || page.SolstatSummary.TotalStakeSOL != "13.5" {
		t.Fatalf("first page: %d %s", w.Code, w.Body.String())
	}
	w = request("?order=stake&offset=9&generation=generation-one", false)
	if json.Unmarshal(w.Body.Bytes(), &page) != nil || len(page.Validators) != 4 || page.NextOffset != nil {
		t.Fatal("last page")
	}
	w = request("?country=US&search=script", false)
	if json.Unmarshal(w.Body.Bytes(), &page) != nil || page.Filtered != 1 || len(page.Validators) != 1 || strings.Contains(w.Body.String(), "<script>") {
		t.Fatal("filter or JSON escaping")
	}
	w = request("", true)
	var m clusterMap
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &m) != nil || m.Total != 13 || m.Located != 13 || len(m.Countries) != 2 || m.Precision != "ip_country_estimate" || m.Reference == nil || m.Reference.Slot != "115" {
		t.Fatalf("map: %s", w.Body.String())
	}
	for _, tc := range []struct {
		query     string
		countries bool
		status    int
	}{
		{"?generation=old", false, 409}, {"?offset=01", false, 400}, {"?offset=-1", false, 400}, {"?offset=2049", false, 400},
		{"?order=fast", false, 400}, {"?country=de", false, 400}, {"?search=%00", false, 400}, {"?search=%ff", false, 400},
		{"?order=name&order=stake", false, 400}, {"?unknown=1", false, 400}, {"?", false, 400}, {"?order=stake", true, 400}, {"?generation=old", true, 409},
	} {
		if w := request(tc.query, tc.countries); w.Code != tc.status {
			t.Errorf("%s map=%v: %d %s", tc.query, tc.countries, w.Code, w.Body.String())
		}
	}
	for _, tc := range []struct {
		method, host, origin, body string
		status                     int
	}{
		{"POST", "stats.example", "", "", 400}, {"GET", "evil.example", "", "", 403}, {"GET", "stats.example", "null", "", 403}, {"GET", "stats.example", "", "body", 400},
	} {
		r := httptest.NewRequest(tc.method, "https://stats.example/api/v1/cluster", strings.NewReader(tc.body))
		r.Host = tc.host
		r.Header.Set("Origin", tc.origin)
		w := httptest.NewRecorder()
		c.handler(allowed, clusterDirectoryRoute)(w, r)
		if w.Code != tc.status {
			t.Errorf("boundary: %d", w.Code)
		}
	}
	c.limit = rate.NewLimiter(0, 1)
	request("", false)
	if w := request("", true); w.Code != 429 {
		t.Fatalf("shared rate bound: %d", w.Code)
	}
}

func TestClusterReaderFileRecoveryAgeAndRegression(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.json")
	now := time.Now()
	c := testClusterReader(nil)
	c.clockOrigin = now
	c.poll(path, now)
	if c.latest.Load().metadata.State != "unavailable" {
		t.Fatal("missing cache became current")
	}
	s := clusterTestSnapshot(now, 2)
	if err := cluster.WriteFile(path, s); err != nil {
		t.Fatal(err)
	}
	c.poll(path, now)
	first := c.latest.Load()
	if first.metadata.State != "fresh" || first.metadata.Generation == "" {
		t.Fatal("valid cache not served")
	}
	// No client-triggered disk access: an already projected page survives removal.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	c.handler(func(*http.Request) bool { return true }, clusterDirectoryRoute)(w, httptest.NewRequest("GET", "http://local/api/v1/cluster", nil))
	if w.Code != 200 {
		t.Fatal("viewer reread file")
	}
	c.poll(path, now.Add(time.Second))
	if v := c.latest.Load(); v.metadata.State != "stale" || v.metadata.Generation != first.metadata.Generation {
		t.Fatal("error hid age or replaced retained data")
	}
	s.Validators[0].Name = "same-time mutation"
	if err := cluster.WriteFile(path, s); err != nil {
		t.Fatal(err)
	}
	c.poll(path, now.Add(2*time.Second))
	if v := c.latest.Load(); v.metadata.Reason != "source_regressed" || v.rows[0].Name == s.Validators[0].Name {
		t.Fatal("same-time changed snapshot accepted")
	}
	s = clusterTestSnapshot(now.Add(time.Second), 1)
	if err := cluster.WriteFile(path, s); err != nil {
		t.Fatal(err)
	}
	c.poll(path, now.Add(3*time.Second))
	if c.latest.Load().metadata.State != "fresh" || len(c.latest.Load().rows) != 1 {
		t.Fatal("new snapshot did not recover")
	}
	c.poll(path, now.Add(cluster.MaxAge+2*time.Second))
	v := c.latest.Load()
	if v.metadata.State != "stale" || v.metadata.Providers[0].State != "stale" {
		t.Fatal("expired provider advertised fresh")
	}
	s = clusterTestSnapshot(now.Add(time.Hour), 3)
	if err := cluster.WriteFile(path, s); err != nil {
		t.Fatal(err)
	}
	c.poll(path, now.Add(4*time.Second))
	if v := c.latest.Load(); v.metadata.Reason != "source_time_invalid" || len(v.rows) != 1 {
		t.Fatal("future cache accepted")
	}
}

func TestClusterPerformanceHistoryRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.json")
	now := time.Now()
	s := clusterTestSnapshot(now, 1)
	s.Reference = &cluster.RPCReference{Slot: "115", EpochStart: "100", SlotsInEpoch: "100", ObservedAt: now}
	s.PerformanceHistory = []cluster.RPCPerformance{{Slot: "114", Transactions: "120", Slots: "4", PeriodSeconds: 60, ObservedAt: now}}
	if err := cluster.WriteFile(path, s); err != nil {
		t.Fatal(err)
	}
	c := testClusterReader(nil)
	c.clockOrigin = now
	c.poll(path, now)
	first := c.latest.Load()
	if first.metadata.State != "fresh" || !strings.Contains(string(first.performanceHistoryJSON), `"transactions":"120"`) {
		t.Fatal("valid history missing")
	}
	if err := os.WriteFile(path, []byte(`{"rpc_performance_history":[`), 0600); err != nil {
		t.Fatal(err)
	}
	c.poll(path, now.Add(time.Second))
	stale := c.latest.Load()
	if stale.metadata.State != "stale" || stale.metadata.Generation != first.metadata.Generation || !strings.Contains(string(stale.performanceHistoryJSON), `"transactions":"120"`) {
		t.Fatal("corruption did not retain stale history")
	}
	s = clusterTestSnapshot(now.Add(2*time.Second), 1)
	if err := cluster.WriteFile(path, s); err != nil {
		t.Fatal(err)
	}
	c.poll(path, now.Add(3*time.Second))
	recovered := c.latest.Load()
	if recovered.metadata.State != "fresh" || recovered.metadata.Generation == first.metadata.Generation || strings.Contains(string(recovered.performanceHistoryJSON), `"rpc_performance_history"`) {
		t.Fatal("replacement retained old history")
	}
}

func TestClusterReaderPicksUpReplacementWithoutViewer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.json")
	now := time.Now().UTC()
	if err := cluster.WriteFile(path, clusterTestSnapshot(now, 2)); err != nil {
		t.Fatal(err)
	}
	c, err := newClusterReader(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.close)
	wait := func(deadline time.Time, check func(*clusterView) bool) {
		t.Helper()
		for time.Now().Before(deadline) {
			if view := c.latest.Load(); view != nil && check(view) {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("cluster reader did not publish the expected snapshot")
	}
	wait(time.Now().Add(3*time.Second), func(v *clusterView) bool { return v.metadata.State == "fresh" && len(v.rows) == 2 })
	first := c.latest.Load().metadata.Generation
	if err := cluster.WriteFile(path, clusterTestSnapshot(now.Add(time.Second), 1)); err != nil {
		t.Fatal(err)
	}
	wait(time.Now().Add(19*time.Second), func(v *clusterView) bool {
		return v.metadata.State == "fresh" && v.metadata.Generation != first && len(v.rows) == 1
	})
}

func TestClusterMapDeduplicatesIdentityAndConflictingCountries(t *testing.T) {
	s := clusterTestSnapshot(time.Now().UTC(), 3)
	s.Validators[1].Identity = s.Validators[0].Identity
	v := projectCluster(&s, "g", "fresh", "", s.CapturedAt)
	var m clusterMap
	if json.Unmarshal(v.mapJSON, &m) != nil || m.Total != 2 || m.Located != 2 {
		t.Fatal("identity counted twice")
	}
	s.Validators[1].CountryCode = "US"
	v = projectCluster(&s, "g", "fresh", "", s.CapturedAt)
	if json.Unmarshal(v.mapJSON, &m) != nil || m.Located != 1 {
		t.Fatal("conflicting estimate retained")
	}
}

func TestClusterVersionsAreBoundedAndKeepSourceCoverage(t *testing.T) {
	now := time.Now().UTC()
	s := clusterTestSnapshot(now, 22)
	for i := range s.Validators {
		s.Validators[i].Version = fmt.Sprintf("version-%02d", i)
	}
	s.Validators[0].Version, s.Validators[1].Version = "shared", "shared"
	s.Validators[2].Version = ""
	s.Validators[3].DirectorySource = "titan"
	v := projectCluster(&s, "generation-one", "fresh", "", now)
	c := testClusterReader(v)
	handler := c.handler(func(*http.Request) bool { return true }, clusterVersionsRoute)
	request := func(query string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		handler(w, httptest.NewRequest("GET", "http://local/api/v1/cluster-versions"+query, nil))
		return w
	}
	w := request("?generation=generation-one")
	var got clusterVersions
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &got) != nil || got.TotalRows != 22 || got.SourceRows != 21 || got.UnknownRows != 1 || got.OtherRows != 3 || len(got.Versions) != clusterVersionLimit || got.Versions[0] != (clusterVersion{Version: "shared", Rows: 2}) {
		t.Fatalf("version summary: %d %s", w.Code, w.Body.String())
	}
	if w = request("?generation=old"); w.Code != http.StatusConflict {
		t.Fatalf("generation mismatch: %d", w.Code)
	}
	if w = request("?order=name"); w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected query accepted: %d", w.Code)
	}

	s = clusterTestSnapshot(now, cluster.MaxRows)
	for i := range s.Validators {
		s.Validators[i].Version = fmt.Sprintf("%s%04d", strings.Repeat("<", 60), i)
	}
	v = projectCluster(&s, "generation-one", "fresh", "", now)
	c.latest.Store(v)
	w = request("")
	if w.Code != http.StatusOK || w.Body.Len() > MaxSnapshotBytes || strings.Contains(w.Body.String(), "<") {
		t.Fatalf("maximum or escaped summary: status=%d bytes=%d", w.Code, w.Body.Len())
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.SourceRows != cluster.MaxRows || got.OtherRows != cluster.MaxRows-clusterVersionLimit {
		t.Fatalf("maximum summary: %v %+v", err, got)
	}
}

func TestOptionalClusterPublicRoutes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := serveProfile(context.Background(), filepath.Join(t.TempDir(), "missing.sock"), listener, "", PublicOptions{ClusterFile: filepath.Join(t.TempDir(), "cluster.json")}); err == nil {
		t.Fatal("cluster file accepted without a public profile")
	}
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			options := PublicOptions{}
			if enabled {
				options.ClusterFile = filepath.Join(t.TempDir(), "not-collected-yet.json")
			}
			done := make(chan error, 1)
			go func() {
				done <- serveProfile(ctx, filepath.Join(t.TempDir(), "missing.sock"), listener, "https://stats.example", options)
			}()
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			client := &http.Client{Timeout: 2 * time.Second}
			for _, tc := range []struct {
				path, origin string
				status       int
			}{
				{"/api/v1/features", "", 200}, {"/api/v1/features?x=1", "", 400}, {"/api/v1/features", "null", 403},
				{"/api/v1/cluster", "", map[bool]int{true: 200, false: 404}[enabled]},
				{"/api/v1/cluster-map", "", map[bool]int{true: 200, false: 404}[enabled]},
				{"/api/v1/cluster-versions", "", map[bool]int{true: 200, false: 404}[enabled]},
				{"/api/v1/cluster-performance-history", "", map[bool]int{true: 200, false: 404}[enabled]},
				{"/api/v1/history", "", 404}, {"/api/v1/cluster/", "", 404}, {"/api/v1/features/", "", 404},
			} {
				req, _ := http.NewRequest("GET", "http://"+listener.Addr().String()+tc.path, nil)
				req.Host = "stats.example"
				if tc.origin != "" {
					req.Header.Set("Origin", tc.origin)
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(io.LimitReader(resp.Body, MaxSnapshotBytes+1))
				resp.Body.Close()
				if err != nil || len(body) > MaxSnapshotBytes || resp.StatusCode != tc.status {
					t.Fatalf("%s: %d %s %v", tc.path, resp.StatusCode, body, err)
				}
				if tc.path == "/api/v1/features" && tc.status == 200 {
					var features struct {
						SchemaVersion int  `json:"schema_version"`
						History       bool `json:"history"`
						Cluster       bool `json:"cluster"`
					}
					if json.Unmarshal(body, &features) != nil || features.SchemaVersion != 1 || features.History || features.Cluster != enabled {
						t.Fatalf("features: %s", body)
					}
				}
				if resp.Header.Get("Cache-Control") != "no-store" {
					t.Fatal("optional route cache policy changed")
				}
			}
		})
	}
}

func TestClusterMaximumPageFitsBudget(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 123456789, time.UTC)
	s := clusterTestSnapshot(now, clusterPageSize+1)
	max := cluster.Count("18446744073709551615")
	*s.Epoch = max
	s.Reference = &cluster.RPCReference{Slot: max, EpochStart: max, SlotsInEpoch: "1", ObservedAt: now}
	for i := uint64(0); i < cluster.MaxPerformanceHistory; i++ {
		s.PerformanceHistory = append(s.PerformanceHistory, cluster.RPCPerformance{
			Slot: cluster.Count(strconv.FormatUint(^uint64(0)-i, 10)), Transactions: max, NonVoteTransactions: &max,
			Slots: max, PeriodSeconds: 65535, ObservedAt: now,
		})
	}
	rank := cluster.Count("2048")
	stake := "18446744073709551615.123456789012345678"
	percent := 99.12345678901234
	s.SolstatSummary = &cluster.SolstatSummary{Epoch: *s.Epoch, TotalValidators: "2048", ActiveValidators: "2047", DelinquentValidators: "1", TotalStakeSOL: "18446744073709551615.123456789012345678", AverageSkipRatePercent: &percent, MedianSkipRatePercent: &percent}
	includedPercent := float64(667) / float64(999) * 100
	delinquent := true
	for i := range s.Validators {
		v := &s.Validators[i]
		// JSON escaping expands these accepted label lengths sixfold.
		v.Name, v.Version = strings.Repeat("<", 128), strings.Repeat("<", 64)
		v.StakeSOL, v.CommissionPercent, v.Delinquent = &stake, &percent, &delinquent
		v.LastVote, v.RootSlot, v.CurrentEpochCredits, v.VoteLagSlots, v.LeaderSlots, v.BlocksProduced, v.EfficiencyRank = &max, &max, &max, &max, &max, &max, &rank
		v.SkipRatePercent, v.EfficiencyPercent = &percent, &percent
		for _, window := range []string{"1000", "5000", "epoch"} {
			v.Inclusion = append(v.Inclusion, cluster.Inclusion{
				Window: window, Epoch: *s.Epoch, Opportunities: "999", Included: "667", Missed: "332", Percent: &includedPercent,
				Rank: &rank, CohortSize: "2048", Observations: "1000", RankPolicy: cluster.RankPolicy,
				FirstSlot: "18446744073709550616", LastSlot: max, FirstObservedAt: now.Add(-time.Hour), LastObservedAt: now,
			})
		}
		x := &v.Inclusion[2]
		x.Opportunities, x.Observations = max, max
		x.Included, x.Missed, x.FirstSlot = "1234567890123456789", "17212176183586094826", "0"
		epochPercent := float64(uint64(1234567890123456789)) / float64(^uint64(0)) * 100
		x.Percent = &epochPercent
	}
	if err := cluster.Validate(s); err != nil {
		t.Fatalf("maximum fixture is not an accepted cache: %v", err)
	}
	generation := strings.Repeat("a", 64)
	c := testClusterReader(projectCluster(&s, generation, "fresh", "", now))
	w := httptest.NewRecorder()
	c.handler(func(*http.Request) bool { return true }, clusterPerformanceHistoryRoute)(w, httptest.NewRequest("GET", "http://local/api/v1/cluster-performance-history", nil))
	var history clusterPerformanceHistory
	if w.Code != http.StatusOK || w.Body.Len() > MaxSnapshotBytes || json.Unmarshal(w.Body.Bytes(), &history) != nil || len(history.History) != cluster.MaxPerformanceHistory {
		t.Fatalf("maximum history: status=%d bytes=%d rows=%d", w.Code, w.Body.Len(), len(history.History))
	}
	handler := c.handler(func(*http.Request) bool { return true }, clusterDirectoryRoute)
	request := func(query string) clusterDirectory {
		t.Helper()
		w := httptest.NewRecorder()
		handler(w, httptest.NewRequest("GET", "http://local/api/v1/cluster"+query, nil))
		var page clusterDirectory
		if w.Code != http.StatusOK || w.Body.Len() > MaxSnapshotBytes || json.Unmarshal(w.Body.Bytes(), &page) != nil {
			t.Fatalf("maximum page: status=%d bytes=%d", w.Code, w.Body.Len())
		}
		return page
	}
	page := request("")
	if len(page.Validators) != clusterPageSize || page.Limit != clusterPageSize || page.NextOffset == nil || *page.NextOffset != clusterPageSize {
		t.Fatal("maximum page lost pagination")
	}
	last := request(fmt.Sprintf("?offset=%d&generation=%s", *page.NextOffset, generation))
	if len(last.Validators) != 1 || last.NextOffset != nil || last.Validators[0].VoteAccount == page.Validators[len(page.Validators)-1].VoteAccount {
		t.Fatal("maximum final page missing or duplicated")
	}
}
