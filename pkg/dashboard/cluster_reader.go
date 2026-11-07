package dashboard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Overclock-Validator/mithril/pkg/dashboard/cluster"
	"golang.org/x/time/rate"
)

const clusterPageSize = 9
const clusterVersionLimit = 16
const clusterRemainingHeader = "Mithril-Cluster-Remaining-Milliseconds"

type clusterRoute uint8

const (
	clusterDirectoryRoute clusterRoute = iota
	clusterMapRoute
	clusterVersionsRoute
	clusterPerformanceHistoryRoute
)

type clusterMetadata struct {
	SchemaVersion  int                      `json:"schema_version"`
	Scope          string                   `json:"scope"`
	State          string                   `json:"state"`
	Reason         string                   `json:"reason,omitempty"`
	CollectorAt    time.Time                `json:"collector_at"`
	CapturedAt     *time.Time               `json:"captured_at"`
	Profile        string                   `json:"profile"`
	Network        string                   `json:"network"`
	GenesisHash    *string                  `json:"genesis_hash"`
	Generation     string                   `json:"generation"`
	Epoch          *cluster.Count           `json:"epoch"`
	Reference      *cluster.RPCReference    `json:"rpc_reference,omitempty"`
	Performance    *cluster.RPCPerformance  `json:"rpc_performance,omitempty"`
	SolstatSummary *cluster.SolstatSummary  `json:"solstat_summary,omitempty"`
	FastState      string                   `json:"fast_state"`
	Providers      []cluster.ProviderStatus `json:"providers"`
}

type clusterDirectory struct {
	clusterMetadata
	Total      int                 `json:"total"`
	Filtered   int                 `json:"filtered"`
	Offset     int                 `json:"offset"`
	NextOffset *int                `json:"next_offset"`
	Limit      int                 `json:"limit"`
	Order      string              `json:"order"`
	Validators []cluster.Validator `json:"validators"`
}

type clusterCountry struct {
	Code  string `json:"code"`
	Nodes int    `json:"nodes"`
}

type clusterMap struct {
	clusterMetadata
	Precision string           `json:"precision"`
	Total     int              `json:"total_nodes"`
	Located   int              `json:"located_nodes"`
	Countries []clusterCountry `json:"countries"`
}

type clusterVersion struct {
	Version string `json:"version"`
	Rows    int    `json:"rows"`
}

type clusterVersions struct {
	clusterMetadata
	TotalRows   int              `json:"total_rows"`
	SourceRows  int              `json:"source_rows"`
	UnknownRows int              `json:"unknown_rows"`
	OtherRows   int              `json:"other_rows"`
	Versions    []clusterVersion `json:"versions"`
}

type clusterPerformanceHistory struct {
	clusterMetadata
	History []cluster.RPCPerformance `json:"rpc_performance_history,omitempty"`
}

type clusterView struct {
	metadata               clusterMetadata
	pollStarted            time.Time
	leaseStarted           time.Time
	rows                   []cluster.Validator
	orders                 map[string][]int
	mapJSON                []byte
	versionsJSON           []byte
	performanceHistoryJSON []byte
}

// clusterReader consumes a separately collected public file. Neither a viewer
// nor the node can initiate an external HTTP request through this reader.
type clusterReader struct {
	latest         atomic.Pointer[clusterView]
	limit          *rate.Limiter
	done           chan struct{}
	cancel         context.CancelFunc
	last           *cluster.Snapshot // collection-loop ownership only
	digest         string
	anchor         time.Time // first observation of the retained generation
	clockOrigin    time.Time
	clockUncertain atomic.Bool
}

func newClusterReader(parent context.Context, path string) (*clusterReader, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("cluster cache must be a clean absolute file path")
	}
	ctx, cancel := context.WithCancel(parent)
	now := time.Now()
	c := &clusterReader{limit: rate.NewLimiter(5, 10), done: make(chan struct{}), cancel: cancel, clockOrigin: now}
	c.latest.Store(projectCluster(nil, "", "unavailable", "waiting_source", now))
	go func() {
		defer close(c.done)
		c.poll(path, time.Now())
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				c.poll(path, now)
			}
		}
	}()
	return c, nil
}

func (c *clusterReader) close() {
	c.cancel()
	<-c.done
}

func (c *clusterReader) poll(path string, now time.Time) {
	if !clusterClockConsistent(c.clockOrigin, now) {
		c.clockUncertain.Store(true)
	}
	snapshot, err := cluster.ReadFile(path)
	state, reason := "fresh", ""
	if err != nil {
		state, reason = "stale", "source_unavailable"
	} else if snapshot.CapturedAt.After(now.Add(5 * time.Second)) {
		state, reason = "stale", "source_time_invalid"
	} else {
		encoded, marshalErr := json.Marshal(snapshot)
		if marshalErr != nil {
			state, reason = "stale", "source_invalid"
		} else {
			hash := sha256.Sum256(encoded)
			digest := hex.EncodeToString(hash[:])
			if c.last != nil && (snapshot.CapturedAt.Before(c.last.CapturedAt) || snapshot.CapturedAt.Equal(c.last.CapturedAt) && digest != c.digest) {
				state, reason = "stale", "source_regressed"
			} else {
				if c.last == nil || digest != c.digest {
					c.anchor = now
				}
				c.last, c.digest = &snapshot, digest
			}
		}
	}
	if c.last == nil {
		state = "unavailable"
	} else if now.Sub(c.last.CapturedAt) > cluster.MaxAge || c.last.CapturedAt.After(now.Add(5*time.Second)) {
		state = "stale"
		if reason == "" {
			reason = "source_expired"
		}
	}
	if c.clockUncertain.Load() && state == "fresh" {
		state, reason = "stale", "source_time_invalid"
	}
	anchor := now
	if c.last != nil {
		anchor = c.anchor
	}
	c.latest.Store(projectClusterAnchored(c.last, c.digest, state, reason, now, anchor))
}

func projectCluster(snapshot *cluster.Snapshot, generation, state, reason string, now time.Time) *clusterView {
	return projectClusterAnchored(snapshot, generation, state, reason, now, now)
}

func projectClusterAnchored(snapshot *cluster.Snapshot, generation, state, reason string, now, anchor time.Time) *clusterView {
	v := &clusterView{metadata: clusterMetadata{SchemaVersion: 1, Scope: "independent_cluster", State: state,
		Reason: reason, CollectorAt: anchor.UTC(), Profile: "alpenglow", Network: "alpenglow-community", FastState: "unavailable",
		Providers: []cluster.ProviderStatus{}}, pollStarted: anchor, leaseStarted: anchor, rows: []cluster.Validator{}, orders: make(map[string][]int)}
	if snapshot != nil {
		// Keep the monotonic clock while accounting for age at the start of the poll.
		includeAge := func(source time.Time) {
			if age := now.Sub(source); age > 0 {
				if start := now.Add(-age); start.Before(v.leaseStarted) {
					v.leaseStarted = start
				}
			}
		}
		includeAge(snapshot.CapturedAt)
		v.metadata.CapturedAt, v.metadata.GenesisHash = &snapshot.CapturedAt, &snapshot.GenesisHash
		v.metadata.Generation, v.metadata.Epoch, v.metadata.Reference, v.metadata.Performance, v.metadata.SolstatSummary = generation, snapshot.Epoch, snapshot.Reference, snapshot.Performance, snapshot.SolstatSummary
		v.rows = snapshot.Validators
		v.metadata.Providers = append([]cluster.ProviderStatus(nil), snapshot.Providers...)
		for i := range v.metadata.Providers {
			p := &v.metadata.Providers[i]
			if p.ID == "puffin" && p.State == "retired" && p.Reason == "provider_retired" {
				p.State, p.Reason = "unavailable", "not_collected"
			}
			if p.State == "fresh" && (state != "fresh" || p.SourceAt == nil || now.Sub(*p.SourceAt) > cluster.MaxAge || p.SourceAt.After(now.Add(5*time.Second)) || now.Sub(p.CapturedAt) > cluster.MaxAge || p.CapturedAt.After(now.Add(5*time.Second))) {
				p.State = "stale"
			}
			if p.State == "fresh" {
				includeAge(p.CapturedAt)
				includeAge(*p.SourceAt)
				if p.ID == "rpc" {
					if snapshot.Performance != nil {
						includeAge(snapshot.Performance.ObservedAt)
					}
					for _, sample := range snapshot.PerformanceHistory {
						includeAge(sample.ObservedAt)
					}
				}
				if p.ID == "titan" {
					for _, row := range v.rows {
						for _, window := range row.Inclusion {
							includeAge(window.LastObservedAt)
						}
					}
				}
			}
		}
	}
	stakes := make([]*big.Rat, len(v.rows))
	for i, row := range v.rows {
		if row.StakeSOL != nil {
			stakes[i], _ = new(big.Rat).SetString(*row.StakeSOL)
		}
	}
	for _, order := range []string{"name", "stake", "epoch", "1000", "5000"} {
		indices := make([]int, len(v.rows))
		for i := range indices {
			indices[i] = i
		}
		slices.SortFunc(indices, func(a, b int) int {
			x, y := v.rows[a], v.rows[b]
			switch order {
			case "name":
				if result := strings.Compare(strings.ToLower(x.Name), strings.ToLower(y.Name)); result != 0 {
					return result
				}
			case "stake":
				if stakes[a] != nil && stakes[b] != nil {
					if result := stakes[b].Cmp(stakes[a]); result != 0 {
						return result
					}
				} else if stakes[a] != nil {
					return -1
				} else if stakes[b] != nil {
					return 1
				}
			default:
				rank := func(row cluster.Validator) uint64 {
					for _, inclusion := range row.Inclusion {
						if inclusion.Window == order && inclusion.Rank != nil {
							n, _ := strconv.ParseUint(string(*inclusion.Rank), 10, 64)
							return n
						}
					}
					return ^uint64(0)
				}
				if result := rank(x); result < rank(y) {
					return -1
				} else if result > rank(y) {
					return 1
				}
			}
			return strings.Compare(x.VoteAccount, y.VoteAccount)
		})
		v.orders[order] = indices
	}
	locations := make(map[string]string)
	for _, row := range v.rows {
		country, seen := locations[row.Identity]
		if !seen {
			locations[row.Identity] = row.CountryCode
		} else if country != row.CountryCode {
			locations[row.Identity] = ""
		}
	}
	counts := make(map[string]int)
	for _, country := range locations {
		if country != "" {
			counts[country]++
		}
	}
	mapValue := clusterMap{clusterMetadata: v.metadata, Precision: "ip_country_estimate", Total: len(locations), Countries: []clusterCountry{}}
	for country, nodes := range counts {
		mapValue.Countries = append(mapValue.Countries, clusterCountry{Code: country, Nodes: nodes})
		mapValue.Located += nodes
	}
	slices.SortFunc(mapValue.Countries, func(a, b clusterCountry) int { return strings.Compare(a.Code, b.Code) })
	v.mapJSON, _ = json.Marshal(mapValue)

	versions := clusterVersions{clusterMetadata: v.metadata, TotalRows: len(v.rows), Versions: []clusterVersion{}}
	versionCounts := make(map[string]int)
	for _, row := range v.rows {
		if row.DirectorySource != "solstat" {
			continue
		}
		versions.SourceRows++
		if row.Version == "" {
			versions.UnknownRows++
		} else {
			versionCounts[row.Version]++
		}
	}
	for version, rows := range versionCounts {
		versions.Versions = append(versions.Versions, clusterVersion{Version: version, Rows: rows})
	}
	slices.SortFunc(versions.Versions, func(a, b clusterVersion) int {
		if a.Rows != b.Rows {
			return b.Rows - a.Rows
		}
		return strings.Compare(a.Version, b.Version)
	})
	if len(versions.Versions) > clusterVersionLimit {
		for _, item := range versions.Versions[clusterVersionLimit:] {
			versions.OtherRows += item.Rows
		}
		versions.Versions = versions.Versions[:clusterVersionLimit]
	}
	v.versionsJSON, _ = json.Marshal(versions)
	performance := clusterPerformanceHistory{clusterMetadata: v.metadata}
	if snapshot != nil {
		performance.History = snapshot.PerformanceHistory
	}
	v.performanceHistoryJSON, _ = json.Marshal(performance)
	return v
}

// A UTC conversion removes Go's monotonic clock. Retain the original poll time
// so the lease expires even when a later poll or the underlying file read stalls.
func clusterProjectionRemaining(start, now time.Time) int64 {
	if !clusterClockConsistent(start, now) {
		return 0
	}
	age := max(now.Sub(start), now.Round(0).Sub(start.Round(0)))
	if age >= cluster.MaxAge-clusterClockTolerance {
		return 0
	}
	return (cluster.MaxAge - clusterClockTolerance - age).Milliseconds()
}

func (c *clusterReader) handler(allowed func(*http.Request) bool, route clusterRoute) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowed(r) {
			http.Error(w, "host or origin not allowed", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodGet || r.ContentLength != 0 || len(r.TransferEncoding) != 0 || len(r.URL.RawQuery) > 512 || r.URL.ForceQuery && r.URL.RawQuery == "" {
			http.Error(w, "unsupported request", http.StatusBadRequest)
			return
		}
		q, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			http.Error(w, "invalid query", http.StatusBadRequest)
			return
		}
		for key, values := range q {
			if len(values) != 1 || key != "generation" && (route != clusterDirectoryRoute || key != "offset" && key != "order" && key != "country" && key != "search") {
				http.Error(w, "unsupported query", http.StatusBadRequest)
				return
			}
		}
		view := c.latest.Load()
		if generation := q.Get("generation"); generation != "" && generation != view.metadata.Generation {
			http.Error(w, "cluster snapshot changed", http.StatusConflict)
			return
		}
		if !c.limit.Allow() {
			http.Error(w, "cluster query limit reached", http.StatusTooManyRequests)
			return
		}
		var body []byte
		if route == clusterMapRoute {
			body = view.mapJSON
		} else if route == clusterVersionsRoute {
			body = view.versionsJSON
		} else if route == clusterPerformanceHistoryRoute {
			body = view.performanceHistoryJSON
		} else {
			order := q.Get("order")
			if order == "" {
				order = "name"
			}
			indices, ok := view.orders[order]
			if !ok {
				http.Error(w, "unsupported order", http.StatusBadRequest)
				return
			}
			offset := 0
			if raw := q.Get("offset"); raw != "" {
				offset, err = strconv.Atoi(raw)
				if err != nil || offset < 0 || offset > cluster.MaxRows || strconv.Itoa(offset) != raw {
					http.Error(w, "invalid offset", http.StatusBadRequest)
					return
				}
			}
			country, search := q.Get("country"), q.Get("search")
			if len(country) != 0 && (len(country) != 2 || strings.IndexFunc(country, func(r rune) bool { return r < 'A' || r > 'Z' }) >= 0) || len(search) > 64 || !utf8.ValidString(search) || strings.IndexFunc(search, unicode.IsControl) >= 0 {
				http.Error(w, "invalid filter", http.StatusBadRequest)
				return
			}
			search = strings.ToLower(search)
			page := clusterDirectory{clusterMetadata: view.metadata, Total: len(view.rows), Offset: offset, Limit: clusterPageSize, Order: order, Validators: []cluster.Validator{}}
			for _, index := range indices {
				row := view.rows[index]
				if country != "" && row.CountryCode != country || search != "" && !strings.Contains(strings.ToLower(row.Name), search) && !strings.Contains(strings.ToLower(row.Identity), search) && !strings.Contains(strings.ToLower(row.VoteAccount), search) {
					continue
				}
				if page.Filtered >= offset && len(page.Validators) < clusterPageSize {
					page.Validators = append(page.Validators, row)
				}
				page.Filtered++
			}
			if next := offset + len(page.Validators); next < page.Filtered {
				page.NextOffset = &next
			}
			body, err = json.Marshal(page)
		}
		if err != nil || len(body) > MaxSnapshotBytes {
			http.Error(w, "cluster response unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		now := time.Now()
		if !clusterClockConsistent(c.clockOrigin, now) {
			c.clockUncertain.Store(true)
		}
		remaining := int64(0)
		if !c.clockUncertain.Load() && view.metadata.State == "fresh" {
			remaining = min(clusterProjectionRemaining(view.pollStarted, now), clusterProjectionRemaining(view.leaseStarted, now))
		}
		w.Header().Set(clusterRemainingHeader, strconv.FormatInt(remaining, 10))
		_, _ = w.Write(body)
	}
}
