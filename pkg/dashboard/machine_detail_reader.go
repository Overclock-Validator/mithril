package dashboard

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"golang.org/x/time/rate"
)

const (
	machineDetailPublicPath = "/api/v1/details/machine"
	machineCollectionLimit  = 8 * time.Second
)

// PublicMachineManifest adds reader state without changing the source digest,
// capture dates, descriptors or canonical page bytes.
type PublicMachineManifest struct {
	SchemaVersion int                     `json:"schema_version"`
	Family        string                  `json:"family"`
	State         string                  `json:"state"`
	Reason        string                  `json:"reason"`
	CollectorAt   time.Time               `json:"collector_at"`
	Binding       MachineDetailBinding    `json:"binding"`
	CapturedAt    time.Time               `json:"captured_at"`
	Generation    string                  `json:"generation"`
	Bytes         int                     `json:"bytes"`
	Pages         []MachinePageDescriptor `json:"pages"`
}

var machinePublicReasons = [...]string{
	"", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid",
	"binding_changed", "response_limit", "waiting_source", "unsupported_source",
	"source_generation_expired", "generation_not_ready", "collection_deadline",
}

type machinePublicFamily struct {
	binding                       MachineDetailBinding
	generation                    string
	manifestDigest                [sha256.Size]byte
	roots                         [len(machinePublicReasons)]string
	pages                         []string
	publishedAt, until, holdUntil time.Time
	stamp                         statusStamp
}

type machineReaderReply struct {
	machineDetailReply
	stamp       statusStamp
	family      *machinePublicFamily
	revision    uint64
	reason      string
	pageRequest bool
	until       time.Time
}

// machineDetailReader has one collection owner and two joined page-request
// lanes. Only encoded families survive a collection; viewers retain one string,
// not decoded rows, a PreparedMachineDetail, or a snapshot of this cache.
type machineDetailReader struct {
	client *http.Client
	done   chan struct{}
	starts *rate.Limiter
	limit  *rate.Limiter
	active chan struct{}
	// ponytail: a short scalar/cache lock, with at most two families. Fetching,
	// validation, encoding and response writes remain outside this lock.
	mu               sync.Mutex
	gate             machineDetailGate
	revision         uint64
	sourceGeneration uint64
	items            [2]*machinePublicFamily
	reason           string
	closed           bool
}

func newMachineDetailReader(client *http.Client) (*machineDetailReader, error) {
	if client == nil {
		return nil, errors.New("machine reader requires a fixed Unix transport")
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport == nil || transport.DialContext == nil {
		return nil, errors.New("machine reader requires a fixed Unix transport")
	}
	return &machineDetailReader{client: detailHTTPClient(transport, 2, time.Second),
		done: make(chan struct{}), starts: rate.NewLimiter(4, 1), limit: rate.NewLimiter(16, 32), active: make(chan struct{}, 2)}, nil
}

// observeStatus copies the status owner's accepted context. Ordinary refreshes
// do not interrupt a transfer, but an incompatible/admission A -> B -> A does.
func (r *machineDetailReader) observeStatus(s *Snapshot, state, reason string, at time.Time, nodeEpoch uint64) statusStamp {
	g := machineDetailGate{observedAt: at, reason: "source_unavailable"}
	if s != nil {
		g = machineGate(*s, at)
	}
	if state != "fresh" {
		g.reason = reason
		if g.reason == "" {
			g.reason = "status_stale"
		}
	}
	if nodeEpoch == 0 || nodeEpoch == math.MaxUint64 {
		g.reason = "source_unavailable"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return statusStamp{}
	}
	changed := !r.gate.matches(g.binding)
	if r.revision == 0 || changed || r.gate.stamp.nodeEpoch != nodeEpoch || (r.gate.reason == "") != (g.reason == "") {
		if r.revision >= math.MaxUint64-1 {
			r.closed, r.items = true, [2]*machinePublicFamily{}
			return statusStamp{}
		}
		r.revision++
	}
	if changed {
		r.items, r.sourceGeneration, r.reason = [2]*machinePublicFamily{}, 0, ""
	}
	if nodeEpoch != 0 && nodeEpoch != math.MaxUint64 {
		g.stamp = statusStamp{nodeEpoch: nodeEpoch, familyEpoch: r.revision}
	}
	r.gate = g
	return g.stamp
}

func (r *machineDetailReader) close() {
	r.mu.Lock()
	r.closed, r.items = true, [2]*machinePublicFamily{}
	r.mu.Unlock()
}

func (r *machineDetailReader) run(ctx context.Context) {
	defer close(r.done)
	defer r.close()
	defer r.client.CloseIdleConnections()
	for ctx.Err() == nil {
		r.collect(ctx)
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (r *machineDetailReader) fetch(parent context.Context, query string) ([]byte, time.Time, time.Duration, string) {
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://node"+machineDetailPath+query, nil)
	if err != nil {
		return nil, time.Time{}, 0, "source_invalid"
	}
	started := time.Now()
	response, err := r.client.Do(request)
	if err != nil {
		return nil, started, 0, "source_unavailable"
	}
	defer response.Body.Close()
	limit := MaxMachinePageBytes
	if query == "" {
		limit = MaxMachineManifestBytes
	}
	if response.StatusCode != http.StatusOK {
		limit = 512
	}
	if response.ContentLength > int64(limit) {
		return nil, started, 0, "response_limit"
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(limit)+1))
	if len(body) > limit {
		return nil, started, 0, "response_limit"
	}
	if err != nil {
		return nil, started, 0, "source_unavailable"
	}
	headers := response.Header.Values(replayDetailRemainingHeader)
	if response.StatusCode != http.StatusOK {
		if len(headers) != 0 {
			return nil, started, 0, "source_invalid"
		}
		if response.StatusCode == http.StatusNotFound {
			return nil, started, 0, "unsupported_source"
		}
		var failure struct {
			SchemaVersion int    `json:"schema_version"`
			Family        string `json:"family"`
			State         string `json:"state"`
			Reason        string `json:"reason"`
		}
		if json.Unmarshal(body, &failure) != nil {
			return nil, started, 0, "source_invalid"
		}
		want := machineDetailFailure(failure.Reason)
		if want.status != response.StatusCode || want.body != string(body) {
			return nil, started, 0, "source_invalid"
		}
		if failure.Reason == "rate_limited" {
			return nil, started, 0, "source_unavailable"
		}
		return nil, started, 0, failure.Reason
	}
	if query != "" {
		if len(headers) != 0 {
			return nil, started, 0, "source_invalid"
		}
		return body, started, 0, ""
	}
	if len(headers) != 1 {
		return nil, started, 0, "source_invalid"
	}
	ms, err := strconv.ParseInt(headers[0], 10, 64)
	if err != nil || strconv.FormatInt(ms, 10) != headers[0] || ms < machineDetailMinimum.Milliseconds() || ms > MachineDetailFreshFor.Milliseconds() {
		return nil, started, 0, "source_invalid"
	}
	return body, started, time.Duration(ms) * time.Millisecond, ""
}

func (r *machineDetailReader) collect(parent context.Context) {
	r.mu.Lock()
	before, revision, old, high := r.gate, r.revision, r.items[0], r.sourceGeneration
	closed := r.closed
	r.mu.Unlock()
	if closed || parent.Err() != nil {
		return
	}
	fail := func(reason string) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if !r.closed && r.revision == revision && parent.Err() == nil {
			r.reason = reason
		}
	}
	if reason := before.failure(time.Now()); reason != "" {
		fail(reason)
		return
	}
	if !before.stamp.valid() {
		fail("waiting_source")
		return
	}
	ctx, cancel := context.WithTimeout(parent, machineCollectionLimit)
	defer cancel()
	limit, _ := ctx.Deadline()
	body, started, remaining, reason := r.fetch(ctx, "")
	if reason != "" {
		fail(reason)
		return
	}
	m, err := decodeMachineManifest(body)
	if err != nil {
		fail("source_invalid")
		return
	}
	if !before.matches(m.Binding) {
		fail("binding_changed")
		return
	}
	now, until := time.Now(), started.Add(remaining)
	if !now.Before(until) || m.CapturedAt.After(now.Add(time.Second)) || now.Sub(m.CapturedAt) >= MachineDetailFreshFor {
		fail("source_time_invalid")
		return
	}
	digest := sha256.Sum256(body)
	repeated := old != nil && old.generation == m.Generation
	if m.Binding.SourceGeneration < high || m.Binding.SourceGeneration == high && !repeated {
		fail("binding_changed")
		return
	}
	if repeated && (m.Binding.SourceGeneration != high || old.manifestDigest != digest) {
		fail("source_invalid")
		return
	}
	var family *machinePublicFamily
	if !repeated {
		pages := make([]string, len(m.Pages))
		jobs := make(chan int, len(pages))
		for i := range pages {
			jobs <- i
		}
		close(jobs)
		failures := make(chan string, 1)
		var workers sync.WaitGroup
		for range min(2, len(pages)) {
			workers.Go(func() {
				for i := range jobs {
					if r.starts.Wait(ctx) != nil {
						select {
						case failures <- "collection_deadline":
						default:
						}
						cancel()
						return
					}
					raw, _, _, reason := r.fetch(ctx, "?generation="+m.Generation+"&page="+strconv.Itoa(i))
					if reason == "" {
						sum := sha256.Sum256(raw)
						if len(raw) != m.Pages[i].Bytes || hex.EncodeToString(sum[:]) != m.Pages[i].Digest {
							reason = "source_invalid"
						}
					}
					if reason != "" {
						select {
						case failures <- reason:
						default:
						}
						cancel()
						return
					}
					pages[i] = string(raw)
				}
			})
		}
		workers.Wait()
		select {
		case reason := <-failures:
			fail(reason)
			return
		default:
		}
		if ctx.Err() != nil || !time.Now().Before(limit) {
			fail("collection_deadline")
			return
		}
		if err := verifyMachinePages(m, pages); err != nil {
			fail("source_invalid")
			return
		}
		family, err = preparePublicMachine(m, pages, digest, time.Now(), until, started.Add(machineDetailHoldFor))
		if err != nil {
			fail("response_limit")
			return
		}
	}
	now = time.Now()
	if ctx.Err() != nil || !now.Before(limit) {
		fail("collection_deadline")
		return
	}
	if !now.Before(until) {
		fail("source_time_invalid")
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.revision != revision || !r.gate.matches(m.Binding) || parent.Err() != nil {
		return
	}
	if reason := r.gate.failure(now); reason != "" {
		r.reason = reason
		return
	}
	if repeated {
		// Only the earlier deadline may change; same bytes never renew the
		// capture's first collection time or the original fixed page hold.
		if until.Before(old.until) {
			old.until = until
		}
		if old.stamp != before.stamp {
			if !now.Before(old.until) || !now.Before(old.holdUntil) {
				r.reason = "status_stale"
				return
			}
			old.stamp = before.stamp
		}
	} else {
		family.stamp = before.stamp
		r.items[1], r.items[0], r.sourceGeneration = r.items[0], family, m.Binding.SourceGeneration
	}
	r.reason = ""
}

// machineArraysBounded prevents a small malicious JSON array from allocating a
// large typed slice before cardinality validation. Legal dictionaries cap at256;
// section-specific limits and exact canonical field shapes are checked below.
func machineArraysBounded(body []byte) bool {
	d := json.NewDecoder(bytes.NewReader(body))
	var value func(int) bool
	value = func(depth int) bool {
		if depth > 12 {
			return false
		}
		token, err := d.Token()
		if err != nil {
			return false
		}
		delim, compound := token.(json.Delim)
		if !compound {
			return true
		}
		if delim != '[' && delim != '{' {
			return false
		}
		count := 0
		for d.More() {
			if delim == '{' {
				if _, err := d.Token(); err != nil {
					return false
				}
			}
			count++
			if delim == '[' && count > 256 || !value(depth+1) {
				return false
			}
		}
		end, err := d.Token()
		return err == nil && (delim == '[' && end == json.Delim(']') || delim == '{' && end == json.Delim('}'))
	}
	if !value(0) {
		return false
	}
	_, err := d.Token()
	return err == io.EOF
}

func decodeMachineManifest(body []byte) (MachineDetailManifest, error) {
	var m MachineDetailManifest
	bad := errors.New("source_invalid")
	if len(body) == 0 || len(body) > MaxMachineManifestBytes || !machineArraysBounded(body) || json.Unmarshal(body, &m) != nil {
		return m, bad
	}
	b := m.Binding
	if m.SchemaVersion != 1 || m.Family != "machine" || !utf8.ValidString(b.RunID) || b.RunID == "" || len(b.RunID) > 128 || !validReplayDetailTime(b.NodeStartedAt) || !validReplayDetailHash(b.GenesisHash, true) || b.SourceGeneration == 0 || b.SourceGeneration == math.MaxUint64 || !validReplayDetailTime(m.CapturedAt) || !validDetailDigest(m.Generation) || m.Bytes <= 0 || m.Bytes > MaxMachineDetailBytes || len(m.Pages) == 0 || len(m.Pages) > machineMaxPages {
		return m, bad
	}
	kinds := []string{"header", "cpus", "numa", "thread_groups", "links"}
	counts, previous := [5]int{}, 0
	for i, p := range m.Pages {
		kind := slices.Index(kinds, p.Kind)
		if kind < 0 || kind < previous || p.Page != i || p.Bytes <= 0 || p.Bytes > MaxMachinePageBytes || !validDetailDigest(p.Digest) || p.Offset != counts[kind] || p.Count < 1 || p.Count > machinePageRows || (i == 0) != (kind == 0) || kind == 0 && p.Count != 1 || i > 0 && kind == previous && m.Pages[i-1].Count != machinePageRows {
			return m, bad
		}
		counts[kind] += p.Count
		if counts[kind] > [5]int{1, 256, 64, 256, 256}[kind] {
			return m, bad
		}
		previous = kind
	}
	canonical, err := json.Marshal(m)
	if err != nil || !bytes.Equal(canonical, body) {
		return m, bad
	}
	return m, nil
}

func verifyMachinePages(m MachineDetailManifest, pages []string) error {
	bad := errors.New("source_invalid")
	if len(pages) != len(m.Pages) {
		return bad
	}
	var detail MachineDetail
	for i, body := range pages {
		desc := m.Pages[i]
		sum := sha256.Sum256([]byte(body))
		if len(body) != desc.Bytes || len(body) > MaxMachinePageBytes || hex.EncodeToString(sum[:]) != desc.Digest || !machineArraysBounded([]byte(body)) {
			return bad
		}
		var p MachineDetailPage
		if json.Unmarshal([]byte(body), &p) != nil || p.SchemaVersion != 1 || p.Family != "machine" || p.Generation != m.Generation || p.Page != i || p.Kind != desc.Kind || p.Offset != desc.Offset {
			return bad
		}
		canonical, err := json.Marshal(p)
		if err != nil || string(canonical) != body {
			return bad
		}
		count := len(p.CPUs) + len(p.Nodes) + len(p.Groups) + len(p.Links)
		if i == 0 {
			if p.Header == nil || count != 0 {
				return bad
			}
			detail = *p.Header
			if t := detail.Topology; t != nil {
				if t.CPUs != nil || t.Nodes != nil {
					return bad
				}
				t.CPUs, t.Nodes = []MachineCPU{}, []MachineNUMA{}
			}
			if a := detail.Affinity; a != nil {
				if a.Groups != nil {
					return bad
				}
				a.Groups = []MachineThreadGroup{}
			}
			if l := detail.Links; l != nil {
				if l.Rows != nil {
					return bad
				}
				l.Rows = []MachineLink{}
			}
			continue
		}
		if p.Header != nil || count != desc.Count {
			return bad
		}
		switch p.Kind {
		case "cpus":
			if detail.Topology == nil || len(p.CPUs) != count || len(detail.Topology.CPUs) != p.Offset {
				return bad
			}
			detail.Topology.CPUs = append(detail.Topology.CPUs, p.CPUs...)
		case "numa":
			if detail.Topology == nil || len(p.Nodes) != count || len(detail.Topology.Nodes) != p.Offset {
				return bad
			}
			detail.Topology.Nodes = append(detail.Topology.Nodes, p.Nodes...)
		case "thread_groups":
			if detail.Affinity == nil || len(p.Groups) != count || len(detail.Affinity.Groups) != p.Offset {
				return bad
			}
			detail.Affinity.Groups = append(detail.Affinity.Groups, p.Groups...)
		case "links":
			if detail.Links == nil || len(p.Links) != count || len(detail.Links.Rows) != p.Offset {
				return bad
			}
			detail.Links.Rows = append(detail.Links.Rows, p.Links...)
		default:
			return bad
		}
	}
	if detail.Binding != m.Binding || !detail.CapturedAt.Equal(m.CapturedAt) || validateMachineDetail(detail) != nil {
		return bad
	}
	body, err := json.Marshal(detail)
	if err != nil || len(body) != m.Bytes {
		return bad
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != m.Generation {
		return bad
	}
	return nil
}

func preparePublicMachine(m MachineDetailManifest, pages []string, digest [sha256.Size]byte, at, until, hold time.Time) (*machinePublicFamily, error) {
	f := &machinePublicFamily{binding: m.Binding, generation: m.Generation, manifestDigest: digest, pages: pages, publishedAt: at, until: until, holdUntil: hold}
	p := PublicMachineManifest{SchemaVersion: 1, Family: "machine", CollectorAt: at.UTC(), Binding: m.Binding, CapturedAt: m.CapturedAt, Generation: m.Generation, Bytes: m.Bytes, Pages: m.Pages}
	for i, reason := range machinePublicReasons {
		p.State, p.Reason = "stale", reason
		if reason == "" {
			p.State = "fresh"
		}
		body, err := json.Marshal(p)
		if err != nil || len(body) > MaxMachineManifestBytes {
			return nil, errors.New("response_limit")
		}
		f.roots[i] = string(body)
	}
	return f, nil
}

func machineReaderFailure(reason string) machineReaderReply {
	switch reason {
	case "unsupported_source", "collection_deadline":
		return machineReaderReply{machineDetailReply: machineDetailReply{status: 503, body: `{"schema_version":1,"family":"machine","state":"unavailable","reason":"` + reason + `"}`}}
	default:
		return machineReaderReply{machineDetailReply: machineDetailFailure(reason)}
	}
}

func (r *machineDetailReader) lookup(generation string, page int, pageRequest bool, now time.Time) machineReaderReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return machineReaderFailure("source_unavailable")
	}
	if reason := r.gate.failure(now); reason != "" {
		return machineReaderFailure(reason)
	}
	if !r.gate.stamp.valid() {
		return machineReaderFailure("waiting_source")
	}
	for i, f := range r.items {
		if f == nil || pageRequest && f.generation != generation || !pageRequest && i != 0 {
			continue
		}
		if !r.gate.matches(f.binding) || f.stamp != r.gate.stamp {
			return machineReaderFailure("binding_changed")
		}
		if now.Before(f.publishedAt) {
			return machineReaderFailure("source_time_invalid")
		}
		if !now.Before(f.holdUntil) {
			return machineReaderFailure("source_generation_expired")
		}
		if pageRequest {
			if page < 0 || page >= len(f.pages) {
				return machineReaderFailure("invalid_request")
			}
			return machineReaderReply{machineDetailReply: machineDetailReply{status: 200, body: f.pages[page]},
				stamp: f.stamp, family: f, revision: r.revision, pageRequest: true}
		}
		reason := r.reason
		remaining := min(f.until.Sub(now), f.holdUntil.Sub(now)).Milliseconds()
		if reason == "" && remaining < 1 {
			reason = "status_stale"
		}
		index := slices.Index(machinePublicReasons[:], reason)
		if index < 0 {
			return machineReaderFailure("source_invalid")
		}
		until := f.until
		if f.holdUntil.Before(until) {
			until = f.holdUntil
		}
		reply := machineReaderReply{machineDetailReply: machineDetailReply{status: 200, body: f.roots[index]},
			stamp: f.stamp, family: f, revision: r.revision, reason: r.reason, until: until}
		if reason == "" {
			reply.remainingMilliseconds = remaining
		}
		return reply
	}
	if pageRequest {
		return machineReaderFailure("source_generation_expired")
	}
	if r.reason != "" {
		return machineReaderFailure(r.reason)
	}
	return machineReaderFailure("waiting_source")
}

func (c *collector) admitMachineDetail(r *machineDetailReader, reply machineReaderReply) (time.Duration, bool) {
	return c.admitLiveDetail(statusMachine, reply.stamp, func() (statusStamp, bool) {
		r.mu.Lock()
		defer r.mu.Unlock()
		gate, family := r.gate, reply.family
		if r.closed || family == nil || r.revision != reply.revision || gate.stamp != reply.stamp ||
			family.stamp != reply.stamp || !gate.matches(family.binding) || gate.failure(time.Now()) != "" {
			return gate.stamp, false
		}
		if r.items[0] != family && (!reply.pageRequest || r.items[1] != family) ||
			!family.holdUntil.After(time.Now()) || !reply.pageRequest && r.reason != reply.reason ||
			reply.remainingMilliseconds > 0 && (!family.until.After(time.Now()) || family.until.Before(reply.until)) {
			return gate.stamp, false
		}
		return gate.stamp, true
	}, time.Now())
}

func (r *machineDetailReader) serveHTTP(w http.ResponseWriter, request *http.Request, admit func(machineReaderReply) (time.Duration, bool)) {
	headers := w.Header()
	headers.Set("Content-Type", "application/json")
	headers.Set("Cache-Control", "no-store")
	generation, page, pageRequest, valid := parseMachineDetailQuery(request.URL.RawQuery, request.URL.ForceQuery)
	reply := machineReaderFailure("invalid_request")
	if request.Method == http.MethodGet && request.URL.Path == machineDetailPublicPath && request.URL.EscapedPath() == machineDetailPublicPath && request.ContentLength == 0 && len(request.TransferEncoding) == 0 && valid && (!pageRequest || page < machineMaxPages) {
		if !r.limit.Allow() {
			reply = machineReaderFailure("rate_limited")
		} else {
			select {
			case r.active <- struct{}{}:
				defer func() { <-r.active }()
				reply = r.lookup(generation, page, pageRequest, time.Now())
			default:
				reply = machineReaderFailure("rate_limited")
			}
		}
	}
	if request.Context().Err() != nil {
		return
	}
	var statusRemaining time.Duration
	if reply.status == http.StatusOK {
		var ok bool
		if admit != nil {
			statusRemaining, ok = admit(reply)
		}
		if !ok || statusRemaining.Milliseconds() < 1 || reply.family == nil {
			reply = machineReaderFailure("status_stale")
		} else if reply.remainingMilliseconds > 0 {
			remaining := time.Until(reply.until).Milliseconds()
			if remaining < 1 {
				reply = machineReaderFailure("status_stale")
			} else {
				reply.remainingMilliseconds = min(reply.remainingMilliseconds, remaining)
			}
		}
	}
	if reply.status == http.StatusOK {
		headers.Set(statusRemainingHeader, strconv.FormatInt(statusRemaining.Milliseconds(), 10))
	}
	if reply.status == 200 && reply.remainingMilliseconds > 0 {
		headers.Set(replayDetailRemainingHeader, strconv.FormatInt(reply.remainingMilliseconds, 10))
	}
	w.WriteHeader(reply.status)
	_, _ = io.WriteString(w, reply.body)
}
