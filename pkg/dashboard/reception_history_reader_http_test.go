package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func receptionPublicFixture(t *testing.T, inputs []ReceptionHistoryChunkInput, metadata bool) (*receptionHistoryReader, *collector, ReceptionMetadata) {
	t.Helper()
	m := receptionCollectorMetadata()
	m.Sources = []ReceptionSource{{Generation: 1, StartedAt: m.PublishedAt, Kind: "live"}}
	root := receptionTestRoot(t, m)
	if len(inputs) != 0 {
		first, last := inputs[0].Descriptor.FirstSequence, inputs[len(inputs)-1].Descriptor.LastSequence
		root.SourceBounds = ReceptionSequenceBounds{First: &first, Last: &last}
		root.ConsumedThrough = last
	}
	r, c := receptionCollectorForTest(t, root)
	receptionAcceptTest(t, r, root, time.Second)
	for first := 0; first < len(inputs); first += 8 {
		if err := r.cache.ImportChunks(root.RecordingID, inputs[first:min(first+8, len(inputs))]); err != nil {
			t.Fatal(err)
		}
	}
	r.cursor = root.ConsumedThrough
	if metadata {
		receptionInstallTest(t, r, m)
	}
	return r, c, m
}

func receptionPublicQuery(id string, start, end time.Time, after uint64) string {
	values := url.Values{"start_time": {start.Format(time.RFC3339Nano)}, "end_time": {end.Format(time.RFC3339Nano)}, "after": {strconv.FormatUint(after, 10)}}
	return receptionHistoryPublicPath + "/query/" + id + "?" + values.Encode()
}

func receptionPublicGet(handler http.HandlerFunc, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func receptionPublicParsed(t *testing.T, path string) receptionPublicRequest {
	t.Helper()
	q, status := parseReceptionPublicRequest(httptest.NewRequest(http.MethodGet, path, nil))
	if status != 0 {
		t.Fatal("invalid test URL", path, status)
	}
	return q
}

func receptionPublicRead(t *testing.T, w *httptest.ResponseRecorder) receptionPublicRows {
	t.Helper()
	var out receptionPublicRows
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &out) != nil || len(w.Body.Bytes()) > MaxReceptionChunkBytes {
		t.Fatal("invalid row response", w.Code, w.Body.String())
	}
	canonical, err := json.Marshal(out)
	if err != nil || !bytes.Equal(canonical, w.Body.Bytes()) || out.State != "historical" || out.SchemaVersion != 1 ||
		out.Family != "reception" || out.Scope != "authenticated_socket_occurrences" || out.Columns != receptionChunkColumns ||
		out.Rows == nil || w.Header().Get(replayDetailRemainingHeader) != "" {
		t.Fatal("row schema/canonical bytes/live lease changed", err)
	}
	return out
}

func TestReceptionPublicGrammar(t *testing.T) {
	id, digest := strings.Repeat("a", 64), strings.Repeat("b", 64)
	start := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	valid := []string{receptionHistoryPublicPath, receptionHistoryPublicPath + "/index/" + id + "?after=0",
		receptionHistoryPublicPath + "/chunk/" + id + "/" + digest,
		receptionHistoryPublicPath + "/metadata/" + id + "/" + digest + "/tvu_ranges/15",
		receptionHistoryPublicPath + "/cursor/" + id + "?after=18446744073709551615",
		receptionPublicQuery(id, start, start.Add(time.Minute), 0),
		receptionPublicQuery(id, time.Date(9999, 12, 31, 23, 58, 59, 1, time.UTC), time.Date(9999, 12, 31, 23, 59, 59, 1, time.UTC), math.MaxUint64)}
	for _, path := range valid {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		before := *request.URL
		if _, status := parseReceptionPublicRequest(request); status != 0 || *request.URL != before {
			t.Fatal("valid grammar refused or request mutated", path, status)
		}
	}
	query := receptionPublicQuery(id, start, start.Add(time.Minute), 0)
	for name, path := range map[string]string{
		"prefix": "/api/v1/details/reception-extra", "slash": receptionHistoryPublicPath + "/", "root_query": receptionHistoryPublicPath + "?after=0",
		"encoded_path": strings.Replace(valid[4], "/cursor/", "/%63ursor/", 1), "extra_path": valid[4] + "/extra",
		"upper_id": strings.Replace(valid[4], id, strings.ToUpper(id), 1), "leading_zero": strings.Replace(valid[4], "18446744073709551615", "01", 1),
		"overflow": strings.Replace(valid[4], "18446744073709551615", "18446744073709551616", 1), "encoded_cursor": strings.Replace(valid[4], "18446744073709551615", "%31", 1),
		"negative": strings.Replace(valid[4], "18446744073709551615", "-1", 1), "unknown": query + "&x=1", "duplicate": query + "&after=0",
		"missing": strings.Replace(query, "after=0&", "", 1), "order": strings.Replace(query, "after=0&", "", 1) + "&after=0",
		"offset": strings.Replace(query, "Z", "%2B00%3A00", 1), "fraction_alias": strings.Replace(query, "Z", ".0Z", 1),
		"span_plus_ns": receptionPublicQuery(id, start, start.Add(time.Minute+time.Nanosecond), 0), "equal": receptionPublicQuery(id, start, start, 0),
		"reverse": receptionPublicQuery(id, start.Add(time.Second), start, 0), "too_long": query + strings.Repeat("x", 513),
		"metadata_page": strings.Replace(valid[3], "/15", "/16", 1), "unknown_kind": strings.Replace(valid[3], "tvu_ranges", "all", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, status := parseReceptionPublicRequest(httptest.NewRequest(http.MethodGet, path, nil)); status != http.StatusBadRequest {
				t.Fatal("malformed grammar accepted", status)
			}
		})
	}
	for _, change := range []func(*http.Request){
		func(r *http.Request) { r.Method = http.MethodHead }, func(r *http.Request) { r.ContentLength = 1 },
		func(r *http.Request) { r.ContentLength = -1 }, func(r *http.Request) { r.TransferEncoding = []string{"chunked"} },
		func(r *http.Request) { r.URL.ForceQuery = true },
	} {
		r := httptest.NewRequest(http.MethodGet, valid[4], nil)
		change(r)
		if _, status := parseReceptionPublicRequest(r); status != http.StatusBadRequest {
			t.Fatal("unsupported request accepted")
		}
	}
	private := httptest.NewRequest(http.MethodGet, receptionHistoryPath+"/cursor/"+id+"?after=0", nil)
	if _, status := parseReceptionHistoryRequest(private); status != http.StatusNotFound {
		t.Fatal("public parser exposed private cursor")
	}
}

func TestReceptionPublicRowsWireAndMaxima(t *testing.T) {
	first := uint64(math.MaxUint64 - 127)
	input := receptionReaderInput(receptionChunkFixture(t, first, 128, true))
	r, _, _ := receptionPublicFixture(t, []ReceptionHistoryChunkInput{input}, true)
	r.cacheGaps = math.MaxUint64
	path := receptionHistoryPublicPath + "/cursor/" + r.last.RecordingID + "?after=" + strconv.FormatUint(first-1, 10)
	w := receptionPublicGet(r.publicHandler(), path)
	out := receptionPublicRead(t, w)
	var original receptionChunkWire
	if json.Unmarshal([]byte(input.Body), &original) != nil || len(out.Rows) != 128 || out.After != first-1 || out.ScannedThrough != math.MaxUint64 ||
		out.ScannedRows != 128 || out.ScannedChunks != 1 || out.More || out.CacheGaps != math.MaxUint64 || out.StartTime != nil || out.EndTime != nil {
		t.Fatal("full-width row envelope lost precision")
	}
	for i, row := range out.Rows {
		if row != original.Rows.values[i] {
			t.Fatal("occurrence changed", i)
		}
	}
	t.Logf("public full-width cursor=%d/%d bytes; existing reader model includes32 response buffers and33 fixed results", w.Body.Len(), MaxReceptionChunkBytes)
	q := receptionPublicParsed(t, path)
	read, err := r.cache.Cursor(context.Background(), q.recording, q.after)
	if err != nil {
		t.Fatal(err)
	}
	// A separately captured source scalar may precede the cache read. It does
	// not authorize rejecting or relabeling those correctly bound later rows.
	source := receptionPublicSourceCapture{PublishedAt: r.last.PublishedAt, ConsumedThrough: 1}
	if _, err := prepareReceptionPublicRows(q, source, 0, &read); err != nil {
		t.Fatal("independent source/cache captures were forced into one instant", err)
	}
	for name, change := range map[string]func(*ReceptionHistoryRead){
		"129_rows": func(v *ReceptionHistoryRead) { v.Count = 129 }, "negative": func(v *ReceptionHistoryRead) { v.Count = -1 },
		"bad_cursor": func(v *ReceptionHistoryRead) { v.ScannedThrough = v.After - 1 }, "duplicate": func(v *ReceptionHistoryRead) { v.Rows[1].Sequence = v.Rows[0].Sequence },
		"scans": func(v *ReceptionHistoryRead) { v.ScannedRows = 32769 }, "chunks": func(v *ReceptionHistoryRead) { v.ScannedChunks = 257 },
		"more": func(v *ReceptionHistoryRead) { v.More = true },
	} {
		t.Run(name, func(t *testing.T) {
			v := read
			change(&v)
			if _, err := prepareReceptionPublicRows(q, source, 0, &v); err == nil {
				t.Fatal("invalid detached row result encoded")
			}
		})
	}
	inside := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath+"/cursor/"+q.recording+"?after="+strconv.FormatUint(first+126, 10))
	if got := receptionPublicRead(t, inside); len(got.Rows) != 1 || got.ScannedThrough != math.MaxUint64 {
		t.Fatal("inside-chunk cursor lost final occurrence")
	}
	empty := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath+"/cursor/"+q.recording+"?after=18446744073709551615")
	if got := receptionPublicRead(t, empty); len(got.Rows) != 0 || got.ScannedThrough != math.MaxUint64 || got.More {
		t.Fatal("maximum future cursor wrapped")
	}
	at := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	dated := receptionReaderDatedInput(t, 1, []time.Time{at.Add(70 * time.Second), at.Add(2 * time.Second), at.Add(time.Second), at.Add(time.Minute)})
	r, _, _ = receptionPublicFixture(t, []ReceptionHistoryChunkInput{dated}, false)
	query := receptionPublicQuery(r.last.RecordingID, at, at.Add(time.Minute), 0)
	out = receptionPublicRead(t, receptionPublicGet(r.publicHandler(), query))
	if len(out.Rows) != 2 || out.Rows[0].Sequence != 2 || out.Rows[1].Sequence != 3 || out.ScannedThrough != 4 || out.ScannedRows != 4 || out.More || !out.StartTime.Equal(at) {
		t.Fatal("query stopped at future date or included exclusive endpoint")
	}
	query = receptionPublicQuery(r.last.RecordingID, at.Add(-time.Minute), at, 0)
	out = receptionPublicRead(t, receptionPublicGet(r.publicHandler(), query))
	if len(out.Rows) != 0 || out.ScannedThrough != 4 || out.ScannedRows != 4 {
		t.Fatal("nonmatching scan did not advance")
	}
	t.Run("full_retained_data", func(t *testing.T) {
		first := uint64(math.MaxUint64 - maxReceptionChunks*128 + 1)
		inputs := make([]ReceptionHistoryChunkInput, 0, maxReceptionChunks)
		for i := range maxReceptionChunks {
			inputs = append(inputs, receptionReaderInput(receptionChunkFixture(t, first+uint64(i*128), 128, true)))
		}
		r, _, _ := receptionPublicFixture(t, inputs, false)
		query := receptionPublicQuery(r.last.RecordingID, at, at.Add(time.Minute), first-1)
		got := receptionPublicRead(t, receptionPublicGet(r.publicHandler(), query))
		if len(got.Rows) != 0 || got.ScannedRows != 32768 || got.ScannedChunks != 256 || got.ScannedThrough != math.MaxUint64 || got.More {
			t.Fatal("public query did not scan the complete retained maximum")
		}
	})
}

func TestReceptionPublicHistoricalAvailability(t *testing.T) {
	input := receptionReaderInput(receptionChunkFixture(t, 1, 2, false))
	r, c, m := receptionPublicFixture(t, []ReceptionHistoryChunkInput{input}, false)
	id := r.last.RecordingID
	chunkPath := receptionHistoryPublicPath + "/chunk/" + id + "/" + input.Descriptor.Digest
	if w := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath); w.Code != 503 || !strings.Contains(w.Body.String(), `"reason":"waiting_source"`) {
		t.Fatal("unfinished metadata root advertised", w.Code, w.Body.String())
	}
	w := receptionPublicGet(r.publicHandler(), chunkPath)
	if w.Code != 200 || w.Body.String() != input.Body || w.Header().Get("ETag") != `"`+input.Descriptor.Digest+`"` || w.Header().Get(replayDetailRemainingHeader) != "" {
		t.Fatal("explicit historical chunk requires completed root")
	}
	page := r.last.Metadata.Pages[0]
	metaPath := receptionHistoryPublicPath + "/metadata/" + id + "/" + r.last.Metadata.Digest + "/sources/0"
	if w := receptionPublicGet(r.publicHandler(), metaPath); w.Code != 409 {
		t.Fatal("unfinished page exposed")
	}
	receptionInstallTest(t, r, m)
	w = receptionPublicGet(r.publicHandler(), metaPath)
	if w.Code != 200 || w.Header().Get("ETag") != `"`+page.Digest+`"` || w.Header().Get(replayDetailRemainingHeader) != "" {
		t.Fatal("completed historical metadata absent")
	}
	c.observeReceptionHistoryStatus("unavailable", time.Now())
	if w := receptionPublicGet(r.publicHandler(), chunkPath); w.Code != 200 || w.Body.String() != input.Body {
		t.Fatal("same-identity status loss erased explicit history")
	}
	if w := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath); w.Code != 409 {
		t.Fatal("old publication epoch became current after status loss")
	}
	c.observeReceptionHistoryStatus("fresh", time.Now())
	if w := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath); w.Code != 409 {
		t.Fatal("status-only recovery revived old root")
	}
	receptionAcceptTest(t, r, r.last, time.Second)
	if err := r.publish(context.Background(), r.status(), true); err != nil {
		t.Fatal(err)
	}
	if w := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath); w.Code != 200 {
		t.Fatal("current reaccepted root unavailable")
	}
	for name, inputs := range map[string][]ReceptionHistoryChunkInput{
		"prefix":   {receptionReaderInput(receptionChunkFixture(t, 10, 1, false))},
		"interior": {input, receptionReaderInput(receptionChunkFixture(t, 4, 1, false))},
		"tail":     {input},
	} {
		t.Run(name, func(t *testing.T) {
			r, _, _ := receptionPublicFixture(t, inputs, false)
			after := "0"
			if name == "tail" {
				r.cursor = 3
				after = "2"
			}
			for _, kind := range []string{"cursor", "index"} {
				if name == "interior" && kind == "index" {
					after = "2"
				}
				w := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath+"/"+kind+"/"+r.last.RecordingID+"?after="+after)
				if w.Code != 409 || !strings.Contains(w.Body.String(), `"reason":"cache_gap"`) || strings.Contains(w.Body.String(), `"rows"`) {
					t.Fatal("missing interval became successful empty/partial history", kind, w.Code, w.Body.String())
				}
				if name == "tail" && !strings.Contains(w.Body.String(), `"next_sequence":null`) {
					t.Fatal("missing tail invented a next row")
				}
			}
		})
	}
	// Consumed-through alone does not establish an import loss.
	r, _, _ = receptionPublicFixture(t, nil, false)
	r.last.ConsumedThrough = 100
	out := receptionPublicRead(t, receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath+"/cursor/"+r.last.RecordingID+"?after=0"))
	if len(out.Rows) != 0 || out.ScannedThrough != 0 || out.SourceCapture.ConsumedThrough != 100 {
		t.Fatal("pending source rows became invented gap/completion")
	}
	t.Run("cached_requests_do_not_collect", func(t *testing.T) {
		m := receptionCollectorMetadata()
		m.Sources = []ReceptionSource{{Generation: 1, StartedAt: m.PublishedAt, Kind: "live"}}
		chunk := receptionChunkFixture(t, 1, 1, false)
		peer, reader, _ := newReceptionCollectorPeer(t, m, []PreparedReceptionChunk{chunk})
		reader.collect(context.Background())
		before, _ := peer.counters()
		id := reader.last.RecordingID
		h := reader.publicHandler()
		for _, path := range []string{receptionHistoryPublicPath, receptionHistoryPublicPath + "/index/" + id + "?after=0",
			receptionHistoryPublicPath + "/cursor/" + id + "?after=0", receptionHistoryPublicPath + "/chunk/" + id + "/" + chunk.descriptor.Digest,
			receptionHistoryPublicPath + "/metadata/" + id + "/" + reader.last.Metadata.Digest + "/sources/0"} {
			if w := receptionPublicGet(h, path); w.Code != 200 {
				t.Fatal("cached route missing", path, w.Code, w.Body.String())
			}
		}
		if after, _ := peer.counters(); after != before {
			t.Fatal("public cached request caused private collection", before, after)
		}
	})
}

func TestReceptionPublicSelectedReplyFences(t *testing.T) {
	for _, state := range []string{"fresh", "stale", "closed", "interrupted", "exhausted"} {
		for _, transition := range []string{"other_node", "unknown", "recording"} {
			t.Run(state+"/"+transition, func(t *testing.T) {
				input := receptionReaderInput(receptionChunkFixture(t, 1, 1, false))
				r, c, m := receptionPublicFixture(t, []ReceptionHistoryChunkInput{input}, true)
				root := r.last
				root.State, root.Closed, root.Interrupted, root.Exhausted = state, state == "closed", state == "interrupted", state == "exhausted"
				root.Startup.Limited = root.Interrupted || root.Exhausted
				remaining := time.Duration(0)
				if state == "fresh" {
					remaining = time.Second
				}
				receptionAcceptTest(t, r, root, remaining)
				if err := r.publish(context.Background(), r.status(), true); err != nil {
					t.Fatal(err)
				}
				paths := []string{receptionHistoryPublicPath, receptionHistoryPublicPath + "/chunk/" + root.RecordingID + "/" + input.Descriptor.Digest,
					receptionHistoryPublicPath + "/cursor/" + root.RecordingID + "?after=0", receptionHistoryPublicPath + "/index/" + root.RecordingID + "?after=0",
					receptionHistoryPublicPath + "/metadata/" + root.RecordingID + "/" + root.Metadata.Digest + "/sources/0",
					receptionHistoryPublicPath + "/chunk/" + root.RecordingID + "/" + strings.Repeat("a", 64)}
				selected := make([]receptionPublicSelection, len(paths))
				for i, path := range paths {
					selected[i] = r.selectPublic(context.Background(), receptionPublicParsed(t, path), time.Now())
				}
				for i, selection := range selected {
					want := 200
					if i == len(selected)-1 {
						want = 409
					}
					if selection.status != want {
						t.Fatal("selection fixture invalid", i, selection.status, selection.reason)
					}
				}
				saved := *c.last
				switch transition {
				case "other_node":
					c.last.RunID = "other-node"
					c.observeReceptionHistoryStatus("fresh", time.Now())
					*c.last = saved
					c.observeReceptionHistoryStatus("fresh", time.Now())
				case "unknown":
					c.observeReceptionHistoryStatus("unavailable", time.Now())
					c.observeReceptionHistoryStatus("fresh", time.Now())
				case "recording":
					m.Binding.RecordingGeneration++
					replacement := receptionTestRoot(t, m)
					receptionAcceptTest(t, r, replacement, time.Second)
				}
				for _, old := range selected {
					got := r.admitPublic(old, time.Now())
					if got.status != 409 || got.reason != "binding_changed" || got.body != "" || len(got.encoded) != 0 || got.recording != "" || got.bounds.First != nil {
						t.Fatal("selected old context survived admission", got.status, got.reason)
					}
				}
			})
		}
	}
	t.Run("lease_and_token", func(t *testing.T) {
		input := receptionReaderInput(receptionChunkFixture(t, 1, 1, false))
		r, _, _ := receptionPublicFixture(t, []ReceptionHistoryChunkInput{input}, true)
		selected := r.selectPublic(context.Background(), receptionPublicParsed(t, receptionHistoryPublicPath), time.Now())
		got := r.admitPublic(selected, selected.expires)
		root, err := decodeReceptionHistoryRoot([]byte(got.body))
		if err != nil || root.State != "stale" || got.remaining != 0 {
			t.Fatal("original lease was renewed at admission", err)
		}
		path := receptionHistoryPublicPath + "/metadata/" + r.last.RecordingID + "/" + r.last.Metadata.Digest + "/sources/0"
		page := r.selectPublic(context.Background(), receptionPublicParsed(t, path), time.Now())
		until := receptionPublicMetadataUntil(r.cache, r.last.RecordingID, r.last.Metadata.Digest, time.Now())
		got = r.admitPublic(page, until)
		if got.status != 409 || got.reason != "metadata_expired" || got.body != "" || got.remaining != 0 {
			t.Fatal("expired token survived admission")
		}
	})
}

func TestReceptionPublicLimitsAndScanBudget(t *testing.T) {
	input := receptionReaderInput(receptionChunkFixture(t, 1, 1, false))
	r, _, _ := receptionPublicFixture(t, []ReceptionHistoryChunkInput{input}, true)
	id := r.last.RecordingID
	at := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	handler := r.publicHandler()
	query := receptionPublicQuery(id, at, at.Add(time.Minute), 0)
	for i := 0; i < 3; i++ {
		w := receptionPublicGet(handler, query)
		want := 200
		if i == 2 {
			want = 429
		}
		if w.Code != want {
			t.Fatal("query2/burst2", i, w.Code, w.Body.String())
		}
	}
	if w := receptionPublicGet(handler, receptionHistoryPublicPath+"/cursor/"+id+"?after=0"); w.Code != 200 {
		t.Fatal("query limiter consumed body budget")
	}
	r.cache.scanMu.Lock()
	w := receptionPublicGet(r.publicHandler(), query)
	r.cache.scanMu.Unlock()
	if w.Code != 429 || !strings.Contains(w.Body.String(), `"reason":"query_budget"`) {
		t.Fatal("overlapping scan did not fail bounded", w.Code, w.Body.String())
	}
	for _, group := range []string{"body", "metadata"} {
		t.Run(group, func(t *testing.T) {
			h := r.publicHandler()
			path := receptionHistoryPublicPath
			if group == "body" {
				path = receptionHistoryPublicPath + "/chunk/" + id + "/" + input.Descriptor.Digest
			}
			refused := false
			for range 64 {
				if w := receptionPublicGet(h, path); w.Code == 429 {
					refused = true
					break
				}
			}
			if !refused {
				t.Fatal("fixed burst was not bounded")
			}
			other := receptionHistoryPublicPath
			if group == "metadata" {
				other = receptionHistoryPublicPath + "/chunk/" + id + "/" + input.Descriptor.Digest
			}
			if w := receptionPublicGet(h, other); w.Code != 200 {
				t.Fatal("independent rate groups conflated", w.Code)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w = httptest.NewRecorder()
	r.publicHandler()(w, httptest.NewRequest(http.MethodGet, query, nil).WithContext(ctx))
	if w.Body.Len() != 0 {
		t.Fatal("canceled request wrote a response")
	}
}

type receptionPublicHeldWriter struct {
	http.ResponseWriter
	ready   chan<- struct{}
	release <-chan struct{}
}

func (w receptionPublicHeldWriter) Write(body []byte) (int, error) {
	w.ready <- struct{}{}
	<-w.release
	return w.ResponseWriter.Write(body)
}
func (w receptionPublicHeldWriter) WriteString(body string) (int, error) {
	w.ready <- struct{}{}
	<-w.release
	return io.WriteString(w.ResponseWriter, body)
}

func TestReceptionPublicThirtyTwoReplyHolders(t *testing.T) {
	var inputs []ReceptionHistoryChunkInput
	for i := range 32 {
		inputs = append(inputs, receptionReaderInput(receptionChunkFixture(t, uint64(1+i*128), 128, true)))
	}
	r, _, _ := receptionPublicFixture(t, inputs, true)
	id := r.last.RecordingID
	var rows [33]ReceptionHistoryRead
	for i := range rows {
		var err error
		rows[i], err = r.cache.Cursor(context.Background(), id, 0)
		if err != nil || rows[i].Count != 128 {
			t.Fatal("holder rows", err)
		}
	}
	ready, release := make(chan struct{}, 32), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	h := r.publicHandler()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		h(receptionPublicHeldWriter{w, ready, release}, request)
	}))
	defer func() { unblock(); s.Close() }()
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxConnsPerHost: 32}}
	defer client.CloseIdleConnections()
	results := make(chan error, 32)
	for _, input := range inputs {
		go func(input ReceptionHistoryChunkInput) {
			response, err := client.Get(s.URL + receptionHistoryPublicPath + "/chunk/" + id + "/" + input.Descriptor.Digest)
			if err == nil {
				var body []byte
				body, err = io.ReadAll(io.LimitReader(response.Body, MaxReceptionChunkBytes+1))
				response.Body.Close()
				if err == nil && (response.StatusCode != 200 || string(body) != input.Body || response.Header.Get(replayDetailRemainingHeader) != "") {
					err = io.ErrUnexpectedEOF
				}
			}
			results <- err
		}(input)
		time.Sleep(20 * time.Millisecond) // Below the shared64/s rate; do not bypass it.
	}
	for range 32 {
		select {
		case <-ready:
		case <-time.After(3 * time.Second):
			unblock()
			t.Fatal("32 real cached replies did not reach write barrier")
		}
	}
	binding := r.last.Binding
	binding.RecordingGeneration++
	if err := r.cache.Reset(binding); err != nil {
		unblock()
		t.Fatal(err)
	}
	unblock()
	for range 32 {
		if err := <-results; err != nil {
			t.Fatal("immutable held body changed", err)
		}
	}
	for _, row := range rows {
		if row.Count != 128 || row.Rows[0].Sequence != 1 || row.Rows[127].Sequence != 128 {
			t.Fatal("detached result changed after reset")
		}
	}
	if unsafe.Sizeof(receptionPublicSelection{}) > 1024 || unsafe.Sizeof(receptionPublicRows{}) > 1024 {
		t.Fatal("HTTP scalar/header allowance exceeded")
	}
	t.Logf("actual32 HTTP bodies +33 detached results; existing64MiB complete model unchanged; response headers=%d+%dB", unsafe.Sizeof(receptionPublicSelection{}), unsafe.Sizeof(receptionPublicRows{}))
}

func TestReceptionPublicActualFeedReaderHTTP(t *testing.T) {
	capture, bodies, snapshot := receptionSourceFixture(t, 2)
	var captures, bodyReads atomic.Uint64
	sample := ReceptionHistorySampler{Metadata: func() (ReceptionHistoryCapture, error) { captures.Add(1); return capture, nil }, ChunkBody: func(b ReceptionRecordingBinding, d ReceptionChunkDescriptor) (string, bool) {
		bodyReads.Add(1)
		if b != capture.Metadata.Binding {
			return "", false
		}
		body, ok := bodies[d.Digest]
		return body, ok
	}}
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { s := snapshot; s.SampledAt = time.Now().UTC(); return s }, DetailSamplers{Reception: &sample})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serveProfile(ctx, path, l, "https://dashboard.test", PublicOptions{Resources: true, ReplayDetails: true, NetworkDetails: true, ReceptionHistory: true})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(2 * time.Second):
			t.Error("public server did not join")
		}
	})
	client := &http.Client{Timeout: time.Second}
	t.Cleanup(client.CloseIdleConnections)
	get := func(path, host, origin string) (int, []byte, http.Header) {
		t.Helper()
		request, err := http.NewRequest(http.MethodGet, "http://"+l.Addr().String()+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Host = host
		if origin != "" {
			request.Header.Set("Origin", origin)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(io.LimitReader(response.Body, MaxReceptionChunkBytes+1))
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, body, response.Header
	}
	var root ReceptionHistoryRoot
	var lastRootPoll time.Time
	waitReceptionTest(t, func() bool {
		// Root and metadata share a bounded limiter; polling at 1 ms can
		// exhaust it before the metadata assertion on a slow startup.
		if time.Since(lastRootPoll) < 25*time.Millisecond {
			return false
		}
		lastRootPoll = time.Now()
		status, body, _ := get(receptionHistoryPublicPath, "dashboard.test", "")
		if status != 200 {
			return false
		}
		var e error
		root, e = decodeReceptionHistoryRoot(body)
		return e == nil && root.CacheBounds.Last != nil && *root.CacheBounds.Last == 2
	})
	for _, headers := range [][2]string{{"attacker.test", ""}, {"dashboard.test", "https://attacker.test"}} {
		status, _, _ := get(receptionHistoryPublicPath, headers[0], headers[1])
		if status != 403 {
			t.Fatal("Host/Origin boundary bypass", status)
		}
	}
	status, body, headers := get(receptionHistoryPublicPath+"/cursor/"+root.RecordingID+"?after=0", "dashboard.test", "https://dashboard.test")
	var out receptionPublicRows
	if status != 200 || json.Unmarshal(body, &out) != nil || len(out.Rows) != 2 || out.Rows[0].Sequence != 1 || out.Rows[1].Sequence != 2 || headers.Get(replayDetailRemainingHeader) != "" {
		t.Fatal("actual Feed/reader/HTTP row projection", status, string(body))
	}
	d := capture.Chunks[0]
	status, body, headers = get(receptionHistoryPublicPath+"/chunk/"+root.RecordingID+"/"+d.Digest, "dashboard.test", "")
	if status != 200 || string(body) != bodies[d.Digest] || headers.Get("ETag") != `"`+d.Digest+`"` {
		t.Fatal("actual canonical body changed")
	}
	page := root.Metadata.Pages[0]
	status, body, headers = get(receptionHistoryPublicPath+"/metadata/"+root.RecordingID+"/"+root.Metadata.Digest+"/sources/0", "dashboard.test", "")
	if status != 200 || receptionDigest(body) != page.Digest || headers.Get("ETag") != `"`+page.Digest+`"` {
		t.Fatalf("actual metadata unavailable: status=%d body=%s etag=%q expected=%q", status, body, headers.Get("ETag"), page.Digest)
	}
	// Stop the actual upstream worker/Unix service, not the reader. Historical
	// requests must then succeed from the already installed immutable cache.
	feed.Close()
	beforeCaptures, beforeBodies := captures.Load(), bodyReads.Load()
	for range 3 {
		status, body, headers = get(receptionHistoryPublicPath+"/chunk/"+root.RecordingID+"/"+d.Digest, "dashboard.test", "")
		if status != 200 || string(body) != bodies[d.Digest] || headers.Get(replayDetailRemainingHeader) != "" {
			t.Fatal("upstream loss erased historical cache", status)
		}
	}
	if captures.Load() != beforeCaptures || bodyReads.Load() != beforeBodies {
		t.Fatal("public handler called stopped source")
	}
	if !reflect.DeepEqual(out.Columns, receptionChunkColumns) {
		t.Fatal("columns changed")
	}
}
