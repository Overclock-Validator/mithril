package dashboard

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"golang.org/x/time/rate"
)

func TestReceptionSourceReplyHolderHeaders(t *testing.T) {
	if bytes := unsafe.Sizeof(receptionHistoryReply{}) + unsafe.Sizeof(receptionHistorySelection{}); bytes > 1024 {
		t.Fatalf("reply and selection headers = %d bytes, want <=1024", bytes)
	}
}

func receptionSourceFixture(t *testing.T, chunks int) (ReceptionHistoryCapture, map[string]string, Snapshot) {
	t.Helper()
	at := time.Now()
	m := ReceptionMetadata{Binding: ReceptionRecordingBinding{RunID: "reception-test", NodeStartedAt: at.Add(-time.Hour).UTC(),
		GenesisHash: (solana.Hash{1}).String(), RecordingGeneration: 1}, PublishedAt: at.UTC(), StartupAtCapture: ReceptionStartup{State: "open"},
		Sources: []ReceptionSource{{Generation: 1, StartedAt: at.Add(-time.Minute).UTC(), Kind: "live"}}}
	c := ReceptionHistoryCapture{Metadata: m, PublishedAt: at, ConsumedThrough: uint64(chunks)}
	bodies := make(map[string]string)
	for i := 1; i <= chunks; i++ {
		p := receptionChunkFixture(t, uint64(i), 1, false)
		c.Chunks = append(c.Chunks, p.Descriptor())
		bodies[p.Descriptor().Digest] = p.Body()
	}
	snapshot := testSnapshot(at.UTC())
	snapshot.RunID, snapshot.StartedAt = m.Binding.RunID, m.Binding.NodeStartedAt
	genesis := m.Binding.GenesisHash
	snapshot.GenesisHash = &genesis
	return c, bodies, snapshot
}

func receptionSourceForTest(c *ReceptionHistoryCapture, bodies map[string]string, snapshot Snapshot) *receptionHistorySource {
	s := newReceptionHistorySource(ReceptionHistorySampler{
		Metadata: func() (ReceptionHistoryCapture, error) { return *c, nil },
		ChunkBody: func(b ReceptionRecordingBinding, d ReceptionChunkDescriptor) (string, bool) {
			if b != c.Metadata.Binding {
				return "", false
			}
			body, ok := bodies[d.Digest]
			return body, ok
		},
	})
	s.observeStatus(snapshot, time.Now())
	return s
}

func receptionSourceRoot(t *testing.T, s *receptionHistorySource, now time.Time) (ReceptionHistoryRoot, receptionHistoryReply) {
	t.Helper()
	reply := s.admitReply(s.reply(receptionHistoryRequest{kind: "root"}, now), now)
	root, err := decodeReceptionHistoryRoot([]byte(reply.body))
	if reply.status != 200 || err != nil {
		t.Fatalf("root status=%d error=%v body=%s", reply.status, err, reply.body)
	}
	return root, reply
}

func TestReceptionSourceImportsAndGaps(t *testing.T) {
	c, bodies, snapshot := receptionSourceFixture(t, 10)
	s := receptionSourceForTest(&c, bodies, snapshot)
	var order []uint64
	original := s.sample.ChunkBody
	s.sample.ChunkBody = func(b ReceptionRecordingBinding, d ReceptionChunkDescriptor) (string, bool) {
		order = append(order, d.FirstSequence)
		if d.FirstSequence == 2 || d.FirstSequence == 10 {
			return "", false
		}
		return original(b, d)
	}
	s.poll(context.Background())
	root, _ := receptionSourceRoot(t, s, time.Now())
	if !reflect.DeepEqual(order, []uint64{1, 2, 3, 4, 5, 6, 7, 8}) || *root.SourceBounds.Last != 10 || *root.CacheBounds.Last != 8 {
		t.Fatal("oldest-eight import bound differs")
	}
	gap := s.reply(receptionHistoryRequest{kind: "index", recording: s.id, after: 1}, time.Now())
	if gap.status != 409 || !strings.Contains(gap.body, `"reason":"cache_gap"`) || !strings.Contains(gap.body, `"next_sequence":"3"`) {
		t.Fatal("missing body did not become an explicit gap")
	}
	s.poll(context.Background())
	if !reflect.DeepEqual(order, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}) {
		t.Fatal("body attempts repeated or import skipped")
	}
	gap = s.reply(receptionHistoryRequest{kind: "index", recording: s.id, after: 9}, time.Now())
	if gap.status != 409 {
		t.Fatal("missing tail silently became complete empty index")
	}
	future := s.reply(receptionHistoryRequest{kind: "index", recording: s.id, after: math.MaxUint64}, time.Now())
	if future.status != 200 {
		t.Fatal("future cursor invented a loss")
	}
	s.poll(context.Background())
	if len(order) != 10 {
		t.Fatal("unchanged source repeated lookups")
	}
	root, _ = receptionSourceRoot(t, s, time.Now())
	if root.Startup.Losses != (ReceptionLosses{}) {
		t.Fatal("cache gap invented source/network loss")
	}
}

func TestReceptionSourceOriginalClocksFailureAndRecovery(t *testing.T) {
	logs := captureFeedLogs(t)
	c, bodies, snapshot := receptionSourceFixture(t, 1)
	s := receptionSourceForTest(&c, bodies, snapshot)
	// Use the same clock origin for the simulated receipt and replies.
	s.observeStatus(snapshot, c.PublishedAt)
	s.poll(context.Background())
	root, first := receptionSourceRoot(t, s, c.PublishedAt.Add(time.Millisecond))
	deadline, metadataAt := s.freshUntil, root.Metadata.PublishedAt
	if first.remaining != receptionHistoryFreshFor.Milliseconds()-1 || deadline != c.PublishedAt.Add(receptionHistoryFreshFor) {
		t.Fatal("lease did not retain source monotonic origin")
	}
	if delayed := s.reply(receptionHistoryRequest{kind: "root"}, c.PublishedAt.Add(1400*time.Millisecond)); delayed.remaining != 1600 {
		t.Fatal("ordinary request latency exhausted the source lease", delayed.remaining)
	}
	s.observeStatus(snapshot, time.Now())
	s.poll(context.Background())
	if s.freshUntil != deadline {
		t.Fatal("identical source renewed liveness")
	}
	stale, reply := receptionSourceRoot(t, s, deadline)
	if stale.State != "stale" || reply.remaining != 0 || !stale.PublishedAt.Equal(root.PublishedAt) {
		t.Fatal("expiry redated or removed history")
	}
	s.sample.Metadata = func() (ReceptionHistoryCapture, error) {
		return ReceptionHistoryCapture{}, errors.New("private-canary-error/path?secret=value")
	}
	for range 3 {
		s.poll(context.Background())
	}
	s.fail(s.gate, "source_time_invalid")
	stale, reply = receptionSourceRoot(t, s, c.PublishedAt.Add(time.Millisecond))
	if stale.State != "stale" || reply.remaining != 0 || !stale.Metadata.PublishedAt.Equal(metadataAt) || s.freshUntil != deadline {
		t.Fatal("failure renewed or erased retained capture")
	}
	chunk := s.reply(receptionHistoryRequest{kind: "chunk", recording: s.id, digest: c.Chunks[0].Digest}, time.Now())
	if chunk.status != 200 || chunk.body != bodies[c.Chunks[0].Digest] {
		t.Fatal("source loss revoked historical body")
	}
	s.sample.Metadata = func() (ReceptionHistoryCapture, error) { return c, nil }
	s.poll(context.Background())
	if s.freshUntil != deadline {
		t.Fatal("recovery renewed an identical capture")
	}
	records := logs.records(t)
	if len(records) != 2 || records[0]["reason"] != "source_unavailable" || records[1]["reason"] != "capture_accepted" {
		t.Fatalf("wrong transition logs: %v", records)
	}
	for _, r := range records {
		b, _ := json.Marshal(r)
		if strings.Contains(string(b), "private-canary") || r["component"] != "dashboard" || r["operation"] != "reception_prepare" || r["entrypoint"] != "private_feed" || r["run_id"] != snapshot.RunID {
			t.Fatal("unsafe or unbounded failure log")
		}
	}
	// Liveness requires the original raw clock, not UTC reconstructed from wire.
	for name, mutate := range map[string]func(*ReceptionHistoryCapture){
		"no_monotonic": func(v *ReceptionHistoryCapture) { v.PublishedAt = v.PublishedAt.UTC() },
		"future_monotonic": func(v *ReceptionHistoryCapture) {
			v.PublishedAt = time.Now().Add(time.Hour)
			v.Metadata.PublishedAt = v.PublishedAt.UTC()
		},
		"different_wall_copy": func(v *ReceptionHistoryCapture) { v.Metadata.PublishedAt = v.Metadata.PublishedAt.Add(time.Nanosecond) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := c
			mutate(&bad)
			if _, err := validateReceptionHistoryCapture(bad, time.Now()); err == nil {
				t.Fatal("invalid original clock accepted")
			}
		})
	}
}

func TestReceptionSourceMetadataHoldAndTerminalStates(t *testing.T) {
	c, bodies, snapshot := receptionSourceFixture(t, 1)
	s := receptionSourceForTest(&c, bodies, snapshot)
	s.poll(context.Background())
	first, _ := receptionSourceRoot(t, s, time.Now())
	originalInstalled := s.cache.metadataInstalledAt
	c.PublishedAt = time.Now()
	c.Metadata.PublishedAt = c.PublishedAt.UTC()
	c.Metadata.Sources[0].Retired = true
	c.Closed = true
	c.Metadata.StartupAtCapture = ReceptionStartup{State: "unknown", Limited: true}
	s.poll(context.Background())
	closed, reply := receptionSourceRoot(t, s, time.Now())
	if closed.State != "closed" || closed.Interrupted || reply.remaining != 0 || closed.Startup.State != "unknown" || closed.Metadata.Digest != first.Metadata.Digest || closed.Metadata.StartupAtCapture.State != "open" {
		t.Fatal("terminal root relabeled old frozen metadata")
	}
	// Model an elapsed original hold without a test-only production interval.
	s.metadataCheckedAt = time.Now().Add(-receptionMetadataHold)
	s.cache.metadataInstalledAt = s.metadataCheckedAt
	s.poll(context.Background())
	second, _ := receptionSourceRoot(t, s, time.Now())
	if second.Metadata.Revision != first.Metadata.Revision+1 || second.Metadata.Digest == first.Metadata.Digest || second.Metadata.StartupAtCapture.State != "unknown" {
		t.Fatal("eligible metadata did not replace")
	}
	oldPage := s.reply(receptionHistoryRequest{kind: "metadata", recording: s.id, digest: first.Metadata.Digest, metadataKind: "sources", page: 0}, time.Now())
	if oldPage.status != 200 || oldPage.etag != first.Metadata.Pages[0].Digest || oldPage.remaining != 0 {
		t.Fatal("previous metadata page/digest was lost")
	}
	secondInstalled := s.cache.metadataInstalledAt
	s.metadataCheckedAt = time.Now().Add(-receptionMetadataHold)
	c.PublishedAt = time.Now()
	c.Metadata.PublishedAt = c.PublishedAt.UTC()
	s.poll(context.Background())
	unchanged, _ := receptionSourceRoot(t, s, time.Now())
	if unchanged.Metadata.Digest != second.Metadata.Digest || !unchanged.Metadata.PublishedAt.Equal(second.Metadata.PublishedAt) || s.cache.metadataInstalledAt != secondInstalled || !secondInstalled.After(originalInstalled) {
		t.Fatal("unchanged metadata renewed its token/date/hold")
	}
	c.Interrupted = true
	s.poll(context.Background())
	interrupted, _ := receptionSourceRoot(t, s, time.Now())
	if interrupted.State != "interrupted" {
		t.Fatal("interruption not independently represented")
	}
	c.Exhausted = true
	s.poll(context.Background())
	exhausted, _ := receptionSourceRoot(t, s, time.Now())
	if exhausted.State != "exhausted" {
		t.Fatal("exhaustion not represented")
	}
}

func TestReceptionSourceRenewsUnchangedMetadataBeforeReaderExpiry(t *testing.T) {
	c, bodies, snapshot := receptionSourceFixture(t, 1)
	s := receptionSourceForTest(&c, bodies, snapshot)
	s.poll(context.Background())
	first, _ := receptionSourceRoot(t, s, time.Now())
	chunk := receptionHistoryRequest{kind: "chunk", recording: s.id, digest: c.Chunks[0].Digest}
	original := s.reply(chunk, time.Now())
	reader, err := NewReceptionHistoryReaderCache(true, c.Metadata.Binding)
	if err != nil {
		t.Fatal(err)
	}
	installed := time.Now()
	if err := reader.InstallMetadata(s.id, s.cache.currentMetadata, installed); err != nil {
		t.Fatal(err)
	}
	// Simulate an unchanged recording older than the reader's token lifetime.
	s.metadataCheckedAt = time.Now().Add(-receptionMetadataHold)
	s.cache.metadataInstalledAt = time.Now().Add(-3 * receptionMetadataHold)
	c.PublishedAt = time.Now()
	c.Metadata.PublishedAt = c.PublishedAt.UTC()
	s.poll(context.Background())
	second, _ := receptionSourceRoot(t, s, time.Now())
	if second.Metadata.Revision != first.Metadata.Revision+1 || second.Metadata.Digest == first.Metadata.Digest {
		t.Fatal("fresh unchanged source did not replace the expiring metadata token")
	}
	if refreshed := s.reply(chunk, time.Now()); original.status != 200 || refreshed.status != 200 || refreshed.body != original.body || refreshed.etag != original.etag {
		t.Fatal("metadata renewal changed retained event data")
	}
	if _, ok := reader.MetadataManifest(s.id, first.Metadata.Digest, installed.Add(receptionReaderMetadataTTL)); ok {
		t.Fatal("expired old token was revived")
	}
	if err := reader.InstallMetadata(s.id, s.cache.currentMetadata, installed.Add(receptionReaderMetadataTTL)); err != nil {
		t.Fatal(err)
	}
	if _, ok := reader.MetadataManifest(s.id, second.Metadata.Digest, installed.Add(receptionReaderMetadataTTL)); !ok {
		t.Fatal("new metadata token unavailable after old reader token expired")
	}
	// A stalled sampler must not turn an expired source capture into a new token.
	s.metadataCheckedAt = time.Now().Add(-receptionMetadataHold)
	s.cache.metadataInstalledAt = time.Now().Add(-3 * receptionMetadataHold)
	c.PublishedAt = time.Now().Add(-receptionHistoryFreshFor)
	c.Metadata.PublishedAt = c.PublishedAt.UTC()
	s.poll(context.Background())
	if got, _ := s.cache.MetadataManifest(s.id); got.Digest != second.Metadata.Digest {
		t.Fatal("stale capture renewed metadata")
	}
}

func TestReceptionSourceBindingAndCallbackFences(t *testing.T) {
	for _, atBody := range []bool{false, true} {
		t.Run(strconv.FormatBool(atBody), func(t *testing.T) {
			c, bodies, snapshot := receptionSourceFixture(t, 1)
			s := receptionSourceForTest(&c, bodies, snapshot)
			changed := snapshot
			changed.RunID = "replacement"
			if atBody {
				s.sample.ChunkBody = func(ReceptionRecordingBinding, ReceptionChunkDescriptor) (string, bool) {
					s.observeStatus(changed, time.Now())
					return bodies[c.Chunks[0].Digest], true
				}
			} else {
				s.sample.Metadata = func() (ReceptionHistoryCapture, error) { s.observeStatus(changed, time.Now()); return c, nil }
			}
			s.poll(context.Background())
			if s.body != "" || s.cache != nil {
				t.Fatal("late incompatible callback installed")
			}
		})
	}
	c, bodies, snapshot := receptionSourceFixture(t, 1)
	s := receptionSourceForTest(&c, bodies, snapshot)
	s.poll(context.Background())
	oldID := s.id
	// Replay attempt/rewind are intentionally not recording identity inputs.
	snapshot.Version += "-later" // An independent status observation, not a reset.
	s.observeStatus(snapshot, time.Now())
	if s.id != oldID {
		t.Fatal("nonbinding status reset history")
	}
	c.Metadata.Binding.RecordingGeneration++
	c.Chunks, c.ConsumedThrough = nil, 0
	s.poll(context.Background())
	root, _ := receptionSourceRoot(t, s, time.Now())
	if root.RecordingID == oldID || root.CacheBounds.First != nil {
		t.Fatal("replacement retained old chunks")
	}
	if s.reply(receptionHistoryRequest{kind: "index", recording: oldID}, time.Now()).status != 409 {
		t.Fatal("retired recording accepted")
	}
	for name, change := range map[string]func(*Snapshot){
		"run":             func(v *Snapshot) { v.RunID += "-new" },
		"start":           func(v *Snapshot) { v.StartedAt = v.StartedAt.Add(time.Second) },
		"genesis":         func(v *Snapshot) { g := (solana.Hash{2}).String(); v.GenesisHash = &g },
		"unknown_genesis": func(v *Snapshot) { v.GenesisHash = nil },
	} {
		t.Run(name, func(t *testing.T) {
			copy := snapshot
			change(&copy)
			s.observeStatus(copy, time.Now())
			s.poll(context.Background())
			if s.reply(receptionHistoryRequest{kind: "root"}, time.Now()).status != 503 {
				t.Fatal("incompatible status exposed retained data")
			}
			s.observeStatus(snapshot, time.Now())
			s.poll(context.Background())
		})
	}
}

func TestReceptionSourceContextABAFence(t *testing.T) {
	for _, atBody := range []bool{false, true} {
		t.Run(strconv.FormatBool(atBody), func(t *testing.T) {
			c, bodies, snapshot := receptionSourceFixture(t, 1)
			s := receptionSourceForTest(&c, bodies, snapshot)
			other := snapshot
			other.RunID = "intermediate-run"
			changeAndReturn := func() {
				s.observeStatus(other, time.Now())
				s.observeStatus(snapshot, time.Now())
			}
			if atBody {
				s.sample.ChunkBody = func(ReceptionRecordingBinding, ReceptionChunkDescriptor) (string, bool) {
					changeAndReturn()
					return bodies[c.Chunks[0].Digest], true
				}
			} else {
				s.sample.Metadata = func() (ReceptionHistoryCapture, error) {
					changeAndReturn()
					return c, nil
				}
			}
			s.poll(context.Background())
			if s.body != "" || s.cache != nil {
				t.Fatal("pre-transition candidate installed after accepted A -> B -> A context changes")
			}
		})
	}
	// The final HTTP admission uses the same epoch fence even after a complete
	// replacement has installed under the original public recording identity.
	c, bodies, snapshot := receptionSourceFixture(t, 1)
	s := receptionSourceForTest(&c, bodies, snapshot)
	s.poll(context.Background())
	selected := s.reply(receptionHistoryRequest{kind: "chunk", recording: s.id, digest: c.Chunks[0].Digest}, time.Now())
	other := snapshot
	other.RunID = "intermediate-run"
	s.observeStatus(other, time.Now())
	s.observeStatus(snapshot, time.Now())
	s.poll(context.Background())
	if s.id != selected.recording || s.body == "" {
		t.Fatal("new same-identity candidate did not install")
	}
	if s.admitReply(selected, time.Now()).status != 409 {
		t.Fatal("pre-transition selected reply survived accepted A -> B -> A")
	}
}

func TestReceptionSourceContextRefreshAndExhaustion(t *testing.T) {
	c, bodies, snapshot := receptionSourceFixture(t, 1)
	s := receptionSourceForTest(&c, bodies, snapshot)
	epoch := s.gate.epoch
	if epoch != 1 || !s.gate.valid() {
		t.Fatal("first known context did not initialize")
	}
	// A same-context status observation inside either callback is not a reset.
	original := s.sample.ChunkBody
	s.sample.Metadata = func() (ReceptionHistoryCapture, error) {
		s.observeStatus(snapshot, time.Now())
		return c, nil
	}
	s.sample.ChunkBody = func(b ReceptionRecordingBinding, d ReceptionChunkDescriptor) (string, bool) {
		s.observeStatus(snapshot, time.Now())
		return original(b, d)
	}
	s.poll(context.Background())
	if s.gate.epoch != epoch || s.body == "" || s.freshUntil != c.PublishedAt.Add(receptionHistoryFreshFor) {
		t.Fatal("ordinary status refresh invalidated capture or renewed its lease")
	}
	selected := s.reply(receptionHistoryRequest{kind: "chunk", recording: s.id, digest: c.Chunks[0].Digest}, time.Now())
	s.observeStatus(snapshot, time.Now())
	if s.admitReply(selected, time.Now()).status != 200 {
		t.Fatal("ordinary status refresh invalidated a selected reply")
	}
	var calls int
	u := newReceptionHistorySource(ReceptionHistorySampler{
		Metadata:  func() (ReceptionHistoryCapture, error) { calls++; return c, nil },
		ChunkBody: original,
	})
	u.poll(context.Background())
	unknown := snapshot
	unknown.GenesisHash = nil
	u.observeStatus(unknown, time.Now())
	u.poll(context.Background())
	if calls != 0 || u.gate.epoch != 1 || u.gate.valid() {
		t.Fatal("zero/unknown context sampled or retired incorrectly")
	}
	u.observeStatus(snapshot, time.Now())
	u.poll(context.Background())
	if calls != 1 || u.gate.epoch != 2 || u.body == "" {
		t.Fatal("known genesis arrival did not enable capture")
	}
	// MaxUint64 is terminal, never an active token or a wrap back to zero.
	u.gate.epoch = math.MaxUint64 - 1
	u.observeStatus(snapshot, time.Now())
	if !u.gate.valid() || u.gate.epoch != math.MaxUint64-1 {
		t.Fatal("unchanged final usable context was retired")
	}
	other := snapshot
	other.RunID = "last-transition"
	u.observeStatus(other, time.Now())
	u.observeStatus(snapshot, time.Now())
	u.poll(context.Background())
	if u.gate.epoch != math.MaxUint64 || u.gate.valid() || calls != 1 || u.body != "" || u.cache != nil {
		t.Fatal("exhausted context token wrapped or revived")
	}
}

func TestReceptionSourceRejectsMalformedCaptureAndBody(t *testing.T) {
	for name, mutate := range map[string]func(*ReceptionHistoryCapture){
		"sources_limit":               func(c *ReceptionHistoryCapture) { c.Metadata.Sources = make([]ReceptionSource, 257) },
		"ranges_limit":                func(c *ReceptionHistoryCapture) { c.Metadata.Ranges[0] = make([]ReceptionRange, 2049) },
		"descriptor_limit":            func(c *ReceptionHistoryCapture) { c.Chunks = make([]ReceptionChunkDescriptor, 257) },
		"consumed_regression":         func(c *ReceptionHistoryCapture) { c.ConsumedThrough = 0 },
		"unknown_without_limited":     func(c *ReceptionHistoryCapture) { c.Metadata.StartupAtCapture.State = "unknown" },
		"interrupted_without_limited": func(c *ReceptionHistoryCapture) { c.Interrupted = true },
		"invalid_range":               func(c *ReceptionHistoryCapture) { c.Metadata.Ranges[0] = []ReceptionRange{{First: 2, Last: 1}} },
	} {
		t.Run(name, func(t *testing.T) {
			c, bodies, snapshot := receptionSourceFixture(t, 1)
			s := receptionSourceForTest(&c, bodies, snapshot)
			mutate(&c)
			s.poll(context.Background())
			if s.cache != nil || s.body != "" {
				t.Fatal("invalid source partially installed")
			}
		})
	}
	c, bodies, snapshot := receptionSourceFixture(t, 1)
	s := receptionSourceForTest(&c, bodies, snapshot)
	bodies[c.Chunks[0].Digest] += "\n"
	s.poll(context.Background())
	if s.cache != nil || s.body != "" {
		t.Fatal("noncanonical source body partially installed")
	}
	// A status replacement after a reply was selected must still fence it.
	bodies[c.Chunks[0].Digest] = strings.TrimSuffix(bodies[c.Chunks[0].Digest], "\n")
	s.poll(context.Background())
	selected := s.reply(receptionHistoryRequest{kind: "chunk", recording: s.id, digest: c.Chunks[0].Digest}, time.Now())
	snapshot.RunID += "-new"
	s.observeStatus(snapshot, time.Now())
	if s.admitReply(selected, time.Now()).status != 409 {
		t.Fatal("incompatible selected body survived final fence")
	}
}

func TestReceptionSourceStrictRequestsAndRate(t *testing.T) {
	c, bodies, snapshot := receptionSourceFixture(t, 1)
	s := receptionSourceForTest(&c, bodies, snapshot)
	s.poll(context.Background())
	root, _ := receptionSourceRoot(t, s, time.Now())
	index := receptionHistoryPath + "/index/" + s.id
	chunk := receptionHistoryPath + "/chunk/" + s.id + "/" + c.Chunks[0].Digest
	page := receptionHistoryPath + "/metadata/" + s.id + "/" + root.Metadata.Digest + "/sources/0"
	for _, path := range []string{receptionHistoryPath, index + "?after=0", chunk, page} {
		q, status := parseReceptionHistoryRequest(httptest.NewRequest("GET", path, nil))
		if status != 0 || s.reply(q, time.Now()).status != 200 {
			t.Fatalf("valid route refused: %s", path)
		}
	}
	for _, path := range []string{receptionHistoryPath + "/", receptionHistoryPath + "?", receptionHistoryPath + "?extra=1", index,
		index + "?after=01", index + "?after=+1", index + "?after=%31", index + "?after=1&after=2", index + "?after=18446744073709551616",
		chunk + "?x=1", strings.Replace(chunk, "/chunk/", "//chunk/", 1), strings.Replace(chunk, "/chunk/", "/%63hunk/", 1), page + "/", strings.TrimSuffix(page, "/0") + "/00"} {
		_, status := parseReceptionHistoryRequest(httptest.NewRequest("GET", path, nil))
		if status != 400 {
			t.Fatalf("noncanonical route status=%d path=%s", status, path)
		}
	}
	for _, path := range []string{receptionHistoryPath + "/cursor/" + s.id + "?after=0", receptionHistoryPath + "/query/" + s.id} {
		_, status := parseReceptionHistoryRequest(httptest.NewRequest("GET", path, nil))
		if status != 404 {
			t.Fatal("reader-only route exposed privately")
		}
	}
	for _, request := range []*http.Request{httptest.NewRequest("POST", receptionHistoryPath, nil), httptest.NewRequest("GET", receptionHistoryPath, strings.NewReader("x"))} {
		if _, status := parseReceptionHistoryRequest(request); status != 400 {
			t.Fatal("method/body accepted")
		}
	}
	s.limit = rate.NewLimiter(0, 1)
	for i, want := range []int{200, 429} {
		w := httptest.NewRecorder()
		s.serveHTTP(w, httptest.NewRequest("GET", chunk, nil))
		if w.Code != want || i == 0 && w.Header().Get("ETag") != `"`+c.Chunks[0].Digest+`"` || i == 1 && (w.Body.Len() > 512 || w.Header().Get(replayDetailRemainingHeader) != "") {
			t.Fatal("rate/body/ETag bound differs")
		}
	}
	s.limit = rate.NewLimiter(64, 16)
	for range cap(s.active) {
		s.active <- struct{}{}
	}
	w := httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest("GET", receptionHistoryPath, nil))
	for range cap(s.active) {
		<-s.active
	}
	if w.Code != 429 {
		t.Fatal("fifth active response accepted")
	}
	selected := s.reply(receptionHistoryRequest{kind: "root"}, c.PublishedAt.Add(time.Millisecond))
	if delayed := s.admitReply(selected, c.PublishedAt.Add(receptionHistoryFreshFor)); delayed.remaining != 0 || !strings.Contains(delayed.body, `"state":"stale"`) {
		t.Fatal("late response extended source lease")
	}
	s.failure = "source_unavailable"
	selected = s.reply(receptionHistoryRequest{kind: "root"}, c.PublishedAt.Add(time.Millisecond))
	s.failure = ""
	if recovered := s.admitReply(selected, c.PublishedAt.Add(2*time.Millisecond)); recovered.remaining != 0 {
		t.Fatal("a selected stale body acquired a fresh header")
	}
}

func waitReceptionTest(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("bounded reception test wait expired")
		}
		time.Sleep(time.Millisecond)
	}
}

func receptionHTTPTest(t *testing.T, client *http.Client, path string) (int, string, http.Header) {
	t.Helper()
	r, err := client.Get("http://node" + path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxReceptionChunkBytes+1))
	if err != nil || len(body) > MaxReceptionChunkBytes {
		t.Fatal("HTTP body exceeded bound")
	}
	return r.StatusCode, string(body), r.Header
}

func TestReceptionSourceRealUnixOwnerAndCachedReplies(t *testing.T) {
	at := time.Now()
	h, err := turbine.NewShredHistory(turbine.ShredHistoryBinding{RunID: "private-reception", NodeStartedAt: at, GenesisHash: solana.Hash{1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.CloseAfterProducers() })
	handle, ok := h.RegisterSource(turbine.ShredHistoryLive, at)
	if !ok || !handle.TryReceipt(turbine.ShredHistoryReceipt{At: at, Origin: turbine.ShredHistoryTVU}) {
		t.Fatal("actual source receipt refused")
	}
	handle.Retire()
	h.MarkMarkerUnknown()
	if !h.CloseAfterProducers() {
		t.Fatal("actual source did not drain")
	}
	m, ok := h.Metadata()
	if !ok || len(m.Chunks) != 1 {
		t.Fatal("actual source did not seal")
	}
	binding := ReceptionRecordingBinding{RunID: m.Binding.RunID, NodeStartedAt: m.Binding.NodeStartedAt.UTC(), GenesisHash: m.Binding.GenesisHash.String(), RecordingGeneration: 1}
	c := ReceptionHistoryCapture{Metadata: ReceptionMetadata{Binding: binding, PublishedAt: m.PublishedAt.UTC(), StartupAtCapture: ReceptionStartup{State: "unknown", Limited: m.StartupLimited},
		Sources: []ReceptionSource{{Generation: m.Sources[0].Generation, StartedAt: m.Sources[0].StartedAt.UTC(), Kind: "live", Retired: m.Sources[0].Retired}}},
		PublishedAt: m.PublishedAt, ConsumedThrough: m.LastSequence, Closed: m.Closed}
	for i := range m.StartupRanges {
		for _, r := range m.StartupRanges[i] {
			c.Metadata.Ranges[i] = append(c.Metadata.Ranges[i], ReceptionRange{First: r.First, Last: r.Last})
		}
	}
	d := m.Chunks[0]
	c.Chunks = []ReceptionChunkDescriptor{{FirstSequence: d.FirstSequence, LastSequence: d.LastSequence, Count: d.Count, MinAt: d.MinAt, MaxAt: d.MaxAt, Digest: hex.EncodeToString(d.Digest[:]), Bytes: d.Bytes}}
	original, ok := h.ChunkBody(d.FirstSequence, d.LastSequence, d.Digest)
	if !ok {
		t.Fatal("actual source body missing")
	}
	var metadataCalls, bodyCalls atomic.Int64
	blocked, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	sample := ReceptionHistorySampler{Metadata: func() (ReceptionHistoryCapture, error) {
		if metadataCalls.Add(1) == 2 {
			close(blocked)
			<-release
		}
		return c, nil
	}, ChunkBody: func(b ReceptionRecordingBinding, requested ReceptionChunkDescriptor) (string, bool) {
		bodyCalls.Add(1)
		if b != binding || !sameReceptionChunk(requested, c.Chunks[0]) {
			return "", false
		}
		return h.ChunkBody(d.FirstSequence, d.LastSequence, d.Digest)
	}}
	snapshot := testSnapshot(at.UTC())
	snapshot.RunID, snapshot.StartedAt = binding.RunID, binding.NodeStartedAt
	snapshot.GenesisHash = &binding.GenesisHash
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{Reception: &sample})
	if err != nil {
		unblock()
		t.Fatal(err)
	}
	t.Cleanup(func() { unblock(); feed.Close() })
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("worker never entered controlled second capture")
	}
	client := newCollector(path).client
	t.Cleanup(client.CloseIdleConnections)
	status, body, header := receptionHTTPTest(t, client, receptionHistoryPath)
	root, err := decodeReceptionHistoryRoot([]byte(body))
	if status != 200 || err != nil || root.State != "closed" || !root.PublishedAt.Equal(m.PublishedAt) || header.Get(replayDetailRemainingHeader) != "" {
		t.Fatal("actual Unix closed root changed source clock/state")
	}
	paths := []string{receptionHistoryPath + "/index/" + root.RecordingID + "?after=0", receptionHistoryPath + "/chunk/" + root.RecordingID + "/" + c.Chunks[0].Digest,
		receptionHistoryPath + "/metadata/" + root.RecordingID + "/" + root.Metadata.Digest + "/sources/0"}
	for i, p := range paths {
		status, body, header = receptionHTTPTest(t, client, p)
		if status != 200 || header.Get("Cache-Control") != "no-store" || header.Get(replayDetailRemainingHeader) != "" {
			t.Fatal("historical Unix route failed")
		}
		if i == 1 && (body != original || header.Get("ETag") != `"`+c.Chunks[0].Digest+`"`) {
			t.Fatal("Unix chunk differs from original source bytes")
		}
		if i == 2 && header.Get("ETag") != `"`+root.Metadata.Pages[0].Digest+`"` {
			t.Fatal("metadata page ETag used bundle digest")
		}
	}
	status, _, _ = receptionHTTPTest(t, client, receptionHistoryPath+"//chunk/"+root.RecordingID+"/"+c.Chunks[0].Digest)
	if status != 400 || metadataCalls.Load() != 2 || bodyCalls.Load() != 1 {
		t.Fatal("HTTP normalized a malformed route or invoked source callbacks")
	}
	closed := make(chan struct{})
	go func() { feed.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close did not join in-flight preparation")
	case <-time.After(10 * time.Millisecond):
	}
	unblock()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close failed to join")
	}
	if metadataCalls.Load() != 2 || bodyCalls.Load() != 1 {
		t.Fatal("cancelled callback performed another import")
	}
}

func TestReceptionSourceFeedDefaultRejectedStatusAndCancellation(t *testing.T) {
	c, bodies, snapshot := receptionSourceFixture(t, 0)
	path := socketPath(t)
	plain, err := ListenFeed(context.Background(), path, func() Snapshot { return snapshot })
	if err != nil {
		t.Fatal(err)
	}
	client := newCollector(path).client
	status, _, _ := receptionHTTPTest(t, client, receptionHistoryPath)
	client.CloseIdleConnections()
	plain.Close()
	if status != 404 || plain.reception != nil {
		t.Fatal("default enabled reception")
	}
	if f, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{Reception: &ReceptionHistorySampler{}}); err == nil {
		f.Close()
		t.Fatal("incomplete callback pair accepted")
	}
	var rejected atomic.Bool
	var calls atomic.Int64
	s := receptionSourceForTest(&c, bodies, snapshot)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot {
		calls.Add(1)
		v := snapshot
		if rejected.Load() {
			v.Mode = "not-a-role"
			v.RunID = "rejected-context"
		}
		return v
	}, DetailSamplers{Reception: &s.sample})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	waitReceptionTest(t, func() bool {
		feed.reception.mu.RLock()
		defer feed.reception.mu.RUnlock()
		return feed.reception.body != ""
	})
	feed.reception.mu.RLock()
	originalGate := feed.reception.gate
	feed.reception.mu.RUnlock()
	rejected.Store(true)
	waitReceptionTest(t, func() bool { return calls.Load() >= 2 })
	feed.Close() // Joins status publication, so a sampler entry alone is not evidence.
	if feed.reception.gate != originalGate {
		t.Fatal("rejected status renewed or replaced the accepted gate")
	}
	_, reply := receptionSourceRoot(t, feed.reception, originalGate.at.Add(StaleAfter+time.Second))
	if reply.remaining != 0 {
		t.Fatal("stopped accepted status retained a live lease")
	}
	ctx, cancel := context.WithCancel(context.Background())
	late := receptionSourceForTest(&c, bodies, snapshot)
	late.sample.Metadata = func() (ReceptionHistoryCapture, error) { cancel(); return c, nil }
	late.poll(ctx)
	if late.body != "" || late.cache != nil {
		t.Fatal("callback after cancellation installed")
	}
}
