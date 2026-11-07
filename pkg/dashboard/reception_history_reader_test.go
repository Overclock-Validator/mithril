package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func receptionCollectorMetadata() ReceptionMetadata {
	m := receptionTestMetadata()
	m.Binding.RunID, m.Binding.RecordingGeneration, m.Revision = "reader-reception", 1, 1
	m.Binding.NodeStartedAt = time.Now().Add(-time.Hour).UTC()
	return m
}

func receptionCollectorForTest(t *testing.T, root ReceptionHistoryRoot) (*receptionHistoryReader, *collector) {
	t.Helper()
	c := newCollector("/unused-reception-test.sock")
	s := testSnapshot(time.Now().UTC())
	s.RunID, s.StartedAt, s.GenesisHash, s.Replay = root.Binding.RunID, root.Binding.NodeStartedAt, &root.Binding.GenesisHash, nil
	c.last = &s
	c.observeReceptionHistoryStatus("fresh", time.Now())
	r, err := newReceptionHistoryReader(c.client, &c.receptionGate)
	if err != nil {
		t.Fatal(err)
	}
	r.admitStatus = func(receptionPublicSelection) (time.Duration, bool) { return time.Second, true }
	t.Cleanup(c.client.CloseIdleConnections)
	t.Cleanup(r.client.CloseIdleConnections)
	return r, c
}

func receptionAcceptTest(t *testing.T, r *receptionHistoryReader, root ReceptionHistoryRoot, remaining time.Duration) {
	t.Helper()
	if err := r.accept(context.Background(), r.status(), root, receptionReaderResponse{started: time.Now(), remaining: remaining}); err != nil {
		t.Fatal(err)
	}
}

func receptionInstallTest(t *testing.T, r *receptionHistoryReader, m ReceptionMetadata) {
	t.Helper()
	p, err := PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.cache.InstallMetadata(r.last.RecordingID, p, time.Now()); err != nil {
		t.Fatal(err)
	}
	r.pending = nil
	if err := r.publish(context.Background(), r.status(), true); err != nil {
		t.Fatal(err)
	}
}

func TestReceptionContextFailuresReportUnavailable(t *testing.T) {
	logs := captureFeedLogs(t)
	for _, tc := range []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
	}{
		{"deadline", func() (context.Context, context.CancelFunc) {
			return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		}},
		{"canceled", func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, cancel
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := receptionTestRoot(t, receptionCollectorMetadata())
			r, _ := receptionCollectorForTest(t, root)
			g := r.status()
			ctx, cancel := tc.ctx()
			defer cancel()
			err := r.commit(ctx, g, func() error { t.Fatal("canceled commit ran"); return nil })
			if err == nil {
				t.Fatal("canceled context was accepted")
			}
			before := len(logs.records(t))
			r.fail(g, err.Error())
			r.fail(g, "source_unavailable")
			source := &receptionHistorySource{gate: g}
			source.fail(g, err.Error())
			records := logs.records(t)
			if r.failure != "source_unavailable" || source.failure != "source_unavailable" || len(records) != before+2 {
				t.Fatalf("incorrect context failure state: reader=%q source=%q logs=%v", r.failure, source.failure, records)
			}
			for _, record := range records[before:] {
				if record["reason"] != "source_unavailable" {
					t.Fatalf("context failure logged as %v", record["reason"])
				}
			}
		})
	}
	if receptionHistoryReason("unrecognized detail") != "source_invalid" {
		t.Fatal("unknown errors stopped failing closed")
	}
}

func TestReceptionCollectorStatusGateAndTransportOwnership(t *testing.T) {
	root := receptionTestRoot(t, receptionCollectorMetadata())
	r, c := receptionCollectorForTest(t, root)
	original := c.client.Transport.(*http.Transport)
	owned := r.client.Transport.(*http.Transport)
	if owned == original || owned.Proxy != nil || !owned.DisableCompression || owned.MaxConnsPerHost != 2 || owned.MaxResponseHeaderBytes != 4096 || r.client.Jar != nil || r.client.Timeout != 100*time.Millisecond {
		t.Fatal("reader did not own bounded fixed transport")
	}
	if err := r.client.CheckRedirect(nil, nil); err != http.ErrUseLastResponse {
		t.Fatal("redirects allowed")
	}
	if _, err := newReceptionHistoryReader(nil, &c.receptionGate); err == nil {
		t.Fatal("nil client accepted")
	}
	if _, err := newReceptionHistoryReader(&http.Client{}, &c.receptionGate); err == nil {
		t.Fatal("non-Unix transport accepted")
	}
	for _, mode := range []string{"verifying", "validator"} {
		t.Run(mode, func(t *testing.T) {
			c.last.Mode, c.last.Replay = mode, nil
			before := r.status()
			c.observeReceptionHistoryStatus("fresh", time.Now())
			if !r.status().fresh(time.Now()) || r.status().epoch != before.epoch {
				t.Fatal("pre-replay/role gated or refresh invalidated")
			}
		})
	}
	before := r.status()
	c.observeReceptionHistoryStatus("stale", time.Now())
	if r.status().known || r.status().epoch == before.epoch {
		t.Fatal("status failure did not fence")
	}
	c.observeReceptionHistoryStatus("fresh", time.Now())
	if r.status().epoch <= before.epoch || !r.status().fresh(time.Now()) {
		t.Fatal("recovery did not fence")
	}
	c.receptionGate.mu.Lock()
	c.receptionGate.value.epoch = math.MaxUint64 - 1
	c.receptionGate.mu.Unlock()
	c.observeReceptionHistoryStatus("unavailable", time.Now())
	c.observeReceptionHistoryStatus("fresh", time.Now())
	if r.status().valid() || r.status().epoch != math.MaxUint64 {
		t.Fatal("gate exhaustion wrapped")
	}
}

func TestReceptionCollectorOriginalLeaseAndHistoricalMetadata(t *testing.T) {
	m := receptionCollectorMetadata()
	root := receptionTestRoot(t, m)
	r, c := receptionCollectorForTest(t, root)
	receptionAcceptTest(t, r, root, receptionHistoryFreshFor)
	receptionInstallTest(t, r, m)
	original := r.until
	// Adapter changes do not become a new source capture. Neither the future
	// original UTC date nor a renewed status sample grants a fresh lease.
	m.Revision++
	root.Metadata = receptionTestRoot(t, m).Metadata
	c.observeReceptionHistoryStatus("fresh", time.Now())
	receptionAcceptTest(t, r, root, receptionHistoryFreshFor)
	if r.until != original {
		t.Fatal("metadata-only change renewed source")
	}
	receptionInstallTest(t, r, m)
	body, until, reason := r.rootBody(original)
	if reason != "" || !until.IsZero() || !strings.Contains(body, `"state":"stale"`) {
		t.Fatal("exact expiry remained fresh", reason)
	}
	r.fail(r.status(), "source_unavailable")
	receptionAcceptTest(t, r, root, receptionHistoryFreshFor)
	if r.until != original {
		t.Fatal("failure erased original source deadline")
	}
	short := receptionReaderResponse{started: time.Now(), remaining: 100 * time.Millisecond}
	if err := r.accept(context.Background(), r.status(), root, short); err != nil {
		t.Fatal(err)
	}
	if !r.until.Equal(short.started.Add(100 * time.Millisecond)) {
		t.Fatal("shorter original deadline ignored")
	}
	// Source wall time can reverse: a distinct observation is dated by its own
	// private remainder, not an ordering comparison of the UTC dates.
	root.PublishedAt = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	receptionAcceptTest(t, r, root, time.Second)
	if !r.until.After(short.started.Add(100 * time.Millisecond)) {
		t.Fatal("reversed wall source was rejected")
	}
	if err := r.publish(context.Background(), r.status(), true); err != nil {
		t.Fatal(err)
	}
	_, _, reason = r.rootBody(time.Now().Add(31 * time.Second))
	if reason != "metadata_expired" {
		t.Fatal("expired token remained advertised", reason)
	}
	// First publication remains absent until complete metadata, not an empty
	// stand-in table or a token naming pages that have not been validated.
	fresh, _ := receptionCollectorForTest(t, root)
	receptionAcceptTest(t, fresh, root, time.Second)
	if err := fresh.publish(context.Background(), fresh.status(), true); err == nil {
		t.Fatal("incomplete metadata published")
	}
	if body, _, _ := fresh.rootBody(time.Now()); body != "" {
		t.Fatal("pending root advertised")
	}
}

func TestReceptionCollectorHighwaterAndContextABA(t *testing.T) {
	m := receptionCollectorMetadata()
	m.Binding.RecordingGeneration = 2
	root := receptionTestRoot(t, m)
	r, c := receptionCollectorForTest(t, root)
	receptionAcceptTest(t, r, root, time.Second)
	receptionInstallTest(t, r, m)
	oldGate := r.status()
	for _, change := range []string{"unknown", "other-node"} {
		t.Run(change, func(t *testing.T) {
			saved := *c.last
			if change == "unknown" {
				c.last.GenesisHash = nil
			} else {
				c.last.RunID = "other"
			}
			c.observeReceptionHistoryStatus("fresh", time.Now())
			bad := root
			if err := r.accept(context.Background(), r.status(), bad, receptionReaderResponse{started: time.Now(), remaining: time.Second}); err == nil {
				t.Fatal("failed new binding accepted")
			}
			*c.last = saved
			c.observeReceptionHistoryStatus("fresh", time.Now())
			if err := r.accept(context.Background(), oldGate, root, receptionReaderResponse{started: time.Now(), remaining: time.Second}); err == nil {
				t.Fatal("ABA request committed")
			}
			lower := m
			lower.Binding.RecordingGeneration = 1
			if err := r.accept(context.Background(), r.status(), receptionTestRoot(t, lower), receptionReaderResponse{started: time.Now(), remaining: time.Second}); err == nil {
				t.Fatal("failure erased generation high-water")
			}
		})
	}
	root.ConsumedThrough = 8
	receptionAcceptTest(t, r, root, time.Second)
	regression := root
	regression.ConsumedThrough = 7
	if err := r.accept(context.Background(), r.status(), regression, receptionReaderResponse{started: time.Now(), remaining: time.Second}); err == nil {
		t.Fatal("consumed regression accepted")
	}
	root.Startup = ReceptionStartup{State: "unknown", Limited: true}
	receptionAcceptTest(t, r, root, time.Second)
	regression = root
	regression.Startup = ReceptionStartup{State: "open"}
	if err := r.accept(context.Background(), r.status(), regression, receptionReaderResponse{started: time.Now(), remaining: time.Second}); err == nil {
		t.Fatal("terminal startup regression accepted")
	}
	m.Revision = 2
	m.StartupAtCapture = root.Startup
	root.Metadata = receptionTestRoot(t, m).Metadata
	receptionAcceptTest(t, r, root, time.Second)
	m.Revision = 3
	m.StartupAtCapture = ReceptionStartup{State: "open"}
	regression = root
	regression.Metadata = receptionTestRoot(t, m).Metadata
	if err := r.accept(context.Background(), r.status(), regression, receptionReaderResponse{started: time.Now(), remaining: time.Second}); err == nil {
		t.Fatal("new metadata revision retracted terminal capture")
	}
	// A fully valid different node can begin at generation1, but cannot retain
	// the previous node's root, partial transfer, rows or cursor.
	m.Binding.RunID, m.Binding.RecordingGeneration = "new-node", 1
	root = receptionTestRoot(t, m)
	c.last.RunID = "new-node"
	c.observeReceptionHistoryStatus("fresh", time.Now())
	receptionAcceptTest(t, r, root, time.Second)
	if r.cursor != 0 || r.publication.body != "" || r.last.Binding.RecordingGeneration != 1 {
		t.Fatal("new context inherited state")
	}
}

func TestReceptionCollectorCommitCancellation(t *testing.T) {
	root := receptionTestRoot(t, receptionCollectorMetadata())
	r, c := receptionCollectorForTest(t, root)
	g := r.status()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	if err := r.commit(ctx, g, func() error { called = true; return nil }); err == nil || called {
		t.Fatal("canceled commit ran")
	}
	c.observeReceptionHistoryStatus("unavailable", time.Now())
	c.observeReceptionHistoryStatus("fresh", time.Now())
	if err := r.commit(context.Background(), g, func() error { called = true; return nil }); err == nil || called {
		t.Fatal("ABA commit ran")
	}
}

func TestReceptionCollectorHistoricalRootRequiresCurrentGatePublication(t *testing.T) {
	for _, state := range []string{"closed", "stale"} {
		for _, transition := range []string{"other-node", "unavailable"} {
			t.Run(state+"/"+transition, func(t *testing.T) {
				m := receptionCollectorMetadata()
				m.Sources = []ReceptionSource{{Generation: 1, StartedAt: m.PublishedAt, Kind: "live"}}
				root := receptionTestRoot(t, m)
				root.State, root.Closed, root.ConsumedThrough = state, state == "closed", 1
				first, last := uint64(1), uint64(1)
				root.SourceBounds = ReceptionSequenceBounds{First: &first, Last: &last}
				if _, err := PrepareReceptionHistoryRoot(root); err != nil {
					t.Fatal("invalid regression fixture", err)
				}
				r, c := receptionCollectorForTest(t, root)
				receptionAcceptTest(t, r, root, 0)
				chunk := receptionChunkFixture(t, 1, 1, false)
				if err := r.cache.ImportChunks(root.RecordingID, []ReceptionHistoryChunkInput{receptionReaderInput(chunk)}); err != nil {
					t.Fatal(err)
				}
				receptionInstallTest(t, r, m)
				if body, _, reason := r.rootBody(time.Now()); body == "" || reason != "" {
					t.Fatal("initial historical root unavailable", reason)
				}
				oldEpoch, saved := r.publication.epoch, *c.last
				if transition == "other-node" {
					c.last.RunID = "different-node"
					c.observeReceptionHistoryStatus("fresh", time.Now())
				} else {
					c.observeReceptionHistoryStatus("unavailable", time.Now())
				}
				assertUnavailable := func(phase string) {
					t.Helper()
					body, deadline, reason := r.rootBody(time.Now())
					if body != "" || !deadline.IsZero() || reason != "binding_changed" {
						t.Errorf("%s advertised old %s root: body=%t deadline=%s reason=%q", phase, state, body != "", deadline, reason)
					}
				}
				assertUnavailable("context invalidated")
				*c.last = saved
				c.observeReceptionHistoryStatus("fresh", time.Now())
				if r.status().epoch == oldEpoch {
					t.Fatal("fixture did not make a real ABA transition")
				}
				assertUnavailable("status recovered before source reaccept")
				// The old token/body remain retained history, but their survival is
				// not permission to advertise an old root as this epoch's current root.
				if body, ok := r.cache.ChunkBody(root.RecordingID, chunk.descriptor.Digest); !ok || body != chunk.body {
					t.Fatal("ABA erased historical chunk")
				}
				if _, _, ok := r.cache.MetadataPage(root.RecordingID, root.Metadata.Digest, "sources", 0, time.Now()); !ok {
					t.Fatal("ABA erased historical metadata")
				}
				receptionAcceptTest(t, r, root, 0)
				assertUnavailable("source reaccepted but not republished")
				if err := r.publish(context.Background(), r.status(), true); err != nil {
					t.Fatal(err)
				}
				body, deadline, reason := r.rootBody(time.Now())
				if body == "" || !deadline.IsZero() || reason != "" || r.publication.epoch != r.status().epoch {
					t.Fatal("current-gate historical publication unavailable", reason)
				}
			})
		}
	}
}

func TestReceptionCollectorPendingTransferHolderBudget(t *testing.T) {
	m := maximumReceptionMetadata()
	p, err := PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.manifest.Pages) != 52 {
		t.Fatal("maximum pages shortened")
	}
	transfer := receptionMetadataTransfer{manifest: p.Manifest(), body: p.Body()}
	for i, d := range p.manifest.Pages {
		page, ok := p.Page(d.Kind, d.Page)
		if !ok {
			t.Fatal("missing page")
		}
		transfer.pages[i], err = decodeReceptionMetadataPageForCache(transfer.manifest, i, []byte(page))
		if err != nil {
			t.Fatal(err)
		}
	}
	decoded, err := finishReceptionMetadata(transfer.body, transfer.pages[:52])
	if err != nil || decoded.Body() != p.Body() {
		t.Fatal("maximum completed transfer", err)
	}
	// A2.4a already reserves one512KiB metadata candidate plus four512KiB
	// input/decode/canonical buffers. Charge the persistent transfer, completed
	// bundle, decoded scalar arrays and conversion overlap to those allowances.
	// Sizeof(transfer) includes all 52 immutable-body/ordering proof slots. Their
	// bodies share the completed bundle's strings, but charge both conservatively;
	// this is not an added allowance on top of its54,492,068B graph.
	parts := []uintptr{
		unsafe.Sizeof(transfer) + MaxReceptionMetadataBytes,
		unsafe.Sizeof(*decoded) + MaxReceptionMetadataBytes,
		maxReceptionSources*unsafe.Sizeof(ReceptionSource{}) + 3*maxReceptionRanges*unsafe.Sizeof(ReceptionRange{}),
		2 * MaxReceptionMetadataBytes,
		unsafe.Sizeof(receptionHistoryReader{}) + 8*MaxReceptionRootBytes,
	}
	var total uintptr
	for _, n := range parts {
		total += n
	}
	if total > 5*MaxReceptionMetadataBytes {
		t.Fatalf("collector metadata holders %d exceed existing allowance", total)
	}
	if len(transfer.pages) != maxReceptionPages || unsafe.Sizeof(transfer.pages) != maxReceptionPages*unsafe.Sizeof(receptionValidatedMetadataPage{}) {
		t.Fatal("fixed validated-page holders not included")
	}
	t.Logf("collector metadata transfer/decode/root holders=%d <= existing %dB allowance; validated_page_slots=%dB; runtime heap remains unmeasured", total, 5*MaxReceptionMetadataBytes, unsafe.Sizeof(transfer.pages))
	// The immutable manifest body and its descriptor count remain exact after
	// decoding; no source or full chunk-generation pointer is retained.
	var manifest ReceptionMetadataManifest
	if json.Unmarshal([]byte(decoded.Body()), &manifest) != nil || len(manifest.Pages) != 52 {
		t.Fatal("complete manifest missing")
	}
}
