package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func receptionReaderFixture(t *testing.T) (*ReceptionHistoryReaderCache, ReceptionRecordingBinding, string) {
	t.Helper()
	b := receptionTestMetadata().Binding
	b.RecordingGeneration = 1
	c, err := NewReceptionHistoryReaderCache(true, b)
	if err != nil {
		t.Fatal(err)
	}
	id, err := ReceptionRecordingID(b)
	if err != nil {
		t.Fatal(err)
	}
	return c, b, id
}

func receptionReaderInput(p PreparedReceptionChunk) ReceptionHistoryChunkInput {
	return ReceptionHistoryChunkInput{Descriptor: p.Descriptor(), Body: p.Body()}
}

func receptionReaderDatedInput(t *testing.T, first uint64, dates []time.Time) ReceptionHistoryChunkInput {
	t.Helper()
	p := receptionChunkFixture(t, first, len(dates), false)
	var wire receptionChunkWire
	if err := json.Unmarshal([]byte(p.Body()), &wire); err != nil {
		t.Fatal(err)
	}
	minimum, maximum := dates[0], dates[0]
	for i, at := range dates {
		wire.Rows.values[i].At = at
		if at.Before(minimum) {
			minimum = at
		}
		if at.After(maximum) {
			maximum = at
		}
	}
	body, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	d := p.Descriptor()
	d.MinAt, d.MaxAt, d.Digest, d.Bytes = minimum, maximum, receptionDigest(body), uint64(len(body))
	return ReceptionHistoryChunkInput{Descriptor: d, Body: string(body)}
}

func TestReceptionReaderDisabledResetAndOwnership(t *testing.T) {
	c, err := NewReceptionHistoryReaderCache(false, ReceptionRecordingBinding{})
	if err != nil || c != nil {
		t.Fatal("disabled construction did work")
	}
	if _, err := c.Cursor(context.Background(), "", 0); err == nil || c.ImportChunks("", nil) == nil || c.Reset(ReceptionRecordingBinding{}) == nil {
		t.Fatal("disabled cache accepted work")
	}
	if _, ok := c.ChunkBody("", ""); ok {
		t.Fatal("disabled body present")
	}
	if _, _, ok := c.Index("", 0); ok {
		t.Fatal("disabled index present")
	}
	if _, ok := c.MetadataManifest("", "", time.Now()); ok {
		t.Fatal("disabled metadata present")
	}
	c, binding, id := receptionReaderFixture(t)
	input := receptionReaderInput(receptionChunkFixture(t, 1, 3, false))
	if err := c.ImportChunks(id, []ReceptionHistoryChunkInput{input}); err != nil {
		t.Fatal(err)
	}
	got, err := c.Cursor(context.Background(), id, 0)
	if err != nil || got.Count != 3 || got.ScannedThrough != 3 || got.More {
		t.Fatalf("first rows: %+v %v", got, err)
	}
	got.Rows[0].Slot = 99
	*got.CacheBounds.First = 99
	again, err := c.Cursor(context.Background(), id, 0)
	if err != nil || again.Rows[0].Slot != 0 || *again.CacheBounds.First != 1 {
		t.Fatal("row or bound aliases cache")
	}
	pinned, ok := c.ChunkBody(id, input.Descriptor.Digest)
	if !ok || pinned != input.Body {
		t.Fatal("canonical body not retained")
	}
	if err := c.Reset(binding); err != nil {
		t.Fatal(err)
	}
	if again, err := c.Cursor(context.Background(), id, 0); err != nil || again.Count != 3 {
		t.Fatal("same recording reset history")
	}
	binding.RecordingGeneration++
	if err := c.Reset(binding); err != nil {
		t.Fatal(err)
	}
	newID, err := ReceptionRecordingID(binding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Cursor(context.Background(), id, 0); err == nil || c.ImportChunks(id, []ReceptionHistoryChunkInput{input}) == nil {
		t.Fatal("retired recording accepted")
	}
	if got, err := c.Cursor(context.Background(), newID, 0); err != nil || got.Count != 0 || got.CacheBounds.First != nil {
		t.Fatal("new recording inherited history")
	}
	if pinned != input.Body {
		t.Fatal("reset mutated pinned immutable string")
	}
	binding.RecordingGeneration--
	if c.Reset(binding) == nil {
		t.Fatal("same-node generation regressed")
	}
	binding.RunID = "replacement-run"
	if err := c.Reset(binding); err != nil {
		t.Fatal("new node context refused", err)
	}
}

func TestReceptionReaderAtomicImportAndInsideChunk(t *testing.T) {
	c, _, id := receptionReaderFixture(t)
	first := receptionReaderInput(receptionChunkFixture(t, 1, 128, false))
	second := receptionReaderInput(receptionChunkFixture(t, 129, 128, false))
	inputs := []ReceptionHistoryChunkInput{first, second}
	if err := c.ImportChunks(id, inputs); err != nil {
		t.Fatal(err)
	}
	inputs[0] = ReceptionHistoryChunkInput{}
	got, err := c.Cursor(context.Background(), id, 3)
	if err != nil || got.Count != 128 || got.Rows[0].Sequence != 4 || got.Rows[127].Sequence != 131 || got.ScannedThrough != 131 || !got.More {
		t.Fatal("inside-chunk continuation skipped or duplicated", err)
	}
	next, err := c.Cursor(context.Background(), id, got.ScannedThrough)
	if err != nil || next.Count != 125 || next.Rows[0].Sequence != 132 || next.ScannedThrough != 256 || next.More {
		t.Fatal("continuation consumed lookahead", err)
	}
	index, gap, ok := c.Index(id, 3)
	if !ok || gap || len(index.Chunks) != 2 || index.Chunks[0].FirstSequence != 1 {
		t.Fatal("index lost containing descriptor")
	}
	bad := receptionReaderInput(receptionChunkFixture(t, 258, 1, false))
	bad.Body += "\n"
	if c.ImportChunks(id, []ReceptionHistoryChunkInput{receptionReaderInput(receptionChunkFixture(t, 257, 1, false)), bad}) == nil {
		t.Fatal("invalid batch accepted")
	}
	if got, err := c.Cursor(context.Background(), id, 256); err != nil || got.Count != 0 || got.ScannedThrough != 256 {
		t.Fatal("failed batch partly installed")
	}
	if c.ImportChunks(id, make([]ReceptionHistoryChunkInput, 9)) == nil ||
		c.ImportChunks(id, []ReceptionHistoryChunkInput{receptionReaderInput(receptionChunkFixture(t, 2, 1, false))}) == nil ||
		c.ImportChunks(id, []ReceptionHistoryChunkInput{receptionReaderInput(receptionChunkFixture(t, 1, 128, true))}) == nil {
		t.Fatal("oversize, overlap or changed interval accepted")
	}
	if err := c.ImportChunks(id, []ReceptionHistoryChunkInput{first, second}); err != nil {
		t.Fatal("exact body reuse refused", err)
	}
	if _, err := c.Cursor(context.Background(), strings.Repeat("0", 64), 0); err == nil {
		t.Fatal("wrong recording read accepted")
	}
}

func TestReceptionReaderQueryWallReversalAndScannedCursor(t *testing.T) {
	c, _, id := receptionReaderFixture(t)
	at := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	dates := []time.Time{at.Add(time.Hour), at, at.Add(2 * time.Second), at.Add(time.Second), at.Add(-time.Hour)}
	input := receptionReaderDatedInput(t, 1, dates)
	if err := c.ImportChunks(id, []ReceptionHistoryChunkInput{input}); err != nil {
		t.Fatal(err)
	}
	got, err := c.Query(context.Background(), id, at, at.Add(2*time.Second), 0)
	if err != nil || got.Count != 2 || got.Rows[0].Sequence != 2 || got.Rows[1].Sequence != 4 ||
		got.ScannedThrough != 5 || got.ScannedRows != 5 || got.More {
		t.Fatal("wall-order shortcut or match-only continuation", err)
	}
	if next, err := c.Query(context.Background(), id, at, at.Add(2*time.Second), got.ScannedThrough); err != nil || next.Count != 0 || next.ScannedThrough != 5 {
		t.Fatal("query continuation repeats unmatched tail")
	}
	none, err := c.Query(context.Background(), id, at.Add(10*time.Second), at.Add(11*time.Second), 0)
	if err != nil || none.Count != 0 || none.ScannedThrough != 5 {
		t.Fatal("empty filter failed to advance scanned cursor")
	}
	if _, err := c.Query(context.Background(), id, at, at.Add(time.Minute), 0); err != nil {
		t.Fatal("exact sixty-second range refused", err)
	}
	for name, pair := range map[string][2]time.Time{
		"empty": {at, at}, "backwards": {at, at.Add(-time.Nanosecond)}, "too_wide": {at, at.Add(time.Minute + time.Nanosecond)},
		"offset":            {at.In(time.FixedZone("offset", 3600)), at.Add(time.Second)},
		"year_zero":         {time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), at},
		"year_ten_thousand": {at, time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := c.Query(context.Background(), id, pair[0], pair[1], 0); err == nil {
				t.Fatal("invalid time span accepted")
			}
		})
	}
	firstDate := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	if got, err := c.Query(context.Background(), id, firstDate, firstDate.Add(time.Second), 0); err != nil || got.ScannedThrough != 5 {
		t.Fatal("year-one lower query boundary treated as missing", err)
	}
}

func TestReceptionReaderGapsFutureAndMaxUint(t *testing.T) {
	c, _, id := receptionReaderFixture(t)
	inputs := []ReceptionHistoryChunkInput{receptionReaderInput(receptionChunkFixture(t, 2, 2, false)), receptionReaderInput(receptionChunkFixture(t, 6, 1, false))}
	if err := c.ImportChunks(id, inputs); err != nil {
		t.Fatal(err)
	}
	for _, after := range []uint64{0, 3, 4} {
		got, err := c.Cursor(context.Background(), id, after)
		want := uint64(6)
		if after == 0 {
			want = 2
		}
		if err == nil || err.Error() != "cache_gap" || got.NextAvailable == nil || *got.NextAvailable != want || got.Count != 0 || got.ScannedThrough != after {
			t.Fatal("gap silently skipped", after, err)
		}
	}
	// A query cannot hide an unavailable interval just because dates in that
	// interval might not match; cache coverage is separate from time matching.
	at := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := c.Query(context.Background(), id, at, at.Add(time.Second), 1); err == nil || err.Error() != "cache_gap" {
		t.Fatal("filtered interior gap ignored")
	}
	if future, err := c.Cursor(context.Background(), id, 99); err != nil || future.Count != 0 || future.ScannedThrough != 99 || future.NextAvailable != nil || future.More {
		t.Fatal("future cursor did not remain unchanged", err)
	}
	maximum := receptionReaderInput(receptionChunkFixture(t, math.MaxUint64-127, 128, true))
	if err := c.ImportChunks(id, []ReceptionHistoryChunkInput{maximum}); err != nil {
		t.Fatal(err)
	}
	got, err := c.Cursor(context.Background(), id, math.MaxUint64-1)
	if err != nil || got.Count != 1 || got.Rows[0].Sequence != math.MaxUint64 || got.ScannedThrough != math.MaxUint64 || got.More {
		t.Fatal("max cursor overflow", err)
	}
	got, err = c.Cursor(context.Background(), id, math.MaxUint64)
	if err != nil || got.Count != 0 || got.ScannedThrough != math.MaxUint64 || got.NextAvailable != nil || got.More {
		t.Fatal("future/max cursor renewed or invented gap", err)
	}
}

func TestReceptionReaderQueryBudgetAndNoSourceCalls(t *testing.T) {
	c, _, id := receptionReaderFixture(t)
	if err := c.ImportChunks(id, []ReceptionHistoryChunkInput{receptionReaderInput(receptionChunkFixture(t, 1, 128, false))}); err != nil {
		t.Fatal(err)
	}
	if got, err := c.scan(context.Background(), id, 0, time.Time{}, time.Time{}, time.Now().Add(-time.Nanosecond)); err == nil || err.Error() != "query_budget" || got.Count != 0 {
		t.Fatal("expired scan budget accepted")
	}
	c.scanMu.Lock()
	_, err := c.Cursor(context.Background(), id, 0)
	c.scanMu.Unlock()
	if err == nil || err.Error() != "query_budget" {
		t.Fatal("concurrent scan not bounded")
	}
	c.mu.Lock()
	_, err = c.Cursor(context.Background(), id, 0)
	c.mu.Unlock()
	if err == nil || err.Error() != "query_budget" {
		t.Fatal("scan waited for cache writer")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := c.Cursor(ctx, id, 0); !errors.Is(err, context.Canceled) || got.Count != 0 {
		t.Fatal("canceled scan accepted", err)
	}
	// This concrete owner has no function, interface, source, client or worker
	// field. HTTP may only use its detached result/body accessors in the later
	// route slice; this check is not a substitute for that integration test.
	for _, typ := range []reflect.Type{reflect.TypeFor[ReceptionHistoryReaderCache](), reflect.TypeFor[ReceptionHistoryCache]()} {
		for i := 0; i < typ.NumField(); i++ {
			if kind := typ.Field(i).Type.Kind(); kind == reflect.Func || kind == reflect.Interface {
				t.Fatal("cache gained source callback/interface", typ.Field(i).Name)
			}
		}
	}
}

func TestReceptionReaderMetadataOriginalTTLAndReset(t *testing.T) {
	c, binding, id := receptionReaderFixture(t)
	now := time.Now()
	m := receptionTestMetadata()
	m.Binding, m.Revision = binding, 1
	m.Sources = []ReceptionSource{{Generation: 1, StartedAt: m.PublishedAt, Kind: "live"}}
	p, err := PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	manifest, body := p.Manifest(), p.Body()
	page, ok := p.Page("sources", 0)
	if !ok {
		t.Fatal("fixture page missing")
	}
	if c.InstallMetadata(id, p, now.UTC()) == nil {
		t.Fatal("wall-only TTL clock accepted")
	}
	oversized := *p
	oversized.bytes = MaxReceptionMetadataBytes + 1
	if c.InstallMetadata(id, &oversized, now) == nil {
		t.Fatal("oversized bundle accepted")
	}
	if err := c.InstallMetadata(id, p, now); err != nil {
		t.Fatal(err)
	}
	if err := c.InstallMetadata(id, p, now.Add(29*time.Second)); err != nil {
		t.Fatal(err)
	}
	*p = PreparedReceptionMetadata{}
	got, ok := c.MetadataManifest(id, manifest.Digest, now.Add(29*time.Second))
	if !ok || !reflect.DeepEqual(got, manifest) {
		t.Fatal("caller header mutation or refresh changed token")
	}
	got.Pages[0].Digest = "changed"
	if value, d, ok := c.MetadataPage(id, manifest.Digest, "sources", 0, now.Add(29*time.Second)); !ok || value != page || d.Digest != manifest.Pages[0].Digest {
		t.Fatal("metadata/page not detached")
	}
	if _, ok := c.MetadataManifest(id, manifest.Digest, now.Add(28*time.Second)); !ok {
		t.Fatal("out-of-order viewer lock acquisition falsely expired token")
	}
	if _, _, ok := c.MetadataPage(id, manifest.Digest, "sources", 0, now.Add(30*time.Second)); ok {
		t.Fatal("token TTL renewed")
	}
	if _, ok := c.MetadataManifest(id, manifest.Digest, now.Add(time.Second)); ok {
		t.Fatal("old clock revived expired token")
	}
	p, err = PrepareReceptionMetadata(m)
	if err != nil || p.Body() != body {
		t.Fatal("fixture changed", err)
	}
	if c.InstallMetadata(id, p, now.Add(31*time.Second)) == nil {
		t.Fatal("expired highest token reinstalled")
	}
	m.Revision++
	m.PublishedAt = m.PublishedAt.Add(-time.Hour)
	p, err = PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.InstallMetadata(id, p, now.Add(31*time.Second)); err != nil {
		t.Fatal("source wall reversal changed monotonic TTL", err)
	}
	if _, _, ok := c.MetadataPage(id, manifest.Digest, "sources", 0, now.Add(31*time.Second)); ok {
		t.Fatal("old token redirected to current page")
	}
	if got, ok := c.MetadataManifest(id, "", now.Add(31*time.Second)); !ok || got.Revision != 2 || !got.PublishedAt.Equal(m.PublishedAt) {
		t.Fatal("latest token date changed")
	}
	binding.RecordingGeneration++
	if err := c.Reset(binding); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.MetadataManifest(id, "", now.Add(32*time.Second)); ok || c.metadataBytes != 0 || c.metadataRevision != 0 {
		t.Fatal("reset kept old metadata")
	}
}

func TestReceptionReaderMetadataSixBundlesAndHighwater(t *testing.T) {
	c, binding, id := receptionReaderFixture(t)
	m := receptionTestMetadata()
	m.Binding = binding
	now := time.Now()
	var tokens [7]string
	for i := range 7 {
		m.Revision = uint64(i + 1)
		p, err := PrepareReceptionMetadata(m)
		if err != nil {
			t.Fatal(err)
		}
		tokens[i] = p.manifest.Digest
		if err := c.InstallMetadata(id, p, now.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := c.MetadataManifest(id, tokens[0], now.Add(7*time.Second)); ok {
		t.Fatal("seventh bundle did not retire oldest")
	}
	for _, token := range tokens[1:] {
		if _, ok := c.MetadataManifest(id, token, now.Add(7*time.Second)); !ok {
			t.Fatal("one of six retained bundles missing")
		}
	}
	m.Revision = 1
	p, err := PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	if c.InstallMetadata(id, p, now.Add(8*time.Second)) == nil {
		t.Fatal("capacity-evicted old token revived")
	}
	m.Revision = 8
	m.StartupAtCapture = ReceptionStartup{State: "unknown", Limited: true}
	p, err = PrepareReceptionMetadata(m)
	if err != nil || c.InstallMetadata(id, p, now.Add(8*time.Second)) != nil {
		t.Fatal("terminal unknown fixture failed", err)
	}
	m.Revision++
	m.StartupAtCapture = ReceptionStartup{State: "open"}
	p, err = PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	if c.InstallMetadata(id, p, now.Add(40*time.Second)) == nil {
		t.Fatal("expiry erased terminal startup highwater")
	}
	if c.metadataBytes != 0 || c.metadataRevision != 8 {
		t.Fatal("expiry retained pages or erased highwater")
	}
}

func TestReceptionReaderConcurrentCopiesAndRetention(t *testing.T) {
	c, _, id := receptionReaderFixture(t)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				index, _, ok := c.Index(id, 0)
				if !ok {
					t.Error("recording disappeared")
					return
				}
				for _, d := range index.Chunks {
					if body, ok := c.ChunkBody(id, d.Digest); ok && receptionDigest([]byte(body)) != d.Digest {
						t.Error("body mutated")
					}
				}
				got, err := c.Cursor(context.Background(), id, 0)
				if err != nil && err.Error() != "query_budget" && err.Error() != "cache_gap" {
					t.Error(err)
				}
				if err == nil {
					for j := 1; j < got.Count; j++ {
						if got.Rows[j].Sequence != got.Rows[j-1].Sequence+1 {
							t.Error("torn rows")
						}
					}
				}
			}
		}()
	}
	for i := uint64(1); i <= 264; i++ {
		if err := c.ImportChunks(id, []ReceptionHistoryChunkInput{receptionReaderInput(receptionChunkFixture(t, i, 1, false))}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	index, gap, ok := c.Index(id, 0)
	if !ok || !gap || *index.CacheBounds.First != 9 || *index.CacheBounds.Last != 264 || c.encoded.count != 256 {
		t.Fatal("retention changed")
	}
	got, err := c.Cursor(context.Background(), id, 8)
	if err != nil || got.Count != 128 || got.Rows[0].Sequence != 9 || got.ScannedThrough != 136 {
		t.Fatal("parsed ring did not track encoded eviction", err)
	}
	if c.ImportChunks(id, []ReceptionHistoryChunkInput{receptionReaderInput(receptionChunkFixture(t, 1, 1, false))}) == nil {
		t.Fatal("evicted interval revived")
	}
}
