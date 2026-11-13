package dashboard

import (
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func receptionCacheFixture(t *testing.T) (*ReceptionHistoryCache, ReceptionRecordingBinding, string) {
	t.Helper()
	b := receptionTestMetadata().Binding
	b.RecordingGeneration = 1
	c, err := NewReceptionHistoryCache(true, b)
	if err != nil {
		t.Fatal(err)
	}
	id, err := ReceptionRecordingID(b)
	if err != nil {
		t.Fatal(err)
	}
	return c, b, id
}

func TestReceptionCacheDisabledAndReplacement(t *testing.T) {
	c, err := NewReceptionHistoryCache(false, ReceptionRecordingBinding{})
	if err != nil || c != nil {
		t.Fatal("disabled construction did work")
	}
	if _, _, ok := c.Index("", 0); ok {
		t.Fatal("disabled index present")
	}
	if _, ok := c.ChunkBody("", ""); ok {
		t.Fatal("disabled body present")
	}
	if _, ok := c.MetadataManifest(""); ok {
		t.Fatal("disabled metadata present")
	}
	if _, ok := c.MetadataPage("", "", "", 0); ok {
		t.Fatal("disabled page present")
	}
	if c.InstallChunks("", nil) == nil || c.InstallMetadata("", nil, time.Time{}) == nil || c.Reset(ReceptionRecordingBinding{}) == nil {
		t.Fatal("disabled write accepted")
	}
	c, b, id := receptionCacheFixture(t)
	p := receptionChunkFixture(t, 1, 1, false)
	if err := c.InstallChunks(id, []PreparedReceptionChunk{p}); err != nil {
		t.Fatal(err)
	}
	if c.InstallChunks(strings.Repeat("0", 64), []PreparedReceptionChunk{p}) == nil {
		t.Fatal("foreign recording accepted")
	}
	if c.Reset(ReceptionRecordingBinding{}) == nil {
		t.Fatal("invalid reset accepted")
	}
	if err := c.Reset(b); err != nil {
		t.Fatal(err)
	}
	if body, ok := c.ChunkBody(id, p.Descriptor().Digest); !ok || body != p.Body() {
		t.Fatal("same binding cleared history")
	}
	b.RecordingGeneration++
	if err := c.Reset(b); err != nil {
		t.Fatal(err)
	}
	newID, err := ReceptionRecordingID(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.ChunkBody(id, p.Descriptor().Digest); ok {
		t.Fatal("retired recording served")
	}
	if index, _, ok := c.Index(newID, 0); !ok || len(index.Chunks) != 0 || index.CacheBounds.First != nil {
		t.Fatal("new recording retained bodies")
	}
	b.RecordingGeneration--
	if c.Reset(b) == nil {
		t.Fatal("same-node generation regressed")
	}
	b.RunID = "new-run"
	if err := c.Reset(b); err != nil {
		t.Fatal("new context cannot reset generation")
	}
}

func TestReceptionCacheBoundedImportsAndGaps(t *testing.T) {
	c, _, id := receptionCacheFixture(t)
	a, b := receptionChunkFixture(t, 1, 2, false), receptionChunkFixture(t, 5, 2, false)
	batch := []PreparedReceptionChunk{a, b}
	if err := c.InstallChunks(id, batch); err != nil {
		t.Fatal(err)
	}
	batch[0] = PreparedReceptionChunk{}
	if body, ok := c.ChunkBody(id, a.Descriptor().Digest); !ok || body != a.Body() {
		t.Fatal("input slice aliases cache")
	}
	index, gap, ok := c.Index(id, 0)
	if !ok || gap || len(index.Chunks) != 2 || *index.CacheBounds.First != 1 || *index.CacheBounds.Last != 6 {
		t.Fatal("initial cache bounds differ")
	}
	index.Chunks[0].Digest = "changed"
	*index.CacheBounds.First = 99
	index, gap, ok = c.Index(id, 2)
	if !ok || !gap || len(index.Chunks) != 1 || index.Chunks[0].FirstSequence != 5 || *index.CacheBounds.First != 1 {
		t.Fatal("interior gap or detached bounds lost")
	}
	if _, err := PrepareReceptionHistoryIndex(index); err != nil {
		t.Fatal(err)
	}
	index, gap, ok = c.Index(id, math.MaxUint64)
	if !ok || gap || len(index.Chunks) != 0 {
		t.Fatal("future cursor invented gap")
	}
	if err := c.InstallChunks(id, []PreparedReceptionChunk{a, b}); err != nil {
		t.Fatal("exact reuse refused")
	}
	changed := receptionChunkFixture(t, 1, 2, true)
	if c.InstallChunks(id, []PreparedReceptionChunk{changed}) == nil {
		t.Fatal("same interval mutated")
	}
	if c.InstallChunks(id, []PreparedReceptionChunk{receptionChunkFixture(t, 2, 2, false)}) == nil {
		t.Fatal("partial overlap accepted")
	}
	if c.InstallChunks(id, make([]PreparedReceptionChunk, 9)) == nil {
		t.Fatal("nine imports accepted")
	}
	if c.InstallChunks(id, []PreparedReceptionChunk{receptionChunkFixture(t, 7, 1, false), PreparedReceptionChunk{}}) == nil {
		t.Fatal("invalid batch accepted")
	}
	index, _, _ = c.Index(id, 6)
	if len(index.Chunks) != 0 {
		t.Fatal("failed batch partly committed")
	}
}

func TestReceptionCacheEvictionAndMaxCursor(t *testing.T) {
	c, _, id := receptionCacheFixture(t)
	first := receptionChunkFixture(t, 1, 1, false)
	var batch []PreparedReceptionChunk
	for i := uint64(1); i <= 256; i++ {
		batch = append(batch, receptionChunkFixture(t, i, 1, false))
		if len(batch) == 8 {
			if err := c.InstallChunks(id, batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	pinned, ok := c.ChunkBody(id, first.Descriptor().Digest)
	if !ok {
		t.Fatal("first body missing")
	}
	if err := c.InstallChunks(id, []PreparedReceptionChunk{receptionChunkFixture(t, 257, 1, false)}); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.ChunkBody(id, first.Descriptor().Digest); ok || pinned != first.Body() {
		t.Fatal("eviction or pinned string changed")
	}
	index, gap, ok := c.Index(id, 0)
	if !ok || !gap || len(index.Chunks) != 8 || *index.CacheBounds.First != 2 || *index.CacheBounds.Last != 257 {
		t.Fatal("retention bound or gap differs")
	}
	if c.count != 256 || c.InstallChunks(id, []PreparedReceptionChunk{first}) == nil {
		t.Fatal("evicted chunk revived")
	}
	var after uint64 = 1
	var seen int
	for {
		index, gap, ok = c.Index(id, after)
		if !ok || gap {
			t.Fatal("retained continuous cursor failed")
		}
		if len(index.Chunks) == 0 {
			break
		}
		seen += len(index.Chunks)
		after = index.Chunks[len(index.Chunks)-1].LastSequence
	}
	if seen != 256 {
		t.Fatal("paged index shortened retention")
	}
	maximum := receptionChunkFixture(t, math.MaxUint64-127, 128, true)
	if err := c.InstallChunks(id, []PreparedReceptionChunk{maximum}); err != nil {
		t.Fatal(err)
	}
	index, gap, ok = c.Index(id, math.MaxUint64)
	if !ok || gap || len(index.Chunks) != 0 {
		t.Fatal("maximum cursor overflow")
	}
	index, gap, ok = c.Index(id, math.MaxUint64-1)
	if !ok || gap || len(index.Chunks) != 1 {
		t.Fatal("cursor inside final chunk refused")
	}
}

func TestReceptionCacheMetadataHoldAndNoRenewal(t *testing.T) {
	c, b, id := receptionCacheFixture(t)
	m := receptionTestMetadata()
	m.Binding, m.Revision = b, 1
	m.Sources = []ReceptionSource{{Generation: 1, StartedAt: m.PublishedAt, Kind: "live"}}
	prepare := func() *PreparedReceptionMetadata {
		p, err := PrepareReceptionMetadata(m)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	now := time.Now()
	first := prepare()
	if c.InstallMetadata(id, first, now.UTC()) == nil {
		t.Fatal("wall-only hold clock accepted")
	}
	if err := c.InstallMetadata(id, first, now); err != nil {
		t.Fatal(err)
	}
	if err := c.InstallMetadata(id, first, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if c.metadataInstalledAt != now {
		t.Fatal("same token renewed hold")
	}
	m.Revision++
	m.PublishedAt = m.PublishedAt.Add(-time.Second)
	second := prepare()
	if c.InstallMetadata(id, second, now.Add(6*time.Second-time.Nanosecond)) == nil {
		t.Fatal("early metadata replacement accepted")
	}
	if err := c.InstallMetadata(id, second, now.Add(6*time.Second)); err != nil {
		t.Fatal("wall reversal invalidated monotonic hold", err)
	}
	if _, ok := c.MetadataPage(id, first.manifest.Digest, "sources", 0); !ok {
		t.Fatal("previous advertised token retired early")
	}
	copy, ok := c.MetadataManifest(id)
	if !ok {
		t.Fatal("current metadata missing")
	}
	copy.Pages[0].Digest = "changed"
	if again, _ := c.MetadataManifest(id); again.Pages[0].Digest == "changed" {
		t.Fatal("metadata copy aliases cache")
	}
	m.Revision++
	third := prepare()
	if err := c.InstallMetadata(id, third, now.Add(12*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.MetadataPage(id, first.manifest.Digest, "sources", 0); ok {
		t.Fatal("third bundle retained or token redirected")
	}
	if _, ok := c.MetadataPage(id, second.manifest.Digest, "sources", 0); !ok {
		t.Fatal("second token lost its six-second hold")
	}
	if c.InstallMetadata(id, first, now.Add(18*time.Second)) == nil {
		t.Fatal("revision regression accepted")
	}
	m.PublishedAt = m.PublishedAt.Add(-time.Second)
	if c.InstallMetadata(id, prepare(), now.Add(18*time.Second)) == nil {
		t.Fatal("same revision changed capture")
	}
	m.Revision++
	m.StartupAtCapture = ReceptionStartup{State: "unknown", Limited: true}
	if err := c.InstallMetadata(id, prepare(), now.Add(18*time.Second)); err != nil {
		t.Fatal(err)
	}
	m.Revision++
	m.StartupAtCapture = ReceptionStartup{State: "open"}
	if c.InstallMetadata(id, prepare(), now.Add(24*time.Second)) == nil {
		t.Fatal("terminal marker revived")
	}
	b.RecordingGeneration++
	if err := c.Reset(b); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.MetadataPage(id, second.manifest.Digest, "sources", 0); ok {
		t.Fatal("old recording metadata served")
	}
}

func TestReceptionCacheOwnsInstalledMetadata(t *testing.T) {
	c, b, id := receptionCacheFixture(t)
	m := receptionTestMetadata()
	m.Binding, m.Revision = b, 1
	m.Sources = []ReceptionSource{{Generation: 1, StartedAt: m.PublishedAt, Kind: "live"}}
	p, err := PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	want := p.Manifest()
	body, ok := p.Page("sources", 0)
	if !ok {
		t.Fatal("fixture page missing")
	}
	if err := c.InstallMetadata(id, p, time.Now()); err != nil {
		t.Fatal(err)
	}
	*p = PreparedReceptionMetadata{}
	if got, ok := c.MetadataManifest(id); !ok || !reflect.DeepEqual(got, want) {
		t.Fatal("caller overwrite changed installed metadata")
	}
	if got, ok := c.MetadataPage(id, want.Digest, "sources", 0); !ok || got != body {
		t.Fatal("caller overwrite changed installed page")
	}
	m.Revision++
	p, err = PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	second := p.Manifest()
	if err := c.InstallMetadata(id, p, c.metadataInstalledAt.Add(receptionMetadataHold)); err != nil {
		t.Fatal(err)
	}
	*p = PreparedReceptionMetadata{}
	if got, ok := c.MetadataManifest(id); !ok || !reflect.DeepEqual(got, second) {
		t.Fatal("caller overwrite changed replacement metadata")
	}
	if got, ok := c.MetadataPage(id, want.Digest, "sources", 0); !ok || got != body {
		t.Fatal("caller overwrite changed previous page")
	}
}

func TestReceptionCacheConcurrentCopiesAndShape(t *testing.T) {
	c, _, id := receptionCacheFixture(t)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 300 {
				index, _, ok := c.Index(id, 0)
				if !ok {
					t.Error("recording changed")
					return
				}
				for _, d := range index.Chunks {
					if body, ok := c.ChunkBody(id, d.Digest); ok && receptionDigest([]byte(body)) != d.Digest {
						t.Error("body changed")
						return
					}
				}
			}
		}()
	}
	for i := uint64(1); i <= 300; i++ {
		if err := c.InstallChunks(id, []PreparedReceptionChunk{receptionChunkFixture(t, i, 1, false)}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if reflect.TypeOf(c.chunks).Kind() != reflect.Array || len(c.chunks) != 256 || unsafe.Sizeof(PreparedReceptionChunk{}) != unsafe.Sizeof(ReceptionChunkDescriptor{})+unsafe.Sizeof("") {
		t.Fatal("cache no longer holds only fixed descriptors and body strings")
	}
	// The combined source/node test imports this concrete type and counts all
	// retained bodies plus its inline array. No independent cache generation or
	// parsed rows are retained by this type; metadata has two explicit pointers.
	if reflect.TypeOf(c.currentMetadata) != reflect.TypeOf((*PreparedReceptionMetadata)(nil)) ||
		reflect.TypeOf(c.previousMetadata) != reflect.TypeOf((*PreparedReceptionMetadata)(nil)) {
		t.Fatal("metadata holder changed")
	}
}
