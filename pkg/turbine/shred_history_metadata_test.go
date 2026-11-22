package turbine

import (
	"crypto/sha256"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func historyCheckMetadata(t *testing.T, h *ShredHistory) ShredHistoryMetadata {
	t.Helper()
	s, ok := h.Snapshot()
	if !ok {
		t.Fatal("snapshot unavailable")
	}
	want := ShredHistoryMetadata{
		Binding: s.Binding, PublishedAt: s.PublishedAt,
		Chunks:  make([]ShredHistoryChunkDescriptor, len(s.Chunks)),
		Sources: s.Sources, StartupRanges: s.StartupRanges, Marker: s.Marker, MarkerState: s.MarkerState,
		PrefixCutoff: s.PrefixCutoff, LastSequence: s.LastSequence,
		QueueRefusals: s.QueueRefusals, InvalidInputs: s.InvalidInputs, RangeRefusals: s.RangeRefusals,
		DescriptorEvictions: s.DescriptorEvictions, EvictedChunks: s.EvictedChunks,
		EvictedRows: s.EvictedRows, ChunkRefusals: s.ChunkRefusals,
		StartupLimited: s.StartupLimited, Interrupted: s.Interrupted, Exhausted: s.Exhausted, Closed: s.Closed,
	}
	for i, c := range s.Chunks {
		want.Chunks[i] = ShredHistoryChunkDescriptor{
			FirstSequence: c.FirstSequence, LastSequence: c.LastSequence, Count: uint64(len(c.Rows())),
			MinAt: c.MinAt, MaxAt: c.MaxAt, Digest: sha256.Sum256([]byte(c.Body())), Bytes: uint64(len(c.Body())),
		}
	}
	got, ok := h.Metadata()
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("metadata differs from controlled snapshot: got %+v, want %+v", got, want)
	}
	return got
}

func TestShredHistoryMetadataDisabledAndBodyFree(t *testing.T) {
	for _, h := range []*ShredHistory{nil, {}} {
		if m, ok := h.Metadata(); ok || !reflect.DeepEqual(m, ShredHistoryMetadata{}) {
			t.Fatal("nil or zero owner exposed metadata")
		}
		if body, ok := h.ChunkBody(0, 0, [32]byte{}); ok || body != "" {
			t.Fatal("nil or zero owner exposed a body")
		}
	}
	// Allow only the concrete metadata graph. time.Time is an opaque clock
	// value; its location pointer is not a source/chunk/body reference.
	var inspect func(reflect.Type, string)
	inspect = func(typ reflect.Type, path string) {
		if typ == reflect.TypeOf(time.Time{}) {
			return
		}
		if typ == reflect.TypeOf(ShredHistoryChunk{}) || typ == reflect.TypeOf(ShredHistoryRow{}) || typ == reflect.TypeOf(ShredHistoryReceipt{}) || typ == reflect.TypeOf(ShredHistorySnapshot{}) {
			t.Fatalf("metadata retains forbidden source type at %s", path)
		}
		switch typ.Kind() {
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				field := typ.Field(i)
				inspect(field.Type, path+"."+field.Name)
			}
		case reflect.Array:
			inspect(typ.Elem(), path+"[]")
		case reflect.Slice:
			if typ.Elem() != reflect.TypeOf(ShredHistoryChunkDescriptor{}) && typ.Elem() != reflect.TypeOf(ShredHistorySource{}) && typ.Elem() != reflect.TypeOf(ShredHistoryRange{}) {
				t.Fatalf("unexpected metadata slice at %s", path)
			}
			inspect(typ.Elem(), path+"[]")
		case reflect.Pointer:
			if typ != reflect.TypeOf((*ShredHistoryMarker)(nil)) {
				t.Fatalf("unexpected metadata pointer at %s", path)
			}
			inspect(typ.Elem(), path+"*")
		case reflect.String:
			if path != "Metadata.Binding.RunID" {
				t.Fatalf("metadata contains a possible body string at %s", path)
			}
		case reflect.Bool, reflect.Uint8, reflect.Uint32, reflect.Uint64:
		default:
			t.Fatalf("unexpected metadata kind %s at %s", typ.Kind(), path)
		}
	}
	inspect(reflect.TypeOf(ShredHistoryMetadata{}), "Metadata")
	h := historyOwner(t, historyLimits())
	want := historyCheckMetadata(t, h)
	full := h.publication.Swap(nil)
	got, ok := h.Metadata() // A full-publication load or Snapshot call cannot work here.
	h.publication.Store(full)
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatal("metadata depends on the full publication")
	}
}

func TestShredHistoryMetadataPublicationAndCopies(t *testing.T) {
	h := historyOwner(t, historyLimits())
	prewarm := historySource(t, h, ShredHistoryPrewarm)
	live := historySource(t, h, ShredHistoryLive)
	at := time.Now()
	for i, origin := range []ShredHistoryOrigin{ShredHistoryTVU, ShredHistoryRepairMatched, ShredHistoryRepairUnmatched} {
		r := historyReceipt(uint64(7+2*i), origin)
		r.At = at.Add(-time.Duration(i) * time.Second)
		if !live.TryReceipt(r) {
			t.Fatal("receipt refused")
		}
	}
	historyDrain(h)
	m := historyCheckMetadata(t, h)
	if m.PublishedAt == m.PublishedAt.Round(0) || m.Chunks[0].MinAt != at.Add(-time.Second).UTC() || m.Chunks[0].MaxAt != at.UTC() || h.chunks[0].rows[0].At != at {
		t.Fatal("publication/receipt monotonic clock or original UTC bounds changed")
	}
	if !prewarm.TryMarker(historyMarker()) {
		t.Fatal("marker refused")
	}
	historyCheckMetadata(t, h)
	h.consume(<-h.queue)
	m = historyCheckMetadata(t, h)
	if m.MarkerState != ShredHistoryMarkerQueued || m.Marker != nil {
		t.Fatal("unpublished closed marker must remain queued")
	}
	prewarm.Retire()
	h.publish(time.Now(), false)
	want := historyCheckMetadata(t, h)
	if want.MarkerState != ShredHistoryMarkerClosed || want.Marker == nil || want.PrefixCutoff != 3 || !want.Sources[0].Retired {
		t.Fatal("published marker/retirement differs")
	}
	internal, full := h.metadata.Load(), h.publication.Load()
	if &internal.Sources[0] != &full.Sources[0] || internal.Marker != full.Marker {
		t.Fatal("internal metadata unexpectedly duplicates shared immutable buffers")
	}
	for i := range internal.StartupRanges {
		if &internal.StartupRanges[i][0] != &full.StartupRanges[i][0] {
			t.Fatal("internal ranges are not shared with their publication")
		}
	}
	got, _ := h.Metadata()
	got.Binding.RunID = "changed"
	got.Binding.NodeStartedAt = time.Time{}
	got.Binding.GenesisHash[0]++
	got.PublishedAt = time.Time{}
	got.Chunks[0] = ShredHistoryChunkDescriptor{}
	got.Sources[0] = ShredHistorySource{}
	for i := range got.StartupRanges {
		got.StartupRanges[i][0] = ShredHistoryRange{}
	}
	*got.Marker = ShredHistoryMarker{}
	got.MarkerState, got.PrefixCutoff, got.LastSequence = ShredHistoryMarkerUnknown, 0, 0
	got.QueueRefusals = ShredHistoryCounter{Value: 77}
	got.RangeRefusals[0] = ShredHistoryCounter{Value: 88}
	got.Closed, got.Interrupted, got.Exhausted, got.StartupLimited = true, true, true, true
	fresh, _ := h.Metadata()
	if !reflect.DeepEqual(fresh, want) {
		t.Fatal("caller mutation changed published metadata")
	}
}

func TestShredHistoryChunkBodyIdentityEvictionAndClose(t *testing.T) {
	l := historyLimits()
	l.rows, l.chunks = 1, 1
	h := historyOwner(t, l)
	source := historySource(t, h, ShredHistoryLive)
	if !source.TryReceipt(historyReceipt(1, ShredHistoryTVU)) {
		t.Fatal("receipt refused")
	}
	historyDrain(h)
	d := historyCheckMetadata(t, h).Chunks[0]
	body, ok := h.ChunkBody(d.FirstSequence, d.LastSequence, d.Digest)
	if !ok || sha256.Sum256([]byte(body)) != d.Digest || uint64(len(body)) != d.Bytes || unsafe.StringData(body) != unsafe.StringData(h.chunks[0].body) {
		t.Fatal("lookup changed the encoded body, digest or backing storage")
	}
	wrong := d.Digest
	wrong[0]++
	for _, query := range []ShredHistoryChunkDescriptor{
		{FirstSequence: d.FirstSequence + 1, LastSequence: d.LastSequence, Digest: d.Digest},
		{FirstSequence: d.FirstSequence, LastSequence: d.LastSequence + 1, Digest: d.Digest},
		{FirstSequence: d.FirstSequence, LastSequence: d.LastSequence, Digest: wrong},
	} {
		if value, found := h.ChunkBody(query.FirstSequence, query.LastSequence, query.Digest); found || value != "" {
			t.Fatal("lookup accepted a mismatched identity")
		}
	}
	if allocations := testing.AllocsPerRun(100, func() {
		if value, found := h.ChunkBody(d.FirstSequence, d.LastSequence, d.Digest); !found || value != body {
			panic("immutable lookup differs")
		}
	}); allocations != 0 {
		t.Fatalf("body lookup allocations = %g, want 0", allocations)
	}
	source.Retire()
	h.publish(time.Now(), false)
	if value, found := h.ChunkBody(d.FirstSequence, d.LastSequence, d.Digest); !found || value != body {
		t.Fatal("retirement erased retained history")
	}
	next := historySource(t, h, ShredHistoryLive)
	if !next.TryReceipt(historyReceipt(2, ShredHistoryTVU)) {
		t.Fatal("replacement receipt refused")
	}
	h.consume(<-h.queue) // Evicts the live ring, not the still-current publication.
	if value, found := h.ChunkBody(d.FirstSequence, d.LastSequence, d.Digest); !found || value != body {
		t.Fatal("unpublished rotation erased the current body")
	}
	h.publish(time.Now(), false)
	if _, found := h.ChunkBody(d.FirstSequence, d.LastSequence, d.Digest); found {
		t.Fatal("evicted body remains lookup-visible after replacement")
	}
	if sha256.Sum256([]byte(body)) != d.Digest {
		t.Fatal("returned old string was mutated by eviction")
	}
	last := historyCheckMetadata(t, h).Chunks[0]
	next.Retire()
	go h.run()
	if !h.CloseAfterProducers() {
		t.Fatal("joined drain failed")
	}
	if m := historyCheckMetadata(t, h); !m.Closed {
		t.Fatal("closed metadata missing")
	}
	if _, found := h.ChunkBody(last.FirstSequence, last.LastSequence, last.Digest); !found || next.TryReceipt(historyReceipt(3, ShredHistoryTVU)) {
		t.Fatal("close erased retained history or accepted a new receipt")
	}
}

func TestShredHistoryMetadataTypedMaxima(t *testing.T) {
	limits := shredHistoryLimits{shredHistoryQueueCapacity, shredHistoryChunkRows, shredHistoryChunks, shredHistoryRanges, shredHistorySources, shredHistoryChunkBytes, shredHistoryRetainedBytes}
	h := historyOwner(t, limits)
	for range shredHistorySources {
		historySource(t, h, ShredHistoryLive).Retire()
	}
	for origin := ShredHistoryTVU; origin <= ShredHistoryRepairUnmatched; origin++ {
		for i := range shredHistoryRanges {
			r := historyReceipt(uint64(2*i), origin)
			r.SourceGeneration = shredHistorySources
			h.consume(shredHistoryInput{receipt: r})
		}
	}
	for h.sequence < shredHistoryChunks*shredHistoryChunkRows {
		r := historyReceipt(0, ShredHistoryTVU)
		r.SourceGeneration = shredHistorySources
		h.consume(shredHistoryInput{receipt: r})
	}
	h.publish(time.Now(), false)
	m := historyCheckMetadata(t, h)
	if len(m.Chunks) != shredHistoryChunks || len(m.Sources) != shredHistorySources || m.StartupLimited {
		t.Fatal("maximum metadata lost descriptors or source lifetimes")
	}
	for _, ranges := range m.StartupRanges {
		if len(ranges) != shredHistoryRanges {
			t.Fatal("maximum metadata lost exact startup ranges")
		}
	}
	for _, d := range m.Chunks {
		body, ok := h.ChunkBody(d.FirstSequence, d.LastSequence, d.Digest)
		if !ok || d.Count != shredHistoryChunkRows || uint64(len(body)) != d.Bytes || sha256.Sum256([]byte(body)) != d.Digest {
			t.Fatal("maximum descriptor/body identity differs")
		}
	}
	t.Logf("metadata_header=%d descriptor=%d descriptors=%d sources=%d ranges=%d", unsafe.Sizeof(m), unsafe.Sizeof(ShredHistoryChunkDescriptor{}), len(m.Chunks), len(m.Sources), 3*shredHistoryRanges)
}

func TestShredHistoryMetadataConcurrentPublicationLookupAndClose(t *testing.T) {
	h := historyOwner(t, historyLimits())
	source := historySource(t, h, ShredHistoryLive)
	stop := make(chan struct{})
	failures := make(chan string, 1)
	var readers sync.WaitGroup
	for range 3 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				m, ok := h.Metadata()
				if !ok {
					select {
					case failures <- "metadata disappeared":
					default:
					}
					return
				}
				for _, d := range m.Chunks {
					body, found := h.ChunkBody(d.FirstSequence, d.LastSequence, d.Digest)
					// A newer publication may evict a descriptor between the calls.
					if found && (uint64(len(body)) != d.Bytes || sha256.Sum256([]byte(body)) != d.Digest) {
						select {
						case failures <- "lookup returned a different body":
						default:
						}
						return
					}
				}
			}
		})
	}
	for slot := range 128 {
		if !source.TryReceipt(historyReceipt(uint64(slot), ShredHistoryTVU)) {
			close(stop)
			readers.Wait()
			t.Fatal("single producer input refused")
		}
		h.consume(<-h.queue)
		h.seal()
		h.publish(time.Now(), false)
	}
	source.Retire()
	go h.run()
	var closers sync.WaitGroup
	for range 2 {
		closers.Go(func() { h.CloseAfterProducers() })
	}
	closers.Wait()
	close(stop)
	readers.Wait()
	select {
	case failure := <-failures:
		t.Fatal(failure)
	default:
	}
	if m := historyCheckMetadata(t, h); !m.Closed || !m.Sources[0].Retired {
		t.Fatal("joined final publication lost closed/retired state")
	}
}
