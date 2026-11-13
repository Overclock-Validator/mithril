package turbine

import (
	"encoding/hex"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/gagliardetto/solana-go"
)

// This is the source/node holder gate, not the later reader's 64 MiB gate or
// a measurement of allocator/GC/RSS peaks. No Snapshot is taken: the adapter
// holds A's encoded bodies only, while the source retains B's publication and
// C's live ring. The temporary metadata view has no chunk or row references.
func TestShredHistoryAdapterWholeHolderBudget(t *testing.T) {
	at := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	var root solana.Hash
	for i := range root {
		root[i] = 255
	}
	binding := ShredHistoryBinding{RunID: strings.Repeat("<", 128), NodeStartedAt: at, GenesisHash: root}
	limits := shredHistoryLimits{shredHistoryQueueCapacity, shredHistoryChunkRows, shredHistoryChunks, shredHistoryRanges, shredHistorySources, shredHistoryChunkBytes, shredHistoryRetainedBytes}
	h, err := newShredHistory(binding, limits)
	if err != nil {
		t.Fatal(err)
	}
	for i := range shredHistorySources {
		h.sources = append(h.sources, ShredHistorySource{Generation: math.MaxUint64 - uint64(shredHistorySources) + uint64(i), StartedAt: at, Kind: ShredHistoryPrewarm})
	}
	for origin := range h.ranges {
		for i := range shredHistoryRanges {
			slot := uint64(math.MaxUint64-2*shredHistoryRanges+1) + uint64(2*i)
			h.ranges[origin] = append(h.ranges[origin], ShredHistoryRange{First: slot, Last: slot})
		}
	}
	fill := func(generation uint64) {
		for chunk := range shredHistoryChunks {
			for row := range shredHistoryChunkRows {
				sequence := uint64(math.MaxUint64-3*shredHistoryChunks*shredHistoryChunkRows) +
					generation*uint64(shredHistoryChunks*shredHistoryChunkRows) + uint64(chunk*shredHistoryChunkRows+row)
				h.open = append(h.open, ShredHistoryRow{ShredHistoryReceipt: ShredHistoryReceipt{At: at, Root: root,
					SourceGeneration: math.MaxUint64 - 1, Slot: math.MaxUint64, Index: math.MaxUint32, FECSetIndex: math.MaxUint32,
					Version: math.MaxUint16, Variant: math.MaxUint8, Origin: ShredHistoryRepairUnmatched}, Sequence: sequence})
				h.sequence = sequence
			}
			h.seal()
		}
	}
	fill(0)
	h.publish(at, false)
	metadata, ok := h.Metadata()
	if !ok || len(metadata.Chunks) != 256 || len(metadata.Sources) != 256 {
		t.Fatal("source maximum metadata missing")
	}
	apiBinding := dashboard.ReceptionRecordingBinding{RunID: binding.RunID, NodeStartedAt: binding.NodeStartedAt,
		GenesisHash: binding.GenesisHash.String(), RecordingGeneration: math.MaxUint64}
	cache, err := dashboard.NewReceptionHistoryCache(true, apiBinding)
	if err != nil {
		t.Fatal(err)
	}
	id, err := dashboard.ReceptionRecordingID(apiBinding)
	if err != nil {
		t.Fatal(err)
	}
	var cacheBodies uintptr
	var batch [8]dashboard.PreparedReceptionChunk
	for i, d := range metadata.Chunks {
		body, ok := h.ChunkBody(d.FirstSequence, d.LastSequence, d.Digest)
		if !ok || len(body) != dashboard.MaxReceptionCanonicalChunkBytes {
			t.Fatal("full legal source body differs from codec ceiling")
		}
		descriptor := dashboard.ReceptionChunkDescriptor{FirstSequence: d.FirstSequence, LastSequence: d.LastSequence, Count: d.Count,
			MinAt: d.MinAt, MaxAt: d.MaxAt, Digest: hex.EncodeToString(d.Digest[:]), Bytes: d.Bytes}
		p, err := dashboard.PrepareReceptionHistoryChunk(descriptor, body)
		if err != nil || p.Body() != body {
			t.Fatalf("actual A1 bytes refused or changed: %v", err)
		}
		batch[i%len(batch)] = p
		cacheBodies += uintptr(len(body))
		if i%len(batch) == len(batch)-1 {
			if err := cache.InstallChunks(id, batch[:]); err != nil {
				t.Fatal(err)
			}
		}
	}
	fill(1)
	h.publish(at, false)
	published := h.publication.Load()
	fill(2)
	if metadata.Chunks[255].LastSequence >= published.Chunks[0].FirstSequence || published.Chunks[255].LastSequence >= h.chunks[0].FirstSequence {
		t.Fatal("holder generations are not disjoint")
	}
	var nativeGenerations uintptr
	for _, chunks := range [][]ShredHistoryChunk{published.Chunks, h.chunks} {
		if len(chunks) != 256 {
			t.Fatal("source retention shortened")
		}
		for _, chunk := range chunks {
			if len(chunk.rows) != 128 || len(chunk.body) != dashboard.MaxReceptionCanonicalChunkBytes {
				t.Fatal("source shape differs")
			}
			nativeGenerations += uintptr(len(chunk.body)) + uintptr(cap(chunk.rows))*unsafe.Sizeof(ShredHistoryRow{})
		}
	}
	var after uint64
	var cached int
	for {
		index, gap, ok := cache.Index(id, after)
		if !ok || cached > 0 && gap {
			t.Fatal("encoded-only cache lost continuous retained rows")
		}
		if len(index.Chunks) == 0 {
			break
		}
		for _, d := range index.Chunks {
			body, ok := cache.ChunkBody(id, d.Digest)
			if !ok || len(body) != dashboard.MaxReceptionCanonicalChunkBytes {
				t.Fatal("old source bytes lost on replacement")
			}
		}
		cached += len(index.Chunks)
		after = index.Chunks[len(index.Chunks)-1].LastSequence
	}
	if cached != 256 {
		t.Fatal("adapter shortened chunk retention")
	}

	// The input conversion is one detached native metadata copy plus one API
	// metadata input. Three encoded bundles account for previous/current/build.
	m := dashboard.ReceptionMetadata{Binding: apiBinding, Revision: math.MaxUint64 - 2, PublishedAt: at}
	m.Sources = make([]dashboard.ReceptionSource, len(metadata.Sources))
	for i, source := range metadata.Sources {
		m.Sources[i] = dashboard.ReceptionSource{Generation: source.Generation, StartedAt: source.StartedAt, Kind: "prewarm", Retired: source.Retired}
	}
	for origin, ranges := range metadata.StartupRanges {
		m.Ranges[origin] = make([]dashboard.ReceptionRange, len(ranges))
		for i, r := range ranges {
			m.Ranges[origin][i] = dashboard.ReceptionRange{First: r.First, Last: r.Last}
		}
	}
	counter := dashboard.ReceptionCounter{Value: math.MaxUint64, Saturated: true}
	m.StartupAtCapture = dashboard.ReceptionStartup{State: "closed", PrefixCutoff: published.LastSequence, Limited: true,
		Marker: &dashboard.ReceptionMarker{At: at, SourceGeneration: math.MaxUint64 - 1, Slot: math.MaxUint64, Bankhash: root.String(),
			ReplayAttempt: math.MaxUint64, Rewinds: math.MaxUint64, CompletedIndex: math.MaxUint64},
		Losses: dashboard.ReceptionLosses{QueueRefusals: counter, InvalidInputs: counter, RangeRefusals: [3]dashboard.ReceptionCounter{counter, counter, counter},
			DescriptorEvictions: counter, EvictedChunks: counter, EvictedRows: counter, ChunkRefusals: counter}}
	var bundles [3]*dashboard.PreparedReceptionMetadata
	var bundleBytes uintptr
	now := time.Now()
	for i := range bundles {
		bundles[i], err = dashboard.PrepareReceptionMetadata(m)
		if err != nil {
			t.Fatal(err)
		}
		p := bundles[i]
		if len(p.Manifest().Pages) != 52 {
			t.Fatal("metadata pages shortened")
		}
		bundleBytes += receptionAdapterMetadataLayout(t, p)
		if i < 2 {
			if err := cache.InstallMetadata(id, p, now.Add(time.Duration(i)*6*time.Second)); err != nil {
				t.Fatal(err)
			}
		}
		if i+1 < len(bundles) {
			m.Revision++
		}
	}
	manifest, ok := cache.MetadataManifest(id)
	if !ok {
		t.Fatal("cache metadata missing")
	}
	first, last := published.Chunks[0].FirstSequence, published.Chunks[255].LastSequence
	cacheFirst, cacheLast := metadata.Chunks[0].FirstSequence, metadata.Chunks[255].LastSequence
	rootBody, err := dashboard.PrepareReceptionHistoryRoot(dashboard.ReceptionHistoryRoot{SchemaVersion: 1, Family: "reception",
		Scope: "authenticated_socket_occurrences", Binding: apiBinding, RecordingID: id, State: "fresh", PublishedAt: at,
		ConsumedThrough: published.LastSequence, SourceBounds: dashboard.ReceptionSequenceBounds{First: &first, Last: &last},
		CacheBounds: dashboard.ReceptionSequenceBounds{First: &cacheFirst, Last: &cacheLast}, Startup: m.StartupAtCapture, Metadata: manifest})
	if err != nil || len(rootBody) > dashboard.MaxReceptionRootBytes {
		t.Fatalf("maximum root: %v", err)
	}

	// Fixed/source-owned headers and backing arrays. Four source/range copies
	// cover live, published, publication candidate and the detached Metadata().
	source := unsafe.Sizeof(ShredHistory{}) + shredHistoryQueueCapacity*unsafe.Sizeof(shredHistoryInput{}) + nativeGenerations +
		2*shredHistoryChunkRows*unsafe.Sizeof(ShredHistoryRow{}) + 4*shredHistoryChunks*unsafe.Sizeof(ShredHistoryChunk{}) +
		4*shredHistorySources*unsafe.Sizeof(ShredHistorySource{}) + 4*3*shredHistoryRanges*unsafe.Sizeof(ShredHistoryRange{}) +
		3*(unsafe.Sizeof(ShredHistoryMetadata{})+shredHistoryChunks*unsafe.Sizeof(ShredHistoryChunkDescriptor{})) +
		2*unsafe.Sizeof(ShredHistorySnapshot{}) + 4*unsafe.Sizeof(ShredHistoryMarker{}) + 4*128
	conversion := unsafe.Sizeof(m) + uintptr(cap(m.Sources))*unsafe.Sizeof(dashboard.ReceptionSource{}) +
		unsafe.Sizeof(dashboard.ReceptionMarker{}) + uintptr(len(m.Binding.RunID)+len(m.Binding.GenesisHash)+len(m.StartupAtCapture.Marker.Bankhash))
	for _, s := range m.Sources {
		conversion += uintptr(len(s.Kind))
	}
	for _, ranges := range m.Ranges {
		conversion += uintptr(cap(ranges)) * unsafe.Sizeof(dashboard.ReceptionRange{})
	}
	// One detached descriptor conversion, three bundles, two manifest copies,
	// twelve root bodies (current/build and two variants per private reply),
	// and two page/manifest encoding buffers.
	metadataLayout := bundleBytes + 2*unsafe.Sizeof(dashboard.PreparedReceptionMetadata{}) + conversion + 256*(unsafe.Sizeof(dashboard.ReceptionChunkDescriptor{})+64) +
		2*(unsafe.Sizeof(dashboard.ReceptionMetadataManifest{})+52*(unsafe.Sizeof(dashboard.ReceptionMetadataPageDescriptor{})+64+16)) +
		2*unsafe.Sizeof(dashboard.ReceptionMarker{}) + 12*(dashboard.MaxReceptionRootBytes+unsafe.Sizeof("")) + 2*dashboard.MaxReceptionMetadataPageBytes
	if metadataLayout > 3<<20 {
		t.Fatalf("metadata layout %d exceeds 3 MiB", metadataLayout)
	}
	cacheLayout := unsafe.Sizeof(*cache) + cacheBodies + 256*64 + 128 + 44 + 64
	feedType := reflect.TypeOf(dashboard.Feed{})
	transportField, ok := feedType.FieldByName("reception")
	if !ok || transportField.Type.Kind() != reflect.Pointer {
		t.Fatal("reception transport owner changed")
	}
	// The dashboard test checks the four reply/selection headers fit this fixed
	// allowance. Their index backing arrays and returned bodies are charged below.
	// One additional KiB reserves the node adapter/sampler and startup lifetime
	// headers; its node-package test checks that allowance independently.
	cacheLayout += feedType.Size() + transportField.Type.Elem().Size() + 4*1024 + 1024
	incoming := 8 * (unsafe.Sizeof(dashboard.PreparedReceptionChunk{}) + dashboard.MaxReceptionCanonicalChunkBytes + 64)
	writers := 4*(dashboard.MaxReceptionCanonicalChunkBytes+unsafe.Sizeof("")) +
		4*(unsafe.Sizeof(dashboard.ReceptionHistoryIndex{})+8*(unsafe.Sizeof(dashboard.ReceptionChunkDescriptor{})+64)+2*unsafe.Sizeof(uint64(0)))
	// Charge source wire rows; six fixed 128-row codec values across decode,
	// shared-helper returns, canonical interfaces and value-receiver copies (the separate codec test
	// caps each row at 96 bytes); three source and three validation conversion
	// buffers at the unchanged outer cap. A separate 1 MiB reserve covers
	// bounded encoding/json scanner/tuple scratch and channel/runtime headers.
	// Actual allocator/GC overlap and steady/peak RSS still require native cost
	// measurements; this fixed owned-layout test does not claim that evidence.
	scratch := shredHistoryChunkRows*unsafe.Sizeof(shredHistoryWireRow{}) + 6*128*96 + 6*shredHistoryChunkBytes + 1<<20
	budget := source + metadataLayout + cacheLayout + incoming + writers + scratch
	if budget > 32<<20 {
		t.Fatalf("complete source/node owned layout %d exceeds 32 MiB", budget)
	}
	t.Logf("source=%d metadata=%d cache=%d incoming=%d writers=%d scratch=%d total=%d limit=%d; reader and native GC/RSS gates remain open",
		source, metadataLayout, cacheLayout, incoming, writers, scratch, budget, 32<<20)
}

func receptionAdapterMetadataLayout(t *testing.T, p *dashboard.PreparedReceptionMetadata) uintptr {
	t.Helper()
	// Reflection reads only concrete slice capacities, including unexported
	// backing-array slack; it neither mutates fields nor fabricates a wire value.
	v := reflect.ValueOf(p).Elem()
	pages := v.FieldByName("pages")
	descriptors := v.FieldByName("manifest").FieldByName("Pages")
	if pages.Len() != 52 || descriptors.Len() != 52 {
		t.Fatal("maximum metadata shape differs")
	}
	m := p.Manifest()
	owned := unsafe.Sizeof(*p) + uintptr(p.EncodedBytes()) + uintptr(pages.Cap())*unsafe.Sizeof("") +
		uintptr(descriptors.Cap())*unsafe.Sizeof(dashboard.ReceptionMetadataPageDescriptor{}) + unsafe.Sizeof(dashboard.ReceptionMarker{}) +
		uintptr(len(m.Binding.RunID)+len(m.Binding.GenesisHash)+len(m.Digest)+len(m.StartupAtCapture.Marker.Bankhash))
	for _, d := range m.Pages {
		owned += uintptr(len(d.Kind) + len(d.Digest))
	}
	return owned
}
