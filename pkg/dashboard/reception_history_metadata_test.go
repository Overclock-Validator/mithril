package dashboard

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func maximumReceptionMetadata() ReceptionMetadata {
	m := receptionTestMetadata()
	for i := range maxReceptionSources {
		m.Sources = append(m.Sources, ReceptionSource{Generation: math.MaxUint64 - uint64(maxReceptionSources) + uint64(i),
			StartedAt: m.PublishedAt.Add(-time.Duration(i) * time.Second), Kind: "prewarm", Retired: false})
	}
	for origin := range m.Ranges {
		for i := range maxReceptionRanges {
			slot := uint64(math.MaxUint64-2*maxReceptionRanges+1) + uint64(2*i)
			m.Ranges[origin] = append(m.Ranges[origin], ReceptionRange{slot, slot})
		}
	}
	c := ReceptionCounter{Value: math.MaxUint64, Saturated: true}
	m.StartupAtCapture = ReceptionStartup{State: "closed", PrefixCutoff: math.MaxUint64, Limited: true,
		Marker: &ReceptionMarker{At: m.PublishedAt, SourceGeneration: math.MaxUint64 - 1, Slot: math.MaxUint64,
			Bankhash: m.Binding.GenesisHash, ReplayAttempt: math.MaxUint64, Rewinds: math.MaxUint64, CompletedIndex: math.MaxUint64},
		Losses: ReceptionLosses{QueueRefusals: c, InvalidInputs: c, RangeRefusals: [3]ReceptionCounter{c, c, c},
			DescriptorEvictions: c, EvictedChunks: c, EvictedRows: c, ChunkRefusals: c}}
	return m
}

func receptionTestPageBodies(t *testing.T, p *PreparedReceptionMetadata) [][]byte {
	t.Helper()
	var pages [][]byte
	for _, d := range p.Manifest().Pages {
		body, ok := p.Page(d.Kind, d.Page)
		if !ok {
			t.Fatal("advertised page unavailable")
		}
		pages = append(pages, []byte(body))
	}
	return pages
}

// This test-only bundle has individually canonical, correctly hashed pages but
// an invalid boundary between them. Never construct production prepared values
// this way: only the complete validator can establish their cross-page order.
func receptionCrossPageTestBundle(t *testing.T, m ReceptionMetadata, kind string) *PreparedReceptionMetadata {
	t.Helper()
	m.Sources, m.Ranges = nil, [3][]ReceptionRange{}
	if strings.HasPrefix(kind, "source_") {
		for i := range 65 {
			m.Sources = append(m.Sources, ReceptionSource{Generation: uint64(i + 1), StartedAt: m.PublishedAt, Kind: "live"})
		}
	} else {
		for i := range 129 {
			m.Ranges[0] = append(m.Ranges[0], ReceptionRange{uint64(i * 2), uint64(i * 2)})
		}
	}
	p, err := PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	var last ReceptionMetadataPage
	if len(p.pages) != 2 || json.Unmarshal([]byte(p.pages[1]), &last) != nil {
		t.Fatal("two-page fixture invalid")
	}
	switch kind {
	case "source_equal":
		last.Sources[0].Generation = 64
	case "source_reversed":
		last.Sources[0].Generation = 63
	case "range_overlap":
		last.Ranges[0] = ReceptionRange{254, 254}
	case "range_adjacent":
		last.Ranges[0] = ReceptionRange{255, 255}
	default:
		t.Fatal("unknown cross-page fixture")
	}
	page, err := json.Marshal(last)
	if err != nil {
		t.Fatal(err)
	}
	p.pages[1] = string(page)
	p.manifest.Pages[1].Digest, p.manifest.Pages[1].Bytes = receptionDigest(page), uint64(len(page))
	p.manifest.Digest, err = receptionMetadataDigest(p.manifest)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(p.manifest)
	if err != nil {
		t.Fatal(err)
	}
	p.body, p.bytes = string(body), len(body)+receptionPageBytes(p.manifest.Pages)
	if _, err := decodeReceptionMetadataManifest(body); err != nil {
		t.Fatal("manifest must remain valid", err)
	}
	for i, d := range p.manifest.Pages {
		if _, err := DecodeReceptionMetadataPage(p.manifest, d.Kind, d.Page, []byte(p.pages[i])); err != nil {
			t.Fatal("individual page must remain valid", err)
		}
	}
	return p
}

func TestReceptionMetadataValidatedCompletion(t *testing.T) {
	p, err := PrepareReceptionMetadata(maximumReceptionMetadata())
	if err != nil {
		t.Fatal(err)
	}
	m, input := p.Manifest(), receptionTestPageBodies(t, p)
	var pages [maxReceptionPages]receptionValidatedMetadataPage
	for i := range input {
		pages[i], err = decodeReceptionMetadataPageForCache(m, i, input[i])
		if err != nil {
			t.Fatal(err)
		}
		input[i][0] = 'x' // The validated immutable body must own its bytes.
	}
	m.Pages[0].Digest, m.StartupAtCapture.Marker.Bankhash = "changed", "changed"
	got, err := finishReceptionMetadata(p.Body(), pages[:])
	if err != nil || got.Body() != p.Body() || got.EncodedBytes() != p.EncodedBytes() || !reflect.DeepEqual(got.Manifest(), p.Manifest()) {
		t.Fatal("completed maximum bundle changed", err)
	}
	for i, d := range p.manifest.Pages {
		body, ok := got.Page(d.Kind, d.Page)
		if !ok || body != p.pages[i] {
			t.Fatal("completion changed original canonical page", i)
		}
	}
	copy := got.Manifest()
	copy.Pages[0].Digest, copy.StartupAtCapture.Marker.Bankhash = "changed", "changed"
	if !reflect.DeepEqual(got.Manifest(), p.Manifest()) {
		t.Fatal("completed manifest aliases returned values")
	}
	for name, change := range map[string]func(*[maxReceptionPages]receptionValidatedMetadataPage){
		"unvalidated": func(pages *[maxReceptionPages]receptionValidatedMetadataPage) {
			pages[0] = receptionValidatedMetadataPage{}
		},
		"wrong_index": func(pages *[maxReceptionPages]receptionValidatedMetadataPage) { pages[0] = pages[1] },
		"wrong_manifest": func(pages *[maxReceptionPages]receptionValidatedMetadataPage) {
			pages[0].manifestDigest = strings.Repeat("0", 64)
		},
		"changed_body": func(pages *[maxReceptionPages]receptionValidatedMetadataPage) {
			pages[0].body = " " + pages[0].body[1:]
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := pages
			change(&bad)
			if _, err := finishReceptionMetadata(p.Body(), bad[:]); err == nil {
				t.Fatal("invalid validated-page binding accepted")
			}
		})
	}
}

func TestReceptionWireTypedMaxima(t *testing.T) {
	m := maximumReceptionMetadata()
	p, err := PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.manifest.Pages) != 52 {
		t.Fatalf("pages = %d, want 52", len(p.manifest.Pages))
	}
	counts := [4]int{}
	pages := receptionTestPageBodies(t, p)
	for i, d := range p.manifest.Pages {
		decoded, err := DecodeReceptionMetadataPage(p.Manifest(), d.Kind, d.Page, pages[i])
		if err != nil || decoded.Revision != math.MaxUint64 || len(pages[i]) > MaxReceptionMetadataPageBytes ||
			d.Digest != receptionDigest(pages[i]) || int(d.Bytes) != len(pages[i]) {
			t.Fatalf("maximum page %d refused or changed: %v", i, err)
		}
		counts[receptionKindIndex(d.Kind)] += d.Count
	}
	if counts != [4]int{256, 2048, 2048, 2048} {
		t.Fatalf("retained metadata rows = %v", counts)
	}
	restored, err := decodeReceptionMetadata([]byte(p.Body()), pages)
	if err != nil || restored.Body() != p.Body() || restored.EncodedBytes() != p.EncodedBytes() {
		t.Fatal("full bundle roundtrip differs")
	}
	for i, d := range p.manifest.Pages {
		body, ok := restored.Page(d.Kind, d.Page)
		if !ok || body != string(pages[i]) {
			t.Fatal("full decoder changed original maximum page", i)
		}
	}
	r := receptionTestRoot(t, m)
	r.ConsumedThrough = math.MaxUint64
	first, last := uint64(math.MaxUint64-maxReceptionChunks*128+1), uint64(math.MaxUint64)
	r.SourceBounds, r.CacheBounds = ReceptionSequenceBounds{&first, &last}, ReceptionSequenceBounds{&first, &last}
	body, err := PrepareReceptionHistoryRoot(r)
	if err != nil || len(body) > MaxReceptionRootBytes {
		t.Fatalf("maximum root rejected: %v", err)
	}
	if _, err := decodeReceptionHistoryRoot([]byte(body)); err != nil {
		t.Fatal(err)
	}
	total := len(body) + receptionPageBytes(p.manifest.Pages)
	if total > MaxReceptionMetadataBytes {
		t.Fatalf("full metadata %d exceeds %d", total, MaxReceptionMetadataBytes)
	}
	chunks := receptionTestChunks()
	if len(chunks) != 256 || ValidateReceptionChunkDescriptors(chunks) != nil {
		t.Fatal("maximum chunk index refused")
	}
	var indexed int
	for first := 0; first < len(chunks); first += 8 {
		index := ReceptionHistoryIndex{RecordingID: r.RecordingID, CacheBounds: r.CacheBounds, Chunks: chunks[first : first+8]}
		if first > 0 {
			index.After = chunks[first-1].LastSequence
		}
		body, err := PrepareReceptionHistoryIndex(index)
		if err != nil || len(body) > MaxReceptionRootBytes {
			t.Fatal("maximum index page refused")
		}
		got, err := decodeReceptionHistoryIndex([]byte(body))
		if err != nil || !reflect.DeepEqual(got.Chunks, index.Chunks) {
			t.Fatal("index rows lost")
		}
		indexed += len(got.Chunks)
	}
	if indexed != 256 {
		t.Fatal("not all chunk descriptors retained")
	}
	// This counts this codec's owned metadata only. The future A2 source/node
	// graph still owes two native generations + encoded-only cache + replies;
	// A1's separate three-full-generation model is not a substitute. Likewise,
	// reader bodies/parsed rows/GC overlap must still fit the reader's 64 MiB.
	one := uintptr(p.EncodedBytes()) + unsafe.Sizeof(*p) + uintptr(cap(p.pages))*unsafe.Sizeof("") +
		uintptr(cap(p.manifest.Pages))*unsafe.Sizeof(ReceptionMetadataPageDescriptor{}) + unsafe.Sizeof(ReceptionMarker{})
	// Charge detached digest strings as well as their string headers; the
	// canonical manifest contains separate encoded copies of these bytes.
	one += uintptr(len(p.manifest.Binding.RunID) + len(p.manifest.Binding.GenesisHash) + len(p.manifest.Digest) + len(p.manifest.StartupAtCapture.Marker.Bankhash))
	for _, d := range p.manifest.Pages {
		one += uintptr(len(d.Kind) + len(d.Digest))
	}
	construction := uintptr(cap(m.Sources))*unsafe.Sizeof(ReceptionSource{}) + unsafe.Sizeof(ReceptionMarker{})
	construction += uintptr(len(m.Binding.RunID) + len(m.Binding.GenesisHash) + len(m.StartupAtCapture.Marker.Bankhash))
	for _, source := range m.Sources {
		construction += uintptr(len(source.Kind))
	}
	for _, rows := range m.Ranges {
		construction += uintptr(cap(rows)) * unsafe.Sizeof(ReceptionRange{})
	}
	threeBundles := 3*one + construction + maxReceptionChunks*(unsafe.Sizeof(ReceptionChunkDescriptor{})+64) + MaxReceptionMetadataPageBytes + MaxReceptionRootBytes
	if threeBundles > 3<<20 {
		t.Fatalf("metadata-only owned-layout subtotal %d exceeds candidate 3 MiB", threeBundles)
	}
	t.Logf("root=%d pages=%d metadata_total=%d one_bundle_encoded=%d metadata_only_layout_subtotal=%d; aggregate 32/64 MiB and runtime heap/RSS NOT proven", len(body), len(pages), total, p.EncodedBytes(), threeBundles)
}

func TestReceptionMetadataCanonicalAndTamper(t *testing.T) {
	m := receptionTestMetadata()
	m.Sources = []ReceptionSource{{Generation: 1, StartedAt: m.PublishedAt, Kind: "live"}}
	m.Ranges[0] = []ReceptionRange{{0, 0}, {2, math.MaxUint64}}
	p, err := PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	pages := receptionTestPageBodies(t, p)
	if _, err := decodeReceptionMetadata([]byte(p.Body()), pages); err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string][]byte{
		"manifest_duplicate":     []byte(strings.Replace(p.Body(), `"revision":"18446744073709551615"`, `"revision":"18446744073709551615","revision":"18446744073709551615"`, 1)),
		"manifest_unknown":       []byte(strings.Replace(p.Body(), `"pages":`, `"unknown":false,"pages":`, 1)),
		"manifest_missing_false": []byte(strings.Replace(p.Body(), `"limited":false,`, "", 1)),
		"manifest_null_counter":  []byte(strings.Replace(p.Body(), `"value":"0"`, `"value":null`, 1)),
		"manifest_cap":           bytes.Repeat([]byte{' '}, MaxReceptionRootBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeReceptionMetadataManifest(bad); err == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
	for name, bad := range map[string][]byte{
		"page_duplicate":    []byte(strings.Replace(string(pages[0]), `"page":0`, `"page":0,"page":0`, 1)),
		"page_missing_zero": []byte(strings.Replace(string(pages[0]), `"page":0,`, "", 1)),
		"page_extra":        []byte(strings.Replace(string(pages[0]), `"kind":"sources"`, `"kind":"sources","extra":0`, 1)),
		"row_leading_zero":  []byte(strings.Replace(string(pages[0]), `["1",`, `["01",`, 1)),
		"row_null_false":    []byte(strings.Replace(string(pages[0]), `,false]`, `,null]`, 1)),
		"row_missing_false": []byte(strings.Replace(string(pages[0]), `,false]`, `]`, 1)),
		"row_extra":         []byte(strings.Replace(string(pages[0]), `,false]`, `,false,0]`, 1)),
		"page_cap":          bytes.Repeat([]byte{' '}, MaxReceptionMetadataPageBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			var page ReceptionMetadataPage
			if json.Unmarshal(bad, &page) == nil {
				t.Fatal("invalid page accepted")
			}
		})
	}
	manifest := p.Manifest()
	if _, err := DecodeReceptionMetadataPage(manifest, "sources", 1, pages[0]); err == nil {
		t.Fatal("wrong requested page accepted")
	}
	bad := append([]byte(nil), pages[0]...)
	bad[len(bad)-1] = ' '
	if _, err := DecodeReceptionMetadataPage(manifest, "sources", 0, bad); err == nil {
		t.Fatal("digest mismatch accepted")
	}
	manifest.Revision--
	manifest.Digest, err = receptionMetadataDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeReceptionMetadataPage(manifest, "sources", 0, pages[0]); err == nil {
		t.Fatal("wrong capture revision accepted")
	}
	if _, err := decodeReceptionMetadata([]byte(p.Body()), pages[:1]); err == nil {
		t.Fatal("missing page accepted")
	}
	if _, err := decodeReceptionMetadata([]byte(p.Body()), append(pages, pages[0])); err == nil {
		t.Fatal("extra page accepted")
	}
}

func TestReceptionMetadataCrossPageAndLimits(t *testing.T) {
	for name, change := range map[string]func(*ReceptionMetadata){
		"257_sources":         func(m *ReceptionMetadata) { m.Sources = append(m.Sources, m.Sources[0]) },
		"2049_ranges":         func(m *ReceptionMetadata) { m.Ranges[0] = append(m.Ranges[0], m.Ranges[0][0]) },
		"duplicate_source":    func(m *ReceptionMetadata) { m.Sources[64].Generation = m.Sources[63].Generation },
		"adjacent_cross_page": func(m *ReceptionMetadata) { m.Ranges[0][128].First = m.Ranges[0][127].Last + 1 },
		"overlap_cross_page":  func(m *ReceptionMetadata) { m.Ranges[0][128].First = m.Ranges[0][127].Last },
		"bad_range":           func(m *ReceptionMetadata) { m.Ranges[0][0].First++ },
		"kind":                func(m *ReceptionMetadata) { m.Sources[0].Kind = "repair" },
		"zero_revision":       func(m *ReceptionMetadata) { m.Revision = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			m := maximumReceptionMetadata()
			change(&m)
			if _, err := PrepareReceptionMetadata(m); err == nil {
				t.Fatal("invalid metadata accepted")
			}
		})
	}
	p, err := PrepareReceptionMetadata(maximumReceptionMetadata())
	if err != nil {
		t.Fatal(err)
	}
	pages := receptionTestPageBodies(t, p)
	m := p.Manifest()
	var second ReceptionMetadataPage
	if err := json.Unmarshal(pages[1], &second); err != nil {
		t.Fatal(err)
	}
	// Both individual pages are valid and every digest is recomputed. Only the
	// complete-bundle ordering check can catch this duplicated source boundary.
	second.Sources[0].Generation--
	pages[1], err = json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	m.Pages[1].Digest, m.Pages[1].Bytes = receptionDigest(pages[1]), uint64(len(pages[1]))
	m.Digest, err = receptionMetadataDigest(m)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeReceptionMetadataPage(m, "sources", 1, pages[1]); err != nil {
		t.Fatal("individual valid page should decode")
	}
	if _, err := decodeReceptionMetadata(body, pages); err == nil {
		t.Fatal("cross-page duplicate accepted")
	}
	// Declared payload sizes are bounded before summing, even when each field
	// is a syntactically valid full-width uint64.
	for _, value := range []uint64{MaxReceptionMetadataPageBytes + 1, math.MaxUint64} {
		bad := p.Manifest()
		bad.Pages[0].Bytes = value
		bad.Digest, err = receptionMetadataDigest(bad)
		if err != nil {
			t.Fatal(err)
		}
		if validateReceptionMetadataManifest(bad) == nil {
			t.Fatal("oversized declared page accepted")
		}
	}
	bad := p.Manifest()
	for i := range bad.Pages {
		bad.Pages[i].Bytes = MaxReceptionMetadataPageBytes
	}
	bad.Digest, err = receptionMetadataDigest(bad)
	if err != nil || validateReceptionMetadataManifest(bad) == nil {
		t.Fatal("52 maximum-size pages accepted above total cap")
	}
	// Keep each declared page in bounds and place the complete manifest/pages
	// exactly one byte over the aggregate limit, including the digest field.
	for range 8 {
		encoded, err := json.Marshal(bad)
		if err != nil {
			t.Fatal(err)
		}
		remaining := MaxReceptionMetadataBytes + 1 - len(encoded) - len(bad.Pages)
		for i := range bad.Pages {
			extra := min(remaining, MaxReceptionMetadataPageBytes-1)
			bad.Pages[i].Bytes = uint64(1 + extra)
			remaining -= extra
		}
		bad.Digest, err = receptionMetadataDigest(bad)
		if err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := json.Marshal(bad)
	if err != nil || len(encoded)+receptionPageBytes(bad.Pages) != MaxReceptionMetadataBytes+1 {
		t.Fatal("cap+1 fixture is not exact")
	}
	if validateReceptionMetadataManifest(bad) == nil {
		t.Fatal("aggregate cap+1 accepted")
	}
}

func TestReceptionMetadataDetachmentAndBodyFree(t *testing.T) {
	m := maximumReceptionMetadata()
	p, err := PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	before, pages := p.Body(), receptionTestPageBodies(t, p)
	m.Sources[0].Kind, m.Ranges[0][0].First, m.StartupAtCapture.Marker.Slot = "changed", 0, 0
	copy := p.Manifest()
	copy.Pages[0].Digest, copy.StartupAtCapture.Marker.Bankhash = "changed", "changed"
	if p.Body() != before || p.Manifest().Pages[0].Digest == "changed" || p.Manifest().StartupAtCapture.Marker.Bankhash == "changed" {
		t.Fatal("caller mutation reached prepared metadata")
	}
	for i, d := range p.Manifest().Pages {
		body, ok := p.Page(d.Kind, d.Page)
		if !ok || body != string(pages[i]) {
			t.Fatal("caller mutation reached page")
		}
	}
	var nilPrepared *PreparedReceptionMetadata
	if nilPrepared.Body() != "" || nilPrepared.EncodedBytes() != 0 {
		t.Fatal("disabled metadata performed preparation")
	}
	if _, ok := nilPrepared.Page("sources", 0); ok {
		t.Fatal("disabled metadata returned a page")
	}
	if _, ok := p.Page("sources", 4); ok {
		t.Fatal("nonexistent page returned")
	}
	var inspect func(reflect.Type)
	inspect = func(typ reflect.Type) {
		if typ == reflect.TypeOf(time.Time{}) {
			return
		}
		if strings.Contains(typ.PkgPath(), "/turbine") || typ.Kind() == reflect.Interface || typ == reflect.TypeOf(json.RawMessage{}) {
			t.Fatalf("retained wire graph can reference native/source/opaque data: %v", typ)
		}
		switch typ.Kind() {
		case reflect.Pointer, reflect.Array, reflect.Slice:
			inspect(typ.Elem())
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				inspect(typ.Field(i).Type)
			}
		case reflect.String, reflect.Bool, reflect.Int, reflect.Uint64:
		default:
			t.Fatalf("unexpected retained type %v", typ)
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeOf(ReceptionMetadata{}), reflect.TypeOf(PreparedReceptionMetadata{}), reflect.TypeOf(receptionValidatedMetadataPage{}), reflect.TypeOf(ReceptionHistoryRoot{}), reflect.TypeOf(ReceptionHistoryIndex{})} {
		inspect(typ)
	}
}
