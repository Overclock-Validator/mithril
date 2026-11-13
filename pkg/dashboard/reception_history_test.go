package dashboard

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

func receptionTestMetadata() ReceptionMetadata {
	at := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	var hash solana.Hash
	for i := range hash {
		hash[i] = 255
	}
	return ReceptionMetadata{
		Binding:  ReceptionRecordingBinding{RunID: strings.Repeat("<", 128), NodeStartedAt: at, GenesisHash: hash.String(), RecordingGeneration: math.MaxUint64},
		Revision: math.MaxUint64, PublishedAt: at,
		StartupAtCapture: ReceptionStartup{State: "open"},
	}
}

func receptionTestRoot(t *testing.T, m ReceptionMetadata) ReceptionHistoryRoot {
	t.Helper()
	p, err := PrepareReceptionMetadata(m)
	if err != nil {
		t.Fatal(err)
	}
	id, err := ReceptionRecordingID(m.Binding)
	if err != nil {
		t.Fatal(err)
	}
	return ReceptionHistoryRoot{SchemaVersion: 1, Family: "reception", Scope: "authenticated_socket_occurrences",
		Binding: m.Binding, RecordingID: id, State: "fresh", PublishedAt: m.PublishedAt,
		Startup: cloneReceptionStartup(m.StartupAtCapture), Metadata: p.Manifest()}
}

func receptionTestChunks() []ReceptionChunkDescriptor {
	at := receptionTestMetadata().PublishedAt
	chunks := make([]ReceptionChunkDescriptor, maxReceptionChunks)
	for i := range chunks {
		first := uint64(math.MaxUint64-maxReceptionChunks*128+1) + uint64(i)*128
		chunks[i] = ReceptionChunkDescriptor{FirstSequence: first, LastSequence: first + 127, Count: 128,
			MinAt: at, MaxAt: at, Digest: strings.Repeat("f", 64), Bytes: MaxReceptionChunkBytes}
	}
	return chunks
}

func TestReceptionRecordingBinding(t *testing.T) {
	b := receptionTestMetadata().Binding
	id, err := ReceptionRecordingID(b)
	if err != nil || !validHeldScheduleDigest(id) {
		t.Fatalf("binding rejected: %v", err)
	}
	body, err := json.Marshal(b)
	if err != nil || id != receptionDigest(body) {
		t.Fatal("ID does not bind canonical bytes")
	}
	for name, change := range map[string]func(*ReceptionRecordingBinding){
		"run":        func(v *ReceptionRecordingBinding) { v.RunID = "other" },
		"date":       func(v *ReceptionRecordingBinding) { v.NodeStartedAt = v.NodeStartedAt.Add(-time.Second) },
		"genesis":    func(v *ReceptionRecordingBinding) { v.GenesisHash = (solana.Hash{1}).String() },
		"generation": func(v *ReceptionRecordingBinding) { v.RecordingGeneration-- },
	} {
		t.Run(name, func(t *testing.T) {
			copy := b
			change(&copy)
			got, err := ReceptionRecordingID(copy)
			if err != nil || got == id {
				t.Fatal("changed binding reused ID")
			}
		})
	}
	for name, change := range map[string]func(*ReceptionRecordingBinding){
		"empty":                func(v *ReceptionRecordingBinding) { v.RunID = "" },
		"long":                 func(v *ReceptionRecordingBinding) { v.RunID += "x" },
		"utf8":                 func(v *ReceptionRecordingBinding) { v.RunID = "\xff" },
		"zero_date":            func(v *ReceptionRecordingBinding) { v.NodeStartedAt = time.Time{} },
		"offset":               func(v *ReceptionRecordingBinding) { v.NodeStartedAt = v.NodeStartedAt.In(time.FixedZone("offset", 1)) },
		"year":                 func(v *ReceptionRecordingBinding) { v.NodeStartedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) },
		"zero_genesis":         func(v *ReceptionRecordingBinding) { v.GenesisHash = (solana.Hash{}).String() },
		"noncanonical_genesis": func(v *ReceptionRecordingBinding) { v.GenesisHash = "1" + v.GenesisHash },
		"zero_generation":      func(v *ReceptionRecordingBinding) { v.RecordingGeneration = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			copy := b
			change(&copy)
			if _, err := ReceptionRecordingID(copy); err == nil {
				t.Fatal("invalid binding accepted")
			}
		})
	}
	b.NodeStartedAt = time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC)
	if _, err := ReceptionRecordingID(b); err != nil {
		t.Fatal("representable year-one date refused")
	}
}

func TestReceptionRootAndIndexCanonical(t *testing.T) {
	r := receptionTestRoot(t, receptionTestMetadata())
	body, err := PrepareReceptionHistoryRoot(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeReceptionHistoryRoot([]byte(body)); err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]string{
		"missing_known_zero": strings.Replace(body, `"consumed_through":"0",`, "", 1),
		"null_known_zero":    strings.Replace(body, `"consumed_through":"0"`, `"consumed_through":null`, 1),
		"number":             strings.Replace(body, `"consumed_through":"0"`, `"consumed_through":0`, 1),
		"leading_zero":       strings.Replace(body, `"consumed_through":"0"`, `"consumed_through":"00"`, 1),
		"duplicate":          strings.Replace(body, `"state":"fresh"`, `"state":"fresh","state":"fresh"`, 1),
		"extra":              strings.Replace(body, `"schema_version":1`, `"schema_version":1,"other":0`, 1),
		"date_spelling":      strings.Replace(body, "9999-12-31T23:59:59.999999999Z", "9999-12-31T23:59:59.999999999+00:00", 1),
		"trailing":           body + "\n",
		"cap_plus_one":       strings.Repeat(" ", MaxReceptionRootBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeReceptionHistoryRoot([]byte(bad)); err == nil {
				t.Fatal("noncanonical root accepted")
			}
		})
	}
	// No liveness decision is made from the ordering of two wall dates.
	r.PublishedAt = r.PublishedAt.Add(-time.Hour)
	if _, err := PrepareReceptionHistoryRoot(r); err != nil {
		t.Fatal("wall-clock reversal rejected")
	}
	r.Binding.NodeStartedAt = r.Binding.NodeStartedAt.In(time.FixedZone("same UTC", 0))
	if _, err := PrepareReceptionHistoryRoot(r); err != nil {
		t.Fatal("equivalent canonical binding refused")
	}
	chunks := receptionTestChunks()
	index := ReceptionHistoryIndex{RecordingID: r.RecordingID, After: chunks[0].FirstSequence,
		CacheBounds: ReceptionSequenceBounds{First: &chunks[0].FirstSequence, Last: &chunks[255].LastSequence}, Chunks: chunks[:8]}
	encoded, err := PrepareReceptionHistoryIndex(index)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeReceptionHistoryIndex([]byte(encoded))
	if err != nil || len(got.Chunks) != 8 || got.Chunks[0] != chunks[0] {
		t.Fatal("index tuple changed")
	}
	index.After, index.Chunks = math.MaxUint64, []ReceptionChunkDescriptor{}
	if _, err := PrepareReceptionHistoryIndex(index); err != nil {
		t.Fatal("future cursor empty response rejected")
	}
	index.Chunks = nil
	if _, err := PrepareReceptionHistoryIndex(index); err == nil {
		t.Fatal("null chunks accepted as empty")
	}
	index.After, index.Chunks = 0, chunks[:9]
	if _, err := PrepareReceptionHistoryIndex(index); err == nil {
		t.Fatal("ninth descriptor accepted")
	}
}

func TestReceptionBoundsMarkerAndLossValidation(t *testing.T) {
	for name, change := range map[string]func(*ReceptionHistoryRoot){
		"partial_bounds":        func(v *ReceptionHistoryRoot) { x := uint64(1); v.SourceBounds.First = &x },
		"zero_bounds":           func(v *ReceptionHistoryRoot) { x := uint64(0); v.CacheBounds = ReceptionSequenceBounds{&x, &x} },
		"beyond_consumed":       func(v *ReceptionHistoryRoot) { x := uint64(1); v.SourceBounds = ReceptionSequenceBounds{&x, &x} },
		"recording":             func(v *ReceptionHistoryRoot) { v.Binding.RecordingGeneration-- },
		"unknown_unlimited":     func(v *ReceptionHistoryRoot) { v.Startup.State = "unknown" },
		"closed_without_marker": func(v *ReceptionHistoryRoot) { v.Startup.State = "closed" },
		"open_cutoff":           func(v *ReceptionHistoryRoot) { v.Startup.PrefixCutoff = 1; v.ConsumedThrough = 1 },
		"loss_unlimited":        func(v *ReceptionHistoryRoot) { v.Startup.Losses.QueueRefusals.Value = 1 },
		"false_saturation":      func(v *ReceptionHistoryRoot) { v.Startup.Losses.QueueRefusals.Saturated = true },
		"missing_saturation": func(v *ReceptionHistoryRoot) {
			v.Startup.Losses.QueueRefusals.Value = math.MaxUint64
			v.Startup.Limited = true
		},
		"fresh_closed":          func(v *ReceptionHistoryRoot) { v.Closed = true },
		"interrupted_unlimited": func(v *ReceptionHistoryRoot) { v.State = "interrupted"; v.Interrupted = true },
	} {
		t.Run(name, func(t *testing.T) {
			r := receptionTestRoot(t, receptionTestMetadata())
			change(&r)
			if _, err := PrepareReceptionHistoryRoot(r); err == nil {
				t.Fatal("invalid root accepted")
			}
		})
	}
	r := receptionTestRoot(t, receptionTestMetadata())
	r.Startup.State, r.Startup.Marker = "closed", &ReceptionMarker{At: r.PublishedAt, SourceGeneration: 99, Bankhash: r.Binding.GenesisHash, ReplayAttempt: 1, CompletedIndex: 1}
	r.Closed, r.State = true, "closed"
	if _, err := PrepareReceptionHistoryRoot(r); err != nil {
		t.Fatal("closed empty prefix or evicted marker-source descriptor refused")
	}
	for _, terminal := range []string{"interrupted", "exhausted"} {
		r.State, r.Startup.Limited = terminal, true
		r.Interrupted, r.Exhausted = terminal == "interrupted", terminal == "exhausted"
		if _, err := PrepareReceptionHistoryRoot(r); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReceptionChunkDescriptorsAndOriginalBytes(t *testing.T) {
	chunks := receptionTestChunks()
	if err := ValidateReceptionChunkDescriptors(chunks); err != nil {
		t.Fatal(err)
	}
	if err := ValidateReceptionChunkDescriptors(append(chunks, chunks[0])); err == nil {
		t.Fatal("257 chunks accepted")
	}
	for name, change := range map[string]func(*ReceptionChunkDescriptor){
		"zero_first":       func(c *ReceptionChunkDescriptor) { c.FirstSequence = 0 },
		"noncontiguous":    func(c *ReceptionChunkDescriptor) { c.Count-- },
		"too_many_rows":    func(c *ReceptionChunkDescriptor) { c.Count = 129 },
		"date_bounds":      func(c *ReceptionChunkDescriptor) { c.MinAt = c.MaxAt.Add(time.Second) },
		"uppercase_digest": func(c *ReceptionChunkDescriptor) { c.Digest = strings.Repeat("F", 64) },
		"body_cap":         func(c *ReceptionChunkDescriptor) { c.Bytes = MaxReceptionChunkBytes + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			c := chunks[0]
			change(&c)
			if err := ValidateReceptionChunkDescriptors([]ReceptionChunkDescriptor{c}); err == nil {
				t.Fatal("invalid descriptor accepted")
			}
		})
	}
	m := receptionTestMetadata()
	hash, err := solana.HashFromBase58(m.Binding.GenesisHash)
	if err != nil {
		t.Fatal(err)
	}
	h, err := turbine.NewShredHistory(turbine.ShredHistoryBinding{RunID: m.Binding.RunID, NodeStartedAt: m.Binding.NodeStartedAt, GenesisHash: hash})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.CloseAfterProducers() })
	source, ok := h.RegisterSource(turbine.ShredHistoryLive, m.PublishedAt)
	if !ok {
		t.Fatal("source refused")
	}
	for i := range 128 {
		at := m.PublishedAt.Add(-time.Duration(i) * time.Second)
		if !source.TryReceipt(turbine.ShredHistoryReceipt{At: at, Slot: math.MaxUint64, Index: math.MaxUint32, FECSetIndex: math.MaxUint32,
			Version: math.MaxUint16, Variant: math.MaxUint8, Origin: turbine.ShredHistoryRepairUnmatched}) {
			t.Fatal("source receipt refused")
		}
	}
	source.Retire()
	if !h.CloseAfterProducers() {
		t.Fatal("source did not join")
	}
	meta, ok := h.Metadata()
	if !ok || meta.LastSequence != 128 {
		t.Fatal("source rows lost")
	}
	var observed uint64
	for _, d := range meta.Chunks {
		body, ok := h.ChunkBody(d.FirstSequence, d.LastSequence, d.Digest)
		if !ok {
			t.Fatal("body unavailable")
		}
		wire := ReceptionChunkDescriptor{d.FirstSequence, d.LastSequence, d.Count, d.MinAt, d.MaxAt, receptionDigest([]byte(body)), d.Bytes}
		if !validReceptionChunk(wire) {
			t.Fatal("actual source descriptor rejected")
		}
		encoded, err := json.Marshal(wire)
		var decoded ReceptionChunkDescriptor
		if err != nil || json.Unmarshal(encoded, &decoded) != nil || decoded != wire {
			t.Fatal("descriptor projection changed")
		}
		after, ok := h.ChunkBody(d.FirstSequence, d.LastSequence, d.Digest)
		if !ok || after != body || uint64(len(after)) != wire.Bytes {
			t.Fatal("source bytes changed")
		}
		observed += d.Count
	}
	if observed != 128 {
		t.Fatal("metadata lost occurrence rows")
	}
}

func TestReceptionRootCapturedStartupCannotRegress(t *testing.T) {
	for name, change := range map[string]func(*ReceptionStartup){
		"closed_to_open":    func(s *ReceptionStartup) { s.State, s.Marker, s.PrefixCutoff = "open", nil, 0 },
		"closed_to_queued":  func(s *ReceptionStartup) { s.State, s.Marker, s.PrefixCutoff = "queued", nil, 0 },
		"closed_to_unknown": func(s *ReceptionStartup) { s.State, s.Marker, s.PrefixCutoff = "unknown", nil, 0 },
		"cutoff":            func(s *ReceptionStartup) { s.PrefixCutoff-- },
		"at":                func(s *ReceptionStartup) { s.Marker.At = s.Marker.At.Add(-time.Nanosecond) },
		"source":            func(s *ReceptionStartup) { s.Marker.SourceGeneration-- },
		"slot":              func(s *ReceptionStartup) { s.Marker.Slot-- },
		"hash":              func(s *ReceptionStartup) { s.Marker.Bankhash = (solana.Hash{1}).String() },
		"attempt":           func(s *ReceptionStartup) { s.Marker.ReplayAttempt-- },
		"rewinds":           func(s *ReceptionStartup) { s.Marker.Rewinds-- },
		"completed":         func(s *ReceptionStartup) { s.Marker.CompletedIndex-- },
		"queue_loss":        func(s *ReceptionStartup) { s.Losses.QueueRefusals = ReceptionCounter{Value: 1} },
		"invalid_loss":      func(s *ReceptionStartup) { s.Losses.InvalidInputs = ReceptionCounter{Value: 1} },
		"tvu_loss":          func(s *ReceptionStartup) { s.Losses.RangeRefusals[0] = ReceptionCounter{Value: 1} },
		"repair_loss":       func(s *ReceptionStartup) { s.Losses.RangeRefusals[1] = ReceptionCounter{Value: 1} },
		"unmatched_loss":    func(s *ReceptionStartup) { s.Losses.RangeRefusals[2] = ReceptionCounter{Value: 1} },
		"descriptor_loss":   func(s *ReceptionStartup) { s.Losses.DescriptorEvictions = ReceptionCounter{Value: 1} },
		"chunk_loss":        func(s *ReceptionStartup) { s.Losses.EvictedChunks = ReceptionCounter{Value: 1} },
		"row_loss":          func(s *ReceptionStartup) { s.Losses.EvictedRows = ReceptionCounter{Value: 1} },
		"chunk_refusal":     func(s *ReceptionStartup) { s.Losses.ChunkRefusals = ReceptionCounter{Value: 1} },
	} {
		t.Run(name, func(t *testing.T) {
			r := receptionTestRoot(t, maximumReceptionMetadata())
			r.ConsumedThrough = math.MaxUint64
			change(&r.Startup)
			if !validReceptionStartup(r.Startup) {
				t.Fatal("regression fixture must be independently valid")
			}
			if _, err := PrepareReceptionHistoryRoot(r); err == nil {
				t.Fatal("same-recording regression accepted")
			}
		})
	}
	for _, captured := range []string{"queued", "unknown"} {
		m := receptionTestMetadata()
		m.StartupAtCapture = ReceptionStartup{State: captured, Limited: true}
		for _, current := range []string{"open", "queued"} {
			if current == captured {
				continue
			}
			r := receptionTestRoot(t, m)
			r.Startup.State = current
			if _, err := PrepareReceptionHistoryRoot(r); err == nil {
				t.Fatalf("%s -> %s accepted", captured, current)
			}
		}
	}
	m := receptionTestMetadata()
	m.StartupAtCapture.Limited = true
	r := receptionTestRoot(t, m)
	r.Startup.Limited = false
	if _, err := PrepareReceptionHistoryRoot(r); err == nil {
		t.Fatal("sticky limited flag cleared")
	}
	// Older open/queued pages remain valid while the current root records the
	// one terminal marker. Neither reversed wall dates nor lost descriptors
	// imply the pages captured that later marker.
	for _, state := range []string{"open", "queued"} {
		m := receptionTestMetadata()
		m.StartupAtCapture.State = state
		r := receptionTestRoot(t, m)
		r.ConsumedThrough = 3
		r.Startup = ReceptionStartup{State: "closed", PrefixCutoff: 3, Limited: true,
			Marker: &ReceptionMarker{At: m.PublishedAt.Add(-time.Hour), SourceGeneration: 99, Slot: 0, Bankhash: m.Binding.GenesisHash, ReplayAttempt: 1, CompletedIndex: 1},
			Losses: ReceptionLosses{DescriptorEvictions: ReceptionCounter{Value: 1}}}
		if _, err := PrepareReceptionHistoryRoot(r); err != nil {
			t.Fatal("new terminal marker beside old pages refused")
		}
	}
	r = receptionTestRoot(t, maximumReceptionMetadata())
	r.ConsumedThrough = math.MaxUint64
	r.Startup.Marker.At = r.Startup.Marker.At.In(time.FixedZone("equivalent UTC", 0))
	if _, err := PrepareReceptionHistoryRoot(r); err != nil {
		t.Fatal("same original instant in equivalent UTC location refused")
	}
}
