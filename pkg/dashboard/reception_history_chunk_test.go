package dashboard

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

func receptionChunkFixture(t *testing.T, first uint64, count int, maximum bool) PreparedReceptionChunk {
	t.Helper()
	var wire receptionChunkWire
	wire.First, wire.Last, wire.Columns = first, first+uint64(count-1), receptionChunkColumns[:]
	wire.Rows.count = count
	at := time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC)
	var root solana.Hash
	if maximum {
		at = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
		for i := range root {
			root[i] = 255
		}
	}
	for i := 0; i < count; i++ {
		r := receptionChunkRow{Sequence: first + uint64(i), At: at, Source: 1, Origin: 1, Root: root}
		if maximum {
			r.Source, r.Slot, r.Index, r.FEC = math.MaxUint64, math.MaxUint64, math.MaxUint32, math.MaxUint32
			r.Version, r.Variant, r.Origin = math.MaxUint16, math.MaxUint8, 3
		}
		wire.Rows.values[i] = r
	}
	body, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	d := ReceptionChunkDescriptor{FirstSequence: first, LastSequence: wire.Last, Count: uint64(count), MinAt: at, MaxAt: at,
		Digest: receptionDigest(body), Bytes: uint64(len(body))}
	p, err := PrepareReceptionHistoryChunk(d, string(body))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestReceptionChunkCanonicalMaximum(t *testing.T) {
	p := receptionChunkFixture(t, math.MaxUint64-127, 128, true)
	if len(p.Body()) != MaxReceptionCanonicalChunkBytes || len(p.Body()) >= MaxReceptionChunkBytes ||
		p.Descriptor().LastSequence != math.MaxUint64 || unsafe.Sizeof(receptionChunkRow{}) > 96 {
		t.Fatalf("maximum bytes=%d row=%d", len(p.Body()), unsafe.Sizeof(receptionChunkRow{}))
	}
	for _, size := range []int{MaxReceptionCanonicalChunkBytes + 1, MaxReceptionChunkBytes + 1} {
		body := strings.Repeat(" ", size)
		d := p.Descriptor()
		d.Digest, d.Bytes = receptionDigest([]byte(body)), uint64(len(body))
		if _, err := PrepareReceptionHistoryChunk(d, body); err == nil {
			t.Fatal("oversized body accepted")
		}
	}
	d := p.Descriptor()
	d.Digest = strings.Repeat("0", 64)
	if _, err := PrepareReceptionHistoryChunk(d, p.Body()); err == nil {
		t.Fatal("digest mismatch accepted")
	}
	d = p.Descriptor()
	d.MinAt = d.MinAt.Add(-time.Nanosecond)
	if _, err := PrepareReceptionHistoryChunk(d, p.Body()); err == nil {
		t.Fatal("descriptor date mismatch accepted")
	}
	d = p.Descriptor()
	d.Count, d.LastSequence = 127, d.LastSequence-1
	if _, err := PrepareReceptionHistoryChunk(d, p.Body()); err == nil {
		t.Fatal("descriptor count mismatch accepted")
	}
	if p.Descriptor().Count != 128 || p.Descriptor().LastSequence != math.MaxUint64 || len(p.Body()) != MaxReceptionCanonicalChunkBytes {
		t.Fatal("descriptor copy changed prepared value")
	}
	pair := receptionChunkFixture(t, 1, 2, false)
	var changed receptionChunkWire
	if err := json.Unmarshal([]byte(pair.Body()), &changed); err != nil {
		t.Fatal(err)
	}
	changed.Rows.values[1].Sequence++
	body, err := json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	d = pair.Descriptor()
	d.Digest, d.Bytes = receptionDigest(body), uint64(len(body))
	if _, err := PrepareReceptionHistoryChunk(d, string(body)); err == nil {
		t.Fatal("noncontiguous tuple sequences accepted")
	}
}

type receptionRawChunkTest struct {
	First   uint64            `json:"first_sequence,string"`
	Last    uint64            `json:"last_sequence,string"`
	Columns []string          `json:"columns"`
	Rows    []json.RawMessage `json:"rows"`
}

func TestReceptionChunkStrictTuples(t *testing.T) {
	p := receptionChunkFixture(t, 1, 1, false)
	changes := map[string]func([]json.RawMessage) []json.RawMessage{
		"short":            func(r []json.RawMessage) []json.RawMessage { return r[:9] },
		"long":             func(r []json.RawMessage) []json.RawMessage { return append(r, json.RawMessage("0")) },
		"numeric_sequence": func(r []json.RawMessage) []json.RawMessage { r[0] = json.RawMessage("1"); return r },
		"zero_sequence":    func(r []json.RawMessage) []json.RawMessage { r[0] = json.RawMessage(`"0"`); return r },
		"leading_zero":     func(r []json.RawMessage) []json.RawMessage { r[0] = json.RawMessage(`"01"`); return r },
		"uint64_overflow": func(r []json.RawMessage) []json.RawMessage {
			r[3] = json.RawMessage(`"18446744073709551616"`)
			return r
		},
		"zero_source":      func(r []json.RawMessage) []json.RawMessage { r[2] = json.RawMessage(`"0"`); return r },
		"null_slot":        func(r []json.RawMessage) []json.RawMessage { r[3] = json.RawMessage("null"); return r },
		"null_index":       func(r []json.RawMessage) []json.RawMessage { r[4] = json.RawMessage("null"); return r },
		"index_overflow":   func(r []json.RawMessage) []json.RawMessage { r[4] = json.RawMessage("4294967296"); return r },
		"fraction":         func(r []json.RawMessage) []json.RawMessage { r[5] = json.RawMessage("1.5"); return r },
		"version_overflow": func(r []json.RawMessage) []json.RawMessage { r[6] = json.RawMessage("65536"); return r },
		"variant_overflow": func(r []json.RawMessage) []json.RawMessage { r[7] = json.RawMessage("256"); return r },
		"origin":           func(r []json.RawMessage) []json.RawMessage { r[8] = json.RawMessage(`"repair"`); return r },
		"root":             func(r []json.RawMessage) []json.RawMessage { r[9] = json.RawMessage(`"111"`); return r },
		"offset_date": func(r []json.RawMessage) []json.RawMessage {
			r[1] = json.RawMessage(`"2000-01-02T03:04:05+00:00"`)
			return r
		},
		"zero_date": func(r []json.RawMessage) []json.RawMessage {
			r[1] = json.RawMessage(`"0001-01-01T00:00:00Z"`)
			return r
		},
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			var wire receptionRawChunkTest
			if err := json.Unmarshal([]byte(p.Body()), &wire); err != nil {
				t.Fatal(err)
			}
			var row []json.RawMessage
			if err := json.Unmarshal(wire.Rows[0], &row); err != nil {
				t.Fatal(err)
			}
			changed, err := json.Marshal(change(row))
			if err != nil {
				t.Fatal(err)
			}
			wire.Rows[0] = changed
			body, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			d := p.Descriptor()
			d.Digest, d.Bytes = receptionDigest(body), uint64(len(body))
			if _, err := PrepareReceptionHistoryChunk(d, string(body)); err == nil {
				t.Fatal("malformed tuple accepted")
			}
		})
	}
	for name, change := range map[string]func(string) string{
		"duplicate_key":   func(s string) string { return strings.Replace(s, "{", `{"first_sequence":"1",`, 1) },
		"extra_key":       func(s string) string { return strings.Replace(s, "{", `{"extra":0,`, 1) },
		"whitespace":      func(s string) string { return s + "\n" },
		"trailing":        func(s string) string { return s + "{}" },
		"missing_columns": func(s string) string { return strings.Replace(s, `"columns":[`, `"old_columns":[`, 1) },
		"extra_column": func(s string) string {
			return strings.Replace(s, `"authenticated_fec_root"]`, `"authenticated_fec_root","extra"]`, 1)
		},
		"short_columns": func(s string) string { return strings.Replace(s, `"sequence","at",`, `"sequence",`, 1) },
		"empty_rows":    func(s string) string { return s[:strings.Index(s, `"rows":`)] + `"rows":[]}` },
	} {
		t.Run(name, func(t *testing.T) {
			body := change(p.Body())
			if body == p.Body() {
				t.Fatal("mutation did not change bytes")
			}
			d := p.Descriptor()
			d.Digest, d.Bytes = receptionDigest([]byte(body)), uint64(len(body))
			if _, err := PrepareReceptionHistoryChunk(d, body); err == nil {
				t.Fatal("noncanonical chunk accepted")
			}
		})
	}
	var wire receptionRawChunkTest
	if err := json.Unmarshal([]byte(p.Body()), &wire); err != nil {
		t.Fatal(err)
	}
	for len(wire.Rows) < 129 {
		wire.Rows = append(wire.Rows, wire.Rows[0])
	}
	body, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	d := p.Descriptor()
	d.Digest, d.Bytes = receptionDigest(body), uint64(len(body))
	if _, err := PrepareReceptionHistoryChunk(d, string(body)); err == nil {
		t.Fatal("129 rows accepted")
	}
}

func TestReceptionChunkActualSourceBytes(t *testing.T) {
	at := time.Now()
	h, err := turbine.NewShredHistory(turbine.ShredHistoryBinding{RunID: "chunk-codec", NodeStartedAt: at, GenesisHash: solana.Hash{1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.CloseAfterProducers() })
	source, ok := h.RegisterSource(turbine.ShredHistoryLive, at)
	if !ok {
		t.Fatal("source registration failed")
	}
	for i := range 3 {
		// The authenticated all-zero FEC root and slot zero are legal. These
		// backwards wall dates must not reorder or redress consumer sequences.
		if !source.TryReceipt(turbine.ShredHistoryReceipt{At: at.Add(-time.Duration(i) * time.Second), Origin: turbine.ShredHistoryOrigin(i + 1)}) {
			t.Fatal("source receipt refused")
		}
	}
	source.Retire()
	if !h.CloseAfterProducers() {
		t.Fatal("source did not drain")
	}
	m, ok := h.Metadata()
	if !ok || m.LastSequence != 3 {
		t.Fatal("source rows missing")
	}
	var count uint64
	for _, d := range m.Chunks {
		body, ok := h.ChunkBody(d.FirstSequence, d.LastSequence, d.Digest)
		if !ok {
			t.Fatal("original body unavailable")
		}
		wire := ReceptionChunkDescriptor{FirstSequence: d.FirstSequence, LastSequence: d.LastSequence, Count: d.Count,
			MinAt: d.MinAt, MaxAt: d.MaxAt, Digest: receptionDigest([]byte(body)), Bytes: d.Bytes}
		p, err := PrepareReceptionHistoryChunk(wire, body)
		if err != nil || p.Body() != body || !sameReceptionChunk(p.Descriptor(), wire) {
			t.Fatalf("source changed: %v", err)
		}
		count += p.Descriptor().Count
	}
	if count != 3 {
		t.Fatal("source codec dropped occurrences")
	}
}
