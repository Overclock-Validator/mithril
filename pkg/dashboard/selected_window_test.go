package dashboard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func selectedWindowFixture(count int) SelectedWindow {
	core := replayDetailTestCore(0)
	w := SelectedWindow{SchemaVersion: 1, Family: "selected_slots", Scope: "recent_selected_and_folded",
		Binding: core.Binding, CapturedAt: core.CapturedAt, ObservedAt: core.UpdatedAt,
		WindowEntries: uint64(count), Records: make([]ReplaySelectedRecord, count)}
	for i := range w.Records {
		v := maximumReplayLatestSelected(w.ObservedAt)
		v.Slot = math.MaxUint64 - uint64(count-1-i)
		units := uint64(math.MaxUint64)
		v.Facts.Transactions.ComputeUnits = &units
		w.Records[i] = ReplaySelectedRecord{Slot: v.Slot, Bankhash: v.Bankhash, BlockID: v.BlockID, Facts: v.Facts}
	}
	return w
}

func selectedWindowBodies(p *preparedSelectedWindow) [][]byte {
	pages := make([][]byte, len(p.pages))
	for i := range p.pages {
		pages[i] = []byte(p.pages[i])
	}
	return pages
}

func TestSelectedWindowCanonicalRoundTripAndBounds(t *testing.T) {
	for _, count := range []int{0, 1, 8, 9, 256} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			w := selectedWindowFixture(count)
			if count == 256 {
				w.WindowEntries = 512
			}
			p, err := prepareSelectedWindow(w)
			if err != nil {
				t.Fatal(err)
			}
			if p.bytes > selectedWindowMaxBytes || len(p.pages) != (count+7)/8 {
				t.Fatalf("unexpected encoded bounds: %d bytes, %d pages", p.bytes, len(p.pages))
			}
			got, err := decodeSelectedWindow([]byte(p.manifest), selectedWindowBodies(p))
			if err != nil || !reflect.DeepEqual(got, w) {
				t.Fatalf("round trip: %v", err)
			}
			if count > 0 {
				*w.Records[0].Facts.Transactions.ComputeUnits = 0
				if *got.Records[0].Facts.Transactions.ComputeUnits != math.MaxUint64 {
					t.Fatal("encoded generation aliases source")
				}
			}
			t.Logf("selected window: %d rows, %d pages, %d encoded bytes", count, len(p.pages), p.bytes)
		})
	}
}

func TestSelectedWindowFeeBurnMaximumBounds(t *testing.T) {
	w := selectedWindowFixture(256)
	w.WindowEntries = 512
	for i := range w.Records {
		w.Records[i].Facts.Fees.BurnedLamports = new(uint64(math.MaxUint64))
	}
	p, err := prepareSelectedWindow(w)
	if err != nil || p.bytes > selectedWindowMaxBytes {
		t.Fatalf("maximum selected fee window: %v", err)
	}
	got, err := decodeSelectedWindow([]byte(p.manifest), selectedWindowBodies(p))
	if err != nil || !reflect.DeepEqual(got, w) {
		t.Fatalf("maximum selected fee window round trip: %v", err)
	}
	*w.Records[0].Facts.Fees.BurnedLamports = 0
	if *got.Records[0].Facts.Fees.BurnedLamports != math.MaxUint64 {
		t.Fatal("decoded burn aliases producer")
	}
}

func TestSelectedWindowRejectsGapsMutationAndMixedPages(t *testing.T) {
	w := selectedWindowFixture(9)
	w.Records[3] = ReplaySelectedRecord{Slot: w.Records[3].Slot, Gap: true}
	if _, err := prepareSelectedWindow(w); err == nil {
		t.Fatal("gap without incomplete coverage accepted")
	}
	w.CoverageIncomplete = true
	p, err := prepareSelectedWindow(w)
	if err != nil {
		t.Fatal(err)
	}
	manifest, pages := []byte(p.manifest), selectedWindowBodies(p)
	if _, err := decodeSelectedWindow(manifest, pages); err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name string
		m    []byte
		p    [][]byte
	}{
		{"missing page", manifest, pages[:1]},
		{"reordered pages", manifest, [][]byte{pages[1], pages[0]}},
		{"page changed", manifest, [][]byte{bytes.Replace(pages[0], []byte(`"saturated":true`), []byte(`"saturated":false`), 1), pages[1]}},
		{"manifest unknown field", bytes.Replace(manifest, []byte(`"family":`), []byte(`"extra":0,"family":`), 1), pages},
		{"manifest duplicate", bytes.Replace(manifest, []byte(`"family":`), []byte(`"family":"selected_slots","family":`), 1), pages},
		{"optional null", manifest, [][]byte{bytes.Replace(pages[0], []byte(`"compute_units":"18446744073709551615"`), []byte(`"compute_units":null`), 1), pages[1]}},
	}
	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeSelectedWindow(tc.m, tc.p); err == nil {
				t.Fatal("invalid generation accepted")
			}
		})
	}
	var m selectedWindowManifest
	if err := json.Unmarshal(manifest, &m); err != nil {
		t.Fatal(err)
	}
	m.Digest = strings.ToUpper(m.Digest)
	bad, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeSelectedWindow(bad, pages); err == nil {
		t.Fatal("noncanonical digest accepted")
	}
}

func TestSelectedWindowRejectsInvalidSource(t *testing.T) {
	for name, change := range map[string]func(*SelectedWindow){
		"count":    func(w *SelectedWindow) { w.WindowEntries++ },
		"limit":    func(w *SelectedWindow) { w.WindowEntries = 513 },
		"nil rows": func(w *SelectedWindow) { w.Records = nil },
		"duplicate": func(w *SelectedWindow) {
			w.Records[1].Slot = w.Records[0].Slot
		},
		"binding": func(w *SelectedWindow) { w.Binding.Attempt = 0 },
		"date":    func(w *SelectedWindow) { w.ObservedAt = w.ObservedAt.AddDate(-3000, 0, 0) },
	} {
		t.Run(name, func(t *testing.T) {
			w := selectedWindowFixture(2)
			change(&w)
			if _, err := prepareSelectedWindow(w); err == nil {
				t.Fatal("invalid source accepted")
			}
		})
	}
}

func TestSelectedWindowMaximumWireShape(t *testing.T) {
	w := selectedWindowFixture(256)
	w.WindowEntries = 512
	w.Binding.RunID = strings.Repeat("<", 128)
	w.Binding.SourceGeneration, w.Binding.Attempt, w.Binding.Rewinds = math.MaxUint64, math.MaxUint64, math.MaxUint64
	genesis := "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG"
	w.Binding.GenesisHash = &genesis
	w.CapturedAt, w.ObservedAt = sizingTime(w.CapturedAt), sizingTime(w.ObservedAt)
	p, err := prepareSelectedWindow(w)
	if err != nil {
		t.Fatal(err)
	}
	maximum := 0
	for _, page := range p.pages {
		maximum = max(maximum, len(page))
	}
	if len(p.pages) != 32 || maximum > MaxSnapshotBytes || p.bytes > selectedWindowMaxBytes {
		t.Fatalf("maximum selected shape exceeds bounds: pages=%d page=%d total=%d", len(p.pages), maximum, p.bytes)
	}
	if _, err := decodeSelectedWindow([]byte(p.manifest), selectedWindowBodies(p)); err != nil {
		t.Fatal("maximum selected shape did not decode", err)
	}
	t.Logf("maximum selected shape: %d pages, %d byte page, %d total bytes", len(p.pages), maximum, p.bytes)
}
