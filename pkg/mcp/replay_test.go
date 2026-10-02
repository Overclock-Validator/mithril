package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func mkEntry(t *testing.T, slot uint64, timings map[string]TimingField) ReplayEntry {
	t.Helper()
	extra := map[string]json.RawMessage{}
	for k, v := range timings {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		extra[k] = b
	}
	return ReplayEntry{Slot: slot, Extra: extra}
}

func completeBlockTimings(txLoopNanoseconds uint64) map[string]TimingField {
	timings := make(map[string]TimingField, len(blockFields))
	for _, field := range blockFields {
		timings[field] = TimingField{}
	}
	timings["TxLoop"] = TimingField{Count: 1, SumNanoseconds: txLoopNanoseconds}
	return timings
}

func TestParseRealReplayLine(t *testing.T) {
	line := `{"Slot":419496101,"PreprocessBlock":{"Count":1,"SumNanoseconds":20969290398},"TxLoop":{"Count":1,"SumNanoseconds":436996525},"BankHash":{"Count":1,"SumNanoseconds":6081142}}`
	var e ReplayEntry
	if err := json.Unmarshal([]byte(line), &e); err != nil {
		t.Fatal(err)
	}
	if e.Slot != 419496101 || len(e.Extra) != 3 {
		t.Errorf("slot/extra = %d/%d", e.Slot, len(e.Extra))
	}
	tx, ok := e.get("TxLoop")
	if !ok || tx.Count != 1 || tx.SumNanoseconds != 436996525 {
		t.Errorf("TxLoop = %+v %v", tx, ok)
	}
}

func TestReplayEntryRejectsNullSlot(t *testing.T) {
	var entry ReplayEntry
	if err := json.Unmarshal([]byte(`{"Slot":null,"TxLoop":{"Count":1,"SumNanoseconds":1000000}}`), &entry); err == nil {
		t.Fatal("null Slot was accepted as slot zero")
	}
}

func TestReplayEntryOmitsSensitiveOrUnsafeDynamicFields(t *testing.T) {
	line := `{"Slot":1,"TxLoop":{"Count":1,"SumNanoseconds":1000000},"token":"TOP_SECRET","unsafe key TOP_SECRET":"value","FutureURL":"https://rpc.example/PATH_SECRET?api-key=QUERY_SECRET"}`
	var entry ReplayEntry
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatal(err)
	}
	if entry.OmittedExtraFieldCount != 2 {
		t.Fatalf("omitted extra fields = %d, want 2", entry.OmittedExtraFieldCount)
	}
	wire, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"TOP_SECRET", "PATH_SECRET", "QUERY_SECRET", `"token"`, "unsafe key"} {
		if strings.Contains(string(wire), secret) {
			t.Fatalf("replay dynamic field leaked %q: %s", secret, wire)
		}
	}
	if !strings.Contains(string(wire), `"omitted_extra_field_count":2`) || !strings.Contains(string(wire), `https://rpc.example:443/`) {
		t.Fatalf("replay omission/redaction metadata missing: %s", wire)
	}
}

func TestReplayEntryGetRequiresCompleteTimingObject(t *testing.T) {
	for _, test := range []struct {
		name, line, invalid, valid string
	}{
		{"scalar", `{"Slot":42,"Version":2,"TxLoop":{"Count":1,"SumNanoseconds":1000000}}`, "Version", "TxLoop"},
		{"missing sum", `{"Slot":1,"Partial":{"Count":1},"Full":{"Count":1,"SumNanoseconds":5}}`, "Partial", "Full"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var entry ReplayEntry
			if err := json.Unmarshal([]byte(test.line), &entry); err != nil {
				t.Fatal(err)
			}
			if _, ok := entry.get(test.invalid); ok {
				t.Errorf("%s must not parse as timing", test.invalid)
			}
			if _, ok := entry.get(test.valid); !ok {
				t.Errorf("%s should parse", test.valid)
			}
		})
	}
}

func TestBlockTotal(t *testing.T) {
	timings := completeBlockTimings(30_000_000)
	timings["PreprocessBlock"] = TimingField{Count: 1, SumNanoseconds: 10_000_000}
	timings["AccountsDeltaHash"] = TimingField{Count: 1, SumNanoseconds: 500_000_000} // nested in BankHash
	timings["BankHash"] = TimingField{Count: 1, SumNanoseconds: 5_000_000}
	timings["IxLoop"] = TimingField{Count: 1, SumNanoseconds: 1_000_000_000} // excluded
	e := mkEntry(t, 1, timings)
	v, ok := e.blockTotalMs()
	if !ok || math.Abs(v-45.0) > 0.001 {
		t.Errorf("block_total = %v %v, want 45 without nested AccountsDeltaHash", v, ok)
	}
	e2 := mkEntry(t, 1, map[string]TimingField{"TxLoop": {1, 1_000_000}})
	if _, ok := e2.blockTotalMs(); ok {
		t.Error("partial block fields should give no block_total")
	}
}

func TestMedianAndPercentile(t *testing.T) {
	if medianSorted([]float64{1, 2, 3}) != 2 {
		t.Error("odd median")
	}
	if medianSorted([]float64{1, 2, 3, 4}) != 2.5 {
		t.Error("even median")
	}
	if medianSorted(nil) != 0 {
		t.Error("empty median")
	}
	values := make([]float64, 52)
	for i := range values {
		values[i] = 10
	}
	values[len(values)-1] = 1000
	if got := percentileSorted(values, 0.99); got != 1000 {
		t.Errorf("nearest-rank p99 = %v, want 1000", got)
	}
	if percentileSorted(values, 0) != 10 || percentileSorted(values, 1) != 1000 || percentileSorted(nil, 0.99) != 0 {
		t.Error("percentile boundary handling")
	}
}

func TestComputeReplayStats(t *testing.T) {
	var entries []ReplayEntry
	for i := uint64(0); i < 100; i++ {
		entries = append(entries, mkEntry(t, 285000000+i, map[string]TimingField{"TxLoop": {1, 1_000_000 + i*500_000}}))
	}
	s := computeReplayStats(entries, "TxLoop")
	if !s.FieldFound || s.Count != 100 || s.P99Ms <= s.P50Ms || s.MinMs > s.MaxMs {
		t.Errorf("stats = %+v", s)
	}
	if s := computeReplayStats(entries, "NoSuchField"); s.FieldFound {
		t.Error("missing field should not be found")
	} else if s.ShapeIncompleteCount != len(entries) {
		t.Errorf("missing arbitrary timing field shape_incomplete_count = %d, want %d", s.ShapeIncompleteCount, len(entries))
	}
	if s := computeReplayStats(nil, "TxLoop"); s.FieldFound {
		t.Error("empty entries should not find field")
	}
}

func TestComputeReplayStatsP99KeepsSingleTailSample(t *testing.T) {
	entries := make([]ReplayEntry, 0, 52)
	for i := uint64(0); i < 52; i++ {
		nanos := uint64(10_000_000)
		if i == 51 {
			nanos = 1_000_000_000
		}
		entries = append(entries, mkEntry(t, 285000000+i, map[string]TimingField{"TxLoop": {Count: 1, SumNanoseconds: nanos}}))
	}
	stats := computeReplayStats(entries, "TxLoop")
	if stats.P99Ms != 1000 {
		t.Fatalf("p99 = %v, want 1000", stats.P99Ms)
	}
	if status, _ := diagnoseReplayEvidence(stats, ReplayMeta{}, 400); status != checkDegraded {
		t.Fatalf("replay status = %q, want %q", status, checkDegraded)
	}
}

func TestComputeReplayStatsBlockTotal(t *testing.T) {
	var entries []ReplayEntry
	for i := uint64(0); i < 10; i++ {
		timings := completeBlockTimings((i + 1) * 1_000_000)
		timings["BankHash"] = TimingField{Count: 1, SumNanoseconds: 1_000_000}
		entries = append(entries, mkEntry(t, i, timings))
	}
	s := computeReplayStats(entries, "block_total")
	if !s.FieldFound || s.Count != 10 || math.Abs(s.MeanMs-6.5) > 0.001 || s.Measurement != "phase_sum_estimate" || !strings.Contains(s.Caveat, "do not prove every phase ran") {
		t.Errorf("block_total stats = %+v (mean want 6.5)", s)
	}
}

func writeReplayFixture(t *testing.T, dir, body string) string {
	t.Helper()
	path := filepath.Join(dir, "replay_timings.jsonl")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeReplayFile(t *testing.T, n uint64) string {
	t.Helper()
	var lines []string
	for slot := uint64(1); slot <= n; slot++ {
		e := mkEntry(t, slot, map[string]TimingField{"TxLoop": {1, slot * 1_000_000}})
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, string(b))
	}
	return writeReplayFixture(t, t.TempDir(), strings.Join(lines, "\n"))
}

func TestReadReplayLastN(t *testing.T) {
	path := writeReplayFile(t, 5)
	n := 2
	entries, stats, meta, err := readReplayTimingsContext(context.Background(), path, nil, nil, &n, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Slot != 4 || entries[1].Slot != 5 {
		t.Errorf("last_n=2 kept wrong entries: %v", entries)
	}
	if meta.TotalMatched != 5 || meta.Returned != 2 || !meta.Truncated || meta.ParseErrors != 0 {
		t.Errorf("meta = %+v", meta)
	}
	if !stats.FieldFound || stats.Count != 2 {
		t.Errorf("stats = %+v", stats)
	}
}

func TestReadReplayRejectsOversizedTimingFieldBeforeOpeningFile(t *testing.T) {
	_, _, _, err := readReplayTimingsContext(context.Background(), filepath.Join(t.TempDir(), "missing"), nil, nil, nil, strings.Repeat("x", maxTimingFieldNameBytes+1))
	if err == nil || !strings.Contains(err.Error(), "timing_field") {
		t.Fatalf("oversized timing field error = %v", err)
	}
}

func TestReadReplayUsesBoundedSuffixForLongRunningFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "replay_timings.jsonl")
	var lines []string
	for slot := uint64(100); slot < 100+minReplayHealthSamples; slot++ {
		data, err := json.Marshal(mkEntry(t, slot, completeBlockTimings(10_000_000)))
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, string(data))
	}
	suffix := []byte(strings.Join(lines, "\n") + "\n")
	size := int64(maxReplayScanBytes) + int64(len(suffix)) + 2
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		t.Fatal(err)
	}
	start := size - int64(maxReplayScanBytes)
	if _, err := f.WriteAt([]byte{'\n'}, start); err != nil {
		f.Close()
		t.Fatal(err)
	}
	suffixOffset := size - int64(len(suffix))
	if _, err := f.WriteAt([]byte{'\n'}, suffixOffset-1); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if _, err := f.WriteAt(suffix, suffixOffset); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	n := minReplayHealthSamples
	entries, stats, meta, err := readReplayTimingsContext(context.Background(), path, nil, nil, &n, timingBlockTotal)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != minReplayHealthSamples || stats.Count != minReplayHealthSamples || !meta.PartialSource || meta.ScannedBytes != maxReplayScanBytes || meta.SourceSizeBytes != size {
		t.Fatalf("bounded suffix result = entries:%d stats:%+v meta:%+v", len(entries), stats, meta)
	}
}

func TestReplayScanReportsConcurrentShrinkAsIncomplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replay_timings.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	observedSize := int64(maxReplayScanBytes + 1)
	if err := f.Truncate(observedSize); err != nil {
		t.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(0); err != nil {
		t.Fatal(err)
	}

	entries, _, meta, err := readReplayTimingsFileContext(context.Background(), f, path, "flat", info, nil, nil, nil, "")
	if err != nil {
		t.Fatalf("concurrent shrink returned a tool error: %v", err)
	}
	if len(entries) != 0 || !meta.SourceChangedDuringScan || meta.ScannedBytes != 0 || meta.SourceSizeBytes != observedSize || meta.resolvedPath != path {
		t.Fatalf("concurrent shrink result = entries:%d meta:%+v", len(entries), meta)
	}
}

func TestReplayScanAllowsAppendAfterSnapshot(t *testing.T) {
	dir := t.TempDir()
	first, err := json.Marshal(mkEntry(t, 1, completeBlockTimings(1_000_000)))
	if err != nil {
		t.Fatal(err)
	}
	path := writeReplayFixture(t, dir, string(first)+"\n")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(mkEntry(t, 2, completeBlockTimings(2_000_000)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(append(second, '\n')); err != nil {
		t.Fatal(err)
	}

	entries, _, meta, err := readReplayTimingsFileContext(context.Background(), f, path, "flat", info, nil, nil, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Slot != 1 || meta.SourceChangedDuringScan || meta.ScannedBytes != info.Size() {
		t.Fatalf("append-after-snapshot result = entries:%+v meta:%+v", entries, meta)
	}
}

func TestReplayScanRejectsSameSizeRewrite(t *testing.T) {
	dir := t.TempDir()
	before, err := json.Marshal(mkEntry(t, 1, completeBlockTimings(1_000_000)))
	if err != nil {
		t.Fatal(err)
	}
	path := writeReplayFixture(t, dir, string(before)+"\n")
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	after, err := json.Marshal(mkEntry(t, 2, completeBlockTimings(1_000_000)))
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("test rewrite changed size: before=%d after=%d", len(before), len(after))
	}
	if _, err := f.WriteAt(after, 0); err != nil {
		t.Fatal(err)
	}
	changedAt := info.ModTime().Add(2 * time.Second)
	if err := os.Chtimes(path, changedAt, changedAt); err != nil {
		t.Fatal(err)
	}

	_, _, meta, err := readReplayTimingsFileContext(context.Background(), f, path, "flat", info, nil, nil, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if !meta.SourceChangedDuringScan {
		t.Fatalf("same-size rewrite was accepted as stable: %+v", meta)
	}
}

func TestReadReplayExplainsEmptyFilteredBoundedSuffix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "replay_timings.jsonl")
	size := int64(maxReplayScanBytes) + 1024
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		t.Fatal(err)
	}
	start := size - int64(maxReplayScanBytes)
	if _, err := f.WriteAt([]byte{'\n'}, start-1); err != nil {
		f.Close()
		t.Fatal(err)
	}
	entry, err := json.Marshal(mkEntry(t, 100, map[string]TimingField{"TxLoop": {Count: 1, SumNanoseconds: 1_000_000}}))
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	if _, err := f.WriteAt(append(entry, '\n'), start); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	from, to := uint64(1), uint64(2)
	entries, _, meta, err := readReplayTimingsContext(context.Background(), path, &from, &to, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 || !meta.PartialSource || !strings.Contains(meta.CoverageNote, "may exist before") {
		t.Fatalf("bounded filtered result did not explain its coverage: entries=%v meta=%+v", entries, meta)
	}
}

func TestReadReplayLastNBoundaries(t *testing.T) {
	zero := 0
	for _, test := range []struct {
		name          string
		lastN         *int
		wantReturned  int
		wantTruncated bool
		wantStats     bool
	}{
		{"zero", &zero, 0, true, false},
		{"unset", nil, 5, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			entries, stats, meta, err := readReplayTimingsContext(context.Background(), writeReplayFile(t, 5), nil, nil, test.lastN, "")
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != test.wantReturned || meta.TotalMatched != 5 || meta.Returned != test.wantReturned || meta.Truncated != test.wantTruncated || stats.FieldFound != test.wantStats {
				t.Errorf("entries=%d meta=%+v stats.found=%v", len(entries), meta, stats.FieldFound)
			}
		})
	}
}

func TestReadReplayLineHandling(t *testing.T) {
	huge := strings.Repeat("x", 2*1024*1024)
	valid1 := `{"Slot":1,"TxLoop":{"Count":1,"SumNanoseconds":1000000}}`
	valid2 := `{"Slot":2,"TxLoop":{"Count":1,"SumNanoseconds":2000000}}`
	for _, test := range []struct {
		name            string
		body            string
		wantSlots       []uint64
		wantParseErrors int64
	}{
		{"oversize line", fmt.Sprintf(valid1+"\n%s\n"+valid2+"\n", huge), []uint64{1, 2}, 1},
		{"no trailing newline", valid1, []uint64{1}, 0},
		{"parse error", valid1 + "\nnot json at all\n\n" + valid2 + "\n", []uint64{1, 2}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := writeReplayFixture(t, t.TempDir(), test.body)
			entries, _, meta, err := readReplayTimingsContext(context.Background(), path, nil, nil, nil, "")
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != len(test.wantSlots) || meta.TotalMatched != len(test.wantSlots) || meta.ParseErrors != test.wantParseErrors {
				t.Fatalf("entries=%v meta=%+v", entries, meta)
			}
			for i, want := range test.wantSlots {
				if entries[i].Slot != want {
					t.Errorf("entry %d slot = %d, want %d", i, entries[i].Slot, want)
				}
			}
		})
	}
}

func TestResolveReplayPath(t *testing.T) {
	// An explicit replay file uses the flat layout.
	p := writeReplayFile(t, 1)
	got, layout, err := resolveReplayPathChecked(p)
	if err != nil || got != p || layout != "flat" {
		t.Errorf("direct path = %q/%q, %v", got, layout, err)
	}
	// A missing flat file falls back to the active run.
	dir := t.TempDir()
	configured := filepath.Join(dir, "replay_timings.jsonl")
	latest := filepath.Join(dir, "latest")
	if err := os.Mkdir(latest, 0o755); err != nil {
		t.Fatal(err)
	}
	actual := writeReplayFixture(t, latest, "x")
	wantResolved, err := filepath.EvalSymlinks(actual)
	if err != nil {
		t.Fatal(err)
	}
	got, layout, err = resolveReplayPathChecked(configured)
	if err != nil || got != wantResolved || layout != "latest" {
		t.Errorf("latest path = %q/%q, %v; want %q/latest", got, layout, err, wantResolved)
	}
	// A rejected symlink escape must remain visible to readers.
	outside := t.TempDir()
	writeReplayFixture(t, outside, "secret")
	dir2 := t.TempDir()
	configured2 := filepath.Join(dir2, "replay_timings.jsonl")
	if err := os.Symlink(outside, filepath.Join(dir2, "latest")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := resolveReplayPathChecked(configured2); err == nil {
		t.Error("read path must surface an unsafe latest symlink")
	}

	// An active latest run wins over a stale flat file.
	dir3 := t.TempDir()
	flat := writeReplayFixture(t, dir3, `{"Slot":1}`)
	latest3 := filepath.Join(dir3, "latest")
	if err := os.Mkdir(latest3, 0o755); err != nil {
		t.Fatal(err)
	}
	active := writeReplayFixture(t, latest3, `{"Slot":2}`)
	got, layout, err = resolveReplayPathChecked(flat)
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(active)
	if err != nil {
		t.Fatal(err)
	}
	if got != want || layout != "latest" {
		t.Fatalf("active path = %q/%q, want %q/latest", got, layout, want)
	}

	// The latest marker is authoritative even before the current run emits its
	// replay file; never fall back to a stale flat file.
	dir4 := t.TempDir()
	flat4 := writeReplayFixture(t, dir4, `{"Slot":1}`)
	run4 := filepath.Join(dir4, "run-current")
	if err := os.Mkdir(run4, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("run-current", filepath.Join(dir4, "latest")); err != nil {
		t.Fatal(err)
	}
	got, layout, err = resolveReplayPathChecked(flat4)
	if err != nil {
		t.Fatal(err)
	}
	resolvedRun4, err := filepath.EvalSymlinks(run4)
	if err != nil {
		t.Fatal(err)
	}
	want4 := filepath.Join(resolvedRun4, "replay_timings.jsonl")
	if got != want4 || layout != "latest" {
		t.Fatalf("missing active replay path = %q/%q", got, layout)
	}
	if _, _, _, err := readReplayTimingsContext(context.Background(), flat4, nil, nil, nil, ""); err == nil {
		t.Fatal("missing active replay file incorrectly fell back to stale flat data")
	}
}

func TestTruncateForDisplay(t *testing.T) {
	var small, big []ReplayEntry
	for i := uint64(0); i < maxReplayDisplayEntries; i++ {
		small = append(small, mkEntry(t, i, map[string]TimingField{"TxLoop": {1, 1}}))
	}
	if shown, trunc := truncateForDisplay(small); trunc || len(shown) != maxReplayDisplayEntries {
		t.Errorf("display-limit entries: shown=%d trunc=%v", len(shown), trunc)
	}
	for i := uint64(0); i < 200; i++ {
		big = append(big, mkEntry(t, i, map[string]TimingField{"TxLoop": {1, 1}}))
	}
	shown, trunc := truncateForDisplay(big)
	if !trunc || len(shown) != maxReplayDisplayEntries {
		t.Errorf("200 entries: shown=%d trunc=%v", len(shown), trunc)
	}
	if shown[0].Slot != 0 || shown[len(shown)-1].Slot != 199 {
		t.Errorf("truncation should keep head+tail: first=%d last=%d", shown[0].Slot, shown[len(shown)-1].Slot)
	}
}

func TestReplayDisplayWorstCaseFitsDefaultWireBudget(t *testing.T) {
	entries := make([]ReplayEntry, 0, 20)
	largeValue, err := json.Marshal(strings.Repeat("x", maxReplayLineBytes-1024))
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 20; i++ {
		entries = append(entries, ReplayEntry{Slot: i, Extra: map[string]json.RawMessage{"FutureField": largeValue}})
	}
	shown, truncated := truncateForDisplay(entries)
	if !truncated {
		t.Fatal("large display fixture should be truncated")
	}
	wire, err := json.Marshal(replayOutput{EntryCount: len(entries), DisplayTruncated: truncated, Entries: shown})
	if err != nil {
		t.Fatal(err)
	}
	// The SDK sends this JSON once as structured content and once as its text
	// compatibility fallback. Leave some room for the surrounding response.
	if 2*len(wire) >= DefaultOutputBudgetBytes-64*1024 {
		t.Fatalf("worst-case replay display is too large for the default MCP budget: %d bytes duplicated", len(wire))
	}
}
