package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	maxReplayEntries        = 10_000
	maxReplayScanBytes      = 8 * 1024 * 1024 // bounded suffix; core JSONL is unrotated
	maxReplayLineBytes      = 64 * 1024       // core's fixed BlockReplay record is only a few KiB
	maxReplayDisplayEntries = 6               // summary-first; also bounds the duplicated MCP wire result
	maxTimingFieldNameBytes = 128             // bounds repeated map hashing across the retained window
	maxReplayExtraNameBytes = 128
	minReplayHealthSamples  = 20
	timingBlockTotal        = "block_total"
	defaultTimingField      = "TxLoop"
)

// blockFields define the expected block-timing field shape. Lower-level
// instruction/sbpf timings are nested inside TxLoop/IxLoop, and
// AccountsDeltaHash is nested inside BankHash, so that field is shape-checked
// but not added twice.
var blockFields = []string{
	"PreprocessBlock", "LoadBlockAccounts", "TxLoop", "Reward", "Rent",
	"RunIncinerator", "BlockUpdateAccounts", "AccountsDeltaHash", "BankHash",
}

// TimingField is one measurement: how many times it ran and total elapsed ns.
type TimingField struct {
	Count          uint64 `json:"Count"`
	SumNanoseconds uint64 `json:"SumNanoseconds"`
}

func (t TimingField) totalMs() float64 { return float64(t.SumNanoseconds) / 1_000_000.0 }

// ReplayEntry is one replay_timings.jsonl record. Extra holds safe dynamic
// timing fields for forward compatibility.
type ReplayEntry struct {
	Slot                   uint64                     `json:"Slot"`
	Extra                  map[string]json.RawMessage `json:"-"`
	OmittedExtraFieldCount int                        `json:"-"`
}

func validReplayExtraName(name string) bool {
	if name == "" || len(name) > maxReplayExtraNameBytes {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.' {
			continue
		}
		return false
	}
	return true
}

func (e *ReplayEntry) UnmarshalJSON(data []byte) error {
	e.OmittedExtraFieldCount = 0
	var all map[string]json.RawMessage
	if err := json.Unmarshal(data, &all); err != nil {
		return err
	}
	slotRaw, ok := all["Slot"]
	if !ok {
		return fmt.Errorf("replay entry missing Slot")
	}
	var slot *uint64
	if err := json.Unmarshal(slotRaw, &slot); err != nil {
		return fmt.Errorf("replay entry Slot: %w", err)
	}
	if slot == nil {
		return errors.New("replay entry Slot must not be null")
	}
	e.Slot = *slot
	delete(all, "Slot")
	for key, raw := range all {
		if !validReplayExtraName(key) || isSensitiveFieldName(key) || key == "omitted_extra_field_count" {
			delete(all, key)
			e.OmittedExtraFieldCount++
			continue
		}
		all[key] = redactRawJSON(raw)
	}
	e.Extra = all
	return nil
}

func (e ReplayEntry) MarshalJSON() ([]byte, error) {
	base := struct {
		Slot                   uint64 `json:"Slot"`
		OmittedExtraFieldCount int    `json:"omitted_extra_field_count,omitempty"`
	}{Slot: e.Slot, OmittedExtraFieldCount: e.OmittedExtraFieldCount}
	named, err := json.Marshal(base)
	if err != nil {
		return nil, err
	}
	return mergeExtra(named, e.Extra)
}

// get parses a timing field by PascalCase name; nil if absent or not shaped like
// a {Count, SumNanoseconds} object (e.g. a future scalar field).
func (e *ReplayEntry) get(name string) (TimingField, bool) {
	raw, ok := e.Extra[name]
	if !ok {
		return TimingField{}, false
	}
	// A timing requires both Count and SumNanoseconds.
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return TimingField{}, false
	}
	if _, hasCount := probe["Count"]; !hasCount {
		return TimingField{}, false
	}
	if _, hasSum := probe["SumNanoseconds"]; !hasSum {
		return TimingField{}, false
	}
	var t TimingField
	if err := json.Unmarshal(raw, &t); err != nil {
		return TimingField{}, false
	}
	return t, true
}

// blockTotalMs sums records with the complete expected block-timing field shape.
// The core writer emits every field (including zero-count fields); field presence
// does not prove that a phase executed. Rejecting a partial shape prevents a
// truncated line that retained only a fast field from producing a healthy p99.
func (e *ReplayEntry) blockTotalMs() (float64, bool) {
	sum := 0.0
	for _, f := range blockFields {
		t, ok := e.get(f)
		if !ok {
			return 0, false
		}
		if f == "AccountsDeltaHash" {
			continue
		}
		sum += t.totalMs()
	}
	return sum, true
}

// ReplayStats is percentile stats for a chosen timing field.
type ReplayStats struct {
	TimingField          string  `json:"timing_field"`
	Measurement          string  `json:"measurement"`
	Caveat               string  `json:"caveat,omitempty"`
	FieldFound           bool    `json:"field_found"`
	Count                int     `json:"count"`
	P50Ms                float64 `json:"p50_ms"`
	P95Ms                float64 `json:"p95_ms"`
	P99Ms                float64 `json:"p99_ms"`
	MeanMs               float64 `json:"mean_ms"`
	MinMs                float64 `json:"min_ms"`
	MaxMs                float64 `json:"max_ms"`
	ShapeIncompleteCount int     `json:"shape_incomplete_count"`
}

// ReplayMeta is read-level metadata about a replay query.
type ReplayMeta struct {
	TotalMatched            int    `json:"total_matched"`
	Returned                int    `json:"returned"`
	Truncated               bool   `json:"truncated"`
	ParseErrors             int64  `json:"parse_errors"`
	SourceLayout            string `json:"source_layout"`
	ModifiedAt              string `json:"modified_at,omitempty"`
	SourceSizeBytes         int64  `json:"source_size_bytes"`
	ScannedBytes            int64  `json:"scanned_bytes"`
	PartialSource           bool   `json:"partial_source"`
	IncompleteTail          bool   `json:"incomplete_tail"`
	SourceChangedDuringScan bool   `json:"source_changed_during_scan,omitempty"`
	CoverageNote            string `json:"coverage_note,omitempty"`
	resolvedPath            string
}

// resolveReplayPathChecked prefers the active latest run over a stale flat
// legacy file. A present but unsafe latest path is an error, not a reason to
// silently fall back to legacy evidence.
func resolveReplayPathChecked(path string) (resolved, layout string, err error) {
	parent := filepath.Dir(path)
	name := filepath.Base(path)
	activeDir, active, err := resolveLatestRunDir(parent)
	if err != nil {
		return "", "", err
	}
	if active {
		latest := filepath.Join(activeDir, name)
		info, statErr := os.Stat(latest)
		if os.IsNotExist(statErr) {
			return latest, "latest", nil
		}
		if statErr != nil {
			return "", "", fmt.Errorf("inspect active replay path: %w", statErr)
		}
		if !info.Mode().IsRegular() {
			return "", "", fmt.Errorf("active replay path is not a regular file: %s", latest)
		}
		resolved, err := resolveConfined(latest, parent)
		if err != nil {
			return "", "", fmt.Errorf("active replay path is unsafe: %w", err)
		}
		return resolved, "latest", nil
	}
	if info, statErr := os.Stat(path); statErr == nil {
		if !info.Mode().IsRegular() {
			return "", "", fmt.Errorf("legacy replay path is not a regular file: %s", path)
		}
		return path, "flat", nil
	} else if !os.IsNotExist(statErr) {
		return "", "", fmt.Errorf("inspect legacy replay path: %w", statErr)
	}
	return path, "missing", nil
}

func readReplayTimingsContext(ctx context.Context, path string, slotFrom, slotTo *uint64, lastN *int, timingField string) ([]ReplayEntry, ReplayStats, ReplayMeta, error) {
	if len(timingField) > maxTimingFieldNameBytes {
		return nil, ReplayStats{}, ReplayMeta{}, fmt.Errorf("timing_field exceeds %d-byte limit", maxTimingFieldNameBytes)
	}
	resolved, layout, err := resolveReplayPathChecked(path)
	if err != nil {
		return nil, ReplayStats{}, ReplayMeta{}, err
	}
	var f *os.File
	if layout == "latest" {
		f, err = openConfinedRegularFile(resolved, filepath.Dir(path))
	} else {
		// The flat path is explicit process configuration and may intentionally
		// be a symlink. Only the derived latest path needs parent confinement.
		f, err = os.OpenFile(resolved, os.O_RDONLY|nonBlockingOpenFlag, 0)
	}
	if os.IsNotExist(err) {
		return nil, ReplayStats{}, ReplayMeta{}, fmt.Errorf("replay timings file not found: %s (tried direct path and 'latest/' fallback)", resolved)
	}
	if err != nil {
		return nil, ReplayStats{}, ReplayMeta{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, ReplayStats{}, ReplayMeta{}, err
	}
	if !info.Mode().IsRegular() {
		return nil, ReplayStats{}, ReplayMeta{}, errors.New("replay path is not a regular file")
	}
	return readReplayTimingsFileContext(ctx, f, resolved, layout, info, slotFrom, slotTo, lastN, timingField)
}

func readReplayTimingsFileContext(ctx context.Context, f *os.File, resolved, layout string, info os.FileInfo, slotFrom, slotTo *uint64, lastN *int, timingField string) ([]ReplayEntry, ReplayStats, ReplayMeta, error) {
	maxEntries := 1000
	if lastN != nil {
		maxEntries = *lastN
	}
	if maxEntries > maxReplayEntries {
		maxEntries = maxReplayEntries
	}
	if maxEntries < 0 {
		maxEntries = 0
	}

	// replay_timings.jsonl is unrotated and can grow for the entire run. Read a
	// bounded suffix instead of aging out permanently or rescanning hundreds of
	// MiB on every diagnostic. If the suffix starts mid-line, discard that one
	// boundary fragment before parsing complete JSONL records.
	scanBytes := info.Size()
	if scanBytes > maxReplayScanBytes {
		scanBytes = maxReplayScanBytes
	}
	startOffset := info.Size() - scanBytes
	section := io.NewSectionReader(f, startOffset, scanBytes)
	r := bufio.NewReaderSize(section, 64*1024)
	sourceChanged := false
	if startOffset > 0 {
		var previous [1]byte
		if _, err := f.ReadAt(previous[:], startOffset-1); err != nil {
			if errors.Is(err, io.EOF) {
				sourceChanged = true
			} else {
				return nil, ReplayStats{}, ReplayMeta{}, err
			}
		}
		if !sourceChanged && previous[0] != '\n' {
			if _, _, _, _, err := readCappedLine(r, maxReplayLineBytes); err != nil {
				return nil, ReplayStats{}, ReplayMeta{}, err
			}
		}
	}

	ring := make([]ReplayEntry, maxEntries)
	totalMatched := 0
	var parseErrors int64
	incompleteTail := false

	for {
		if err := ctx.Err(); err != nil {
			return nil, ReplayStats{}, ReplayMeta{}, err
		}
		lineBytes, oversize, terminated, eof, e := readCappedLine(r, maxReplayLineBytes)
		if e != nil {
			return nil, ReplayStats{}, ReplayMeta{}, e
		}
		if eof {
			break
		}
		if oversize {
			if !terminated {
				incompleteTail = true
				break
			}
			parseErrors++
			continue
		}
		trimmed := strings.TrimSpace(string(lineBytes))
		if trimmed == "" {
			continue
		}
		var entry ReplayEntry
		if err := json.Unmarshal([]byte(trimmed), &entry); err != nil {
			if !terminated {
				incompleteTail = true
				break
			}
			parseErrors++
			continue
		}
		if slotFrom != nil && entry.Slot < *slotFrom {
			continue
		}
		if slotTo != nil && entry.Slot > *slotTo {
			continue
		}
		totalMatched++
		// Keep the newest maxEntries while counting every match.
		if maxEntries > 0 {
			ring[(totalMatched-1)%maxEntries] = entry
		}
	}
	retained := min(totalMatched, maxEntries)
	window := make([]ReplayEntry, 0, retained)
	start := 0
	if totalMatched > maxEntries && maxEntries > 0 {
		start = totalMatched % maxEntries
	}
	for i := 0; i < retained; i++ {
		window = append(window, ring[(start+i)%maxEntries])
	}
	chosen := timingField
	if chosen == "" {
		chosen = defaultTimingField
	}
	stats := computeReplayStats(window, chosen)
	scannedBytes, err := section.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, ReplayStats{}, ReplayMeta{}, err
	}
	if scannedBytes < scanBytes {
		sourceChanged = true
	}
	currentInfo, err := f.Stat()
	if err != nil {
		return nil, ReplayStats{}, ReplayMeta{}, err
	}
	if currentInfo.Size() < info.Size() ||
		(currentInfo.Size() == info.Size() && fileMetadataChanged(info, currentInfo)) {
		sourceChanged = true
	}
	meta := ReplayMeta{
		TotalMatched:            totalMatched,
		Returned:                len(window),
		Truncated:               totalMatched > len(window),
		ParseErrors:             parseErrors,
		SourceLayout:            layout,
		ModifiedAt:              info.ModTime().UTC().Format(time.RFC3339Nano),
		SourceSizeBytes:         info.Size(),
		ScannedBytes:            scannedBytes,
		PartialSource:           startOffset > 0,
		IncompleteTail:          incompleteTail,
		SourceChangedDuringScan: sourceChanged,
		resolvedPath:            resolved,
	}
	if meta.SourceChangedDuringScan {
		meta.CoverageNote = "source changed during scan; returned replay evidence may be incomplete"
	} else if meta.PartialSource {
		meta.CoverageNote = "results cover only the newest 8 MiB; older replay entries were not scanned"
		if totalMatched == 0 && (slotFrom != nil || slotTo != nil) {
			meta.CoverageNote = "no matching slots were found in the newest 8 MiB; requested slots may exist before the scanned window"
		}
	}
	return window, stats, meta, nil
}

func computeReplayStats(entries []ReplayEntry, timingField string) ReplayStats {
	times := make([]float64, 0, len(entries))
	shapeIncomplete := 0
	measurement := "recorded_timing_field"
	caveat := ""
	if timingField == timingBlockTotal {
		measurement = "phase_sum_estimate"
		caveat = "phase sum, not wall-clock latency; complete fields do not prove every phase ran, and asynchronous timing may be assigned to another slot"
	}
	for i := range entries {
		if timingField == timingBlockTotal {
			if v, ok := entries[i].blockTotalMs(); ok {
				times = append(times, v)
			} else {
				shapeIncomplete++
			}
		} else if t, ok := entries[i].get(timingField); ok {
			times = append(times, t.totalMs())
		} else {
			shapeIncomplete++
		}
	}
	if len(times) == 0 {
		return ReplayStats{TimingField: timingField, Measurement: measurement, Caveat: caveat, FieldFound: false, ShapeIncompleteCount: shapeIncomplete}
	}
	sort.Float64s(times)
	sum := 0.0
	for _, v := range times {
		sum += v
	}
	return ReplayStats{
		TimingField:          timingField,
		Measurement:          measurement,
		Caveat:               caveat,
		FieldFound:           true,
		Count:                len(times),
		P50Ms:                medianSorted(times),
		P95Ms:                percentileSorted(times, 0.95),
		P99Ms:                percentileSorted(times, 0.99),
		MeanMs:               sum / float64(len(times)),
		MinMs:                times[0],
		MaxMs:                times[len(times)-1],
		ShapeIncompleteCount: shapeIncomplete,
	}
}

// medianSorted averages the middle values for even input.
func medianSorted(sorted []float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2.0
}

// percentileSorted is nearest-rank (for p95/p99 where averaging isn't meaningful).
func percentileSorted(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[idx]
}

type replayInput struct {
	SlotFrom    *uint64 `json:"slot_from,omitempty" jsonschema:"only include scanned slots >= this; long-running files scan only the newest 8 MiB"`
	SlotTo      *uint64 `json:"slot_to,omitempty" jsonschema:"only include scanned slots <= this; long-running files scan only the newest 8 MiB"`
	LastN       *int    `json:"last_n,omitempty" jsonschema:"keep only the most-recent N matching entries (default 1000, max 10000)"`
	TimingField string  `json:"timing_field,omitempty" jsonschema:"which timing field to compute stats on (default TxLoop; 'block_total' aggregates block-level steps)"`
}

type replayOutput struct {
	ReplayPath       string        `json:"replay_path"`
	Stats            ReplayStats   `json:"stats"`
	Meta             ReplayMeta    `json:"meta"`
	EntryCount       int           `json:"entry_count"`
	DisplayTruncated bool          `json:"display_truncated"`
	Entries          []ReplayEntry `json:"entries"`
}

// truncateForDisplay bounds the entries payload so a large window doesn't
// produce a huge tool response: keep the first three and last three. Percentile
// statistics still cover the full retained window.
func truncateForDisplay(entries []ReplayEntry) (shown []ReplayEntry, truncated bool) {
	if len(entries) <= maxReplayDisplayEntries {
		return entries, false
	}
	half := maxReplayDisplayEntries / 2
	out := make([]ReplayEntry, 0, maxReplayDisplayEntries)
	out = append(out, entries[:half]...)
	out = append(out, entries[len(entries)-half:]...)
	return out, true
}

func registerReplayTools(server *mcpsdk.Server, cfg Config) {
	addTool(server, cfg, &mcpsdk.Tool{
		Name:         "mithril_read_replay_timings",
		Annotations:  annReadOnlyLocal,
		OutputSchema: dynamicObjectOutputSchema,
		Description:  "Summarize the newest 8 MiB of replay timings with p50, p95, p99, mean, min, and max. Slot filters apply only to that scanned window, so no match does not prove an older slot is absent. block_total is a phase sum, not wall-clock latency.",
		// Out is `any`: ReplayEntry's dynamic timing keys require the permissive
		// object schema above rather than strict struct inference.
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, in replayInput) (*mcpsdk.CallToolResult, any, error) {
		path, err := requireConfiguredPath(cfg.ReplayPath, "MITHRIL_REPLAY_PATH is not configured")
		if err != nil {
			return nil, nil, err
		}
		entries, stats, meta, err := readReplayTimingsContext(ctx, path, in.SlotFrom, in.SlotTo, in.LastN, in.TimingField)
		if err != nil {
			return nil, nil, err
		}
		shown, truncated := truncateForDisplay(entries)
		return nil, replayOutput{
			ReplayPath:       meta.resolvedPath,
			Stats:            stats,
			Meta:             meta,
			EntryCount:       len(entries),
			DisplayTruncated: truncated,
			Entries:          shown,
		}, nil
	})
}
