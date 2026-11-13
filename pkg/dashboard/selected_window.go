package dashboard

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

const (
	selectedWindowCapacity = 256
	selectedWindowPageSize = 8
	selectedWindowMaxBytes = 1 << 20
)

// SelectedWindow is one replay attempt's bounded selected-and-folded source
// projection. It is neither an archive nor a complete epoch.
type SelectedWindow struct {
	SchemaVersion      int                    `json:"schema_version"`
	Family             string                 `json:"family"`
	Scope              string                 `json:"scope"`
	Binding            ReplayDetailBinding    `json:"binding"`
	CapturedAt         time.Time              `json:"captured_at"`
	ObservedAt         time.Time              `json:"observed_at"`
	WindowEntries      uint64                 `json:"window_entries,string"`
	CoverageIncomplete bool                   `json:"coverage_incomplete"`
	Records            []ReplaySelectedRecord `json:"records"`
}

type selectedWindowManifest struct {
	SchemaVersion      int                 `json:"schema_version"`
	Family             string              `json:"family"`
	Scope              string              `json:"scope"`
	Binding            ReplayDetailBinding `json:"binding"`
	CapturedAt         time.Time           `json:"captured_at"`
	ObservedAt         time.Time           `json:"observed_at"`
	WindowEntries      uint64              `json:"window_entries,string"`
	CoverageIncomplete bool                `json:"coverage_incomplete"`
	Count              int                 `json:"count"`
	PageSize           int                 `json:"page_size"`
	PageCount          int                 `json:"page_count"`
	Digest             string              `json:"digest"`
}

type selectedWindowPage struct {
	SchemaVersion int                    `json:"schema_version"`
	Family        string                 `json:"family"`
	Digest        string                 `json:"digest"`
	Binding       ReplayDetailBinding    `json:"binding"`
	Page          int                    `json:"page"`
	Offset        int                    `json:"offset"`
	Records       []ReplaySelectedRecord `json:"records"`
}

// preparedSelectedWindow retains encoded bytes only; it never holds the
// caller's mutable records or marshals data in a viewer request.
type preparedSelectedWindow struct {
	manifest string
	digest   string
	pages    []string
	bytes    int
}

func validSelectedWindow(w SelectedWindow) bool {
	if w.SchemaVersion != 1 || w.Family != "selected_slots" || w.Scope != "recent_selected_and_folded" ||
		!validReplayDetailBinding(w.Binding) || !validReplayDetailTime(w.CapturedAt) || !validReplayDetailTime(w.ObservedAt) ||
		w.WindowEntries > 512 || w.Records == nil || len(w.Records) != int(min(w.WindowEntries, selectedWindowCapacity)) {
		return false
	}
	for i, row := range w.Records {
		if !validReplayLatestSelected(&ReplayLatestSelected{Scope: "latest_selected_and_folded", ObservedAt: w.ObservedAt,
			Slot: row.Slot, Bankhash: row.Bankhash, BlockID: row.BlockID, Gap: row.Gap, Facts: row.Facts}) ||
			row.Gap && !w.CoverageIncomplete || i > 0 && row.Slot <= w.Records[i-1].Slot {
			return false
		}
	}
	return true
}

func prepareSelectedWindow(w SelectedWindow) (*preparedSelectedWindow, error) {
	if !validSelectedWindow(w) {
		return nil, errors.New("source_invalid")
	}
	data, err := json.Marshal(w)
	if err != nil || len(data) > selectedWindowMaxBytes {
		return nil, errors.New("response_limit")
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	count := len(w.Records)
	pageCount := (count + selectedWindowPageSize - 1) / selectedWindowPageSize
	m := selectedWindowManifest{SchemaVersion: 1, Family: w.Family, Scope: w.Scope, Binding: w.Binding,
		CapturedAt: w.CapturedAt, ObservedAt: w.ObservedAt, WindowEntries: w.WindowEntries,
		CoverageIncomplete: w.CoverageIncomplete, Count: count, PageSize: selectedWindowPageSize, PageCount: pageCount, Digest: digest}
	manifest, err := json.Marshal(m)
	if err != nil || len(manifest) > MaxSnapshotBytes {
		return nil, errors.New("response_limit")
	}
	p := &preparedSelectedWindow{manifest: string(manifest), digest: digest, pages: make([]string, pageCount), bytes: len(manifest)}
	for page := range pageCount {
		offset := page * selectedWindowPageSize
		body, err := json.Marshal(selectedWindowPage{SchemaVersion: 1, Family: w.Family, Digest: digest,
			Binding: w.Binding, Page: page, Offset: offset, Records: w.Records[offset:min(offset+selectedWindowPageSize, count)]})
		if err != nil || len(body) > MaxSnapshotBytes || p.bytes+len(body) > selectedWindowMaxBytes {
			return nil, errors.New("response_limit")
		}
		p.pages[page], p.bytes = string(body), p.bytes+len(body)
	}
	return p, nil
}

func decodeSelectedWindowManifest(manifest []byte) (selectedWindowManifest, error) {
	var m selectedWindowManifest
	if len(manifest) == 0 || len(manifest) > MaxSnapshotBytes || json.Unmarshal(manifest, &m) != nil ||
		m.SchemaVersion != 1 || m.Family != "selected_slots" || m.Scope != "recent_selected_and_folded" ||
		!validReplayDetailBinding(m.Binding) || !validReplayDetailTime(m.CapturedAt) || !validReplayDetailTime(m.ObservedAt) ||
		m.WindowEntries > 512 || m.Count != int(min(m.WindowEntries, selectedWindowCapacity)) ||
		m.PageSize != selectedWindowPageSize || m.PageCount != (m.Count+selectedWindowPageSize-1)/selectedWindowPageSize ||
		len(m.Digest) != sha256.Size*2 {
		return selectedWindowManifest{}, errors.New("source_invalid")
	}
	if digest, err := hex.DecodeString(m.Digest); err != nil || hex.EncodeToString(digest) != m.Digest {
		return selectedWindowManifest{}, errors.New("source_invalid")
	}
	canonical, err := json.Marshal(m)
	if err != nil || !bytes.Equal(canonical, manifest) {
		return selectedWindowManifest{}, errors.New("source_invalid")
	}
	return m, nil
}

// decodeSelectedWindow accepts only a complete canonical generation. Transport
// freshness and the current status gate must be checked separately.
func decodeSelectedWindow(manifest []byte, pages [][]byte) (SelectedWindow, error) {
	m, err := decodeSelectedWindowManifest(manifest)
	if err != nil || len(pages) != m.PageCount {
		return SelectedWindow{}, errors.New("source_invalid")
	}
	w := SelectedWindow{SchemaVersion: 1, Family: m.Family, Scope: m.Scope, Binding: m.Binding,
		CapturedAt: m.CapturedAt, ObservedAt: m.ObservedAt, WindowEntries: m.WindowEntries,
		CoverageIncomplete: m.CoverageIncomplete, Records: make([]ReplaySelectedRecord, 0, m.Count)}
	total := len(manifest)
	for i, body := range pages {
		total += len(body)
		var page selectedWindowPage
		if len(body) == 0 || len(body) > MaxSnapshotBytes || total > selectedWindowMaxBytes || json.Unmarshal(body, &page) != nil ||
			page.SchemaVersion != 1 || page.Family != m.Family || page.Digest != m.Digest || !sameReplayDetailBinding(page.Binding, m.Binding) ||
			page.Page != i || page.Offset != i*selectedWindowPageSize || page.Records == nil ||
			len(page.Records) != min(selectedWindowPageSize, m.Count-page.Offset) {
			return SelectedWindow{}, errors.New("source_invalid")
		}
		canonical, err := json.Marshal(page)
		if err != nil || !bytes.Equal(canonical, body) {
			return SelectedWindow{}, errors.New("source_invalid")
		}
		w.Records = append(w.Records, page.Records...)
	}
	if !validSelectedWindow(w) {
		return SelectedWindow{}, errors.New("source_invalid")
	}
	data, err := json.Marshal(w)
	if err != nil || len(data) > selectedWindowMaxBytes {
		return SelectedWindow{}, errors.New("source_invalid")
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != m.Digest {
		return SelectedWindow{}, errors.New("source_invalid")
	}
	return w, nil
}
