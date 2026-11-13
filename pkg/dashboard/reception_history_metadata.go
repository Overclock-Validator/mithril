package dashboard

import (
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"
)

// ReceptionSource is a historical receiver lifetime, encoded as a fixed tuple.
// Missing older descriptors remain unknown; no active receiver fills them in.
type ReceptionSource struct {
	Generation uint64
	StartedAt  time.Time
	Kind       string
	Retired    bool
}

func (s ReceptionSource) MarshalJSON() ([]byte, error) {
	return json.Marshal([4]any{strconv.FormatUint(s.Generation, 10), s.StartedAt, s.Kind, s.Retired})
}

func (s *ReceptionSource) UnmarshalJSON(body []byte) error {
	row, err := receptionTuple(body, 4)
	if err != nil {
		return err
	}
	var out ReceptionSource
	if receptionUint(row[0], &out.Generation) != nil || json.Unmarshal(row[1], &out.StartedAt) != nil ||
		json.Unmarshal(row[2], &out.Kind) != nil || json.Unmarshal(row[3], &out.Retired) != nil || !validReceptionSource(out) {
		return errors.New("source_invalid")
	}
	*s = out
	return nil
}

// ReceptionRange is an inclusive observed-slot range, not produced blocks or
// canonical history. Slot zero is valid and is not used as an unknown sentinel.
type ReceptionRange struct{ First, Last uint64 }

func (r ReceptionRange) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]string{strconv.FormatUint(r.First, 10), strconv.FormatUint(r.Last, 10)})
}

func (r *ReceptionRange) UnmarshalJSON(body []byte) error {
	row, err := receptionTuple(body, 2)
	if err != nil {
		return err
	}
	var out ReceptionRange
	if receptionUint(row[0], &out.First) != nil || receptionUint(row[1], &out.Last) != nil || out.Last < out.First {
		return errors.New("source_invalid")
	}
	*r = out
	return nil
}

// ReceptionMetadata is a construction-only body-free copy. Preparation does
// not retain its source/range slices, nor any native source owner or snapshot.
type ReceptionMetadata struct {
	Binding          ReceptionRecordingBinding
	Revision         uint64
	PublishedAt      time.Time
	StartupAtCapture ReceptionStartup
	Sources          []ReceptionSource
	Ranges           [3][]ReceptionRange
}

// ReceptionMetadataPage contains exactly one kind of typed row. Its JSON is
// revision/kind/page/rows; enclosing hashes live only in the manifest.
type ReceptionMetadataPage struct {
	Revision uint64
	Kind     string
	Page     int
	Sources  []ReceptionSource
	Ranges   []ReceptionRange
}

func (p ReceptionMetadataPage) MarshalJSON() ([]byte, error) {
	if validateReceptionMetadataPage(p) != nil {
		return nil, errors.New("source_invalid")
	}
	var rows any = p.Ranges
	if p.Kind == "sources" {
		rows = p.Sources
	}
	return json.Marshal(struct {
		Revision uint64 `json:"revision,string"`
		Kind     string `json:"kind"`
		Page     int    `json:"page"`
		Rows     any    `json:"rows"`
	}{p.Revision, p.Kind, p.Page, rows})
}

func (p *ReceptionMetadataPage) UnmarshalJSON(body []byte) error {
	var wire struct {
		Revision uint64          `json:"revision,string"`
		Kind     string          `json:"kind"`
		Page     int             `json:"page"`
		Rows     json.RawMessage `json:"rows"`
	}
	if len(body) > MaxReceptionMetadataPageBytes || json.Unmarshal(body, &wire) != nil {
		return errors.New("source_invalid")
	}
	out := ReceptionMetadataPage{Revision: wire.Revision, Kind: wire.Kind, Page: wire.Page}
	if wire.Kind == "sources" {
		if json.Unmarshal(wire.Rows, &out.Sources) != nil {
			return errors.New("source_invalid")
		}
	} else if json.Unmarshal(wire.Rows, &out.Ranges) != nil {
		return errors.New("source_invalid")
	}
	if validateReceptionMetadataPage(out) != nil || !receptionCanonical(body, out) {
		return errors.New("source_invalid")
	}
	*p = out
	return nil
}

// ReceptionMetadataPageDescriptor names one complete immutable metadata page.
type ReceptionMetadataPageDescriptor struct {
	Kind   string `json:"kind"`
	Page   int    `json:"page"`
	Count  int    `json:"count"`
	Digest string `json:"digest"`
	Bytes  uint64 `json:"bytes,string"`
}

// ReceptionMetadataManifest binds every page to one original source capture.
// Digest covers the canonical fields before Digest, avoiding a circular hash.
type ReceptionMetadataManifest struct {
	Binding          ReceptionRecordingBinding         `json:"binding"`
	Revision         uint64                            `json:"revision,string"`
	PublishedAt      time.Time                         `json:"published_at"`
	StartupAtCapture ReceptionStartup                  `json:"startup_at_capture"`
	Pages            []ReceptionMetadataPageDescriptor `json:"pages"`
	Digest           string                            `json:"digest"`
}

// PreparedReceptionMetadata holds only encoded metadata pages and their small
// detached manifest. It cannot pin native rows, chunks, bodies or a publication.
// It is one bundle, not a cache or TTL owner; no construction happens by default.
type PreparedReceptionMetadata struct {
	manifest ReceptionMetadataManifest
	body     string
	pages    []string
	bytes    int
}

// Manifest returns detached marker and descriptor values, never cache storage.
func (p *PreparedReceptionMetadata) Manifest() ReceptionMetadataManifest {
	if p == nil {
		return ReceptionMetadataManifest{}
	}
	m := p.manifest
	m.StartupAtCapture = cloneReceptionStartup(m.StartupAtCapture)
	m.Pages = slices.Clone(m.Pages)
	return m
}

// Body returns the immutable canonical manifest, not a source-history body.
func (p *PreparedReceptionMetadata) Body() string {
	if p == nil {
		return ""
	}
	return p.body
}

// EncodedBytes includes all page bytes and the manifest. Root preparation also
// checks its larger enclosing root plus all pages against the same total cap.
func (p *PreparedReceptionMetadata) EncodedBytes() int {
	if p == nil {
		return 0
	}
	return p.bytes
}

// Page returns one immutable string. It never exposes the page-cache slice.
func (p *PreparedReceptionMetadata) Page(kind string, page int) (string, bool) {
	if p != nil {
		for i, d := range p.manifest.Pages {
			if d.Kind == kind && d.Page == page {
				return p.pages[i], true
			}
		}
	}
	return "", false
}

// PrepareReceptionMetadata seals every bounded source/range row without a
// source call, clock, goroutine, partial truncation or chunk-body reference.
func PrepareReceptionMetadata(m ReceptionMetadata) (*PreparedReceptionMetadata, error) {
	if _, err := ReceptionRecordingID(m.Binding); err != nil || m.Revision == 0 || !validReplayDetailTime(m.PublishedAt) ||
		!validReceptionStartup(m.StartupAtCapture) || len(m.Sources) > maxReceptionSources || !validReceptionSources(m.Sources) {
		return nil, errors.New("source_invalid")
	}
	for _, ranges := range m.Ranges {
		if len(ranges) > maxReceptionRanges || !validReceptionRanges(ranges) {
			return nil, errors.New("source_invalid")
		}
	}
	p := &PreparedReceptionMetadata{manifest: ReceptionMetadataManifest{
		Binding: m.Binding, Revision: m.Revision, PublishedAt: m.PublishedAt,
		StartupAtCapture: cloneReceptionStartup(m.StartupAtCapture), Pages: make([]ReceptionMetadataPageDescriptor, 0, maxReceptionPages),
	}}
	for kindIndex, kind := range receptionMetadataKinds {
		count, perPage := len(m.Sources), 64
		if kindIndex > 0 {
			count, perPage = len(m.Ranges[kindIndex-1]), 128
		}
		for first, page := 0, 0; first < count; first, page = first+perPage, page+1 {
			last := min(first+perPage, count)
			value := ReceptionMetadataPage{Revision: m.Revision, Kind: kind, Page: page}
			if kindIndex == 0 {
				value.Sources = m.Sources[first:last]
			} else {
				value.Ranges = m.Ranges[kindIndex-1][first:last]
			}
			body, err := json.Marshal(value)
			if err != nil || len(body) > MaxReceptionMetadataPageBytes {
				return nil, errors.New("response_limit")
			}
			p.pages = append(p.pages, string(body))
			p.bytes += len(body)
			p.manifest.Pages = append(p.manifest.Pages, ReceptionMetadataPageDescriptor{Kind: kind, Page: page, Count: last - first, Digest: receptionDigest(body), Bytes: uint64(len(body))})
		}
	}
	digest, err := receptionMetadataDigest(p.manifest)
	if err != nil {
		return nil, err
	}
	p.manifest.Digest = digest
	body, err := json.Marshal(p.manifest)
	if err != nil || len(body) > MaxReceptionRootBytes || p.bytes+len(body) > MaxReceptionMetadataBytes {
		return nil, errors.New("response_limit")
	}
	p.body, p.bytes = string(body), p.bytes+len(body)
	return p, nil
}

func decodeReceptionMetadataManifest(body []byte) (ReceptionMetadataManifest, error) {
	var m ReceptionMetadataManifest
	if len(body) > MaxReceptionRootBytes || json.Unmarshal(body, &m) != nil ||
		validateReceptionMetadataManifest(m) != nil || !receptionCanonical(body, m) {
		return ReceptionMetadataManifest{}, errors.New("source_invalid")
	}
	return m, nil
}

// DecodeReceptionMetadataPage validates the exact requested page against an
// already validated manifest. A matching digest without that binding is not
// sufficient. Full-bundle decoding also checks ordering across page boundaries.
func DecodeReceptionMetadataPage(m ReceptionMetadataManifest, kind string, page int, body []byte) (ReceptionMetadataPage, error) {
	if len(body) > MaxReceptionMetadataPageBytes || validateReceptionMetadataManifest(m) != nil {
		return ReceptionMetadataPage{}, errors.New("source_invalid")
	}
	for _, d := range m.Pages {
		if d.Kind != kind || d.Page != page {
			continue
		}
		var out ReceptionMetadataPage
		if d.Bytes != uint64(len(body)) || d.Digest != receptionDigest(body) || json.Unmarshal(body, &out) != nil ||
			out.Revision != m.Revision || out.Kind != kind || out.Page != page || d.Count != len(out.Sources)+len(out.Ranges) {
			return ReceptionMetadataPage{}, errors.New("source_invalid")
		}
		return out, nil
	}
	return ReceptionMetadataPage{}, errors.New("source_invalid")
}

// receptionValidatedMetadataPage can only be constructed by the strict decoder
// below. The body is a detached immutable string; its two ordering scalars are
// from those exact validated rows. Neither native rows nor the input byte slice
// survive. The manifest digest and index prevent mixing otherwise valid pages.
type receptionValidatedMetadataPage struct {
	body           string
	first, last    uint64
	manifestDigest string
	index          int
}

func decodeReceptionMetadataPageForCache(m ReceptionMetadataManifest, index int, body []byte) (receptionValidatedMetadataPage, error) {
	if index < 0 || index >= len(m.Pages) {
		return receptionValidatedMetadataPage{}, errors.New("source_invalid")
	}
	d := m.Pages[index]
	page, err := DecodeReceptionMetadataPage(m, d.Kind, d.Page, body)
	if err != nil {
		return receptionValidatedMetadataPage{}, err
	}
	out := receptionValidatedMetadataPage{body: string(body), manifestDigest: m.Digest, index: index}
	if d.Kind == "sources" {
		out.first, out.last = page.Sources[0].Generation, page.Sources[len(page.Sources)-1].Generation
	} else {
		out.first, out.last = page.Ranges[0].First, page.Ranges[len(page.Ranges)-1].Last
	}
	return out, nil
}

// finishReceptionMetadata accepts only values from the strict page constructor,
// not raw pages or caller-supplied ordering summaries. It checks the remaining
// cross-page invariant once and preserves the original canonical bytes. The
// incremental collector must not reparse/re-encode all 52 pages in its last cycle.
func finishReceptionMetadata(body string, pages []receptionValidatedMetadataPage) (*PreparedReceptionMetadata, error) {
	m, err := decodeReceptionMetadataManifest([]byte(body))
	if err != nil || len(pages) != len(m.Pages) {
		return nil, errors.New("source_invalid")
	}
	p := &PreparedReceptionMetadata{manifest: m, body: body, pages: make([]string, len(pages)), bytes: len(body)}
	previousKind := ""
	var previousLast uint64
	for i, page := range pages {
		d := m.Pages[i]
		if page.body == "" || page.manifestDigest != m.Digest || page.index != i || page.first > page.last ||
			uint64(len(page.body)) != d.Bytes || receptionDigest([]byte(page.body)) != d.Digest {
			return nil, errors.New("source_invalid")
		}
		if d.Kind == previousKind && (page.first <= previousLast || d.Kind != "sources" && page.first-previousLast == 1) {
			return nil, errors.New("source_invalid")
		}
		previousKind, previousLast = d.Kind, page.last
		p.pages[i], p.bytes = page.body, p.bytes+len(page.body)
	}
	if p.bytes > MaxReceptionMetadataBytes {
		return nil, errors.New("response_limit")
	}
	return p, nil
}

func decodeReceptionMetadata(body []byte, pages [][]byte) (*PreparedReceptionMetadata, error) {
	m, err := decodeReceptionMetadataManifest(body)
	if err != nil || len(pages) != len(m.Pages) {
		return nil, errors.New("source_invalid")
	}
	var validated [maxReceptionPages]receptionValidatedMetadataPage
	for i := range pages {
		validated[i], err = decodeReceptionMetadataPageForCache(m, i, pages[i])
		if err != nil {
			return nil, err
		}
	}
	return finishReceptionMetadata(string(body), validated[:len(pages)])
}

var receptionMetadataKinds = [...]string{"sources", "tvu_ranges", "repair_ranges", "unmatched_ranges"}

func receptionKindIndex(kind string) int {
	for i, known := range receptionMetadataKinds {
		if kind == known {
			return i
		}
	}
	return -1
}

func validateReceptionMetadataPage(p ReceptionMetadataPage) error {
	kind := receptionKindIndex(p.Kind)
	if p.Revision == 0 || kind < 0 || p.Page < 0 {
		return errors.New("source_invalid")
	}
	if kind == 0 {
		if p.Page >= 4 || len(p.Sources) == 0 || len(p.Sources) > 64 || p.Ranges != nil || !validReceptionSources(p.Sources) {
			return errors.New("source_invalid")
		}
	} else if p.Page >= 16 || len(p.Ranges) == 0 || len(p.Ranges) > 128 || p.Sources != nil || !validReceptionRanges(p.Ranges) {
		return errors.New("source_invalid")
	}
	return nil
}

func validateReceptionMetadataManifest(m ReceptionMetadataManifest) error {
	if _, err := ReceptionRecordingID(m.Binding); err != nil || m.Revision == 0 || !validReplayDetailTime(m.PublishedAt) ||
		!validReceptionStartup(m.StartupAtCapture) || m.Pages == nil || len(m.Pages) > maxReceptionPages || !validHeldScheduleDigest(m.Digest) {
		return errors.New("source_invalid")
	}
	previousKind, previousPage, previousCount := -1, -1, 0
	for _, d := range m.Pages {
		kind := receptionKindIndex(d.Kind)
		perPage, pageLimit := 128, 16
		if kind == 0 {
			perPage, pageLimit = 64, 4
		}
		if kind < 0 || kind < previousKind || d.Page < 0 || d.Page >= pageLimit || d.Count < 1 || d.Count > perPage ||
			d.Bytes < 1 || d.Bytes > MaxReceptionMetadataPageBytes || !validHeldScheduleDigest(d.Digest) {
			return errors.New("source_invalid")
		}
		if kind == previousKind {
			if d.Page != previousPage+1 || previousCount != perPage {
				return errors.New("source_invalid")
			}
		} else if d.Page != 0 {
			return errors.New("source_invalid")
		}
		previousKind, previousPage, previousCount = kind, d.Page, d.Count
	}
	digest, err := receptionMetadataDigest(m)
	if err != nil || digest != m.Digest {
		return errors.New("source_invalid")
	}
	body, err := json.Marshal(m)
	if err != nil || len(body) > MaxReceptionRootBytes || len(body)+receptionPageBytes(m.Pages) > MaxReceptionMetadataBytes {
		return errors.New("response_limit")
	}
	return nil
}

func receptionMetadataDigest(m ReceptionMetadataManifest) (string, error) {
	body, err := json.Marshal(struct {
		Binding          ReceptionRecordingBinding         `json:"binding"`
		Revision         uint64                            `json:"revision,string"`
		PublishedAt      time.Time                         `json:"published_at"`
		StartupAtCapture ReceptionStartup                  `json:"startup_at_capture"`
		Pages            []ReceptionMetadataPageDescriptor `json:"pages"`
	}{m.Binding, m.Revision, m.PublishedAt, m.StartupAtCapture, m.Pages})
	if err != nil {
		return "", err
	}
	return receptionDigest(body), nil
}

func receptionPageBytes(pages []ReceptionMetadataPageDescriptor) int {
	total := 0
	for _, page := range pages {
		total += int(page.Bytes)
	}
	return total
}

func validReceptionSource(s ReceptionSource) bool {
	return s.Generation != 0 && validReplayDetailTime(s.StartedAt) && (s.Kind == "prewarm" || s.Kind == "live")
}

func validReceptionSources(sources []ReceptionSource) bool {
	for i, s := range sources {
		if !validReceptionSource(s) || i > 0 && s.Generation <= sources[i-1].Generation {
			return false
		}
	}
	return true
}

func validReceptionRanges(ranges []ReceptionRange) bool {
	for i, r := range ranges {
		if r.Last < r.First {
			return false
		}
		if i > 0 && (r.First <= ranges[i-1].Last || r.First-ranges[i-1].Last == 1) {
			return false
		}
	}
	return true
}
