package dashboard

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"
	"unicode/utf8"
)

const (
	// MaxReceptionRootBytes bounds roots and incremental chunk indexes separately.
	MaxReceptionRootBytes = 16 << 10
	// MaxReceptionChunkBytes preserves the existing source chunk byte ceiling.
	MaxReceptionChunkBytes = 64 << 10
	// MaxReceptionMetadataPageBytes bounds one sources or ranges page.
	MaxReceptionMetadataPageBytes = 16 << 10
	// MaxReceptionMetadataBytes includes all pages and their enclosing root.
	MaxReceptionMetadataBytes = 512 << 10
	maxReceptionChunks        = 256
	maxReceptionSources       = 256
	maxReceptionRanges        = 2048
	maxReceptionPages         = 52
)

// ReceptionRecordingBinding identifies a recorder, not a receiver session or
// replay attempt. Generation belongs to the node and must never wrap.
type ReceptionRecordingBinding struct {
	RunID               string    `json:"run_id"`
	NodeStartedAt       time.Time `json:"node_started_at"`
	GenesisHash         string    `json:"genesis_hash"`
	RecordingGeneration uint64    `json:"recording_generation,string"`
}

// ReceptionRecordingID hashes the complete canonical fixed binding once.
func ReceptionRecordingID(b ReceptionRecordingBinding) (string, error) {
	if b.RunID == "" || len(b.RunID) > 128 || !utf8.ValidString(b.RunID) ||
		!validReplayDetailTime(b.NodeStartedAt) || !validReplayDetailHash(b.GenesisHash, true) || b.RecordingGeneration == 0 {
		return "", errors.New("source_invalid")
	}
	body, err := json.Marshal(b)
	if err != nil {
		return "", err
	}
	return receptionDigest(body), nil
}

// ReceptionSequenceBounds uses paired nulls for an empty retained set. Sequence
// zero is only a request cursor, never an invented retained occurrence.
type ReceptionSequenceBounds struct {
	First *uint64 `json:"first,string"`
	Last  *uint64 `json:"last,string"`
}

// ReceptionCounter preserves the source's saturation witness.
type ReceptionCounter struct {
	Value     uint64 `json:"value,string"`
	Saturated bool   `json:"saturated"`
}

// ReceptionLosses are run-wide source observations, not exact losses before a
// startup marker and not transport-cache gaps or lost network packets.
type ReceptionLosses struct {
	QueueRefusals       ReceptionCounter    `json:"queue_refusals"`
	InvalidInputs       ReceptionCounter    `json:"invalid_inputs"`
	RangeRefusals       [3]ReceptionCounter `json:"range_refusals"`
	DescriptorEvictions ReceptionCounter    `json:"descriptor_evictions"`
	EvictedChunks       ReceptionCounter    `json:"evicted_chunks"`
	EvictedRows         ReceptionCounter    `json:"evicted_rows"`
	ChunkRefusals       ReceptionCounter    `json:"chunk_refusals"`
}

// ReceptionMarker is a historical replay-near-input observation. SourceGeneration
// is its original receiver handle, not proof that receiver supplied the bank.
type ReceptionMarker struct {
	At               time.Time `json:"at"`
	SourceGeneration uint64    `json:"source_generation,string"`
	Slot             uint64    `json:"slot,string"`
	Bankhash         string    `json:"bankhash"`
	ReplayAttempt    uint64    `json:"replay_attempt,string"`
	Rewinds          uint64    `json:"rewinds,string"`
	CompletedIndex   uint64    `json:"completed_index,string"`
}

// ReceptionStartup freezes prefix and loss facts together. A later root marker
// must not relabel pages whose StartupAtCapture was still open or queued.
type ReceptionStartup struct {
	State        string           `json:"state"`
	PrefixCutoff uint64           `json:"prefix_cutoff,string"`
	Marker       *ReceptionMarker `json:"marker"`
	Limited      bool             `json:"limited"`
	Losses       ReceptionLosses  `json:"losses"`
}

// ReceptionChunkDescriptor is a body-free seven-column tuple. The digest is
// over the original source bytes; this layer never re-encodes those chunks.
type ReceptionChunkDescriptor struct {
	FirstSequence uint64
	LastSequence  uint64
	Count         uint64
	MinAt         time.Time
	MaxAt         time.Time
	Digest        string
	Bytes         uint64
}

func (d ReceptionChunkDescriptor) MarshalJSON() ([]byte, error) {
	return json.Marshal([7]any{strconv.FormatUint(d.FirstSequence, 10), strconv.FormatUint(d.LastSequence, 10),
		strconv.FormatUint(d.Count, 10), d.MinAt, d.MaxAt, d.Digest, strconv.FormatUint(d.Bytes, 10)})
}

func (d *ReceptionChunkDescriptor) UnmarshalJSON(body []byte) error {
	row, err := receptionTuple(body, 7)
	if err != nil {
		return err
	}
	var out ReceptionChunkDescriptor
	for i, target := range []*uint64{&out.FirstSequence, &out.LastSequence, &out.Count} {
		if err := receptionUint(row[i], target); err != nil {
			return err
		}
	}
	if json.Unmarshal(row[3], &out.MinAt) != nil || json.Unmarshal(row[4], &out.MaxAt) != nil ||
		json.Unmarshal(row[5], &out.Digest) != nil || receptionUint(row[6], &out.Bytes) != nil || !validReceptionChunk(out) {
		return errors.New("source_invalid")
	}
	*d = out
	return nil
}

// ReceptionHistoryRoot contains only scalar facts and a frozen metadata
// manifest. State is transport liveness, not the age of retained occurrences.
// No wall-date subtraction in this codec creates or renews a live lease.
type ReceptionHistoryRoot struct {
	SchemaVersion   int                       `json:"schema_version"`
	Family          string                    `json:"family"`
	Scope           string                    `json:"scope"`
	Binding         ReceptionRecordingBinding `json:"binding"`
	RecordingID     string                    `json:"recording_id"`
	State           string                    `json:"state"`
	PublishedAt     time.Time                 `json:"published_at"`
	ConsumedThrough uint64                    `json:"consumed_through,string"`
	SourceBounds    ReceptionSequenceBounds   `json:"source_bounds"`
	CacheBounds     ReceptionSequenceBounds   `json:"cache_bounds"`
	Closed          bool                      `json:"closed"`
	Interrupted     bool                      `json:"interrupted"`
	Exhausted       bool                      `json:"exhausted"`
	Startup         ReceptionStartup          `json:"startup"`
	Metadata        ReceptionMetadataManifest `json:"metadata"`
}

// PrepareReceptionHistoryRoot validates and encodes a cache-only root. The
// caller owns real monotonic liveness and must select its state accordingly.
func PrepareReceptionHistoryRoot(r ReceptionHistoryRoot) (string, error) {
	id, err := ReceptionRecordingID(r.Binding)
	metadataID, metadataErr := ReceptionRecordingID(r.Metadata.Binding)
	if err != nil || r.SchemaVersion != 1 || r.Family != "reception" || r.Scope != "authenticated_socket_occurrences" ||
		r.RecordingID != id || !validReplayDetailTime(r.PublishedAt) || !validReceptionBounds(r.SourceBounds, r.ConsumedThrough) ||
		!validReceptionBounds(r.CacheBounds, r.ConsumedThrough) || !validReceptionStartup(r.Startup) || r.Startup.PrefixCutoff > r.ConsumedThrough ||
		validateReceptionMetadataManifest(r.Metadata) != nil || metadataErr != nil || metadataID != id || r.Metadata.StartupAtCapture.PrefixCutoff > r.ConsumedThrough ||
		!receptionStartupCompatible(r.Metadata.StartupAtCapture, r.Startup) {
		return "", errors.New("source_invalid")
	}
	switch r.State {
	case "fresh", "stale":
		if r.Closed || r.Interrupted || r.Exhausted {
			return "", errors.New("source_invalid")
		}
	case "closed":
		if !r.Closed || r.Interrupted || r.Exhausted {
			return "", errors.New("source_invalid")
		}
	case "interrupted":
		if !r.Interrupted || r.Exhausted {
			return "", errors.New("source_invalid")
		}
	case "exhausted":
		if !r.Exhausted {
			return "", errors.New("source_invalid")
		}
	default:
		return "", errors.New("source_invalid")
	}
	if (r.Interrupted || r.Exhausted) && !r.Startup.Limited {
		return "", errors.New("source_invalid")
	}
	body, err := json.Marshal(r)
	if err != nil || len(body) > MaxReceptionRootBytes || len(body)+receptionPageBytes(r.Metadata.Pages) > MaxReceptionMetadataBytes {
		return "", errors.New("response_limit")
	}
	return string(body), nil
}

func decodeReceptionHistoryRoot(body []byte) (ReceptionHistoryRoot, error) {
	var r ReceptionHistoryRoot
	if len(body) > MaxReceptionRootBytes || json.Unmarshal(body, &r) != nil {
		return r, errors.New("source_invalid")
	}
	canonical, err := PrepareReceptionHistoryRoot(r)
	if err != nil || canonical != string(body) {
		return ReceptionHistoryRoot{}, errors.New("source_invalid")
	}
	return r, nil
}

// ReceptionHistoryIndex is one bounded cache index response, not a complete
// source inventory. A descriptor containing After is legal; its later rows
// still need fetching. Interior availability gaps are not inferred as loss.
type ReceptionHistoryIndex struct {
	RecordingID string                     `json:"recording_id"`
	After       uint64                     `json:"after,string"`
	CacheBounds ReceptionSequenceBounds    `json:"cache_bounds"`
	Chunks      []ReceptionChunkDescriptor `json:"chunks"`
}

// PrepareReceptionHistoryIndex encodes at most eight detached descriptors.
func PrepareReceptionHistoryIndex(v ReceptionHistoryIndex) (string, error) {
	if !validHeldScheduleDigest(v.RecordingID) || !validReceptionBounds(v.CacheBounds, math.MaxUint64) ||
		v.Chunks == nil || len(v.Chunks) > 8 || validateReceptionChunks(v.Chunks) != nil {
		return "", errors.New("source_invalid")
	}
	for _, c := range v.Chunks {
		if v.CacheBounds.First == nil || c.FirstSequence < *v.CacheBounds.First || c.LastSequence > *v.CacheBounds.Last || c.LastSequence <= v.After {
			return "", errors.New("source_invalid")
		}
	}
	body, err := json.Marshal(v)
	if err != nil || len(body) > MaxReceptionRootBytes {
		return "", errors.New("response_limit")
	}
	return string(body), nil
}

func decodeReceptionHistoryIndex(body []byte) (ReceptionHistoryIndex, error) {
	var v ReceptionHistoryIndex
	if len(body) > MaxReceptionRootBytes || json.Unmarshal(body, &v) != nil {
		return v, errors.New("source_invalid")
	}
	canonical, err := PrepareReceptionHistoryIndex(v)
	if err != nil || canonical != string(body) {
		return ReceptionHistoryIndex{}, errors.New("source_invalid")
	}
	return v, nil
}

// ValidateReceptionChunkDescriptors checks all retained descriptors without
// reading, copying, decoding or retaining any chunk body.
func ValidateReceptionChunkDescriptors(chunks []ReceptionChunkDescriptor) error {
	return validateReceptionChunks(chunks)
}

func validateReceptionChunks(chunks []ReceptionChunkDescriptor) error {
	if len(chunks) > maxReceptionChunks {
		return errors.New("response_limit")
	}
	for i, c := range chunks {
		if !validReceptionChunk(c) || i > 0 && c.FirstSequence <= chunks[i-1].LastSequence {
			return errors.New("source_invalid")
		}
	}
	return nil
}

func validReceptionChunk(c ReceptionChunkDescriptor) bool {
	return c.FirstSequence != 0 && c.LastSequence >= c.FirstSequence && c.Count >= 1 && c.Count <= 128 &&
		c.LastSequence-c.FirstSequence == c.Count-1 && validReplayDetailTime(c.MinAt) && validReplayDetailTime(c.MaxAt) &&
		!c.MaxAt.UTC().Before(c.MinAt.UTC()) && validHeldScheduleDigest(c.Digest) && c.Bytes > 0 && c.Bytes <= MaxReceptionChunkBytes
}

func validReceptionBounds(b ReceptionSequenceBounds, through uint64) bool {
	if b.First == nil || b.Last == nil {
		return b.First == nil && b.Last == nil
	}
	return *b.First != 0 && *b.Last >= *b.First && *b.Last <= through
}

func validReceptionStartup(s ReceptionStartup) bool {
	if s.State == "closed" {
		m := s.Marker
		if m == nil || !validReplayDetailTime(m.At) || m.SourceGeneration == 0 || m.ReplayAttempt == 0 ||
			m.CompletedIndex == 0 || !validReplayDetailHash(m.Bankhash, true) {
			return false
		}
	} else if (s.State != "open" && s.State != "queued" && s.State != "unknown") || s.Marker != nil || s.PrefixCutoff != 0 {
		return false
	}
	limited := s.State == "unknown"
	for _, c := range receptionLossCounters(s.Losses) {
		if c.Saturated != (c.Value == math.MaxUint64) {
			return false
		}
		limited = limited || c.Value != 0
	}
	return !limited || s.Limited
}

// receptionStartupCompatible permits a newer root beside older frozen pages,
// but cannot revive a terminal marker or retract sticky run-wide loss facts.
// Dates are original wall instants, not a means to order these observations.
func receptionStartupCompatible(captured, current ReceptionStartup) bool {
	if captured.Limited && !current.Limited {
		return false
	}
	switch captured.State {
	case "queued":
		if current.State == "open" {
			return false
		}
	case "unknown":
		if current.State != "unknown" {
			return false
		}
	case "closed":
		if current.State != "closed" || captured.PrefixCutoff != current.PrefixCutoff || captured.Marker == nil || current.Marker == nil {
			return false
		}
		a, b := *captured.Marker, *current.Marker
		if !a.At.UTC().Equal(b.At.UTC()) {
			return false
		}
		a.At, b.At = time.Time{}, time.Time{}
		if a != b {
			return false
		}
	}
	previous, latest := receptionLossCounters(captured.Losses), receptionLossCounters(current.Losses)
	for i := range previous {
		if latest[i].Value < previous[i].Value || previous[i].Saturated && !latest[i].Saturated {
			return false
		}
	}
	return true
}

func receptionLossCounters(v ReceptionLosses) [9]ReceptionCounter {
	return [9]ReceptionCounter{v.QueueRefusals, v.InvalidInputs, v.RangeRefusals[0], v.RangeRefusals[1], v.RangeRefusals[2],
		v.DescriptorEvictions, v.EvictedChunks, v.EvictedRows, v.ChunkRefusals}
}

func cloneReceptionStartup(s ReceptionStartup) ReceptionStartup {
	if s.Marker != nil {
		m := *s.Marker
		s.Marker = &m
	}
	return s
}

func receptionDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func receptionTuple(body []byte, count int) ([]json.RawMessage, error) {
	var row []json.RawMessage
	if json.Unmarshal(body, &row) != nil || len(row) != count {
		return nil, errors.New("source_invalid")
	}
	return row, nil
}

func receptionUint(body []byte, out *uint64) error {
	var text string
	if json.Unmarshal(body, &text) != nil {
		return errors.New("source_invalid")
	}
	v, err := strconv.ParseUint(text, 10, 64)
	if err != nil || strconv.FormatUint(v, 10) != text {
		return errors.New("source_invalid")
	}
	*out = v
	return nil
}

func receptionCanonical(body []byte, v any) bool {
	canonical, err := json.Marshal(v)
	return err == nil && bytes.Equal(body, canonical)
}
