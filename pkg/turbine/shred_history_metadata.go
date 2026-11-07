package turbine

import (
	"slices"
	"time"
)

// ShredHistoryChunkDescriptor identifies one immutable encoded chunk without
// retaining its body or native rows. MinAt and MaxAt are original UTC wall
// bounds; they do not order the consumer's sequence or measure elapsed time.
type ShredHistoryChunkDescriptor struct {
	FirstSequence uint64
	LastSequence  uint64
	Count         uint64
	MinAt         time.Time
	MaxAt         time.Time
	Digest        [32]byte
	Bytes         uint64
}

// ShredHistoryMetadata is a body-free view of one consumer publication.
// PublishedAt preserves the original monotonic clock. Loss counters remain
// run-wide observations, not exact losses before the startup cutoff.
type ShredHistoryMetadata struct {
	Binding             ShredHistoryBinding
	PublishedAt         time.Time
	Chunks              []ShredHistoryChunkDescriptor
	Sources             []ShredHistorySource
	StartupRanges       [3][]ShredHistoryRange
	Marker              *ShredHistoryMarker
	MarkerState         ShredHistoryMarkerState
	PrefixCutoff        uint64
	LastSequence        uint64
	QueueRefusals       ShredHistoryCounter
	InvalidInputs       ShredHistoryCounter
	RangeRefusals       [3]ShredHistoryCounter
	DescriptorEvictions ShredHistoryCounter
	EvictedChunks       ShredHistoryCounter
	EvictedRows         ShredHistoryCounter
	ChunkRefusals       ShredHistoryCounter
	StartupLimited      bool
	Interrupted         bool
	Exhausted           bool
	Closed              bool
}

// shredHistoryMetadata runs only on the consumer. Sources, ranges and marker
// share the corresponding immutable snapshot buffers internally; no reference
// to that snapshot or any chunk/row/body is retained by this publication.
func shredHistoryMetadata(s *ShredHistorySnapshot) *ShredHistoryMetadata {
	m := &ShredHistoryMetadata{
		Binding: s.Binding, PublishedAt: s.PublishedAt,
		Chunks:  make([]ShredHistoryChunkDescriptor, len(s.Chunks)),
		Sources: s.Sources, StartupRanges: s.StartupRanges, Marker: s.Marker,
		PrefixCutoff: s.PrefixCutoff, LastSequence: s.LastSequence,
		RangeRefusals: s.RangeRefusals, DescriptorEvictions: s.DescriptorEvictions,
		EvictedChunks: s.EvictedChunks, EvictedRows: s.EvictedRows, ChunkRefusals: s.ChunkRefusals,
		StartupLimited: s.StartupLimited, Closed: s.Closed,
	}
	for i, c := range s.Chunks {
		m.Chunks[i] = ShredHistoryChunkDescriptor{
			FirstSequence: c.FirstSequence, LastSequence: c.LastSequence, Count: uint64(len(c.rows)),
			MinAt: c.MinAt, MaxAt: c.MaxAt, Digest: c.digest, Bytes: uint64(len(c.body)),
		}
	}
	return m
}

// Metadata copies only bounded typed metadata. It never calls Snapshot or
// Rows, so retaining its result cannot pin an old complete chunk generation.
// Copy-out holds replacement briefly so it cannot pin extra internal source
// and range buffers across successive publications.
func (h *ShredHistory) Metadata() (ShredHistoryMetadata, bool) {
	if h == nil {
		return ShredHistoryMetadata{}, false
	}
	h.publicationMu.RLock()
	defer h.publicationMu.RUnlock()
	view := h.metadata.Load()
	if view == nil {
		return ShredHistoryMetadata{}, false
	}
	m := *view
	m.Chunks = slices.Clone(m.Chunks)
	m.Sources = slices.Clone(m.Sources)
	for i := range m.StartupRanges {
		m.StartupRanges[i] = slices.Clone(m.StartupRanges[i])
	}
	if m.Marker != nil {
		marker := *m.Marker
		m.Marker = &marker
	}
	// Match Snapshot's out-of-band overlays without loading its full publication.
	m.QueueRefusals = shredHistoryCounter(h.queueRefusals.Load())
	m.InvalidInputs = shredHistoryCounter(h.invalidInputs.Load())
	m.MarkerState = ShredHistoryMarkerState(h.markerState.Load())
	if m.MarkerState == ShredHistoryMarkerClosed && m.Marker == nil {
		m.MarkerState = ShredHistoryMarkerQueued
	}
	m.Interrupted = h.interrupted.Load()
	m.Exhausted = h.exhausted.Load()
	m.StartupLimited = m.StartupLimited || m.QueueRefusals.Value > 0 || m.InvalidInputs.Value > 0 || m.Interrupted || m.Exhausted || m.MarkerState == ShredHistoryMarkerUnknown
	return m, true
}

// ChunkBody returns only one exact currently published immutable body. The
// bounded scan does no clock, hashing, encoding, receiver or database work.
// Replacement waits until the temporary full-publication reference is released;
// only the returned string can outlive this lock, never the chunk or snapshot.
func (h *ShredHistory) ChunkBody(first, last uint64, digest [32]byte) (string, bool) {
	if h == nil {
		return "", false
	}
	h.publicationMu.RLock()
	defer h.publicationMu.RUnlock()
	view := h.publication.Load()
	if view != nil {
		for _, c := range view.Chunks {
			if c.FirstSequence == first && c.LastSequence == last && c.digest == digest {
				return c.body, true
			}
		}
	}
	return "", false
}
