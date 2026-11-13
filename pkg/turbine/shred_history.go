package turbine

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/gagliardetto/solana-go"
)

const (
	shredHistoryQueueCapacity = 4096
	shredHistoryChunkRows     = 128
	shredHistoryChunks        = 256
	shredHistoryRanges        = 2048
	shredHistorySources       = 256
	shredHistoryChunkBytes    = 64 << 10
	shredHistoryRetainedBytes = 16 << 20
	shredHistoryCadence       = 50 * time.Millisecond
)

// ShredHistoryOrigin identifies the admitted socket path, not uniqueness,
// timeliness, reconstruction, or acceptance of a canonical fork.
type ShredHistoryOrigin uint8

const (
	ShredHistoryTVU ShredHistoryOrigin = iota + 1
	ShredHistoryRepairMatched
	ShredHistoryRepairUnmatched
)

// ShredHistoryBinding identifies one explicitly enabled node-run recorder.
type ShredHistoryBinding struct {
	RunID         string
	NodeStartedAt time.Time
	GenesisHash   solana.Hash
}

// ShredHistoryReceipt is a copied admitted occurrence. At is the original
// process-entry clock; Root is an authenticated FEC root, not a bankhash.
// The source handle supplies SourceGeneration; callers must leave it zero.
type ShredHistoryReceipt struct {
	At               time.Time
	Root             solana.Hash
	SourceGeneration uint64
	Slot             uint64
	Index            uint32
	FECSetIndex      uint32
	Version          uint16
	Variant          uint8
	Origin           ShredHistoryOrigin
}

// ShredHistoryRow adds consumer publication order, not NIC or wall-time order.
type ShredHistoryRow struct {
	ShredHistoryReceipt
	Sequence uint64
}

// ShredHistoryMarker is the original successful near-input completion context.
// SourceGeneration is supplied by its receiver handle, never resolved later.
type ShredHistoryMarker struct {
	At               time.Time
	Bankhash         solana.Hash
	SourceGeneration uint64
	Slot             uint64
	ReplayAttempt    uint64
	Rewinds          uint64
	CompletedIndex   uint64
}

// ShredHistoryMarkerState separates producer enqueue from consumer closure.
type ShredHistoryMarkerState uint32

const (
	ShredHistoryMarkerOpen ShredHistoryMarkerState = iota
	ShredHistoryMarkerQueued
	ShredHistoryMarkerClosed
	ShredHistoryMarkerUnknown
)

// ShredHistoryRange contains only observed adjacent slots, inclusive at both ends.
type ShredHistoryRange struct{ First, Last uint64 }

// ShredHistoryCounter reserves MaxUint64 as saturated, not an exact count.
type ShredHistoryCounter struct {
	Value     uint64
	Saturated bool
}

// ShredHistorySourceKind identifies the actual constructor phase, not a label
// inferred later from dates or the currently active receiver.
type ShredHistorySourceKind uint8

const (
	ShredHistoryPrewarm ShredHistorySourceKind = iota + 1
	ShredHistoryLive
)

// ShredHistorySource describes a bounded receiver lifetime. Retired does not
// erase queued receipts; absent old descriptors are separately counted.
type ShredHistorySource struct {
	Generation uint64
	StartedAt  time.Time
	Kind       ShredHistorySourceKind
	Retired    bool
}

// ShredHistoryChunk owns immutable rows and their prepared JSON representation.
// Its body is source-only; the transport binding/envelope is a later layer.
type ShredHistoryChunk struct {
	FirstSequence uint64
	LastSequence  uint64
	MinAt         time.Time
	MaxAt         time.Time
	rows          []ShredHistoryRow
	body          string
	digest        [32]byte
}

// Rows returns a detached bounded copy. It never reads a receiver.
func (c ShredHistoryChunk) Rows() []ShredHistoryRow { return slices.Clone(c.rows) }

// Body returns already encoded immutable bytes without encoding or copying.
func (c ShredHistoryChunk) Body() string { return c.body }

// ShredHistorySnapshot is a detached view of the latest consumer publication.
// PublishedAt retains its original monotonic component for a later owner lease.
// QueueRefusals are run-wide, not exact losses before PrefixCutoff.
type ShredHistorySnapshot struct {
	Binding             ShredHistoryBinding
	PublishedAt         time.Time
	Chunks              []ShredHistoryChunk
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

// ShredHistory owns one bounded queue and one consumer. The caller must join
// all producers before CloseAfterProducers; Interrupt makes no drain promise.
// No receiver, protocol, node, or HTTP registration is performed here.
type ShredHistory struct {
	// ponytail: a short admission RWMutex makes retirement/close panic-free;
	// this is capacity-nonblocking, not wait-free. Revisit only after native cost measurement.
	gate                sync.RWMutex
	closing             bool
	nextSource          uint64
	sources             []ShredHistorySource
	descriptorEvictions ShredHistoryCounter
	queue               chan shredHistoryInput
	stop                chan bool
	done                chan struct{}
	closeOnce           sync.Once
	markerState         atomic.Uint32
	queueRefusals       atomic.Uint64
	invalidInputs       atomic.Uint64
	exhausted           atomic.Bool
	interrupted         atomic.Bool
	// Publication replacement, metadata copy-out and one-body lookup use this
	// lock. Packet submission and response writes never take it.
	publicationMu sync.RWMutex
	publication   atomic.Pointer[ShredHistorySnapshot]
	metadata      atomic.Pointer[ShredHistoryMetadata]
	binding       ShredHistoryBinding
	limits        shredHistoryLimits
	// The following fields belong only to the consumer.
	open          []ShredHistoryRow
	chunks        []ShredHistoryChunk
	encodedBytes  int
	ranges        [3][]ShredHistoryRange
	marker        *ShredHistoryMarker
	cutoff        uint64
	sequence      uint64
	rangeRefusals [3]ShredHistoryCounter
	evictedChunks ShredHistoryCounter
	evictedRows   ShredHistoryCounter
	chunkRefusals ShredHistoryCounter
}

// ShredHistoryHandle belongs to one receiver. Retire after that receiver's
// submitters join. A stale handle can only refuse; it cannot panic or rebind.
type ShredHistoryHandle struct {
	owner      *ShredHistory
	generation uint64
	retirement *shredHistoryRetirement
}

// Retirement is shared by value copies of an exported handle. Its flag is
// protected by the owner's admission gate, just like close and submission.
type shredHistoryRetirement struct{ retired bool }

type shredHistoryInput struct {
	receipt ShredHistoryReceipt
	marker  *ShredHistoryMarker // One detached allocation for the one-shot marker.
}

type shredHistoryLimits struct{ queue, rows, chunks, ranges, sources, chunkBytes, retainedBytes int }

// NewShredHistory starts an explicitly owned consumer. A known run, start and
// nonzero genesis are mandatory. Nil handles are the disabled path.
func NewShredHistory(binding ShredHistoryBinding) (*ShredHistory, error) {
	h, err := newShredHistory(binding, shredHistoryLimits{
		shredHistoryQueueCapacity, shredHistoryChunkRows, shredHistoryChunks,
		shredHistoryRanges, shredHistorySources, shredHistoryChunkBytes, shredHistoryRetainedBytes,
	})
	if err != nil {
		return nil, err
	}
	h.publish(time.Now(), false)
	go h.run()
	return h, nil
}

// newShredHistory constructs without starting; deterministic owner tests drive
// the same consumer operations with tiny limits, without a test timer override.
func newShredHistory(binding ShredHistoryBinding, limits shredHistoryLimits) (*ShredHistory, error) {
	if binding.RunID == "" || len(binding.RunID) > 128 || !utf8.ValidString(binding.RunID) ||
		!shredHistoryTime(binding.NodeStartedAt) || binding.GenesisHash == (solana.Hash{}) {
		return nil, errors.New("invalid shred history binding")
	}
	if limits.queue < 1 || limits.queue > shredHistoryQueueCapacity ||
		limits.rows < 1 || limits.rows > shredHistoryChunkRows ||
		limits.chunks < 1 || limits.chunks > shredHistoryChunks ||
		limits.ranges < 1 || limits.ranges > shredHistoryRanges ||
		limits.sources < 1 || limits.sources > shredHistorySources ||
		limits.chunkBytes < 1 || limits.chunkBytes > shredHistoryChunkBytes ||
		limits.retainedBytes < limits.chunkBytes || limits.retainedBytes > shredHistoryRetainedBytes {
		return nil, errors.New("invalid shred history limits")
	}
	h := &ShredHistory{
		binding: binding, limits: limits, queue: make(chan shredHistoryInput, limits.queue),
		stop: make(chan bool, 1), done: make(chan struct{}),
		sources: make([]ShredHistorySource, 0, limits.sources),
		open:    make([]ShredHistoryRow, 0, limits.rows),
		chunks:  make([]ShredHistoryChunk, 0, limits.chunks),
	}
	for i := range h.ranges {
		h.ranges[i] = make([]ShredHistoryRange, 0, limits.ranges)
	}
	h.publish(binding.NodeStartedAt, false)
	return h, nil
}

// RegisterSource assigns a generation before a receiver starts. MaxUint64 is
// reserved as terminal exhaustion; registration never wraps or revives a run.
func (h *ShredHistory) RegisterSource(kind ShredHistorySourceKind, startedAt time.Time) (*ShredHistoryHandle, bool) {
	if h == nil || !shredHistoryTime(startedAt) || (kind != ShredHistoryPrewarm && kind != ShredHistoryLive) {
		return nil, false
	}
	h.gate.Lock()
	defer h.gate.Unlock()
	if h.closing || h.exhausted.Load() {
		return nil, false
	}
	if h.nextSource == math.MaxUint64-1 {
		h.exhausted.Store(true)
		return nil, false
	}
	h.nextSource++
	if len(h.sources) == h.limits.sources {
		copy(h.sources, h.sources[1:])
		h.sources = h.sources[:len(h.sources)-1]
		shredHistoryIncrement(&h.descriptorEvictions)
	}
	h.sources = append(h.sources, ShredHistorySource{Generation: h.nextSource, StartedAt: startedAt, Kind: kind})
	return &ShredHistoryHandle{owner: h, generation: h.nextSource, retirement: new(shredHistoryRetirement)}, true
}

// Generation is the stable generation assigned to this handle, or zero if nil.
func (s *ShredHistoryHandle) Generation() uint64 {
	if s == nil {
		return 0
	}
	return s.generation
}

// TryReceipt copies an already authenticated occurrence without allocating,
// reading a clock, or waiting for queue capacity. Authentication is the caller's job.
func (s *ShredHistoryHandle) TryReceipt(receipt ShredHistoryReceipt) bool {
	if s == nil || s.owner == nil || s.retirement == nil {
		return false
	}
	h := s.owner
	h.gate.RLock()
	defer h.gate.RUnlock()
	if s.retirement.retired || h.closing || h.exhausted.Load() {
		return false
	}
	if !shredHistoryTime(receipt.At) || receipt.SourceGeneration != 0 ||
		receipt.Origin < ShredHistoryTVU || receipt.Origin > ShredHistoryRepairUnmatched {
		shredHistoryAtomicIncrement(&h.invalidInputs)
		return false
	}
	receipt.SourceGeneration = s.generation
	select {
	case h.queue <- shredHistoryInput{receipt: receipt}:
		return true
	default:
		shredHistoryAtomicIncrement(&h.queueRefusals)
		return false
	}
}

// TryMarker makes one node-run attempt. A retired source or full queue records
// terminal unknown; later contexts cannot retry. Release diagnostics locks first.
func (s *ShredHistoryHandle) TryMarker(marker ShredHistoryMarker) bool {
	if s == nil || s.owner == nil || s.retirement == nil {
		return false
	}
	h := s.owner
	h.gate.RLock()
	defer h.gate.RUnlock()
	if h.closing {
		return false
	}
	if !h.markerState.CompareAndSwap(uint32(ShredHistoryMarkerOpen), uint32(ShredHistoryMarkerQueued)) {
		return false
	}
	if s.retirement.retired || h.exhausted.Load() ||
		!shredHistoryTime(marker.At) || marker.SourceGeneration != 0 || marker.ReplayAttempt == 0 ||
		marker.CompletedIndex == 0 || marker.Bankhash == (solana.Hash{}) {
		h.markerState.Store(uint32(ShredHistoryMarkerUnknown))
		return false
	}
	marker.SourceGeneration = s.generation
	select {
	case h.queue <- shredHistoryInput{marker: &marker}:
		return true
	default:
		h.markerState.Store(uint32(ShredHistoryMarkerUnknown))
		return false
	}
}

// MarkMarkerUnknown consumes the first marker attempt when its original
// receiver context is missing. It never replaces a queued or terminal marker.
func (h *ShredHistory) MarkMarkerUnknown() bool {
	if h == nil || h.queue == nil {
		return false
	}
	h.gate.RLock()
	defer h.gate.RUnlock()
	return !h.closing && h.markerState.CompareAndSwap(uint32(ShredHistoryMarkerOpen), uint32(ShredHistoryMarkerUnknown))
}

// Retire refuses future submissions while preserving already queued history.
func (s *ShredHistoryHandle) Retire() {
	if s == nil || s.owner == nil || s.retirement == nil {
		return
	}
	h := s.owner
	h.gate.Lock()
	defer h.gate.Unlock()
	s.retirement.retired = true
	for i := range h.sources {
		if h.sources[i].Generation == s.generation {
			h.sources[i].Retired = true
			break
		}
	}
}

// CloseAfterProducers joins the consumer and fully drains accepted input. The
// caller must first join every producer. False means Interrupt won the close.
func (h *ShredHistory) CloseAfterProducers() bool {
	if h == nil {
		return true
	}
	h.close(true)
	return !h.interrupted.Load()
}

// Interrupt closes admission and joins without promising a complete drain.
// It never overwrites marker_unknown with an apparently completed prefix.
func (h *ShredHistory) Interrupt() {
	if h != nil {
		h.close(false)
	}
}

func (h *ShredHistory) close(drain bool) {
	h.closeOnce.Do(func() {
		h.gate.Lock()
		h.closing = true
		if !drain {
			h.interrupted.Store(true)
		}
		h.gate.Unlock()
		h.stop <- drain
	})
	<-h.done
}

// Snapshot returns detached metadata and immutable chunk strings. Consumers
// must bound retained snapshots; this owner cannot reclaim a caller's copies.
func (h *ShredHistory) Snapshot() (ShredHistorySnapshot, bool) {
	if h == nil {
		return ShredHistorySnapshot{}, false
	}
	s := *h.publication.Load()
	s.Chunks = slices.Clone(s.Chunks)
	s.Sources = slices.Clone(s.Sources)
	for i := range s.StartupRanges {
		s.StartupRanges[i] = slices.Clone(s.StartupRanges[i])
	}
	if s.Marker != nil {
		marker := *s.Marker
		s.Marker = &marker
	}
	// Loss counters are intentionally not ordered with the consumer's cutoff.
	s.QueueRefusals = shredHistoryCounter(h.queueRefusals.Load())
	s.InvalidInputs = shredHistoryCounter(h.invalidInputs.Load())
	s.MarkerState = ShredHistoryMarkerState(h.markerState.Load())
	// Closed requires the matching marker/cutoff publication, not merely its CAS.
	if s.MarkerState == ShredHistoryMarkerClosed && s.Marker == nil {
		s.MarkerState = ShredHistoryMarkerQueued
	}
	s.Interrupted = h.interrupted.Load()
	s.Exhausted = h.exhausted.Load()
	s.StartupLimited = s.StartupLimited || s.QueueRefusals.Value > 0 || s.InvalidInputs.Value > 0 || s.Interrupted || s.Exhausted || s.MarkerState == ShredHistoryMarkerUnknown
	return s, true
}

func (h *ShredHistory) run() {
	ticker := time.NewTicker(shredHistoryCadence)
	defer ticker.Stop()
	defer close(h.done)
	for {
		select {
		case input := <-h.queue:
			h.consume(input)
		case <-ticker.C:
			h.seal()
			h.publish(time.Now(), false)
		case drain := <-h.stop:
			if drain {
				for {
					select {
					case input := <-h.queue:
						h.consume(input)
					default:
						goto drained
					}
				}
			}
		drained:
			h.markerState.CompareAndSwap(uint32(ShredHistoryMarkerQueued), uint32(ShredHistoryMarkerUnknown))
			h.seal()
			h.publish(time.Now(), true)
			return
		}
	}
}

func (h *ShredHistory) consume(input shredHistoryInput) {
	if input.marker != nil {
		if h.markerState.CompareAndSwap(uint32(ShredHistoryMarkerQueued), uint32(ShredHistoryMarkerClosed)) {
			h.marker = input.marker
			h.cutoff = h.sequence
		}
		return
	}
	if h.sequence == math.MaxUint64-1 {
		h.exhausted.Store(true)
		return
	}
	h.sequence++
	row := ShredHistoryRow{ShredHistoryReceipt: input.receipt, Sequence: h.sequence}
	h.open = append(h.open, row)
	if h.marker == nil {
		h.addRange(int(row.Origin)-1, row.Slot)
	}
	if len(h.open) == h.limits.rows {
		// Seal immediately, but copy the retained index/ranges only at the
		// publication cadence. High ingress must not amplify metadata copying.
		h.seal()
	}
}

func (h *ShredHistory) addRange(origin int, slot uint64) {
	ranges := h.ranges[origin]
	i := sort.Search(len(ranges), func(i int) bool {
		return ranges[i].Last >= slot || slot-ranges[i].Last == 1
	})
	if i < len(ranges) && (ranges[i].First <= slot || ranges[i].First-slot == 1) {
		if slot < ranges[i].First {
			ranges[i].First = slot
		}
		if slot > ranges[i].Last {
			ranges[i].Last = slot
		}
		if i+1 < len(ranges) && ranges[i].Last != math.MaxUint64 && ranges[i+1].First == ranges[i].Last+1 {
			ranges[i].Last = ranges[i+1].Last
			copy(ranges[i+1:], ranges[i+2:])
			ranges = ranges[:len(ranges)-1]
		}
		h.ranges[origin] = ranges
		return
	}
	if len(ranges) == h.limits.ranges {
		shredHistoryIncrement(&h.rangeRefusals[origin])
		return
	}
	ranges = append(ranges, ShredHistoryRange{})
	copy(ranges[i+1:], ranges[i:])
	ranges[i] = ShredHistoryRange{First: slot, Last: slot}
	h.ranges[origin] = ranges
}

type shredHistoryWireRow struct {
	Sequence uint64    `json:"sequence,string"`
	At       time.Time `json:"at"`
	Source   uint64    `json:"source,string"`
	Slot     uint64    `json:"slot,string"`
	Index    uint32    `json:"index"`
	FEC      uint32    `json:"fec_set_index"`
	Version  uint16    `json:"version"`
	Variant  uint8     `json:"variant"`
	Origin   string    `json:"origin"`
	Root     string    `json:"authenticated_fec_root"`
}

type shredHistoryWireChunk struct {
	First   uint64                `json:"first_sequence,string"`
	Last    uint64                `json:"last_sequence,string"`
	Columns [10]string            `json:"columns"`
	Rows    []shredHistoryWireRow `json:"rows"`
}

// MarshalJSON emits the fixed column order once named by the chunk. Repeating
// object keys on every row would exceed the source's three-generation budget.
// This encoding runs only on the consumer, never on packet submission.
func (row shredHistoryWireRow) MarshalJSON() ([]byte, error) {
	return json.Marshal([10]any{
		strconv.FormatUint(row.Sequence, 10), row.At, strconv.FormatUint(row.Source, 10),
		strconv.FormatUint(row.Slot, 10), row.Index, row.FEC, row.Version, row.Variant, row.Origin, row.Root,
	})
}

func (h *ShredHistory) seal() {
	if len(h.open) == 0 {
		return
	}
	c := ShredHistoryChunk{FirstSequence: h.open[0].Sequence, LastSequence: h.open[len(h.open)-1].Sequence, MinAt: h.open[0].At.UTC(), MaxAt: h.open[0].At.UTC()}
	wire := shredHistoryWireChunk{First: c.FirstSequence, Last: c.LastSequence,
		Columns: [10]string{"sequence", "at", "source", "slot", "index", "fec_set_index", "version", "variant", "origin", "authenticated_fec_root"},
		Rows:    make([]shredHistoryWireRow, len(h.open))}
	origins := [...]string{"tvu", "repair_matched", "repair_socket_unmatched"}
	for i, row := range h.open {
		// Query bounds follow the serialized wall dates even across a wall-clock
		// reversal. The stored rows still retain their original monotonic clocks.
		at := row.At.UTC()
		if at.Before(c.MinAt) {
			c.MinAt = at
		}
		if at.After(c.MaxAt) {
			c.MaxAt = at
		}
		wire.Rows[i] = shredHistoryWireRow{row.Sequence, at, row.SourceGeneration, row.Slot, row.Index, row.FECSetIndex, row.Version, row.Variant, origins[int(row.Origin)-1], row.Root.String()}
	}
	body, err := json.Marshal(wire)
	if err != nil || len(body) > h.limits.chunkBytes {
		shredHistoryIncrement(&h.chunkRefusals)
		h.open = h.open[:0]
		return
	}
	c.rows = slices.Clone(h.open)
	c.body = string(body)
	c.digest = sha256.Sum256(body)
	for len(h.chunks) == h.limits.chunks || h.encodedBytes+len(c.body) > h.limits.retainedBytes {
		old := h.chunks[0]
		h.encodedBytes -= len(old.body)
		shredHistoryIncrement(&h.evictedChunks)
		for range old.rows {
			shredHistoryIncrement(&h.evictedRows)
		}
		copy(h.chunks, h.chunks[1:])
		h.chunks[len(h.chunks)-1] = ShredHistoryChunk{}
		h.chunks = h.chunks[:len(h.chunks)-1]
	}
	h.chunks = append(h.chunks, c)
	h.encodedBytes += len(c.body)
	h.open = h.open[:0]
}

func (h *ShredHistory) publish(at time.Time, closed bool) {
	s := &ShredHistorySnapshot{Binding: h.binding, PublishedAt: at, Chunks: slices.Clone(h.chunks),
		Marker: h.marker, PrefixCutoff: h.cutoff, LastSequence: h.sequence, RangeRefusals: h.rangeRefusals,
		EvictedChunks: h.evictedChunks, EvictedRows: h.evictedRows, ChunkRefusals: h.chunkRefusals, Closed: closed}
	for i := range h.ranges {
		s.StartupRanges[i] = slices.Clone(h.ranges[i])
		s.StartupLimited = s.StartupLimited || h.rangeRefusals[i].Value > 0
	}
	h.gate.RLock()
	s.Sources = slices.Clone(h.sources)
	s.DescriptorEvictions = h.descriptorEvictions
	h.gate.RUnlock()
	s.StartupLimited = s.StartupLimited || s.DescriptorEvictions.Value > 0 || s.EvictedChunks.Value > 0 || s.ChunkRefusals.Value > 0
	m := shredHistoryMetadata(s)
	h.publicationMu.Lock()
	h.publication.Store(s)
	h.metadata.Store(m)
	h.publicationMu.Unlock()
}

func shredHistoryTime(at time.Time) bool { return !at.IsZero() && at.Year() >= 1 && at.Year() <= 9999 }

func shredHistoryCounter(value uint64) ShredHistoryCounter {
	return ShredHistoryCounter{Value: value, Saturated: value == math.MaxUint64}
}

func shredHistoryIncrement(counter *ShredHistoryCounter) {
	if counter.Value < math.MaxUint64 {
		counter.Value++
	}
	counter.Saturated = counter.Value == math.MaxUint64
}

func shredHistoryAtomicIncrement(counter *atomic.Uint64) {
	for value := counter.Load(); value != math.MaxUint64; value = counter.Load() {
		if counter.CompareAndSwap(value, value+1) {
			return
		}
	}
}
