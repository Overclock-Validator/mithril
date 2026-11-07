package turbine

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/gagliardetto/solana-go"
)

func historyBinding() ShredHistoryBinding {
	return ShredHistoryBinding{RunID: "history-test", NodeStartedAt: time.Unix(1_700_000_000, 123).UTC(), GenesisHash: solana.Hash{1}}
}

func historyLimits() shredHistoryLimits {
	return shredHistoryLimits{queue: 8, rows: 2, chunks: 2, ranges: 2, sources: 2, chunkBytes: shredHistoryChunkBytes, retainedBytes: 2 * shredHistoryChunkBytes}
}

func historyOwner(t *testing.T, limits shredHistoryLimits) *ShredHistory {
	t.Helper()
	h, err := newShredHistory(historyBinding(), limits)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func historySource(t *testing.T, h *ShredHistory, kind ShredHistorySourceKind) *ShredHistoryHandle {
	t.Helper()
	s, ok := h.RegisterSource(kind, historyBinding().NodeStartedAt.Add(time.Second))
	if !ok {
		t.Fatal("source registration refused")
	}
	return s
}

func historyReceipt(slot uint64, origin ShredHistoryOrigin) ShredHistoryReceipt {
	return ShredHistoryReceipt{At: historyBinding().NodeStartedAt.Add(2 * time.Second), Root: solana.Hash{2}, Slot: slot, Index: 3, FECSetIndex: 4, Version: 5, Variant: 6, Origin: origin}
}

func historyMarker() ShredHistoryMarker {
	return ShredHistoryMarker{At: historyBinding().NodeStartedAt.Add(3 * time.Second), Slot: 12, ReplayAttempt: 2, Rewinds: 1, CompletedIndex: 7, Bankhash: solana.Hash{3}}
}

func historyDrain(h *ShredHistory) {
	for len(h.queue) > 0 {
		h.consume(<-h.queue)
	}
	h.seal()
	h.publish(time.Now(), false)
}

func historyCheckEncodedRow(t *testing.T, body string, index int, want ShredHistoryRow) {
	t.Helper()
	var wire struct {
		Columns [10]string            `json:"columns"`
		Rows    [][10]json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal([]byte(body), &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Columns != [10]string{"sequence", "at", "source", "slot", "index", "fec_set_index", "version", "variant", "origin", "authenticated_fec_root"} || index < 0 || index >= len(wire.Rows) {
		t.Fatal("fixed chunk column schema or row index differs")
	}
	origins := map[ShredHistoryOrigin]string{ShredHistoryTVU: "tvu", ShredHistoryRepairMatched: "repair_matched", ShredHistoryRepairUnmatched: "repair_socket_unmatched"}
	values := [10]any{strconv.FormatUint(want.Sequence, 10), want.At.UTC(), strconv.FormatUint(want.SourceGeneration, 10), strconv.FormatUint(want.Slot, 10), want.Index, want.FECSetIndex, want.Version, want.Variant, origins[want.Origin], want.Root.String()}
	for column, value := range values {
		expected, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(wire.Rows[index][column], expected) {
			t.Fatalf("column %s = %s, want %s", wire.Columns[column], wire.Rows[index][column], expected)
		}
	}
}

func TestShredHistoryBindingAndDisabled(t *testing.T) {
	var absent *ShredHistory
	var handle *ShredHistoryHandle
	if _, ok := absent.Snapshot(); ok || handle.Generation() != 0 || handle.TryReceipt(historyReceipt(0, ShredHistoryTVU)) || handle.TryMarker(historyMarker()) || !absent.CloseAfterProducers() {
		t.Fatal("nil owner or handle is not disabled")
	}
	absent.Interrupt()
	handle.Retire()
	for name, change := range map[string]func(*ShredHistoryBinding){
		"run":     func(b *ShredHistoryBinding) { b.RunID = "" },
		"long":    func(b *ShredHistoryBinding) { b.RunID = strings.Repeat("x", 129) },
		"utf8":    func(b *ShredHistoryBinding) { b.RunID = "\xff" },
		"start":   func(b *ShredHistoryBinding) { b.NodeStartedAt = time.Time{} },
		"year":    func(b *ShredHistoryBinding) { b.NodeStartedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) },
		"genesis": func(b *ShredHistoryBinding) { b.GenesisHash = solana.Hash{} },
	} {
		t.Run(name, func(t *testing.T) {
			b := historyBinding()
			change(&b)
			if _, err := NewShredHistory(b); err == nil {
				t.Fatal("invalid binding accepted")
			}
		})
	}
	h := historyOwner(t, historyLimits())
	if _, ok := h.RegisterSource(0, time.Now()); ok {
		t.Fatal("unknown source kind accepted")
	}
	if _, ok := h.RegisterSource(ShredHistoryLive, time.Time{}); ok {
		t.Fatal("unknown source clock accepted")
	}
	if allocs := testing.AllocsPerRun(100, func() { handle.TryReceipt(ShredHistoryReceipt{}) }); allocs != 0 {
		t.Fatalf("disabled allocations = %g, want 0", allocs)
	}
}

func TestShredHistoryOrderMarkerAndDetachedSnapshots(t *testing.T) {
	h := historyOwner(t, historyLimits())
	prewarm := historySource(t, h, ShredHistoryPrewarm)
	live := historySource(t, h, ShredHistoryLive)
	first := historyReceipt(0, ShredHistoryTVU)
	first.At = first.At.Add(2 * time.Second)
	second := historyReceipt(7, ShredHistoryRepairMatched)
	if !prewarm.TryReceipt(first) || !live.TryReceipt(second) || !prewarm.TryMarker(historyMarker()) {
		t.Fatal("bounded input refused")
	}
	late := historyReceipt(math.MaxUint64, ShredHistoryRepairUnmatched)
	late.At = late.At.Add(-time.Second)
	if !live.TryReceipt(late) {
		t.Fatal("older event after marker refused")
	}
	prewarm.Retire()
	if prewarm.TryReceipt(first) || prewarm.TryMarker(historyMarker()) {
		t.Fatal("retired handle accepted")
	}
	historyDrain(h)
	s, _ := h.Snapshot()
	if s.MarkerState != ShredHistoryMarkerClosed || s.Marker == nil || s.Marker.SourceGeneration != 1 || s.PrefixCutoff != 2 || s.LastSequence != 3 || s.StartupLimited {
		t.Fatalf("incorrect prefix: %+v", s)
	}
	if len(s.Chunks) != 2 || s.Chunks[0].MinAt != second.At || s.Chunks[0].MaxAt != first.At || s.Chunks[1].MinAt != late.At {
		t.Fatal("wall clocks were sorted or re-dated")
	}
	if len(s.StartupRanges[0]) != 1 || s.StartupRanges[0][0] != (ShredHistoryRange{0, 0}) || s.StartupRanges[1][0] != (ShredHistoryRange{7, 7}) || len(s.StartupRanges[2]) != 0 {
		t.Fatalf("incorrect exact prefix ranges: %+v", s.StartupRanges)
	}
	if !s.Sources[0].Retired || s.Sources[0].Kind != ShredHistoryPrewarm || s.Sources[1].Kind != ShredHistoryLive {
		t.Fatal("source phase or retirement lost")
	}
	first.SourceGeneration, second.SourceGeneration = 1, 2
	historyCheckEncodedRow(t, s.Chunks[0].Body(), 0, ShredHistoryRow{ShredHistoryReceipt: first, Sequence: 1})
	historyCheckEncodedRow(t, s.Chunks[0].Body(), 1, ShredHistoryRow{ShredHistoryReceipt: second, Sequence: 2})
	oldBody := s.Chunks[0].Body()
	rows := s.Chunks[0].Rows()
	rows[0].Slot = 99
	s.Chunks[0].FirstSequence = 99
	s.StartupRanges[0][0].First = 99
	s.Sources[0].Kind = ShredHistoryLive
	s.Marker.Slot = 99
	again, _ := h.Snapshot()
	if again.Chunks[0].Rows()[0].Slot != 0 || again.Chunks[0].FirstSequence != 1 || again.Chunks[0].Body() != oldBody || again.StartupRanges[0][0].First != 0 || again.Sources[0].Kind != ShredHistoryPrewarm || again.Marker.Slot != 12 {
		t.Fatal("caller mutated retained owner data")
	}
}

func TestShredHistoryExactRangesAndCapacity(t *testing.T) {
	l := historyLimits()
	l.queue, l.rows = 16, 16
	h := historyOwner(t, l)
	source := historySource(t, h, ShredHistoryLive)
	for _, slot := range []uint64{1, 3, 5, 2, 5, 4, math.MaxUint64, math.MaxUint64 - 1, 0, 1} {
		if !source.TryReceipt(historyReceipt(slot, ShredHistoryTVU)) {
			t.Fatal("input queue unexpectedly full")
		}
	}
	if !source.TryReceipt(historyReceipt(0, ShredHistoryRepairMatched)) || !source.TryReceipt(historyReceipt(math.MaxUint64, ShredHistoryRepairUnmatched)) {
		t.Fatal("other origin refused")
	}
	historyDrain(h)
	s, _ := h.Snapshot()
	if s.LastSequence != 12 || s.RangeRefusals[0].Value != 1 || !s.StartupLimited ||
		len(s.StartupRanges[0]) != 2 || s.StartupRanges[0][0] != (ShredHistoryRange{0, 5}) || s.StartupRanges[0][1] != (ShredHistoryRange{math.MaxUint64 - 1, math.MaxUint64}) ||
		s.StartupRanges[1][0] != (ShredHistoryRange{0, 0}) || s.StartupRanges[2][0] != (ShredHistoryRange{math.MaxUint64, math.MaxUint64}) {
		t.Fatalf("range loss/merge fabricated a gap or overflowed: %+v", s)
	}
}

func TestShredHistoryChunkWallBoundsPreserveOriginalClocks(t *testing.T) {
	h := historyOwner(t, historyLimits())
	source := historySource(t, h, ShredHistoryLive)
	later := historyReceipt(1, ShredHistoryTVU)
	later.At = time.Now()
	earlier := historyReceipt(2, ShredHistoryTVU)
	earlier.At = later.At.Add(-time.Second)
	if !source.TryReceipt(later) || !source.TryReceipt(earlier) {
		t.Fatal("reordered clocks refused")
	}
	historyDrain(h)
	s, _ := h.Snapshot()
	c := s.Chunks[0]
	if c.MinAt != earlier.At.UTC() || c.MaxAt != later.At.UTC() {
		t.Fatal("chunk query bounds must use serialized UTC wall clocks, not monotonic ordering")
	}
	rows := c.Rows()
	if rows[0].At != later.At || rows[1].At != earlier.At || rows[0].Sequence != 1 || rows[1].Sequence != 2 {
		t.Fatal("original clocks or publication order changed")
	}
	historyCheckEncodedRow(t, c.Body(), 0, rows[0])
	historyCheckEncodedRow(t, c.Body(), 1, rows[1])
}

func TestShredHistoryMarkerUnknownSurvivesInterruption(t *testing.T) {
	l := historyLimits()
	l.queue = 1
	h := historyOwner(t, l)
	source := historySource(t, h, ShredHistoryPrewarm)
	if !source.TryReceipt(historyReceipt(1, ShredHistoryTVU)) || source.TryReceipt(historyReceipt(2, ShredHistoryTVU)) || source.TryMarker(historyMarker()) {
		t.Fatal("queue-full refusal failed")
	}
	historyDrain(h)
	if source.TryMarker(historyMarker()) || historySource(t, h, ShredHistoryLive).TryMarker(historyMarker()) {
		t.Fatal("later context retried failed marker")
	}
	go h.run()
	h.Interrupt()
	s, _ := h.Snapshot()
	if s.MarkerState != ShredHistoryMarkerUnknown || !s.Interrupted || !s.Closed || !s.StartupLimited || s.QueueRefusals.Value != 1 || s.Marker != nil || h.CloseAfterProducers() {
		t.Fatalf("failed marker or interrupted close was relabeled: %+v", s)
	}
	if source.TryReceipt(historyReceipt(3, ShredHistoryTVU)) || source.TryMarker(historyMarker()) {
		t.Fatal("closed handle accepted")
	}
	if _, ok := h.RegisterSource(ShredHistoryLive, time.Now()); ok {
		t.Fatal("registration after close accepted")
	}
}

func TestShredHistoryOneMarkerClaimAndPendingDrain(t *testing.T) {
	h := historyOwner(t, historyLimits())
	source := historySource(t, h, ShredHistoryLive)
	var accepted atomic.Int32
	var producers sync.WaitGroup
	for range 2 {
		producers.Go(func() {
			if source.TryMarker(historyMarker()) {
				accepted.Add(1)
			}
		})
	}
	producers.Wait()
	if accepted.Load() != 1 || ShredHistoryMarkerState(h.markerState.Load()) != ShredHistoryMarkerQueued {
		t.Fatal("marker claim was not one-shot")
	}
	source.Retire()
	go h.run()
	if !h.CloseAfterProducers() {
		t.Fatal("ordered close did not drain")
	}
	s, _ := h.Snapshot()
	if s.MarkerState != ShredHistoryMarkerClosed || s.Marker == nil || s.PrefixCutoff != 0 || !s.Closed || s.Interrupted || len(s.Chunks) != 0 {
		t.Fatalf("known empty prefix lost on drain: %+v", s)
	}
}

func TestShredHistoryRetiredMarkerCannotUseReplacementSource(t *testing.T) {
	h := historyOwner(t, historyLimits())
	old := historySource(t, h, ShredHistoryPrewarm)
	alias := *old
	old.Retire()
	if alias.TryMarker(historyMarker()) {
		t.Fatal("retired source accepted the first marker")
	}
	replacement := historySource(t, h, ShredHistoryLive)
	if replacement.TryMarker(historyMarker()) {
		t.Fatal("replacement source relabeled the lost first marker")
	}
	historyDrain(h)
	s, _ := h.Snapshot()
	if s.MarkerState != ShredHistoryMarkerUnknown || s.Marker != nil || !s.StartupLimited || s.LastSequence != 0 {
		t.Fatalf("lost first marker was not terminally unknown: %+v", s)
	}
}

func TestShredHistoryMarkerRetirementRaceKeepsOriginalAttempt(t *testing.T) {
	for range 32 {
		h := historyOwner(t, historyLimits())
		old := historySource(t, h, ShredHistoryPrewarm)
		start := make(chan struct{})
		var workers sync.WaitGroup
		var accepted bool
		workers.Go(func() { <-start; old.Retire() })
		workers.Go(func() { <-start; accepted = old.TryMarker(historyMarker()) })
		close(start)
		workers.Wait()
		if historySource(t, h, ShredHistoryLive).TryMarker(historyMarker()) {
			t.Fatal("replacement claimed a racing original marker attempt")
		}
		historyDrain(h)
		s, _ := h.Snapshot()
		if accepted {
			if s.MarkerState != ShredHistoryMarkerClosed || s.Marker == nil || s.Marker.SourceGeneration != old.Generation() {
				t.Fatalf("accepted original marker lost its receiver: %+v", s)
			}
		} else if s.MarkerState != ShredHistoryMarkerUnknown || s.Marker != nil {
			t.Fatalf("lost original marker was not terminally unknown: %+v", s)
		}
	}
}

func TestShredHistoryMissingMarkerContextIsTerminal(t *testing.T) {
	var absent *ShredHistory
	if absent.MarkMarkerUnknown() || new(ShredHistory).MarkMarkerUnknown() {
		t.Fatal("disabled history accepted a missing-context attempt")
	}
	h := historyOwner(t, historyLimits())
	if !h.MarkMarkerUnknown() || h.MarkMarkerUnknown() || historySource(t, h, ShredHistoryLive).TryMarker(historyMarker()) {
		t.Fatal("missing receiver context did not consume exactly one marker attempt")
	}
	historyDrain(h)
	s, _ := h.Snapshot()
	if s.MarkerState != ShredHistoryMarkerUnknown || s.Marker != nil || !s.StartupLimited {
		t.Fatalf("missing receiver context became a known prefix: %+v", s)
	}
	queued := historyOwner(t, historyLimits())
	if !historySource(t, queued, ShredHistoryLive).TryMarker(historyMarker()) || queued.MarkMarkerUnknown() {
		t.Fatal("missing context overwrote an already accepted marker")
	}
	historyDrain(queued)
	if queued.MarkMarkerUnknown() {
		t.Fatal("missing context overwrote a closed marker")
	}
	go queued.run()
	queued.CloseAfterProducers()
	if queued.MarkMarkerUnknown() {
		t.Fatal("closed owner accepted a new marker attempt")
	}
}

func TestShredHistoryCopiedHandleRetirement(t *testing.T) {
	for _, marker := range []bool{false, true} {
		name := "receipt"
		if marker {
			name = "marker"
		}
		t.Run(name, func(t *testing.T) {
			h := historyOwner(t, historyLimits())
			source := historySource(t, h, ShredHistoryLive)
			alias := *source
			source.Retire()
			var accepted bool
			if marker {
				accepted = alias.TryMarker(historyMarker())
			} else {
				accepted = alias.TryReceipt(historyReceipt(1, ShredHistoryTVU))
			}
			wantState := ShredHistoryMarkerOpen
			if marker {
				wantState = ShredHistoryMarkerUnknown
			}
			if accepted || len(h.queue) != 0 || h.markerState.Load() != uint32(wantState) {
				t.Fatal("copied handle accepted input after original retirement")
			}
			other := historySource(t, h, ShredHistoryLive)
			otherAlias := *other
			otherAlias.Retire()
			if other.TryReceipt(historyReceipt(2, ShredHistoryTVU)) || other.TryMarker(historyMarker()) {
				t.Fatal("retiring a copied handle left its original active")
			}
		})
	}
}

func TestShredHistoryEvictionInvalidAndExhaustion(t *testing.T) {
	l := historyLimits()
	l.rows, l.chunks, l.sources = 1, 1, 1
	h := historyOwner(t, l)
	old := historySource(t, h, ShredHistoryPrewarm)
	source := historySource(t, h, ShredHistoryLive)
	for _, slot := range []uint64{10, 20} {
		if !old.TryReceipt(historyReceipt(slot, ShredHistoryTVU)) {
			t.Fatal("descriptor eviction invalidated an active old handle")
		}
		historyDrain(h)
	}
	s, _ := h.Snapshot()
	if len(s.Chunks) != 1 || s.Chunks[0].FirstSequence != 2 || s.Chunks[0].Rows()[0].SourceGeneration != old.Generation() || s.EvictedChunks.Value != 1 || s.EvictedRows.Value != 1 || s.DescriptorEvictions.Value != 1 || s.Sources[0].Generation != source.Generation() {
		t.Fatalf("eviction accounting or historical generation wrong: %+v", s)
	}
	for _, bad := range []ShredHistoryReceipt{{}, {At: time.Now(), Origin: 4}, {At: time.Now(), Origin: ShredHistoryTVU, SourceGeneration: 10}} {
		if source.TryReceipt(bad) {
			t.Fatal("invalid input accepted")
		}
	}
	h.sequence = math.MaxUint64 - 2
	if !source.TryReceipt(historyReceipt(21, ShredHistoryTVU)) || !source.TryReceipt(historyReceipt(22, ShredHistoryTVU)) {
		t.Fatal("queued input refused before consumer exhaustion")
	}
	historyDrain(h)
	s, _ = h.Snapshot()
	if !s.Exhausted || s.LastSequence != math.MaxUint64-1 || s.InvalidInputs.Value != 3 || source.TryReceipt(historyReceipt(23, ShredHistoryTVU)) {
		t.Fatal("sequence exhaustion wrapped or revived")
	}
	g := historyOwner(t, historyLimits())
	g.nextSource = math.MaxUint64 - 2
	last := historySource(t, g, ShredHistoryLive)
	if last.Generation() != math.MaxUint64-1 {
		t.Fatal("last source generation wrong")
	}
	if _, ok := g.RegisterSource(ShredHistoryLive, time.Now()); ok || !g.exhausted.Load() {
		t.Fatal("source generation wrapped")
	}
	var count ShredHistoryCounter
	count.Value = math.MaxUint64 - 1
	shredHistoryIncrement(&count)
	shredHistoryIncrement(&count)
	h.queueRefusals.Store(math.MaxUint64 - 1)
	shredHistoryAtomicIncrement(&h.queueRefusals)
	shredHistoryAtomicIncrement(&h.queueRefusals)
	if !count.Saturated || count.Value != math.MaxUint64 || !shredHistoryCounter(h.queueRefusals.Load()).Saturated {
		t.Fatal("counter saturation wrapped or claimed an exact zero")
	}
	limited := historyLimits()
	limited.chunkBytes = 1
	refused := historyOwner(t, limited)
	if !historySource(t, refused, ShredHistoryLive).TryReceipt(historyReceipt(0, ShredHistoryTVU)) {
		t.Fatal("receipt unexpectedly refused")
	}
	historyDrain(refused)
	r, _ := refused.Snapshot()
	if len(r.Chunks) != 0 || r.ChunkRefusals.Value != 1 || !r.StartupLimited || r.LastSequence != 1 {
		t.Fatal("oversize chunk was truncated or hidden")
	}
}

func TestShredHistoryConcurrentCloseAndJoinedDrain(t *testing.T) {
	h, err := NewShredHistory(historyBinding())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Interrupt)
	var producers sync.WaitGroup
	var accepted atomic.Uint64
	for range 5 {
		source := historySource(t, h, ShredHistoryLive)
		producers.Go(func() {
			for i := range 100 {
				if source.TryReceipt(historyReceipt(uint64(i), ShredHistoryTVU)) {
					accepted.Add(1)
				}
			}
			source.Retire()
		})
	}
	producers.Wait()
	if !h.CloseAfterProducers() {
		t.Fatal("joined producers did not drain")
	}
	s, _ := h.Snapshot()
	if s.LastSequence != accepted.Load() || s.LastSequence != 500 || !s.Closed || s.Interrupted {
		t.Fatalf("lost accepted rows: sequence=%d accepted=%d", s.LastSequence, accepted.Load())
	}
	other, err := NewShredHistory(historyBinding())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(other.Interrupt)
	source := historySource(t, other, ShredHistoryLive)
	var concurrent sync.WaitGroup
	for range 5 {
		concurrent.Go(func() {
			for i := range 200 {
				source.TryReceipt(historyReceipt(uint64(i), ShredHistoryRepairMatched))
				other.Snapshot()
			}
		})
		concurrent.Go(other.Interrupt)
	}
	concurrent.Wait()
	s, _ = other.Snapshot()
	if !s.Interrupted || !s.Closed || source.TryReceipt(historyReceipt(0, ShredHistoryTVU)) {
		t.Fatal("concurrent interruption did not close/join safely")
	}
}

func TestShredHistoryPublicationCadenceAndNoEmptyChunks(t *testing.T) {
	h, err := NewShredHistory(historyBinding())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Interrupt)
	source := historySource(t, h, ShredHistoryLive)
	initial, _ := h.Snapshot()
	if !source.TryReceipt(historyReceipt(1, ShredHistoryTVU)) {
		t.Fatal("receipt refused")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		s, _ := h.Snapshot()
		if len(s.Chunks) == 1 && s.PublishedAt.After(initial.PublishedAt) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("partial chunk was never published")
		}
		time.Sleep(time.Millisecond)
	}
	source.Retire()
	if !h.CloseAfterProducers() {
		t.Fatal("drain failed")
	}
	s, _ := h.Snapshot()
	if len(s.Chunks) != 1 || s.Chunks[0].Rows()[0].At != historyReceipt(1, ShredHistoryTVU).At || s.PublishedAt == s.PublishedAt.Round(0) {
		t.Fatal("empty close added a chunk, receipt was redated, or owner clock lost monotonic time")
	}
}

func TestShredHistoryFullChunksDoNotAmplifyPublication(t *testing.T) {
	h := historyOwner(t, historyLimits())
	source := historySource(t, h, ShredHistoryLive)
	initial := h.publication.Load()
	for slot := range 4 {
		if !source.TryReceipt(historyReceipt(uint64(slot), ShredHistoryTVU)) {
			t.Fatal("input refused")
		}
		h.consume(<-h.queue)
	}
	if len(h.chunks) != 2 || len(h.open) != 0 || h.publication.Load() != initial {
		t.Fatal("full chunks must seal without rebuilding the publication")
	}
	h.publish(time.Now(), false)
	s, _ := h.Snapshot()
	if len(s.Chunks) != 2 || s.LastSequence != 4 || h.publication.Load() == initial {
		t.Fatal("cadenced publication did not include retained full chunks")
	}
}

func TestShredHistoryTypedMaximaAndMemoryLayout(t *testing.T) {
	if size := unsafe.Sizeof(shredHistoryInput{}); size > 128 {
		t.Fatalf("actual queue input = %d bytes, limit 128", size)
	}
	limits := shredHistoryLimits{shredHistoryQueueCapacity, shredHistoryChunkRows, shredHistoryChunks, shredHistoryRanges, shredHistorySources, shredHistoryChunkBytes, shredHistoryRetainedBytes}
	h := historyOwner(t, limits)
	at := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	var root solana.Hash
	for i := range root {
		root[i] = 255
	}
	fill := func(generation uint64) {
		for chunk := range shredHistoryChunks {
			for row := range shredHistoryChunkRows {
				sequence := math.MaxUint64 - uint64(3*shredHistoryChunks*shredHistoryChunkRows) + generation*uint64(shredHistoryChunks*shredHistoryChunkRows) + uint64(chunk*shredHistoryChunkRows+row)
				h.open = append(h.open, ShredHistoryRow{ShredHistoryReceipt: ShredHistoryReceipt{At: at, Root: root, SourceGeneration: math.MaxUint64 - 1, Slot: math.MaxUint64, Index: math.MaxUint32, FECSetIndex: math.MaxUint32, Version: math.MaxUint16, Variant: math.MaxUint8, Origin: ShredHistoryRepairUnmatched}, Sequence: sequence})
			}
			h.seal()
		}
	}
	fill(0)
	if len(h.chunks) != 256 || h.encodedBytes > shredHistoryRetainedBytes || h.chunkRefusals.Value != 0 || h.evictedChunks.Value != 0 {
		t.Fatal("full legal retained shape exceeds source bounds")
	}
	for _, chunk := range h.chunks {
		if len(chunk.Body())+2048 > shredHistoryChunkBytes || len(chunk.Rows()) != 128 {
			t.Fatal("actual typed chunk does not leave the proposed transport-envelope reserve")
		}
	}
	h.publish(time.Now(), false)
	callerA, _ := h.Snapshot()
	metadataCallerA, _ := h.Metadata()
	fill(1)
	h.publish(time.Now(), false)
	publishedB := h.publication.Load()
	fill(2) // C rotates the live ring before the next publication.
	// Model a pending C metadata publication alongside cached B and one
	// detached caller A. The publication lock bounds internal copy-out lifetime.
	metadataCandidateC := shredHistoryMetadata(&ShredHistorySnapshot{Chunks: h.chunks})
	var metadataBytes uintptr
	for _, m := range []*ShredHistoryMetadata{&metadataCallerA, h.metadata.Load(), metadataCandidateC} {
		if len(m.Chunks) != shredHistoryChunks {
			t.Fatal("maximum publication lost body-free descriptors")
		}
		metadataBytes += unsafe.Sizeof(*m) + uintptr(cap(m.Chunks))*unsafe.Sizeof(ShredHistoryChunkDescriptor{})
	}
	// Cached/candidate metadata share their snapshots' immutable source/range
	// buffers, already charged below. One detached Metadata caller adds its own
	// source/range/marker copies; the separate maximum-metadata test fills them.
	metadataBytes += shredHistorySources*unsafe.Sizeof(ShredHistorySource{}) +
		3*shredHistoryRanges*unsafe.Sizeof(ShredHistoryRange{}) + unsafe.Sizeof(ShredHistoryMarker{})
	if callerA.Chunks[len(callerA.Chunks)-1].LastSequence >= publishedB.Chunks[0].FirstSequence ||
		publishedB.Chunks[len(publishedB.Chunks)-1].LastSequence >= h.chunks[0].FirstSequence {
		t.Fatal("fixture did not retain three nonoverlapping generations")
	}
	var generations uintptr
	for _, chunks := range [][]ShredHistoryChunk{callerA.Chunks, publishedB.Chunks, h.chunks} {
		if len(chunks) != shredHistoryChunks {
			t.Fatal("generation lost promised retention")
		}
		for _, chunk := range chunks {
			if len(chunk.rows) != shredHistoryChunkRows || chunk.rows[0].At != at || chunk.rows[0].Root != root {
				t.Fatal("retained generation changed original receipt values")
			}
			generations += uintptr(len(chunk.body)) + uintptr(cap(chunk.rows))*unsafe.Sizeof(ShredHistoryRow{})
		}
		historyCheckEncodedRow(t, chunks[0].Body(), 0, chunks[0].rows[0])
		last := chunks[len(chunks)-1]
		historyCheckEncodedRow(t, last.Body(), len(last.rows)-1, last.rows[len(last.rows)-1])
	}
	// Count all three disjoint bodies AND native rows, plus open/new sealing
	// overlap, metadata copies, queue and bounded scratch. This allows one old
	// Snapshot caller and one Metadata copy, not an unbounded caller archive.
	// It is an owned-layout bound, not a Go heap/RSS theorem or full A2 ledger.
	budget := unsafe.Sizeof(ShredHistory{}) +
		shredHistoryQueueCapacity*unsafe.Sizeof(shredHistoryInput{}) +
		generations + 2*shredHistoryChunkRows*unsafe.Sizeof(ShredHistoryRow{}) +
		4*shredHistoryChunks*unsafe.Sizeof(ShredHistoryChunk{}) +
		4*shredHistorySources*unsafe.Sizeof(ShredHistorySource{}) +
		4*3*shredHistoryRanges*unsafe.Sizeof(ShredHistoryRange{}) +
		metadataBytes +
		shredHistoryChunkRows*unsafe.Sizeof(shredHistoryWireRow{}) +
		3*shredHistoryChunkBytes + 1<<20
	if budget > 32<<20 {
		t.Fatalf("owned-layout bound %d exceeds 32 MiB", budget)
	}
	t.Logf("input=%d row=%d maximum_chunk=%d retained_encoded=%d three_generation_rows_and_bodies=%d metadata=%d owned_layout_bound=%d", unsafe.Sizeof(shredHistoryInput{}), unsafe.Sizeof(ShredHistoryRow{}), len(h.chunks[0].Body()), h.encodedBytes, generations, metadataBytes, budget)
}
