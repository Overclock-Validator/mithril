package dashboard

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	replayDetailCollectionLimit = 12 * time.Second
	// Poll between source captures and keep advertised generations until their
	// original leases expire, including when source and reader phases differ.
	replayDetailReaderInterval    = 5 * time.Second
	replayDetailReaderGenerations = 4
)

type replayDetailCursor struct {
	core       ReplayDetailCore
	generation string
	missed     uint64
	gap        *ReplayDetailGap
}

type replayDetailCandidate struct {
	cursor           replayDetailCursor
	before           replayDetailGate
	requestStarted   time.Time
	preparedAt       time.Time
	sourceFreshUntil time.Time
	public           *replayDetailPublicFamily
}

// replayDetailReader owns one collection loop and a separate, fixed Unix client.
// Its source deadline is local request-start plus the advertised remainder, not
// an attempted reconstruction of the producer's monotonic capture clock.
type replayDetailReader struct {
	client   *http.Client
	gate     *atomic.Pointer[replayDetailGate]
	done     chan struct{}
	limit    *rate.Limiter
	active   chan struct{}
	rooted   *rootedHistoryReader
	held     *heldScheduleReader
	selected *selectedWindowReader
	// ponytail: one short lock and four holders; change only if measured
	// contention warrants it. Network, validation, encoding and writes stay out.
	mu      sync.RWMutex
	cursor  *replayDetailCursor
	items   [replayDetailReaderGenerations]*replayDetailPublicFamily
	failure *replayDetailCaptureFailure
}

// newReplayDetailReader requires a trusted factory's fixed Unix DialContext.
// It cannot prove a function's destination, but it owns and bounds its cloned
// transport without copying caller cookies or mutating the status client.
func newReplayDetailReader(client *http.Client, gate *atomic.Pointer[replayDetailGate]) (*replayDetailReader, error) {
	if client == nil || gate == nil {
		return nil, errors.New("replay detail reader requires a client and status gate")
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport == nil || transport.DialContext == nil {
		return nil, errors.New("replay detail reader requires a fixed Unix transport")
	}
	return &replayDetailReader{
		client: detailHTTPClient(transport, 2, time.Second),
		gate:   gate, done: make(chan struct{}), limit: rate.NewLimiter(16, 32), active: make(chan struct{}, 4),
	}, nil
}

func (r *replayDetailReader) run(ctx context.Context) {
	defer close(r.done)
	defer r.client.CloseIdleConnections()
	first := true
	// Related families retain their normal cadence when replay is polled sooner.
	var nextRelated time.Time
	for ctx.Err() == nil {
		r.collect(ctx)
		if !time.Now().Before(nextRelated) {
			if r.rooted != nil {
				r.rooted.collect(ctx)
			}
			if r.held != nil {
				r.held.collect(ctx)
			}
			if r.selected != nil {
				r.selected.collect(ctx)
			}
			nextRelated = time.Now().Add(replayDetailCaptureInterval)
		}
		delay := replayDetailReaderInterval
		if first {
			first = false
			r.mu.RLock()
			waiting := r.cursor == nil
			r.mu.RUnlock()
			if r.selected != nil {
				r.selected.mu.Lock()
				waiting = waiting || r.selected.last == nil && (r.selected.failure == nil || r.selected.failure.reason != "unsupported_source")
				r.selected.mu.Unlock()
			}
			if waiting {
				// The node's first detail capture may still be in progress.
				// Retry once promptly, then retain the normal bounded cadence.
				delay = time.Second
				nextRelated = time.Time{}
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// fetch is used only by the collection loop's fixed manifest/two page workers.
func (r *replayDetailReader) fetch(ctx context.Context, query string) ([]byte, time.Duration, time.Time, string) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://node/details/replay"+query, nil)
	if err != nil {
		return nil, 0, time.Time{}, "source_invalid"
	}
	started := time.Now()
	response, err := r.client.Do(req)
	if err != nil {
		return nil, 0, started, "source_unavailable"
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusNotFound && query == "" {
			return nil, 0, started, "unsupported_source"
		}
		if response.StatusCode == http.StatusConflict {
			return nil, 0, started, "source_generation_expired"
		}
		return nil, 0, started, "source_unavailable"
	}
	if response.ContentLength > MaxSnapshotBytes {
		return nil, 0, started, "response_limit"
	}
	var remaining time.Duration
	if query == "" {
		values := response.Header.Values(replayDetailRemainingHeader)
		if len(values) != 1 {
			return nil, 0, started, "source_invalid"
		}
		milliseconds, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil || milliseconds < 13000 || milliseconds > 30000 || strconv.FormatInt(milliseconds, 10) != values[0] {
			return nil, 0, started, "source_invalid"
		}
		remaining = time.Duration(milliseconds) * time.Millisecond
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxSnapshotBytes+1))
	if len(body) > MaxSnapshotBytes {
		return nil, 0, started, "response_limit"
	}
	if err != nil {
		return nil, 0, started, "source_unavailable"
	}
	return body, remaining, started, ""
}

func (r *replayDetailReader) collect(parent context.Context) {
	if parent.Err() != nil {
		return
	}
	before := r.gate.Load()
	if before == nil {
		return
	}
	fail := func(reason string) {
		if parent.Err() == nil {
			r.mu.Lock()
			r.failure = &replayDetailCaptureFailure{gate: *before, reason: reason}
			r.mu.Unlock()
		}
	}
	if reason := before.failure(time.Now()); reason != "" {
		fail(reason)
		return
	}
	if before.State != "fresh" {
		fail("status_stale")
		return
	}
	if !before.stamp.valid() || before.retired || before.stopping {
		fail("waiting_source")
		return
	}
	ctx, cancel := context.WithTimeout(parent, replayDetailCollectionLimit)
	defer cancel()
	body, remaining, started, reason := r.fetch(ctx, "")
	if reason != "" {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			reason = "collection_deadline"
		}
		fail(reason)
		return
	}
	manifest, err := decodeReplayDetailManifest(body)
	if err != nil {
		fail("source_invalid")
		return
	}
	if manifest.State != "fresh" {
		fail(manifest.Reason)
		return
	}
	if !before.matches(manifest.Binding) {
		fail("binding_changed")
		return
	}
	records := make([]ReplayDetailRecord, manifest.Count)
	var received atomic.Int64
	received.Store(int64(len(body)))
	jobs := make(chan int, manifest.PageCount)
	for page := range manifest.PageCount {
		jobs <- page
	}
	close(jobs)
	failures := make(chan string, 1)
	var workers sync.WaitGroup
	for range min(2, manifest.PageCount) {
		workers.Go(func() {
			for page := range jobs {
				if ctx.Err() != nil {
					return
				}
				query := "?generation=" + manifest.Generation + "&page=" + strconv.Itoa(page)
				body, _, _, reason := r.fetch(ctx, query)
				if reason == "" && received.Add(int64(len(body))) > replayDetailFamilyBytes {
					reason = "response_limit"
				}
				if reason == "" {
					value, err := decodeReplayDetailPage(body, manifest, page)
					if err != nil {
						reason = "source_invalid"
					} else {
						copy(records[value.Offset:], value.Records)
					}
				}
				if reason != "" {
					select {
					case failures <- reason:
					default:
					}
					cancel()
					return
				}
			}
		})
	}
	workers.Wait()
	select {
	case reason := <-failures:
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			reason = "collection_deadline"
		}
		fail(reason)
		return
	default:
	}
	if ctx.Err() != nil {
		fail("collection_deadline")
		return
	}
	core, err := replayDetailCoreFromRecords(manifest, records)
	if err != nil {
		fail("source_invalid")
		return
	}
	// Request start, not header receipt, subtracts response delivery and every
	// later fetch/validation delay from the producer's advertised remainder.
	candidate, reason := r.prepare(core, manifest.Generation, *before, started, started.Add(remaining), time.Now())
	if reason != "" {
		fail(reason)
		return
	}
	if ctx.Err() != nil {
		fail("collection_deadline")
		return
	}
	if reason := r.commit(candidate, time.Now()); reason != "" {
		fail(reason)
	}
}

// prepare and commit have one background owner; HTTP never calls either.
func (r *replayDetailReader) prepare(core ReplayDetailCore, generation string, before replayDetailGate, started, deadline, now time.Time) (*replayDetailCandidate, string) {
	if now.Before(started) || !now.Before(deadline) || core.CapturedAt.After(now.Add(time.Second)) {
		return nil, "source_time_invalid"
	}
	r.mu.RLock()
	previous := r.cursor
	r.mu.RUnlock()
	cursor := replayDetailCursor{core: core, generation: generation}
	if previous != nil && sameReplayDetailBinding(previous.core.Binding, core.Binding) {
		if prior := previous.core.NearReception; prior != nil && (core.NearReception == nil || *prior != *core.NearReception) {
			return nil, "source_invalid"
		}
		if previous.core.LastSequence > core.LastSequence {
			return nil, "source_invalid"
		}
		first, last := max(previous.core.FirstSequence, core.FirstSequence), min(previous.core.LastSequence, core.LastSequence)
		if previous.core.Count != 0 && core.Count != 0 {
			for sequence := first; sequence <= last; sequence++ {
				if !replayDetailRecordsEqual(previous.core.Records[sequence-previous.core.FirstSequence], core.Records[sequence-core.FirstSequence]) {
					return nil, "source_invalid"
				}
				if sequence == math.MaxUint64 {
					break
				}
			}
		}
		cursor.missed, cursor.gap = previous.missed, previous.gap
		if core.Count != 0 && previous.core.LastSequence != math.MaxUint64 && core.FirstSequence > previous.core.LastSequence+1 {
			difference := core.FirstSequence - previous.core.LastSequence - 1
			if difference > math.MaxUint64-cursor.missed {
				return nil, "source_invalid"
			}
			cursor.missed += difference
			cursor.gap = &ReplayDetailGap{First: previous.core.LastSequence + 1, Last: core.FirstSequence - 1}
		}
	}
	// Own the accepted cursor independently of decoded/caller storage.
	cursor.core.Records = cloneReplayDetailRecords(core.Records)
	cursor.core.ScheduleWindow = cloneHeldSchedule(core.ScheduleWindow)
	cursor.core.FoldCapacity = cloneReplayFoldCapacity(core.FoldCapacity)
	cursor.core.NearReception = cloneReplayNearReception(core.NearReception)
	cursor.core.LatestSelected = cloneReplayLatestSelected(core.LatestSelected)
	cursor.core.RecentSelected = cloneReplayRecentSelected(core.RecentSelected)
	if core.Binding.GenesisHash != nil {
		value := *core.Binding.GenesisHash
		cursor.core.Binding.GenesisHash = &value
	}
	if cursor.gap != nil {
		gap := *cursor.gap
		cursor.gap = &gap
	}
	candidate := &replayDetailCandidate{cursor: cursor, before: before, requestStarted: started, preparedAt: now, sourceFreshUntil: deadline}
	if deadline.Sub(now) >= replayDetailMinimum {
		family, err := prepareReplayDetailPublic(cursor, now)
		if err != nil {
			return nil, "response_limit"
		}
		candidate.public = family
	}
	return candidate, ""
}

func (r *replayDetailReader) commit(c *replayDetailCandidate, now time.Time) string {
	if c == nil || now.Before(c.requestStarted) || now.Before(c.preparedAt) || !now.Before(c.sourceFreshUntil) || c.cursor.core.CapturedAt.After(now.Add(time.Second)) {
		return "source_time_invalid"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	gate := r.gate.Load()
	if gate == nil {
		return "waiting_source"
	}
	if reason := gate.failure(now); reason != "" {
		return reason
	}
	if gate.State != "fresh" {
		return "status_stale"
	}
	if !sameReplayDetailStatusContext(c.before, *gate) || !gate.matches(c.cursor.core.Binding) || gate.retired || gate.stopping {
		return "binding_changed"
	}
	if r.cursor != nil && r.cursor.generation == c.cursor.generation {
		for i, family := range r.items {
			if family == nil || family.generation != c.cursor.generation {
				continue
			}
			deadline := family.sourceFreshUntil
			if c.sourceFreshUntil.Before(deadline) {
				deadline = c.sourceFreshUntil
			}
			if family.stamp != gate.stamp || deadline.Before(family.sourceFreshUntil) {
				updated := *family
				updated.stamp, updated.sourceFreshUntil = gate.stamp, deadline
				r.items[i] = &updated
			}
			break
		}
		r.failure = nil
		return ""
	}
	var retained [replayDetailReaderGenerations]*replayDetailPublicFamily
	n := 0
	for _, family := range r.items {
		if family != nil && now.Before(family.holdUntil) && sameReplayDetailBinding(family.binding, c.cursor.core.Binding) {
			retained[n], n = family, n+1
		}
	}
	if c.public != nil && c.sourceFreshUntil.Sub(now) >= replayDetailMinimum {
		if n == len(retained) {
			return "generation_not_ready"
		}
		family := *c.public
		family.pages = slices.Clone(c.public.pages)
		if family.binding.GenesisHash != nil {
			value := *family.binding.GenesisHash
			family.binding.GenesisHash = &value
		}
		family.publishedAt, family.holdUntil, family.sourceFreshUntil, family.stamp = now, now.Add(replayDetailHoldFor), c.sourceFreshUntil, gate.stamp
		retained[n] = &family
	}
	// prepare owns its cursor copy; detach again from the caller-retained
	// candidate at this publication boundary, as with encoded holder ownership.
	cursor := c.cursor
	cursor.core.Records = cloneReplayDetailRecords(cursor.core.Records)
	cursor.core.ScheduleWindow = cloneHeldSchedule(cursor.core.ScheduleWindow)
	cursor.core.FoldCapacity = cloneReplayFoldCapacity(cursor.core.FoldCapacity)
	cursor.core.NearReception = cloneReplayNearReception(cursor.core.NearReception)
	cursor.core.LatestSelected = cloneReplayLatestSelected(cursor.core.LatestSelected)
	cursor.core.RecentSelected = cloneReplayRecentSelected(cursor.core.RecentSelected)
	if cursor.core.Binding.GenesisHash != nil {
		value := *cursor.core.Binding.GenesisHash
		cursor.core.Binding.GenesisHash = &value
	}
	if cursor.gap != nil {
		gap := *cursor.gap
		cursor.gap = &gap
	}
	r.cursor, r.items, r.failure = &cursor, retained, nil
	return ""
}
