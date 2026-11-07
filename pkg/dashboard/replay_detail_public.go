package dashboard

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ReplayDetailGap is an observed gap between accepted reader collections, not
// a claim about unobserved startup history or every attempted replay bank.
type ReplayDetailGap struct {
	First uint64 `json:"first,string"`
	Last  uint64 `json:"last,string"`
}

// PublicReplayDetailManifest adds reader observations to immutable source facts.
// CollectorAt and RetainUntil belong to this reader; capture/update do not.
type PublicReplayDetailManifest struct {
	SchemaVersion    int                   `json:"schema_version"`
	Family           string                `json:"family"`
	Scope            string                `json:"scope"`
	State            string                `json:"state"`
	Reason           string                `json:"reason"`
	CollectorAt      time.Time             `json:"collector_at"`
	Binding          ReplayDetailBinding   `json:"binding"`
	Generation       string                `json:"generation"`
	CapturedAt       time.Time             `json:"captured_at"`
	RetainUntil      time.Time             `json:"retain_until"`
	UpdatedAt        time.Time             `json:"updated_at"`
	Capacity         int                   `json:"capacity"`
	Count            int                   `json:"count"`
	PageSize         int                   `json:"page_size"`
	PageCount        int                   `json:"page_count"`
	FirstSequence    uint64                `json:"first_sequence,string"`
	LastSequence     uint64                `json:"last_sequence,string"`
	Evicted          uint64                `json:"evicted,string"`
	Exhausted        bool                  `json:"exhausted"`
	ScheduleWindow   *HeldScheduleWindow   `json:"schedule_window,omitempty"`
	FoldCapacity     *ReplayFoldCapacity   `json:"fold_capacity,omitempty"`
	NearReception    *ReplayNearReception  `json:"near_reception,omitempty"`
	LatestSelected   *ReplayLatestSelected `json:"latest_selected,omitempty"`
	RecentSelected   *ReplayRecentSelected `json:"recent_selected,omitempty"`
	ReaderMissedRows uint64                `json:"reader_missed_rows,string"`
	LastReaderGap    *ReplayDetailGap      `json:"last_reader_gap"`
}

// PublicReplayDetailPage is re-encoded from validated typed records. Private
// response bytes are never forwarded to public viewers.
type PublicReplayDetailPage struct {
	SchemaVersion int                  `json:"schema_version"`
	Family        string               `json:"family"`
	Generation    string               `json:"generation"`
	Binding       ReplayDetailBinding  `json:"binding"`
	Page          int                  `json:"page"`
	Offset        int                  `json:"offset"`
	NextPage      *int                 `json:"next_page"`
	Records       []ReplayDetailRecord `json:"records"`
}

var replayDetailPublicReasons = [...]string{
	"", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid",
	"unsupported_source", "source_generation_expired", "binding_changed", "response_limit", "collection_deadline", "generation_not_ready",
}

type replayDetailPublicFamily struct {
	binding          ReplayDetailBinding
	generation       string
	manifests        [len(replayDetailPublicReasons)]string
	pages            []string
	publishedAt      time.Time
	holdUntil        time.Time
	sourceFreshUntil time.Time
	stamp            statusStamp
}

func prepareReplayDetailPublic(cursor replayDetailCursor, collectedAt time.Time) (*replayDetailPublicFamily, error) {
	c := cursor.core
	f := &replayDetailPublicFamily{binding: c.Binding, generation: cursor.generation}
	if c.Binding.GenesisHash != nil {
		value := *c.Binding.GenesisHash
		f.binding.GenesisHash = &value
	}
	pages := (c.Count + replayDetailPageSize - 1) / replayDetailPageSize
	f.pages = make([]string, pages)
	total := 0
	for page := range pages {
		offset := page * replayDetailPageSize
		var next *int
		if page+1 < pages {
			i := page + 1
			next = &i
		}
		body, err := json.Marshal(PublicReplayDetailPage{SchemaVersion: c.SchemaVersion, Family: "replay", Generation: cursor.generation,
			Binding: f.binding, Page: page, Offset: offset, NextPage: next,
			Records: c.Records[offset:min(offset+replayDetailPageSize, c.Count)]})
		if err != nil || len(body) > MaxSnapshotBytes {
			return nil, errors.New("response_limit")
		}
		f.pages[page], total = string(body), total+len(body)
	}
	m := PublicReplayDetailManifest{
		SchemaVersion: c.SchemaVersion, Family: "replay", Scope: "replay_row_history", State: "fresh",
		CollectorAt: collectedAt.UTC(), Binding: f.binding, Generation: cursor.generation,
		CapturedAt: c.CapturedAt, UpdatedAt: c.UpdatedAt, RetainUntil: collectedAt.Add(replayDetailHoldFor).UTC(),
		Capacity: c.Capacity, Count: c.Count, PageSize: replayDetailPageSize, PageCount: pages,
		FirstSequence: c.FirstSequence, LastSequence: c.LastSequence, Evicted: c.Evicted, Exhausted: c.Exhausted,
		ScheduleWindow:   c.ScheduleWindow,
		FoldCapacity:     c.FoldCapacity,
		NearReception:    c.NearReception,
		LatestSelected:   c.LatestSelected,
		RecentSelected:   c.RecentSelected,
		ReaderMissedRows: cursor.missed, LastReaderGap: cursor.gap,
	}
	for i, reason := range replayDetailPublicReasons {
		m.Reason = reason
		if i != 0 {
			m.State = "stale"
		}
		body, err := json.Marshal(m)
		if err != nil || len(body) > MaxSnapshotBytes {
			return nil, errors.New("response_limit")
		}
		f.manifests[i], total = string(body), total+len(body)
	}
	if total > replayDetailFamilyBytes {
		return nil, errors.New("response_limit")
	}
	return f, nil
}

func (r *replayDetailReader) lookup(rawQuery string, force bool, now time.Time) replayDetailReply {
	generation, page, pageRequest, ok := parseReplayDetailQuery(rawQuery, force)
	if !ok {
		return replayDetailFailure("invalid_request")
	}
	reply, _ := r.lookupPublicFamily(generation, page, pageRequest, false, now)
	return reply
}

// lookupPublicFamily holds only immutable cached strings after admission. Bundle
// selection uses the same generation, context and original deadlines as pages.
func (r *replayDetailReader) lookupPublicFamily(generation string, page int, pageRequest, bundle bool, now time.Time) (replayDetailReply, *replayDetailPublicFamily) {
	gate := r.gate.Load()
	if gate == nil {
		return replayDetailFailure("waiting_source"), nil
	}
	if reason := gate.failure(now); reason != "" {
		return replayDetailFailure(reason), nil
	}
	reason := ""
	if gate.State == "stale" {
		reason = gate.Reason
		if reason == "" {
			reason = "status_stale"
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var family *replayDetailPublicFamily
	for _, f := range r.items {
		if f != nil && (!(pageRequest || bundle) || f.generation == generation) {
			family = f
		}
	}
	if family == nil {
		if pageRequest || bundle {
			return replayDetailFailure("source_generation_expired"), nil
		}
		if reason != "" {
			return replayDetailFailure(reason), nil
		}
		if r.failure != nil && sameReplayDetailStatusContext(r.failure.gate, *gate) {
			return replayDetailReaderFailure(r.failure.reason), nil
		}
		return replayDetailFailure("waiting_source"), nil
	}
	if !gate.matches(family.binding) || !gate.stamp.valid() || gate.stamp != family.stamp {
		return replayDetailFailure("binding_changed"), nil
	}
	if now.Before(family.publishedAt) {
		return replayDetailFailure("source_time_invalid"), nil
	}
	if !now.Before(family.holdUntil) {
		if pageRequest || bundle {
			return replayDetailFailure("source_generation_expired"), nil
		}
		return replayDetailFailure("generation_not_ready"), nil
	}
	if pageRequest {
		if page >= len(family.pages) {
			return replayDetailFailure("invalid_request"), nil
		}
		return replayDetailReply{status: http.StatusOK, body: family.pages[page], failure: r.failure}, family
	}
	remaining := min(family.sourceFreshUntil.Sub(now), family.holdUntil.Sub(now))
	if remaining < replayDetailMinimum {
		return replayDetailFailure("generation_not_ready"), nil
	}
	if reason == "" && r.failure != nil && sameReplayDetailStatusContext(r.failure.gate, *gate) {
		reason = r.failure.reason
	}
	for i, variant := range replayDetailPublicReasons {
		if reason == variant {
			return replayDetailReply{status: http.StatusOK, body: family.manifests[i], remainingMilliseconds: remaining.Milliseconds(), failure: r.failure}, family
		}
	}
	return replayDetailFailure("source_invalid"), nil
}

func parseReplayDetailPublicQuery(raw string, force bool) (generation string, page int, pageRequest, bundle, ok bool) {
	if generation, page, pageRequest, ok = parseReplayDetailQuery(raw, force); ok {
		return
	}
	first, second, found := strings.Cut(raw, "&")
	if len(raw) > 128 || !found || strings.Contains(second, "&") {
		return "", 0, false, false, false
	}
	if second == "bundle=1" {
		first, second = second, first
	}
	if first != "bundle=1" || !strings.HasPrefix(second, "generation=") {
		return "", 0, false, false, false
	}
	generation, _, _, ok = parseReplayDetailQuery(second+"&page=0", force)
	return generation, 0, false, ok, ok
}

func replayDetailBundleBytes(manifest string, family *replayDetailPublicFamily) (int, bool) {
	// Both delimiters, one comma between adjacent immutable pages, and no newline.
	n := len(`{"manifest":`) + len(manifest) + len(`,"pages":[`) + len(`]}`)
	if family == nil || len(family.pages) > ReplayDetailCapacity/replayDetailPageSize {
		return 0, false
	}
	for i, body := range family.pages {
		if len(body) == 0 || len(body) > MaxSnapshotBytes {
			return 0, false
		}
		n += len(body)
		if i != 0 {
			n++
		}
	}
	return n, n <= replayDetailFamilyBytes
}

func (r *replayDetailReader) admitPublicFamily(family *replayDetailPublicFamily, failure *replayDetailCaptureFailure, page bool,
	admit func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool)) (time.Duration, bool) {
	if family == nil || admit == nil {
		return 0, false
	}
	return admit(family.stamp, func() (statusStamp, bool) {
		gate := r.gate.Load()
		if gate == nil {
			return statusStamp{}, false
		}
		r.mu.RLock()
		present := false
		for _, item := range r.items {
			if item == family || page && item != nil && item.generation == family.generation && item.stamp == family.stamp &&
				item.publishedAt.Equal(family.publishedAt) && item.holdUntil.Equal(family.holdUntil) {
				present = true
				break
			}
		}
		sameFailure := page || r.failure == failure
		r.mu.RUnlock()
		now := time.Now()
		return gate.stamp, present && sameFailure && !gate.retired && !gate.stopping && gate.State == "fresh" &&
			gate.failure(now) == "" && gate.matches(family.binding) && now.Before(family.holdUntil) &&
			(page || now.Before(family.sourceFreshUntil))
	})
}

func (r *replayDetailReader) serveBundle(w http.ResponseWriter, request *http.Request, generation string,
	admit func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool)) {
	reply, family := r.lookupPublicFamily(generation, 0, false, true, time.Now())
	if reply.status == http.StatusOK {
		if _, ok := replayDetailBundleBytes(reply.body, family); !ok {
			reply, family = replayDetailReaderFailure("response_limit"), nil
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if family != nil {
		// Repeat admission after preparing headers, without retaining a cache lock
		// across writes. A reset/expired generation cannot borrow the first check.
		latest, current := r.lookupPublicFamily(generation, 0, false, true, time.Now())
		if current != family {
			reply, family = latest, nil
			if reply.status == http.StatusOK {
				reply = replayDetailFailure("binding_changed")
			}
		} else {
			reply = latest
		}
	}
	if family == nil {
		w.WriteHeader(reply.status)
		_, _ = io.WriteString(w, reply.body)
		return
	}
	statusRemaining, admitted := r.admitPublicFamily(family, reply.failure, false, admit)
	reply.remainingMilliseconds = min(reply.remainingMilliseconds, time.Until(family.sourceFreshUntil).Milliseconds(), time.Until(family.holdUntil).Milliseconds())
	if !admitted || statusRemaining.Milliseconds() < 1 || reply.remainingMilliseconds < 1 {
		reply = replayDetailFailure("status_stale")
		w.WriteHeader(reply.status)
		_, _ = io.WriteString(w, reply.body)
		return
	}
	length, ok := replayDetailBundleBytes(reply.body, family)
	if !ok {
		reply = replayDetailReaderFailure("response_limit")
		w.WriteHeader(reply.status)
		_, _ = io.WriteString(w, reply.body)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(length))
	w.Header().Set(statusRemainingHeader, strconv.FormatInt(statusRemaining.Milliseconds(), 10))
	if reply.body == family.manifests[0] && reply.remainingMilliseconds > 0 {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(reply.remainingMilliseconds, 10))
	}
	w.WriteHeader(http.StatusOK)
	write := func(body string) bool {
		if request.Context().Err() != nil {
			return false
		}
		if _, ok := r.admitPublicFamily(family, reply.failure, false, admit); !ok {
			return false
		}
		_, err := io.WriteString(w, body)
		return err == nil
	}
	if !write(`{"manifest":`) || !write(reply.body) || !write(`,"pages":[`) {
		return
	}
	for i, body := range family.pages {
		if i != 0 && !write(",") || !write(body) {
			return
		}
	}
	write(`]}`)
}

func replayDetailReaderFailure(reason string) replayDetailReply {
	switch reason {
	case "collection_deadline":
		return replayDetailReply{status: 503, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"collection_deadline"}`}
	case "response_limit":
		return replayDetailCaptureFailureReply(reason)
	case "rate_limited":
		return replayDetailReply{status: 429, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"rate_limited"}`}
	default:
		return replayDetailFailure(reason)
	}
}

func (r *replayDetailReader) serveHTTP(w http.ResponseWriter, request *http.Request,
	admit func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool)) {
	reply := replayDetailFailure("invalid_request")
	var family *replayDetailPublicFamily
	generation, pageIndex, page, bundle, valid := parseReplayDetailPublicQuery(request.URL.RawQuery, request.URL.ForceQuery)
	if request.Method == http.MethodGet && request.URL.Path == "/api/v1/details/replay" && request.URL.EscapedPath() == "/api/v1/details/replay" &&
		request.ContentLength == 0 && len(request.TransferEncoding) == 0 && valid {
		if !r.limit.Allow() {
			reply = replayDetailReaderFailure("rate_limited")
		} else if page || bundle {
			select {
			case r.active <- struct{}{}:
				defer func() { <-r.active }()
				if bundle {
					r.serveBundle(w, request, generation, admit)
					return
				}
				reply, family = r.lookupPublicFamily(generation, pageIndex, true, false, time.Now())
			default:
				reply = replayDetailReaderFailure("rate_limited")
			}
		} else {
			reply, family = r.lookupPublicFamily(generation, pageIndex, false, false, time.Now())
		}
	}
	var statusRemaining time.Duration
	if reply.status == http.StatusOK {
		var ok bool
		statusRemaining, ok = r.admitPublicFamily(family, reply.failure, page, admit)
		if !page && family != nil {
			reply.remainingMilliseconds = min(reply.remainingMilliseconds, time.Until(family.sourceFreshUntil).Milliseconds(), time.Until(family.holdUntil).Milliseconds())
		}
		if !ok || statusRemaining.Milliseconds() < 1 || !page && reply.remainingMilliseconds < 1 {
			reply = replayDetailFailure("status_stale")
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if reply.status == http.StatusOK {
		w.Header().Set(statusRemainingHeader, strconv.FormatInt(statusRemaining.Milliseconds(), 10))
	}
	if reply.status == http.StatusOK && !page && family != nil && reply.body == family.manifests[0] && reply.remainingMilliseconds > 0 {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(reply.remainingMilliseconds, 10))
	}
	w.WriteHeader(reply.status)
	if _, err := io.WriteString(w, reply.body); err != nil {
		return
	}
}
