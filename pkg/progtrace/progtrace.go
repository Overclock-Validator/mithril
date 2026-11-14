// Package progtrace is a record-only diagnostic of program readiness: when the
// account prefetch read a block's accounts and preloaded its programs, when
// replay loaded and began the block, and every program compile with why the
// program cache did not serve it and whether the compiled program was admitted
// to the cache.
//
// It is off unless MITHRIL_PROGRAM_READINESS_LOG names a file, to which it
// writes one JSON object per line. Every time is nanoseconds since one process
// origin on the monotonic clock. Recording never waits: a record that does not
// fit the queue, or comes after the record limit
// (MITHRIL_PROGRAM_READINESS_MAX, default 2,000,000), is counted and dropped,
// and the counts are written as "drops" records. It reads no cache and takes
// no lock replay takes.
package progtrace

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
)

const (
	defaultQueue      = 1 << 16
	defaultMaxRecords = 2_000_000
	// maxTrackedKeys bounds the admission history (see NoteAdmit).
	maxTrackedKeys = 1 << 16
)

var origin = time.Now()

// Now returns nanoseconds since the process origin.
func Now() int64 { return int64(time.Since(origin)) }

// At converts a time taken with time.Now to nanoseconds since the origin.
func At(t time.Time) int64 { return int64(t.Sub(origin)) }

type recorder struct {
	queue      chan any
	max        int64
	taken      atomic.Int64 // records accepted against max
	dropped    atomic.Uint64
	over       atomic.Uint64
	quit       chan struct{}
	done       chan struct{}
	closing    atomic.Bool
	emitting   atomic.Int64
	stopOnce   sync.Once
	err        error // published by closing done
	closer     io.Closer
	completion bool

	mu                         sync.Mutex
	admitted                   map[[32]byte]admission
	untrack                    uint64
	cacheSequence, cacheActive uint64
}

type admission struct {
	at, invalidatedAt int64
	source            uint64
	ambiguous         bool
}

var current atomic.Pointer[recorder]
var startupErr error

func init() {
	path := os.Getenv("MITHRIL_PROGRAM_READINESS_LOG")
	if path == "" {
		return
	}
	f, err := os.Create(path)
	if err != nil {
		startupErr = fmt.Errorf("program readiness trace: %w", err)
		return
	}
	limit := int64(defaultMaxRecords)
	if n, err := strconv.ParseInt(os.Getenv("MITHRIL_PROGRAM_READINESS_MAX"), 10, 64); err == nil && n > 0 {
		limit = n
	}
	startRecorder(f, defaultQueue, limit, f, true)
}

// On reports whether records are being taken. Callers test it before building
// a record, so the diagnostic costs one atomic load when off.
func On() bool { return current.Load() != nil }

// Start begins recording to w and returns a function that stops it, writes
// what is queued and the final drop counts, and waits for the writer. For the
// environment switch and tests.
func Start(w io.Writer, queue int, maxRecords int64) (stop func()) {
	r := startRecorder(w, queue, maxRecords, nil, false)
	return func() { _ = r.stopWithin(30 * time.Second) }
}

func startRecorder(w io.Writer, queue int, maxRecords int64, closer io.Closer, completion bool) *recorder {
	r := &recorder{queue: make(chan any, queue), max: maxRecords, quit: make(chan struct{}), done: make(chan struct{}), admitted: make(map[[32]byte]admission), closer: closer, completion: completion}
	current.Store(r)
	go r.write(w)
	return r
}

// StartupError is non-nil when the explicitly requested trace could not open.
func StartupError() error { return startupErr }

// Finish cuts off producers, drains accepted records and reports write/close
// errors. The CLI calls it after command cleanup and before choosing its exit
// status. Missing completion, nonzero exit, or a returned error means the
// diagnostic is incomplete. No transaction-path call waits for the writer.
func Finish() error {
	if r := current.Load(); r != nil {
		return r.stopWithin(30 * time.Second)
	}
	return startupErr
}

func (r *recorder) stopWithin(timeout time.Duration) error {
	r.stopOnce.Do(func() {
		r.closing.Store(true)
		current.CompareAndSwap(r, nil)
		// Emit is nonblocking. Emitters that already passed the cutoff must
		// finish their queue operation before the writer performs its drain.
		go func() {
			for r.emitting.Load() != 0 {
				runtime.Gosched()
			}
			close(r.quit)
		}()
	})
	select {
	case <-r.done:
		return r.err
	case <-time.After(timeout):
		return fmt.Errorf("program readiness trace: graceful drain exceeded %s", timeout)
	}
}

// Completion is emitted only for the environment-owned trace at a graceful
// CLI exit. Count only Emit payloads in Written; drops/completion are metadata.
type Completion struct {
	Kind      string `json:"kind"`
	Version   int    `json:"format_version"`
	T         int64  `json:"t_ns"`
	Clean     bool   `json:"clean"`
	Attempted int64  `json:"attempted_records"`
	Written   uint64 `json:"written_records"`
	QueueFull uint64 `json:"queue_full"`
	OverLimit uint64 `json:"over_limit"`
	Untracked uint64 `json:"untracked_admissions"`
}

// Drops is the "drops" record: records lost to a full queue and to the limit.
type Drops struct {
	Kind      string `json:"kind"`
	T         int64  `json:"t_ns"`
	QueueFull uint64 `json:"queue_full"`
	OverLimit uint64 `json:"over_limit"`
	// Untracked counts admissions not remembered because the admission
	// history was full; a later miss of such a key carries no prior admission.
	Untracked uint64 `json:"untracked_admissions"`
}

func (r *recorder) write(w io.Writer) {
	var firstErr error
	defer func() { r.err = firstErr; close(r.done) }()
	out := bufio.NewWriterSize(w, 1<<16)
	enc := json.NewEncoder(out)
	encode := func(rec any) bool {
		if err := enc.Encode(rec); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			return false
		}
		return true
	}
	flush := func() {
		if err := out.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	var written uint64
	payload := func(rec any) {
		if encode(rec) {
			written++
		}
	}
	var reported [3]uint64
	drops := func() {
		r.mu.Lock()
		untracked := r.untrack
		r.mu.Unlock()
		now := [3]uint64{r.dropped.Load(), r.over.Load(), untracked}
		if now != reported {
			reported = now
			encode(Drops{Kind: "drops", T: Now(), QueueFull: now[0], OverLimit: now[1], Untracked: now[2]})
		}
	}
	for {
		select {
		case rec := <-r.queue:
			payload(rec)
			continue
		default:
		}
		// Idle: report drops and flush, so the file is complete without a
		// shutdown hook, then wait for the next record.
		drops()
		flush()
		select {
		case rec := <-r.queue:
			payload(rec)
		case <-r.quit:
			// Accepted emitters have quiesced; nothing can enqueue after this.
			for {
				select {
				case rec := <-r.queue:
					payload(rec)
					continue
				default:
				}
				break
			}
			drops()
			r.mu.Lock()
			active, untracked := r.cacheActive, r.untrack
			r.mu.Unlock()
			if active != 0 && firstErr == nil {
				firstErr = fmt.Errorf("program readiness trace: %d cache mutations still active at finish", active)
			}
			if r.completion && firstErr == nil {
				encode(Completion{Kind: "complete", Version: 2, T: Now(), Clean: true,
					Attempted: r.taken.Load(), Written: written, QueueFull: r.dropped.Load(),
					OverLimit: r.over.Load(), Untracked: untracked})
			}
			flush()
			if r.closer != nil {
				if err := r.closer.Close(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
			return
		}
	}
}

// Emit queues one record (a struct of this package) without waiting.
func Emit(rec any) {
	r := current.Load()
	if r == nil {
		return
	}
	r.emitting.Add(1)
	defer r.emitting.Add(-1)
	if r.closing.Load() {
		return
	}
	if r.taken.Add(1) > r.max {
		r.over.Add(1)
		return
	}
	select {
	case r.queue <- rec:
	default:
		r.dropped.Add(1)
	}
}

// SourceDigest identifies program source bytes without exposing them.
func SourceDigest(source []byte) uint64 { return xxhash.Sum64(source) }

// Hex formats a digest for a record; zero (unknown) is the empty string.
func Hex(digest uint64) string {
	if digest == 0 {
		return ""
	}
	var b [8]byte
	for i := range b {
		b[i] = byte(digest >> (56 - 8*i))
	}
	return hex.EncodeToString(b[:])
}

// NoteAdmit remembers that the program cache admitted an entry for key built
// from source (a SourceDigest, or 0 when unknown) at time at.
func NoteAdmit(key [32]byte, source uint64, at int64) {
	r := current.Load()
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closing.Load() {
		r.mu.Unlock()
		return
	}
	if _, ok := r.admitted[key]; ok || len(r.admitted) < maxTrackedKeys {
		// Unbracketed legacy notes cannot establish cache-operation order.
		r.admitted[key] = admission{at: at, source: source, ambiguous: true}
	} else {
		r.untrack++
	}
	r.cacheSequence++
	r.mu.Unlock()
}

// NoteInvalidate remembers an explicit removal of key's entry.
func NoteInvalidate(key [32]byte, at int64) {
	r := current.Load()
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closing.Load() {
		r.mu.Unlock()
		return
	}
	if a, ok := r.admitted[key]; ok {
		a.invalidatedAt = at
		a.ambiguous = true
		r.admitted[key] = a
	}
	r.cacheSequence++
	r.mu.Unlock()
}

// NoteInvalidateAll remembers that the whole cache was cleared.
func NoteInvalidateAll(at int64) {
	r := current.Load()
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closing.Load() {
		r.mu.Unlock()
		return
	}
	for key, a := range r.admitted {
		a.invalidatedAt = at
		a.ambiguous = true
		r.admitted[key] = a
	}
	r.cacheSequence++
	r.mu.Unlock()
}

// PriorAdmit returns the last admission remembered for key: when, from which
// source, and when it was explicitly invalidated afterwards (0 if it was not).
// This accessor is for inspecting history, not attributing a cache miss.
// Attribute only from BeginLookup/EndLookup's lookup-time, race-marked snapshot.
func PriorAdmit(key [32]byte) (at int64, source uint64, invalidatedAt int64, ok bool) {
	r := current.Load()
	if r == nil {
		return 0, 0, 0, false
	}
	r.mu.Lock()
	a, ok := r.admitted[key]
	r.mu.Unlock()
	return a.at, a.source, a.invalidatedAt, ok
}

// Job is one account-prefetch job from request to outcome.
type Job struct {
	Kind string `json:"kind"` // "job"
	Slot uint64 `json:"slot"`
	ID   uint64 `json:"job"`
	// Whole is a whole-block job; otherwise a tip-warming batch group.
	Whole bool `json:"whole"`
	// Requested is when the block was offered (0 for tip warming); Started when
	// the worker began the job; ReadDone when its account read returned;
	// PreloadDone when program preloading returned. A step not reached is 0.
	Requested   int64 `json:"t_requested_ns"`
	Started     int64 `json:"t_started_ns"`
	ReadDone    int64 `json:"t_read_done_ns"`
	PreloadDone int64 `json:"t_preload_done_ns"`
	// Outcome: finished, late_at_offer, no_refs, queue_full, late_in_queue,
	// read_error, cancelled, never_started; Preload: done, no_program_cache,
	// no_features, program_read_error, cancelled, over_budget, not_reached.
	Outcome string `json:"outcome"`
	Preload string `json:"preload"`
	// ReplayAt is the slot replay had most recently begun when the job started.
	ReplayAt uint64 `json:"replay_slot_at_start"`
	Keys     int    `json:"keys"`
	// Programs resolved to a source; Cached of them skipped because the cache
	// held some entry for the key (not compared with the source); Compiled
	// built; Admitted of those accepted by the cache, Refused not.
	Programs int `json:"programs"`
	Cached   int `json:"cached_uncompared"`
	Compiled int `json:"compiled"`
	Admitted int `json:"admitted"`
	Refused  int `json:"refused"`
}

// Foreground is a replay event for a slot: "turn" (replay began the slot),
// "fg_load" (its account load, T to End) or "fg_exec" (a transaction loop or
// streaming group began executing).
type Foreground struct {
	Kind string `json:"kind"`
	Slot uint64 `json:"slot"`
	T    int64  `json:"t_ns"`
	End  int64  `json:"t_end_ns,omitempty"`
	Path string `json:"path,omitempty"` // block or streaming
	Txs  int    `json:"txs,omitempty"`
}

// Compile is one program load, verify and cache publication.
type Compile struct {
	Kind string `json:"kind"` // "compile"
	Slot uint64 `json:"slot"`
	// Tx is the first signature of the transaction, empty for prefetch; Job is
	// the prefetch job. Executions of one transaction (early and canonical)
	// share Tx and differ in Exec and T.
	Tx    string `json:"tx,omitempty"`
	Job   uint64 `json:"job,omitempty"`
	Depth uint64 `json:"depth,omitempty"`
	Key   string `json:"key"`
	// Exec: canonical, work_ahead, simulation or prefetch.
	Exec    string `json:"exec"`
	Private bool   `json:"private_programs"`
	// Cache is what the lookup found: absent, source_mismatch,
	// feature_mismatch, source_and_feature_mismatch, or present_bypassed (an
	// entry existed and private loading ignored it, uncompared).
	Cache string `json:"cache"`
	T     int64  `json:"t_start_ns"`
	// CompileNs covers load, verify and source binding; worker time, which
	// overlaps other workers and is not wall time of the block.
	CompileNs int64  `json:"compile_ns"`
	Bytes     int    `json:"bytes"`
	Source    string `json:"source_digest"`
	Features  string `json:"features_id"`
	// Result: ok, load_error or verify_error. Publish: admitted, refused (the
	// cache declined the entry), skipped_speculative, skipped_simulation,
	// skipped_no_db, or not_reached.
	Result      string `json:"result"`
	Publish     string `json:"publish"`
	PublishedAt int64  `json:"t_published_ns,omitempty"`
	// Admission history captured at the actual lookup, not at compile end.
	// Do not infer eviction when HistoryAmbiguous is true or source is unknown.
	PriorAdmit       int64  `json:"prior_admit_ns,omitempty"`
	PriorSource      string `json:"prior_source_digest,omitempty"`
	PriorInvalidated int64  `json:"prior_invalidated_ns,omitempty"`
	LookupAt         int64  `json:"t_lookup_ns,omitempty"`
	HistoryKnown     bool   `json:"history_known_at_lookup"`
	HistoryAmbiguous bool   `json:"history_ambiguous"`
}

// Publish is a cache publication outside a compile-on-miss: a deploy or
// upgrade ("deploy"), or an explicit removal ("invalidate", "invalidate_all").
type Publish struct {
	Kind    string `json:"kind"`
	Slot    uint64 `json:"slot,omitempty"`
	T       int64  `json:"t_ns"`
	Key     string `json:"key,omitempty"`
	Publish string `json:"publish,omitempty"`
	Reason  string `json:"reason,omitempty"` // the calling function, for removals
}
