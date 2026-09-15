package turbine

// Temporary, opt-in pipeline diagnostics. No transaction bytes or keys are logged.
// Timestamps are monotonic nanoseconds relative to origin_unix_ns. Worker elapsed
// time includes descheduling; summed job durations are NOT wall-clock critical paths.
import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
)

var entryTraceOrigin = time.Now()
var entryTraceDropped atomic.Uint64
var entryTraceConfig = configureEntryTrace()

type entryTraceSettings struct {
	modulo  uint64
	until   time.Time
	reports chan entryPipelineReport
}

func configureEntryTrace() entryTraceSettings {
	n, err := strconv.ParseUint(os.Getenv("MITHRIL_ENTRY_TRACE_MOD"), 10, 32)
	if err != nil || n == 0 {
		return entryTraceSettings{}
	}
	seconds, err := strconv.Atoi(os.Getenv("MITHRIL_ENTRY_TRACE_SECONDS"))
	if err != nil || seconds < 1 || seconds > 1800 {
		seconds = 600
	}
	path := os.Getenv("MITHRIL_ENTRY_TRACE_FILE")
	if path == "" {
		return entryTraceSettings{}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "entry pipeline trace disabled: %v\n", err)
		return entryTraceSettings{}
	}
	ch := make(chan entryPipelineReport, 8)
	go func() {
		defer f.Close()
		enc := json.NewEncoder(f)
		for r := range ch {
			r.Dropped = entryTraceDropped.Load()
			r.finish()
			if err := enc.Encode(r); err != nil {
				entryTraceDropped.Add(1)
			}
		}
	}()
	return entryTraceSettings{n, time.Now().Add(time.Duration(seconds) * time.Second), ch}
}

func entryTraceNow() int64             { return time.Since(entryTraceOrigin).Nanoseconds() }
func entryTraceTime(t time.Time) int64 { return t.Sub(entryTraceOrigin).Nanoseconds() }

type entryTraceContextKey struct{}

func withEntryPipelineTrace(ctx context.Context, t *entryPipelineTrace) context.Context {
	if t == nil {
		return ctx
	}
	return context.WithValue(ctx, entryTraceContextKey{}, true)
}
func entryTraceContext(ctx context.Context) bool {
	return ctx != nil && ctx.Value(entryTraceContextKey{}) == true
}

type entryPipelineTrace struct {
	arrivals   map[uint32]int64
	discovered map[uint32]int64
	sealed     bool // frozen at first completion claim, including retry/error paths
}

// Called only after successful admission, under the assembler lock. Includes
// FEC-reconstructed data. This is local availability, not a NIC timestamp.
func (s *slotState) traceAcceptedShred(sh *Shred) {
	if sh.Type != ShredTypeData {
		return
	}
	if s.pipelineTrace == nil {
		c := entryTraceConfig
		if c.modulo == 0 || s.slot%c.modulo != 0 || time.Now().After(c.until) {
			return
		}
		s.pipelineTrace = &entryPipelineTrace{arrivals: make(map[uint32]int64), discovered: make(map[uint32]int64)}
	}
	if !s.pipelineTrace.sealed {
		s.pipelineTrace.arrivals[sh.Index] = entryTraceNow()
	}
}

type entryVerificationTrace struct {
	Transactions int   `json:"transactions"`
	Submit       int64 `json:"submit_ns"`
	Admitted     int64 `json:"admitted_ns"`
	FirstWorker  int64 `json:"first_worker_ns"`
	LastWorker   int64 `json:"last_worker_ns"`
	Finished     int64 `json:"finished_ns"`
	Jobs         int   `json:"jobs"`
	JobWaitSum   int64 `json:"job_offer_to_start_sum_ns"`
	JobWaitMax   int64 `json:"job_offer_to_start_max_ns"`
	WorkerSum    int64 `json:"worker_elapsed_sum_ns"`
	WorkerMax    int64 `json:"worker_elapsed_max_ns"`
}

func (t *entryVerificationTrace) observe(j *transactionVerifyJob) {
	if t.FirstWorker == 0 || j.workerStart < t.FirstWorker {
		t.FirstWorker = j.workerStart
	}
	t.LastWorker = max(t.LastWorker, j.workerEnd)
	t.Jobs++
	wait, work := j.workerStart-j.offeredAt, j.workerEnd-j.workerStart
	t.JobWaitSum += wait
	t.JobWaitMax = max(t.JobWaitMax, wait)
	t.WorkerSum += work
	t.WorkerMax = max(t.WorkerMax, work)
}

type entryBatchTraceReport struct {
	Start             uint32                  `json:"start"`
	End               uint32                  `json:"end"`
	Transactions      int                     `json:"transactions"`
	Retained          bool                    `json:"retained"`
	Prefetched        bool                    `json:"prefetched"`
	AvailabilityKnown bool                    `json:"availability_known"`
	Available         int64                   `json:"available_ns"`
	Discovered        int64                   `json:"discovered_ns"`
	DecodeStart       int64                   `json:"decode_start_ns"`
	DecodeEnd         int64                   `json:"decode_end_ns"`
	Verification      *entryVerificationTrace `json:"verification,omitempty"`
}

type entryPipelineReport struct {
	Origin          int64                   `json:"origin_unix_ns"`
	Slot            uint64                  `json:"slot"`
	Transactions    int                     `json:"transactions"`
	Full            int64                   `json:"full_ns"`
	CompletionStart int64                   `json:"completion_start_ns"`
	Ready           int64                   `json:"ready_ns"`
	Dropped         uint64                  `json:"dropped_reports"`
	Batches         []entryBatchTraceReport `json:"batches"`
	Fallback        *entryVerificationTrace `json:"fallback,omitempty"`
	source          *entryPipelineTrace
	all, retained   []*prefetchedShredBatch
	fallback        *transactionVerification
}

func completedEntryVerification(r *transactionVerification) *entryVerificationTrace {
	if r == nil {
		return nil
	}
	select {
	case <-r.done:
		return r.trace
	default:
		return nil
	}
}

// All source maps are sealed, and decode has joined all preparation readers.
// Reading request metrics additionally requires the verification done barrier.
func (r *entryPipelineReport) finish() {
	retained := make(map[*prefetchedShredBatch]bool, len(r.retained))
	for _, b := range r.retained {
		retained[b] = true
	}
	for _, b := range r.all {
		row := entryBatchTraceReport{Start: b.start, End: b.end, Retained: retained[b], Prefetched: b.ready != nil,
			DecodeStart: b.traceDecodeStart, DecodeEnd: b.traceDecodeEnd, Discovered: r.source.discovered[b.start],
			AvailabilityKnown: true, Verification: completedEntryVerification(b.verification)}
		for _, e := range b.entries {
			row.Transactions += len(e.Txns)
		}
		// Require the preceding DATA_COMPLETE boundary as well as every shred in
		// this batch. An end marker alone cannot establish an independent start.
		start := b.start
		if start > 0 {
			start--
		}
		for i := start; i <= b.end; i++ {
			at, ok := r.source.arrivals[i]
			if !ok {
				row.AvailabilityKnown = false
			}
			row.Available = max(row.Available, at)
		}
		r.Batches = append(r.Batches, row)
	}
	r.Fallback = completedEntryVerification(r.fallback)
}

func queueEntryPipelineReport(s *slotState, b *block.Block, d *entryDecodeTimings, start, ready time.Time) {
	if s.pipelineTrace == nil || len(b.Transactions) < 10000 || entryTraceConfig.reports == nil {
		return
	}
	r := entryPipelineReport{Origin: entryTraceOrigin.UnixNano(), Slot: s.slot, Transactions: len(b.Transactions),
		Full: entryTraceTime(s.fullAt), CompletionStart: entryTraceTime(start), Ready: entryTraceTime(ready),
		source: s.pipelineTrace, all: d.all, retained: d.retained, fallback: d.traceFallback}
	select {
	case entryTraceConfig.reports <- r:
	default:
		entryTraceDropped.Add(1)
	}
}
