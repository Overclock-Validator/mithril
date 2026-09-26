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
	repairs chan entryRepairTrace
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
	repairs := make(chan entryRepairTrace, 256)
	go func() {
		defer f.Close()
		enc := json.NewEncoder(f)
		for {
			select {
			case r := <-repairs:
				r.Dropped = entryTraceDropped.Load()
				if err := enc.Encode(r); err != nil {
					entryTraceDropped.Add(1)
				}
			case r := <-ch:
				r.Dropped = entryTraceDropped.Load()
				r.finish()
				if err := enc.Encode(r); err != nil {
					entryTraceDropped.Add(1)
				}
			}
		}
	}()
	return entryTraceSettings{modulo: n, until: time.Now().Add(time.Duration(seconds) * time.Second), reports: ch, repairs: repairs}
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
	sources    map[uint32]entryShredSource
	discovered map[uint32]int64
	sealed     bool // frozen at first completion claim, including retry/error paths
}

// Called only after successful admission, under the assembler lock. Includes
// FEC-reconstructed data. This is local availability, not a NIC timestamp.
func (s *slotState) traceAcceptedShred(sh *Shred, source ...entryShredSource) {
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
		if _, exists := s.pipelineTrace.arrivals[sh.Index]; exists {
			return
		}
		s.pipelineTrace.arrivals[sh.Index] = entryTraceNow()
		if len(source) > 0 {
			if s.pipelineTrace.sources == nil {
				s.pipelineTrace.sources = make(map[uint32]entryShredSource)
			}
			s.pipelineTrace.sources[sh.Index] = source[0]
		}
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
	CriticalIndex     *uint32                 `json:"critical_shred_index,omitempty"`
	CriticalSource    *entryShredSource       `json:"critical_shred_source,omitempty"`
	CriticalTies      int                     `json:"critical_timestamp_ties"`
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
			if ok && (row.CriticalIndex == nil || at > row.Available) {
				index := i
				row.CriticalIndex = &index
				row.CriticalTies = 1
				row.CriticalSource = nil
				if source, exists := r.source.sources[i]; exists {
					row.CriticalSource = &source
				}
				row.Available = at
			} else if ok && at == row.Available {
				row.CriticalTies++
			}
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

// Attribution starts at assembler entry, not socket receipt.
// "non_repair" includes direct/spooled admission, not proof of socket origin.
// A recovered shred records the packet that triggered reconstruction; it is not itself a
// received repair response. Missing source fields in older reports mean unknown.
type entryShredSource struct {
	Path             string `json:"path"`
	FEC              uint32 `json:"fec_set"`
	TriggerIndex     uint32 `json:"trigger_index"`
	TriggerCoding    bool   `json:"trigger_coding"`
	TriggerRepair    bool   `json:"trigger_repair"`
	AdmissionEntered int64  `json:"admission_entered_ns"`
}

// Separate JSONL records join by origin/slot/index. The time pair brackets the
// UDP write syscall, NOT delivery. Attempt IDs can reset; order by timestamps.
// Highest-index probes are not exact requests for the returned shred index.
type entryRepairTrace struct {
	AdmissionStart int64  `json:"admission_start_ns,omitempty"`
	Peer           string `json:"peer,omitempty"`
	Nonce          uint32 `json:"nonce"`
	ResponseAt     int64  `json:"response_ns,omitempty"`
	RequestedAt    int64  `json:"requested_ns,omitempty"`
	ReturnedIndex  uint32 `json:"returned_index"`
	Late           bool   `json:"late"`
	FEC            uint32 `json:"fec_set"`
	DeficitBefore  int    `json:"deficit_before"`
	DeficitAfter   int    `json:"deficit_after"`
	Recovered      int    `json:"recovered"`
	Outcome        string `json:"outcome,omitempty"`
	Event          string `json:"event"`
	Origin         int64  `json:"origin_unix_ns"`
	Slot           uint64 `json:"slot"`
	Index          uint32 `json:"index"`
	Highest        bool   `json:"highest_index_probe"`
	Attempt        uint8  `json:"attempt"`
	SendStart      int64  `json:"send_start_ns"`
	SendEnd        int64  `json:"send_end_ns"`
	Success        bool   `json:"success"`
	Dropped        uint64 `json:"dropped_reports"`
}

func entryTraceSelected(slot uint64) bool {
	c := entryTraceConfig
	return c.modulo != 0 && slot%c.modulo == 0 && time.Now().Before(c.until)
}
func traceRepairSend(slot uint64, index uint32, kind repairRequestKind, attempt uint8, start int64, success bool, binding ...entryRepairTrace) {
	if start == 0 || entryTraceConfig.repairs == nil {
		return
	}
	r := entryRepairTrace{Event: "repair_send", Origin: entryTraceOrigin.UnixNano(), Slot: slot, Index: index, Highest: kind == repairRequestHighestWindowIndex, Attempt: attempt, SendStart: start, SendEnd: entryTraceNow(), Success: success}
	if len(binding) > 0 {
		r.Peer = binding[0].Peer
		r.Nonce = binding[0].Nonce
	}
	emitRepairTrace(r)
}

func emitRepairTrace(r entryRepairTrace) {
	if entryTraceConfig.repairs == nil {
		return
	}
	select {
	case entryTraceConfig.repairs <- r:
	default:
		entryTraceDropped.Add(1)
	}
}

// -1 means no authenticated coding layout is known. Zero means sufficient
// shards, not that reconstruction necessarily succeeded (see outcome/recovered).
func traceFECDeficit(s *slotState, index uint32) int {
	if s == nil {
		return -1
	}
	f := s.fecSets[index]
	if f == nil || !f.haveLayout {
		return -1
	}
	return max(0, int(f.layout.dataShreds)-len(f.data)-len(f.coding))
}
func traceRepairResponse(rec outstandingRepairRequest, sh *Shred, peer string, late bool) {
	if !entryTraceSelected(sh.Slot) {
		return
	}
	emitRepairTrace(entryRepairTrace{Event: "repair_response", Origin: entryTraceOrigin.UnixNano(), Slot: sh.Slot, Index: rec.key.index, Highest: rec.key.kind == repairRequestHighestWindowIndex, Attempt: rec.key.attempt, Nonce: rec.nonce, Peer: peer, RequestedAt: entryTraceTime(rec.sentAt), ResponseAt: entryTraceNow(), ReturnedIndex: sh.Index, FEC: sh.FECSetIndex, Late: late})
}
