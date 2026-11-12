package accountsdb

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// defaultFoldWritebackMB is the step used when MITHRIL_FOLD_WRITEBACK_MB is not
// set, on platforms that implement the stepping.
const defaultFoldWritebackMB = 8

// foldWritebackBytes is the step in which a fold hands its segment file to the
// device while writing it, waiting for each step before starting the next.
// Without stepping the whole segment (100 MiB and more) sits in the page cache
// until one fsync submits it at once; that burst fills the device queue, and
// account index reads issued by block loading during it wait behind the
// writes. Durability is the same either way: the final fsync still decides it.
//
// MITHRIL_FOLD_WRITEBACK_MB sets the step in MiB (1 to 1024); 0 turns stepping
// off and restores the single fsync. Unset, or a value that does not parse,
// uses the default. Linux only: elsewhere stepping is off unless asked for,
// and asking for it only reports that it is unsupported.
var foldWritebackBytes = parseFoldWritebackMB(os.LookupEnv("MITHRIL_FOLD_WRITEBACK_MB"))

func parseFoldWritebackMB(v string, set bool) int64 {
	def := int64(0)
	if boundedWritebackSupported {
		def = defaultFoldWritebackMB << 20
	}
	if !set {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 0 || n > 1024 {
		return def
	}
	return int64(n) << 20
}

// segmentWriteback is boundedWriteback; tests replace it.
var segmentWriteback = boundedWriteback

// segmentFileWriter writes a fold segment and, when step is positive, hands it
// to the device a step at a time. The bytes that are dirty or in flight then
// stay near two steps plus the largest single write (the buffered writer above
// passes an account larger than its buffer through in one piece); that is a
// pacing target, not a hard cap.
type segmentFileWriter struct {
	f       *os.File
	step    int64 // step asked for; 0 for none
	written int64
	handed  int64 // bytes handed to the device by successful steps
	calls   int   // successful steps
	err     error // first step failure; it ends stepping for this file
}

func (w *segmentFileWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.written += int64(n)
	if err != nil || w.step <= 0 || w.err != nil || w.written-w.handed < w.step {
		return n, err
	}
	// Wait for what was handed over earlier, then start the new bytes. A
	// failure here does not fail the fold: the fsync that follows submits
	// everything and reports real errors. It is kept for the fold's report, so
	// a run with the setting on cannot turn itself off unseen.
	if werr := segmentWriteback(w.f, w.handed, w.written); werr != nil {
		w.err = werr
		return n, nil
	}
	w.handed = w.written
	w.calls++
	return n, nil
}

func (w *segmentFileWriter) report() FoldWriteback {
	rep := FoldWriteback{StepBytes: w.step, Calls: w.calls, HandedBytes: w.handed, SegmentBytes: w.written}
	if w.err != nil {
		rep.Err = w.err.Error()
	}
	return rep
}

// FoldWriteback reports what the stepwise segment writeback did in one fold.
type FoldWriteback struct {
	StepBytes    int64  // step in effect (MITHRIL_FOLD_WRITEBACK_MB or the default); 0 when off
	Calls        int    // steps handed to the device
	HandedBytes  int64  // segment bytes those steps covered; the final fsync submits the rest
	SegmentBytes int64  // segment file size
	Err          string // first step failure, which ended stepping for this file; empty when none
}

func (w FoldWriteback) String() string {
	msg := "none"
	if w.Err != "" {
		msg = strconv.Quote(w.Err)
	}
	return fmt.Sprintf("writeback_step_mb=%d writeback_calls=%d writeback_handed_bytes=%d segment_bytes=%d writeback_err=%s",
		w.StepBytes>>20, w.Calls, w.HandedBytes, w.SegmentBytes, msg)
}

// Fold phases, for attributing a slow account load to what the fold was doing.
const (
	foldPhaseIdle uint64 = iota
	foldPhasePrepare
	foldPhaseSegmentWrite
	foldPhaseSegmentSync
	foldPhaseManifest
	foldPhaseIndexCommit
	foldPhaseCacheRefresh
)

var foldPhaseNames = [...]string{"none", "prepare", "segment_write", "segment_sync", "manifest", "index_commit", "cache_refresh"}

// foldPhaseBase is the origin of the phase clock (monotonic).
var foldPhaseBase = time.Now()

const foldPhaseSinceBits = 56 // nanoseconds since foldPhaseBase: about two years

// foldPhase holds the fold's current phase and the time it began in one word,
// so a reader never pairs one phase with another phase's start time.
type foldPhase struct{ v atomic.Uint64 }

func (p *foldPhase) set(phase uint64) {
	since := uint64(time.Since(foldPhaseBase)) & (1<<foldPhaseSinceBits - 1)
	p.v.Store(phase<<foldPhaseSinceBits | since)
}

// foldPhaseSnap is the phase as seen at one instant. Taking one is a single
// atomic load; formatting is left to String, for after locks are released.
type foldPhaseSnap struct {
	packed uint64
	at     time.Time
}

func (p *foldPhase) at(t time.Time) foldPhaseSnap { return foldPhaseSnap{packed: p.v.Load(), at: t} }

// String returns the phase name and, during a fold, how long the phase had
// run when the snapshot was taken: "segment_sync+35ms", or "none".
func (s foldPhaseSnap) String() string {
	phase := s.packed >> foldPhaseSinceBits
	if phase == foldPhaseIdle || phase >= uint64(len(foldPhaseNames)) {
		return foldPhaseNames[foldPhaseIdle]
	}
	began := time.Duration(s.packed & (1<<foldPhaseSinceBits - 1))
	age := s.at.Sub(foldPhaseBase) - began
	if age < 0 { // the phase began between the caller's clock read and its load
		age = 0
	}
	return foldPhaseNames[phase] + "+" + age.Round(time.Millisecond).String()
}

// slowBatchReadLog is the durable-read time above which one batch load is
// logged with the fold phase it ran in.
const slowBatchReadLog = 20 * time.Millisecond
