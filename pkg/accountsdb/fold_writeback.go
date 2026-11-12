package accountsdb

import (
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// foldWritebackBytes: MITHRIL_FOLD_WRITEBACK_MB=N makes the fold hand its
// segment file to the device N MiB at a time while it is being written, and
// wait for each step before starting the next. Unset or 0 keeps the earlier
// behaviour: the whole segment (100 MiB and more) sits in the page cache until
// one fsync submits it at once. That burst fills the device queue, and account
// index reads issued by block loading during it wait behind the writes.
// Durability is unchanged either way: the final fsync still decides it.
// Linux only; elsewhere the setting has no effect.
var foldWritebackBytes = func() int64 {
	n, err := strconv.Atoi(os.Getenv("MITHRIL_FOLD_WRITEBACK_MB"))
	if err != nil || n < 1 || n > 1024 {
		return 0
	}
	return int64(n) << 20
}()

// segmentFileWriter writes a fold segment and, when step is positive, bounds
// the amount of it that is dirty or in flight to about two steps.
type segmentFileWriter struct {
	f       *os.File
	step    int64
	written int64
	handed  int64 // bytes already handed to the device
	err     error // first writeback error; reported by the caller's fsync too
}

func (w *segmentFileWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.written += int64(n)
	if err != nil || w.step <= 0 || w.written-w.handed < w.step {
		return n, err
	}
	// Wait for what was handed over earlier, then start the new bytes. A
	// failure here is not fatal: the fsync that follows reports real errors.
	if werr := boundedWriteback(w.f, w.handed, w.written); werr != nil {
		w.err = werr
		w.step = 0
		return n, nil
	}
	w.handed = w.written
	return n, nil
}

// Fold phases, for attributing a slow account load to what the fold was doing.
const (
	foldPhaseIdle uint32 = iota
	foldPhasePrepare
	foldPhaseSegmentWrite
	foldPhaseSegmentSync
	foldPhaseManifest
	foldPhaseIndexCommit
	foldPhaseCacheRefresh
)

var foldPhaseNames = [...]string{"none", "prepare", "segment_write", "segment_sync", "manifest", "index_commit", "cache_refresh"}

// foldPhase packs the phase with the time it began.
type foldPhase struct {
	phase atomic.Uint32
	since atomic.Int64
}

func (p *foldPhase) set(phase uint32) {
	p.since.Store(time.Now().UnixNano())
	p.phase.Store(phase)
}

// describe returns the phase name and, during a fold, how long it has run.
func (p *foldPhase) describe() string {
	phase := p.phase.Load()
	if phase == foldPhaseIdle || int(phase) >= len(foldPhaseNames) {
		return foldPhaseNames[foldPhaseIdle]
	}
	age := time.Duration(time.Now().UnixNano() - p.since.Load())
	return foldPhaseNames[phase] + "+" + age.Round(time.Millisecond).String()
}

// slowBatchReadLog is the durable-read time above which one batch load is
// logged with the fold phase it ran in.
const slowBatchReadLog = 20 * time.Millisecond
