package turbine

import (
	"fmt"
	"io"
)

// Completion records are repair-cache hints, not voting or checkpoint state.
// A bounded writer removes their disk I/O from block delivery. Only completion
// hints may be dropped when the queue is full; live completeness stays in memory
// and Close retries the current hints before the next opener takes ownership.
const spoolJournalQueueSize = 256

type spoolJournalFile interface {
	io.Writer
	Truncate(int64) error
	Close() error
}

type spoolJournalRequest struct {
	record [spoolJournalRecordSize]byte
	done   chan error // non-nil for an invalidation that must precede file mutation
}

type spoolCompletionJournal struct {
	requests chan spoolJournalRequest
	done     chan struct{}
}

func newSpoolCompletionJournal(file spoolJournalFile) *spoolCompletionJournal {
	j := &spoolCompletionJournal{requests: make(chan spoolJournalRequest, spoolJournalQueueSize), done: make(chan struct{})}
	go j.run(file)
	return j
}

func journalRequest(slot uint64, meta SpoolSlotMeta) spoolJournalRequest {
	var req spoolJournalRequest
	copy(req.record[:], spoolJournalRecord(slot, meta))
	return req
}

// The spool mutex serializes submissions and Close, but the worker never takes
// that mutex. A stalled write therefore cannot directly stall MarkComplete.
func (j *spoolCompletionJournal) tryComplete(slot uint64, meta SpoolSlotMeta) bool {
	select {
	case j.requests <- journalRequest(slot, meta):
		return true
	default:
		return false
	}
}

func (j *spoolCompletionJournal) complete(slot uint64, meta SpoolSlotMeta) {
	j.requests <- journalRequest(slot, meta)
}

// Never drop or reorder invalidations. Wait for earlier completions and this
// tombstone before deleting/replacing/truncating a slot file. These rare paths
// may still wait for storage while holding the spool mutex. Queueing tombstones
// without this fence could resurrect an old completion after a crash.
func (j *spoolCompletionJournal) invalidate(slot uint64) error {
	req := journalRequest(slot, SpoolSlotMeta{})
	req.done = make(chan error, 1)
	j.requests <- req
	return <-req.done
}

func (j *spoolCompletionJournal) close() {
	close(j.requests)
	<-j.done
}

func (j *spoolCompletionJournal) run(file spoolJournalFile) {
	defer close(j.done)
	defer file.Close()
	failed, invalidated := false, false
	for req := range j.requests {
		var err error
		if !failed {
			var n int
			n, err = file.Write(req.record[:])
			if err == nil && n != len(req.record) {
				err = io.ErrShortWrite
			}
			failed = err != nil
		}
		if failed && !invalidated {
			// Never append behind a short record, or acknowledge an invalidation
			// while old completion hints remain. Empty the disposable journal and
			// disable further hint writes for this opener. If even truncation fails,
			// the caller must leave the slot file unchanged and retry later.
			err = file.Truncate(0)
			invalidated = err == nil
			if err != nil {
				err = fmt.Errorf("invalidate failed shred completeness journal: %w", err)
			}
		}
		if req.done != nil {
			req.done <- err
		}
	}
}
