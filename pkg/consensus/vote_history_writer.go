package consensus

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
)

// One serial writer, one in-flight snapshot and at most one newer pending
// snapshot. A newer complete history supersedes an unwritten snapshot; every
// retained, unrooted voting decision is still present in that newer history.
// The independently durable reservation, not this queue, authorizes signing.
// In-flight/pending snapshots may be lost on process death; even a completed
// unsynced replacement may be lost on host/power failure. Neither submitted nor
// written is a durable vote acknowledgement. Recovery must use the startup
// reservation unless the separate clean-history seal validates.
type voteHistoryWriter struct {
	mu        sync.Mutex
	pending   *alpenglow.VoteHistorySnapshot
	closing   bool
	failure   error
	submitted uint64
	written   uint64
	coalesced uint64
	wake      chan struct{}
	done      chan struct{}
	persist   func(*alpenglow.VoteHistorySnapshot) error
	onError   func(error)
}

func newVoteHistoryWriter(persist func(*alpenglow.VoteHistorySnapshot) error, onError func(error)) *voteHistoryWriter {
	w := &voteHistoryWriter{wake: make(chan struct{}, 1), done: make(chan struct{}), persist: persist, onError: onError}
	go w.run()
	return w
}

// submit does no I/O and never waits for the writer. The mutex only protects
// pointer/counter changes; neither persistence nor error callbacks hold it.
// A nil return means queued only. It must never replace the reservation check.
func (w *voteHistoryWriter) submit(snapshot *alpenglow.VoteHistorySnapshot) error {
	if snapshot == nil {
		return errors.New("nil vote-history snapshot")
	}
	w.mu.Lock()
	if w.failure != nil {
		err := w.failure
		w.mu.Unlock()
		return err
	}
	if w.closing {
		w.mu.Unlock()
		return errors.New("vote-history writer is closed")
	}
	if w.pending != nil {
		w.coalesced++
	}
	w.pending = snapshot
	w.submitted++
	w.mu.Unlock()
	w.notify()
	return nil
}

func (w *voteHistoryWriter) notify() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *voteHistoryWriter) run() {
	defer close(w.done)
	for range w.wake {
		for {
			w.mu.Lock()
			snapshot := w.pending
			w.pending = nil
			closing := w.closing
			w.mu.Unlock()
			if snapshot == nil {
				if closing {
					return
				}
				break
			}
			if err := w.persist(snapshot); err != nil {
				err = fmt.Errorf("background vote-history write: %w", err)
				w.mu.Lock()
				w.failure = err
				w.pending = nil
				w.closing = true
				w.mu.Unlock()
				if w.onError != nil {
					w.onError(err)
				}
				return
			}
			w.mu.Lock()
			w.written++
			w.mu.Unlock()
		}
	}
}

// close rejects new submissions and drains every retained snapshot. The voter
// must join this worker before writing and syncing its final clean history,
// otherwise an older in-flight rename could overwrite the sealed history.
func (w *voteHistoryWriter) close() error {
	w.mu.Lock()
	w.closing = true
	w.mu.Unlock()
	w.notify()
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failure
}

func (w *voteHistoryWriter) counters() (submitted, written, coalesced uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.submitted, w.written, w.coalesced
}
