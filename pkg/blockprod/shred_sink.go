package blockprod

import (
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// ShredSink shreds forged entry batches and broadcasts them via turbine.
// OnEntryBatch only enqueues; a dedicated goroutine shreds in FIFO order so
// forge can keep packing. Wait drains the queue before footer/ending-tick
// so the chained merkle state stays sequential.
type ShredSink struct {
	broadcast func([]turbine.Entry) error

	mu      sync.Mutex
	cond    *sync.Cond
	queue   [][]turbine.Entry
	err     error
	busy    bool
	closed  bool
	discard bool
	exited  bool
}

func NewShredSink(session *turbine.BroadcastSession) *ShredSink {
	s := &ShredSink{}
	if session != nil {
		s.broadcast = session.BroadcastEntryBatch
	}
	s.cond = sync.NewCond(&s.mu)
	go s.loop()
	return s
}

func (s *ShredSink) OnEntryBatch(entries []turbine.Entry, _ int) {
	if s == nil || s.broadcast == nil || len(entries) == 0 {
		return
	}
	job := cloneEntries(entries)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.err != nil {
		return
	}
	s.queue = append(s.queue, job)
	s.cond.Signal()
}

func (s *ShredSink) Err() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Wait blocks until every enqueued batch has been shredded (or dropped).
func (s *ShredSink) Wait() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for !s.exited && (s.busy || len(s.queue) > 0) {
		s.cond.Wait()
	}
}

// Discard stops the worker and drops any batches not yet shredded.
func (s *ShredSink) Discard() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if !s.exited {
		s.discard = true
		s.closed = true
		s.queue = nil
		s.cond.Broadcast()
		for !s.exited {
			s.cond.Wait()
		}
	}
	s.mu.Unlock()
}

func (s *ShredSink) loop() {
	for {
		s.mu.Lock()
		for len(s.queue) == 0 && !s.closed {
			s.cond.Wait()
		}
		if len(s.queue) == 0 {
			s.exited = true
			s.busy = false
			s.cond.Broadcast()
			s.mu.Unlock()
			return
		}
		job := s.queue[0]
		s.queue[0] = nil
		s.queue = s.queue[1:]
		discard := s.discard
		broadcast := s.broadcast
		s.busy = true
		s.mu.Unlock()

		if !discard && broadcast != nil {
			if err := broadcast(job); err != nil {
				s.mu.Lock()
				if s.err == nil {
					s.err = err
					mlog.Log.Warnf("blockprod shred broadcast entry batch failed: %v", err)
				}
				s.queue = nil
				s.mu.Unlock()
			}
		}

		s.mu.Lock()
		s.busy = false
		if len(s.queue) == 0 {
			s.cond.Broadcast()
		}
		s.mu.Unlock()
	}
}

func cloneEntries(entries []turbine.Entry) []turbine.Entry {
	out := make([]turbine.Entry, len(entries))
	for i, e := range entries {
		out[i] = turbine.Entry{
			NumHashes: e.NumHashes,
			Hash:      e.Hash,
			Txns:      append([]solana.Transaction(nil), e.Txns...),
		}
	}
	return out
}
