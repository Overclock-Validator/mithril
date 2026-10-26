package harmonic

import (
	"container/heap"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/gagliardetto/solana-go"
)

// bundleNode is an admitted block bundle in its slot's schedule.
type bundleNode struct {
	st       *slotState
	seq      uint64
	prepared *blockprod.PreparedBundle
	executed *blockprod.ExecutedBundle
	// deps counts the earlier conflicting bundles that have not finished.
	deps       int
	dependents []*bundleNode
	// lastDependent is the most recent bundle made to depend on this one, so a
	// bundle sharing several accounts with it is linked once.
	lastDependent *bundleNode
	done          bool
}

// accountLocks is an account's latest unfinished writer and the bundles that
// read it since.
type accountLocks struct {
	writer  *bundleNode
	readers []*bundleNode
}

// schedule orders a slot's bundles so that executing them concurrently gives
// the same result as executing them one after another in stream order.
//
// A bundle depends on the latest earlier writer of every account it uses and,
// for each account it writes, on the readers since that writer. It is linked
// only to these direct predecessors: each of them waits for its own, so
// conflicts are ordered transitively. A bundle is ready once every
// predecessor has finished, and ready bundles start lowest sequence first.
// Bundles that conflict with no unfinished earlier one start at once, ahead
// of earlier bundles still waiting.
type schedule struct {
	locks map[solana.PublicKey]*accountLocks
	ready readyQueue
	// waiting counts bundles with unfinished predecessors.
	waiting int
	// retry holds bundles that did not fit the block's limits while others
	// were executing; they keep their place and try again later.
	retry  []*bundleNode
	closed bool
}

// accountSet calls fn once for each account a bundle uses, with whether it
// writes it.
type accountSet func(fn func(key solana.PublicKey, writable bool))

// add schedules n, which uses accounts, after every earlier bundle it
// conflicts with.
func (s *schedule) add(n *bundleNode, accounts accountSet) {
	if s.locks == nil {
		s.locks = make(map[solana.PublicKey]*accountLocks)
	}
	accounts(func(key solana.PublicKey, writable bool) {
		l := s.locks[key]
		if l == nil {
			l = &accountLocks{}
			s.locks[key] = l
		}
		s.dependOn(n, l.writer)
		if !writable {
			l.readers = append(l.readers, n)
			return
		}
		for _, r := range l.readers {
			s.dependOn(n, r)
		}
		l.writer, l.readers = n, nil
	})
	if n.deps == 0 {
		heap.Push(&s.ready, n)
	} else {
		s.waiting++
	}
}

func (s *schedule) dependOn(n, p *bundleNode) {
	if p == nil || p.done || p.lastDependent == n {
		return
	}
	p.lastDependent = n
	p.dependents = append(p.dependents, n)
	n.deps++
}

// next returns the ready bundle earliest in the stream, or nil.
func (s *schedule) next() *bundleNode {
	if s.closed || s.ready.Len() == 0 {
		return nil
	}
	return heap.Pop(&s.ready).(*bundleNode)
}

// finish marks n finished, landed or not, and readies the bundles that waited
// only on it.
func (s *schedule) finish(n *bundleNode) {
	n.done = true
	for _, d := range n.dependents {
		d.deps--
		if d.deps == 0 && !s.closed {
			s.waiting--
			heap.Push(&s.ready, d)
		}
	}
	n.dependents = nil
}

// retryLater keeps n, which did not fit the block's limits, to try again once
// another bundle finishes. It keeps its account locks meanwhile.
func (s *schedule) retryLater(n *bundleNode) {
	s.retry = append(s.retry, n)
}

// requeueRetries readies the bundles waiting to retry.
func (s *schedule) requeueRetries() {
	if s.closed {
		return
	}
	for _, n := range s.retry {
		heap.Push(&s.ready, n)
	}
	s.retry = nil
}

// close drops every bundle not executing and returns how many there were.
// Nothing is scheduled afterwards.
func (s *schedule) close() int {
	if s.closed {
		return 0
	}
	dropped := s.ready.Len() + s.waiting + len(s.retry)
	s.ready, s.waiting, s.retry, s.closed = nil, 0, nil, true
	return dropped
}

// readyQueue is a min-heap of bundles by stream sequence.
type readyQueue []*bundleNode

func (q readyQueue) Len() int           { return len(q) }
func (q readyQueue) Less(i, j int) bool { return q[i].seq < q[j].seq }
func (q readyQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *readyQueue) Push(x any)        { *q = append(*q, x.(*bundleNode)) }
func (q *readyQueue) Pop() any {
	old := *q
	n := old[len(old)-1]
	*q = old[:len(old)-1]
	return n
}
