package alpenglow

import (
	"errors"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/quic-go/quic-go"
)

const (
	defaultVotorPeerSendQueue = 256
	votorSendTimeout          = time.Second
	votorSendWatchInterval    = 100 * time.Millisecond
)

// VotorPeerQueueStats describes local queueing, not remote delivery. Counters
// here cover the current connection; broadcaster totals survive reconnects.
type VotorPeerQueueStats struct {
	Identity       solana.PublicKey `json:"identity"`
	Address        string           `json:"address"`
	Queued         int              `json:"queued"`
	SendingFor     time.Duration    `json:"sending_for_ns"`
	LastQueueDelay time.Duration    `json:"last_queue_delay_ns"`
	MaxQueueDelay  time.Duration    `json:"max_queue_delay_ns"`
	QueueDrops     uint64           `json:"queue_drops"`
}

type votorDatagram struct {
	payload  []byte // immutable, shared across the peer queues
	queuedAt time.Time
}

// Each authenticated connection owns one sender. No mutex is held while
// SendDatagram blocks on quic-go's bounded datagram queue.
type votorPeerSender struct {
	b     *VotorBroadcaster
	peer  VotorPeer
	conn  *quic.Conn
	queue chan votorDatagram
	done  chan struct{}

	mu             sync.Mutex
	closed         bool
	sendingSince   time.Time
	lastQueueDelay time.Duration
	maxQueueDelay  time.Duration
	queueDrops     uint64
}

func (s *votorPeerSender) enqueue(job votorDatagram) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.conn.Context().Err() != nil {
		s.b.sendsSkipped.Add(1)
		return
	}
	select {
	case s.queue <- job:
	default:
		// A full queue rejects only this peer's copy. As before, fanout is
		// best effort; the global Enqueue error contract is unchanged.
		s.queueDrops++
		s.b.peerQueueDrops.Add(1)
		s.b.dropped.Add(1)
	}
}

func (s *votorPeerSender) run() {
	defer s.b.wg.Done()
	defer close(s.done)
	defer func() {
		s.mu.Lock()
		s.closed = true
		// Old-connection work is not replayed onto a new address/connection.
		// Account for every queued copy discarded on failure or shutdown.
		for {
			select {
			case <-s.queue:
				s.b.peerQueueDiscarded.Add(1)
			default:
				s.mu.Unlock()
				return
			}
		}
	}()
	for {
		select {
		case <-s.b.done:
			return
		case <-s.conn.Context().Done():
			return
		case job := <-s.queue:
			s.mu.Lock()
			if s.closed || s.conn.Context().Err() != nil || s.b.closed.Load() {
				s.mu.Unlock()
				s.b.peerQueueDiscarded.Add(1)
				return
			}
			now := time.Now()
			s.sendingSince = now
			s.lastQueueDelay = now.Sub(job.queuedAt)
			s.maxQueueDelay = max(s.maxQueueDelay, s.lastQueueDelay)
			for old := s.b.peerQueueMaxDelay.Load(); int64(s.lastQueueDelay) > old; old = s.b.peerQueueMaxDelay.Load() {
				if s.b.peerQueueMaxDelay.CompareAndSwap(old, int64(s.lastQueueDelay)) {
					break
				}
			}
			s.mu.Unlock()
			err := s.conn.SendDatagram(job.payload)
			s.mu.Lock()
			s.sendingSince = time.Time{}
			s.mu.Unlock()
			if err != nil {
				s.b.recordSendError(s.peer, err)
				var tooLarge *quic.DatagramTooLargeError
				if errors.As(err, &tooLarge) {
					continue
				}
				s.b.dropConnection(s.peer.Identity, s.conn)
				s.b.queueConnect(s.peer.Identity)
				return
			}
			s.b.sends.Add(1)
		}
	}
}

func (s *votorPeerSender) stats() VotorPeerQueueStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var sendingFor time.Duration
	if !s.sendingSince.IsZero() {
		sendingFor = time.Since(s.sendingSince)
	}
	return VotorPeerQueueStats{
		Identity: s.peer.Identity, Address: s.peer.Addr.String(),
		Queued: len(s.queue), SendingFor: sendingFor,
		LastQueueDelay: s.lastQueueDelay, MaxQueueDelay: s.maxQueueDelay, QueueDrops: s.queueDrops,
	}
}

func (b *VotorBroadcaster) expirePeerSends(now time.Time) {
	senders, _ := b.connectedSenders()
	for _, s := range senders {
		s.mu.Lock()
		expired := !s.closed && !s.sendingSince.IsZero() && now.Sub(s.sendingSince) >= votorSendTimeout
		if expired {
			// Serialize with send completion so a late watchdog cannot close a
			// later, unrelated send after the blocked operation has finished.
			s.closed = true
		}
		s.mu.Unlock()
		if expired {
			b.peerSendTimeouts.Add(1)
			// Closing wakes SendDatagram without leaking a timeout goroutine.
			b.dropConnection(s.peer.Identity, s.conn)
			b.queueConnect(s.peer.Identity)
		}
	}
}
