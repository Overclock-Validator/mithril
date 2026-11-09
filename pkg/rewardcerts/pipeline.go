package rewardcerts

import (
	"sync"
	"time"
)

// Retain at most four leader windows, including pending and completed work.
// A newer request also retires older abandoned slots, without requiring their
// footers to be produced. Only one build can be in flight outside this bound.
const pipelineRetainedSlots = 16

// CertificateBuilder is the consuming, synchronous reward-certificate builder.
// A Pipeline must be its only caller of BuildForLeaderSlot.
type CertificateBuilder interface {
	BuildForLeaderSlot(slot uint64) RewardCertificates
}

// PreparationStatus distinguishes a prepared empty certificate set from a
// missing result. BuildDuration measures the background work, not footer lookup.
type PreparationStatus struct {
	Ready         bool
	BuildDuration time.Duration
}

type preparation struct {
	started bool
	status  PreparationStatus
	certs   RewardCertificates
}

// Pipeline prepares optional rewards before block completion. Preparation uses
// the votes available when the worker runs; later votes are not added to that
// footer. Rewards concern slot N-8, so preparation when production starts
// normally sees the completed vote set. A missing result is omitted,
// rather than delaying the block or changing an already prepared certificate.
//
// Requests and footer consumption advance monotonically through leader slots.
// The worker builds in slot order because Builder consumes and prunes votes.
type Pipeline struct {
	builder CertificateBuilder
	mu      sync.Mutex
	work    map[uint64]*preparation
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	closed  bool

	retiredThrough uint64
	hasRetired     bool
	startedThrough uint64
	hasStarted     bool
}

func NewPipeline(builder CertificateBuilder) *Pipeline {
	p := &Pipeline{
		builder: builder,
		work:    make(map[uint64]*preparation),
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go p.run()
	return p
}

// PrepareForLeaderSlot queues work without waiting for the builder. Repeated
// requests, expired slots and slots older than an already started build are
// ignored, so the underlying consuming builder is never called twice per slot.
func (p *Pipeline) PrepareForLeaderSlot(slot uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || (p.hasRetired && slot <= p.retiredThrough) {
		return
	}
	if _, exists := p.work[slot]; exists || (p.hasStarted && slot <= p.startedThrough) {
		return
	}
	if slot >= pipelineRetainedSlots {
		p.retireLocked(slot - pipelineRetainedSlots)
	}
	p.work[slot] = &preparation{}
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// BuildForLeaderSlot implements the leader's existing builder interface, but
// performs only a nonblocking lookup. The certificate buffers transfer to the
// caller and are never modified or returned again by the pipeline.
func (p *Pipeline) BuildForLeaderSlot(slot uint64) RewardCertificates {
	certs, _ := p.TakeForLeaderSlot(slot)
	return certs
}

// TakeForLeaderSlot consumes exactly this slot's completed result and retires
// all older work. If preparation is unfinished, its eventual result is dropped.
// No certificate building, signature work, logging or waiting occurs here.
func (p *Pipeline) TakeForLeaderSlot(slot uint64) (RewardCertificates, PreparationStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var certs RewardCertificates
	var status PreparationStatus
	if prepared := p.work[slot]; prepared != nil && prepared.status.Ready {
		certs, status = prepared.certs, prepared.status
	}
	p.retireLocked(slot)
	return certs, status
}

func (p *Pipeline) retireLocked(slot uint64) {
	if p.hasRetired && slot <= p.retiredThrough {
		return
	}
	p.retiredThrough, p.hasRetired = slot, true
	for pending := range p.work {
		if pending <= slot {
			delete(p.work, pending)
		}
	}
}

func (p *Pipeline) next() (uint64, *preparation, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, nil, false
	}
	var slot uint64
	var next *preparation
	for candidate, work := range p.work {
		if !work.started && (next == nil || candidate < slot) {
			slot, next = candidate, work
		}
	}
	if next != nil {
		next.started = true
		p.startedThrough, p.hasStarted = slot, true
	}
	return slot, next, true
}

func (p *Pipeline) run() {
	defer close(p.done)
	for {
		slot, pending, open := p.next()
		if !open {
			return
		}
		if pending == nil {
			select {
			case <-p.stop:
				return
			case <-p.wake:
				continue
			}
		}
		started := time.Now()
		var certs RewardCertificates
		if p.builder != nil {
			certs = p.builder.BuildForLeaderSlot(slot)
		}
		// Own the buffers even when a custom builder reuses its output storage.
		certs.Skip = append([]byte(nil), certs.Skip...)
		certs.Notar = append([]byte(nil), certs.Notar...)
		status := PreparationStatus{Ready: true, BuildDuration: time.Since(started)}
		p.mu.Lock()
		if !p.closed && p.work[slot] == pending {
			pending.certs, pending.status = certs, status
		}
		p.mu.Unlock()
	}
}

// Close drops queued work and waits for the single in-flight builder call, if
// any. The builder has no cancellation API, so that call must return before
// shutdown completes. Concurrent Close calls are safe and join the same worker.
func (p *Pipeline) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		p.work = nil
		close(p.stop)
	}
	p.mu.Unlock()
	<-p.done
}
