package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/gagliardetto/solana-go"
)

// BankSource supplies the active leader working bank.
type BankSource interface {
	WorkingBank() *blockprod.WorkingBank
}

// Stats tracks scheduler buffer and forge outcomes.
type Stats struct {
	InPackets               uint64
	InBytes                 uint64
	Buffered                uint64
	DroppedParse            uint64
	DroppedVote             uint64
	DroppedDuplicate        uint64
	DroppedCapacity         uint64
	EvictedCapacity         uint64
	DroppedExpired          uint64
	DroppedAlreadyProcessed uint64
	Accepted                uint64
	DroppedNoBank           uint64
	DroppedCost             uint64
	DroppedExecution        uint64
	DroppedBlockCost        uint64
	DroppedAccountCost      uint64
	DroppedAllocCost        uint64
	DroppedBatchBytes       uint64
}

// Scheduler buffers verified TPU transactions in a reward-ordered heap and
// drains into the active WorkingBank when one is published.
type Scheduler struct {
	banks  BankSource
	feats  *features.Features
	buffer *Buffer

	seq  atomic.Uint64
	wake chan struct{}

	mu    sync.Mutex
	stats Stats

	// bankGen increments whenever the drain loop observes a different WorkingBank
	// pointer so cost-rejected txs can be retried in a later slot.
	bankGen    uint64
	activeBank *blockprod.WorkingBank

	startOnce sync.Once
	stopOnce  sync.Once
	cancel    context.CancelFunc
	done      chan struct{}

	// insertsSinceCleanup counts accepted Receive inserts. Cleanup walks the
	// whole heap, so the drain loop does it once per capacity/5 inserts
	// instead of before every pop.
	insertsSinceCleanup atomic.Uint64
}

func New(banks BankSource) *Scheduler {
	feats := features.NewFeaturesDefault()
	return &Scheduler{
		banks:  banks,
		feats:  feats,
		buffer: NewBuffer(MaxBufferedTxns),
		wake:   make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
}

// Start launches the bank-gated drain loop.
func (s *Scheduler) Start(ctx context.Context) {
	s.startOnce.Do(func() {
		runCtx, cancel := context.WithCancel(ctx)
		s.cancel = cancel
		go s.drainLoop(runCtx)
	})
}

// Stop cancels the drain loop and waits for it to exit.
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		if s.cancel == nil {
			return
		}
		s.cancel()
		<-s.done
	})
}

func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *Scheduler) Buffered() int {
	return s.buffer.Len()
}

// Receive scores and buffers a verified packet. Packets are always released.
func (s *Scheduler) Receive(pkt packet.Packet) {
	data := pkt.Data()
	s.mu.Lock()
	s.stats.InPackets++
	s.stats.InBytes += uint64(len(data))
	s.mu.Unlock()

	// Packet buffers are pooled. Copy before Release — solana-go's decoder
	// aliases instruction/signature slices into the source buffer, and the
	// scheduler retains txs across slots.
	wire := append([]byte(nil), data...)
	pkt.Release()

	tx, err := solana.TransactionFromBytes(wire)
	if err != nil {
		s.addStat(func(st *Stats) { st.DroppedParse++ })
		return
	}

	reward, messageHash, err := scoreTransaction(tx, s.feats)
	if err != nil {
		s.addStat(func(st *Stats) { st.DroppedParse++ })
		return
	}

	e := &entry{
		tx:          tx,
		wire:        wire,
		wireSize:    len(wire),
		messageHash: messageHash,
		blockhash:   tx.Message.RecentBlockhash,
		reward:      reward,
		seq:         s.seq.Add(1),
	}
	result, evicted := s.buffer.Insert(e)
	s.mu.Lock()
	switch result {
	case InsertAccepted:
		s.stats.Buffered++
		if evicted != nil {
			s.stats.EvictedCapacity++
		}
		s.signal()
	case InsertDuplicate:
		s.stats.DroppedDuplicate++
	default:
		s.stats.DroppedCapacity++
	}
	s.mu.Unlock()
	if result == InsertAccepted {
		s.insertsSinceCleanup.Add(1)
	}
}

// Cleanup drops expired / already-processed buffered txs using bank state.
func (s *Scheduler) Cleanup(bank *blockprod.WorkingBank) int {
	if bank == nil {
		return 0
	}
	var expired, already uint64
	dropped := s.buffer.Cleanup(func(e *entry) bool {
		switch bank.ClassifyBuffered(e.blockhash, e.messageHash) {
		case blockprod.BufferedExpired:
			expired++
			return true
		case blockprod.BufferedAlreadyProcessed:
			already++
			return true
		default:
			return false
		}
	})
	if dropped > 0 {
		s.mu.Lock()
		s.stats.DroppedExpired += expired
		s.stats.DroppedAlreadyProcessed += already
		s.mu.Unlock()
	}
	return dropped
}

func cleanupInsertInterval(capacity int) uint64 {
	n := uint64(capacity / 5)
	if n == 0 {
		n = 1
	}
	return n
}

// maybeCleanup walks the heap when enough new inserts have arrived.
func (s *Scheduler) maybeCleanup(bank *blockprod.WorkingBank) int {
	if bank == nil {
		return 0
	}
	if s.insertsSinceCleanup.Load() < cleanupInsertInterval(s.buffer.Capacity()) {
		return 0
	}
	dropped := s.Cleanup(bank)
	s.insertsSinceCleanup.Store(0)
	return dropped
}

func (s *Scheduler) drainLoop(ctx context.Context) {
	defer close(s.done)
	for {
		if ctx.Err() != nil {
			return
		}
		bank := s.banks.WorkingBank()
		s.noteBank(bank)
		if bank == nil {
			if !s.wait(ctx, 5*time.Millisecond) {
				return
			}
			continue
		}

		s.maybeCleanup(bank)

		e, skipped := s.popSchedulable(s.bankGen)
		for _, sk := range skipped {
			s.rebuffer(sk)
		}
		if e == nil {
			if !s.wait(ctx, time.Millisecond) {
				return
			}
			continue
		}

		// Bank may have been cleared between the nil check and pop.
		bank = s.banks.WorkingBank()
		s.noteBank(bank)
		if bank == nil {
			s.rebuffer(e)
			continue
		}

		// Close a full FEC set, or refuse, before execute. Slot entry bytes are
		// a schedule-time budget (shred-safe / SIMD-0525), like Firedancer pack.
		if reason := bank.PrepareSchedule(e.wireSize); reason != costmodel.ExceedNone {
			e.skipGen = s.bankGen
			s.rebuffer(e)
			s.recordForge(blockprod.ForgeDroppedCost, reason)
			continue
		}

		// Prefer the owned wire so forge reparses from stable bytes even if the
		// retained tx view was somehow mutated after buffering.
		var result blockprod.ForgeResult
		var reason costmodel.ExceedReason
		if len(e.wire) > 0 {
			result, reason = bank.Forge(e.wire)
		} else {
			result, reason = bank.ForgeTransaction(e.tx, e.wireSize)
		}
		switch result {
		case blockprod.ForgeDroppedNoLeader:
			s.rebuffer(e)
		case blockprod.ForgeDroppedCost:
			e.skipGen = s.bankGen
			s.rebuffer(e)
			s.recordForge(result, reason)
		default:
			s.recordForge(result, reason)
		}
	}
}

// popSchedulable pops the highest-reward entry that is not marked skip for gen.
func (s *Scheduler) popSchedulable(gen uint64) (picked *entry, skipped []*entry) {
	for {
		e := s.buffer.PopMax()
		if e == nil {
			return nil, skipped
		}
		if e.skipGen == gen {
			skipped = append(skipped, e)
			continue
		}
		return e, skipped
	}
}

func (s *Scheduler) noteBank(bank *blockprod.WorkingBank) {
	if bank == s.activeBank {
		return
	}
	s.activeBank = bank
	s.bankGen++
}

func (s *Scheduler) rebuffer(e *entry) {
	if res, _ := s.buffer.Insert(e); res != InsertAccepted {
		s.addStat(func(st *Stats) { st.DroppedCapacity++ })
	}
}

func (s *Scheduler) recordForge(result blockprod.ForgeResult, reason costmodel.ExceedReason) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch result {
	case blockprod.ForgeAccepted:
		s.stats.Accepted++
	case blockprod.ForgeDroppedVote:
		s.stats.DroppedVote++
	case blockprod.ForgeDroppedParse:
		s.stats.DroppedParse++
	case blockprod.ForgeDroppedCost:
		s.stats.DroppedCost++
		s.recordCostDropLocked(reason)
	case blockprod.ForgeDroppedExecution:
		s.stats.DroppedExecution++
	case blockprod.ForgeDroppedAlreadyProcessed:
		s.stats.DroppedAlreadyProcessed++
	default:
		s.stats.DroppedNoBank++
	}
}

func (s *Scheduler) recordCostDropLocked(reason costmodel.ExceedReason) {
	switch reason {
	case costmodel.ExceedBlockCost:
		s.stats.DroppedBlockCost++
	case costmodel.ExceedWritableAccountCost:
		s.stats.DroppedAccountCost++
	case costmodel.ExceedAllocatedDataSize:
		s.stats.DroppedAllocCost++
	case costmodel.ExceedBatchBytes:
		s.stats.DroppedBatchBytes++
	}
}

func (s *Scheduler) addStat(fn func(*Stats)) {
	s.mu.Lock()
	fn(&s.stats)
	s.mu.Unlock()
}

func (s *Scheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-s.wake:
		return true
	case <-timer.C:
		return true
	}
}
