package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
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
	DroppedAccountLocks     uint64
	DroppedLookup           uint64
	Accepted                uint64
	DroppedNoBank           uint64
	DroppedCost             uint64
	DroppedExecution        uint64
	DroppedBlockCost        uint64
	DroppedAccountCost      uint64
	DroppedAllocCost        uint64
	DroppedBatchBytes       uint64
	PreparedRetained        uint64
	PreparationEager        uint64
	PreparationLazy         uint64
	BackgroundCleanups      uint64
	BackgroundExpired       uint64
}

// Scheduler buffers verified TPU transactions in a reward-ordered heap and
// drains into the active WorkingBank when one is published.
type Scheduler struct {
	banks                  BankSource
	feats                  *features.Features
	buffer                 *Buffer
	preparer               atomic.Pointer[replay.TransactionPreparer]
	featureSource          func() *features.Features
	lastPreparationRefresh time.Time
	maxPrepared            int64
	preparedRetained       atomic.Int64
	cleanupSource          func() *sealevel.BankSysvars
	cleanupHistory         expiryHistory
	lastMaintenance        time.Time
	lastMemoryLog          time.Time
	lastExpiryChunk        time.Time
	expiryCursor           int
	expiryRemaining        int
	expirySnapshot         *sealevel.BankSysvars

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
		banks:       banks,
		feats:       feats,
		buffer:      NewBuffer(MaxBufferedTxns),
		maxPrepared: defaultMaxPreparedTransactions,
		wake:        make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
}

// NewWithFeatureSource prepares queued messages against a replay-tip snapshot.
// The active bank independently verifies feature compatibility before reuse.
func NewWithFeatureSource(banks BankSource, source func() *features.Features) *Scheduler {
	return NewWithConfig(banks, Config{FeatureSource: source})
}

// Config controls the bounded TPU queue and static preparation source.
type Config struct {
	// MaxBufferedTransactions defaults to MaxBufferedTxns when zero.
	MaxBufferedTransactions int
	FeatureSource           func() *features.Features
	// MaxPreparedTransactions bounds the optional expanded instruction cache,
	// independently of transaction admission. Zero uses the default; negative
	// disables eager preparation. Transactions without it prepare when selected.
	MaxPreparedTransactions int
	// CleanupSource publishes immutable recent blockhashes from replay. It is
	// used while no leader bank is published, including healthy Harmonic slots.
	CleanupSource func() *sealevel.BankSysvars
}

func NewWithConfig(banks BankSource, cfg Config) *Scheduler {
	s := New(banks)
	if cfg.MaxBufferedTransactions > 0 {
		s.buffer = NewBuffer(cfg.MaxBufferedTransactions)
	}
	s.featureSource = cfg.FeatureSource
	if cfg.MaxPreparedTransactions != 0 {
		s.maxPrepared = max(0, int64(cfg.MaxPreparedTransactions))
	}
	s.cleanupSource = cfg.CleanupSource
	s.refreshPreparation()
	return s
}

func (s *Scheduler) refreshPreparation() {
	if s.featureSource == nil || time.Since(s.lastPreparationRefresh) < 100*time.Millisecond {
		return
	}
	s.preparer.Store(replay.NewTransactionPreparer(s.featureSource()))
	s.lastPreparationRefresh = time.Now()
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
	stats := s.stats
	stats.PreparedRetained = uint64(s.preparedRetained.Load())
	return stats
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
	result := s.buffer.precheck(e)
	var evicted *entry
	if result == InsertAccepted {
		s.prepareRetained(e)
		result, evicted = s.buffer.Insert(e)
	}
	if result != InsertAccepted {
		s.releasePrepared(e)
	}
	s.releasePrepared(evicted)
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
			s.releasePrepared(e)
			return true
		case blockprod.BufferedAlreadyProcessed:
			already++
			s.releasePrepared(e)
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
		s.refreshPreparation()
		s.maintain(time.Now())
		bank := s.tpuBank()
		s.noteBank(bank)
		if bank == nil {
			// Polling is how a new bank, or a gated one opening, is noticed.
			if !s.wait(ctx, time.Millisecond) {
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
		bank = s.tpuBank()
		s.noteBank(bank)
		if bank == nil {
			s.rebuffer(e)
			continue
		}

		// Close a full FEC set, or refuse, before execute. Slot entry bytes are
		// a schedule-time budget (shred-safe / SIMD-0525), like Firedancer pack.
		res, reason := bank.PrepareSchedule(e.wireSize)
		if reason != costmodel.ExceedNone {
			e.skipGen = s.bankGen
			s.rebuffer(e)
			s.recordForge(blockprod.ForgeDroppedCost, reason)
			continue
		}

		// Receive owns the wire and its decoded transaction for the entire queue
		// lifetime. Execution modifies transaction-local account clones, not
		// this immutable message, so reuse the decoded transaction across banks.
		prepared := e.prepared
		if prepared == nil {
			prepared = s.preparer.Load().Prepare(e.tx)
			if prepared != nil {
				s.addStat(func(st *Stats) { st.PreparationLazy++ })
			}
		}
		result, reason := bank.ForgeScheduledTransaction(e.tx, e.wireSize, prepared, res)
		switch result {
		case blockprod.ForgeDroppedNoLeader:
			s.rebuffer(e)
		case blockprod.ForgeDroppedCost:
			e.skipGen = s.bankGen
			s.rebuffer(e)
			s.recordForge(result, reason)
		default:
			s.releasePrepared(e)
			s.recordForge(result, reason)
		}
	}
}

// tpuBank returns the working bank while it admits TPU transactions. A gated
// bank is treated like no bank for execution; background expiry may still
// discard transactions proven stale from immutable bank history.
func (s *Scheduler) tpuBank() *blockprod.WorkingBank {
	bank := s.banks.WorkingBank()
	if bank == nil || !bank.TPUAllowed() {
		return nil
	}
	return bank
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
	res, evicted := s.buffer.Insert(e)
	s.releasePrepared(evicted)
	if res != InsertAccepted {
		s.releasePrepared(e)
		s.addStat(func(st *Stats) { st.DroppedCapacity++ })
	}
}

// maintain expires only hashes observed leaving a continuous immutable bank
// history. Unknown hashes may belong to transactions ahead of local replay;
// they must survive until an execution bank can classify them authoritatively.
func (s *Scheduler) maintain(now time.Time) {
	chunkDue := s.expiryRemaining > 0 && now.Sub(s.lastExpiryChunk) >= 2*time.Millisecond
	if now.Sub(s.lastMaintenance) >= time.Second || chunkDue {
		s.lastMaintenance = now
		var snapshot *sealevel.BankSysvars
		if bank := s.banks.WorkingBank(); bank != nil && bank.SlotCtx() != nil {
			snapshot = bank.SlotCtx().BankSysvars()
		}
		if snapshot == nil && s.cleanupSource != nil {
			snapshot = s.cleanupSource()
		}
		if snapshot != s.expirySnapshot {
			s.expirySnapshot = snapshot
			if hashes, ok := snapshot.RecentBlockhashes(); ok && len(hashes) > 0 {
				if s.cleanupHistory.observe(hashes) {
					s.expiryRemaining = s.Buffered()
				}
				if len(s.cleanupHistory.old) == 0 {
					s.expiryRemaining = 0
				}
			} else {
				s.cleanupHistory = expiryHistory{}
				s.expiryRemaining = 0
			}
		}
	}
	if s.expiryRemaining > 0 && now.Sub(s.lastExpiryChunk) >= 2*time.Millisecond {
		s.lastExpiryChunk = now
		next, inspected, dropped := s.buffer.cleanupSome(s.expiryCursor, min(512, s.expiryRemaining), func(e *entry) bool {
			if !s.cleanupHistory.expired(e.blockhash) {
				return false
			}
			s.releasePrepared(e)
			return true
		})
		s.expiryCursor = next
		if dropped > 0 {
			// Heap removals may move entries behind the cursor. Finish only
			// after a complete pass with no removals, spread across chunks.
			s.expiryRemaining = s.Buffered()
		} else {
			s.expiryRemaining -= inspected
			if inspected == 0 {
				s.expiryRemaining = 0
			}
		}
		s.addStat(func(st *Stats) {
			st.BackgroundCleanups++
			st.BackgroundExpired += uint64(dropped)
			st.DroppedExpired += uint64(dropped)
		})
	}
	if now.Sub(s.lastMemoryLog) >= 10*time.Second {
		s.lastMemoryLog = now
		stats := s.Stats()
		mlog.Log.FileOnlyf("TPU scheduler memory: queued=%d prepared=%d prepared_limit=%d eager_total=%d lazy_total=%d cleanup_total=%d expired_background_total=%d", s.Buffered(), stats.PreparedRetained, s.maxPrepared, stats.PreparationEager, stats.PreparationLazy, stats.BackgroundCleanups, stats.BackgroundExpired)
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
	case blockprod.ForgeDroppedAccountLocks:
		s.stats.DroppedAccountLocks++
	case blockprod.ForgeDroppedLookup:
		s.stats.DroppedLookup++
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
