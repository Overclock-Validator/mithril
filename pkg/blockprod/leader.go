package blockprod

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

const (
	slotDuration = 400 * time.Millisecond
	// pocLeaderLookahead caps how far ahead of replay we scan the leader schedule.
	// POC policy still requires replay to finish leaderSlot-1 (see pocLeaderSlotReplayReady)
	// before start; this only bounds schedule scanning cost.
	pocLeaderLookahead = 128
)

var errParentNotReady = errors.New("parent not ready")

// ShredSink shreds forged entry batches and broadcasts them via turbine.
type ShredSink struct {
	session *turbine.BroadcastSession
}

func NewShredSink(session *turbine.BroadcastSession) *ShredSink {
	return &ShredSink{session: session}
}

func (s *ShredSink) OnEntryBatch(entries []turbine.Entry, _ int) {
	if s.session == nil || len(entries) == 0 {
		return
	}
	if err := s.session.BroadcastEntryBatch(entries); err != nil {
		mlog.Log.Warnf("blockprod shred broadcast entry batch failed: %v", err)
	}
}

// LeaderLoop activates forge when this validator is the scheduled leader.
// RewardCertBuilder produces skip/notar reward certificates for block footers.
type RewardCertBuilder interface {
	BuildForLeaderSlot(slot uint64) rewardcerts.RewardCertificates
}

type LeaderLoop struct {
	controller       *Controller
	identity         solana.PrivateKey
	accountsDb       *accountsdb.AccountsDb
	broadcaster      turbine.PacketBroadcaster
	shredVersion     uint16
	userAgent        []byte
	rewardCerts      RewardCertBuilder
	epochSchedule    *sealevel.SysvarEpochSchedule
	alpenglowClock   bool
	parentContext    func(slot, parentSlot uint64) ParentContext
	productionParent func(slot uint64) alpenglow.BlockProductionParent
	replayReady      func() bool
	onFatal          func(error)
	prepareCommit    func(replay.CommitLeaderInput) (*replay.PreparedLeaderCommit, error)
	finalizeCommit   func(*replay.PreparedLeaderCommit, solana.Hash) error

	currentSlot   func() uint64
	leaderForSlot func(uint64) (solana.PublicKey, bool)
	parentBlockID func(uint64) (solana.Hash, bool)
	bankHash      func(*WorkingBank) solana.Hash

	pollInterval time.Duration

	mu                  sync.Mutex
	activeSlot          uint64
	activeBank          *WorkingBank
	activeSess          *turbine.BroadcastSession
	parentCtx           ParentContext
	activeParentID      solana.Hash
	finishedLeaderSlots map[uint64]struct{}
	halted              bool
	fatalOnce           sync.Once
}

type LeaderLoopConfig struct {
	Controller       *Controller
	Identity         solana.PrivateKey
	AccountsDb       *accountsdb.AccountsDb
	Broadcaster      turbine.PacketBroadcaster
	ShredVersion     uint16
	UserAgent        []byte
	EpochSchedule    *sealevel.SysvarEpochSchedule
	AlpenglowClock   bool
	ParentContext    func(slot, parentSlot uint64) ParentContext
	ProductionParent func(slot uint64) alpenglow.BlockProductionParent
	ReplayReady      func() bool
	OnFatal          func(error)
	PrepareCommit    func(replay.CommitLeaderInput) (*replay.PreparedLeaderCommit, error)
	FinalizeCommit   func(*replay.PreparedLeaderCommit, solana.Hash) error
	CurrentSlot      func() uint64
	LeaderForSlot    func(uint64) (solana.PublicKey, bool)
	ParentBlockID    func(slot uint64) (solana.Hash, bool)
	BankHash         func(*WorkingBank) solana.Hash
	RewardCerts      RewardCertBuilder
	PollInterval     time.Duration
}

func NewLeaderLoop(cfg LeaderLoopConfig) *LeaderLoop {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.UserAgent == nil {
		cfg.UserAgent = []byte("mithril")
	}
	if cfg.PrepareCommit == nil {
		cfg.PrepareCommit = replay.CommitLeaderSlot
	}
	if cfg.FinalizeCommit == nil {
		cfg.FinalizeCommit = replay.FinalizeLeaderCommit
	}
	return &LeaderLoop{
		controller:          cfg.Controller,
		identity:            cfg.Identity,
		accountsDb:          cfg.AccountsDb,
		broadcaster:         cfg.Broadcaster,
		shredVersion:        cfg.ShredVersion,
		userAgent:           cfg.UserAgent,
		epochSchedule:       cfg.EpochSchedule,
		alpenglowClock:      cfg.AlpenglowClock,
		parentContext:       cfg.ParentContext,
		productionParent:    cfg.ProductionParent,
		replayReady:         cfg.ReplayReady,
		onFatal:             cfg.OnFatal,
		prepareCommit:       cfg.PrepareCommit,
		finalizeCommit:      cfg.FinalizeCommit,
		currentSlot:         cfg.CurrentSlot,
		leaderForSlot:       cfg.LeaderForSlot,
		parentBlockID:       cfg.ParentBlockID,
		bankHash:            cfg.BankHash,
		rewardCerts:         cfg.RewardCerts,
		pollInterval:        cfg.PollInterval,
		finishedLeaderSlots: make(map[uint64]struct{}),
	}
}

func (l *LeaderLoop) Run(stop <-chan struct{}) {
	ticker := time.NewTicker(l.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			l.finishActiveSlot()
			return
		case <-ticker.C:
			l.tick()
		}
	}
}

func (l *LeaderLoop) tick() {
	if l.currentSlot == nil || l.leaderForSlot == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.halted {
		return
	}

	for {
		wallSlot := l.currentSlot()

		if l.activeBank != nil {
			if wallSlot <= l.activeSlot {
				return
			}
			l.finishActiveSlotLocked()
			continue
		}

		targetSlot, hasPending := l.nextPendingLeaderSlotLocked()
		if !hasPending {
			leader, ok := l.leaderForSlot(wallSlot)
			if !ok || leader != l.identity.PublicKey() {
				return
			}
			if l.isLeaderSlotFinished(wallSlot) {
				return
			}
			targetSlot = wallSlot
		}
		if l.isLeaderSlotFinished(targetSlot) {
			return
		}

		if ready, _ := l.pocLeaderSlotReplayReady(targetSlot); !ready {
			return
		}

		if err := l.startSlotLocked(targetSlot); err != nil {
			return
		}

		if wallSlot <= l.activeSlot {
			return
		}
	}
}

func (l *LeaderLoop) isLeaderSlotFinished(slot uint64) bool {
	_, ok := l.finishedLeaderSlots[slot]
	return ok
}

func (l *LeaderLoop) markLeaderSlotFinished(slot uint64) {
	if slot == 0 {
		return
	}
	l.finishedLeaderSlots[slot] = struct{}{}
	for finished := range l.finishedLeaderSlots {
		if finished+256 < slot {
			delete(l.finishedLeaderSlots, finished)
		}
	}
}

// nextPendingLeaderSlotLocked returns the earliest own-leader slot at or after
// replay+1 that passes the POC replay gate (see pocLeaderSlotReplayReady).
func (l *LeaderLoop) nextPendingLeaderSlotLocked() (uint64, bool) {
	replaySlot := global.Slot()
	wallSlot := l.currentSlot()
	start := replaySlot + 1
	maxSlot := wallSlot
	if maxSlot > replaySlot+pocLeaderLookahead {
		maxSlot = replaySlot + pocLeaderLookahead
	}
	for slot := start; slot <= maxSlot; slot++ {
		if l.isLeaderSlotFinished(slot) {
			continue
		}
		leader, ok := l.leaderForSlot(slot)
		if !ok || leader != l.identity.PublicKey() {
			continue
		}
		if ready, _ := l.pocLeaderSlotReplayReady(slot); !ready {
			continue
		}
		return slot, true
	}
	return 0, false
}

func (l *LeaderLoop) finishActiveSlot() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.finishActiveSlotLocked()
}

// clampProducerTimeNanos clamps the leader's wall-clock footer timestamp into the
// Alpenglow nanosecond-clock bounds derived from the parent bank, matching Agave's
// block_creation_loop::skew_block_producer_time_nanos. Without this, a leader that
// just caught up stamps real "now", which is typically >2*slot_duration past the
// parent clock and is rejected by the network as NanosecondClockOutOfBounds.
func (l *LeaderLoop) clampProducerTimeNanos(slot uint64, nowNanos int64) uint64 {
	if !l.alpenglowClock || l.accountsDb == nil || l.activeBank == nil {
		return uint64(nowNanos)
	}
	slotCtx := l.activeBank.SlotCtx()
	if slotCtx == nil {
		return uint64(nowNanos)
	}
	parentSlot := slotCtx.ParentSlot
	parentNanos, ok := replay.ReadResolvedNanosecondClockAt(l.accountsDb, parentSlot)
	if !ok {
		return uint64(nowNanos)
	}
	var elapsed uint64
	if slot > parentSlot {
		elapsed = (slot - parentSlot) * uint64(slotDuration.Nanoseconds())
	}
	clamped := replay.SkewBlockProducerTimeNanos(parentNanos, nowNanos, elapsed)
	if clamped < 0 {
		clamped = 0
	}
	return uint64(clamped)
}

func (l *LeaderLoop) finishActiveSlotLocked() {
	if l.activeBank == nil {
		return
	}
	slot := l.activeSlot

	l.activeBank.FlushEntries()

	pohTip := l.activeBank.EntryHash()
	tickHash := turbine.AlpentickHash(pohTip)
	producerTimeNanos := l.clampProducerTimeNanos(slot, time.Now().UnixNano())
	footerTimestamp := int64(producerTimeNanos / 1_000_000_000)

	var footerRewards rewardcerts.RewardCertificates
	if l.rewardCerts != nil {
		footerRewards = l.rewardCerts.BuildForLeaderSlot(slot)
	}

	var prepared *replay.PreparedLeaderCommit
	var prepareErr error
	commitRequired := l.accountsDb != nil && l.epochSchedule != nil
	if l.accountsDb != nil && l.epochSchedule != nil {
		block := BuildLeaderBlock(LeaderBlockInput{
			Bank:             l.activeBank,
			EpochSchedule:    l.epochSchedule,
			ParentBankhash:   l.parentCtx.ParentBankhash,
			PrevNumSigs:      l.parentCtx.PrevNumSigs,
			PrevFeeGovernor:  l.parentCtx.PrevFeeGovernor,
			EntryBlockhash:   tickHash,
			ParentBlockID:    l.activeParentID,
			TxFeeAccumulator: l.activeBank.TxFeeAccumulator(),
		})
		block.SkipRewardCert = append([]byte(nil), footerRewards.Skip...)
		block.NotarRewardCert = append([]byte(nil), footerRewards.Notar...)
		block.FooterProducerTimeNanos = producerTimeNanos

		prepared, prepareErr = l.prepareCommit(replay.CommitLeaderInput{
			AcctsDb:                 l.accountsDb,
			SlotCtx:                 l.activeBank.SlotCtx(),
			Block:                   block,
			EpochSchedule:           l.epochSchedule,
			TxFeeAccumulator:        l.activeBank.TxFeeAccumulator(),
			AlpenglowClock:          l.alpenglowClock,
			AlpenglowShredVersion:   l.shredVersion,
			FooterTimestamp:         footerTimestamp,
			FooterProducerTimeNanos: producerTimeNanos,
		})
		if prepareErr != nil {
			mlog.Log.Errorf("leader slot %d commit failed: %v", slot, prepareErr)
			prepared = nil
		}
	}
	if commitRequired && prepared == nil {
		if prepareErr == nil {
			prepareErr = errors.New("commit preparation returned no state")
		}
		l.haltLocked(fmt.Errorf("leader slot %d state preparation failed: %w", slot, prepareErr))
		l.markLeaderSlotFinished(slot)
		l.clearActiveSlotLocked()
		return
	}

	var blockID solana.Hash
	broadcastComplete := false
	if l.activeSess != nil {
		if err := l.activeSess.BroadcastFooter(l.bankHash(l.activeBank), producerTimeNanos, footerRewards.Skip, footerRewards.Notar); err != nil {
			mlog.Log.Warnf("leader slot %d footer broadcast failed: %v", slot, err)
		} else if err := l.activeSess.BroadcastEndingTickLast(tickHash); err != nil {
			mlog.Log.Warnf("leader slot %d ending tick broadcast failed: %v", slot, err)
		} else {
			parentSlot := l.activeBank.SlotCtx().ParentSlot
			blockID = l.activeSess.BlockID(parentSlot, l.activeParentID)
			global.SetAlpenglowBlockID(slot, blockID)
			if chained := l.activeSess.ChainedMerkleRoot(); chained != (solana.Hash{}) {
				global.SetAlpenglowChainedMerkleRoot(slot, chained)
			}
			broadcastComplete = true
		}
	}
	if prepared != nil && broadcastComplete {
		if err := l.finalizeCommit(prepared, blockID); err != nil {
			l.haltLocked(fmt.Errorf("leader slot %d was broadcast but could not enter replay: %w", slot, err))
		}
	} else if commitRequired && !broadcastComplete {
		l.haltLocked(fmt.Errorf("leader slot %d state was prepared but block broadcast did not complete", slot))
	}

	l.markLeaderSlotFinished(slot)

	l.clearActiveSlotLocked()
}

func (l *LeaderLoop) clearActiveSlotLocked() {
	if l.controller != nil {
		l.controller.ClearWorkingBank()
	}
	l.activeBank = nil
	l.activeSess = nil
	l.activeSlot = 0
	l.activeParentID = solana.Hash{}
}

func (l *LeaderLoop) haltLocked(err error) {
	if err == nil {
		return
	}
	l.halted = true
	mlog.Log.Errorf("ALPENGLOW VALIDATOR SAFETY HALT: %v", err)
	l.fatalOnce.Do(func() {
		if l.onFatal != nil {
			l.onFatal(err)
		}
	})
}

func (l *LeaderLoop) startSlotLocked(slot uint64) error {
	if l.replayReady != nil && !l.replayReady() {
		return fmt.Errorf("%w: speculative replay is not ready", errParentNotReady)
	}
	parent, err := l.resolveProductionParent(slot)
	if err != nil {
		return err
	}
	parentSlot := parent.Slot

	parentCtx := ParentContext{}
	if l.parentContext != nil {
		parentCtx = l.parentContext(slot, parentSlot)
	}
	if ready, err := l.pocLeaderSlotReplayReady(slot); !ready {
		return err
	}
	if slot > 0 && parentCtx.ParentBankhash == (solana.Hash{}) {
		return fmt.Errorf("%w: parent bankhash missing for slot %d", errParentNotReady, parentSlot)
	}
	l.parentCtx = parentCtx

	parentID := parent.Hash

	parentChainedRoot := solana.Hash{}
	if slot > 0 {
		var ok bool
		_, parentChainedRoot, ok = replay.ResolveActiveAlpenglowIdentity(parentSlot)
		if !ok {
			return fmt.Errorf("%w: chained merkle root missing for parent slot %d", errParentNotReady, parentSlot)
		}
	}

	session := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
		Leader:                  l.identity,
		Slot:                    slot,
		ParentSlot:              parentSlot,
		ParentBlockID:           parentID,
		ParentChainedMerkleRoot: parentChainedRoot,
		Version:                 l.shredVersion,
		Broadcaster:             l.broadcaster,
		UserAgent:               l.userAgent,
	})
	if err := session.BroadcastHeader(parentID); err != nil {
		return fmt.Errorf("broadcast header parent_block_id=%s: %w", parentID, err)
	}

	slotCtx, err := NewLeaderSlotCtx(slot, parentSlot, l.accountsDb, parentCtx, l.epochSchedule)
	if err != nil {
		return fmt.Errorf("new leader slot ctx: %w", err)
	}
	startEntryHash := parentCtx.ParentLastEntryHash
	if startEntryHash == (solana.Hash{}) {
		startEntryHash = parentCtx.ParentBankhash
	}
	bank := NewWorkingBank(BankConfig{
		SlotCtx:   slotCtx,
		Slot:      slot,
		Leader:    l.identity.PublicKey(),
		Limits:    costmodel.DefaultLimits(),
		EntryHash: startEntryHash,
		Sink:      NewShredSink(session),
	})
	l.activeSlot = slot
	l.activeBank = bank
	l.activeSess = session
	l.activeParentID = parentID
	l.controller.SetWorkingBank(bank)
	return nil
}
