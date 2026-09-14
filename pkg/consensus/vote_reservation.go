package consensus

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

const signingReserveSlots = uint64(32)
const signingRenewRemaining = uint64(16)

// The worker owns record after startup. Only its acknowledged Through is
// published to signers. Requested slots never grant signing permission.
type signingReservation struct {
	uncertain      bool // Worker only, read after halt. Failed sync may have reached storage.
	record         alpenglow.VoteReservation
	through        atomic.Uint64
	desired        atomic.Uint64
	stopped        atomic.Bool
	recoverThrough uint64 // Immutable bound read at startup, zero after verified clean shutdown.
	leaderThrough  uint64 // Exact leader production history is not saved: always skip the old range.
	wake           chan struct{}
	changed        chan struct{}
	stop           chan struct{}
	done           chan struct{}
	stopOnce       sync.Once
	persist        func(alpenglow.VoteReservation) error
}

func openSigningReservation(cfg VotingConfig, node solana.PublicKey, shredVersion uint16, history *alpenglow.VoteHistory) (*signingReservation, error) {
	if cfg.Genesis == (solana.Hash{}) {
		return nil, errors.New("reserved voting requires the bound genesis hash")
	}
	expected := alpenglow.VoteReservation{Version: 1, Node: node, VoteAccount: cfg.VoteAccount, AuthorizedVoter: solana.PublicKey(cfg.AuthorizedVoter.Public().(ed25519.PublicKey)), Genesis: cfg.Genesis, ShredVersion: shredVersion}
	record, err := alpenglow.LoadVoteReservation(cfg.HistoryDir, node)
	initializing := errors.Is(err, os.ErrNotExist)
	if initializing {
		if !cfg.InitializeVoteReservation || history.ReservationRequired {
			return nil, errors.New("missing vote reservation; explicit first enrollment with complete synchronous history is required")
		}
		record = expected
		record.Generation = 1
		record.Through = history.Root
		for slot := range history.VotesCast {
			record.Through = max(record.Through, slot)
		}
		// The baseline is made durable before the first reservation is created.
		if err := alpenglow.SaveVoteHistory(cfg.HistoryDir, history, cfg.Identity); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, fmt.Errorf("refuse unsafe reservation reset: %w", err)
	}
	if record.Node != expected.Node || record.VoteAccount != expected.VoteAccount || record.AuthorizedVoter != expected.AuthorizedVoter || record.Genesis != expected.Genesis || record.ShredVersion != expected.ShredVersion {
		return nil, errors.New("vote reservation cluster or signing identity mismatch; explicit domain migration is required")
	}
	if record.Through == math.MaxUint64 || record.Generation == math.MaxUint64 {
		return nil, errors.New("vote reservation exhausted")
	}
	for slot := range history.VotesCast {
		if slot > record.Through {
			return nil, fmt.Errorf("history slot %d exceeds durable reservation %d", slot, record.Through)
		}
	}
	r := &signingReservation{record: record, recoverThrough: record.Through, leaderThrough: record.Through, wake: make(chan struct{}, 1), changed: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	r.persist = func(next alpenglow.VoteReservation) error {
		return alpenglow.SaveVoteReservation(cfg.HistoryDir, next, cfg.Identity)
	}
	if initializing {
		r.recoverThrough = 0
	} else if len(record.CleanHistoryDigest) != 0 {
		digest, err := alpenglow.VoteHistoryDigest(cfg.HistoryDir, node)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(digest, record.CleanHistoryDigest) {
			r.recoverThrough = 0
		}
	}
	// Consume the clean marker before any new signature or history mutation.
	r.record.CleanHistoryDigest = nil
	r.record.Generation++
	if err := r.persist(r.record); err != nil {
		return nil, fmt.Errorf("consume vote reservation session: %w", err)
	}
	history.ReservationRequired = true
	// Version 2 is deliberately rejected by older binaries that do not enforce H.
	if err := alpenglow.SaveVoteHistory(cfg.HistoryDir, history, cfg.Identity); err != nil {
		return nil, err
	}
	r.through.Store(r.record.Through)
	mlog.Log.Infof("ALPENGLOW signing reservation: through=%d recovery_through=%d leader_recovery_through=%d", r.record.Through, r.recoverThrough, r.leaderThrough)
	go r.run()
	return r, nil
}

// allow is nonblocking and can also be called by the leader loop. Finality
// comes from verified consensus/checkpoint state, never an RPC wall-clock tip.
func (r *signingReservation) allow(slot, finalized uint64, leader bool) bool {
	if r.stopped.Load() {
		return false
	}
	floor := r.recoverThrough
	if leader {
		floor = r.leaderThrough
	}
	if floor != 0 && (slot <= floor || finalized < floor) {
		return false
	}
	r.request(slot)
	return slot <= r.through.Load()
}

func (r *signingReservation) request(slot uint64) {
	for {
		old := r.desired.Load()
		if slot <= old || r.desired.CompareAndSwap(old, slot) {
			break
		}
	}
	through := r.through.Load()
	if slot > through || through-slot <= signingRenewRemaining {
		select {
		case r.wake <- struct{}{}:
		default:
		}
	}
}

func (r *signingReservation) run() {
	defer close(r.done)
	retry := time.NewTicker(250 * time.Millisecond)
	defer retry.Stop()
	var warned bool
	for {
		select {
		case <-r.stop:
			return
		case <-r.wake:
		case <-retry.C:
		}
		if r.stopped.Load() {
			return
		}
		slot := r.desired.Load()
		if slot == 0 || (slot <= r.record.Through && r.record.Through-slot > signingRenewRemaining) {
			continue
		}
		if slot > math.MaxUint64-signingReserveSlots || r.record.Generation == math.MaxUint64 {
			continue
		} // Never wrap or grant permission.
		next := r.record
		next.Through = max(next.Through, slot+signingReserveSlots)
		next.Generation++
		next.CleanHistoryDigest = nil
		if err := r.persist(next); err != nil {
			r.uncertain = true
			if !warned {
				mlog.Log.Errorf("ALPENGLOW signing reservation renewal failed; permission remains through %d: %v", r.through.Load(), err)
				warned = true
			}
			continue // Retry the same or a greater bound; an uncertain sync never grants permission.
		}
		warned = false
		r.uncertain = false
		r.record = next
		r.through.Store(next.Through)
		select {
		case r.changed <- struct{}{}:
		default:
		}
	}
}

func (r *signingReservation) halt() {
	r.stopOnce.Do(func() { r.stopped.Store(true); close(r.stop) })
	<-r.done
}

// Called only after the voter loop and leader producer have stopped. A failure
// leaves dirty recovery in force; history must be synced before its digest.
func (r *signingReservation) seal(dir string, history *alpenglow.VoteHistory, identity ed25519.PrivateKey) error {
	r.halt()
	if r.uncertain {
		return errors.New("uncertain reservation write; retaining unclean recovery")
	}
	if err := alpenglow.SaveVoteHistory(dir, history, identity); err != nil {
		return err
	}
	digest, err := alpenglow.VoteHistoryDigest(dir, history.NodePubkey)
	if err != nil {
		return err
	}
	if r.record.Generation == math.MaxUint64 {
		return errors.New("vote reservation generation exhausted")
	}
	next := r.record
	next.Generation++
	next.CleanHistoryDigest = digest
	return r.persist(next)
}

// Retain only events blocked on renewal, not historical catch-up traffic.
// Replay the original event after acknowledgement so normal finality, parent,
// execution and invalidation checks still decide whether to vote.
func (v *alpenglowVoter) retainReservationEvent(event voterEvent) {
	r := v.reservation
	if r == nil {
		return
	}
	var slot uint64
	switch event.kind {
	case voterEventBlock:
		slot = event.block.Block.Slot
	case voterEventBlockTimeout, voterEventCrashedLeaderTimeout:
		slot = event.slot | (alpenglow.LeaderWindowSlots - 1)
	case voterEventConsensus:
		switch event.consensus.Kind {
		case alpenglow.ConsensusEventBlockNotarized, alpenglow.ConsensusEventParentReady, alpenglow.ConsensusEventSafeToNotar, alpenglow.ConsensusEventSafeToSkip:
			slot = event.consensus.Slot
			if event.consensus.Kind == alpenglow.ConsensusEventSafeToNotar || event.consensus.Kind == alpenglow.ConsensusEventSafeToSkip {
				slot |= alpenglow.LeaderWindowSlots - 1
			}
		default:
			return
		}
	default:
		return
	}
	if slot <= r.through.Load() || slot <= v.admissionFloor() || slot < v.waitToVoteSlot || v.engine.alpenglowVerifiedFinalityFloor() < r.recoverThrough || slot <= r.recoverThrough {
		return
	}
	if !v.votingStarted && v.readyToVote != nil && !v.readyToVote(slot) {
		return
	}
	r.request(slot)
	if len(v.reservationEvents) < votorEventQueueSize {
		v.reservationEvents = append(v.reservationEvents, event)
	}
}

// AlpenglowCanSignLeaderSlot protects every produced slot, including the
// trailing slots of a leader window. A clean vote-history marker is not a
// complete leader-block history, so leaders always skip the old reservation.
func (e *AlpenglowObserverEngine) AlpenglowCanSignLeaderSlot(slot uint64) bool {
	if e.safetyError() != nil {
		return false
	}
	e.voterMu.RLock()
	defer e.voterMu.RUnlock()
	v := e.voter
	if v == nil {
		return false
	}
	if v.reservation == nil {
		return true
	}
	return v.reservation.allow(slot, e.alpenglowVerifiedFinalityFloor(), true)
}
