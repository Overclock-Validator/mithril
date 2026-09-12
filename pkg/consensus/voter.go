package consensus

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

const (
	votorDeltaTimeout       = 400 * time.Millisecond
	votorStandstillTimeout  = 10 * time.Second
	votorRefreshPerSecond   = 20
	votorEventQueueSize     = 4096
	votorStatsLogInterval   = 10 * time.Second
	defaultAlpenglowSlotDur = 200 * time.Millisecond
)

var (
	errVoterNotEligible = errors.New("validator is not eligible to vote in this epoch")
	errVoterNotReady    = errors.New("validator has not joined live voting")
)

// VotingPeerSource resolves gossip endpoints for the already-merged Votor
// transport membership. The message slot is intentionally absent: callers
// must not narrow this union back to one epoch at an epoch boundary.
type VotingPeerSource func(validators []alpenglow.ValidatorStake) []alpenglow.VotorPeer

// VotingConfig contains the operator-held material and network discovery
// boundary required to activate voting. VoteAccount is the on-chain vote
// account address; AuthorizedVoter is the Ed25519 signer from which its BLS key
// was registered.
type VotingConfig struct {
	Identity        ed25519.PrivateKey
	AuthorizedVoter ed25519.PrivateKey
	VoteAccount     solana.PublicKey
	HistoryDir      string
	EpochForSlot    func(slot uint64) uint64
	Peers           VotingPeerSource
	SlotDuration    time.Duration
	WaitToVoteSlot  uint64
	ReadyToVote     func(slot uint64) bool
}

// VotingStats exposes positive network evidence separately from local casting.
// NetworkLandedVotes counts unique persisted votes whose rank appeared in the
// exact BLS-verified certificate proof received over Votor QUIC.
type VotingStats struct {
	Enabled                        bool                      `json:"enabled"`
	VotesCastThisRun               uint64                    `json:"votes_cast_this_run"`
	NetworkLandedVotes             uint64                    `json:"network_landed_votes"`
	LastNetworkLandedSlot          uint64                    `json:"last_network_landed_slot,omitempty"`
	LastNetworkLandedVoteType      alpenglow.VoteType        `json:"last_network_landed_vote_type,omitempty"`
	LastNetworkCertificateType     alpenglow.CertificateType `json:"last_network_certificate_type,omitempty"`
	LastNetworkLandedAt            time.Time                 `json:"last_network_landed_at,omitempty"`
	BroadcastMessagesQueued        uint64                    `json:"broadcast_messages_queued"`
	BroadcastMessagesDropped       uint64                    `json:"broadcast_messages_dropped"`
	BroadcastPeerSends             uint64                    `json:"broadcast_peer_sends"`
	BroadcastPeerSendsSkipped      uint64                    `json:"broadcast_peer_sends_skipped"`
	BroadcastPeerSendErrors        uint64                    `json:"broadcast_peer_send_errors"`
	BroadcastDesiredPeers          int                       `json:"broadcast_desired_peers"`
	BroadcastActiveConnections     int                       `json:"broadcast_active_connections"`
	BroadcastPendingConnections    int                       `json:"broadcast_pending_connections"`
	BroadcastConnectionAttempts    uint64                    `json:"broadcast_connection_attempts"`
	BroadcastConnectionErrors      uint64                    `json:"broadcast_connection_errors"`
	BroadcastConnectionJobsDropped uint64                    `json:"broadcast_connection_jobs_dropped"`
	BroadcastLastPeerSendError     string                    `json:"broadcast_last_peer_send_error,omitempty"`
	BroadcastLastPeerSendErrorAt   time.Time                 `json:"broadcast_last_peer_send_error_at,omitempty"`
	BroadcastLastConnectionError   string                    `json:"broadcast_last_connection_error,omitempty"`
	BroadcastLastConnectionErrorAt time.Time                 `json:"broadcast_last_connection_error_at,omitempty"`
}

type voterEventKind uint8

const (
	voterEventConsensus voterEventKind = iota
	voterEventBlock
	voterEventFirstShred
	voterEventCrashedLeaderTimeout
	voterEventBlockTimeout
	voterEventValidatorSet
	voterEventRoot
	voterEventNetworkCertificate
)

type voterEvent struct {
	kind        voterEventKind
	consensus   alpenglow.ConsensusEvent
	block       alpenglow.ReplayBlockObservation
	slot        uint64
	set         alpenglow.ValidatorSet
	root        alpenglow.BlockID
	certificate alpenglow.Certificate
}

type pendingVotorBlock struct {
	block  alpenglow.BlockID
	parent alpenglow.BlockID
}

// alpenglowVoter is a single-threaded port of Agave's Votor event handler. All
// history decisions run on loop; validator-set snapshots are protected only so
// the outbound peer callback can read them from broadcast workers.
type alpenglowVoter struct {
	engine          *AlpenglowObserverEngine
	identity        ed25519.PrivateKey
	node            solana.PublicKey
	voteAccount     solana.PublicKey
	signer          *alpenglow.BLSSigner
	historyDir      string
	history         *alpenglow.VoteHistory
	epochForSlot    func(uint64) uint64
	peerSource      VotingPeerSource
	slotDuration    time.Duration
	waitToVoteSlot  uint64
	readyToVote     func(slot uint64) bool
	broadcaster     *alpenglow.VotorBroadcaster
	events          chan voterEvent
	done            chan struct{}
	startOnce       sync.Once
	closeOnce       sync.Once
	wg              sync.WaitGroup
	setsMu          sync.RWMutex
	sets            map[uint64]alpenglow.ValidatorSet
	restored        map[alpenglow.VoteMessageKey]bool
	pending         map[uint64][]pendingVotorBlock
	receivedShred   map[uint64]bool
	timeoutsSet     map[uint64]bool
	executedBlocks  map[alpenglow.BlockID]bool
	highestFinal    uint64
	lastFinalizedAt time.Time
	votingStarted   bool
	latestLiveSlot  uint64
	standstillSlot  *uint64
	refreshQueue    []alpenglow.Message
	refreshCursor   int
	lastWarn        map[uint64]time.Time
	landingMu       sync.RWMutex
	landed          map[alpenglow.VoteMessageKey]struct{}
	stats           VotingStats
	lastStatsLog    time.Time
	// beforeVoteGuard is a deterministic test seam for invalidation races. It
	// is nil in production.
	beforeVoteGuard func(alpenglow.BlockID)
	// beforeLocalVoteInject is a deterministic test seam for the interval after
	// durable history persistence and before atomic consensus-pool admission.
	beforeLocalVoteInject func(alpenglow.Vote)
}

func newAlpenglowVoter(engine *AlpenglowObserverEngine, cfg VotingConfig, root alpenglow.BlockID) (*alpenglowVoter, error) {
	return newAlpenglowVoterWithStart(engine, cfg, root, true, nil)
}

func newAlpenglowVoterUnstarted(engine *AlpenglowObserverEngine, cfg VotingConfig, root alpenglow.BlockID, sets []alpenglow.ValidatorSet) (*alpenglowVoter, error) {
	return newAlpenglowVoterWithStart(engine, cfg, root, false, sets)
}

func newAlpenglowVoterWithStart(engine *AlpenglowObserverEngine, cfg VotingConfig, root alpenglow.BlockID, start bool, sets []alpenglow.ValidatorSet) (*alpenglowVoter, error) {
	if engine == nil {
		return nil, fmt.Errorf("enable Alpenglow voting: nil consensus engine")
	}
	if len(cfg.Identity) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("enable Alpenglow voting: validator identity keypair is required")
	}
	if len(cfg.AuthorizedVoter) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("enable Alpenglow voting: authorized-voter keypair is required")
	}
	if cfg.VoteAccount == (solana.PublicKey{}) {
		return nil, fmt.Errorf("enable Alpenglow voting: vote account is required")
	}
	if cfg.HistoryDir == "" {
		return nil, fmt.Errorf("enable Alpenglow voting: vote-history directory is required")
	}
	if cfg.EpochForSlot == nil || cfg.Peers == nil {
		return nil, fmt.Errorf("enable Alpenglow voting: epoch and peer sources are required")
	}
	if len(engine.identity) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("enable Alpenglow voting: consensus engine has no validator transport identity")
	}
	if cfg.SlotDuration <= 0 {
		cfg.SlotDuration = defaultAlpenglowSlotDur
	}
	node := solana.PublicKey(cfg.Identity.Public().(ed25519.PublicKey))
	engineNode := solana.PublicKey(engine.identity.Public().(ed25519.PublicKey))
	if node != engineNode {
		return nil, fmt.Errorf("enable Alpenglow voting: identity %s does not match consensus transport identity %s", node, engineNode)
	}
	history, err := alpenglow.LoadVoteHistory(cfg.HistoryDir, node)
	if err != nil {
		if !errors.Is(err, alpenglow.ErrVoteHistoryNotFound) {
			return nil, fmt.Errorf("enable Alpenglow voting: refuse unsafe vote-history reset: %w", err)
		}
		history = alpenglow.NewVoteHistory(node, root.Slot)
		if err := alpenglow.SaveVoteHistory(cfg.HistoryDir, history, cfg.Identity); err != nil {
			return nil, fmt.Errorf("initialize Alpenglow vote history: %w", err)
		}
		mlog.Log.FileOnlyf("ALPENGLOW voting: created new vote history for %s at root %d; do not reuse this vote account on another validator", node, root.Slot)
	}
	if history.Root < root.Slot {
		history.SetRoot(root.Slot)
		if err := alpenglow.SaveVoteHistory(cfg.HistoryDir, history, cfg.Identity); err != nil {
			return nil, fmt.Errorf("advance Alpenglow vote-history root: %w", err)
		}
	}
	signer, err := alpenglow.DeriveBLSSigner(cfg.AuthorizedVoter)
	if err != nil {
		return nil, fmt.Errorf("derive authorized-voter BLS key: %w", err)
	}
	v := &alpenglowVoter{
		engine:          engine,
		identity:        append(ed25519.PrivateKey(nil), cfg.Identity...),
		node:            node,
		voteAccount:     cfg.VoteAccount,
		signer:          signer,
		historyDir:      cfg.HistoryDir,
		history:         history,
		epochForSlot:    cfg.EpochForSlot,
		peerSource:      cfg.Peers,
		slotDuration:    cfg.SlotDuration,
		waitToVoteSlot:  cfg.WaitToVoteSlot,
		readyToVote:     cfg.ReadyToVote,
		events:          make(chan voterEvent, votorEventQueueSize),
		done:            make(chan struct{}),
		sets:            make(map[uint64]alpenglow.ValidatorSet),
		restored:        make(map[alpenglow.VoteMessageKey]bool),
		pending:         make(map[uint64][]pendingVotorBlock),
		receivedShred:   make(map[uint64]bool),
		timeoutsSet:     make(map[uint64]bool),
		executedBlocks:  make(map[alpenglow.BlockID]bool),
		highestFinal:    root.Slot,
		lastFinalizedAt: time.Now(),
		lastWarn:        make(map[uint64]time.Time),
		landed:          make(map[alpenglow.VoteMessageKey]struct{}),
		stats:           VotingStats{Enabled: true},
	}
	// NewVotorBroadcaster performs its first peer reconciliation synchronously
	// and starts a periodic reconciler before returning. Seed the immutable
	// startup snapshot before either can read v.sets.
	for _, set := range sets {
		v.sets[set.Epoch] = cloneValidatorSet(set)
	}
	broadcaster, err := alpenglow.NewVotorBroadcaster(alpenglow.VotorBroadcasterConfig{
		Identity:     cfg.Identity,
		ShredVersion: engine.shredVersion,
		Peers: func() []alpenglow.VotorPeer {
			validators := v.votorTransportValidators()
			if len(validators) == 0 {
				return nil
			}
			return v.peerSource(validators)
		},
	})
	if err != nil {
		return nil, err
	}
	v.broadcaster = broadcaster
	if start {
		v.start()
	}
	return v, nil
}

func (v *alpenglowVoter) start() {
	v.startOnce.Do(func() {
		v.wg.Add(1)
		go v.loop()
	})
}

func (v *alpenglowVoter) enqueue(event voterEvent) error {
	// Once the loop has exited there is no consumer. Check closure before the
	// send select so a buffered events channel cannot randomly win against an
	// already-closed done channel and retain work forever.
	select {
	case <-v.done:
		return fmt.Errorf("Alpenglow voter is closed")
	default:
	}
	select {
	case <-v.done:
		return fmt.Errorf("Alpenglow voter is closed")
	case v.events <- event:
		return nil
	default:
		return fmt.Errorf("Alpenglow voter event queue is full")
	}
}

func (v *alpenglowVoter) loop() {
	defer func() {
		// A safety error exits the loop without an external Close call. Publish
		// that terminal state so producers stop enqueueing behind a dead consumer.
		v.closeOnce.Do(func() { close(v.done) })
		v.wg.Done()
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-v.done:
			return
		case event := <-v.events:
			if err := v.handle(event); err != nil {
				v.engine.latchSafetyError(fmt.Errorf("voting engine: %w", err))
				mlog.Log.Errorf("ALPENGLOW VOTING SAFETY: %v", err)
				return
			}
		case <-ticker.C:
			if err := v.handleStandstill(); err != nil {
				v.engine.latchSafetyError(fmt.Errorf("voting standstill refresh: %w", err))
				mlog.Log.Errorf("ALPENGLOW VOTING SAFETY: %v", err)
				return
			}
			v.maybeLogStats()
		}
	}
}

func (v *alpenglowVoter) handle(event voterEvent) error {
	floor := v.admissionFloor()
	if event.kind == voterEventBlock && v.engine.ensureChain().IsObjectivelyInvalidBlock(event.block.Block) {
		return nil
	}
	if event.kind == voterEventConsensus && event.consensus.Block.HasHash() &&
		v.engine.ensureChain().IsObjectivelyInvalidBlock(event.consensus.Block) {
		if event.consensus.Kind == alpenglow.ConsensusEventFinalized {
			return fmt.Errorf("finalized event targets objectively invalid block %s", event.consensus.Block)
		}
		// The event may have been queued before validation failed. Preserve any
		// already-durable history, but never add or act on the stale edge now.
		return nil
	}
	switch event.kind {
	case voterEventFirstShred:
		if event.slot > floor {
			v.receivedShred[event.slot] = true
		}
		return nil
	case voterEventValidatorSet:
		v.setsMu.Lock()
		v.sets[event.set.Epoch] = cloneValidatorSet(event.set)
		v.setsMu.Unlock()
		v.logEligibility(event.set)
		if err := v.restoreVotesForEpoch(event.set.Epoch); err != nil {
			return err
		}
		return v.checkPending()
	case voterEventRoot:
		advanced := event.root.Slot > v.history.Root
		if advanced {
			v.history.SetRoot(event.root.Slot)
			v.pruneLandedBefore(v.history.Root)
		}
		v.pruneVolatileAtOrBelow(v.admissionFloor())
		if !advanced {
			return nil
		}
		return v.saveHistory()
	case voterEventNetworkCertificate:
		v.recordNetworkCertificate(event.certificate)
		return nil
	case voterEventBlock:
		if event.block.Block.Slot <= floor {
			return nil
		}
		if !event.block.Block.HasHash() {
			return fmt.Errorf("replayed live block at slot %d has no Alpenglow block id", event.block.Block.Slot)
		}
		retainExecutedBlockID(v.executedBlocks, event.block.Block, true)
		// Block-target votes are deliberately not re-published merely because
		// they exist in history: invalid-block tombstones are process-local.
		// Successful replay of the exact identity re-establishes the positive
		// validity boundary needed to restore them safely.
		if _, err := v.restoreVotesForBlock(event.block.Block); err != nil {
			return err
		}
		if err := v.votingGateError(event.block.Block.Slot); err != nil {
			// Do not retain historical catch-up blocks. Once voting opens,
			// backfilling them could emit stale votes and grow pending state
			// without bound. Join at the next live ParentReady boundary.
			v.warnVoteSkipped(event.block.Block.Slot, err)
			return nil
		}
		v.markVotingStarted(event.block.Block.Slot, "replay")
		v.receivedShred[event.block.Block.Slot] = true
		parent := alpenglow.BlockID{Slot: event.block.ParentSlot, Hash: event.block.ParentHash}
		voted, err := v.tryNotar(event.block.Block, parent)
		if err != nil {
			return err
		}
		if voted {
			return v.checkPending()
		}
		if !v.history.VotedAt(event.block.Block.Slot) && event.block.Block.Slot > v.admissionFloor() {
			candidate := pendingVotorBlock{block: event.block.Block, parent: parent}
			if !pendingContains(v.pending[event.block.Block.Slot], candidate) {
				v.pending[event.block.Block.Slot] = append(v.pending[event.block.Block.Slot], candidate)
			}
		}
		return nil
	case voterEventCrashedLeaderTimeout:
		if event.slot <= floor || v.history.VotedAt(event.slot) || v.receivedShred[event.slot] {
			return nil
		}
		return v.trySkipWindow(event.slot)
	case voterEventBlockTimeout:
		if event.slot <= floor || v.history.VotedAt(event.slot) {
			return nil
		}
		return v.trySkipWindow(event.slot)
	case voterEventConsensus:
		return v.handleConsensus(event.consensus)
	default:
		return nil
	}
}

func (v *alpenglowVoter) handleConsensus(event alpenglow.ConsensusEvent) error {
	if event.Kind == alpenglow.ConsensusEventConflict {
		return fmt.Errorf("consensus conflict at slot %d: %s", event.Slot, event.Reason)
	}
	if event.Kind == alpenglow.ConsensusEventFinalized {
		if event.Slot > v.highestFinal {
			v.highestFinal = event.Slot
		}
		v.lastFinalizedAt = time.Now()
		v.standstillSlot = nil
		v.refreshQueue = nil
		v.refreshCursor = 0
		v.pruneVolatileAtOrBelow(v.admissionFloor())
		return nil
	}
	if event.Slot <= v.admissionFloor() {
		return nil
	}
	switch event.Kind {
	case alpenglow.ConsensusEventBlockNotarized:
		v.history.AddBlockNotarized(event.Block)
		_, err := v.tryFinal(event.Block)
		return err
	case alpenglow.ConsensusEventParentReady:
		first := v.history.AddParentReady(event.Slot, event.Block)
		if err := v.checkPending(); err != nil {
			return err
		}
		if first {
			v.setTimeouts(event.Slot)
		}
	case alpenglow.ConsensusEventSafeToNotar:
		if err := v.trySkipWindow(event.Slot); err != nil {
			return err
		}
		if v.history.IsOver(event.Slot) || v.history.HasNotarFallback(event.Block) {
			return nil
		}
		_, err := v.cast(alpenglow.NewNotarizationFallbackVote(event.Slot, event.Block.Hash), false)
		return err
	case alpenglow.ConsensusEventSafeToSkip:
		if err := v.trySkipWindow(event.Slot); err != nil {
			return err
		}
		if v.history.IsOver(event.Slot) || v.history.HasSkipFallback(event.Slot) {
			return nil
		}
		_, err := v.cast(alpenglow.NewSkipFallbackVote(event.Slot), false)
		return err
	}
	return nil
}

func (v *alpenglowVoter) admissionFloor() uint64 {
	floor := v.history.Root
	if v.highestFinal > floor {
		floor = v.highestFinal
	}
	if engineFloor := v.engine.alpenglowVoteActionFloor(); engineFloor > floor {
		floor = engineFloor
	}
	return floor
}

func (v *alpenglowVoter) pruneVolatileAtOrBelow(floor uint64) {
	for slot := range v.pending {
		if slot <= floor {
			delete(v.pending, slot)
		}
	}
	for slot := range v.receivedShred {
		if slot <= floor {
			delete(v.receivedShred, slot)
		}
	}
	for slot := range v.timeoutsSet {
		if slot <= floor {
			delete(v.timeoutsSet, slot)
		}
	}
	for block := range v.executedBlocks {
		if block.Slot <= floor {
			delete(v.executedBlocks, block)
		}
	}
	for slot := range v.lastWarn {
		if slot <= floor {
			delete(v.lastWarn, slot)
		}
	}
	for vote := range v.restored {
		if vote.Slot <= floor {
			delete(v.restored, vote)
		}
	}
}

func (v *alpenglowVoter) pruneLandedBefore(root uint64) {
	v.landingMu.Lock()
	for vote := range v.landed {
		if vote.Slot < root {
			delete(v.landed, vote)
		}
	}
	v.landingMu.Unlock()
}

func (v *alpenglowVoter) tryNotar(block, parent alpenglow.BlockID) (bool, error) {
	if block.Slot <= v.admissionFloor() {
		return false, nil
	}
	if v.engine.ensureChain().IsObjectivelyInvalidBlock(block) ||
		v.engine.ensureChain().IsObjectivelyInvalidBlock(parent) {
		return false, nil
	}
	if v.history.VotedAt(block.Slot) {
		return false, nil
	}
	if block.Slot == 1 || block.Slot%alpenglow.LeaderWindowSlots == 0 {
		if !v.history.IsParentReady(block.Slot, parent) {
			return false, nil
		}
	} else {
		if parent.Slot+1 != block.Slot {
			return false, nil
		}
		hash, ok := v.history.NotarizedVote(parent.Slot)
		if !ok || hash != parent.Hash {
			return false, nil
		}
	}
	voted, err := v.cast(alpenglow.NewNotarizationVote(block.Slot, block.Hash), false)
	if err != nil || !voted {
		return voted, err
	}
	delete(v.pending, block.Slot)
	_, err = v.tryFinal(block)
	return true, err
}

func (v *alpenglowVoter) tryFinal(block alpenglow.BlockID) (bool, error) {
	if block.Slot <= v.admissionFloor() {
		return false, nil
	}
	if v.engine.ensureChain().IsObjectivelyInvalidBlock(block) {
		return false, nil
	}
	if !v.history.IsBlockNotarized(block) || v.history.IsOver(block.Slot) || v.history.BadWindow(block.Slot) {
		return false, nil
	}
	hash, ok := v.history.NotarizedVote(block.Slot)
	if !ok || hash != block.Hash {
		return false, nil
	}
	return v.castTarget(alpenglow.NewFinalizationVote(block.Slot), false, block, true)
}

func (v *alpenglowVoter) trySkipWindow(slot uint64) error {
	floor := v.admissionFloor()
	if slot <= floor {
		return nil
	}
	start := slot - slot%alpenglow.LeaderWindowSlots
	if start <= floor {
		start = floor + 1
	}
	if start < 1 {
		start = 1
	}
	end := slot - slot%alpenglow.LeaderWindowSlots + alpenglow.LeaderWindowSlots - 1
	if start > end {
		return nil
	}
	for s := start; s <= end; s++ {
		if v.history.VotedAt(s) {
			continue
		}
		if _, err := v.cast(alpenglow.NewSkipVote(s), false); err != nil {
			return err
		}
	}
	return nil
}

func (v *alpenglowVoter) checkPending() error {
	floor := v.admissionFloor()
	slots := make([]uint64, 0, len(v.pending))
	for slot := range v.pending {
		if slot <= floor || v.history.VotedAt(slot) {
			delete(v.pending, slot)
			continue
		}
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	var candidates []pendingVotorBlock
	for _, slot := range slots {
		candidates = append(candidates, v.pending[slot]...)
	}
	for _, candidate := range candidates {
		if _, err := v.tryNotar(candidate.block, candidate.parent); err != nil {
			return err
		}
	}
	return nil
}

func (v *alpenglowVoter) cast(vote alpenglow.Vote, restoring bool) (bool, error) {
	guardedBlock, hasGuard := v.voteGuard(vote)
	return v.castTarget(vote, restoring, guardedBlock, hasGuard)
}

func (v *alpenglowVoter) voteGuard(vote alpenglow.Vote) (alpenglow.BlockID, bool) {
	if block, ok := vote.Block(); ok {
		return block, true
	}
	if vote.Type == alpenglow.VoteTypeFinalize {
		if hash, ok := v.history.NotarizedVote(vote.Slot); ok {
			return alpenglow.BlockID{Slot: vote.Slot, Hash: hash}, true
		}
	}
	return alpenglow.BlockID{}, false
}

func (v *alpenglowVoter) castTarget(vote alpenglow.Vote, restoring bool, guardedBlock alpenglow.BlockID, hasGuard bool) (bool, error) {
	if !restoring {
		if vote.Slot <= v.admissionFloor() {
			return false, nil
		}
		if err := v.history.ValidateVote(vote); err != nil {
			return false, nil
		}
	}
	if hasGuard {
		// Finalize is slot-scoped on the wire, so tryFinal/voteGuard supply its
		// notarized block explicitly. Other block-target votes carry the guard.
		if !restoring && v.beforeVoteGuard != nil {
			v.beforeVoteGuard(guardedBlock)
		}
		// Hold through signing, durable history, pool admission, and broadcast.
		// Objective invalidation takes the write side before changing the chain,
		// so a new or restored vote is wholly before it or sees the tombstone.
		v.engine.invalidActionMu.RLock()
		defer v.engine.invalidActionMu.RUnlock()
		if v.engine.ensureChain().IsObjectivelyInvalidBlock(guardedBlock) {
			return false, nil
		}
	}
	message, result, err := v.sign(vote, !restoring)
	if err != nil {
		if errors.Is(err, errVoterNotEligible) || errors.Is(err, errVoterNotReady) {
			v.warnVoteSkipped(vote.Slot, err)
			return false, nil
		}
		return false, err
	}
	if restoring && v.restored[message.Key()] {
		return false, nil
	}
	if !restoring {
		// Finality can advance while the BLS signature is computed. Avoid a
		// durable stale record when that race is already visible here; if it
		// advances later, atomic admission below classifies it benignly.
		if vote.Slot <= v.admissionFloor() {
			return false, nil
		}
		if err := v.history.AddVote(vote); err != nil {
			return false, fmt.Errorf("record %s vote at slot %d: %w", vote.Type, vote.Slot, err)
		}
		// Pool admission may synchronously assemble and publish a certificate.
		// Persist the anti-equivocation record first so no externally visible
		// proof can survive a crash without its signed local history.
		if err := v.saveHistory(); err != nil {
			return false, err
		}
		if v.beforeLocalVoteInject != nil {
			v.beforeLocalVoteInject(vote)
		}
	}
	if err := v.engine.injectLocalVote(message, result); err != nil {
		if errors.Is(err, errLocalVoteStale) {
			v.warnVoteSkipped(vote.Slot, err)
			return false, nil
		}
		return false, fmt.Errorf("inject local %s vote at slot %d: %w", vote.Type, vote.Slot, err)
	}
	if restoring {
		if err := v.broadcaster.Enqueue(alpenglow.Message{Vote: &message}); err != nil {
			return false, fmt.Errorf("rebroadcast restored %s vote at slot %d: %w", vote.Type, vote.Slot, err)
		}
		v.restored[message.Key()] = true
		mlog.Log.FileOnlyf("ALPENGLOW voting: restored and rebroadcast %s vote slot=%d rank=%d block=%s", vote.Type, vote.Slot, message.Rank, vote.BlockHash)
		return true, nil
	}
	if err := v.broadcaster.Enqueue(alpenglow.Message{Vote: &message}); err != nil {
		return false, fmt.Errorf("broadcast persisted %s vote at slot %d: %w", vote.Type, vote.Slot, err)
	}
	v.markVotingStarted(vote.Slot, "vote")
	v.landingMu.Lock()
	v.stats.VotesCastThisRun++
	v.landingMu.Unlock()
	mlog.Log.FileOnlyf("ALPENGLOW voting: cast %s vote slot=%d rank=%d block=%s", vote.Type, vote.Slot, message.Rank, vote.BlockHash)
	return true, nil
}

func (v *alpenglowVoter) sign(vote alpenglow.Vote, respectVotingGate bool) (alpenglow.VoteMessage, alpenglow.VoteVerifyResult, error) {
	if respectVotingGate {
		if err := v.votingGateError(vote.Slot); err != nil {
			return alpenglow.VoteMessage{}, alpenglow.VoteVerifyResult{}, err
		}
	}
	set, ok := v.validatorSet(vote.Slot)
	if !ok {
		return alpenglow.VoteMessage{}, alpenglow.VoteVerifyResult{}, fmt.Errorf("%w: no validator set for slot %d", errVoterNotEligible, vote.Slot)
	}
	public := v.signer.PublicKeyCompressed()
	for _, validator := range set.Validators {
		if validator.VoteAccount != v.voteAccount {
			continue
		}
		if validator.NodePubkey != v.node {
			return alpenglow.VoteMessage{}, alpenglow.VoteVerifyResult{}, fmt.Errorf("%w: vote account %s belongs to node %s, not identity %s", errVoterNotEligible, v.voteAccount, validator.NodePubkey, v.node)
		}
		if validator.BlsPubkeyCompressed != public {
			return alpenglow.VoteMessage{}, alpenglow.VoteVerifyResult{}, fmt.Errorf("%w: authorized-voter BLS key does not match vote account %s", errVoterNotEligible, v.voteAccount)
		}
		payload, err := alpenglow.EncodeVotePayloadToSign(vote, v.engine.shredVersion)
		if err != nil {
			return alpenglow.VoteMessage{}, alpenglow.VoteVerifyResult{}, err
		}
		signature, err := v.signer.Sign(payload)
		if err != nil {
			return alpenglow.VoteMessage{}, alpenglow.VoteVerifyResult{}, err
		}
		message := alpenglow.VoteMessage{Vote: vote, Signature: signature, Rank: validator.Rank, At: time.Now()}
		result := alpenglow.VoteVerifyResult{Epoch: set.Epoch, Rank: validator.Rank, Stake: validator.Stake, TotalStake: set.TotalStake}
		return message, result, nil
	}
	return alpenglow.VoteMessage{}, alpenglow.VoteVerifyResult{}, fmt.Errorf("%w: vote account %s has no BLS rank in epoch %d", errVoterNotEligible, v.voteAccount, set.Epoch)
}

func (v *alpenglowVoter) votingGateError(slot uint64) error {
	if floor := v.admissionFloor(); slot <= floor {
		return fmt.Errorf("%w: slot %d is at or below consensus action floor %d", errVoterNotReady, slot, floor)
	}
	if slot < v.waitToVoteSlot {
		return fmt.Errorf("%w: waiting for startup watermark slot %d", errVoterNotReady, v.waitToVoteSlot)
	}
	// ReadyToVote is a startup join guard, not a perpetual clock check. Once an
	// accepted live block or vote joins Votor, verified ParentReady and timeout
	// events are the clock authority (as in Agave). A locally extrapolated wall
	// slot can drift ahead of a healthy cluster and must not later stop voting.
	if !v.votingStarted && v.readyToVote != nil && !v.readyToVote(slot) {
		return fmt.Errorf("%w: slot is still behind the startup live voting window", errVoterNotReady)
	}
	return nil
}

func (v *alpenglowVoter) markVotingStarted(slot uint64, source string) {
	if slot > v.latestLiveSlot {
		v.latestLiveSlot = slot
	}
	if v.votingStarted {
		return
	}
	v.votingStarted = true
	v.lastFinalizedAt = time.Now()
	mlog.Log.FileOnlyf("ALPENGLOW voting: joined the live voting window at slot %d (source=%s)", slot, source)
}

func (v *alpenglowVoter) validatorSet(slot uint64) (alpenglow.ValidatorSet, bool) {
	epoch := v.epochForSlot(slot)
	v.setsMu.RLock()
	set, ok := v.sets[epoch]
	v.setsMu.RUnlock()
	return set, ok
}

// votorTransportValidators mirrors Agave's merged transport peer list rather
// than sending only to the message slot's validator set. This keeps trailing
// reward traffic connected across an epoch boundary and preconnects the next
// set inside the vote-verification horizon.
func (v *alpenglowVoter) votorTransportValidators() []alpenglow.ValidatorStake {
	epochs, ok := v.engine.votorPeerEpochs()
	if !ok {
		return nil
	}
	v.setsMu.RLock()
	defer v.setsMu.RUnlock()
	seen := make(map[solana.PublicKey]struct{})
	validators := make([]alpenglow.ValidatorStake, 0)
	localAdmitted := false
	for _, epoch := range epochs {
		set, exists := v.sets[epoch]
		if !exists {
			continue
		}
		for _, validator := range set.Validators {
			if validator.NodePubkey == v.node {
				localAdmitted = true
			}
			if _, duplicate := seen[validator.NodePubkey]; duplicate {
				continue
			}
			seen[validator.NodePubkey] = struct{}{}
			validators = append(validators, validator)
		}
	}
	if !localAdmitted {
		return nil
	}
	return validators
}

func (v *alpenglowVoter) restoreVotesForEpoch(epoch uint64) error {
	floor := v.admissionFloor()
	for _, vote := range v.history.VotesAfter(v.history.Root - minU64(v.history.Root, 1)) {
		// Keep every signed history entry for anti-equivocation, but do not
		// restore votes that finality or the pool admission root has retired.
		if vote.Slot <= floor {
			continue
		}
		if v.epochForSlot(vote.Slot) != epoch {
			continue
		}
		if vote.Type == alpenglow.VoteTypeNotarize || vote.Type == alpenglow.VoteTypeSkip {
			// Provenance is not a tally, but it still represents a vote this
			// configured authorized voter could have signed in the slot's epoch.
			// Do not let an unrelated/corrupt history seed local safety state.
			message, _, err := v.sign(vote, false)
			if err != nil {
				if errors.Is(err, errVoterNotEligible) {
					return nil
				}
				return err
			}
			if err := v.engine.restoreLocalFirstVoteProvenance(vote, message.Rank); err != nil {
				return err
			}
		}
		guard, guarded := v.voteGuard(vote)
		if vote.Type == alpenglow.VoteTypeFinalize && !guarded {
			continue
		}
		if guarded && !v.executedBlocks[guard] {
			continue
		}
		if _, err := v.castTarget(vote, true, guard, guarded); err != nil {
			if errors.Is(err, errVoterNotEligible) {
				return nil
			}
			return err
		}
	}
	return nil
}

// restoreVotesForBlock re-publishes durable block-target votes only after this
// process has successfully replayed the exact block. This closes the
// persist-before-admission crash window without trusting process-local invalid
// block state across a restart.
func (v *alpenglowVoter) restoreVotesForBlock(block alpenglow.BlockID) (bool, error) {
	if block.Slot <= v.admissionFloor() {
		return false, nil
	}
	restoredAny := false
	for _, vote := range v.history.VotesCast[block.Slot] {
		guard, guarded := v.voteGuard(vote)
		if !guarded || guard != block {
			continue
		}
		restored, err := v.castTarget(vote, true, guard, true)
		if err != nil {
			return restoredAny, err
		}
		restoredAny = restoredAny || restored
	}
	if v.history.VotedAt(block.Slot) {
		delete(v.pending, block.Slot)
	}
	return restoredAny, nil
}

func (v *alpenglowVoter) setTimeouts(slot uint64) {
	if slot <= v.admissionFloor() {
		return
	}
	if v.timeoutsSet[slot] {
		return
	}
	v.timeoutsSet[slot] = true
	multiplier := 1.0
	if v.standstillSlot != nil {
		windows := 0.0
		if slot > *v.standstillSlot {
			windows = float64(slot-*v.standstillSlot) / float64(alpenglow.LeaderWindowSlots)
		}
		if windows > 0 {
			// Clamp before exponentiation as well as after duration conversion;
			// a long snapshot catch-up must not overflow into a negative timer.
			maxWindows := math.Log(float64(time.Hour)/float64(votorDeltaTimeout)) / math.Log(1.05)
			if windows > maxWindows {
				windows = maxWindows
			}
			multiplier = math.Pow(1.05, math.Floor(windows))
		}
	}
	delta := time.Duration(float64(votorDeltaTimeout) * multiplier)
	if delta > time.Hour {
		delta = time.Hour
	}
	first := delta + v.slotDuration
	time.AfterFunc(first, func() {
		if err := v.enqueue(voterEvent{kind: voterEventCrashedLeaderTimeout, slot: slot}); err != nil {
			v.latchTimerEnqueueError(err)
		}
	})
	for offset := uint64(0); offset < alpenglow.LeaderWindowSlots; offset++ {
		s := slot + offset
		delay := first + time.Duration(offset)*v.slotDuration
		time.AfterFunc(delay, func() {
			if err := v.enqueue(voterEvent{kind: voterEventBlockTimeout, slot: s}); err != nil {
				v.latchTimerEnqueueError(err)
			}
		})
	}
}

func (v *alpenglowVoter) latchTimerEnqueueError(err error) {
	select {
	case <-v.done:
		return
	default:
		v.engine.latchSafetyError(err)
	}
}

func (v *alpenglowVoter) handleStandstill() error {
	if !v.votingStarted || !v.isIdentityStaked(v.latestLiveSlot) {
		return nil
	}
	if time.Since(v.lastFinalizedAt) < votorStandstillTimeout {
		return nil
	}
	if v.standstillSlot == nil {
		slot := v.highestFinal
		v.standstillSlot = &slot
		v.refreshQueue = v.buildRefreshQueue()
		v.refreshCursor = 0
		mlog.Log.FileOnlyf("ALPENGLOW voting: standstill after finalized slot %d; refreshing %d votes/certificates", slot, len(v.refreshQueue))
	}
	if len(v.refreshQueue) == 0 {
		return nil
	}
	inspectLimit := len(v.refreshQueue)
	for sent, inspected := 0, 0; sent < votorRefreshPerSecond && inspected < inspectLimit && len(v.refreshQueue) > 0; inspected++ {
		message := v.refreshQueue[v.refreshCursor]
		queued, err := v.enqueueRefreshMessage(message)
		if err != nil {
			return err
		}
		if !queued {
			v.refreshQueue = append(v.refreshQueue[:v.refreshCursor], v.refreshQueue[v.refreshCursor+1:]...)
			if len(v.refreshQueue) == 0 {
				v.refreshCursor = 0
				break
			}
			if v.refreshCursor >= len(v.refreshQueue) {
				v.refreshCursor = 0
			}
			continue
		}
		sent++
		v.refreshCursor = (v.refreshCursor + 1) % len(v.refreshQueue)
		if v.refreshCursor == 0 {
			break
		}
	}
	return nil
}

func (v *alpenglowVoter) buildRefreshQueue() []alpenglow.Message {
	queue := make([]alpenglow.Message, 0)
	floor := v.admissionFloor()
	for _, vote := range v.history.VotesAfter(v.highestFinal) {
		if vote.Slot <= floor {
			continue
		}
		if guard, guarded := v.voteGuard(vote); guarded &&
			(!v.executedBlocks[guard] || v.engine.ensureChain().IsObjectivelyInvalidBlock(guard)) {
			continue
		}
		message, _, err := v.sign(vote, true)
		if err == nil {
			queue = append(queue, alpenglow.Message{Vote: &message})
		}
	}
	for _, certificate := range v.engine.ensurePool().CertificatesAtOrAfter(v.highestFinal) {
		if block, guarded := certificate.Block(); guarded && v.engine.ensureChain().IsObjectivelyInvalidBlock(block) {
			continue
		}
		cert := certificate
		queue = append(queue, alpenglow.Message{Certificate: &cert})
	}
	return queue
}

func (v *alpenglowVoter) enqueueRefreshMessage(message alpenglow.Message) (bool, error) {
	if message.Vote != nil && message.Vote.Vote.Slot <= v.admissionFloor() {
		return false, nil
	}
	guard, guarded, needsExecution := v.refreshMessageGuard(message)
	if !guarded {
		return true, v.broadcaster.Enqueue(message)
	}
	v.engine.invalidActionMu.RLock()
	defer v.engine.invalidActionMu.RUnlock()
	if v.engine.ensureChain().IsObjectivelyInvalidBlock(guard) || needsExecution && !v.executedBlocks[guard] {
		return false, nil
	}
	return true, v.broadcaster.Enqueue(message)
}

func (v *alpenglowVoter) refreshMessageGuard(message alpenglow.Message) (alpenglow.BlockID, bool, bool) {
	if message.Vote != nil {
		block, guarded := v.voteGuard(message.Vote.Vote)
		return block, guarded, guarded
	}
	if message.Certificate != nil {
		block, guarded := message.Certificate.Block()
		return block, guarded, false
	}
	return alpenglow.BlockID{}, false, false
}

func (v *alpenglowVoter) broadcastCertificates(certificates []alpenglow.Certificate) error {
	for _, certificate := range certificates {
		if !v.isIdentityStaked(certificate.Slot) {
			continue
		}
		if block, guarded := certificate.Block(); guarded && v.engine.ensureChain().IsObjectivelyInvalidBlock(block) {
			continue
		}
		cert := certificate
		if err := v.broadcaster.Enqueue(alpenglow.Message{Certificate: &cert}); err != nil {
			return err
		}
	}
	return nil
}

func (v *alpenglowVoter) isIdentityStaked(slot uint64) bool {
	set, ok := v.validatorSet(slot)
	if !ok {
		return false
	}
	for _, validator := range set.Validators {
		if validator.NodePubkey == v.node && validator.Stake > 0 {
			return true
		}
	}
	return false
}

func (v *alpenglowVoter) saveHistory() error {
	if err := alpenglow.SaveVoteHistory(v.historyDir, v.history, v.identity); err != nil {
		return fmt.Errorf("persist vote history before consensus publication: %w", err)
	}
	return nil
}

func (v *alpenglowVoter) recordNetworkCertificate(cert alpenglow.Certificate) {
	set, ok := v.validatorSet(cert.Slot)
	if !ok {
		return
	}
	public := v.signer.PublicKeyCompressed()
	var rank uint16
	eligible := false
	for _, validator := range set.Validators {
		if validator.VoteAccount == v.voteAccount && validator.NodePubkey == v.node && validator.BlsPubkeyCompressed == public {
			rank = validator.Rank
			eligible = true
			break
		}
	}
	if !eligible {
		return
	}

	bitmap, err := alpenglow.DecodeSignerStoreBitmap(cert.Bitmap, alpenglow.CertificateBitmapCapacity)
	if err != nil || int(rank) >= bitmap.Length {
		return
	}
	useFallback := int(rank) < len(bitmap.Fallback) && bitmap.Fallback[rank]
	usePrimary := int(rank) < len(bitmap.Base) && bitmap.Base[rank]
	if !usePrimary && !useFallback {
		return
	}
	vote, ok := certificateVoteForSigner(cert, useFallback)
	if !ok || !voteHistoryContains(v.history, vote) {
		// A certificate can mention our rank only as evidence for a vote this
		// process persisted. This guard prevents network data from manufacturing
		// an operational success metric for an uncast or conflicting vote.
		return
	}

	key := alpenglow.VoteMessageKey{Type: vote.Type, Slot: vote.Slot, BlockHash: vote.BlockHash, Rank: rank}
	v.landingMu.Lock()
	if _, exists := v.landed[key]; exists {
		v.landingMu.Unlock()
		return
	}
	v.landed[key] = struct{}{}
	v.stats.NetworkLandedVotes++
	v.stats.LastNetworkLandedSlot = vote.Slot
	v.stats.LastNetworkLandedVoteType = vote.Type
	v.stats.LastNetworkCertificateType = cert.Type
	v.stats.LastNetworkLandedAt = time.Now()
	landed := v.stats.NetworkLandedVotes
	v.landingMu.Unlock()

	block := "-"
	if vote.BlockHash != (solana.Hash{}) {
		block = vote.BlockHash.String()
	}
	mlog.Log.FileOnlyf("ALPENGLOW voting: vote landed source=votor-quic proof=verified-aggregate vote=%s slot=%d rank=%d certificate=%s block=%s network_landed=%d",
		vote.Type, vote.Slot, rank, cert.Type, block, landed)
}

func certificateVoteForSigner(cert alpenglow.Certificate, fallback bool) (alpenglow.Vote, bool) {
	switch cert.Type {
	case alpenglow.CertificateNotarize, alpenglow.CertificateFinalizeFast:
		if fallback {
			return alpenglow.Vote{}, false
		}
		return alpenglow.NewNotarizationVote(cert.Slot, cert.BlockHash), true
	case alpenglow.CertificateFinalize:
		if fallback {
			return alpenglow.Vote{}, false
		}
		return alpenglow.NewFinalizationVote(cert.Slot), true
	case alpenglow.CertificateGenesis:
		if fallback {
			return alpenglow.Vote{}, false
		}
		return alpenglow.NewGenesisVote(cert.Slot, cert.BlockHash), true
	case alpenglow.CertificateNotarizeFallback:
		if fallback {
			return alpenglow.NewNotarizationFallbackVote(cert.Slot, cert.BlockHash), true
		}
		return alpenglow.NewNotarizationVote(cert.Slot, cert.BlockHash), true
	case alpenglow.CertificateSkip:
		if fallback {
			return alpenglow.NewSkipFallbackVote(cert.Slot), true
		}
		return alpenglow.NewSkipVote(cert.Slot), true
	default:
		return alpenglow.Vote{}, false
	}
}

func voteHistoryContains(history *alpenglow.VoteHistory, vote alpenglow.Vote) bool {
	if history == nil {
		return false
	}
	for _, cast := range history.VotesCast[vote.Slot] {
		if cast == vote {
			return true
		}
	}
	return false
}

func (v *alpenglowVoter) snapshot() VotingStats {
	v.landingMu.RLock()
	stats := v.stats
	v.landingMu.RUnlock()
	stats.Enabled = true
	if v.broadcaster != nil {
		broadcast := v.broadcaster.Stats()
		stats.BroadcastMessagesQueued = broadcast.MessagesQueued
		stats.BroadcastMessagesDropped = broadcast.MessagesDropped
		stats.BroadcastPeerSends = broadcast.PeerSends
		stats.BroadcastPeerSendsSkipped = broadcast.PeerSendsSkipped
		stats.BroadcastPeerSendErrors = broadcast.PeerSendErrors
		stats.BroadcastDesiredPeers = broadcast.DesiredPeers
		stats.BroadcastActiveConnections = broadcast.Connections
		stats.BroadcastPendingConnections = broadcast.PendingConnections
		stats.BroadcastConnectionAttempts = broadcast.ConnectionAttempts
		stats.BroadcastConnectionErrors = broadcast.ConnectionErrors
		stats.BroadcastConnectionJobsDropped = broadcast.ConnectionJobsDropped
		stats.BroadcastLastPeerSendError = broadcast.LastPeerSendError
		stats.BroadcastLastPeerSendErrorAt = broadcast.LastPeerSendErrorAt
		stats.BroadcastLastConnectionError = broadcast.LastConnectionError
		stats.BroadcastLastConnectionErrorAt = broadcast.LastConnectionErrorAt
	}
	return stats
}

func (v *alpenglowVoter) maybeLogStats() {
	if !v.lastStatsLog.IsZero() && time.Since(v.lastStatsLog) < votorStatsLogInterval {
		return
	}
	v.lastStatsLog = time.Now()
	stats := v.snapshot()
	mlog.Log.FileOnlyf("alpenglow voting stats: votes_cast_this_run=%d network_landed=%d last_landed_slot=%d broadcast_queued=%d broadcast_dropped=%d peer_sends=%d peer_sends_skipped=%d peer_send_errors=%d desired_peers=%d active_connections=%d pending_connections=%d connection_attempts=%d connection_errors=%d connection_jobs_dropped=%d",
		stats.VotesCastThisRun,
		stats.NetworkLandedVotes,
		stats.LastNetworkLandedSlot,
		stats.BroadcastMessagesQueued,
		stats.BroadcastMessagesDropped,
		stats.BroadcastPeerSends,
		stats.BroadcastPeerSendsSkipped,
		stats.BroadcastPeerSendErrors,
		stats.BroadcastDesiredPeers,
		stats.BroadcastActiveConnections,
		stats.BroadcastPendingConnections,
		stats.BroadcastConnectionAttempts,
		stats.BroadcastConnectionErrors,
		stats.BroadcastConnectionJobsDropped,
	)
}

func (v *alpenglowVoter) logEligibility(set alpenglow.ValidatorSet) {
	public := v.signer.PublicKeyCompressed()
	for _, validator := range set.Validators {
		if validator.VoteAccount != v.voteAccount {
			continue
		}
		if validator.NodePubkey == v.node && validator.BlsPubkeyCompressed == public {
			mlog.Log.FileOnlyf("ALPENGLOW voting: eligible in epoch %d as rank %d (stake=%d vote_account=%s)", set.Epoch, validator.Rank, validator.Stake, v.voteAccount)
			return
		}
		break
	}
	mlog.Log.FileOnlyf("ALPENGLOW voting: identity/vote-account/BLS key is not eligible in epoch %d; remaining observer-only for that epoch", set.Epoch)
}

func (v *alpenglowVoter) warnVoteSkipped(slot uint64, err error) {
	epoch := v.epochForSlot(slot)
	if last := v.lastWarn[epoch]; !last.IsZero() && time.Since(last) < time.Minute {
		return
	}
	v.lastWarn[epoch] = time.Now()
	mlog.Log.FileOnlyf("ALPENGLOW voting: not casting at slot %d: %v", slot, err)
}

func (v *alpenglowVoter) close() error {
	if v == nil {
		return nil
	}
	v.closeOnce.Do(func() { close(v.done) })
	v.wg.Wait()
	return v.broadcaster.Close()
}

func pendingContains(blocks []pendingVotorBlock, candidate pendingVotorBlock) bool {
	for _, block := range blocks {
		if block == candidate {
			return true
		}
	}
	return false
}

func cloneValidatorSet(set alpenglow.ValidatorSet) alpenglow.ValidatorSet {
	set.Validators = append([]alpenglow.ValidatorStake(nil), set.Validators...)
	return set
}

func minU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
