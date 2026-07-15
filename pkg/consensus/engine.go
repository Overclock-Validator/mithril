package consensus

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/gagliardetto/solana-go"
)

type Mode string

const (
	ModeClassic           Mode = "classic"
	ModeAlpenglowObserver Mode = "alpenglow-observer"
	ModeAlpenglow         Mode = "alpenglow"
)

var ErrAlpenglowVotingNotImplemented = errors.New("alpenglow voting mode is not implemented yet; use consensus.mode=\"alpenglow-observer\"")

const (
	maxRecentAlpenglowBlockIDs          = 8192
	alpenglowVoteVerifySamplesPerWindow = 16
)

type BlockObservation struct {
	Block  *block.Block
	Source string
	At     time.Time
}

type SlotReplayResult struct {
	Slot     uint64
	Bankhash [32]byte
	Source   string
	At       time.Time
}

type AlpenglowBlockIDSink func(slot uint64, blockID solana.Hash)

type AlpenglowBlockIDPublisher interface {
	SetAlpenglowBlockIDSink(sink AlpenglowBlockIDSink)
}

type AlpenglowRepairPrioritySink func(slot uint64)

type AlpenglowRepairPriorityPublisher interface {
	SetAlpenglowRepairPrioritySink(sink AlpenglowRepairPrioritySink)
}

type AlpenglowDecisionSource interface {
	NextAlpenglowDecision(anchorSlot uint64) (alpenglow.ChainDecision, bool)
}

type AlpenglowCandidateBlockObserver interface {
	ObserveAlpenglowCandidateBlock(obs alpenglow.ReplayBlockObservation)
}

type AlpenglowValidatorSetSink interface {
	SetAlpenglowValidatorSet(set alpenglow.ValidatorSet) error
}

type AlpenglowEpochLookupSink interface {
	SetAlpenglowEpochLookup(fn func(slot uint64) uint64)
}

type AlpenglowRootSink interface {
	SetAlpenglowRoot(block alpenglow.BlockID)
}

type AlpenglowEventSink func(alpenglow.ConsensusEvent)

type AlpenglowEventPublisher interface {
	SetAlpenglowEventSink(sink AlpenglowEventSink)
}

type AlpenglowParentSource interface {
	AlpenglowBlockProductionParent(slot uint64) alpenglow.BlockProductionParent
}

type Snapshot struct {
	Mode           Mode                             `json:"mode"`
	ObservedBlocks uint64                           `json:"observed_blocks"`
	ReplayedSlots  uint64                           `json:"replayed_slots"`
	Alpenglow      *alpenglow.Snapshot              `json:"alpenglow,omitempty"`
	AlpenglowChain *alpenglow.ChainSnapshot         `json:"alpenglow_chain,omitempty"`
	AlpenglowPool  *alpenglow.ConsensusPoolSnapshot `json:"alpenglow_pool,omitempty"`
	Receiver       *alpenglow.ReceiverStats         `json:"receiver,omitempty"`
}

type Engine interface {
	Name() string
	Start(ctx context.Context) error
	ObserveBlock(ctx context.Context, obs BlockObservation) error
	OnReplayResult(ctx context.Context, result SlotReplayResult) error
	Snapshot() Snapshot
	Close() error
}

type Config struct {
	AlpenglowObserverBindAddr  string
	AlpenglowMaxMessageBytes   int64
	AlpenglowVoteVerifyWorkers int
	AlpenglowVoteVerifyQueue   int
	AlpenglowShredVersion      uint16
}

func NormalizeMode(raw string) (Mode, error) {
	mode := Mode(strings.ToLower(strings.TrimSpace(raw)))
	if mode == "" {
		return ModeClassic, nil
	}
	if mode == "legacy" {
		return ModeClassic, nil
	}

	switch mode {
	case ModeClassic, ModeAlpenglowObserver, ModeAlpenglow:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid consensus.mode %q (must be \"classic\", \"alpenglow-observer\", or \"alpenglow\")", raw)
	}
}

func NewEngine(mode Mode) (Engine, error) {
	return NewEngineWithConfig(mode, Config{})
}

func NewEngineWithConfig(mode Mode, cfg Config) (Engine, error) {
	switch mode {
	case ModeClassic:
		return &ClassicEngine{}, nil
	case ModeAlpenglowObserver:
		verifier := alpenglow.NewCertificateVerifier()
		verifier.SetShredVersion(cfg.AlpenglowShredVersion)
		engine := &AlpenglowObserverEngine{
			observer:                alpenglow.NewObserver(),
			chain:                   newAlpenglowObserverChainTracker(),
			verifier:                verifier,
			receiverBindAddr:        strings.TrimSpace(cfg.AlpenglowObserverBindAddr),
			receiverMaxMessageBytes: cfg.AlpenglowMaxMessageBytes,
			recentBlockIDs:          make(map[uint64]solana.Hash),
			voteVerifyWorkers:       cfg.AlpenglowVoteVerifyWorkers,
			voteVerifyQueueSize:     cfg.AlpenglowVoteVerifyQueue,
		}
		engine.pool = alpenglow.NewConsensusPool(alpenglow.DefaultConsensusPoolConfig())
		return engine, nil
	case ModeAlpenglow:
		return &AlpenglowEngine{}, nil
	default:
		return nil, fmt.Errorf("unsupported consensus mode %q", mode)
	}
}

type ClassicEngine struct {
	observedBlocks atomic.Uint64
	replayedSlots  atomic.Uint64
}

func (e *ClassicEngine) Name() string { return string(ModeClassic) }

func (e *ClassicEngine) Start(context.Context) error {
	mlog.Log.Infof("Consensus engine started: %s", e.Name())
	return nil
}

func (e *ClassicEngine) ObserveBlock(_ context.Context, obs BlockObservation) error {
	if obs.Block != nil {
		e.observedBlocks.Add(1)
	}
	return nil
}

func (e *ClassicEngine) OnReplayResult(_ context.Context, result SlotReplayResult) error {
	if result.Slot != 0 {
		e.replayedSlots.Add(1)
	}
	return nil
}

func (e *ClassicEngine) Snapshot() Snapshot {
	return Snapshot{
		Mode:           ModeClassic,
		ObservedBlocks: e.observedBlocks.Load(),
		ReplayedSlots:  e.replayedSlots.Load(),
	}
}

func (e *ClassicEngine) Close() error { return nil }

type AlpenglowObserverEngine struct {
	observedBlocks          atomic.Uint64
	replayedSlots           atomic.Uint64
	observer                *alpenglow.Observer
	chain                   *alpenglow.ChainTracker
	verifier                *alpenglow.CertificateVerifier
	pool                    *alpenglow.ConsensusPool
	receiverBindAddr        string
	receiverMaxMessageBytes int64
	receiver                *alpenglow.Receiver
	blockIDSinkMu           sync.RWMutex
	blockIDSink             AlpenglowBlockIDSink
	repairPrioritySinkMu    sync.RWMutex
	repairPrioritySink      AlpenglowRepairPrioritySink
	recentBlockIDs          map[uint64]solana.Hash
	recentBlockIDOrder      []uint64
	epochLookupMu           sync.RWMutex
	epochForSlot            func(slot uint64) uint64
	certVerifyLogMu         sync.Mutex
	certVerifyDropCount     uint64
	certVerifyDetailCount   uint64
	lastCertVerifyLog       time.Time
	voteVerifyLogMu         sync.Mutex
	voteVerifyWindowStart   time.Time
	voteVerifySamples       int
	voteVerifyChecked       uint64
	voteVerifyOK            uint64
	voteVerifyFailed        uint64
	voteVerifyNoSet         uint64
	lastVoteVerifyLog       time.Time
	lastVoteVerifyErr       string
	votorMessageHookMu      sync.RWMutex
	votorMessageHook        func(alpenglow.Message)
	eventSinkMu             sync.RWMutex
	eventSink               AlpenglowEventSink
	voteVerifyWorkers       int
	voteVerifyQueueSize     int
	voteVerifyQueue         chan alpenglow.VoteMessage
	voteVerifyCancel        context.CancelFunc
	voteVerifyWG            sync.WaitGroup
	voteVerifyDropped       atomic.Uint64
	footerFinalMu           sync.Mutex
	pendingFooterFinals     map[uint64][]byte
}

// SetVotorMessageHook registers a handler for verified Votor messages. Raw
// network input is never delivered to this hook. Block production uses it to
// accumulate skip/notar votes for footer reward certificates.
func (e *AlpenglowObserverEngine) SetVotorMessageHook(fn func(alpenglow.Message)) {
	e.votorMessageHookMu.Lock()
	e.votorMessageHook = fn
	e.votorMessageHookMu.Unlock()
}

func (e *AlpenglowObserverEngine) SetAlpenglowEventSink(sink AlpenglowEventSink) {
	e.eventSinkMu.Lock()
	e.eventSink = sink
	e.eventSinkMu.Unlock()
}

func (e *AlpenglowObserverEngine) Name() string { return string(ModeAlpenglowObserver) }

func (e *AlpenglowObserverEngine) Start(ctx context.Context) error {
	observer := e.ensureObserver()
	e.ensureChain()
	e.ensureVerifier()
	e.ensurePool()
	mlog.Log.Infof("Consensus engine started: %s (passive; no votes will be signed)", e.Name())
	mlog.Log.FileOnlyf("ALPENGLOW observer: certified path resolver requires stake and aggregate BLS signature verified certificates")
	if e.receiverBindAddr == "" {
		mlog.Log.FileOnlyf("ALPENGLOW observer: Votor receiver disabled; set consensus.alpenglow_observer_bind_addr to listen for Votor QUIC messages")
		return nil
	}
	e.startVoteVerifier(ctx)

	receiver, err := alpenglow.NewReceiver(alpenglow.ReceiverConfig{
		BindAddr:        e.receiverBindAddr,
		MaxMessageBytes: e.receiverMaxMessageBytes,
		OnMessage:       e.observeVotorMessage,
		SkipObserver:    true,
	}, observer)
	if err != nil {
		return err
	}
	e.receiver = receiver
	go func() {
		if err := receiver.Run(ctx); err != nil {
			mlog.Log.Warnf("ALPENGLOW Votor receiver stopped: %v", err)
		}
	}()
	return nil
}

func (e *AlpenglowObserverEngine) ObserveBlock(_ context.Context, obs BlockObservation) error {
	if obs.Block != nil {
		e.ensurePool().NoteLiveSlot(obs.Block.Slot)
		if e.observedBlocks.Add(1) == 1 {
			mlog.Log.Infof("ALPENGLOW observer: first replay block observed at slot %d (source=%s, alpenglow_block_id=%t)", obs.Block.Slot, obs.Source, obs.Block.HasAlpenglowBlockID)
		}
		blockID := alpenglow.BlockID{Slot: obs.Block.Slot}
		if obs.Block.HasAlpenglowBlockID {
			blockID.Hash = solana.Hash(obs.Block.AlpenglowBlockID)
		}
		replayObs := alpenglow.ReplayBlockObservation{
			Block:      blockID,
			ParentSlot: alpenglowParentSlot(obs.Block),
			ParentHash: alpenglowParentBlockID(obs.Block),
			Source:     obs.Source,
			At:         obs.At,
		}
		e.enrichReplayBlockObservation(&replayObs)
		e.ensureObserver().ObserveReplayBlock(replayObs)
		if blockID.HasHash() {
			e.observeChainReplayBlock(replayObs)
			e.rememberRecentAlpenglowBlockID(blockID.Slot, blockID.Hash)
		}
		e.observeFooterFinalCertificate(obs.Block)
	}
	return nil
}

func (e *AlpenglowObserverEngine) OnReplayResult(_ context.Context, result SlotReplayResult) error {
	if result.Slot != 0 {
		e.ensurePool().NoteLiveSlot(result.Slot)
		if e.replayedSlots.Add(1) == 1 {
			mlog.Log.Infof("ALPENGLOW observer: first replay result at slot %d", result.Slot)
		}
		e.ensureObserver().ObserveReplayResult(alpenglow.ReplayResultObservation{
			Slot:     result.Slot,
			Bankhash: solana.Hash(result.Bankhash),
			Source:   result.Source,
			At:       result.At,
		})
	}
	return nil
}

func (e *AlpenglowObserverEngine) Snapshot() Snapshot {
	snapshot := Snapshot{
		Mode:           ModeAlpenglowObserver,
		ObservedBlocks: e.observedBlocks.Load(),
		ReplayedSlots:  e.replayedSlots.Load(),
	}
	if e.observer != nil {
		agSnapshot := e.observer.Snapshot()
		snapshot.Alpenglow = &agSnapshot
	}
	if e.receiver != nil {
		receiverStats := e.receiver.Stats()
		snapshot.Receiver = &receiverStats
	}
	if e.chain != nil {
		chainSnapshot := e.chain.Snapshot()
		snapshot.AlpenglowChain = &chainSnapshot
	}
	if e.pool != nil {
		poolSnapshot := e.pool.Snapshot()
		snapshot.AlpenglowPool = &poolSnapshot
	}
	return snapshot
}

func (e *AlpenglowObserverEngine) SetAlpenglowBlockIDSink(sink AlpenglowBlockIDSink) {
	e.blockIDSinkMu.Lock()
	e.blockIDSink = sink
	recent := make([]struct {
		slot    uint64
		blockID solana.Hash
	}, 0, len(e.recentBlockIDs))
	if sink != nil {
		for slot, blockID := range e.recentBlockIDs {
			recent = append(recent, struct {
				slot    uint64
				blockID solana.Hash
			}{slot: slot, blockID: blockID})
		}
	}
	e.blockIDSinkMu.Unlock()

	for _, entry := range recent {
		sink(entry.slot, entry.blockID)
	}
}

func (e *AlpenglowObserverEngine) SetAlpenglowRepairPrioritySink(sink AlpenglowRepairPrioritySink) {
	e.repairPrioritySinkMu.Lock()
	e.repairPrioritySink = sink
	e.repairPrioritySinkMu.Unlock()
}

func (e *AlpenglowObserverEngine) observeVotorMessage(msg alpenglow.Message) {
	if msg.Vote != nil {
		if e.voteVerifyQueue != nil {
			select {
			case e.voteVerifyQueue <- *msg.Vote:
			default:
				e.voteVerifyDropped.Add(1)
			}
			return
		}
		e.processVotorVote(*msg.Vote)
		return
	}
	if msg.Certificate != nil {
		e.processVotorCertificate(*msg.Certificate)
	}
}

func (e *AlpenglowObserverEngine) processVotorVote(msg alpenglow.VoteMessage) {
	result, err := e.verifyVoteMessage(msg)
	e.recordVoteVerification(msg, result, err)
	if err != nil {
		return
	}
	if _, err := e.ensureObserver().ObserveVote(msg); err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW observer: ignored verified vote: %v", err)
		return
	}
	update, err := e.ensurePool().AddVerifiedVote(alpenglow.VerifiedVote{Message: msg, Result: result})
	if err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW observer: verified vote rejected by consensus pool: %v", err)
		return
	}
	e.handleConsensusUpdate(update)
	e.deliverVerifiedVotorMessage(alpenglow.Message{Vote: &msg})
}

func (e *AlpenglowObserverEngine) processVotorCertificate(cert alpenglow.Certificate) {
	verified, result, err := e.verifyCertificate(cert)
	if err != nil {
		e.logCertificateVerifyDrop(cert, result, err)
		return
	}
	e.acceptVerifiedCertificate(verified)
}

func (e *AlpenglowObserverEngine) acceptVerifiedCertificate(verified alpenglow.Certificate) {
	if _, err := e.ensureObserver().ObserveCertificate(verified); err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW observer: ignored verified certificate: %v", err)
		return
	}
	if _, err := e.ensureChain().ObserveCertificate(verified); err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW observer: ignored invalid certificate: %v", err)
		return
	}
	update, err := e.ensurePool().AddVerifiedCertificate(verified)
	if err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW observer: verified certificate rejected by consensus pool: %v", err)
		return
	}
	e.observeVotorBlockID(alpenglow.NewCertificateMessage(verified))
	e.handleConsensusUpdate(update)
	e.deliverVerifiedVotorMessage(alpenglow.NewCertificateMessage(verified))
}

func (e *AlpenglowObserverEngine) handleConsensusUpdate(update alpenglow.ConsensusUpdate) {
	for _, cert := range update.Certificates {
		if _, err := e.ensureObserver().ObserveCertificate(cert); err != nil {
			mlog.Log.FileOnlyf("ALPENGLOW observer: local certificate observer rejected %s@%d: %v", cert.Type, cert.Slot, err)
			continue
		}
		if _, err := e.ensureChain().ObserveCertificate(cert); err != nil {
			mlog.Log.FileOnlyf("ALPENGLOW observer: local certificate chain rejected %s@%d: %v", cert.Type, cert.Slot, err)
			continue
		}
		e.observeVotorBlockID(alpenglow.NewCertificateMessage(cert))
	}
	for _, event := range update.Events {
		if event.Kind == alpenglow.ConsensusEventParentReady && event.Block.Slot != 0 {
			e.prioritizeRepairSlot(event.Block.Slot)
		}
		e.eventSinkMu.RLock()
		sink := e.eventSink
		e.eventSinkMu.RUnlock()
		if sink != nil {
			sink(event)
		}
	}
}

func (e *AlpenglowObserverEngine) deliverVerifiedVotorMessage(msg alpenglow.Message) {
	e.votorMessageHookMu.RLock()
	hook := e.votorMessageHook
	e.votorMessageHookMu.RUnlock()
	if hook != nil {
		hook(msg)
	}
}

func (e *AlpenglowObserverEngine) observeVotorBlockID(msg alpenglow.Message) {
	if msg.Certificate == nil {
		return
	}
	blockID, ok := msg.Certificate.Block()
	if !ok || !blockID.HasHash() {
		return
	}

	e.blockIDSinkMu.Lock()
	if e.recentBlockIDs == nil {
		e.recentBlockIDs = make(map[uint64]solana.Hash)
	}
	if existing, exists := e.recentBlockIDs[blockID.Slot]; !exists {
		e.recentBlockIDOrder = append(e.recentBlockIDOrder, blockID.Slot)
	} else if existing == blockID.Hash {
		sink := e.blockIDSink
		e.blockIDSinkMu.Unlock()
		if sink != nil {
			sink(blockID.Slot, blockID.Hash)
		}
		e.repairPrioritySinkMu.Lock()
		repairSink := e.repairPrioritySink
		e.repairPrioritySinkMu.Unlock()
		if repairSink != nil {
			repairSink(blockID.Slot)
		}
		return
	}
	e.recentBlockIDs[blockID.Slot] = blockID.Hash
	for len(e.recentBlockIDOrder) > maxRecentAlpenglowBlockIDs {
		old := e.recentBlockIDOrder[0]
		e.recentBlockIDOrder = e.recentBlockIDOrder[1:]
		delete(e.recentBlockIDs, old)
	}
	sink := e.blockIDSink
	e.blockIDSinkMu.Unlock()
	if sink != nil {
		sink(blockID.Slot, blockID.Hash)
	}

	e.repairPrioritySinkMu.Lock()
	repairSink := e.repairPrioritySink
	e.repairPrioritySinkMu.Unlock()
	if repairSink != nil {
		repairSink(blockID.Slot)
	}
}

func (e *AlpenglowObserverEngine) NextAlpenglowDecision(anchorSlot uint64) (alpenglow.ChainDecision, bool) {
	return e.ensureChain().NextDecision(anchorSlot)
}

func (e *AlpenglowObserverEngine) SetAlpenglowRoot(block alpenglow.BlockID) {
	e.ensurePool().SetRoot(block)
}

func (e *AlpenglowObserverEngine) AlpenglowBlockProductionParent(slot uint64) alpenglow.BlockProductionParent {
	return e.ensurePool().BlockProductionParent(slot)
}

func (e *AlpenglowObserverEngine) prioritizeRepairSlot(slot uint64) {
	if slot == 0 {
		return
	}
	e.repairPrioritySinkMu.Lock()
	sink := e.repairPrioritySink
	e.repairPrioritySinkMu.Unlock()
	if sink != nil {
		sink(slot)
	}
}

func (e *AlpenglowObserverEngine) ObserveAlpenglowCandidateBlock(obs alpenglow.ReplayBlockObservation) {
	if !obs.Block.HasHash() {
		return
	}
	e.enrichReplayBlockObservation(&obs)
	if obs.ParentSlot != 0 && obs.ParentHash != (solana.Hash{}) {
		e.rememberRecentAlpenglowBlockID(obs.ParentSlot, obs.ParentHash)
		e.prioritizeRepairSlot(obs.ParentSlot)
	}
	e.observeChainReplayBlock(obs)
	e.rememberRecentAlpenglowBlockID(obs.Block.Slot, obs.Block.Hash)
}

func (e *AlpenglowObserverEngine) observeChainReplayBlock(obs alpenglow.ReplayBlockObservation) {
	e.ensureChain().ObserveReplayBlock(obs)
}

func (e *AlpenglowObserverEngine) enrichReplayBlockObservation(obs *alpenglow.ReplayBlockObservation) {
	if obs == nil || !obs.Block.HasHash() {
		return
	}
	if obs.ParentSlot == 0 || obs.ParentHash != (solana.Hash{}) {
		return
	}
	if parentID, ok := e.lookupParentAlpenglowBlockID(obs.ParentSlot); ok {
		obs.ParentHash = parentID
	}
}

func (e *AlpenglowObserverEngine) lookupParentAlpenglowBlockID(parentSlot uint64) (solana.Hash, bool) {
	if parentSlot == 0 {
		return solana.Hash{}, false
	}

	e.blockIDSinkMu.Lock()
	if id, ok := e.recentBlockIDs[parentSlot]; ok && id != (solana.Hash{}) {
		e.blockIDSinkMu.Unlock()
		return id, true
	}
	e.blockIDSinkMu.Unlock()

	if id, ok := global.AlpenglowBlockID(parentSlot); ok && id != (solana.Hash{}) {
		return id, true
	}

	if block, ok := e.ensureChain().KnownBlockAtSlot(parentSlot); ok && block.HasHash() {
		return block.Hash, true
	}
	return solana.Hash{}, false
}

func (e *AlpenglowObserverEngine) rememberRecentAlpenglowBlockID(slot uint64, blockID solana.Hash) {
	if slot == 0 || blockID == (solana.Hash{}) {
		return
	}
	e.blockIDSinkMu.Lock()
	if e.recentBlockIDs == nil {
		e.recentBlockIDs = make(map[uint64]solana.Hash)
	}
	if existing, exists := e.recentBlockIDs[slot]; exists && existing == blockID {
		e.blockIDSinkMu.Unlock()
		return
	}
	if _, exists := e.recentBlockIDs[slot]; !exists {
		e.recentBlockIDOrder = append(e.recentBlockIDOrder, slot)
	}
	e.recentBlockIDs[slot] = blockID
	for len(e.recentBlockIDOrder) > maxRecentAlpenglowBlockIDs {
		old := e.recentBlockIDOrder[0]
		e.recentBlockIDOrder = e.recentBlockIDOrder[1:]
		delete(e.recentBlockIDs, old)
	}
	sink := e.blockIDSink
	e.blockIDSinkMu.Unlock()
	if sink != nil {
		sink(slot, blockID)
	}
	e.ensureChain().RefreshParentLinkagesFromSlot(slot, blockID)
}

func (e *AlpenglowObserverEngine) SetAlpenglowValidatorSet(set alpenglow.ValidatorSet) error {
	if err := e.ensureVerifier().SetValidatorSet(set); err != nil {
		return err
	}
	e.retryFooterFinalCertificates(set.Epoch)
	mlog.Log.Infof("ALPENGLOW observer: installed validator set for epoch %d (validators=%d total_stake=%d)",
		set.Epoch, len(set.Validators), set.TotalStake)
	return nil
}

func (e *AlpenglowObserverEngine) observeFooterFinalCertificate(block *block.Block) {
	if block == nil || len(block.BlockFinalCert) == 0 {
		return
	}
	decoded, err := rewardcerts.DecodeFinalCertificate(block.BlockFinalCert)
	if err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW observer: slot %d footer final certificate decode failed: %v", block.Slot, err)
		return
	}
	epoch, ok := e.alpenglowEpochForSlot(decoded.Slot)
	if !ok {
		e.deferFooterFinalCertificate(decoded.Slot, block.BlockFinalCert)
		return
	}
	set, ok := e.ensureVerifier().ValidatorSetForEpoch(epoch)
	if !ok {
		e.deferFooterFinalCertificate(decoded.Slot, block.BlockFinalCert)
		return
	}
	validated, err := rewardcerts.ValidateBlockFinalCertificate(block.BlockFinalCert, set, e.ensureVerifier().ShredVersion())
	if err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW observer: slot %d footer final certificate rejected: %v", block.Slot, err)
		return
	}
	for _, cert := range validated.Certificates {
		e.acceptVerifiedCertificate(cert)
	}
}

func (e *AlpenglowObserverEngine) deferFooterFinalCertificate(slot uint64, raw []byte) {
	e.footerFinalMu.Lock()
	if e.pendingFooterFinals == nil {
		e.pendingFooterFinals = make(map[uint64][]byte)
	}
	if len(e.pendingFooterFinals) < 4096 {
		e.pendingFooterFinals[slot] = append([]byte(nil), raw...)
	}
	e.footerFinalMu.Unlock()
}

func (e *AlpenglowObserverEngine) retryFooterFinalCertificates(epoch uint64) {
	e.footerFinalMu.Lock()
	var pending [][]byte
	for slot, raw := range e.pendingFooterFinals {
		if resolved, ok := e.alpenglowEpochForSlot(slot); ok && resolved == epoch {
			pending = append(pending, raw)
			delete(e.pendingFooterFinals, slot)
		}
	}
	e.footerFinalMu.Unlock()
	set, ok := e.ensureVerifier().ValidatorSetForEpoch(epoch)
	if !ok {
		return
	}
	for _, raw := range pending {
		validated, err := rewardcerts.ValidateBlockFinalCertificate(raw, set, e.ensureVerifier().ShredVersion())
		if err != nil {
			mlog.Log.FileOnlyf("ALPENGLOW observer: deferred footer final certificate rejected: %v", err)
			continue
		}
		for _, cert := range validated.Certificates {
			e.acceptVerifiedCertificate(cert)
		}
	}
}

func (e *AlpenglowObserverEngine) SetAlpenglowEpochLookup(fn func(slot uint64) uint64) {
	e.epochLookupMu.Lock()
	e.epochForSlot = fn
	e.epochLookupMu.Unlock()
}

func (e *AlpenglowObserverEngine) Close() error {
	var receiverErr error
	if e.receiver != nil {
		receiverErr = e.receiver.Close()
	}
	if e.voteVerifyCancel != nil {
		e.voteVerifyCancel()
	}
	e.voteVerifyWG.Wait()
	return receiverErr
}

func (e *AlpenglowObserverEngine) ensureObserver() *alpenglow.Observer {
	if e.observer == nil {
		e.observer = alpenglow.NewObserver()
	}
	return e.observer
}

func (e *AlpenglowObserverEngine) ensureChain() *alpenglow.ChainTracker {
	if e.chain == nil {
		e.chain = newAlpenglowObserverChainTracker()
	}
	return e.chain
}

func (e *AlpenglowObserverEngine) ensureVerifier() *alpenglow.CertificateVerifier {
	if e.verifier == nil {
		e.verifier = alpenglow.NewCertificateVerifier()
	}
	return e.verifier
}

func (e *AlpenglowObserverEngine) ensurePool() *alpenglow.ConsensusPool {
	if e.pool == nil {
		e.pool = alpenglow.NewConsensusPool(alpenglow.DefaultConsensusPoolConfig())
	}
	return e.pool
}

func (e *AlpenglowObserverEngine) startVoteVerifier(parent context.Context) {
	if e.voteVerifyQueue != nil {
		return
	}
	workers := e.voteVerifyWorkers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0) / 4
		if workers < 2 {
			workers = 2
		}
		if workers > 8 {
			workers = 8
		}
	}
	queueSize := e.voteVerifyQueueSize
	if queueSize <= 0 {
		queueSize = 8192
	}
	ctx, cancel := context.WithCancel(parent)
	e.voteVerifyCancel = cancel
	e.voteVerifyQueue = make(chan alpenglow.VoteMessage, queueSize)
	for range workers {
		e.voteVerifyWG.Add(1)
		go func() {
			defer e.voteVerifyWG.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case vote := <-e.voteVerifyQueue:
					e.processVotorVote(vote)
				}
			}
		}()
	}
	mlog.Log.FileOnlyf("ALPENGLOW observer: vote BLS verifier workers=%d queue=%d", workers, queueSize)
}

func (e *AlpenglowObserverEngine) verifyCertificate(cert alpenglow.Certificate) (alpenglow.Certificate, alpenglow.CertificateVerifyResult, error) {
	if epoch, ok := e.alpenglowEpochForSlot(cert.Slot); ok {
		return e.ensureVerifier().VerifyCertificateForEpoch(epoch, cert)
	}
	return e.ensureVerifier().VerifyCertificate(cert)
}

func (e *AlpenglowObserverEngine) verifyVoteMessage(msg alpenglow.VoteMessage) (alpenglow.VoteVerifyResult, error) {
	if epoch, ok := e.alpenglowEpochForSlot(msg.Vote.Slot); ok {
		return e.ensureVerifier().VerifyVoteMessageForEpoch(epoch, msg)
	}
	return e.ensureVerifier().VerifyVoteMessage(msg)
}

func (e *AlpenglowObserverEngine) alpenglowEpochForSlot(slot uint64) (uint64, bool) {
	e.epochLookupMu.RLock()
	fn := e.epochForSlot
	e.epochLookupMu.RUnlock()
	if fn == nil {
		return 0, false
	}
	return fn(slot), true
}

func (e *AlpenglowObserverEngine) logCertificateVerifyDrop(cert alpenglow.Certificate, result alpenglow.CertificateVerifyResult, err error) {
	e.certVerifyLogMu.Lock()
	e.certVerifyDropCount++
	now := time.Now()
	shouldLog := e.lastCertVerifyLog.IsZero() || now.Sub(e.lastCertVerifyLog) >= 10*time.Second
	if shouldLog {
		e.lastCertVerifyLog = now
		e.certVerifyDetailCount++
	}
	drops := e.certVerifyDropCount
	details := e.certVerifyDetailCount
	e.certVerifyLogMu.Unlock()

	if !shouldLog {
		return
	}

	var diag alpenglow.CertificateDiagnostics
	if epoch, ok := e.alpenglowEpochForSlot(cert.Slot); ok {
		diag = e.ensureVerifier().DiagnoseCertificateForEpoch(epoch, cert, 8)
	} else {
		diag = e.ensureVerifier().DiagnoseCertificate(cert, 8)
	}
	mlog.Log.FileOnlyf("ALPENGLOW observer: ignored certificate before certificate verification (drops=%d latest=%v)", drops, err)
	mlog.Log.FileOnlyf("ALPENGLOW observer: certificate verify debug #%d: cert=%s slot=%d block=%s epoch=%d validators=%d bitmap=%s bits=%d bytes=%d signers=%d base=%d fallback=%d stake=%d/%d result={epoch:%d signers:%d stake:%d/%d stake_ok:%t sig_ok:%t} payload_lens=%d/%d base_ranks=%v fallback_ranks=%v signer_samples=%s bitmap_error=%q",
		details,
		cert.Type,
		cert.Slot,
		cert.BlockHash,
		diag.Epoch,
		diag.ValidatorCount,
		diag.BitmapEncoding,
		diag.BitmapLength,
		diag.BitmapBytes,
		diag.SignerCount,
		diag.BaseSignerCount,
		diag.FallbackSignerCount,
		diag.IncludedStake,
		diag.TotalStake,
		result.Epoch,
		result.SignerCount,
		result.IncludedStake,
		result.TotalStake,
		result.StakeVerified,
		result.SignatureVerified,
		diag.PrimaryPayloadLen,
		diag.FallbackPayloadLen,
		diag.BaseRanks,
		diag.FallbackRanks,
		formatSignerSamples(diag.SignerSamples),
		diag.BitmapError,
	)
}

func (e *AlpenglowObserverEngine) recordVoteVerification(msg alpenglow.VoteMessage, result alpenglow.VoteVerifyResult, err error) {
	now := time.Now()
	e.voteVerifyLogMu.Lock()
	e.voteVerifyChecked++
	switch {
	case err == nil:
		e.voteVerifyOK++
	case strings.Contains(err.Error(), "no validator set"):
		e.voteVerifyNoSet++
		e.lastVoteVerifyErr = err.Error()
	default:
		e.voteVerifyFailed++
		e.lastVoteVerifyErr = err.Error()
	}
	shouldLog := e.lastVoteVerifyLog.IsZero() || now.Sub(e.lastVoteVerifyLog) >= 10*time.Second
	if shouldLog {
		e.lastVoteVerifyLog = now
	}
	checked := e.voteVerifyChecked
	ok := e.voteVerifyOK
	failed := e.voteVerifyFailed
	noSet := e.voteVerifyNoSet
	dropped := e.voteVerifyDropped.Load()
	e.voteVerifyLogMu.Unlock()
	if !shouldLog {
		return
	}
	if err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW observer: vote BLS verification rejected: checked=%d ok=%d failed=%d no_set=%d queue_dropped=%d vote=%s slot=%d rank=%d err=%v",
			checked, ok, failed, noSet, dropped, msg.Vote.Type, msg.Vote.Slot, msg.Rank, err)
		return
	}
	mlog.Log.FileOnlyf("ALPENGLOW observer: vote BLS verification: checked=%d ok=%d failed=%d no_set=%d queue_dropped=%d latest_vote=%s slot=%d rank=%d epoch=%d stake=%d/%d",
		checked, ok, failed, noSet, dropped, msg.Vote.Type, msg.Vote.Slot, msg.Rank, result.Epoch, result.Stake, result.TotalStake)
}

func (e *AlpenglowObserverEngine) sampleVoteVerification(msg alpenglow.VoteMessage) {
	now := time.Now()
	e.voteVerifyLogMu.Lock()
	if e.voteVerifyWindowStart.IsZero() || now.Sub(e.voteVerifyWindowStart) >= 10*time.Second {
		e.voteVerifyWindowStart = now
		e.voteVerifySamples = 0
	}
	if e.voteVerifySamples >= alpenglowVoteVerifySamplesPerWindow {
		e.voteVerifyLogMu.Unlock()
		return
	}
	e.voteVerifySamples++
	e.voteVerifyLogMu.Unlock()

	result, err := e.verifyVoteMessage(msg)

	e.voteVerifyLogMu.Lock()
	e.voteVerifyChecked++
	switch {
	case err == nil:
		e.voteVerifyOK++
	case strings.Contains(err.Error(), "no validator set"):
		e.voteVerifyNoSet++
		e.lastVoteVerifyErr = err.Error()
	default:
		e.voteVerifyFailed++
		e.lastVoteVerifyErr = err.Error()
	}
	shouldLog := e.lastVoteVerifyLog.IsZero() || now.Sub(e.lastVoteVerifyLog) >= 10*time.Second || err != nil
	if shouldLog {
		e.lastVoteVerifyLog = now
	}
	checked := e.voteVerifyChecked
	ok := e.voteVerifyOK
	failed := e.voteVerifyFailed
	noSet := e.voteVerifyNoSet
	lastErr := e.lastVoteVerifyErr
	e.voteVerifyLogMu.Unlock()

	if !shouldLog {
		return
	}
	if err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW observer: sampled vote BLS verification failed: checked=%d ok=%d failed=%d no_set=%d vote=%s slot=%d rank=%d err=%v",
			checked, ok, failed, noSet, msg.Vote.Type, msg.Vote.Slot, msg.Rank, err)
		if !strings.Contains(err.Error(), "no validator set") {
			var diag alpenglow.VoteSignatureDiagnostics
			if epoch, ok := e.alpenglowEpochForSlot(msg.Vote.Slot); ok {
				diag = e.ensureVerifier().DiagnoseVoteMessageForEpoch(epoch, msg, 4)
			} else {
				diag = e.ensureVerifier().DiagnoseVoteMessage(msg, 4)
			}
			mlog.Log.FileOnlyf("ALPENGLOW observer: sampled vote BLS debug: vote=%s slot=%d advertised_rank=%d epoch=%d validators=%d payload_len=%d payload_hex=%s sig_len=%d sig_hex=%s advertised=%s advertised_rank_err=%q matches=%d match_samples=%s epoch_matches=%s diag_error=%q",
				msg.Vote.Type,
				msg.Vote.Slot,
				msg.Rank,
				diag.Epoch,
				diag.ValidatorCount,
				diag.PayloadLen,
				diag.PayloadHex,
				diag.SignatureLen,
				diag.SignatureHex,
				formatSignerSample(diag.AdvertisedSigner),
				diag.AdvertisedRankErr,
				diag.MatchCount,
				formatSignerSamples(diag.MatchSamples),
				formatEpochVoteDiagnostics(diag.Epochs),
				diag.DiagnosticError,
			)
		}
		return
	}
	mlog.Log.FileOnlyf("ALPENGLOW observer: sampled vote BLS verification ok: checked=%d ok=%d failed=%d no_set=%d latest_vote=%s slot=%d rank=%d epoch=%d stake=%d/%d last_err=%q",
		checked, ok, failed, noSet, msg.Vote.Type, msg.Vote.Slot, msg.Rank, result.Epoch, result.Stake, result.TotalStake, lastErr)
}

func formatEpochVoteDiagnostics(diags []alpenglow.EpochVoteSignatureDiagnostics) string {
	if len(diags) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(diags))
	for _, diag := range diags {
		parts = append(parts, fmt.Sprintf("{epoch:%d validators:%d advertised:%s advertised_rank_err:%q matches:%d samples:%s}",
			diag.Epoch,
			diag.ValidatorCount,
			formatSignerSample(diag.AdvertisedSigner),
			diag.AdvertisedRankErr,
			diag.MatchCount,
			formatSignerSamples(diag.MatchSamples),
		))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func formatSignerSamples(samples []alpenglow.SignerSample) string {
	if len(samples) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(samples))
	for _, sample := range samples {
		bls := sample.BLSPubkeyHex
		if bls == "" {
			bls = sample.BLSPubkeyPrefix
		}
		parts = append(parts, fmt.Sprintf("{rank:%d stake:%d vote:%s node:%s bls:%s}",
			sample.Rank,
			sample.Stake,
			sample.VoteAccount,
			sample.NodePubkey,
			bls,
		))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func formatSignerSample(sample *alpenglow.SignerSample) string {
	if sample == nil {
		return "none"
	}
	return formatSignerSamples([]alpenglow.SignerSample{*sample})
}

func newAlpenglowObserverChainTracker() *alpenglow.ChainTracker {
	return alpenglow.NewChainTrackerWithConfig(alpenglow.ChainConfig{
		RequireVerifiedCertificates:      true,
		RequireStakeVerifiedCertificates: true,
	})
}

func alpenglowParentSlot(block *block.Block) uint64 {
	if block == nil {
		return 0
	}
	if block.SourceParentSlot != 0 {
		return block.SourceParentSlot
	}
	return block.ParentSlot
}

// alpenglowParentBlockID returns the parent's Alpenglow block id when known,
// or a zero hash. The chain tracker uses this to link observed blocks into
// finalized ancestor chains, so it must never be an unrelated hash.
func alpenglowParentBlockID(block *block.Block) solana.Hash {
	if block == nil || !block.HasAlpenglowParentBlockID {
		return solana.Hash{}
	}
	return solana.Hash(block.AlpenglowParentBlockID)
}

type AlpenglowEngine struct{}

func (e *AlpenglowEngine) Name() string { return string(ModeAlpenglow) }

func (e *AlpenglowEngine) Start(context.Context) error {
	return ErrAlpenglowVotingNotImplemented
}

func (e *AlpenglowEngine) ObserveBlock(context.Context, BlockObservation) error { return nil }

func (e *AlpenglowEngine) OnReplayResult(context.Context, SlotReplayResult) error { return nil }

func (e *AlpenglowEngine) Snapshot() Snapshot {
	return Snapshot{Mode: ModeAlpenglow}
}

func (e *AlpenglowEngine) Close() error { return nil }
