package consensus

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

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

type AlpenglowDecisionSource interface {
	NextAlpenglowDecision(anchorSlot uint64) (alpenglow.ChainDecision, bool)
}

// AlpenglowFinalityIndex answers per-slot finality queries for the promotion gate.
type AlpenglowFinalityIndex interface {
	FinalizedBlockAt(slot uint64) (alpenglow.BlockID, bool)
	FinalityConflictAt(slot uint64) bool
}

type AlpenglowCandidateBlockObserver interface {
	ObserveAlpenglowCandidateBlock(obs alpenglow.ReplayBlockObservation)
}

// AlpenglowFooterCertificateSink ingests finalization certs decoded from block
// footers (the unstaked finality path). Implemented only by the observer engine.
type AlpenglowFooterCertificateSink interface {
	// ObserveFooterCertificates returns the blocks finalized by the verified certs,
	// so the caller can capture expected finality at ingest time (the tracker's own
	// state may be pruned by the time promotion consults it).
	ObserveFooterCertificates(certs []alpenglow.Certificate) []alpenglow.BlockID
}

type AlpenglowValidatorSetSink interface {
	SetAlpenglowValidatorSet(set alpenglow.ValidatorSet) error
}

type AlpenglowEpochLookupSink interface {
	SetAlpenglowEpochLookup(fn func(slot uint64) uint64)
}

// AlpenglowPruneSink lets replay prune consensus bookkeeping as slots fold.
type AlpenglowPruneSink interface {
	PruneAlpenglowBefore(slot uint64)
}

// AlpenglowChainQuery answers the switch sweep's decisive-certificate
// questions (unique-strength or finalized block per slot; certified skips).
type AlpenglowChainQuery interface {
	CertifiedBlockAt(slot uint64) (alpenglow.BlockID, alpenglow.CertificateType, bool)
	SkipCertifiedAt(slot uint64) bool
	// ChainDecisionVersion gates the sweep: it advances on ANY decision-relevant
	// change (cert acceptance, replay-derived parent links, finalized ancestry,
	// indirect skips, conflicts), not only on new certificates — so a
	// contradiction that arises without a new cert is never missed.
	ChainDecisionVersion() uint64
}

// AlpenglowWantedBlocksSource surfaces certified-but-unobserved blocks so the
// block source can steer turbine/repair toward data the cluster has already
// voted real (cert-driven repair).
type AlpenglowWantedBlocksSource interface {
	AlpenglowWantedBlocks(afterSlot uint64, max int) []alpenglow.WantedBlock
	SkipCertifiedAt(slot uint64) bool
}

type Snapshot struct {
	Mode           string                   `json:"mode"`
	ObservedBlocks uint64                   `json:"observed_blocks"`
	ReplayedSlots  uint64                   `json:"replayed_slots"`
	Alpenglow      *alpenglow.Snapshot      `json:"alpenglow,omitempty"`
	AlpenglowChain *alpenglow.ChainSnapshot `json:"alpenglow_chain,omitempty"`
	Receiver       *alpenglow.ReceiverStats `json:"receiver,omitempty"`
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
	AlpenglowObserverBindAddr string
	AlpenglowMaxMessageBytes  int64
	AlpenglowBLSDST           string // BLS hash-to-curve DST; empty keeps the default (must match cluster's solana-bls version)
}

// NewEngine constructs the Alpenglow observer engine — the only consensus
// engine in this Alpenglow-only build.
func NewEngine(cfg Config) (*AlpenglowObserverEngine, error) {
	alpenglow.SetHashToPointDST(strings.TrimSpace(cfg.AlpenglowBLSDST))
	e := &AlpenglowObserverEngine{
		observer:                alpenglow.NewObserver(),
		chain:                   newAlpenglowObserverChainTracker(),
		verifier:                alpenglow.NewCertificateVerifier(),
		receiverBindAddr:        strings.TrimSpace(cfg.AlpenglowObserverBindAddr),
		receiverMaxMessageBytes: cfg.AlpenglowMaxMessageBytes,
		recentBlockIDs:          make(map[uint64]solana.Hash),
	}
	// The cert pool assembles certificates locally from raw Votor votes;
	// pool-assembled certs are fully stake+signature verified and enter the
	// chain tracker exactly like verified wire certs.
	e.certPool = alpenglow.NewCertPool(alpenglow.DefaultCertPoolConfig(), e.verifier, func(cert alpenglow.Certificate) {
		if _, err := e.ensureChain().ObserveCertificate(cert); err != nil {
			mlog.Log.FileOnlyf("ALPENGLOW observer: pool-assembled certificate rejected by tracker: %v", err)
			return
		}
		e.observeVotorBlockID(alpenglow.Message{Certificate: &cert})
	})
	return e, nil
}

type AlpenglowObserverEngine struct {
	observedBlocks          atomic.Uint64
	replayedSlots           atomic.Uint64
	observer                *alpenglow.Observer
	certPool                *alpenglow.CertPool
	chain                   *alpenglow.ChainTracker
	verifier                *alpenglow.CertificateVerifier
	receiverBindAddr        string
	receiverMaxMessageBytes int64
	receiver                *alpenglow.Receiver
	blockIDSinkMu           sync.RWMutex
	blockIDSink             AlpenglowBlockIDSink
	recentBlockIDs          map[uint64]solana.Hash
	recentBlockIDOrder      []uint64
	epochLookupMu           sync.RWMutex
	epochForSlot            func(slot uint64) uint64
	pendingCertsMu          sync.Mutex
	pendingCerts            map[uint64][]alpenglow.Certificate // deferred until their epoch's stakes install
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
}

func (e *AlpenglowObserverEngine) Name() string { return "alpenglow-observer" }

func (e *AlpenglowObserverEngine) Start(ctx context.Context) error {
	observer := e.ensureObserver()
	e.ensureChain()
	e.ensureVerifier()
	mlog.Log.Infof("Consensus engine started: %s (passive; no votes will be signed)", e.Name())
	mlog.Log.FileOnlyf("ALPENGLOW observer: certified path resolver requires stake and aggregate BLS signature verified certificates")
	if e.receiverBindAddr == "" {
		mlog.Log.FileOnlyf("ALPENGLOW observer: Votor receiver disabled; set consensus.alpenglow_observer_bind_addr to listen for Votor QUIC messages")
		return nil
	}

	receiver, err := alpenglow.NewReceiver(alpenglow.ReceiverConfig{
		BindAddr:        e.receiverBindAddr,
		MaxMessageBytes: e.receiverMaxMessageBytes,
		OnMessage:       e.observeVotorMessage,
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
			// ParentHash must stay in the alpenglow block-id domain (the tracker keys
			// blocks by cert block-id); PoH hashes never match, so zero = unknown.
			ParentHash: parentBlockIDOrZero(obs.Block),
			Source:     obs.Source,
			At:         obs.At,
		}
		e.ensureObserver().ObserveReplayBlock(replayObs)
		if blockID.HasHash() {
			e.ensureChain().ObserveReplayBlock(replayObs)
		}
	}
	return nil
}

func (e *AlpenglowObserverEngine) OnReplayResult(_ context.Context, result SlotReplayResult) error {
	if result.Slot != 0 {
		if e.replayedSlots.Add(1) == 1 {
			mlog.Log.Infof("ALPENGLOW observer: first replay result at slot %d", result.Slot)
		}
		e.ensureObserver().ObserveReplayResult(alpenglow.ReplayResultObservation{
			Slot:     result.Slot,
			Bankhash: solana.Hash(result.Bankhash),
			Source:   result.Source,
			At:       result.At,
		})
		// Anchor the cert pool's vote window to execution-proven progress — a
		// trusted signal an attacker cannot advance (unlike raw vote slots).
		if e.certPool != nil {
			e.certPool.NoteLiveSlot(result.Slot)
		}
	}
	return nil
}

func (e *AlpenglowObserverEngine) Snapshot() Snapshot {
	snapshot := Snapshot{
		Mode:           "alpenglow-observer",
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

func (e *AlpenglowObserverEngine) observeVotorMessage(msg alpenglow.Message) {
	if msg.Vote != nil {
		// The cert pool owns vote verification now (lazy, batched); the old
		// per-window sampling verifier is redundant with it.
		e.certPool.AddVote(*msg.Vote)
	}
	if msg.Certificate != nil {
		verified, result, err := e.verifyCertificate(*msg.Certificate)
		if err != nil {
			e.logCertificateVerifyDrop(*msg.Certificate, result, err)
			e.deferCertIfStakesMissing(*msg.Certificate, err)
			return
		} else if _, err := e.ensureChain().ObserveCertificate(verified); err != nil {
			mlog.Log.FileOnlyf("ALPENGLOW observer: ignored invalid certificate: %v", err)
			return
		}
		msg.Certificate = &verified
	}
	e.observeVotorBlockID(msg)
}

// ObserveFooterCertificates verifies and ingests certificates decoded from a block
// footer (the unstaked finality path — no Votor QUIC needed). Each is verified and
// fed to the chain tracker exactly like a QUIC cert. Returns the blocks these certs
// finalize (fast: the FinalizeFast block; slow: the notarized block paired with the
// Finalize cert), derived from the verified certs themselves so the result cannot
// be raced away by tracker pruning.
func (e *AlpenglowObserverEngine) ObserveFooterCertificates(certs []alpenglow.Certificate) []alpenglow.BlockID {
	var finalized []alpenglow.BlockID
	notarized := make(map[uint64]alpenglow.BlockID)
	finalizeSlots := make(map[uint64]struct{})
	for _, cert := range certs {
		verified, result, err := e.verifyCertificate(cert)
		if err != nil {
			e.logCertificateVerifyDrop(cert, result, err)
			e.deferCertIfStakesMissing(cert, err)
			continue
		}
		if _, err := e.ensureChain().ObserveCertificate(verified); err != nil {
			mlog.Log.FileOnlyf("ALPENGLOW observer: ignored invalid footer certificate: %v", err)
			continue
		}
		switch verified.Type {
		case alpenglow.CertificateFinalizeFast:
			if block, ok := verified.Block(); ok {
				finalized = append(finalized, block)
			}
		case alpenglow.CertificateNotarize:
			if block, ok := verified.Block(); ok {
				notarized[verified.Slot] = block
			}
		case alpenglow.CertificateFinalize:
			finalizeSlots[verified.Slot] = struct{}{}
		}
	}
	for slot := range finalizeSlots {
		if block, ok := notarized[slot]; ok {
			finalized = append(finalized, block)
		}
	}
	return finalized
}

// alpenglowPendingCertCap bounds deferred certs per epoch; alpenglowPendingEpochCap
// bounds the number of distinct epoch buckets. Certs carry a network-controlled slot
// and are buffered before authentication, so both caps are needed to keep a peer
// feeding far-future/garbage slots from growing the buffer without bound.
const (
	alpenglowPendingCertCap  = 512
	alpenglowPendingEpochCap = 4
)

// deferCertIfStakesMissing buffers a certificate that failed verification only
// because its epoch's validator set isn't installed yet, so it can be replayed once
// the stakes arrive (otherwise a QUIC cert that races ahead of its epoch is lost and
// that slot's decision stalls). Certs that fail for any other reason are not buffered.
func (e *AlpenglowObserverEngine) deferCertIfStakesMissing(cert alpenglow.Certificate, err error) {
	if err == nil || !strings.Contains(err.Error(), "no validator set") {
		return
	}
	epoch, ok := e.alpenglowEpochForSlot(cert.Slot)
	if !ok {
		return
	}
	// Only defer certs for epochs plausibly near the installed sets — a cert with a
	// far-off epoch can never verify soon and would just occupy buckets.
	if latest := e.ensureVerifier().LatestEpoch(); latest > 0 {
		if epoch+1 < latest || epoch > latest+2 {
			return
		}
	}
	e.pendingCertsMu.Lock()
	defer e.pendingCertsMu.Unlock()
	if e.pendingCerts == nil {
		e.pendingCerts = make(map[uint64][]alpenglow.Certificate)
	}
	// Bound distinct epoch buckets: evict the HIGHEST epoch when a new one would
	// exceed the cap — garbage slots are typically far-future, genuine pending
	// epochs sit near the current one.
	if _, exists := e.pendingCerts[epoch]; !exists && len(e.pendingCerts) >= alpenglowPendingEpochCap {
		highest, first := uint64(0), true
		for ep := range e.pendingCerts {
			if first || ep > highest {
				highest, first = ep, false
			}
		}
		delete(e.pendingCerts, highest)
	}
	q := e.pendingCerts[epoch]
	if len(q) >= alpenglowPendingCertCap {
		q = q[1:] // drop oldest
	}
	e.pendingCerts[epoch] = append(q, cert)
}

// replayPendingCertsForEpoch re-verifies and ingests certs deferred until this
// epoch's validator set installed.
func (e *AlpenglowObserverEngine) replayPendingCertsForEpoch(epoch uint64) {
	e.pendingCertsMu.Lock()
	certs := e.pendingCerts[epoch]
	delete(e.pendingCerts, epoch)
	e.pendingCertsMu.Unlock()
	for _, cert := range certs {
		verified, _, err := e.verifyCertificate(cert)
		if err != nil {
			continue
		}
		_, _ = e.ensureChain().ObserveCertificate(verified)
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
}

func (e *AlpenglowObserverEngine) NextAlpenglowDecision(anchorSlot uint64) (alpenglow.ChainDecision, bool) {
	return e.ensureChain().NextDecision(anchorSlot)
}

func (e *AlpenglowObserverEngine) FinalizedBlockAt(slot uint64) (alpenglow.BlockID, bool) {
	return e.ensureChain().FinalizedBlockAt(slot)
}

func (e *AlpenglowObserverEngine) FinalityConflictAt(slot uint64) bool {
	return e.ensureChain().FinalityConflictAt(slot)
}

func (e *AlpenglowObserverEngine) ObserveAlpenglowCandidateBlock(obs alpenglow.ReplayBlockObservation) {
	if !obs.Block.HasHash() {
		return
	}
	e.ensureChain().ObserveReplayBlock(obs)
}

func (e *AlpenglowObserverEngine) SetAlpenglowValidatorSet(set alpenglow.ValidatorSet) error {
	if err := e.ensureVerifier().SetValidatorSet(set); err != nil {
		return err
	}
	mlog.Log.FileOnlyf("ALPENGLOW observer: installed validator set for epoch %d (validators=%d total_stake=%d)", set.Epoch, len(set.Validators), set.TotalStake)
	e.replayPendingCertsForEpoch(set.Epoch)
	if e.certPool != nil {
		e.certPool.OnValidatorSetInstalled(set.Epoch)
	}
	return nil
}

// CertifiedBlockAt exposes the tracker's decisive block for the switch sweep.
func (e *AlpenglowObserverEngine) CertifiedBlockAt(slot uint64) (alpenglow.BlockID, alpenglow.CertificateType, bool) {
	return e.ensureChain().CertifiedBlockAt(slot)
}

// SkipCertifiedAt exposes certified skips for the switch sweep.
func (e *AlpenglowObserverEngine) SkipCertifiedAt(slot uint64) bool {
	return e.ensureChain().SkipCertifiedAt(slot)
}

// ChainDecisionVersion gates the sweep: it re-walks executed slots whenever the
// tracker became more decisive, including replay-derived changes that land
// without a new certificate.
func (e *AlpenglowObserverEngine) ChainDecisionVersion() uint64 {
	return e.ensureChain().DecisionVersion()
}

// AlpenglowWantedBlocks lists certified-but-unobserved blocks for cert-driven
// repair (ascending, capped).
func (e *AlpenglowObserverEngine) AlpenglowWantedBlocks(afterSlot uint64, max int) []alpenglow.WantedBlock {
	return e.ensureChain().WantedBlocks(afterSlot, max)
}

func (e *AlpenglowObserverEngine) SetAlpenglowEpochLookup(fn func(slot uint64) uint64) {
	e.epochLookupMu.Lock()
	e.epochForSlot = fn
	e.epochLookupMu.Unlock()
	if e.certPool != nil {
		e.certPool.SetEpochLookup(fn)
	}
}

// PruneAlpenglowBefore drops consensus bookkeeping for slots at or below the
// durably-folded watermark: chain-tracker state and cert-pool tallies.
// Equivocation and conflict evidence survive pruning by design.
func (e *AlpenglowObserverEngine) PruneAlpenglowBefore(slot uint64) {
	if slot == 0 {
		return
	}
	e.ensureChain().PruneBeforeSlot(slot)
	if e.certPool != nil {
		e.certPool.ObserveFloor(slot)
	}
}

func (e *AlpenglowObserverEngine) Close() error {
	if e.receiver != nil {
		return e.receiver.Close()
	}
	return nil
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

func parentBlockIDOrZero(block *block.Block) solana.Hash {
	if block == nil || !block.HasAlpenglowParentBlockID {
		return solana.Hash{}
	}
	return solana.Hash(block.AlpenglowParentBlockID)
}
