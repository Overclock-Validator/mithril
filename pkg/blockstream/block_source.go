package blockstream

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	gossipclient "github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/overcast"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BlockSourceType int

const (
	BlockSourceRpc = iota
	BlockSourceFile
	BlockSourceLightbringer
	BlockSourceTurbine
)

type BlockSourceOpts struct {
	RpcClient               *rpcclient.RpcClient // Primary RPC for block fetching (getBlock)
	SourceType              BlockSourceType
	LightbringerEndpoint    string
	TurbineBindAddr         string
	TurbineGossipEntrypoint string
	TurbineGossipBindAddr   string
	TurbineAdvertisedIP     string
	TurbineShredVersion     uint16
	TurbineAlpenglowAddr    string
	// Enables Alpenglow/Votor block-id hints for the Turbine assembler. Classic
	// Solana clusters leave this off even when blocks are sourced from Turbine.
	TurbineAlpenglowBlockIDHints bool
	TurbineIdentity              ed25519.PrivateKey
	LeaderForSlot                func(slot uint64) (solana.PublicKey, bool)
	AlpenglowDecisionSource      func(anchorSlot uint64) (alpenglow.ChainDecision, bool)
	AlpenglowCandidateBlockSink  func(alpenglow.ReplayBlockObservation)
	// Cert-driven repair feed: certified-but-unobserved blocks the repair loop
	// steers turbine toward, and the skip oracle that cancels shred state for
	// certificate-skipped slots.
	AlpenglowWantedBlocks  func(afterSlot uint64, max int) []alpenglow.WantedBlock
	AlpenglowSkipCertified func(slot uint64) bool
	StartSlot              uint64
	EndSlot                uint64
	BlockDir               string
	// When enabled, an active near-tip Lightbringer stream is delivered to replay
	// as an observation feed for consensus buffering instead of requiring the
	// block source to resolve every local gap before delivery.

	// Backup RPC endpoints for failover (optional)
	// These are tried in order if the primary fails with hard connectivity errors
	// (connection refused, no such host, etc.). NOT used for timeouts or rate limits.
	// After 100 slots, the primary is retried and restored if working.
	BackupRpcEndpoints []string

	// Parallel fetch settings
	MaxRPS          int    // Rate limit (requests per second), 0 = use default
	MaxInflight     int    // Max concurrent workers, 0 = use default
	TipPollMs       int    // Tip poll interval ms, 0 = use default
	TipSafetyMargin uint64 // Don't fetch within N slots of tip, 0 = use default

	// Mode thresholds (hysteresis)
	NearTipThreshold int // Enter near-tip when gap <= this, 0 = use default
	CatchupThreshold int // Exit near-tip when gap >= this, 0 = use default

	// Tip gate: only apply safety margin when gap > this
	CatchupTipGateThreshold int // 0 = use default

	// Near-tip tuning
	NearTipPollMs    int // Faster poll interval in near-tip, 0 = use default
	NearTipLookahead int // Slots ahead to schedule in near-tip, 0 = use default
}

// slotStatus tracks the state of each slot being fetched
type slotStatus int

const (
	slotPending slotStatus = iota
	slotInflight
	slotDone
)

// fetchResult is sent from workers to the emitter
type fetchResult struct {
	slot                 uint64
	block                *b.Block
	err                  error
	skipped              bool   // true if SlotSkipped error
	absentOK             bool   // true if a secondary confirmed-RPC probe verified the slot is absent
	rpcIdx               int32  // which RPC endpoint produced this result (for error attribution)
	latencyMs            int64  // fetch latency in milliseconds (for stall diagnostics)
	liveStreamGeneration uint64 // live-stream result generation, used to discard stale handoff blocks
}

var errBeyondTip = errors.New("slot beyond confirmed tip")

type blockSourceStopReason uint32

const (
	blockSourceStopReasonNone blockSourceStopReason = iota
	blockSourceStopReasonCompleted
	blockSourceStopReasonStalled
	blockSourceStopReasonUnexpectedLiveEnd
	blockSourceStopReasonAlpenglowConflict
)

// slotErrorInfo tracks error history for a specific slot (for stall diagnostics)
type slotErrorInfo struct {
	slot           uint64
	retryCount     int
	firstSeenAt    time.Time
	lastSeenAt     time.Time
	lastError      string
	lastErrorClass string // "slot_not_available", "skipped", "rate_limited", "beyond_tip", "transient", "hard_conn", "other"
	lastRpcIdx     int32
	lastLatencyMs  int64
}

// StallDiagnostics captures comprehensive state when a stall is detected
type StallDiagnostics struct {
	// Waiting slot context
	WaitingSlot      uint64
	LastExecutedSlot uint64
	ConfirmedTip     uint64
	Gap              uint64
	Mode             string // "near-tip" or "catchup"
	LastProgressTs   time.Time
	StallElapsed     time.Duration

	// Slot state snapshot
	InflightCount    int
	RetryQueueLen    int
	WorkQueueLen     int
	ReorderBufLen    int
	SkippedSlotsLen  int
	MaxBufferedSlot  uint64
	WaitingSlotState string // "inflight", "done", "pending", "missing"

	// Waiting slot error info
	WaitingSlotErrors *slotErrorInfo

	// RPC health snapshot
	ActiveRpcIdx     int32
	ActiveRpcURL     string
	FailoverCount    uint64
	LastFailoverTime time.Time
	IsOnPrimary      bool

	// Per-RPC error counts (current window)
	ErrSlotNotAvail uint64
	ErrRateLimited  uint64
	ErrBeyondTip    uint64
	ErrHistory      uint64
	ErrTransient    uint64
	ErrHardConn     uint64
	ErrOther        uint64

	// Worker pool stats
	WorkersTotal int
	RateLimitRPS float64
}

// BlockSourceStats contains metrics for parallel block fetching
type BlockSourceStats struct {
	// Fetch counts
	FetchAttempts  atomic.Uint64
	FetchSuccesses atomic.Uint64
	FetchRetries   atomic.Uint64
	FetchSkipped   atomic.Uint64

	// Speculative/backup requests
	SpeculativeRetries atomic.Uint64 // Backup requests sent for slow slots

	// Error buckets
	ErrSlotNotAvail atomic.Uint64
	ErrRateLimited  atomic.Uint64
	ErrBeyondTip    atomic.Uint64
	ErrHistory      atomic.Uint64
	ErrTransient    atomic.Uint64 // EOF, timeout, 502/503, connection reset, etc.
	ErrOther        atomic.Uint64

	// Latency tracking (nanoseconds)
	TotalFetchLatencyNs atomic.Uint64
	FetchLatencyCount   atomic.Uint64

	// Buffer stats
	MaxBufferedSlot atomic.Uint64
}

type BlockSource struct {
	rpcClients  []*rpcclient.RpcClient // All RPC clients for block fetching (index 0 = primary)
	streamChan  chan *b.Block
	startSlot   uint64
	endSlot     uint64
	currentSlot uint64
	blockDir    string
	sourceType  BlockSourceType

	// RPC failover tracking
	activeRpcIdx       atomic.Int32  // Currently active RPC index (0 = primary)
	slotsSinceFailover atomic.Uint64 // Slots emitted since failover (for retry timing)
	failoverCount      atomic.Uint64 // Total failovers (for stats)
	hardErrCount       atomic.Uint64 // Consecutive hard connectivity errors (reset on success)
	lastHardErrTime    atomic.Int64  // Unix timestamp of last hard error (for time-windowing)

	// Rate limiting
	rateLimiter *rate.Limiter
	maxInflight int

	// Tip tracking
	confirmedTip      atomic.Uint64
	processedTip      atomic.Uint64 // Processed commitment tip (super tip)
	tipAtSlot         atomic.Uint64 // What slot we had executed when tip was measured
	lastExecutedSlot  atomic.Uint64 // Last slot fully executed by replay (set by SetLastExecutedSlot)
	tipSafetyMargin   uint64
	tipPollInterval   time.Duration
	lastTipUpdate     atomic.Int64  // Unix timestamp of last successful tip poll
	tipPollFailures   atomic.Uint64 // Consecutive tip poll failures
	totalTipPollFails atomic.Uint64 // Total tip poll failures (for stats)

	// Reorder buffer
	reorderMu     sync.Mutex
	reorderBuffer map[uint64]*b.Block
	skippedSlots  map[uint64]bool
	// Tracks skipped slots inferred from a reconnecting Lightbringer descendant.
	// These are not provisional RPC skip results and must not be discarded at handoff.
	lightbringerSynthesizedSkips map[uint64]bool
	// Tracks skips certified by Alpenglow consensus. These are not provisional
	// RPC skip results and must not be discarded after a Turbine handoff.
	alpenglowCertifiedSkips map[uint64]bool
	nextSlotToSend          uint64
	lastEmittedBlockSlot    uint64 // Last non-skipped block emitted to replay; used to validate Lightbringer ancestry at handoff.
	maxPending              int

	// Slot state tracking (prevents duplicates)
	slotStateMu   sync.Mutex
	slotState     map[uint64]slotStatus
	inflightStart map[uint64]time.Time // When each slot started fetching

	// Retry queue for slots that need rescheduling
	retryMu    sync.Mutex
	retrySlots []uint64

	// Worker coordination
	workQueue   chan uint64
	resultQueue chan fetchResult
	stopChan    chan struct{}
	stopped     atomic.Bool

	// Stall detection
	lastProgress atomic.Int64 // Unix timestamp of last successful block emit
	stallTimeout time.Duration
	stallError   atomic.Bool // Set when stall timeout triggers

	// Stall diagnostics
	waitingSlotErrorsMu    sync.Mutex
	waitingSlotErrors      map[uint64]*slotErrorInfo // Per-slot error tracking
	lastStallHeartbeat     atomic.Int64              // Unix timestamp of last stall heartbeat log
	lastFailoverTime       atomic.Int64              // Unix timestamp of last RPC failover
	lastPriorityBlockedLog atomic.Int64              // Unix timestamp of last "priority slot blocked" log
	lastReorderGapLog      atomic.Int64              // Unix timestamp of last buffered-gap warning log

	// Near-tip mode tracking
	isNearTip        atomic.Bool // True when close to confirmed tip
	catchupTipSafety uint64      // Original tip safety margin for catchup mode

	// Configurable mode thresholds
	nearTipThreshold    uint64        // Enter near-tip when gap <= this
	catchupThreshold    uint64        // Exit near-tip when gap >= this
	tipGateThreshold    uint64        // Only apply safety margin when gap > this
	nearTipPollInterval time.Duration // Faster poll in near-tip mode
	nearTipLookahead    uint64        // Slots ahead to schedule in near-tip

	// Lightbringer live-stream handoff
	lightbringerEndpoint           string
	turbineBindAddr                string
	turbineGossipEntrypoint        string
	turbineGossipBindAddr          string
	turbineAdvertisedIP            string
	turbineShredVersion            uint16
	turbineAlpenglowAddr           string
	turbineAlpenglowBlockIDHints   bool
	turbineIdentity                ed25519.PrivateKey
	leaderForSlot                  func(slot uint64) (solana.PublicKey, bool)
	lightbringerStarted            atomic.Bool
	lightbringerConnected          atomic.Bool
	lightbringerLastStreamSlot     atomic.Uint64
	lightbringerLastRecvUnix       atomic.Int64
	lightbringerReconnectRequested atomic.Bool
	lightbringerCancelMu           sync.Mutex
	lightbringerCancel             context.CancelFunc
	lightbringerHandoffSlot        atomic.Uint64 // First slot from the active stream connection, 0 = no active handoff
	lightbringerResultGeneration   atomic.Uint64 // Incremented whenever a live-stream handoff/runway is invalidated
	lightbringerForceRPCUntil      atomic.Uint64 // While set, ignore Lightbringer and use RPC until this slot is executed
	lightbringerCooldownUntil      atomic.Uint64 // After a missing-slot recovery, keep RPC active until this slot executes
	lightbringerNeedRPCResume      atomic.Bool   // Set when a live handoff disconnects and RPC must fill the gap again
	lightbringerActive             atomic.Bool   // True once emitted blocks are being sourced from Lightbringer
	lightbringerGapSlot            atomic.Uint64 // Waiting slot currently being watched for a Lightbringer gap
	lightbringerGapSinceUnix       atomic.Int64  // UnixNano when the current Lightbringer gap was first observed
	lightbringerGapLastLogUnix     atomic.Int64  // UnixNano of the last active-gap wait log
	lightbringerGapReconnectSlot   atomic.Uint64 // Waiting slot that already triggered a Lightbringer reconnect
	lightbringerRepairSlot         atomic.Uint64 // Missing streamed slot currently being repaired via RPC, 0 = no repair in flight
	lightbringerWg                 sync.WaitGroup
	lightbringerBufferMu           sync.Mutex
	lightbringerBuffer             map[uint64]*b.Block
	lightbringerBufferOrder        []uint64
	alpenglowMu                    sync.Mutex
	knownAlpenglowBlockIDs         map[uint64]solana.Hash
	knownAlpenglowBlockIDOrder     []uint64
	activeTurbineReceiver          *turbine.UDPReceiver
	alpenglowDecisionSource        func(anchorSlot uint64) (alpenglow.ChainDecision, bool)
	alpenglowCandidateBlockSink    func(alpenglow.ReplayBlockObservation)
	alpenglowWantedBlocksFn        func(afterSlot uint64, max int) []alpenglow.WantedBlock
	alpenglowSkipCertifiedFn       func(slot uint64) bool

	// Stats tracking
	stats          BlockSourceStats
	statsResetTime atomic.Int64 // Unix timestamp when stats were last reset

	// Rare safety probe used before finalizing a skipped slot.
	// Tests can override this to avoid live RPC calls.
	confirmSlotAbsent func(slot uint64) bool

	// Shutdown tracking so replay can distinguish normal finite completion
	// from an unexpected live-stream termination.
	stopReason  atomic.Uint32
	stopSlot    atomic.Uint64
	stopEndSlot atomic.Uint64
}

// Default values
const (
	defaultMaxRPS          = 10
	defaultMaxInflight     = 10
	defaultTipPollMs       = 5000
	defaultTipSafetyMargin = 64
	defaultMaxPending      = 100
	streamChanBuffer       = 100
	defaultStallTimeout    = 5 * time.Minute // Trigger graceful shutdown if no progress

	// RPC failover settings
	primaryRetryInterval  = 100             // Retry primary RPC every N slots after failover (progress-based)
	primaryProbeInterval  = 1 * time.Minute // Probe primary RPC every minute when on backup (time-based)
	failoverErrThreshold  = 10              // Consecutive hard errors before failover (conservative)
	failoverErrWindowSecs = 5               // Reset error count if no errors for this long

	// Near-tip mode defaults
	// When within nearTipThreshold slots of confirmed tip, switch to low-latency mode
	defaultNearTipThreshold   = 32  // Switch to near-tip mode when gap <= this
	defaultCatchupThreshold   = 64  // Switch back to catchup mode when gap >= this (hysteresis)
	defaultNearTipPollMs      = 500 // Faster tip polling in near-tip mode (ms)
	defaultNearTipLookahead   = 2   // Schedule up to N slots ahead in near-tip mode
	maxKnownAlpenglowBlockIDs = 8192
	// RPC latency ~300ms, execution ~100ms - need 1-2 slots buffered to avoid waiting
	// Note: tip_safety_margin is NOT applied in near-tip mode by design (we rely on retries)

	// Stall diagnostics thresholds
	stallHeartbeatThreshold = 2 * time.Minute  // Start logging heartbeats when stall exceeds this
	stallHeartbeatInterval  = 30 * time.Second // Log heartbeat every this interval
	reorderGapWarnInterval  = 5 * time.Second  // Rate-limit buffered-gap warnings
	reorderGapWarnThreshold = 16               // Warn once buffered blocks pile up behind a missing slot
	staleBackupResendEvery  = 5 * time.Second  // Re-send speculative backup fetches for slots that stay inflight
	staleWaitingSlotRetry   = 15 * time.Second // Reset and retry a waiting slot if it stays inflight with no result

	// Tip gate threshold: only apply tip safety margin when gap > this
	// When gap <= 128, the gate causes more harm than good (bt storms, buffer drain)
	// because headroom becomes too small (e.g., gap=70, margin=64 → only 6 slots headroom)
	defaultTipGateThreshold = 128

	// Lightbringer stream settings
	lightbringerDialTimeout           = 10 * time.Second
	lightbringerRetryBackoff          = 2 * time.Second
	lightbringerMaxRetryBackoff       = 15 * time.Second
	lightbringerBufferSlots           = 256
	lightbringerFirstSlotWarn         = 10 * time.Second
	lightbringerIdleReconnect         = 30 * time.Second
	lightbringerNoEmitReconnect       = 30 * time.Second
	lightbringerGapReconnectAfter     = 30 * time.Second
	lightbringerDeepGapReconnect      = 15 * time.Second
	lightbringerMinHandoffRun         = 8
	lightbringerLiveEdgeHandoffMaxLag = 4
	lightbringerGapFallbackWait       = 8 * time.Second
	lightbringerGapBufferDepth        = 32
	lightbringerRecoverySlots         = 0

	// RPC getBlock can transiently report SlotSkipped or "block not available"
	// for slots that later turn out to have real blocks. Never emit a skipped
	// slot based on getBlock alone. Only finalize a skip after a separate
	// confirmed-RPC slot-listing probe shows the slot is genuinely absent and
	// the slot is safely behind confirmed tip.
	rpcSkipConfirmDepth = 64
)

func NewBlockSource(opts *BlockSourceOpts) *BlockSource {
	// Apply defaults for global fetch settings
	maxRPS := opts.MaxRPS
	if maxRPS <= 0 {
		maxRPS = defaultMaxRPS
	}

	maxInflight := opts.MaxInflight
	if maxInflight <= 0 {
		maxInflight = defaultMaxInflight
	}

	tipPollMs := opts.TipPollMs
	if tipPollMs <= 0 {
		tipPollMs = defaultTipPollMs
	}

	tipSafetyMargin := opts.TipSafetyMargin
	if tipSafetyMargin == 0 {
		tipSafetyMargin = defaultTipSafetyMargin
	}

	// Apply defaults for mode thresholds
	nearTipThreshold := opts.NearTipThreshold
	if nearTipThreshold <= 0 {
		nearTipThreshold = defaultNearTipThreshold
	}

	catchupThreshold := opts.CatchupThreshold
	if catchupThreshold <= 0 {
		catchupThreshold = defaultCatchupThreshold
	}

	tipGateThreshold := opts.CatchupTipGateThreshold
	if tipGateThreshold <= 0 {
		tipGateThreshold = defaultTipGateThreshold
	}

	// Apply defaults for near-tip tuning
	nearTipPollMs := opts.NearTipPollMs
	if nearTipPollMs <= 0 {
		nearTipPollMs = defaultNearTipPollMs
	}

	nearTipLookahead := opts.NearTipLookahead
	if nearTipLookahead <= 0 {
		nearTipLookahead = defaultNearTipLookahead
	}

	// Build list of RPC clients: primary + backups
	rpcClients := make([]*rpcclient.RpcClient, 0, 1+len(opts.BackupRpcEndpoints))
	rpcClients = append(rpcClients, opts.RpcClient)
	for _, endpoint := range opts.BackupRpcEndpoints {
		rpcClients = append(rpcClients, rpcclient.NewRpcClient(endpoint))
	}

	bs := &BlockSource{
		rpcClients:                   rpcClients,
		streamChan:                   make(chan *b.Block, streamChanBuffer),
		startSlot:                    opts.StartSlot,
		endSlot:                      opts.EndSlot,
		currentSlot:                  opts.StartSlot,
		blockDir:                     opts.BlockDir,
		sourceType:                   opts.SourceType,
		rateLimiter:                  rate.NewLimiter(rate.Limit(maxRPS), maxRPS),
		maxInflight:                  maxInflight,
		tipSafetyMargin:              tipSafetyMargin,
		tipPollInterval:              time.Duration(tipPollMs) * time.Millisecond,
		reorderBuffer:                make(map[uint64]*b.Block),
		skippedSlots:                 make(map[uint64]bool),
		lightbringerSynthesizedSkips: make(map[uint64]bool),
		alpenglowCertifiedSkips:      make(map[uint64]bool),
		nextSlotToSend:               opts.StartSlot,
		maxPending:                   defaultMaxPending,
		slotState:                    make(map[uint64]slotStatus),
		inflightStart:                make(map[uint64]time.Time),
		workQueue:                    make(chan uint64, maxInflight*2),
		resultQueue:                  make(chan fetchResult, maxInflight*2),
		stopChan:                     make(chan struct{}),
		stallTimeout:                 defaultStallTimeout,
		catchupTipSafety:             tipSafetyMargin, // Store original for switching back to catchup
		lightbringerEndpoint:         opts.LightbringerEndpoint,
		turbineBindAddr:              opts.TurbineBindAddr,
		turbineGossipEntrypoint:      opts.TurbineGossipEntrypoint,
		turbineGossipBindAddr:        opts.TurbineGossipBindAddr,
		turbineAdvertisedIP:          opts.TurbineAdvertisedIP,
		turbineShredVersion:          opts.TurbineShredVersion,
		turbineAlpenglowAddr:         opts.TurbineAlpenglowAddr,
		turbineAlpenglowBlockIDHints: opts.TurbineAlpenglowBlockIDHints,
		turbineIdentity:              clonePrivateKey(opts.TurbineIdentity),
		leaderForSlot:                opts.LeaderForSlot,
		alpenglowDecisionSource:      opts.AlpenglowDecisionSource,
		alpenglowCandidateBlockSink:  opts.AlpenglowCandidateBlockSink,
		alpenglowWantedBlocksFn:      opts.AlpenglowWantedBlocks,
		alpenglowSkipCertifiedFn:     opts.AlpenglowSkipCertified,
		lightbringerBuffer:           make(map[uint64]*b.Block),
		knownAlpenglowBlockIDs:       make(map[uint64]solana.Hash),

		// Configurable mode thresholds
		nearTipThreshold:    uint64(nearTipThreshold),
		catchupThreshold:    uint64(catchupThreshold),
		tipGateThreshold:    uint64(tipGateThreshold),
		nearTipPollInterval: time.Duration(nearTipPollMs) * time.Millisecond,
		nearTipLookahead:    uint64(nearTipLookahead),

		// Stall diagnostics
		waitingSlotErrors: make(map[uint64]*slotErrorInfo),
	}

	// Initialize lastProgress to now (first block hasn't been fetched yet)
	bs.lastProgress.Store(time.Now().Unix())

	// Initialize stats reset time for RPS calculation
	bs.statsResetTime.Store(time.Now().Unix())
	bs.confirmSlotAbsent = bs.confirmSlotAbsentViaRPC

	// Log RPC configuration
	if len(rpcClients) > 1 {
		mlog.Log.Infof("Block fetching configured with %d RPC endpoints (primary + %d backups)",
			len(rpcClients), len(rpcClients)-1)
	}
	if opts.SourceType == BlockSourceLightbringer && opts.LightbringerEndpoint != "" {
		mlog.Log.Infof("Lightbringer live handoff configured for %s (RPC catchup remains enabled)", opts.LightbringerEndpoint)
	} else if opts.SourceType == BlockSourceTurbine && opts.TurbineBindAddr != "" {
		mlog.Log.Infof("Native turbine live handoff configured on %s (RPC catchup remains enabled)", opts.TurbineBindAddr)
		if opts.TurbineGossipEntrypoint != "" {
			mlog.Log.Infof("Native turbine gossip configured with entrypoint %s", opts.TurbineGossipEntrypoint)
		}
	}

	return bs
}

func clonePrivateKey(key ed25519.PrivateKey) ed25519.PrivateKey {
	if len(key) == 0 {
		return nil
	}
	return append(ed25519.PrivateKey(nil), key...)
}

func (bs *BlockSource) SetKnownAlpenglowBlockID(slot uint64, blockID solana.Hash) {
	if !bs.turbineAlpenglowBlockIDHints || slot == 0 || blockID == (solana.Hash{}) {
		return
	}

	bs.alpenglowMu.Lock()
	if existing, ok := bs.knownAlpenglowBlockIDs[slot]; !ok {
		bs.knownAlpenglowBlockIDOrder = append(bs.knownAlpenglowBlockIDOrder, slot)
	} else if existing == blockID {
		receiver := bs.activeTurbineReceiver
		bs.alpenglowMu.Unlock()
		if receiver != nil {
			receiver.SetKnownAlpenglowBlockID(slot, blockID)
		}
		return
	}
	bs.knownAlpenglowBlockIDs[slot] = blockID
	for len(bs.knownAlpenglowBlockIDOrder) > maxKnownAlpenglowBlockIDs {
		old := bs.knownAlpenglowBlockIDOrder[0]
		bs.knownAlpenglowBlockIDOrder = bs.knownAlpenglowBlockIDOrder[1:]
		delete(bs.knownAlpenglowBlockIDs, old)
	}
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()

	if receiver != nil {
		receiver.SetKnownAlpenglowBlockID(slot, blockID)
	}
}

func (bs *BlockSource) resetTurbineSlotForAlpenglowBlock(slot uint64, blockID solana.Hash) {
	if !bs.turbineAlpenglowBlockIDHints || slot == 0 || blockID == (solana.Hash{}) {
		return
	}
	bs.SetKnownAlpenglowBlockID(slot, blockID)

	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver != nil {
		receiver.ResetSlot(slot)
		receiver.PrioritizeRepairSlot(slot)
	}
}

// TurbineShredEdges reports the monotonic shred frontier (latest shred slot,
// highest full slot) from the active turbine receiver. ok is false when no
// receiver is active (RPC-only / pre-handoff) — callers must not fabricate
// shred stats then.
func (bs *BlockSource) TurbineShredEdges() (latestShredSlot, highestFullSlot uint64, ok bool) {
	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver == nil {
		return 0, 0, false
	}
	latest, full := receiver.ShredEdges()
	return latest, full, true
}

// TurbineShredObservation reports partial shred arrivals for a slot that never
// became full — "the leader sent SOMETHING" skip observability. ok is false
// when no receiver is active or no shred was ever accepted for the slot.
func (bs *BlockSource) TurbineShredObservation(slot uint64) (dataShreds, repairedShreds int, firstNanos int64, ok bool) {
	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver == nil {
		return 0, 0, 0, false
	}
	obs, found := receiver.ShredObservation(slot)
	if !found {
		return 0, 0, 0, false
	}
	return obs.DataShreds, obs.RepairedShreds, obs.FirstNanos, true
}

func (bs *BlockSource) prioritizeTurbineRepairRange(start, end uint64) {
	if bs.sourceType != BlockSourceTurbine || !bs.turbineAlpenglowBlockIDHints || start == 0 {
		return
	}
	if end < start {
		end = start
	}

	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver != nil {
		receiver.PrioritizeRepairRange(start, end)
	}
}

func (bs *BlockSource) prioritizeTurbineRepairForLiveGap(waitingSlot, firstBufferedParentSlot uint64) {
	end := waitingSlot
	if firstBufferedParentSlot > waitingSlot {
		end = firstBufferedParentSlot
	}
	bs.prioritizeTurbineRepairRange(waitingSlot, end)
}

// Cert-driven repair. Certificates prove which block data the cluster voted
// real BEFORE turbine finishes delivering it here: a certified-but-unobserved
// block means we are missing (or mis-assembled) data the chain has already
// settled on. The repair loop closes that gap continuously instead of waiting
// for the emission frontier to stall on it:
//
//   - certified block not yet observed  -> pin the assembler to the certified
//     block id and pull repair for the slot
//   - buffered pre-emission candidate carrying a DIFFERENT id -> provably
//     non-canonical; discard it and re-arm the slot for the certified version
//     (the post-emission case is the replay switch sweep's job)
//   - certificate-skipped slot -> drop its partial shred state so the
//     receiver stops assembling and repairing data the chain discarded
//
// This also services the switch sweep: after a wrong-sibling unwind the
// certified sibling stays "wanted" until observed, so the hints re-fire every
// nudge interval until the data lands.
const (
	alpenglowRepairTick       = 250 * time.Millisecond
	alpenglowRepairMaxWanted  = 32
	alpenglowRepairNudgePause = time.Second // at most one nudge per slot per second
)

func (bs *BlockSource) alpenglowRepairLoop() {
	ticker := time.NewTicker(alpenglowRepairTick)
	defer ticker.Stop()
	nudged := make(map[uint64]time.Time) // loop-local: single-goroutine rate limiter
	for {
		select {
		case <-bs.stopChan:
			return
		case <-ticker.C:
			bs.serviceAlpenglowWantedBlocks(nudged, time.Now())
		}
	}
}

// serviceAlpenglowWantedBlocks runs one repair pass. nudged is the per-slot
// rate limiter (owned by the calling goroutine); entries at or below the
// emission frontier are pruned as it advances.
func (bs *BlockSource) serviceAlpenglowWantedBlocks(nudged map[uint64]time.Time, now time.Time) {
	if bs.alpenglowWantedBlocksFn == nil || bs.sourceType != BlockSourceTurbine || !bs.turbineAlpenglowBlockIDHints {
		return
	}
	if !bs.isNearTip.Load() {
		return // catch-up fills gaps via ordinary backfill; hints would be noise
	}

	bs.reorderMu.Lock()
	waiting := bs.nextSlotToSend
	bs.reorderMu.Unlock()
	if waiting == 0 {
		return
	}
	after := waiting - 1

	for slot := range nudged {
		if slot <= after {
			delete(nudged, slot)
		}
	}

	for _, w := range bs.alpenglowWantedBlocksFn(after, alpenglowRepairMaxWanted) {
		slot := w.Block.Slot
		if last, ok := nudged[slot]; ok && now.Sub(last) < alpenglowRepairNudgePause {
			continue
		}

		bs.reorderMu.Lock()
		blk := bs.reorderBuffer[slot]
		haveCertified := blk != nil && blk.HasAlpenglowBlockID && solana.Hash(blk.AlpenglowBlockID) == w.Block.Hash
		mismatch := blk != nil && blk.HasAlpenglowBlockID && !haveCertified
		if mismatch {
			delete(bs.reorderBuffer, slot)
			bs.slotStateMu.Lock()
			delete(bs.slotState, slot)
			delete(bs.inflightStart, slot)
			bs.slotStateMu.Unlock()
		}
		bs.reorderMu.Unlock()

		if haveCertified {
			continue // assembled and waiting its emission turn; nothing to repair
		}
		nudged[slot] = now
		if mismatch {
			bs.clearSlotErrors(slot)
			bs.resetTurbineSlotForAlpenglowBlock(slot, w.Block.Hash)
			mlog.Log.Warnf("ALPENGLOW repair: discarded buffered non-certified candidate at slot %d; repairing toward certified block %s",
				slot, w.Block.Hash)
			continue
		}
		bs.SetKnownAlpenglowBlockID(slot, w.Block.Hash)
		bs.prioritizeTurbineRepairRange(slot, slot)
	}

	// Skip-cancel: certificate-skipped slots ahead of the frontier stop
	// accumulating shred state and stop generating repair requests. Slots with
	// a finalized block over a skip never reach here — the wanted-block nudge
	// above refreshes their limiter entry first (finality outranks a skip).
	if bs.alpenglowSkipCertifiedFn == nil {
		return
	}
	for slot := after + 1; slot <= after+alpenglowRepairMaxWanted; slot++ {
		if last, ok := nudged[slot]; ok && now.Sub(last) < alpenglowRepairNudgePause {
			continue
		}
		if !bs.alpenglowSkipCertifiedFn(slot) {
			continue
		}
		nudged[slot] = now
		bs.alpenglowMu.Lock()
		receiver := bs.activeTurbineReceiver
		bs.alpenglowMu.Unlock()
		if receiver != nil {
			receiver.ResetSlot(slot)
		}
	}
}

func (bs *BlockSource) attachAlpenglowBlockIDHintsToReceiver(receiver *turbine.UDPReceiver) {
	if !bs.turbineAlpenglowBlockIDHints || receiver == nil {
		return
	}

	bs.alpenglowMu.Lock()
	bs.activeTurbineReceiver = receiver
	known := make([]struct {
		slot    uint64
		blockID solana.Hash
	}, 0, len(bs.knownAlpenglowBlockIDs))
	for slot, blockID := range bs.knownAlpenglowBlockIDs {
		known = append(known, struct {
			slot    uint64
			blockID solana.Hash
		}{slot: slot, blockID: blockID})
	}
	bs.alpenglowMu.Unlock()

	for _, entry := range known {
		receiver.SetKnownAlpenglowBlockID(entry.slot, entry.blockID)
	}
}

func (bs *BlockSource) observeAlpenglowCandidateBlock(blk *b.Block) {
	if bs.alpenglowCandidateBlockSink == nil || !bs.turbineAlpenglowBlockIDHints {
		return
	}
	if blk == nil || !blk.HasAlpenglowBlockID {
		return
	}
	parentSlot := blk.SourceParentSlot
	if parentSlot == 0 {
		parentSlot = blk.ParentSlot
	}
	// ParentHash stays in the alpenglow block-id domain (zero = unknown); the PoH
	// LastBlockhash never matches the tracker's cert-keyed blocks.
	var parentHash solana.Hash
	if blk.HasAlpenglowParentBlockID {
		parentHash = solana.Hash(blk.AlpenglowParentBlockID)
	}
	bs.alpenglowCandidateBlockSink(alpenglow.ReplayBlockObservation{
		Block: alpenglow.BlockID{
			Slot: blk.Slot,
			Hash: solana.Hash(blk.AlpenglowBlockID),
		},
		ParentSlot: parentSlot,
		ParentHash: parentHash,
		Source:     bs.liveShredStreamName(),
		At:         time.Now(),
	})
}

func (bs *BlockSource) usesLiveShredStream() bool {
	switch bs.sourceType {
	case BlockSourceLightbringer:
		return bs.lightbringerEndpoint != ""
	case BlockSourceTurbine:
		return bs.turbineBindAddr != ""
	default:
		return false
	}
}

func (bs *BlockSource) liveShredStreamName() string {
	if bs.sourceType == BlockSourceTurbine {
		return "TURBINE"
	}
	return "LIGHTBRINGER"
}

func (bs *BlockSource) setStopReason(reason blockSourceStopReason, slot uint64) {
	if reason == blockSourceStopReasonNone {
		return
	}
	if bs.stopReason.CompareAndSwap(uint32(blockSourceStopReasonNone), uint32(reason)) {
		bs.stopSlot.Store(slot)
		bs.stopEndSlot.Store(bs.endSlot)
	}
}

func (bs *BlockSource) stopReasonEnum() blockSourceStopReason {
	return blockSourceStopReason(bs.stopReason.Load())
}

// updateMode checks the gap to tip and switches between catchup and near-tip mode.
// Uses hysteresis to avoid flapping: enter near-tip at <=32 slots, exit at >=64 slots.
func (bs *BlockSource) updateMode() {
	tip := bs.confirmedTip.Load()
	lastExecuted := bs.lastExecutedSlot.Load()

	// Can't determine mode without tip
	if tip == 0 || lastExecuted == 0 {
		return
	}

	// Calculate gap (tip should always be >= lastExecuted, but handle wrap)
	var gap uint64
	if tip > lastExecuted {
		gap = tip - lastExecuted
	} else {
		gap = 0
	}

	wasNearTip := bs.isNearTip.Load()

	if wasNearTip {
		// Currently in near-tip mode - switch to catchup if gap exceeds threshold
		if gap >= bs.catchupThreshold {
			bs.isNearTip.Store(false)
			mlog.Log.Infof("MODE SWITCH: near-tip → CATCHUP | gap=%d (threshold=%d) | exec_slot=%d | tip=%d",
				gap, bs.catchupThreshold, lastExecuted, tip)
			bs.forceRPCForCatchup(gap)
		}
	} else {
		// Currently in catchup mode - switch to near-tip if gap is small
		if gap <= bs.nearTipThreshold {
			bs.isNearTip.Store(true)
			mlog.Log.Infof("MODE SWITCH: catchup → NEAR-TIP | gap=%d (threshold=%d) | exec_slot=%d | tip=%d",
				gap, bs.nearTipThreshold, lastExecuted, tip)
			bs.logLightbringerModeState("near-tip", gap)
		}
	}

	if bs.usesLiveShredStream() {
		bs.maybeStartLightbringerStream()
		if bs.isNearTip.Load() {
			bs.maybePrepareLightbringerHandoff()
		}
	}
}

func (bs *BlockSource) effectiveTipSafetyMargin() uint64 {
	if bs.isNearTip.Load() {
		return 0 // Near-tip mode: no safety margin, rely on retries
	}
	return bs.catchupTipSafety
}

func (bs *BlockSource) currentModeString() string {
	if bs.isNearTip.Load() {
		return "near-tip"
	}
	return "catchup"
}

func (bs *BlockSource) rewindLiveStreamFrontierForRPCFallbackLocked() (waitingSlot uint64, previousWaitingSlot uint64) {
	waitingSlot = bs.nextSlotToSend
	previousWaitingSlot = waitingSlot

	replayNextSlot := bs.startSlot
	if lastExecuted := bs.lastExecutedSlot.Load(); lastExecuted != 0 {
		replayNextSlot = lastExecuted + 1
	}
	if replayNextSlot == 0 || replayNextSlot >= waitingSlot {
		return waitingSlot, previousWaitingSlot
	}

	bs.nextSlotToSend = replayNextSlot
	return replayNextSlot, previousWaitingSlot
}

func (bs *BlockSource) ForceRPCFallback(reason string) {
	if reason == "" {
		reason = "requested"
	}
	tip := bs.confirmedTip.Load()
	lastExecuted := bs.lastExecutedSlot.Load()
	var gap uint64
	if tip > lastExecuted {
		gap = tip - lastExecuted
	}
	bs.forceRPCForCatchupWithReason(gap, reason)
}

func (bs *BlockSource) forceRPCForCatchup(gap uint64) {
	bs.forceRPCForCatchupWithReason(gap, "lost_tip")
}

func (bs *BlockSource) forceRPCForCatchupWithReason(gap uint64, reason string) {
	if !bs.usesLiveShredStream() {
		return
	}
	if reason == "" {
		reason = "lost_tip"
	}

	bs.lightbringerForceRPCUntil.Store(0)
	bs.lightbringerCooldownUntil.Store(0)
	oldHandoff := bs.lightbringerHandoffSlot.Swap(0)
	wasActive := bs.lightbringerActive.Swap(false)
	resultGeneration := bs.invalidateLightbringerResults()
	bs.lightbringerNeedRPCResume.Store(true)
	bs.clearLightbringerGapWatch()
	bs.resetLightbringerRepairSlot()
	clearedPrefetched := bs.clearBufferedLightbringerBlocks()

	bs.reorderMu.Lock()
	waitingSlot := bs.nextSlotToSend
	previousWaitingSlot := waitingSlot
	if wasActive {
		waitingSlot, previousWaitingSlot = bs.rewindLiveStreamFrontierForRPCFallbackLocked()
	}
	rewoundEmissionFrontier := previousWaitingSlot != waitingSlot
	removedSlots := make([]uint64, 0)
	for slot, blk := range bs.reorderBuffer {
		if blk != nil && blk.FromLightbringer && slot >= waitingSlot {
			delete(bs.reorderBuffer, slot)
			removedSlots = append(removedSlots, slot)
		}
	}
	bs.reorderMu.Unlock()

	if len(removedSlots) > 0 {
		bs.slotStateMu.Lock()
		for _, slot := range removedSlots {
			delete(bs.slotState, slot)
			delete(bs.inflightStart, slot)
		}
		bs.slotStateMu.Unlock()
	}
	if rewoundEmissionFrontier {
		bs.slotStateMu.Lock()
		for slot := range bs.slotState {
			if slot >= waitingSlot {
				delete(bs.slotState, slot)
				delete(bs.inflightStart, slot)
			}
		}
		bs.slotStateMu.Unlock()

		bs.retryMu.Lock()
		if len(bs.retrySlots) > 0 {
			filtered := bs.retrySlots[:0]
			for _, slot := range bs.retrySlots {
				if slot < waitingSlot {
					filtered = append(filtered, slot)
				}
			}
			bs.retrySlots = filtered
		}
		bs.retryMu.Unlock()
	}

	if wasActive {
		if previousWaitingSlot != waitingSlot {
			mlog.Log.Warnf("BLOCK SOURCE SWITCH: %s -> RPC at slot %d | reason=%s | gap=%d | rewound_emission_frontier_from=%d | cleared_buffered_live_stream=%d | dropped_prefetched_live_stream=%d | live_generation=%d",
				bs.liveShredStreamName(), waitingSlot, reason, gap, previousWaitingSlot, len(removedSlots), clearedPrefetched, resultGeneration)
			return
		}
		mlog.Log.Warnf("BLOCK SOURCE SWITCH: %s -> RPC at slot %d | reason=%s | gap=%d | cleared_buffered_live_stream=%d | dropped_prefetched_live_stream=%d | live_generation=%d",
			bs.liveShredStreamName(), waitingSlot, reason, gap, len(removedSlots), clearedPrefetched, resultGeneration)
		return
	}
	if oldHandoff != 0 || len(removedSlots) > 0 || clearedPrefetched > 0 {
		if previousWaitingSlot != waitingSlot {
			mlog.Log.Warnf("BLOCK SOURCE STATUS: abandoning pending %s handoff and forcing RPC catchup | reason=%s | waiting_slot=%d | gap=%d | rewound_emission_frontier_from=%d | cleared_buffered_live_stream=%d | dropped_prefetched_live_stream=%d | live_generation=%d",
				bs.liveShredStreamName(), reason, waitingSlot, gap, previousWaitingSlot, len(removedSlots), clearedPrefetched, resultGeneration)
			return
		}
		mlog.Log.Warnf("BLOCK SOURCE STATUS: abandoning pending %s handoff and forcing RPC catchup | reason=%s | waiting_slot=%d | gap=%d | cleared_buffered_live_stream=%d | dropped_prefetched_live_stream=%d | live_generation=%d",
			bs.liveShredStreamName(), reason, waitingSlot, gap, len(removedSlots), clearedPrefetched, resultGeneration)
		return
	}
	mlog.Log.Infof("BLOCK SOURCE STATUS: catchup mode is using RPC | reason=%s | waiting_slot=%d | gap=%d",
		reason, waitingSlot, gap)
}

func (bs *BlockSource) clearLightbringerGapWatch() {
	bs.lightbringerGapSlot.Store(0)
	bs.lightbringerGapSinceUnix.Store(0)
	bs.lightbringerGapLastLogUnix.Store(0)
	bs.lightbringerGapReconnectSlot.Store(0)
}

func (bs *BlockSource) invalidateLightbringerResults() uint64 {
	return bs.lightbringerResultGeneration.Add(1)
}

func (bs *BlockSource) setLightbringerCancel(cancel context.CancelFunc) {
	bs.lightbringerCancelMu.Lock()
	bs.lightbringerCancel = cancel
	bs.lightbringerCancelMu.Unlock()
}

func (bs *BlockSource) clearLightbringerCancel() {
	bs.lightbringerCancelMu.Lock()
	bs.lightbringerCancel = nil
	bs.lightbringerCancelMu.Unlock()
}

func (bs *BlockSource) requestLightbringerReconnect(reason string) bool {
	if !bs.lightbringerConnected.Load() {
		return false
	}
	if !bs.lightbringerReconnectRequested.CompareAndSwap(false, true) {
		return false
	}

	bs.reorderMu.Lock()
	waitingSlot := bs.nextSlotToSend
	lastEmitted := bs.lastEmittedBlockSlot
	bs.reorderMu.Unlock()
	latestStreamed := bs.lightbringerLastStreamSlot.Load()

	mlog.Log.Warnf("%s stream reconnect requested: %s | waiting_slot=%d | last_emitted=%d | latest_streamed=%d",
		bs.liveShredStreamName(), reason, waitingSlot, lastEmitted, latestStreamed)

	bs.lightbringerCancelMu.Lock()
	cancel := bs.lightbringerCancel
	bs.lightbringerCancelMu.Unlock()
	if cancel == nil {
		bs.lightbringerReconnectRequested.Store(false)
		return false
	}
	cancel()
	return true
}

func isLightbringerReconnectCancel(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	return status.Code(err) == codes.Canceled
}

func (bs *BlockSource) maybeReconnectActiveLightbringerForNoProgress(stallDuration time.Duration) {
	if !bs.usesLiveShredStream() {
		return
	}
	if !bs.lightbringerActive.Load() || !bs.isNearTip.Load() {
		return
	}
	if stallDuration < lightbringerNoEmitReconnect {
		return
	}

	bs.reorderMu.Lock()
	waitingSlot := bs.nextSlotToSend
	waitingReady := bs.reorderBuffer[waitingSlot] != nil || bs.skippedSlots[waitingSlot]
	lastEmitted := bs.lastEmittedBlockSlot
	firstBufferedSlot := uint64(0)
	bufferedLightbringer := 0
	foundBuffered := false
	for slot, blk := range bs.reorderBuffer {
		if blk == nil || !blk.FromLightbringer || slot <= waitingSlot {
			continue
		}
		bufferedLightbringer++
		if !foundBuffered || slot < firstBufferedSlot {
			firstBufferedSlot = slot
			foundBuffered = true
		}
	}
	bs.reorderMu.Unlock()

	if waitingReady || len(bs.streamChan) > 0 {
		return
	}

	reason := fmt.Sprintf("no block emitted for %s while %s is active and replay is waiting on slot %d",
		stallDuration.Round(time.Second), bs.liveShredStreamName(), waitingSlot)
	if foundBuffered {
		reason = fmt.Sprintf("no block emitted for %s while waiting on slot %d (first_buffered=%d buffered_live_stream=%d last_emitted=%d)",
			stallDuration.Round(time.Second), waitingSlot, firstBufferedSlot, bufferedLightbringer, lastEmitted)
	}

	bs.requestLightbringerReconnect(reason)
}

func (bs *BlockSource) isLightbringerRepairSlot(slot uint64) bool {
	return slot != 0 && bs.lightbringerRepairSlot.Load() == slot
}

func (bs *BlockSource) clearLightbringerRepairSlot(slot uint64) {
	if slot == 0 {
		return
	}
	bs.lightbringerRepairSlot.CompareAndSwap(slot, 0)
}

func (bs *BlockSource) resetLightbringerRepairSlot() {
	bs.lightbringerRepairSlot.Store(0)
}

func (bs *BlockSource) lightbringerBlockConnectsLocked(blk *b.Block) bool {
	if blk == nil || !blk.FromLightbringer {
		return true
	}
	if bs.lastEmittedBlockSlot == 0 {
		return true
	}
	if blk.SourceParentSlot == 0 {
		return false
	}
	return blk.SourceParentSlot == bs.lastEmittedBlockSlot
}

func (bs *BlockSource) shouldPreferIncomingLightbringerBlockLocked(existing, incoming *b.Block) bool {
	if existing == nil || incoming == nil {
		return false
	}
	if !existing.FromLightbringer || !incoming.FromLightbringer {
		return false
	}
	if existing.Slot != incoming.Slot {
		return false
	}
	return !bs.lightbringerBlockConnectsLocked(existing) && bs.lightbringerBlockConnectsLocked(incoming)
}

func (bs *BlockSource) waitingLightbringerParentMismatchLocked() (waitingSlot uint64, observedParentSlot uint64, expectedParentSlot uint64, mismatch bool) {
	blk := bs.reorderBuffer[bs.nextSlotToSend]
	if blk == nil || !blk.FromLightbringer {
		return 0, 0, 0, false
	}
	if bs.lightbringerBlockConnectsLocked(blk) {
		return 0, 0, 0, false
	}
	return blk.Slot, blk.SourceParentSlot, bs.lastEmittedBlockSlot, true
}

func (bs *BlockSource) repairLightbringerGap(waitingSlot, firstBufferedSlot, firstBufferedParentSlot uint64, bufferedCount int) {
	if !bs.usesLiveShredStream() {
		return
	}
	if !bs.lightbringerActive.Load() {
		bs.forceRPCForLightbringerGap(waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount)
		return
	}

	gapSinceUnix := bs.lightbringerGapSinceUnix.Load()
	gapAge := time.Duration(0)
	if gapSinceUnix != 0 {
		gapAge = time.Since(time.Unix(0, gapSinceUnix))
	}

	streamName := bs.liveShredStreamName()
	waitReason := fmt.Sprintf("first buffered %s block %d still depends on parent slot %d", streamName, firstBufferedSlot, firstBufferedParentSlot)
	switch {
	case firstBufferedParentSlot == waitingSlot:
		waitReason = fmt.Sprintf("waiting on live %s slot %d; later buffered block %d still depends on it", streamName, waitingSlot, firstBufferedSlot)
	case firstBufferedParentSlot > waitingSlot:
		waitReason = fmt.Sprintf("waiting on missing %s ancestry range %d-%d; later buffered block %d still depends on slot %d",
			streamName, waitingSlot, firstBufferedParentSlot, firstBufferedSlot, firstBufferedParentSlot)
	case firstBufferedParentSlot == bs.lastEmittedBlockSlot:
		waitReason = fmt.Sprintf("later buffered block %d points back to anchor %d, but that only proves an observed alternate branch and is not treated as a canonical skipped run", firstBufferedSlot, bs.lastEmittedBlockSlot)
	}

	now := time.Now()
	lastLog := time.Unix(0, bs.lightbringerGapLastLogUnix.Load())
	if lastLog.IsZero() || now.Sub(lastLog) >= reorderGapWarnInterval {
		bs.lightbringerGapLastLogUnix.Store(now.UnixNano())
		mlog.Log.Warnf("BLOCK SOURCE STATUS: waiting for missing %s slot %d from live stream while keeping %s active | first_buffered=%d | first_parent_slot=%d | buffered_live_stream=%d | reason=%s | mode=%s",
			streamName, waitingSlot, streamName, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, waitReason, bs.currentModeString())
	}

	shouldReconnect := false
	reconnectReason := ""
	switch {
	case firstBufferedParentSlot != waitingSlot:
		if firstBufferedParentSlot <= waitingSlot {
			// A reconnect only helps when later buffered live-stream blocks still
			// depend on an unseen ancestor beyond the current anchor.
			break
		}
		switch {
		case bufferedCount >= reorderGapWarnThreshold && gapAge >= lightbringerDeepGapReconnect:
			shouldReconnect = true
			reconnectReason = fmt.Sprintf("waiting %s for %s ancestry range %d-%d while later buffered block %d still depends on slot %d",
				gapAge.Round(time.Second), streamName, waitingSlot, firstBufferedParentSlot, firstBufferedSlot, firstBufferedParentSlot)
		case gapAge >= lightbringerGapReconnectAfter:
			shouldReconnect = true
			reconnectReason = fmt.Sprintf("waiting %s for %s ancestry range %d-%d while later buffered block %d still depends on slot %d",
				gapAge.Round(time.Second), streamName, waitingSlot, firstBufferedParentSlot, firstBufferedSlot, firstBufferedParentSlot)
		}
	case bufferedCount >= reorderGapWarnThreshold && gapAge >= lightbringerDeepGapReconnect:
		shouldReconnect = true
		reconnectReason = fmt.Sprintf("waiting %s for live %s slot %d while %d later buffered blocks still depend on it",
			gapAge.Round(time.Second), streamName, waitingSlot, bufferedCount)
	case gapAge >= lightbringerGapReconnectAfter:
		shouldReconnect = true
		reconnectReason = fmt.Sprintf("waiting %s for live %s slot %d while later buffered blocks still depend on it",
			gapAge.Round(time.Second), streamName, waitingSlot)
	}

	if shouldReconnect && bs.lightbringerGapReconnectSlot.CompareAndSwap(0, waitingSlot) {
		bs.requestLightbringerReconnect(reconnectReason)
	}
}

func (bs *BlockSource) forceRPCForLightbringerGap(waitingSlot, firstBufferedSlot, firstBufferedParentSlot uint64, bufferedCount int) {
	if !bs.usesLiveShredStream() {
		return
	}

	recoveryUntil := waitingSlot + lightbringerRecoverySlots
	if recoveryUntil < waitingSlot {
		recoveryUntil = ^uint64(0)
	}

	bs.lightbringerForceRPCUntil.Store(waitingSlot)
	bs.lightbringerCooldownUntil.Store(recoveryUntil)
	oldHandoff := bs.lightbringerHandoffSlot.Swap(0)
	wasActive := bs.lightbringerActive.Swap(false)
	resultGeneration := bs.invalidateLightbringerResults()
	bs.lightbringerNeedRPCResume.Store(true)
	bs.clearLightbringerGapWatch()
	bs.resetLightbringerRepairSlot()
	clearedPrefetched := bs.clearBufferedLightbringerBlocks()

	bs.reorderMu.Lock()
	removedSlots := make([]uint64, 0)
	for slot, blk := range bs.reorderBuffer {
		if blk != nil && blk.FromLightbringer && slot >= waitingSlot {
			delete(bs.reorderBuffer, slot)
			removedSlots = append(removedSlots, slot)
		}
	}
	bs.reorderMu.Unlock()

	if len(removedSlots) > 0 {
		bs.slotStateMu.Lock()
		for _, slot := range removedSlots {
			delete(bs.slotState, slot)
			delete(bs.inflightStart, slot)
		}
		bs.slotStateMu.Unlock()
	}

	if wasActive || oldHandoff != 0 {
		mlog.Log.Warnf("BLOCK SOURCE SWITCH: %s -> RPC at slot %d | reason=missing_streamed_slot | first_buffered=%d | first_parent_slot=%d | buffered_live_stream=%d | cleared_prefetched_live_stream=%d | rpc_forced_until=%d | rpc_cooldown_until=%d | live_generation=%d | mode=%s",
			bs.liveShredStreamName(), waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, clearedPrefetched, waitingSlot, recoveryUntil, resultGeneration, bs.currentModeString())
		return
	}

	mlog.Log.Warnf("BLOCK SOURCE STATUS: forcing RPC because %s skipped waiting slot %d | first_buffered=%d | first_parent_slot=%d | buffered_live_stream=%d | cleared_prefetched_live_stream=%d | rpc_forced_until=%d | rpc_cooldown_until=%d | live_generation=%d | mode=%s",
		bs.liveShredStreamName(), waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, clearedPrefetched, waitingSlot, recoveryUntil, resultGeneration, bs.currentModeString())
}

func (bs *BlockSource) handleDetectedLightbringerGap(waitingSlot, firstBufferedSlot, firstBufferedParentSlot uint64, bufferedCount int) {
	if bs.lightbringerActive.Load() {
		bs.repairLightbringerGap(waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount)
		return
	}
	bs.forceRPCForLightbringerGap(waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount)
}

func (bs *BlockSource) forceRPCForLightbringerParentMismatch(waitingSlot, observedParentSlot, expectedParentSlot uint64) {
	if !bs.usesLiveShredStream() {
		return
	}

	recoveryUntil := waitingSlot + lightbringerRecoverySlots
	if recoveryUntil < waitingSlot {
		recoveryUntil = ^uint64(0)
	}

	bs.lightbringerForceRPCUntil.Store(waitingSlot)
	bs.lightbringerCooldownUntil.Store(recoveryUntil)
	oldHandoff := bs.lightbringerHandoffSlot.Swap(0)
	wasActive := bs.lightbringerActive.Swap(false)
	resultGeneration := bs.invalidateLightbringerResults()
	bs.lightbringerNeedRPCResume.Store(true)
	bs.clearLightbringerGapWatch()
	bs.resetLightbringerRepairSlot()
	clearedPrefetched := bs.clearBufferedLightbringerBlocks()

	bs.reorderMu.Lock()
	removedSlots := make([]uint64, 0)
	for slot, blk := range bs.reorderBuffer {
		if blk != nil && blk.FromLightbringer && slot >= waitingSlot {
			delete(bs.reorderBuffer, slot)
			removedSlots = append(removedSlots, slot)
		}
	}
	bs.reorderMu.Unlock()

	if len(removedSlots) > 0 {
		bs.slotStateMu.Lock()
		for _, slot := range removedSlots {
			delete(bs.slotState, slot)
			delete(bs.inflightStart, slot)
		}
		bs.slotStateMu.Unlock()
	}

	reason := "parent_slot_mismatch"
	if observedParentSlot == 0 {
		reason = "missing_parent_slot"
	}

	if wasActive || oldHandoff != 0 {
		mlog.Log.Warnf("BLOCK SOURCE SWITCH: %s -> RPC at slot %d | reason=%s | observed_parent_slot=%d | expected_parent_slot=%d | cleared_prefetched_live_stream=%d | rpc_forced_until=%d | rpc_cooldown_until=%d | live_generation=%d | mode=%s",
			bs.liveShredStreamName(), waitingSlot, reason, observedParentSlot, expectedParentSlot, clearedPrefetched, waitingSlot, recoveryUntil, resultGeneration, bs.currentModeString())
		return
	}

	mlog.Log.Warnf("BLOCK SOURCE STATUS: rejecting %s handoff at slot %d | reason=%s | observed_parent_slot=%d | expected_parent_slot=%d | cleared_prefetched_live_stream=%d | rpc_forced_until=%d | rpc_cooldown_until=%d | live_generation=%d | mode=%s",
		bs.liveShredStreamName(), waitingSlot, reason, observedParentSlot, expectedParentSlot, clearedPrefetched, waitingSlot, recoveryUntil, resultGeneration, bs.currentModeString())
}

func (bs *BlockSource) logLightbringerModeState(mode string, gap uint64) {
	if !bs.usesLiveShredStream() {
		return
	}

	handoffSlot := bs.lightbringerHandoffSlot.Load()
	active := bs.lightbringerActive.Load()
	cooldownUntil := bs.lightbringerCooldownUntil.Load()
	connected := bs.lightbringerConnected.Load()
	lastSlot := bs.lightbringerLastStreamSlot.Load()
	started := bs.lightbringerStarted.Load()

	bs.reorderMu.Lock()
	waitingSlot := bs.nextSlotToSend
	bs.reorderMu.Unlock()

	switch mode {
	case "near-tip":
		if active {
			mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained and blocks are already arriving from %s | waiting_slot=%d | handoff_slot=%d | gap=%d",
				bs.liveShredStreamName(), waitingSlot, handoffSlot, gap)
			return
		}
		if cooldownUntil != 0 {
			if connected && lastSlot != 0 {
				mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; staying on RPC until slot %d after a %s gap (latest streamed slot %d) | waiting_slot=%d | gap=%d",
					cooldownUntil, bs.liveShredStreamName(), lastSlot, waitingSlot, gap)
				return
			}
			mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; staying on RPC until slot %d after a %s gap | waiting_slot=%d | gap=%d",
				cooldownUntil, bs.liveShredStreamName(), waitingSlot, gap)
			return
		}
		if handoffSlot != 0 {
			mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; waiting to switch block receipt from RPC to %s at handoff slot %d | waiting_slot=%d | gap=%d",
				bs.liveShredStreamName(), handoffSlot, waitingSlot, gap)
			return
		}
		if connected {
			if lastSlot != 0 {
				mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; %s stream is connected (latest streamed slot %d) and waiting to arm handoff | waiting_slot=%d | gap=%d",
					bs.liveShredStreamName(), lastSlot, waitingSlot, gap)
				return
			}
			mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; %s stream is connected and waiting for its first streamed slot | waiting_slot=%d | gap=%d",
				bs.liveShredStreamName(), waitingSlot, gap)
			return
		}
		if started {
			mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; waiting for %s stream connection before handoff | waiting_slot=%d | gap=%d",
				bs.liveShredStreamName(), waitingSlot, gap)
			return
		}
		mlog.Log.Infof("BLOCK SOURCE STATUS: near-tip regained; preparing to switch block receipt from RPC to %s | waiting_slot=%d | gap=%d",
			bs.liveShredStreamName(), waitingSlot, gap)
	}
}

func (bs *BlockSource) maybeStartLightbringerStream() {
	if !bs.usesLiveShredStream() {
		return
	}
	if bs.lightbringerStarted.CompareAndSwap(false, true) {
		bs.lightbringerWg.Add(1)
		if bs.sourceType == BlockSourceTurbine {
			go bs.runTurbineStream()
		} else {
			go bs.runLightbringerStream()
		}
	}
}

func (bs *BlockSource) bufferLightbringerBlock(blk *b.Block) {
	if blk == nil {
		return
	}

	bs.lightbringerBufferMu.Lock()
	defer bs.lightbringerBufferMu.Unlock()

	if _, exists := bs.lightbringerBuffer[blk.Slot]; exists {
		return
	}

	bs.lightbringerBuffer[blk.Slot] = blk
	bs.lightbringerBufferOrder = append(bs.lightbringerBufferOrder, blk.Slot)

	for len(bs.lightbringerBufferOrder) > lightbringerBufferSlots {
		oldest := bs.lightbringerBufferOrder[0]
		bs.lightbringerBufferOrder = bs.lightbringerBufferOrder[1:]
		delete(bs.lightbringerBuffer, oldest)
	}
}

func (bs *BlockSource) clearBufferedLightbringerBlocks() int {
	bs.lightbringerBufferMu.Lock()
	defer bs.lightbringerBufferMu.Unlock()

	cleared := len(bs.lightbringerBuffer)
	if cleared == 0 {
		return 0
	}

	bs.lightbringerBuffer = make(map[uint64]*b.Block)
	bs.lightbringerBufferOrder = nil
	return cleared
}

func (bs *BlockSource) purgeRPCStateAtOrBeyondSlot(slot uint64) {
	if slot == 0 {
		return
	}

	bs.reorderMu.Lock()
	for bufferedSlot, blk := range bs.reorderBuffer {
		if bufferedSlot < slot {
			continue
		}
		if blk == nil || !blk.FromLightbringer {
			delete(bs.reorderBuffer, bufferedSlot)
		}
	}
	for skippedSlot := range bs.skippedSlots {
		if skippedSlot >= slot {
			delete(bs.skippedSlots, skippedSlot)
			delete(bs.lightbringerSynthesizedSkips, skippedSlot)
			delete(bs.alpenglowCertifiedSkips, skippedSlot)
		}
	}
	bs.reorderMu.Unlock()

	bs.slotStateMu.Lock()
	for trackedSlot := range bs.slotState {
		if trackedSlot >= slot && !bs.isLightbringerRepairSlot(trackedSlot) {
			delete(bs.slotState, trackedSlot)
			delete(bs.inflightStart, trackedSlot)
		}
	}
	bs.slotStateMu.Unlock()

	bs.retryMu.Lock()
	if len(bs.retrySlots) > 0 {
		filtered := bs.retrySlots[:0]
		for _, retrySlot := range bs.retrySlots {
			if retrySlot < slot || bs.isLightbringerRepairSlot(retrySlot) {
				filtered = append(filtered, retrySlot)
			}
		}
		bs.retrySlots = filtered
	}
	bs.retryMu.Unlock()
}

func (bs *BlockSource) lightbringerHandoffMaxReplayGap() uint64 {
	// Arm Lightbringer only in the lower half of the near-tip window. Once
	// forkchoice buffering starts, replay can wait for vote-confirmed path
	// resolution; keeping this headroom prevents immediate lost-tip fallback.
	maxGap := bs.nearTipThreshold / 2
	if maxGap == 0 && bs.nearTipThreshold > 0 {
		maxGap = 1
	}
	if bs.catchupThreshold > 0 && maxGap >= bs.catchupThreshold {
		maxGap = bs.catchupThreshold - 1
	}
	return maxGap
}

func (bs *BlockSource) lightbringerHandoffTipEstimate() uint64 {
	tip := bs.confirmedTip.Load()
	if bs.lightbringerConnected.Load() {
		if streamed := bs.lightbringerLastStreamSlot.Load(); streamed > tip {
			tip = streamed
		}
	}
	return tip
}

func (bs *BlockSource) lightbringerHandoffReplayGapOK() (bool, uint64, uint64, uint64, uint64) {
	maxGap := bs.lightbringerHandoffMaxReplayGap()
	tip := bs.lightbringerHandoffTipEstimate()
	lastExecuted := bs.lastExecutedSlot.Load()
	if tip == 0 || lastExecuted == 0 {
		return true, 0, maxGap, tip, lastExecuted
	}

	var gap uint64
	if tip > lastExecuted {
		gap = tip - lastExecuted
	}
	return gap <= maxGap, gap, maxGap, tip, lastExecuted
}

func lightbringerDefaultHandoffLastSlot(waitingSlot uint64) uint64 {
	requiredLastSlot := waitingSlot + uint64(lightbringerMinHandoffRun) - 1
	if requiredLastSlot < waitingSlot {
		requiredLastSlot = math.MaxUint64
	}
	return requiredLastSlot
}

func (bs *BlockSource) lightbringerLiveEdgeHandoffMaxLag() uint64 {
	maxLag := bs.nearTipLookahead + 2
	if maxLag < lightbringerLiveEdgeHandoffMaxLag {
		maxLag = lightbringerLiveEdgeHandoffMaxLag
	}
	return maxLag
}

func (bs *BlockSource) allowsLiveEdgeHandoff() bool {
	return bs.sourceType == BlockSourceTurbine
}

func (bs *BlockSource) lightbringerHandoffRequiredLastSlot(waitingSlot uint64) uint64 {
	requiredLastSlot := lightbringerDefaultHandoffLastSlot(waitingSlot)
	if !bs.allowsLiveEdgeHandoff() || !bs.lightbringerConnected.Load() {
		return requiredLastSlot
	}

	latestStreamed := bs.lightbringerLastStreamSlot.Load()
	if latestStreamed < waitingSlot {
		return requiredLastSlot
	}

	tip := bs.lightbringerHandoffTipEstimate()
	if tip > latestStreamed && tip-latestStreamed > bs.lightbringerLiveEdgeHandoffMaxLag() {
		return requiredLastSlot
	}

	if latestStreamed < requiredLastSlot {
		return latestStreamed
	}
	return requiredLastSlot
}

func (bs *BlockSource) prepareLightbringerHandoff(waitingSlot uint64, anchorSlot uint64) ([]*b.Block, uint64, bool) {
	if !bs.isNearTip.Load() {
		return nil, 0, false
	}
	if handoffSlot := bs.lightbringerHandoffSlot.Load(); handoffSlot != 0 {
		return nil, handoffSlot, false
	}
	if ok, _, _, _, _ := bs.lightbringerHandoffReplayGapOK(); !ok {
		return nil, 0, false
	}

	bs.lightbringerBufferMu.Lock()
	defer bs.lightbringerBufferMu.Unlock()

	runwayBlocks, coveredUntil, _, _, ok := bs.connectedLightbringerRunwayLocked(waitingSlot, anchorSlot)
	if !ok {
		return nil, 0, false
	}

	requiredLastSlot := bs.lightbringerHandoffRequiredLastSlot(waitingSlot)
	if coveredUntil < requiredLastSlot {
		return nil, 0, false
	}

	handoffSlot := waitingSlot
	if !bs.lightbringerHandoffSlot.CompareAndSwap(0, handoffSlot) {
		return nil, bs.lightbringerHandoffSlot.Load(), false
	}

	bs.lightbringerNeedRPCResume.Store(false)
	bs.purgeRPCStateAtOrBeyondSlot(handoffSlot)

	blocks := append([]*b.Block(nil), runwayBlocks...)

	bs.lightbringerBuffer = make(map[uint64]*b.Block)
	bs.lightbringerBufferOrder = nil

	return blocks, handoffSlot, true
}

func (bs *BlockSource) connectedLightbringerRunwayLocked(waitingSlot uint64, anchorSlot uint64) ([]*b.Block, uint64, uint64, uint64, bool) {
	candidates := make([]*b.Block, 0, len(bs.lightbringerBuffer))
	var firstBufferedSlot uint64
	var maxBufferedSlot uint64
	foundFirstBuffered := false
	for slot, blk := range bs.lightbringerBuffer {
		if blk == nil || slot < waitingSlot {
			continue
		}
		candidates = append(candidates, blk)
		if !foundFirstBuffered || slot < firstBufferedSlot {
			firstBufferedSlot = slot
			foundFirstBuffered = true
		}
		if slot > maxBufferedSlot {
			maxBufferedSlot = slot
		}
	}
	if len(candidates) == 0 {
		return nil, 0, 0, 0, false
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Slot == candidates[j].Slot {
			return candidates[i].SourceParentSlot < candidates[j].SourceParentSlot
		}
		return candidates[i].Slot < candidates[j].Slot
	})

	currentAnchor := anchorSlot
	minSlot := waitingSlot
	coveredUntil := waitingSlot - 1
	if waitingSlot == 0 {
		coveredUntil = 0
	}

	runway := make([]*b.Block, 0, len(candidates))
	used := make(map[uint64]bool, len(candidates))
	for {
		var next *b.Block
		for _, blk := range candidates {
			if blk == nil || used[blk.Slot] || blk.Slot < minSlot {
				continue
			}
			if blk.SourceParentSlot != currentAnchor {
				continue
			}
			next = blk
			break
		}
		if next == nil {
			break
		}

		runway = append(runway, next)
		used[next.Slot] = true
		coveredUntil = next.Slot
		currentAnchor = next.Slot
		minSlot = next.Slot + 1
	}

	return runway, coveredUntil, firstBufferedSlot, maxBufferedSlot, len(runway) != 0
}

func (bs *BlockSource) lightbringerHandoffWaitReason(waitingSlot uint64, anchorSlot uint64) string {
	bs.lightbringerBufferMu.Lock()
	defer bs.lightbringerBufferMu.Unlock()

	runway, coveredUntil, firstBufferedSlot, maxBufferedSlot, ok := bs.connectedLightbringerRunwayLocked(waitingSlot, anchorSlot)
	if !ok {
		if len(bs.lightbringerBuffer) == 0 {
			return fmt.Sprintf("waiting for first buffered %s slot", bs.liveShredStreamName())
		}
		return fmt.Sprintf("no buffered %s slot at or beyond waiting slot %d", bs.liveShredStreamName(), waitingSlot)
	}

	requiredLastSlot := bs.lightbringerHandoffRequiredLastSlot(waitingSlot)
	if coveredUntil >= requiredLastSlot {
		if ok, gap, maxGap, tip, lastExecuted := bs.lightbringerHandoffReplayGapOK(); !ok {
			return fmt.Sprintf("handoff-ready runway buffered through slot %d, but replay gap %d exceeds handoff arm threshold %d (last executed %d, live tip estimate %d)",
				coveredUntil, gap, maxGap, lastExecuted, tip)
		}
		return fmt.Sprintf("handoff-ready runway buffered through slot %d", coveredUntil)
	}

	firstRunwaySlot := runway[0].Slot
	if firstRunwaySlot > waitingSlot {
		return fmt.Sprintf("waiting for slot %d or skipped-slot inference; first buffered %s block is slot %d (parent %d), connected runway covers through slot %d, latest buffered slot %d",
			waitingSlot, bs.liveShredStreamName(), firstRunwaySlot, runway[0].SourceParentSlot, coveredUntil, maxBufferedSlot)
	}

	if firstBufferedSlot != 0 && firstRunwaySlot != firstBufferedSlot {
		return fmt.Sprintf("earliest buffered slot %d is not on the current connected runway; connected runway covers through slot %d, latest buffered slot %d",
			firstBufferedSlot, coveredUntil, maxBufferedSlot)
	}

	return fmt.Sprintf("connected runway only covers through slot %d; need through slot %d before handoff", coveredUntil, requiredLastSlot)
}

func (bs *BlockSource) enqueueLightbringerBlocks(blocks []*b.Block) {
	if len(blocks) == 0 {
		return
	}
	generation := bs.lightbringerResultGeneration.Load()

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Slot < blocks[j].Slot
	})

	for _, blk := range blocks {
		if blk == nil {
			continue
		}

		select {
		case bs.resultQueue <- fetchResult{slot: blk.Slot, block: blk, rpcIdx: -1, liveStreamGeneration: generation}:
		case <-bs.stopChan:
			return
		}
	}
}

func (bs *BlockSource) maybePrepareLightbringerHandoff() {
	if !bs.usesLiveShredStream() || !bs.isNearTip.Load() {
		return
	}
	if bs.lightbringerForceRPCUntil.Load() != 0 {
		return
	}
	if bs.lightbringerCooldownUntil.Load() != 0 {
		return
	}

	bs.reorderMu.Lock()
	waitingSlot := bs.nextSlotToSend
	anchorSlot := bs.lastEmittedBlockSlot
	bs.reorderMu.Unlock()

	if ok, _, _, _, _ := bs.lightbringerHandoffReplayGapOK(); !ok {
		return
	}

	blocks, handoffSlot, prepared := bs.prepareLightbringerHandoff(waitingSlot, anchorSlot)
	if !prepared {
		return
	}

	runwayCoveredUntil := handoffSlot - 1
	if len(blocks) > 0 {
		runwayCoveredUntil = blocks[len(blocks)-1].Slot
	}
	mlog.Log.Infof("%s handoff ready at slot %d (connected runway buffered through slot %d; RPC catchup continues until then)",
		bs.liveShredStreamName(), handoffSlot, runwayCoveredUntil)
	bs.enqueueLightbringerBlocks(blocks)
}

func (bs *BlockSource) lightbringerStagingMaxReplayGap() uint64 {
	maxGap := bs.catchupThreshold
	minGap := bs.nearTipThreshold + uint64(lightbringerMinHandoffRun)
	if minGap < bs.nearTipThreshold {
		minGap = math.MaxUint64
	}
	if maxGap < minGap {
		maxGap = minGap
	}
	return maxGap
}

func (bs *BlockSource) shouldStageLightbringerSlot(slot uint64) bool {
	if bs.lightbringerForceRPCUntil.Load() != 0 {
		return false
	}
	if bs.lightbringerCooldownUntil.Load() != 0 {
		return false
	}

	lastExecuted := bs.lastExecutedSlot.Load()
	if lastExecuted == 0 || slot <= lastExecuted {
		return false
	}

	bs.reorderMu.Lock()
	nextSlot := bs.nextSlotToSend
	bs.reorderMu.Unlock()
	if nextSlot != 0 && slot < nextSlot {
		return false
	}

	tip := bs.lightbringerHandoffTipEstimate()
	if tip <= lastExecuted {
		return false
	}
	return tip-lastExecuted <= bs.lightbringerStagingMaxReplayGap()
}

func (bs *BlockSource) shouldDecodeLightbringerSlot(slot uint64) bool {
	if bs.lightbringerForceRPCUntil.Load() != 0 {
		return false
	}
	if bs.lightbringerCooldownUntil.Load() != 0 {
		return false
	}
	handoffSlot := bs.lightbringerHandoffSlot.Load()
	if handoffSlot == 0 {
		return bs.isNearTip.Load() || bs.shouldStageLightbringerSlot(slot)
	}
	return bs.isNearTip.Load() && slot >= handoffSlot
}

func (bs *BlockSource) shouldUseRPCForSlot(slot uint64) bool {
	if !bs.usesLiveShredStream() {
		return true
	}
	if bs.lightbringerForceRPCUntil.Load() != 0 {
		return true
	}
	if bs.lightbringerCooldownUntil.Load() != 0 {
		return true
	}
	if bs.isLightbringerRepairSlot(slot) {
		return true
	}
	if !bs.isNearTip.Load() {
		return true
	}

	handoffSlot := bs.lightbringerHandoffSlot.Load()
	if handoffSlot == 0 {
		return true
	}

	return slot < handoffSlot
}

func (bs *BlockSource) shouldDiscardRPCResultAfterHandoff(slot uint64, blk *b.Block) bool {
	if !bs.usesLiveShredStream() {
		return false
	}
	handoffSlot := bs.lightbringerHandoffSlot.Load()
	if handoffSlot == 0 || slot < handoffSlot {
		return false
	}
	if bs.shouldUseRPCForSlot(slot) {
		return false
	}
	return blk == nil || !blk.FromLightbringer
}

func (bs *BlockSource) shouldDiscardSkippedSlotAfterHandoff(slot uint64) bool {
	if !bs.shouldDiscardRPCResultAfterHandoff(slot, nil) {
		return false
	}
	return !bs.lightbringerSynthesizedSkips[slot] && !bs.alpenglowCertifiedSkips[slot]
}

func (bs *BlockSource) shouldDiscardLiveStreamResult(slot uint64, generation uint64) bool {
	if generation != bs.lightbringerResultGeneration.Load() {
		return true
	}
	if bs.lightbringerForceRPCUntil.Load() != 0 {
		return true
	}
	if bs.lightbringerCooldownUntil.Load() != 0 {
		return true
	}
	if bs.isLightbringerRepairSlot(slot) {
		return true
	}
	if !bs.isNearTip.Load() {
		return true
	}

	handoffSlot := bs.lightbringerHandoffSlot.Load()
	return handoffSlot == 0 || slot < handoffSlot
}

// inspectLaterLightbringerBlocksLocked summarizes later buffered live shred
// traffic for the currently waiting slot so diagnostics can distinguish
// "we have later stream traffic" from "we have a descendant that reconnects
// directly to the current anchor".
func (bs *BlockSource) inspectLaterLightbringerBlocksLocked(waitingSlot uint64) (firstBufferedSlot uint64, firstBufferedParentSlot uint64, bufferedCount int, firstConnectedSlot uint64, firstConnectedParentSlot uint64, foundConnected bool) {
	for slot, blk := range bs.reorderBuffer {
		if blk == nil || !blk.FromLightbringer || slot <= waitingSlot {
			continue
		}
		bufferedCount++
		if firstBufferedSlot == 0 || slot < firstBufferedSlot {
			firstBufferedSlot = slot
			firstBufferedParentSlot = blk.SourceParentSlot
		}
		if blk.SourceParentSlot != bs.lastEmittedBlockSlot {
			continue
		}
		if !foundConnected || slot < firstConnectedSlot {
			firstConnectedSlot = slot
			firstConnectedParentSlot = blk.SourceParentSlot
			foundConnected = true
		}
	}
	return firstBufferedSlot, firstBufferedParentSlot, bufferedCount, firstConnectedSlot, firstConnectedParentSlot, foundConnected
}

// applyAlpenglowCertifiedSkipLocked marks the waiting slot skipped when the
// consensus decision source certifies it skipped — in ANY block-source mode
// (RPC catchup / pre-handoff included). A certified skip is a consensus fact,
// so applying it early is always safe; it keeps a certified-skipped slot from
// being re-fetched/re-run and makes the skip decision survive block-source
// recreation on a post-switch re-replay. The emit loop then advances the
// frontier for the marked skip mode-independently. Returns true if it newly
// marked the slot.
func (bs *BlockSource) applyAlpenglowCertifiedSkipLocked() bool {
	if bs.alpenglowDecisionSource == nil {
		return false
	}
	waitingSlot := bs.nextSlotToSend
	if waitingSlot == 0 || bs.skippedSlots[waitingSlot] {
		return false
	}
	decision, ok := bs.alpenglowDecisionSource(waitingSlot - 1)
	if !ok || decision.Slot != waitingSlot || decision.Kind != alpenglow.ChainDecisionKindSkip {
		return false
	}
	delete(bs.reorderBuffer, waitingSlot)
	bs.skippedSlots[waitingSlot] = true
	bs.alpenglowCertifiedSkips[waitingSlot] = true
	delete(bs.lightbringerSynthesizedSkips, waitingSlot)
	bs.slotStateMu.Lock()
	bs.slotState[waitingSlot] = slotDone
	delete(bs.inflightStart, waitingSlot)
	bs.slotStateMu.Unlock()
	bs.clearSlotErrors(waitingSlot)
	bs.stats.FetchSkipped.Add(1)
	mlog.Log.FileOnlyf("ALPENGLOW consensus decision: slot %d certified-skipped (mode-independent)", waitingSlot)
	return true
}

func (bs *BlockSource) applyAlpenglowDecisionLocked() bool {
	if bs.alpenglowDecisionSource == nil || bs.sourceType != BlockSourceTurbine || !bs.turbineAlpenglowBlockIDHints {
		return false
	}
	// Certified skips are consensus facts — apply them regardless of mode so a
	// certified-skipped slot is never re-run and the decision survives source
	// recreation. The marked skip advances via the normal emit path.
	if bs.applyAlpenglowCertifiedSkipLocked() {
		return false
	}
	// Buffered-candidate block steering needs an active near-tip Turbine stream.
	if !bs.lightbringerActive.Load() || !bs.isNearTip.Load() {
		return false
	}

	waitingSlot := bs.nextSlotToSend
	if waitingSlot == 0 {
		return false
	}
	decision, ok := bs.alpenglowDecisionSource(waitingSlot - 1)
	if !ok || decision.Slot != waitingSlot {
		return false
	}

	switch decision.Kind {
	case alpenglow.ChainDecisionKindSkip:
		if !bs.skippedSlots[waitingSlot] {
			delete(bs.reorderBuffer, waitingSlot)
			bs.skippedSlots[waitingSlot] = true
			bs.alpenglowCertifiedSkips[waitingSlot] = true
			delete(bs.lightbringerSynthesizedSkips, waitingSlot)
			bs.slotStateMu.Lock()
			bs.slotState[waitingSlot] = slotDone
			delete(bs.inflightStart, waitingSlot)
			bs.slotStateMu.Unlock()
			bs.clearSlotErrors(waitingSlot)
			bs.stats.FetchSkipped.Add(1)
			mlog.Log.FileOnlyf("ALPENGLOW consensus decision: slot %d is skipped (%s)", waitingSlot, decision.Reason)
		}
		return false
	case alpenglow.ChainDecisionKindBlock:
		if bs.skippedSlots[waitingSlot] {
			delete(bs.skippedSlots, waitingSlot)
			delete(bs.lightbringerSynthesizedSkips, waitingSlot)
			delete(bs.alpenglowCertifiedSkips, waitingSlot)
		}
		blk := bs.reorderBuffer[waitingSlot]
		if blk == nil || !blk.HasAlpenglowBlockID {
			return false
		}
		if solana.Hash(blk.AlpenglowBlockID) == decision.Block.Hash {
			return false
		}
		delete(bs.reorderBuffer, waitingSlot)
		bs.slotStateMu.Lock()
		delete(bs.slotState, waitingSlot)
		delete(bs.inflightStart, waitingSlot)
		bs.slotStateMu.Unlock()
		bs.clearSlotErrors(waitingSlot)
		bs.resetTurbineSlotForAlpenglowBlock(waitingSlot, decision.Block.Hash)
		mlog.Log.Warnf("ALPENGLOW consensus decision: discarded non-canonical turbine block for slot %d (got=%s want=%s)",
			waitingSlot, solana.Hash(blk.AlpenglowBlockID), decision.Block.Hash)
		return true
	case alpenglow.ChainDecisionKindConflict:
		// Two blocks certified for one slot (equivocation) is a consensus safety
		// violation. Fail closed: drop the buffered candidate so no fork is emitted,
		// and record a fatal stop reason. The candidate never resolves, so the
		// scheduler stalls on it and replay halts (setStopReason's CAS keeps this
		// conflict reason over the later "stalled").
		mlog.Log.Warnf("ALPENGLOW SAFETY: consensus conflict (equivocation) at slot %d (%s) — halting replay", waitingSlot, decision.Reason)
		delete(bs.reorderBuffer, waitingSlot)
		bs.setStopReason(blockSourceStopReasonAlpenglowConflict, waitingSlot)
		return false
	default:
		return false
	}
}

func (bs *BlockSource) waitForStopOrTimeout(delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-bs.stopChan:
			return true
		default:
			return false
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-bs.stopChan:
		return true
	case <-timer.C:
		return false
	}
}

func (bs *BlockSource) ingestLiveShredBlock(blk *b.Block) bool {
	if blk == nil {
		return true
	}

	bs.lightbringerLastStreamSlot.Store(blk.Slot)
	bs.lightbringerLastRecvUnix.Store(time.Now().Unix())

	if !bs.shouldDecodeLightbringerSlot(blk.Slot) {
		return true
	}

	if bs.lightbringerHandoffSlot.Load() == 0 {
		// Stage a bounded runway before near-tip so handoff does not have
		// to build its whole connected run while replay is already at tip.
		bs.bufferLightbringerBlock(blk)
		bs.maybePrepareLightbringerHandoff()
		return true
	}

	if !bs.isNearTip.Load() || blk.Slot < bs.lightbringerHandoffSlot.Load() {
		return true
	}

	select {
	case bs.resultQueue <- fetchResult{slot: blk.Slot, block: blk, rpcIdx: -1, liveStreamGeneration: bs.lightbringerResultGeneration.Load()}:
		return true
	case <-bs.stopChan:
		return false
	}
}

func (bs *BlockSource) handleLiveShredStreamClosed(reason string) int {
	bs.lightbringerConnected.Store(false)
	bs.clearLightbringerCancel()
	interrupted := bs.lightbringerHandoffSlot.Load() != 0 || bs.lightbringerActive.Load()
	if interrupted {
		bs.reorderMu.Lock()
		waitingSlot := bs.nextSlotToSend
		bs.reorderMu.Unlock()
		if waitingSlot == 0 {
			if lastExecuted := bs.lastExecutedSlot.Load(); lastExecuted != 0 {
				waitingSlot = lastExecuted + 1
			} else {
				waitingSlot = bs.startSlot
			}
		}
		bs.forceRPCForLightbringerGap(waitingSlot, 0, 0, 0)
		mlog.Log.Warnf("%s handoff interrupted; replay will resume RPC fallback from slot %d until a fresh stream runway is armed",
			bs.liveShredStreamName(), waitingSlot)
		if reason != "" {
			mlog.Log.Warnf("%s stream closed: %s", bs.liveShredStreamName(), reason)
		}
		return 0
	}

	bs.invalidateLightbringerResults()
	clearedPrefetched := bs.clearBufferedLightbringerBlocks()
	bs.lightbringerHandoffSlot.Store(0)
	if reason != "" {
		mlog.Log.Warnf("%s stream closed: %s", bs.liveShredStreamName(), reason)
	}
	return clearedPrefetched
}

func (bs *BlockSource) runTurbineStream() {
	defer bs.lightbringerWg.Done()

	backoff := lightbringerRetryBackoff
	for {
		if bs.stopped.Load() {
			return
		}

		bs.lightbringerConnected.Store(false)
		streamCtx, cancelStream := context.WithCancel(context.Background())
		bs.setLightbringerCancel(cancelStream)

		var gossipClient *gossipclient.Client
		var gossipDone <-chan error
		if bs.turbineGossipEntrypoint != "" {
			gossipCfg := gossipclient.Config{
				Entrypoint:    bs.turbineGossipEntrypoint,
				BindAddr:      bs.turbineGossipBindAddr,
				TVUAddr:       bs.turbineBindAddr,
				AlpenglowAddr: bs.turbineAlpenglowAddr,
				AdvertisedIP:  bs.turbineAdvertisedIP,
				ShredVersion:  bs.turbineShredVersion,
				Identity:      bs.turbineIdentity,
				Name:          gossipclient.ClientName,
			}
			client, err := gossipclient.NewClient(gossipCfg)
			if err != nil {
				cancelStream()
				bs.handleLiveShredStreamClosed(fmt.Sprintf("native turbine gossip client setup failed: %v", err))
				if bs.waitForStopOrTimeout(backoff) {
					return
				}
				backoff *= 2
				if backoff > lightbringerMaxRetryBackoff {
					backoff = lightbringerMaxRetryBackoff
				}
				continue
			}
			gossipClient = client
		}

		receiver := turbine.NewUDPReceiver(bs.turbineBindAddr)
		bs.attachAlpenglowBlockIDHintsToReceiver(receiver)
		receiver.SetLeaderForSlot(bs.leaderForSlot)
		if gossipClient != nil {
			if err := receiver.SetRepairPeerSource(gossipClient.Identity(), gossipClient.RepairPeers); err != nil {
				cancelStream()
				bs.handleLiveShredStreamClosed(fmt.Sprintf("native turbine repair setup failed: %v", err))
				if bs.waitForStopOrTimeout(backoff) {
					return
				}
				backoff *= 2
				if backoff > lightbringerMaxRetryBackoff {
					backoff = lightbringerMaxRetryBackoff
				}
				continue
			}
		}
		streamDone := make(chan error, 1)
		go func() {
			streamDone <- receiver.Run(streamCtx)
		}()
		go func() {
			select {
			case <-bs.stopChan:
				cancelStream()
			case <-streamCtx.Done():
			}
		}()

		select {
		case err := <-receiver.Ready():
			if err != nil {
				cancelStream()
				<-streamDone
				bs.handleLiveShredStreamClosed(err.Error())
				if bs.waitForStopOrTimeout(backoff) {
					return
				}
				backoff *= 2
				if backoff > lightbringerMaxRetryBackoff {
					backoff = lightbringerMaxRetryBackoff
				}
				continue
			}
		case <-bs.stopChan:
			cancelStream()
			<-streamDone
			return
		}

		mlog.Log.Infof("Native turbine receiver listening on %s", bs.turbineBindAddr)
		bs.lightbringerConnected.Store(true)
		bs.lightbringerLastRecvUnix.Store(time.Now().Unix())
		backoff = lightbringerRetryBackoff

		if gossipClient != nil {
			done := make(chan error, 1)
			go func() {
				done <- gossipClient.Run(streamCtx)
			}()
			gossipDone = done
			bindAddr := bs.turbineGossipBindAddr
			if bindAddr == "" {
				bindAddr = gossipclient.DefaultBindAddr
			}
			alpenglowAddr := "disabled"
			if bs.turbineAlpenglowAddr != "" {
				alpenglowAddr = bs.turbineAlpenglowAddr
			}
			mlog.Log.Infof("Native turbine gossip client starting: entrypoint=%s bind=%s client=%s repair=enabled alpenglow=%s", bs.turbineGossipEntrypoint, bindAddr, gossipclient.ClientName, alpenglowAddr)
		} else {
			mlog.Log.Warnf("Native turbine gossip entrypoint is not configured; receiver is running UDP-only on %s with repair disabled", bs.turbineBindAddr)
		}

		var streamErr error
		streamDoneConsumed := false
		var ignoredPacketCount uint64
		var ignoredPacketLastLog time.Time
		statsTicker := time.NewTicker(10 * time.Second)
	streamLoop:
		for {
			select {
			case blk, ok := <-receiver.Blocks():
				if !ok {
					streamErr = <-streamDone
					streamDoneConsumed = true
					break streamLoop
				}
				if !bs.ingestLiveShredBlock(blk) {
					statsTicker.Stop()
					cancelStream()
					<-streamDone
					return
				}
			case err, ok := <-receiver.Errors():
				if ok && err != nil {
					ignoredPacketCount++
					now := time.Now()
					if ignoredPacketLastLog.IsZero() || now.Sub(ignoredPacketLastLog) >= 10*time.Second {
						mlog.Log.FileOnlyf("native turbine packets ignored: count=%d latest=%v", ignoredPacketCount, err)
						ignoredPacketCount = 0
						ignoredPacketLastLog = now
					} else {
						mlog.Log.Debugf("native turbine packet ignored: %v", err)
					}
				}
			case <-statsTicker.C:
				stats := receiver.Stats()
				lastPacketAge := "never"
				if stats.LastPacketUnix != 0 {
					lastPacketAge = time.Since(time.Unix(stats.LastPacketUnix, 0)).Round(time.Second).String()
				}
				nonCanonicalDesc := "none"
				if stats.NonCanonicalBlockIDs != 0 {
					nonCanonicalDesc = fmt.Sprintf("%d:%s!=%s", stats.LastNonCanonicalSlot, stats.LastNonCanonicalGot, stats.LastNonCanonicalWant)
				}
				mlog.Log.FileOnlyf("native turbine receiver stats: packets=%d data=%d coding=%d recovered=%d blocks=%d active_slots=%d evicted_slots=%d ignored_old_shreds=%d priority_repair_slots=%d noncanonical_block_ids=%d last_noncanonical=%s repair_requests=%d repair_responses=%d repair_timeouts=%d repair_outstanding=%d repair_peers=%d repair_pings=%d/%d repair_errors=%d parse_errors=%d sig_errors=%d missing_leaders=%d assembly_errors=%d last_packet=%s last_data_slot=%d last_block_slot=%d",
					stats.Packets, stats.DataShreds, stats.CodingShreds, stats.RecoveredData, stats.BlocksEmitted, stats.ActiveSlots,
					stats.EvictedSlots, stats.IgnoredOldShreds, stats.PriorityRepairSlots, stats.NonCanonicalBlockIDs, nonCanonicalDesc, stats.Repair.Requests, stats.Repair.Responses, stats.Repair.Timeouts, stats.Repair.Outstanding, stats.Repair.Peers,
					stats.Repair.Pings, stats.Repair.Pongs, stats.Repair.Errors, stats.ParseErrors, stats.SignatureErrors, stats.MissingLeaders, stats.AssemblyErrors,
					lastPacketAge, stats.LastDataSlot, stats.LastBlockSlot)
			case err := <-streamDone:
				streamErr = err
				streamDoneConsumed = true
				break streamLoop
			case err := <-gossipDone:
				if err != nil {
					streamErr = fmt.Errorf("native turbine gossip client stopped: %w", err)
				} else {
					streamErr = fmt.Errorf("native turbine gossip client stopped")
				}
				break streamLoop
			case <-bs.stopChan:
				statsTicker.Stop()
				cancelStream()
				<-streamDone
				return
			}
		}

		statsTicker.Stop()
		cancelStream()
		if !streamDoneConsumed {
			if err := <-streamDone; streamErr == nil {
				streamErr = err
			}
		}
		if streamErr == nil && bs.stopped.Load() {
			return
		}
		reason := ""
		if streamErr != nil {
			reason = streamErr.Error()
		}
		bs.handleLiveShredStreamClosed(reason)
		if bs.waitForStopOrTimeout(backoff) {
			return
		}
		backoff *= 2
		if backoff > lightbringerMaxRetryBackoff {
			backoff = lightbringerMaxRetryBackoff
		}
	}
}

func (bs *BlockSource) runLightbringerStream() {
	defer bs.lightbringerWg.Done()

	backoff := lightbringerRetryBackoff

	for {
		if bs.stopped.Load() {
			return
		}

		bs.lightbringerConnected.Store(false)

		conn, err := grpc.NewClient(bs.lightbringerEndpoint, grpc.WithInsecure())
		if err != nil {
			mlog.Log.Warnf("Lightbringer dial failed for %s: %v", bs.lightbringerEndpoint, err)
			if bs.waitForStopOrTimeout(backoff) {
				return
			}
			backoff *= 2
			if backoff > lightbringerMaxRetryBackoff {
				backoff = lightbringerMaxRetryBackoff
			}
			continue
		}

		streamCtx, cancelStream := context.WithCancel(context.Background())
		bs.setLightbringerCancel(cancelStream)
		streamDone := make(chan struct{})
		go func() {
			defer close(streamDone)
			select {
			case <-bs.stopChan:
				cancelStream()
			case <-streamCtx.Done():
			}
		}()

		client := overcast.NewSlotStreamClient(conn)
		stream, err := client.StreamSlots(streamCtx, &overcast.SlotStreamRequest{})
		if err != nil {
			cancelStream()
			<-streamDone
			bs.clearLightbringerCancel()
			_ = conn.Close()
			mlog.Log.Warnf("Lightbringer stream setup failed for %s: %v", bs.lightbringerEndpoint, err)
			if bs.waitForStopOrTimeout(backoff) {
				return
			}
			backoff *= 2
			if backoff > lightbringerMaxRetryBackoff {
				backoff = lightbringerMaxRetryBackoff
			}
			continue
		}

		mlog.Log.Infof("Lightbringer stream connected to %s", bs.lightbringerEndpoint)
		bs.lightbringerConnected.Store(true)
		bs.lightbringerLastRecvUnix.Store(time.Now().Unix())
		backoff = lightbringerRetryBackoff
		firstSlotReceived := make(chan struct{})
		firstSlotOnce := sync.Once{}
		connectionClosed := make(chan struct{})
		connectionClosedOnce := sync.Once{}
		go func(endpoint string) {
			ticker := time.NewTicker(lightbringerFirstSlotWarn)
			defer ticker.Stop()

			connectedAt := time.Now()
			for {
				select {
				case <-bs.stopChan:
					return
				case <-connectionClosed:
					return
				case <-firstSlotReceived:
					return
				case <-ticker.C:
					bs.reorderMu.Lock()
					waitingSlot := bs.nextSlotToSend
					bs.reorderMu.Unlock()
					mlog.Log.Warnf("Lightbringer stream connected to %s but has not delivered its first slot after %s | mode=%s | waiting_slot=%d",
						endpoint, time.Since(connectedAt).Round(time.Second), bs.currentModeString(), waitingSlot)
				}
			}
		}(bs.lightbringerEndpoint)
		go func(endpoint string) {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-bs.stopChan:
					return
				case <-connectionClosed:
					return
				case <-ticker.C:
					if !bs.lightbringerActive.Load() || !bs.isNearTip.Load() {
						continue
					}
					lastRecvUnix := bs.lightbringerLastRecvUnix.Load()
					if lastRecvUnix == 0 {
						continue
					}
					idleFor := time.Since(time.Unix(lastRecvUnix, 0))
					if idleFor < lightbringerIdleReconnect {
						// The stream may still be delivering some traffic while failing to
						// make useful forward progress on the next slot Mithril needs. If
						// we've emitted nothing for a while and the next slot is still not
						// available to replay, reconnect the Lightbringer stream anyway.
						lastProgressUnix := bs.lastProgress.Load()
						if lastProgressUnix == 0 {
							continue
						}
						noEmitFor := time.Since(time.Unix(lastProgressUnix, 0))
						if noEmitFor < lightbringerNoEmitReconnect {
							continue
						}

						bs.reorderMu.Lock()
						waitingSlot := bs.nextSlotToSend
						waitingReady := bs.reorderBuffer[waitingSlot] != nil || bs.skippedSlots[waitingSlot]
						bs.reorderMu.Unlock()
						if waitingReady || len(bs.streamChan) > 0 {
							continue
						}

						bs.requestLightbringerReconnect(fmt.Sprintf("no block emitted for %s while Lightbringer is active and replay is waiting on slot %d",
							noEmitFor.Round(time.Second), waitingSlot))
						continue
					}
					bs.requestLightbringerReconnect(fmt.Sprintf("live stream idle for %s while near-tip replay is active",
						idleFor.Round(time.Second)))
				}
			}
		}(bs.lightbringerEndpoint)

		for {
			resp, err := stream.Recv()
			if err != nil {
				connectionClosedOnce.Do(func() {
					close(connectionClosed)
				})
				bs.handleLiveShredStreamClosed("")
				cancelStream()
				<-streamDone
				_ = conn.Close()

				reconnectRequested := bs.lightbringerReconnectRequested.Swap(false)
				canceledByReconnect := reconnectRequested && isLightbringerReconnectCancel(err)

				if bs.stopped.Load() || isLightbringerReconnectCancel(err) || errors.Is(err, io.EOF) {
					if bs.stopped.Load() {
						return
					}
					if reconnectRequested {
						if canceledByReconnect {
							mlog.Log.Warnf("Lightbringer stream reconnecting after watchdog request")
						} else {
							mlog.Log.Warnf("Lightbringer stream reconnecting after watchdog request (recv err: %v)", err)
						}
					} else if isLightbringerReconnectCancel(err) {
						return
					}
					if !reconnectRequested {
						mlog.Log.Warnf("Lightbringer stream closed, retrying: %v", err)
					}
				} else {
					if reconnectRequested {
						mlog.Log.Warnf("Lightbringer stream reconnecting after watchdog request (recv err: %v)", err)
					} else {
						mlog.Log.Warnf("Lightbringer stream receive failed, retrying: %v", err)
					}
				}

				if bs.waitForStopOrTimeout(backoff) {
					return
				}
				backoff *= 2
				if backoff > lightbringerMaxRetryBackoff {
					backoff = lightbringerMaxRetryBackoff
				}
				break
			}

			if resp == nil {
				continue
			}

			firstSlotOnce.Do(func() {
				close(firstSlotReceived)
			})
			bs.lightbringerLastStreamSlot.Store(resp.Slot)
			bs.lightbringerLastRecvUnix.Store(time.Now().Unix())

			if len(resp.Entries) == 0 {
				mlog.Log.Warnf("Lightbringer delivered slot %d with no entries; ignoring", resp.Slot)
				continue
			}

			if !bs.shouldDecodeLightbringerSlot(resp.Slot) {
				continue
			}

			blk := block.FromLightbringerStreamMsg(resp)
			if !bs.ingestLiveShredBlock(blk) {
				cancelStream()
				<-streamDone
				_ = conn.Close()
				return
			}
		}
	}
}

func (bs *BlockSource) maybeLogReorderGapLocked() {
	waitingSlot := bs.nextSlotToSend
	if len(bs.reorderBuffer) < reorderGapWarnThreshold {
		return
	}
	if bs.reorderBuffer[waitingSlot] != nil || bs.skippedSlots[waitingSlot] {
		return
	}

	now := time.Now()
	lastLogUnix := bs.lastReorderGapLog.Load()
	if lastLogUnix != 0 && now.Sub(time.Unix(lastLogUnix, 0)) < reorderGapWarnInterval {
		return
	}
	bs.lastReorderGapLog.Store(now.Unix())

	var minSlot uint64
	var maxSlot uint64
	var lightbringerCount int
	first := true
	for slot, blk := range bs.reorderBuffer {
		if first || slot < minSlot {
			minSlot = slot
		}
		if first || slot > maxSlot {
			maxSlot = slot
		}
		if blk != nil && blk.FromLightbringer {
			lightbringerCount++
		}
		first = false
	}
	if first {
		return
	}

	waitingState := "missing"
	bs.slotStateMu.Lock()
	if state, exists := bs.slotState[waitingSlot]; exists {
		switch state {
		case slotInflight:
			waitingState = "inflight"
		case slotDone:
			waitingState = "done"
		case slotPending:
			waitingState = "pending"
		}
	}
	bs.slotStateMu.Unlock()

	var gapToFirst uint64
	if minSlot > waitingSlot {
		gapToFirst = minSlot - waitingSlot
	}

	firstLightbringerSlot, firstLightbringerParentSlot, _, firstConnectedSlot, firstConnectedParentSlot, foundConnected := bs.inspectLaterLightbringerBlocksLocked(waitingSlot)

	firstLightbringerDesc := "none"
	if firstLightbringerSlot != 0 {
		firstLightbringerDesc = fmt.Sprintf("%d(parent=%d)", firstLightbringerSlot, firstLightbringerParentSlot)
	}

	firstConnectedDesc := "none"
	if foundConnected {
		firstConnectedDesc = fmt.Sprintf("%d(parent=%d gap_span=%d)", firstConnectedSlot, firstConnectedParentSlot, firstConnectedSlot-waitingSlot)
	}

	mlog.Log.Warnf("reorderBuffer growth: waiting on missing slot %d | waiting_state=%s | buffered=%d slots (%d lightbringer) | buffered_range=%d-%d | gap_to_first_buffered=%d | first_lightbringer=%s | first_connected_to_anchor=%s | mode=%s",
		waitingSlot, waitingState, len(bs.reorderBuffer), lightbringerCount, minSlot, maxSlot, gapToFirst, firstLightbringerDesc, firstConnectedDesc, bs.currentModeString())
}

func (bs *BlockSource) detectLightbringerGapLocked() (waitingSlot uint64, firstBufferedSlot uint64, firstBufferedParentSlot uint64, bufferedCount int, shouldFallback bool) {
	if !bs.usesLiveShredStream() {
		return 0, 0, 0, 0, false
	}
	if bs.lightbringerForceRPCUntil.Load() != 0 {
		bs.clearLightbringerGapWatch()
		return 0, 0, 0, 0, false
	}
	handoffSlot := bs.lightbringerHandoffSlot.Load()
	lightbringerActive := bs.lightbringerActive.Load()
	if handoffSlot == 0 && !lightbringerActive {
		bs.clearLightbringerGapWatch()
		return 0, 0, 0, 0, false
	}
	waitingSlot = bs.nextSlotToSend
	if !lightbringerActive && handoffSlot != 0 && waitingSlot < handoffSlot {
		// RPC still owns slots before the pending handoff boundary. Buffered
		// Lightbringer blocks beyond that boundary are expected and must not be
		// treated as evidence that the current RPC-owned waiting slot is missing.
		bs.clearLightbringerGapWatch()
		return 0, 0, 0, 0, false
	}
	if bs.reorderBuffer[waitingSlot] != nil || bs.skippedSlots[waitingSlot] {
		bs.clearLightbringerGapWatch()
		return 0, 0, 0, 0, false
	}
	if lightbringerActive && bs.isLightbringerRepairSlot(waitingSlot) {
		bs.clearLightbringerGapWatch()
		return 0, 0, 0, 0, false
	}

	first := true
	for slot, blk := range bs.reorderBuffer {
		if blk == nil || !blk.FromLightbringer || slot <= waitingSlot {
			continue
		}
		bufferedCount++
		if first || slot < firstBufferedSlot {
			firstBufferedSlot = slot
			firstBufferedParentSlot = blk.SourceParentSlot
			first = false
		}
	}

	if bufferedCount == 0 || first {
		bs.clearLightbringerGapWatch()
		return 0, 0, 0, 0, false
	}

	now := time.Now()
	gapSlot := bs.lightbringerGapSlot.Load()
	gapSinceUnix := bs.lightbringerGapSinceUnix.Load()
	if gapSlot != waitingSlot || gapSinceUnix == 0 {
		bs.lightbringerGapSlot.Store(waitingSlot)
		bs.lightbringerGapSinceUnix.Store(now.UnixNano())
		bs.lightbringerGapLastLogUnix.Store(0)
		bs.lightbringerGapReconnectSlot.Store(0)
		return waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, false
	}

	if bufferedCount >= lightbringerGapBufferDepth {
		return waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, true
	}
	if now.Sub(time.Unix(0, gapSinceUnix)) < lightbringerGapFallbackWait {
		return waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, false
	}

	return waitingSlot, firstBufferedSlot, firstBufferedParentSlot, bufferedCount, true
}

func classifyError(err error) string {
	if err == nil {
		return "success"
	}
	if err == rpcclient.SlotSkipped {
		return "skipped"
	}
	if err == errBeyondTip {
		return "beyond_tip"
	}
	if isSlotNotAvailableErr(err) {
		return "slot_not_available"
	}
	if isRateLimitedErr(err) {
		return "rate_limited"
	}
	if isHistoryUnavailableErr(err) {
		return "history_unavailable"
	}
	if isHardConnectivityErr(err) {
		return "hard_conn"
	}
	if isTransientNetworkErr(err) {
		return "transient"
	}
	return "other"
}

// trackSlotError records an error for a specific slot (for stall diagnostics)
func (bs *BlockSource) trackSlotError(slot uint64, err error, rpcIdx int32, latencyMs int64) {
	bs.waitingSlotErrorsMu.Lock()
	defer bs.waitingSlotErrorsMu.Unlock()

	info, exists := bs.waitingSlotErrors[slot]
	if !exists {
		info = &slotErrorInfo{
			slot:        slot,
			firstSeenAt: time.Now(),
		}
		bs.waitingSlotErrors[slot] = info
	}

	info.retryCount++
	info.lastSeenAt = time.Now()
	if err != nil {
		info.lastError = err.Error()
		// Truncate long error messages
		if len(info.lastError) > 200 {
			info.lastError = info.lastError[:200] + "..."
		}
	} else {
		info.lastError = ""
	}
	info.lastErrorClass = classifyError(err)
	info.lastRpcIdx = rpcIdx
	info.lastLatencyMs = latencyMs

	// Clean up old entries (keep only last 100 slots)
	if len(bs.waitingSlotErrors) > 100 {
		// Find the minimum slot to keep
		minToKeep := slot
		if minToKeep > 100 {
			minToKeep -= 100
		} else {
			minToKeep = 0
		}
		for s := range bs.waitingSlotErrors {
			if s < minToKeep {
				delete(bs.waitingSlotErrors, s)
			}
		}
	}
}

// clearSlotErrors removes error tracking for a slot (called on success)
func (bs *BlockSource) clearSlotErrors(slot uint64) {
	bs.waitingSlotErrorsMu.Lock()
	delete(bs.waitingSlotErrors, slot)
	bs.waitingSlotErrorsMu.Unlock()
}

func (bs *BlockSource) shouldLogDetailedRPCOutcome(slot uint64) bool {
	bs.reorderMu.Lock()
	waitingSlot := bs.nextSlotToSend
	bs.reorderMu.Unlock()

	if slot < waitingSlot {
		return false
	}
	return slot <= waitingSlot+2
}

func (bs *BlockSource) rpcEndpointForIndex(rpcIdx int32) string {
	if rpcIdx < 0 || int(rpcIdx) >= len(bs.rpcClients) {
		if len(bs.rpcClients) == 0 {
			return "unknown"
		}
		return bs.rpcClients[0].Endpoint()
	}
	return bs.rpcClients[rpcIdx].Endpoint()
}

func (bs *BlockSource) shouldProbeAbsentConfirmation(slot uint64) bool {
	return bs.confirmedTip.Load() >= slot+rpcSkipConfirmDepth
}

func (bs *BlockSource) shouldFinalizeSkippedSlot(slot uint64, absentOK bool) bool {
	if !absentOK || !bs.shouldProbeAbsentConfirmation(slot) {
		return false
	}

	bs.waitingSlotErrorsMu.Lock()
	info, exists := bs.waitingSlotErrors[slot]
	bs.waitingSlotErrorsMu.Unlock()
	if !exists {
		return false
	}
	return info.lastErrorClass == "skipped" || info.lastErrorClass == "slot_not_available"
}

func (bs *BlockSource) confirmSlotAbsentViaRPC(slot uint64) bool {
	if len(bs.rpcClients) == 0 {
		return false
	}

	activeIdx := int(bs.activeRpcIdx.Load())
	if activeIdx < 0 || activeIdx >= len(bs.rpcClients) {
		activeIdx = 0
	}

	probeOrder := make([]int, 0, len(bs.rpcClients))
	probeOrder = append(probeOrder, activeIdx)
	for idx := range bs.rpcClients {
		if idx != activeIdx {
			probeOrder = append(probeOrder, idx)
		}
	}

	confirmedAbsent := false
	shouldLog := bs.shouldLogDetailedRPCOutcome(slot)
	for _, idx := range probeOrder {
		slots, err := bs.rpcClients[idx].GetBlocksWithLimitConfirmed(slot, 1)
		if shouldLog {
			switch {
			case err != nil:
				mlog.Log.Warnf("RPC omission probe: slot %d via %s | err=%v",
					slot, bs.rpcClients[idx].Endpoint(), err)
			case len(slots) == 0:
				mlog.Log.Warnf("RPC omission probe: slot %d via %s | returned no confirmed slots",
					slot, bs.rpcClients[idx].Endpoint())
			default:
				mlog.Log.Warnf("RPC omission probe: slot %d via %s | first_confirmed_slot=%d",
					slot, bs.rpcClients[idx].Endpoint(), slots[0])
			}
		}
		if err != nil || len(slots) == 0 {
			continue
		}
		if slots[0] == slot {
			return false
		}
		if slots[0] > slot {
			confirmedAbsent = true
		}
	}

	return confirmedAbsent
}

// collectStallDiagnostics gathers comprehensive state for stall analysis
func (bs *BlockSource) collectStallDiagnostics() StallDiagnostics {
	diag := StallDiagnostics{}

	// Waiting slot context
	bs.reorderMu.Lock()
	diag.WaitingSlot = bs.nextSlotToSend
	diag.ReorderBufLen = len(bs.reorderBuffer)
	diag.SkippedSlotsLen = len(bs.skippedSlots)
	bs.reorderMu.Unlock()

	diag.LastExecutedSlot = bs.lastExecutedSlot.Load()
	diag.ConfirmedTip = bs.confirmedTip.Load()
	if diag.ConfirmedTip > diag.LastExecutedSlot {
		diag.Gap = diag.ConfirmedTip - diag.LastExecutedSlot
	}
	if bs.isNearTip.Load() {
		diag.Mode = "near-tip"
	} else {
		diag.Mode = "catchup"
	}
	diag.LastProgressTs = time.Unix(bs.lastProgress.Load(), 0)
	diag.StallElapsed = time.Since(diag.LastProgressTs)

	// Slot state snapshot
	bs.slotStateMu.Lock()
	diag.InflightCount = 0
	for _, state := range bs.slotState {
		if state == slotInflight {
			diag.InflightCount++
		}
	}
	// Check waiting slot state
	if state, exists := bs.slotState[diag.WaitingSlot]; exists {
		switch state {
		case slotInflight:
			diag.WaitingSlotState = "inflight"
		case slotDone:
			diag.WaitingSlotState = "done"
		case slotPending:
			diag.WaitingSlotState = "pending"
		}
	} else {
		diag.WaitingSlotState = "missing"
	}
	bs.slotStateMu.Unlock()

	bs.retryMu.Lock()
	diag.RetryQueueLen = len(bs.retrySlots)
	bs.retryMu.Unlock()

	diag.WorkQueueLen = len(bs.workQueue)
	diag.MaxBufferedSlot = bs.stats.MaxBufferedSlot.Load()

	// Waiting slot error info
	bs.waitingSlotErrorsMu.Lock()
	if info, exists := bs.waitingSlotErrors[diag.WaitingSlot]; exists {
		diag.WaitingSlotErrors = &slotErrorInfo{
			slot:           info.slot,
			retryCount:     info.retryCount,
			firstSeenAt:    info.firstSeenAt,
			lastSeenAt:     info.lastSeenAt,
			lastError:      info.lastError,
			lastErrorClass: info.lastErrorClass,
			lastRpcIdx:     info.lastRpcIdx,
			lastLatencyMs:  info.lastLatencyMs,
		}
	}
	bs.waitingSlotErrorsMu.Unlock()

	// RPC health snapshot
	diag.ActiveRpcIdx = bs.activeRpcIdx.Load()
	if int(diag.ActiveRpcIdx) < len(bs.rpcClients) {
		diag.ActiveRpcURL = bs.rpcClients[diag.ActiveRpcIdx].Endpoint()
	}
	diag.FailoverCount = bs.failoverCount.Load()
	diag.LastFailoverTime = time.Unix(bs.lastFailoverTime.Load(), 0)
	diag.IsOnPrimary = bs.isOnPrimary()

	// Error counts
	diag.ErrSlotNotAvail = bs.stats.ErrSlotNotAvail.Load()
	diag.ErrRateLimited = bs.stats.ErrRateLimited.Load()
	diag.ErrBeyondTip = bs.stats.ErrBeyondTip.Load()
	diag.ErrHistory = bs.stats.ErrHistory.Load()
	diag.ErrTransient = bs.stats.ErrTransient.Load()
	diag.ErrOther = bs.stats.ErrOther.Load()

	// Worker pool stats
	diag.WorkersTotal = bs.maxInflight
	diag.RateLimitRPS = float64(bs.rateLimiter.Limit())

	return diag
}

// logStallDiagnostics logs comprehensive stall diagnostic information
func (bs *BlockSource) logStallDiagnostics(prefix string) {
	diag := bs.collectStallDiagnostics()

	mlog.Log.Errorf("=== %s ===", prefix)

	// 1) Waiting slot context
	mlog.Log.Errorf("Waiting slot context:")
	mlog.Log.Errorf("  waiting_slot=%d last_executed=%d confirmed_tip=%d gap=%d",
		diag.WaitingSlot, diag.LastExecutedSlot, diag.ConfirmedTip, diag.Gap)
	mlog.Log.Errorf("  mode=%s stall_elapsed=%v last_progress=%s",
		diag.Mode, diag.StallElapsed.Round(time.Second), diag.LastProgressTs.Format("15:04:05"))

	// 2) Slot state snapshot
	mlog.Log.Errorf("Slot state snapshot:")
	mlog.Log.Errorf("  inflight=%d retry_queue=%d work_queue=%d",
		diag.InflightCount, diag.RetryQueueLen, diag.WorkQueueLen)
	mlog.Log.Errorf("  reorder_buf=%d skipped_slots=%d max_buffered=%d",
		diag.ReorderBufLen, diag.SkippedSlotsLen, diag.MaxBufferedSlot)
	mlog.Log.Errorf("  waiting_slot_state=%s", diag.WaitingSlotState)

	// 3) Waiting slot error info
	if diag.WaitingSlotErrors != nil {
		e := diag.WaitingSlotErrors
		mlog.Log.Errorf("Waiting slot %d error history:", e.slot)
		mlog.Log.Errorf("  retry_count=%d first_seen=%s last_seen=%s",
			e.retryCount, e.firstSeenAt.Format("15:04:05"), e.lastSeenAt.Format("15:04:05"))
		mlog.Log.Errorf("  last_error_class=%s last_rpc_idx=%d last_latency_ms=%d",
			e.lastErrorClass, e.lastRpcIdx, e.lastLatencyMs)
		if e.lastError != "" {
			mlog.Log.Errorf("  last_error: %s", e.lastError)
		}
	} else {
		mlog.Log.Errorf("Waiting slot %d: no error history (never attempted or cleared)", diag.WaitingSlot)
	}

	// 4) RPC health snapshot
	mlog.Log.Errorf("RPC health:")
	mlog.Log.Errorf("  active_rpc_idx=%d active_url=%s is_primary=%v",
		diag.ActiveRpcIdx, diag.ActiveRpcURL, diag.IsOnPrimary)
	mlog.Log.Errorf("  failover_count=%d last_failover=%s",
		diag.FailoverCount, diag.LastFailoverTime.Format("15:04:05"))
	mlog.Log.Errorf("  errors: slot_not_avail=%d rate_limited=%d beyond_tip=%d history_unavailable=%d transient=%d other=%d",
		diag.ErrSlotNotAvail, diag.ErrRateLimited, diag.ErrBeyondTip, diag.ErrHistory, diag.ErrTransient, diag.ErrOther)

	// 5) Worker pool stats
	mlog.Log.Errorf("Worker pool: workers=%d rate_limit_rps=%.1f",
		diag.WorkersTotal, diag.RateLimitRPS)

	mlog.Log.Errorf("=== End %s ===", prefix)
}

func (bs *BlockSource) tryGetBlockFromFile(slot uint64) (*block.Block, error) {
	if bs.blockDir == "" {
		return nil, fmt.Errorf("no block directory specified")
	}
	blockFilename := filepath.Join(filepath.Clean(bs.blockDir), fmt.Sprintf("%d.json", slot))
	file, err := os.Open(blockFilename)
	if err != nil {
		return nil, fmt.Errorf("error opening blockFilename=%s: %w", blockFilename, err)
	}

	decoder := json.NewDecoder(file)
	out := &block.Block{}

	err = decoder.Decode(out)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("block decode error: %w", err)
	}
	out.FixupTxVersions()

	file.Close()
	os.Remove(blockFilename)

	return out, nil
}

// getActiveRpc returns the currently active RPC client for block fetching
func (bs *BlockSource) getActiveRpc() *rpcclient.RpcClient {
	idx := bs.activeRpcIdx.Load()
	if idx < 0 || int(idx) >= len(bs.rpcClients) {
		return bs.rpcClients[0]
	}
	return bs.rpcClients[idx]
}

// getPrimaryRpc returns the primary (first) RPC client
func (bs *BlockSource) getPrimaryRpc() *rpcclient.RpcClient {
	return bs.rpcClients[0]
}

// isOnPrimary returns true if we're using the primary RPC
func (bs *BlockSource) isOnPrimary() bool {
	return bs.activeRpcIdx.Load() == 0
}

// failoverToNext switches to the next RPC endpoint on hard connectivity errors
// (connection refused, no such host, TLS failures, network unreachable).
// Does NOT failover on timeouts, 502/503, or rate limits - those retry same endpoint.
// Only fails over if there are backup endpoints available.
// Returns true if failover occurred.
func (bs *BlockSource) failoverToNext() bool {
	if len(bs.rpcClients) <= 1 {
		return false // No backups available
	}

	currentIdx := bs.activeRpcIdx.Load()
	nextIdx := (currentIdx + 1) % int32(len(bs.rpcClients))

	// Use CompareAndSwap to avoid race conditions
	if bs.activeRpcIdx.CompareAndSwap(currentIdx, nextIdx) {
		bs.failoverCount.Add(1)
		bs.slotsSinceFailover.Store(0)
		bs.lastFailoverTime.Store(time.Now().Unix())

		currentEndpoint := bs.rpcClients[currentIdx].Endpoint()
		nextEndpoint := bs.rpcClients[nextIdx].Endpoint()

		if nextIdx == 0 {
			mlog.Log.Infof("RPC failover: restored to primary endpoint %s", nextEndpoint)
		} else {
			mlog.Log.Errorf("RPC failover: switching from %s to backup %s (failover #%d)",
				currentEndpoint, nextEndpoint, bs.failoverCount.Load())
		}
		return true
	}
	return false
}

// probePrimary does a quick health check on the primary RPC endpoint.
// Returns true if the primary responds successfully to a getSlot call.
// Uses a 5-second timeout to prevent blocking the scheduler on hanging RPCs.
func (bs *BlockSource) probePrimary() bool {
	primary := bs.getPrimaryRpc()
	_, err := primary.GetSlotWithTimeout(5 * time.Second)
	return err == nil
}

// restoreToPrimary attempts to switch back to the primary RPC after probing it.
// Returns true if we were on a backup and successfully switched back.
// The probe prevents predictable error bursts from switching to a still-down primary.
func (bs *BlockSource) restoreToPrimary() bool {
	if bs.isOnPrimary() {
		return false // Already on primary
	}

	// Probe primary before switching - avoids predictable error bursts
	if !bs.probePrimary() {
		// Primary still down, stay on backup
		return false
	}

	currentIdx := bs.activeRpcIdx.Load()
	if bs.activeRpcIdx.CompareAndSwap(currentIdx, 0) {
		bs.slotsSinceFailover.Store(0)
		bs.hardErrCount.Store(0) // Reset error count when switching back
		mlog.Log.Infof("RPC restored to primary endpoint %s (health probe succeeded)",
			bs.rpcClients[0].Endpoint())
		return true
	}
	return false
}

// fetchBlockOnce fetches a single block without internal retry loop.
// rpcIdx specifies which RPC client to use (for consistent error attribution).
func (bs *BlockSource) fetchBlockOnce(slot uint64, rpcIdx int32) (*b.Block, error) {
	// Try file first
	if blk, err := bs.tryGetBlockFromFile(slot); err == nil {
		return blk, nil
	}

	// Use the specified RPC client (not getActiveRpc - that could race with failover)
	if rpcIdx < 0 || int(rpcIdx) >= len(bs.rpcClients) {
		rpcIdx = 0
	}
	rpc := bs.rpcClients[rpcIdx]

	// Single RPC attempt (no internal retry - scheduler handles retries)
	blockResult, err := rpc.GetBlockConfirmedOnce(slot)
	if err != nil {
		return nil, err
	}

	return block.FromBlockResult(blockResult, slot, rpc), nil
}

// pollTip periodically updates the confirmed tip by querying all configured RPCs
// and using the maximum slot value. This handles RPCs that fall behind.
// Uses GetSlotWithTimeout to prevent hung RPCs from blocking the poll loop.
// Polls faster in near-tip mode for lower latency.
func (bs *BlockSource) pollTip() {
	const tipPollTimeout = 5 * time.Second

	// Get initial tip and update mode
	if tip := bs.getMaxTipFromAllRpcs(tipPollTimeout); tip > 0 {
		bs.updateTipSnapshot(tip)
		bs.tipPollFailures.Store(0)
		bs.updateMode()
	} else {
		bs.tipPollFailures.Add(1)
		bs.totalTipPollFails.Add(1)
	}

	// Start with catchup interval, will adjust based on mode
	currentInterval := bs.tipPollInterval
	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bs.stopChan:
			return
		case <-ticker.C:
			if tip := bs.getMaxTipFromAllRpcs(tipPollTimeout); tip > 0 {
				bs.updateTipSnapshot(tip)
				bs.tipPollFailures.Store(0)
				bs.updateMode() // Check if we should switch modes
			} else {
				bs.tipPollFailures.Add(1)
				bs.totalTipPollFails.Add(1)
			}

			// Adjust poll interval based on mode
			var targetInterval time.Duration
			if bs.isNearTip.Load() {
				targetInterval = bs.nearTipPollInterval
			} else {
				targetInterval = bs.tipPollInterval
			}
			if targetInterval != currentInterval {
				currentInterval = targetInterval
				ticker.Reset(currentInterval)
			}
		}
	}
}

// SetLastExecutedSlot is called by the replay loop after each block is fully executed.
// This allows accurate tip distance calculation without blocking on replay progress.
// Also triggers mode switching based on replay progress (not just tip polling).
//
// In near-tip mode, this also:
// - Schedules N+2 (prefetch while N+1 executes)
// - Immediately retries N+1 if it failed (don't wait for 200ms ticker)
// RewindForAlpenglowSwitch rewinds the emission frontier to re-serve `slot`
// after certificates named a different outcome than the executed one (wrong
// sibling or certificate-skipped). Buffered and in-flight state at or above
// the slot is dropped, live-stream results are invalidated, and the certified
// block id (zero for a skip) narrows the turbine assembler + prioritizes
// repair so the certified version arrives fast.
func (bs *BlockSource) RewindForAlpenglowSwitch(slot uint64, certified solana.Hash) {
	if slot == 0 {
		return
	}
	bs.reorderMu.Lock()
	if bs.nextSlotToSend > slot {
		bs.nextSlotToSend = slot
	}
	if bs.lastEmittedBlockSlot >= slot {
		bs.lastEmittedBlockSlot = slot - 1
	}
	for bufferedSlot := range bs.reorderBuffer {
		if bufferedSlot >= slot {
			delete(bs.reorderBuffer, bufferedSlot)
		}
	}
	for skippedSlot := range bs.skippedSlots {
		if skippedSlot >= slot {
			delete(bs.skippedSlots, skippedSlot)
			delete(bs.lightbringerSynthesizedSkips, skippedSlot)
			delete(bs.alpenglowCertifiedSkips, skippedSlot)
		}
	}
	bs.reorderMu.Unlock()

	bs.slotStateMu.Lock()
	for trackedSlot := range bs.slotState {
		if trackedSlot >= slot {
			delete(bs.slotState, trackedSlot)
			delete(bs.inflightStart, trackedSlot)
		}
	}
	bs.slotStateMu.Unlock()

	bs.retryMu.Lock()
	if len(bs.retrySlots) > 0 {
		filtered := bs.retrySlots[:0]
		for _, retrySlot := range bs.retrySlots {
			if retrySlot < slot {
				filtered = append(filtered, retrySlot)
			}
		}
		bs.retrySlots = filtered
	}
	bs.retryMu.Unlock()

	// Drop prefetched live-stream blocks for the rewound range and invalidate
	// in-flight results so stale emissions can't race the re-serve.
	bs.invalidateLightbringerResults()

	if certified != (solana.Hash{}) {
		bs.SetKnownAlpenglowBlockID(slot, certified)
		bs.resetTurbineSlotForAlpenglowBlock(slot, certified)
	}
	mlog.Log.Warnf("BLOCK SOURCE REWIND: re-serving slot %d after certificate switch (certified=%s)", slot, certified)
}

func (bs *BlockSource) SetLastExecutedSlot(slot uint64) {
	bs.lastExecutedSlot.Store(slot)

	advancedFrontier := false
	bs.reorderMu.Lock()
	if slot+1 > bs.nextSlotToSend {
		bs.nextSlotToSend = slot + 1
		advancedFrontier = true
	}
	if advancedFrontier {
		for bufferedSlot := range bs.reorderBuffer {
			if bufferedSlot <= slot {
				delete(bs.reorderBuffer, bufferedSlot)
			}
		}
		for skippedSlot := range bs.skippedSlots {
			if skippedSlot <= slot {
				delete(bs.skippedSlots, skippedSlot)
				delete(bs.lightbringerSynthesizedSkips, skippedSlot)
				delete(bs.alpenglowCertifiedSkips, skippedSlot)
			}
		}
	}
	bs.reorderMu.Unlock()

	if advancedFrontier {
		bs.slotStateMu.Lock()
		for trackedSlot := range bs.slotState {
			if trackedSlot <= slot {
				delete(bs.slotState, trackedSlot)
				delete(bs.inflightStart, trackedSlot)
			}
		}
		bs.slotStateMu.Unlock()

		bs.retryMu.Lock()
		if len(bs.retrySlots) > 0 {
			filtered := bs.retrySlots[:0]
			for _, retrySlot := range bs.retrySlots {
				if retrySlot > slot {
					filtered = append(filtered, retrySlot)
				}
			}
			bs.retrySlots = filtered
		}
		bs.retryMu.Unlock()
	}

	if forcedUntil := bs.lightbringerForceRPCUntil.Load(); forcedUntil != 0 && slot >= forcedUntil {
		if bs.lightbringerForceRPCUntil.CompareAndSwap(forcedUntil, 0) {
			if cooldownUntil := bs.lightbringerCooldownUntil.Load(); cooldownUntil != 0 && cooldownUntil > slot {
				mlog.Log.Infof("BLOCK SOURCE STATUS: missing Lightbringer slot recovered at slot %d; staying on RPC until slot %d before re-arming Lightbringer", slot, cooldownUntil)
			} else {
				mlog.Log.Infof("BLOCK SOURCE STATUS: RPC recovery for missing Lightbringer slot complete at slot %d; Lightbringer handoff may resume", slot)
			}
		}
	}
	if cooldownUntil := bs.lightbringerCooldownUntil.Load(); cooldownUntil != 0 && slot >= cooldownUntil {
		if bs.lightbringerCooldownUntil.CompareAndSwap(cooldownUntil, 0) {
			mlog.Log.Infof("BLOCK SOURCE STATUS: Lightbringer recovery window complete at slot %d; handoff may resume", slot)
		}
	}
	bs.updateMode() // React to replay progress immediately

	// Near-tip: schedule N+2 and flush N+1 retry
	if bs.isNearTip.Load() {
		nextSlot := slot + 1
		nextNextSlot := slot + 2

		// Check if N+1 needs immediate retry (pull from retry queue)
		bs.flushRetrySlot(nextSlot)

		// Schedule N+2 if not already scheduled/done
		bs.tryScheduleSlot(nextNextSlot)
	}
}

// flushRetrySlot removes a specific slot from the retry queue and schedules it immediately
func (bs *BlockSource) flushRetrySlot(slot uint64) {
	bs.retryMu.Lock()
	var remaining []uint64
	found := false
	for _, s := range bs.retrySlots {
		if s == slot {
			found = true
		} else {
			remaining = append(remaining, s)
		}
	}
	bs.retrySlots = remaining
	bs.retryMu.Unlock()

	if found {
		bs.scheduleSlot(slot)
	}
}

// tryScheduleSlot schedules a slot if not already scheduled/done/buffered
func (bs *BlockSource) tryScheduleSlot(slot uint64) {
	if !bs.shouldUseRPCForSlot(slot) {
		return
	}

	// Check slotState
	bs.slotStateMu.Lock()
	state, exists := bs.slotState[slot]
	bs.slotStateMu.Unlock()
	if exists && (state == slotInflight || state == slotDone) {
		return
	}

	// Check reorder buffer
	bs.reorderMu.Lock()
	alreadyHave := bs.reorderBuffer[slot] != nil || bs.skippedSlots[slot]
	bs.reorderMu.Unlock()
	if alreadyHave {
		return
	}

	bs.scheduleSlot(slot)
}

// NotifyBlockStart is called at the START of block execution.
// In near-tip mode, this triggers fetching N+1 so the RPC latency (~200ms)
// overlaps with execution time, hiding the wait from the user.
//
// NOTE: We can't use canScheduleMore() here because lastExecutedSlot hasn't
// been updated yet (we're about to execute, not finished). Instead, we directly
// check if the next slot is already scheduled/done and schedule it if not.
func (bs *BlockSource) NotifyBlockStart(slot uint64) {
	if !bs.isNearTip.Load() {
		return
	}

	nextSlot := slot + 1

	// Check if already scheduled/inflight/done
	bs.slotStateMu.Lock()
	state, exists := bs.slotState[nextSlot]
	bs.slotStateMu.Unlock()
	if exists && (state == slotInflight || state == slotDone) {
		return
	}

	// Check if we already have the block
	bs.reorderMu.Lock()
	alreadyHave := bs.reorderBuffer[nextSlot] != nil || bs.skippedSlots[nextSlot]
	bs.reorderMu.Unlock()
	if alreadyHave {
		return
	}

	// Schedule the next slot
	if bs.scheduleSlot(nextSlot) {
		mlog.Log.Debugf("near-tip: prefetch slot %d at start of %d", nextSlot, slot)
	}
}

// updateTipSnapshot stores the confirmed tip along with what slot was last executed.
// This allows accurate distance calculation: tip - tipAtSlot is precise at measurement time.
func (bs *BlockSource) updateTipSnapshot(confirmedTip uint64) {
	slotAtTip := bs.lastExecutedSlot.Load()

	bs.confirmedTip.Store(confirmedTip)
	bs.tipAtSlot.Store(slotAtTip)
	bs.lastTipUpdate.Store(time.Now().Unix())
}

// RefreshTipsForSummary triggers an async refresh of both confirmed and processed tips.
// Call this near summary time (e.g., at slot 95) so fresh tips are ready at slot 100.
// This is non-blocking - it spawns a goroutine to do the work.
func (bs *BlockSource) RefreshTipsForSummary() {
	go func() {
		const timeout = 5 * time.Second

		// Capture last executed slot before RPC calls (set by replay loop)
		slotAtTip := bs.lastExecutedSlot.Load()

		// Query all RPCs for both confirmed and processed tips concurrently
		type result struct {
			confirmed uint64
			processed uint64
		}
		results := make(chan result, len(bs.rpcClients))

		for _, rpc := range bs.rpcClients {
			go func(client *rpcclient.RpcClient) {
				var r result
				if tip, err := client.GetSlotWithTimeout(timeout); err == nil {
					r.confirmed = tip
				}
				if tip, err := client.GetSlotProcessedWithTimeout(timeout); err == nil {
					r.processed = tip
				}
				results <- r
			}(rpc)
		}

		// Collect results and find max for each
		var maxConfirmed, maxProcessed uint64
		for i := 0; i < len(bs.rpcClients); i++ {
			r := <-results
			if r.confirmed > maxConfirmed {
				maxConfirmed = r.confirmed
			}
			if r.processed > maxProcessed {
				maxProcessed = r.processed
			}
		}

		// Store results
		if maxConfirmed > 0 {
			bs.confirmedTip.Store(maxConfirmed)
			bs.tipAtSlot.Store(slotAtTip)
			bs.lastTipUpdate.Store(time.Now().Unix())
			bs.tipPollFailures.Store(0)
		}
		if maxProcessed > 0 {
			bs.processedTip.Store(maxProcessed)
		}
	}()
}

// getMaxTipFromAllRpcs queries all configured RPCs for the current slot (confirmed)
// and returns the maximum value. This handles RPCs that fall behind.
func (bs *BlockSource) getMaxTipFromAllRpcs(timeout time.Duration) uint64 {
	if len(bs.rpcClients) == 0 {
		return 0
	}

	// For single RPC, no need for goroutines
	if len(bs.rpcClients) == 1 {
		if tip, err := bs.rpcClients[0].GetSlotWithTimeout(timeout); err == nil {
			return tip
		}
		return 0
	}

	// Query all RPCs concurrently
	type result struct {
		tip uint64
		err error
	}
	results := make(chan result, len(bs.rpcClients))

	for _, rpc := range bs.rpcClients {
		go func(client *rpcclient.RpcClient) {
			tip, err := client.GetSlotWithTimeout(timeout)
			results <- result{tip: tip, err: err}
		}(rpc)
	}

	// Collect results and find max
	var maxTip uint64
	for i := 0; i < len(bs.rpcClients); i++ {
		r := <-results
		if r.err == nil && r.tip > maxTip {
			maxTip = r.tip
		}
	}

	return maxTip
}

// worker fetches blocks from the work queue
func (bs *BlockSource) worker(wg *sync.WaitGroup, id int) {
	defer wg.Done()

	for slot := range bs.workQueue {
		// Check for shutdown
		if bs.stopped.Load() {
			return
		}

		// Wait for rate limiter
		bs.rateLimiter.Wait(context.Background())

		// Lightbringer handoff may have become active after this slot was queued.
		// If so, skip the RPC fetch and let the stream source satisfy the slot.
		if !bs.shouldUseRPCForSlot(slot) {
			bs.slotStateMu.Lock()
			delete(bs.slotState, slot)
			delete(bs.inflightStart, slot)
			bs.slotStateMu.Unlock()
			continue
		}

		// Tip safety gate strategy:
		// - Near-tip mode: No gate - rely on RPC "slot not available" + 200ms retries
		// - Catchup mode with large gap: Apply tip safety margin to avoid "slot not available"
		// - Catchup mode with small gap: No gate - avoid bt storms when headroom is tight
		//
		// The key insight is that when gap is ~70 with margin=64, headroom is only ~6 slots,
		// which causes bt storms (scheduler tries to go past, workers reject, massive retries).
		// Solution: only apply margin when gap > tipGateThreshold (plenty of headroom).
		if !bs.isNearTip.Load() {
			tip := bs.confirmedTip.Load()
			lastExec := bs.lastExecutedSlot.Load()

			// Calculate gap
			var gap uint64
			if tip > lastExec {
				gap = tip - lastExec
			}

			// Only apply tip gate if gap > tipGateThreshold (true catchup with plenty of headroom)
			// When gap <= threshold, we're close enough that the gate does more harm than good
			if gap > bs.tipGateThreshold {
				margin := bs.effectiveTipSafetyMargin()
				var maxSlot uint64
				if tip <= margin {
					maxSlot = 0
				} else {
					maxSlot = tip - margin
				}

				// Beyond tip - send back for retry
				if maxSlot > 0 && slot > maxSlot {
					bs.stats.ErrBeyondTip.Add(1)
					bs.resultQueue <- fetchResult{slot: slot, err: errBeyondTip, rpcIdx: -1}
					continue
				}
			}
			// If gap <= 128: no tip gate, just try to fetch and handle RPC errors naturally
		}

		// Capture which RPC we're using BEFORE the fetch (for error attribution)
		// Pass this to fetchBlockOnce so it uses the same client we're attributing to
		rpcIdx := bs.activeRpcIdx.Load()

		// Fetch block with latency tracking
		bs.stats.FetchAttempts.Add(1)
		fetchStart := time.Now()
		blk, err := bs.fetchBlockOnce(slot, rpcIdx)
		fetchLatency := time.Since(fetchStart)

		// Track latency for successful fetches
		if err == nil {
			bs.stats.TotalFetchLatencyNs.Add(uint64(fetchLatency.Nanoseconds()))
			bs.stats.FetchLatencyCount.Add(1)
		}

		skipped := err == rpcclient.SlotSkipped
		bs.resultQueue <- fetchResult{
			slot:      slot,
			block:     blk,
			err:       err,
			skipped:   skipped,
			rpcIdx:    rpcIdx,
			latencyMs: fetchLatency.Milliseconds(),
		}
	}
}

// emitOrderedBlocks receives results and emits blocks in order
func (bs *BlockSource) emitOrderedBlocks() {
	for result := range bs.resultQueue {
		if result.block != nil {
			bs.observeAlpenglowCandidateBlock(result.block)
		}

		bs.reorderMu.Lock()
		var gapWaitingSlot uint64
		var gapFirstBufferedSlot uint64
		var gapFirstBufferedParentSlot uint64
		var gapBufferedCount int
		var shouldFallbackToRPC bool

		if result.slot < bs.nextSlotToSend {
			bs.slotStateMu.Lock()
			delete(bs.slotState, result.slot)
			delete(bs.inflightStart, result.slot)
			bs.slotStateMu.Unlock()
			bs.clearSlotErrors(result.slot)
			bs.reorderMu.Unlock()
			continue
		}

		if result.block != nil && result.block.FromLightbringer {
			if bs.shouldDiscardLiveStreamResult(result.slot, result.liveStreamGeneration) {
				bs.reorderMu.Unlock()
				continue
			}
			handoffSlot := bs.lightbringerHandoffSlot.Load()
			if handoffSlot != 0 && result.slot >= handoffSlot {
				if existing := bs.reorderBuffer[result.slot]; existing != nil && !existing.FromLightbringer {
					delete(bs.reorderBuffer, result.slot)
				}
				if bs.skippedSlots[result.slot] {
					delete(bs.skippedSlots, result.slot)
					delete(bs.lightbringerSynthesizedSkips, result.slot)
					delete(bs.alpenglowCertifiedSkips, result.slot)
				}
			}
			if existing := bs.reorderBuffer[result.slot]; bs.shouldPreferIncomingLightbringerBlockLocked(existing, result.block) {
				delete(bs.reorderBuffer, result.slot)
			}
		}

		// Check if this is a duplicate result (from backup request)
		// If slot is already done or we already have the block, discard this result
		bs.slotStateMu.Lock()
		isDuplicate := bs.slotState[result.slot] == slotDone
		bs.slotStateMu.Unlock()
		if isDuplicate || bs.reorderBuffer[result.slot] != nil || bs.skippedSlots[result.slot] {
			bs.reorderMu.Unlock()
			continue
		}

		if bs.shouldDiscardRPCResultAfterHandoff(result.slot, result.block) {
			bs.slotStateMu.Lock()
			delete(bs.slotState, result.slot)
			delete(bs.inflightStart, result.slot)
			bs.slotStateMu.Unlock()
			bs.clearSlotErrors(result.slot)
			bs.reorderMu.Unlock()
			continue
		}

		if result.block != nil && result.block.FromLightbringer {
			handoffSlot := bs.lightbringerHandoffSlot.Load()
			if !bs.isNearTip.Load() {
				bs.reorderMu.Unlock()
				continue
			}
			if !bs.lightbringerActive.Load() && (handoffSlot == 0 || result.slot < handoffSlot) {
				bs.reorderMu.Unlock()
				continue
			}
		}

		// Track error buckets
		// Note: isHardConnectivityErr is checked independently because hard errors
		// (connection refused, no such host) are NOT in isTransientNetworkErr anymore
		isHardErr := false
		isHistoryErr := false
		if result.err != nil {
			if result.err == errBeyondTip {
				// Already tracked in worker
			} else if isSlotNotAvailableErr(result.err) {
				bs.stats.ErrSlotNotAvail.Add(1)
			} else if isRateLimitedErr(result.err) {
				bs.stats.ErrRateLimited.Add(1)
			} else if isHistoryUnavailableErr(result.err) {
				bs.stats.ErrHistory.Add(1)
				isHistoryErr = true
			} else if isHardConnectivityErr(result.err) {
				// Hard connectivity errors: endpoint is down (connection refused, no such host, etc.)
				// These count as transient for stats but also trigger failover logic
				bs.stats.ErrTransient.Add(1)
				isHardErr = true
			} else if isTransientNetworkErr(result.err) {
				// Soft transient errors: retry same endpoint (502/503, timeouts, EOF)
				bs.stats.ErrTransient.Add(1)
			} else if result.err != rpcclient.SlotSkipped {
				bs.stats.ErrOther.Add(1)
			}
		}

		// RPC failover: a node that returns -32011 cannot serve getBlock history.
		// That endpoint may still serve snapshots and getSlot, but catchup needs
		// transaction history for every slot between the snapshot and live turbine.
		if isHistoryErr && len(bs.rpcClients) > 1 && result.rpcIdx == bs.activeRpcIdx.Load() {
			if result.rpcIdx >= 0 && int(result.rpcIdx) < len(bs.rpcClients) {
				mlog.Log.Errorf("RPC endpoint %s cannot serve getBlock transaction history; trying backup RPC",
					bs.rpcClients[result.rpcIdx].Endpoint())
			}
			bs.failoverToNext()
			bs.hardErrCount.Store(0)
		}

		// RPC failover: only after repeated HARD connectivity errors
		// - Only count errors from the currently active RPC (ignore late errors from old endpoint)
		// - Time-windowed: reset count if no errors for 5 seconds
		// - Threshold-based: require 10 consecutive errors before failover
		if isHardErr && len(bs.rpcClients) > 1 && result.rpcIdx == bs.activeRpcIdx.Load() {
			now := time.Now().Unix()
			lastErr := bs.lastHardErrTime.Load()

			// Time-window: reset if no errors for failoverErrWindowSecs
			if lastErr > 0 && (now-lastErr) > failoverErrWindowSecs {
				bs.hardErrCount.Store(0)
			}
			bs.lastHardErrTime.Store(now)

			errCount := bs.hardErrCount.Add(1)
			if errCount >= failoverErrThreshold {
				bs.failoverToNext()
				bs.hardErrCount.Store(0) // Reset after failover
			}
		}

		// Handle result
		// CRITICAL: All errors except SlotSkipped are retriable.
		isRetriable := result.err != nil && !result.skipped

		if result.err != nil {
			bs.trackSlotError(result.slot, result.err, result.rpcIdx, result.latencyMs)
		}

		if result.skipped {
			bs.stats.FetchSkipped.Add(1)
			bs.skippedSlots[result.slot] = true
			delete(bs.lightbringerSynthesizedSkips, result.slot)
			delete(bs.alpenglowCertifiedSkips, result.slot)
			bs.hardErrCount.Store(0)        // Reset on progress
			bs.clearSlotErrors(result.slot) // Clear stall diagnostics for this slot
			isRetriable = false
		} else if result.block != nil {
			// Success! This takes priority over any pending error results.
			if !result.block.FromLightbringer {
				bs.stats.FetchSuccesses.Add(1)
			}
			bs.reorderBuffer[result.slot] = result.block
			bs.hardErrCount.Store(0)        // Reset error count on success
			bs.clearSlotErrors(result.slot) // Clear stall diagnostics for this slot
			// Track max buffered slot
			if result.slot > bs.stats.MaxBufferedSlot.Load() {
				bs.stats.MaxBufferedSlot.Store(result.slot)
			}
			// Local tip proxy: confirmed tip >= any successfully fetched slot (monotonic)
			// This lets us advance without waiting for the next pollTip()
			if result.slot > bs.confirmedTip.Load() {
				bs.confirmedTip.Store(result.slot)
				bs.tipAtSlot.Store(bs.lastExecutedSlot.Load())
				bs.lastTipUpdate.Store(time.Now().Unix())
			}
		} else if isRetriable {
			// All non-skip errors are retriable - schedule retry
			bs.stats.FetchRetries.Add(1)
			// CRITICAL: Must delete from slotState BEFORE adding to retry queue!
			// Otherwise race condition: scheduler picks up from retry queue while
			// slot is still marked slotInflight, scheduleSlot fails, slot is lost.
			bs.slotStateMu.Lock()
			delete(bs.slotState, result.slot)
			delete(bs.inflightStart, result.slot)
			bs.slotStateMu.Unlock()
			if bs.shouldUseRPCForSlot(result.slot) {
				bs.scheduleRetry(result.slot)
			}
		}
		// Note: if result.block == nil && result.err == nil && !result.skipped,
		// that's a bug in the worker, but we don't skip - it will stall and be detected.

		// Mark slot done (for non-retriable cases)
		if !isRetriable {
			bs.slotStateMu.Lock()
			bs.slotState[result.slot] = slotDone
			delete(bs.inflightStart, result.slot) // Clean up timing data
			bs.slotStateMu.Unlock()
		}

		// Emit consecutive blocks
		for {
			if bs.applyAlpenglowDecisionLocked() {
				continue
			}

			if waitingSlot, observedParentSlot, expectedParentSlot, mismatch := bs.waitingLightbringerParentMismatchLocked(); mismatch {
				bs.reorderMu.Unlock()
				bs.forceRPCForLightbringerParentMismatch(waitingSlot, observedParentSlot, expectedParentSlot)
				bs.reorderMu.Lock()
				continue
			}

			if blk, ok := bs.reorderBuffer[bs.nextSlotToSend]; ok {
				if bs.shouldDiscardRPCResultAfterHandoff(blk.Slot, blk) {
					delete(bs.reorderBuffer, bs.nextSlotToSend)
					continue
				}

				delete(bs.reorderBuffer, bs.nextSlotToSend)
				bs.lastEmittedBlockSlot = blk.Slot
				bs.reorderMu.Unlock()

				repairingSlot := bs.isLightbringerRepairSlot(blk.Slot)
				if bs.usesLiveShredStream() {
					if blk.FromLightbringer {
						if repairingSlot {
							bs.clearLightbringerRepairSlot(blk.Slot)
						}
						if !bs.lightbringerActive.Swap(true) {
							mlog.Log.Infof("BLOCK SOURCE SWITCH: RPC -> %s at slot %d | mode=%s", bs.liveShredStreamName(), blk.Slot, bs.currentModeString())
						}
					} else if repairingSlot {
						bs.clearLightbringerRepairSlot(blk.Slot)
						mlog.Log.Infof("BLOCK SOURCE STATUS: missing streamed slot recovered via RPC at slot %d; staying on %s stream", blk.Slot, bs.liveShredStreamName())
					} else if bs.lightbringerActive.Swap(false) {
						mlog.Log.Warnf("BLOCK SOURCE SWITCH: %s -> RPC at slot %d | mode=%s", bs.liveShredStreamName(), blk.Slot, bs.currentModeString())
					}
				}

				bs.streamChan <- blk
				// Update progress timestamp for stall detection
				bs.lastProgress.Store(time.Now().Unix())

				// Track slots since failover for primary retry
				if !bs.isOnPrimary() {
					slots := bs.slotsSinceFailover.Add(1)
					// Every primaryRetryInterval slots, try the primary again
					if slots%primaryRetryInterval == 0 {
						bs.restoreToPrimary()
						// If primary fails, failover will trigger again on next transient error
					}
				}

				bs.reorderMu.Lock()
				bs.nextSlotToSend++
			} else if bs.skippedSlots[bs.nextSlotToSend] {
				if bs.shouldDiscardSkippedSlotAfterHandoff(bs.nextSlotToSend) {
					delete(bs.skippedSlots, bs.nextSlotToSend)
					delete(bs.lightbringerSynthesizedSkips, bs.nextSlotToSend)
					delete(bs.alpenglowCertifiedSkips, bs.nextSlotToSend)
					continue
				}

				// Slot was skipped. Preserve whether this skip came from the live
				// stream/Alpenglow side so fallback can discard stale queued markers.
				skippedSlot := bs.nextSlotToSend
				liveStreamSkip := bs.lightbringerSynthesizedSkips[skippedSlot] || bs.alpenglowCertifiedSkips[skippedSlot]
				delete(bs.skippedSlots, skippedSlot)
				delete(bs.lightbringerSynthesizedSkips, skippedSlot)
				delete(bs.alpenglowCertifiedSkips, skippedSlot)
				bs.nextSlotToSend++
				bs.reorderMu.Unlock()

				// Emit a minimal block with IsSkipped=true for logging
				skipBlock := &b.Block{
					Slot:             skippedSlot,
					IsSkipped:        true,
					FromLightbringer: liveStreamSkip,
				}

				if bs.usesLiveShredStream() {
					if bs.isLightbringerRepairSlot(skippedSlot) {
						bs.clearLightbringerRepairSlot(skippedSlot)
						mlog.Log.Infof("BLOCK SOURCE STATUS: missing streamed slot %d was confirmed skipped via RPC; staying on %s stream", skippedSlot, bs.liveShredStreamName())
					}
				}
				bs.streamChan <- skipBlock

				// Update progress - skipped slots count as progress
				bs.lastProgress.Store(time.Now().Unix())

				// Track slots since failover for primary retry (skipped slots count too)
				if !bs.isOnPrimary() {
					slots := bs.slotsSinceFailover.Add(1)
					if slots%primaryRetryInterval == 0 {
						bs.restoreToPrimary()
					}
				}

				bs.reorderMu.Lock()
			} else {
				break
			}
		}

		gapWaitingSlot, gapFirstBufferedSlot, gapFirstBufferedParentSlot, gapBufferedCount, shouldFallbackToRPC = bs.detectLightbringerGapLocked()
		bs.maybeLogReorderGapLocked()
		bs.reorderMu.Unlock()
		if gapWaitingSlot != 0 {
			bs.prioritizeTurbineRepairForLiveGap(gapWaitingSlot, gapFirstBufferedParentSlot)
		}
		if shouldFallbackToRPC {
			bs.handleDetectedLightbringerGap(gapWaitingSlot, gapFirstBufferedSlot, gapFirstBufferedParentSlot, gapBufferedCount)
		}
	}
}

// scheduleRetry adds a slot to the retry queue
func (bs *BlockSource) scheduleRetry(slot uint64) {
	bs.retryMu.Lock()
	bs.retrySlots = append(bs.retrySlots, slot)
	bs.retryMu.Unlock()
}

// getRetrySlots returns and clears the retry queue
func (bs *BlockSource) getRetrySlots() []uint64 {
	bs.retryMu.Lock()
	slots := bs.retrySlots
	bs.retrySlots = nil
	bs.retryMu.Unlock()
	return slots
}

// canScheduleMore returns true if we can schedule the given slot.
// In catchup mode: gate on buffer size (up to defaultMaxPending).
// In near-tip mode: allow scheduling a small lookahead (bs.nearTipLookahead slots)
// to hide RPC latency behind execution time.
func (bs *BlockSource) canScheduleMore(slot uint64) bool {
	if !bs.shouldUseRPCForSlot(slot) {
		return false
	}
	if bs.slotBeforeEmissionFrontier(slot) {
		return false
	}

	if bs.isNearTip.Load() {
		// Near-tip mode: allow scheduling up to nearTipLookahead slots ahead
		// This provides enough buffer to hide RPC latency (~300ms) behind execution (~100ms)
		//
		// CRITICAL: Use nextSlotToSend (not lastExecutedSlot) because skipped slots
		// advance nextSlotToSend but don't advance lastExecutedSlot. If we used
		// lastExecutedSlot, consecutive skipped slots would block scheduling and stall.
		bs.reorderMu.Lock()
		nextToSend := bs.nextSlotToSend
		alreadyHave := bs.reorderBuffer[slot] != nil || bs.skippedSlots[slot]
		bs.reorderMu.Unlock()

		if alreadyHave {
			return false
		}

		// Allow scheduling up to nearTipLookahead slots ahead of what we're waiting to emit
		if nextToSend > 0 && slot > nextToSend+bs.nearTipLookahead {
			return false
		}

		// Check slotState - don't schedule if already inflight/done
		bs.slotStateMu.Lock()
		state, exists := bs.slotState[slot]
		bs.slotStateMu.Unlock()
		if exists && (state == slotInflight || state == slotDone) {
			return false
		}

		return true
	}

	// Catchup mode: gate on buffer capacity
	bs.reorderMu.Lock()
	pending := len(bs.reorderBuffer)
	bs.reorderMu.Unlock()
	return pending < defaultMaxPending
}

func (bs *BlockSource) slotBeforeEmissionFrontier(slot uint64) bool {
	bs.reorderMu.Lock()
	nextToSend := bs.nextSlotToSend
	bs.reorderMu.Unlock()
	return nextToSend != 0 && slot < nextToSend
}

// scheduleSlot schedules a slot if not already scheduled
func (bs *BlockSource) scheduleSlot(slot uint64) bool {
	if !bs.shouldUseRPCForSlot(slot) {
		return false
	}
	if bs.slotBeforeEmissionFrontier(slot) {
		return false
	}

	bs.slotStateMu.Lock()
	if _, exists := bs.slotState[slot]; exists {
		bs.slotStateMu.Unlock()
		return false // Already scheduled
	}
	bs.slotState[slot] = slotInflight
	bs.inflightStart[slot] = time.Now()
	bs.slotStateMu.Unlock()

	select {
	case bs.workQueue <- slot:
		return true
	case <-bs.stopChan:
		return false
	}
}

// scheduleBackupRequest sends a backup request for a slow slot (bypasses slotState check)
// Returns true only if the request was actually queued.
func (bs *BlockSource) scheduleBackupRequest(slot uint64) bool {
	if !bs.shouldUseRPCForSlot(slot) {
		return false
	}
	if bs.slotBeforeEmissionFrontier(slot) {
		return false
	}

	select {
	case bs.workQueue <- slot:
		bs.stats.SpeculativeRetries.Add(1) // Only count if actually queued
		return true
	case <-bs.stopChan:
		return false
	default:
		// Work queue full, skip backup request
		return false
	}
}

// getStaleSlots returns slots that have been inflight for longer than threshold
func (bs *BlockSource) getStaleSlots(threshold time.Duration) []uint64 {
	bs.slotStateMu.Lock()
	defer bs.slotStateMu.Unlock()

	now := time.Now()
	var stale []uint64
	for slot, status := range bs.slotState {
		if status == slotInflight {
			if start, ok := bs.inflightStart[slot]; ok {
				if now.Sub(start) > threshold {
					stale = append(stale, slot)
				}
			}
		}
	}
	return stale
}

// rescueStaleWaitingSlot clears and retries the currently waiting slot if it has
// remained inflight for too long without producing any result. This prevents a
// lost or hung fetch from pinning ordered emission forever.
func (bs *BlockSource) rescueStaleWaitingSlot(slot uint64, threshold time.Duration) bool {
	bs.slotStateMu.Lock()
	state, exists := bs.slotState[slot]
	start, hasStart := bs.inflightStart[slot]
	if !exists || state != slotInflight || !hasStart || time.Since(start) < threshold {
		bs.slotStateMu.Unlock()
		return false
	}
	delete(bs.slotState, slot)
	delete(bs.inflightStart, slot)
	bs.slotStateMu.Unlock()

	bs.waitingSlotErrorsMu.Lock()
	info := bs.waitingSlotErrors[slot]
	bs.waitingSlotErrorsMu.Unlock()
	bs.scheduleRetry(slot)
	if info != nil {
		mlog.Log.Warnf("rescuing stale inflight waiting slot %d after %v without a result; scheduling fresh retry | last_err_class=%s | last_err=%s | retries=%d",
			slot, time.Since(start).Round(time.Second), info.lastErrorClass, info.lastError, info.retryCount)
	} else {
		mlog.Log.Warnf("rescuing stale inflight waiting slot %d after %v without a result; scheduling fresh retry | no prior RPC error history",
			slot, time.Since(start).Round(time.Second))
	}
	return true
}

// scheduler feeds slots to the work queue
func (bs *BlockSource) scheduler() {
	nextToSchedule := bs.startSlot

	retryTicker := time.NewTicker(200 * time.Millisecond)
	defer retryTicker.Stop()

	// Time-based primary probe ticker (independent of progress)
	primaryProbeTicker := time.NewTicker(primaryProbeInterval)
	defer primaryProbeTicker.Stop()

	// Track which slots we've already sent backup requests for
	backupSent := make(map[uint64]bool)

	for {
		// Check for shutdown
		if bs.stopped.Load() {
			return
		}

		// Check for stall timeout - if no progress for too long, trigger graceful shutdown
		lastProgressTime := time.Unix(bs.lastProgress.Load(), 0)
		stallDuration := time.Since(lastProgressTime)

		bs.maybeReconnectActiveLightbringerForNoProgress(stallDuration)

		// Periodic heartbeat logging when stall exceeds 2 minutes (but before shutdown)
		if stallDuration > stallHeartbeatThreshold && stallDuration <= bs.stallTimeout {
			lastHeartbeat := time.Unix(bs.lastStallHeartbeat.Load(), 0)
			if time.Since(lastHeartbeat) >= stallHeartbeatInterval {
				bs.lastStallHeartbeat.Store(time.Now().Unix())
				bs.logStallDiagnostics(fmt.Sprintf("STALL HEARTBEAT (stall=%v, timeout=%v)", stallDuration.Round(time.Second), bs.stallTimeout))
			}
		}

		if stallDuration > bs.stallTimeout {
			bs.reorderMu.Lock()
			waitingSlot := bs.nextSlotToSend
			bs.reorderMu.Unlock()

			// Last-chance probe: if we're on a backup, try primary before giving up
			// This handles the case where backup can't serve getBlock but primary recovered
			if !bs.isOnPrimary() && len(bs.rpcClients) > 1 {
				mlog.Log.Infof("Block fetch stalled on backup endpoint - probing primary before shutdown...")
				if bs.restoreToPrimary() {
					mlog.Log.Infof("Primary RPC restored, resetting stall timer and continuing")
					bs.lastProgress.Store(time.Now().Unix())
					continue // Give primary a chance
				}
				mlog.Log.Infof("Primary RPC still unavailable, proceeding with shutdown")
			}

			// Final stall diagnostics dump before shutdown
			bs.logStallDiagnostics("FINAL STALL DIAGNOSTICS (before shutdown)")

			mlog.Log.Errorf("FATAL: Block fetch stalled for %v - no progress since slot %d",
				bs.stallTimeout, waitingSlot)
			mlog.Log.Errorf("This indicates persistent network issues or RPC unavailability.")
			mlog.Log.Errorf("Triggering graceful shutdown to preserve AccountsDB state.")

			bs.setStopReason(blockSourceStopReasonStalled, waitingSlot)
			bs.stallError.Store(true)
			return // Exit scheduler, which triggers shutdown
		}

		// Check if all slots are done
		bs.reorderMu.Lock()
		waitingSlot := bs.nextSlotToSend
		allDone := waitingSlot >= bs.endSlot
		bs.reorderMu.Unlock()

		if allDone {
			if bs.endSlot == uint64(math.MaxUint64) {
				bs.setStopReason(blockSourceStopReasonUnexpectedLiveEnd, waitingSlot)
				mlog.Log.Errorf("FATAL: block source reached terminal completion in live mode at slot %d (start=%d end=%d)",
					waitingSlot, bs.startSlot, bs.endSlot)
				return
			}
			bs.setStopReason(blockSourceStopReasonCompleted, waitingSlot)
			return
		}

		if bs.lightbringerNeedRPCResume.CompareAndSwap(true, false) {
			nextToSchedule = waitingSlot
		}

		// Keep the scheduler aligned with actual replay progress so RPC fallback
		// resumes from the current gap if Lightbringer disconnects.
		if nextToSchedule < waitingSlot {
			nextToSchedule = waitingSlot
		}

		// Process retry slots, backup requests, and primary probes on tickers
		select {
		case <-primaryProbeTicker.C:
			// Time-based primary probe - try to restore to primary every minute when on backup
			// This runs independently of block progress, so we'll try primary even if stalled
			if !bs.isOnPrimary() && len(bs.rpcClients) > 1 {
				if bs.restoreToPrimary() {
					// Primary restored! Reset stall timer since we have a fresh endpoint to try
					bs.lastProgress.Store(time.Now().Unix())
				}
			}
		case <-retryTicker.C:
			// Handle normal retries
			// CRITICAL: Get the slot we're waiting for FIRST - this slot must always
			// be allowed to schedule, even if buffer is full. Otherwise we deadlock:
			// buffer fills with N+1..N+100, slot N keeps failing, can't reschedule N
			// because buffer is full, buffer can't drain because waiting for N.
			bs.reorderMu.Lock()
			waitingSlot = bs.nextSlotToSend
			bs.reorderMu.Unlock()

			for _, slot := range bs.getRetrySlots() {
				if waitingSlot != 0 && slot < waitingSlot {
					continue
				}
				if !bs.shouldUseRPCForSlot(slot) {
					continue
				}

				// Always allow scheduling the slot we're waiting for (breaks deadlock)
				isPrioritySlot := slot == waitingSlot
				if isPrioritySlot || bs.canScheduleMore(slot) {
					if isPrioritySlot && !bs.canScheduleMore(slot) {
						// Log when deadlock prevention kicks in (Debugf to avoid noise)
						bs.reorderMu.Lock()
						bufLen := len(bs.reorderBuffer)
						bs.reorderMu.Unlock()
						if bs.isNearTip.Load() {
							mlog.Log.Debugf("priority retry: scheduling waiting slot %d (near-tip lookahead, buf=%d)", slot, bufLen)
						} else {
							mlog.Log.Debugf("priority retry: scheduling waiting slot %d (buffer full, buf=%d)", slot, bufLen)
						}
					}
					// Check return value - if scheduleSlot fails, put back in retry queue.
					// This makes the retry path lossless even if scheduleSlot fails for
					// other reasons (work queue full, stopChan closed, etc.)
					if !bs.scheduleSlot(slot) {
						if bs.shouldUseRPCForSlot(slot) {
							bs.scheduleRetry(slot)
						}
					}
				} else {
					bs.scheduleRetry(slot) // Put back
				}
			}

			// Check for priority slot blocked condition (rate-limited to once per 2s)
			// This detects when the waiting slot keeps getting retried but isn't making progress
			bs.waitingSlotErrorsMu.Lock()
			waitingSlotInfo := bs.waitingSlotErrors[waitingSlot]
			bs.waitingSlotErrorsMu.Unlock()

			if waitingSlotInfo != nil && waitingSlotInfo.retryCount >= 3 {
				bs.slotStateMu.Lock()
				state, exists := bs.slotState[waitingSlot]
				isInflight := exists && state == slotInflight
				bs.slotStateMu.Unlock()

				// Only log if slot is not currently inflight (stuck in retry cycle)
				if !isInflight {
					lastLog := time.Unix(bs.lastPriorityBlockedLog.Load(), 0)
					if time.Since(lastLog) >= 2*time.Second {
						bs.lastPriorityBlockedLog.Store(time.Now().Unix())
						modeStr := "catchup"
						if bs.isNearTip.Load() {
							modeStr = "near-tip"
						}
						mlog.Log.Warnf("priority slot blocked: slot %d retried %d times but not inflight | mode: %s | last_err: %s",
							waitingSlot, waitingSlotInfo.retryCount, modeStr, waitingSlotInfo.lastErrorClass)
					}
				}
			}

			// Send backup requests for stale slots (>1 second old)
			for _, slot := range bs.getStaleSlots(1 * time.Second) {
				if !backupSent[slot] {
					if bs.scheduleBackupRequest(slot) {
						backupSent[slot] = true // Only mark if actually queued
					}
				}
			}

			// Clean up backupSent for completed or retried slots
			// Clear when slot is done OR no longer inflight (was retried/reset)
			bs.slotStateMu.Lock()
			for slot := range backupSent {
				state, exists := bs.slotState[slot]
				if state == slotDone || !exists || state != slotInflight {
					delete(backupSent, slot)
				}
			}
			bs.slotStateMu.Unlock()
		default:
		}

		if nextToSchedule >= bs.endSlot {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if !bs.shouldUseRPCForSlot(nextToSchedule) {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// Schedule new slots if we have capacity
		if bs.canScheduleMore(nextToSchedule) {
			if bs.scheduleSlot(nextToSchedule) {
				nextToSchedule++
			} else {
				// Channel full or stopped, wait a bit
				time.Sleep(10 * time.Millisecond)
			}
		} else {
			// At capacity, wait a bit
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// DownloadInitialBlocks is kept for backward compatibility but is a no-op
// The parallel scheduler handles initial prefetch naturally
func (bs *BlockSource) DownloadInitialBlocks() {
	// No-op - parallel scheduler handles this
}

// Start begins parallel block fetching
func (bs *BlockSource) Start() {
	// File-backed blocks still use the old sequential approach.
	// Lightbringer uses the parallel RPC fetcher for catchup plus a live stream handoff.
	if bs.sourceType == BlockSourceFile {
		bs.startSequential()
		return
	}

	if bs.usesLiveShredStream() {
		bs.maybeStartLightbringerStream()
	}

	// Start tip poller
	go bs.pollTip()

	// Cert-driven repair: steer turbine toward certified-but-unobserved blocks.
	if bs.sourceType == BlockSourceTurbine && bs.turbineAlpenglowBlockIDHints && bs.alpenglowWantedBlocksFn != nil {
		go bs.alpenglowRepairLoop()
	}

	// Wait for initial tip
	time.Sleep(100 * time.Millisecond)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < bs.maxInflight; i++ {
		wg.Add(1)
		go bs.worker(&wg, i)
	}

	// Start emitter
	emitterDone := make(chan struct{})
	go func() {
		bs.emitOrderedBlocks()
		close(emitterDone)
	}()

	// Start scheduler (runs until all slots done)
	bs.scheduler()

	if bs.stopReasonEnum() == blockSourceStopReasonNone {
		bs.reorderMu.Lock()
		waitingSlot := bs.nextSlotToSend
		bs.reorderMu.Unlock()
		bs.setStopReason(blockSourceStopReasonUnexpectedLiveEnd, waitingSlot)
	}

	// Shutdown
	bs.stopped.Store(true)
	close(bs.stopChan)
	close(bs.workQueue)
	wg.Wait()
	bs.lightbringerWg.Wait()
	close(bs.resultQueue)
	<-emitterDone
	close(bs.streamChan)
}

// startSequential is the old sequential approach for non-RPC sources
func (bs *BlockSource) startSequential() {
	for ; bs.currentSlot < bs.endSlot; bs.currentSlot++ {
		newBlock, _ := bs.fetchAndParseBlockSequential(bs.currentSlot)
		if newBlock != nil {
			bs.streamChan <- newBlock
		}
	}
	close(bs.streamChan)
}

// fetchAndParseBlockSequential is the old sequential fetch with retry
func (bs *BlockSource) fetchAndParseBlockSequential(slot uint64) (*b.Block, error) {
	var err error
	var blockResult *rpc.GetBlockResult
	var blk *b.Block

	if bs.sourceType == BlockSourceFile {
		blk, err = bs.tryGetBlockFromFile(slot)
		if err != nil {
			rpc := bs.getActiveRpc()
			for {
				// Use single-attempt fetch to avoid inner retry loop bypassing rate limits
				blockResult, err = rpc.GetBlockConfirmedOnce(uint64(slot))
				if err == nil {
					break
				} else if err == rpcclient.SlotSkipped {
					return nil, err
				} else if isSlotNotAvailableErr(err) {
					time.Sleep(500 * time.Millisecond)
				} else if isRateLimitedErr(err) {
					time.Sleep(2 * time.Second)
				} else if isHardConnectivityErr(err) || isTransientNetworkErr(err) {
					time.Sleep(1 * time.Second)
				} else {
					return nil, fmt.Errorf("error fetching block: %w", err)
				}
			}
			blk = block.FromBlockResult(blockResult, slot, rpc)
		}
	} else if bs.sourceType == BlockSourceLightbringer || bs.sourceType == BlockSourceTurbine {
		// Legacy sequential mode does not support the live stream handoff.
		// If this path is ever used, fall back to the RPC catchup fetcher.
		rpc := bs.getActiveRpc()
		for {
			blockResult, err = rpc.GetBlockConfirmedOnce(slot)
			if err == nil {
				break
			} else if err == rpcclient.SlotSkipped {
				return nil, err
			} else if isSlotNotAvailableErr(err) {
				time.Sleep(500 * time.Millisecond)
			} else if isRateLimitedErr(err) {
				time.Sleep(2 * time.Second)
			} else if isHardConnectivityErr(err) || isTransientNetworkErr(err) {
				time.Sleep(1 * time.Second)
			} else {
				return nil, fmt.Errorf("error fetching block: %w", err)
			}
		}
		blk = block.FromBlockResult(blockResult, slot, rpc)
	}

	return blk, nil
}

func (bs *BlockSource) NextBlock() *b.Block {
	block := <-bs.streamChan
	return block
}

func (bs *BlockSource) BufferDepth() int {
	return len(bs.streamChan)
}

// Stalled returns true if the block source stalled due to persistent fetch failures.
// When true, the caller should trigger a graceful shutdown to preserve AccountsDB state.
func (bs *BlockSource) Stalled() bool {
	return bs.stallError.Load()
}

// StallTimeout returns the configured stall timeout duration.
func (bs *BlockSource) StallTimeout() time.Duration {
	return bs.stallTimeout
}

// Completed returns true when the block source reached a finite configured end slot.
func (bs *BlockSource) Completed() bool {
	return bs.stopReasonEnum() == blockSourceStopReasonCompleted
}

// StopReason returns a human-readable explanation for why the block source ended.
func (bs *BlockSource) StopReason() string {
	stopSlot := bs.stopSlot.Load()
	endSlot := bs.stopEndSlot.Load()

	switch bs.stopReasonEnum() {
	case blockSourceStopReasonCompleted:
		return fmt.Sprintf("completed finite replay at slot %d (endSlot=%d)", stopSlot, endSlot)
	case blockSourceStopReasonStalled:
		return fmt.Sprintf("block fetch stalled while waiting for slot %d", stopSlot)
	case blockSourceStopReasonUnexpectedLiveEnd:
		return fmt.Sprintf("scheduler terminated unexpectedly in live mode at slot %d (endSlot=%d)", stopSlot, endSlot)
	case blockSourceStopReasonAlpenglowConflict:
		return fmt.Sprintf("halted on Alpenglow consensus conflict (equivocation) at slot %d", stopSlot)
	default:
		return "stream closed without an explicit block-source stop reason"
	}
}

// FetchStatsSnapshot returns a snapshot of fetch stats for logging
type FetchStatsSnapshot struct {
	Attempts           uint64
	Successes          uint64
	Retries            uint64
	Skipped            uint64
	SpeculativeRetries uint64 // Backup requests sent for slow slots
	AvgLatencyMs       float64
	ErrNotAvail        uint64
	ErrRateLimit       uint64
	ErrBeyondTip       uint64
	ErrHistory         uint64
	ErrTransient       uint64 // EOF, timeout, 502/503, connection reset, etc.
	ErrOther           uint64
	BufferDepth        int
	LeadSlots          int64 // MaxBufferedSlot - NextSlotToSend
	NextSlot           uint64
	MaxBuffered        uint64
	ConfirmedTip       uint64
	ProcessedTip       uint64 // Processed commitment tip (super tip)
	TipAtSlot          uint64 // What slot we were emitting when tip was measured
	WorkQueueLen       int
	ReorderBufLen      int
	// Tip poll health
	TipStaleSecs      int64  // Seconds since last successful tip update (0 = healthy)
	TipPollFailures   uint64 // Consecutive tip poll failures
	TotalTipPollFails uint64 // Total tip poll failures this window
	// Mode tracking
	IsNearTip bool // True when in near-tip mode (low-latency, just-in-time scheduling)
	// RPS tracking (for RPC credit usage visibility)
	GetBlockRPS float64 // getBlock calls per second over the stats window
	SuccessRate float64 // Percentage of fetches that returned block data (vs skipped)
	WindowSecs  float64 // How long the stats window has been open
	// Stall diagnostics (for STALL log in replay loop)
	WaitingSlotState   string // "inflight", "done", "pending", "missing"
	WaitingSlotRetries int    // How many times the waiting slot has been retried
	InflightCount      int    // Number of slots currently being fetched
	RetryQueueLen      int    // Number of slots waiting to be retried
	CurrentSource      string // "rpc", "lightbringer", "turbine", or "file"
	SourceStatus       string // Human-readable description of current source state
	HandoffSlot        uint64 // First slot at which Lightbringer can take over (0 = none pending)
}

func (bs *BlockSource) currentSourceSnapshot() (string, string, uint64) {
	switch bs.sourceType {
	case BlockSourceFile:
		return "file", "file playback", 0
	case BlockSourceRpc:
		return "rpc", "rpc", 0
	case BlockSourceLightbringer, BlockSourceTurbine:
		source := "lightbringer"
		if bs.sourceType == BlockSourceTurbine {
			source = "turbine"
		}
		handoffSlot := bs.lightbringerHandoffSlot.Load()
		cooldownUntil := bs.lightbringerCooldownUntil.Load()
		connected := bs.lightbringerConnected.Load()
		lastStreamSlot := bs.lightbringerLastStreamSlot.Load()
		if bs.lightbringerActive.Load() {
			return source, source + " live stream", handoffSlot
		}
		if cooldownUntil != 0 {
			if connected && lastStreamSlot != 0 {
				return "rpc", fmt.Sprintf("rpc, stabilising after %s gap until slot %d (latest streamed slot %d)", source, cooldownUntil, lastStreamSlot), 0
			}
			return "rpc", fmt.Sprintf("rpc, stabilising after %s gap until slot %d", source, cooldownUntil), 0
		}
		if bs.isNearTip.Load() {
			if handoffSlot != 0 {
				return "rpc", fmt.Sprintf("rpc, waiting for %s handoff at slot %d", source, handoffSlot), handoffSlot
			}
			if connected {
				bs.reorderMu.Lock()
				waitingSlot := bs.nextSlotToSend
				anchorSlot := bs.lastEmittedBlockSlot
				bs.reorderMu.Unlock()
				waitReason := bs.lightbringerHandoffWaitReason(waitingSlot, anchorSlot)
				if lastStreamSlot != 0 {
					return "rpc", fmt.Sprintf("rpc, %s connected (latest streamed slot %d); %s", source, lastStreamSlot, waitReason), 0
				}
				return "rpc", fmt.Sprintf("rpc, %s connected; %s", source, waitReason), 0
			}
			if bs.lightbringerStarted.Load() {
				return "rpc", fmt.Sprintf("rpc, waiting for %s stream connection", source), 0
			}
			return "rpc", fmt.Sprintf("rpc, starting %s stream", source), 0
		}
		if connected && lastStreamSlot != 0 {
			return "rpc", fmt.Sprintf("rpc catchup (%s connected, latest streamed slot %d)", source, lastStreamSlot), 0
		}
		return "rpc", "rpc catchup", 0
	default:
		return "rpc", "rpc", 0
	}
}

// GetFetchStats returns a snapshot of current fetch statistics
func (bs *BlockSource) GetFetchStats() FetchStatsSnapshot {
	attempts := bs.stats.FetchAttempts.Load()
	successes := bs.stats.FetchSuccesses.Load()
	latencyCount := bs.stats.FetchLatencyCount.Load()
	totalLatencyNs := bs.stats.TotalFetchLatencyNs.Load()

	var avgLatencyMs float64
	if latencyCount > 0 {
		avgLatencyMs = float64(totalLatencyNs) / float64(latencyCount) / 1e6
	}

	bs.reorderMu.Lock()
	nextSlot := bs.nextSlotToSend
	reorderLen := len(bs.reorderBuffer)
	bs.reorderMu.Unlock()

	// Stall diagnostics: waiting slot state and retry info
	var waitingSlotState string
	var inflightCount int
	bs.slotStateMu.Lock()
	if state, exists := bs.slotState[nextSlot]; exists {
		switch state {
		case slotInflight:
			waitingSlotState = "inflight"
		case slotDone:
			waitingSlotState = "done"
		case slotPending:
			waitingSlotState = "pending"
		}
	} else {
		waitingSlotState = "missing"
	}
	for _, state := range bs.slotState {
		if state == slotInflight {
			inflightCount++
		}
	}
	bs.slotStateMu.Unlock()

	var waitingSlotRetries int
	bs.waitingSlotErrorsMu.Lock()
	if info, exists := bs.waitingSlotErrors[nextSlot]; exists {
		waitingSlotRetries = info.retryCount
	}
	bs.waitingSlotErrorsMu.Unlock()

	bs.retryMu.Lock()
	retryQueueLen := len(bs.retrySlots)
	bs.retryMu.Unlock()

	maxBuffered := bs.stats.MaxBufferedSlot.Load()
	var leadSlots int64
	if maxBuffered >= nextSlot {
		leadSlots = int64(maxBuffered - nextSlot)
	}

	// Calculate tip staleness
	var tipStaleSecs int64
	lastTipUpdate := bs.lastTipUpdate.Load()
	if lastTipUpdate > 0 {
		tipStaleSecs = time.Now().Unix() - lastTipUpdate
	}

	// Calculate RPS and success rate
	skipped := bs.stats.FetchSkipped.Load()
	var getBlockRPS, successRate, windowSecs float64
	resetTime := bs.statsResetTime.Load()
	if resetTime > 0 {
		windowSecs = float64(time.Now().Unix() - resetTime)
		if windowSecs > 0 {
			getBlockRPS = float64(attempts) / windowSecs
		}
	}
	// Success rate = blocks returned / (blocks + skipped)
	// This shows what percentage of slots had blocks vs were empty
	totalCompleted := successes + skipped
	if totalCompleted > 0 {
		successRate = float64(successes) / float64(totalCompleted) * 100
	}

	currentSource, sourceStatus, handoffSlot := bs.currentSourceSnapshot()

	return FetchStatsSnapshot{
		Attempts:           attempts,
		Successes:          successes,
		Retries:            bs.stats.FetchRetries.Load(),
		Skipped:            skipped,
		SpeculativeRetries: bs.stats.SpeculativeRetries.Load(),
		AvgLatencyMs:       avgLatencyMs,
		ErrNotAvail:        bs.stats.ErrSlotNotAvail.Load(),
		ErrRateLimit:       bs.stats.ErrRateLimited.Load(),
		ErrBeyondTip:       bs.stats.ErrBeyondTip.Load(),
		ErrHistory:         bs.stats.ErrHistory.Load(),
		ErrTransient:       bs.stats.ErrTransient.Load(),
		ErrOther:           bs.stats.ErrOther.Load(),
		BufferDepth:        len(bs.streamChan),
		LeadSlots:          leadSlots,
		NextSlot:           nextSlot,
		MaxBuffered:        maxBuffered,
		ConfirmedTip:       bs.confirmedTip.Load(),
		ProcessedTip:       bs.processedTip.Load(),
		TipAtSlot:          bs.tipAtSlot.Load(),
		WorkQueueLen:       len(bs.workQueue),
		ReorderBufLen:      reorderLen,
		TipStaleSecs:       tipStaleSecs,
		TipPollFailures:    bs.tipPollFailures.Load(),
		TotalTipPollFails:  bs.totalTipPollFails.Load(),
		IsNearTip:          bs.isNearTip.Load(),
		GetBlockRPS:        getBlockRPS,
		SuccessRate:        successRate,
		WindowSecs:         windowSecs,
		// Stall diagnostics
		WaitingSlotState:   waitingSlotState,
		WaitingSlotRetries: waitingSlotRetries,
		InflightCount:      inflightCount,
		RetryQueueLen:      retryQueueLen,
		CurrentSource:      currentSource,
		SourceStatus:       sourceStatus,
		HandoffSlot:        handoffSlot,
	}
}

// ResetStats resets the fetch statistics (useful between 100-slot windows)
func (bs *BlockSource) ResetStats() {
	bs.stats.FetchAttempts.Store(0)
	bs.stats.FetchSuccesses.Store(0)
	bs.stats.FetchRetries.Store(0)
	bs.stats.FetchSkipped.Store(0)
	bs.stats.SpeculativeRetries.Store(0)
	bs.stats.ErrSlotNotAvail.Store(0)
	bs.stats.ErrRateLimited.Store(0)
	bs.stats.ErrBeyondTip.Store(0)
	bs.stats.ErrHistory.Store(0)
	bs.stats.ErrTransient.Store(0)
	bs.stats.ErrOther.Store(0)
	bs.stats.TotalFetchLatencyNs.Store(0)
	bs.stats.FetchLatencyCount.Store(0)
	// Reset tip poll failures for this window (but keep consecutive count for alerting)
	bs.totalTipPollFails.Store(0)
	// Reset the stats window start time for RPS calculation
	bs.statsResetTime.Store(time.Now().Unix())
}
