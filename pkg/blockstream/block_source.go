package blockstream

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"golang.org/x/time/rate"
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
	TurbineServeRepairAddr  string
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
	TurbineStakesForSlot         func(slot uint64) map[solana.PublicKey]uint64
	TurbineEpochForSlot          func(slot uint64) uint64
	TurbineRootSlot              func() uint64
	TurbineUseChaCha8            bool
	TurbineDedupAddrs            bool
	LocalLeaderForSlot           func(slot uint64) bool
	GossipClient                 *gossip.Client
	AlpenglowDecisionSource      func(anchorSlot uint64) (alpenglow.ChainDecision, bool)
	AlpenglowCandidateBlockSink  func(alpenglow.ReplayBlockObservation)
	AlpenglowInvalidBlockSink    func(alpenglow.BlockID, string) error
	// AlpenglowCandidateValidator prevents objectively invalid assembled blocks
	// from polluting the early ancestry tracker. Replay independently validates
	// again at the consensus boundary before observing or executing the block.
	AlpenglowCandidateValidator func(*b.Block) error
	AlpenglowFirstShredSink     func(slot uint64)
	// Cert-driven repair feed: certified-but-unobserved blocks the repair loop
	// steers turbine toward, and the skip oracle that cancels shred state for
	// certificate-skipped slots.
	AlpenglowWantedBlocks  func(afterSlot uint64, max int) []alpenglow.WantedBlock
	AlpenglowSkipCertified func(slot uint64) bool
	// Footer certificates from ASSEMBLED blocks, fed to the consensus engine
	// at ingest time — before ordered emission. During catchup the certs
	// proving decisions for older slots (including skips) arrive inside
	// LATER blocks that cannot emit until those very decisions are known.
	AlpenglowFooterCertSink func(raw []byte)
	// Exact identity of StartSlot-1 on checkpoint resume. This is Alpenglow-
	// only and lets a fresh source validate a parent-linked skip run before it
	// has emitted its first post-resume block.
	InitialAlpenglowBlockID    solana.Hash
	HasInitialAlpenglowBlockID bool
	// RepairCatchupMaxGapSlots: when resuming behind the live shred edge by at
	// most this many slots, fill the gap via turbine repair instead of RPC
	// getBlock (0 disables). Repaired shreds carry block ids + footer certs, so
	// catchup finality is cryptographic rather than delegated to the RPC's
	// "finalized" commitment. In verifying mode this also leaves the RPC budget
	// to the trailing verifier; validator mode never fetches RPC blocks.
	RepairCatchupMaxGapSlots uint64
	// RepairMaxRequestsPerSecond overrides the repair request-rate ceiling
	// (0 = built-in default). Peer-side serve-repair QoS bans heavy unstaked
	// requesters, so raise with care.
	RepairMaxRequestsPerSecond int
	// ShredSpoolDir: on-disk spool for verified raw shreds (typically under
	// storage.shredstore). Bounds catchup RAM: far-future shreds live on
	// disk and hydrate in batches ahead of replay; survives restarts, so a
	// rebooted node re-hydrates instead of re-repairing. Empty = disabled.
	ShredSpoolDir string
	// PrewarmBlocks: turbine blocks collected by the boot-time prewarm
	// receiver (see TurbinePrewarm), injected into the staging buffer at
	// construction so the catchup handoff can arm from them immediately.
	PrewarmBlocks []*b.Block
	// LocalBlocks carries fully frozen blocks from this node's producer. The
	// source owns the consumer for exactly its own lifetime, avoiding stale
	// consumers across a replay/fork-recovery restart. Replay adopts the
	// producer SlotCtx for these slots and does not re-execute them.
	LocalBlocks <-chan *b.Block
	// DisableRPCBlockFetch (config block.rpc_fallback=false): a live-shred
	// source NEVER fetches blocks over RPC — shreds via turbine + repair are
	// the only block path, no matter how far behind replay is, and every
	// force-RPC recovery path routes to turbine repair or holds for
	// certificate adjudication instead. RPC still serves control-plane queries
	// and, in verifying mode, the trailing verifier. Ignored for non-shred sources (source = "rpc",
	// "file"), which are their own block path.
	DisableRPCBlockFetch bool
	StartSlot            uint64
	EndSlot              uint64
	BlockDir             string
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
	candidateObserved    bool // live ingest already validated and published candidate ancestry
	err                  error
	skipped              bool   // true if SlotSkipped error
	absentOK             bool   // true if a secondary confirmed-RPC probe verified the slot is absent
	rpcIdx               int32  // which RPC endpoint produced this result (for error attribution)
	latencyMs            int64  // fetch latency in milliseconds (for stall diagnostics)
	liveStreamGeneration uint64 // live-stream result generation, used to discard stale handoff blocks
	local                bool
	wakeEmitter          bool // control event: re-run ordered emission after an Alpenglow rewind
}

// AlpenglowParentSwitch is an exact, shred-derived fork transition. Child's
// parent block ID matches a block we previously emitted, but that parent is
// older than the current speculative tip. Replay must select the replacement
// branch from SwitchSlot before the source may emit ChildSlot and descendants;
// account state is unwound only if replay already executed the divergence.
//
// This is deliberately not a certificate decision. It selects a coherent
// speculative branch for liveness; later certificates remain authoritative
// and can switch replay again through the normal certified-switch sweep.
type AlpenglowParentSwitch struct {
	SwitchSlot uint64
	ParentSlot uint64
	ParentID   solana.Hash
	ChildSlot  uint64
	ChildID    solana.Hash
}

var errBeyondTip = errors.New("slot beyond confirmed tip")

type blockSourceStopReason uint32

const (
	blockSourceStopReasonNone blockSourceStopReason = iota
	blockSourceStopReasonCompleted
	blockSourceStopReasonStalled
	blockSourceStopReasonUnexpectedLiveEnd
	blockSourceStopReasonAlpenglowConflict
	blockSourceStopReasonInvalidAlpenglowCertificate
	blockSourceStopReasonHistoryUnavailable
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
	rpcClients []*rpcclient.RpcClient // All RPC clients for block fetching (index 0 = primary)
	streamChan chan *b.Block
	// replayEmissionWg covers the unlocked channel-send window. Add happens
	// under reorderMu before selection is published; invalid-block quarantine
	// raises its gate, crosses reorderMu, then waits before its final drain.
	replayEmissionWg sync.WaitGroup
	// Test-only scheduling hook, deliberately invoked after reorderMu is
	// released and before streamChan send.
	beforeReplayBlockSend func(*b.Block)
	// Test-only scheduling hook invoked after early candidate validation and
	// before a live block acquires its staging/reorder commit lock.
	beforeLiveBlockCommit func(*b.Block)
	startSlot             uint64
	endSlot               uint64
	currentSlot           uint64
	blockDir              string
	sourceType            BlockSourceType

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
	// Tracks provisional skipped slots inferred from an exact Alpenglow parent
	// block-ID link. These are not RPC skip results and survive handoff, but a
	// later certificate can unwind them through replay's switch sweep.
	liveSynthesizedSkips map[uint64]bool
	// Tracks skips certified by Alpenglow consensus. These are not provisional
	// RPC skip results and must not be discarded after a Turbine handoff.
	alpenglowCertifiedSkips        map[uint64]bool
	nextSlotToSend                 uint64
	lastEmittedBlockSlot           uint64 // Last non-skipped block emitted to replay; used to validate live ancestry at handoff.
	lastEmittedAlpenglowBlockID    solana.Hash
	hasLastEmittedAlpenglowBlockID bool
	emittedAlpenglowBlockIDs       map[uint64]solana.Hash
	emittedAlpenglowBlockIDOrder   []uint64
	rejectedAlpenglowMu            sync.RWMutex
	rejectedAlpenglowBlockIDs      map[uint64]map[solana.Hash]struct{}
	// invalidAlpenglowBlockIDs contains identities rejected by deterministic
	// block validation. Unlike speculative branch tombstones, these may never
	// be cleared by a certificate naming the same objectively-invalid block.
	invalidAlpenglowBlockIDs       map[uint64]map[solana.Hash]struct{}
	rejectedAlpenglowSkipRanges    []alpenglowSlotRange
	authoritativeAlpenglowBlockIDs map[uint64]solana.Hash
	// While non-zero, ordered emission is held at and above this slot while
	// replay atomically quarantines an invalid emitted suffix.
	alpenglowQuarantineFrom      atomic.Uint64
	alpenglowParentSwitchCh      chan AlpenglowParentSwitch
	pendingAlpenglowParentSwitch *AlpenglowParentSwitch // guarded by reorderMu
	maxPending                   int

	// Slot state tracking (prevents duplicates)
	slotStateMu   sync.Mutex
	slotState     map[uint64]slotStatus
	inflightStart map[uint64]time.Time // When each slot started fetching

	// Retry queue for slots that need rescheduling
	retryMu    sync.Mutex
	retrySlots []uint64

	// Worker coordination
	workQueue    chan uint64
	resultQueue  chan fetchResult
	stopChan     chan struct{}
	stopOnce     sync.Once
	stopped      atomic.Bool
	backgroundWg sync.WaitGroup

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
	lightbringerEndpoint         string
	turbineBindAddr              string
	turbineServeRepairAddr       string
	repairMaxRequestsPerSecond   int
	turbineGossipEntrypoint      string
	turbineGossipBindAddr        string
	turbineAdvertisedIP          string
	turbineShredVersion          uint16
	turbineAlpenglowAddr         string
	turbineAlpenglowBlockIDHints bool
	turbineIdentity              ed25519.PrivateKey
	leaderForSlot                func(slot uint64) (solana.PublicKey, bool)
	turbineStakesForSlot         func(slot uint64) map[solana.PublicKey]uint64
	turbineEpochForSlot          func(slot uint64) uint64
	turbineRootSlot              func() uint64
	turbineUseChaCha8            bool
	turbineDedupAddrs            bool
	localLeaderForSlot           func(slot uint64) bool
	localBlocks                  <-chan *b.Block
	gossipClient                 *gossip.Client
	liveStreamStarted            atomic.Bool
	liveStreamConnected          atomic.Bool
	liveLastStreamSlot           atomic.Uint64
	liveLastRecvUnix             atomic.Int64
	liveReconnectRequested       atomic.Bool
	liveCancelMu                 sync.Mutex
	liveCancel                   context.CancelFunc
	liveHandoffSlot              atomic.Uint64 // First slot from the active stream connection, 0 = no active handoff
	liveResultGeneration         atomic.Uint64 // Incremented whenever a live-stream handoff/runway is invalidated
	liveForceRPCUntil            atomic.Uint64 // While set, ignore Lightbringer and use RPC until this slot is executed
	liveCooldownUntil            atomic.Uint64 // After a missing-slot recovery, keep RPC active until this slot executes
	liveNeedRPCResume            atomic.Bool   // Set when a live handoff disconnects and RPC must fill the gap again
	liveStreamActive             atomic.Bool   // True once emitted blocks are being sourced from Lightbringer
	lightbringerGapSlot          atomic.Uint64 // Waiting slot currently being watched for a Lightbringer gap
	liveGapSinceUnix             atomic.Int64  // UnixNano when the current Lightbringer gap was first observed
	liveGapLastLogUnix           atomic.Int64  // UnixNano of the last active-gap wait log
	lightbringerGapReconnectSlot atomic.Uint64 // Waiting slot that already triggered a Lightbringer reconnect
	liveRepairSlot               atomic.Uint64 // Missing streamed slot currently being repaired via RPC, 0 = no repair in flight
	liveStreamWg                 sync.WaitGroup
	liveStagingMu                sync.Mutex
	liveStagingBuffer            map[uint64]*b.Block
	liveStagingOrder             []uint64
	liveDeliveryMu               sync.Mutex
	liveDeliveryPending          map[uint64]int // assembled blocks queued between stream intake and ordered emitter
	alpenglowMu                  sync.Mutex
	knownAlpenglowBlockIDs       map[uint64]solana.Hash
	knownAlpenglowBlockIDOrder   []uint64
	activeTurbineReceiver        *turbine.UDPReceiver
	// Repair-first catchup: gap slots [repairCatchupFrom, repairCatchupUntil]
	// fill via turbine repair; RPC never fetches at/above the gate while
	// pending or active. The pending hold persists from construction until
	// either repair arms (a fresh boot WAITS for turbine shreds rather than
	// opening an RPC catchup, as long as the gap is within threshold) or
	// repair is ruled out (gap too large, no tip signal). Turbine repair
	// OWNS the gap — no timer ever hands it to RPC. The only ways out are
	// the far-behind rule (replay more than repair_catchup_max_gap_slots
	// behind the live edge, re-evaluated continuously) and stream loss; the
	// monitor keeps watching and re-arms once the gap closes back under the
	// threshold, cooldown-gated.
	repairCatchupMaxGapSlots uint64
	shredSpoolDir            string
	rpcFallbackEnabled       bool // false (block.rpc_fallback=false): RPC never fetches blocks on a live-shred source
	repairCatchupPending     atomic.Bool
	repairCatchupFrom        atomic.Uint64 // first gap slot (0 = inactive)
	repairCatchupUntil       atomic.Uint64 // live edge at activation (0 = inactive)
	noRPCFallbackLogUnix     atomic.Int64  // rate limit: shreds-only recovery notices
	parentMismatchHoldUnix   atomic.Int64  // rate limit: shreds-only parent-mismatch hold warns
	// Catchup stall rescue: when RPC catchup is head-of-line blocked on one
	// slot (typically a giant block the RPC is slow to serialize) and turbine
	// is connected, pull that slot via turbine repair and deliver the
	// assembled block directly — instead of waiting tens of seconds for RPC
	// to serve data the shred path already has.
	repairCatchupCooldownUntil  atomic.Int64  // unix seconds: no repair-catchup re-arming until then (after a stall fallback)
	rescueFrom                  atomic.Uint64 // rescue window start (0 = inactive)
	rescueUntil                 atomic.Uint64 // rescue window end
	rescueWaitingSlot           atomic.Uint64 // slot the stall timer is tracking
	rescueWaitingSince          atomic.Int64  // unix seconds the waiting slot was first seen stalled
	lastRescueLogSlot           atomic.Uint64 // rate-limit: one rescue log per slot
	lastReorderGapSlot          atomic.Uint64 // backoff: repeated warns for the same waiting slot slow down
	alpenglowDecisionSource     func(anchorSlot uint64) (alpenglow.ChainDecision, bool)
	alpenglowCandidateBlockSink func(alpenglow.ReplayBlockObservation)
	alpenglowCandidateValidator func(*b.Block) error
	alpenglowInvalidBlockSink   func(alpenglow.BlockID, string) error
	alpenglowFirstShredSink     func(slot uint64)
	alpenglowWantedBlocksFn     func(afterSlot uint64, max int) []alpenglow.WantedBlock
	alpenglowSkipCertifiedFn    func(slot uint64) bool
	alpenglowFooterCertSink     func(raw []byte)

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
	lightbringerDialTimeout       = 10 * time.Second
	liveRetryBackoff              = 2 * time.Second
	liveMaxRetryBackoff           = 15 * time.Second
	liveStagingBufferSlots        = 256
	lightbringerFirstSlotWarn     = 10 * time.Second
	lightbringerIdleReconnect     = 30 * time.Second
	liveNoEmitReconnect           = 30 * time.Second
	lightbringerGapReconnectAfter = 30 * time.Second
	lightbringerDeepGapReconnect  = 15 * time.Second
	liveMinHandoffRun             = 8

	// Repair-first catchup pacing.
	// Catchup stall rescue: how long the emitter must be head-of-line blocked
	// on one slot before turbine repair is asked to fetch it, and how many
	// slots ahead the rescue window covers.
	catchupRescueAfterStall  = 10 * time.Second
	catchupRescueWindowSlots = uint64(16)

	// Repair-catchup pacing. There is NO timer-based RPC fallback inside the
	// drive: turbine repair owns the gap, a stalled head is warned about
	// (with repair counters) every repairCatchupStallWarnEvery but stays on
	// repair. RPC takes catchup back only via the far-behind rule — replay
	// more than block.repair_catchup_max_gap_slots behind the live edge,
	// re-evaluated continuously — or stream loss. After that fallback, a
	// PRODUCTIVE attempt (repair delivered blocks, replay just fell behind
	// anyway) re-arms on the short cooldown; a BARREN attempt (not one
	// shred-path block produced) re-arms on the long one, so wholesale RPC
	// can genuinely close a big gap before repair is retried instead of
	// oscillating at the threshold boundary.
	// Auto-tuned repair rate bounds (active only when the operator has not
	// pinned block.repair_max_requests_per_second): step size and hard
	// ceiling. Raised to an aggressive posture matching cavey's kv-block-
	// production, which runs repair with no rate limiter; the peer set is meant
	// to be the binder, not our throttle. Dial back via config if serve-repair
	// freezes. The controller can also step up when a healthy peer set is
	// answering promptly but replay is still losing ground: the configured rate
	// sizes both the token bucket and the in-flight admission window, so send-rate
	// utilization alone is not a sufficient saturation signal.
	repairAutoRateMin  = 2500
	repairAutoRateStep = 5000
	repairAutoRateMax  = 50000
	// Ignore a few slots of heartbeat-to-heartbeat jitter. Sustained growth
	// beyond this means the cluster edge is outrunning shred-only replay.
	repairAutoGapGrowthSlots = uint64(4)

	// QoS-throttle detector thresholds (evaluated per heartbeat interval). The
	// serve-repair rate-ban signature is "responses freeze while requests keep
	// flowing", so we flag a suspected throttle only when all three hold: we are
	// actually pushing (send >= repairQoSMinSendRate), the PREVIOUS interval was
	// being served well (resp >= repairQoSHealthyRespRate — rules out steady
	// scarcity, which never had a healthy response rate), and this interval's
	// response rate collapsed below repairQoSCollapseFraction of that. This
	// catches the freeze at onset without false-flagging a legitimate
	// high-timeout resume-gap catchup where only a few peers retain old shreds.
	repairQoSMinSendRate      = 300.0
	repairQoSHealthyRespRate  = 150.0
	repairQoSCollapseFraction = 0.35
)

// qosThrottleSuspected reports whether an interval looks like a serve-repair
// throttle rather than ordinary scarcity or a latency blip. All three must
// hold: we are pushing hard (sendRate), the PREVIOUS interval was answered well
// (prevAnswerRate — rules out steady scarcity that never had a healthy rate),
// and this interval's answer rate collapsed below a fraction of that. Callers
// pass ANSWER rate = (timely + late)/s, not timely-only: a latency spike shifts
// timely to late without a real throttle, so timely-only would false-fire.
func qosThrottleSuspected(sendRate, prevAnswerRate, answerRate float64) bool {
	return sendRate >= repairQoSMinSendRate &&
		prevAnswerRate >= repairQoSHealthyRespRate &&
		answerRate < prevAnswerRate*repairQoSCollapseFraction
}

// shouldIncreaseRepairRate recognizes both ways the repair client can be
// locally constrained. A full token bucket is the obvious one. The subtler
// case is a growing replay gap with healthy service: rate also sizes the
// admission window, so that window can bind while actual sends remain below
// the nominal ceiling. Requiring healthy/timely peers keeps a remote serving
// shortage from being mistaken for a reason to send harder.
func shouldIncreaseRepairRate(timelyPct uint64, healthyPeers, utilized, gapGrowing, repairStarved bool) bool {
	return timelyPct >= 75 && healthyPeers && repairStarved && (utilized || gapGrowing)
}

const (
	repairCatchupReArmCooldown   = 2 * time.Minute
	repairCatchupBarrenCooldown  = 30 * time.Minute
	repairCatchupDecisionTimeout = 15 * time.Second
	repairCatchupPollInterval    = 250 * time.Millisecond
	repairCatchupWindowSlots     = uint64(64) // matches the assembler's per-call priority range
	repairCatchupStallWarnEvery  = 60 * time.Second
	// Post-handoff live blocks farther than this beyond the waiting slot go
	// to the CAPPED staging buffer instead of the reorder buffer: during a
	// long catchup the live edge runs hundreds of slots ahead, and feeding
	// every edge block straight into the reorder buffer is an unbounded
	// memory path (monster blocks x hundreds). The drive drains staging
	// into the queue as replay advances.
	repairCatchupLiveDeliverWindow = uint64(256)
	// On-disk shred spool byte cap (highest slots dropped first).
	shredSpoolMaxBytes = int64(8) << 30
	// Stuck-head self-heal: only a recorded assembly/decode failure is evidence
	// of poisoned state (for example a bad first shred pinning an FEC signature,
	// variant, or layout). A merely incomplete slot is never reset: the missing
	// shreds may be scarce, and throwing away verified progress makes repair
	// start the entire slot over. Error-backed resets remain bounded and require
	// the head to have stopped growing.
	repairCatchupHeadResetAfter = 90 * time.Second
	// A full contiguous shred set with a recorded decode failure cannot gain
	// anything from more repair responses. Purge its persisted packets quickly
	// rather than waiting for the generic no-growth timeout.
	repairCatchupDecodeErrorResetAfter = 2 * time.Second
	repairCatchupMaxHeadResets         = 2
	repairCatchupHeadGrowthGrace       = 30 * time.Second

	liveEdgeHandoffMaxLag = 4
	liveGapFallbackWait   = 8 * time.Second
	liveGapBufferDepth    = 32
	liveRecoverySlots     = 0

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
		rpcClients:                     rpcClients,
		streamChan:                     make(chan *b.Block, streamChanBuffer),
		startSlot:                      opts.StartSlot,
		endSlot:                        opts.EndSlot,
		currentSlot:                    opts.StartSlot,
		blockDir:                       opts.BlockDir,
		sourceType:                     opts.SourceType,
		rateLimiter:                    rate.NewLimiter(rate.Limit(maxRPS), maxRPS),
		maxInflight:                    maxInflight,
		tipSafetyMargin:                tipSafetyMargin,
		tipPollInterval:                time.Duration(tipPollMs) * time.Millisecond,
		reorderBuffer:                  make(map[uint64]*b.Block),
		skippedSlots:                   make(map[uint64]bool),
		liveSynthesizedSkips:           make(map[uint64]bool),
		alpenglowCertifiedSkips:        make(map[uint64]bool),
		liveDeliveryPending:            make(map[uint64]int),
		emittedAlpenglowBlockIDs:       make(map[uint64]solana.Hash),
		rejectedAlpenglowBlockIDs:      make(map[uint64]map[solana.Hash]struct{}),
		invalidAlpenglowBlockIDs:       make(map[uint64]map[solana.Hash]struct{}),
		authoritativeAlpenglowBlockIDs: make(map[uint64]solana.Hash),
		alpenglowParentSwitchCh:        make(chan AlpenglowParentSwitch, 1),
		nextSlotToSend:                 opts.StartSlot,
		maxPending:                     defaultMaxPending,
		slotState:                      make(map[uint64]slotStatus),
		inflightStart:                  make(map[uint64]time.Time),
		workQueue:                      make(chan uint64, maxInflight*2),
		resultQueue:                    make(chan fetchResult, maxInflight*2),
		stopChan:                       make(chan struct{}),
		stallTimeout:                   defaultStallTimeout,
		catchupTipSafety:               tipSafetyMargin, // Store original for switching back to catchup
		lightbringerEndpoint:           opts.LightbringerEndpoint,
		turbineBindAddr:                opts.TurbineBindAddr,
		turbineServeRepairAddr:         opts.TurbineServeRepairAddr,
		repairCatchupMaxGapSlots:       opts.RepairCatchupMaxGapSlots,
		repairMaxRequestsPerSecond:     opts.RepairMaxRequestsPerSecond,
		shredSpoolDir:                  opts.ShredSpoolDir,
		rpcFallbackEnabled:             !opts.DisableRPCBlockFetch,
		turbineGossipEntrypoint:        opts.TurbineGossipEntrypoint,
		turbineGossipBindAddr:          opts.TurbineGossipBindAddr,
		turbineAdvertisedIP:            opts.TurbineAdvertisedIP,
		turbineShredVersion:            opts.TurbineShredVersion,
		turbineAlpenglowAddr:           opts.TurbineAlpenglowAddr,
		turbineAlpenglowBlockIDHints:   opts.TurbineAlpenglowBlockIDHints,
		turbineIdentity:                clonePrivateKey(opts.TurbineIdentity),
		leaderForSlot:                  opts.LeaderForSlot,
		turbineStakesForSlot:           opts.TurbineStakesForSlot,
		turbineEpochForSlot:            opts.TurbineEpochForSlot,
		turbineRootSlot:                opts.TurbineRootSlot,
		turbineUseChaCha8:              opts.TurbineUseChaCha8,
		turbineDedupAddrs:              opts.TurbineDedupAddrs,
		localLeaderForSlot:             opts.LocalLeaderForSlot,
		localBlocks:                    opts.LocalBlocks,
		gossipClient:                   opts.GossipClient,
		alpenglowDecisionSource:        opts.AlpenglowDecisionSource,
		alpenglowCandidateBlockSink:    opts.AlpenglowCandidateBlockSink,
		alpenglowCandidateValidator:    opts.AlpenglowCandidateValidator,
		alpenglowInvalidBlockSink:      opts.AlpenglowInvalidBlockSink,
		alpenglowFirstShredSink:        opts.AlpenglowFirstShredSink,
		alpenglowFooterCertSink:        opts.AlpenglowFooterCertSink,
		alpenglowWantedBlocksFn:        opts.AlpenglowWantedBlocks,
		alpenglowSkipCertifiedFn:       opts.AlpenglowSkipCertified,
		liveStagingBuffer:              make(map[uint64]*b.Block),
		knownAlpenglowBlockIDs:         make(map[uint64]solana.Hash),

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
	if opts.HasInitialAlpenglowBlockID && opts.StartSlot > 0 && opts.TurbineAlpenglowBlockIDHints {
		anchorSlot := opts.StartSlot - 1
		bs.lastEmittedBlockSlot = anchorSlot
		bs.lastEmittedAlpenglowBlockID = opts.InitialAlpenglowBlockID
		bs.hasLastEmittedAlpenglowBlockID = true
		bs.emittedAlpenglowBlockIDs[anchorSlot] = opts.InitialAlpenglowBlockID
		bs.emittedAlpenglowBlockIDOrder = append(bs.emittedAlpenglowBlockIDOrder, anchorSlot)
		mlog.Log.Infof("ALPENGLOW resume anchor: seeded block id %s at rooted slot %d", opts.InitialAlpenglowBlockID, anchorSlot)
	}

	// Prewarm handover: blocks the boot-time receiver collected while the
	// AccountsDB built. Staged now so the handoff runway can arm the moment
	// the drive starts — the pre-join repair hole these replace is the
	// single most expensive thing repair-only catchup does.
	if len(opts.PrewarmBlocks) > 0 {
		for _, blk := range opts.PrewarmBlocks {
			bs.bufferLiveStreamBlock(blk)
		}
		first := opts.PrewarmBlocks[0].Slot
		last := opts.PrewarmBlocks[len(opts.PrewarmBlocks)-1].Slot
		mlog.Log.Infof("turbine prewarm: staged %d pre-collected blocks (slots %d..%d)", len(opts.PrewarmBlocks), first, last)
	}

	// Log RPC configuration
	if len(rpcClients) > 1 {
		mlog.Log.Infof("Block fetching configured with %d RPC endpoints (primary + %d backups)",
			len(rpcClients), len(rpcClients)-1)
	}
	if opts.SourceType == BlockSourceLightbringer && opts.LightbringerEndpoint != "" {
		mlog.Log.Infof("Lightbringer live handoff configured for %s (RPC catchup remains enabled)", opts.LightbringerEndpoint)
	} else if opts.SourceType == BlockSourceTurbine && opts.TurbineBindAddr != "" {
		mlog.Log.FileOnlyf("Native turbine live handoff configured on %s", opts.TurbineBindAddr)
		if opts.TurbineGossipEntrypoint != "" {
			mlog.Log.FileOnlyf("Native turbine gossip configured with entrypoint %s", opts.TurbineGossipEntrypoint)
		}
	}

	// Repair-first catchup is decided once turbine reports its first shred
	// edge; until then (bounded) RPC holds off the gap. Armed here so the
	// fetch scheduler can never race ahead of the decision. In shreds-only
	// mode the monitor runs regardless of the threshold — repair is the only
	// catchup there is.
	if bs.sourceType == BlockSourceTurbine && (bs.repairCatchupMaxGapSlots > 0 || !bs.rpcFallbackEnabled) {
		bs.repairCatchupPending.Store(true)
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
	if bs.isInvalidAlpenglowBlockID(slot, blockID) {
		mlog.Log.Warnf("ALPENGLOW repair hint refused: block %s at slot %d is objectively invalid",
			blockID, slot)
		return
	}
	// Certificate-derived block-ID hints are authoritative and may legitimately
	// revive an identity rejected by an earlier speculative branch selection.
	bs.allowRejectedAlpenglowBlockID(slot, blockID)

	bs.alpenglowMu.Lock()
	var evictedKnownSlots []uint64
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
		evictedKnownSlots = append(evictedKnownSlots, old)
	}
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if len(evictedKnownSlots) > 0 {
		// Authoritative tombstone overrides need no longer lifetime than the
		// corresponding bounded block-ID hint cache.
		bs.rejectedAlpenglowMu.Lock()
		for _, old := range evictedKnownSlots {
			delete(bs.authoritativeAlpenglowBlockIDs, old)
		}
		bs.rejectedAlpenglowMu.Unlock()
	}

	if receiver != nil {
		receiver.SetKnownAlpenglowBlockID(slot, blockID)
	}
}

func (bs *BlockSource) observeAlpenglowCandidateBlock(blk *b.Block) bool {
	if !bs.turbineAlpenglowBlockIDHints {
		return true
	}
	if blk == nil || !blk.HasAlpenglowBlockID {
		return true
	}
	id := solana.Hash(blk.AlpenglowBlockID)
	parentSlot := blk.SourceParentSlot
	if parentSlot == 0 {
		parentSlot = blk.ParentSlot
	}
	if bs.alpenglowCandidateValidator != nil {
		if err := bs.alpenglowCandidateValidator(blk); err != nil {
			// Only live Alpenglow candidates can be safely quarantined in place.
			// RPC/file/local inputs remain fail-closed at replay prevalidation.
			if !blk.FromLiveStream {
				return true
			}
			bs.markInvalidAlpenglowBlockID(blk.Slot, id)
			bs.rejectInvalidTurbineBlockState(blk.Slot, id)
			if bs.alpenglowInvalidBlockSink != nil {
				if sinkErr := bs.alpenglowInvalidBlockSink(alpenglow.BlockID{Slot: blk.Slot, Hash: id}, err.Error()); sinkErr != nil {
					bs.setStopReason(blockSourceStopReasonInvalidAlpenglowCertificate, blk.Slot)
					mlog.Log.Errorf("ALPENGLOW SAFETY: invalid assembly at slot %d conflicts with consensus: %v", blk.Slot, sinkErr)
				}
			}
			mlog.Log.Warnf("ALPENGLOW invalid assembly rejected before candidate observation: block %s at slot %d: %v", id, blk.Slot, err)
			return false
		}
	}
	if blk.HasAlpenglowParentBlockID && bs.isInvalidAlpenglowBlockID(parentSlot, solana.Hash(blk.AlpenglowParentBlockID)) {
		bs.markInvalidAlpenglowBlockID(blk.Slot, id)
		bs.rejectInvalidTurbineBlockState(blk.Slot, id)
		reason := fmt.Sprintf("exact parent %s at slot %d is objectively invalid", solana.Hash(blk.AlpenglowParentBlockID), parentSlot)
		if bs.alpenglowInvalidBlockSink != nil {
			if sinkErr := bs.alpenglowInvalidBlockSink(alpenglow.BlockID{Slot: blk.Slot, Hash: id}, reason); sinkErr != nil {
				bs.setStopReason(blockSourceStopReasonInvalidAlpenglowCertificate, blk.Slot)
				mlog.Log.Errorf("ALPENGLOW SAFETY: invalid descendant at slot %d conflicts with consensus: %v", blk.Slot, sinkErr)
			}
		}
		mlog.Log.Warnf("ALPENGLOW invalid descendant rejected before candidate observation: block %s at slot %d names invalid parent %s at slot %d",
			id, blk.Slot, solana.Hash(blk.AlpenglowParentBlockID), parentSlot)
		return false
	}
	if bs.alpenglowCandidateBlockSink == nil {
		return true
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
	return true
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
			if bs.rpcBlockFetchAllowed() {
				bs.forceRPCForCatchup(gap)
			} else {
				// Shreds-only: there is nothing to force — the live stream
				// and its staged/buffered blocks ARE the recovery path, and
				// tearing them down here (the RPC handover's job) would close
				// every intake gate with no fetcher behind them, wedging the
				// node until the stall watchdog kills it. Reopen the catchup
				// intake gates now; the resident repair monitor re-arms the
				// drive on its next tick.
				bs.repairCatchupPending.Store(true)
				mlog.Log.Infof("repair catchup: replay fell %d slots behind the tip — re-arming turbine repair over the gap (shreds-only; live stream kept)", gap)
			}
		}
	} else {
		// Currently in catchup mode - switch to near-tip if gap is small
		if gap <= bs.nearTipThreshold {
			bs.isNearTip.Store(true)
			mlog.Log.Infof("MODE SWITCH: catchup → NEAR-TIP | gap=%d (threshold=%d) | exec_slot=%d | tip=%d",
				gap, bs.nearTipThreshold, lastExecuted, tip)
			bs.logLiveStreamModeState("near-tip", gap)
		}
	}

	if bs.usesLiveShredStream() {
		bs.maybeStartLightbringerStream()
		if bs.isNearTip.Load() {
			bs.maybePrepareLiveHandoff()
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

	bs.liveForceRPCUntil.Store(0)
	bs.liveCooldownUntil.Store(0)
	oldHandoff := bs.liveHandoffSlot.Swap(0)
	wasActive := bs.liveStreamActive.Swap(false)
	resultGeneration := bs.invalidateLiveStreamResults()
	bs.liveNeedRPCResume.Store(true)
	bs.clearLiveGapWatch()
	bs.resetLiveRepairSlot()
	clearedPrefetched := bs.clearBufferedLiveStreamBlocks()

	bs.reorderMu.Lock()
	waitingSlot := bs.nextSlotToSend
	previousWaitingSlot := waitingSlot
	if wasActive {
		waitingSlot, previousWaitingSlot = bs.rewindLiveStreamFrontierForRPCFallbackLocked()
	}
	rewoundEmissionFrontier := previousWaitingSlot != waitingSlot
	removedSlots := make([]uint64, 0)
	for slot, blk := range bs.reorderBuffer {
		if blk != nil && blk.FromLiveStream && slot >= waitingSlot {
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

// rpcBlockFetchAllowed reports whether RPC may fetch BLOCKS at all. With
// block.rpc_fallback=false (the shipped default) a NATIVE TURBINE source
// never fetches blocks over RPC — turbine + repair are the only block path,
// no matter how far behind replay is; RPC serves control-plane queries and the
// verifying mode's trailing verifier. Scoped to turbine because only turbine has the repair
// machinery to fill gaps itself: the Lightbringer sidecar streams near-tip
// only and NEEDS RPC for old/evicted shreds, and non-shred sources are their
// own block path.
func (bs *BlockSource) rpcBlockFetchAllowed() bool {
	return bs.rpcFallbackEnabled || bs.sourceType != BlockSourceTurbine
}

func (bs *BlockSource) shouldUseRPCForSlot(slot uint64) bool {
	if bs.localLeaderForSlot != nil && bs.localLeaderForSlot(slot) {
		return false
	}
	if !bs.usesLiveShredStream() {
		return true
	}
	// Shreds-only mode (native turbine only): the single choke point — no
	// slot is ever RPC-fetchable, so the scheduler idles and stray results
	// are discarded.
	if !bs.rpcBlockFetchAllowed() {
		return false
	}
	if bs.liveForceRPCUntil.Load() != 0 {
		return true
	}
	if bs.liveCooldownUntil.Load() != 0 {
		return true
	}
	if bs.isLiveRepairSlot(slot) {
		return true
	}
	// Repair-first catchup: the gap belongs to turbine repair. While the
	// decision is pending RPC holds off entirely — including at startup,
	// where the node WAITS for turbine shreds instead of opening an RPC
	// catchup, for as long as the gap stays within the repair threshold.
	// The pending flag clears only when repair arms or is ruled out.
	if (bs.repairCatchupPending.Load() || bs.repairCatchupActive()) && slot >= bs.repairCatchupGateSlot() {
		return false
	}
	if !bs.isNearTip.Load() {
		return true
	}

	handoffSlot := bs.liveHandoffSlot.Load()
	if handoffSlot == 0 {
		return true
	}

	return slot < handoffSlot
}

func (bs *BlockSource) shouldDiscardRPCResultAfterHandoff(slot uint64, blk *b.Block) bool {
	if blk != nil && blk.FromLocalProduction {
		return false
	}
	if !bs.usesLiveShredStream() {
		return false
	}
	handoffSlot := bs.liveHandoffSlot.Load()
	if handoffSlot == 0 || slot < handoffSlot {
		return false
	}
	if bs.shouldUseRPCForSlot(slot) {
		return false
	}
	return blk == nil || !blk.FromLiveStream
}

func (bs *BlockSource) shouldDiscardSkippedSlotAfterHandoff(slot uint64) bool {
	if !bs.shouldDiscardRPCResultAfterHandoff(slot, nil) {
		return false
	}
	return !bs.liveSynthesizedSkips[slot] && !bs.alpenglowCertifiedSkips[slot]
}

func (bs *BlockSource) shouldDiscardLiveStreamResult(slot uint64, generation uint64) bool {
	if generation != bs.liveResultGeneration.Load() {
		return true
	}
	if bs.liveForceRPCUntil.Load() != 0 {
		return true
	}
	if bs.liveCooldownUntil.Load() != 0 {
		return true
	}
	if bs.isLiveRepairSlot(slot) {
		return true
	}
	// Catchup stall rescue: the delivery exists precisely BECAUSE replay is
	// far from the tip — it races the slow RPC fetch for the blocked head.
	if bs.catchupRescueCovers(slot) {
		return false
	}
	// Repair catchup delivers live-stream results far from the tip by
	// design. The near-tip requirement below is the stale-result guard for
	// ORDINARY live streaming only — applied during catchup it silently
	// discarded every handoff delivery: the drained head "vanished", the
	// assembler kept its completed marker, and the heal/re-fetch cycle spun
	// forever (observed live at slot 6181681).
	if bs.repairCatchupActive() {
		handoffSlot := bs.liveHandoffSlot.Load()
		return handoffSlot == 0 || slot < handoffSlot
	}
	if !bs.isNearTip.Load() {
		return true
	}

	handoffSlot := bs.liveHandoffSlot.Load()
	return handoffSlot == 0 || slot < handoffSlot
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
	delete(bs.liveSynthesizedSkips, waitingSlot)
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
	waitingSlot := bs.nextSlotToSend
	if waitingSlot == 0 {
		return false
	}
	decision, ok := bs.alpenglowDecisionSource(waitingSlot - 1)
	if !ok || decision.Slot != waitingSlot {
		return false
	}
	if decision.Kind == alpenglow.ChainDecisionKindConflict {
		mlog.Log.Errorf("ALPENGLOW SAFETY: consensus conflict at slot %d: %s", waitingSlot, decision.Reason)
		bs.setStopReason(blockSourceStopReasonAlpenglowConflict, waitingSlot)
		delete(bs.reorderBuffer, waitingSlot)
		return false
	}
	// Objective-invalid contents outrank identity hints. A decisive decision
	// naming one is a safety contradiction in every source mode, including
	// repair catchup; fallback wanted-block hints are filtered separately.
	if decision.Kind == alpenglow.ChainDecisionKindBlock && bs.isInvalidAlpenglowBlockID(waitingSlot, decision.Block.Hash) {
		mlog.Log.Errorf("ALPENGLOW SAFETY: decisive certificate names objectively invalid block %s at slot %d — refusing to re-authorize it",
			decision.Block.Hash, waitingSlot)
		bs.setStopReason(blockSourceStopReasonInvalidAlpenglowCertificate, waitingSlot)
		delete(bs.reorderBuffer, waitingSlot)
		return false
	}
	if decision.Kind == alpenglow.ChainDecisionKindBlock {
		// A decisive block identity is authoritative over soft tombstones left
		// by an earlier speculative parent switch. Publish it even outside
		// active near-tip mode and when no candidate is buffered yet, so the
		// next repaired/spooled copy is admitted instead of rejected forever.
		bs.SetKnownAlpenglowBlockID(waitingSlot, decision.Block.Hash)
	}
	// Buffered-candidate steering otherwise needs active near-tip Turbine.
	if !bs.liveStreamActive.Load() || !bs.isNearTip.Load() {
		return false
	}

	switch decision.Kind {
	case alpenglow.ChainDecisionKindSkip:
		if !bs.skippedSlots[waitingSlot] {
			delete(bs.reorderBuffer, waitingSlot)
			bs.skippedSlots[waitingSlot] = true
			bs.alpenglowCertifiedSkips[waitingSlot] = true
			delete(bs.liveSynthesizedSkips, waitingSlot)
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
			delete(bs.liveSynthesizedSkips, waitingSlot)
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
	bs.trackLiveDelivery(blk.Slot)

	bs.liveLastStreamSlot.Store(blk.Slot)
	bs.liveLastRecvUnix.Store(time.Now().Unix())

	// Consensus feed at ASSEMBLY time, before ANY emission gating: footer
	// certificates and parent links from blocks we may only stage (or even
	// drop) are how the chain tracker learns decisions for older slots
	// during catchup. Without this, a certificate-skipped slot inside the
	// gap deadlocks the emitter — the proof of the skip is buffered one
	// slot ahead of it, waiting on the skip to resolve. The engine dedupes,
	// so the emission-time observations remain unchanged.
	validCandidate := bs.observeAlpenglowCandidateBlock(blk)
	if bs.alpenglowFooterCertSink != nil && len(blk.AlpenglowFinalCert) > 0 {
		bs.alpenglowFooterCertSink(blk.AlpenglowFinalCert)
	}
	if !validCandidate {
		bs.finishLiveDelivery(blk.Slot)
		return true
	}

	if !bs.shouldDecodeLiveSlot(blk.Slot) {
		bs.finishLiveDelivery(blk.Slot)
		return true
	}

	if bs.liveHandoffSlot.Load() == 0 {
		// Catchup stall rescue: deliver a repair-assembled block for the
		// blocked emitter directly. The parent-connect check at emission
		// keeps this safe — a non-connecting block stays buffered and the
		// RPC fetch still races it.
		if bs.catchupRescueCovers(blk.Slot) && !bs.isNearTip.Load() {
			select {
			case bs.resultQueue <- fetchResult{slot: blk.Slot, block: blk, candidateObserved: true, rpcIdx: -1, liveStreamGeneration: bs.liveResultGeneration.Load()}:
				return true
			case <-bs.stopChan:
				bs.finishLiveDelivery(blk.Slot)
				return false
			}
		}
		// Stage a bounded runway before near-tip so handoff does not have
		// to build its whole connected run while replay is already at tip.
		bs.bufferLiveStreamBlock(blk)
		bs.maybePrepareLiveHandoff()
		bs.finishLiveDelivery(blk.Slot)
		return true
	}

	catchupAccepting := bs.repairCatchupAcceptingLiveBlocks()
	if (!bs.isNearTip.Load() && !catchupAccepting) || blk.Slot < bs.liveHandoffSlot.Load() {
		bs.finishLiveDelivery(blk.Slot)
		return true
	}

	// During catchup, only blocks NEAR the replay head go straight to the
	// emitter's queue; the far live edge stages instead (capped, catchup
	// eviction keeps the low end). The drive drains staging into the queue
	// as replay advances — bounding reorder-buffer memory without ever
	// discarding a block outright.
	if catchupAccepting && !bs.isNearTip.Load() {
		bs.reorderMu.Lock()
		waiting := bs.nextSlotToSend
		bs.reorderMu.Unlock()
		if waiting != 0 && blk.Slot > waiting+repairCatchupLiveDeliverWindow {
			bs.bufferLiveStreamBlock(blk)
			bs.finishLiveDelivery(blk.Slot)
			return true
		}
	}

	select {
	case bs.resultQueue <- fetchResult{slot: blk.Slot, block: blk, candidateObserved: true, rpcIdx: -1, liveStreamGeneration: bs.liveResultGeneration.Load()}:
		return true
	case <-bs.stopChan:
		bs.finishLiveDelivery(blk.Slot)
		return false
	}
}

func (bs *BlockSource) trackLiveDelivery(slot uint64) {
	if slot == 0 {
		return
	}
	bs.liveDeliveryMu.Lock()
	if bs.liveDeliveryPending == nil {
		bs.liveDeliveryPending = make(map[uint64]int)
	}
	bs.liveDeliveryPending[slot]++
	bs.liveDeliveryMu.Unlock()
}

func (bs *BlockSource) finishLiveDelivery(slot uint64) {
	bs.liveDeliveryMu.Lock()
	if count := bs.liveDeliveryPending[slot]; count <= 1 {
		delete(bs.liveDeliveryPending, slot)
	} else {
		bs.liveDeliveryPending[slot] = count - 1
	}
	bs.liveDeliveryMu.Unlock()
}

func (bs *BlockSource) liveDeliveryInFlight(slot uint64) bool {
	bs.liveDeliveryMu.Lock()
	pending := bs.liveDeliveryPending[slot] > 0
	bs.liveDeliveryMu.Unlock()
	return pending
}

func (bs *BlockSource) handleLiveShredStreamClosed(reason string) int {
	bs.liveStreamConnected.Store(false)
	bs.clearLiveStreamCancel()
	// A dead stream ends any repair-catchup attempt: the receiver (and its
	// retention floor) is gone; the interrupted-handoff path below forces RPC.
	bs.deactivateRepairCatchup(nil)
	interrupted := bs.liveHandoffSlot.Load() != 0 || bs.liveStreamActive.Load()
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
		if !bs.rpcBlockFetchAllowed() {
			// Shreds-only mode: no RPC resume. Reset the live-path state for
			// a clean re-handoff when the receiver restarts; replay waits on
			// shreds, exactly as configured. Re-arm the repair-catchup
			// pending flag NOW so the replacement receiver starts its
			// monitor with boot semantics.
			bs.repairCatchupPending.Store(true)
			bs.liveStreamActive.Store(false)
			bs.invalidateLiveStreamResults()
			cleared := bs.clearBufferedLiveStreamBlocks()
			bs.liveHandoffSlot.Store(0)
			bs.clearLiveGapWatch()
			bs.resetLiveRepairSlot()
			mlog.Log.Warnf("%s receiver stopped mid-handoff at slot %d; RPC block fetch is disabled — the local receiver restarts and repair catchup re-arms to refill",
				bs.liveShredStreamName(), waitingSlot)
			if reason != "" {
				mlog.Log.Warnf("%s stream closed: %s", bs.liveShredStreamName(), reason)
			}
			return cleared
		}
		bs.forceRPCForLiveGap(waitingSlot, 0, 0, 0)
		mlog.Log.Warnf("%s handoff interrupted; replay will resume RPC fallback from slot %d until a fresh stream runway is armed",
			bs.liveShredStreamName(), waitingSlot)
		if reason != "" {
			mlog.Log.Warnf("%s stream closed: %s", bs.liveShredStreamName(), reason)
		}
		return 0
	}

	bs.invalidateLiveStreamResults()
	clearedPrefetched := bs.clearBufferedLiveStreamBlocks()
	bs.liveHandoffSlot.Store(0)
	if reason != "" {
		mlog.Log.Warnf("%s stream closed: %s", bs.liveShredStreamName(), reason)
	}
	return clearedPrefetched
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
	if err := out.FixupTxVersions(); err != nil {
		file.Close()
		return nil, fmt.Errorf("block transaction-version fixup: %w", err)
	}

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

	return block.FromBlockResult(blockResult, slot, rpc)
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

// rewindAlpenglowEmissionAnchorLocked drops emitted identities at and above
// slot and restores the newest real block below it as the ancestry anchor.
// Caller holds reorderMu.
func (bs *BlockSource) rewindAlpenglowEmissionAnchorLocked(slot uint64) {
	if slot == 0 || bs.lastEmittedBlockSlot < slot {
		return
	}
	anchorSlot := uint64(0)
	anchorID := solana.Hash{}
	kept := bs.emittedAlpenglowBlockIDOrder[:0]
	for _, emittedSlot := range bs.emittedAlpenglowBlockIDOrder {
		if emittedSlot >= slot {
			delete(bs.emittedAlpenglowBlockIDs, emittedSlot)
			continue
		}
		kept = append(kept, emittedSlot)
		if emittedSlot > anchorSlot {
			anchorSlot = emittedSlot
			anchorID = bs.emittedAlpenglowBlockIDs[emittedSlot]
		}
	}
	bs.emittedAlpenglowBlockIDOrder = kept
	if anchorSlot != 0 {
		bs.lastEmittedBlockSlot = anchorSlot
		bs.lastEmittedAlpenglowBlockID = anchorID
		bs.hasLastEmittedAlpenglowBlockID = true
	} else {
		bs.lastEmittedBlockSlot = slot - 1
		bs.lastEmittedAlpenglowBlockID = solana.Hash{}
		bs.hasLastEmittedAlpenglowBlockID = false
	}
}

// RewindForAlpenglowSwitch rewinds the emission frontier to re-serve `slot`
// after finality or a decisive block certificate names a different block than
// the one executed. Buffered and in-flight state at or above the slot is
// dropped, live-stream results are invalidated, and the certified block id
// narrows the turbine assembler + prioritizes repair so that version arrives
// quickly.
func (bs *BlockSource) RewindForAlpenglowSwitch(slot uint64, certified solana.Hash) {
	if slot == 0 {
		return
	}
	bs.reorderMu.Lock()
	if bs.nextSlotToSend > slot {
		bs.nextSlotToSend = slot
	}
	bs.rejectAlpenglowEmissionSuffixLocked(slot)
	bs.rewindAlpenglowEmissionAnchorLocked(slot)
	for bufferedSlot := range bs.reorderBuffer {
		if bufferedSlot >= slot {
			delete(bs.reorderBuffer, bufferedSlot)
		}
	}
	for skippedSlot := range bs.skippedSlots {
		if skippedSlot >= slot {
			delete(bs.skippedSlots, skippedSlot)
			delete(bs.liveSynthesizedSkips, skippedSlot)
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
	bs.invalidateLiveStreamResults()

	if certified != (solana.Hash{}) {
		bs.SetKnownAlpenglowBlockID(slot, certified)
		bs.resetTurbineSlotForAlpenglowBlock(slot, certified)
	}
	mlog.Log.Warnf("BLOCK SOURCE REWIND: re-serving slot %d after certificate switch (certified=%s)", slot, certified)
}

// RewindForAlpenglowParentSwitch acknowledges a parent-linked speculative fork
// event after replay accepts the replacement branch. The alternate child and
// already-buffered descendants are retained; the discarded source suffix is
// cleared and ordered emission is kicked from the restored parent anchor.
func (bs *BlockSource) RewindForAlpenglowParentSwitch(event AlpenglowParentSwitch) bool {
	if bs.stopped.Load() || event.SwitchSlot == 0 || event.ParentSlot+1 != event.SwitchSlot || event.ChildSlot < event.SwitchSlot {
		return false
	}

	bs.reorderMu.Lock()
	pending := bs.pendingAlpenglowParentSwitch
	if pending == nil || *pending != event {
		bs.reorderMu.Unlock()
		return false
	}
	parentID, ok := bs.emittedAlpenglowBlockIDs[event.ParentSlot]
	child := bs.reorderBuffer[event.ChildSlot]
	if !ok || parentID != event.ParentID || child == nil || !child.HasAlpenglowBlockID ||
		solana.Hash(child.AlpenglowBlockID) != event.ChildID || !child.HasAlpenglowParentBlockID ||
		solana.Hash(child.AlpenglowParentBlockID) != event.ParentID {
		bs.pendingAlpenglowParentSwitch = nil
		bs.reorderMu.Unlock()
		return false
	}

	discardedTip := bs.lastEmittedBlockSlot
	bs.nextSlotToSend = event.SwitchSlot
	bs.rejectAlpenglowEmissionSuffixLocked(event.SwitchSlot)
	bs.rejectAlpenglowSkipRange(event.SwitchSlot, event.ChildSlot)
	bs.rewindAlpenglowEmissionAnchorLocked(event.SwitchSlot)
	for slot := range bs.reorderBuffer {
		if slot >= event.SwitchSlot && slot < event.ChildSlot {
			delete(bs.reorderBuffer, slot)
		}
	}
	for slot := range bs.skippedSlots {
		if slot >= event.SwitchSlot && slot < event.ChildSlot {
			delete(bs.skippedSlots, slot)
			delete(bs.liveSynthesizedSkips, slot)
			delete(bs.alpenglowCertifiedSkips, slot)
		}
	}
	bs.pendingAlpenglowParentSwitch = nil
	bs.reorderMu.Unlock()

	bs.slotStateMu.Lock()
	for slot := range bs.slotState {
		if slot >= event.SwitchSlot && slot < event.ChildSlot {
			delete(bs.slotState, slot)
			delete(bs.inflightStart, slot)
		}
	}
	bs.slotStateMu.Unlock()

	bs.retryMu.Lock()
	if len(bs.retrySlots) > 0 {
		kept := bs.retrySlots[:0]
		for _, slot := range bs.retrySlots {
			if slot < event.SwitchSlot || slot >= event.ChildSlot {
				kept = append(kept, slot)
			}
		}
		bs.retrySlots = kept
	}
	bs.retryMu.Unlock()

	// If the alternate child arrived late, the assembler may already have
	// marked descendants on the discarded branch complete. Re-open those
	// slots so the new branch can arrive; exact parent-ID checks reject any
	// stale sibling that the spool happens to hydrate first.
	if discardedTip > event.ChildSlot {
		bs.alpenglowMu.Lock()
		receiver := bs.activeTurbineReceiver
		bs.alpenglowMu.Unlock()
		if receiver != nil {
			for slot := event.ChildSlot + 1; slot <= discardedTip; slot++ {
				receiver.ResetSlotAndDiscardSpool(slot)
			}
		}
	}

	// Results emitted before replay saw the control event belong to the
	// discarded suffix. Remove them before replay resumes. No producer can
	// advance beyond child while the ancestry guard is holding it.
	retained := make([]*b.Block, 0)
drainStream:
	for {
		select {
		case blk, open := <-bs.streamChan:
			if !open {
				break drainStream
			}
			if blk != nil && blk.Slot < event.SwitchSlot {
				retained = append(retained, blk)
			}
		default:
			break drainStream
		}
	}
	for _, blk := range retained {
		bs.streamChan <- blk
	}
	for {
		select {
		case <-bs.alpenglowParentSwitchCh:
		default:
			goto switchChannelDrained
		}
	}
switchChannelDrained:

	bs.invalidateLiveStreamResults()
	bs.lastProgress.Store(time.Now().Unix())
	select {
	case bs.resultQueue <- fetchResult{wakeEmitter: true}:
	default:
		// A full queue already guarantees an emitter iteration, which is all
		// the wake is needed for.
	}
	mlog.Log.Warnf("BLOCK SOURCE REWIND: restored parent-linked ancestor %s at slot %d; re-serving speculative branch from slot %d toward child %s at slot %d",
		event.ParentID, event.ParentSlot, event.SwitchSlot, event.ChildID, event.ChildSlot)
	return true
}

// RejectAlpenglowParentSwitch resolves a pending parent-linked fork observation
// in favor of the branch already emitted by this source. Replay calls this only
// when the proposed divergence is at/below its rooted-durable watermark: the
// alternate child is merely a late speculative sibling and cannot replace
// finalized state. Tombstoning its exact identity prevents spool hydration or
// another delayed assembler result from proposing the same reverse switch.
// Certificates remain authoritative: SetKnownAlpenglowBlockID can explicitly
// clear this tombstone if consensus later reports a safety conflict.
func (bs *BlockSource) RejectAlpenglowParentSwitch(event AlpenglowParentSwitch) bool {
	if event.SwitchSlot == 0 || event.ParentSlot+1 != event.SwitchSlot || event.ChildSlot < event.SwitchSlot || event.ChildID == (solana.Hash{}) {
		return false
	}

	bs.reorderMu.Lock()
	pending := bs.pendingAlpenglowParentSwitch
	if pending == nil || *pending != event {
		bs.reorderMu.Unlock()
		return false
	}
	child := bs.reorderBuffer[event.ChildSlot]
	if child == nil || !child.HasAlpenglowBlockID || solana.Hash(child.AlpenglowBlockID) != event.ChildID {
		bs.pendingAlpenglowParentSwitch = nil
		bs.reorderMu.Unlock()
		return false
	}
	delete(bs.reorderBuffer, event.ChildSlot)
	bs.pendingAlpenglowParentSwitch = nil
	bs.rejectAlpenglowBlockID(event.ChildSlot, event.ChildID)
	bs.reorderMu.Unlock()

	bs.slotStateMu.Lock()
	delete(bs.slotState, event.ChildSlot)
	delete(bs.inflightStart, event.ChildSlot)
	bs.slotStateMu.Unlock()
	bs.clearSlotErrors(event.ChildSlot)
	bs.discardTurbineSlotState(event.ChildSlot)
	bs.lastProgress.Store(time.Now().Unix())
	return true
}

// SetLastExecutedSlot is called by the replay loop after each block is fully executed.
// This allows accurate tip distance calculation without blocking on replay progress.
// Also triggers mode switching based on replay progress (not just tip polling).
//
// In near-tip mode, this also:
// - Schedules N+2 (prefetch while N+1 executes)
// - Immediately retries N+1 if it failed (don't wait for 200ms ticker)
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
				delete(bs.liveSynthesizedSkips, skippedSlot)
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

	if forcedUntil := bs.liveForceRPCUntil.Load(); forcedUntil != 0 && slot >= forcedUntil {
		if bs.liveForceRPCUntil.CompareAndSwap(forcedUntil, 0) {
			if cooldownUntil := bs.liveCooldownUntil.Load(); cooldownUntil != 0 && cooldownUntil > slot {
				mlog.Log.Infof("BLOCK SOURCE STATUS: missing Lightbringer slot recovered at slot %d; staying on RPC until slot %d before re-arming Lightbringer", slot, cooldownUntil)
			} else {
				mlog.Log.Infof("BLOCK SOURCE STATUS: RPC recovery for missing Lightbringer slot complete at slot %d; Lightbringer handoff may resume", slot)
			}
		}
	}
	if cooldownUntil := bs.liveCooldownUntil.Load(); cooldownUntil != 0 && slot >= cooldownUntil {
		if bs.liveCooldownUntil.CompareAndSwap(cooldownUntil, 0) {
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
					select {
					case bs.resultQueue <- fetchResult{slot: slot, err: errBeyondTip, rpcIdx: -1}:
					case <-bs.stopChan:
						return
					}
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
		select {
		case bs.resultQueue <- fetchResult{
			slot:      slot,
			block:     blk,
			err:       err,
			skipped:   skipped,
			rpcIdx:    rpcIdx,
			latencyMs: fetchLatency.Milliseconds(),
		}:
		case <-bs.stopChan:
			return
		}
	}
}

func (bs *BlockSource) emitReplayBlock(blk *b.Block) bool {
	select {
	case bs.streamChan <- blk:
		return true
	case <-bs.stopChan:
		return false
	}
}

// emitOrderedBlocks receives results and emits blocks in order
func (bs *BlockSource) emitOrderedBlocks() {
	for result := range bs.resultQueue {
		if result.block != nil && result.block.FromLiveStream {
			bs.finishLiveDelivery(result.slot)
		}
		var gapWaitingSlot uint64
		var gapFirstBufferedSlot uint64
		var gapFirstBufferedParentSlot uint64
		var gapBufferedCount int
		var shouldFallbackToRPC bool
		var isDuplicate bool
		var isHardErr bool
		var isHistoryErr bool
		var isRetriable bool

		if result.wakeEmitter {
			bs.reorderMu.Lock()
			goto emitConsecutive
		}
		if result.block != nil && !result.candidateObserved {
			if !bs.observeAlpenglowCandidateBlock(result.block) {
				bs.slotStateMu.Lock()
				delete(bs.slotState, result.slot)
				delete(bs.inflightStart, result.slot)
				bs.slotStateMu.Unlock()
				bs.clearSlotErrors(result.slot)
				continue
			}
		}
		if result.block != nil && result.block.FromLiveStream && bs.beforeLiveBlockCommit != nil {
			bs.beforeLiveBlockCommit(result.block)
		}

		bs.reorderMu.Lock()
		if result.block != nil && result.block.FromLiveStream {
			if reason, invalid := bs.invalidAlpenglowCandidateReason(result.block); invalid {
				bs.reorderMu.Unlock()
				bs.rejectInvalidAlpenglowCandidate(result.block, reason)
				bs.slotStateMu.Lock()
				delete(bs.slotState, result.slot)
				delete(bs.inflightStart, result.slot)
				bs.slotStateMu.Unlock()
				bs.clearSlotErrors(result.slot)
				continue
			}
		}

		if result.slot < bs.nextSlotToSend {
			// A late alternate child may arrive only after the source already
			// emitted farther down a competing suffix. Preserve it and notify
			// replay before the ordinary stale-result path drops it.
			if result.block != nil && result.block.FromLiveStream &&
				!bs.shouldDiscardLiveStreamResult(result.slot, result.liveStreamGeneration) &&
				bs.queueAlpenglowParentSwitchLocked(result.block) {
				bs.reorderBuffer[result.slot] = result.block
				bs.reorderMu.Unlock()
				continue
			}
			bs.slotStateMu.Lock()
			delete(bs.slotState, result.slot)
			delete(bs.inflightStart, result.slot)
			bs.slotStateMu.Unlock()
			bs.clearSlotErrors(result.slot)
			bs.reorderMu.Unlock()
			continue
		}

		if result.block != nil && result.block.FromLiveStream {
			if bs.shouldDiscardLiveStreamResult(result.slot, result.liveStreamGeneration) {
				bs.reorderMu.Unlock()
				continue
			}
			handoffSlot := bs.liveHandoffSlot.Load()
			if handoffSlot != 0 && result.slot >= handoffSlot {
				if existing := bs.reorderBuffer[result.slot]; existing != nil && !existing.FromLiveStream {
					delete(bs.reorderBuffer, result.slot)
				}
				if bs.skippedSlots[result.slot] {
					delete(bs.skippedSlots, result.slot)
					delete(bs.liveSynthesizedSkips, result.slot)
					delete(bs.alpenglowCertifiedSkips, result.slot)
				}
			}
			if existing := bs.reorderBuffer[result.slot]; bs.shouldPreferIncomingLiveBlockLocked(existing, result.block) {
				delete(bs.reorderBuffer, result.slot)
			}
		}

		// Check if this is a duplicate result (from backup request)
		// If slot is already done or we already have the block, discard this result
		bs.slotStateMu.Lock()
		isDuplicate = bs.slotState[result.slot] == slotDone
		bs.slotStateMu.Unlock()
		if !result.local && (isDuplicate || bs.reorderBuffer[result.slot] != nil || bs.skippedSlots[result.slot]) {
			bs.reorderMu.Unlock()
			continue
		}
		if result.local {
			delete(bs.reorderBuffer, result.slot)
			delete(bs.skippedSlots, result.slot)
			delete(bs.liveSynthesizedSkips, result.slot)
			delete(bs.alpenglowCertifiedSkips, result.slot)
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

		if result.block != nil && result.block.FromLiveStream {
			handoffSlot := bs.liveHandoffSlot.Load()
			// Catchup handoff and stall-rescue deliveries are legitimate FAR
			// from the tip — the same exemption shouldDiscardLiveStreamResult
			// carries. This inline twin of that gate ate the drained head a
			// second time after the intake fix: assembled, handoff armed,
			// queued, then silently dropped right here.
			catchupDelivery := (bs.repairCatchupActive() && handoffSlot != 0 && result.slot >= handoffSlot) ||
				bs.catchupRescueCovers(result.slot)
			if !catchupDelivery {
				if !bs.isNearTip.Load() {
					bs.reorderMu.Unlock()
					continue
				}
				if !bs.liveStreamActive.Load() && (handoffSlot == 0 || result.slot < handoffSlot) {
					bs.reorderMu.Unlock()
					continue
				}
			}
		}

		// Track error buckets
		// Note: isHardConnectivityErr is checked independently because hard errors
		// (connection refused, no such host) are NOT in isTransientNetworkErr anymore
		isHardErr = false
		isHistoryErr = false
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
		isRetriable = result.err != nil && !result.skipped

		if result.err != nil {
			bs.trackSlotError(result.slot, result.err, result.rpcIdx, result.latencyMs)
		}

		if result.skipped {
			bs.stats.FetchSkipped.Add(1)
			bs.skippedSlots[result.slot] = true
			delete(bs.liveSynthesizedSkips, result.slot)
			if result.rpcIdx >= 0 {
				// An RPC-provisional skip distrusts any older cert marker. A
				// certificate-driven skip (rpcIdx=-1, injected by the repair
				// drive) KEEPS its certified marker — post-handoff logic
				// trusts certified skips and discards provisional ones.
				delete(bs.alpenglowCertifiedSkips, result.slot)
			}
			bs.hardErrCount.Store(0)        // Reset on progress
			bs.clearSlotErrors(result.slot) // Clear stall diagnostics for this slot
			isRetriable = false
		} else if result.block != nil {
			// Success! This takes priority over any pending error results.
			if !result.block.FromLiveStream && !result.block.FromLocalProduction {
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

	emitConsecutive:
		// Emit consecutive blocks
		for {
			if quarantineFrom := bs.alpenglowQuarantineFrom.Load(); quarantineFrom != 0 && bs.nextSlotToSend >= quarantineFrom {
				// Replay is rewinding an invalid emitted suffix. Hold the frontier
				// until quarantine has drained every already-emitted descendant.
				break
			}
			if bs.applyAlpenglowDecisionLocked() {
				continue
			}
			bs.synthesizeAlpenglowParentLinkedSkipsLocked()
			if bs.queueBufferedAlpenglowParentSwitchLocked() {
				// Replay must unwind before any block on the selected alternate
				// branch can be emitted. The control event wakes NextBlock.
				break
			}

			if waitingSlot, observedParentSlot, expectedParentSlot, mismatch := bs.waitingLiveParentMismatchLocked(); mismatch {
				candidate := bs.reorderBuffer[waitingSlot]
				if bs.isRejectedAlpenglowBlock(candidate) {
					// A delayed assembly from a branch replay already discarded must
					// not reverse that switch. Remove only the source candidate; a
					// later parent-linked child or certificate decides whether this
					// slot is skipped or has another block identity.
					delete(bs.reorderBuffer, waitingSlot)
					bs.slotStateMu.Lock()
					delete(bs.slotState, waitingSlot)
					delete(bs.inflightStart, waitingSlot)
					bs.slotStateMu.Unlock()
					bs.clearSlotErrors(waitingSlot)
					mlog.Log.Warnf("ALPENGLOW rejected branch: dropping delayed block %s at slot %d so it cannot reverse an accepted speculative switch",
						solana.Hash(candidate.AlpenglowBlockID), waitingSlot)
					continue
				}
				// A complete Alpenglow child can expose a fork only after we have
				// emitted its competing suffix. Exact block-ID ancestry is enough
				// to choose that coherent branch speculatively, but replay owns the
				// account-state unwind. Hold child and wake replay through the
				// control channel; certificates can still override this branch.
				if bs.queueAlpenglowParentSwitchLocked(candidate) {
					break
				}
				if bs.turbineAlpenglowBlockIDHints && observedParentSlot == expectedParentSlot &&
					candidate != nil && candidate.HasAlpenglowParentBlockID && bs.hasLastEmittedAlpenglowBlockID {
					// Same parent SLOT but a different parent ID is a stale sibling
					// from the discarded branch. It cannot become connected without
					// another rewind, so release the assembler's completed marker and
					// repair the slot again instead of holding forever.
					observedParentID := solana.Hash(candidate.AlpenglowParentBlockID)
					if observedParentID != bs.lastEmittedAlpenglowBlockID {
						delete(bs.reorderBuffer, waitingSlot)
						bs.slotStateMu.Lock()
						delete(bs.slotState, waitingSlot)
						delete(bs.inflightStart, waitingSlot)
						bs.slotStateMu.Unlock()
						bs.discardTurbineSlotState(waitingSlot)
						bs.prioritizeTurbineRepairRange(waitingSlot, waitingSlot)
						mlog.Log.Warnf("ALPENGLOW stale sibling: dropping block %s at slot %d because parent id %s does not match selected parent %s at slot %d; re-repairing",
							solana.Hash(candidate.AlpenglowBlockID), waitingSlot, observedParentID, bs.lastEmittedAlpenglowBlockID, expectedParentSlot)
						continue
					}
				}
				// Shreds-only mode: no RPC arbiter exists for a parent
				// mismatch. Hold emission — the block stays buffered — and
				// let certificate adjudication resolve it: a decision naming
				// a different block discards and re-repairs it; a skip cert
				// clears it; a decision CONFIRMING it means our anchor is
				// wrong and the certified-switch sweep re-replays. break, not
				// continue: re-checking the same mismatch in this pass would
				// spin.
				if !bs.rpcBlockFetchAllowed() {
					now := time.Now()
					if last := time.Unix(bs.parentMismatchHoldUnix.Load(), 0); now.Sub(last) >= 30*time.Second {
						bs.parentMismatchHoldUnix.Store(now.Unix())
						mlog.Log.Warnf("turbine block at slot %d claims parent %d but the last emitted block is slot %d; RPC arbitration is disabled (block.rpc_fallback=false) — holding emission for certificate adjudication",
							waitingSlot, observedParentSlot, expectedParentSlot)
					}
					break
				}
				bs.reorderMu.Unlock()
				bs.forceRPCForLiveParentMismatch(waitingSlot, observedParentSlot, expectedParentSlot)
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
				bs.recordEmittedAlpenglowBlockIDLocked(blk)
				bs.replayEmissionWg.Add(1)
				bs.reorderMu.Unlock()
				if bs.beforeReplayBlockSend != nil {
					bs.beforeReplayBlockSend(blk)
				}

				repairingSlot := bs.isLiveRepairSlot(blk.Slot)
				if bs.usesLiveShredStream() {
					if blk.FromLiveStream {
						if repairingSlot {
							bs.clearLiveRepairSlot(blk.Slot)
						}
						if !bs.liveStreamActive.Swap(true) {
							if bs.rpcBlockFetchAllowed() {
								mlog.Log.Infof("BLOCK SOURCE SWITCH: RPC -> %s at slot %d | mode=%s", bs.liveShredStreamName(), blk.Slot, bs.currentModeString())
							} else {
								mlog.Log.Infof("BLOCK SOURCE: %s live block emission active at slot %d | mode=%s", bs.liveShredStreamName(), blk.Slot, bs.currentModeString())
							}
						}
					} else if blk.FromLocalProduction {
						// Local production is sequenced into replay so it can adopt
						// the producer SlotCtx; it is not a source handoff.
					} else if repairingSlot {
						bs.clearLiveRepairSlot(blk.Slot)
						mlog.Log.Infof("BLOCK SOURCE STATUS: missing streamed slot recovered via RPC at slot %d; staying on %s stream", blk.Slot, bs.liveShredStreamName())
					} else if bs.liveStreamActive.Swap(false) {
						mlog.Log.Warnf("BLOCK SOURCE SWITCH: %s -> RPC at slot %d | mode=%s", bs.liveShredStreamName(), blk.Slot, bs.currentModeString())
					}
				}

				if !bs.emitReplayBlock(blk) {
					bs.replayEmissionWg.Done()
					return
				}
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
				bs.replayEmissionWg.Done()
			} else if bs.skippedSlots[bs.nextSlotToSend] {
				if bs.shouldDiscardSkippedSlotAfterHandoff(bs.nextSlotToSend) {
					delete(bs.skippedSlots, bs.nextSlotToSend)
					delete(bs.liveSynthesizedSkips, bs.nextSlotToSend)
					delete(bs.alpenglowCertifiedSkips, bs.nextSlotToSend)
					continue
				}

				// Slot was skipped. Preserve whether this skip came from the live
				// stream/Alpenglow side so fallback can discard stale queued markers.
				skippedSlot := bs.nextSlotToSend
				liveStreamSkip := bs.liveSynthesizedSkips[skippedSlot] || bs.alpenglowCertifiedSkips[skippedSlot]
				delete(bs.skippedSlots, skippedSlot)
				delete(bs.liveSynthesizedSkips, skippedSlot)
				delete(bs.alpenglowCertifiedSkips, skippedSlot)
				bs.nextSlotToSend++
				bs.replayEmissionWg.Add(1)
				bs.reorderMu.Unlock()

				// Emit a minimal block with IsSkipped=true for logging
				skipBlock := &b.Block{
					Slot:           skippedSlot,
					IsSkipped:      true,
					FromLiveStream: liveStreamSkip,
				}

				if bs.usesLiveShredStream() {
					if bs.isLiveRepairSlot(skippedSlot) {
						bs.clearLiveRepairSlot(skippedSlot)
						mlog.Log.Infof("BLOCK SOURCE STATUS: missing streamed slot %d was confirmed skipped via RPC; staying on %s stream", skippedSlot, bs.liveShredStreamName())
					}
				}
				if !bs.emitReplayBlock(skipBlock) {
					bs.replayEmissionWg.Done()
					return
				}
				bs.replayEmissionWg.Done()

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

		gapWaitingSlot, gapFirstBufferedSlot, gapFirstBufferedParentSlot, gapBufferedCount, shouldFallbackToRPC = bs.detectLiveGapLocked()
		bs.maybeLogReorderGapLocked()
		bs.reorderMu.Unlock()
		if gapWaitingSlot != 0 {
			bs.prioritizeTurbineRepairForLiveGap(gapWaitingSlot, gapFirstBufferedParentSlot)
		}
		bs.maybeRescueStalledCatchupSlot()
		if shouldFallbackToRPC {
			bs.handleDetectedLiveGap(gapWaitingSlot, gapFirstBufferedSlot, gapFirstBufferedParentSlot, gapBufferedCount)
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
		if bs.stopped.Load() || bs.stopReasonEnum() != blockSourceStopReasonNone {
			return
		}

		// Check for stall timeout - if no progress for too long, trigger graceful shutdown
		lastProgressTime := time.Unix(bs.lastProgress.Load(), 0)
		stallDuration := time.Since(lastProgressTime)

		bs.maybeReconnectActiveLiveStreamForNoProgress(stallDuration)

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

		if bs.liveNeedRPCResume.CompareAndSwap(true, false) {
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
					// Pruned history is not retryable: the RPC will never have
					// this block again. Fail fast with an actionable stop
					// instead of spinning on the same slot forever.
					if waitingSlotInfo.lastErrorClass == "history_unavailable" && waitingSlotInfo.retryCount >= 6 {
						mlog.Log.Errorf("catchup slot %d is pruned on the RPC endpoint (%d attempts, history unavailable) — halting: %s",
							waitingSlot, waitingSlotInfo.retryCount, waitingSlotInfo.lastError)
						bs.setStopReason(blockSourceStopReasonHistoryUnavailable, waitingSlot)
						bs.stallError.Store(true)
						return // exit scheduler — same shutdown path as the stall halt
					}
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

// InjectLocalBlock queues a fully frozen locally produced block through the
// same ordered emitter used by network blocks so replay can adopt the
// already-mutated producer SlotCtx in slot order.
func (bs *BlockSource) InjectLocalBlock(blk *b.Block) bool {
	if bs == nil || blk == nil {
		return false
	}
	blk.FromLocalProduction = true
	select {
	case bs.resultQueue <- fetchResult{slot: blk.Slot, block: blk, rpcIdx: -1, local: true}:
		return true
	case <-bs.stopChan:
		return false
	}
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

	// Start tip poller.
	bs.backgroundWg.Add(1)
	go func() {
		defer bs.backgroundWg.Done()
		bs.pollTip()
	}()

	// Cert-driven repair: steer turbine toward certified-but-unobserved blocks.
	if bs.sourceType == BlockSourceTurbine && bs.turbineAlpenglowBlockIDHints && bs.alpenglowWantedBlocksFn != nil {
		bs.backgroundWg.Add(1)
		go func() {
			defer bs.backgroundWg.Done()
			bs.alpenglowRepairLoop()
		}()
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

	var localBlocksWg sync.WaitGroup
	if bs.localBlocks != nil {
		localBlocksWg.Add(1)
		go func() {
			defer localBlocksWg.Done()
			for {
				select {
				case <-bs.stopChan:
					return
				case blk, ok := <-bs.localBlocks:
					if !ok || bs.stopped.Load() || !bs.InjectLocalBlock(blk) {
						return
					}
				}
			}
		}()
	}

	// Start scheduler (runs until all slots done)
	bs.scheduler()

	if bs.stopReasonEnum() == blockSourceStopReasonNone && !bs.stopped.Load() {
		bs.reorderMu.Lock()
		waitingSlot := bs.nextSlotToSend
		bs.reorderMu.Unlock()
		bs.setStopReason(blockSourceStopReasonUnexpectedLiveEnd, waitingSlot)
	}

	// Shutdown. Stop is also called by replay when it abandons this source for
	// a rooted fork recovery. Keeping the close idempotent lets that external
	// teardown race safely with an ordinary scheduler completion.
	bs.Stop()
	close(bs.workQueue)
	wg.Wait()
	bs.liveStreamWg.Wait()
	bs.backgroundWg.Wait()
	localBlocksWg.Wait()
	close(bs.resultQueue)
	<-emitterDone
	close(bs.streamChan)
}

// Stop asks every component owned by this block source to exit. Callers that
// need ownership to be fully released (notably an in-process fork-replay that
// will bind the same Turbine socket and reopen the same shred spool) must also
// join the goroutine running Start.
//
// Stop is intentionally idempotent: the scheduler may finish at the same time
// as replay decides to abandon an attempt.
func (bs *BlockSource) Stop() {
	if bs == nil {
		return
	}
	bs.stopped.Store(true)
	bs.stopOnce.Do(func() {
		close(bs.stopChan)
	})
}

// startSequential is the old sequential approach for non-RPC sources
func (bs *BlockSource) startSequential() {
	for ; bs.currentSlot < bs.endSlot; bs.currentSlot++ {
		newBlock, _ := bs.fetchAndParseBlockSequential(bs.currentSlot)
		if newBlock != nil {
			if !bs.emitReplayBlock(newBlock) {
				break
			}
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
			blk, err = block.FromBlockResult(blockResult, slot, rpc)
			if err != nil {
				return nil, err
			}
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
		blk, err = block.FromBlockResult(blockResult, slot, rpc)
		if err != nil {
			return nil, err
		}
	}

	return blk, nil
}

func (bs *BlockSource) NextBlock() *b.Block {
	block := <-bs.streamChan
	return block
}

// NextBlockOrAlpenglowParentSwitch lets replay react to a parent-linked fork
// while normal block emission is intentionally held. The fast pre-check gives
// an already-queued switch priority over speculative blocks buffered just
// before the alternate child exposed the fork.
func (bs *BlockSource) NextBlockOrAlpenglowParentSwitch(ctx context.Context) (*b.Block, *AlpenglowParentSwitch) {
	select {
	case event := <-bs.alpenglowParentSwitchCh:
		return nil, &event
	default:
	}
	select {
	case event := <-bs.alpenglowParentSwitchCh:
		return nil, &event
	case block := <-bs.streamChan:
		return block, nil
	case <-ctx.Done():
		return nil, nil
	}
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
	case blockSourceStopReasonInvalidAlpenglowCertificate:
		return fmt.Sprintf("halted because an Alpenglow certificate named an objectively invalid block at slot %d", stopSlot)
	case blockSourceStopReasonHistoryUnavailable:
		return fmt.Sprintf("RPC history unavailable for slot %d — the endpoint's ledger retention has pruned it. Options: (1) use an RPC endpoint that retains older blocks, (2) raise block.repair_catchup_max_gap_slots so the gap fills via turbine repair instead (peers serve repair from their blockstores), or (3) re-bootstrap from a fresher snapshot (--bootstrap new-snapshot)", stopSlot)
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
		handoffSlot := bs.liveHandoffSlot.Load()
		cooldownUntil := bs.liveCooldownUntil.Load()
		connected := bs.liveStreamConnected.Load()
		lastStreamSlot := bs.liveLastStreamSlot.Load()
		if bs.liveStreamActive.Load() {
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
				waitReason := bs.liveHandoffWaitReason(waitingSlot, anchorSlot)
				if lastStreamSlot != 0 {
					return "rpc", fmt.Sprintf("rpc, %s connected (latest streamed slot %d); %s", source, lastStreamSlot, waitReason), 0
				}
				return "rpc", fmt.Sprintf("rpc, %s connected; %s", source, waitReason), 0
			}
			if bs.liveStreamStarted.Load() {
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
