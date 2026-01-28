package blockstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/gagliardetto/solana-go/rpc"
	"golang.org/x/time/rate"
)

type BlockSourceType int

const (
	BlockSourceRpc = iota
	BlockSourceFile
	BlockSourceLightbringer
)

type BlockSourceOpts struct {
	RpcClient  *rpcclient.RpcClient // Primary RPC for block fetching (getBlock)
	SourceType BlockSourceType
	StartSlot  uint64
	EndSlot    uint64
	BlockDir   string

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
	slot      uint64
	block     *b.Block
	err       error
	skipped   bool  // true if SlotSkipped error
	rpcIdx    int32 // which RPC endpoint produced this result (for error attribution)
	latencyMs int64 // fetch latency in milliseconds (for stall diagnostics)
}

var errBeyondTip = errors.New("slot beyond confirmed tip")

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
	reorderMu      sync.Mutex
	reorderBuffer  map[uint64]*b.Block
	skippedSlots   map[uint64]bool
	nextSlotToSend uint64
	maxPending     int

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

	// Near-tip mode tracking
	isNearTip        atomic.Bool // True when close to confirmed tip
	catchupTipSafety uint64      // Original tip safety margin for catchup mode

	// Configurable mode thresholds
	nearTipThreshold    uint64        // Enter near-tip when gap <= this
	catchupThreshold    uint64        // Exit near-tip when gap >= this
	tipGateThreshold    uint64        // Only apply safety margin when gap > this
	nearTipPollInterval time.Duration // Faster poll in near-tip mode
	nearTipLookahead    uint64        // Slots ahead to schedule in near-tip

	// Stats tracking
	stats          BlockSourceStats
	statsResetTime atomic.Int64 // Unix timestamp when stats were last reset
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
	defaultNearTipThreshold = 32  // Switch to near-tip mode when gap <= this
	defaultCatchupThreshold = 64  // Switch back to catchup mode when gap >= this (hysteresis)
	defaultNearTipPollMs    = 500 // Faster tip polling in near-tip mode (ms)
	defaultNearTipLookahead = 2   // Schedule up to N slots ahead in near-tip mode
	// RPC latency ~300ms, execution ~100ms - need 1-2 slots buffered to avoid waiting
	// Note: tip_safety_margin is NOT applied in near-tip mode by design (we rely on retries)

	// Stall diagnostics thresholds
	stallHeartbeatThreshold = 2 * time.Minute  // Start logging heartbeats when stall exceeds this
	stallHeartbeatInterval  = 30 * time.Second // Log heartbeat every this interval

	// Tip gate threshold: only apply tip safety margin when gap > this
	// When gap <= 128, the gate causes more harm than good (bt storms, buffer drain)
	// because headroom becomes too small (e.g., gap=70, margin=64 → only 6 slots headroom)
	defaultTipGateThreshold = 128
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
		rpcClients:       rpcClients,
		streamChan:       make(chan *b.Block, streamChanBuffer),
		startSlot:        opts.StartSlot,
		endSlot:          opts.EndSlot,
		currentSlot:      opts.StartSlot,
		blockDir:         opts.BlockDir,
		sourceType:       opts.SourceType,
		rateLimiter:      rate.NewLimiter(rate.Limit(maxRPS), maxRPS),
		maxInflight:      maxInflight,
		tipSafetyMargin:  tipSafetyMargin,
		tipPollInterval:  time.Duration(tipPollMs) * time.Millisecond,
		reorderBuffer:    make(map[uint64]*b.Block),
		skippedSlots:     make(map[uint64]bool),
		nextSlotToSend:   opts.StartSlot,
		maxPending:       defaultMaxPending,
		slotState:        make(map[uint64]slotStatus),
		inflightStart:    make(map[uint64]time.Time),
		workQueue:        make(chan uint64, maxInflight*2),
		resultQueue:      make(chan fetchResult, maxInflight*2),
		stopChan:         make(chan struct{}),
		stallTimeout:     defaultStallTimeout,
		catchupTipSafety: tipSafetyMargin, // Store original for switching back to catchup

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

	// Log RPC configuration
	if len(rpcClients) > 1 {
		mlog.Log.Infof("Block fetching configured with %d RPC endpoints (primary + %d backups)",
			len(rpcClients), len(rpcClients)-1)
	}

	return bs
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
		}
	} else {
		// Currently in catchup mode - switch to near-tip if gap is small
		if gap <= bs.nearTipThreshold {
			bs.isNearTip.Store(true)
			mlog.Log.Infof("MODE SWITCH: catchup → NEAR-TIP | gap=%d (threshold=%d) | exec_slot=%d | tip=%d",
				gap, bs.nearTipThreshold, lastExecuted, tip)
		}
	}
}

// effectiveTipSafetyMargin returns the tip safety margin for the current mode.
// In near-tip mode, we return 0 (no margin) - we rely on fast retries instead.
func (bs *BlockSource) effectiveTipSafetyMargin() uint64 {
	if bs.isNearTip.Load() {
		return 0 // Near-tip mode: no safety margin, rely on retries
	}
	return bs.catchupTipSafety
}

// classifyError returns a string classification for an error
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
	mlog.Log.Errorf("  errors: slot_not_avail=%d rate_limited=%d beyond_tip=%d transient=%d other=%d",
		diag.ErrSlotNotAvail, diag.ErrRateLimited, diag.ErrBeyondTip, diag.ErrTransient, diag.ErrOther)

	// 5) Worker pool stats
	mlog.Log.Errorf("Worker pool: workers=%d rate_limit_rps=%.1f",
		diag.WorkersTotal, diag.RateLimitRPS)

	mlog.Log.Errorf("=== End %s ===", prefix)
}

func (bs *BlockSource) tryGetBlockFromFile(slot uint64) (*b.Block, error) {
	if bs.blockDir == "" {
		return nil, fmt.Errorf("no block directory specified")
	}
	blockFilename := filepath.Join(filepath.Clean(bs.blockDir), fmt.Sprintf("%d.json", slot))
	file, err := os.Open(blockFilename)
	if err != nil {
		return nil, fmt.Errorf("error opening blockFilename=%s: %w", blockFilename, err)
	}

	decoder := json.NewDecoder(file)
	out := &b.Block{}

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

	return b.FromBlockResult(blockResult, slot, rpc), nil
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
func (bs *BlockSource) SetLastExecutedSlot(slot uint64) {
	bs.lastExecutedSlot.Store(slot)
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
		bs.resultQueue <- fetchResult{slot: slot, block: blk, err: err, skipped: skipped, rpcIdx: rpcIdx, latencyMs: fetchLatency.Milliseconds()}
	}
}

// emitOrderedBlocks receives results and emits blocks in order
func (bs *BlockSource) emitOrderedBlocks() {
	for result := range bs.resultQueue {
		bs.reorderMu.Lock()

		// Check if this is a duplicate result (from backup request)
		// If slot is already done or we already have the block, discard this result
		bs.slotStateMu.Lock()
		isDuplicate := bs.slotState[result.slot] == slotDone
		bs.slotStateMu.Unlock()
		if isDuplicate || bs.reorderBuffer[result.slot] != nil || bs.skippedSlots[result.slot] {
			bs.reorderMu.Unlock()
			continue
		}

		// Track error buckets
		// Note: isHardConnectivityErr is checked independently because hard errors
		// (connection refused, no such host) are NOT in isTransientNetworkErr anymore
		isHardErr := false
		if result.err != nil {
			if result.err == errBeyondTip {
				// Already tracked in worker
			} else if isSlotNotAvailableErr(result.err) {
				bs.stats.ErrSlotNotAvail.Add(1)
			} else if isRateLimitedErr(result.err) {
				bs.stats.ErrRateLimited.Add(1)
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
		// Never skip a non-skipped slot - that causes silent state divergence.
		// If we truly can't fetch a block, we'll stall and eventually timeout.
		isRetriable := result.err != nil && !result.skipped

		// Track error for stall diagnostics
		if isRetriable {
			bs.trackSlotError(result.slot, result.err, result.rpcIdx, result.latencyMs)
		}

		if result.skipped {
			bs.stats.FetchSkipped.Add(1)
			bs.skippedSlots[result.slot] = true
			bs.hardErrCount.Store(0)        // Reset on progress
			bs.clearSlotErrors(result.slot) // Clear stall diagnostics for this slot
		} else if result.block != nil {
			// Success! This takes priority over any pending error results.
			bs.stats.FetchSuccesses.Add(1)
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
			bs.scheduleRetry(result.slot)
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
			if blk, ok := bs.reorderBuffer[bs.nextSlotToSend]; ok {
				delete(bs.reorderBuffer, bs.nextSlotToSend)
				bs.reorderMu.Unlock()
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
				// Slot was skipped (SlotSkipped from RPC) - emit a skip marker block
				skippedSlot := bs.nextSlotToSend
				delete(bs.skippedSlots, skippedSlot)
				bs.nextSlotToSend++
				bs.reorderMu.Unlock()

				// Emit a minimal block with IsSkipped=true for logging
				skipBlock := &b.Block{
					Slot:      skippedSlot,
					IsSkipped: true,
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

		bs.reorderMu.Unlock()
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

// scheduleSlot schedules a slot if not already scheduled
func (bs *BlockSource) scheduleSlot(slot uint64) bool {
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

			bs.stallError.Store(true)
			return // Exit scheduler, which triggers shutdown
		}

		// Check if all slots are done
		bs.reorderMu.Lock()
		allDone := bs.nextSlotToSend >= bs.endSlot
		bs.reorderMu.Unlock()

		if allDone {
			return
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
			waitingSlot := bs.nextSlotToSend
			bs.reorderMu.Unlock()

			for _, slot := range bs.getRetrySlots() {
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
						bs.scheduleRetry(slot)
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

		// Schedule new slots if we have capacity
		if bs.canScheduleMore(nextToSchedule) && nextToSchedule < bs.endSlot {
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
	// For non-RPC sources, use the old sequential approach
	if bs.sourceType != BlockSourceRpc {
		bs.startSequential()
		return
	}

	// Start tip poller
	go bs.pollTip()

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

	// Shutdown
	bs.stopped.Store(true)
	close(bs.stopChan)
	close(bs.workQueue)
	wg.Wait()
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
			blk = b.FromBlockResult(blockResult, slot, rpc)
		}
	} else if bs.sourceType == BlockSourceLightbringer {
		// NOTE: BlockSourceLightbringer is TEMPORARILY NON-FUNCTIONAL.
		// The background block downloader that populated files for this path was removed.
		// This code path will return SlotSkipped for every slot until Lightbringer streaming
		// is re-implemented as a push-based source feeding directly into the reorder buffer.
		// TODO: Implement Lightbringer as a streaming source (gRPC stream -> reorder buffer)
		blk, err = bs.tryGetBlockFromFile(slot)
		if err != nil {
			return nil, rpcclient.SlotSkipped
		}
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
	bs.stats.ErrTransient.Store(0)
	bs.stats.ErrOther.Store(0)
	bs.stats.TotalFetchLatencyNs.Store(0)
	bs.stats.FetchLatencyCount.Store(0)
	// Reset tip poll failures for this window (but keep consecutive count for alerting)
	bs.totalTipPollFails.Store(0)
	// Reset the stats window start time for RPS calculation
	bs.statsResetTime.Store(time.Now().Unix())
}
