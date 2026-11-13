package replay

import (
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// ReplayDiagnosticCapacity bounds retained completed replay-row observations.
const ReplayDiagnosticCapacity = 256

// ReplayDiagnosticScope identifies what the recorded operation timings cover.
type ReplayDiagnosticScope uint8

// ReplayDiagnosticRowScope denotes measurements retained in the closing row.
const ReplayDiagnosticRowScope ReplayDiagnosticScope = 1

// String returns the fixed scope name, or empty for an unknown scope.
func (s ReplayDiagnosticScope) String() string {
	if s == ReplayDiagnosticRowScope {
		return "replay_row"
	}
	return ""
}

// ReplayDiagnosticDiscardReason bounds the last recorded discard's category.
// It does not retain producer error strings or identify the discarded slot.
type ReplayDiagnosticDiscardReason uint8

const (
	ReplayDiscardNone ReplayDiagnosticDiscardReason = iota
	ReplayDiscardOtherBlock
	ReplayDiscardParentChanged
	ReplayDiscardForkSwitch
	ReplayDiscardTimeout
	ReplayDiscardCancelled
	ReplayDiscardGenerationGone
	ReplayDiscardInputRejected
	ReplayDiscardFinalization
	ReplayDiscardShutdown
	ReplayDiscardOther
)

// String returns the fixed category name, or empty for an unknown category.
func (r ReplayDiagnosticDiscardReason) String() string {
	switch r {
	case ReplayDiscardNone:
		return "none"
	case ReplayDiscardOtherBlock:
		return "other_block"
	case ReplayDiscardParentChanged:
		return "parent_changed"
	case ReplayDiscardForkSwitch:
		return "fork_switch"
	case ReplayDiscardTimeout:
		return "timeout"
	case ReplayDiscardCancelled:
		return "cancelled"
	case ReplayDiscardGenerationGone:
		return "generation_gone"
	case ReplayDiscardInputRejected:
		return "input_rejected"
	case ReplayDiscardFinalization:
		return "finalization"
	case ReplayDiscardShutdown:
		return "shutdown"
	case ReplayDiscardOther:
		return "other"
	default:
		return ""
	}
}

// ReplayDiagnosticStreaming copies the legacy row's streaming observations.
// Open/discard flags are not attempt counts. Group/transaction counters can
// include discarded work, or be rebuilt from an accepted stream spanning rows.
// They cannot establish per-attempt costs or the discarded slot/transaction count.
type ReplayDiagnosticStreaming struct {
	OpenedObserved, DiscardObserved      bool
	RecordedGroups, RecordedTransactions uint64
	LastDiscardReason                    ReplayDiagnosticDiscardReason
	AttemptBreakdownUnavailable          bool
}

// ReplayDiagnosticStage indexes fixed stage names in a replay row. A stage may
// repeat across speculative work and its whole-block fallback.
type ReplayDiagnosticStage uint8

const (
	ReplayStageParameterArena ReplayDiagnosticStage = iota
	ReplayStageDependencyPlanner
	ReplayStageLoadAccounts
	ReplayStageTransactionLoop
	ReplayStageDistributeFees
	ReplayStageCollectRent
	ReplayStageIncinerator
	ReplayStageCompileAccounts
	ReplayStageBankHash
	ReplayStageStoreAccounts
	ReplayStageDone
	ReplayStageStreamingWait
	ReplayStageStreamingSignatureWait
	ReplayDiagnosticStageCount
)

// String returns the producer's fixed stage name, or empty for an unknown stage.
func (s ReplayDiagnosticStage) String() string {
	switch s {
	case ReplayStageParameterArena:
		return "parameter_arena"
	case ReplayStageDependencyPlanner:
		return "prepare_dependency_planner"
	case ReplayStageLoadAccounts:
		return "load_accounts"
	case ReplayStageTransactionLoop:
		return "tx_loop"
	case ReplayStageDistributeFees:
		return "distribute_fees"
	case ReplayStageCollectRent:
		return "collect_rent"
	case ReplayStageIncinerator:
		return "run_incinerator"
	case ReplayStageCompileAccounts:
		return "compile_accounts"
	case ReplayStageBankHash:
		return "bankhash"
	case ReplayStageStoreAccounts:
		return "store_accounts"
	case ReplayStageDone:
		return "done"
	case ReplayStageStreamingWait:
		return "streaming_wait"
	case ReplayStageStreamingSignatureWait:
		return "streaming_sigverify_wait"
	default:
		return ""
	}
}

// ReplayDiagnosticTimings contains existing replay-row wall-time measurements.
// Only PreprocessBlock, ProcessBlock and PostProcessBlock partition SlotReplay.
// Other fields are nested or overlapping; LoopSourceWait can execute streaming
// callbacks, and FoldOverlap/DependencyPlannerBuild are not additive phases.
type ReplayDiagnosticTimings struct {
	SinceLastRow, LoopIteration, LoopSourceWait, LoopBeforeReplay, LoopAfterReplay metrics.Timing
	SlotReplay, PreprocessBlock, ProcessBlock, PostProcessBlock                    metrics.Timing
	ProcessBlockUnstaged, RewardCertificatePreflight                               metrics.Timing
	TransactionExecutionPlan, TransactionStatusValidation                          metrics.Timing
	DependencyPlannerPreparation, DependencyPlannerBuild                           metrics.Timing
	DependencyPlannerWait, DependencyPlannerDispatch                               metrics.Timing
	LoadBlockAccounts, SlotCtxSetup, TxLoop                                        metrics.Timing
	Reward, Rent, RunIncinerator                                                   metrics.Timing
	AlpenglowFooterClock, AlpenglowVoteRewards, AlpenglowFooterVerification        metrics.Timing
	CompileWritableAndModifiedAccts, EnsureParentAccountsForModified               metrics.Timing
	BlockUpdateAccounts, TransactionStatusCommit                                   metrics.Timing
	TransactionStatusPreparation, TransactionStatusPreparationWait                 metrics.Timing
	SignatureVerificationJoin, AccountsDeltaHash                                   metrics.Timing
	LtHashDedupe, LtHashWorkerCompute, LtHashPartialReduce, BankHashFinalize       metrics.Timing
	BankHash, TransactionStatusView, ChainTipUpdate, ResumeContext                 metrics.Timing
	FullToReplayed, FoldOverlap                                                    metrics.Timing
}

// ReplayDiagnosticWork contains replay-row summed operation times, not serial
// wall phases, measured CPU or accepted-bank-only work. It can include discarded
// speculative work and omit a stream's earlier rows. Shared transaction/BPF
// timers are omitted because RPC simulation can update them concurrently.
type ReplayDiagnosticWork struct {
	Sigverify, TxUpdateAccounts, TxFailedUpdateAccounts metrics.Timing
}

// ReplayDiagnostic pairs exact completed-bank metadata with the closing replay
// row's measurements. Scope applies to timings/work, not the completed-bank
// transaction/CU counts. Row work is not necessarily exclusive to that bank or
// its full execution. Completion does not establish selected-fork finality.
type ReplayDiagnostic struct {
	Sequence, CompletedIndex, Attempt, Rewinds uint64
	Slot, Epoch, BlockHeight                   uint64
	Bankhash                                   solana.Hash
	CompletedAt, ObservedAt                    time.Time
	Transactions, ComputeUnits                 uint64
	WorkCountersSaturated                      bool
	SharedExecutionTimingsUnavailable          bool
	// These row markers can both be true after speculative work and fallback.
	// False means not observed in this row, not that execution was sequential.
	DependencyPlannerPreparedObserved        bool
	DependencyPlannerFallbackObserved        bool
	Scope                                    ReplayDiagnosticScope
	Streaming                                ReplayDiagnosticStreaming
	Timings                                  ReplayDiagnosticTimings
	Work                                     ReplayDiagnosticWork
	Stages                                   [ReplayDiagnosticStageCount]metrics.Timing
	UnknownStages                            bool
	AccountLoader                            metrics.AccountLoader
	UnrootedRetainedBytes, UnrootedHeldSlots uint64
	RetentionLimits                          ReplayDiagnosticRetentionLimits
}

// ReplayDiagnosticRetentionLimits records the completed row's configured
// account-retention checks. Zero disables that check; usage may overshoot.
// These are not process-memory limits or a durable storage frontier.
type ReplayDiagnosticRetentionLimits struct {
	Observed             bool
	RetainedBytesPauseAt uint64
	HeldSlotsHaltAbove   uint64
}

// ReplayDiagnosticSnapshot is a detached oldest-to-newest bounded history.
// An empty initialized snapshot is distinct from a disabled/unstarted owner.
type ReplayDiagnosticSnapshot struct {
	RunID                                string
	Generation, Attempt, Rewinds         uint64
	StartedAt, ResetAt, UpdatedAt        time.Time
	FirstSequence, LastSequence, Evicted uint64
	Exhausted                            bool
	Records                              []ReplayDiagnostic
	FoldCapacity                         ReplayDiagnosticFoldCapacity
	NearReception                        ReplayDiagnosticNearReception
	RootedHistory                        RootedSlotHistory
}

type replayDiagnostics struct {
	// ponytail: one mutex bounds copies to 256 records; immutable paged chunks
	// are only needed if measured background snapshot contention is significant.
	mu                   sync.Mutex
	runID                string
	generation           uint64
	attempt, rewinds     uint64
	startedAt, resetAt   time.Time
	updatedAt            time.Time
	lastCompletion       uint64
	sequence, evicted    uint64
	exhausted            bool
	start, count         int
	records              [ReplayDiagnosticCapacity]ReplayDiagnostic
	foldCapacity         ReplayDiagnosticFoldCapacity
	foldCapacitySequence uint64
	nearReception        ReplayDiagnosticNearReception
	rootedHistory        RootedSlotHistory
}

// EnableReplayDiagnostics enables bounded completed replay-row capture before the
// first replay attempt. It does not enable any node/API publication by itself.
func (p *Progress) EnableReplayDiagnostics(runID string) error {
	if p == nil || runID == "" || len(runID) > 128 {
		return errors.New("replay diagnostics requires a progress owner and bounded run ID")
	}
	if p.latest.Load() != nil {
		return errors.New("replay diagnostics must be enabled before replay starts")
	}
	if !p.diagnostics.CompareAndSwap(nil, &replayDiagnostics{runID: runID}) {
		return errors.New("replay diagnostics is already enabled")
	}
	return nil
}

// ReplayDiagnosticsSnapshot copies retained records without reading live metrics.
func (p *Progress) ReplayDiagnosticsSnapshot() (ReplayDiagnosticSnapshot, bool) {
	if p == nil {
		return ReplayDiagnosticSnapshot{}, false
	}
	d := p.diagnostics.Load()
	if d == nil {
		return ReplayDiagnosticSnapshot{}, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.generation == 0 {
		return ReplayDiagnosticSnapshot{}, false
	}
	s := ReplayDiagnosticSnapshot{
		RunID: d.runID, Generation: d.generation, Attempt: d.attempt, Rewinds: d.rewinds,
		StartedAt: d.startedAt, ResetAt: d.resetAt, UpdatedAt: d.updatedAt,
		LastSequence: d.sequence, Evicted: d.evicted, Exhausted: d.exhausted,
		FoldCapacity: d.foldCapacity, NearReception: d.nearReception,
		RootedHistory: d.rootedHistory,
	}
	if d.count != 0 {
		s.Records = make([]ReplayDiagnostic, d.count)
		for i := range s.Records {
			s.Records[i] = d.records[(d.start+i)%ReplayDiagnosticCapacity]
		}
		s.FirstSequence = s.Records[0].Sequence
	}
	return s, true
}

func (p *Progress) resetReplayDiagnostics(s *ProgressSnapshot) {
	d := p.diagnostics.Load()
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.attempt, d.rewinds, d.startedAt = s.Attempt, s.Rewinds, s.StartedAt
	d.resetAt, d.updatedAt = s.UpdatedAt, s.UpdatedAt
	d.lastCompletion = s.ExecutedBlocks
	d.start, d.count, d.sequence, d.evicted = 0, 0, 0, 0
	clear(d.records[:])
	d.foldCapacity, d.foldCapacitySequence = ReplayDiagnosticFoldCapacity{}, 0
	d.nearReception = ReplayDiagnosticNearReception{}
	d.rootedHistory = RootedSlotHistory{}
	if d.generation == math.MaxUint64 || s.Attempt == 0 || d.exhausted {
		d.exhausted = true
		return
	}
	d.generation++
}

// recordReplayDiagnostic is called only by the replay owner after all work
// contributing to a completed row has joined and before that row is reset.
func (p *Progress) recordReplayDiagnostic(at time.Time, row *metrics.BlockReplay, tail *unrootedTail) {
	if p == nil {
		return
	}
	d := p.diagnostics.Load()
	if d == nil || row == nil {
		return
	}
	s, ok := p.Snapshot()
	if !ok || !s.HasCompleted || s.Completed.Slot != row.Slot || s.Completed.Bankhash == (solana.Hash{}) || s.Completed.At.IsZero() || at.IsZero() {
		return
	}
	d.mu.Lock()
	if d.exhausted || d.generation == 0 || d.attempt != s.Attempt || d.rewinds != s.Rewinds {
		d.mu.Unlock()
		return
	}
	if d.sequence == math.MaxUint64 || s.ExecutedBlocks == 0 {
		d.exhausted = true
		d.mu.Unlock()
		return
	}
	if s.ExecutedBlocks <= d.lastCompletion {
		d.mu.Unlock()
		return
	}
	var markerSource *blockstream.BlockSource
	var markerTarget *turbine.ShredHistoryHandle
	if d.nearReception.ObservedAt.IsZero() {
		source := p.source.Load()
		if started, receipt, ok := source.CachedReceptionObservation(); ok && p.source.Load() == source &&
			receipt.Progress.HasSlot && !receipt.Progress.CoverageLimited && !receipt.Progress.ValidUntil.IsZero() {
			if d.recordNearReceptionLocked(s.Completed, started, receipt, time.Now()) {
				markerSource, markerTarget = source, receipt.ShredHistoryTarget()
			}
		}
	}
	r := completedReplayDiagnostic(s, at, row)
	if tail != nil {
		r.RetentionLimits = ReplayDiagnosticRetentionLimits{Observed: true, RetainedBytesPauseAt: tail.maxBytes, HeldSlotsHaltAbove: tail.slotLimit()}
	}
	d.sequence++
	r.Sequence = d.sequence
	index := (d.start + d.count) % ReplayDiagnosticCapacity
	if d.count == ReplayDiagnosticCapacity {
		d.start = (d.start + 1) % ReplayDiagnosticCapacity
		d.evicted++
	} else {
		d.count++
	}
	d.records[index] = r
	d.lastCompletion, d.updatedAt = s.ExecutedBlocks, r.ObservedAt
	d.mu.Unlock()
	// The marker uses the accepted completion's original receiver and clock.
	// History admission never runs while the diagnostics lock is held.
	if markerTarget != nil {
		markerTarget.TryMarker(turbine.ShredHistoryMarker{At: s.completedAt, Slot: s.Completed.Slot,
			Bankhash: s.Completed.Bankhash, ReplayAttempt: s.Attempt, Rewinds: s.Rewinds, CompletedIndex: s.ExecutedBlocks})
	} else if markerSource != nil {
		markerSource.MarkReceptionHistoryUnknown()
	}
}

func completedReplayDiagnostic(s ProgressSnapshot, at time.Time, m *metrics.BlockReplay) ReplayDiagnostic {
	b := s.Completed
	r := ReplayDiagnostic{
		CompletedIndex: s.ExecutedBlocks, Attempt: s.Attempt, Rewinds: s.Rewinds,
		Slot: b.Slot, Bankhash: b.Bankhash, Epoch: b.Epoch, BlockHeight: b.BlockHeight,
		CompletedAt: b.At.UTC(), ObservedAt: at.UTC(), Transactions: b.Transactions,
		ComputeUnits: b.ComputeUnits, WorkCountersSaturated: b.WorkCountersSaturated,
		SharedExecutionTimingsUnavailable: true,
		DependencyPlannerPreparedObserved: m.DependencyPlannerPrepared != 0,
		DependencyPlannerFallbackObserved: m.DependencyPlannerFallback != 0,
		Scope:                             ReplayDiagnosticRowScope,
		Streaming: ReplayDiagnosticStreaming{
			OpenedObserved: m.StreamingExecution.Opened != 0, DiscardObserved: m.StreamingExecution.Discarded != 0,
			RecordedGroups: m.StreamingExecution.Groups, RecordedTransactions: m.StreamingExecution.Transactions,
			LastDiscardReason:           replayDiagnosticDiscardReason(m.StreamingExecution.Discarded != 0, m.StreamingExecution.DiscardReason),
			AttemptBreakdownUnavailable: true,
		},
		AccountLoader: m.AccountLoader, UnrootedRetainedBytes: m.UnrootedRetainedBytes,
		UnrootedHeldSlots: m.UnrootedHeldSlots,
		Timings: ReplayDiagnosticTimings{
			SinceLastRow: m.SinceLastRow, LoopIteration: m.LoopIteration, LoopSourceWait: m.LoopSourceWait,
			LoopBeforeReplay: m.LoopBeforeReplay, LoopAfterReplay: m.LoopAfterReplay,
			SlotReplay: m.SlotReplay, PreprocessBlock: m.PreprocessBlock, ProcessBlock: m.ProcessBlock, PostProcessBlock: m.PostProcessBlock,
			ProcessBlockUnstaged: m.ProcessBlockUnstaged, RewardCertificatePreflight: m.RewardCertificatePreflight,
			TransactionExecutionPlan: m.TransactionExecutionPlan, TransactionStatusValidation: m.TransactionStatusValidation,
			DependencyPlannerPreparation: m.DependencyPlannerPreparation, DependencyPlannerBuild: m.DependencyPlannerBuild,
			DependencyPlannerWait: m.DependencyPlannerWait, DependencyPlannerDispatch: m.DependencyPlannerDispatch,
			LoadBlockAccounts: m.LoadBlockAccounts, SlotCtxSetup: m.SlotCtxSetup, TxLoop: m.TxLoop,
			Reward: m.Reward, Rent: m.Rent, RunIncinerator: m.RunIncinerator,
			AlpenglowFooterClock: m.AlpenglowFooterClock, AlpenglowVoteRewards: m.AlpenglowVoteRewards, AlpenglowFooterVerification: m.AlpenglowFooterVerification,
			CompileWritableAndModifiedAccts: m.CompileWritableAndModifiedAccts, EnsureParentAccountsForModified: m.EnsureParentAccountsForModified,
			BlockUpdateAccounts: m.BlockUpdateAccounts, TransactionStatusCommit: m.TransactionStatusCommit,
			TransactionStatusPreparation: m.TransactionStatusPreparation, TransactionStatusPreparationWait: m.TransactionStatusPreparationWait,
			SignatureVerificationJoin: m.SignatureVerificationJoin, AccountsDeltaHash: m.AccountsDeltaHash,
			LtHashDedupe: m.LtHashDedupe, LtHashWorkerCompute: m.LtHashWorkerCompute, LtHashPartialReduce: m.LtHashPartialReduce, BankHashFinalize: m.BankHashFinalize,
			BankHash: m.BankHash, TransactionStatusView: m.TransactionStatusView, ChainTipUpdate: m.ChainTipUpdate, ResumeContext: m.ResumeContext,
			FullToReplayed: m.FullToReplayed, FoldOverlap: m.FoldOverlap,
		},
		Work: ReplayDiagnosticWork{
			Sigverify:        m.Sigverify,
			TxUpdateAccounts: m.TxUpdateAccounts, TxFailedUpdateAccounts: m.TxFailedUpdateAccounts,
		},
	}
	known := 0
	for stage := ReplayDiagnosticStage(0); stage < ReplayDiagnosticStageCount; stage++ {
		if value, ok := m.ReplayStages[stage.String()]; ok {
			r.Stages[stage] = value
			known++
		}
	}
	r.UnknownStages = known != len(m.ReplayStages)
	return r
}

func replayDiagnosticDiscardReason(discarded bool, reason string) ReplayDiagnosticDiscardReason {
	if !discarded {
		return ReplayDiscardNone
	}
	switch reason {
	case "other_block":
		return ReplayDiscardOtherBlock
	case "update_parent":
		return ReplayDiscardParentChanged
	case "fork_switch":
		return ReplayDiscardForkSwitch
	case "timeout":
		return ReplayDiscardTimeout
	case "gone":
		return ReplayDiscardGenerationGone
	case "shutdown":
		return ReplayDiscardShutdown
	case "decode_error", "versions", "plan", "prefix_bookkeeping", "execution_mask", "status_validation", "quarantined":
		return ReplayDiscardInputRejected
	}
	switch {
	case strings.HasPrefix(reason, "cancelled:"):
		return ReplayDiscardCancelled
	case strings.HasPrefix(reason, "prefix_mismatch:"):
		return ReplayDiscardInputRejected
	case strings.HasPrefix(reason, "finalize:"):
		return ReplayDiscardFinalization
	default:
		return ReplayDiscardOther
	}
}
