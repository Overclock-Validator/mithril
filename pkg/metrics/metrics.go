package metrics

import (
	"sync/atomic"
	"time"
)

type Timing struct {
	Count          uint64
	SumNanoseconds uint64
}

func (t *Timing) AddTiming(d time.Duration) {
	atomic.AddUint64(&t.Count, 1)
	atomic.AddUint64(&t.SumNanoseconds, uint64(d.Nanoseconds()))
}

// StartTiming avoids reading the clock when a caller does not record timings.
func StartTiming(enabled bool) time.Time {
	if enabled {
		return time.Now()
	}
	return time.Time{}
}

func (t *Timing) AddTimingSince(start time.Time) {
	if !start.IsZero() {
		t.AddTiming(time.Since(start))
	}
}

// AccountLoader is the per-slot decomposition of LoadBlockAccounts. Counters
// describe logical loader work; allocation counters cover objects/data created
// directly by the batch loader rather than runtime or Pebble internals.
type AccountLoader struct {
	AddressTableLookups Timing
	DedupeBlockAccounts Timing
	SourceBatch         Timing
	ParentMapBuild      Timing
	SysvarUpdates       Timing

	SysvarClockRead             Timing
	SysvarSlotHashesRead        Timing
	SysvarRecentBlockhashesRead Timing
	SysvarSlotHistoryRead       Timing
	SysvarStakeHistoryRead      Timing
	SysvarLastRestartSlotRead   Timing

	WorkingSetLookup     Timing
	InProgressLookup     Timing
	AppendVecPinWait     Timing
	ReadCacheEpochWait   Timing
	CacheLookup          Timing
	AdmissionFilter      Timing
	IndexLookup          Timing
	ReadPlanning         Timing
	AppendVecRead        Timing
	CachePublicationWait Timing
	CachePublication     Timing

	SysvarWorkingSetLookup      Timing
	SysvarClone                 Timing
	SysvarAppendVecPinWait      Timing
	SysvarInProgressLookup      Timing
	SysvarReadCacheEpochWait    Timing
	SysvarCacheLookup           Timing
	SysvarIndexAndAppendVecRead Timing
	SysvarCachePublicationWait  Timing
	SysvarCachePublication      Timing

	RequestedKeys  uint64
	DurableKeys    uint64
	ParentAccounts uint64

	WorkingSetHits    uint64
	InProgressHits    uint64
	PendingFoldHits   uint64
	CacheHits         uint64
	IndexHits         uint64
	IndexMisses       uint64
	UniqueAppendVecs  uint64
	AppendVecChunks   uint64
	AppendVecAccounts uint64
	OpenFailures      uint64
	ReadFailures      uint64
	RetryAccounts     uint64

	CommonCacheAdmissions        uint64
	CommonCacheAdmissionsSkipped uint64
	VoteCacheAdmissions          uint64
	VoteCacheAdmissionsSkipped   uint64
	CachePublicationEpochRejects uint64

	DecodedAccountObjects uint64
	DecodedAccountBytes   uint64
	PlaceholderObjects    uint64

	SysvarReads                        uint64
	SysvarWorkingSetHits               uint64
	SysvarInProgressHits               uint64
	SysvarPendingFoldHits              uint64
	SysvarCacheHits                    uint64
	SysvarDurableReads                 uint64
	SysvarCachePublicationEpochRejects uint64
}

// TurbineIngress records per-slot pre-replay pipeline observations.
// It is written to replay_timings.jsonl without high-cardinality metric labels.
type TurbineIngress struct {
	ShredCollection      Timing
	CompletionQueueDelay Timing
	BlockDecode          Timing
	// Completion-only parse and outstanding-signature join/verification time.
	TransactionParse     Timing
	TransactionSigverify Timing
	ReplayAdmission      Timing
	// Summed completed prefetched component durations, including any discarded
	// optimistic prefix. Overlap reception; not CPU time or additive wall stages.
	// Early sigverify includes queueing.
	EarlyTransactionParse     Timing
	EarlyTransactionSigverify Timing
	// Completion wait for claimed background parsing/submission, outside BlockDecode.
	EarlyPreparationWait      Timing
	EarlyVerifiedTransactions uint64
	// FullToReady contains the completion stages above, excluding admission.
	FullToReady Timing
}

// VoteRewardDetails decomposes RewardCertificatePreflight and
// AlpenglowVoteRewards together. Certificate timers retain exact BLS verification;
// validator preparation measures only immutable epoch-material lookup/build work.
type VoteRewardDetails struct {
	ValidatorPreparation       Timing
	SkipCertificateValidation  Timing
	NotarCertificateValidation Timing
	FinalCertificateDecode     Timing
	FinalCertificateValidation Timing
	StatePreparation           Timing
	AccountMutation            Timing

	ValidatorCacheHits   uint64
	ValidatorCacheMisses uint64
	RewardValidators     uint64
	FinalSigners         uint64
	VoteAccountsUpdated  uint64
}

// StreamingExecution records execution that ran while a block's shreds were
// still arriving. Groups are the contiguous ready-batch sets executed per
// wake-up; Transactions counts what they executed. TxLoopBeforeFull is the
// group execution wall time that finished before the slot was fully
// assembled, i.e. the work hidden behind reception. OpenDelay runs from the
// header batch being decoded to the stream opening; the timeline fields and
// the OpenWait* timings below say what held the child (its parent's arrival,
// its parent's replay, or the loop itself). Discarded is 1 when a stream for
// this slot was thrown away and the block was executed whole; DiscardReason
// names why.
type StreamingExecution struct {
	Opened           uint64
	Groups           uint64
	Transactions     uint64
	TxLoopBeforeFull Timing
	OpenDelay        Timing
	Discarded        uint64
	DiscardReason    string

	// Timeline: wall-clock unix nanoseconds of the events that bound the
	// stream's open, zero when unknown. They join with the parent's record
	// (ParentFullNanos is the parent's FullNanos) and with external captures.
	//
	//   HeaderReadyNanos     the child's header batch was decoded
	//   HeaderSeenNanos      the executor first handled that header (from its
	//                        wake-up, or recovered from the assembler after a
	//                        dropped wake-up, in which case ready is the
	//                        lookup instant and seen follows it at once)
	//   ParentFullNanos      the parent's last shred (0: parent was a skip or
	//                        not a turbine block)
	//   ParentAdmittedNanos  the source handed the parent to replay (its own
	//                        ancestors replayed, the emitter released it)
	//   ParentReplayedNanos  the parent's replay result reached consensus and
	//                        the frontier advanced to it (0: unknown, e.g. the
	//                        frontier was re-based by a fork switch)
	//   OpenedNanos          the stream's bank opened
	//   FirstGroupStartNanos the first executed group started
	//   WaitEnteredNanos     the loop first entered the replay wait after the
	//                        parent was replayed (0: unknown)
	//   FullNanos            this block's last shred
	//   FinalizeStartNanos   the complete block reached the stream
	HeaderReadyNanos     int64
	HeaderSeenNanos      int64
	ParentFullNanos      int64
	ParentAdmittedNanos  int64
	ParentReplayedNanos  int64
	WaitEnteredNanos     int64
	OpenedNanos          int64
	FirstGroupStartNanos int64
	FullNanos            int64
	FinalizeStartNanos   int64

	// OpenDelay decomposed into attributable waits (each zero when the
	// timeline cannot support it):
	//   OpenWaitParentArrival  header ready → parent's last shred: the child's
	//                          header was decoded before its parent was even
	//                          fully received (a leader/arrival gap, not ours)
	//   OpenWaitParentReplay   parent's last shred (or header ready, whichever
	//                          is later) → parent replayed: the parent's own
	//                          post-full path held the child; split, when the
	//                          parent's admission is known, into
	//   OpenWaitParentQueue    … → the parent's admission: the parent's own
	//                          post-full path in the source (completion,
	//                          verification, and waiting for its ancestors —
	//                          a leader window's earlier slots still replaying)
	//   OpenWaitParentExec     admission → replayed: the parent's execution
	//                          and tail
	//   OpenWaitLoop           parent replayed (or header seen, whichever is
	//                          later) → opened: the replay loop's own latency
	//                          to open once nothing else stood in the way;
	//                          split, when the wait entry is known, into
	//   OpenWaitPostReplay     … → the loop's first wait entry after the
	//                          parent: the parent's post-replay tail
	//                          (promotion, RPC, stats) held the child
	//   OpenWaitDispatch       wait entry (or header seen) → opened: events
	//                          ahead of the header in the feed, the poll
	OpenWaitParentArrival Timing
	OpenWaitParentReplay  Timing
	OpenWaitParentQueue   Timing
	OpenWaitParentExec    Timing
	OpenWaitLoop          Timing
	OpenWaitPostReplay    Timing
	OpenWaitDispatch      Timing

	// NotOpenedReason is set when the block was executed whole without a
	// stream having opened for it: why the executor never opened one
	// ("header_not_seen", "declined:<eligibility>", "waiting_for_parent:…").
	// Empty when a stream opened (see Discarded for the ones thrown away).
	NotOpenedReason string
}

// Metrics for replaying a single block
type BlockReplay struct {
	Slot           uint64
	AccountLoader  AccountLoader
	TurbineIngress TurbineIngress

	// RewardCertificatePreflight runs before consensus admission and bank replay,
	// outside SlotReplay, which starts after the candidate is admitted.
	RewardCertificatePreflight Timing

	// Exact slot wall-clock closure: SlotReplay equals the sum of the disjoint
	// PreprocessBlock, ProcessBlock, and PostProcessBlock intervals. The more
	// detailed timers below are nested diagnostics and must not be added to that
	// top-level sum.
	SlotReplay                   Timing
	PreprocessBlock              Timing
	ProcessBlock                 Timing
	TransactionExecutionPlan     Timing
	TransactionStatusValidation  Timing
	DependencyPlannerPreparation Timing
	LoadBlockAccounts            Timing
	SlotCtxSetup                 Timing
	// DependencyPlannerBuild is measured account-extraction plus graph/batch
	// construction work, independent of which planner route a block uses and
	// excluding goroutine scheduling delay. On the prepared route some or all
	// of it overlaps LoadBlockAccounts and it is therefore not a top-level
	// additive phase. DependencyPlannerWait is the residual join nested within
	// TxLoop. DependencyPlannerDispatch runs from a ready plan until its final
	// transaction wave is enqueued, excluding the final wave's execution, and
	// is also nested within TxLoop.
	DependencyPlannerBuild          Timing
	DependencyPlannerWait           Timing
	DependencyPlannerDispatch       Timing
	DependencyPlannerPrepared       uint64
	DependencyPlannerFallback       uint64
	TxLoop                          Timing
	Reward                          Timing
	Rent                            Timing
	RunIncinerator                  Timing
	AlpenglowFooterClock            Timing
	AlpenglowVoteRewards            Timing
	VoteRewardDetails               VoteRewardDetails
	CompileWritableAndModifiedAccts Timing
	EnsureParentAccountsForModified Timing
	// BlockUpdateAccounts is synchronous critical-path work: rooted-tail
	// buffering (including its callback) or legacy store enqueue. It excludes
	// legacy asynchronous disk completion.
	BlockUpdateAccounts     Timing
	TransactionStatusCommit Timing
	// Preparation overlaps execution and is not additive with replay wall time.
	// PreparationWait is the residual join nested within TransactionStatusCommit.
	TransactionStatusPreparation     Timing
	TransactionStatusPreparationWait Timing
	SignatureVerificationJoin        Timing
	AccountsDeltaHash                Timing
	LtHashDedupe                     Timing
	LtHashWorkerCompute              Timing
	LtHashPartialReduce              Timing
	BankHashFinalize                 Timing
	BankHash                         Timing
	AlpenglowFooterVerification      Timing
	// PostProcessBlock is caller-side state publication and replay
	// bookkeeping after ProcessBlock returns. TransactionStatusView,
	// ChainTipUpdate, and ResumeContext are nested sub-phases; logging, summary
	// generation, and metric I/O are deliberately excluded.
	PostProcessBlock      Timing
	TransactionStatusView Timing
	ChainTipUpdate        Timing
	ResumeContext         Timing

	// FullToReplayed is the vote-path latency Mithril controls: wall time from
	// the last shred of a turbine block being assembled (the assembler's fullAt)
	// to the replay result being handed to consensus. Absent for blocks that
	// did not arrive as shreds. It is the number streaming execution reduces.
	FullToReplayed Timing
	// StreamingExecution summarizes any execution that overlapped shred
	// reception for this block; all zero when the block was executed whole.
	StreamingExecution StreamingExecution

	LtHashInputAccounts     uint64
	LtHashUniqueAccounts    uint64
	LtHashUnchangedAccounts uint64
	LtHashCreatedAccounts   uint64
	LtHashDeletedAccounts   uint64
	LtHashOldDataBytes      uint64
	LtHashNewDataBytes      uint64

	// Tx-level latencies summed for all the txs in a block.
	InstructionsAndAccountMetasFromTx  Timing
	ComputeBudgetExecutionInstructions Timing
	AccountsFromTx                     Timing
	PreBalanceDivergenceCheck          Timing
	CalcAndDeductFees                  Timing
	ReadRentSysvar                     Timing
	PreTxRentStates                    Timing
	IxLoop                             Timing
	PostTxRentStates                   Timing
	PostBalanceDivergenceCheck         Timing
	// TxUpdateAccounts is the inclusive successful-transaction publication
	// total. The TxPublish* fields below are nested children and must not be
	// added to it. TouchedAccountState intentionally covers the complete scan,
	// zero-lamport cleanup, MemAccounts.SetAccount, and RecordModifiedAcct loop:
	// separating those calls requires observer-costly per-account clocks.
	TxUpdateAccounts                 Timing
	TxPublishRecordWritableAcct      Timing
	TxPublishTouchedAccountState     Timing
	TxPublishStakeVoteBookkeeping    Timing
	TxPublicationTouchedAccounts     uint64
	TxPublicationTouchedAccountBytes uint64

	// TxFailedUpdateAccounts is the inclusive publication total for failed
	// transactions that still charge the payer and may advance a durable nonce.
	// Preparation, payer, and nonce timers are nested children.
	TxFailedUpdateAccounts         Timing
	TxFailedPublicationPreparation Timing
	TxFailedPayerPublication       Timing
	TxFailedNoncePublication       Timing

	// Sigverify is summed asynchronous worker time. It overlaps other wall-clock
	// phases; only SignatureVerificationJoin above is a disjoint blocking phase.
	Sigverify Timing

	// Ix-level latencies summed across all the instructions in a block.
	GetNextIxCtx                            Timing
	NextIxCtxConfigure                      Timing
	IxPush                                  Timing
	IxPop                                   Timing
	ExecIxResolveNativeProgram              Timing
	ExecIxNativeProgramSystem               Timing
	ExecIxNativeProgramStake                Timing
	ExecIxNativeProgramVote                 Timing
	ExecIxNativeProgramComputeBudget        Timing
	ExecIxNativeProgramBpfLoader2           Timing
	ExecIxNativeProgramBpfLoaderDeprecated  Timing
	ExecIxNativeProgramBpfLoaderUpgradeable Timing
	ExecIxNativeProgramZkElgamalProof       Timing
	ExecIxNativeProgramEd25519Precompile    Timing
	ExecIxNativeProgramSecp256kPrecompile   Timing
	FixupInstructionsSysvarAccount          Timing
	InstructionAccountsFromAccountMetas     Timing

	// BPF Loader
	SbpfInterpreterNew               Timing
	SbpfInterpreterRun               Timing
	AddProgramToCache                Timing
	GetProgramAccount                Timing
	GetProgramDataCached             Timing
	GetProgramDataUncachedAccountsDb Timing
	GetProgramDataUncachedAccounts   Timing
	GetProgramDataUncachedMarshal    Timing
}

var GlobalBlockReplay = BlockReplay{}

// Counter is a monotonic atomic counter for events. Operations are
// lock-free; use it for high-frequency increments where Timing's
// nanosecond accumulator is unnecessary.
type Counter struct {
	value uint64
}

func (c *Counter) Inc() {
	atomic.AddUint64(&c.value, 1)
}

func (c *Counter) Get() uint64 {
	return atomic.LoadUint64(&c.value)
}

// Simulate tracks RPC simulateTransaction handler events. Counters are
// surfaced through the standard metrics endpoint; latency uses the same
// Timing type as block replay so dashboards can reuse existing rendering.
type Simulate struct {
	TotalCalls         Counter
	SanitizeFailures   Counter
	AddressLookupFails Counter
	NonceFallbackHits  Counter
	Errors             Counter
	Successes          Counter
	HandlerLatency     Timing
}

var GlobalSimulate = Simulate{}
