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

func (t *Timing) AddTimingSince(start time.Time) {
	t.AddTiming(time.Since(start))
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

// TurbineIngress is the exact per-slot pre-replay pipeline decomposition.
// It is written to replay_timings.jsonl without high-cardinality metric labels.
type TurbineIngress struct {
	ShredCollection      Timing
	CompletionQueueDelay Timing
	BlockDecode          Timing
	TransactionParse     Timing
	TransactionSigverify Timing
	ReplayAdmission      Timing
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
	// DependencyPlannerBuild is the graph/batch construction time nested
	// within TxLoop. DependencyPlannerDispatch is the scheduler's wall-clock
	// lifetime, including dependency waits, and is also nested within TxLoop.
	DependencyPlannerBuild          Timing
	DependencyPlannerDispatch       Timing
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
	BlockUpdateAccounts         Timing
	TransactionStatusCommit     Timing
	SignatureVerificationJoin   Timing
	AccountsDeltaHash           Timing
	LtHashDedupe                Timing
	LtHashWorkerCompute         Timing
	LtHashPartialReduce         Timing
	BankHashFinalize            Timing
	BankHash                    Timing
	AlpenglowFooterVerification Timing
	// PostProcessBlock is caller-side state publication and replay
	// bookkeeping after ProcessBlock returns. TransactionStatusView,
	// ChainTipUpdate, and ResumeContext are nested sub-phases; logging, summary
	// generation, and metric I/O are deliberately excluded.
	PostProcessBlock      Timing
	TransactionStatusView Timing
	ChainTipUpdate        Timing
	ResumeContext         Timing

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
