package metrics

import (
	"encoding/binary"
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

// Transaction timing sampling.
//
// The per-transaction and per-instruction timers (the "Tx-level" and
// "Ix-level" BlockReplay fields below) cost two clock reads and two contended
// atomic adds each, roughly a dozen times per transaction and half a dozen
// times per instruction, from every executor goroutine at once. On a block of
// 6,000 System transfers that is comparable to the work being measured.
//
// Instead of recording every transaction, replay records one in
// 2^TxTimingSampleShift of them and scales each sampled observation by
// 2^TxTimingSampleShift, so per-block sums keep their wall-clock meaning
// (milliseconds per block, share of ProcessBlock) and Count/SumNanoseconds
// ratios are unbiased per-call averages; only the variance changes. A shift of
// zero restores exact, unsampled recording. Block-level timers are never
// sampled.
//
// The choice is made once per transaction by TxTimingSampled so every timer
// within a transaction, including the instruction dispatch and sBPF timers,
// sees the same decision. It is derived from the transaction's first
// signature, so it is stateless, uniform (ed25519 R is a random point), and
// the same transactions are sampled by every node and every replay of the
// same block. Transactions without a real signature (RPC simulation) fall back
// to a round-robin counter.
const maxTxTimingSampleShift = 7

var txTimingSampleShift atomic.Uint32
var txTimingSampleCounter atomic.Uint64

func init() {
	txTimingSampleShift.Store(DefaultTxTimingSampleShift)
}

// DefaultTxTimingSampleShift samples one transaction in eight.
const DefaultTxTimingSampleShift = 3

// TxTimingSampleShift returns the current sampling shift (0 = every
// transaction is recorded).
func TxTimingSampleShift() uint32 {
	return txTimingSampleShift.Load()
}

// SetTxTimingSampleShift sets the sampling shift, clamped to
// [0, maxTxTimingSampleShift], and returns the previous value. It is intended
// for start-up configuration and tests; changing it while blocks are being
// replayed mixes scales within a block.
func SetTxTimingSampleShift(shift uint32) (previous uint32) {
	if shift > maxTxTimingSampleShift {
		shift = maxTxTimingSampleShift
	}
	return txTimingSampleShift.Swap(shift)
}

// TxTimingSampled reports whether the transaction with the given first
// signature records its transaction- and instruction-level timings.
func TxTimingSampled(sig []byte) bool {
	shift := txTimingSampleShift.Load()
	if shift == 0 {
		return true
	}
	mask := uint64(1)<<shift - 1
	if len(sig) >= 8 {
		if r := binary.LittleEndian.Uint64(sig); r != 0 {
			return r&mask == 0
		}
	}
	return txTimingSampleCounter.Add(1)&mask == 0
}

// AddSampledTiming records one observation from a sampled transaction, scaled
// by the sampling rate so that sums over a block estimate the unsampled total.
func (t *Timing) AddSampledTiming(d time.Duration) {
	shift := txTimingSampleShift.Load()
	atomic.AddUint64(&t.Count, 1<<shift)
	atomic.AddUint64(&t.SumNanoseconds, uint64(d.Nanoseconds())<<shift)
}

// AddSampledTimingSince is AddTimingSince for timers started with
// StartTiming(TxTimingSampled(...)): a zero start records nothing.
func (t *Timing) AddSampledTimingSince(start time.Time) {
	if !start.IsZero() {
		t.AddSampledTiming(time.Since(start))
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
