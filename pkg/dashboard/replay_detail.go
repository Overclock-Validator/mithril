package dashboard

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"time"
	"unicode/utf8"
)

// ReplayDetailCapacity is a retained row count, not a time-retention guarantee.
const ReplayDetailCapacity = 256

// ReplayDetailBinding identifies a node run and one replay source context.
// GenesisHash is nil when unknown. Identity is deliberately not row ownership.
type ReplayDetailBinding struct {
	RunID            string    `json:"run_id"`
	NodeStartedAt    time.Time `json:"node_started_at"`
	GenesisHash      *string   `json:"genesis_hash"`
	SourceGeneration uint64    `json:"source_generation,string"`
	Attempt          uint64    `json:"attempt,string"`
	Rewinds          uint64    `json:"rewinds,string"`
	AttemptStartedAt time.Time `json:"attempt_started_at"`
	ResetAt          time.Time `json:"reset_at"`
}

// ReplayDetailCore is the complete immutable publication identity. Capture time
// participates in the digest even if an idle source retains unchanged rows.
// Transport state, leases, collector timestamps and reader gaps are separate.
type ReplayDetailCore struct {
	SchemaVersion  int                   `json:"schema_version"`
	Family         string                `json:"family"`
	Scope          string                `json:"scope"`
	Binding        ReplayDetailBinding   `json:"binding"`
	CapturedAt     time.Time             `json:"captured_at"`
	UpdatedAt      time.Time             `json:"updated_at"`
	Capacity       int                   `json:"capacity"`
	Count          int                   `json:"count"`
	FirstSequence  uint64                `json:"first_sequence,string"`
	LastSequence   uint64                `json:"last_sequence,string"`
	Evicted        uint64                `json:"evicted,string"`
	Exhausted      bool                  `json:"exhausted"`
	Records        []ReplayDetailRecord  `json:"records"`
	ScheduleWindow *HeldScheduleWindow   `json:"schedule_window,omitempty"`
	FoldCapacity   *ReplayFoldCapacity   `json:"fold_capacity,omitempty"`
	NearReception  *ReplayNearReception  `json:"near_reception,omitempty"`
	LatestSelected *ReplayLatestSelected `json:"latest_selected,omitempty"`
	RecentSelected *ReplayRecentSelected `json:"recent_selected,omitempty"`
}

// ReplayDetailTiming preserves raw operation count and summed nanoseconds.
// Timers may overlap or include discarded work; these are not measured CPU.
type ReplayDetailTiming struct {
	Count          uint64 `json:"count,string"`
	SumNanoseconds uint64 `json:"sum_nanoseconds,string"`
}

// ReplayDetailRecord attaches completed-bank context to a closing replay row.
// Its work is not necessarily exclusive to that bank or its full execution;
// completion is not finality. No rates or additive phase totals are derived.
type ReplayDetailRecord struct {
	Sequence                          uint64                      `json:"sequence,string"`
	CompletedIndex                    uint64                      `json:"completed_index,string"`
	Attempt                           uint64                      `json:"attempt,string"`
	Rewinds                           uint64                      `json:"rewinds,string"`
	Slot                              uint64                      `json:"slot,string"`
	Epoch                             uint64                      `json:"epoch,string"`
	BlockHeight                       uint64                      `json:"block_height,string"`
	Bankhash                          string                      `json:"bankhash"`
	CompletedAt                       time.Time                   `json:"completed_at"`
	ObservedAt                        time.Time                   `json:"observed_at"`
	Transactions                      uint64                      `json:"transactions,string"`
	ComputeUnits                      uint64                      `json:"compute_units,string"`
	WorkCountersSaturated             bool                        `json:"work_counters_saturated"`
	SharedExecutionTimingsUnavailable bool                        `json:"shared_execution_timings_unavailable"`
	DependencyPlannerPreparedObserved bool                        `json:"dependency_planner_prepared_observed"`
	DependencyPlannerFallbackObserved bool                        `json:"dependency_planner_fallback_observed"`
	Scope                             string                      `json:"scope"`
	Streaming                         ReplayDetailStreaming       `json:"streaming"`
	Timings                           ReplayDetailTimings         `json:"timings"`
	Work                              ReplayDetailWork            `json:"work"`
	Stages                            ReplayDetailStages          `json:"stages"`
	UnknownStages                     bool                        `json:"unknown_stages"`
	AccountLoader                     ReplayDetailAccountLoader   `json:"account_loader"`
	UnrootedRetainedBytes             uint64                      `json:"unrooted_retained_bytes,string"`
	UnrootedHeldSlots                 uint64                      `json:"unrooted_held_slots,string"`
	RetentionLimits                   ReplayDetailRetentionLimits `json:"retention_limits,omitzero"`
}

// ReplayDetailRetentionLimits is historical account-retention configuration,
// not physical RAM. Observed preserves known zero-disabled checks; a zero value
// omits the entire member, keeping records comparable and detached by value.
type ReplayDetailRetentionLimits struct {
	Observed             bool   `json:"observed"`
	RetainedBytesPauseAt uint64 `json:"retained_bytes_pause_at,string"`
	HeldSlotsHaltAbove   uint64 `json:"held_slots_halt_above,string"`
}

// ReplayDetailStreaming retains row flags and raw recorded counters. Flags are
// not attempt counts; recorded counters can span a stream rebuilt across rows.
type ReplayDetailStreaming struct {
	OpenedObserved              bool   `json:"opened_observed"`
	DiscardObserved             bool   `json:"discard_observed"`
	RecordedGroups              uint64 `json:"recorded_groups,string"`
	RecordedTransactions        uint64 `json:"recorded_transactions,string"`
	LastDiscardReason           string `json:"last_discard_reason"`
	AttemptBreakdownUnavailable bool   `json:"attempt_breakdown_unavailable"`
}

// ReplayDetailTimings is the explicit allowlist of replay-row wall timings.
type ReplayDetailTimings struct {
	SinceLastRow                     ReplayDetailTiming `json:"since_last_row"`
	LoopIteration                    ReplayDetailTiming `json:"loop_iteration"`
	LoopSourceWait                   ReplayDetailTiming `json:"loop_source_wait"`
	LoopBeforeReplay                 ReplayDetailTiming `json:"loop_before_replay"`
	LoopAfterReplay                  ReplayDetailTiming `json:"loop_after_replay"`
	SlotReplay                       ReplayDetailTiming `json:"slot_replay"`
	PreprocessBlock                  ReplayDetailTiming `json:"preprocess_block"`
	ProcessBlock                     ReplayDetailTiming `json:"process_block"`
	PostProcessBlock                 ReplayDetailTiming `json:"post_process_block"`
	ProcessBlockUnstaged             ReplayDetailTiming `json:"process_block_unstaged"`
	RewardCertificatePreflight       ReplayDetailTiming `json:"reward_certificate_preflight"`
	TransactionExecutionPlan         ReplayDetailTiming `json:"transaction_execution_plan"`
	TransactionStatusValidation      ReplayDetailTiming `json:"transaction_status_validation"`
	DependencyPlannerPreparation     ReplayDetailTiming `json:"dependency_planner_preparation"`
	DependencyPlannerBuild           ReplayDetailTiming `json:"dependency_planner_build"`
	DependencyPlannerWait            ReplayDetailTiming `json:"dependency_planner_wait"`
	DependencyPlannerDispatch        ReplayDetailTiming `json:"dependency_planner_dispatch"`
	LoadBlockAccounts                ReplayDetailTiming `json:"load_block_accounts"`
	SlotCtxSetup                     ReplayDetailTiming `json:"slot_ctx_setup"`
	TxLoop                           ReplayDetailTiming `json:"tx_loop"`
	Reward                           ReplayDetailTiming `json:"reward"`
	Rent                             ReplayDetailTiming `json:"rent"`
	RunIncinerator                   ReplayDetailTiming `json:"run_incinerator"`
	AlpenglowFooterClock             ReplayDetailTiming `json:"alpenglow_footer_clock"`
	AlpenglowVoteRewards             ReplayDetailTiming `json:"alpenglow_vote_rewards"`
	AlpenglowFooterVerification      ReplayDetailTiming `json:"alpenglow_footer_verification"`
	CompileWritableAndModifiedAccts  ReplayDetailTiming `json:"compile_writable_and_modified_accts"`
	EnsureParentAccountsForModified  ReplayDetailTiming `json:"ensure_parent_accounts_for_modified"`
	BlockUpdateAccounts              ReplayDetailTiming `json:"block_update_accounts"`
	TransactionStatusCommit          ReplayDetailTiming `json:"transaction_status_commit"`
	TransactionStatusPreparation     ReplayDetailTiming `json:"transaction_status_preparation"`
	TransactionStatusPreparationWait ReplayDetailTiming `json:"transaction_status_preparation_wait"`
	SignatureVerificationJoin        ReplayDetailTiming `json:"signature_verification_join"`
	AccountsDeltaHash                ReplayDetailTiming `json:"accounts_delta_hash"`
	LtHashDedupe                     ReplayDetailTiming `json:"lt_hash_dedupe"`
	LtHashWorkerCompute              ReplayDetailTiming `json:"lt_hash_worker_compute"`
	LtHashPartialReduce              ReplayDetailTiming `json:"lt_hash_partial_reduce"`
	BankHashFinalize                 ReplayDetailTiming `json:"bank_hash_finalize"`
	BankHash                         ReplayDetailTiming `json:"bank_hash"`
	TransactionStatusView            ReplayDetailTiming `json:"transaction_status_view"`
	ChainTipUpdate                   ReplayDetailTiming `json:"chain_tip_update"`
	ResumeContext                    ReplayDetailTiming `json:"resume_context"`
	FullToReplayed                   ReplayDetailTiming `json:"full_to_replayed"`
	FoldOverlap                      ReplayDetailTiming `json:"fold_overlap"`
}

// ReplayDetailWork contains summed row operation times, not serial phases.
type ReplayDetailWork struct {
	Sigverify              ReplayDetailTiming `json:"sigverify"`
	TxUpdateAccounts       ReplayDetailTiming `json:"tx_update_accounts"`
	TxFailedUpdateAccounts ReplayDetailTiming `json:"tx_failed_update_accounts"`
}

// ReplayDetailStages contains fixed row-stage names; a stage can repeat.
type ReplayDetailStages struct {
	ParameterArena         ReplayDetailTiming `json:"parameter_arena"`
	DependencyPlanner      ReplayDetailTiming `json:"prepare_dependency_planner"`
	LoadAccounts           ReplayDetailTiming `json:"load_accounts"`
	TransactionLoop        ReplayDetailTiming `json:"tx_loop"`
	DistributeFees         ReplayDetailTiming `json:"distribute_fees"`
	CollectRent            ReplayDetailTiming `json:"collect_rent"`
	Incinerator            ReplayDetailTiming `json:"run_incinerator"`
	CompileAccounts        ReplayDetailTiming `json:"compile_accounts"`
	BankHash               ReplayDetailTiming `json:"bankhash"`
	StoreAccounts          ReplayDetailTiming `json:"store_accounts"`
	Done                   ReplayDetailTiming `json:"done"`
	StreamingWait          ReplayDetailTiming `json:"streaming_wait"`
	StreamingSignatureWait ReplayDetailTiming `json:"streaming_sigverify_wait"`
}

// ReplayDetailAccountLoader contains logical loader work recorded in the row.
// Counts are not unique keys/files, and byte counts are not runtime heap usage.
type ReplayDetailAccountLoader struct {
	AddressTableLookups                ReplayDetailTiming  `json:"address_table_lookups"`
	DedupeBlockAccounts                ReplayDetailTiming  `json:"dedupe_block_accounts"`
	SourceBatch                        ReplayDetailTiming  `json:"source_batch"`
	ParentMapBuild                     ReplayDetailTiming  `json:"parent_map_build"`
	SysvarUpdates                      ReplayDetailTiming  `json:"sysvar_updates"`
	SysvarClockRead                    ReplayDetailTiming  `json:"sysvar_clock_read"`
	SysvarSlotHashesRead               ReplayDetailTiming  `json:"sysvar_slot_hashes_read"`
	SysvarRecentBlockhashesRead        ReplayDetailTiming  `json:"sysvar_recent_blockhashes_read"`
	SysvarSlotHistoryRead              ReplayDetailTiming  `json:"sysvar_slot_history_read"`
	SysvarStakeHistoryRead             ReplayDetailTiming  `json:"sysvar_stake_history_read"`
	SysvarLastRestartSlotRead          ReplayDetailTiming  `json:"sysvar_last_restart_slot_read"`
	WorkingSetLookup                   ReplayDetailTiming  `json:"working_set_lookup"`
	InProgressLookup                   *ReplayDetailTiming `json:"in_progress_lookup"`
	AppendVecPinWait                   ReplayDetailTiming  `json:"append_vec_pin_wait"`
	ReadCacheEpochWait                 ReplayDetailTiming  `json:"read_cache_epoch_wait"`
	CacheLookup                        ReplayDetailTiming  `json:"cache_lookup"`
	AdmissionFilter                    ReplayDetailTiming  `json:"admission_filter"`
	IndexLookup                        ReplayDetailTiming  `json:"index_lookup"`
	ReadPlanning                       ReplayDetailTiming  `json:"read_planning"`
	AppendVecRead                      ReplayDetailTiming  `json:"append_vec_read"`
	CachePublicationWait               ReplayDetailTiming  `json:"cache_publication_wait"`
	CachePublication                   ReplayDetailTiming  `json:"cache_publication"`
	SysvarWorkingSetLookup             ReplayDetailTiming  `json:"sysvar_working_set_lookup"`
	SysvarClone                        ReplayDetailTiming  `json:"sysvar_clone"`
	SysvarAppendVecPinWait             ReplayDetailTiming  `json:"sysvar_append_vec_pin_wait"`
	SysvarInProgressLookup             *ReplayDetailTiming `json:"sysvar_in_progress_lookup"`
	SysvarReadCacheEpochWait           ReplayDetailTiming  `json:"sysvar_read_cache_epoch_wait"`
	SysvarCacheLookup                  ReplayDetailTiming  `json:"sysvar_cache_lookup"`
	SysvarIndexAndAppendVecRead        ReplayDetailTiming  `json:"sysvar_index_and_append_vec_read"`
	SysvarCachePublicationWait         ReplayDetailTiming  `json:"sysvar_cache_publication_wait"`
	SysvarCachePublication             ReplayDetailTiming  `json:"sysvar_cache_publication"`
	RequestedKeys                      uint64              `json:"requested_keys,string"`
	DurableKeys                        uint64              `json:"durable_keys,string"`
	ParentAccounts                     uint64              `json:"parent_accounts,string"`
	WorkingSetHits                     uint64              `json:"working_set_hits,string"`
	InProgressHits                     *uint64             `json:"in_progress_hits,string"`
	PendingFoldHits                    uint64              `json:"pending_fold_hits,string"`
	CacheHits                          uint64              `json:"cache_hits,string"`
	NegativeCacheHits                  uint64              `json:"negative_cache_hits,string"`
	IndexHits                          uint64              `json:"index_hits,string"`
	IndexMisses                        uint64              `json:"index_misses,string"`
	UniqueAppendVecs                   uint64              `json:"unique_append_vecs,string"`
	AppendVecChunks                    uint64              `json:"append_vec_chunks,string"`
	AppendVecAccounts                  uint64              `json:"append_vec_accounts,string"`
	OpenFailures                       uint64              `json:"open_failures,string"`
	ReadFailures                       uint64              `json:"read_failures,string"`
	RetryAccounts                      uint64              `json:"retry_accounts,string"`
	CommonCacheAdmissions              uint64              `json:"common_cache_admissions,string"`
	CommonCacheAdmissionsSkipped       uint64              `json:"common_cache_admissions_skipped,string"`
	VoteCacheAdmissions                uint64              `json:"vote_cache_admissions,string"`
	VoteCacheAdmissionsSkipped         uint64              `json:"vote_cache_admissions_skipped,string"`
	CachePublicationEpochRejects       uint64              `json:"cache_publication_epoch_rejects,string"`
	DecodedAccountObjects              uint64              `json:"decoded_account_objects,string"`
	DecodedAccountBytes                uint64              `json:"decoded_account_bytes,string"`
	PlaceholderObjects                 uint64              `json:"placeholder_objects,string"`
	SysvarReads                        uint64              `json:"sysvar_reads,string"`
	SysvarWorkingSetHits               uint64              `json:"sysvar_working_set_hits,string"`
	SysvarInProgressHits               *uint64             `json:"sysvar_in_progress_hits,string"`
	SysvarPendingFoldHits              uint64              `json:"sysvar_pending_fold_hits,string"`
	SysvarCacheHits                    uint64              `json:"sysvar_cache_hits,string"`
	SysvarDurableReads                 uint64              `json:"sysvar_durable_reads,string"`
	SysvarCachePublicationEpochRejects uint64              `json:"sysvar_cache_publication_epoch_rejects,string"`
}

// ValidateReplayDetailCore checks source context and the explicit row contract.
// It does not establish freshness, a lease, selected-fork validity or completeness
// of attempted work. Wall dates are not used to invent elapsed-time ordering.
func ValidateReplayDetailCore(c ReplayDetailCore) error {
	if (c.SchemaVersion != 1 && c.SchemaVersion != 2) || c.Family != "replay" || c.Scope != "replay_row_history" || !validHeldSchedule(c.ScheduleWindow) ||
		!validReplayFoldCapacity(c.FoldCapacity) || !validReplayNearReception(c.NearReception) || c.NearReception != nil && c.Count == 0 || !validReplaySelectedMetadata(c.LatestSelected, c.RecentSelected) {
		return errors.New("invalid replay detail context")
	}
	if err := validateReplayDetailMetadata(c.Binding, c.CapturedAt, c.UpdatedAt, c.Capacity, c.Count, c.FirstSequence, c.LastSequence, c.Evicted); err != nil {
		return err
	}
	if c.Records == nil || c.Count != len(c.Records) {
		return errors.New("invalid replay detail count")
	}
	return validateReplayDetailRows(c.Records, c.Binding, c.FirstSequence, c.SchemaVersion)
}

// validateReplayDetailRows checks a complete core or a page's ordered rows.
// The complete core check also covers completion order across page boundaries.
func validateReplayDetailRows(records []ReplayDetailRecord, b ReplayDetailBinding, first uint64, version int) error {
	var previousCompleted uint64
	for i, r := range records {
		loader := r.AccountLoader
		if version == 1 && (loader.InProgressLookup == nil || loader.SysvarInProgressLookup == nil || loader.InProgressHits == nil || loader.SysvarInProgressHits == nil) ||
			version == 2 && (loader.InProgressLookup != nil || loader.SysvarInProgressLookup != nil || loader.InProgressHits != nil || loader.SysvarInProgressHits != nil) {
			return errors.New("invalid replay loader availability")
		}
		if !r.RetentionLimits.Observed && r.RetentionLimits != (ReplayDetailRetentionLimits{}) {
			return errors.New("invalid replay retention limits")
		}
		if r.Sequence != first+uint64(i) || r.CompletedIndex <= previousCompleted ||
			r.Attempt != b.Attempt || r.Rewinds != b.Rewinds ||
			!validReplayDetailHash(r.Bankhash, true) ||
			!validReplayDetailTime(r.CompletedAt) || !validReplayDetailTime(r.ObservedAt) ||
			r.Scope != "replay_row" || !r.SharedExecutionTimingsUnavailable ||
			!r.Streaming.AttemptBreakdownUnavailable || !validReplayDetailDiscard(r.Streaming) {
			return errors.New("invalid replay detail row")
		}
		previousCompleted = r.CompletedIndex
	}
	return nil
}

// cloneReplayDetailRecords detaches legacy optional measurements at reader boundaries.
func cloneReplayDetailRecords(records []ReplayDetailRecord) []ReplayDetailRecord {
	cloned := slices.Clone(records)
	for i := range cloned {
		loader := &cloned[i].AccountLoader
		if loader.InProgressLookup != nil {
			v := *loader.InProgressLookup
			loader.InProgressLookup = &v
		}
		if loader.SysvarInProgressLookup != nil {
			v := *loader.SysvarInProgressLookup
			loader.SysvarInProgressLookup = &v
		}
		if loader.InProgressHits != nil {
			v := *loader.InProgressHits
			loader.InProgressHits = &v
		}
		if loader.SysvarInProgressHits != nil {
			v := *loader.SysvarInProgressHits
			loader.SysvarInProgressHits = &v
		}
	}
	return cloned
}

func replayDetailRecordsEqual(a, b ReplayDetailRecord) bool {
	x, y := &a.AccountLoader, &b.AccountLoader
	if (x.InProgressLookup == nil) != (y.InProgressLookup == nil) ||
		(x.SysvarInProgressLookup == nil) != (y.SysvarInProgressLookup == nil) ||
		(x.InProgressHits == nil) != (y.InProgressHits == nil) ||
		(x.SysvarInProgressHits == nil) != (y.SysvarInProgressHits == nil) {
		return false
	}
	if x.InProgressLookup != nil && *x.InProgressLookup != *y.InProgressLookup ||
		x.SysvarInProgressLookup != nil && *x.SysvarInProgressLookup != *y.SysvarInProgressLookup ||
		x.InProgressHits != nil && *x.InProgressHits != *y.InProgressHits ||
		x.SysvarInProgressHits != nil && *x.SysvarInProgressHits != *y.SysvarInProgressHits {
		return false
	}
	x.InProgressLookup, y.InProgressLookup = nil, nil
	x.SysvarInProgressLookup, y.SysvarInProgressLookup = nil, nil
	x.InProgressHits, y.InProgressHits = nil, nil
	x.SysvarInProgressHits, y.SysvarInProgressHits = nil, nil
	return a == b
}

// validateReplayDetailMetadata checks shared source-core and manifest fields,
// without inventing records for a manifest whose pages have not arrived.
func validateReplayDetailMetadata(b ReplayDetailBinding, capturedAt, updatedAt time.Time, capacity, count int, first, last, evicted uint64) error {
	if !validReplayDetailBinding(b) || !validReplayDetailTime(capturedAt) || !validReplayDetailTime(updatedAt) {
		return errors.New("invalid replay detail context")
	}
	if capacity != ReplayDetailCapacity || count < 0 || count > ReplayDetailCapacity {
		return errors.New("invalid replay detail count")
	}
	if count == 0 {
		if first != 0 || last != 0 || evicted != 0 {
			return errors.New("invalid empty replay detail range")
		}
		return nil
	}
	if last < uint64(count) || first != last-uint64(count)+1 ||
		evicted != first-1 || evicted != 0 && count != ReplayDetailCapacity {
		return errors.New("invalid replay detail range")
	}
	return nil
}

func validReplayDetailBinding(b ReplayDetailBinding) bool {
	return b.RunID != "" && len(b.RunID) <= 128 && utf8.ValidString(b.RunID) && b.SourceGeneration != 0 && b.Attempt != 0 &&
		validReplayDetailTime(b.NodeStartedAt) && validReplayDetailTime(b.AttemptStartedAt) && validReplayDetailTime(b.ResetAt) &&
		(b.GenesisHash == nil || validReplayDetailHash(*b.GenesisHash, false))
}

// ReplayDetailGeneration hashes every validated row and producer capture date.
// The reader must reconstruct this same typed core before accepting its token.
func ReplayDetailGeneration(c ReplayDetailCore) (string, error) {
	if err := ValidateReplayDetailCore(c); err != nil {
		return "", err
	}
	body, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

func validReplayDetailDiscard(s ReplayDetailStreaming) bool {
	switch s.LastDiscardReason {
	case "none":
		return !s.DiscardObserved
	case "other_block", "parent_changed", "fork_switch", "timeout", "cancelled",
		"generation_gone", "input_rejected", "finalization", "shutdown", "other":
		return s.DiscardObserved
	default:
		return false
	}
}
