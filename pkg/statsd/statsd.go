package statsd

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/Overclock-Validator/mithril/internal/mcpwire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	mithrilmetrics "github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

type metricType int

const (
	TimingT metricType = iota
	CountT
	GaugeT
)

// SAFE METRIC TYPE
type Metric struct {
	name string // unexported → cannot be created outside this package
}

func (m Metric) String() string { return m.name }

var (
	PreprocessBlock      = Metric{"preprocess_block"}
	TxLoop               = Metric{"tx_loop"}
	LoadBlockAccounts    = Metric{"load_block_accounts"}
	RunIncinerator       = Metric{"run_incinerator"}
	Reward               = Metric{"reward"}
	Rent                 = Metric{"rent"}
	BlockUpdateAccounts  = Metric{"block_update_accounts"}
	AccountsDeltaHash    = Metric{"accounts_delta_hash"}
	BankHash             = Metric{"bank_hash"}
	AlpenglowVoteRewards = Metric{"alpenglow_vote_rewards_duration_seconds"}

	VoteRewardValidatorPreparation       = Metric{"vote_reward_validator_preparation_duration_seconds"}
	VoteRewardSkipCertificateValidation  = Metric{"vote_reward_skip_certificate_validation_duration_seconds"}
	VoteRewardNotarCertificateValidation = Metric{"vote_reward_notar_certificate_validation_duration_seconds"}
	VoteRewardFinalCertificateDecode     = Metric{"vote_reward_final_certificate_decode_duration_seconds"}
	VoteRewardFinalCertificateValidation = Metric{"vote_reward_final_certificate_validation_duration_seconds"}
	VoteRewardStatePreparation           = Metric{"vote_reward_state_preparation_duration_seconds"}
	VoteRewardAccountMutation            = Metric{"vote_reward_account_mutation_duration_seconds"}
	VoteRewardValidatorCacheHits         = Metric{"vote_reward_validator_cache_hits"}
	VoteRewardValidatorCacheMisses       = Metric{"vote_reward_validator_cache_misses"}
	VoteRewardValidators                 = Metric{"vote_reward_validators"}
	VoteRewardFinalSigners               = Metric{"vote_reward_final_signers"}
	VoteRewardAccountsUpdated            = Metric{"vote_reward_accounts_updated"}

	InstructionsAndAccountMetasFromTx  = Metric{"instructions_and_account_metas_from_tx"}
	ComputeBudgetExecutionInstructions = Metric{"compute_budget_execution_instructions"}
	AccountsFromTx                     = Metric{"accounts_from_tx"}
	PreBalanceDivergenceCheck          = Metric{"pre_balance_divergence_check"}
	CalcAndDeductFees                  = Metric{"calc_and_deduct_fees"}
	ReadRentSysvar                     = Metric{"read_rent_sysvar"}
	PreTxRentStates                    = Metric{"pre_tx_rent_states"}
	IxLoop                             = Metric{"ix_loop"}
	PostTxRentStates                   = Metric{"post_tx_rent_states"}
	PostBalanceDivergenceCheck         = Metric{"post_balance_divergence_check"}
	TxUpdateAccounts                   = Metric{"tx_update_accounts"}
	TxUpdateAccountsDuration           = Metric{"tx_update_accounts_duration_seconds"}
	TxPublishRecordWritableAcct        = Metric{"tx_publish_record_writable_acct_duration_seconds"}
	TxPublishTouchedAccountState       = Metric{"tx_publish_touched_account_state_duration_seconds"}
	TxPublishStakeVoteBookkeeping      = Metric{"tx_publish_stake_vote_bookkeeping_duration_seconds"}
	TxPublicationTouchedAccounts       = Metric{"tx_publication_touched_accounts"}
	TxPublicationTouchedAccountBytes   = Metric{"tx_publication_touched_account_bytes"}
	TxFailedUpdateAccounts             = Metric{"tx_failed_update_accounts_duration_seconds"}
	TxFailedPublicationPreparation     = Metric{"tx_failed_publication_preparation_duration_seconds"}
	TxFailedPayerPublication           = Metric{"tx_failed_payer_publication_duration_seconds"}
	TxFailedNoncePublication           = Metric{"tx_failed_nonce_publication_duration_seconds"}

	GetNextIxCtx                            = Metric{"get_next_ix_ctx"}
	NextIxCtxConfigure                      = Metric{"next_ix_ctx_configure"}
	IxPush                                  = Metric{"ix_push"}
	IxPop                                   = Metric{"ix_pop"}
	ExecIxResolveNativeProgram              = Metric{"exec_ix_resolve_native_program"}
	ExecIxNativeProgramSystem               = Metric{"exec_ix_native_program_system"}
	ExecIxNativeProgramStake                = Metric{"exec_ix_native_program_stake"}
	ExecIxNativeProgramVote                 = Metric{"exec_ix_native_program_vote"}
	ExecIxNativeProgramComputeBudget        = Metric{"exec_ix_native_program_compute_budget"}
	ExecIxNativeProgramBpfLoader2           = Metric{"exec_ix_native_program_bpf_loader2"}
	ExecIxNativeProgramBpfLoaderDeprecated  = Metric{"exec_ix_native_program_bpf_loader_deprecated"}
	ExecIxNativeProgramBpfLoaderUpgradeable = Metric{"exec_ix_native_program_bpf_loader_upgradeable"}
	ExecIxNativeProgramZkElgamalProof       = Metric{"exec_ix_native_program_zk_elgamal_proof"}
	ExecIxNativeProgramEd25519Precompile    = Metric{"exec_ix_native_program_ed25519precompile"}
	ExecIxNativeProgramSecp256kPrecompile   = Metric{"exec_ix_native_program_secp256k_precompile"}
	FixupInstructionsSysvarAccount          = Metric{"fixup_instructions_sysvar_account"}
	InstructionAccountsFromAccountMetas     = Metric{"instruction_accounts_from_account_metas"}

	SbpfInterpreterNew = Metric{"sbpf_interpreter_new"}
	SbpfInterpreterRun = Metric{"sbpf_interpreter_run"}

	AddProgramToCache                = Metric{"add_program_to_cache"}
	GetProgramAccount                = Metric{"get_program_account"}
	GetProgramDataCached             = Metric{"get_program_data_cached"}
	GetProgramDataUncachedAccountsDb = Metric{"get_program_data_uncached_accounts_db"}
	GetProgramDataUncachedAccounts   = Metric{"get_program_data_uncached_accounts"}
	GetProgramDataUncachedMarshal    = Metric{"get_program_data_uncached_marshal"}

	TaskIndexEntryCommitterLatency              = Metric{"tasks_index_entry_committer_latency"}
	TaskSetIfSlotHigherLatency                  = Metric{"tasks_set_if_slot_higher_latency"}
	TasksIndexEntryBuilderLatency               = Metric{"tasks_index_entry_builder_latency"}
	TasksAppendVecCopyingLatency                = Metric{"tasks_append_vec_copying_latency"}
	SlotReplayDurationMs                        = Metric{"slot_replay_duration_ms"}
	TxsPerBlock                                 = Metric{"txs_per_block"}
	SnapshotTarBytesRead                        = Metric{"snapshot_tar_bytes_read"}
	SnapshotBootstrapActive                     = Metric{"snapshot_bootstrap_active"}
	SnapshotBootstrapStartedAt                  = Metric{"snapshot_bootstrap_started_timestamp_seconds"}
	SlotReplays                                 = Metric{"slot_replays"}
	BlockProductionLeaderSlots                  = Metric{"block_production_leader_slots_total"}
	BlockProductionLeaderSlotTerminals          = Metric{"block_production_leader_slot_terminals_total"}
	BlockProductionParentReady                  = Metric{"block_production_parent_ready_activations_total"}
	BlockProductionParentReadyAge               = Metric{"block_production_parent_ready_age_seconds"}
	BlockProductionStartCutoffLate              = Metric{"block_production_start_cutoff_lateness_seconds"}
	BlockProductionStartDecisionTickDeliveryLag = Metric{"block_production_start_decision_tick_delivery_lag_seconds"}
	BlockProductionStartDecisionTickWork        = Metric{"block_production_start_decision_tick_work_seconds"}
	BlockProductionStartAttempt                 = Metric{"block_production_start_attempt_seconds"}
	TurbineShredCollection                      = Metric{"turbine_shred_collection_duration_seconds"}
	TurbineBlockCompletionQueueDelay            = Metric{"turbine_block_completion_queue_delay_seconds"}
	TurbineBlockDecode                          = Metric{"turbine_block_decode_duration_seconds"}
	TurbineTransactionParse                     = Metric{"turbine_transaction_parse_duration_seconds"}
	TurbineTransactionSigverify                 = Metric{"turbine_transaction_sigverify_duration_seconds"}
	// ReplaySigverifyGroup times one drained group of transaction signatures
	// and ReplaySigverifyGroupSignatures counts how many signatures were in it.
	// The pair is what tells an operator whether batching is actually happening:
	// a group width stuck near one means work is arriving too thinly to fill a
	// vector group, which is a throughput ceiling no backend choice can lift.
	ReplaySigverifyGroup           = Metric{"replay_sigverify_group_duration_seconds"}
	ReplaySigverifyGroupSignatures = Metric{"replay_sigverify_group_signatures_total"}
	TurbineReplayAdmission         = Metric{"turbine_replay_admission_duration_seconds"}
	TurbineReceiverActive          = Metric{"turbine_receiver_active"}
	TurbinePacketsReceived         = Metric{"turbine_packets_received_total"}
	TurbineDataShredsReceived      = Metric{"turbine_data_shreds_received_total"}
	TurbineBlocksEmitted           = Metric{"turbine_blocks_emitted_total"}
	TurbineShredsRejected          = Metric{"turbine_shreds_rejected_total"}
	TurbineAssemblerActiveSlots    = Metric{"turbine_assembler_active_slots"}
	TurbineLastPacketTimestamp     = Metric{"turbine_last_packet_timestamp_seconds"}
	TurbineLastDataSlot            = Metric{"turbine_last_data_slot"}
	TurbineLastBlockTimestamp      = Metric{"turbine_last_block_timestamp_seconds"}
	TurbineLastBlockSlot           = Metric{"turbine_last_block_slot"}

	SnapshotWorkerPoolUtilization = Metric{"snapshot_worker_pool_utilization"}
	TasksSetIfSlotHigherQueueSize = Metric{"tasks_set_if_slot_higher_queue_size"}
	Epoch                         = Metric{"epoch"}
	Slot                          = Metric{"slot"}

	TestCount = Metric{"test_count"} // used for testing purposes, not a real metric
)

//
// MAP METRICS TO TYPES + LABELS
//

// making these public so they can be used in tests and validate that if there is metrics there are corresponding types and labels

var MetricToType = map[Metric]metricType{
	PreprocessBlock:      TimingT,
	TxLoop:               TimingT,
	LoadBlockAccounts:    TimingT,
	RunIncinerator:       TimingT,
	Reward:               TimingT,
	Rent:                 TimingT,
	BlockUpdateAccounts:  TimingT,
	AccountsDeltaHash:    TimingT,
	BankHash:             TimingT,
	AlpenglowVoteRewards: TimingT,

	VoteRewardValidatorPreparation:       TimingT,
	VoteRewardSkipCertificateValidation:  TimingT,
	VoteRewardNotarCertificateValidation: TimingT,
	VoteRewardFinalCertificateDecode:     TimingT,
	VoteRewardFinalCertificateValidation: TimingT,
	VoteRewardStatePreparation:           TimingT,
	VoteRewardAccountMutation:            TimingT,
	VoteRewardValidatorCacheHits:         CountT,
	VoteRewardValidatorCacheMisses:       CountT,
	VoteRewardValidators:                 CountT,
	VoteRewardFinalSigners:               CountT,
	VoteRewardAccountsUpdated:            CountT,

	InstructionsAndAccountMetasFromTx:  TimingT,
	ComputeBudgetExecutionInstructions: TimingT,
	AccountsFromTx:                     TimingT,
	PreBalanceDivergenceCheck:          TimingT,
	CalcAndDeductFees:                  TimingT,
	ReadRentSysvar:                     TimingT,
	PreTxRentStates:                    TimingT,
	IxLoop:                             TimingT,
	PostTxRentStates:                   TimingT,
	PostBalanceDivergenceCheck:         TimingT,
	TxUpdateAccounts:                   TimingT,
	TxUpdateAccountsDuration:           TimingT,
	TxPublishRecordWritableAcct:        TimingT,
	TxPublishTouchedAccountState:       TimingT,
	TxPublishStakeVoteBookkeeping:      TimingT,
	TxPublicationTouchedAccounts:       CountT,
	TxPublicationTouchedAccountBytes:   CountT,
	TxFailedUpdateAccounts:             TimingT,
	TxFailedPublicationPreparation:     TimingT,
	TxFailedPayerPublication:           TimingT,
	TxFailedNoncePublication:           TimingT,

	GetNextIxCtx:                            TimingT,
	NextIxCtxConfigure:                      TimingT,
	IxPush:                                  TimingT,
	IxPop:                                   TimingT,
	ExecIxResolveNativeProgram:              TimingT,
	ExecIxNativeProgramSystem:               TimingT,
	ExecIxNativeProgramStake:                TimingT,
	ExecIxNativeProgramVote:                 TimingT,
	ExecIxNativeProgramComputeBudget:        TimingT,
	ExecIxNativeProgramBpfLoader2:           TimingT,
	ExecIxNativeProgramBpfLoaderDeprecated:  TimingT,
	ExecIxNativeProgramBpfLoaderUpgradeable: TimingT,
	ExecIxNativeProgramZkElgamalProof:       TimingT,
	ExecIxNativeProgramEd25519Precompile:    TimingT,
	ExecIxNativeProgramSecp256kPrecompile:   TimingT,
	FixupInstructionsSysvarAccount:          TimingT,
	InstructionAccountsFromAccountMetas:     TimingT,

	SbpfInterpreterNew:               TimingT,
	SbpfInterpreterRun:               TimingT,
	AddProgramToCache:                TimingT,
	GetProgramAccount:                TimingT,
	GetProgramDataCached:             TimingT,
	GetProgramDataUncachedAccountsDb: TimingT,
	GetProgramDataUncachedAccounts:   TimingT,
	GetProgramDataUncachedMarshal:    TimingT,

	TaskIndexEntryCommitterLatency: TimingT,
	TaskSetIfSlotHigherLatency:     TimingT,
	TasksIndexEntryBuilderLatency:  TimingT,
	TasksAppendVecCopyingLatency:   TimingT,

	SlotReplayDurationMs: TimingT,
	TxsPerBlock:          TimingT,

	SnapshotTarBytesRead:               CountT,
	SlotReplays:                        CountT,
	BlockProductionLeaderSlots:         CountT,
	BlockProductionLeaderSlotTerminals: CountT,
	BlockProductionParentReady:         CountT,

	BlockProductionParentReadyAge:               TimingT,
	BlockProductionStartCutoffLate:              TimingT,
	BlockProductionStartDecisionTickDeliveryLag: TimingT,
	BlockProductionStartDecisionTickWork:        TimingT,
	BlockProductionStartAttempt:                 TimingT,
	TurbineShredCollection:                      TimingT,
	TurbineBlockCompletionQueueDelay:            TimingT,
	TurbineBlockDecode:                          TimingT,
	TurbineTransactionParse:                     TimingT,
	TurbineTransactionSigverify:                 TimingT,
	ReplaySigverifyGroup:                        TimingT,
	ReplaySigverifyGroupSignatures:              CountT,
	TurbineReplayAdmission:                      TimingT,
	TurbinePacketsReceived:                      CountT,
	TurbineDataShredsReceived:                   CountT,
	TurbineBlocksEmitted:                        CountT,
	TurbineShredsRejected:                       CountT,

	TestCount: CountT,

	SnapshotWorkerPoolUtilization: GaugeT,
	SnapshotBootstrapActive:       GaugeT,
	SnapshotBootstrapStartedAt:    GaugeT,
	TasksSetIfSlotHigherQueueSize: GaugeT,
	Epoch:                         GaugeT,
	Slot:                          GaugeT,
	TurbineReceiverActive:         GaugeT,
	TurbineAssemblerActiveSlots:   GaugeT,
	TurbineLastPacketTimestamp:    GaugeT,
	TurbineLastDataSlot:           GaugeT,
	TurbineLastBlockTimestamp:     GaugeT,
	TurbineLastBlockSlot:          GaugeT,
}
var MetricToLabels = map[Metric][]string{
	PreprocessBlock:      {"phase"},
	TxLoop:               {"phase"},
	LoadBlockAccounts:    {"phase"},
	RunIncinerator:       {"phase"},
	Reward:               {"phase"},
	Rent:                 {"phase"},
	BlockUpdateAccounts:  {"phase"},
	AccountsDeltaHash:    {"phase"},
	BankHash:             {"phase"},
	AlpenglowVoteRewards: {"phase"},

	VoteRewardValidatorPreparation:       {"phase"},
	VoteRewardSkipCertificateValidation:  {"phase"},
	VoteRewardNotarCertificateValidation: {"phase"},
	VoteRewardFinalCertificateDecode:     {"phase"},
	VoteRewardFinalCertificateValidation: {"phase"},
	VoteRewardStatePreparation:           {"phase"},
	VoteRewardAccountMutation:            {"phase"},
	VoteRewardValidatorCacheHits:         {"phase"},
	VoteRewardValidatorCacheMisses:       {"phase"},
	VoteRewardValidators:                 {"phase"},
	VoteRewardFinalSigners:               {"phase"},
	VoteRewardAccountsUpdated:            {"phase"},

	InstructionsAndAccountMetasFromTx:  {"phase"},
	ComputeBudgetExecutionInstructions: {"phase"},
	AccountsFromTx:                     {"phase"},
	PreBalanceDivergenceCheck:          {"phase"},
	CalcAndDeductFees:                  {"phase"},
	ReadRentSysvar:                     {"phase"},
	PreTxRentStates:                    {"phase"},
	IxLoop:                             {"phase"},
	PostTxRentStates:                   {"phase"},
	PostBalanceDivergenceCheck:         {"phase"},
	TxUpdateAccounts:                   {"phase"},
	TxUpdateAccountsDuration:           {"phase"},
	TxPublishRecordWritableAcct:        {"phase"},
	TxPublishTouchedAccountState:       {"phase"},
	TxPublishStakeVoteBookkeeping:      {"phase"},
	TxPublicationTouchedAccounts:       {"phase"},
	TxPublicationTouchedAccountBytes:   {"phase"},
	TxFailedUpdateAccounts:             {"phase"},
	TxFailedPublicationPreparation:     {"phase"},
	TxFailedPayerPublication:           {"phase"},
	TxFailedNoncePublication:           {"phase"},

	GetNextIxCtx:                            {"phase"},
	NextIxCtxConfigure:                      {"phase"},
	IxPush:                                  {"phase"},
	IxPop:                                   {"phase"},
	ExecIxResolveNativeProgram:              {"phase"},
	ExecIxNativeProgramSystem:               {"phase"},
	ExecIxNativeProgramStake:                {"phase"},
	ExecIxNativeProgramVote:                 {"phase"},
	ExecIxNativeProgramComputeBudget:        {"phase"},
	ExecIxNativeProgramBpfLoader2:           {"phase"},
	ExecIxNativeProgramBpfLoaderDeprecated:  {"phase"},
	ExecIxNativeProgramBpfLoaderUpgradeable: {"phase"},
	ExecIxNativeProgramZkElgamalProof:       {"phase"},
	ExecIxNativeProgramEd25519Precompile:    {"phase"},
	ExecIxNativeProgramSecp256kPrecompile:   {"phase"},
	FixupInstructionsSysvarAccount:          {"phase"},
	InstructionAccountsFromAccountMetas:     {"phase"},

	SbpfInterpreterNew:               {"phase"},
	SbpfInterpreterRun:               {"phase"},
	AddProgramToCache:                {"phase"},
	GetProgramAccount:                {"phase"},
	GetProgramDataCached:             {"phase"},
	GetProgramDataUncachedAccountsDb: {"phase"},
	GetProgramDataUncachedAccounts:   {"phase"},
	GetProgramDataUncachedMarshal:    {"phase"},

	TaskIndexEntryCommitterLatency: {},
	TaskSetIfSlotHigherLatency:     {},
	TasksIndexEntryBuilderLatency:  {},
	TasksAppendVecCopyingLatency:   {},

	SlotReplayDurationMs:               {},
	TxsPerBlock:                        {},
	SnapshotTarBytesRead:               {},
	SlotReplays:                        {},
	BlockProductionLeaderSlots:         {"outcome", "reason"},
	BlockProductionLeaderSlotTerminals: {"outcome", "terminal", "cause"},
	BlockProductionParentReady:         {"activation", "status"},

	BlockProductionParentReadyAge:               {"activation"},
	BlockProductionStartCutoffLate:              {"phase"},
	BlockProductionStartDecisionTickDeliveryLag: {"outcome"},
	BlockProductionStartDecisionTickWork:        {"outcome"},
	BlockProductionStartAttempt:                 {"result"},
	TurbineShredCollection:                      {},
	TurbineBlockCompletionQueueDelay:            {},
	TurbineBlockDecode:                          {},
	TurbineTransactionParse:                     {},
	TurbineTransactionSigverify:                 {},
	ReplaySigverifyGroup:                        {},
	ReplaySigverifyGroupSignatures:              {},
	TurbineReplayAdmission:                      {},
	TurbinePacketsReceived:                      {},
	TurbineDataShredsReceived:                   {},
	TurbineBlocksEmitted:                        {},
	TurbineShredsRejected:                       {"reason"},

	SnapshotWorkerPoolUtilization: {"task"},
	SnapshotBootstrapActive:       {},
	SnapshotBootstrapStartedAt:    {},
	TasksSetIfSlotHigherQueueSize: {},
	Epoch:                         {},
	Slot:                          {},
	TurbineReceiverActive:         {},
	TurbineAssemblerActiveSlots:   {},
	TurbineLastPacketTimestamp:    {},
	TurbineLastDataSlot:           {},
	TurbineLastBlockTimestamp:     {},
	TurbineLastBlockSlot:          {},

	TestCount: {"test"}, // used for testing purposes, not a real metric
}

var blockProductionDurationBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.010, 0.025, 0.050, 0.075,
	0.100, 0.125, 0.150, 0.200, 0.400, 0.800, 1.600, 3.200,
}

var turbinePipelineDurationBuckets = []float64{
	0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005,
	0.010, 0.025, 0.050, 0.100, 0.200, 0.400, 0.800, 1.600,
	3.200, 6.400, 12.800, 30.000, 60.000,
}

// MetricToBuckets gives duration histograms explicit seconds-based buckets.
// The generic Timing metrics predate unit-specific naming and retain the
// Prometheus defaults for compatibility.
var MetricToBuckets = map[Metric][]float64{
	BlockProductionParentReadyAge:               blockProductionDurationBuckets,
	BlockProductionStartCutoffLate:              blockProductionDurationBuckets,
	BlockProductionStartDecisionTickDeliveryLag: blockProductionDurationBuckets,
	BlockProductionStartDecisionTickWork:        blockProductionDurationBuckets,
	BlockProductionStartAttempt:                 blockProductionDurationBuckets,
	TurbineShredCollection:                      turbinePipelineDurationBuckets,
	TurbineBlockCompletionQueueDelay:            turbinePipelineDurationBuckets,
	TurbineBlockDecode:                          turbinePipelineDurationBuckets,
	TurbineTransactionParse:                     turbinePipelineDurationBuckets,
	TurbineTransactionSigverify:                 turbinePipelineDurationBuckets,
	ReplaySigverifyGroup:                        turbinePipelineDurationBuckets,
	TurbineReplayAdmission:                      turbinePipelineDurationBuckets,
	AlpenglowVoteRewards:                        turbinePipelineDurationBuckets,
	VoteRewardValidatorPreparation:              turbinePipelineDurationBuckets,
	VoteRewardSkipCertificateValidation:         turbinePipelineDurationBuckets,
	VoteRewardNotarCertificateValidation:        turbinePipelineDurationBuckets,
	VoteRewardFinalCertificateDecode:            turbinePipelineDurationBuckets,
	VoteRewardFinalCertificateValidation:        turbinePipelineDurationBuckets,
	VoteRewardStatePreparation:                  turbinePipelineDurationBuckets,
	VoteRewardAccountMutation:                   turbinePipelineDurationBuckets,
	TxUpdateAccountsDuration:                    turbinePipelineDurationBuckets,
	TxPublishRecordWritableAcct:                 turbinePipelineDurationBuckets,
	TxPublishTouchedAccountState:                turbinePipelineDurationBuckets,
	TxPublishStakeVoteBookkeeping:               turbinePipelineDurationBuckets,
	TxFailedUpdateAccounts:                      turbinePipelineDurationBuckets,
	TxFailedPublicationPreparation:              turbinePipelineDurationBuckets,
	TxFailedPayerPublication:                    turbinePipelineDurationBuckets,
	TxFailedNoncePublication:                    turbinePipelineDurationBuckets,
}

type Prometheusmetrics struct {
	counters   map[Metric]*prometheus.CounterVec
	gauges     map[Metric]*prometheus.GaugeVec
	histograms map[Metric]*prometheus.HistogramVec
}

var metricsCollection *Prometheusmetrics
var metricsServerStarted bool

const metricsListenAddr = ":9090"

const (
	mithrilEndpointHeader  = mcpwire.EndpointHeader
	mithrilMetricsEndpoint = mcpwire.MetricsEndpoint
)

func init() {
	initializeStatsdMetrics()
}

func newMetricsHandler() http.Handler {
	mux := http.NewServeMux()
	metrics := promhttp.Handler()
	mux.Handle("/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(mithrilEndpointHeader, mithrilMetricsEndpoint)
		metrics.ServeHTTP(w, r)
	}))
	return mux
}

// StartMetricsServer starts the Prometheus metrics HTTP server.
// It binds before returning so callers can report a port collision without
// mistaking another process on port 9090 for this node's metrics endpoint.
func StartMetricsServer() error {
	if metricsServerStarted {
		return nil
	}
	if err := startMetricsServer(metricsListenAddr); err != nil {
		return err
	}
	metricsServerStarted = true
	return nil
}

func startMetricsServer(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	go func() {
		if err := http.Serve(listener, newMetricsHandler()); err != nil {
			mlog.Log.Errorf("Prometheus metrics server failed: %v", err)
		}
	}()
	return nil
}

func initializeStatsdMetrics() Prometheusmetrics {
	metricsCollection = &Prometheusmetrics{
		counters:   make(map[Metric]*prometheus.CounterVec),
		gauges:     make(map[Metric]*prometheus.GaugeVec),
		histograms: make(map[Metric]*prometheus.HistogramVec),
	}
	for m, t := range MetricToType {
		labelNames := MetricToLabels[m]

		switch t {
		case TimingT:
			opts := prometheus.HistogramOpts{
				Name: m.name,
				Help: fmt.Sprintf("Histogram for %s", m.name),
			}
			if buckets := MetricToBuckets[m]; len(buckets) != 0 {
				opts.Buckets = buckets
			}
			hv := promauto.NewHistogramVec(opts, labelNames)
			metricsCollection.histograms[m] = hv
		case CountT:
			cv := promauto.NewCounterVec(prometheus.CounterOpts{
				Name: m.name,
				Help: fmt.Sprintf("Counter for %s", m.name),
			}, labelNames)
			metricsCollection.counters[m] = cv
		case GaugeT:
			gv := promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: m.name,
				Help: fmt.Sprintf("Gauge for %s", m.name),
			}, labelNames)
			metricsCollection.gauges[m] = gv
		}
	}
	return *metricsCollection
}

func Count(m Metric, value int64, labels []string) error {
	if labels == nil {
		labels = []string{}
	}
	metricsCollection.counters[m].WithLabelValues(labels...).Add(float64(value))
	return nil
}

func Gauge(m Metric, value float64, labels []string) error {
	if labels == nil {
		labels = []string{}
	}
	metricsCollection.gauges[m].WithLabelValues(labels...).Set(value)
	return nil
}

// BeginSnapshotBootstrap exposes the node-owned bootstrap lifetime. The
// returned function clears only the active flag; the timestamp remains as the
// most recent start for post-run evidence.
func BeginSnapshotBootstrap() func() {
	_ = Gauge(SnapshotBootstrapStartedAt, float64(time.Now().Unix()), nil)
	_ = Gauge(SnapshotBootstrapActive, 1, nil)
	return func() { _ = Gauge(SnapshotBootstrapActive, 0, nil) }
}

// TurbineReceiverSnapshot is the bounded receiver state exported to metrics.
// It lives here so the Turbine package can continue using statsd without an
// import cycle.
type TurbineReceiverSnapshot struct {
	Packets         uint64
	DataShreds      uint64
	BlocksEmitted   uint64
	ParseErrors     uint64
	SignatureErrors uint64
	MissingLeaders  uint64
	AssemblyErrors  uint64
	ActiveSlots     int
	LastPacketUnix  int64
	LastDataSlot    uint64
	LastBlockUnix   int64
	LastBlockSlot   uint64
}

// SetTurbineReceiverActive records whether a native receiver is listening.
func SetTurbineReceiverActive(active bool) {
	value := 0.0
	if active {
		value = 1
	}
	_ = Gauge(TurbineReceiverActive, value, nil)
}

func addTurbineCounterDelta(metric Metric, current, previous uint64, labels ...string) {
	delta := current
	if current >= previous {
		delta = current - previous
	}
	metricsCollection.counters[metric].WithLabelValues(labels...).Add(float64(delta))
}

// SendTurbineReceiverMetrics publishes one receiver snapshot. Counter deltas
// remain monotonic when a receiver restarts.
func SendTurbineReceiverMetrics(current, previous TurbineReceiverSnapshot, active bool) {
	SetTurbineReceiverActive(active)
	addTurbineCounterDelta(TurbinePacketsReceived, current.Packets, previous.Packets)
	addTurbineCounterDelta(TurbineDataShredsReceived, current.DataShreds, previous.DataShreds)
	addTurbineCounterDelta(TurbineBlocksEmitted, current.BlocksEmitted, previous.BlocksEmitted)
	addTurbineCounterDelta(TurbineShredsRejected, current.ParseErrors, previous.ParseErrors, "parse")
	addTurbineCounterDelta(TurbineShredsRejected, current.SignatureErrors, previous.SignatureErrors, "signature")
	addTurbineCounterDelta(TurbineShredsRejected, current.MissingLeaders, previous.MissingLeaders, "missing_leader")
	addTurbineCounterDelta(TurbineShredsRejected, current.AssemblyErrors, previous.AssemblyErrors, "assembly")
	_ = Gauge(TurbineAssemblerActiveSlots, float64(current.ActiveSlots), nil)
	_ = Gauge(TurbineLastPacketTimestamp, float64(current.LastPacketUnix), nil)
	_ = Gauge(TurbineLastDataSlot, float64(current.LastDataSlot), nil)
	_ = Gauge(TurbineLastBlockTimestamp, float64(current.LastBlockUnix), nil)
	_ = Gauge(TurbineLastBlockSlot, float64(current.LastBlockSlot), nil)
}

// Create timing function for readability, but under the hood its implemented via distribution
func Timing(m Metric, nanos uint64, labels []string) error {
	if labels == nil {
		labels = []string{}
	}
	metricsCollection.histograms[m].WithLabelValues(labels...).Observe(float64(nanos))
	return nil
}

// Duration records a duration in seconds. Use it with metrics whose names end
// in _seconds and whose buckets are declared in MetricToBuckets.
func Duration(m Metric, duration time.Duration, labels []string) error {
	if duration < 0 {
		return fmt.Errorf("duration metric %s cannot observe negative duration %s", m, duration)
	}
	metricType, registered := MetricToType[m]
	if !registered {
		return fmt.Errorf("duration metric %s is not registered", m)
	}
	if metricType != TimingT {
		return fmt.Errorf("duration metric %s has non-histogram type %d", m, metricType)
	}
	if _, ok := MetricToBuckets[m]; !ok {
		return fmt.Errorf("metric %s is not registered for duration observations", m)
	}
	histogram, ok := metricsCollection.histograms[m]
	if !ok || histogram == nil {
		return fmt.Errorf("duration metric %s has no initialized histogram", m)
	}
	if labels == nil {
		labels = []string{}
	}
	observer, err := histogram.GetMetricWithLabelValues(labels...)
	if err != nil {
		return fmt.Errorf("duration metric %s labels: %w", m, err)
	}
	observer.Observe(duration.Seconds())
	return nil
}

func sendReplayDuration(metric Metric, timing mithrilmetrics.Timing, labels []string) {
	if timing.Count == 0 {
		return
	}
	_ = Duration(metric, time.Duration(timing.SumNanoseconds), labels)
}

func SendBlockReplayMetrics(r mithrilmetrics.BlockReplay) {
	blockLatency := "replay_block"
	txLatency := "replay_tx_sum"
	ixLatency := "replay_ix_sum"
	sbpfLatency := "replay_sbpf"

	Timing(PreprocessBlock, r.PreprocessBlock.SumNanoseconds, []string{blockLatency})
	Timing(LoadBlockAccounts, r.LoadBlockAccounts.SumNanoseconds, []string{blockLatency})
	Timing(TxLoop, r.TxLoop.SumNanoseconds, []string{blockLatency})
	Timing(Reward, r.Reward.SumNanoseconds, []string{blockLatency})
	Timing(Rent, r.Rent.SumNanoseconds, []string{blockLatency})
	Timing(RunIncinerator, r.RunIncinerator.SumNanoseconds, []string{blockLatency})
	Timing(BlockUpdateAccounts, r.BlockUpdateAccounts.SumNanoseconds, []string{blockLatency})
	Timing(AccountsDeltaHash, r.AccountsDeltaHash.SumNanoseconds, []string{blockLatency})
	Timing(BankHash, r.BankHash.SumNanoseconds, []string{blockLatency})
	sendReplayDuration(AlpenglowVoteRewards, r.AlpenglowVoteRewards, []string{blockLatency})
	sendReplayDuration(VoteRewardValidatorPreparation, r.VoteRewardDetails.ValidatorPreparation, []string{blockLatency})
	sendReplayDuration(VoteRewardSkipCertificateValidation, r.VoteRewardDetails.SkipCertificateValidation, []string{blockLatency})
	sendReplayDuration(VoteRewardNotarCertificateValidation, r.VoteRewardDetails.NotarCertificateValidation, []string{blockLatency})
	sendReplayDuration(VoteRewardFinalCertificateDecode, r.VoteRewardDetails.FinalCertificateDecode, []string{blockLatency})
	sendReplayDuration(VoteRewardFinalCertificateValidation, r.VoteRewardDetails.FinalCertificateValidation, []string{blockLatency})
	sendReplayDuration(VoteRewardStatePreparation, r.VoteRewardDetails.StatePreparation, []string{blockLatency})
	sendReplayDuration(VoteRewardAccountMutation, r.VoteRewardDetails.AccountMutation, []string{blockLatency})
	Count(VoteRewardValidatorCacheHits, int64(r.VoteRewardDetails.ValidatorCacheHits), []string{blockLatency})
	Count(VoteRewardValidatorCacheMisses, int64(r.VoteRewardDetails.ValidatorCacheMisses), []string{blockLatency})
	Count(VoteRewardValidators, int64(r.VoteRewardDetails.RewardValidators), []string{blockLatency})
	Count(VoteRewardFinalSigners, int64(r.VoteRewardDetails.FinalSigners), []string{blockLatency})
	Count(VoteRewardAccountsUpdated, int64(r.VoteRewardDetails.VoteAccountsUpdated), []string{blockLatency})
	Timing(InstructionsAndAccountMetasFromTx, r.InstructionsAndAccountMetasFromTx.SumNanoseconds, []string{txLatency})
	Timing(ComputeBudgetExecutionInstructions, r.ComputeBudgetExecutionInstructions.SumNanoseconds, []string{txLatency})
	Timing(AccountsFromTx, r.AccountsFromTx.SumNanoseconds, []string{txLatency})
	Timing(PreBalanceDivergenceCheck, r.PreBalanceDivergenceCheck.SumNanoseconds, []string{txLatency})
	Timing(CalcAndDeductFees, r.CalcAndDeductFees.SumNanoseconds, []string{txLatency})
	Timing(ReadRentSysvar, r.ReadRentSysvar.SumNanoseconds, []string{txLatency})
	Timing(PreTxRentStates, r.PreTxRentStates.SumNanoseconds, []string{txLatency})
	Timing(IxLoop, r.IxLoop.SumNanoseconds, []string{txLatency})
	Timing(PostTxRentStates, r.PostTxRentStates.SumNanoseconds, []string{txLatency})
	Timing(PostBalanceDivergenceCheck, r.PostBalanceDivergenceCheck.SumNanoseconds, []string{txLatency})
	Timing(TxUpdateAccounts, r.TxUpdateAccounts.SumNanoseconds, []string{txLatency})
	sendReplayDuration(TxUpdateAccountsDuration, r.TxUpdateAccounts, []string{txLatency})
	sendReplayDuration(TxPublishRecordWritableAcct, r.TxPublishRecordWritableAcct, []string{txLatency})
	sendReplayDuration(TxPublishTouchedAccountState, r.TxPublishTouchedAccountState, []string{txLatency})
	sendReplayDuration(TxPublishStakeVoteBookkeeping, r.TxPublishStakeVoteBookkeeping, []string{txLatency})
	Count(TxPublicationTouchedAccounts, int64(r.TxPublicationTouchedAccounts), []string{txLatency})
	Count(TxPublicationTouchedAccountBytes, int64(r.TxPublicationTouchedAccountBytes), []string{txLatency})
	sendReplayDuration(TxFailedUpdateAccounts, r.TxFailedUpdateAccounts, []string{txLatency})
	sendReplayDuration(TxFailedPublicationPreparation, r.TxFailedPublicationPreparation, []string{txLatency})
	sendReplayDuration(TxFailedPayerPublication, r.TxFailedPayerPublication, []string{txLatency})
	sendReplayDuration(TxFailedNoncePublication, r.TxFailedNoncePublication, []string{txLatency})
	Timing(GetNextIxCtx, r.GetNextIxCtx.SumNanoseconds, []string{ixLatency})
	Timing(NextIxCtxConfigure, r.NextIxCtxConfigure.SumNanoseconds, []string{ixLatency})
	Timing(IxPush, r.IxPush.SumNanoseconds, []string{ixLatency})
	Timing(IxPop, r.IxPop.SumNanoseconds, []string{ixLatency})
	Timing(ExecIxResolveNativeProgram, r.ExecIxResolveNativeProgram.SumNanoseconds, []string{ixLatency})
	Timing(ExecIxNativeProgramSystem, r.ExecIxNativeProgramSystem.SumNanoseconds, []string{ixLatency})
	Timing(ExecIxNativeProgramStake, r.ExecIxNativeProgramStake.SumNanoseconds, []string{ixLatency})
	Timing(ExecIxNativeProgramVote, r.ExecIxNativeProgramVote.SumNanoseconds, []string{ixLatency})
	Timing(ExecIxNativeProgramComputeBudget, r.ExecIxNativeProgramComputeBudget.SumNanoseconds, []string{ixLatency})
	Timing(ExecIxNativeProgramBpfLoader2, r.ExecIxNativeProgramBpfLoader2.SumNanoseconds, []string{ixLatency})
	Timing(ExecIxNativeProgramBpfLoaderDeprecated, r.ExecIxNativeProgramBpfLoaderDeprecated.SumNanoseconds, []string{ixLatency})
	Timing(ExecIxNativeProgramBpfLoaderUpgradeable, r.ExecIxNativeProgramBpfLoaderUpgradeable.SumNanoseconds, []string{ixLatency})
	Timing(ExecIxNativeProgramZkElgamalProof, r.ExecIxNativeProgramZkElgamalProof.SumNanoseconds, []string{ixLatency})
	Timing(ExecIxNativeProgramEd25519Precompile, r.ExecIxNativeProgramEd25519Precompile.SumNanoseconds, []string{ixLatency})
	Timing(ExecIxNativeProgramSecp256kPrecompile, r.ExecIxNativeProgramSecp256kPrecompile.SumNanoseconds, []string{ixLatency})
	Timing(FixupInstructionsSysvarAccount, r.FixupInstructionsSysvarAccount.SumNanoseconds, []string{ixLatency})
	Timing(InstructionAccountsFromAccountMetas, r.InstructionAccountsFromAccountMetas.SumNanoseconds, []string{ixLatency})
	Timing(SbpfInterpreterNew, r.SbpfInterpreterNew.SumNanoseconds, []string{sbpfLatency})
	Timing(SbpfInterpreterRun, r.SbpfInterpreterRun.SumNanoseconds, []string{sbpfLatency})
	Timing(AddProgramToCache, r.AddProgramToCache.SumNanoseconds, []string{sbpfLatency})
	Timing(GetProgramAccount, r.GetProgramAccount.SumNanoseconds, []string{sbpfLatency})
	Timing(GetProgramDataCached, r.GetProgramDataCached.SumNanoseconds, []string{sbpfLatency})
	Timing(GetProgramDataUncachedAccountsDb, r.GetProgramDataUncachedAccountsDb.SumNanoseconds, []string{sbpfLatency})
	Timing(GetProgramDataUncachedAccounts, r.GetProgramDataUncachedAccounts.SumNanoseconds, []string{sbpfLatency})
	Timing(GetProgramDataUncachedMarshal, r.GetProgramDataUncachedMarshal.SumNanoseconds, []string{sbpfLatency})
}
