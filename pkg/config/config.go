package config

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

const LightbringerQuietDefault = true

func ApplyDefaults(v *viper.Viper) {
	v.SetDefault("lightbringer.quiet", LightbringerQuietDefault)
	// network.cluster and block.source default in the run command itself
	// (Alpenglow/turbine, classic/RPC) — NOT here, because the lightbringer
	// auto-switch needs to distinguish "operator chose a source" from "defaulted".
	v.SetDefault("consensus.mode", "verifying")
	v.SetDefault("consensus.alpenglow_observer_bind_addr", "")
	v.SetDefault("consensus.alpenglow_bls_dst", "")
	v.SetDefault("validator.identity_keypair", "")
	v.SetDefault("validator.vote_account_keypair", "")
	v.SetDefault("validator.authorized_voter_keypair", "")
	v.SetDefault("validator.authorized_withdrawer_keypair", "")
	v.SetDefault("validator.tpu_quic_bind_addr", "")
	v.SetDefault("validator.advertised_ip", "")
	v.SetDefault("validator.tpu_sigverify_workers", 0)
}

// LedgerConfig holds ledger-related configuration (matches Firedancer [ledger] section)
type LedgerConfig struct {
	Path                string `toml:"path" mapstructure:"path"`                                   // was: blockdir
	AccountsPath        string `toml:"accounts_path" mapstructure:"accounts_path"`                 // was: out
	SnapshotArchivePath string `toml:"snapshot_archive_path" mapstructure:"snapshot_archive_path"` // was: path
	IncrementalSnapshot string `toml:"incremental_snapshot" mapstructure:"incremental_snapshot"`   // was: incremental-snapshot-filename
}

// RpcConfig holds RPC-related configuration (matches Firedancer [rpc] section)
type RpcConfig struct {
	Rpc  []string `toml:"rpc" mapstructure:"rpc"`   // List of RPC endpoints
	Port int      `toml:"port" mapstructure:"port"` // was: rpc-server-port
}

// ReplayConfig holds replay-related configuration
type ReplayConfig struct {
	Txpar    int64 `toml:"txpar" mapstructure:"txpar"`
	NumSlots int64 `toml:"num_slots" mapstructure:"num_slots"`
	EndSlot  int64 `toml:"end_slot" mapstructure:"end_slot"`
}

// PprofConfig holds pprof-related configuration
type PprofConfig struct {
	Port           int64  `toml:"port" mapstructure:"port"`                         // was: pprofport
	CpuProfilePath string `toml:"cpu_profile_path" mapstructure:"cpu_profile_path"` // was: cpuprof-filename
}

// DebugConfig holds debug-related configuration
type DebugConfig struct {
	TransactionSignatures     []string `toml:"transaction_signatures" mapstructure:"transaction_signatures"` // was: debugtx
	AccountWrites             []string `toml:"account_writes" mapstructure:"account_writes"`                 // was: debugacctwrites
	DumpEpochVotingRewardDiff bool     `toml:"dump_epoch_voting_reward_diff" mapstructure:"dump_epoch_voting_reward_diff"`
}

// DevelopmentConfig holds development/tuning configuration (matches Firedancer [development] section)
type DevelopmentConfig struct {
	ZstdDecoderConcurrency        int         `toml:"zstd_decoder_concurrency" mapstructure:"zstd_decoder_concurrency"`                 // was: zstd-decoder-concurrency
	MaxConcurrentFlushers         int         `toml:"max_concurrent_flushers" mapstructure:"max_concurrent_flushers"`                   // was: max-concurrent-flushers
	SnapshotAppendVecWorkers      int         `toml:"snapshot_append_vec_workers" mapstructure:"snapshot_append_vec_workers"`           // Snapshot appendvec write workers
	SnapshotIndexBuilderWorkers   int         `toml:"snapshot_index_builder_workers" mapstructure:"snapshot_index_builder_workers"`     // Snapshot index parsing workers
	SnapshotIndexCommitterWorkers int         `toml:"snapshot_index_committer_workers" mapstructure:"snapshot_index_committer_workers"` // Snapshot index shard enqueue workers
	SnapshotIndexShards           int         `toml:"snapshot_index_shards" mapstructure:"snapshot_index_shards"`                       // Snapshot account-index shard count
	SnapshotIndexTempDir          string      `toml:"snapshot_index_temp_dir" mapstructure:"snapshot_index_temp_dir"`                   // Optional temp dir for snapshot index shard logs/SST staging
	ParamArenaSizeMB              uint64      `toml:"param_arena_size_mb" mapstructure:"param_arena_size_mb"`                           // was: param-arena-size-mb
	BorrowedAccountArenaSize      uint64      `toml:"borrowed_account_arena_size" mapstructure:"borrowed_account_arena_size"`           // was: borrowed-account-arena-size
	UsePool                       bool        `toml:"use_pool" mapstructure:"use_pool"`                                                 // was: use-pool
	Pprof                         PprofConfig `toml:"pprof" mapstructure:"pprof"`
	ProgramCacheMaxMB             int         `toml:"program_cache_max_mb" mapstructure:"program_cache_max_mb"`               // Approximate SBPF program cache size in MiB
	CommonAccountCacheMaxMB       int         `toml:"common_account_cache_max_mb" mapstructure:"common_account_cache_max_mb"` // Retained decoded account cache budget in MiB
	Debug                         DebugConfig `toml:"debug" mapstructure:"debug"`
}

// ReportingConfig holds metrics/reporting configuration (matches Firedancer [reporting] section)
type ReportingConfig struct {
	MetricsPath string `toml:"metrics_path" mapstructure:"metrics_path"` // was: metrics-filename
}

// BlockConfig holds block source configuration
type BlockConfig struct {
	Source               string `toml:"source" mapstructure:"source"`                               // "rpc", "lightbringer", or "turbine"
	LightbringerEndpoint string `toml:"lightbringer_endpoint" mapstructure:"lightbringer_endpoint"` // Lightbringer endpoint (optional)
	TurbineBindAddr      string `toml:"turbine_bind_addr" mapstructure:"turbine_bind_addr"`         // Native turbine UDP receiver bind address (optional)
	// Repair-first catchup: resume gaps up to this many slots fill via turbine
	// repair instead of RPC getBlock (0 disables; default 1024).
	RepairCatchupMaxGapSlots int `toml:"repair_catchup_max_gap_slots" mapstructure:"repair_catchup_max_gap_slots"`
	// Repair request-rate ceiling, requests/second (0 = adaptive default). Peer
	// service health and replay starvation tune the default automatically. A
	// positive value pins the ceiling and disables auto-tuning.
	RepairMaxRequestsPerSecond int `toml:"repair_max_requests_per_second" mapstructure:"repair_max_requests_per_second"`
	// RPCFallback: allow RPC to fetch blocks at all on a shred source. False
	// (the default) is shreds-only: turbine + repair are the only block
	// path regardless of how far behind replay is; RPC serves control-plane
	// queries and, in verifying mode only, the trailing verifier. True enables RPC catchup when
	// replay is more than repair_catchup_max_gap_slots behind.
	RPCFallback bool `toml:"rpc_fallback" mapstructure:"rpc_fallback"`

	// Global fetch tuning
	MaxRPS          int `toml:"max_rps" mapstructure:"max_rps"`                           // Rate limit (requests per second)
	MaxInflight     int `toml:"max_inflight" mapstructure:"max_inflight"`                 // Max concurrent workers
	TipPollMs       int `toml:"tip_poll_interval_ms" mapstructure:"tip_poll_interval_ms"` // Tip poll interval in ms
	TipSafetyMargin int `toml:"tip_safety_margin" mapstructure:"tip_safety_margin"`       // Don't fetch within N slots of tip

	// Mode thresholds (hysteresis)
	NearTipThreshold int `toml:"near_tip_threshold" mapstructure:"near_tip_threshold"` // Enter near-tip when gap <= this
	CatchupThreshold int `toml:"catchup_threshold" mapstructure:"catchup_threshold"`   // Exit near-tip when gap >= this

	// Tip gate: only apply safety margin when gap > this
	CatchupTipGateThreshold int `toml:"catchup_tip_gate_threshold" mapstructure:"catchup_tip_gate_threshold"`

	// Near-tip tuning
	NearTipPollMs    int `toml:"near_tip_poll_interval_ms" mapstructure:"near_tip_poll_interval_ms"` // Faster poll in near-tip
	NearTipLookahead int `toml:"near_tip_lookahead" mapstructure:"near_tip_lookahead"`               // Slots ahead to schedule
}

// TurbineConfig holds native gossip/turbine receiver configuration.
type TurbineConfig struct {
	BindAddr         string `toml:"bind_addr" mapstructure:"bind_addr"`                 // UDP address for incoming turbine shreds
	GossipEntrypoint string `toml:"gossip_entrypoint" mapstructure:"gossip_entrypoint"` // Solana gossip entrypoint for turbine tree joining
	GossipBindAddr   string `toml:"gossip_bind_addr" mapstructure:"gossip_bind_addr"`   // UDP address for Mithril gossip traffic
	AdvertisedIP     string `toml:"advertised_ip" mapstructure:"advertised_ip"`         // Public IP to advertise in gossip
	ShredVersion     uint16 `toml:"shred_version" mapstructure:"shred_version"`         // Optional override; normally discovered from entrypoint
}

// SnapshotConfig holds snapshot download configuration
type SnapshotConfig struct {
	// MaxFullSnapshots controls both saving and retention:
	//   0 = Stream-only mode (don't save snapshots to disk)
	//   1+ = Save snapshots and keep up to N on disk
	MaxFullSnapshots int    `toml:"max_full_snapshots" mapstructure:"max_full_snapshots"`
	DownloadPath     string `toml:"download_path" mapstructure:"download_path"` // Path to download snapshot to

	// Output verbosity
	Verbose bool `toml:"verbose" mapstructure:"verbose"` // Enable detailed statistics output

	// AlwaysDownloadFull controls whether to always download a new full snapshot
	// even if a valid one exists on disk within the age threshold.
	// When false (default), uses existing full snapshot if fresh enough.
	AlwaysDownloadFull bool `toml:"always_download_full" mapstructure:"always_download_full"`

	// Stage 1: Fast parallel triage
	Stage1WarmKiB     int64 `toml:"stage1_warm_kib" mapstructure:"stage1_warm_kib"`
	Stage1WindowKiB   int64 `toml:"stage1_window_kib" mapstructure:"stage1_window_kib"`
	Stage1Windows     int   `toml:"stage1_windows" mapstructure:"stage1_windows"`
	Stage1TimeoutMS   int64 `toml:"stage1_timeout_ms" mapstructure:"stage1_timeout_ms"`
	Stage1Concurrency int   `toml:"stage1_concurrency" mapstructure:"stage1_concurrency"`

	// Stage 2: Sustained speed test
	Stage2TopK       int     `toml:"stage2_top_k" mapstructure:"stage2_top_k"`
	Stage2WarmSec    int     `toml:"stage2_warm_sec" mapstructure:"stage2_warm_sec"`
	Stage2MeasureSec int     `toml:"stage2_measure_sec" mapstructure:"stage2_measure_sec"`
	Stage2MinRatio   float64 `toml:"stage2_min_ratio" mapstructure:"stage2_min_ratio"`
	Stage2MinAbsMBs  float64 `toml:"stage2_min_abs_mbs" mapstructure:"stage2_min_abs_mbs"`

	// Node filtering
	MaxRTTMs            int      `toml:"max_rtt_ms" mapstructure:"max_rtt_ms"`
	TCPTimeoutMs        int      `toml:"tcp_timeout_ms" mapstructure:"tcp_timeout_ms"`
	MinNodeVersion      string   `toml:"min_node_version" mapstructure:"min_node_version"`
	AllowedNodeVersions []string `toml:"allowed_node_versions" mapstructure:"allowed_node_versions"`
	NodeBlacklist       []string `toml:"node_blacklist" mapstructure:"node_blacklist"`

	// Snapshot age thresholds
	FullThreshold        int `toml:"full_threshold" mapstructure:"full_threshold"`
	IncrementalThreshold int `toml:"incremental_threshold" mapstructure:"incremental_threshold"`
	SafetyMarginSlots    int `toml:"safety_margin_slots" mapstructure:"safety_margin_slots"`

	// Performance
	WorkerCount int `toml:"worker_count" mapstructure:"worker_count"`

	// Fallback resilience
	MaxSnapshotURLAttempts int `toml:"max_snapshot_url_attempts" mapstructure:"max_snapshot_url_attempts"`

	// Incremental snapshot selection
	MinIncrementalSpeedMBs float64 `toml:"min_incremental_speed_mbs" mapstructure:"min_incremental_speed_mbs"`
}

// LightbringerConfig holds Lightbringer sidecar configuration.
// When Enabled, Mithril manages Lightbringer's lifecycle: generates its config,
// spawns the process, captures logs, and shuts it down on exit.
type LightbringerConfig struct {
	Enabled          bool   `toml:"enabled" mapstructure:"enabled"`                     // Enable managed Lightbringer sidecar
	BinaryPath       string `toml:"binary_path" mapstructure:"binary_path"`             // Path to lightbringer binary
	GossipEntrypoint string `toml:"gossip_entrypoint" mapstructure:"gossip_entrypoint"` // Solana gossip entrypoint (required when enabled)
	Storage          string `toml:"storage" mapstructure:"storage"`                     // Shred storage directory
	RpcAddr          string `toml:"rpc_addr" mapstructure:"rpc_addr"`                   // Debug HTTP endpoint
	GrpcAddr         string `toml:"grpc_addr" mapstructure:"grpc_addr"`                 // gRPC stream endpoint (auto-synced to block.lightbringer_endpoint)
	ConfigDir        string `toml:"config_dir" mapstructure:"config_dir"`               // Directory to write Lightbringer.toml

	// Optional: InfluxDB metrics — written as [influxdb] section in generated Lightbringer.toml
	InfluxdbHost     string `toml:"influxdb_host" mapstructure:"influxdb_host"`
	InfluxdbDatabase string `toml:"influxdb_database" mapstructure:"influxdb_database"`
	InfluxdbToken    string `toml:"influxdb_token" mapstructure:"influxdb_token"`

	// Optional: Block confirmation — written as [block_confirmation] section in generated Lightbringer.toml
	BlockConfirmRpcHTTP string `toml:"block_confirmation_rpc_http" mapstructure:"block_confirmation_rpc_http"`
	BlockConfirmRpcWS   string `toml:"block_confirmation_rpc_websocket" mapstructure:"block_confirmation_rpc_websocket"`

	// Quiet suppresses Lightbringer info/debug logs when true. Defaults to true. Written as
	// [log] quiet = true in the generated Lightbringer.toml.
	Quiet bool `toml:"quiet" mapstructure:"quiet"`
}

// LogConfig holds logging configuration
type LogConfig struct {
	Dir        string `toml:"dir" mapstructure:"dir"`                   // Log directory (default: /mnt/mithril-logs)
	Level      string `toml:"level" mapstructure:"level"`               // Log level: debug, info, warn, error
	ToStdout   bool   `toml:"to_stdout" mapstructure:"to_stdout"`       // Also write to stdout (default: true)
	MaxSizeMB  int    `toml:"max_size_mb" mapstructure:"max_size_mb"`   // Max log file size in MB before rotation
	MaxAgeDays int    `toml:"max_age_days" mapstructure:"max_age_days"` // Delete logs older than this many days
	MaxBackups int    `toml:"max_backups" mapstructure:"max_backups"`   // Keep up to N old log files
}

// ConsensusConfig holds Alpenglow consensus configuration.
type ConsensusConfig struct {
	Mode                      string `toml:"mode" mapstructure:"mode"`                                                 // "verifying" or Alpenglow-only active "validator"
	AlpenglowObserverBindAddr string `toml:"alpenglow_observer_bind_addr" mapstructure:"alpenglow_observer_bind_addr"` // Optional passive Alpenglow Votor QUIC listener
	AlpenglowMaxMessageBytes  int64  `toml:"alpenglow_max_message_bytes" mapstructure:"alpenglow_max_message_bytes"`   // Max Votor QUIC datagram payload size
	AlpenglowBLSDST           string `toml:"alpenglow_bls_dst" mapstructure:"alpenglow_bls_dst"`                       // BLS hash-to-curve DST; empty = default (must match cluster's solana-bls version)
}

// ValidatorConfig holds Alpenglow TPU/block-production identity and listener settings.
type ValidatorConfig struct {
	IdentityKeypair             string `toml:"identity_keypair" mapstructure:"identity_keypair"`                           // Validator identity keypair used for native gossip
	VoteAccountKeypair          string `toml:"vote_account_keypair" mapstructure:"vote_account_keypair"`                   // On-chain vote account public key or keypair path
	AuthorizedVoterKeypair      string `toml:"authorized_voter_keypair" mapstructure:"authorized_voter_keypair"`           // Authorized voter used to derive the registered Alpenglow BLS key
	AuthorizedWithdrawerKeypair string `toml:"authorized_withdrawer_keypair" mapstructure:"authorized_withdrawer_keypair"` // Authorized withdrawer keypair path for diagnostics
	TPUQUICBindAddr             string `toml:"tpu_quic_bind_addr" mapstructure:"tpu_quic_bind_addr"`
	AdvertisedIP                string `toml:"advertised_ip" mapstructure:"advertised_ip"`
	TPUSigverifyWorkers         int    `toml:"tpu_sigverify_workers" mapstructure:"tpu_sigverify_workers"`
}

// Config holds all configuration options for Mithril (Firedancer-style hierarchy)
type Config struct {
	// Top-level (matches Firedancer style)
	Name             string `toml:"name" mapstructure:"name"`
	ScratchDirectory string `toml:"scratch_directory" mapstructure:"scratch_directory"` // was: scratchdir

	// Sections
	Ledger       LedgerConfig       `toml:"ledger" mapstructure:"ledger"`
	Rpc          RpcConfig          `toml:"rpc" mapstructure:"rpc"`
	Replay       ReplayConfig       `toml:"replay" mapstructure:"replay"`
	Block        BlockConfig        `toml:"block" mapstructure:"block"`
	Consensus    ConsensusConfig    `toml:"consensus" mapstructure:"consensus"`
	Validator    ValidatorConfig    `toml:"validator" mapstructure:"validator"`
	Lightbringer LightbringerConfig `toml:"lightbringer" mapstructure:"lightbringer"`
	Turbine      TurbineConfig      `toml:"turbine" mapstructure:"turbine"`
	Snapshot     SnapshotConfig     `toml:"snapshot" mapstructure:"snapshot"`
	Development  DevelopmentConfig  `toml:"development" mapstructure:"development"`
	Reporting    ReportingConfig    `toml:"reporting" mapstructure:"reporting"`
	Log          LogConfig          `toml:"log" mapstructure:"log"`
}

// ConfigFile holds the path to the config file (set via --config flag)
var ConfigFile string

// InitConfig loads configuration from TOML file.
// If no --config flag is provided, defaults to "config.toml" in current directory.
// CLI flag precedence is handled separately in initConfigAndBindFlags.
// FileUsed reports the config file viper actually loaded ("" when none) —
// the first thing to name when a value surprises the operator.
func FileUsed() string {
	return viper.ConfigFileUsed()
}

func InitConfig() error {
	ApplyDefaults(viper.GetViper())

	configPath := ConfigFile
	if configPath == "" {
		configPath = "config.toml" // Default config file
	}

	// Get the directory and filename
	dir := filepath.Dir(configPath)
	filename := filepath.Base(configPath)

	// Remove extension for viper
	ext := filepath.Ext(filename)
	name := strings.TrimSuffix(filename, ext)

	viper.SetConfigName(name)
	viper.SetConfigType("toml")
	viper.AddConfigPath(dir)

	// Try to read config file (not an error if default doesn't exist)
	if err := viper.ReadInConfig(); err != nil {
		// Only return error if user explicitly specified a config file
		if ConfigFile != "" {
			return fmt.Errorf("error reading config file: %w", err)
		}
		// Default config.toml not found is fine, just continue without it
	}

	// Note: We don't bind flags here - precedence is handled manually
	// by checking cmd.Flags().Lookup(name).Changed in initConfigAndBindFlags

	return nil
}

// GetString returns a string value from viper (config file or flag)
func GetString(key string) string {
	return viper.GetString(key)
}

// GetInt returns an int value from viper (config file or flag)
func GetInt(key string) int {
	return viper.GetInt(key)
}

// GetInt64 returns an int64 value from viper (config file or flag)
func GetInt64(key string) int64 {
	return viper.GetInt64(key)
}

// GetUint64 returns a uint64 value from viper (config file or flag)
func GetUint64(key string) uint64 {
	return viper.GetUint64(key)
}

// GetBool returns a bool value from viper (config file or flag)
func GetBool(key string) bool {
	return viper.GetBool(key)
}

// GetStringSlice returns a string slice value from viper (config file or flag)
func GetStringSlice(key string) []string {
	return viper.GetStringSlice(key)
}

// IsSet returns true if a key has been set (either via config file or flag)
func IsSet(key string) bool {
	return viper.IsSet(key)
}

// GetFloat64 returns a float64 value from viper (config file or flag)
func GetFloat64(key string) float64 {
	return viper.GetFloat64(key)
}
