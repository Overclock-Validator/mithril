package config

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

// LedgerConfig holds ledger-related configuration (matches Firedancer [ledger] section)
type LedgerConfig struct {
	Path                string `toml:"path" mapstructure:"path"`                                 // was: blockdir
	AccountsPath        string `toml:"accounts_path" mapstructure:"accounts_path"`               // was: out
	SnapshotArchivePath string `toml:"snapshot_archive_path" mapstructure:"snapshot_archive_path"` // was: path
	IncrementalSnapshot string `toml:"incremental_snapshot" mapstructure:"incremental_snapshot"` // was: incremental-snapshot-filename
}

// RpcConfig holds RPC-related configuration (matches Firedancer [rpc] section)
type RpcConfig struct {
	Rpc  []string `toml:"rpc" mapstructure:"rpc"` // List of RPC endpoints
	Port int      `toml:"port" mapstructure:"port"` // was: rpc-server-port
}

// ReplayConfig holds replay-related configuration
type ReplayConfig struct {
	Txpar              int64 `toml:"txpar" mapstructure:"txpar"`                             // UNCHANGED
	NumSlots           int64 `toml:"num_slots" mapstructure:"num_slots"`                     // was: num-replay-slots
	EndSlot            int64 `toml:"end_slot" mapstructure:"end_slot"`                       // was: endslot
	LoadFromSnapshot   bool  `toml:"load_from_snapshot" mapstructure:"load_from_snapshot"`   // was: snapshot
	LoadFromAccountsDb bool  `toml:"load_from_accounts_db" mapstructure:"load_from_accounts_db"` // was: accountsdb
}

// PprofConfig holds pprof-related configuration
type PprofConfig struct {
	Port           int64  `toml:"port" mapstructure:"port"`                       // was: pprofport
	CpuProfilePath string `toml:"cpu_profile_path" mapstructure:"cpu_profile_path"` // was: cpuprof-filename
}

// DebugConfig holds debug-related configuration
type DebugConfig struct {
	TransactionSignatures []string `toml:"transaction_signatures" mapstructure:"transaction_signatures"` // was: debugtx
	AccountWrites         []string `toml:"account_writes" mapstructure:"account_writes"`                 // was: debugacctwrites
}

// CacheConfig holds LRU cache sizing for AccountsDB
// These are DIFFERENT from the global vote/stake caches used for leader schedule -
// those are unbounded maps holding vote state and delegation info.
// These LRU caches store full account data for fast reads during replay.
type CacheConfig struct {
	VoteAcctLRU          int `toml:"vote_acct_lru" mapstructure:"vote_acct_lru"`                     // Vote account data (default: 5000)
	StakeAcctLRU         int `toml:"stake_acct_lru" mapstructure:"stake_acct_lru"`                   // Stake account data (default: 2000)
	SmallAcctLRU         int `toml:"small_acct_lru" mapstructure:"small_acct_lru"`                   // Small accounts ≤512 bytes (default: 50000)
	MediumAcctLRU        int `toml:"medium_acct_lru" mapstructure:"medium_acct_lru"`                 // Medium accounts 512-64KB (default: 20000)
	HugeAcctLRU          int `toml:"huge_acct_lru" mapstructure:"huge_acct_lru"`                     // Huge accounts >64KB (default: 500)
	ProgramLRU           int `toml:"program_lru" mapstructure:"program_lru"`                         // Compiled BPF programs (default: 5000)
	SeenOnceFilterSize   int `toml:"seen_once_filter_size" mapstructure:"seen_once_filter_size"`     // Admit-on-second-hit LRU capacity (0=disabled, default: 0)
}

// DevelopmentConfig holds development/tuning configuration (matches Firedancer [development] section)
type DevelopmentConfig struct {
	ZstdDecoderConcurrency   int         `toml:"zstd_decoder_concurrency" mapstructure:"zstd_decoder_concurrency"`     // was: zstd-decoder-concurrency
	MaxConcurrentFlushers    int         `toml:"max_concurrent_flushers" mapstructure:"max_concurrent_flushers"`       // was: max-concurrent-flushers
	ParamArenaSizeMB         uint64      `toml:"param_arena_size_mb" mapstructure:"param_arena_size_mb"`               // was: param-arena-size-mb
	BorrowedAccountArenaSize uint64      `toml:"borrowed_account_arena_size" mapstructure:"borrowed_account_arena_size"` // was: borrowed-account-arena-size
	UsePool                  bool        `toml:"use_pool" mapstructure:"use_pool"`                                     // was: use-pool
	Pprof                    PprofConfig `toml:"pprof" mapstructure:"pprof"`
	Debug                    DebugConfig `toml:"debug" mapstructure:"debug"`
	Cache                    CacheConfig `toml:"cache" mapstructure:"cache"`
}

// ReportingConfig holds metrics/reporting configuration (matches Firedancer [reporting] section)
type ReportingConfig struct {
	MetricsPath string `toml:"metrics_path" mapstructure:"metrics_path"` // was: metrics-filename
}

// BlockConfig holds block source configuration
type BlockConfig struct {
	Source              string `toml:"source" mapstructure:"source"`                             // "rpc" or "lightbringer"
	LightbringerEndpoint string `toml:"lightbringer_endpoint" mapstructure:"lightbringer_endpoint"` // Lightbringer endpoint (optional)

	// Global fetch tuning
	MaxRPS          int `toml:"max_rps" mapstructure:"max_rps"`                       // Rate limit (requests per second)
	MaxInflight     int `toml:"max_inflight" mapstructure:"max_inflight"`             // Max concurrent workers
	TipPollMs       int `toml:"tip_poll_interval_ms" mapstructure:"tip_poll_interval_ms"` // Tip poll interval in ms
	TipSafetyMargin int `toml:"tip_safety_margin" mapstructure:"tip_safety_margin"`   // Don't fetch within N slots of tip

	// Mode thresholds (hysteresis)
	NearTipThreshold int `toml:"near_tip_threshold" mapstructure:"near_tip_threshold"` // Enter near-tip when gap <= this
	CatchupThreshold int `toml:"catchup_threshold" mapstructure:"catchup_threshold"`   // Exit near-tip when gap >= this

	// Tip gate: only apply safety margin when gap > this
	CatchupTipGateThreshold int `toml:"catchup_tip_gate_threshold" mapstructure:"catchup_tip_gate_threshold"`

	// Near-tip tuning
	NearTipPollMs    int `toml:"near_tip_poll_interval_ms" mapstructure:"near_tip_poll_interval_ms"` // Faster poll in near-tip
	NearTipLookahead int `toml:"near_tip_lookahead" mapstructure:"near_tip_lookahead"`               // Slots ahead to schedule
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

// LogConfig holds logging configuration
type LogConfig struct {
	Dir        string `toml:"dir" mapstructure:"dir"`                 // Log directory (default: /mnt/mithril-logs)
	Level      string `toml:"level" mapstructure:"level"`             // Log level: debug, info, warn, error
	ToStdout   bool   `toml:"to_stdout" mapstructure:"to_stdout"`     // Also write to stdout (default: true)
	MaxSizeMB  int    `toml:"max_size_mb" mapstructure:"max_size_mb"` // Max log file size in MB before rotation
	MaxAgeDays int    `toml:"max_age_days" mapstructure:"max_age_days"` // Delete logs older than this many days
	MaxBackups int    `toml:"max_backups" mapstructure:"max_backups"` // Keep up to N old log files
}

// Config holds all configuration options for Mithril (Firedancer-style hierarchy)
type Config struct {
	// Top-level (matches Firedancer style)
	Name             string `toml:"name" mapstructure:"name"`
	ScratchDirectory string `toml:"scratch_directory" mapstructure:"scratch_directory"` // was: scratchdir

	// Sections
	Ledger      LedgerConfig      `toml:"ledger" mapstructure:"ledger"`
	Rpc         RpcConfig         `toml:"rpc" mapstructure:"rpc"`
	Replay      ReplayConfig      `toml:"replay" mapstructure:"replay"`
	Block       BlockConfig       `toml:"block" mapstructure:"block"`
	Snapshot    SnapshotConfig    `toml:"snapshot" mapstructure:"snapshot"`
	Development DevelopmentConfig `toml:"development" mapstructure:"development"`
	Reporting   ReportingConfig   `toml:"reporting" mapstructure:"reporting"`
	Log         LogConfig         `toml:"log" mapstructure:"log"`
}

// ConfigFile holds the path to the config file (set via --config flag)
var ConfigFile string

// InitConfig loads configuration from TOML file.
// If no --config flag is provided, defaults to "config.toml" in current directory.
// CLI flag precedence is handled separately in initConfigAndBindFlags.
func InitConfig() error {
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
