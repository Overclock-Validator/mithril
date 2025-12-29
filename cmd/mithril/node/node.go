package node

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/snapshot"
	"github.com/Overclock-Validator/mithril/pkg/snapshotdl"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

var (
	VerifyRange = cobra.Command{
		Use:   "verify-range",
		Short: "Verify a range of slots from snapshot",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initConfigAndBindFlags(cmd)
		},
		Run: func(cmd *cobra.Command, args []string) {
			runVerifyRange(cmd, args)
		},
	}

	// Run is the main command for running Mithril as a live verifier.
	// This is the primary way most users will run Mithril.
	Run = cobra.Command{
		Use:   "run",
		Short: "Run Mithril as a live verifier (downloads snapshot, builds AccountsDB, verifies blocks)",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initConfigAndBindFlags(cmd)
		},
		Run: func(cmd *cobra.Command, args []string) {
			runLive(cmd, args)
		},
	}

	// VerifyLive is an alias for Run (kept for backwards compatibility)
	VerifyLive = cobra.Command{
		Use:    "verify-live",
		Short:  "Alias for 'run' (deprecated, use 'mithril run' instead)",
		Hidden: true, // Hide from help but still works
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initConfigAndBindFlags(cmd)
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Note: 'verify-live' is deprecated. Use 'mithril run' instead.")
			runLive(cmd, args)
		},
	}

	loadFromSnapshot            bool
	loadFromAccountsDb          bool
	bootstrapMode               string // "auto", "snapshot", or "accountsdb"
	snapshotArchivePath         string
	incrementalSnapshotFilename string
	accountsPath                string
	scratchDirectory            string
	rpcEndpoints                []string
	blockSource                 string // "rpc" or "overcast"
	overcastEndpoint            string
	snapshotDlPath              string
	numReplaySlots              int64
	endSlot                     int64
	pprofPort                   int64
	ledgerPath                  string
	txParallelism               int64

	debugTxs        []string
	debugAcctWrites []string
	metricsPath     string
	cpuprofPath     string

	paramArenaSizeMB         uint64
	borrowedAccountArenaSize uint64
	persistProgramCache      bool = true
	voteCacheSize            int
	programCacheSize         int
	commonCacheSize          int

	rpcPort int
)

func init() {
	// flags for verifier mode
	// [replay] section flags
	VerifyRange.Flags().BoolVarP(&loadFromSnapshot, "load-from-snapshot", "s", false, "Load from a full snapshot")
	VerifyRange.Flags().BoolVarP(&loadFromAccountsDb, "load-from-accounts-db", "a", false, "Load from AccountsDB")
	VerifyRange.Flags().Int64Var(&numReplaySlots, "num-slots", 0, "Number of slots to replay.")
	VerifyRange.Flags().Int64VarP(&endSlot, "end-slot", "e", -1, "Block at which to stop replaying, inclusive")
	VerifyRange.Flags().Int64Var(&txParallelism, "txpar", 0, "Set to 0 to use sequential execution, or >0 to execute a topsort tx plan with the given number of workers")

	// [ledger] section flags
	VerifyRange.Flags().StringVarP(&snapshotArchivePath, "snapshot-archive-path", "p", "", "Path of full snapshot or AccountsDB to load from")
	VerifyRange.Flags().StringVar(&incrementalSnapshotFilename, "incremental-snapshot", "", "Filename containing incremental snapshot")
	VerifyRange.Flags().StringVarP(&accountsPath, "accounts-path", "o", "", "Output path for writing AccountsDB data to")
	VerifyRange.Flags().StringVar(&ledgerPath, "ledger-path", "/tmp/blocks", "Path containing slot.json files")

	// [rpc] section flags
	VerifyRange.Flags().StringSliceVarP(&rpcEndpoints, "rpc", "r", []string{}, "URL(s) for RPC endpoint(s) - can specify multiple")
	VerifyRange.Flags().IntVar(&rpcPort, "rpc-port", 0, "RPC server port. Default off.")

	// [development] section flags
	VerifyRange.Flags().Uint64Var(&paramArenaSizeMB, "param-arena-size-mb", 512, "Size in MB for serialized parameter arena (0 to disable)")
	VerifyRange.Flags().Uint64Var(&borrowedAccountArenaSize, "borrowed-account-arena-size", 1024, "Number of borrowed accounts to preallocate in arena (0 to disable)")
	VerifyRange.Flags().IntVar(&snapshot.ZstdDecoderConcurrency, "zstd-decoder-concurrency", runtime.NumCPU(), "Zstd decoder concurrency")
	VerifyRange.Flags().IntVar(&snapshot.MaxConcurrentFlushers, "max-concurrent-flushers", 16, "Bound for number of log shards to flush to Accounts DB Index at once.")
	VerifyRange.Flags().BoolVar(&sbpf.UsePool, "use-pool", true, "Disable to allocate fresh slices")
	VerifyRange.Flags().IntVar(&voteCacheSize, "vote-cache-size", 2000, "Max vote accounts to cache")
	VerifyRange.Flags().IntVar(&programCacheSize, "program-cache-size", 5000, "Max compiled programs to cache")
	VerifyRange.Flags().IntVar(&commonCacheSize, "common-cache-size", 10000, "Max common accounts to cache")

	// [tuning.pprof] section flags
	VerifyRange.Flags().Int64Var(&pprofPort, "pprof-port", -1, "Port to serve HTTP pprof endpoint")
	VerifyRange.Flags().StringVar(&cpuprofPath, "cpu-profile-path", "", "Filename to write CPU profile")

	// [debug] section flags
	VerifyRange.Flags().StringSliceVar(&debugTxs, "transaction-signatures", []string{}, "Pass tx signature strings to enable debug logging during that transaction's execution")
	VerifyRange.Flags().StringSliceVar(&debugAcctWrites, "account-writes", []string{}, "Pass account pubkeys to enable debug logging of transactions that modify the account")

	// [reporting] section flags
	VerifyRange.Flags().StringVar(&metricsPath, "metrics-path", "", "Filename to write JSONL records of latencies")

	// [overcast] section flags
	VerifyRange.Flags().StringVar(&snapshotDlPath, "download-snapshot-path", "", "Path to download snapshot to")

	// flags for 'mithril run' (live verifier mode)
	// [bootstrap] section flags
	Run.Flags().StringVar(&bootstrapMode, "bootstrap-mode", "auto", "Bootstrap mode: 'auto' (use AccountsDB if exists, else snapshot), 'snapshot' (always download), 'accountsdb' (require existing)")

	// [ledger] section flags
	Run.Flags().StringVarP(&accountsPath, "accounts-path", "o", "", "Output path for writing AccountsDB data to")
	Run.Flags().StringVar(&ledgerPath, "ledger-path", "/tmp/blocks", "Path containing slot.json files")

	// [rpc] section flags
	Run.Flags().StringSliceVarP(&rpcEndpoints, "rpc", "r", []string{}, "URL(s) for RPC endpoint(s) - can specify multiple")
	Run.Flags().IntVar(&rpcPort, "rpc-port", 0, "RPC server port. Default off.")

	// [replay] section flags
	Run.Flags().Int64Var(&txParallelism, "txpar", 0, "Set to 0 to use sequential execution, or >0 to execute a topsort tx plan with the given number of workers")

	// [tuning] section flags
	Run.Flags().Uint64Var(&paramArenaSizeMB, "param-arena-size-mb", 512, "Size in MB for serialized parameter arena (0 to disable)")
	Run.Flags().Uint64Var(&borrowedAccountArenaSize, "borrowed-account-arena-size", 1024, "Number of borrowed accounts to preallocate in arena (0 to disable)")
	Run.Flags().IntVar(&snapshot.ZstdDecoderConcurrency, "zstd-decoder-concurrency", runtime.NumCPU(), "Zstd decoder concurrency")
	Run.Flags().IntVar(&snapshot.MaxConcurrentFlushers, "max-concurrent-flushers", 16, "Bound for number of log shards to flush to Accounts DB Index at once.")
	Run.Flags().BoolVar(&sbpf.UsePool, "use-pool", true, "Disable to allocate fresh slices")
	Run.Flags().IntVar(&voteCacheSize, "vote-cache-size", 2000, "Max vote accounts to cache")
	Run.Flags().IntVar(&programCacheSize, "program-cache-size", 5000, "Max compiled programs to cache")
	Run.Flags().IntVar(&commonCacheSize, "common-cache-size", 10000, "Max common accounts to cache")

	// [tuning.pprof] section flags
	Run.Flags().StringVar(&cpuprofPath, "cpu-profile-path", "", "Filename to write CPU profile")

	// [debug] section flags
	Run.Flags().StringSliceVar(&debugTxs, "transaction-signatures", []string{}, "Pass tx signature strings to enable debug logging during that transaction's execution")
	Run.Flags().StringSliceVar(&debugAcctWrites, "account-writes", []string{}, "Pass account pubkeys to enable debug logging of transactions that modify the account")

	// [reporting] section flags
	Run.Flags().StringVar(&metricsPath, "metrics-path", "", "Filename to write JSONL records of latencies")

	// Top-level flags
	Run.Flags().StringVar(&scratchDirectory, "scratch-directory", "/tmp", "Path for downloads (e.g. snapshots) and other temp state")

	// [block] section flags
	Run.Flags().StringVar(&blockSource, "block-source", "rpc", "Block source: 'rpc' or 'overcast'")
	Run.Flags().StringVar(&overcastEndpoint, "overcast-endpoint", "", "Address for Overcast endpoint (only used when block-source=overcast)")

	// Copy all Run flags to VerifyLive for backwards compatibility
	VerifyLive.Flags().AddFlagSet(Run.Flags())
}

// initConfigAndBindFlags loads TOML config file (if specified) and binds flags to viper.
// After this runs, config values can be read from either CLI flags or config file,
// with CLI flags taking precedence.
func initConfigAndBindFlags(cmd *cobra.Command) error {
	// Initialize config from file (do NOT bind flags - we handle precedence manually)
	if err := config.InitConfig(); err != nil {
		return err
	}

	// Check if a CLI flag was explicitly set by the user
	flagChanged := func(name string) bool {
		if f := cmd.Flags().Lookup(name); f != nil {
			return f.Changed
		}
		return false
	}

	// Helper to get string: CLI flag if explicitly set, otherwise TOML config
	getString := func(cliKey, tomlKey string) string {
		if flagChanged(cliKey) {
			if f := cmd.Flags().Lookup(cliKey); f != nil {
				return f.Value.String()
			}
		}
		return config.GetString(tomlKey)
	}

	// Helper to get int: CLI flag if explicitly set, then TOML config, then flag default
	getInt := func(cliKey, tomlKey string) int {
		if flagChanged(cliKey) {
			if f := cmd.Flags().Lookup(cliKey); f != nil {
				if v, err := strconv.Atoi(f.Value.String()); err == nil {
					return v
				}
			}
		}
		if config.IsSet(tomlKey) {
			return config.GetInt(tomlKey)
		}
		// Fall back to flag default value
		if f := cmd.Flags().Lookup(cliKey); f != nil {
			if v, err := strconv.Atoi(f.DefValue); err == nil {
				return v
			}
		}
		return 0
	}

	// Helper to get int64: CLI flag if explicitly set, then TOML config, then flag default
	getInt64 := func(cliKey, tomlKey string) int64 {
		if flagChanged(cliKey) {
			if f := cmd.Flags().Lookup(cliKey); f != nil {
				if v, err := strconv.ParseInt(f.Value.String(), 10, 64); err == nil {
					return v
				}
			}
		}
		if config.IsSet(tomlKey) {
			return config.GetInt64(tomlKey)
		}
		// Fall back to flag default value
		if f := cmd.Flags().Lookup(cliKey); f != nil {
			if v, err := strconv.ParseInt(f.DefValue, 10, 64); err == nil {
				return v
			}
		}
		return 0
	}

	// Helper to get uint64: CLI flag if explicitly set, then TOML config, then flag default
	getUint64 := func(cliKey, tomlKey string) uint64 {
		if flagChanged(cliKey) {
			if f := cmd.Flags().Lookup(cliKey); f != nil {
				if v, err := strconv.ParseUint(f.Value.String(), 10, 64); err == nil {
					return v
				}
			}
		}
		if config.IsSet(tomlKey) {
			return config.GetUint64(tomlKey)
		}
		// Fall back to flag default value
		if f := cmd.Flags().Lookup(cliKey); f != nil {
			if v, err := strconv.ParseUint(f.DefValue, 10, 64); err == nil {
				return v
			}
		}
		return 0
	}

	// Helper to get bool: CLI flag if explicitly set, otherwise TOML config
	getBool := func(cliKey, tomlKey string) bool {
		if flagChanged(cliKey) {
			if f := cmd.Flags().Lookup(cliKey); f != nil {
				return f.Value.String() == "true"
			}
		}
		return config.GetBool(tomlKey)
	}

	// Helper to get string slice: CLI flag if explicitly set, otherwise TOML config
	getStringSlice := func(cliKey, tomlKey string) []string {
		if flagChanged(cliKey) {
			// Get value directly from the flag, not viper (flags aren't bound)
			if f := cmd.Flags().Lookup(cliKey); f != nil {
				if ss, ok := f.Value.(interface{ GetSlice() []string }); ok {
					return ss.GetSlice()
				}
			}
		}
		return config.GetStringSlice(tomlKey)
	}

	// Update variables (CLI flags take precedence over TOML config when explicitly set)
	// CLI flag names -> TOML nested keys (Firedancer-style)

	// [bootstrap] section (new unified mode replacing two booleans)
	bootstrapMode = getString("bootstrap-mode", "bootstrap.mode")
	if bootstrapMode == "" {
		bootstrapMode = "snapshot" // default: always download fresh snapshot
	}

	// [replay] section (legacy booleans for verify-range)
	loadFromSnapshot = getBool("load-from-snapshot", "replay.load_from_snapshot")
	loadFromAccountsDb = getBool("load-from-accounts-db", "replay.load_from_accounts_db")
	numReplaySlots = getInt64("num-slots", "replay.num_slots")
	endSlot = getInt64("end-slot", "replay.end_slot")
	txParallelism = getInt64("txpar", "replay.txpar")

	// [storage] section (with fallback to legacy [ledger] keys for backwards compatibility)
	snapshotArchivePath = getString("snapshot-archive-path", "storage.snapshots")
	if snapshotArchivePath == "" {
		snapshotArchivePath = getString("snapshot-archive-path", "ledger.snapshot_archive_path")
	}
	incrementalSnapshotFilename = getString("incremental-snapshot", "storage.incremental_snapshot")
	if incrementalSnapshotFilename == "" {
		incrementalSnapshotFilename = getString("incremental-snapshot", "ledger.incremental_snapshot")
	}
	accountsPath = getString("accounts-path", "storage.accounts")
	if accountsPath == "" {
		accountsPath = getString("accounts-path", "ledger.accounts_path")
	}
	ledgerPath = getString("ledger-path", "storage.blockstore")
	if ledgerPath == "" {
		ledgerPath = getString("ledger-path", "ledger.path")
	}

	// [network] section (with fallback to legacy [rpc] keys)
	rpcEndpoints = getStringSlice("rpc", "network.rpc")
	if len(rpcEndpoints) == 0 {
		rpcEndpoints = getStringSlice("rpc", "rpc.rpc")
	}

	// [rpc] section - Mithril's RPC server
	rpcPort = getInt("rpc-port", "rpc.port")

	// Top-level
	scratchDirectory = getString("scratch-directory", "scratch_directory")

	// [block] section
	blockSource = getString("block-source", "block.source")
	if blockSource == "" {
		blockSource = "rpc" // default
	}
	if blockSource == "rpc" && len(rpcEndpoints) == 0 {
		return fmt.Errorf("blockSource=rpc but no endpoints were provided")
	}
	overcastEndpoint = getString("overcast-endpoint", "block.overcast_endpoint")

	// [snapshot] section
	snapshotDlPath = getString("download-snapshot-path", "snapshot.download_path")

	// [tuning.pprof] section (with fallback to legacy [development.pprof])
	pprofPort = getInt64("pprof-port", "tuning.pprof.port")
	if pprofPort == 0 {
		pprofPort = getInt64("pprof-port", "development.pprof.port")
	}
	cpuprofPath = getString("cpu-profile-path", "tuning.pprof.cpu_profile_path")
	if cpuprofPath == "" {
		cpuprofPath = getString("cpu-profile-path", "development.pprof.cpu_profile_path")
	}

	// [debug] section (with fallback to legacy [development.debug])
	debugTxs = getStringSlice("transaction-signatures", "debug.transaction_signatures")
	if len(debugTxs) == 0 {
		debugTxs = getStringSlice("transaction-signatures", "development.debug.transaction_signatures")
	}
	debugAcctWrites = getStringSlice("account-writes", "debug.account_writes")
	if len(debugAcctWrites) == 0 {
		debugAcctWrites = getStringSlice("account-writes", "development.debug.account_writes")
	}

	// [tuning] section (with fallback to legacy [development])
	paramArenaSizeMB = getUint64("param-arena-size-mb", "tuning.param_arena_size_mb")
	if paramArenaSizeMB == 0 {
		paramArenaSizeMB = getUint64("param-arena-size-mb", "development.param_arena_size_mb")
	}
	borrowedAccountArenaSize = getUint64("borrowed-account-arena-size", "tuning.borrowed_account_arena_size")
	if borrowedAccountArenaSize == 0 {
		borrowedAccountArenaSize = getUint64("borrowed-account-arena-size", "development.borrowed_account_arena_size")
	}

	// [reporting] section
	metricsPath = getString("metrics-path", "reporting.metrics_path")

	// Handle external package variables (try tuning.* first, fallback to development.*)
	if flagChanged("zstd-decoder-concurrency") {
		snapshot.ZstdDecoderConcurrency = config.GetInt("zstd-decoder-concurrency")
	} else if config.IsSet("tuning.zstd_decoder_concurrency") {
		snapshot.ZstdDecoderConcurrency = config.GetInt("tuning.zstd_decoder_concurrency")
	} else if config.IsSet("development.zstd_decoder_concurrency") {
		snapshot.ZstdDecoderConcurrency = config.GetInt("development.zstd_decoder_concurrency")
	}
	if flagChanged("max-concurrent-flushers") {
		snapshot.MaxConcurrentFlushers = config.GetInt("max-concurrent-flushers")
	} else if config.IsSet("tuning.max_concurrent_flushers") {
		snapshot.MaxConcurrentFlushers = config.GetInt("tuning.max_concurrent_flushers")
	} else if config.IsSet("development.max_concurrent_flushers") {
		snapshot.MaxConcurrentFlushers = config.GetInt("development.max_concurrent_flushers")
	}
	if flagChanged("use-pool") {
		sbpf.UsePool = config.GetBool("use-pool")
	} else if config.IsSet("tuning.use_pool") {
		sbpf.UsePool = config.GetBool("tuning.use_pool")
	} else if config.IsSet("development.use_pool") {
		sbpf.UsePool = config.GetBool("development.use_pool")
	}

	// Read persist_program_cache setting (try tuning.*, fallback to development.*)
	if config.IsSet("tuning.persist_program_cache") {
		persistProgramCache = config.GetBool("tuning.persist_program_cache")
	} else if config.IsSet("development.persist_program_cache") {
		persistProgramCache = config.GetBool("development.persist_program_cache")
	}

	// Read cache size settings (CLI flags take precedence, then tuning.*, then development.*)
	if !flagChanged("vote-cache-size") {
		if config.IsSet("tuning.vote_cache_size") {
			voteCacheSize = config.GetInt("tuning.vote_cache_size")
		} else if config.IsSet("development.vote_cache_size") {
			voteCacheSize = config.GetInt("development.vote_cache_size")
		}
	}
	if !flagChanged("program-cache-size") {
		if config.IsSet("tuning.program_cache_size") {
			programCacheSize = config.GetInt("tuning.program_cache_size")
		} else if config.IsSet("development.program_cache_size") {
			programCacheSize = config.GetInt("development.program_cache_size")
		}
	}
	if !flagChanged("common-cache-size") {
		if config.IsSet("tuning.common_cache_size") {
			commonCacheSize = config.GetInt("tuning.common_cache_size")
		} else if config.IsSet("development.common_cache_size") {
			commonCacheSize = config.GetInt("development.common_cache_size")
		}
	}

	return nil
}

// buildSnapshotConfig creates a snapshotdl.SnapshotConfig from the mithril config.
// It starts with defaults and overrides with any values set in the TOML file.
func buildSnapshotConfig() snapshotdl.SnapshotConfig {
	cfg := snapshotdl.DefaultSnapshotConfig()

	// Override with TOML values if set
	if config.IsSet("snapshot.verbose") {
		cfg.Verbose = config.GetBool("snapshot.verbose")
	}
	if config.IsSet("snapshot.download_path") {
		cfg.DownloadPath = config.GetString("snapshot.download_path")
	}
	if config.IsSet("snapshot.stage1_warm_kib") {
		cfg.Stage1WarmKiB = config.GetInt64("snapshot.stage1_warm_kib")
	}
	if config.IsSet("snapshot.stage1_window_kib") {
		cfg.Stage1WindowKiB = config.GetInt64("snapshot.stage1_window_kib")
	}
	if config.IsSet("snapshot.stage1_windows") {
		cfg.Stage1Windows = config.GetInt("snapshot.stage1_windows")
	}
	if config.IsSet("snapshot.stage1_timeout_ms") {
		cfg.Stage1TimeoutMS = config.GetInt64("snapshot.stage1_timeout_ms")
	}
	if config.IsSet("snapshot.stage1_concurrency") {
		cfg.Stage1Concurrency = config.GetInt("snapshot.stage1_concurrency")
	}
	if config.IsSet("snapshot.stage2_top_k") {
		cfg.Stage2TopK = config.GetInt("snapshot.stage2_top_k")
	}
	if config.IsSet("snapshot.stage2_warm_sec") {
		cfg.Stage2WarmSec = config.GetInt("snapshot.stage2_warm_sec")
	}
	if config.IsSet("snapshot.stage2_measure_sec") {
		cfg.Stage2MeasureSec = config.GetInt("snapshot.stage2_measure_sec")
	}
	if config.IsSet("snapshot.stage2_min_ratio") {
		cfg.Stage2MinRatio = config.GetFloat64("snapshot.stage2_min_ratio")
	}
	if config.IsSet("snapshot.stage2_min_abs_mbs") {
		cfg.Stage2MinAbsMBs = config.GetFloat64("snapshot.stage2_min_abs_mbs")
	}
	if config.IsSet("snapshot.max_rtt_ms") {
		cfg.MaxRTTMs = config.GetInt("snapshot.max_rtt_ms")
	}
	if config.IsSet("snapshot.tcp_timeout_ms") {
		cfg.TCPTimeoutMs = config.GetInt("snapshot.tcp_timeout_ms")
	}
	if config.IsSet("snapshot.min_node_version") {
		cfg.MinNodeVersion = config.GetString("snapshot.min_node_version")
	}
	if config.IsSet("snapshot.allowed_node_versions") {
		cfg.AllowedNodeVersions = config.GetStringSlice("snapshot.allowed_node_versions")
	}
	if config.IsSet("snapshot.full_threshold") {
		cfg.FullThreshold = config.GetInt("snapshot.full_threshold")
	}
	if config.IsSet("snapshot.incremental_threshold") {
		cfg.IncrementalThreshold = config.GetInt("snapshot.incremental_threshold")
	}
	if config.IsSet("snapshot.safety_margin_slots") {
		cfg.SafetyMarginSlots = config.GetInt("snapshot.safety_margin_slots")
	}
	if config.IsSet("snapshot.max_full_snapshots") {
		cfg.MaxFullSnapshots = config.GetInt("snapshot.max_full_snapshots")
	}
	if config.IsSet("snapshot.worker_count") {
		cfg.WorkerCount = config.GetInt("snapshot.worker_count")
	}
	if config.IsSet("snapshot.max_snapshot_url_attempts") {
		cfg.MaxSnapshotURLAttempts = config.GetInt("snapshot.max_snapshot_url_attempts")
	}
	if config.IsSet("snapshot.min_incremental_speed_mbs") {
		cfg.MinIncrementalSpeedMBs = config.GetFloat64("snapshot.min_incremental_speed_mbs")
	}

	return cfg
}

func runVerifyRange(c *cobra.Command, args []string) {
	ctx := c.Context()
	if pprofPort != -1 {
		startPprofHandlers(int(pprofPort))
	}
	if endSlot != -1 && numReplaySlots != 0 {
		klog.Fatalf("specify at most one of --end-slot and --num-slots")
	}

	if !loadFromSnapshot && !loadFromAccountsDb && snapshotDlPath == "" {
		klog.Fatalf("must specify either to load from a snapshot, or load from an existing AccountsDB, or download a snapshot.")
	}

	var err error
	var accountsDbDir string
	var accountsDb *accountsdb.AccountsDb
	var manifest *snapshot.SnapshotManifest
	dbgOpts, err := replay.NewDebugOptions(debugTxs, debugAcctWrites)
	if err != nil {
		klog.Fatalf("failed to parse --transaction-signatures or --account-writes values: %v", err)
	}

	logVCSInfo()
	cpuprofWriter, cpuprofCleanup, err := createBufWriter(cpuprofPath)
	if err != nil {
		klog.Fatalf("unable to create metrics writer to filename=%s: %v", metricsPath, err)
	}
	defer cpuprofCleanup()
	if cpuprofWriter != nil {
		pprof.StartCPUProfile(cpuprofWriter)
		defer pprof.StopCPUProfile()
	}

	if len(rpcEndpoints) == 0 {
		rpcEndpoints = []string{"https://api.mainnet-beta.solana.com"}
	}

	if loadFromSnapshot {
		if snapshotArchivePath == "" || accountsPath == "" {
			klog.Fatalf("must specify snapshot path and directory path for writing generated AccountsDB")
		}

		mlog.Log.Infof("building AccountsDB from snapshot at %s\n", snapshotArchivePath)

		// extract accountvecs from full snapshot, build accountsdb index, and write it all out to disk
		accountsDb, manifest, err = snapshot.BuildAccountsDb(ctx, snapshotArchivePath, incrementalSnapshotFilename, accountsPath)
		if err != nil {
			klog.Fatalf("failed to populate new accounts db from snapshot %s: %s", snapshotArchivePath, err)
		}

		//mlog.Log.Debugf("successfully created accounts db from snapshot %s", snapshotArchivePath)

		accountsDbDir = accountsPath
	} else if loadFromAccountsDb {
		if snapshotArchivePath == "" {
			klog.Fatalf("must specify an AccountsDB directory path to load from")
		}

		accountsDbDir = snapshotArchivePath
	} else if snapshotDlPath != "" {
		if accountsPath == "" {
			klog.Fatalf("must specify a path to download a snapshot to")
		}

		mlog.Log.Infof("downloading snapshot...")

		snapCfg := buildSnapshotConfig()
		var dlPath string
		dlPath, _, _, err = snapshotdl.DownloadSnapshotWithConfig(rpcEndpoints[0], snapshotDlPath, snapCfg)
		if err != nil {
			klog.Fatalf("error downloading snapshot: %s", err)
		}

		accountsDb, manifest, err = snapshot.BuildAccountsDb(ctx, dlPath, incrementalSnapshotFilename, accountsPath)
		if err != nil {
			klog.Fatalf("failed to populate new accounts db from snapshot %s: %s", dlPath, err)
		}

		accountsDbDir = accountsPath
	}

	if accountsDb == nil || manifest == nil {
		mlog.Log.Infof("loading from AccountsDB at %s", accountsDbDir)

		accountsDb, err = accountsdb.OpenDb(accountsDbDir)
		if err != nil {
			klog.Fatalf("unable to open accounts db %s\n", accountsDbDir)
		}

		manifest, err = snapshot.LoadManifestFromFile(filepath.Join(accountsDbDir, "manifest"))
		if err != nil {
			klog.Fatalf("unable to open manifest file")
		}
	}

	startSlot := int64(manifest.Bank.Slot + 1)
	if endSlot != -1 {
		numReplaySlots = endSlot - startSlot
	} else if numReplaySlots != 0 {
		endSlot = startSlot + numReplaySlots
	}

	// just processing the snapshot - not executing blocks.
	if numReplaySlots == 0 {
		return
	}

	if endSlot != -1 && endSlot < startSlot {
		klog.Fatalf("end slot cannot be lower than start slot")
	}
	mlog.Log.Infof("will replay startSlot=%d endSlot=%d", startSlot, endSlot)

	mlog.Log.Infof("initializing caches (program cache persistence: %v)", persistProgramCache)
	accountsDb.InitCachesWithConfig(accountsdb.CacheConfig{
		VoteCacheSize:    voteCacheSize,
		ProgramCacheSize: programCacheSize,
		CommonCacheSize:  commonCacheSize,
	})
	if persistProgramCache {
		if err := accountsDb.LoadProgramCache(manifest.Bank.Slot); err != nil {
			mlog.Log.Infof("warning: failed to load program cache: %v", err)
		}
	}

	metricsWriter, metricsWriterCleanup, err := createBufWriter(metricsPath)
	if err != nil {
		klog.Fatalf("unable to create metrics writer to filename=%s: %v", metricsPath, err)
	}
	defer metricsWriterCleanup()

	if paramArenaSizeMB > 0 {
		replay.SerializedParameterArena = arena.New[byte](paramArenaSizeMB << 20)
	}
	if borrowedAccountArenaSize > 0 {
		sealevel.BorrowedAccountArenas = make([]*arena.Arena[sealevel.BorrowedAccount], txParallelism)
		for i := range txParallelism {
			sealevel.BorrowedAccountArenas[i] = arena.New[sealevel.BorrowedAccount](borrowedAccountArenaSize)
		}
	}

	var rpcServer *rpcserver.RpcServer
	if rpcPort < 0 || rpcPort > 65535 {
		klog.Fatalf("invalid port: %d", rpcPort)
	} else if rpcPort != 0 {
		rpcServer = rpcserver.NewRpcServer(accountsDb, uint16(rpcPort))
		rpcServer.Start()
		mlog.Log.Infof("started RPC server on port %d", rpcPort)
	}

	replay.ReplayBlocks(ctx, accountsDb, accountsDbDir, manifest, uint64(startSlot), uint64(endSlot), rpcEndpoints[0], ledgerPath, int(txParallelism), false, false, dbgOpts, metricsWriter, rpcServer)
	mlog.Log.Infof("done replaying, closing DB")
	if persistProgramCache {
		if err := accountsDb.SaveProgramCache(manifest.Bank.Slot); err != nil {
			mlog.Log.Infof("warning: failed to save program cache: %v", err)
		}
	}
	accountsDb.CloseDb()
}

func runLive(c *cobra.Command, args []string) {
	ctx := c.Context()

	// Print the Mithril banner first, before any other output
	progress.PrintBanner()

	// Kill any existing mithril processes to prevent zombie accumulation
	if killed := killExistingMithrilProcesses(); killed > 0 {
		fmt.Printf("  ⚠ Killed %d existing mithril process(es)\n\n", killed)
	}

	// Print consolidated startup info
	printStartupInfo("run")

	// Now start the metrics server (after banner so errors don't appear first)
	statsd.StartMetricsServer()

	// Determine if using Overcast based on block source
	useOvercast := blockSource == "overcast"
	if useOvercast && overcastEndpoint == "" {
		mlog.Log.Infof("block.source=overcast but no overcast_endpoint provided, falling back to RPC")
		useOvercast = false
	}

	dbgOpts, err := replay.NewDebugOptions(debugTxs, debugAcctWrites)
	if err != nil {
		klog.Fatalf("failed to parse --transaction-signatures or --account-writes values: %v", err)
	}

	cpuprofWriter, cpuprofCleanup, err := createBufWriter(cpuprofPath)
	if err != nil {
		klog.Fatalf("unable to create metrics writer to filename=%s: %v", metricsPath, err)
	}
	defer cpuprofCleanup()
	if cpuprofWriter != nil {
		pprof.StartCPUProfile(cpuprofWriter)
		defer pprof.StopCPUProfile()
	}

	if len(rpcEndpoints) == 0 {
		rpcEndpoints = []string{"https://api.mainnet-beta.solana.com"}
	}

	// Bootstrap: determine how to initialize AccountsDB based on mode
	var accountsDb *accountsdb.AccountsDb
	var manifest *snapshot.SnapshotManifest
	snapshotDownloadPath := scratchDirectory

	// Detect existing state
	hasAccountsDB, accountsDBSlot := detectExistingAccountsDB(accountsPath)

	// Pass overcast endpoint if using overcast, otherwise empty string for RPC mode
	var overcastAddr string
	if useOvercast {
		overcastAddr = overcastEndpoint
	}

	switch bootstrapMode {
	case "accountsdb":
		// Mode: Require existing AccountsDB, never download
		if !hasAccountsDB {
			klog.Fatalf("mode=accountsdb requires existing AccountsDB at %s", accountsPath)
		}
		mlog.Log.Infof("resuming from existing AccountsDB at slot %d", accountsDBSlot)
		accountsDb, err = accountsdb.OpenDb(accountsPath)
		if err != nil {
			klog.Fatalf("failed to open AccountsDB at %s: %v", accountsPath, err)
		}
		manifest, err = snapshot.LoadManifestFromFile(filepath.Join(accountsPath, "manifest"))
		if err != nil {
			klog.Fatalf("failed to load manifest: %v", err)
		}

	case "snapshot":
		// Mode: Always download fresh snapshot, clean up existing data
		mlog.Log.Infof("mode=snapshot: downloading fresh snapshot")
		if accountsPath != "" {
			mlog.Log.Infof("cleaning up previous AccountsDB artifacts in %s", accountsPath)
			snapshot.CleanAccountsDbDir(accountsPath)
		}
		// Clean up old snapshot files based on retention settings
		if snapshotDownloadPath != "" {
			maxSnapshots := config.GetInt("snapshot.max_full_snapshots")
			if maxSnapshots == 0 {
				maxSnapshots = 2 // default
			}
			deleteOld := config.GetBool("snapshot.delete_old_snapshots")
			snapshot.CleanSnapshotDownloadDir(snapshotDownloadPath, maxSnapshots, deleteOld)
		}
		accountsDb, manifest, err = downloadAndBuildFromSnapshot(ctx, rpcEndpoints, snapshotDownloadPath, accountsPath, ledgerPath, overcastAddr)
		if err != nil {
			klog.Fatalf("failed to build AccountsDB from snapshot: %v", err)
		}

	case "auto":
		fallthrough
	default:
		// Mode: auto - prefer AccountsDB, then existing snapshot, then download
		if hasAccountsDB {
			mlog.Log.Infof("mode=auto: resuming from existing AccountsDB at slot %d", accountsDBSlot)
			accountsDb, err = accountsdb.OpenDb(accountsPath)
			if err != nil {
				klog.Fatalf("failed to open AccountsDB at %s: %v", accountsPath, err)
			}
			manifest, err = snapshot.LoadManifestFromFile(filepath.Join(accountsPath, "manifest"))
			if err != nil {
				klog.Fatalf("failed to load manifest: %v", err)
			}
		} else {
			// No existing AccountsDB - need to build from snapshot
			mlog.Log.Infof("mode=auto: no existing AccountsDB, will download snapshot")
			if accountsPath != "" {
				mlog.Log.Infof("cleaning up previous AccountsDB artifacts in %s", accountsPath)
				snapshot.CleanAccountsDbDir(accountsPath)
			}
			// Clean up old snapshot files based on retention settings
			if snapshotDownloadPath != "" {
				maxSnapshots := config.GetInt("snapshot.max_full_snapshots")
				if maxSnapshots == 0 {
					maxSnapshots = 2 // default
				}
				deleteOld := config.GetBool("snapshot.delete_old_snapshots")
				snapshot.CleanSnapshotDownloadDir(snapshotDownloadPath, maxSnapshots, deleteOld)
			}
			accountsDb, manifest, err = downloadAndBuildFromSnapshot(ctx, rpcEndpoints, snapshotDownloadPath, accountsPath, ledgerPath, overcastAddr)
			if err != nil {
				klog.Fatalf("failed to build AccountsDB from snapshot: %v", err)
			}
		}
	}

	mlog.Log.Infof("AccountsDB ready at slot %d", manifest.Bank.Slot)

	startSlot := int64(manifest.Bank.Slot + 1)
	liveEndSlot := uint64(math.MaxUint64)

	mlog.Log.Infof("starting replay from slot %d", startSlot)

	mlog.Log.Infof("initializing caches (program cache persistence: %v)", persistProgramCache)
	accountsDb.InitCachesWithConfig(accountsdb.CacheConfig{
		VoteCacheSize:    voteCacheSize,
		ProgramCacheSize: programCacheSize,
		CommonCacheSize:  commonCacheSize,
	})
	if persistProgramCache {
		if err := accountsDb.LoadProgramCache(manifest.Bank.Slot); err != nil {
			mlog.Log.Infof("warning: failed to load program cache: %v", err)
		}
	}

	metricsWriter, metricsWriterCleanup, err := createBufWriter(metricsPath)
	if err != nil {
		klog.Fatalf("unable to create metrics writer to filename=%s: %v", metricsPath, err)
	}
	defer metricsWriterCleanup()

	if paramArenaSizeMB > 0 {
		replay.SerializedParameterArena = arena.New[byte](paramArenaSizeMB << 20)
	}
	if borrowedAccountArenaSize > 0 {
		sealevel.BorrowedAccountArenas = make([]*arena.Arena[sealevel.BorrowedAccount], txParallelism)
		for i := range txParallelism {
			sealevel.BorrowedAccountArenas[i] = arena.New[sealevel.BorrowedAccount](borrowedAccountArenaSize)
		}
	}

	var rpcServer *rpcserver.RpcServer
	if rpcPort < 0 || rpcPort > 65535 {
		klog.Fatalf("invalid port: %d", rpcPort)
	} else if rpcPort != 0 {
		rpcServer = rpcserver.NewRpcServer(accountsDb, uint16(rpcPort))
		rpcServer.Start()
		mlog.Log.Infof("started RPC server on port %d", rpcPort)
	}

	replay.ReplayBlocks(ctx, accountsDb, accountsPath, manifest, uint64(startSlot), liveEndSlot, rpcEndpoints[0], ledgerPath, int(txParallelism), true, useOvercast, dbgOpts, metricsWriter, rpcServer)
	mlog.Log.Infof("done replaying, closing DB")
	if persistProgramCache {
		if err := accountsDb.SaveProgramCache(manifest.Bank.Slot); err != nil {
			mlog.Log.Infof("warning: failed to save program cache: %v", err)
		}
	}
	accountsDb.CloseDb()
}

func logVCSInfo() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		mlog.Log.Errorf("VCS info: not available")
		return
	}

	var revision, vcsTime, modified string

	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.time":
			vcsTime = setting.Value
		case "vcs.modified":
			modified = setting.Value
		}
	}

	mlog.Log.Infof("VCS info: revision=%s time=%s modified=%s", revision, vcsTime, modified)
}

// printStartupInfo prints consolidated startup info including version, timestamp, and configuration
func printStartupInfo(commandName string) {
	// Gold/amber color like the banner
	gold := "\x1b[38;2;217;164;65m"
	reset := "\x1b[0m"
	dim := "\x1b[2m"
	green := "\x1b[32m"
	cyan := "\x1b[36m"

	fmt.Printf("%s━━━ Startup Info ━━━%s\n", gold, reset)

	// Get version info from build
	var revision, modified string
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				modified = setting.Value
			}
		}
	}

	// Shorten the commit hash to 8 chars
	if len(revision) > 8 {
		revision = revision[:8]
	}

	// Timestamp (current time)
	fmt.Printf("  Timestamp:    %s%s%s\n", dim, time.Now().Format(time.RFC3339), reset)

	// Commit info
	if revision != "" {
		commitStr := revision
		if modified == "true" {
			commitStr += " (modified)"
		}
		fmt.Printf("  Commit:       %s%s%s\n", dim, commitStr, reset)
	}

	// Go version
	fmt.Printf("  Go:           %s%s%s\n", dim, runtime.Version(), reset)

	// Command being run
	fmt.Printf("  Command:      %s%s%s\n", cyan, commandName, reset)

	// Detect what's on disk
	hasAccountsDB, accountsDBSlot := detectExistingAccountsDB(accountsPath)

	// Check for existing snapshots - try configured paths in order of preference:
	// 1. snapshotArchivePath (storage.snapshots)
	// 2. snapshotDlPath (snapshot.download_path)
	// 3. scratchDirectory (scratch_directory)
	var existingSnapshots []snapshotInfo
	for _, searchPath := range []string{snapshotArchivePath, snapshotDlPath, scratchDirectory} {
		if searchPath != "" {
			if snaps := detectExistingSnapshots(searchPath); len(snaps) > 0 {
				existingSnapshots = snaps
				break
			}
		}
	}

	// Determine actual action based on bootstrap mode and what exists
	var actionStr string
	var actionDetail string

	switch bootstrapMode {
	case "auto":
		if hasAccountsDB {
			actionStr = "Resuming from AccountsDB"
			if accountsDBSlot > 0 {
				actionDetail = fmt.Sprintf("slot %d", accountsDBSlot)
			}
		} else if len(existingSnapshots) > 0 {
			actionStr = "Building from existing snapshot"
			actionDetail = existingSnapshots[0].filename
		} else {
			actionStr = "Downloading new snapshot"
		}
	case "snapshot":
		actionStr = "Downloading new snapshot"
		actionDetail = "fresh start"
	case "accountsdb":
		if hasAccountsDB {
			actionStr = "Resuming from AccountsDB"
			if accountsDBSlot > 0 {
				actionDetail = fmt.Sprintf("slot %d", accountsDBSlot)
			}
		} else {
			actionStr = "ERROR: No AccountsDB found"
			actionDetail = "mode=accountsdb requires existing data"
		}
	}

	// Print bootstrap action (the main thing users care about)
	fmt.Printf("  Bootstrap:    %s%s%s", green, actionStr, reset)
	if actionDetail != "" {
		fmt.Printf(" %s(%s)%s", dim, actionDetail, reset)
	}
	fmt.Printf(" %s[mode=%s]%s\n", dim, bootstrapMode, reset)

	// Show existing AccountsDB info if present
	if hasAccountsDB && bootstrapMode != "snapshot" {
		fmt.Printf("  AccountsDB:   %s%s%s", cyan, accountsPath, reset)
		if accountsDBSlot > 0 {
			fmt.Printf(" %s(slot %d)%s\n", dim, accountsDBSlot, reset)
		} else {
			fmt.Println()
		}
	} else if accountsPath != "" {
		fmt.Printf("  AccountsDB:   %s%s%s %s(will create)%s\n", gold, accountsPath, reset, dim, reset)
	}

	// Show existing full snapshot only when actually using it
	// (mode=auto without AccountsDB and an existing snapshot will be used)
	usingExistingSnapshot := bootstrapMode == "auto" && !hasAccountsDB && len(existingSnapshots) > 0
	if usingExistingSnapshot {
		// Only show full snapshots, not incrementals
		for _, snap := range existingSnapshots {
			if snap.isIncr {
				continue // skip incremental snapshots
			}
			fmt.Printf("  Snapshot:     %s%s%s", cyan, snap.filename, reset)
			if snap.slot > 0 {
				fmt.Printf(" %s(slot %d)%s", dim, snap.slot, reset)
			}
			fmt.Println()
			break // only show first full snapshot
		}
	}

	// Block source
	fmt.Printf("  Block source: %s%s%s", gold, blockSource, reset)
	if blockSource == "overcast" && overcastEndpoint != "" {
		fmt.Printf(" %s(%s)%s\n", dim, overcastEndpoint, reset)
	} else {
		fmt.Println()
	}

	// Blockstore path
	if ledgerPath != "" {
		fmt.Printf("  Blockstore:   %s%s%s\n", gold, ledgerPath, reset)
	}

	// RPC endpoints
	if len(rpcEndpoints) > 0 {
		fmt.Printf("  RPC:          %s%s%s\n", gold, rpcEndpoints[0], reset)
		for _, ep := range rpcEndpoints[1:] {
			fmt.Printf("                %s%s%s\n", gold, ep, reset)
		}
	}

	fmt.Println()
}

// snapshotInfo holds information about a detected snapshot file
type snapshotInfo struct {
	filename string
	slot     uint64
	isIncr   bool
}

// detectExistingAccountsDB checks if a valid AccountsDB exists at the given path
// Returns (exists, slot) where slot is parsed from the manifest if available
func detectExistingAccountsDB(path string) (bool, uint64) {
	if path == "" {
		return false, 0
	}

	// Check for the accounts directory
	accountsDir := filepath.Join(path, "accounts")
	if _, err := os.Stat(accountsDir); os.IsNotExist(err) {
		return false, 0
	}

	// Check for manifest file
	manifestPath := filepath.Join(path, "manifest")
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		return false, 0
	}

	// Try to load the manifest to get the slot
	manifest, err := snapshot.LoadManifestFromFile(manifestPath)
	if err != nil {
		// AccountsDB exists but manifest is unreadable
		return true, 0
	}

	return true, manifest.Bank.Slot
}

// detectExistingSnapshots finds snapshot files in the given directory
func detectExistingSnapshots(dir string) []snapshotInfo {
	if dir == "" {
		return nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	var snapshots []snapshotInfo

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()

		// Full snapshot: snapshot-{slot}-{hash}.tar.zst
		if len(name) > 9 && name[:9] == "snapshot-" && filepath.Ext(name) == ".zst" {
			slot := parseSlotFromSnapshotName(name)
			snapshots = append(snapshots, snapshotInfo{
				filename: name,
				slot:     slot,
				isIncr:   false,
			})
		}

		// Incremental snapshot: incremental-snapshot-{baseSlot}-{endSlot}-{hash}.tar.zst
		if len(name) > 21 && name[:21] == "incremental-snapshot-" && filepath.Ext(name) == ".zst" {
			slot := parseSlotFromIncrementalName(name)
			snapshots = append(snapshots, snapshotInfo{
				filename: name,
				slot:     slot,
				isIncr:   true,
			})
		}
	}

	return snapshots
}

// parseSlotFromSnapshotName extracts slot from "snapshot-{slot}-{hash}.tar.zst"
func parseSlotFromSnapshotName(name string) uint64 {
	// Remove "snapshot-" prefix and ".tar.zst" suffix
	if len(name) <= 17 {
		return 0
	}
	trimmed := name[9 : len(name)-8] // "slot-hash"
	// Find the first dash after the slot number
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] == '-' {
			slot, err := strconv.ParseUint(trimmed[:i], 10, 64)
			if err != nil {
				return 0
			}
			return slot
		}
	}
	return 0
}

// parseSlotFromIncrementalName extracts end slot from "incremental-snapshot-{baseSlot}-{endSlot}-{hash}.tar.zst"
func parseSlotFromIncrementalName(name string) uint64 {
	// Remove "incremental-snapshot-" prefix and ".tar.zst" suffix
	if len(name) <= 29 {
		return 0
	}
	trimmed := name[21 : len(name)-8] // "baseSlot-endSlot-hash"

	// Find first dash (after baseSlot)
	firstDash := -1
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] == '-' {
			firstDash = i
			break
		}
	}
	if firstDash == -1 {
		return 0
	}

	// Find second dash (after endSlot)
	remaining := trimmed[firstDash+1:]
	for i := 0; i < len(remaining); i++ {
		if remaining[i] == '-' {
			slot, err := strconv.ParseUint(remaining[:i], 10, 64)
			if err != nil {
				return 0
			}
			return slot
		}
	}
	return 0
}

// downloadAndBuildFromSnapshot finds, downloads, and builds AccountsDB from a snapshot
func downloadAndBuildFromSnapshot(ctx context.Context, rpcEndpoints []string, snapshotDownloadPath, accountsPath, ledgerPath, overcastAddr string) (*accountsdb.AccountsDb, *snapshot.SnapshotManifest, error) {
	snapCfg := buildSnapshotConfig()
	fullSnapshotDlStart := time.Now()
	fullSnapshotInfo, err := snapshotdl.GetSnapshotURLWithInfo(rpcEndpoints[0], snapCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting snapshot URL: %w", err)
	}
	fullSnapshotURL := fullSnapshotInfo.URL
	fullSnapshotSlot := fullSnapshotInfo.Slot

	// Print a clean summary of the selected snapshot source
	progress.PrintSnapshotSourceSummary(
		fullSnapshotInfo.NodeIP,
		fullSnapshotInfo.Slot,
		fullSnapshotInfo.ReferenceSlot,
		fullSnapshotInfo.NodeVersion,
		fullSnapshotInfo.SpeedMBs,
		fullSnapshotInfo.RTTMs,
		time.Since(fullSnapshotDlStart),
	)

	// Create progress display for snapshot download and extract
	dp := progress.NewDualProgress()

	accountsDb, manifest, err := snapshot.BuildAccountsDbWithIncr(ctx, fullSnapshotURL, snapshotDownloadPath, fullSnapshotSlot, fullSnapshotSlot, accountsPath, rpcEndpoints, ledgerPath, overcastAddr, snapCfg, dp)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build AccountsDB from snapshot: %w", err)
	}
	mlog.Log.Infof("finished building AccountsDB")

	return accountsDb, manifest, nil
}

// killExistingMithrilProcesses finds and kills any other running mithril processes.
// This prevents zombie processes from accumulating and holding disk space.
// Returns the number of processes killed.
func killExistingMithrilProcesses() int {
	myPID := os.Getpid()
	myPPID := os.Getppid()

	// Use pgrep to find mithril processes by executable name (not full command line)
	// This avoids matching sudo or shell processes that have "mithril" in args
	cmd := exec.Command("pgrep", "-x", "mithril")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		// No processes found or pgrep not available
		return 0
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	killed := 0

	for _, line := range lines {
		if line == "" {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil {
			continue
		}

		// Don't kill ourselves or our parent (sudo)
		if pid == myPID || pid == myPPID {
			continue
		}

		// Try to kill the process
		proc, err := os.FindProcess(pid)
		if err != nil {
			continue
		}

		// Send SIGKILL
		if err := proc.Signal(syscall.SIGKILL); err == nil {
			killed++
		}
	}

	return killed
}

func createBufWriter(filename string) (io.Writer, func(), error) {
	if filename == "" {
		return nil, func() {}, nil
	}

	file, err := os.Create(filename)
	if err != nil {
		return nil, nil, err
	}

	writer := bufio.NewWriter(file)

	cleanup := func() {
		writer.Flush()
		file.Close()
	}

	return writer, cleanup, nil
}
