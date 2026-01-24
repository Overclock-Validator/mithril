package node

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
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

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/snapshot"
	"github.com/Overclock-Validator/mithril/pkg/snapshotdl"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/Overclock-Validator/mithril/pkg/version"
	solrpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/mr-tron/base58"
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

	// Run is the main command for running Mithril as a live full node.
	// This is the primary way most users will run Mithril.
	Run = cobra.Command{
		Use:   "run",
		Short: "Run Mithril full node (downloads snapshot, builds AccountsDB, replays blocks)",
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
	cluster                     string // "mainnet-beta", "testnet", "devnet"
	blockSource                 string // "rpc" or "lightbringer"
	lightbringerEndpoint        string
	blockMaxRPS                 int // Rate limit for block fetching
	blockMaxInflight            int // Max concurrent block fetch workers
	blockTipPollIntervalMs      int // Tip poll interval in milliseconds
	blockTipSafetyMargin        int // Don't fetch within N slots of tip

	// Mode thresholds
	blockNearTipThreshold        int // Enter near-tip when gap <= this
	blockCatchupThreshold        int // Exit near-tip when gap >= this
	blockCatchupTipGateThreshold int // Only apply safety margin when gap > this

	// Near-tip tuning
	blockNearTipPollMs    int // Faster poll in near-tip mode
	blockNearTipLookahead int // Slots ahead to schedule in near-tip

	snapshotDlPath string
	logDir         string
	numReplaySlots int64
	endSlot        int64
	pprofPort      int64
	blockstorePath string
	txParallelism  int64

	debugTxs        []string
	debugAcctWrites []string
	cpuprofPath     string

	paramArenaSizeMB         uint64
	borrowedAccountArenaSize uint64

	rpcPort int
)

func init() {
	// flags for verify-range mode
	// [replay] section flags
	VerifyRange.Flags().BoolVarP(&loadFromSnapshot, "load-from-snapshot", "s", false, "Load from a full snapshot")
	VerifyRange.Flags().BoolVarP(&loadFromAccountsDb, "load-from-accounts-db", "a", false, "Load from AccountsDB")
	VerifyRange.Flags().Int64Var(&numReplaySlots, "num-slots", 0, "Number of slots to replay.")
	VerifyRange.Flags().Int64VarP(&endSlot, "end-slot", "e", -1, "Block at which to stop replaying, inclusive")
	VerifyRange.Flags().Int64Var(&txParallelism, "txpar", 0, "Set to 0 to use sequential execution, or >0 to execute a topsort tx plan with the given number of workers")

	// [ledger] section flags
	VerifyRange.Flags().StringVar(&snapshotArchivePath, "snapshot", "", "Filename containing full snapshot")
	VerifyRange.Flags().StringVar(&incrementalSnapshotFilename, "incremental-snapshot", "", "Filename containing incremental snapshot")
	VerifyRange.Flags().StringVarP(&accountsPath, "accounts-path", "o", "", "Output path for writing AccountsDB data to")
	VerifyRange.Flags().StringVar(&blockstorePath, "ledger-path", "/tmp/blocks", "Path containing slot.json files")

	// [rpc] section flags
	VerifyRange.Flags().StringSliceVarP(&rpcEndpoints, "rpc", "r", []string{}, "URL(s) for RPC endpoint(s) - can specify multiple")
	VerifyRange.Flags().IntVar(&rpcPort, "rpc-port", 0, "RPC server port. Default off.")

	// [development] section flags
	VerifyRange.Flags().Uint64Var(&paramArenaSizeMB, "param-arena-size-mb", 512, "Size in MB for serialized parameter arena (0 to disable)")
	VerifyRange.Flags().Uint64Var(&borrowedAccountArenaSize, "borrowed-account-arena-size", 1024, "Number of borrowed accounts to preallocate in arena (0 to disable)")
	VerifyRange.Flags().IntVar(&snapshot.ZstdDecoderConcurrency, "zstd-decoder-concurrency", runtime.NumCPU(), "Zstd decoder concurrency")
	VerifyRange.Flags().IntVar(&snapshot.MaxConcurrentFlushers, "max-concurrent-flushers", 16, "Bound for number of log shards to flush to Accounts DB Index at once.")
	VerifyRange.Flags().BoolVar(&sbpf.UsePool, "use-pool", true, "Disable to allocate fresh slices")

	// [tuning.pprof] section flags
	VerifyRange.Flags().Int64Var(&pprofPort, "pprof-port", -1, "Port to serve HTTP pprof endpoint")
	VerifyRange.Flags().StringVar(&cpuprofPath, "cpu-profile-path", "", "Filename to write CPU profile")

	// [debug] section flags
	VerifyRange.Flags().StringSliceVar(&debugTxs, "transaction-signatures", []string{}, "Pass tx signature strings to enable debug logging during that transaction's execution")
	VerifyRange.Flags().StringSliceVar(&debugAcctWrites, "account-writes", []string{}, "Pass account pubkeys to enable debug logging of transactions that modify the account")

	// [lightbringer] section flags
	VerifyRange.Flags().StringVar(&snapshotDlPath, "download-snapshot-path", "", "Path to download snapshot to")

	// flags for 'mithril run' (live full node mode)
	// [bootstrap] section flags
	Run.Flags().StringVar(&bootstrapMode, "bootstrap", "auto", "Bootstrap mode: 'auto' (use AccountsDB if exists, else snapshot), 'accountsdb' (require existing), 'snapshot' (rebuild from snapshot), 'new-snapshot' (always download fresh)")
	Run.Flags().StringVar(&snapshotArchivePath, "snapshot", "", "Path to specific full snapshot file (bypasses auto-discovery)")
	Run.Flags().StringVar(&incrementalSnapshotFilename, "incremental-snapshot", "", "Path to specific incremental snapshot file (bypasses auto-discovery)")

	// [ledger] section flags
	Run.Flags().StringVarP(&accountsPath, "accounts-path", "o", "", "Output path for writing AccountsDB data to")
	Run.Flags().StringVar(&blockstorePath, "ledger-path", "/tmp/blocks", "Path containing slot.json files")

	// [network] section flags
	Run.Flags().StringSliceVarP(&rpcEndpoints, "rpc", "r", []string{}, "URL(s) for RPC endpoint(s) - can specify multiple")
	Run.Flags().StringVar(&cluster, "cluster", "", "Solana cluster: 'mainnet-beta', 'testnet', or 'devnet'")

	// [rpc] section flags (Mithril's RPC server)
	Run.Flags().IntVar(&rpcPort, "rpc-port", 0, "RPC server port. Default off.")

	// [replay] section flags
	Run.Flags().Int64Var(&txParallelism, "txpar", 0, "Set to 0 to use sequential execution, or >0 to execute a topsort tx plan with the given number of workers")

	// [tuning] section flags
	Run.Flags().Uint64Var(&paramArenaSizeMB, "param-arena-size-mb", 512, "Size in MB for serialized parameter arena (0 to disable)")
	Run.Flags().Uint64Var(&borrowedAccountArenaSize, "borrowed-account-arena-size", 1024, "Number of borrowed accounts to preallocate in arena (0 to disable)")
	Run.Flags().IntVar(&snapshot.ZstdDecoderConcurrency, "zstd-decoder-concurrency", runtime.NumCPU(), "Zstd decoder concurrency")
	Run.Flags().IntVar(&snapshot.MaxConcurrentFlushers, "max-concurrent-flushers", 16, "Bound for number of log shards to flush to Accounts DB Index at once.")
	Run.Flags().BoolVar(&sbpf.UsePool, "use-pool", true, "Disable to allocate fresh slices")
	Run.Flags().IntVar(&accountsdb.StoreAccountsWorkers, "store-accounts-workers", 128, "Number of workers to write account updates")
	Run.Flags().BoolVar(&accountsdb.StoreAsync, "store-async", false, "Store accounts asynchronously")

	// [tuning.pprof] section flags
	Run.Flags().Int64Var(&pprofPort, "pprof-port", -1, "Port to serve HTTP pprof endpoint")
	Run.Flags().StringVar(&cpuprofPath, "cpu-profile-path", "", "Filename to write CPU profile")

	// [debug] section flags
	Run.Flags().StringSliceVar(&debugTxs, "transaction-signatures", []string{}, "Pass tx signature strings to enable debug logging during that transaction's execution")
	Run.Flags().StringSliceVar(&debugAcctWrites, "account-writes", []string{}, "Pass account pubkeys to enable debug logging of transactions that modify the account")

	// Top-level flags
	Run.Flags().StringVar(&scratchDirectory, "scratch-directory", "/tmp", "Path for downloads (e.g. snapshots) and other temp state")

	// [block] section flags
	Run.Flags().StringVar(&blockSource, "block-source", "rpc", "Block source: 'rpc' or 'lightbringer'")
	Run.Flags().StringVar(&lightbringerEndpoint, "lightbringer-endpoint", "", "Address for Lightbringer endpoint (only used when block-source=lightbringer)")
	Run.Flags().IntVar(&blockMaxRPS, "block-max-rps", 0, "Max RPC requests per second for block fetching (0 = use default)")
	Run.Flags().IntVar(&blockMaxInflight, "block-max-inflight", 0, "Max concurrent block fetch workers (0 = use default)")
	Run.Flags().IntVar(&blockTipPollIntervalMs, "block-tip-poll-ms", 0, "Tip poll interval in milliseconds (0 = use default)")
	Run.Flags().IntVar(&blockTipSafetyMargin, "block-tip-safety-margin", 0, "Don't fetch within N slots of tip (0 = use default)")

	// Copy all Run flags to VerifyLive for backwards compatibility
	VerifyLive.Flags().AddFlagSet(Run.Flags())
}

// checkDirWritable verifies the current user can write to a directory.
// Returns an error with a helpful fix message if the directory exists but is not writable.
func checkDirWritable(path, description string) error {
	if path == "" {
		return nil
	}

	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		// Directory doesn't exist yet - will be created later
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot access %s at %s: %v", description, path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s path is not a directory: %s", description, path)
	}

	// Try to create a temp file to verify write permission
	testFile := filepath.Join(path, ".mithril_write_test")
	f, err := os.Create(testFile)
	if err != nil {
		// Get owner info for helpful error message
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			return fmt.Errorf("%s directory not writable: %s (owned by uid %d, running as uid %d)\n\nFix: sudo chown -R $USER:$USER %s",
				description, path, stat.Uid, os.Getuid(), path)
		}
		return fmt.Errorf("%s directory not writable: %s\n\nFix: sudo chown -R $USER:$USER %s",
			description, path, path)
	}
	f.Close()
	os.Remove(testFile)
	return nil
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

	// [bootstrap] section
	bootstrapMode = getString("bootstrap", "bootstrap.mode")
	if bootstrapMode == "" {
		bootstrapMode = "auto" // default: use existing AccountsDB if valid, else download snapshot
	}

	// [replay] section (legacy booleans for verify-range)
	loadFromSnapshot = getBool("load-from-snapshot", "replay.load_from_snapshot")
	loadFromAccountsDb = getBool("load-from-accounts-db", "replay.load_from_accounts_db")
	numReplaySlots = getInt64("num-slots", "replay.num_slots")
	endSlot = getInt64("end-slot", "replay.end_slot")
	txParallelism = getInt64("txpar", "replay.txpar")

	// [storage] section (with fallback to legacy [ledger] keys for backwards compatibility)
	// snapshotArchivePath: CLI flags --snapshot/--snapshot-archive-path ONLY (explicit file path)
	// storage.snapshots config is handled via snapshotDlPath fallback below
	snapshotArchivePath = getString("snapshot", "")
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
	// Check write permission early to fail fast with helpful error
	if err := checkDirWritable(accountsPath, "AccountsDB"); err != nil {
		return err
	}
	blockstorePath = getString("ledger-path", "storage.blockstore")
	if blockstorePath == "" {
		blockstorePath = getString("ledger-path", "ledger.path")
	}

	// [network] section (with fallback to legacy [rpc] keys)
	rpcEndpoints = getStringSlice("rpc", "network.rpc")
	if len(rpcEndpoints) == 0 {
		rpcEndpoints = getStringSlice("rpc", "rpc.rpc")
	}

	// Cluster is required for safety (prevents mainnet/testnet mixups)
	cluster = getString("cluster", "network.cluster")
	if cluster == "" {
		return fmt.Errorf("network.cluster is required - set to 'mainnet-beta', 'testnet', or 'devnet'")
	}
	// Validate cluster value
	switch cluster {
	case "mainnet-beta", "testnet", "devnet":
		// Valid
	default:
		return fmt.Errorf("invalid network.cluster %q - must be 'mainnet-beta', 'testnet', or 'devnet'", cluster)
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
	lightbringerEndpoint = getString("lightbringer-endpoint", "block.lightbringer_endpoint")

	// Validate: blockSource=rpc requires RPC endpoints
	if blockSource == "rpc" && len(rpcEndpoints) == 0 {
		return fmt.Errorf("block.source=rpc but no RPC endpoints provided (set network.rpc)")
	}

	blockMaxRPS = getInt("block-max-rps", "block.max_rps")
	blockMaxInflight = getInt("block-max-inflight", "block.max_inflight")
	blockTipPollIntervalMs = getInt("block-tip-poll-ms", "block.tip_poll_interval_ms")
	blockTipSafetyMargin = getInt("block-tip-safety-margin", "block.tip_safety_margin")

	// Mode thresholds (hysteresis)
	blockNearTipThreshold = getInt("block-near-tip-threshold", "block.near_tip_threshold")
	blockCatchupThreshold = getInt("block-catchup-threshold", "block.catchup_threshold")
	blockCatchupTipGateThreshold = getInt("block-catchup-tip-gate-threshold", "block.catchup_tip_gate_threshold")

	// Near-tip tuning
	blockNearTipPollMs = getInt("block-near-tip-poll-ms", "block.near_tip_poll_interval_ms")
	blockNearTipLookahead = getInt("block-near-tip-lookahead", "block.near_tip_lookahead")

	// Validate block fetch parameters - negative values wrap to huge uint64, causing stalls
	if blockMaxRPS < 0 {
		blockMaxRPS = 0
	}
	if blockMaxInflight < 0 {
		blockMaxInflight = 0
	}
	if blockTipPollIntervalMs < 0 {
		blockTipPollIntervalMs = 0
	}
	if blockTipSafetyMargin < 0 {
		blockTipSafetyMargin = 0
	}
	if blockNearTipThreshold < 0 {
		blockNearTipThreshold = 0
	}
	if blockCatchupThreshold < 0 {
		blockCatchupThreshold = 0
	}
	if blockCatchupTipGateThreshold < 0 {
		blockCatchupTipGateThreshold = 0
	}
	if blockNearTipPollMs < 0 {
		blockNearTipPollMs = 0
	}
	if blockNearTipLookahead < 0 {
		blockNearTipLookahead = 0
	}

	// Snapshot download path - for auto-discovery/download of snapshots
	// Priority: CLI --download-snapshot-path > snapshot.download_path > storage.snapshots
	snapshotDlPath = getString("download-snapshot-path", "snapshot.download_path")
	if snapshotDlPath == "" {
		snapshotDlPath = config.GetString("storage.snapshots")
	}
	if snapshotDlPath == "" {
		snapshotDlPath = snapshotArchivePath // Fallback to explicit path if set
	}

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

	snapshot.ZstdDecoderConcurrency = getInt("zstd-decoder-concurrency", "tuning.zstd_decoder_concurrency")
	snapshot.MaxConcurrentFlushers = getInt("max-concurrent-flushers", "tuning.max_concurrent_flushers")
	sbpf.UsePool = getBool("use-pool", "tuning.use_pool")
	accountsdb.StoreAccountsWorkers = getInt("store-accounts-workers", "tuning.store_accounts_workers")
	accountsdb.StoreAsync = getBool("store-async", "tuning.store_async")

	return nil
}

// buildSnapshotConfig creates a snapshotdl.SnapshotConfig from the mithril config.
// It starts with defaults and overrides with any values set in the TOML file.
func buildSnapshotConfig(rpcEndpoints []string) snapshotdl.SnapshotConfig {
	cfg := snapshotdl.DefaultSnapshotConfig()
	cfg.RPCAddresses = rpcEndpoints

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

	// Print consolidated startup info (same as run command)
	printStartupInfo("verify-range")

	cpuprofWriter, cpuprofCleanup, err := createBufWriter(cpuprofPath)
	if err != nil {
		klog.Fatalf("unable to create cpuprof writer to filename=%s: %v", cpuprofPath, err)
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
			klog.Fatalf("must specify snapshot path (was \"%s\") and directory path for writing generated AccountsDB (was \"%s\")", snapshotArchivePath, accountsPath)
		}

		mlog.Log.Infof("building AccountsDB from snapshot at %s\n", snapshotArchivePath)

		// extract accountvecs from full snapshot, build accountsdb index, and write it all out to disk
		dp := progress.NewDualProgress()
		accountsDb, manifest, err = snapshot.BuildAccountsDbPaths(ctx, snapshotArchivePath, incrementalSnapshotFilename, accountsPath, dp)
		if err != nil {
			klog.Fatalf("failed to populate new accounts db from snapshot %s: %s", snapshotArchivePath, err)
		}

		//mlog.Log.Debugf("successfully created accounts db from snapshot %s", snapshotArchivePath)

		accountsDbDir = accountsPath
	} else if loadFromAccountsDb {
		if accountsPath == "" {
			klog.Fatalf("must specify an AccountsDB directory path to load from")
		}

		accountsDbDir = accountsPath
	} else if snapshotDlPath != "" {
		if accountsPath == "" {
			klog.Fatalf("must specify a path to download a snapshot to")
		}

		mlog.Log.Infof("downloading snapshot...")

		snapCfg := buildSnapshotConfig(rpcEndpoints)
		var dlPath string
		dlPath, _, _, err = snapshotdl.DownloadSnapshotWithConfig(ctx, snapshotDlPath, snapCfg)
		if err != nil {
			klog.Fatalf("error downloading snapshot: %s", err)
		}

		dp := progress.NewDualProgress()
		accountsDb, manifest, err = snapshot.BuildAccountsDbPaths(ctx, dlPath, incrementalSnapshotFilename, accountsPath, dp)
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

	// Check for state file to resume from correct slot
	var snapshotBaseSlot = manifest.Bank.Slot
	startSlot := int64(manifest.Bank.Slot + 1)
	mithrilState, _ := state.CheckAndLoadValidState(accountsDbDir)
	if mithrilState != nil {
		// Validate state file matches current manifest
		if mithrilState.SnapshotSlot != manifest.Bank.Slot {
			mlog.Log.Infof("state file snapshot_slot (%d) doesn't match manifest (%d), ignoring state file",
				mithrilState.SnapshotSlot, manifest.Bank.Slot)
			mithrilState = nil
		} else if mithrilState.LastSlot > 0 {
			// Validate last_slot is reasonable (not in the future relative to what we could have replayed)
			if mithrilState.LastSlot < manifest.Bank.Slot {
				mlog.Log.Infof("state file last_slot (%d) is before snapshot slot (%d), ignoring",
					mithrilState.LastSlot, manifest.Bank.Slot)
				mithrilState = nil
			} else {
				startSlot = int64(mithrilState.LastSlot + 1)
			}
		}
	}

	// Create ResumeState if we have resume data from a previous graceful shutdown
	var resumeState *replay.ResumeState
	if mithrilState != nil && mithrilState.HasResumeData() {
		// Decode bankhash from base58
		parentBankhash, err := base58.Decode(mithrilState.LastBankhash)
		if err != nil {
			mlog.Log.Infof("warning: failed to decode last_bankhash from state file: %v", err)
		} else {
			// Decode LtHash from base64
			ltHashBytes, err := base64.StdEncoding.DecodeString(mithrilState.LastAcctsLtHash)
			if err != nil {
				mlog.Log.Infof("warning: failed to decode last_accts_lt_hash from state file: %v", err)
			} else {
				ltHash := &lthash.LtHash{}
				ltHash.InitWithHash(ltHashBytes)
				resumeState = &replay.ResumeState{
					ParentSlot:               mithrilState.LastSlot,
					ParentBankhash:           parentBankhash,
					AcctsLtHash:              ltHash,
					LamportsPerSignature:     mithrilState.LastLamportsPerSignature,
					PrevLamportsPerSignature: mithrilState.LastPrevLamportsPerSig,
					NumSignatures:            mithrilState.LastNumSignatures,
					// ReplayCtx fields
					Capitalization:          mithrilState.LastCapitalization,
					SlotsPerYear:            mithrilState.LastSlotsPerYear,
					InflationInitial:        mithrilState.LastInflationInitial,
					InflationTerminal:       mithrilState.LastInflationTerminal,
					InflationTaper:          mithrilState.LastInflationTaper,
					InflationFoundation:     mithrilState.LastInflationFoundation,
					InflationFoundationTerm: mithrilState.LastInflationFoundationTerm,
				}

				// Decode blockhash context
				if mithrilState.LastRecentBlockhashes != nil && len(mithrilState.LastRecentBlockhashes) > 0 {
					recentBlockhashes := decodeRecentBlockhashes(mithrilState.LastRecentBlockhashes)
					resumeState.RecentBlockhashes = &recentBlockhashes

					if mithrilState.LastEvictedBlockhash != "" {
						evictedBytes, err := base58.Decode(mithrilState.LastEvictedBlockhash)
						if err == nil && len(evictedBytes) == 32 {
							copy(resumeState.EvictedBlockhash[:], evictedBytes)
						}
					}

					if mithrilState.LastBlockhash != "" {
						lastBhBytes, err := base58.Decode(mithrilState.LastBlockhash)
						if err == nil && len(lastBhBytes) == 32 {
							copy(resumeState.LastBlockhash[:], lastBhBytes)
						}
					}
					mlog.Log.Infof("loaded resume data with %d blockhashes from state file", len(*resumeState.RecentBlockhashes))
				} else {
					mlog.Log.Infof("loaded resume data from state file (no blockhashes)")
				}

				// Decode SlotHashes context (vote program needs accurate slot→hash mappings)
				if mithrilState.LastSlotHashes != nil && len(mithrilState.LastSlotHashes) > 0 {
					slotHashes := decodeSlotHashes(mithrilState.LastSlotHashes)
					resumeState.SlotHashes = &slotHashes
				}

				// Load persisted epoch stakes - required for correct leader schedule
				if mithrilState.ComputedEpochStakes != nil && len(mithrilState.ComputedEpochStakes) > 0 {
					resumeState.ComputedEpochStakes = make(map[uint64][]byte, len(mithrilState.ComputedEpochStakes))
					for epoch, data := range mithrilState.ComputedEpochStakes {
						resumeState.ComputedEpochStakes[epoch] = []byte(data)
					}
				}
			}
		}
	}

	if mithrilState == nil {
		// Create a new state file for this session
		var snapshotEpoch uint64
		if sealevel.SysvarCache.EpochSchedule.Sysvar != nil {
			snapshotEpoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(manifest.Bank.Slot)
		}
		mithrilState = state.NewReadyState(manifest.Bank.Slot, snapshotEpoch, "", "", 0, 0)
		if err := mithrilState.Save(accountsDbDir); err != nil {
			mlog.Log.Errorf("failed to save state file: %v", err)
		}
	}

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
	accountsDb.InitCaches()

	// Write replay timings to accounts directory for verify-range
	replayTimingsPath := filepath.Join(accountsDbDir, "replay_timings.jsonl")
	metricsWriter, metricsWriterCleanup, err := createBufWriter(replayTimingsPath)
	if err != nil {
		klog.Fatalf("unable to create replay timings writer: %v", err)
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
		mlog.Log.Infof("Started RPC server on port %d", rpcPort)
	}

	replayStartTime := time.Now()
	blockFetchOpts := &replay.BlockFetchOpts{
		MaxRPS:          blockMaxRPS,
		MaxInflight:     blockMaxInflight,
		TipPollMs:       blockTipPollIntervalMs,
		TipSafetyMargin: uint64(blockTipSafetyMargin),

		// Mode thresholds
		NearTipThreshold:        blockNearTipThreshold,
		CatchupThreshold:        blockCatchupThreshold,
		CatchupTipGateThreshold: blockCatchupTipGateThreshold,

		// Near-tip tuning
		NearTipPollMs:    blockNearTipPollMs,
		NearTipLookahead: blockNearTipLookahead,
	}
	result := runReplayWithRecovery(ctx, accountsDb, accountsDbDir, manifest, resumeState, uint64(startSlot), uint64(endSlot), rpcEndpoints, blockstorePath, int(txParallelism), false, false, dbgOpts, metricsWriter, rpcServer, mithrilState, blockFetchOpts, replayStartTime)

	// Update state file with last persisted slot and shutdown context
	// Skip if already written during cancellation (eliminates timing window)
	if result.LastPersistedSlot > 0 && mithrilState != nil && !result.StateWrittenOnCancel {
		// Build shutdown context for graceful shutdown
		var shutdownCtx *state.ShutdownContext
		if result.LastAcctsLtHash != nil {
			// Calculate epoch for the last persisted slot
			var lastEpoch uint64
			if sealevel.SysvarCache.EpochSchedule.Sysvar != nil {
				lastEpoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(result.LastPersistedSlot)
			}
			// Determine shutdown reason
			shutdownReason := state.ShutdownReasonCompleted
			if result.WasCancelled {
				shutdownReason = state.ShutdownReasonNormal
			} else if result.Error != nil {
				if strings.Contains(result.Error.Error(), "stall") {
					shutdownReason = state.ShutdownReasonStall
				} else if strings.Contains(result.Error.Error(), "leader schedule") {
					shutdownReason = state.ShutdownReasonLeaderSchedule
				} else {
					// Include the actual error for easier debugging
					shutdownReason = fmt.Sprintf("%s: %v", state.ShutdownReasonError, result.Error)
				}
			}

			shutdownCtx = &state.ShutdownContext{
				RunID:          replay.CurrentRunID,
				WriterVersion:  getVersion(),
				WriterCommit:   getCommit(),
				WriterBranch:   getBranch(),
				ShutdownReason: shutdownReason,
				Epoch:          lastEpoch,

				// LtHash and fee state
				AcctsLtHash:          base64.StdEncoding.EncodeToString(result.LastAcctsLtHash.Hash()),
				LamportsPerSignature: result.LastLamportsPerSignature,
				PrevLamportsPerSig:   result.LastPrevLamportsPerSig,
				NumSignatures:        result.LastNumSignatures,

				// Blockhash context
				RecentBlockhashes: encodeRecentBlockhashes(result.LastRecentBlockhashes),
				EvictedBlockhash:  base58.Encode(result.LastEvictedBlockhash[:]),
				LastBlockhash:     base58.Encode(result.LastBlockhash[:]),

				// SlotHashes context
				SlotHashes: encodeSlotHashes(result.LastSlotHashes),

				// ReplayCtx fields
				Capitalization:          result.LastCapitalization,
				SlotsPerYear:            result.LastSlotsPerYear,
				InflationInitial:        result.LastInflation.Initial,
				InflationTerminal:       result.LastInflation.Terminal,
				InflationTaper:          result.LastInflation.Taper,
				InflationFoundation:     result.LastInflation.FoundationVal,
				InflationFoundationTerm: result.LastInflation.FoundationTerm,

				// EpochStakes - required for correct leader schedule on resume
				ComputedEpochStakes: result.ComputedEpochStakes,
			}
		}
		if err := mithrilState.UpdateOnShutdown(accountsDbDir, result.LastPersistedSlot, result.LastPersistedBankhash, shutdownCtx); err != nil {
			mlog.Log.Errorf("failed to update state file: %v", err)
		}
	}

	// Print shutdown summary if cancelled or error
	if (result.WasCancelled || result.Error != nil) && result.LastPersistedSlot > 0 {
		// Calculate epoch from slot using epoch schedule
		var epoch, snapshotEpoch uint64
		if sealevel.SysvarCache.EpochSchedule.Sysvar != nil {
			epoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(result.LastPersistedSlot)
			snapshotEpoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(snapshotBaseSlot)
		}
		progress.PrintShutdownSummary(progress.ShutdownInfo{
			LastSlot:         result.LastPersistedSlot,
			LastBankhash:     result.LastPersistedBankhash,
			SnapshotBaseSlot: snapshotBaseSlot,
			AccountsDBPath:   accountsDbDir,
			ReplayDuration:   time.Since(replayStartTime),
			WasCancelled:     result.WasCancelled,
			RunID:            replay.CurrentRunID,
			Epoch:            epoch,
			SnapshotEpoch:    snapshotEpoch,
		})
	}

	mlog.Log.Infof("Done replaying, closing DB")
	accountsDb.CloseDb()
}

func runLive(c *cobra.Command, args []string) {
	if pprofPort != -1 {
		startPprofHandlers(int(pprofPort))
	}
	ctx := c.Context()

	// Print the Mithril banner first, before any other output
	progress.PrintBanner()

	// Generate run ID early so it's available for logging and state tracking
	replay.CurrentRunID = replay.GenerateRunID()

	// Initialize file logging with defaults
	// Use config.IsSet() to allow explicit empty/zero values:
	//   dir = ""          → disable file logging (stdout only)
	//   max_size_mb = 0   → no limit
	//   max_age_days = 0  → never delete
	//   max_backups = 0   → unlimited
	logCfg := mlog.LogConfig{
		ToStdout: true, // Default true, override if explicitly set false
	}

	// Dir: default to /mnt/mithril-logs, but "" disables file logging
	if config.IsSet("storage.logs") {
		logCfg.Dir = config.GetString("storage.logs")
	} else {
		logCfg.Dir = "/mnt/mithril-logs"
	}

	// Level: default to "info"
	if config.IsSet("log.level") {
		logCfg.Level = config.GetString("log.level")
	} else {
		logCfg.Level = "info"
	}

	// ToStdout: default true (viper returns false for missing bool)
	if config.IsSet("log.to_stdout") {
		logCfg.ToStdout = config.GetBool("log.to_stdout")
	}

	// MaxSizeMB: default 100, but 0 means no limit
	if config.IsSet("log.max_size_mb") {
		logCfg.MaxSizeMB = config.GetInt("log.max_size_mb")
	} else {
		logCfg.MaxSizeMB = 100
	}

	// MaxAgeDays: default 7, but 0 means never delete
	if config.IsSet("log.max_age_days") {
		logCfg.MaxAgeDays = config.GetInt("log.max_age_days")
	} else {
		logCfg.MaxAgeDays = 7
	}

	// MaxBackups: default 10, but 0 means unlimited
	if config.IsSet("log.max_backups") {
		logCfg.MaxBackups = config.GetInt("log.max_backups")
	} else {
		logCfg.MaxBackups = 10
	}

	// Store log dir for startup info display
	logDir = logCfg.Dir

	if err := mlog.Initialize(logCfg, replay.CurrentRunID); err != nil {
		// Non-fatal, continue with stdout-only logging
		fmt.Fprintf(os.Stderr, "warning: failed to initialize file logging: %v\n", err)
	}
	defer mlog.Shutdown()

	// Kill any existing mithril processes to prevent zombie accumulation
	if killed := killExistingMithrilProcesses(); killed > 0 {
		fmt.Printf("  ⚠ Killed %d existing mithril process(es)\n\n", killed)
	}

	// Override bootstrap mode display when explicit snapshot paths are provided
	if snapshotArchivePath != "" {
		bootstrapMode = "explicit"
	}

	// Print consolidated startup info
	printStartupInfo("run")

	// Now start the metrics server (after banner so errors don't appear first)
	statsd.StartMetricsServer()

	// Determine if using Lightbringer based on block source
	// NOTE: Lightbringer mode is TEMPORARILY DISABLED. The background block downloader that
	// wrote Lightbringer blocks to disk was removed due to reliability issues (panics, race conditions).
	// Until Lightbringer streaming is re-implemented with the new parallel fetcher architecture,
	// all Lightbringer requests will fall back to RPC mode.
	// TODO: Re-implement Lightbringer as a streaming BlockSource (push model -> reorder buffer)
	useLightbringer := blockSource == "lightbringer"
	if useLightbringer {
		mlog.Log.Infof("WARNING: block.source=lightbringer is temporarily disabled - falling back to RPC")
		mlog.Log.Infof("  The background block downloader was removed. Lightbringer streaming will be")
		mlog.Log.Infof("  re-implemented in a future PR with the new parallel fetcher architecture.")
		useLightbringer = false

		// Validate RPC endpoints since we're falling back to RPC mode
		// (This check is normally done in loadConfig but only when blockSource == "rpc")
		if len(rpcEndpoints) == 0 {
			klog.Fatalf("block.source=lightbringer requires fallback to RPC, but no RPC endpoints are configured. " +
				"Set network.rpc in config, or use --rpc flag.")
		}
	}

	dbgOpts, err := replay.NewDebugOptions(debugTxs, debugAcctWrites)
	if err != nil {
		klog.Fatalf("failed to parse --transaction-signatures or --account-writes values: %v", err)
	}

	cpuprofWriter, cpuprofCleanup, err := createBufWriter(cpuprofPath)
	if err != nil {
		klog.Fatalf("unable to create cpuprof writer to filename=%s: %v", cpuprofPath, err)
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
	var mithrilState *state.MithrilState
	// Use configured snapshot directory (storage.snapshots / snapshot.download_path), not scratch
	snapshotDownloadPath := snapshotDlPath

	// Prune old history entries if needed (keeps last 100)
	if accountsPath != "" {
		if err := state.PruneHistory(accountsPath); err != nil {
			mlog.Log.Errorf("failed to prune state history: %v", err)
		}
	}

	// Check for valid state file first (this is the authoritative source of truth)
	mithrilState, _ = state.CheckAndLoadValidState(accountsPath)
	hasValidState := mithrilState != nil

	// Validate genesis hash if we have a valid state (prevents mainnet/testnet mixups)
	if hasValidState {
		genesisHash := fetchGenesisHash(ctx)
		if genesisHash != "" {
			if err := mithrilState.ValidateGenesisHash(genesisHash); err != nil {
				klog.Fatalf("FATAL: %v\nThis AccountsDB was built for a different cluster. Use --bootstrap snapshot to rebuild.", err)
			}
			// If state has no genesis hash (older version), set it now
			if mithrilState.GenesisHash == "" {
				mlog.Log.Infof("Updating state file with cluster=%s genesis=%s", cluster, genesisHash[:12]+"...")
				mithrilState.SetClusterInfo(cluster, genesisHash)
				if err := mithrilState.Save(accountsPath); err != nil {
					mlog.Log.Infof("WARNING: failed to update state file with cluster info: %v", err)
				}
			}
		}
	}

	// Fall back to legacy detection if no state file
	hasAccountsDB, accountsDBSlot := detectExistingAccountsDB(accountsPath)
	if hasValidState {
		hasAccountsDB = true
		accountsDBSlot = mithrilState.GetCurrentSlot() // Use current slot (LastSlot if replayed, else SnapshotSlot)
	}

	// Handle explicit --snapshot flag (bypasses all auto-discovery, does NOT delete snapshot files)
	if snapshotArchivePath != "" {
		mlog.Log.Infof("Using snapshot file: %s", snapshotArchivePath)

		// Parse full snapshot slot from filename for validation
		fullSnapshotSlot := parseSlotFromSnapshotName(filepath.Base(snapshotArchivePath))
		if fullSnapshotSlot == 0 {
			klog.Fatalf("could not parse slot from snapshot filename: %s", snapshotArchivePath)
		}

		if incrementalSnapshotFilename != "" {
			mlog.Log.Infof("Using incremental snapshot: %s", incrementalSnapshotFilename)

			// Validate incremental base matches full snapshot slot
			incrBase, incrEnd := parseSlotsFromIncrementalName(filepath.Base(incrementalSnapshotFilename))
			if incrBase == 0 {
				klog.Fatalf("could not parse base slot from incremental snapshot filename: %s", incrementalSnapshotFilename)
			}
			if incrBase != fullSnapshotSlot {
				klog.Fatalf("Incremental base slot %d does not match full snapshot slot %d", incrBase, fullSnapshotSlot)
			}
			mlog.Log.Infof("Incremental snapshot: base=%d end=%d (validated)", incrBase, incrEnd)
		}

		// Build directly from the specified files (BuildAccountsDbPaths handles AccountsDB cleanup internally)
		// NOTE: We do NOT clean snapshot files in explicit mode - user wants to keep their explicit snapshots
		dp := progress.NewDualProgress()
		accountsDb, manifest, err = snapshot.BuildAccountsDbPaths(ctx, snapshotArchivePath, incrementalSnapshotFilename, accountsPath, dp)
		if err != nil {
			klog.Fatalf("failed to build AccountsDB from snapshot: %v", err)
		}

		// Write state file
		var snapshotEpoch uint64
		if sealevel.SysvarCache.EpochSchedule.Sysvar != nil {
			snapshotEpoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(manifest.Bank.Slot)
		}
		mithrilState = state.NewReadyState(manifest.Bank.Slot, snapshotEpoch, "", "", 0, 0)
		if err := mithrilState.Save(accountsPath); err != nil {
			mlog.Log.Errorf("failed to save state file: %v", err)
		}
		state.RecordBootstrap(accountsPath, manifest.Bank.Slot, "", replay.CurrentRunID, getVersion(), getCommit(), getBranch())
		goto postBootstrap
	}

	switch bootstrapMode {
	case "accountsdb":
		// Mode: Require existing AccountsDB, never download
		if !hasValidState && !hasAccountsDB {
			klog.Fatalf("mode=accountsdb requires existing AccountsDB at %s", accountsPath)
		}
		if !hasValidState {
			mlog.Log.Infof("WARNING: no state file found, AccountsDB may be from incomplete build")
		}
		mlog.Log.Infof("Resuming from existing AccountsDB at slot %d", accountsDBSlot)
		accountsDb, err = accountsdb.OpenDb(accountsPath)
		if err != nil {
			klog.Fatalf("failed to open AccountsDB at %s: %v", accountsPath, err)
		}
		manifest, err = snapshot.LoadManifestFromFile(filepath.Join(accountsPath, "manifest"))
		if err != nil {
			klog.Fatalf("failed to load manifest: %v", err)
		}
		// Run integrity check if we have a state file (warn only, don't fail - user chose force mode)
		if hasValidState {
			if err := mithrilState.ValidateAgainstBankhashDB(accountsDb); err != nil {
				mlog.Log.Errorf("WARNING: integrity check failed: %v", err)
				mlog.Log.Errorf("WARNING: AccountsDB may be corrupted. Consider using --bootstrap snapshot to rebuild.")
			}
		}

	case "new-snapshot":
		// Mode: Always download fresh snapshot, clean everything
		if snapshotDownloadPath == "" {
			klog.Fatalf("mode=new-snapshot requires a snapshot directory (set storage.snapshots or snapshot.download_path in config)")
		}
		mlog.Log.Infof("mode=new-snapshot: Downloading fresh snapshot")
		if accountsPath != "" {
			// Record rebuild in history before cleanup (history file is preserved)
			if mithrilState != nil {
				state.RecordRebuild(accountsPath, mithrilState.LastSlot, mithrilState.LastBankhash, getVersion(), getCommit(), getBranch(), "new-snapshot mode")
			} else {
				state.RecordRebuild(accountsPath, 0, "", getVersion(), getCommit(), getBranch(), "new-snapshot mode (no prior state)")
			}
			mlog.Log.Infof("Cleaning up previous AccountsDB artifacts in %s", accountsPath)
			snapshot.CleanAccountsDbDir(accountsPath)
		}
		// Clean existing snapshots (respecting retention setting)
		if snapshotDownloadPath != "" {
			maxSnapshots := config.GetInt("snapshot.max_full_snapshots")
			if maxSnapshots < 0 {
				maxSnapshots = 0
			}
			mlog.Log.Infof("Cleaning up existing snapshot files in %s (keeping %d)", snapshotDownloadPath, maxSnapshots)
			snapshot.CleanSnapshotDownloadDir(snapshotDownloadPath, maxSnapshots)
		}
		accountsDb, manifest, err = downloadAndBuildFromSnapshot(ctx, rpcEndpoints, snapshotDownloadPath, accountsPath, blockstorePath)
		if err != nil {
			klog.Fatalf("failed to build AccountsDB from snapshot: %v", err)
		}
		// Write state file to mark build as complete
		var snapshotEpoch uint64
		if sealevel.SysvarCache.EpochSchedule.Sysvar != nil {
			snapshotEpoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(manifest.Bank.Slot)
		}
		mithrilState = state.NewReadyState(manifest.Bank.Slot, snapshotEpoch, "", "", 0, 0)
		if err := mithrilState.Save(accountsPath); err != nil {
			mlog.Log.Errorf("failed to save state file: %v", err)
		}
		// Record bootstrap in history
		state.RecordBootstrap(accountsPath, manifest.Bank.Slot, "", replay.CurrentRunID, getVersion(), getCommit(), getBranch())

	case "snapshot":
		// Mode: Rebuild AccountsDB from snapshot, reuse existing snapshot file if fresh enough
		if snapshotDownloadPath == "" {
			klog.Fatalf("mode=snapshot requires a snapshot directory (set storage.snapshots or snapshot.download_path in config)")
		}
		mlog.Log.Infof("mode=snapshot: Will rebuild AccountsDB from snapshot")
		if accountsPath != "" {
			// Record rebuild in history before cleanup (history file is preserved)
			if mithrilState != nil {
				state.RecordRebuild(accountsPath, mithrilState.LastSlot, mithrilState.LastBankhash, getVersion(), getCommit(), getBranch(), "snapshot mode")
			} else {
				state.RecordRebuild(accountsPath, 0, "", getVersion(), getCommit(), getBranch(), "snapshot mode (no prior state)")
			}
			mlog.Log.Infof("Cleaning up previous AccountsDB artifacts in %s", accountsPath)
			snapshot.CleanAccountsDbDir(accountsPath)
		}

		// Check for existing fresh snapshot
		fullThreshold := config.GetInt("snapshot.full_threshold")
		if fullThreshold == 0 {
			fullThreshold = 100000 // default
		}
		existingSnap := detectFreshSnapshot(snapshotDownloadPath, fullThreshold, rpcEndpoints, ctx)

		if existingSnap != nil {
			// Reuse existing snapshot
			mlog.Log.Infof("Reusing existing snapshot file at slot %d", existingSnap.slot)
			accountsDb, manifest, err = buildFromExistingSnapshot(ctx, existingSnap, snapshotDownloadPath, accountsPath, blockstorePath, rpcEndpoints)
		} else {
			// Download fresh
			mlog.Log.Infof("no fresh snapshot file found, downloading new one")
			// Clean up old snapshot files based on retention settings
			if snapshotDownloadPath != "" {
				maxSnapshots := config.GetInt("snapshot.max_full_snapshots")
				if maxSnapshots == 0 {
					maxSnapshots = 1 // default: keep 1 snapshot
				}
				snapshot.CleanSnapshotDownloadDir(snapshotDownloadPath, maxSnapshots)
			}
			accountsDb, manifest, err = downloadAndBuildFromSnapshot(ctx, rpcEndpoints, snapshotDownloadPath, accountsPath, blockstorePath)
		}
		if err != nil {
			klog.Fatalf("failed to build AccountsDB from snapshot: %v", err)
		}
		// Write state file to mark build as complete
		var snapshotEpoch uint64
		if sealevel.SysvarCache.EpochSchedule.Sysvar != nil {
			snapshotEpoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(manifest.Bank.Slot)
		}
		mithrilState = state.NewReadyState(manifest.Bank.Slot, snapshotEpoch, "", "", 0, 0)
		if err := mithrilState.Save(accountsPath); err != nil {
			mlog.Log.Errorf("failed to save state file: %v", err)
		}
		// Record bootstrap in history
		state.RecordBootstrap(accountsPath, manifest.Bank.Slot, "", replay.CurrentRunID, getVersion(), getCommit(), getBranch())

	case "auto":
		fallthrough
	default:
		// Mode: auto - prefer valid AccountsDB (with state file), then download fresh
		fullThreshold := config.GetInt("snapshot.full_threshold")
		if fullThreshold == 0 {
			fullThreshold = 100000 // default
		}

		if hasValidState {
			// Check if AccountsDB is behind chain tip (more than 2000 slots triggers prompt)
			// Use queryCurrentSlot instead of queryLatestSnapshotSlot to avoid expensive node discovery
			const stalePromptThreshold = 2000
			currentSlot, err := queryCurrentSlot(ctx, rpcEndpoints)
			if err != nil {
				mlog.Log.Infof("could not query current slot: %v (continuing with existing AccountsDB)", err)
			} else if mithrilState.IsStale(currentSlot, stalePromptThreshold) {
				slotsBehind := currentSlot - mithrilState.GetCurrentSlot()
				mlog.Log.Infof("AccountsDB is %d slots behind chain tip", slotsBehind)

				// Interactive prompt
				choice := progress.PromptStaleAccountsDB(progress.StaleInfo{
					AccountsDBSlot:     mithrilState.GetCurrentSlot(),
					LatestSnapshotSlot: currentSlot, // Using current chain tip slot
					SlotsBehind:        slotsBehind,
				})

				if choice == 2 {
					// User chose to start fresh from snapshot
					if snapshotDownloadPath == "" {
						klog.Fatalf("cannot rebuild from snapshot: no snapshot directory configured (set storage.snapshots or snapshot.download_path in config)")
					}
					mlog.Log.Infof("User chose to rebuild from latest snapshot")
					if accountsPath != "" {
						// Record rebuild in history before cleanup (history file is preserved)
						// mithrilState is guaranteed non-nil here (we prompted because it was stale)
						state.RecordRebuild(accountsPath, mithrilState.LastSlot, mithrilState.LastBankhash, getVersion(), getCommit(), getBranch(), "user chose rebuild (stale AccountsDB)")
						mlog.Log.Infof("Cleaning up previous AccountsDB artifacts in %s", accountsPath)
						snapshot.CleanAccountsDbDir(accountsPath)
					}
					// Check for existing fresh snapshot
					existingSnap := detectFreshSnapshot(snapshotDownloadPath, fullThreshold, rpcEndpoints, ctx)
					if existingSnap != nil {
						mlog.Log.Infof("Reusing existing snapshot file at slot %d", existingSnap.slot)
						accountsDb, manifest, err = buildFromExistingSnapshot(ctx, existingSnap, snapshotDownloadPath, accountsPath, blockstorePath, rpcEndpoints)
					} else {
						// Clean up old snapshot files
						if snapshotDownloadPath != "" {
							maxSnapshots := config.GetInt("snapshot.max_full_snapshots")
							if maxSnapshots == 0 {
								maxSnapshots = 1 // default: keep 1 snapshot
							}
							snapshot.CleanSnapshotDownloadDir(snapshotDownloadPath, maxSnapshots)
						}
						accountsDb, manifest, err = downloadAndBuildFromSnapshot(ctx, rpcEndpoints, snapshotDownloadPath, accountsPath, blockstorePath)
					}
					if err != nil {
						klog.Fatalf("failed to build AccountsDB from snapshot: %v", err)
					}
					var snapshotEpoch uint64
					if sealevel.SysvarCache.EpochSchedule.Sysvar != nil {
						snapshotEpoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(manifest.Bank.Slot)
					}
					mithrilState = state.NewReadyState(manifest.Bank.Slot, snapshotEpoch, "", "", 0, 0)
					if err := mithrilState.Save(accountsPath); err != nil {
						mlog.Log.Errorf("failed to save state file: %v", err)
					}
					// Record bootstrap in history
					state.RecordBootstrap(accountsPath, manifest.Bank.Slot, "", replay.CurrentRunID, getVersion(), getCommit(), getBranch())
					break // Exit the switch, continue with fresh AccountsDB
				}
				// choice == 1: continue with existing AccountsDB
			}

			mlog.Log.Infof("mode=auto: Resuming from existing AccountsDB at slot %d", accountsDBSlot)
			// Record resume in history
			state.RecordResume(accountsPath, mithrilState.LastSlot, mithrilState.LastBankhash, replay.CurrentRunID, getVersion(), getCommit(), getBranch())
			accountsDb, err = accountsdb.OpenDb(accountsPath)
			if err != nil {
				klog.Fatalf("failed to open AccountsDB at %s: %v", accountsPath, err)
			}
			manifest, err = snapshot.LoadManifestFromFile(filepath.Join(accountsPath, "manifest"))
			if err != nil {
				klog.Fatalf("failed to load manifest: %v", err)
			}

			// Validate state file matches AccountsDB (detect Ctrl+Z / kill -9 corruption)
			if err := mithrilState.ValidateAgainstBankhashDB(accountsDb); err != nil {
				mlog.Log.Errorf("INTEGRITY CHECK FAILED: %v", err)
				mlog.Log.Infof("AccountsDB appears to have been modified beyond what state file records")
				mlog.Log.Infof("This usually happens when the process was killed with Ctrl+Z or kill -9")

				// Mark state as corrupted so next startup knows to rebuild
				if markErr := mithrilState.MarkCorrupted(accountsPath, err.Error()); markErr != nil {
					mlog.Log.Errorf("failed to mark state as corrupted: %v", markErr)
				} else {
					mlog.Log.Infof("state file updated to indicate corruption")
					// Record corruption in history
					state.RecordCorrupted(accountsPath, mithrilState.LastSlot, mithrilState.LastBankhash, replay.CurrentRunID, getVersion(), getCommit(), getBranch(), err.Error())
				}

				// Close AccountsDB before exiting
				accountsDb.CloseDb()

				mlog.Log.Infof("restart mithril to automatically rebuild from snapshot")
				klog.Fatalf("AccountsDB corrupted - restart to rebuild")
			}
		} else {
			// No valid state - need to clean and rebuild from snapshot
			if snapshotDownloadPath == "" {
				klog.Fatalf("mode=auto requires a snapshot directory to rebuild (set storage.snapshots or snapshot.download_path in config)")
			}
			if hasAccountsDB {
				mlog.Log.Infof("mode=auto: AccountsDB exists but state invalid, rebuilding from snapshot")
			} else {
				mlog.Log.Infof("mode=auto: No existing AccountsDB, will download snapshot")
			}
			if accountsPath != "" {
				// Record rebuild in history before cleanup (history file is preserved)
				// Try to load any existing state (even invalid) to capture slot info
				var reason string
				if hasAccountsDB {
					reason = "auto mode (AccountsDB exists but state invalid)"
				} else {
					reason = "auto mode (no existing AccountsDB)"
				}
				if existingState, _ := state.LoadState(accountsPath); existingState != nil {
					state.RecordRebuild(accountsPath, existingState.LastSlot, existingState.LastBankhash, getVersion(), getCommit(), getBranch(), reason)
				} else {
					state.RecordRebuild(accountsPath, 0, "", getVersion(), getCommit(), getBranch(), reason)
				}
				mlog.Log.Infof("Cleaning up previous AccountsDB artifacts in %s", accountsPath)
				snapshot.CleanAccountsDbDir(accountsPath)
			}

			// Check for existing fresh snapshot
			existingSnap := detectFreshSnapshot(snapshotDownloadPath, fullThreshold, rpcEndpoints, ctx)
			if existingSnap != nil {
				mlog.Log.Infof("Reusing existing snapshot file at slot %d", existingSnap.slot)
				accountsDb, manifest, err = buildFromExistingSnapshot(ctx, existingSnap, snapshotDownloadPath, accountsPath, blockstorePath, rpcEndpoints)
			} else {
				// Clean up old snapshot files based on retention settings
				maxSnapshots := config.GetInt("snapshot.max_full_snapshots")
				if maxSnapshots == 0 {
					maxSnapshots = 1 // default: keep 1 snapshot
				}
				snapshot.CleanSnapshotDownloadDir(snapshotDownloadPath, maxSnapshots)
				accountsDb, manifest, err = downloadAndBuildFromSnapshot(ctx, rpcEndpoints, snapshotDownloadPath, accountsPath, blockstorePath)
			}
			if err != nil {
				klog.Fatalf("failed to build AccountsDB from snapshot: %v", err)
			}
			// Write state file to mark build as complete
			var snapshotEpoch uint64
			if sealevel.SysvarCache.EpochSchedule.Sysvar != nil {
				snapshotEpoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(manifest.Bank.Slot)
			}
			mithrilState = state.NewReadyStateWithOpts(state.NewReadyStateOpts{
				SnapshotSlot:  manifest.Bank.Slot,
				SnapshotEpoch: snapshotEpoch,
				BuildMode:     bootstrapMode,
				Cluster:       cluster,
				GenesisHash:   fetchGenesisHash(ctx),
				WriterVersion: getVersion(),
				WriterCommit:  getCommit(),
			})
			if err := mithrilState.Save(accountsPath); err != nil {
				mlog.Log.Errorf("failed to save state file: %v", err)
			}
			// Record bootstrap in history
			state.RecordBootstrap(accountsPath, manifest.Bank.Slot, "", replay.CurrentRunID, getVersion(), getCommit(), getBranch())
		}
	}

postBootstrap:
	// Determine start slot from state file or manifest
	var snapshotBaseSlot = manifest.Bank.Slot
	startSlot := int64(manifest.Bank.Slot + 1)
	if mithrilState != nil {
		// Validate state file matches current manifest
		if mithrilState.SnapshotSlot != manifest.Bank.Slot {
			mlog.Log.Infof("state file snapshot_slot (%d) doesn't match manifest (%d), ignoring state file",
				mithrilState.SnapshotSlot, manifest.Bank.Slot)
			mithrilState = nil
		} else if mithrilState.LastSlot > 0 {
			// Validate last_slot is reasonable
			if mithrilState.LastSlot < manifest.Bank.Slot {
				mlog.Log.Infof("state file last_slot (%d) is before snapshot slot (%d), ignoring",
					mithrilState.LastSlot, manifest.Bank.Slot)
				mithrilState = nil
			} else {
				startSlot = int64(mithrilState.LastSlot + 1)
			}
		}
	}

	// Create ResumeState if we have resume context from state file
	var resumeState *replay.ResumeState
	if mithrilState != nil && mithrilState.HasResumeData() {
		// Decode parent bankhash
		parentBankhash, err := base58.Decode(mithrilState.LastBankhash)
		if err != nil {
			mlog.Log.Errorf("failed to decode last_bankhash from state file: %v", err)
			mlog.Log.Infof("will start fresh from snapshot")
			mithrilState = nil
		} else {
			// Decode AcctsLtHash
			ltHashBytes, err := base64.StdEncoding.DecodeString(mithrilState.LastAcctsLtHash)
			if err != nil {
				mlog.Log.Errorf("failed to decode accts_lt_hash from state file: %v", err)
				mlog.Log.Infof("will start fresh from snapshot")
				mithrilState = nil
			} else {
				ltHash := &lthash.LtHash{}
				ltHash.InitWithHash(ltHashBytes)

				resumeState = &replay.ResumeState{
					ParentSlot:               mithrilState.LastSlot,
					ParentBankhash:           parentBankhash,
					AcctsLtHash:              ltHash,
					LamportsPerSignature:     mithrilState.LastLamportsPerSignature,
					PrevLamportsPerSignature: mithrilState.LastPrevLamportsPerSig,
					NumSignatures:            mithrilState.LastNumSignatures,
					// ReplayCtx fields
					Capitalization:          mithrilState.LastCapitalization,
					SlotsPerYear:            mithrilState.LastSlotsPerYear,
					InflationInitial:        mithrilState.LastInflationInitial,
					InflationTerminal:       mithrilState.LastInflationTerminal,
					InflationTaper:          mithrilState.LastInflationTaper,
					InflationFoundation:     mithrilState.LastInflationFoundation,
					InflationFoundationTerm: mithrilState.LastInflationFoundationTerm,
				}

				// Decode blockhash context
				if mithrilState.LastRecentBlockhashes != nil && len(mithrilState.LastRecentBlockhashes) > 0 {
					recentBlockhashes := decodeRecentBlockhashes(mithrilState.LastRecentBlockhashes)
					resumeState.RecentBlockhashes = &recentBlockhashes

					if mithrilState.LastEvictedBlockhash != "" {
						evictedBytes, err := base58.Decode(mithrilState.LastEvictedBlockhash)
						if err == nil && len(evictedBytes) == 32 {
							copy(resumeState.EvictedBlockhash[:], evictedBytes)
						}
					}

					if mithrilState.LastBlockhash != "" {
						lastBhBytes, err := base58.Decode(mithrilState.LastBlockhash)
						if err == nil && len(lastBhBytes) == 32 {
							copy(resumeState.LastBlockhash[:], lastBhBytes)
						}
					}
				}

				// Decode SlotHashes context (vote program needs accurate slot→hash mappings)
				if mithrilState.LastSlotHashes != nil && len(mithrilState.LastSlotHashes) > 0 {
					slotHashes := decodeSlotHashes(mithrilState.LastSlotHashes)
					resumeState.SlotHashes = &slotHashes
				}

				// Load persisted epoch stakes - required for correct leader schedule
				if mithrilState.ComputedEpochStakes != nil && len(mithrilState.ComputedEpochStakes) > 0 {
					resumeState.ComputedEpochStakes = make(map[uint64][]byte, len(mithrilState.ComputedEpochStakes))
					for epoch, data := range mithrilState.ComputedEpochStakes {
						resumeState.ComputedEpochStakes[epoch] = []byte(data)
					}
				}
			}
		}
	}

	if mithrilState == nil {
		// Initialize state for this session
		var snapshotEpoch uint64
		if sealevel.SysvarCache.EpochSchedule.Sysvar != nil {
			snapshotEpoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(manifest.Bank.Slot)
		}
		mithrilState = state.NewReadyState(manifest.Bank.Slot, snapshotEpoch, "", "", 0, 0)
		if err := mithrilState.Save(accountsPath); err != nil {
			mlog.Log.Errorf("failed to save state file: %v", err)
		}
		// Record bootstrap in history
		state.RecordBootstrap(accountsPath, manifest.Bank.Slot, "", replay.CurrentRunID, getVersion(), getCommit(), getBranch())
	}

	liveEndSlot := uint64(math.MaxUint64)
	accountsDb.InitCaches()

	// Write replay timings to run-specific log directory
	replayTimingsPath := filepath.Join(mlog.GetLogDir(), "replay_timings.jsonl")
	metricsWriter, metricsWriterCleanup, err := createBufWriter(replayTimingsPath)
	if err != nil {
		klog.Fatalf("unable to create replay timings writer: %v", err)
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
		mlog.Log.Infof("Started RPC server on port %d", rpcPort)
	}

	replayStartTime := time.Now()
	blockFetchOpts := &replay.BlockFetchOpts{
		MaxRPS:          blockMaxRPS,
		MaxInflight:     blockMaxInflight,
		TipPollMs:       blockTipPollIntervalMs,
		TipSafetyMargin: uint64(blockTipSafetyMargin),

		// Mode thresholds
		NearTipThreshold:        blockNearTipThreshold,
		CatchupThreshold:        blockCatchupThreshold,
		CatchupTipGateThreshold: blockCatchupTipGateThreshold,

		// Near-tip tuning
		NearTipPollMs:    blockNearTipPollMs,
		NearTipLookahead: blockNearTipLookahead,
	}
	result := runReplayWithRecovery(ctx, accountsDb, accountsPath, manifest, resumeState, uint64(startSlot), liveEndSlot, rpcEndpoints, blockstorePath, int(txParallelism), true, useLightbringer, dbgOpts, metricsWriter, rpcServer, mithrilState, blockFetchOpts, replayStartTime)

	// Update state file with last persisted slot and shutdown context
	// Skip if already written during cancellation (eliminates timing window)
	if result.LastPersistedSlot > 0 && mithrilState != nil && !result.StateWrittenOnCancel {
		var shutdownCtx *state.ShutdownContext
		if result.LastAcctsLtHash != nil {
			// Calculate epoch for the last persisted slot
			var lastEpoch uint64
			if sealevel.SysvarCache.EpochSchedule.Sysvar != nil {
				lastEpoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(result.LastPersistedSlot)
			}
			// Determine shutdown reason
			shutdownReason := state.ShutdownReasonCompleted
			if result.WasCancelled {
				shutdownReason = state.ShutdownReasonNormal
			} else if result.Error != nil {
				if strings.Contains(result.Error.Error(), "stall") {
					shutdownReason = state.ShutdownReasonStall
				} else if strings.Contains(result.Error.Error(), "leader schedule") {
					shutdownReason = state.ShutdownReasonLeaderSchedule
				} else {
					// Include the actual error for easier debugging
					shutdownReason = fmt.Sprintf("%s: %v", state.ShutdownReasonError, result.Error)
				}
			}

			shutdownCtx = &state.ShutdownContext{
				RunID:          replay.CurrentRunID,
				WriterVersion:  getVersion(),
				WriterCommit:   getCommit(),
				WriterBranch:   getBranch(),
				ShutdownReason: shutdownReason,
				Epoch:          lastEpoch,

				// LtHash and fee state
				AcctsLtHash:          base64.StdEncoding.EncodeToString(result.LastAcctsLtHash.Hash()),
				LamportsPerSignature: result.LastLamportsPerSignature,
				PrevLamportsPerSig:   result.LastPrevLamportsPerSig,
				NumSignatures:        result.LastNumSignatures,

				// Blockhash context
				RecentBlockhashes: encodeRecentBlockhashes(result.LastRecentBlockhashes),
				EvictedBlockhash:  base58.Encode(result.LastEvictedBlockhash[:]),
				LastBlockhash:     base58.Encode(result.LastBlockhash[:]),

				// SlotHashes context
				SlotHashes: encodeSlotHashes(result.LastSlotHashes),

				// ReplayCtx fields
				Capitalization:          result.LastCapitalization,
				SlotsPerYear:            result.LastSlotsPerYear,
				InflationInitial:        result.LastInflation.Initial,
				InflationTerminal:       result.LastInflation.Terminal,
				InflationTaper:          result.LastInflation.Taper,
				InflationFoundation:     result.LastInflation.FoundationVal,
				InflationFoundationTerm: result.LastInflation.FoundationTerm,

				// EpochStakes - required for correct leader schedule on resume
				ComputedEpochStakes: result.ComputedEpochStakes,
			}
			// Record shutdown in history (must be inside this block where shutdownReason is defined)
			if err := mithrilState.UpdateOnShutdown(accountsPath, result.LastPersistedSlot, result.LastPersistedBankhash, shutdownCtx); err != nil {
				mlog.Log.Errorf("failed to update state file: %v", err)
			}
			state.RecordShutdown(accountsPath, result.LastPersistedSlot, base58.Encode(result.LastPersistedBankhash), replay.CurrentRunID, getVersion(), getCommit(), getBranch(), shutdownReason)
		} else {
			// No shutdown context - just update slot
			if err := mithrilState.UpdateOnShutdown(accountsPath, result.LastPersistedSlot, result.LastPersistedBankhash, shutdownCtx); err != nil {
				mlog.Log.Errorf("failed to update state file: %v", err)
			}
		}
	}

	// Print shutdown summary if cancelled or error
	if (result.WasCancelled || result.Error != nil) && result.LastPersistedSlot > 0 {
		// Calculate epoch from slot using epoch schedule
		var epoch, snapshotEpoch uint64
		if sealevel.SysvarCache.EpochSchedule.Sysvar != nil {
			epoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(result.LastPersistedSlot)
			snapshotEpoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(snapshotBaseSlot)
		}
		progress.PrintShutdownSummary(progress.ShutdownInfo{
			LastSlot:         result.LastPersistedSlot,
			LastBankhash:     result.LastPersistedBankhash,
			SnapshotBaseSlot: snapshotBaseSlot,
			AccountsDBPath:   accountsPath,
			ReplayDuration:   time.Since(replayStartTime),
			WasCancelled:     result.WasCancelled,
			RunID:            replay.CurrentRunID,
			Epoch:            epoch,
			SnapshotEpoch:    snapshotEpoch,
		})
	}

	mlog.Log.Infof("Done replaying, closing DB")
	accountsDb.CloseDb()
}

// getVersion returns the build version from the shared version package.
func getVersion() string {
	return version.Version
}

// getCommit returns the git commit hash, preferring ldflags but falling back to
// runtime/debug.BuildInfo for dev builds.
func getCommit() string {
	// If set via ldflags (release builds), use that
	if version.GitCommit != "" && version.GitCommit != "unknown" {
		return version.GitCommit
	}
	// Fallback to runtime/debug for dev builds (go build without ldflags)
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				// Return short hash (8 chars) for consistency
				if len(setting.Value) > 8 {
					return setting.Value[:8]
				}
				return setting.Value
			}
		}
	}
	return "unknown"
}

// getBranch returns the git branch name, preferring ldflags but falling back to
// runtime/debug.BuildInfo for dev builds. Returns empty string if unavailable.
func getBranch() string {
	// If set via ldflags (release builds), use that
	if version.GitBranch != "" && version.GitBranch != "unknown" {
		return version.GitBranch
	}
	// runtime/debug doesn't expose git branch, so return empty for dev builds
	return ""
}

// fetchGenesisHash fetches the genesis hash from the first RPC endpoint.
// Returns empty string on error (caller should log and continue).
func fetchGenesisHash(ctx context.Context) string {
	if len(rpcEndpoints) == 0 {
		return ""
	}
	client := solrpc.New(rpcEndpoints[0])
	hash, err := client.GetGenesisHash(ctx)
	if err != nil {
		mlog.Log.Infof("WARNING: failed to fetch genesis hash from RPC: %v", err)
		return ""
	}
	return hash.String()
}

// formatDurationShort formats a duration in a compact human-readable format (e.g., "2h 30m", "45m", "3d 2h")
func formatDurationShort(d time.Duration) string {
	if d < time.Minute {
		return "< 1m"
	}

	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60

	if days > 0 {
		if hours > 0 {
			return fmt.Sprintf("%dd %dh", days, hours)
		}
		return fmt.Sprintf("%dd", days)
	}
	if hours > 0 {
		if minutes > 0 {
			return fmt.Sprintf("%dh %dm", hours, minutes)
		}
		return fmt.Sprintf("%dh", hours)
	}
	return fmt.Sprintf("%dm", minutes)
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
		// Add branch info if available (skip "unknown" and "HEAD" for detached state)
		branch := version.GitBranch
		if branch != "" && branch != "unknown" && branch != "HEAD" {
			if modified == "true" {
				commitStr += fmt.Sprintf(" (%s, modified)", branch)
			} else {
				commitStr += fmt.Sprintf(" (%s)", branch)
			}
		} else if modified == "true" {
			commitStr += " (modified)"
		}
		fmt.Printf("  Commit:       %s%s%s\n", dim, commitStr, reset)
	}

	// Go version
	fmt.Printf("  Go:           %s%s%s\n", dim, runtime.Version(), reset)

	// Run ID (generated fresh each run)
	runID := replay.CurrentRunID
	if runID == "" {
		// Generate one early if not yet set, and assign to global for consistency
		runID = fmt.Sprintf("%08x", time.Now().UnixNano()&0xFFFFFFFF)
		replay.CurrentRunID = runID
	}
	fmt.Printf("  Run ID:       %s%s%s\n", dim, runID, reset)

	// Bootstrap mode with description
	var bootstrapDesc string
	switch bootstrapMode {
	case "auto":
		bootstrapDesc = "use existing AccountsDB if valid, else download snapshot"
	case "snapshot":
		bootstrapDesc = "rebuild from local snapshot"
	case "new-snapshot":
		bootstrapDesc = "download fresh snapshot from network"
	case "accountsdb":
		bootstrapDesc = "require existing AccountsDB"
	case "explicit":
		bootstrapDesc = "build from explicit --snapshot path"
	default:
		bootstrapDesc = ""
	}
	if bootstrapDesc != "" {
		fmt.Printf("  Bootstrap:    %s%s%s %s(%s)%s\n", green, bootstrapMode, reset, dim, bootstrapDesc, reset)
	} else {
		fmt.Printf("  Bootstrap:    %s%s%s\n", green, bootstrapMode, reset)
	}

	// Load state file for detailed info (only show for modes that use existing AccountsDB)
	// In snapshot/new-snapshot modes, we're rebuilding so state info is not relevant
	willUseExistingAccountsDB := bootstrapMode == "auto" || bootstrapMode == "accountsdb"
	if willUseExistingAccountsDB {
		mithrilState, _ := state.LoadState(accountsPath)

		// Show state info if available
		if mithrilState != nil && mithrilState.IsReady() {
			fmt.Println()
			fmt.Printf("%s━━━ State Info ━━━%s\n", gold, reset)

			// Full snapshot info
			snapshotInfo := fmt.Sprintf("slot %d", mithrilState.SnapshotSlot)
			if mithrilState.SnapshotEpoch > 0 {
				snapshotInfo = fmt.Sprintf("slot %d (epoch %d)", mithrilState.SnapshotSlot, mithrilState.SnapshotEpoch)
			}
			fmt.Printf("  Full snapshot:  %s%s%s\n", dim, snapshotInfo, reset)

			// Incremental snapshot info (if available)
			if mithrilState.IncrSnapshot != nil && mithrilState.IncrSnapshot.Slot > 0 {
				incrInfo := fmt.Sprintf("slot %d (based on %d)", mithrilState.IncrSnapshot.Slot, mithrilState.IncrSnapshot.BaseSlot)
				fmt.Printf("  Incr snapshot:  %s%s%s\n", dim, incrInfo, reset)
			}

			// Current AccountsDB state / resume info
			if mithrilState.LastSlot > 0 {
				resumeInfo := fmt.Sprintf("AccountsDB at slot %d", mithrilState.LastSlot)
				if mithrilState.LastEpoch > 0 {
					resumeInfo = fmt.Sprintf("AccountsDB at slot %d (epoch %d)", mithrilState.LastSlot, mithrilState.LastEpoch)
				}
				fmt.Printf("  Resume from:    %s%s%s\n", cyan, resumeInfo, reset)

				// Slots replayed since snapshot
				slotsReplayed := mithrilState.LastSlot - mithrilState.SnapshotSlot
				fmt.Printf("  Replayed:       %s%d slots since snapshot bootstrap%s\n", dim, slotsReplayed, reset)
			} else {
				fmt.Printf("  Resume from:    %ssnapshot (fresh start)%s\n", dim, reset)
			}

			// Previous run info with timestamp and time ago
			if mithrilState.LastRunID != "" {
				runInfo := mithrilState.LastRunID
				if !mithrilState.LastRunAt.IsZero() {
					runInfo += fmt.Sprintf(" at %s", mithrilState.LastRunAt.Format("2006-01-02 15:04:05"))
					// Add time since last run
					elapsed := time.Since(mithrilState.LastRunAt)
					runInfo += fmt.Sprintf(" (%s ago)", formatDurationShort(elapsed))
				}
				if mithrilState.LastCommit != "" && mithrilState.LastCommit != revision {
					runInfo += fmt.Sprintf(" (commit: %s)", mithrilState.LastCommit)
				}
				fmt.Printf("  Last run:       %s%s%s\n", dim, runInfo, reset)
			}

			// Last shutdown reason (if available)
			if mithrilState.LastShutdownReason != "" {
				reasonColor := dim
				reason := mithrilState.LastShutdownReason
				// Choose color based on reason type
				switch {
				case reason == state.ShutdownReasonNormal:
					reasonColor = dim // normal is fine
				case reason == state.ShutdownReasonCompleted:
					reasonColor = dim // completed is fine
				case strings.HasPrefix(reason, state.ShutdownReasonStall):
					reasonColor = "\x1b[33m" // yellow - network issue
				case strings.HasPrefix(reason, state.ShutdownReasonLeaderSchedule):
					reasonColor = "\x1b[33m" // yellow - network issue
				case strings.HasPrefix(reason, state.ShutdownReasonError):
					reasonColor = "\x1b[31m" // red - actual error
				}
				shutdownInfo := reason
				if !mithrilState.LastShutdownAt.IsZero() {
					shutdownInfo += fmt.Sprintf(" at %s", mithrilState.LastShutdownAt.Format("2006-01-02 15:04:05"))
				}
				fmt.Printf("  Last shutdown:  %s%s%s\n", reasonColor, shutdownInfo, reset)
			}
		}
	}

	fmt.Println()
	fmt.Printf("%s━━━ Paths ━━━%s\n", gold, reset)

	// AccountsDB path with disk info
	if accountsPath != "" {
		diskInfo := progress.FormatDiskInfo(progress.GetDiskInfo(accountsPath))
		if diskInfo != "" {
			fmt.Printf("  AccountsDB:   %s%s%s  %s%s%s\n", gold, accountsPath, reset, dim, diskInfo, reset)
		} else {
			fmt.Printf("  AccountsDB:   %s%s%s\n", gold, accountsPath, reset)
		}
	}

	// Blockstore path with disk info
	if blockstorePath != "" {
		diskInfo := progress.FormatDiskInfo(progress.GetDiskInfo(blockstorePath))
		if diskInfo != "" {
			fmt.Printf("  Blockstore:   %s%s%s  %s%s%s\n", gold, blockstorePath, reset, dim, diskInfo, reset)
		} else {
			fmt.Printf("  Blockstore:   %s%s%s\n", gold, blockstorePath, reset)
		}
	}

	// Snapshots path with disk info
	snapshotDir := snapshotDlPath
	if snapshotDir == "" {
		snapshotDir = snapshotArchivePath
	}
	if snapshotDir != "" {
		diskInfo := progress.FormatDiskInfo(progress.GetDiskInfo(snapshotDir))
		if diskInfo != "" {
			fmt.Printf("  Snapshots:    %s%s%s  %s%s%s\n", gold, snapshotDir, reset, dim, diskInfo, reset)
		} else {
			fmt.Printf("  Snapshots:    %s%s%s\n", gold, snapshotDir, reset)
		}
	}

	// Log directory with disk info
	if logDir != "" {
		diskInfo := progress.FormatDiskInfo(progress.GetDiskInfo(logDir))
		if diskInfo != "" {
			fmt.Printf("  Logs:         %s%s%s  %s%s%s\n", gold, logDir, reset, dim, diskInfo, reset)
		} else {
			fmt.Printf("  Logs:         %s%s%s\n", gold, logDir, reset)
		}
	}

	// Block source
	fmt.Printf("  Block source: %s%s%s", gold, blockSource, reset)
	if blockSource == "lightbringer" {
		// Lightbringer is temporarily disabled - show this in startup info
		fmt.Printf(" %s(disabled, using rpc)%s\n", dim, reset)
	} else {
		fmt.Println()
	}

	// RPC endpoints - show auxiliary (network.rpc) endpoints
	if len(rpcEndpoints) > 0 {
		fmt.Printf("  RPC:          %s%s%s (primary)\n", gold, rpcEndpoints[0], reset)
		for _, ep := range rpcEndpoints[1:] {
			fmt.Printf("                %s%s%s (fallback)\n", gold, ep, reset)
		}
	}

	fmt.Println()
}

// snapshotInfo holds information about a detected snapshot file
type snapshotInfo struct {
	filename string
	slot     uint64
	baseSlot uint64 // For incrementals: the base (full) snapshot slot
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
			base, end := parseSlotsFromIncrementalName(name)
			snapshots = append(snapshots, snapshotInfo{
				filename: name,
				slot:     end,
				baseSlot: base,
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
	_, endSlot := parseSlotsFromIncrementalName(name)
	return endSlot
}

// parseSlotsFromIncrementalName extracts both base and end slots from "incremental-snapshot-{baseSlot}-{endSlot}-{hash}.tar.zst"
func parseSlotsFromIncrementalName(name string) (baseSlot, endSlot uint64) {
	// Remove "incremental-snapshot-" prefix and ".tar.zst" suffix
	if len(name) <= 29 {
		return 0, 0
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
		return 0, 0
	}

	// Parse base slot
	base, err := strconv.ParseUint(trimmed[:firstDash], 10, 64)
	if err != nil {
		return 0, 0
	}

	// Find second dash (after endSlot)
	remaining := trimmed[firstDash+1:]
	for i := 0; i < len(remaining); i++ {
		if remaining[i] == '-' {
			end, err := strconv.ParseUint(remaining[:i], 10, 64)
			if err != nil {
				return base, 0
			}
			return base, end
		}
	}
	return base, 0
}

// findMatchingIncremental finds a local incremental snapshot that matches the given base slot.
// Returns the best (highest end slot) matching incremental, or nil if none found.
func findMatchingIncremental(snapshotDir string, baseSlot uint64) *snapshotInfo {
	snapshots := detectExistingSnapshots(snapshotDir)

	var best *snapshotInfo
	for i := range snapshots {
		snap := &snapshots[i]
		if snap.isIncr && snap.baseSlot == baseSlot {
			if best == nil || snap.slot > best.slot {
				best = snap
			}
		}
	}
	return best
}

// detectFreshSnapshot checks for an existing snapshot file within the freshness threshold.
// Returns the snapshotInfo if found, nil otherwise.
func detectFreshSnapshot(snapshotDir string, fullThreshold int, rpcEndpoints []string, ctx context.Context) *snapshotInfo {
	if snapshotDir == "" {
		return nil
	}

	snapshots := detectExistingSnapshots(snapshotDir)
	if len(snapshots) == 0 {
		return nil
	}

	// Get current slot estimate from RPC
	currentSlot, err := queryCurrentSlot(ctx, rpcEndpoints)
	if err != nil {
		mlog.Log.Infof("could not query current slot: %v", err)
		return nil
	}

	// Find the freshest full snapshot within threshold
	var bestSnapshot *snapshotInfo
	for i := range snapshots {
		snap := &snapshots[i]
		if snap.isIncr {
			continue // Skip incrementals
		}
		if currentSlot > snap.slot && (currentSlot-snap.slot) <= uint64(fullThreshold) {
			if bestSnapshot == nil || snap.slot > bestSnapshot.slot {
				bestSnapshot = snap
			}
		}
	}

	return bestSnapshot
}

// queryCurrentSlot gets the current slot from RPC.
func queryCurrentSlot(ctx context.Context, rpcEndpoints []string) (uint64, error) {
	if len(rpcEndpoints) == 0 {
		return 0, fmt.Errorf("no RPC endpoints configured")
	}

	// Use the first RPC endpoint
	client := solrpc.New(rpcEndpoints[0])
	slot, err := client.GetSlot(ctx, solrpc.CommitmentFinalized)
	if err != nil {
		return 0, fmt.Errorf("failed to get slot from RPC: %w", err)
	}
	return slot, nil
}

// queryLatestSnapshotSlot queries the latest available snapshot slot from the network.
func queryLatestSnapshotSlot(ctx context.Context, rpcEndpoints []string) (uint64, error) {
	if len(rpcEndpoints) == 0 {
		return 0, fmt.Errorf("no RPC endpoints configured")
	}

	snapCfg := buildSnapshotConfig(rpcEndpoints)
	info, err := snapshotdl.GetSnapshotURLWithInfo(ctx, snapCfg)
	if err != nil {
		return 0, fmt.Errorf("failed to query snapshot info: %w", err)
	}
	return uint64(info.Slot), nil
}

// buildFromExistingSnapshot builds AccountsDB from an existing downloaded snapshot file.
func buildFromExistingSnapshot(ctx context.Context, snap *snapshotInfo, snapshotDir, accountsPath, blockstorePath string, rpcEndpoints []string) (*accountsdb.AccountsDb, *snapshot.SnapshotManifest, error) {
	snapCfg := buildSnapshotConfig(rpcEndpoints)

	// Construct full path to snapshot file
	fullSnapshotPath := filepath.Join(snapshotDir, snap.filename)
	mlog.Log.Infof("building AccountsDB from existing snapshot: %s", fullSnapshotPath)

	// Create progress display for extract
	dp := progress.NewDualProgress()

	accountsDb, manifest, err := snapshot.BuildAccountsDbAuto(ctx, fullSnapshotPath, snapshotDir, int(snap.slot), int(snap.slot), accountsPath, rpcEndpoints, blockstorePath, snapCfg, dp)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build AccountsDB from snapshot: %w", err)
	}
	mlog.Log.Infof("Finished building AccountsDB from existing snapshot")

	return accountsDb, manifest, nil
}

// downloadAndBuildFromSnapshot finds, downloads, and builds AccountsDB from a snapshot
func downloadAndBuildFromSnapshot(ctx context.Context, rpcEndpoints []string, snapshotDownloadPath, accountsPath, blockstorePath string) (*accountsdb.AccountsDb, *snapshot.SnapshotManifest, error) {
	snapCfg := buildSnapshotConfig(rpcEndpoints)
	fullSnapshotDlStart := time.Now()
	fullSnapshotInfo, err := snapshotdl.GetSnapshotURLWithInfo(ctx, snapCfg)
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

	accountsDb, manifest, err := snapshot.BuildAccountsDbAuto(ctx, fullSnapshotURL, snapshotDownloadPath, fullSnapshotSlot, fullSnapshotSlot, accountsPath, rpcEndpoints, blockstorePath, snapCfg, dp)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build AccountsDB from snapshot: %w", err)
	}
	mlog.Log.Infof("Finished building AccountsDB")

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

// decodeRecentBlockhashes converts state.BlockhashEntry list to sealevel.SysvarRecentBlockhashes
func decodeRecentBlockhashes(entries []state.BlockhashEntry) sealevel.SysvarRecentBlockhashes {
	result := make(sealevel.SysvarRecentBlockhashes, 0, len(entries))
	dropped := 0
	for _, entry := range entries {
		hashBytes, err := base58.Decode(entry.Blockhash)
		if err != nil || len(hashBytes) != 32 {
			dropped++
			continue
		}
		var blockhash [32]byte
		copy(blockhash[:], hashBytes)
		result = append(result, sealevel.RecentBlockHashesEntry{
			Blockhash:     blockhash,
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: entry.LamportsPerSignature},
		})
	}
	if dropped > 0 {
		mlog.Log.Errorf("dropped %d/%d RecentBlockhashes entries due to invalid base58 - state file may be corrupted", dropped, len(entries))
	}
	return result
}

// decodeSlotHashes converts state.SlotHashEntry list to sealevel.SysvarSlotHashes
func decodeSlotHashes(entries []state.SlotHashEntry) sealevel.SysvarSlotHashes {
	result := make(sealevel.SysvarSlotHashes, 0, len(entries))
	dropped := 0
	for _, entry := range entries {
		hashBytes, err := base58.Decode(entry.Hash)
		if err != nil || len(hashBytes) != 32 {
			dropped++
			continue
		}
		var hash [32]byte
		copy(hash[:], hashBytes)
		result = append(result, sealevel.SlotHash{
			Slot: entry.Slot,
			Hash: hash,
		})
	}
	if dropped > 0 {
		mlog.Log.Errorf("dropped %d/%d SlotHashes entries due to invalid base58 - state file may be corrupted", dropped, len(entries))
	}
	return result
}

// encodeRecentBlockhashes converts sealevel.SysvarRecentBlockhashes to state.BlockhashEntry list
func encodeRecentBlockhashes(sysvar *sealevel.SysvarRecentBlockhashes) []state.BlockhashEntry {
	if sysvar == nil {
		return nil
	}
	result := make([]state.BlockhashEntry, 0, len(*sysvar))
	for _, entry := range *sysvar {
		result = append(result, state.BlockhashEntry{
			Blockhash:            base58.Encode(entry.Blockhash[:]),
			LamportsPerSignature: entry.FeeCalculator.LamportsPerSignature,
		})
	}
	return result
}

// encodeSlotHashes converts sealevel.SysvarSlotHashes to state.SlotHashEntry list
func encodeSlotHashes(sysvar *sealevel.SysvarSlotHashes) []state.SlotHashEntry {
	if sysvar == nil {
		return nil
	}
	result := make([]state.SlotHashEntry, 0, len(*sysvar))
	for _, entry := range *sysvar {
		result = append(result, state.SlotHashEntry{
			Slot: entry.Slot,
			Hash: base58.Encode(entry.Hash[:]),
		})
	}
	return result
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

// runReplayWithRecovery wraps replay.ReplayBlocks with panic recovery.
// It distinguishes between:
// - Panics during commit (commitInProgress=true): AccountsDB may be corrupted, marks state as corrupted
// - Panics outside commit (divergence): AccountsDB is safe, logs helpful message
// After handling, it re-panics to propagate the error.
func runReplayWithRecovery(
	ctx context.Context,
	accountsDb *accountsdb.AccountsDb,
	accountsDbPath string,
	manifest *snapshot.SnapshotManifest,
	resumeState *replay.ResumeState,
	startSlot, endSlot uint64,
	rpcEndpoints []string, // RPC endpoints in priority order (first = primary)
	blockDir string,
	txParallelism int,
	isLive bool,
	useLightbringer bool,
	dbgOpts *replay.DebugOptions,
	metricsWriter io.Writer,
	rpcServer *rpcserver.RpcServer,
	mithrilState *state.MithrilState,
	blockFetchOpts *replay.BlockFetchOpts,
	replayStartTime time.Time, // Start time for resume context
) *replay.ReplayResult {
	var result *replay.ReplayResult

	// Create callback to write state immediately on cancellation
	// This eliminates the timing window between bankhash persistence and state file update
	onCancelWriteState := func(r *replay.ReplayResult) error {
		if mithrilState == nil {
			return nil
		}

		// Calculate epoch for the last persisted slot
		var lastEpoch uint64
		if sealevel.SysvarCache.EpochSchedule.Sysvar != nil {
			lastEpoch = sealevel.SysvarCache.EpochSchedule.Sysvar.GetEpoch(r.LastPersistedSlot)
		}

		// Build shutdown context
		var shutdownCtx *state.ShutdownContext
		if r.LastAcctsLtHash != nil {
			shutdownCtx = &state.ShutdownContext{
				RunID:          replay.CurrentRunID,
				WriterVersion:  getVersion(),
				WriterCommit:   getCommit(),
				WriterBranch:   getBranch(),
				ShutdownReason: state.ShutdownReasonNormal, // This is always a cancel (Ctrl+C)
				Epoch:          lastEpoch,

				// LtHash and fee state
				AcctsLtHash:          base64.StdEncoding.EncodeToString(r.LastAcctsLtHash.Hash()),
				LamportsPerSignature: r.LastLamportsPerSignature,
				PrevLamportsPerSig:   r.LastPrevLamportsPerSig,
				NumSignatures:        r.LastNumSignatures,

				// Blockhash context
				RecentBlockhashes: encodeRecentBlockhashes(r.LastRecentBlockhashes),
				EvictedBlockhash:  base58.Encode(r.LastEvictedBlockhash[:]),
				LastBlockhash:     base58.Encode(r.LastBlockhash[:]),

				// SlotHashes context
				SlotHashes: encodeSlotHashes(r.LastSlotHashes),

				// ReplayCtx fields
				Capitalization:          r.LastCapitalization,
				SlotsPerYear:            r.LastSlotsPerYear,
				InflationInitial:        r.LastInflation.Initial,
				InflationTerminal:       r.LastInflation.Terminal,
				InflationTaper:          r.LastInflation.Taper,
				InflationFoundation:     r.LastInflation.FoundationVal,
				InflationFoundationTerm: r.LastInflation.FoundationTerm,

				// EpochStakes - required for correct leader schedule on resume
				ComputedEpochStakes: r.ComputedEpochStakes,
			}
		}

		// Write state immediately
		if err := mithrilState.UpdateOnShutdown(accountsDbPath, r.LastPersistedSlot, r.LastPersistedBankhash, shutdownCtx); err != nil {
			return err
		}

		// Record shutdown in history
		state.RecordShutdown(accountsDbPath, r.LastPersistedSlot, base58.Encode(r.LastPersistedBankhash), replay.CurrentRunID, getVersion(), getCommit(), getBranch(), state.ShutdownReasonNormal)

		mlog.Log.Infof("State saved to %s/mithril_state.json at slot %d", accountsDbPath, r.LastPersistedSlot)
		return nil
	}

	defer func() {
		if r := recover(); r != nil {
			runID := replay.CurrentRunID
			if replay.IsCommitInProgress() {
				commitSlot := replay.GetCommitSlot()
				mlog.Log.Errorf("[run:%s] FATAL: Panic during commit at slot %d - AccountsDB may be corrupted", runID, commitSlot)
				mlog.Log.Errorf("[run:%s] Panic occurred between StoreAccounts and StoreBankHashForSlot", runID)
				// Mark state as corrupted so next startup knows to rebuild
				if mithrilState != nil {
					reason := fmt.Sprintf("panic during commit at slot %d: %v", commitSlot, r)
					if err := mithrilState.MarkCorrupted(accountsDbPath, reason); err != nil {
						mlog.Log.Errorf("[run:%s] Failed to mark state as corrupted: %v", runID, err)
					} else {
						// Record corruption in history
						state.RecordCorrupted(accountsDbPath, mithrilState.LastSlot, mithrilState.LastBankhash, runID, getVersion(), getCommit(), getBranch(), reason)
					}
				}
			} else {
				mlog.Log.Errorf("[run:%s] Panic during replay (outside commit window) - AccountsDB is SAFE", runID)
				mlog.Log.Errorf("[run:%s] This appears to be a divergence panic. You can resume from the last persisted slot.", runID)
			}
			// Print debug info and paths
			mlog.Log.Errorf("[run:%s] Debug info:", runID)
			if mithrilState != nil {
				mlog.Log.Errorf("[run:%s]   Snapshot slot: %d", runID, mithrilState.SnapshotSlot)
				if mithrilState.FullSnapshot != nil {
					mlog.Log.Errorf("[run:%s]   Full snapshot:  %s (slot %d)", runID, mithrilState.FullSnapshot.Path, mithrilState.FullSnapshot.Slot)
				}
				if mithrilState.IncrSnapshot != nil {
					mlog.Log.Errorf("[run:%s]   Incr snapshot:  %s (base %d -> slot %d)", runID, mithrilState.IncrSnapshot.Path, mithrilState.IncrSnapshot.BaseSlot, mithrilState.IncrSnapshot.Slot)
				}
				if mithrilState.LastSlot > 0 {
					mlog.Log.Errorf("[run:%s]   Last persisted: slot %d, bankhash %s", runID, mithrilState.LastSlot, mithrilState.LastBankhash)
				}
			}
			mlog.Log.Errorf("[run:%s] Debug files:", runID)
			mlog.Log.Errorf("[run:%s]   Bankhash log: %s/bankhash.log", runID, accountsDbPath)
			mlog.Log.Errorf("[run:%s]   State file:   %s/mithril_state.json", runID, accountsDbPath)
			// Re-panic to propagate the error
			panic(r)
		}
	}()

	result = replay.ReplayBlocks(ctx, accountsDb, accountsDbPath, manifest, resumeState, startSlot, endSlot, rpcEndpoints, blockDir, txParallelism, isLive, useLightbringer, dbgOpts, metricsWriter, rpcServer, blockFetchOpts, onCancelWriteState)
	return result
}
