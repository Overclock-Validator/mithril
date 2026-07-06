package node

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/config"
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/lightbringer"
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
	solana "github.com/gagliardetto/solana-go"
	solrpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/mr-tron/base58"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

var (
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

	bootstrapMode               string // "auto", "snapshot", or "accountsdb"
	snapshotArchivePath         string
	incrementalSnapshotFilename string
	accountsPath                string
	scratchDirectory            string
	rpcEndpoints                []string
	cluster                     string // "alpenglow" (the only cluster this build boots)
	blockSource                 string // "turbine" (default), "rpc", or "lightbringer"
	lightbringerEndpoint        string
	blockMaxRPS                 int    // Rate limit for block fetching
	blockMaxInflight            int    // Max concurrent block fetch workers
	blockTipPollIntervalMs      int    // Tip poll interval in milliseconds
	blockTipSafetyMargin        int    // Don't fetch within N slots of tip
	consensusModeFlag           string // raw --consensus-mode value (cobra binding)
	consensusMode               string // resolved: "verifying" (default) or "validator"
	alpenglowObserverBindAddr   string
	alpenglowMaxMessageBytes    int64
	alpenglowBLSDST             string
	validatorIdentityKeypair    string
	validatorVoteAccountKeypair string
	validatorWithdrawerKeypair  string

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
	rewindToSlot   int64 // --rewind-to-slot: restore durable state to a fold batch boundary at startup
	pprofPort      int64
	blockstorePath string
	txParallelism  int64

	debugTxs                       []string
	debugAcctWrites                []string
	debugDumpEpochVotingRewardDiff bool
	cpuprofPath                    string

	paramArenaSizeMB         uint64
	borrowedAccountArenaSize uint64

	rpcPort int

	// Lightbringer sidecar config
	lightbringerEnabled          bool
	lightbringerBinaryPath       string
	lightbringerGossipEntrypoint string
	lightbringerStorage          string
	lightbringerRpcAddr          string
	lightbringerGrpcAddr         string
	lightbringerConfigDir        string
	lightbringerInfluxdbHost     string
	lightbringerInfluxdbDatabase string
	lightbringerInfluxdbToken    string
	lightbringerBlockConfirmHTTP string
	lightbringerBlockConfirmWS   string
	lightbringerQuiet            bool

	// Native turbine receiver config
	turbineBindAddr         string
	turbineGossipEntrypoint string
	turbineGossipBindAddr   string
	turbineAdvertisedIP     string
	turbineShredVersion     int
)

func snapshotEpochForState(manifest *snapshot.SnapshotManifest) uint64 {
	if manifest == nil || manifest.Bank == nil {
		return 0
	}
	if manifest.Bank.EpochSchedule.SlotsPerEpoch != 0 {
		epoch := manifest.Bank.EpochSchedule.GetEpoch(manifest.Bank.Slot)
		if manifest.Bank.Epoch != epoch {
			mlog.Log.Warnf("manifest bank epoch %d differs from manifest epoch schedule epoch %d at slot %d; using schedule-derived epoch",
				manifest.Bank.Epoch, epoch, manifest.Bank.Slot)
		}
		return epoch
	}

	return manifest.Bank.Epoch
}

func epochScheduleFromState(s *state.MithrilState) *sealevel.SysvarEpochSchedule {
	if s != nil && s.ManifestEpochSchedule != nil && s.ManifestEpochSchedule.SlotsPerEpoch != 0 {
		return &sealevel.SysvarEpochSchedule{
			SlotsPerEpoch:            s.ManifestEpochSchedule.SlotsPerEpoch,
			LeaderScheduleSlotOffset: s.ManifestEpochSchedule.LeaderScheduleSlotOffset,
			Warmup:                   s.ManifestEpochSchedule.Warmup,
			FirstNormalEpoch:         s.ManifestEpochSchedule.FirstNormalEpoch,
			FirstNormalSlot:          s.ManifestEpochSchedule.FirstNormalSlot,
		}
	}
	return sealevel.SysvarCache.EpochSchedule.Sysvar
}

func epochForStateSlot(s *state.MithrilState, slot uint64) uint64 {
	if epochSchedule := epochScheduleFromState(s); epochSchedule != nil {
		return epochSchedule.GetEpoch(slot)
	}
	return 0
}

// alpenglowAddrForGossip returns the Votor QUIC address to advertise in gossip,
// or "" when the bind address is empty or has no fixed port.
func alpenglowAddrForGossip(bindAddr string) string {
	bindAddr = strings.TrimSpace(bindAddr)
	if bindAddr == "" {
		return ""
	}
	_, portRaw, err := net.SplitHostPort(bindAddr)
	if err != nil {
		mlog.Log.Warnf("ALPENGLOW observer: not advertising invalid Votor bind address %q in gossip: %v", bindAddr, err)
		return ""
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port <= 0 || port > 0xffff {
		mlog.Log.Warnf("ALPENGLOW observer: not advertising Votor bind address %q in gossip because the port is not fixed", bindAddr)
		return ""
	}
	return bindAddr
}

func loadValidatorIdentityKeypair(path string) (ed25519.PrivateKey, string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, "", nil
	}
	key, err := solana.PrivateKeyFromSolanaKeygenFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("load validator identity keypair %s: %w", path, err)
	}
	if len(key) != ed25519.PrivateKeySize {
		return nil, "", fmt.Errorf("validator identity keypair %s has invalid private key size %d", path, len(key))
	}
	return ed25519.PrivateKey(append([]byte(nil), key...)), key.PublicKey().String(), nil
}

func loadValidatorKeypairPubkey(label, path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", nil
	}
	key, err := solana.PrivateKeyFromSolanaKeygenFile(path)
	if err != nil {
		return "", fmt.Errorf("load %s keypair %s: %w", label, path, err)
	}
	return key.PublicKey().String(), nil
}

func manifestEpochScheduleSeedMatches(s *state.MithrilState, manifest *snapshot.SnapshotManifest) bool {
	if s == nil || s.ManifestEpochSchedule == nil || manifest == nil || manifest.Bank == nil {
		return false
	}
	m := manifest.Bank.EpochSchedule
	return s.ManifestEpochSchedule.SlotsPerEpoch == m.SlotsPerEpoch &&
		s.ManifestEpochSchedule.LeaderScheduleSlotOffset == m.LeaderScheduleSlotOffset &&
		s.ManifestEpochSchedule.Warmup == m.Warmup &&
		s.ManifestEpochSchedule.FirstNormalEpoch == m.FirstNormalEpoch &&
		s.ManifestEpochSchedule.FirstNormalSlot == m.FirstNormalSlot
}

func refreshManifestSeedFromManifest(accountsPath string, s *state.MithrilState, manifest *snapshot.SnapshotManifest) {
	if s == nil || manifest == nil || manifest.Bank == nil {
		return
	}

	snapshotEpoch := snapshotEpochForState(manifest)
	if manifestEpochScheduleSeedMatches(s, manifest) && s.SnapshotEpoch == snapshotEpoch {
		return
	}

	oldSnapshotEpoch := s.SnapshotEpoch
	if oldSnapshotEpoch != 0 && oldSnapshotEpoch != snapshotEpoch && s.LastSlot > s.SnapshotSlot {
		reason := fmt.Sprintf("snapshot epoch frame changed from %d to %d after replay had already persisted slot %d; rebuild AccountsDB from snapshot",
			oldSnapshotEpoch, snapshotEpoch, s.LastSlot)
		if err := s.MarkCorrupted(accountsPath, reason); err != nil {
			mlog.Log.Errorf("failed to mark state as corrupted: %v", err)
		}
		klog.Fatalf(reason)
	}

	s.SnapshotEpoch = snapshotEpoch
	snapshot.PopulateManifestSeed(s, manifest)
	if s.LastSlot > 0 {
		s.LastEpoch = epochForStateSlot(s, s.LastSlot)
	}
	if err := s.Save(accountsPath); err != nil {
		mlog.Log.Errorf("failed to refresh manifest seed data in state file: %v", err)
		return
	}
	mlog.Log.Warnf("refreshed manifest-derived state seed data from snapshot manifest (snapshot_epoch %d -> %d)",
		oldSnapshotEpoch, snapshotEpoch)
}

func init() {
	// [bootstrap] section flags
	Run.Flags().StringVar(&bootstrapMode, "bootstrap", "auto", "Bootstrap mode: 'auto' (use AccountsDB if exists, else snapshot), 'accountsdb' (require existing), 'snapshot' (rebuild from snapshot), 'new-snapshot' (always download fresh)")
	Run.Flags().StringVar(&snapshotArchivePath, "snapshot", "", "Path to specific full snapshot file (bypasses auto-discovery)")
	Run.Flags().StringVar(&incrementalSnapshotFilename, "incremental-snapshot", "", "Path to specific incremental snapshot file (bypasses auto-discovery)")
	Run.Flags().StringVar(&snapshotDlPath, "download-snapshot-path", "", "Directory for discovered/downloaded snapshots")

	// [ledger] section flags
	Run.Flags().StringVarP(&accountsPath, "accounts-path", "o", "", "Output path for writing AccountsDB data to")
	Run.Flags().StringVar(&blockstorePath, "ledger-path", "/tmp/blocks", "Path containing slot.json files")

	// [network] section flags
	Run.Flags().StringSliceVarP(&rpcEndpoints, "rpc", "r", []string{}, "URL(s) for RPC endpoint(s) - can specify multiple")
	Run.Flags().StringVar(&cluster, "cluster", "alpenglow", "Solana cluster: 'alpenglow' (default; the only cluster this build boots — 'mainnet-beta'/'testnet'/'devnet' need a dev-branch TowerBFT build until they upgrade to Alpenglow)")

	// [rpc] section flags (Mithril's RPC server)
	Run.Flags().IntVar(&rpcPort, "rpc-port", 0, "RPC server port. Default off.")

	// [replay] section flags
	Run.Flags().Int64Var(&txParallelism, "txpar", 0, "Set to 0 to use sequential execution, or >0 to execute a topsort tx plan with the given number of workers")
	Run.Flags().Int64Var(&numReplaySlots, "num-slots", 0, "Number of slots to replay (0 = run continuously)")
	Run.Flags().Int64VarP(&endSlot, "end-slot", "e", -1, "Block at which to stop replaying, inclusive (-1 = run continuously)")

	// [consensus] section flags
	Run.Flags().StringVar(&consensusModeFlag, "consensus-mode", "verifying", "Node mode: 'verifying' (default; non-voting — observe, execute, and verify) or 'validator' (requires identity + vote-account keypairs, turbine source, gossip entrypoint, and the Votor QUIC listener; voting engine not yet active)")
	Run.Flags().StringVar(&alpenglowObserverBindAddr, "alpenglow-observer-bind-addr", "", "Passive Alpenglow Votor QUIC listener address")
	Run.Flags().StringVar(&alpenglowBLSDST, "alpenglow-bls-dst", "", "BLS hash-to-curve DST override (must match cluster's solana-bls version; empty = default)")
	Run.Flags().Int64Var(&alpenglowMaxMessageBytes, "alpenglow-max-message-bytes", 0, "Maximum Alpenglow Votor QUIC stream payload size (0 = default)")
	Run.Flags().StringVar(&validatorIdentityKeypair, "identity-keypair", "", "Validator identity keypair for native turbine gossip (Solana keygen JSON)")
	Run.Flags().StringVar(&validatorVoteAccountKeypair, "vote-account-keypair", "", "Vote account keypair path for validator diagnostics (Solana keygen JSON)")
	Run.Flags().StringVar(&validatorWithdrawerKeypair, "authorized-withdrawer-keypair", "", "Authorized withdrawer keypair path for validator diagnostics (Solana keygen JSON)")

	// [tuning] section flags
	Run.Flags().Uint64Var(&paramArenaSizeMB, "param-arena-size-mb", 512, "Size in MB for serialized parameter arena (0 to disable)")
	Run.Flags().Uint64Var(&borrowedAccountArenaSize, "borrowed-account-arena-size", 1024, "Number of borrowed accounts to preallocate in arena (0 to disable)")
	Run.Flags().IntVar(&snapshot.ZstdDecoderConcurrency, "zstd-decoder-concurrency", runtime.NumCPU(), "Zstd decoder concurrency")
	Run.Flags().IntVar(&snapshot.MaxConcurrentFlushers, "max-concurrent-flushers", snapshot.DefaultSnapshotMaxConcurrentFlushers, "Bound for number of log shards to flush to Accounts DB Index at once")
	Run.Flags().IntVar(&snapshot.SnapshotAppendVecCopyingWorkers, "snapshot-append-vec-workers", snapshot.DefaultSnapshotAppendVecCopyingWorkers, "Snapshot bootstrap appendvec write workers")
	Run.Flags().IntVar(&snapshot.SnapshotIndexEntryBuilderWorkers, "snapshot-index-builder-workers", snapshot.DefaultSnapshotIndexEntryBuilderWorkers, "Snapshot bootstrap account-index parser workers")
	Run.Flags().IntVar(&snapshot.SnapshotIndexEntryCommitterWorkers, "snapshot-index-committer-workers", snapshot.DefaultSnapshotIndexEntryCommitterWorkers, "Snapshot bootstrap account-index shard enqueue workers")
	Run.Flags().IntVar(&snapshot.SnapshotIndexShards, "snapshot-index-shards", snapshot.DefaultSnapshotIndexShards, "Snapshot bootstrap account-index shard count")
	Run.Flags().StringVar(&snapshot.SnapshotIndexTempDir, "snapshot-index-temp-dir", "", "Optional directory for snapshot index shard logs/SST staging")
	Run.Flags().BoolVar(&sbpf.UsePool, "use-pool", true, "Disable to allocate fresh slices")
	Run.Flags().IntVar(&accountsdb.StoreAccountsWorkers, "store-accounts-workers", 128, "Number of workers to write account updates")
	Run.Flags().IntVar(&accountsdb.ProgramCacheMaxMB, "program-cache-max-mb", accountsdb.DefaultProgramCacheMaxMB, "Maximum approximate SBPF program cache size in MiB")
	Run.Flags().Int64Var(&rewindToSlot, "rewind-to-slot", 0, "Rewind durable account state to the fold batch boundary at this slot before replaying (must be a retained boundary; run once to list boundaries on mismatch)")

	// [tuning.pprof] section flags
	Run.Flags().Int64Var(&pprofPort, "pprof-port", -1, "Port to serve HTTP pprof endpoint")
	Run.Flags().StringVar(&cpuprofPath, "cpu-profile-path", "", "Filename to write CPU profile")

	// [debug] section flags
	Run.Flags().StringSliceVar(&debugTxs, "transaction-signatures", []string{}, "Pass tx signature strings to enable debug logging during that transaction's execution")
	Run.Flags().StringSliceVar(&debugAcctWrites, "account-writes", []string{}, "Pass account pubkeys to enable debug logging of transactions that modify the account")
	Run.Flags().BoolVar(&debugDumpEpochVotingRewardDiff, "dump-epoch-voting-reward-diff", false, "Write epoch-boundary reward artifacts, including a voting diff and a full dump of locally calculated rewards")

	// Top-level flags
	Run.Flags().StringVar(&scratchDirectory, "scratch-directory", "/tmp", "Path for downloads (e.g. snapshots) and other temp state")

	// [block] section flags
	Run.Flags().StringVar(&blockSource, "block-source", "turbine", "Block source: 'turbine' (default, live), 'rpc' (catch-up/debug), or 'lightbringer'")
	Run.Flags().StringVar(&lightbringerEndpoint, "lightbringer-endpoint", "", "Address for Lightbringer endpoint (only used when block-source=lightbringer)")
	Run.Flags().StringVar(&turbineBindAddr, "turbine-bind-addr", "", "UDP address for native turbine shred receiver (only used when block-source=turbine)")
	Run.Flags().StringVar(&turbineGossipEntrypoint, "turbine-gossip-entrypoint", "", "Solana gossip entrypoint for native turbine tree joining")
	Run.Flags().StringVar(&turbineGossipBindAddr, "turbine-gossip-bind-addr", "", "UDP address for native turbine gossip traffic (only used when block-source=turbine)")
	Run.Flags().StringVar(&turbineAdvertisedIP, "turbine-advertised-ip", "", "Public IP advertised by native turbine gossip (optional)")
	Run.Flags().IntVar(&turbineShredVersion, "turbine-shred-version", 0, "Shred version for native turbine gossip (0 = discover from entrypoint)")
	Run.Flags().IntVar(&blockMaxRPS, "block-max-rps", 0, "Max RPC requests per second for block fetching (0 = use default)")
	Run.Flags().IntVar(&blockMaxInflight, "block-max-inflight", 0, "Max concurrent block fetch workers (0 = use default)")
	Run.Flags().IntVar(&blockTipPollIntervalMs, "block-tip-poll-ms", 0, "Tip poll interval in milliseconds (0 = use default)")
	Run.Flags().IntVar(&blockTipSafetyMargin, "block-tip-safety-margin", 0, "Don't fetch within N slots of tip (0 = use default)")

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

	// [replay] section (txpar moved to [tuning], with backwards-compatible fallback)
	numReplaySlots = getInt64("num-slots", "replay.num_slots")
	endSlot = getInt64("end-slot", "replay.end_slot")
	if config.IsSet("tuning.txpar") {
		txParallelism = getInt64("txpar", "tuning.txpar")
	} else if config.IsSet("replay.txpar") {
		txParallelism = getInt64("txpar", "replay.txpar")
		mlog.Log.Warnf("config: replay.txpar is deprecated, move to tuning.txpar")
	} else {
		txParallelism = getInt64("txpar", "tuning.txpar") // CLI flag or default
	}

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
	blockstorePath = getString("ledger-path", "storage.shredstore")
	if blockstorePath == "" && config.IsSet("storage.blockstore") {
		blockstorePath = getString("ledger-path", "storage.blockstore")
		mlog.Log.Warnf("config: storage.blockstore is deprecated, use storage.shredstore")
	}
	if blockstorePath == "" {
		blockstorePath = getString("ledger-path", "ledger.path")
	}

	// [network] section (with fallback to legacy [rpc] keys)
	rpcEndpoints = getStringSlice("rpc", "network.rpc")
	if len(rpcEndpoints) == 0 {
		rpcEndpoints = getStringSlice("rpc", "rpc.rpc")
	}

	// Cluster is required for safety (prevents mainnet/testnet mixups).
	// This build is Alpenglow-only: replay applies Alpenglow clock/feature
	// semantics unconditionally, which would diverge the bankhash on a
	// TowerBFT cluster. Refuse anything except an Alpenglow cluster.
	cluster = getString("cluster", "network.cluster")
	if cluster == "" {
		cluster = "alpenglow" // the only cluster this build boots
		mlog.Log.Infof("network.cluster not set; defaulting to %q", cluster)
	}
	if cluster != "alpenglow" {
		return fmt.Errorf("network.cluster %q is not supported by this Alpenglow-only build (TowerBFT clusters need a mithril build from the dev branch until they upgrade to Alpenglow)", cluster)
	}

	// Must be decided before any OpenDb call: with the WAL off, the fold
	// manifests are the index redo log (recovery replays them).
	if config.IsSet("storage.index_wal") && !config.GetBool("storage.index_wal") {
		accountsdb.DisableIndexWAL = true
		mlog.Log.Warnf("storage.index_wal=false: account index runs without a Pebble WAL; fold manifests serve as the index redo log")
	}

	// [rpc] section - Mithril's RPC server
	rpcPort = getInt("rpc-port", "rpc.port")

	// Top-level
	scratchDirectory = getString("scratch-directory", "scratch_directory")

	// [consensus] + [validator] keypairs
	consensusMode = getString("consensus-mode", "consensus.mode")
	switch consensusMode {
	case "", "verifying":
		consensusMode = "verifying"
	case "validator":
		// Selectable now so validator deployments (keypairs, sockets, gossip)
		// are provisioned and validated ahead of the voting engine landing.
		// Requirements are enforced below once the block/turbine settings are
		// resolved; until the engine ships the node runs the same verifying
		// pipeline and casts no votes (warned loudly at startup).
	default:
		return fmt.Errorf("unknown consensus.mode %q (valid: \"verifying\", \"validator\")", consensusMode)
	}
	alpenglowObserverBindAddr = getString("alpenglow-observer-bind-addr", "consensus.alpenglow_observer_bind_addr")
	alpenglowMaxMessageBytes = getInt64("alpenglow-max-message-bytes", "consensus.alpenglow_max_message_bytes")
	alpenglowBLSDST = getString("alpenglow-bls-dst", "consensus.alpenglow_bls_dst")
	validatorIdentityKeypair = getString("identity-keypair", "validator.identity_keypair")
	validatorVoteAccountKeypair = getString("vote-account-keypair", "validator.vote_account_keypair")
	validatorWithdrawerKeypair = getString("authorized-withdrawer-keypair", "validator.authorized_withdrawer_keypair")

	// [block] section
	blockSource = getString("block-source", "block.source")
	blockSourceExplicit := blockSource != "" // operator chose (flag or config) vs defaulted
	if blockSource == "" {
		blockSource = "turbine" // default: native turbine is the live Alpenglow source
	}
	// RPC is a catch-up/debug source on Alpenglow, not a live one: RPC blocks
	// carry no Alpenglow block ids or footer certificates, so certificate
	// gating cannot adjudicate their identity near tip, and durable folds only
	// advance if certificates arrive some other way (the Votor QUIC listener).
	if blockSource == "rpc" {
		if alpenglowObserverBindAddr == "" {
			mlog.Log.Warnf("block source \"rpc\" with no consensus.alpenglow_observer_bind_addr: no certificate feed exists, so durable folds will NOT advance (fail-closed halt once the in-RAM tail fills); use block source \"turbine\" for live operation")
		} else {
			mlog.Log.Warnf("block source \"rpc\" is catch-up/debug only on Alpenglow (RPC blocks carry no block ids or footer certificates); live near-tip operation should use \"turbine\"")
		}
	}
	lightbringerEndpoint = getString("lightbringer-endpoint", "block.lightbringer_endpoint")
	turbineBindAddr = getString("turbine-bind-addr", "block.turbine_bind_addr")
	if turbineBindAddr == "" {
		turbineBindAddr = config.GetString("turbine.bind_addr")
	}
	turbineGossipEntrypoint = getString("turbine-gossip-entrypoint", "turbine.gossip_entrypoint")
	turbineGossipBindAddr = getString("turbine-gossip-bind-addr", "turbine.gossip_bind_addr")
	turbineAdvertisedIP = getString("turbine-advertised-ip", "turbine.advertised_ip")
	turbineShredVersion = getInt("turbine-shred-version", "turbine.shred_version")

	// [lightbringer] section — sidecar management
	lightbringerEnabled = config.GetBool("lightbringer.enabled")
	lightbringerBinaryPath = config.GetString("lightbringer.binary_path")
	if lightbringerBinaryPath == "" {
		lightbringerBinaryPath = "./lightbringer"
	}
	lightbringerGossipEntrypoint = config.GetString("lightbringer.gossip_entrypoint")
	lightbringerStorage = getString("ledger-path", "storage.shredstore")
	if lightbringerStorage == "" && config.IsSet("lightbringer.storage") {
		lightbringerStorage = getString("ledger-path", "lightbringer.storage")
		mlog.Log.Warnf("config: lightbringer.storage is deprecated, use storage.shredstore")
	}
	if lightbringerStorage == "" {
		lightbringerStorage = "./shred-store"
	}
	lightbringerRpcAddr = config.GetString("lightbringer.rpc_addr")
	if lightbringerRpcAddr == "" {
		lightbringerRpcAddr = "127.0.0.1:3000"
	}
	lightbringerGrpcAddr = config.GetString("lightbringer.grpc_addr")
	if lightbringerGrpcAddr == "" {
		lightbringerGrpcAddr = "127.0.0.1:3001"
	}
	lightbringerConfigDir = config.GetString("lightbringer.config_dir")
	if lightbringerConfigDir == "" {
		lightbringerConfigDir = "."
	}
	lightbringerInfluxdbHost = config.GetString("lightbringer.influxdb_host")
	lightbringerInfluxdbDatabase = config.GetString("lightbringer.influxdb_database")
	lightbringerInfluxdbToken = config.GetString("lightbringer.influxdb_token")
	lightbringerBlockConfirmHTTP = config.GetString("lightbringer.block_confirmation_rpc_http")
	lightbringerBlockConfirmWS = config.GetString("lightbringer.block_confirmation_rpc_websocket")
	lightbringerQuiet = config.GetBool("lightbringer.quiet")

	// Auto-sync: when lightbringer is enabled, override block source settings
	if lightbringerEnabled {
		if lightbringerGossipEntrypoint == "" {
			return fmt.Errorf("lightbringer.enabled=true but lightbringer.gossip_entrypoint is empty")
		}
		// Default block.source to "lightbringer" if not explicitly configured
		if !blockSourceExplicit {
			blockSource = "lightbringer"
		} else if blockSource != "lightbringer" {
			mlog.Log.Warnf("lightbringer.enabled=true but block source %q was set explicitly; sidecar will start but will not be used for block delivery", blockSource)
		}
		// Auto-sync grpc_addr to lightbringer_endpoint
		if lightbringerEndpoint == "" {
			lightbringerEndpoint = lightbringerGrpcAddr
		} else if lightbringerEndpoint != lightbringerGrpcAddr {
			mlog.Log.Warnf("lightbringer.grpc_addr (%s) differs from block.lightbringer_endpoint (%s) — using block.lightbringer_endpoint",
				lightbringerGrpcAddr, lightbringerEndpoint)
		}
	}

	// Validate block source requirements
	switch blockSource {
	case "rpc":
		if len(rpcEndpoints) == 0 {
			return fmt.Errorf("block.source=rpc but no RPC endpoints provided (set network.rpc)")
		}
	case "lightbringer":
		if lightbringerEndpoint == "" && !lightbringerEnabled {
			return fmt.Errorf("block.source=lightbringer requires either lightbringer.enabled=true or block.lightbringer_endpoint")
		}
		if len(rpcEndpoints) == 0 {
			return fmt.Errorf("block.source=lightbringer requires RPC endpoints for catchup (set network.rpc)")
		}
	case "turbine":
		if turbineBindAddr == "" {
			turbineBindAddr = "0.0.0.0:8001" // documented default shred port
			mlog.Log.Infof("turbine bind address not set; defaulting to %s", turbineBindAddr)
		}
		if turbineShredVersion < 0 || turbineShredVersion > 0xffff {
			return fmt.Errorf("turbine.shred_version must be between 0 and 65535")
		}
		if len(rpcEndpoints) == 0 {
			return fmt.Errorf("block.source=turbine requires RPC endpoints for catchup and tip polling (set network.rpc)")
		}
	default:
		return fmt.Errorf("invalid block.source %q - must be 'rpc', 'lightbringer', or 'turbine'", blockSource)
	}

	// Validator mode: enforce the deployment shape a voting node needs, even
	// though the voting engine has not landed yet — operators provision once
	// and the config stays valid when voting activates.
	if consensusMode == "validator" {
		var missing []string
		if validatorIdentityKeypair == "" {
			missing = append(missing, "validator.identity_keypair (signs gossip/turbine identity and, later, votes)")
		}
		if validatorVoteAccountKeypair == "" {
			missing = append(missing, "validator.vote_account_keypair (the vote account votes are cast for)")
		}
		if blockSource != "turbine" {
			missing = append(missing, fmt.Sprintf("block.source=turbine (a validator cannot run off %q)", blockSource))
		}
		if turbineGossipEntrypoint == "" {
			missing = append(missing, "turbine.gossip_entrypoint (join the cluster's turbine tree)")
		}
		if alpenglowObserverBindAddr == "" {
			missing = append(missing, "consensus.alpenglow_observer_bind_addr (receive Votor vote/cert traffic)")
		}
		if len(missing) > 0 {
			return fmt.Errorf("consensus.mode=validator requires:\n  - %s", strings.Join(missing, "\n  - "))
		}
		// The authorized withdrawer is deliberately NOT required at runtime —
		// best practice keeps it offline.
		mlog.Log.Warnf("consensus.mode=validator: the voting engine is not implemented yet in this build — running the verifying pipeline, NO votes will be cast")
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
	debugDumpEpochVotingRewardDiff = getBool("dump-epoch-voting-reward-diff", "debug.dump_epoch_voting_reward_diff")
	if !flagChanged("dump-epoch-voting-reward-diff") && !config.IsSet("debug.dump_epoch_voting_reward_diff") {
		debugDumpEpochVotingRewardDiff = getBool("dump-epoch-voting-reward-diff", "development.debug.dump_epoch_voting_reward_diff")
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
	snapshot.SnapshotAppendVecCopyingWorkers = getInt("snapshot-append-vec-workers", "tuning.snapshot_append_vec_workers")
	snapshot.SnapshotIndexEntryBuilderWorkers = getInt("snapshot-index-builder-workers", "tuning.snapshot_index_builder_workers")
	snapshot.SnapshotIndexEntryCommitterWorkers = getInt("snapshot-index-committer-workers", "tuning.snapshot_index_committer_workers")
	snapshot.SnapshotIndexShards = getInt("snapshot-index-shards", "tuning.snapshot_index_shards")
	snapshot.SnapshotIndexTempDir = getString("snapshot-index-temp-dir", "tuning.snapshot_index_temp_dir")
	if snapshot.MaxConcurrentFlushers <= 0 {
		return fmt.Errorf("tuning.max_concurrent_flushers must be > 0")
	}
	if snapshot.SnapshotAppendVecCopyingWorkers <= 0 {
		return fmt.Errorf("tuning.snapshot_append_vec_workers must be > 0")
	}
	if snapshot.SnapshotIndexEntryBuilderWorkers <= 0 {
		return fmt.Errorf("tuning.snapshot_index_builder_workers must be > 0")
	}
	if snapshot.SnapshotIndexEntryCommitterWorkers <= 0 {
		return fmt.Errorf("tuning.snapshot_index_committer_workers must be > 0")
	}
	if snapshot.SnapshotIndexShards <= 0 || snapshot.SnapshotIndexShards > 1000 {
		return fmt.Errorf("tuning.snapshot_index_shards must be between 1 and 1000")
	}
	sbpf.UsePool = getBool("use-pool", "tuning.use_pool")
	accountsdb.StoreAccountsWorkers = getInt("store-accounts-workers", "tuning.store_accounts_workers")
	accountsdb.ProgramCacheMaxMB = getInt("program-cache-max-mb", "tuning.program_cache_max_mb")
	if accountsdb.ProgramCacheMaxMB <= 0 {
		return fmt.Errorf("tuning.program_cache_max_mb must be > 0")
	}

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
	} else if cluster == "alpenglow" {
		// The public Alpenglow test cluster advertises Agave/Votor node versions in
		// the 0.x series; the mainnet default of 3.0.0 would filter every source.
		cfg.MinNodeVersion = "0.3.0"
	}
	if config.IsSet("snapshot.allowed_node_versions") {
		cfg.AllowedNodeVersions = config.GetStringSlice("snapshot.allowed_node_versions")
	}
	if config.IsSet("snapshot.node_blacklist") {
		cfg.NodeBlacklist = config.GetStringSlice("snapshot.node_blacklist")
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

	// MaxAgeDays: default 0 = never delete by age (retention is bounded by
	// max_size_mb per file x max_backups). Set >0 to also prune by age.
	if config.IsSet("log.max_age_days") {
		logCfg.MaxAgeDays = config.GetInt("log.max_age_days")
	} else {
		logCfg.MaxAgeDays = 0
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

	// Lightbringer sidecar management
	var lbManager *lightbringer.Manager
	useLightbringer := blockSource == "lightbringer"
	useTurbine := blockSource == "turbine"
	validatorIdentity, validatorIdentityPubkey, err := loadValidatorIdentityKeypair(validatorIdentityKeypair)
	if err != nil {
		klog.Fatalf("%v", err)
	}
	if validatorVoteAccountKeypair != "" {
		votePubkey, err := loadValidatorKeypairPubkey("vote account", validatorVoteAccountKeypair)
		if err != nil {
			klog.Fatalf("%v", err)
		}
		mlog.Log.Infof("validator vote account configured: %s", votePubkey)
	}
	if validatorWithdrawerKeypair != "" {
		withdrawerPubkey, err := loadValidatorKeypairPubkey("authorized withdrawer", validatorWithdrawerKeypair)
		if err != nil {
			klog.Fatalf("%v", err)
		}
		mlog.Log.FileOnlyf("validator authorized withdrawer configured: %s", withdrawerPubkey)
	}
	if validatorIdentityPubkey != "" {
		mlog.Log.Infof("validator identity configured for native gossip: %s", validatorIdentityPubkey)
	} else if useTurbine && alpenglowObserverBindAddr != "" {
		mlog.Log.Warnf("ALPENGLOW observer: no validator.identity_keypair configured; native gossip will use an ephemeral identity and may not receive staked Votor traffic")
	}

	if lightbringerEnabled {
		lbLogWriter := mlog.Log.CreateSubprocessWriter("lightbringer")

		lbManager = lightbringer.NewManager(lightbringer.ManagerConfig{
			BinaryPath: lightbringerBinaryPath,
			ConfigDir:  lightbringerConfigDir,
			GrpcAddr:   lightbringerGrpcAddr,
			TOML: lightbringer.LightbringerTOML{
				GossipEntrypoint:    lightbringerGossipEntrypoint,
				Storage:             lightbringerStorage,
				RpcAddr:             lightbringerRpcAddr,
				GrpcAddr:            lightbringerGrpcAddr,
				InfluxdbHost:        lightbringerInfluxdbHost,
				InfluxdbDatabase:    lightbringerInfluxdbDatabase,
				InfluxdbToken:       lightbringerInfluxdbToken,
				BlockConfirmRpcHTTP: lightbringerBlockConfirmHTTP,
				BlockConfirmRpcWS:   lightbringerBlockConfirmWS,
				Quiet:               lightbringerQuiet,
			},
			LogWriter: lbLogWriter,
		})

		configPath, err := lbManager.WriteConfig()
		if err != nil {
			klog.Fatalf("failed to write Lightbringer config: %v", err)
		}
		mlog.Log.Infof("lightbringer: wrote config to %s", configPath)

		if err := lbManager.Start(); err != nil {
			mlog.Log.Warnf("lightbringer: failed to start: %v — falling back to RPC", err)
			useLightbringer = false
			if len(rpcEndpoints) == 0 {
				klog.Fatalf("lightbringer failed to start and no RPC endpoints configured for fallback (set network.rpc)")
			}
		} else {
			defer func() {
				if err := lbManager.Stop(10 * time.Second); err != nil {
					mlog.Log.Warnf("lightbringer: shutdown error: %v", err)
				}
			}()

			// Monitor for crashes and auto-restart in background.
			// Use sync.Once for safe channel close from both the fallback path and the defer.
			lbStopMonitor := make(chan struct{})
			var lbStopOnce sync.Once
			stopMonitor := func() { lbStopOnce.Do(func() { close(lbStopMonitor) }) }
			go lbManager.MonitorAndRestart(lbStopMonitor, 5)
			defer stopMonitor()

			if err := lbManager.WaitReady(30 * time.Second); err != nil {
				mlog.Log.Warnf("lightbringer: %v — falling back to RPC", err)
				useLightbringer = false
				// Stop the monitor and Lightbringer immediately so they don't run unused during replay.
				stopMonitor()
				if stopErr := lbManager.Stop(5 * time.Second); stopErr != nil {
					mlog.Log.Warnf("lightbringer: stop on fallback: %v", stopErr)
				}
				if len(rpcEndpoints) == 0 {
					klog.Fatalf("lightbringer not ready and no RPC endpoints configured for fallback (set network.rpc)")
				}
			}
		}
	} else if useLightbringer {
		// block.source=lightbringer but lightbringer.enabled=false — standalone Lightbringer mode
		mlog.Log.Infof("block.source=lightbringer with external Lightbringer at %s", lightbringerEndpoint)
	} else if useTurbine {
		mlog.Log.Infof("block.source=turbine with native turbine receiver on %s", turbineBindAddr)
		if turbineGossipEntrypoint != "" {
			mlog.Log.Infof("native turbine gossip enabled with entrypoint %s", turbineGossipEntrypoint)
		} else {
			mlog.Log.Warnf("native turbine gossip entrypoint is empty; Mithril will receive turbine packets only if shreds are sent directly to %s", turbineBindAddr)
		}
	}

	dbgOpts, err := replay.NewDebugOptions(debugTxs, debugAcctWrites, debugDumpEpochVotingRewardDiff)
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

	// Fold recovery runs once on existing-DB startup, BEFORE
	// ValidateAgainstBankhashDB, because it restores the index tail and the
	// NoSync bankhash rows from the manifests — validating first could falsely
	// condemn a healthy store whose R bankhash row was dropped by a hard kill.
	// The result is memoized and reused for the state-watermark reconcile below.
	var foldRecovery accountsdb.RecoveryResult
	foldRecovered := false
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
		mlog.Log.Infof("Using full snapshot: %s", snapshotArchivePath)

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
		snapshotEpoch := snapshotEpochForState(manifest)
		mithrilState = state.NewReadyState(manifest.Bank.Slot, snapshotEpoch, "", "", 0, 0)
		// Populate manifest seed data so replay doesn't need manifest at runtime
		snapshot.PopulateManifestSeed(mithrilState, manifest)
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
		refreshManifestSeedFromManifest(accountsPath, mithrilState, manifest)
		// Restore the durable fold frontier (index tail + NoSync bankhash rows)
		// from the manifests before the integrity check reads those rows.
		foldRecovery = mustRecoverFoldState(accountsDb)
		foldRecovered = true
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
		snapshotEpoch := snapshotEpochForState(manifest)
		mithrilState = state.NewReadyState(manifest.Bank.Slot, snapshotEpoch, "", "", 0, 0)
		// Populate manifest seed data so replay doesn't need manifest at runtime
		snapshot.PopulateManifestSeed(mithrilState, manifest)
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
		snapshotEpoch := snapshotEpochForState(manifest)
		mithrilState = state.NewReadyState(manifest.Bank.Slot, snapshotEpoch, "", "", 0, 0)
		// Populate manifest seed data so replay doesn't need manifest at runtime
		snapshot.PopulateManifestSeed(mithrilState, manifest)
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
					snapshotEpoch := snapshotEpochForState(manifest)
					mithrilState = state.NewReadyState(manifest.Bank.Slot, snapshotEpoch, "", "", 0, 0)
					// Populate manifest seed data so replay doesn't need manifest at runtime
					snapshot.PopulateManifestSeed(mithrilState, manifest)
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
			refreshManifestSeedFromManifest(accountsPath, mithrilState, manifest)

			// Restore the durable fold frontier (index tail + NoSync bankhash
			// rows) from the manifests BEFORE the integrity check reads those
			// rows — otherwise a hard kill that dropped a NoSync bankhash row
			// would falsely condemn a healthy store and force a re-bootstrap.
			foldRecovery = mustRecoverFoldState(accountsDb)
			foldRecovered = true

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
			snapshotEpoch := snapshotEpochForState(manifest)
			mithrilState = state.NewReadyStateWithOpts(state.NewReadyStateOpts{
				SnapshotSlot:  manifest.Bank.Slot,
				SnapshotEpoch: snapshotEpoch,
				BuildMode:     bootstrapMode,
				Cluster:       cluster,
				GenesisHash:   fetchGenesisHash(ctx),
				WriterVersion: getVersion(),
				WriterCommit:  getCommit(),
			})
			// Populate manifest seed data so replay doesn't need manifest at runtime
			snapshot.PopulateManifestSeed(mithrilState, manifest)
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
		} else if mithrilState.LastRootedContext != nil {
			// Rooted-durable resume: durable position is the last rooted slot; the in-RAM
			// slots above it were lost. Resume from the slot after the last rooted slot
			// (GetResumeSlot returns it when LastRootedSlot > 0).
			startSlot = int64(mithrilState.GetResumeSlot())
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
	if mithrilState != nil && mithrilState.LastRootedContext != nil {
		// Rooted-durable resume: build from the context as of the last rooted slot
		// (durable), not the Last* fields at the replayed tip (lost in RAM on restart).
		rs, err := replay.ResumeStateFromRootedContext(mithrilState.LastRootedContext, mithrilState.ComputedEpochStakes)
		if err != nil {
			mlog.Log.Errorf("failed to build rooted-durable resume state: %v; will start fresh from snapshot", err)
			mithrilState = nil
		} else {
			resumeState = rs
			mlog.Log.Infof("rooted-durable resume: continuing from rooted slot R=%d (durable checkpoint)", mithrilState.LastRootedContext.Slot)
		}
	} else if mithrilState != nil && mithrilState.HasResumeData() {
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
					ParentBlockHeight:        mithrilState.LastBlockHeight,
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
					recentBlockhashes := replay.DecodeRecentBlockhashes(mithrilState.LastRecentBlockhashes)
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
					slotHashes := replay.DecodeSlotHashes(mithrilState.LastSlotHashes)
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
		snapshotEpoch := snapshotEpochForState(manifest)
		mithrilState = state.NewReadyState(manifest.Bank.Slot, snapshotEpoch, "", "", 0, 0)
		// Populate manifest seed data so replay doesn't need manifest at runtime
		snapshot.PopulateManifestSeed(mithrilState, manifest)
		if err := mithrilState.Save(accountsPath); err != nil {
			mlog.Log.Errorf("failed to save state file: %v", err)
		}
		// Record bootstrap in history
		state.RecordBootstrap(accountsPath, manifest.Bank.Slot, "", replay.CurrentRunID, getVersion(), getCommit(), getBranch())
	}

	// Support finite replay: --end-slot or --num-slots
	if endSlot != -1 && numReplaySlots != 0 {
		klog.Fatalf("specify at most one of --end-slot and --num-slots")
	}
	liveEndSlot := uint64(math.MaxUint64)
	if endSlot != -1 {
		liveEndSlot = uint64(endSlot)
	} else if numReplaySlots != 0 {
		liveEndSlot = uint64(startSlot + numReplaySlots)
	}
	if liveEndSlot != uint64(math.MaxUint64) {
		mlog.Log.Infof("finite replay: startSlot=%d endSlot=%d", startSlot, liveEndSlot)
	}
	accountsDb.InitCaches()

	// Alpenglow-only build: rooted-durable is the ONLY storage mode. Durable
	// state holds finalized slots exclusively; replayed slots buffer in RAM and
	// fold to disk in batches (storage.fold_batch_slots) once rooted.
	if config.IsSet("storage.durable_commit") {
		klog.Fatalf("storage.durable_commit was removed: batch folds replaced the per-slot redo-log commit path — delete the key (rooted-durable batching is always on)")
	}
	if config.IsSet("storage.rooted_durable") && !config.GetBool("storage.rooted_durable") {
		klog.Fatalf("storage.rooted_durable=false is not supported by this build (durable state is rooted-only by design) — delete the key")
	}
	accountsDb.RootedDurable = true
	if config.IsSet("storage.fork_aware") {
		mlog.Log.Warnf("storage.fork_aware was removed: the working-set suffix engine handles fork switches via unwind+re-execute — delete the key")
	}
	// [verifier]: the trailing execution verifier (dual-watermark second leg).
	vcfg := replay.TrailingVerifierDefaults()
	if config.IsSet("verifier.enabled") {
		vcfg.Enabled = config.GetBool("verifier.enabled")
	}
	if config.IsSet("verifier.required") {
		vcfg.Required = config.GetBool("verifier.required")
	}
	if v := config.GetInt("verifier.lag_slots"); v > 0 {
		vcfg.LagSlots = uint64(v)
	}
	if v := config.GetInt("verifier.max_rps"); v > 0 {
		vcfg.MaxRPS = v
	}
	replay.TrailingVerifierCfg = vcfg

	if foldBatch := config.GetInt("storage.fold_batch_slots"); foldBatch > 0 {
		replay.FoldBatchSlots = min(max(foldBatch, 32), 512)
		if replay.FoldBatchSlots != foldBatch {
			mlog.Log.Warnf("storage.fold_batch_slots=%d clamped to %d (allowed range 32..512)", foldBatch, replay.FoldBatchSlots)
		}
	}

	// [storage] rewind horizon + compaction. The horizon bounds how far back
	// RewindToBatchBoundary can restore durable state AND pins the files the
	// compactor must not reclaim (undo-pointer targets stay alive inside it).
	// Per-cycle work is deliberately small: CompactOnce holds the store's fold
	// lock for a cycle, so large scan/move budgets could stall fold/promotion/
	// rewind — keep validator-safe defaults, overridable once soaked.
	compactCfg := accountsdb.CompactionConfig{
		RewindHorizonBatches: 64,
		MaxMoveBytesPerCycle: 64 << 20,  // 64 MiB moved per cycle
		MaxScanBytesPerCycle: 256 << 20, // 256 MiB scanned per cycle
	}
	if v := config.GetInt("storage.rewind_horizon_batches"); v > 0 {
		compactCfg.RewindHorizonBatches = uint64(v)
	}
	if v := config.GetFloat64("storage.compact.min_dead_fraction"); v > 0 {
		compactCfg.MinDeadFraction = v
	}
	if v := config.GetInt("storage.compact.max_move_mb"); v > 0 {
		compactCfg.MaxMoveBytesPerCycle = int64(v) << 20
	}
	if v := config.GetInt("storage.compact.max_scan_mb"); v > 0 {
		compactCfg.MaxScanBytesPerCycle = int64(v) << 20
	}
	// Background compaction is OFF by default until soaked (it holds the store's
	// fold lock during each cycle); operators opt in via storage.compact.enabled.
	compactEnabled := false
	if config.IsSet("storage.compact.enabled") {
		compactEnabled = config.GetBool("storage.compact.enabled")
	}

	// Crash recovery reconcile: the durable fold frontier (recovered above from
	// the store's manifests + index meta — the state file is written only on
	// graceful shutdown and is stale after a hard kill) is reconciled against
	// the in-memory state watermark. Fresh-build paths fall through to the
	// recovery call here, which is a no-op on a store with no manifests.
	if mithrilState != nil {
		if !foldRecovered {
			foldRecovery = mustRecoverFoldState(accountsDb)
			foldRecovered = true
		}
		rec := foldRecovery
		switch {
		case rec.RewindInProgress:
			// An interrupted --rewind-to-slot left parked ".rewound" manifests.
			// Complete it uniformly — regardless of whether the crash landed before
			// or after the atomic meta rollback — by rewinding to the highest
			// retained boundary below the parked suffix, then adopt that boundary.
			completeInterruptedRewind(accountsDb, mithrilState)
		case rec.DurableThrough > mithrilState.LastRootedSlot:
			// Store is ahead of the (stale) state file: adopt the manifest-carried
			// watermark + resume context.
			var ctx state.ResumeContext
			if len(rec.ResumeCtx) == 0 {
				klog.Fatalf("store is durably folded through slot %d but state file says %d and the fold manifest carries no resume context; re-bootstrap with --bootstrap snapshot",
					rec.DurableThrough, mithrilState.LastRootedSlot)
			}
			if jerr := json.Unmarshal(rec.ResumeCtx, &ctx); jerr != nil {
				klog.Fatalf("fold manifest resume context for slot %d is unreadable: %v; re-bootstrap with --bootstrap snapshot", rec.DurableThrough, jerr)
			}
			mlog.Log.Infof("fold recovery advanced rooted watermark R from %d to %d (manifest-carried context)", mithrilState.LastRootedSlot, rec.DurableThrough)
			mithrilState.LastRootedSlot = rec.DurableThrough
			mithrilState.LastRootedBankhash = ctx.Bankhash
			mithrilState.LastRootedContext = &ctx
			if mithrilState.LastSlot < rec.DurableThrough {
				mithrilState.LastSlot = rec.DurableThrough
			}
		case rec.DurableThrough < mithrilState.LastRootedSlot:
			// State file claims MORE durable progress than the store holds: the
			// disk lost committed writes (or the wrong data dir is mounted).
			klog.Fatalf("state file shows last_rooted_slot=%d but the store is only durably folded through %d — disk lost committed state; re-bootstrap with --bootstrap snapshot",
				mithrilState.LastRootedSlot, rec.DurableThrough)
		}
	}

	// --rewind-to-slot: operator-directed restore of durable state to a retained
	// fold batch boundary (e.g. a divergence whose root cause predates the slot
	// the verifier halted at). Runs after fold recovery so the boundary set is
	// authoritative; replay then re-executes forward from the boundary.
	if rewindToSlot > 0 {
		if mithrilState == nil {
			klog.Fatalf("--rewind-to-slot=%d: no state file to reconcile (fresh bootstrap has nothing to rewind)", rewindToSlot)
		}
		res, rerr := accountsDb.RewindToBatchBoundary(uint64(rewindToSlot))
		if rerr != nil {
			points, _ := accountsDb.ListRewindPoints()
			klog.Fatalf("--rewind-to-slot=%d failed: %v\navailable fold boundaries: %s", rewindToSlot, rerr, formatRewindPoints(points))
		}
		if err := adoptRewindResult(mithrilState, res); err != nil {
			klog.Fatalf("--rewind-to-slot=%d: %v; re-bootstrap with --bootstrap snapshot", rewindToSlot, err)
		}
		mlog.Log.Warnf("rewound durable state to fold boundary at slot %d (%d batches / %d keys undone); replay resumes from slot %d",
			res.NewThrough, res.UndoneBatches, res.UndoneKeys, mithrilState.GetResumeSlot())
	}

	// Rooted-durable resume needs a context at the last rooted slot. The fold
	// manifest is the primary source (handled above); the state file is the
	// fallback for stores from before the first fold.
	if mithrilState != nil && mithrilState.LastSlot > 0 && mithrilState.LastRootedSlot > 0 {
		if mithrilState.LastRootedContext == nil || mithrilState.LastRootedContext.Slot != mithrilState.LastRootedSlot {
			ctxSlot := uint64(0)
			if mithrilState.LastRootedContext != nil {
				ctxSlot = mithrilState.LastRootedContext.Slot
			}
			klog.Fatalf("rooted-durable resume needs a context-at-R matching LastRootedSlot=%d (have context-slot=%d); re-bootstrap with --bootstrap snapshot",
				mithrilState.LastRootedSlot, ctxSlot)
		}
	}

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
		rpcServer = rpcserver.NewRpcServer(accountsDb, uint16(rpcPort), epochScheduleFromState(mithrilState))
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
	// Alpenglow-only: the observer engine is the only consensus engine.
	consensusEngine, err := consensusengine.NewEngine(consensusengine.Config{
		AlpenglowObserverBindAddr: alpenglowObserverBindAddr,
		AlpenglowMaxMessageBytes:  alpenglowMaxMessageBytes,
		AlpenglowBLSDST:           alpenglowBLSDST,
	})
	if err != nil {
		klog.Fatalf("unable to create consensus engine: %v", err)
	}
	if err := consensusEngine.Start(ctx); err != nil {
		klog.Fatalf("unable to start consensus engine %q: %v", consensusEngine.Name(), err)
	}
	defer func() {
		if err := consensusEngine.Close(); err != nil {
			mlog.Log.Warnf("consensus engine close failed: %v", err)
		}
	}()

	consensusOpts := &replay.ConsensusOpts{
		Engine: consensusEngine,
	}

	var slotCtxSetter replay.SlotCtxSetter
	if rpcServer != nil {
		slotCtxSetter = rpcServer
	}

	// Background compaction: folds never overwrite, so dead bytes accumulate in
	// out-of-horizon segments and bootstrap appendvecs. Each cycle is bounded
	// (move + scan budgets) and serialized against folds via the store's
	// internal lock; the rewind horizon pins are what it must never touch.
	if compactEnabled {
		go func() {
			ticker := time.NewTicker(10 * time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if _, cerr := accountsDb.CompactOnce(compactCfg); cerr != nil {
						mlog.Log.Warnf("compaction cycle failed: %v", cerr)
					}
				}
			}
		}()
	}

	turbineAlpenglowAddr := alpenglowAddrForGossip(alpenglowObserverBindAddr)
	result := runReplayWithRecovery(ctx, accountsDb, accountsPath, manifest, resumeState, uint64(startSlot), liveEndSlot, rpcEndpoints, lightbringerEndpoint, turbineBindAddr, turbineGossipEntrypoint, turbineGossipBindAddr, turbineAdvertisedIP, uint16(turbineShredVersion), turbineAlpenglowAddr, validatorIdentity, blockstorePath, int(txParallelism), true, useLightbringer, useTurbine, dbgOpts, metricsWriter, slotCtxSetter, mithrilState, blockFetchOpts, consensusOpts, replayStartTime)

	if result.Error != nil {
		if result.LastPersistedSlot == 0 {
			mlog.Log.Errorf("Replay stopped before persisting the first post-start slot: %v", result.Error)
		} else {
			mlog.Log.Errorf("Replay stopped with error after persisting slot %d: %v", result.LastPersistedSlot, result.Error)
		}
	}

	// Update state file with last persisted slot and shutdown context
	// Skip if already written during cancellation (eliminates timing window)
	if result.LastPersistedSlot > 0 && mithrilState != nil && !result.StateWrittenOnCancel {
		var shutdownCtx *state.ShutdownContext
		if result.LastAcctsLtHash != nil {
			// Calculate epoch for the last persisted slot
			lastEpoch := epochForStateSlot(mithrilState, result.LastPersistedSlot)
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
				BlockHeight:    result.LastBlockHeight,

				// LtHash and fee state
				AcctsLtHash:          base64.StdEncoding.EncodeToString(result.LastAcctsLtHash.Hash()),
				LamportsPerSignature: result.LastLamportsPerSignature,
				PrevLamportsPerSig:   result.LastPrevLamportsPerSig,
				NumSignatures:        result.LastNumSignatures,

				// Blockhash context
				RecentBlockhashes: replay.EncodeRecentBlockhashes(result.LastRecentBlockhashes),
				EvictedBlockhash:  base58.Encode(result.LastEvictedBlockhash[:]),
				LastBlockhash:     base58.Encode(result.LastBlockhash[:]),

				// SlotHashes context
				SlotHashes: replay.EncodeSlotHashes(result.LastSlotHashes),

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
		epoch := epochForStateSlot(mithrilState, result.LastPersistedSlot)
		snapshotEpoch := epochForStateSlot(mithrilState, snapshotBaseSlot)
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
	switch {
	case blockSource == "lightbringer" && lightbringerEnabled:
		fmt.Printf(" %s(managed sidecar)%s\n", dim, reset)
	case blockSource == "lightbringer":
		fmt.Printf(" %s(external)%s\n", dim, reset)
	case blockSource == "turbine":
		fmt.Printf(" %s(native)%s\n", dim, reset)
	default:
		fmt.Println()
	}

	// RPC endpoints - show auxiliary (network.rpc) endpoints
	if len(rpcEndpoints) > 0 {
		fmt.Printf("  RPC:          %s%s%s (primary)\n", gold, rpcEndpoints[0], reset)
		for _, ep := range rpcEndpoints[1:] {
			fmt.Printf("                %s%s%s (fallback)\n", gold, ep, reset)
		}
	}
	if blockSource == "lightbringer" && lightbringerEndpoint != "" {
		fmt.Printf("  Lightbringer: %s%s%s\n", gold, lightbringerEndpoint, reset)
	}
	if blockSource == "turbine" && turbineBindAddr != "" {
		fmt.Printf("  Turbine UDP:  %s%s%s\n", gold, turbineBindAddr, reset)
	}
	if blockSource == "turbine" && turbineGossipEntrypoint != "" {
		fmt.Printf("  Gossip:       %s%s%s\n", gold, turbineGossipEntrypoint, reset)
	}
	if blockSource == "turbine" && turbineGossipBindAddr != "" {
		fmt.Printf("  Gossip UDP:   %s%s%s\n", gold, turbineGossipBindAddr, reset)
	}
	if blockSource == "turbine" && turbineAdvertisedIP != "" {
		fmt.Printf("  Advertised:   %s%s%s\n", gold, turbineAdvertisedIP, reset)
	}
	if validatorIdentityKeypair != "" {
		fmt.Printf("  Identity key: %s%s%s\n", gold, validatorIdentityKeypair, reset)
	}
	consensusLabel := "verifying (non-voting)"
	if consensusMode == "validator" {
		consensusLabel = "validator (voting engine not yet active)"
	}
	fmt.Printf("  Consensus:    %s%s%s\n", gold, consensusLabel, reset)
	if alpenglowObserverBindAddr != "" {
		fmt.Printf("  Votor QUIC:   %s%s%s\n", gold, alpenglowObserverBindAddr, reset)
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

// detectExistingSnapshots finds snapshot files in the given directory.
// Skips .partial files (incomplete downloads from crashed runs).
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

		// Skip partial downloads (incomplete files from crashed runs)
		if strings.HasSuffix(name, ".partial") {
			continue
		}

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

// mustRecoverFoldState derives the durable fold frontier from the store
// (manifests + index meta), restoring the index tail and the NoSync bankhash
// rows, and fatals on error. Safe on a freshly built store (no manifests ->
// empty result). Callers run it before ValidateAgainstBankhashDB so the
// integrity check observes the manifest-restored bankhash rows.
func mustRecoverFoldState(accountsDb *accountsdb.AccountsDb) accountsdb.RecoveryResult {
	rec, rerr := accountsDb.RecoverFoldState()
	if rerr != nil {
		klog.Fatalf("fold recovery failed: %v", rerr)
	}
	if len(rec.ReplayedBatches) > 0 {
		mlog.Log.Infof("fold recovery completed %d decided batch(es) from manifests: %v", len(rec.ReplayedBatches), rec.ReplayedBatches)
	}
	if len(rec.OrphansRemoved) > 0 {
		mlog.Log.Infof("fold recovery removed %d undecided orphan file(s)", len(rec.OrphansRemoved))
	}
	return rec
}

// formatRewindPoints renders the retained fold boundaries for operator messages.
func formatRewindPoints(points []accountsdb.RewindPoint) string {
	if len(points) == 0 {
		return "(none retained — horizon empty or store never folded)"
	}
	slots := make([]string, 0, len(points))
	for _, p := range points {
		slots = append(slots, strconv.FormatUint(p.ThroughSlot, 10))
	}
	return strings.Join(slots, ", ")
}

// adoptRewindResult reconciles the in-memory state with a completed store
// rewind: the manifest-carried context at the boundary becomes the rooted
// checkpoint and everything above it is forgotten (replay re-executes it).
func adoptRewindResult(s *state.MithrilState, res accountsdb.RewindResult) error {
	if len(res.ResumeCtx) == 0 {
		if res.NewThrough == s.LastRootedSlot && s.LastRootedContext != nil && s.LastRootedContext.Slot == res.NewThrough {
			return nil // no-op rewind to the current boundary; state already consistent
		}
		return fmt.Errorf("rewound to slot %d but its fold manifest carries no resume context", res.NewThrough)
	}
	var rctx state.ResumeContext
	if err := json.Unmarshal(res.ResumeCtx, &rctx); err != nil {
		return fmt.Errorf("resume context at rewound boundary %d is unreadable: %w", res.NewThrough, err)
	}
	if rctx.Slot != res.NewThrough {
		return fmt.Errorf("resume context at rewound boundary %d names slot %d", res.NewThrough, rctx.Slot)
	}
	s.LastRootedSlot = res.NewThrough
	s.LastRootedBankhash = rctx.Bankhash
	s.LastRootedContext = &rctx
	if s.LastSlot > res.NewThrough {
		s.LastSlot = res.NewThrough
	}
	return nil
}

// completeInterruptedRewind finishes a rewind that crashed mid-flight (parked
// ".rewound" manifests remain). It rewinds to the highest retained boundary
// below the parked suffix — RewindToBatchBoundary is idempotent, so this works
// whether the crash landed before the meta rollback (does the full rewind now)
// or after it (a no-op that just finalizes the leftovers) — and adopts that
// boundary as the rooted checkpoint. Fatals only when nothing is left to finish
// the rewind with.
func completeInterruptedRewind(accountsDb *accountsdb.AccountsDb, mithrilState *state.MithrilState) {
	points, err := accountsDb.ListRewindPoints()
	if err != nil || len(points) == 0 {
		klog.Fatalf("interrupted rewind detected but no retained fold boundary remains to complete it; re-bootstrap with --bootstrap snapshot")
	}
	// The rewind parks every batch above its target, so the highest still-present
	// (non-parked) boundary IS the target.
	target := points[len(points)-1].ThroughSlot
	oldRooted := mithrilState.LastRootedSlot
	res, rerr := accountsDb.RewindToBatchBoundary(target)
	if rerr != nil {
		klog.Fatalf("could not complete interrupted rewind to slot %d: %v; re-run --rewind-to-slot=%d", target, rerr, target)
	}
	if aerr := adoptRewindResult(mithrilState, res); aerr != nil {
		klog.Fatalf("interrupted rewind at slot %d: %v; re-run --rewind-to-slot=%d", res.NewThrough, aerr, target)
	}
	mlog.Log.Warnf("completed interrupted rewind: durable state at fold boundary %d (state file said %d)", res.NewThrough, oldRooted)
}

// rewindStoreBelowDivergence rewinds durable state to the newest retained fold
// boundary strictly below divSlot and adopts that boundary's context as the
// rooted checkpoint. Returns false (caller halts) when no boundary below the
// divergence is retained or the rewind/reconcile fails.
func rewindStoreBelowDivergence(accountsDb *accountsdb.AccountsDb, s *state.MithrilState, divSlot uint64) bool {
	points, err := accountsDb.ListRewindPoints()
	if err != nil {
		mlog.Log.Errorf("fork switch: cannot list rewind boundaries: %v; halting", err)
		return false
	}
	target, found := uint64(0), false
	for _, p := range points { // ascending; keep the newest boundary below divSlot
		if p.ThroughSlot < divSlot {
			target, found = p.ThroughSlot, true
		}
	}
	if !found {
		mlog.Log.Errorf("fork switch: divergence at slot %d is at/below the durable watermark %d and no fold boundary below it is retained (rewind horizon exceeded) — re-bootstrap with --bootstrap snapshot; halting",
			divSlot, s.LastRootedSlot)
		return false
	}
	oldRooted := s.LastRootedSlot
	res, err := accountsDb.RewindToBatchBoundary(target)
	if err != nil {
		mlog.Log.Errorf("fork switch: rewind to boundary %d failed: %v; halting", target, err)
		return false
	}
	if err := adoptRewindResult(s, res); err != nil {
		mlog.Log.Errorf("fork switch: %v; halting", err)
		return false
	}
	mlog.Log.Warnf("fork switch: divergence at slot %d was already folded (R=%d); rewound durable state to boundary %d (%d batches / %d keys undone)",
		divSlot, oldRooted, res.NewThrough, res.UndoneBatches, res.UndoneKeys)
	return true
}

// maxForkSwitchRetries bounds fork-aware dump-then-repair re-replays per run.
const maxForkSwitchRetries = 3

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
	lightbringerEndpoint string,
	turbineBindAddr string,
	turbineGossipEntrypoint string,
	turbineGossipBindAddr string,
	turbineAdvertisedIP string,
	turbineShredVersion uint16,
	turbineAlpenglowAddr string,
	turbineIdentity ed25519.PrivateKey,
	blockDir string,
	txParallelism int,
	isLive bool,
	useLightbringer bool,
	useTurbine bool,
	dbgOpts *replay.DebugOptions,
	metricsWriter io.Writer,
	rpcServer replay.SlotCtxSetter,
	mithrilState *state.MithrilState,
	blockFetchOpts *replay.BlockFetchOpts,
	consensusOpts *replay.ConsensusOpts,
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
		lastEpoch := epochForStateSlot(mithrilState, r.LastPersistedSlot)

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
				BlockHeight:    r.LastBlockHeight,

				// LtHash and fee state
				AcctsLtHash:          base64.StdEncoding.EncodeToString(r.LastAcctsLtHash.Hash()),
				LamportsPerSignature: r.LastLamportsPerSignature,
				PrevLamportsPerSig:   r.LastPrevLamportsPerSig,
				NumSignatures:        r.LastNumSignatures,

				// Blockhash context
				RecentBlockhashes: replay.EncodeRecentBlockhashes(r.LastRecentBlockhashes),
				EvictedBlockhash:  base58.Encode(r.LastEvictedBlockhash[:]),
				LastBlockhash:     base58.Encode(r.LastBlockhash[:]),

				// SlotHashes context
				SlotHashes: replay.EncodeSlotHashes(r.LastSlotHashes),

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

	result = replay.ReplayBlocks(ctx, accountsDb, accountsDbPath, mithrilState, resumeState, startSlot, endSlot, rpcEndpoints, lightbringerEndpoint, turbineBindAddr, turbineGossipEntrypoint, turbineGossipBindAddr, turbineAdvertisedIP, turbineShredVersion, turbineAlpenglowAddr, turbineIdentity, blockDir, txParallelism, isLive, useLightbringer, useTurbine, dbgOpts, metricsWriter, rpcServer, blockFetchOpts, consensusOpts, onCancelWriteState)

	// Fork-aware dump-then-repair: on a confirmed divergence (bankhash vs confirmed
	// leaf) or an Alpenglow finality mismatch (executed block-id vs certificate), the
	// RAM tail (run-local) is already gone; re-replay the confirmed chain from the
	// durable rooted checkpoint — repair then fetches the certified version. Any
	// REPEATED divergence = deterministic replay divergence -> fail closed. The retry
	// budget resets when the rooted watermark advances (progress).
	seenDiv := make(map[string]bool)
	attempt := 0
	var prevRooted uint64
	if mithrilState != nil {
		prevRooted = mithrilState.LastRootedSlot
	}
	for {
		if result == nil {
			break
		}
		var divSlot uint64
		var key string
		var certSwitch *replay.CertifiedSwitch
		var finMismatch *replay.AlpenglowFinalityMismatch
		switch {
		case errors.As(result.Error, &certSwitch):
			// Execute-on-receipt ran the wrong sibling (or a certified-skipped
			// slot); the certified version replays from the rooted checkpoint.
			divSlot = certSwitch.Slot
			key = fmt.Sprintf("sw:%d/%x/%x/%v", certSwitch.Slot, certSwitch.Executed, certSwitch.Certified, certSwitch.Skip)
		case errors.As(result.Error, &finMismatch):
			if finMismatch.Conflict {
				// Conflict-shaped (equivocation evidence, not a wrong local block):
				// re-replay cannot help — halt without burning the retry budget.
				mlog.Log.Errorf("fork switch: consensus conflict at slot %d is equivocation evidence — not retryable; halting", finMismatch.Slot)
			} else {
				divSlot = finMismatch.Slot
				key = fmt.Sprintf("ag:%d/%x/%x", finMismatch.Slot, finMismatch.Executed, finMismatch.Finalized)
			}
		}
		if key == "" { // neither divergence type: not a fork-switch error
			break
		}
		if seenDiv[key] {
			mlog.Log.Errorf("fork switch: repeated divergence at slot %d - deterministic replay divergence, halting", divSlot)
			break
		}
		seenDiv[key] = true
		if mithrilState == nil {
			mlog.Log.Errorf("fork switch: no state to re-replay from; halting")
			break
		}
		// Late-detected divergence: the contradicted slot is at/below the durable
		// watermark, so it is already folded into the store — re-replaying from
		// the rooted checkpoint would rebuild on corrupted ground. Rewind the
		// store to the newest retained fold boundary BELOW the divergence first
		// (this is exactly why durable state deliberately lags and undo pointers
		// are retained for a horizon). Beyond the horizon -> fail closed.
		if divSlot <= mithrilState.LastRootedSlot {
			if !rewindStoreBelowDivergence(accountsDb, mithrilState, divSlot) {
				break
			}
		}
		if mithrilState.LastRootedContext == nil {
			mlog.Log.Errorf("fork switch: no rooted checkpoint context to re-replay from; halting")
			break
		}
		if mithrilState.LastRootedSlot > prevRooted { // progress since last attempt -> fresh budget
			attempt = 0
			prevRooted = mithrilState.LastRootedSlot
		}
		attempt++
		if attempt > maxForkSwitchRetries {
			mlog.Log.Errorf("fork switch: retry budget exhausted without rooted progress; halting")
			break
		}
		retryStart := mithrilState.GetResumeSlot()
		// Fail closed if the divergent span crosses an epoch boundary: in-process
		// epoch-stakes/StakeHistory caches may hold failed-run state for the new epoch.
		if epochForStateSlot(mithrilState, retryStart) != epochForStateSlot(mithrilState, divSlot) {
			mlog.Log.Errorf("fork switch: divergent span %d..%d crosses an epoch boundary; halting (restart to recover)", retryStart, divSlot)
			break
		}
		// Prefer the failed attempt's epoch stakes: the in-memory state file copy
		// can lag (it refreshes at boundaries and on shutdown) and can be empty
		// on a fresh bootstrap. The serialized form is raw PersistedEpochStakes
		// JSON — pass it through verbatim (string(b), NOT base64: the consumer
		// json.Unmarshals it directly, matching the state-file convention).
		stakes := mithrilState.ComputedEpochStakes
		if len(result.ComputedEpochStakes) > 0 {
			stakes = make(map[uint64]string, len(result.ComputedEpochStakes))
			for e, b := range result.ComputedEpochStakes {
				stakes[e] = string(b)
			}
		}
		rs, err := replay.ResumeStateFromRootedContext(mithrilState.LastRootedContext, stakes)
		if err != nil {
			mlog.Log.Errorf("fork switch: cannot rebuild resume context: %v; halting", err)
			break
		}
		mlog.Log.Warnf("fork switch: confirmed divergence at slot %d; re-replaying the confirmed chain from rooted slot %d (attempt %d/%d)",
			divSlot, retryStart-1, attempt, maxForkSwitchRetries)
		result = replay.ReplayBlocks(ctx, accountsDb, accountsDbPath, mithrilState, rs, retryStart, endSlot, rpcEndpoints, lightbringerEndpoint, turbineBindAddr, turbineGossipEntrypoint, turbineGossipBindAddr, turbineAdvertisedIP, turbineShredVersion, turbineAlpenglowAddr, turbineIdentity, blockDir, txParallelism, isLive, useLightbringer, useTurbine, dbgOpts, metricsWriter, rpcServer, blockFetchOpts, consensusOpts, onCancelWriteState)
	}
	return result
}
