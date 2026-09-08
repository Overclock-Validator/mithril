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
	"unicode/utf8"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/blockprod/scheduler"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/config"
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/lightbringer"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/sigverify"
	"github.com/Overclock-Validator/mithril/pkg/snapshot"
	"github.com/Overclock-Validator/mithril/pkg/snapshotdl"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/Overclock-Validator/mithril/pkg/tpu"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
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

	bootstrapMode                   string // "auto", "snapshot", "new-snapshot", "new-incremental", or "accountsdb"
	snapshotArchivePath             string
	incrementalSnapshotFilename     string
	accountsPath                    string
	scratchDirectory                string
	rpcEndpoints                    []string
	cluster                         string // "alpenglow", "mainnet-beta", "testnet", or "devnet"
	legacyGenesisHash               string // explicit lineage for pre-binding AccountsDB/ledger artifacts
	blockSource                     string // "turbine", "rpc", or "lightbringer"
	lightbringerEndpoint            string
	repairCatchupMaxGapSlots        int    // Resume gaps up to this fill via turbine repair instead of RPC (0 = off)
	repairMaxRequestsPerSecond      int    // Repair request-rate ceiling override (0 = adaptive default)
	blockRPCFallback                bool   // Allow RPC block fetch when > repairCatchupMaxGapSlots behind (default false: shreds only)
	blockMaxRPS                     int    // Rate limit for block fetching
	blockMaxInflight                int    // Max concurrent block fetch workers
	blockTipPollIntervalMs          int    // Tip poll interval in milliseconds
	blockTipSafetyMargin            int    // Don't fetch within N slots of tip
	consensusModeFlag               string // raw --consensus-mode value (cobra binding)
	consensusMode                   string // resolved: "verifying" (default) or "validator"
	alpenglowObserverBindAddr       string
	alpenglowMaxMessageBytes        int64
	alpenglowBLSDST                 string
	validatorIdentityKeypair        string
	validatorVoteAccountKeypair     string
	validatorAuthorizedVoterKeypair string
	validatorWithdrawerKeypair      string
	validatorTPUQUICBind            string
	validatorAdvertisedIP           string
	validatorSigverifyWorkers       int

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

	// resolvedSigverifyBackend is what the backend selector actually chose. It
	// differs from the configured value under "auto", which is exactly when an
	// operator needs to be told, so it is reported at startup.
	resolvedSigverifyBackend string
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
			// Benign, common on this cluster: some snapshot producers leave
			// the bank epoch field zeroed; the schedule-derived value is
			// authoritative. File-only — it fires on every boot.
			mlog.Log.FileOnlyf("manifest bank epoch %d differs from manifest epoch schedule epoch %d at slot %d; using schedule-derived epoch",
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

func alpenglowModeForCluster(cluster string) (bool, error) {
	switch cluster {
	case "alpenglow":
		return true, nil
	case "mainnet-beta", "testnet", "devnet":
		return false, nil
	default:
		return false, fmt.Errorf("invalid network.cluster %q - must be 'alpenglow', 'mainnet-beta', 'testnet', or 'devnet'", cluster)
	}
}

func defaultBlockSourceForMode(alpenglow bool) string {
	if alpenglow {
		return "turbine"
	}
	return "rpc"
}

// txParallelismForMode gives full-validator replay a usable default without
// changing the classic verifying flow. Zero remains a meaningful, supported
// operator choice when it was explicitly supplied through CLI or config.
func txParallelismForMode(mode string, configured int64, explicitlySet bool, cpuCount int) int64 {
	if mode != "validator" || explicitlySet || configured != 0 {
		return configured
	}
	if cpuCount < 1 {
		cpuCount = 1
	}
	return int64(cpuCount * 2)
}

// A full validator validates the block it will vote on by executing it and
// comparing the resulting bank hash with the footer committed by the
// certificate's double-Merkle block id. Fetching finalized getBlock metadata
// afterwards is a verifying-node oracle, not part of validator operation.
func verifierConfigForConsensusMode(mode string, cfg replay.VerifierConfig) replay.VerifierConfig {
	if mode == "validator" {
		cfg.Enabled = false
		cfg.Required = false
		cfg.ValidatorFooterHash = true
	}
	return cfg
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

// catchupShredSpoolDir places the disposable verified-shred spool on the
// shredstore disk, away from AccountsDB I/O.
func catchupShredSpoolDir() string {
	if blockstorePath == "" {
		return ""
	}
	return filepath.Join(blockstorePath, "catchup-spool")
}

func shortHash(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}

// newReadyStateForBootstrap is the single construction path for a freshly
// built AccountsDB. A build without a confirmed genesis must remain invalid
// rather than producing another unbound "ready" marker.
func newReadyStateForBootstrap(snapshotSlot, snapshotEpoch uint64, buildMode, cluster, genesisHash string) (*state.MithrilState, error) {
	if strings.TrimSpace(cluster) == "" {
		return nil, fmt.Errorf("cannot mark AccountsDB ready without a cluster")
	}
	if strings.TrimSpace(genesisHash) == "" {
		return nil, fmt.Errorf("cannot mark AccountsDB ready without a confirmed RPC genesis hash")
	}
	if err := validateGenesisHashString("ready state", genesisHash); err != nil {
		return nil, err
	}
	return state.NewReadyStateWithOpts(state.NewReadyStateOpts{
		SnapshotSlot:  snapshotSlot,
		SnapshotEpoch: snapshotEpoch,
		BuildMode:     buildMode,
		Cluster:       cluster,
		GenesisHash:   genesisHash,
		WriterVersion: getVersion(),
		WriterCommit:  getCommit(),
	}), nil
}

func saveReadyStateForBootstrap(accountsPath string, manifest *snapshot.SnapshotManifest, buildMode, cluster, genesisHash string, buildStartedAt time.Time) (*state.MithrilState, error) {
	if manifest == nil || manifest.Bank == nil {
		return nil, fmt.Errorf("cannot mark AccountsDB ready without a snapshot manifest")
	}
	mithrilState, err := newReadyStateForBootstrap(
		manifest.Bank.Slot, snapshotEpochForState(manifest), buildMode, cluster, genesisHash,
	)
	if err != nil {
		return nil, err
	}
	mithrilState.BuildStartedAt = buildStartedAt
	snapshot.PopulateManifestSeed(mithrilState, manifest)
	if err := mithrilState.Save(accountsPath); err != nil {
		return nil, fmt.Errorf("save ready state: %w", err)
	}
	return mithrilState, nil
}

func validateAccountsGenesisForBootstrap(hasValidState bool, storedGenesis, assertedLegacyGenesis, currentGenesis, buildMode string) (priorGenesis string, mismatch bool, err error) {
	if err := validateGenesisHashString("current RPC", currentGenesis); err != nil {
		return "", false, err
	}
	if !hasValidState {
		return "", false, nil
	}
	priorGenesis = strings.TrimSpace(storedGenesis)
	if priorGenesis != "" {
		if err := validateGenesisHashString("stored AccountsDB", priorGenesis); err != nil {
			return priorGenesis, false, err
		}
	}
	if priorGenesis == "" {
		priorGenesis = strings.TrimSpace(assertedLegacyGenesis)
		if priorGenesis != "" {
			if err := validateGenesisHashString("legacy assertion", priorGenesis); err != nil {
				return priorGenesis, false, err
			}
		}
	}
	if priorGenesis == "" {
		if buildMode == "new-snapshot" {
			return "", false, nil
		}
		return "", false, fmt.Errorf("existing AccountsDB has no genesis binding; refusing to adopt the current RPC genesis implicitly")
	}
	if priorGenesis == currentGenesis {
		return priorGenesis, false, nil
	}
	if buildMode != "new-snapshot" {
		return priorGenesis, true, fmt.Errorf("genesis hash mismatch: existing AccountsDB belongs to %s, RPC returned %s", priorGenesis, currentGenesis)
	}
	return priorGenesis, true, nil
}

func validateGenesisHashString(label, hash string) error {
	decoded, err := base58.Decode(hash)
	if err != nil || len(decoded) != 32 {
		return fmt.Errorf("%s genesis hash must be base58 encoding of exactly 32 bytes", label)
	}
	return nil
}

// loadBootstrapState distinguishes an absent or deliberately invalid state
// marker from one that cannot be decoded or checked. Only new-snapshot is
// allowed to replace an unreadable state file; every other mode fails closed
// because it cannot prove the lineage of a local or explicitly supplied input.
func loadBootstrapState(accountsPath, buildMode string) (*state.MithrilState, error) {
	return loadBootstrapStateWith(
		accountsPath,
		buildMode,
		state.LoadState,
		state.CheckAndLoadValidState,
	)
}

func loadBootstrapStateWith(
	accountsPath, buildMode string,
	loadState func(string) (*state.MithrilState, error),
	checkState func(string) (*state.MithrilState, error),
) (*state.MithrilState, error) {
	// CheckAndLoadValidState intentionally classifies non-ready/corrupted states
	// as unusable, but older implementations also classified decode errors as
	// absence. LoadState first so malformed state cannot silently enter an auto
	// rebuild or legacy-detection path.
	if _, err := loadState(accountsPath); err != nil {
		if buildMode == "new-snapshot" {
			mlog.Log.Warnf("mode=new-snapshot: unreadable existing state will be replaced: %v", err)
			return nil, nil
		}
		return nil, fmt.Errorf("load AccountsDB state: %w", err)
	}

	validState, err := checkState(accountsPath)
	if err != nil {
		if buildMode == "new-snapshot" {
			mlog.Log.Warnf("mode=new-snapshot: invalid existing state will be replaced: %v", err)
			return nil, nil
		}
		return nil, fmt.Errorf("check AccountsDB state: %w", err)
	}
	return validState, nil
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

func loadValidatorPrivateKey(label, path string) (ed25519.PrivateKey, solana.PublicKey, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, solana.PublicKey{}, fmt.Errorf("%s keypair path is empty", label)
	}
	key, err := solana.PrivateKeyFromSolanaKeygenFile(path)
	if err != nil {
		return nil, solana.PublicKey{}, fmt.Errorf("load %s keypair %s: %w", label, path, err)
	}
	if len(key) != ed25519.PrivateKeySize {
		return nil, solana.PublicKey{}, fmt.Errorf("%s keypair %s has invalid private key size %d", label, path, len(key))
	}
	return ed25519.PrivateKey(append([]byte(nil), key...)), key.PublicKey(), nil
}

func loadValidatorPubkey(label, value string) (solana.PublicKey, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return solana.PublicKey{}, fmt.Errorf("%s is empty", label)
	}
	if pubkey, err := solana.PublicKeyFromBase58(value); err == nil {
		return pubkey, nil
	}
	key, err := solana.PrivateKeyFromSolanaKeygenFile(value)
	if err != nil {
		return solana.PublicKey{}, fmt.Errorf("load %s %s as a public key or keypair file: %w", label, value, err)
	}
	return key.PublicKey(), nil
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
	// ManifestEpochStakes is deliberately cleared after replay starts to release
	// the large JSON seed.  A later graceful state save persists that cleared
	// value, so schedule metadata alone does not mean the manifest seed is
	// complete on restart.  Rehydrate when the stakes payload is absent.
	if manifestEpochScheduleSeedMatches(s, manifest) &&
		s.SnapshotEpoch == snapshotEpoch && len(s.ManifestEpochStakes) > 0 {
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
	Run.Flags().StringVar(&bootstrapMode, "bootstrap", "auto", "Bootstrap mode: 'auto' (use AccountsDB if exists, else snapshot), 'accountsdb' (require existing), 'snapshot' (rebuild from snapshot), 'new-snapshot' (always download fresh), 'new-incremental' (reuse local full, fetch fresh incremental)")
	Run.Flags().StringVar(&snapshotArchivePath, "snapshot", "", "Path to specific full snapshot file (bypasses auto-discovery)")
	Run.Flags().StringVar(&incrementalSnapshotFilename, "incremental-snapshot", "", "Path to specific incremental snapshot file (bypasses auto-discovery)")
	Run.Flags().StringVar(&snapshotDlPath, "download-snapshot-path", "", "Directory for discovered/downloaded snapshots")

	// [ledger] section flags
	Run.Flags().StringVarP(&accountsPath, "accounts-path", "o", "", "Output path for writing AccountsDB data to")
	Run.Flags().StringVar(&blockstorePath, "ledger-path", "/tmp/blocks", "Path containing slot.json files")

	// [network] section flags
	Run.Flags().StringSliceVarP(&rpcEndpoints, "rpc", "r", []string{}, "URL(s) for RPC endpoint(s) - can specify multiple")
	Run.Flags().StringVar(&cluster, "cluster", "", "Solana cluster: 'alpenglow' (default), 'mainnet-beta', 'testnet', or 'devnet'")
	Run.Flags().StringVar(&legacyGenesisHash, "legacy-genesis-hash", "", "Genesis hash owning legacy unbound AccountsDB/ledger artifacts (one-time re-genesis safety acknowledgement)")

	// [rpc] section flags (Mithril's RPC server)
	Run.Flags().IntVar(&rpcPort, "rpc-port", 0, "RPC server port. Default off.")

	// [replay] section flags
	Run.Flags().Int64Var(&txParallelism, "txpar", 0, "Transaction execution workers (>0 enables topsort parallelism; explicit 0 is sequential; unset validator mode defaults to 2x CPU cores)")
	Run.Flags().Int64Var(&numReplaySlots, "num-slots", 0, "Number of slots to replay (0 = run continuously)")
	Run.Flags().Int64VarP(&endSlot, "end-slot", "e", -1, "Block at which to stop replaying, inclusive (-1 = run continuously)")

	// [consensus] section flags
	Run.Flags().StringVar(&consensusModeFlag, "consensus-mode", "verifying", "Node mode: 'verifying' (default; non-voting) or Alpenglow-only 'validator' (TPU, block production, and Votor voting active)")
	Run.Flags().StringVar(&alpenglowObserverBindAddr, "alpenglow-observer-bind-addr", "", "Alpenglow Votor QUIC listener address")
	Run.Flags().StringVar(&alpenglowBLSDST, "alpenglow-bls-dst", "", "BLS hash-to-curve DST override (must match cluster's solana-bls version; empty = default)")
	Run.Flags().Int64Var(&alpenglowMaxMessageBytes, "alpenglow-max-message-bytes", 0, "Maximum Alpenglow Votor QUIC datagram payload size (0 = default)")
	Run.Flags().StringVar(&validatorIdentityKeypair, "identity-keypair", "", "Validator identity keypair for native turbine gossip (Solana keygen JSON)")
	Run.Flags().StringVar(&validatorVoteAccountKeypair, "vote-account-keypair", "", "On-chain vote account public key or keypair path")
	Run.Flags().StringVar(&validatorAuthorizedVoterKeypair, "authorized-voter-keypair", "", "Authorized voter keypair used to derive the Alpenglow BLS signer (defaults to identity-keypair, matching Agave)")
	Run.Flags().StringVar(&validatorWithdrawerKeypair, "authorized-withdrawer-keypair", "", "Authorized withdrawer keypair path for validator diagnostics (Solana keygen JSON)")
	Run.Flags().StringVar(&validatorTPUQUICBind, "tpu-quic-bind-addr", "", "Validator TPU QUIC listen address (default 0.0.0.0:8004)")
	Run.Flags().StringVar(&validatorAdvertisedIP, "validator-advertised-ip", "", "Public IP advertised for validator TPU QUIC")
	Run.Flags().IntVar(&validatorSigverifyWorkers, "tpu-sigverify-workers", 0, "TPU signature verification workers (0 = GOMAXPROCS)")

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
	Run.Flags().StringVar(&sigverify.Cfg.Backend, "sigverify-backend", sigverify.Defaults().Backend,
		"ed25519 verification backend: auto|r51|generic|stdlib")
	Run.Flags().BoolVar(&sbpf.UsePool, "use-pool", true, "Disable to allocate fresh slices")
	Run.Flags().IntVar(&accountsdb.StoreAccountsWorkers, "store-accounts-workers", 128, "Number of workers to write account updates")
	Run.Flags().IntVar(&accountsdb.ProgramCacheMaxMB, "program-cache-max-mb", accountsdb.DefaultProgramCacheMaxMB, "Maximum approximate SBPF program cache size in MiB")
	Run.Flags().IntVar(&accountsdb.CommonAccountCacheMaxMB, "common-account-cache-max-mb", accountsdb.DefaultCommonAccountCacheMaxMB, "Approximate retained decoded account cache weight budget in MiB")
	Run.Flags().Int64Var(&rewindToSlot, "rewind-to-slot", 0, "Rewind durable account state to the fold batch boundary at this slot before replaying (must be a retained boundary; run once to list boundaries on mismatch)")

	// [tuning.pprof] section flags
	Run.Flags().Int64Var(&pprofPort, "pprof-port", -1, "Port to serve the loopback-only HTTP pprof endpoint (-1 disables it)")
	Run.Flags().StringVar(&cpuprofPath, "cpu-profile-path", "", "Filename to write CPU profile")

	// [debug] section flags
	Run.Flags().StringSliceVar(&debugTxs, "transaction-signatures", []string{}, "Pass tx signature strings to enable debug logging during that transaction's execution")
	Run.Flags().StringSliceVar(&debugAcctWrites, "account-writes", []string{}, "Pass account pubkeys to enable debug logging of transactions that modify the account")
	Run.Flags().BoolVar(&debugDumpEpochVotingRewardDiff, "dump-epoch-voting-reward-diff", false, "Write epoch-boundary reward artifacts, including a voting diff and a full dump of locally calculated rewards")

	// Top-level flags
	Run.Flags().StringVar(&scratchDirectory, "scratch-directory", "/tmp", "Path for downloads (e.g. snapshots) and other temp state")

	// [block] section flags
	Run.Flags().StringVar(&blockSource, "block-source", "", "Block source: 'turbine', 'rpc', or 'lightbringer' (default: turbine for Alpenglow, rpc otherwise)")
	Run.Flags().StringVar(&lightbringerEndpoint, "lightbringer-endpoint", "", "Address for Lightbringer endpoint (only used when block-source=lightbringer)")
	Run.Flags().StringVar(&turbineBindAddr, "turbine-bind-addr", "", "UDP address for native turbine shred receiver (only used when block-source=turbine)")
	Run.Flags().StringVar(&turbineGossipEntrypoint, "turbine-gossip-entrypoint", "", "Solana gossip entrypoint for native turbine tree joining")
	Run.Flags().StringVar(&turbineGossipBindAddr, "turbine-gossip-bind-addr", "", "UDP address for native turbine gossip traffic (only used when block-source=turbine)")
	Run.Flags().IntVar(&repairCatchupMaxGapSlots, "repair-catchup-max-gap-slots", 8192, "Fill resume gaps up to this many slots via turbine repair instead of RPC getBlock (0 = always RPC catchup)")
	Run.Flags().BoolVar(&blockRPCFallback, "rpc-fallback", false, "Allow RPC to fetch blocks when replay is more than repair-catchup-max-gap-slots behind (default false: shreds via turbine + repair are the only block path)")
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
	txParallelismExplicit := flagChanged("txpar") || config.IsSet("tuning.txpar") || config.IsSet("replay.txpar")
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

	// Cluster selects the protocol path as well as protecting against
	// mainnet/testnet AccountsDB mixups. Alpenglow uses certificate forkchoice
	// and rooted speculative replay; the established clusters retain the
	// classic verifying flow.
	cluster = getString("cluster", "network.cluster")
	if cluster == "" {
		cluster = "alpenglow"
		mlog.Log.Infof("network.cluster not set; defaulting to %q", cluster)
	}
	legacyGenesisHash = strings.TrimSpace(getString("legacy-genesis-hash", "network.legacy_genesis_hash"))
	if legacyGenesisHash != "" {
		if err := validateGenesisHashString("network.legacy_genesis_hash", legacyGenesisHash); err != nil {
			return err
		}
	}
	alpenglowMode, err := alpenglowModeForCluster(cluster)
	if err != nil {
		return err
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
		// Requirements are enforced below once the block/turbine settings are
		// resolved. Validator mode activates Votor voting, TPU ingress, and
		// scheduled block production on the shared verified consensus pipeline.
	default:
		return fmt.Errorf("unknown consensus.mode %q (valid: \"verifying\", \"validator\")", consensusMode)
	}
	resolvedTxParallelism := txParallelismForMode(consensusMode, txParallelism, txParallelismExplicit, runtime.NumCPU())
	if resolvedTxParallelism != txParallelism {
		txParallelism = resolvedTxParallelism
		mlog.Log.Infof("consensus.mode=validator: tuning.txpar was not set; enabling %d transaction execution workers (set --txpar 0 explicitly to retain sequential execution)", txParallelism)
	}
	alpenglowObserverBindAddr = getString("alpenglow-observer-bind-addr", "consensus.alpenglow_observer_bind_addr")
	alpenglowMaxMessageBytes = getInt64("alpenglow-max-message-bytes", "consensus.alpenglow_max_message_bytes")
	alpenglowBLSDST = getString("alpenglow-bls-dst", "consensus.alpenglow_bls_dst")
	validatorIdentityKeypair = getString("identity-keypair", "validator.identity_keypair")
	validatorVoteAccountKeypair = getString("vote-account-keypair", "validator.vote_account_keypair")
	validatorAuthorizedVoterKeypair = getString("authorized-voter-keypair", "validator.authorized_voter_keypair")
	validatorWithdrawerKeypair = getString("authorized-withdrawer-keypair", "validator.authorized_withdrawer_keypair")
	validatorTPUQUICBind = getString("tpu-quic-bind-addr", "validator.tpu_quic_bind_addr")
	if validatorTPUQUICBind == "" {
		validatorTPUQUICBind = "0.0.0.0:8004"
	}
	validatorAdvertisedIP = getString("validator-advertised-ip", "validator.advertised_ip")
	validatorSigverifyWorkers = getInt("tpu-sigverify-workers", "validator.tpu_sigverify_workers")

	// [block] section
	blockSource = getString("block-source", "block.source")
	blockSourceExplicit := blockSource != "" // operator chose (flag or config) vs defaulted
	if blockSource == "" {
		blockSource = defaultBlockSourceForMode(alpenglowMode)
	}
	// RPC is a catch-up/debug source on Alpenglow, not a live one: RPC blocks
	// carry no Alpenglow block ids or footer certificates, so certificate
	// gating cannot adjudicate their identity near tip, and durable folds only
	// advance if certificates arrive some other way (the Votor QUIC listener).
	if alpenglowMode && blockSource == "rpc" {
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
	repairCatchupMaxGapSlots = getInt("repair-catchup-max-gap-slots", "block.repair_catchup_max_gap_slots")
	if repairCatchupMaxGapSlots < 0 {
		repairCatchupMaxGapSlots = 0
	}
	repairMaxRequestsPerSecond = config.GetInt("block.repair_max_requests_per_second")
	if repairMaxRequestsPerSecond < 0 {
		repairMaxRequestsPerSecond = 0
	}
	blockRPCFallback = getBool("rpc-fallback", "block.rpc_fallback")

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

	// Validator mode: enforce the complete block-production and voting shape.
	if consensusMode == "validator" {
		var missing []string
		if !alpenglowMode {
			missing = append(missing, "network.cluster=alpenglow (classic clusters remain verifying-only)")
		}
		if validatorIdentityKeypair == "" {
			missing = append(missing, "validator.identity_keypair (signs gossip/turbine and authenticates Votor QUIC)")
		}
		if validatorVoteAccountKeypair == "" {
			missing = append(missing, "validator.vote_account_keypair (the on-chain vote account address)")
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
		if validatorAdvertisedIP == "" && turbineAdvertisedIP == "" {
			missing = append(missing, "validator.advertised_ip or turbine.advertised_ip (publish TPU QUIC)")
		}
		if len(missing) > 0 {
			return fmt.Errorf("consensus.mode=validator requires:\n  - %s", strings.Join(missing, "\n  - "))
		}
		// The authorized withdrawer is deliberately NOT required at runtime —
		// best practice keeps it offline.
		mlog.Log.Infof("consensus.mode=validator: Alpenglow TPU ingress, block production, and fail-closed Votor voting enabled")
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
	// Resolve and install the signature-verification backend here rather than
	// later: narya pins its backend on first use, and selecting it explicitly
	// doubles as a startup health check, so a machine that cannot run the
	// requested backend fails now instead of at the first block.
	sigverify.Cfg.Backend = getString("sigverify-backend", "tuning.sigverify_backend")
	resolved, err := sigverify.Configure(sigverify.Cfg)
	if err != nil {
		return fmt.Errorf("tuning.sigverify_backend: %w", err)
	}
	resolvedSigverifyBackend = resolved
	sbpf.UsePool = getBool("use-pool", "tuning.use_pool")
	accountsdb.StoreAccountsWorkers = getInt("store-accounts-workers", "tuning.store_accounts_workers")
	accountsdb.ProgramCacheMaxMB = getInt("program-cache-max-mb", "tuning.program_cache_max_mb")
	if accountsdb.ProgramCacheMaxMB <= 0 {
		return fmt.Errorf("tuning.program_cache_max_mb must be > 0")
	}
	accountsdb.CommonAccountCacheMaxMB = getInt("common-account-cache-max-mb", "tuning.common_account_cache_max_mb")
	if accountsdb.CommonAccountCacheMaxMB <= 0 {
		return fmt.Errorf("tuning.common_account_cache_max_mb must be > 0")
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
	alpenglowMode := cluster == "alpenglow"
	if pprofPort != -1 {
		if err := startPprofHandlers(int(pprofPort)); err != nil {
			klog.Errorf("pprof server unavailable: %v", err)
		}
	}
	ctx, cancelRun := context.WithCancel(c.Context())
	defer cancelRun()

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

	// Discover once, before constructing any turbine, consensus, or production
	// component. Gossip clients can discover a missing version internally, but
	// leaving the shared CLI value at zero makes Alpenglow BLS verification hash
	// a different vote payload than the cluster and makes every certificate fail.
	if blockSource == "turbine" && turbineShredVersion == 0 {
		discovered, err := resolveTurbineShredVersion(0, turbineGossipEntrypoint, gossip.QueryEntrypoint)
		if err != nil {
			klog.Fatalf("discover turbine shred version: %v", err)
		}
		turbineShredVersion = discovered
		mlog.Log.Infof("discovered turbine shred version %d from gossip entrypoint %s", turbineShredVersion, turbineGossipEntrypoint)
	}

	// Override bootstrap mode display when explicit snapshot paths are provided
	if snapshotArchivePath != "" {
		bootstrapMode = "explicit"
	}

	// Print consolidated startup info
	printStartupInfo("run")

	// Now start the metrics server (after banner so errors don't appear first)
	if err := statsd.StartMetricsServer(); err != nil {
		klog.Errorf("metrics server unavailable: %v", err)
	}

	// Chain lineage is a startup gate, not part of AccountsDB bootstrap. Resolve
	// it before Lightbringer can open ledger storage and before history records
	// describe any mutation. Fetch genesis exactly once and carry that value
	// through every fresh ready-state write below.
	if len(rpcEndpoints) == 0 {
		rpcEndpoints = []string{"https://api.mainnet-beta.solana.com"}
	}
	networkGenesisHash := fetchGenesisHash(ctx)
	if networkGenesisHash == "" {
		klog.Fatalf("FATAL: could not confirm the RPC genesis hash; refusing startup because AccountsDB, shred spool, and vote-history lineage cannot be validated")
	}

	mithrilState, err := loadBootstrapState(accountsPath, bootstrapMode)
	if err != nil {
		klog.Fatalf("FATAL: %v. Re-run with --bootstrap new-snapshot to replace the unreadable state and AccountsDB.", err)
	}
	hasValidState := mithrilState != nil

	// Read both bindings before mutating either location. The ledger marker
	// survives AccountsDB cleanup and therefore remains authoritative for the
	// signed vote history and catchup spool across snapshot rebuilds.
	ledgerBinding, err := state.LoadLedgerChainBinding(blockstorePath)
	if err != nil {
		klog.Fatalf("ledger chain binding is invalid: %v", err)
	}
	if ledgerBinding != nil && legacyGenesisHash != "" && legacyGenesisHash != ledgerBinding.GenesisHash {
		klog.Fatalf("FATAL: --legacy-genesis-hash %s conflicts with stored ledger binding %s; refusing to choose a lineage",
			legacyGenesisHash, ledgerBinding.GenesisHash)
	}
	storedAccountsGenesis := ""
	if hasValidState {
		storedAccountsGenesis = mithrilState.GenesisHash
	}
	accountsPriorGenesis, accountsGenesisMismatch, accountsGenesisErr := validateAccountsGenesisForBootstrap(
		hasValidState, storedAccountsGenesis, legacyGenesisHash, networkGenesisHash, bootstrapMode,
	)
	if accountsGenesisErr != nil {
		if accountsPriorGenesis == "" {
			klog.Fatalf("FATAL: %v. Re-run once with --legacy-genesis-hash <the genesis hash this AccountsDB belongs to>, or use --bootstrap new-snapshot to replace it.", accountsGenesisErr)
		}
		klog.Fatalf("FATAL: %v. Re-run with --bootstrap new-snapshot; old ledger artifacts will be quarantined, not deleted.", accountsGenesisErr)
	}

	transition, lineageErr := state.EnsureLedgerChainBinding(
		blockstorePath, cluster, networkGenesisHash, legacyGenesisHash,
	)
	if lineageErr != nil {
		if errors.Is(lineageErr, state.ErrUnboundLedgerArtifacts) {
			klog.Fatalf("FATAL: %v. Refusing to guess the lineage of signed vote history. Re-run once with --legacy-genesis-hash <the genesis hash these artifacts belong to>.", lineageErr)
		}
		klog.Fatalf("FATAL: reconcile ledger chain lineage: %v", lineageErr)
	}
	if transition.QuarantineDir != "" {
		mlog.Log.Warnf("confirmed re-genesis %s -> %s: quarantined %d ledger artifact(s) in %s",
			transition.PreviousGenesisHash, transition.CurrentGenesisHash, len(transition.MovedPaths), transition.QuarantineDir)
	}

	if hasValidState && mithrilState.GenesisHash == "" && accountsPriorGenesis == networkGenesisHash {
		mlog.Log.Infof("Binding legacy state file to cluster=%s genesis=%s...", cluster, shortHash(networkGenesisHash))
		mithrilState.SetClusterInfo(cluster, networkGenesisHash)
		if err := mithrilState.Save(accountsPath); err != nil {
			klog.Fatalf("persist legacy state genesis binding: %v", err)
		}
	}
	if accountsGenesisMismatch {
		// new-snapshot is the only mode admitted above. Make sure no later auto
		// path can accidentally resume the old AccountsDB.
		hasValidState = false
	}

	// Lightbringer sidecar management
	var lbManager *lightbringer.Manager
	useLightbringer := blockSource == "lightbringer"
	useTurbine := blockSource == "turbine"
	validatorIdentity, validatorIdentityPubkey, err := loadValidatorIdentityKeypair(validatorIdentityKeypair)
	if err != nil {
		klog.Fatalf("%v", err)
	}

	// Turbine prewarm: start collecting live shreds the moment a snapshot
	// manifest supplies stakes good enough to verify them — every second of
	// earlier collection is thousands of shreds repair never has to fetch
	// one per round trip (repair carries no coding shreds, so pre-join slots
	// get zero FEC leverage). The FULL manifest parses minutes before the
	// incremental in the with-incremental flow and its epoch stakes usually
	// already cover the live tip's leader schedule, so prewarm starts there;
	// the incremental hook then refreshes stakes and — if the full's
	// schedule horizon missed the live tip — rebuilds the schedule, which
	// the running receiver picks up per shred (self-healing, no restart).
	// Fail-open: any error just means no (or later) prewarm.
	var turbinePrewarm *blockstream.TurbinePrewarm
	if useTurbine && turbineBindAddr != "" && turbineGossipEntrypoint != "" {
		var prewarmMu sync.Mutex
		seedManifestStakes := func(m *snapshot.SnapshotManifest) bool {
			tmp := &state.MithrilState{}
			snapshot.PopulateManifestSeed(tmp, m)
			loaded := false
			for epoch, data := range tmp.ManifestEpochStakes {
				if _, err := global.DeserializeAndLoadEpochStakes([]byte(data)); err != nil {
					mlog.Log.FileOnlyf("turbine prewarm: loading manifest epoch %d stakes: %v", epoch, err)
					continue
				}
				loaded = true
			}
			return loaded
		}
		// ensureLeaderSchedule makes LeaderForSlot cover the manifest's
		// resume region, building the manifest epoch's schedule when it does
		// not already.
		ensureLeaderSchedule := func(m *snapshot.SnapshotManifest) bool {
			if _, ok := global.LeaderForSlot(m.Bank.Slot + 1); ok {
				return true
			}
			epochSchedule := m.Bank.EpochSchedule
			epoch := epochSchedule.GetEpoch(m.Bank.Slot)
			if _, err := replay.PrepareLeaderScheduleLocal(epoch, &epochSchedule, ""); err != nil {
				mlog.Log.Warnf("turbine prewarm: leader schedule for epoch %d: %v", epoch, err)
				return false
			}
			mlog.Log.Infof("leader schedule ready for epoch %d (shred verification live)", epoch)
			return true
		}
		startPrewarm := func(m *snapshot.SnapshotManifest, origin string) {
			epochSchedule := m.Bank.EpochSchedule
			rootSlot := m.Bank.Slot
			stakesForSlot := func(slot uint64) map[solana.PublicKey]uint64 {
				epoch := epochSchedule.GetEpoch(slot)
				voteAccounts := global.EpochStakesVoteAccts(epoch)
				return turbine.StakedNodes(global.EpochStakes(epoch), func(vote solana.PublicKey) (solana.PublicKey, bool) {
					voteAccount, ok := voteAccounts[vote]
					if !ok || voteAccount == nil {
						return solana.PublicKey{}, false
					}
					return voteAccount.NodePubkey, true
				})
			}
			pw, err := blockstream.StartTurbinePrewarm(blockstream.TurbinePrewarmConfig{
				BindAddr:                   turbineBindAddr,
				GossipEntrypoint:           turbineGossipEntrypoint,
				GossipBindAddr:             turbineGossipBindAddr,
				AdvertisedIP:               turbineAdvertisedIP,
				ShredVersion:               uint16(turbineShredVersion),
				AlpenglowAddr:              alpenglowObserverBindAddr,
				Identity:                   validatorIdentity,
				LeaderForSlot:              global.LeaderForSlot,
				StakesForSlot:              stakesForSlot,
				EpochForSlot:               epochSchedule.GetEpoch,
				RootSlot:                   func() uint64 { return rootSlot },
				UseChaCha8:                 true,
				DedupAddrs:                 false,
				FloorSlot:                  m.Bank.Slot + 1,
				ShredSpoolDir:              catchupShredSpoolDir(),
				RepairMaxRequestsPerSecond: repairMaxRequestsPerSecond,
			})
			if err != nil {
				mlog.Log.Warnf("turbine prewarm failed to start (continuing without): %v", err)
				return
			}
			turbinePrewarm = pw
			mlog.Log.Infof("turbine prewarm: collecting live shreds from slot %d while the AccountsDB builds (stakes %s)", m.Bank.Slot+1, origin)
		}
		snapshot.OnFullSnapshotManifestParsed = func(m *snapshot.SnapshotManifest) {
			prewarmMu.Lock()
			defer prewarmMu.Unlock()
			if turbinePrewarm != nil || m == nil {
				return
			}
			if !seedManifestStakes(m) {
				mlog.Log.FileOnlyf("turbine prewarm: full manifest carried no loadable epoch stakes; waiting for the incremental")
				return
			}
			if !ensureLeaderSchedule(m) {
				return // stale full: the incremental hook is the fallback start
			}
			startPrewarm(m, "from the full manifest")
		}
		snapshot.OnIncrementalManifestParsed = func(m *snapshot.SnapshotManifest) {
			prewarmMu.Lock()
			defer prewarmMu.Unlock()
			if m == nil {
				return
			}
			if turbinePrewarm != nil {
				// Prewarm already live off the full manifest. Refresh with
				// the incremental's strictly-fresher stakes; if the full's
				// schedule horizon missed the resume region, this rebuild is
				// what turns the receiver's missing-leader drops into
				// verified shreds.
				if seedManifestStakes(m) {
					ensureLeaderSchedule(m)
				}
				return
			}
			if !seedManifestStakes(m) {
				mlog.Log.Warnf("turbine prewarm: incremental manifest carried no loadable epoch stakes; prewarm skipped")
				return
			}
			if !ensureLeaderSchedule(m) {
				mlog.Log.Warnf("turbine prewarm: no leader schedule for the resume region; prewarm skipped")
				return
			}
			startPrewarm(m, "from the incremental manifest")
		}
	}
	var validatorVoteAccount solana.PublicKey
	var validatorAuthorizedVoter ed25519.PrivateKey
	if validatorVoteAccountKeypair != "" {
		votePubkey, err := loadValidatorPubkey("vote account", validatorVoteAccountKeypair)
		if err != nil {
			klog.Fatalf("%v", err)
		}
		validatorVoteAccount = votePubkey
		if validatorAuthorizedVoterKeypair == "" {
			if len(validatorIdentity) == ed25519.PrivateKeySize {
				validatorAuthorizedVoter = append(ed25519.PrivateKey(nil), validatorIdentity...)
				mlog.Log.Infof("validator vote account configured: %s (authorized voter defaults to identity %s, matching Agave)", votePubkey, validatorIdentityPubkey)
			}
		} else {
			authorizedVoter, authorizedVoterPubkey, err := loadValidatorPrivateKey("authorized voter", validatorAuthorizedVoterKeypair)
			if err != nil {
				klog.Fatalf("%v", err)
			}
			validatorAuthorizedVoter = authorizedVoter
			mlog.Log.Infof("validator vote account configured: %s (authorized voter %s)", votePubkey, authorizedVoterPubkey)
		}
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

	// Bootstrap: determine how to initialize AccountsDB based on mode
	var accountsDb *accountsdb.AccountsDb
	var manifest *snapshot.SnapshotManifest

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

	// Fall back to legacy detection if no state file
	hasAccountsDB, accountsDBSlot := detectExistingAccountsDB(accountsPath)
	if hasValidState {
		hasAccountsDB = true
		accountsDBSlot = mithrilState.GetCurrentSlot() // Use current slot (LastSlot if replayed, else SnapshotSlot)
	}
	if bootstrapMode == "accountsdb" && hasAccountsDB && !hasValidState {
		if legacyGenesisHash == "" {
			klog.Fatalf("FATAL: mode=accountsdb found an AccountsDB without a valid genesis-bound state. Supply --legacy-genesis-hash only if its lineage is known, or rebuild with --bootstrap new-snapshot.")
		}
		if legacyGenesisHash != networkGenesisHash {
			klog.Fatalf("FATAL: mode=accountsdb cannot open legacy AccountsDB from genesis %s against RPC genesis %s; rebuild with --bootstrap new-snapshot.",
				legacyGenesisHash, networkGenesisHash)
		}
	}
	var snapshotBuildStartedAt time.Time

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
		snapshotBuildStartedAt = time.Now().UTC()
		accountsDb, manifest, err = snapshot.BuildAccountsDbPaths(ctx, snapshotArchivePath, incrementalSnapshotFilename, accountsPath, dp)
		if err != nil {
			klog.Fatalf("failed to build AccountsDB from snapshot: %v", err)
		}

		// Write the genesis-bound state file only after a complete build.
		mithrilState, err = saveReadyStateForBootstrap(accountsPath, manifest, bootstrapMode, cluster, networkGenesisHash, snapshotBuildStartedAt)
		if err != nil {
			klog.Fatalf("failed to persist ready state: %v", err)
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
		if alpenglowMode {
			foldRecovery = mustRecoverFoldState(accountsDb)
			foldRecovered = true
			if foldRecovery.RewindInProgress {
				if mithrilState == nil {
					klog.Fatalf("interrupted durable rewind has no state context to reconcile; re-bootstrap with --bootstrap snapshot")
				}
				completeInterruptedRewind(accountsPath, accountsDb, mithrilState)
				foldRecovery = mustRecoverFoldState(accountsDb)
			}
			if err := adoptRecoveredManifestContextBeforeIntegrity(accountsPath, mithrilState, foldRecovery); err != nil {
				klog.Fatalf("fold manifest checkpoint at durable slot %d is invalid: %v; re-bootstrap with --bootstrap snapshot", foldRecovery.DurableThrough, err)
			}
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
		snapshotBuildStartedAt = time.Now().UTC()
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
			klog.Fatalf("failed to build AccountsDB from snapshot: %s", rpcclient.SanitizeErrorForDisplay(err))
		}
		// Write state file to mark build as complete.
		mithrilState, err = saveReadyStateForBootstrap(accountsPath, manifest, bootstrapMode, cluster, networkGenesisHash, snapshotBuildStartedAt)
		if err != nil {
			klog.Fatalf("failed to persist ready state: %v", err)
		}
		// Record bootstrap in history
		state.RecordBootstrap(accountsPath, manifest.Bank.Slot, "", replay.CurrentRunID, getVersion(), getCommit(), getBranch())

	case "new-incremental":
		// Mode: reuse the local full snapshot as the base, fetch the freshest
		// incremental for it, rebuild — the cheap refresh (incrementals are a
		// fraction of a full download).
		if snapshotDownloadPath == "" {
			klog.Fatalf("mode=new-incremental requires a snapshot directory (set storage.snapshots or snapshot.download_path in config)")
		}
		fullThreshold := config.GetInt("snapshot.full_threshold")
		if fullThreshold == 0 {
			fullThreshold = 100000 // default
		}
		existingSnap := detectLocalFullSnapshot(snapshotDownloadPath, fullThreshold, rpcEndpoints, ctx)
		if existingSnap == nil {
			klog.Fatalf("mode=new-incremental: no usable local full snapshot in %s — use --bootstrap new-snapshot to download one", snapshotDownloadPath)
		}
		mlog.Log.Infof("mode=new-incremental: reusing local full snapshot at slot %d, fetching a fresh incremental", existingSnap.slot)
		if accountsPath != "" {
			if mithrilState != nil {
				state.RecordRebuild(accountsPath, mithrilState.LastSlot, mithrilState.LastBankhash, getVersion(), getCommit(), getBranch(), "new-incremental mode")
			} else {
				state.RecordRebuild(accountsPath, 0, "", getVersion(), getCommit(), getBranch(), "new-incremental mode (no prior state)")
			}
			mlog.Log.Infof("Cleaning up previous AccountsDB artifacts in %s", accountsPath)
			snapshot.CleanAccountsDbDir(accountsPath)
		}
		snapshotBuildStartedAt = time.Now().UTC()
		accountsDb, manifest, err = buildFromExistingSnapshot(ctx, existingSnap, snapshotDownloadPath, accountsPath, blockstorePath, rpcEndpoints)
		if err != nil {
			klog.Fatalf("failed to build AccountsDB from snapshot: %v", err)
		}
		mithrilState, err = saveReadyStateForBootstrap(accountsPath, manifest, bootstrapMode, cluster, networkGenesisHash, snapshotBuildStartedAt)
		if err != nil {
			klog.Fatalf("failed to persist ready state: %v", err)
		}
		state.RecordBootstrap(accountsPath, manifest.Bank.Slot, "", replay.CurrentRunID, getVersion(), getCommit(), getBranch())

	case "snapshot":
		// Mode: Rebuild AccountsDB from snapshot, reuse existing snapshot file if fresh enough
		if snapshotDownloadPath == "" {
			klog.Fatalf("mode=snapshot requires a snapshot directory (set storage.snapshots or snapshot.download_path in config)")
		}
		snapshotBuildStartedAt = time.Now().UTC()
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
			klog.Fatalf("failed to build AccountsDB from snapshot: %s", rpcclient.SanitizeErrorForDisplay(err))
		}
		// Write state file to mark build as complete.
		mithrilState, err = saveReadyStateForBootstrap(accountsPath, manifest, bootstrapMode, cluster, networkGenesisHash, snapshotBuildStartedAt)
		if err != nil {
			klog.Fatalf("failed to persist ready state: %v", err)
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
			// Check if AccountsDB is behind chain tip. Use queryCurrentSlot
			// instead of queryLatestSnapshotSlot to avoid expensive node
			// discovery. The default prompts early: a fresh incremental is a
			// seconds-long download, while repairing many hundreds of slots
			// of a high-volume cluster takes far longer.
			stalePromptThreshold := config.GetInt("snapshot.stale_prompt_slots")
			if stalePromptThreshold <= 0 {
				stalePromptThreshold = 500
			}
			currentSlot, err := queryCurrentSlot(ctx, rpcEndpoints)
			if err != nil {
				mlog.Log.Infof("could not query current slot: %s (continuing with existing AccountsDB)", rpcclient.SanitizeErrorForDisplay(err))
			} else if mithrilState.IsStale(currentSlot, uint64(stalePromptThreshold)) {
				slotsBehind := currentSlot - mithrilState.GetCurrentSlot()
				mlog.Log.Infof("AccountsDB is %d slots behind chain tip", slotsBehind)

				// Interactive prompt
				choice := progress.PromptStaleAccountsDB(progress.StaleInfo{
					AccountsDBSlot:     mithrilState.GetCurrentSlot(),
					LatestSnapshotSlot: currentSlot, // Using current chain tip slot
					SlotsBehind:        slotsBehind,
				})

				if choice >= 2 {
					// choice 2: rebuild from the best available snapshot (reuse
					// the local full only if it is the freshest the cluster
					// offers). choice 3: force a brand-new full download,
					// ignoring any local snapshot. choice 4: pin the LOCAL full
					// as the base and just fetch a fresh incremental — the
					// cheap path (incrementals are a fraction of a full).
					if snapshotDownloadPath == "" {
						klog.Fatalf("cannot rebuild from snapshot: no snapshot directory configured (set storage.snapshots or snapshot.download_path in config)")
					}
					snapshotBuildStartedAt = time.Now().UTC()
					switch choice {
					case 2:
						mlog.Log.Infof("User chose to rebuild from the best available snapshot")
					case 3:
						mlog.Log.Infof("User chose to download a brand-new full snapshot")
					case 4:
						mlog.Log.Infof("User chose to refresh the incremental only (reusing the local full snapshot)")
					}
					var existingSnap *snapshotInfo
					if choice == 4 {
						// Resolve the base BEFORE any cleanup so a missing local
						// full fails without destroying the current AccountsDB.
						existingSnap = detectLocalFullSnapshot(snapshotDownloadPath, fullThreshold, rpcEndpoints, ctx)
						if existingSnap == nil {
							klog.Fatalf("incremental-only refresh needs a usable local full snapshot in %s and none was found; choose [2]/[3] instead (or --bootstrap new-snapshot)", snapshotDownloadPath)
						}
					}
					if accountsPath != "" {
						// Record rebuild in history before cleanup (history file is preserved)
						// mithrilState is guaranteed non-nil here (we prompted because it was stale)
						state.RecordRebuild(accountsPath, mithrilState.LastSlot, mithrilState.LastBankhash, getVersion(), getCommit(), getBranch(), "user chose rebuild (stale AccountsDB)")
						mlog.Log.Infof("Cleaning up previous AccountsDB artifacts in %s", accountsPath)
						snapshot.CleanAccountsDbDir(accountsPath)
					}
					// choice 3 forces a fresh download: skip the local-reuse check.
					if choice == 2 {
						existingSnap = detectFreshSnapshot(snapshotDownloadPath, fullThreshold, rpcEndpoints, ctx)
					}
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
						klog.Fatalf("failed to build AccountsDB from snapshot: %s", rpcclient.SanitizeErrorForDisplay(err))
					}
					mithrilState, err = saveReadyStateForBootstrap(accountsPath, manifest, bootstrapMode, cluster, networkGenesisHash, snapshotBuildStartedAt)
					if err != nil {
						klog.Fatalf("failed to persist ready state: %v", err)
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
			if alpenglowMode {
				foldRecovery = mustRecoverFoldState(accountsDb)
				foldRecovered = true
				if foldRecovery.RewindInProgress {
					completeInterruptedRewind(accountsPath, accountsDb, mithrilState)
					foldRecovery = mustRecoverFoldState(accountsDb)
				}
				if err := adoptRecoveredManifestContextBeforeIntegrity(accountsPath, mithrilState, foldRecovery); err != nil {
					klog.Fatalf("fold manifest checkpoint at durable slot %d is invalid: %v; re-bootstrap with --bootstrap snapshot", foldRecovery.DurableThrough, err)
				}
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
			snapshotBuildStartedAt = time.Now().UTC()
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
				klog.Fatalf("failed to build AccountsDB from snapshot: %s", rpcclient.SanitizeErrorForDisplay(err))
			}
			// Write state file to mark build as complete.
			mithrilState, err = saveReadyStateForBootstrap(accountsPath, manifest, bootstrapMode, cluster, networkGenesisHash, snapshotBuildStartedAt)
			if err != nil {
				klog.Fatalf("failed to persist ready state: %v", err)
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
		} else if alpenglowMode && mithrilState.LastRootedContext != nil {
			// Rooted-durable resume: durable position is the last rooted slot; the in-RAM
			// slots above it were lost. Resume from the slot after the last rooted slot
			// (GetResumeSlot returns it when LastRootedSlot > 0).
			startSlot = int64(mithrilState.GetResumeSlot())
		} else if !alpenglowMode && mithrilState.LastSlot > 0 {
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
	if alpenglowMode && mithrilState != nil && mithrilState.LastRootedContext != nil {
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
	} else if !alpenglowMode && mithrilState != nil && mithrilState.HasResumeData() {
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
		mithrilState, err = saveReadyStateForBootstrap(accountsPath, manifest, bootstrapMode, cluster, networkGenesisHash, snapshotBuildStartedAt)
		if err != nil {
			klog.Fatalf("failed to persist ready state: %v", err)
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

	// Alpenglow buffers replayed slots in a fork-aware WorkingSet and folds only
	// finalized prefixes. Classic verifying mode retains the established
	// per-slot AccountsDB persistence path.
	if config.IsSet("storage.durable_commit") {
		klog.Fatalf("storage.durable_commit was removed: batch folds replaced the per-slot redo-log commit path — delete the key (rooted-durable batching is always on)")
	}
	if config.IsSet("storage.rooted_durable") && config.GetBool("storage.rooted_durable") != alpenglowMode {
		klog.Fatalf("storage.rooted_durable must be %t for network.cluster=%s", alpenglowMode, cluster)
	}
	accountsDb.RootedDurable = alpenglowMode
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
	vcfg = verifierConfigForConsensusMode(consensusMode, vcfg)
	replay.TrailingVerifierCfg = vcfg

	if foldBatch := config.GetInt("storage.fold_batch_slots"); alpenglowMode && foldBatch > 0 {
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
	if alpenglowMode && config.IsSet("storage.compact.enabled") {
		compactEnabled = config.GetBool("storage.compact.enabled")
	}

	// Crash recovery reconcile: the durable fold frontier (recovered above from
	// the store's manifests + index meta — the state file is written only on
	// graceful shutdown and is stale after a hard kill) is reconciled against
	// the in-memory state watermark. Fresh-build paths fall through to the
	// recovery call here, which is a no-op on a store with no manifests.
	if alpenglowMode && mithrilState != nil {
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
			completeInterruptedRewind(accountsPath, accountsDb, mithrilState)
		case rec.DurableThrough > mithrilState.LastRootedSlot:
			// Store is ahead of the (stale) state file: adopt the manifest-carried
			// watermark + resume context.
			ctx, cerr := decodeAndValidateDurableResumeContext(accountsPath, rec.DurableThrough, rec.ResumeCtx)
			if cerr != nil {
				klog.Fatalf("store is durably folded through slot %d but its manifest checkpoint is unusable: %v; re-bootstrap with --bootstrap snapshot", rec.DurableThrough, cerr)
			}
			mlog.Log.Infof("fold recovery advanced rooted watermark R from %d to %d (manifest-carried context)", mithrilState.LastRootedSlot, rec.DurableThrough)
			mithrilState.LastRootedSlot = rec.DurableThrough
			mithrilState.LastRootedBankhash = ctx.Bankhash
			mithrilState.LastRootedContext = ctx
			if mithrilState.LastSlot < rec.DurableThrough {
				mithrilState.LastSlot = rec.DurableThrough
			}
		case rec.DurableThrough == mithrilState.LastRootedSlot && rec.DurableThrough > 0 && len(rec.ResumeCtx) > 0:
			// The fold manifest is the commit authority even when the state file
			// happens to carry the same numeric watermark. A hard kill can leave a
			// stale context-at-R; adopting it would combine the right accounts with
			// the wrong transaction-status lineage.
			ctx, cerr := decodeAndValidateDurableResumeContext(accountsPath, rec.DurableThrough, rec.ResumeCtx)
			if cerr != nil {
				klog.Fatalf("fold manifest checkpoint at durable slot %d is unusable: %v; re-bootstrap with --bootstrap snapshot", rec.DurableThrough, cerr)
			}
			mithrilState.LastRootedBankhash = ctx.Bankhash
			mithrilState.LastRootedContext = ctx
		case rec.DurableThrough == mithrilState.LastRootedSlot && rec.BatchSeq > 0:
			// The head fold manifest may already have been compacted outside the
			// rewind horizon. In that case the state-file reference is the only
			// surviving selector and must itself be complete and hash-valid.
			if err := validateDurableResumeContext(accountsPath, rec.DurableThrough, mithrilState.LastRootedContext); err != nil {
				klog.Fatalf("durable slot %d has no usable manifest context and its state-file checkpoint is invalid: %v; re-bootstrap with --bootstrap snapshot", rec.DurableThrough, err)
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
		if !alpenglowMode {
			klog.Fatalf("--rewind-to-slot is available only in Alpenglow rooted-durable mode")
		}
		if mithrilState == nil {
			klog.Fatalf("--rewind-to-slot=%d: no state file to reconcile (fresh bootstrap has nothing to rewind)", rewindToSlot)
		}
		points, perr := accountsDb.ListRewindPoints()
		if perr != nil {
			klog.Fatalf("--rewind-to-slot=%d: cannot list retained fold boundaries: %v", rewindToSlot, perr)
		}
		validPoints, rejected := validTransactionStatusRewindPoints(accountsPath, accountsDb, points, compactCfg.RewindHorizonBatches)
		if reason, bad := rejected[uint64(rewindToSlot)]; bad {
			klog.Fatalf("--rewind-to-slot=%d rejected before changing AccountsDB: transaction-status checkpoint is invalid: %v\navailable checkpoint-valid fold boundaries: %s",
				rewindToSlot, reason, formatRewindPoints(validPoints))
		}
		if !hasRewindPoint(validPoints, uint64(rewindToSlot)) {
			klog.Fatalf("--rewind-to-slot=%d is not a checkpoint-valid retained fold boundary\navailable checkpoint-valid fold boundaries: %s",
				rewindToSlot, formatRewindPoints(validPoints))
		}
		res, rerr := accountsDb.RewindToBatchBoundary(uint64(rewindToSlot))
		if rerr != nil {
			klog.Fatalf("--rewind-to-slot=%d failed: %v\navailable checkpoint-valid fold boundaries: %s", rewindToSlot, rerr, formatRewindPoints(validPoints))
		}
		if err := adoptRewindResult(accountsPath, mithrilState, res); err != nil {
			klog.Fatalf("--rewind-to-slot=%d: %v; re-bootstrap with --bootstrap snapshot", rewindToSlot, err)
		}
		mlog.Log.Warnf("rewound durable state to fold boundary at slot %d (%d batches / %d keys undone); replay resumes from slot %d",
			res.NewThrough, res.UndoneBatches, res.UndoneKeys, mithrilState.GetResumeSlot())
	}

	// Rooted-durable resume needs a context at the last rooted slot. The fold
	// manifest is the primary source (handled above); the state file is the
	// fallback for stores from before the first fold.
	if alpenglowMode && mithrilState != nil && mithrilState.LastRootedSlot > 0 {
		if mithrilState.LastRootedContext == nil || mithrilState.LastRootedContext.Slot != mithrilState.LastRootedSlot {
			ctxSlot := uint64(0)
			if mithrilState.LastRootedContext != nil {
				ctxSlot = mithrilState.LastRootedContext.Slot
			}
			klog.Fatalf("rooted-durable resume needs a context-at-R matching LastRootedSlot=%d (have context-slot=%d); re-bootstrap with --bootstrap snapshot",
				mithrilState.LastRootedSlot, ctxSlot)
		}
		if mithrilState.LastRootedSlot > mithrilState.SnapshotSlot {
			if err := validateDurableResumeContext(accountsPath, mithrilState.LastRootedSlot, mithrilState.LastRootedContext); err != nil {
				klog.Fatalf("rooted-durable resume checkpoint at slot %d is invalid: %v; re-bootstrap with --bootstrap snapshot",
					mithrilState.LastRootedSlot, err)
			}
		}
	}

	// Startup is the one point at which replay/folds are certainly quiescent.
	// Bound sidecar retention immediately, including after a crash before the
	// next fold's post-commit cleanup could run.
	if alpenglowMode {
		var currentRef *state.TransactionStatusCheckpointRef
		if mithrilState != nil && mithrilState.LastRootedContext != nil {
			currentRef = mithrilState.LastRootedContext.TransactionStatusCheckpoint
		}
		if removed, gerr := cleanupRetainedTransactionStatusCheckpoints(accountsPath, accountsDb, compactCfg.RewindHorizonBatches, currentRef); gerr != nil {
			mlog.Log.Warnf("transaction-status checkpoint startup cleanup failed: %v", gerr)
		} else if len(removed) > 0 {
			mlog.Log.Infof("transaction-status checkpoint startup cleanup removed %d unreferenced file(s)", len(removed))
		}
	}

	// Fold recovery and an operator rewind can move the durable watermark after
	// the initial state-file decode above. Rebuild the Alpenglow start position
	// from the reconciled context so replay never starts from a stale watermark.
	if alpenglowMode {
		startSlot = int64(manifest.Bank.Slot + 1)
		resumeState = nil
		if mithrilState != nil && mithrilState.LastRootedContext != nil {
			rs, rerr := replay.ResumeStateFromRootedContext(mithrilState.LastRootedContext, mithrilState.ComputedEpochStakes)
			if rerr != nil {
				klog.Fatalf("cannot rebuild reconciled Alpenglow resume state at rooted slot %d: %v", mithrilState.LastRootedContext.Slot, rerr)
			}
			resumeState = rs
			startSlot = int64(mithrilState.LastRootedContext.Slot + 1)
			mlog.Log.Infof("rooted-durable replay start reconciled to R=%d", mithrilState.LastRootedContext.Slot)
		}
		if endSlot != -1 {
			liveEndSlot = uint64(endSlot)
		} else if numReplaySlots != 0 {
			liveEndSlot = uint64(startSlot + numReplaySlots)
		} else {
			liveEndSlot = uint64(math.MaxUint64)
		}
	}

	// Validate the exact local parent bank before opening operational RPC and
	// before starting consensus, voting, or block production. Passive turbine
	// prewarm ingress may already be running, but it cannot participate in
	// consensus. In particular, an Alpenglow cluster setting is not evidence that
	// the snapshot has crossed Alpenglow genesis: the deployed feature and its
	// consensus metadata must be present in AccountsDB.
	if alpenglowMode {
		if startSlot < 1 {
			klog.Fatalf("Alpenglow replay has invalid start slot %d", startSlot)
		}
		parentSlot := uint64(startSlot - 1)
		if err := replay.ValidateAlpenglowStartupState(accountsDb, parentSlot); err != nil {
			klog.Fatalf("Alpenglow startup safety check failed: %v", err)
		}
		mlog.Log.Infof(
			"Alpenglow startup safety check passed at parent slot %d (feature=%s alpenclock=%s vote_reward=%s)",
			parentSlot,
			features.AlpenglowFeatureGateAddress,
			replay.NanosecondClockAccountAddr(),
			replay.VoteRewardAccountAddr(),
		)
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

	// The Alpenglow observer is forkchoice authority in both Alpenglow modes.
	// Classic clusters deliberately run without a certificate engine and keep
	// their verifying-only replay semantics.
	var consensusEngine *consensusengine.AlpenglowObserverEngine
	var alpenglowEpochSchedule *sealevel.SysvarEpochSchedule
	if alpenglowMode {
		alpenglowEpochSchedule = epochScheduleFromState(mithrilState)
		if alpenglowEpochSchedule == nil {
			klog.Fatalf("Alpenglow consensus requires an epoch schedule in the snapshot/state seed")
		}
		consensusEngine, err = consensusengine.NewEngine(consensusengine.Config{
			AlpenglowObserverBindAddr: alpenglowObserverBindAddr,
			AlpenglowMaxMessageBytes:  alpenglowMaxMessageBytes,
			AlpenglowBLSDST:           alpenglowBLSDST,
			AlpenglowShredVersion:     uint16(turbineShredVersion),
			AlpenglowIdentity:         validatorIdentity,
		})
		if err != nil {
			klog.Fatalf("unable to create consensus engine: %v", err)
		}
		root := alpenglow.BlockID{}
		if startSlot > 0 {
			root.Slot = uint64(startSlot - 1)
		}
		if resumeState != nil {
			root.Slot = resumeState.ParentSlot
			if resumeState.HasParentAlpenglowBlockID {
				root.Hash = resumeState.ParentAlpenglowBlockID
			}
		}
		// Load the same persisted/manifest stake view replay will use before the
		// receiver or broadcaster can make an admission decision. This is crucial
		// on AccountsDB resume, where global epoch stakes are otherwise populated
		// only inside ReplayBlocks after the network services have started.
		replayStartEpoch := alpenglowEpochSchedule.GetEpoch(uint64(startSlot))
		snapshotEpoch := alpenglowEpochSchedule.GetEpoch(mithrilState.ManifestParentSlot)
		if err := replay.LoadInitialEpochStakesCache(mithrilState, resumeState, replayStartEpoch, snapshotEpoch); err != nil {
			klog.Fatalf("load Alpenglow epoch stakes before Votor startup: %v", err)
		}
		// Publish the lookup and cached VAT/rank sets before opening the QUIC
		// listener. Otherwise an admitted validator is briefly indistinguishable
		// from an unstaked TLS peer and every startup connection is rejected.
		consensusEngine.SetAlpenglowEpochLookup(alpenglowEpochSchedule.GetEpoch)
		rootEpoch := alpenglowEpochSchedule.GetEpoch(root.Slot)
		installed, rootInstalled := replay.InstallCachedAlpenglowValidatorSets(consensusEngine, rootEpoch)
		if !rootInstalled {
			klog.Fatalf("unable to install the Alpenglow validator set for Votor root slot %d epoch %d before transport startup (installed %d other cached set(s))", root.Slot, rootEpoch, installed)
		}
		consensusEngine.SetAlpenglowRoot(root)
		if err := consensusEngine.Start(ctx); err != nil {
			klog.Fatalf("unable to start consensus engine %q: %v", consensusEngine.Name(), err)
		}
		defer func() {
			if err := consensusEngine.Close(); err != nil {
				mlog.Log.Warnf("consensus engine close failed: %v", err)
			}
		}()
	}
	var turbineStakesForSlot func(slot uint64) map[solana.PublicKey]uint64
	if alpenglowEpochSchedule != nil {
		turbineStakesForSlot = func(slot uint64) map[solana.PublicKey]uint64 {
			epoch := alpenglowEpochSchedule.GetEpoch(slot)
			voteAccounts := global.EpochStakesVoteAccts(epoch)
			return turbine.StakedNodes(global.EpochStakes(epoch), func(vote solana.PublicKey) (solana.PublicKey, bool) {
				voteAccount, ok := voteAccounts[vote]
				if !ok || voteAccount == nil {
					return solana.PublicKey{}, false
				}
				return voteAccount.NodePubkey, true
			})
		}
	}

	localBlocks := make(chan *block.Block, 16)
	var sharedGossip *gossip.Client
	var validatorPrewarmBlocks []*block.Block
	if consensusMode == "validator" {
		if turbinePrewarm != nil {
			var dropped int
			validatorPrewarmBlocks, dropped = turbinePrewarm.Handover()
			turbinePrewarm = nil
			if len(validatorPrewarmBlocks) > 0 || dropped > 0 {
				mlog.Log.Infof("turbine prewarm handover for validator startup: %d blocks (%d dropped)", len(validatorPrewarmBlocks), dropped)
			}
		}

		advertisedIP := validatorAdvertisedIP
		if advertisedIP == "" {
			advertisedIP = turbineAdvertisedIP
		}
		sharedGossip, err = gossip.NewClient(gossip.Config{
			Entrypoint:    turbineGossipEntrypoint,
			BindAddr:      turbineGossipBindAddr,
			TVUAddr:       turbineBindAddr,
			AlpenglowAddr: alpenglowAddrForGossip(alpenglowObserverBindAddr),
			AdvertisedIP:  advertisedIP,
			ShredVersion:  uint16(turbineShredVersion),
			Identity:      validatorIdentity,
			Name:          gossip.ClientName,
		})
		if err != nil {
			klog.Fatalf("validator gossip client: %v", err)
		}
		go func() {
			if err := sharedGossip.Run(ctx); err != nil && ctx.Err() == nil {
				mlog.Log.Errorf("validator gossip client stopped: %v", err)
				if errors.Is(err, gossip.ErrIdentityInUse) {
					cancelRun()
				}
			}
		}()
		go monitorValidatorIdentityOwnership(ctx, rpcEndpoints, solana.PrivateKey(validatorIdentity).PublicKey(), sharedGossip, cancelRun)

		epochSchedule := alpenglowEpochSchedule
		global.SetWallClockSlotDuration(blockprod.AlpenglowSlotDuration)
		wallClockSeed, err := queryWallClockSeedSlot(ctx, rpcEndpoints)
		if err != nil {
			klog.Fatalf("validator voting requires a confirmed network wall-clock slot; refusing unsafe snapshot-slot fallback: %v", err)
		}
		global.SeedWallClockSlot(wallClockSeed)
		startupWallSlot := global.WallClockSlot()
		waitToVoteSlot := startupWallSlot - startupWallSlot%alpenglow.LeaderWindowSlots
		if waitToVoteSlot <= math.MaxUint64-2*alpenglow.LeaderWindowSlots {
			waitToVoteSlot += 2 * alpenglow.LeaderWindowSlots
		} else {
			waitToVoteSlot = math.MaxUint64
		}
		mlog.Log.Infof("ALPENGLOW voting startup watermark: wall_clock=%d wait_to_vote=%d", startupWallSlot, waitToVoteSlot)

		identityPubkey := solana.PrivateKey(validatorIdentity).PublicKey()
		if err := consensusEngine.EnableVoting(consensusengine.VotingConfig{
			Identity:        validatorIdentity,
			AuthorizedVoter: validatorAuthorizedVoter,
			VoteAccount:     validatorVoteAccount,
			HistoryDir:      blockstorePath,
			EpochForSlot:    epochSchedule.GetEpoch,
			SlotDuration:    blockprod.AlpenglowSlotDuration,
			WaitToVoteSlot:  waitToVoteSlot,
			ReadyToVote: func(slot uint64) bool {
				wallSlot := global.WallClockSlot()
				if liveSlot, ok := consensusEngine.AlpenglowLiveSlot(); ok {
					wallSlot = liveSlot
				}
				return slot >= wallSlot || wallSlot-slot <= alpenglow.LeaderWindowSlots
			},
			Peers: func(validators []alpenglow.ValidatorStake) []alpenglow.VotorPeer {
				peers := make([]alpenglow.VotorPeer, 0, len(validators))
				seen := make(map[solana.PublicKey]struct{}, len(validators))
				for _, validator := range validators {
					if validator.Stake == 0 || validator.NodePubkey == identityPubkey {
						continue
					}
					addr, ok := sharedGossip.LookupAlpenglow(validator.NodePubkey)
					if !ok {
						continue
					}
					if _, duplicate := seen[validator.NodePubkey]; duplicate {
						continue
					}
					seen[validator.NodePubkey] = struct{}{}
					peers = append(peers, alpenglow.VotorPeer{Identity: validator.NodePubkey, Addr: addr})
				}
				return peers
			},
		}); err != nil {
			klog.Fatalf("enable Alpenglow voting: %v", err)
		}
		broadcaster, err := turbine.NewTurbineBroadcaster(turbine.TurbineBroadcasterConfig{
			Self:          solana.PrivateKey(validatorIdentity).PublicKey(),
			Peers:         sharedGossip,
			Stakes:        turbineStakesForSlot,
			EpochForSlot:  epochSchedule.GetEpoch,
			LeaderForSlot: global.LeaderForSlot,
			UseChaCha8:    true,
			// Agave treats custom/development clusters as non-deduplicating so
			// every validator derives the same deterministic Turbine node list.
			DedupAddrs: false,
		})
		if err != nil {
			klog.Fatalf("validator turbine broadcaster: %v", err)
		}
		defer broadcaster.Close()

		controller := blockprod.NewController()
		topicSink := scheduler.New(controller)
		topicSink.Start(ctx)
		defer topicSink.Stop()
		tpuCfg := tpu.DefaultConfig()
		tpuCfg.Identity = validatorIdentity
		tpuCfg.ListenAddr = validatorTPUQUICBind
		tpuCfg.AdvertisedIP = advertisedIP
		tpuCfg.Pipeline.Sink = topicSink
		tpuCfg.Pipeline.TxV1Enabled = func() bool {
			// During our leader window, admission must use that working bank's
			// feature snapshot rather than the concurrently advancing replay tip.
			if bank := controller.WorkingBank(); bank != nil {
				slotCtx := bank.SlotCtx()
				return slotCtx != nil && slotCtx.Features != nil && slotCtx.Features.IsActive(features.EnableTxV1)
			}
			return replay.ChainTipFeatureActive(features.EnableTxV1)
		}
		if validatorSigverifyWorkers > 0 {
			tpuCfg.Pipeline.SigverifyWorkers = validatorSigverifyWorkers
		}
		tpuService, err := tpu.Start(ctx, tpuCfg)
		if err != nil {
			klog.Fatalf("start validator TPU: %v", err)
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := tpuService.Stop(shutdownCtx); err != nil {
				mlog.Log.Warnf("validator TPU shutdown: %v", err)
			}
		}()
		tpuAdvertise, err := tpuService.AdvertisedQUICAddr()
		if err != nil {
			klog.Fatalf("validator TPU advertised address: %v", err)
		}
		if err := sharedGossip.SetTPUQUIC(tpuAdvertise); err != nil {
			mlog.Log.Warnf("validator gossip TPU advertisement: %v", err)
		}

		rewardBuilder := rewardcerts.NewBuilder(rewardcerts.BuilderConfig{
			RootSlot:    global.Slot,
			BeforeBuild: consensusEngine.FlushAlpenglowRewardVotes,
		})
		consensusEngine.SetVotorMessageHook(rewardBuilder.ObserveVotorMessage)
		leaderStop := make(chan struct{})
		leaderDone := make(chan struct{})
		leaderLoop := blockprod.NewLeaderLoop(blockprod.LeaderLoopConfig{
			Controller:     controller,
			Identity:       solana.PrivateKey(validatorIdentity),
			AccountsDb:     accountsDb,
			Broadcaster:    broadcaster,
			ShredVersion:   uint16(turbineShredVersion),
			EpochSchedule:  epochSchedule,
			AlpenglowClock: true,
			SlotDuration:   blockprod.AlpenglowSlotDuration,
			ParentContext: func(slot uint64) blockprod.ParentContext {
				tip := replay.ChainTipParentContext()
				// Blockprod owns the replay-readiness rule. In particular, the first
				// slot of an Alpenglow leader window may legitimately build on an
				// older ParentReady ancestor when the preceding window was skipped;
				// requiring frontier==slot-1 here would silently defeat that gate.
				if slot == 0 || tip.Slot >= slot {
					return blockprod.ParentContext{}
				}
				return blockprod.ParentContext{
					ReplayGeneration:           tip.Generation,
					ParentSlot:                 tip.Slot,
					ParentBankhash:             tip.Bankhash,
					ParentBlockID:              tip.AlpenglowBlockID,
					HasParentBlockID:           tip.HasAlpenglowBlockID,
					ParentChainedMerkleRoot:    tip.AlpenglowChainedMerkleRoot,
					HasParentChainedMerkleRoot: tip.HasAlpenglowChainedMerkleRoot,
					ParentLastEntryHash:        tip.LastEntryHash,
					ParentLastBlockhash:        tip.LastBlockhash,
					ParentBlockHeight:          tip.BlockHeight,
					LatestEvictedBlockhash:     tip.LatestEvictedBlockhash,
					EpochRewardsActive:         tip.EpochRewardsActive,
					PrevNumSigs:                tip.PrevNumSigs,
					PrevFeeGovernor:            tip.PrevFeeGovernor,
					AcctsLtHash:                tip.AcctsLtHash,
					Features:                   tip.Features,
					BankSysvars:                tip.BankSysvars,
					EpochStakes:                tip.EpochStakes,
					TotalEpochStake:            tip.TotalEpochStake,
					NanosecondClockAccount:     tip.NanosecondClockAccount,
					HasNanosecondClockAccount:  tip.HasNanosecondClockAccount,
					UnrootedRead:               tip.UnrootedRead,
					TransactionStatuses:        tip.TransactionStatuses,
				}
			},
			ProductionParent: consensusEngine.AlpenglowBlockProductionParent,
			CurrentSlot: func() uint64 {
				if slot, ok := consensusEngine.AlpenglowLiveSlot(); ok {
					return slot
				}
				return global.WallClockSlot()
			},
			LeaderForSlot: global.LeaderForSlot,
			RewardCerts:   rewardBuilder,
			OnBlock: func(produced *block.Block) {
				select {
				case localBlocks <- produced:
				case <-ctx.Done():
				}
			},
		})
		go func() {
			defer close(leaderDone)
			leaderLoop.Run(leaderStop)
		}()
		defer func() {
			close(leaderStop)
			<-leaderDone
		}()
		mlog.Log.Infof("Alpenglow validator production active: TPU=%s advertised=%s", tpuService.ListenAddr(), tpuAdvertise)
	}

	replayStartTime := time.Now()
	blockFetchOpts := &replay.BlockFetchOpts{
		MaxRPS:                     blockMaxRPS,
		MaxInflight:                blockMaxInflight,
		TipPollMs:                  blockTipPollIntervalMs,
		TipSafetyMargin:            uint64(blockTipSafetyMargin),
		NearTipThreshold:           blockNearTipThreshold,
		CatchupThreshold:           blockCatchupThreshold,
		CatchupTipGateThreshold:    blockCatchupTipGateThreshold,
		NearTipPollMs:              blockNearTipPollMs,
		NearTipLookahead:           blockNearTipLookahead,
		RepairCatchupMaxGapSlots:   uint64(repairCatchupMaxGapSlots),
		RepairMaxRequestsPerSecond: repairMaxRequestsPerSecond,
		DisableRPCBlockFetch:       !blockRPCFallback,
		TurbinePrewarm:             turbinePrewarm,
		PrewarmBlocks:              validatorPrewarmBlocks,
		ShredSpoolDir:              catchupShredSpoolDir(),
		GossipClient:               sharedGossip,
		TurbineStakesForSlot:       turbineStakesForSlot,
		TurbineEpochForSlot: func(slot uint64) uint64 {
			if alpenglowEpochSchedule == nil {
				return slot
			}
			return alpenglowEpochSchedule.GetEpoch(slot)
		},
		TurbineRootSlot:   global.ReplayFrontier,
		TurbineUseChaCha8: true,
		TurbineDedupAddrs: false,
	}
	if consensusMode == "validator" {
		identity := solana.PrivateKey(validatorIdentity).PublicKey()
		blockFetchOpts.LocalBlocks = localBlocks
		blockFetchOpts.LocalLeaderForSlot = func(slot uint64) bool {
			leader, ok := global.LeaderForSlot(slot)
			return ok && leader == identity
		}
	}

	consensusOpts := &replay.ConsensusOpts{
		Alpenglow: alpenglowMode,
		Engine:    consensusEngine,
	}
	if alpenglowMode {
		consensusOpts.TransactionStatusCheckpointAfterCommit = func(selected *state.TransactionStatusCheckpointRef) error {
			removed, err := cleanupRetainedTransactionStatusCheckpoints(
				accountsPath, accountsDb, compactCfg.RewindHorizonBatches, selected,
			)
			if err == nil && len(removed) > 0 {
				mlog.Log.FileOnlyf("transaction-status checkpoint retention removed %d file(s) outside the %d-batch rewind horizon",
					len(removed), compactCfg.RewindHorizonBatches)
			}
			return err
		}
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

	startSigverifyReporter(ctx)

	turbineAlpenglowAddr := ""
	if alpenglowMode {
		turbineAlpenglowAddr = alpenglowAddrForGossip(alpenglowObserverBindAddr)
	}
	result := runReplayWithRecovery(ctx, accountsDb, accountsPath, manifest, resumeState, uint64(startSlot), liveEndSlot, rpcEndpoints, lightbringerEndpoint, turbineBindAddr, turbineGossipEntrypoint, turbineGossipBindAddr, turbineAdvertisedIP, uint16(turbineShredVersion), turbineAlpenglowAddr, validatorIdentity, blockstorePath, int(txParallelism), true, useLightbringer, useTurbine, dbgOpts, metricsWriter, slotCtxSetter, mithrilState, blockFetchOpts, consensusOpts, compactCfg.RewindHorizonBatches, replayStartTime)

	if result.Error != nil {
		if result.LastPersistedSlot == 0 {
			mlog.Log.Errorf("Replay stopped before persisting the first post-start slot: %s", rpcclient.SanitizeErrorForDisplay(result.Error))
		} else {
			mlog.Log.Errorf("Replay stopped with error after persisting slot %d: %s", result.LastPersistedSlot, rpcclient.SanitizeErrorForDisplay(result.Error))
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
					shutdownReason = fmt.Sprintf("%s: %s", state.ShutdownReasonError, rpcclient.SanitizeErrorForDisplay(result.Error))
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

type turbineEntrypointQuery func(*net.UDPAddr, time.Duration) (gossip.EchoResponse, error)

func resolveTurbineShredVersion(configured int, entrypoint string, query turbineEntrypointQuery) (int, error) {
	if configured != 0 {
		return configured, nil
	}
	if strings.TrimSpace(entrypoint) == "" {
		return 0, fmt.Errorf("turbine gossip entrypoint is required when shred version is auto-discovered")
	}
	addr, err := net.ResolveUDPAddr("udp", entrypoint)
	if err != nil {
		return 0, fmt.Errorf("resolve gossip entrypoint %q: %w", entrypoint, err)
	}
	if query == nil {
		query = gossip.QueryEntrypoint
	}
	response, err := query(addr, 5*time.Second)
	if err != nil {
		return 0, fmt.Errorf("query gossip entrypoint %s: %w", addr, err)
	}
	return int(response.ShredVersion), nil
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
		mlog.Log.Infof("WARNING: failed to fetch genesis hash from RPC: %s", rpcclient.SanitizeErrorForDisplay(err))
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

	// Node mode, right at the top where an operator looks first
	modeLabel := "verifying node (non-voting)"
	if consensusMode == "validator" {
		modeLabel = "validator (voting and block production active)"
	}
	fmt.Printf("  Mode:         %s%s%s\n", gold, modeLabel, reset)

	// Bootstrap mode with description
	var bootstrapDesc string
	switch bootstrapMode {
	case "auto":
		bootstrapDesc = "use existing AccountsDB if valid, else download snapshot"
	case "snapshot":
		bootstrapDesc = "rebuild from local snapshot"
	case "new-snapshot":
		bootstrapDesc = "download fresh snapshot from network"
	case "new-incremental":
		bootstrapDesc = "reuse local full snapshot, fetch fresh incremental"
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

	if resolvedSigverifyBackend != "" {
		sigverifyDesc := "AVX-512 accelerated"
		switch resolvedSigverifyBackend {
		case sigverify.BackendGeneric:
			sigverifyDesc = "portable; no AVX512-IFMA on this CPU"
		case sigverify.BackendStdlib:
			sigverifyDesc = "Go's crypto/ed25519, but also uses Narya's strictness checks"
		}
		fmt.Printf("  Sigverify:    %s%s%s %s(%s)%s\n",
			green, resolvedSigverifyBackend, reset, dim, sigverifyDesc, reset)
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
				reason := rpcclient.SanitizeTextForDisplay(mithrilState.LastShutdownReason)
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
		fmt.Printf("  RPC:          %s%s%s (primary)\n", gold, rpcclient.SanitizeEndpointForDisplay(rpcEndpoints[0]), reset)
		for _, ep := range rpcEndpoints[1:] {
			fmt.Printf("                %s%s%s (fallback)\n", gold, rpcclient.SanitizeEndpointForDisplay(ep), reset)
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
// detectLocalFullSnapshot scans the snapshot dir for the newest local full
// snapshot within fullThreshold of the tip — WITHOUT the cluster-freshness
// veto detectFreshSnapshot adds. The incremental-only refresh pins this as
// its base deliberately: the point is the cheap path (reuse the tens-of-GB
// full already on disk, fetch only a fresh incremental for its base).
func detectLocalFullSnapshot(snapshotDir string, fullThreshold int, rpcEndpoints []string, ctx context.Context) *snapshotInfo {
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
		mlog.Log.Infof("could not query current slot: %s", rpcclient.SanitizeErrorForDisplay(err))
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

func detectFreshSnapshot(snapshotDir string, fullThreshold int, rpcEndpoints []string, ctx context.Context) *snapshotInfo {
	bestSnapshot := detectLocalFullSnapshot(snapshotDir, fullThreshold, rpcEndpoints, ctx)
	if bestSnapshot == nil {
		return nil
	}
	currentSlot, err := queryCurrentSlot(ctx, rpcEndpoints)
	if err != nil {
		mlog.Log.Infof("could not query current slot: %v", err)
		return nil
	}

	// full_threshold (100k) is a loose "is this snapshot usable at all" gate,
	// not "is it the FRESHEST available." A local full tens of thousands of
	// slots old passes it, yet reusing it strands the node that far behind —
	// and because the incremental search keys off the reused full's base, it
	// never even looks for a fresher base the cluster may have. So when the
	// local snapshot is itself materially stale (more than the incremental
	// freshness band behind tip), probe the cluster and prefer a genuinely
	// fresher full. If the cluster has nothing better, reuse and say so — that
	// surfaces cluster-side staleness instead of silently starting far behind.
	incThreshold := config.GetInt("snapshot.incremental_threshold")
	if incThreshold <= 0 {
		incThreshold = 2000
	}
	if currentSlot > bestSnapshot.slot && currentSlot-bestSnapshot.slot > uint64(incThreshold) {
		if latest, lerr := queryLatestSnapshotSlot(ctx, rpcEndpoints); lerr == nil && latest > 0 {
			if latest > bestSnapshot.slot+uint64(incThreshold) {
				mlog.Log.Infof("On-disk snapshot at slot %d is %d slots behind tip; the cluster offers a fresher full at slot %d — downloading fresh instead of reusing the stale local file",
					bestSnapshot.slot, currentSlot-bestSnapshot.slot, latest)
				return nil
			}
			mlog.Log.Infof("On-disk snapshot at slot %d is the freshest the cluster offers (latest available %d, %d slots behind tip) — reusing; the cluster is not publishing anything newer",
				bestSnapshot.slot, latest, currentSlot-bestSnapshot.slot)
		} else if lerr != nil {
			mlog.Log.Infof("could not check cluster for a fresher snapshot (%v); reusing the on-disk file at slot %d", lerr, bestSnapshot.slot)
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

// queryWallClockSeedSlot uses confirmed commitment for leader timing; finalized
// commitment intentionally lags too far behind a live production window.
func queryWallClockSeedSlot(ctx context.Context, rpcEndpoints []string) (uint64, error) {
	if len(rpcEndpoints) == 0 {
		return 0, fmt.Errorf("no RPC endpoints configured")
	}
	var best uint64
	found := false
	failures := make([]string, 0, len(rpcEndpoints))
	for _, endpoint := range rpcEndpoints {
		client := solrpc.New(endpoint)
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		slot, err := client.GetSlot(requestCtx, solrpc.CommitmentConfirmed)
		cancel()
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", endpoint, err))
			continue
		}
		if !found || slot > best {
			best = slot
			found = true
		}
	}
	if !found {
		return 0, fmt.Errorf("failed to get confirmed slot from configured RPC endpoints: %s", strings.Join(failures, "; "))
	}
	return best, nil
}

const (
	validatorIdentityOwnershipGrace        = 30 * time.Second
	validatorIdentityOwnershipPollInterval = 5 * time.Second
	validatorIdentityConflictConfirmations = 2
)

// monitorValidatorIdentityOwnership checks the cluster-wide CRDS winner for
// our validator pubkey. Two processes can both sign valid ContactInfo records
// with the same key, but only the newest record wins; when the other process
// wins, turbine and Votor ingress move there while our RPC-derived tip remains
// healthy. Passive gossip cannot always observe the winning record after its
// own endpoint has been displaced, so use the already-configured RPC view as
// an independent ownership check and stop cleanly instead of stalling replay.
func monitorValidatorIdentityOwnership(ctx context.Context, rpcEndpoints []string, identity solana.PublicKey, client *gossip.Client, cancel context.CancelFunc) {
	if len(rpcEndpoints) == 0 || client == nil || cancel == nil {
		return
	}
	grace := time.NewTimer(validatorIdentityOwnershipGrace)
	defer grace.Stop()
	select {
	case <-ctx.Done():
		return
	case <-grace.C:
	}

	ticker := time.NewTicker(validatorIdentityOwnershipPollInterval)
	defer ticker.Stop()
	conflicts := 0
	for {
		expected := client.AdvertisedGossipAddr()
		if expected != nil {
			lookupCtx, lookupCancel := context.WithTimeout(ctx, 5*time.Second)
			observed, found, err := queryValidatorIdentityGossip(lookupCtx, rpcEndpoints, identity)
			lookupCancel()
			switch {
			case err != nil:
				conflicts = 0 // an unavailable observer is not evidence of a collision
				mlog.Log.FileOnlyf("validator identity ownership check unavailable: %v", err)
			case !found:
				conflicts = 0 // CRDS convergence/expiry window
			case sameUDPAddrString(observed, expected):
				conflicts = 0
			default:
				conflicts++
				if conflicts >= validatorIdentityConflictConfirmations {
					mlog.Log.Errorf("validator identity ownership lost: cluster advertises %s at gossip=%s, but this Mithril instance advertises gossip=%s; another live process is using the same identity — stop it before restarting Mithril",
						identity.String(), observed, expected.String())
					cancel()
					return
				}
				mlog.Log.Warnf("validator identity ownership check: cluster currently advertises gossip=%s for %s, expected %s (confirmation %d/%d)",
					observed, identity.String(), expected.String(), conflicts, validatorIdentityConflictConfirmations)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func queryValidatorIdentityGossip(ctx context.Context, rpcEndpoints []string, identity solana.PublicKey) (string, bool, error) {
	var lastErr error
	var succeeded bool
	for _, endpoint := range rpcEndpoints {
		nodes, err := solrpc.New(endpoint).GetClusterNodes(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		succeeded = true
		for _, node := range nodes {
			if node == nil || node.Pubkey != identity || node.Gossip == nil || strings.TrimSpace(*node.Gossip) == "" {
				continue
			}
			return strings.TrimSpace(*node.Gossip), true, nil
		}
	}
	if succeeded {
		return "", false, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no RPC endpoints configured")
	}
	return "", false, lastErr
}

func sameUDPAddrString(observed string, expected *net.UDPAddr) bool {
	if expected == nil {
		return false
	}
	addr, err := net.ResolveUDPAddr("udp", observed)
	return err == nil && addr.Port == expected.Port && addr.IP.Equal(expected.IP)
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
	finishBootstrap := statsd.BeginSnapshotBootstrap()
	defer finishBootstrap()

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
	finishBootstrap := statsd.BeginSnapshotBootstrap()
	defer finishBootstrap()

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

	accountsDb, manifest, err := snapshot.BuildAccountsDbAuto(ctx, fullSnapshotURL, snapshotDownloadPath, fullSnapshotSlot, fullSnapshotInfo.ReferenceSlot, accountsPath, rpcEndpoints, blockstorePath, snapCfg, dp)
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
		} else {
			// A survivor competes for ports AND doubles our repair/gossip
			// traffic against the cluster (peer-side rate limiting then
			// throttles BOTH instances) — never skip one silently.
			fmt.Printf("  ⚠ could not kill existing mithril process %d: %v — kill it manually before continuing\n", pid, err)
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

// adoptRecoveredManifestContextBeforeIntegrity runs before the legacy
// state-vs-bankhash check. A committed fold manifest is the authority when its
// root is equal to or ahead of the stale state file; otherwise a first-fold
// hard kill can be misclassified as corruption before normal reconciliation.
func adoptRecoveredManifestContextBeforeIntegrity(accountsDbPath string, s *state.MithrilState, rec accountsdb.RecoveryResult) error {
	if s == nil || rec.RewindInProgress || rec.DurableThrough == 0 || rec.DurableThrough < s.LastRootedSlot {
		return nil
	}
	if len(rec.ResumeCtx) == 0 {
		if rec.DurableThrough > s.LastRootedSlot {
			return fmt.Errorf("store advanced from rooted slot %d to %d but the committed fold context is unavailable", s.LastRootedSlot, rec.DurableThrough)
		}
		return nil // equal-root head manifest may have aged out; state is checked later.
	}
	ctx, err := decodeAndValidateDurableResumeContext(accountsDbPath, rec.DurableThrough, rec.ResumeCtx)
	if err != nil {
		return err
	}
	oldRoot := s.LastRootedSlot
	s.LastRootedBankhash = ctx.Bankhash
	s.LastRootedContext = ctx
	s.LastRootedSlot = rec.DurableThrough
	if s.LastSlot < rec.DurableThrough {
		s.LastSlot = rec.DurableThrough
	}
	if rec.DurableThrough > oldRoot {
		mlog.Log.Infof("fold recovery advanced rooted watermark R from %d to %d before state integrity validation", oldRoot, rec.DurableThrough)
	}
	return nil
}

func decodeAndValidateDurableResumeContext(accountsDbPath string, expectedRoot uint64, encoded []byte) (*state.ResumeContext, error) {
	if len(encoded) == 0 {
		return nil, fmt.Errorf("fold manifest at slot %d carries no resume context", expectedRoot)
	}
	var ctx state.ResumeContext
	if err := json.Unmarshal(encoded, &ctx); err != nil {
		return nil, fmt.Errorf("decode resume context at slot %d: %w", expectedRoot, err)
	}
	if err := validateDurableResumeContext(accountsDbPath, expectedRoot, &ctx); err != nil {
		return nil, err
	}
	return &ctx, nil
}

// validateDurableResumeContext verifies both selectors: the small manifest
// reference and the immutable sidecar bytes. Decoding the status snapshot is
// intentional here; a matching SHA alone cannot prove complete coverage or a
// tip aligned with the account-state boundary.
func validateDurableResumeContext(accountsDbPath string, expectedRoot uint64, ctx *state.ResumeContext) error {
	if ctx == nil {
		return fmt.Errorf("resume context at slot %d is missing", expectedRoot)
	}
	if ctx.Slot != expectedRoot {
		return fmt.Errorf("resume context at durable boundary %d names slot %d", expectedRoot, ctx.Slot)
	}
	ref := ctx.TransactionStatusCheckpoint
	if err := replay.ValidateTransactionStatusCheckpointRef(ref, expectedRoot); err != nil {
		return err
	}
	payload, err := replay.ReadTransactionStatusCheckpoint(accountsDbPath, ref)
	if err != nil {
		return err
	}
	cache, err := replay.NewTransactionStatusCacheFromSnapshot(payload)
	if err != nil {
		return fmt.Errorf("decode transaction status checkpoint at slot %d: %w", expectedRoot, err)
	}
	if !cache.CoverageComplete() {
		return fmt.Errorf("transaction status checkpoint at slot %d has incomplete retained-root coverage", expectedRoot)
	}
	if rooted := cache.RootedThrough(); rooted != expectedRoot {
		return fmt.Errorf("transaction status checkpoint rooted through slot %d, want %d", rooted, expectedRoot)
	}
	if tip, ok := cache.TipSlot(); !ok || tip != expectedRoot {
		if !ok {
			return fmt.Errorf("transaction status checkpoint at slot %d has no selected tip", expectedRoot)
		}
		return fmt.Errorf("transaction status checkpoint tip is slot %d, want %d", tip, expectedRoot)
	}
	if ctx.AlpenglowBlockID == "" {
		return fmt.Errorf("resume context at post-snapshot durable slot %d has no Alpenglow block id", expectedRoot)
	}
	blockID, err := solana.HashFromBase58(ctx.AlpenglowBlockID)
	if err != nil {
		return fmt.Errorf("decode resume context Alpenglow block id at slot %d: %w", expectedRoot, err)
	}
	if err := cache.BindTipBlockID(expectedRoot, blockID); err != nil {
		return fmt.Errorf("transaction status checkpoint lineage at slot %d: %w", expectedRoot, err)
	}
	if ctx.AlpenglowChainedMerkleRoot == "" {
		return fmt.Errorf("resume context at post-snapshot durable slot %d has no Alpenglow chained merkle root", expectedRoot)
	}
	if _, err := solana.HashFromBase58(ctx.AlpenglowChainedMerkleRoot); err != nil {
		return fmt.Errorf("decode resume context Alpenglow chained merkle root at slot %d: %w", expectedRoot, err)
	}
	return nil
}

func hasRewindPoint(points []accountsdb.RewindPoint, slot uint64) bool {
	for _, point := range points {
		if point.ThroughSlot == slot {
			return true
		}
	}
	return false
}

func rewindPointManifest(accountsDb *accountsdb.AccountsDb, point accountsdb.RewindPoint) (*accountsdb.SegmentManifest, error) {
	headers, err := accountsdb.ListFoldManifests(accountsDb.AcctsDir)
	if err != nil {
		return nil, fmt.Errorf("list fold manifests: %w", err)
	}
	for _, header := range headers {
		if header.BatchSeq != point.BatchSeq || header.ThroughSlot != point.ThroughSlot {
			continue
		}
		manifest, err := accountsdb.ReadSegmentManifest(header.Path)
		if err != nil {
			return nil, fmt.Errorf("read fold manifest for boundary %d: %w", point.ThroughSlot, err)
		}
		return manifest, nil
	}
	return nil, fmt.Errorf("fold manifest for boundary %d (batch %d) is missing", point.ThroughSlot, point.BatchSeq)
}

func validateTransactionStatusRewindPoint(accountsDbPath string, accountsDb *accountsdb.AccountsDb, point accountsdb.RewindPoint) error {
	manifest, err := rewindPointManifest(accountsDb, point)
	if err != nil {
		return err
	}
	_, err = decodeAndValidateDurableResumeContext(accountsDbPath, point.ThroughSlot, manifest.ResumeCtx)
	return err
}

// validTransactionStatusRewindPoints filters operator-visible boundaries. A
// corrupt historical sidecar reduces the usable rewind horizon; it never gets
// selected and discovered only after AccountsDB has already been changed.
func rewindPointsWithinHorizon(points []accountsdb.RewindPoint, horizon uint64) []accountsdb.RewindPoint {
	retainCount := horizon + 1 // H suffix batches plus the boundary below them.
	if retainCount == 0 || uint64(len(points)) <= retainCount {
		return points
	}
	return points[len(points)-int(retainCount):]
}

func validTransactionStatusRewindPoints(accountsDbPath string, accountsDb *accountsdb.AccountsDb, points []accountsdb.RewindPoint, horizon uint64) ([]accountsdb.RewindPoint, map[uint64]error) {
	points = rewindPointsWithinHorizon(points, horizon)
	valid := make([]accountsdb.RewindPoint, 0, len(points))
	rejected := make(map[uint64]error)
	for _, point := range points {
		if err := validateTransactionStatusRewindPoint(accountsDbPath, accountsDb, point); err != nil {
			rejected[point.ThroughSlot] = err
			continue
		}
		valid = append(valid, point)
	}
	return valid, rejected
}

func addTransactionStatusCheckpointRef(keep map[string]*state.TransactionStatusCheckpointRef, expectedRoot uint64, ref *state.TransactionStatusCheckpointRef) error {
	if err := replay.ValidateTransactionStatusCheckpointRef(ref, expectedRoot); err != nil {
		return err
	}
	copyRef := *ref
	keep[copyRef.File] = &copyRef
	return nil
}

func addTransactionStatusManifestRef(keep map[string]*state.TransactionStatusCheckpointRef, manifest *accountsdb.SegmentManifest) error {
	if manifest == nil {
		return errors.New("nil fold manifest")
	}
	if len(manifest.ResumeCtx) == 0 {
		return fmt.Errorf("fold manifest through slot %d carries no resume context", manifest.ThroughSlot)
	}
	var ctx struct {
		Slot                        uint64                                `json:"slot"`
		TransactionStatusCheckpoint *state.TransactionStatusCheckpointRef `json:"transaction_status_checkpoint,omitempty"`
	}
	if err := json.Unmarshal(manifest.ResumeCtx, &ctx); err != nil {
		return fmt.Errorf("decode fold manifest context through slot %d: %w", manifest.ThroughSlot, err)
	}
	if ctx.Slot != manifest.ThroughSlot {
		return fmt.Errorf("fold manifest through slot %d carries context for slot %d", manifest.ThroughSlot, ctx.Slot)
	}
	return addTransactionStatusCheckpointRef(keep, manifest.ThroughSlot, ctx.TransactionStatusCheckpoint)
}

// retainedTransactionStatusCheckpointRefs mirrors AccountsDB rewind pinning:
// H suffix batches require H+1 boundary checkpoints (the H committed batches
// plus their rewind target). Parked manifests are retained unconditionally so
// an interrupted rewind never loses the status side of its target selection.
// This collector validates references structurally but deliberately does not
// reread tens of megabytes per boundary on every fold; rewind selection and
// startup head adoption perform full size/SHA/decode validation.
func retainedTransactionStatusCheckpointRefs(accountsDb *accountsdb.AccountsDb, horizon uint64, current *state.TransactionStatusCheckpointRef) ([]*state.TransactionStatusCheckpointRef, error) {
	if accountsDb == nil {
		return nil, errors.New("collect transaction status checkpoints: AccountsDB is nil")
	}
	keep := make(map[string]*state.TransactionStatusCheckpointRef)
	if current != nil {
		if err := addTransactionStatusCheckpointRef(keep, current.Root, current); err != nil {
			return nil, fmt.Errorf("current transaction status checkpoint: %w", err)
		}
	}

	headers, err := accountsdb.ListFoldManifests(accountsDb.AcctsDir)
	if err != nil {
		return nil, fmt.Errorf("list retained fold manifests: %w", err)
	}
	retainCount := horizon + 1
	if retainCount == 0 { // uint64 overflow is defensive; configuration is tiny.
		retainCount = ^uint64(0)
	}
	start := 0
	if uint64(len(headers)) > retainCount {
		start = len(headers) - int(retainCount)
	}
	for _, header := range headers[start:] {
		manifest, err := accountsdb.ReadSegmentManifestContext(header.Path)
		if err != nil {
			return nil, fmt.Errorf("read in-horizon fold manifest %s: %w", header.Path, err)
		}
		if err := addTransactionStatusManifestRef(keep, manifest); err != nil {
			return nil, fmt.Errorf("in-horizon fold boundary %d is not rewind-safe: %w", manifest.ThroughSlot, err)
		}
	}

	// Parked manifests remain in the AccountsDB segment directory only while a
	// rewind is incomplete. Completed forensic manifests live under rewound/
	// and are no longer actionable, so they do not pin sidecars forever.
	entries, err := os.ReadDir(accountsDb.AcctsDir)
	if err != nil {
		return nil, fmt.Errorf("scan parked fold manifests: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".manifest.rewound") {
			continue
		}
		path := filepath.Join(accountsDb.AcctsDir, entry.Name())
		manifest, err := accountsdb.ReadSegmentManifest(path)
		if err != nil {
			return nil, fmt.Errorf("read parked fold manifest %s: %w", path, err)
		}
		if err := addTransactionStatusManifestRef(keep, manifest); err != nil {
			return nil, fmt.Errorf("parked fold boundary %d is not resumable: %w", manifest.ThroughSlot, err)
		}
	}

	refs := make([]*state.TransactionStatusCheckpointRef, 0, len(keep))
	for _, ref := range keep {
		refs = append(refs, ref)
	}
	return refs, nil
}

func cleanupRetainedTransactionStatusCheckpoints(accountsDbPath string, accountsDb *accountsdb.AccountsDb, horizon uint64, current *state.TransactionStatusCheckpointRef) ([]string, error) {
	keep, err := retainedTransactionStatusCheckpointRefs(accountsDb, horizon, current)
	if err != nil {
		return nil, err
	}
	return replay.CleanupTransactionStatusCheckpoints(accountsDbPath, keep)
}

// formatRewindPoints renders the retained fold boundaries for operator messages.
func formatRewindPoints(points []accountsdb.RewindPoint) string {
	if len(points) == 0 {
		return "(none checkpoint-valid within the configured rewind horizon)"
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
func adoptRewindResult(accountsDbPath string, s *state.MithrilState, res accountsdb.RewindResult) error {
	if len(res.ResumeCtx) == 0 {
		if res.NewThrough == s.LastRootedSlot && s.LastRootedContext != nil && s.LastRootedContext.Slot == res.NewThrough {
			return validateDurableResumeContext(accountsDbPath, res.NewThrough, s.LastRootedContext)
		}
		return fmt.Errorf("rewound to slot %d but its fold manifest carries no resume context", res.NewThrough)
	}
	rctx, err := decodeAndValidateDurableResumeContext(accountsDbPath, res.NewThrough, res.ResumeCtx)
	if err != nil {
		return fmt.Errorf("resume context at rewound boundary %d is unusable: %w", res.NewThrough, err)
	}
	s.LastRootedSlot = res.NewThrough
	s.LastRootedBankhash = rctx.Bankhash
	s.LastRootedContext = rctx
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
func completeInterruptedRewind(accountsDbPath string, accountsDb *accountsdb.AccountsDb, mithrilState *state.MithrilState) {
	points, err := accountsDb.ListRewindPoints()
	if err != nil || len(points) == 0 {
		klog.Fatalf("interrupted rewind detected but no retained fold boundary remains to complete it; re-bootstrap with --bootstrap snapshot")
	}
	// The rewind parks every batch above its target, so the highest still-present
	// (non-parked) boundary IS the target.
	targetPoint := points[len(points)-1]
	target := targetPoint.ThroughSlot
	if err := validateTransactionStatusRewindPoint(accountsDbPath, accountsDb, targetPoint); err != nil {
		klog.Fatalf("interrupted rewind target at slot %d has an invalid transaction-status checkpoint: %v; re-bootstrap with --bootstrap snapshot", target, err)
	}
	oldRooted := mithrilState.LastRootedSlot
	res, rerr := accountsDb.RewindToBatchBoundary(target)
	if rerr != nil {
		klog.Fatalf("could not complete interrupted rewind to slot %d: %v; re-run --rewind-to-slot=%d", target, rerr, target)
	}
	if aerr := adoptRewindResult(accountsDbPath, mithrilState, res); aerr != nil {
		klog.Fatalf("interrupted rewind at slot %d: %v; re-run --rewind-to-slot=%d", res.NewThrough, aerr, target)
	}
	mlog.Log.Warnf("completed interrupted rewind: durable state at fold boundary %d (state file said %d)", res.NewThrough, oldRooted)
}

// rewindStoreBelowDivergence rewinds durable state to the newest retained fold
// boundary strictly below divSlot and adopts that boundary's context as the
// rooted checkpoint. Returns false (caller halts) when no boundary below the
// divergence is retained or the rewind/reconcile fails.
func rewindStoreBelowDivergence(accountsDbPath string, accountsDb *accountsdb.AccountsDb, s *state.MithrilState, divSlot uint64, rewindHorizon uint64) bool {
	points, err := accountsDb.ListRewindPoints()
	if err != nil {
		mlog.Log.Errorf("fork switch: cannot list rewind boundaries: %v; halting", err)
		return false
	}
	points = rewindPointsWithinHorizon(points, rewindHorizon)
	target, found := uint64(0), false
	for index := len(points) - 1; index >= 0; index-- {
		point := points[index]
		if point.ThroughSlot >= divSlot {
			continue
		}
		if verr := validateTransactionStatusRewindPoint(accountsDbPath, accountsDb, point); verr != nil {
			mlog.Log.Warnf("fork switch: skipping rewind boundary %d because its transaction-status checkpoint is invalid: %v", point.ThroughSlot, verr)
			continue
		}
		target, found = point.ThroughSlot, true
		break
	}
	if !found {
		mlog.Log.Errorf("fork switch: superseded slot %d is at/below the durable watermark %d and no fold boundary below it is retained (rewind horizon exceeded) — re-bootstrap with --bootstrap snapshot; halting",
			divSlot, s.LastRootedSlot)
		return false
	}
	oldRooted := s.LastRootedSlot
	res, err := accountsDb.RewindToBatchBoundary(target)
	if err != nil {
		mlog.Log.Errorf("fork switch: rewind to boundary %d failed: %v; halting", target, err)
		return false
	}
	if err := adoptRewindResult(accountsDbPath, s, res); err != nil {
		mlog.Log.Errorf("fork switch: %v; halting", err)
		return false
	}
	mlog.Log.Warnf("fork switch: superseded slot %d was already folded (R=%d); rewound durable state to boundary %d (%d batches / %d keys undone)",
		divSlot, oldRooted, res.NewThrough, res.UndoneBatches, res.UndoneKeys)
	return true
}

// maxForkSwitchRetries bounds fork-aware dump-then-repair re-replays per run.
const maxForkSwitchRetries = 3

const (
	maxSanitizedPanicValueBytes = 4 * 1024
	maxSanitizedPanicStackBytes = 64 * 1024
)

type replayPanic struct {
	Value string
	Stack string
}

// captureSanitizedPanic records the faulting stack while the original panic is
// active. The caller can log this bounded, sanitized diagnostic after the
// original panic has fully unwound, then re-panic with only the safe value.
func captureSanitizedPanic(run func()) (captured replayPanic, recovered bool) {
	func() {
		defer func() {
			if r := recover(); r != nil {
				captured.Value = truncatePanicText(rpcclient.SanitizeTextForDisplay(fmt.Sprint(r)), maxSanitizedPanicValueBytes)
				captured.Stack = sanitizePanicStack(debug.Stack())
				recovered = true
			}
		}()
		run()
	}()
	return captured, recovered
}

func sanitizePanicStack(raw []byte) string {
	lines := strings.Split(string(raw), "\n")
	for i := range lines {
		lines[i] = rpcclient.SanitizeTextForDisplay(lines[i])
	}
	return truncatePanicText(strings.Join(lines, "\n"), maxSanitizedPanicStackBytes)
}

func truncatePanicText(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(value) <= maxBytes {
		return value
	}
	const suffix = "\n...[truncated]"
	if maxBytes <= len(suffix) {
		return suffix[:maxBytes]
	}
	end := maxBytes - len(suffix)
	for end > 0 && !utf8.ValidString(value[:end]) {
		end--
	}
	return value[:end] + suffix
}

func reportReplayPanic(captured replayPanic, accountsDbPath string, mithrilState *state.MithrilState) {
	runID := replay.CurrentRunID
	if replay.IsCommitInProgress() {
		commitSlot := replay.GetCommitSlot()
		mlog.Log.Errorf("[run:%s] FATAL: Panic during commit at slot %d - AccountsDB may be corrupted", runID, commitSlot)
		mlog.Log.Errorf("[run:%s] Panic occurred between StoreAccounts and StoreBankHashForSlot", runID)
		if mithrilState != nil {
			reason := fmt.Sprintf("panic during commit at slot %d: %s", commitSlot, captured.Value)
			if err := mithrilState.MarkCorrupted(accountsDbPath, reason); err != nil {
				mlog.Log.Errorf("[run:%s] Failed to mark state as corrupted: %v", runID, err)
			} else {
				state.RecordCorrupted(accountsDbPath, mithrilState.LastSlot, mithrilState.LastBankhash, runID, getVersion(), getCommit(), getBranch(), reason)
			}
		}
	} else {
		mlog.Log.Errorf("[run:%s] Panic during replay (outside commit window) - AccountsDB is SAFE", runID)
		mlog.Log.Errorf("[run:%s] This appears to be a divergence panic. You can resume from the last persisted slot.", runID)
	}
	if captured.Stack != "" {
		mlog.Log.Errorf("[run:%s] Sanitized panic stack:\n%s", runID, captured.Stack)
	}

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
	panic(captured.Value)
}

// runReplayWithRecovery wraps replay.ReplayBlocks with panic recovery.
// It distinguishes between:
// - Panics during commit (commitInProgress=true): AccountsDB may be corrupted, marks state as corrupted
// - Panics outside commit (divergence): AccountsDB is safe, logs helpful message
// After handling, it re-panics with a sanitized value.
func runReplayWithRecovery(
	ctx context.Context,
	accountsDb *accountsdb.AccountsDb,
	accountsDbPath string,
	manifest *snapshot.SnapshotManifest,
	resumeState *replay.ResumeState,
	startSlot, endSlot uint64,
	rpcEndpoints []string,
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
	rewindHorizon uint64,
	replayStartTime time.Time,
) *replay.ReplayResult {
	var result *replay.ReplayResult
	captured, recovered := captureSanitizedPanic(func() {
		result = runReplayWithForkRecovery(
			ctx, accountsDb, accountsDbPath, manifest, resumeState,
			startSlot, endSlot, rpcEndpoints, lightbringerEndpoint,
			turbineBindAddr, turbineGossipEntrypoint, turbineGossipBindAddr,
			turbineAdvertisedIP, turbineShredVersion, turbineAlpenglowAddr,
			turbineIdentity, blockDir, txParallelism, isLive, useLightbringer,
			useTurbine, dbgOpts, metricsWriter, rpcServer, mithrilState,
			blockFetchOpts, consensusOpts, rewindHorizon, replayStartTime,
		)
	})
	if recovered {
		reportReplayPanic(captured, accountsDbPath, mithrilState)
	}
	return result
}

func runReplayWithForkRecovery(
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
	rewindHorizon uint64,
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

	result = replay.ReplayBlocks(ctx, accountsDb, accountsDbPath, mithrilState, resumeState, startSlot, endSlot, rpcEndpoints, lightbringerEndpoint, turbineBindAddr, turbineGossipEntrypoint, turbineGossipBindAddr, turbineAdvertisedIP, turbineShredVersion, turbineAlpenglowAddr, turbineIdentity, blockDir, txParallelism, isLive, useLightbringer, useTurbine, dbgOpts, metricsWriter, rpcServer, blockFetchOpts, consensusOpts, onCancelWriteState)
	if consensusOpts == nil || !consensusOpts.Alpenglow {
		return result
	}

	// Fork-aware dump-then-repair: when an Alpenglow branch switch cannot unwind
	// safely in RAM, or execution disagrees with finality, re-replay the selected
	// chain from the durable rooted checkpoint. A branch switch is normal
	// speculative-fork resolution, not an execution or bank-hash divergence.
	// Repeating the same selection after replay means recovery did not converge and
	// fails closed. The retry budget resets when the rooted watermark advances.
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
		var recoveryReason string
		var certSwitch *replay.CertifiedSwitch
		var finMismatch *replay.AlpenglowFinalityMismatch
		switch {
		case errors.As(result.Error, &certSwitch):
			// Execute-on-receipt ran the wrong sibling (or a certified-skipped
			// slot), or a later parent-linked child exposed a competing
			// speculative suffix; the selected version replays from the rooted
			// checkpoint when the in-RAM unwind was guarded out.
			divSlot = certSwitch.Slot
			key = fmt.Sprintf("sw:%d/%x/%x/%v/%v/%d/%x", certSwitch.Slot, certSwitch.Executed, certSwitch.Certified,
				certSwitch.Skip, certSwitch.ParentLinked, certSwitch.ChildSlot, certSwitch.ChildID)
			switch {
			case certSwitch.ParentLinked:
				recoveryReason = fmt.Sprintf("parent-linked branch switch at slot %d (child %s at slot %d links to parent %s at slot %d)",
					certSwitch.Slot, certSwitch.ChildID, certSwitch.ChildSlot, certSwitch.ParentID, certSwitch.ParentSlot)
			case certSwitch.Skip:
				recoveryReason = fmt.Sprintf("skip certificate replaced the locally executed block at slot %d", certSwitch.Slot)
			default:
				recoveryReason = fmt.Sprintf("certificate selected a different block identity at slot %d (executed=%s selected=%s)",
					certSwitch.Slot, certSwitch.Executed, certSwitch.Certified)
			}
		case errors.As(result.Error, &finMismatch):
			if finMismatch.Conflict {
				// Conflict-shaped (equivocation evidence, not a wrong local block):
				// re-replay cannot help — halt without burning the retry budget.
				mlog.Log.Errorf("fork switch: consensus conflict at slot %d is equivocation evidence — not retryable; halting", finMismatch.Slot)
			} else {
				divSlot = finMismatch.Slot
				key = fmt.Sprintf("ag:%d/%x/%x", finMismatch.Slot, finMismatch.Executed, finMismatch.Finalized)
				recoveryReason = fmt.Sprintf("confirmed finality mismatch at slot %d (executed=%s finalized=%s)",
					finMismatch.Slot, finMismatch.Executed, finMismatch.Finalized)
			}
		}
		if key == "" { // neither retryable branch-switch/finality-mismatch type
			break
		}
		if seenDiv[key] {
			if certSwitch != nil {
				mlog.Log.Errorf("fork switch: repeated branch selection at slot %d after re-replay — recovery did not converge; halting", divSlot)
			} else {
				mlog.Log.Errorf("fork switch: repeated finality mismatch at slot %d — deterministic replay divergence; halting", divSlot)
			}
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
			if !rewindStoreBelowDivergence(accountsDbPath, accountsDb, mithrilState, divSlot, rewindHorizon) {
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
		mlog.Log.Warnf("fork switch: %s; re-replaying the selected chain from rooted slot %d (attempt %d/%d)",
			recoveryReason, retryStart-1, attempt, maxForkSwitchRetries)
		result = replay.ReplayBlocks(ctx, accountsDb, accountsDbPath, mithrilState, rs, retryStart, endSlot, rpcEndpoints, lightbringerEndpoint, turbineBindAddr, turbineGossipEntrypoint, turbineGossipBindAddr, turbineAdvertisedIP, turbineShredVersion, turbineAlpenglowAddr, turbineIdentity, blockDir, txParallelism, isLive, useLightbringer, useTurbine, dbgOpts, metricsWriter, rpcServer, blockFetchOpts, consensusOpts, onCancelWriteState)
	}
	return result
}
