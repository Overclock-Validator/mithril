package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/shredcap"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/spf13/cobra"
)

// replayPhaseTimers mirrors --replay-phase-timers.
var replayPhaseTimers = true

// backtestMode is set while `mithril backtest` drives the node in-process.
var backtestMode bool

// lastReplayResult is the replay result of the most recent runLive, read by
// backtest to decide its exit status.
var lastReplayResult *replay.ReplayResult

var bt struct {
	capture, snapshot, incremental, accounts, workdir string
	cluster, genesisHash                              string
	endSlot, measureFrom                              uint64
	warmup, txpar                                     int
	verifySignatures, phaseTimers                     bool
}

// Backtest replays a shredcap capture offline, the way firedancer-dev
// backtest does: state from snapshot files (or an existing AccountsDB at the
// capture's checkpoint), blocks only from the capture, every bank hash checked
// against the capture, then a timing summary.
var Backtest = cobra.Command{
	Use:   "backtest",
	Short: "Replay a shredcap capture offline and report replay timing",
	Long: `Replay a Firedancer shredcap capture (.pcapng or .pcapng.zst) with no network.

Settings come from flags or from a backtest configuration file (--config FILE,
laid out like firedancer-dev backtest's; see backtest.example.toml). Flags
override the file.

The node starts from local snapshot files (--snapshot, --incremental) or from
an existing AccountsDB that holds the capture's starting slot (--accounts),
replays every block in the capture, stops at the first block whose bank hash
differs from the capture's record (exit status 1), and prints a timing summary.
All files it writes go under --workdir. Repeat runs reuse the workdir: the
imported capture is kept and the AccountsDB is rewound instead of rebuilt.`,
	Args: cobra.NoArgs,
	RunE: runBacktest,
}

func init() {
	f := Backtest.Flags()
	f.StringVar(&bt.capture, "capture", "", "Shredcap capture to replay (.pcapng or .pcapng.zst)")
	f.StringVar(&bt.snapshot, "snapshot", "", "Full snapshot archive to start from (snapshot-<slot>-<hash>.tar.zst)")
	f.StringVar(&bt.incremental, "incremental", "", "Incremental snapshot archive on top of --snapshot")
	f.StringVar(&bt.accounts, "accounts", "", "Existing AccountsDB at (or rewindable to) the capture's starting slot, instead of --snapshot")
	f.StringVar(&bt.workdir, "workdir", "", "Directory for everything the backtest writes (created if missing; reused across runs)")
	f.StringVar(&bt.cluster, "cluster", "mainnet", "Cluster the capture comes from (mainnet, testnet, devnet, or a custom name with --genesis-hash)")
	f.StringVar(&bt.genesisHash, "genesis-hash", "", "Genesis hash of a custom cluster")
	f.Uint64Var(&bt.endSlot, "end-slot", 0, "Last slot to replay (0 = the capture's last block)")
	f.Uint64Var(&bt.measureFrom, "measure-from", 0, "Leave blocks before this slot out of the timing summary")
	f.IntVar(&bt.warmup, "warmup", 30, "Leading measured blocks left out of the timing summary")
	f.IntVar(&bt.txpar, "txpar", 8, "Transaction execution workers")
	f.BoolVar(&bt.phaseTimers, "phase-timers", false, "Time each transaction phase and instruction dispatch, as a live node does (about 1% of SlotReplay; SlotReplay and TxLoop are timed either way)")
	f.BoolVar(&bt.verifySignatures, "verify-shred-signatures", false, "Check every shred's leader signature as replay reads it (off by default, like firedancer-dev backtest; adds signature work to the timed run)")
}

func runBacktest(cmd *cobra.Command, _ []string) error {
	started := time.Now()
	cmd.SilenceUsage = true // a refusal is a message, not a usage error
	// --config names a backtest configuration file (never a node config);
	// flags override what it sets.
	userConfig := config.ConfigFile
	if userConfig != "" {
		f, err := loadBacktestFile(userConfig)
		if err != nil {
			return err
		}
		applyBacktestFile(f, userConfig, cmd.Flags())
	}
	if err := validateBacktestOptions(); err != nil {
		return err
	}
	clusterInfo, err := config.ResolveCluster(bt.cluster, bt.genesisHash)
	if err != nil {
		return err
	}
	workdir, err := filepath.Abs(bt.workdir)
	if err != nil {
		return err
	}
	marker, err := openBacktestWorkdir(workdir)
	if err != nil {
		return err
	}
	fp, err := fingerprintCapture(bt.capture)
	if err != nil {
		return err
	}

	var snapshotSlot uint64
	if bt.snapshot != "" {
		if snapshotSlot, err = backtestSnapshotSlot(bt.snapshot, bt.incremental); err != nil {
			return err
		}
		for _, p := range []string{bt.snapshot, bt.incremental} {
			if p == "" {
				continue
			}
			if _, err := os.Stat(p); err != nil {
				return err
			}
		}
	}
	// Firedancer skips capture shreds at or below the snapshot slot; the
	// import keeps only the slots after it.
	fromSlot := uint64(0)
	if snapshotSlot > 0 {
		fromSlot = snapshotSlot + 1
	}
	man, err := backtestCorpus(cmd, workdir, &marker, fp, fromSlot)
	if err != nil {
		return err
	}
	if snapshotSlot > 0 && man.Checkpoint != snapshotSlot {
		if man.Checkpoint > snapshotSlot {
			return fmt.Errorf("snapshot too old for capture: the capture's first block after slot %d builds on slot %d, and the blocks between are not in the capture", snapshotSlot, man.Checkpoint)
		}
		return fmt.Errorf("the capture's first block after the snapshot (slot %d) builds on slot %d, not on the snapshot slot %d", man.Slots[0], man.Checkpoint, snapshotSlot)
	}

	store := bt.accounts
	builtAt := uint64(0)
	if store == "" {
		store = filepath.Join(workdir, "accounts")
		if marker.SnapshotSlot != 0 && marker.SnapshotSlot != snapshotSlot {
			return fmt.Errorf("%s was built from a snapshot at slot %d, not %d; use a new --workdir", store, marker.SnapshotSlot, snapshotSlot)
		}
		builtAt = marker.SnapshotSlot
	} else if store, err = filepath.Abs(store); err != nil {
		return err
	}
	view, err := backtestStoreView(store)
	if err != nil {
		return err
	}
	plan, err := planBacktestStart(man.Checkpoint, snapshotSlot > 0, builtAt, view)
	if err != nil {
		return err
	}
	if plan.Note != "" {
		fmt.Fprintln(cmd.OutOrStdout(), plan.Note)
	}
	if plan.RewindTo != 0 && bt.accounts == "" {
		// Rewound data in the workdir's own store is never needed again.
		_ = os.RemoveAll(filepath.Join(store, "accounts", "rewound"))
	}

	expected, err := captureBankHashes(man)
	if err != nil {
		return err
	}
	head := man.Head
	if bt.endSlot != 0 {
		if bt.endSlot < plan.FirstSlot || bt.endSlot > man.Head {
			return fmt.Errorf("--end-slot %d is outside the capture's replayable range %d..%d", bt.endSlot, plan.FirstSlot, man.Head)
		}
		head = bt.endSlot
	}
	blocks := 0
	for _, s := range man.Slots {
		if s >= plan.FirstSlot && s <= head {
			blocks++
		}
	}

	shredDir := filepath.Join(workdir, "shred")
	if _, err := prepareBacktestShredStore(filepath.Join(workdir, "corpus"), shredDir, clusterInfo); err != nil {
		return fmt.Errorf("restore the capture into the shred spool: %w", err)
	}
	cfgPath := filepath.Join(workdir, "backtest.toml")
	horizon := max(64, len(man.Slots)/128+16)
	if err := os.WriteFile(cfgPath, []byte(backtestConfig(clusterInfo, store, workdir, man.ShredVersion, bt.txpar, horizon)), 0o644); err != nil {
		return err
	}
	for _, dir := range []string{"logs", "scratch", "snapshots"} {
		if err := os.MkdirAll(filepath.Join(workdir, dir), 0o755); err != nil {
			return err
		}
	}

	flags := map[string]string{
		"bootstrap":             plan.Bootstrap,
		"end-slot":              strconv.FormatUint(head+1, 10), // the block source stops before end-slot
		"txpar":                 strconv.Itoa(bt.txpar),
		"streaming-execution":   "true",
		"offline":               "true",
		"turbine-shred-version": strconv.Itoa(int(man.ShredVersion)),
		"scratch-directory":     filepath.Join(workdir, "scratch"),
		"replay-phase-timers":   strconv.FormatBool(bt.phaseTimers),
	}
	if plan.Bootstrap == "snapshot" {
		flags["snapshot"] = bt.snapshot
		if bt.incremental != "" {
			flags["incremental-snapshot"] = bt.incremental
		}
	}
	if plan.RewindTo != 0 {
		flags["rewind-to-slot"] = strconv.FormatUint(plan.RewindTo, 10)
	}
	for name, value := range flags {
		if err := Run.Flags().Set(name, value); err != nil {
			return fmt.Errorf("set --%s: %w", name, err)
		}
	}
	config.ConfigFile = cfgPath
	if err := initConfigAndBindFlags(&Run); err != nil {
		return err
	}
	backtestMode = true
	replay.CaptureBankHashes = expected
	logsBefore := listLogDirs(filepath.Join(workdir, "logs"))
	fmt.Fprintf(cmd.OutOrStdout(), "backtest: %d blocks, slots %d..%d, from %s at slot %d\n", blocks, plan.FirstSlot, head, plan.Bootstrap, plan.FirstSlot-1)

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var rejected struct {
		sync.Mutex
		err error
	}
	if bt.verifySignatures {
		turbine.VerifySpooledShreds = true
		turbine.OnSpooledShredRejected = func(slot uint64, err error) {
			rejected.Lock()
			defer rejected.Unlock()
			if rejected.err == nil {
				rejected.err = fmt.Errorf("shred signature check failed: %w", err)
				cancel() // the slot cannot complete; stop instead of waiting for it
			}
		}
	}
	Run.SetContext(ctx) // runLive derives its run context from the command
	runLive(&Run, nil)
	rejected.Lock()
	rejectErr := rejected.err
	rejected.Unlock()
	if rejectErr != nil {
		return rejectErr
	}

	if plan.Bootstrap == "snapshot" {
		marker.SnapshotSlot = snapshotSlot
		if err := writeBacktestMarker(workdir, marker); err != nil {
			return err
		}
	}
	result := lastReplayResult
	if result == nil {
		return errors.New("replay did not run")
	}
	if result.Error != nil {
		if errors.Is(result.Error, replay.ErrCaptureBankHash) {
			return result.Error
		}
		return fmt.Errorf("replay stopped: %w", result.Error)
	}
	logDir := newestLogDir(filepath.Join(workdir, "logs"), logsBefore)
	if logDir == "" {
		return errors.New("replay wrote no timing log")
	}
	rows, err := readTimingRows(filepath.Join(logDir, "replay_timings.jsonl"))
	if err != nil {
		return err
	}
	if len(rows) == 0 || rows[len(rows)-1].Slot != head {
		last := uint64(0)
		if len(rows) > 0 {
			last = rows[len(rows)-1].Slot
		}
		return fmt.Errorf("replay stopped at slot %d before the capture's last block %d", last, head)
	}
	summary := summarizeBacktest(rows, bt.warmup, bt.measureFrom)
	settings := backtestSettings(man)
	if userConfig != "" {
		settings = append([][2]string{{"config", userConfig}}, settings...)
	}
	summary.write(cmd.OutOrStdout(), settings, time.Since(started).Seconds())
	return nil
}

// backtestCorpus imports the capture into workdir/corpus, or reuses an
// earlier import of the same capture and slot range.
func backtestCorpus(cmd *cobra.Command, workdir string, marker *backtestMarker, fp captureFingerprint, fromSlot uint64) (*shredcap.Manifest, error) {
	corpusDir := filepath.Join(workdir, "corpus")
	if marker.Capture == fp && marker.FromSlot == fromSlot {
		if man, err := shredcap.ReadManifest(corpusDir); err == nil {
			return man, nil
		}
	}
	if marker.Capture != fp && marker.SnapshotSlot != 0 {
		return nil, fmt.Errorf("%s holds runs of capture %s; use a new --workdir for %s", workdir, marker.Capture.Name, fp.Name)
	}
	if err := os.RemoveAll(corpusDir); err != nil {
		return nil, err
	}
	in, err := os.Open(bt.capture)
	if err != nil {
		return nil, err
	}
	defer in.Close()
	fmt.Fprintf(cmd.OutOrStdout(), "importing %s\n", fp.Name)
	man, stats, err := shredcap.ImportCorpus(in, corpusDir, shredcap.ImportOptions{FromSlot: fromSlot, Source: fp.Name})
	if err != nil {
		return nil, err
	}
	if len(stats.NoBankHash) > 0 {
		return nil, fmt.Errorf("no bank hash available for slot %d (and %d more) in the capture", stats.NoBankHash[0], len(stats.NoBankHash)-1)
	}
	if len(stats.ParentBreaks) > 0 {
		return nil, fmt.Errorf("the capture's chain breaks at slot %d: its parent is not the previous replayable slot", stats.ParentBreaks[0])
	}
	fmt.Fprintf(cmd.OutOrStdout(), "imported %d blocks, slots %d..%d, checkpoint %d, shred version %d (%d slots recovered from coding shreds)\n",
		man.Blocks, man.Slots[0], man.Head, man.Checkpoint, man.ShredVersion, len(stats.RecoveredSlots))
	marker.Capture, marker.FromSlot = fp, fromSlot
	return man, writeBacktestMarker(workdir, *marker)
}

func captureBankHashes(man *shredcap.Manifest) (map[uint64][32]byte, error) {
	out := make(map[uint64][32]byte, len(man.BankHashes))
	for slot, encoded := range man.BankHashes {
		s, err := strconv.ParseUint(slot, 10, 64)
		if err != nil {
			return nil, err
		}
		h, err := solana.HashFromBase58(encoded)
		if err != nil {
			return nil, fmt.Errorf("slot %s bank hash: %w", slot, err)
		}
		out[s] = h
	}
	return out, nil
}

func backtestStoreView(store string) (storeView, error) {
	var v storeView
	st, err := state.LoadState(store)
	if err != nil || st == nil {
		return v, nil
	}
	v.HasState, v.StateSlot = true, st.LastRootedSlot
	headers, err := accountsdb.ListFoldManifests(filepath.Join(store, "accounts"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return v, err
	}
	for _, h := range headers {
		if h.BatchSeq > 0 {
			v.Boundaries = append(v.Boundaries, h.ThroughSlot)
		}
	}
	sort.Slice(v.Boundaries, func(i, j int) bool { return v.Boundaries[i] < v.Boundaries[j] })
	return v, nil
}

func listLogDirs(dir string) map[string]bool {
	out := make(map[string]bool)
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		out[e.Name()] = true
	}
	return out
}

// newestLogDir returns the run directory this run created under dir.
func newestLogDir(dir string, before map[string]bool) string {
	entries, _ := os.ReadDir(dir)
	var names []string
	for _, e := range entries {
		if e.IsDir() && !before[e.Name()] {
			names = append(names, e.Name())
		}
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	return filepath.Join(dir, names[len(names)-1])
}

func backtestSettings(man *shredcap.Manifest) [][2]string {
	limit := "none"
	if l := debug.SetMemoryLimit(-1); l != int64(^uint64(0)>>1) {
		limit = fmt.Sprintf("%d MiB", l>>20)
	}
	asm := os.Getenv("MITHRIL_SBPF_ASM_LOOP")
	if asm == "" {
		asm = "off (unset)"
	}
	return [][2]string{
		{"commit", getCommit()},
		{"go", runtime.Version()},
		{"GOMAXPROCS", strconv.Itoa(runtime.GOMAXPROCS(0))},
		{"usable CPUs", strconv.Itoa(runtime.NumCPU())},
		{"txpar", strconv.Itoa(bt.txpar)},
		{"phase timers", map[bool]string{true: "on (--phase-timers, as a live node)", false: "off (backtest default)"}[bt.phaseTimers]},
		{"shred signatures", map[bool]string{true: "checked (--verify-shred-signatures)", false: "not checked (as firedancer-dev backtest)"}[bt.verifySignatures]},
		{"MITHRIL_SBPF_ASM_LOOP", asm},
		{"memory limit", limit},
		{"shred version", strconv.Itoa(int(man.ShredVersion))},
	}
}

// backtestConfig is the node configuration a backtest runs with: the corpus
// benchmark settings (compaction, verifier and RPC server off; the same cache
// and arena sizes), offline with no RPC endpoint, every path under the workdir
// or --accounts.
func backtestConfig(cluster config.Cluster, store, workdir string, shredVersion uint16, txpar, horizon int) string {
	var b strings.Builder
	q := strconv.Quote
	fmt.Fprintf(&b, "# Generated by mithril backtest. Rewritten on every run.\nname = \"mithril-backtest\"\n\n")
	fmt.Fprintf(&b, "[storage]\naccounts = %s\nsnapshots = %s\nlogs = %s\nshredstore = %s\nmin_free_mb = 2048\nfold_batch_slots = 128\nrewind_horizon_batches = %d\n\n",
		q(store), q(filepath.Join(workdir, "snapshots")), q(filepath.Join(workdir, "logs")), q(filepath.Join(workdir, "shred")), horizon)
	fmt.Fprintf(&b, "[storage.compact]\nenabled = false\n\n")
	fmt.Fprintf(&b, "[network]\ncluster = %s\noffline = true\n", q(cluster.Name))
	if cluster.Custom {
		fmt.Fprintf(&b, "genesis_hash = %s\n", q(cluster.GenesisHash))
	}
	fmt.Fprintf(&b, "\n[consensus]\nprotocol = \"alpenglow\"\nmode = \"verifying\"\n\n")
	fmt.Fprintf(&b, "[block]\nsource = \"turbine\"\nrpc_fallback = false\nrepair_catchup_max_gap_slots = 100000\nrepair_max_requests_per_second = 0\n\n")
	fmt.Fprintf(&b, "[turbine]\nshred_version = %d\n\n", shredVersion)
	fmt.Fprintf(&b, "[lightbringer]\nenabled = false\n\n[rpc]\nport = 0\n\n[verifier]\nenabled = false\nrequired = false\n\n")
	fmt.Fprintf(&b, "[tuning]\nstreaming_execution = true\ntxpar = %d\nsnapshot_index_shards = 256\nmax_concurrent_flushers = 8\n", txpar)
	fmt.Fprintf(&b, "snapshot_append_vec_workers = 32\nsnapshot_index_builder_workers = 64\nsnapshot_index_committer_workers = 64\n")
	fmt.Fprintf(&b, "param_arena_size_mb = 512\nborrowed_account_arena_size = 1024\nuse_pool = true\nprogram_cache_max_mb = 1024\n")
	fmt.Fprintf(&b, "common_account_cache_max_mb = 768\nindex_cache_max_mb = 256\ncommon_account_cache_layout = \"fixed\"\n\n")
	fmt.Fprintf(&b, "[snapshot]\nmax_full_snapshots = 1\n\n[log]\nlevel = \"info\"\nto_stdout = false\n")
	return b.String()
}
