package node

import (
	"bufio"
	"context"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/snapshot"
	"github.com/Overclock-Validator/mithril/pkg/snapshotdl"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

const (
	// Default RPC endpoint
	defaultRpcEndpoint              = "https://api.mainnet-beta.solana.com"
	minPort                         = 0
	maxPort                         = 65535
	defaultParamArenaSizeMB         = 512
	defaultBorrowedAccountArenaSize = 1024
	defaultMaxConcurrentFlushers    = 16
	defaultTmpDir                   = "/tmp"
	portDisabled                    = 0
	portNotSet                      = -1
	slotNotSet                      = -1
	manifestFilename                = "manifest"
)

var (
	Verifier = cobra.Command{
		Use:   "verifier",
		Short: "Run mithril verifier node",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			cmd.SetContext(ctx)
			defer cancel()
			runVerifier(cmd, args)
		},
	}

	Catchup = cobra.Command{
		Use:   "catchup",
		Short: "Catchup and run live",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			cmd.SetContext(ctx)
			defer cancel()
			runCatchup(cmd, args)
		},
	}

	loadFromSnapshot            bool
	loadFromAccountsDb          bool
	path                        string
	incrementalSnapshotFilename string
	outputDir                   string
	scratchDir                  string
	rpcEndpoint                 string
	rpcEndpointFile             string
	snapshotDlPath              string
	numReplaySlots              int64
	endSlot                     int64
	pprofPort                   int64
	blockDir                    string
	txParallelism               int64

	debugTxs        []string
	debugAcctWrites []string
	metricsFilename string
	cpuprofFilename string

	paramArenaSizeMB         uint64
	borrowedAccountArenaSize uint64

	rpcPort int
)

func init() {
	// flags for verifier mode
	Verifier.Flags().BoolVarP(&loadFromSnapshot, "snapshot", "s", false, "Load from a full snapshot")
	Verifier.Flags().BoolVarP(&loadFromAccountsDb, "accountsdb", "a", false, "Load from AccountsDB")
	Verifier.Flags().StringVarP(&path, "path", "p", "", "Path of full snapshot or AccountsDB to load from")
	Verifier.Flags().StringVar(&incrementalSnapshotFilename, "incremental-snapshot-filename", "", "Filename containing incremental snapshot")
	Verifier.Flags().StringVarP(&outputDir, "out", "o", "", "Output path for writing AccountsDB data to")
	Verifier.Flags().StringVarP(&rpcEndpoint, "rpc", "r", "", "URL for RPC endpoint")
	Verifier.Flags().Int64Var(&numReplaySlots, "num-replay-slots", 0, "Number of slots to replay.")
	Verifier.Flags().Int64VarP(&endSlot, "endslot", "e", slotNotSet, "Block at which to stop replaying, inclusive")
	Verifier.Flags().Int64Var(&pprofPort, "pprofport", portNotSet, "Port to serve HTTP pprof endpoint")
	Verifier.Flags().StringVar(&blockDir, "blockdir", "", "Path containing slot.json files")
	Verifier.Flags().Int64Var(&txParallelism, "txpar", 0, "Set to 0 to use sequential execution, or >0 to execute a topsort tx plan with the given number of workers")
	Verifier.Flags().StringSliceVar(&debugTxs, "debugtx", []string{}, "Pass tx signature strings to enable debug logging during that transaction's execution")
	Verifier.Flags().StringSliceVar(&debugAcctWrites, "debugacctwrites", []string{}, "Pass account pubkeys to enable debug logging of transactions that modify the account")
	Verifier.Flags().StringVar(&metricsFilename, "metrics-filename", "", "Filename to write JSONL records of latencies")
	Verifier.Flags().StringVar(&cpuprofFilename, "cpuprof-filename", "", "Filename to write CPU profile")
	Verifier.Flags().Uint64Var(&paramArenaSizeMB, "param-arena-size-mb", defaultParamArenaSizeMB, "Size in MB for serialized parameter arena (0 to disable)")
	Verifier.Flags().Uint64Var(&borrowedAccountArenaSize, "borrowed-account-arena-size", defaultBorrowedAccountArenaSize, "Number of borrowed accounts to preallocate in arena (0 to disable)")
	Verifier.Flags().IntVar(&snapshot.ZstdDecoderConcurrency, "zstd-decoder-concurrency", runtime.NumCPU(), "Zstd decoder concurrency")
	Verifier.Flags().IntVar(&snapshot.MaxConcurrentFlushers, "max-concurrent-flushers", defaultMaxConcurrentFlushers, "Bound for number of log shards to flush to Accounts DB Index at once.")
	Verifier.Flags().BoolVar(&sbpf.UsePool, "use-pool", true, "Disable to allocate fresh slices")
	Verifier.Flags().StringVar(&snapshotDlPath, "download-snapshot", "", "Path to download snapshot to")
	Verifier.Flags().IntVar(&rpcPort, "rpc-server-port", 0, "RPC server port. Default off.")

	// flags for catchup mode
	Catchup.Flags().StringVarP(&outputDir, "out", "o", "", "Output path for writing AccountsDB data to")
	Catchup.Flags().StringVarP(&rpcEndpoint, "rpc", "r", "", "URL for RPC endpoint")
	Catchup.Flags().StringVarP(&rpcEndpointFile, "rpc-node-list", "n", "", "Path for RPC node list file")
	Catchup.Flags().Int64Var(&txParallelism, "txpar", 0, "Set to 0 to use sequential execution, or >0 to execute a topsort tx plan with the given number of workers")
	Catchup.Flags().StringSliceVar(&debugTxs, "debugtx", []string{}, "Pass tx signature strings to enable debug logging during that transaction's execution")
	Catchup.Flags().StringSliceVar(&debugAcctWrites, "debugacctwrites", []string{}, "Pass account pubkeys to enable debug logging of transactions that modify the account")
	Catchup.Flags().StringVar(&metricsFilename, "metrics-filename", "", "Filename to write JSONL records of latencies")
	Catchup.Flags().StringVar(&cpuprofFilename, "cpuprof-filename", "", "Filename to write CPU profile")
	Catchup.Flags().Uint64Var(&paramArenaSizeMB, "param-arena-size-mb", defaultParamArenaSizeMB, "Size in MB for serialized parameter arena (0 to disable)")
	Catchup.Flags().Uint64Var(&borrowedAccountArenaSize, "borrowed-account-arena-size", defaultBorrowedAccountArenaSize, "Number of borrowed accounts to preallocate in arena (0 to disable)")
	Catchup.Flags().IntVar(&snapshot.ZstdDecoderConcurrency, "zstd-decoder-concurrency", runtime.NumCPU(), "Zstd decoder concurrency")
	Catchup.Flags().IntVar(&snapshot.MaxConcurrentFlushers, "max-concurrent-flushers", defaultMaxConcurrentFlushers, "Bound for number of log shards to flush to Accounts DB Index at once.")
	Catchup.Flags().BoolVar(&sbpf.UsePool, "use-pool", true, "Disable to allocate fresh slices")
	Catchup.Flags().StringVar(&blockDir, "blockdir", defaultTmpDir, "Path containing slot.json files")
	Catchup.Flags().StringVar(&scratchDir, "scratchdir", defaultTmpDir, "Path for downloads (e.g. snapshots) and other temp state")
	Catchup.Flags().IntVar(&rpcPort, "rpc-server-port", 0, "RPC server port. Default off.")
}

func runVerifier(c *cobra.Command, args []string) {
	if pprofPort != portNotSet {
		startPprofHandlers(int(pprofPort))
	}
	if endSlot != slotNotSet && numReplaySlots != 0 {
		klog.Fatalf("specify at most one of --endslot and --num-replay-slots")
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
		klog.Fatalf("failed to parse --debugtx or --debugacctwrites values: %v", err)
	}

	logVCSInfo()
	cpuprofWriter, cpuprofCleanup, err := createBufWriter(cpuprofFilename)
	if err != nil {
		klog.Fatalf("unable to create metrics writer to filename=%s: %v", metricsFilename, err)
	}
	defer cpuprofCleanup()
	if cpuprofWriter != nil {
		pprof.StartCPUProfile(cpuprofWriter)
		defer pprof.StopCPUProfile()
	}

	if rpcEndpoint == "" {
		rpcEndpoint = defaultRpcEndpoint
	}

	if loadFromSnapshot {
		if path == "" || outputDir == "" {
			klog.Fatalf("must specify snapshot path and directory path for writing generated AccountsDB")
		}

		mlog.Log.Infof("building AccountsDB from snapshot at %s\n", path)

		// extract accountvecs from full snapshot, build accountsdb index, and write it all out to disk
		accountsDb, manifest, err = snapshot.BuildAccountsDb(path, incrementalSnapshotFilename, outputDir)
		if err != nil {
			klog.Fatalf("failed to populate new accounts db from snapshot %s: %s", path, err)
		}

		//mlog.Log.Debugf("successfully created accounts db from snapshot %s", path)

		accountsDbDir = outputDir
	} else if loadFromAccountsDb {
		if path == "" {
			klog.Fatalf("must specify an AccountsDB directory path to load from")
		}

		accountsDbDir = path
	} else if snapshotDlPath != "" {
		if outputDir == "" {
			klog.Fatalf("must specify a path to download a snapshot to")
		}

		mlog.Log.Infof("downloading snapshot...")

		path, _, _, err = snapshotdl.DownloadSnapshot(defaultRpcEndpoint, snapshotDlPath)
		if err != nil {
			klog.Fatalf("error downloading snapshot: %s", err)
		}

		accountsDb, manifest, err = snapshot.BuildAccountsDb(path, incrementalSnapshotFilename, outputDir)
		if err != nil {
			klog.Fatalf("failed to populate new accounts db from snapshot %s: %s", path, err)
		}

		accountsDbDir = outputDir
	}

	if accountsDb == nil || manifest == nil {
		mlog.Log.Infof("loading from AccountsDB at %s", accountsDbDir)

		accountsDb, err = accountsdb.OpenDb(accountsDbDir)
		if err != nil {
			klog.Fatalf("unable to open accounts db %s\n", accountsDbDir)
		}

		manifest, err = snapshot.LoadManifestFromFile(filepath.Join(accountsDbDir, manifestFilename))
		if err != nil {
			klog.Fatalf("unable to open manifest file")
		}
	}

	startSlot := int64(manifest.Bank.Slot + 1)
	if endSlot != slotNotSet {
		numReplaySlots = endSlot - startSlot
	} else if numReplaySlots != 0 {
		endSlot = startSlot + numReplaySlots
	}

	// just processing the snapshot - not executing blocks.
	if numReplaySlots == 0 {
		return
	}

	if endSlot != slotNotSet && endSlot < startSlot {
		klog.Fatalf("end slot cannot be lower than start slot")
	}
	mlog.Log.Infof("will replay startSlot=%d endSlot=%d", startSlot, endSlot)

	mlog.Log.Infof("initializing caches")
	accountsDb.InitCaches()

	metricsWriter, metricsWriterCleanup, err := createBufWriter(metricsFilename)
	if err != nil {
		klog.Fatalf("unable to create metrics writer to filename=%s: %v", metricsFilename, err)
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
	if rpcPort < minPort || rpcPort > maxPort {
		klog.Fatalf("invalid port: %d", rpcPort)
	} else if rpcPort != portDisabled {
		rpcServer = rpcserver.NewRpcServer(accountsDb, uint16(rpcPort))
		rpcServer.Start()
		mlog.Log.Infof("started RPC server on port %d", rpcPort)
	}

	replay.ReplayBlocks(c.Context(), accountsDb, accountsDbDir, manifest, uint64(startSlot), uint64(endSlot), rpcEndpoint, blockDir, int(txParallelism), nil, false, dbgOpts, metricsWriter, rpcServer)
	mlog.Log.Infof("done replaying, closing DB")
	accountsDb.CloseDb()
}

func runCatchup(c *cobra.Command, args []string) {
	logVCSInfo()
	snapshotDownloadPath := scratchDir

	dbgOpts, err := replay.NewDebugOptions(debugTxs, debugAcctWrites)
	if err != nil {
		klog.Fatalf("failed to parse --debugtx or --debugacctwrites values: %v", err)
	}

	cpuprofWriter, cpuprofCleanup, err := createBufWriter(cpuprofFilename)
	if err != nil {
		klog.Fatalf("unable to create metrics writer to filename=%s: %v", metricsFilename, err)
	}
	defer cpuprofCleanup()
	if cpuprofWriter != nil {
		pprof.StartCPUProfile(cpuprofWriter)
		defer pprof.StopCPUProfile()
	}

	if rpcEndpoint == "" {
		rpcEndpoint = defaultRpcEndpoint
	}

	if rpcEndpointFile == "" {
		klog.Fatalf("must provide list of RPC nodes to use")
	}

	mlog.Log.Infof("downloading full snapshot...")
	fullSnapshotDlStart := time.Now()
	fullSnapshotPath, _, fullSnapshotSlot, err := snapshotdl.DownloadSnapshot(defaultRpcEndpoint, snapshotDownloadPath)
	if err != nil {
		klog.Fatalf("error downloading snapshot: %s", err)
	}
	mlog.Log.Infof("finished downloading full snapshot in %s to %s", time.Since(fullSnapshotDlStart), fullSnapshotPath)

	accountsDb, manifest, err := snapshot.BuildAccountsDbWithIncr(fullSnapshotPath, snapshotDownloadPath, fullSnapshotSlot, fullSnapshotSlot, outputDir, rpcEndpoint, rpcEndpointFile, blockDir, nil)
	if err != nil {
		klog.Fatalf("failed to populate new accounts db from snapshot %s: %s", path, err)
	}
	mlog.Log.Infof("finished building accountsdb")

	startSlot := int64(manifest.Bank.Slot + 1)
	endSlot := uint64(math.MaxUint64)

	mlog.Log.Infof("will replay startSlot=%d endSlot=%d", startSlot, endSlot)

	mlog.Log.Infof("initializing caches")
	accountsDb.InitCaches()

	metricsWriter, metricsWriterCleanup, err := createBufWriter(metricsFilename)
	if err != nil {
		klog.Fatalf("unable to create metrics writer to filename=%s: %v", metricsFilename, err)
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
	if rpcPort < minPort || rpcPort > maxPort {
		klog.Fatalf("invalid port: %d", rpcPort)
	} else if rpcPort != portDisabled {
		rpcServer = rpcserver.NewRpcServer(accountsDb, uint16(rpcPort))
		rpcServer.Start()
		mlog.Log.Infof("started RPC server on port %d", rpcPort)
	}

	replay.ReplayBlocks(c.Context(), accountsDb, outputDir, manifest, uint64(startSlot), uint64(endSlot), rpcEndpoint, blockDir, int(txParallelism), nil, true, dbgOpts, metricsWriter, rpcServer)
	mlog.Log.Infof("done replaying, closing DB")
	accountsDb.CloseDb()
}

func logVCSInfo() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		mlog.Log.Errorf("VCS info: not available")
		return
	}

	var revision, time, modified string

	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.time":
			time = setting.Value
		case "vcs.modified":
			modified = setting.Value
		}
	}

	mlog.Log.Infof("VCS info: revision=%s time=%s modified=%s", revision, time, modified)
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
