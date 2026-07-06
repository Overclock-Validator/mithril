package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"

	"time"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
)

func usage() {
	fmt.Fprintf(os.Stderr, `usage: replaybench <command> [flags]

commands:
  export    package state at slot B plus blocks B+1..B+n from RPC into a bundle
  run       replay a bundle against a scratch accountsdb and report timing

run "replaybench <command> -h" for command flags
`)
	os.Exit(2)
}

func runExport(args []string) {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	accounts := fs.String("accounts", "", "comma-separated AccountsDB shard mounts, primary first (same as storage.accounts)")
	rpc := fs.String("rpc", "https://api.mainnet-beta.solana.com", "RPC endpoint to fetch blocks from")
	blocks := fs.Uint64("n", 100, "number of blocks past the last replayed slot to bundle")
	out := fs.String("out", "", "bundle output directory")
	rps := fs.Float64("rps", 2, "max block fetches per second (0 = unpaced)")
	fs.Parse(args)

	if *accounts == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "export: -accounts and -out are required")
		fs.Usage()
		os.Exit(2)
	}

	err := replay.ExportBenchBundle(replay.BenchExportOpts{
		AccountsPaths: strings.Split(*accounts, ","),
		RpcEndpoint:   *rpc,
		NumBlocks:     *blocks,
		OutDir:        *out,
		MaxRPS:        *rps,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "export failed: %v\n", err)
		os.Exit(1)
	}
}

func runRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	bundle := fs.String("bundle", "", "bundle directory")
	db := fs.String("db", "", "scratch accountsdb directory (wiped each run)")
	parallelism := fs.Int("tx-parallelism", 0, "parallel tx workers (0 = sequential)")
	programStats := fs.Bool("program-stats", false, "report per-program interpreter time (sequential runs only)")
	cpuProfile := fs.String("cpuprofile", "", "write a CPU profile of the replay loop to this file")
	memProfile := fs.String("memprofile", "", "write a heap profile after the replay loop to this file")
	fs.Parse(args)

	if *bundle == "" || *db == "" {
		fmt.Fprintln(os.Stderr, "run: -bundle and -db are required")
		fs.Usage()
		os.Exit(2)
	}

	if *programStats {
		if *parallelism > 0 {
			fmt.Fprintln(os.Stderr, "run: -program-stats requires sequential execution (-tx-parallelism 0)")
			os.Exit(2)
		}
		sbpf.Stats = sbpf.NewStatsCollector()
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "creating cpu profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "starting cpu profile: %v\n", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}

	result, err := replay.RunBenchBundle(replay.BenchRunOpts{
		BundleDir:     *bundle,
		DbDir:         *db,
		TxParallelism: *parallelism,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench run failed: %v\n", err)
		os.Exit(1)
	}

	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "creating heap profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "writing heap profile: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Println()
	fmt.Println("=== replaybench summary ===")
	fmt.Printf("blocks:          %d (slots ..%d)\n", len(result.Blocks), result.FinalSlot)
	fmt.Printf("transactions:    %d\n", result.TotalTxs)
	fmt.Printf("compute units:   %d\n", result.TotalCU)
	fmt.Printf("exec time:       %.3fs\n", result.TotalExec.Seconds())
	fmt.Printf("txs/sec:         %.0f\n", float64(result.TotalTxs)/result.TotalExec.Seconds())
	fmt.Printf("blocks/sec:      %.2f\n", float64(len(result.Blocks))/result.TotalExec.Seconds())
	fmt.Printf("final bankhash:  %s\n", result.FinalBankhash)

	if sbpf.Stats != nil {
		stats := sbpf.Stats.Results()
		var totalSelf time.Duration
		var totalInsns uint64
		for _, st := range stats {
			totalSelf += st.SelfTime
			totalInsns += st.Insns
		}
		fmt.Println()
		fmt.Printf("=== per-program interpreter time (%d programs, VM self-time %.3fs, %d insns) ===\n",
			len(stats), totalSelf.Seconds(), totalInsns)
		fmt.Printf("%4s %6s %6s %4s %9s %12s %14s  %s\n", "rank", "cum%", "self%", "sbpf", "execs", "insns(M)", "self", "program")
		byVersion := map[uint32]time.Duration{}
		for _, st := range stats {
			byVersion[st.SbpfVersion] += st.SelfTime
		}
		var cum time.Duration
		for i, st := range stats {
			if i >= 40 {
				break
			}
			cum += st.SelfTime
			fmt.Printf("%4d %5.1f%% %5.1f%% %4d %9d %12.1f %14s  %s\n",
				i+1,
				100*float64(cum)/float64(totalSelf),
				100*float64(st.SelfTime)/float64(totalSelf),
				st.SbpfVersion,
				st.Executions,
				float64(st.Insns)/1e6,
				st.SelfTime.Round(time.Millisecond),
				st.ProgramId)
		}
		fmt.Println()
		fmt.Println("VM self-time by SBPF version:")
		for v := uint32(0); v <= 3; v++ {
			if d := byVersion[v]; d > 0 {
				fmt.Printf("  v%d: %.3fs (%.1f%%)\n", v, d.Seconds(), 100*float64(d)/float64(totalSelf))
			}
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "export":
		runExport(os.Args[2:])
	case "run":
		runRun(os.Args[2:])
	default:
		usage()
	}
}
