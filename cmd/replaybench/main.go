package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/replay"
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
	fs.Parse(args)

	if *bundle == "" || *db == "" {
		fmt.Fprintln(os.Stderr, "run: -bundle and -db are required")
		fs.Usage()
		os.Exit(2)
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

	fmt.Println()
	fmt.Println("=== replaybench summary ===")
	fmt.Printf("blocks:          %d (slots ..%d)\n", len(result.Blocks), result.FinalSlot)
	fmt.Printf("transactions:    %d\n", result.TotalTxs)
	fmt.Printf("compute units:   %d\n", result.TotalCU)
	fmt.Printf("exec time:       %.3fs\n", result.TotalExec.Seconds())
	fmt.Printf("txs/sec:         %.0f\n", float64(result.TotalTxs)/result.TotalExec.Seconds())
	fmt.Printf("blocks/sec:      %.2f\n", float64(len(result.Blocks))/result.TotalExec.Seconds())
	fmt.Printf("final bankhash:  %s\n", result.FinalBankhash)
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
