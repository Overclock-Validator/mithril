// mithril-dashboard-collect writes one bounded external-cluster cache snapshot.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/Overclock-Validator/mithril/pkg/dashboard/cluster"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	profile := flag.String("profile", "alpenglow", "fixed public provider profile (alpenglow, mainnet, or mainnet-publicnode)")
	output := flag.String("output", "", "atomic snapshot destination in an existing private/group-readable directory")
	performance := flag.Bool("performance-sample", false, "Include one RPC performance sample; requires a compatible reader and client")
	history := flag.Bool("performance-history", false, "Include up to 120 RPC performance samples; requires a compatible reader and client")
	flag.Parse()
	if !filepath.IsAbs(*output) || filepath.Clean(*output) != *output || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "--output must be clean and absolute; positional arguments are not accepted")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	collect := cluster.Collect
	if *history {
		collect = cluster.CollectWithPerformanceHistory
	} else if *performance {
		collect = cluster.CollectWithPerformance
	}
	snapshot, err := collect(ctx, *profile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "collection failed:", err)
		os.Exit(1)
	}
	if err := cluster.WriteFile(*output, snapshot); err != nil {
		fmt.Fprintln(os.Stderr, "snapshot write failed:", err)
		os.Exit(1)
	}
	// A degraded capture is still published so an older success cannot appear current.
	if !collectionSucceeded(snapshot) {
		os.Exit(1)
	}
}

func collectionSucceeded(snapshot cluster.Snapshot) bool {
	for _, p := range snapshot.Providers {
		if snapshot.Profile == "mainnet" && p.ID != "rpc" {
			continue
		}
		if p.State != "fresh" && !(p.ID == "puffin" && ((p.State == "unavailable" && p.Reason == "not_collected") || (p.State == "retired" && p.Reason == "provider_retired"))) {
			return false
		}
	}
	return true
}
