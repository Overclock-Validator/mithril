package main

import (
	"context"
	"flag"
	"net/http"          // ← added
	_ "net/http/pprof"  // ← added
	"os"
	"os/signal"

	"github.com/Overclock-Validator/mithril/cmd/mithril/node"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"

	// Load in instruction pretty-printing
	_ "github.com/gagliardetto/solana-go/programs/system"
	_ "github.com/gagliardetto/solana-go/programs/vote"
)

var cmd = cobra.Command{
	Use:   "mithril",
	Short: "mithril Solana verifier node",
}

func init() {
	klogFlags := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(klogFlags)
	cmd.PersistentFlags().AddGoFlagSet(klogFlags)

	cmd.AddCommand(
		&node.Cmd,
	)
}

func main() {
	mlog.Log.Infof("mithril verifying node\n")

	// --- start pprof server on localhost:6060 ---
	go func() {
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			mlog.Log.Infof("pprof server stopped: %v", err) // changed Warnf → Infof
		}
	}()
	// --------------------------------------------

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	cobra.CheckErr(cmd.ExecuteContext(ctx))
}
