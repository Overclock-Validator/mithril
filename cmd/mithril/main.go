package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/Overclock-Validator/mithril/cmd/mithril/alpenglowcmd"
	"github.com/Overclock-Validator/mithril/cmd/mithril/configcmd"
	"github.com/Overclock-Validator/mithril/cmd/mithril/dashboardcmd"
	"github.com/Overclock-Validator/mithril/cmd/mithril/mcpcmd"
	"github.com/Overclock-Validator/mithril/cmd/mithril/node"
	"github.com/Overclock-Validator/mithril/cmd/mithril/setupcmd"
	"github.com/Overclock-Validator/mithril/cmd/mithril/statecmd"
	"github.com/Overclock-Validator/mithril/cmd/mithril/statuscmd"
	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"

	// Load in instruction pretty-printing
	_ "github.com/gagliardetto/solana-go/programs/system"
	_ "github.com/gagliardetto/solana-go/programs/vote"
)

var cmd = cobra.Command{
	Use:     "mithril",
	Short:   "Mithril - Solana full node client",
	Version: Version,
	Long: `Mithril - Solana Full Node Client

A lightweight full node client for Solana written in Go. Mithril replays and
validates transactions, enabling independent verification of the Solana blockchain.

Quick start:
  1. mithril config init              # Generate config.toml
  2. Edit config.toml                 # Set storage paths
  3. mithril run --config config.toml # Start Mithril

Disk setup (recommended before first run):
  sudo ./scripts/disk-setup.sh --setup    # Format and mount NVMe drives
  ./scripts/disk-setup.sh --status        # Show current storage status`,
}

func init() {
	klogFlags := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(klogFlags)
	cmd.PersistentFlags().AddGoFlagSet(klogFlags)

	// Add config file flag
	cmd.PersistentFlags().StringVar(&config.ConfigFile, "config", "", "Path to TOML config file")

	cmd.AddCommand(
		&node.Run,                  // Primary command for running Mithril
		&alpenglowcmd.AlpenglowCmd, // Alpenglow diagnostics and tools
		&configcmd.ConfigCmd,       // Config management (init, etc.)
		&statecmd.StateCmd,         // State file inspection and management
		&setupcmd.SetupCmd,         // Interactive setup wizard
		&setupcmd.DoctorCmd,        // System health check
		&statuscmd.StatusCmd,       // Node status
		&dashboardcmd.DashboardCmd, // Interactive dashboard
		&mcpcmd.MCPCmd,             // Read-only node diagnostics over MCP
	)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	cobra.CheckErr(cmd.ExecuteContext(ctx))
}
