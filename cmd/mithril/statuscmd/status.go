package statuscmd

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/procctl"
	statepkg "github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/tui"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
)

var (
	accountsPath string

	StatusCmd = cobra.Command{
		Use:   "status",
		Short: "Show current Mithril node status",
		Long:  "Check if Mithril is running, what slot it's at, and Lightbringer connectivity.",
		Run: func(cmd *cobra.Command, args []string) {
			runStatus()
		},
	}
)

func init() {
	StatusCmd.Flags().StringVar(&accountsPath, "accounts", "", "Path to AccountsDB directory (to find state file)")
}

var (
	titleStyle   = tui.TitleStyle
	successStyle = tui.SuccessStyle
	warnStyle    = tui.WarnStyle
	dimStyle     = tui.DimStyle
	valueStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("255"))
)

// mithrilState mirrors the relevant fields from pkg/state/state.go
type mithrilState struct {
	SnapshotSlot   uint64 `json:"snapshot_slot"`
	LastSlot       uint64 `json:"last_slot"`
	LastBankhash   string `json:"last_bankhash"`
	GenesisHash    string `json:"genesis_hash"`
	ShutdownReason string `json:"last_shutdown_reason"`
}

func runStatus() {
	fmt.Println()
	fmt.Println(titleStyle.Render("◎ Mithril Status"))
	fmt.Println()

	// Load config before state discovery so status follows the same storage
	// path runtime uses when --accounts is not provided.
	configErr := config.InitConfig()

	configuredAccounts := ""
	legacyAccounts := ""
	if configErr == nil {
		configuredAccounts = config.GetString("storage.accounts")
		legacyAccounts = config.GetString("ledger.accounts_path")
	}

	process, processErr := procctl.Detect(
		procctl.DefaultPidFile(),
		procctl.DefaultLockFile(),
		statusDetectionAccountsPath(accountsPath, configuredAccounts, legacyAccounts),
	)

	nodeState, statePath, stateFound := loadStatusState(statusStateSearchPaths(
		accountsPath,
		configuredAccounts,
		legacyAccounts,
	))

	printProcessStatus(process, processErr)

	if stateFound {
		fmt.Printf("  %s State file: %s\n", successStyle.Render("✓"), dimStyle.Render(statePath))
		fmt.Printf("  %s Last slot:  %s\n", successStyle.Render("✓"), valueStyle.Render(fmt.Sprintf("%d", nodeState.LastSlot)))
		if nodeState.SnapshotSlot > 0 {
			fmt.Printf("  %s Snapshot:   %s\n", dimStyle.Render("-"), valueStyle.Render(fmt.Sprintf("slot %d", nodeState.SnapshotSlot)))
		}
		if nodeState.ShutdownReason != "" {
			fmt.Printf("  %s Last stop:  %s\n", dimStyle.Render("-"), valueStyle.Render(nodeState.ShutdownReason))
		}
		if nodeState.LastBankhash != "" {
			short := nodeState.LastBankhash
			if len(short) > 12 {
				short = short[:12] + "..."
			}
			fmt.Printf("  %s Bankhash:   %s\n", dimStyle.Render("-"), dimStyle.Render(short))
		}
	} else {
		headline, detail := missingStateStatusText(processRunning(process))
		fmt.Printf("  %s %s\n", warnStyle.Render("~"), headline)
		fmt.Printf("    %s %s\n", dimStyle.Render(""), detail)
	}

	fmt.Println()

	// Read service addresses from config (fall back to defaults)
	if configErr != nil {
		fmt.Printf("  %s Failed to read config: %v\n", warnStyle.Render("~"), configErr)
		fmt.Println("  Using default service addresses")
	}
	rpcAddr := "127.0.0.1:8899"
	lbAddr := "127.0.0.1:3001"
	lbHTTP := "127.0.0.1:3000"
	if port := config.GetString("rpc.port"); port != "" && port != "0" {
		rpcAddr = "127.0.0.1:" + port
	}
	if addr := config.GetString("lightbringer.grpc_addr"); addr != "" {
		lbAddr = addr
	}
	if addr := config.GetString("lightbringer.rpc_addr"); addr != "" {
		lbHTTP = addr
	}
	blockSource := config.GetString("block.source")
	lightbringerEnabled := config.GetBool("lightbringer.enabled")
	effectiveBlockSource := blockSource
	if effectiveBlockSource == "" {
		effectiveBlockSource = "rpc"
	}
	// Match run-time behavior: enabling the managed sidecar promotes block delivery
	// to Lightbringer unless a CLI flag explicitly forced RPC.
	if lightbringerEnabled && effectiveBlockSource == "rpc" {
		effectiveBlockSource = "lightbringer"
	}

	// External LB mode: use block.lightbringer_endpoint if set
	if extAddr := config.GetString("block.lightbringer_endpoint"); extAddr != "" {
		lbAddr = extAddr
	}

	fmt.Println("  " + dimStyle.Render("Services:"))

	// Check Mithril RPC (skip if port=0 means disabled)
	if config.GetString("rpc.port") == "0" {
		fmt.Printf("  %s Mithril RPC disabled (port=0)\n", dimStyle.Render("-"))
	} else {
		conn, err := net.DialTimeout("tcp", rpcAddr, 2*time.Second)
		if err == nil {
			conn.Close()
			fmt.Printf("  %s Mithril RPC responding on %s\n", successStyle.Render("✓"), redactedStatusAddr(rpcAddr))
		} else {
			fmt.Printf("  %s Mithril RPC not responding on %s\n", dimStyle.Render("-"), redactedStatusAddr(rpcAddr))
		}
	}

	// Only probe Lightbringer when it is the effective block source.
	if effectiveBlockSource == "lightbringer" {
		conn, err := net.DialTimeout("tcp", lbAddr, 2*time.Second)
		if err == nil {
			conn.Close()
			fmt.Printf("  %s Lightbringer gRPC responding on %s\n", successStyle.Render("✓"), redactedStatusAddr(lbAddr))
		} else {
			fmt.Printf("  %s Lightbringer gRPC not responding on %s\n", dimStyle.Render("-"), redactedStatusAddr(lbAddr))
		}

		// Only probe HTTP when using managed sidecar (not external)
		if lightbringerEnabled {
			conn, err = net.DialTimeout("tcp", lbHTTP, 2*time.Second)
			if err == nil {
				conn.Close()
				fmt.Printf("  %s Lightbringer HTTP responding on %s\n", successStyle.Render("✓"), redactedStatusAddr(lbHTTP))
			} else {
				fmt.Printf("  %s Lightbringer HTTP not responding on %s\n", dimStyle.Render("-"), redactedStatusAddr(lbHTTP))
			}
			if config.GetBool("lightbringer.quiet") {
				fmt.Printf("  %s Lightbringer quiet mode: enabled (warn/error only)\n", dimStyle.Render("·"))
			}
		}
	}

	fmt.Println()
}

func redactedStatusAddr(addr string) string {
	return config.RedactSecretsInText(config.RedactEndpointForDisplay(addr))
}

func printProcessStatus(process *procctl.Detection, err error) {
	if err != nil {
		fmt.Printf("  %s Process state unavailable: %v\n", warnStyle.Render("~"), err)
		return
	}
	if process == nil {
		return
	}

	switch process.Status {
	case procctl.StatusRunning:
		fmt.Printf("  %s Process: running (pid %d)\n", successStyle.Render("✓"), process.Pid)
		if process.SpawnedBy != "" {
			fmt.Printf("  %s Started by: %s\n", dimStyle.Render("-"), valueStyle.Render(process.SpawnedBy))
		}
		if process.LogDir != "" {
			fmt.Printf("  %s Logs:       %s\n", dimStyle.Render("-"), dimStyle.Render(process.LogDir))
		}
	case procctl.StatusCrashed:
		fmt.Printf("  %s Process: not running (previous run may need attention)\n", warnStyle.Render("~"))
		if process.LastShutdownReason != "" {
			fmt.Printf("  %s Last stop:  %s\n", dimStyle.Render("-"), valueStyle.Render(process.LastShutdownReason))
		}
	}
}

func processRunning(process *procctl.Detection) bool {
	return process != nil && process.Status == procctl.StatusRunning
}

func missingStateStatusText(running bool) (string, string) {
	if running {
		return "State file: not ready yet", "Mithril is running; AccountsDB has not produced a ready state yet (bootstrap/build in progress)"
	}
	return "No state file found", "Mithril hasn't run yet, or --accounts path is wrong"
}

func statusDetectionAccountsPath(cliAccountsPath, configuredAccountsPath, legacyAccountsPath string) string {
	if cliAccountsPath != "" {
		return cliAccountsPath
	}
	if configuredAccountsPath != "" {
		return configuredAccountsPath
	}
	if legacyAccountsPath != "" {
		return legacyAccountsPath
	}
	return config.DefaultStoragePaths().Accounts
}

func statusStateSearchPaths(cliAccountsPath, configuredAccountsPath, legacyAccountsPath string) []string {
	if cliAccountsPath != "" {
		return []string{cliAccountsPath}
	}

	paths := make([]string, 0, 5)
	paths = appendUniquePath(paths, configuredAccountsPath)
	paths = appendUniquePath(paths, legacyAccountsPath)
	paths = appendUniquePath(paths, config.DefaultStoragePaths().Accounts)
	paths = appendUniquePath(paths, "./data/accounts")
	paths = appendUniquePath(paths, ".")
	return paths
}

func appendUniquePath(paths []string, path string) []string {
	if path == "" {
		return paths
	}
	for _, existing := range paths {
		if existing == path {
			return paths
		}
	}
	return append(paths, path)
}

func loadStatusState(searchPaths []string) (mithrilState, string, bool) {
	for _, dir := range searchPaths {
		p := filepath.Join(dir, statepkg.StateFileName)
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var nodeState mithrilState
		if err := json.Unmarshal(data, &nodeState); err != nil {
			continue
		}
		return nodeState, p, true
	}
	return mithrilState{}, "", false
}
