package setupcmd

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/config"
)

func runDoctor() {
	fmt.Println()
	fmt.Println(titleStyle.Render("◎ Mithril Doctor"))
	fmt.Println()

	passed := 0
	total := 0

	// 1. Config file
	total++
	configPath := config.ConfigFile
	if configPath == "" {
		configPath = "config.toml"
	}
	if _, err := os.Stat(configPath); err == nil {
		fmt.Printf("  %s Config file found (%s)\n", successStyle.Render("✓"), configPath)
		passed++

		// Check if config needs migration (missing new sections)
		data, _ := os.ReadFile(configPath)
		content := string(data)
		if !strings.Contains(content, "[lightbringer]") || !strings.Contains(content, "[consensus]") {
			fmt.Printf("  %s Config is missing new sections (lightbringer/consensus)\n", warnStyle.Render("~"))
			fmt.Printf("    %s Run: mithril doctor --migrate to add them\n", dimStyle.Render("→"))
		}
	} else {
		fmt.Printf("  %s Config file not found (%s)\n", errorStyle.Render("✗"), configPath)
		fmt.Printf("    %s Run: mithril setup\n", dimStyle.Render("→"))
	}

	// Load config for further checks
	if err := config.InitConfig(); err != nil {
		fmt.Printf("  %s Failed to parse config: %v\n", errorStyle.Render("✗"), err)
		fmt.Printf("\n  %d/%d checks passed\n", passed, total)
		return
	}

	// 2. Cluster — this build boots Alpenglow only.
	total++
	cluster := config.GetString("network.cluster")
	if cluster == "alpenglow" {
		fmt.Printf("  %s Network: %s\n", successStyle.Render("✓"), cluster)
		passed++
	} else if cluster == "" {
		fmt.Printf("  %s Network: alpenglow (default; network.cluster not set)\n", successStyle.Render("✓"))
		passed++
	} else if cluster == "mainnet-beta" || cluster == "testnet" || cluster == "devnet" {
		fmt.Printf("  %s Cluster %q needs a dev-branch (TowerBFT) build until it upgrades to Alpenglow — this build boots \"alpenglow\" only\n", errorStyle.Render("✗"), cluster)
	} else {
		fmt.Printf("  %s Invalid cluster: %s\n", errorStyle.Render("✗"), cluster)
	}

	// 2b. Node mode — "verifying" (non-voting) is the default; "validator"
	// enforces the voting-deployment shape (keypairs, turbine, Votor listener).
	total++
	consensusMode := config.GetString("consensus.mode")
	switch consensusMode {
	case "", "verifying":
		fmt.Printf("  %s Node mode: verifying (non-voting)\n", successStyle.Render("✓"))
		passed++
	case "validator":
		var missing []string
		if config.GetString("validator.identity_keypair") == "" {
			missing = append(missing, "validator.identity_keypair")
		}
		if config.GetString("validator.vote_account_keypair") == "" {
			missing = append(missing, "validator.vote_account_keypair")
		}
		if config.GetString("turbine.gossip_entrypoint") == "" {
			missing = append(missing, "turbine.gossip_entrypoint")
		}
		if config.GetString("consensus.alpenglow_observer_bind_addr") == "" {
			missing = append(missing, "consensus.alpenglow_observer_bind_addr")
		}
		if len(missing) == 0 {
			fmt.Printf("  %s Node mode: validator (voting engine not yet active — runs verify-only)\n", successStyle.Render("✓"))
			passed++
		} else {
			fmt.Printf("  %s validator mode is missing: %s\n", errorStyle.Render("✗"), strings.Join(missing, ", "))
		}
	default:
		fmt.Printf("  %s Invalid consensus.mode: %q (valid: \"verifying\", \"validator\")\n", errorStyle.Render("✗"), consensusMode)
	}

	// 3. RPC endpoint
	total++
	rpcEndpoints := config.GetStringSlice("network.rpc")
	if len(rpcEndpoints) > 0 {
		ep := rpcEndpoints[0]
		fmt.Printf("  %s RPC endpoint configured (%s)\n", successStyle.Render("✓"), ep)
		passed++
	} else {
		fmt.Printf("  %s No RPC endpoints configured\n", errorStyle.Render("✗"))
		fmt.Printf("    %s Set network.rpc in config\n", dimStyle.Render("→"))
	}

	// 4. Storage paths
	total++
	accountsPath := config.GetString("storage.accounts")
	if accountsPath != "" {
		if info, err := os.Stat(accountsPath); err == nil && info.IsDir() {
			fmt.Printf("  %s AccountsDB path exists (%s)\n", successStyle.Render("✓"), accountsPath)
			passed++
		} else if accountsPath != "" {
			fmt.Printf("  %s AccountsDB path: %s (will be created)\n", warnStyle.Render("~"), accountsPath)
			passed++
		}
	} else {
		fmt.Printf("  %s storage.accounts not set\n", errorStyle.Render("✗"))
	}

	// 5. Lightbringer
	lbEnabled := config.GetBool("lightbringer.enabled")
	if lbEnabled {
		total++
		binaryPath := config.GetString("lightbringer.binary_path")
		if binaryPath == "" {
			binaryPath = "./lightbringer"
		}
		if _, err := os.Stat(binaryPath); err == nil {
			fmt.Printf("  %s Lightbringer binary found (%s)\n", successStyle.Render("✓"), binaryPath)
			passed++
		} else {
			fmt.Printf("  %s Lightbringer binary not found at %s\n", errorStyle.Render("✗"), binaryPath)
		}

		total++
		gossip := config.GetString("lightbringer.gossip_entrypoint")
		if gossip != "" {
			if _, _, err := net.SplitHostPort(gossip); err != nil {
				fmt.Printf("  %s Gossip entrypoint invalid format (%s): %v\n", errorStyle.Render("✗"), gossip, err)
			} else {
				fmt.Printf("  %s Gossip entrypoint set (%s)\n", successStyle.Render("✓"), gossip)
				passed++
			}
		} else {
			fmt.Printf("  %s lightbringer.gossip_entrypoint not set\n", errorStyle.Render("✗"))
		}

		total++
		grpcAddr := config.GetString("lightbringer.grpc_addr")
		if grpcAddr == "" {
			grpcAddr = "127.0.0.1:3001"
		}
		conn, err := net.DialTimeout("tcp", grpcAddr, 2*time.Second)
		if err == nil {
			conn.Close()
			fmt.Printf("  %s Lightbringer gRPC port responding (%s)\n", successStyle.Render("✓"), grpcAddr)
			passed++
		} else {
			fmt.Printf("  %s Lightbringer gRPC not responding at %s (not running yet?)\n", dimStyle.Render("-"), grpcAddr)
			// Don't count as failure — it's not running yet
			total--
		}
	} else {
		// Check for invalid or external Lightbringer configs
		blockSource := config.GetString("block.source")
		lbEndpoint := config.GetString("block.lightbringer_endpoint")
		if blockSource == "lightbringer" && lbEndpoint != "" {
			fmt.Printf("  %s Lightbringer: external at %s\n", successStyle.Render("✓"), lbEndpoint)
			passed++
			total++
		} else if blockSource == "lightbringer" {
			fmt.Printf("  %s block.source=lightbringer but no sidecar enabled and no endpoint set\n", errorStyle.Render("✗"))
			total++
		} else {
			fmt.Printf("  %s Lightbringer: disabled\n", dimStyle.Render("-"))
		}
	}

	// 6. Logs directory
	total++
	logsDir := config.GetString("storage.logs")
	if logsDir == "" {
		logsDir = config.GetString("log.dir")
	}
	if logsDir != "" {
		fmt.Printf("  %s Log directory: %s\n", successStyle.Render("✓"), logsDir)
		passed++
	} else {
		fmt.Printf("  %s No log directory configured (logs go to stderr only)\n", warnStyle.Render("~"))
		passed++ // Not critical
	}

	// Summary
	fmt.Println()
	if passed == total {
		fmt.Printf("  %s\n", successStyle.Render(fmt.Sprintf("%d/%d checks passed — ready to run!", passed, total)))
	} else {
		fmt.Printf("  %s\n", warnStyle.Render(fmt.Sprintf("%d/%d checks passed", passed, total)))
	}
	fmt.Println()
}
