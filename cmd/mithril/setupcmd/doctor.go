package setupcmd

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/lightbringer"
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

		// Can't read config; skip migration check.
		if data, readErr := os.ReadFile(configPath); readErr != nil {
			fmt.Printf("  %s Could not read config for migration check: %v\n", warnStyle.Render("~"), readErr)
		} else {
			content := string(data)
			var missingSections []string
			if !hasTomlSection(content, "lightbringer") {
				missingSections = append(missingSections, "lightbringer")
			}
			if !hasTomlSection(content, "consensus") {
				missingSections = append(missingSections, "consensus")
			}
			if len(missingSections) > 0 {
				fmt.Printf("  %s Config is missing new section(s): %s\n", warnStyle.Render("~"), strings.Join(missingSections, ", "))
				fmt.Printf("    %s Run: mithril doctor --migrate to add them\n", dimStyle.Render("→"))
			}
		}
	} else {
		fmt.Printf("  %s Config file not found (%s)\n", errorStyle.Render("✗"), configPath)
		fmt.Printf("    %s Run: mithril setup\n", dimStyle.Render("→"))
	}

	// Load config for further checks
	if err := config.InitConfig(); err != nil {
		total++ // count the parse attempt as a check
		fmt.Printf("  %s Failed to parse config: %v\n", errorStyle.Render("✗"), err)
		fmt.Printf("\n  %s\n", warnStyle.Render(fmt.Sprintf("%d/%d checks passed", passed, total)))
		return
	}

	// 2. Cluster
	total++
	cluster := config.GetString("network.cluster")
	if cluster == "mainnet-beta" || cluster == "testnet" || cluster == "devnet" {
		fmt.Printf("  %s Network: %s\n", successStyle.Render("✓"), cluster)
		passed++
	} else if cluster == "" {
		fmt.Printf("  %s network.cluster not set\n", errorStyle.Render("✗"))
	} else {
		fmt.Printf("  %s Invalid cluster: %s\n", errorStyle.Render("✗"), cluster)
	}

	// 3. RPC endpoint
	total++
	rpcEndpoints := config.GetStringSlice("network.rpc")
	if len(rpcEndpoints) == 0 {
		rpcEndpoints = config.GetStringSlice("rpc.rpc")
	}
	if len(rpcEndpoints) > 0 {
		ep := config.RedactEndpointForDisplay(rpcEndpoints[0])
		fmt.Printf("  %s RPC endpoint configured (%s)\n", successStyle.Render("✓"), ep)
		passed++
	} else {
		fmt.Printf("  %s No RPC endpoints configured\n", errorStyle.Render("✗"))
		fmt.Printf("    %s Set network.rpc in config\n", dimStyle.Render("→"))
	}

	// 4. Storage paths
	total++
	accountsPath := config.GetString("storage.accounts")
	if accountsPath == "" {
		accountsPath = config.GetString("ledger.accounts_path")
	}
	if accountsPath != "" {
		info, err := os.Stat(accountsPath)
		switch {
		case err == nil && info.IsDir():
			fmt.Printf("  %s AccountsDB path exists (%s)\n", successStyle.Render("✓"), accountsPath)
			passed++
		case err == nil && !info.IsDir():
			// path is a file, not a usable AccountsDB dir
			fmt.Printf("  %s storage.accounts exists but is not a directory (%s)\n", errorStyle.Render("✗"), accountsPath)
		default:
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

		gossipPort := config.GetInt("lightbringer.gossip_port")
		if gossipPort == 0 {
			gossipPort = 65400
		}
		portRangeStart := config.GetInt("lightbringer.port_range_start")
		if portRangeStart == 0 {
			portRangeStart = 65401
		}
		portRangeEnd := config.GetInt("lightbringer.port_range_end")
		if portRangeEnd == 0 {
			portRangeEnd = 65500
		}
		total++
		if err := lightbringer.ValidateGossipPorts(gossipPort, portRangeStart, portRangeEnd); err != nil {
			fmt.Printf("  %s Lightbringer gossip/repair ports invalid: %v\n", errorStyle.Render("✗"), err)
		} else {
			fmt.Printf("  %s Lightbringer opens public Solana UDP gossip/repair ports: %d, %d-%d\n",
				warnStyle.Render("~"), gossipPort, portRangeStart, portRangeEnd)
			passed++
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
			fmt.Printf("  %s Lightbringer: external at %s\n", successStyle.Render("✓"), config.RedactEndpointForDisplay(lbEndpoint))
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
		info, err := os.Stat(logsDir)
		switch {
		case err == nil && info.IsDir():
			fmt.Printf("  %s Log directory: %s\n", successStyle.Render("✓"), logsDir)
			passed++
		case err == nil && !info.IsDir():
			fmt.Printf("  %s storage.logs exists but is not a directory (%s)\n", errorStyle.Render("✗"), logsDir)
		default:
			fmt.Printf("  %s Log directory: %s (will be created)\n", warnStyle.Render("~"), logsDir)
			passed++
		}
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

func hasTomlSection(content, section string) bool {
	for _, line := range strings.Split(content, "\n") {
		if sectionName, ok := tomlSectionName(line); ok && sectionName == section {
			return true
		}
	}
	return false
}

func tomlSectionName(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "#") || !strings.HasPrefix(trimmed, "[") || strings.HasPrefix(trimmed, "[[") {
		return "", false
	}
	end := strings.Index(trimmed, "]")
	if end <= 1 {
		return "", false
	}
	tail := strings.TrimSpace(trimmed[end+1:])
	if tail != "" && !strings.HasPrefix(tail, "#") {
		return "", false
	}
	return strings.TrimSpace(trimmed[1:end]), true
}
