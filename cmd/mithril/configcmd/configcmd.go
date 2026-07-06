package configcmd

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/tui"
	"github.com/spf13/cobra"
)

var (
	// ConfigCmd is the parent command for config-related subcommands
	ConfigCmd = cobra.Command{
		Use:   "config",
		Short: "Configuration management commands",
	}

	// InitCmd creates a starter config.toml
	InitCmd = cobra.Command{
		Use:   "init",
		Short: "Create a starter config.toml from the example template",
		Long: `Creates a config.toml file in the current directory with sensible defaults.

The generated config has all parameters with good defaults - you only need to
customize the storage paths for your setup.

Two profiles:
  mithril config init              Verifying node (non-voting) — the default.
  mithril config init --validator  Validator — consensus.mode=validator with the
                                   required keypair/socket fields laid out
                                   (identity + vote-account keypairs, turbine
                                   gossip entrypoint, Votor QUIC listener).
                                   The voting engine is not yet active; the
                                   node runs verify-only until it lands.

If config.toml already exists, this command will not overwrite it.`,
		Run: func(cmd *cobra.Command, args []string) {
			runConfigInit()
		},
	}

	// SetCmd updates a config value
	SetCmd = cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a configuration value in config.toml",
		Long: `Updates a configuration value in config.toml.

Examples:
  mithril config set storage.accounts /mnt/accounts
  mithril config set storage.shredstore /mnt/shredstore
  mithril config set storage.snapshots /mnt/snapshots
  mithril config set bootstrap.mode auto
  mithril config set tuning.txpar 48

Common keys:
  storage.accounts    - Path to AccountsDB directory
  storage.shredstore  - Path to shredstore directory
  storage.snapshots   - Path to snapshots directory
  bootstrap.mode      - Startup mode: auto, snapshot, or accountsdb
  tuning.txpar        - Transaction parallelism (recommended: 2x CPU cores)
  network.rpc         - RPC endpoint(s)`,
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			runConfigSet(args[0], args[1])
		},
	}

	// GetCmd reads a config value
	GetCmd = cobra.Command{
		Use:   "get <key>",
		Short: "Get a configuration value from config.toml",
		Long: `Reads a configuration value from config.toml.

Examples:
  mithril config get storage.accounts
  mithril config get bootstrap.mode`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			runConfigGet(args[0])
		},
	}

	outputPath    string
	initValidator bool
	configFile    string
)

func init() {
	ConfigCmd.AddCommand(&InitCmd)
	ConfigCmd.AddCommand(&SetCmd)
	ConfigCmd.AddCommand(&GetCmd)
	InitCmd.Flags().StringVarP(&outputPath, "output", "o", "config.toml", "Output path for config file")
	InitCmd.Flags().BoolVar(&initValidator, "validator", false, "Generate a validator config (consensus.mode=validator with required keypair/socket fields)")
	SetCmd.Flags().StringVarP(&configFile, "config", "c", "config.toml", "Path to config file")
	GetCmd.Flags().StringVarP(&configFile, "config", "c", "config.toml", "Path to config file")
}

func runConfigInit() {
	// Check if file already exists
	if _, err := os.Stat(outputPath); err == nil {
		fmt.Printf("Error: %s already exists. Remove it first or use -o to specify a different path.\n", outputPath)
		os.Exit(1)
	}

	// Generate the config content
	config := generateStarterConfig(initValidator)

	// Write to file
	if err := tui.AtomicWriteFile(outputPath, []byte(config), 0600); err != nil {
		fmt.Printf("Error writing config file: %v\n", err)
		os.Exit(1)
	}

	absPath, _ := filepath.Abs(outputPath)
	fmt.Printf("Created %s\n", absPath)
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  1. Edit the [storage] paths for your setup")
	fmt.Println("  2. Set [network].rpc and [turbine].gossip_entrypoint for your Alpenglow cluster")
	if initValidator {
		fmt.Println("  3. Set [validator].identity_keypair and vote_account_keypair —")
		fmt.Println("     validator mode refuses to start without them")
		fmt.Println("     (keep the authorized withdrawer keypair OFFLINE; it is not needed at runtime)")
	} else {
		fmt.Println("  3. For a staked node, set [validator].identity_keypair and")
		fmt.Println("     [consensus].alpenglow_observer_bind_addr (Votor QUIC cert feed)")
	}
	fmt.Println("  4. Run: mithril run --config config.toml")
	fmt.Println()
	fmt.Println("See config.example.toml for detailed documentation of all options.")
}

func generateStarterConfig(validator bool) string {
	// Pick storage paths that work for the current environment: production
	// /mnt/mithril-* when scripts/disk-setup.sh has been run, ~/.mithril/*
	// otherwise. See pkg/config/defaults.go for detection details.
	s := config.DefaultStoragePaths()

	// The [validator] + [consensus] sections are the profile split: a
	// verifying node needs neither keypairs nor the Votor listener; validator
	// mode REQUIRES identity + vote-account keypairs, a turbine gossip
	// entrypoint, and the Votor QUIC listener (enforced at startup).
	nodeSections := `[validator]
identity_keypair = ""              # Validator identity — advertises this node into turbine gossip; set for a staked Alpenglow node
vote_account_keypair = ""          # Vote account keypair path (used once voting activates)
authorized_withdrawer_keypair = "" # Authorized withdrawer keypair path (diagnostics only)

[consensus]
mode = "verifying"                # "verifying" (default, non-voting) | "validator"
alpenglow_observer_bind_addr = "" # Votor QUIC cert listener, e.g. "0.0.0.0:8010" (empty = rely on footer certs in shreds)
alpenglow_max_message_bytes = 0   # 0 = default
alpenglow_bls_dst = ""            # BLS DST override (must match cluster solana-bls version)`
	if validator {
		nodeSections = `[validator]
# REQUIRED in validator mode — the node refuses to start without these two.
identity_keypair = "/path/to/validator-keypair.json"        # Signs gossip/turbine identity and, once voting activates, votes
vote_account_keypair = "/path/to/vote-account-keypair.json" # The vote account votes are cast for
# NOT required at runtime — keep the withdrawer keypair OFFLINE.
authorized_withdrawer_keypair = ""

[consensus]
# Validator mode enforces the full voting-deployment shape (keypairs above,
# turbine source + gossip entrypoint, Votor QUIC listener below) so the
# deployment is provisioned before the voting engine activates. Until it
# lands the node runs the same verifying pipeline and casts NO votes.
mode = "validator"
alpenglow_observer_bind_addr = "0.0.0.0:8010" # REQUIRED: Votor QUIC vote/cert listener
alpenglow_max_message_bytes = 0               # 0 = default
alpenglow_bls_dst = ""                        # BLS DST override (must match cluster solana-bls version)`
	}
	return fmt.Sprintf(`# Mithril Configuration
# Generated by: mithril config init
# See config.example.toml for detailed documentation of all options.

name = "mithril"

[bootstrap]
mode = "auto"   # "auto" | "snapshot" | "new-snapshot" | "accountsdb"

[storage]
accounts = %q           # AccountsDB (~500GB, use fastest NVMe)
shredstore = %q          # Lightbringer shred storage
snapshots = %q           # ~100GB for full + incremental
logs = %q                # Log files (created if missing)

[network]
cluster = "alpenglow"  # This build boots Alpenglow only (TowerBFT clusters need a dev-branch build)
rpc = ["https://alpenglow.rpcpool.com"]

[block]
# "turbine" is the live mode: shreds carry the Alpenglow block ids and footer
# certificates that gate durable state. "rpc" is catch-up/debug only — RPC
# blocks carry no certificates, so near-tip operation cannot adjudicate them
# and durable folds stall without a Votor QUIC cert feed ([consensus] below).
source = "turbine"   # "turbine" (live) | "rpc" (catch-up/debug) | "lightbringer"
turbine_bind_addr = "0.0.0.0:8001"
# lightbringer_endpoint = "localhost:9000"

[turbine]
gossip_entrypoint = ""     # REQUIRED for turbine: a gossip entrypoint of your Alpenglow cluster
# gossip_bind_addr = "0.0.0.0:65401"
# advertised_ip = "203.0.113.10"
# shred_version = 0

# [lightbringer]
# enabled = false
# binary_path = "./lightbringer"
# gossip_entrypoint = "1.2.3.4:8000"
# shredstore stored in [storage] section
# rpc_addr = "127.0.0.1:3000"
# grpc_addr = "127.0.0.1:3001"
# See config.example.toml for full Lightbringer sidecar options.

[tuning]
txpar = 24   # Recommended: 2x your CPU core count

%s

[rpc]
port = 8899  # Mithril's RPC server (binds to all interfaces)

[log]
dir = %q  # Log files (created if missing)
level = "info"             # "debug" | "info" | "warn" | "error"
to_stdout = true           # Also write to stdout
max_size_mb = 100          # Max log file size before rotation
# max_age_days = 0         # Delete logs older than N days (0/unset = never delete by age)

# Advanced options (defaults work well for most setups)
# See config.example.toml for: [tuning], [debug], [snapshot], [reporting]
`, s.Accounts, s.Shredstore, s.Snapshots, s.Logs, nodeSections, s.Logs)
}

// runConfigSet updates a key in the config file
func runConfigSet(key, value string) {
	// Check if config file exists
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		fmt.Printf("Error: %s does not exist. Run 'mithril config init' first.\n", configFile)
		os.Exit(1)
	}

	// Parse key into section and field (e.g., "storage.accounts" -> "storage", "accounts")
	parts := strings.SplitN(key, ".", 2)
	if len(parts) != 2 {
		fmt.Printf("Error: Invalid key format. Use 'section.field' (e.g., 'storage.accounts')\n")
		os.Exit(1)
	}
	section := parts[0]
	field := parts[1]

	// Read the config file
	content, err := os.ReadFile(configFile)
	if err != nil {
		fmt.Printf("Error reading config file: %v\n", err)
		os.Exit(1)
	}

	lines := strings.Split(string(content), "\n")
	var result []string
	inSection := false
	found := false
	currentSection := ""

	// Determine if value needs quotes (strings vs numbers/booleans)
	quotedValue := formatTOMLValue(value)

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Check for section headers
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			// If we were in the target section and didn't find the key, add it
			if inSection && !found {
				result = append(result, fmt.Sprintf("%s = %s", field, quotedValue))
				found = true
			}
			currentSection = strings.Trim(trimmed, "[]")
			inSection = (currentSection == section)
		}

		// Check if this line sets our field
		if inSection && !found {
			// Match field = value pattern
			pattern := fmt.Sprintf(`^\s*%s\s*=`, regexp.QuoteMeta(field))
			matched, _ := regexp.MatchString(pattern, line)
			if matched {
				// Replace this line
				// Preserve indentation
				indent := ""
				for _, c := range line {
					if c == ' ' || c == '\t' {
						indent += string(c)
					} else {
						break
					}
				}
				result = append(result, fmt.Sprintf("%s%s = %s", indent, field, quotedValue))
				found = true
				continue
			}
		}

		result = append(result, line)
	}

	// If section doesn't exist, create it
	if !found {
		// Check if we need to add the section
		sectionExists := false
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if trimmed == fmt.Sprintf("[%s]", section) {
				sectionExists = true
				break
			}
		}

		if !sectionExists {
			// Add section and key at the end
			result = append(result, "")
			result = append(result, fmt.Sprintf("[%s]", section))
			result = append(result, fmt.Sprintf("%s = %s", field, quotedValue))
		} else {
			// Section exists but key wasn't found - this shouldn't happen
			// but handle it by adding after section header
			var newResult []string
			for _, line := range result {
				newResult = append(newResult, line)
				trimmed := strings.TrimSpace(line)
				if trimmed == fmt.Sprintf("[%s]", section) {
					newResult = append(newResult, fmt.Sprintf("%s = %s", field, quotedValue))
				}
			}
			result = newResult
		}
	}

	// Write back
	err = tui.AtomicWriteFile(configFile, []byte(strings.Join(result, "\n")), 0600)
	if err != nil {
		fmt.Printf("Error writing config file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Set %s = %s\n", key, quotedValue)
}

// formatTOMLValue formats a value appropriately for TOML.
// Returns the canonical form of the parsed value to prevent injection
// via newlines or other control characters in the raw input.
func formatTOMLValue(value string) string {
	// Check if it's an integer — return the parsed number, not raw input
	var n int
	if _, err := fmt.Sscanf(value, "%d", &n); err == nil && fmt.Sprintf("%d", n) == strings.TrimSpace(value) {
		return fmt.Sprintf("%d", n)
	}
	// Check if it's a float — return the parsed number, not raw input
	var f float64
	if _, err := fmt.Sscanf(value, "%f", &f); err == nil && !strings.ContainsAny(value, "\n\r") {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}

	// Check if it's a boolean
	if value == "true" || value == "false" {
		return value
	}

	// Check if it's already an array (no newlines allowed)
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") && !strings.ContainsAny(value, "\n\r") {
		return value
	}

	// Otherwise, quote it as a string (safe — %q escapes all control chars)
	return fmt.Sprintf("%q", value)
}

// runConfigGet reads a key from the config file
func runConfigGet(key string) {
	// Check if config file exists
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		fmt.Printf("Error: %s does not exist.\n", configFile)
		os.Exit(1)
	}

	// Parse key into section and field
	parts := strings.SplitN(key, ".", 2)
	if len(parts) != 2 {
		fmt.Printf("Error: Invalid key format. Use 'section.field' (e.g., 'storage.accounts')\n")
		os.Exit(1)
	}
	section := parts[0]
	field := parts[1]

	// Read and scan the file
	file, err := os.Open(configFile)
	if err != nil {
		fmt.Printf("Error opening config file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	inSection := false
	currentSection := ""

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// Check for section headers
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			currentSection = strings.Trim(trimmed, "[]")
			inSection = (currentSection == section)
			continue
		}

		// Check if this line sets our field
		if inSection {
			pattern := fmt.Sprintf(`^\s*%s\s*=\s*(.+)$`, regexp.QuoteMeta(field))
			re := regexp.MustCompile(pattern)
			if matches := re.FindStringSubmatch(line); len(matches) > 1 {
				value := strings.TrimSpace(matches[1])
				// Remove inline comments
				if idx := strings.Index(value, "#"); idx > 0 {
					// Check it's not inside a string
					if !strings.Contains(value[:idx], `"`) || strings.Count(value[:idx], `"`)%2 == 0 {
						value = strings.TrimSpace(value[:idx])
					}
				}
				fmt.Println(value)
				return
			}
		}
	}

	fmt.Printf("Key '%s' not found in %s\n", key, configFile)
	os.Exit(1)
}
