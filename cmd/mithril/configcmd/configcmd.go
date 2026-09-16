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
	                                   (identity keypair + vote-account address, turbine
	                                   gossip entrypoint, advertised IP, TPU and
	                                   Votor QUIC listeners). Block production is
	                                   active and the node casts Alpenglow votes.

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
		fmt.Println("  3. Set [validator] identity_keypair, vote_account_keypair, and advertised_ip —")
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

	validatorNote := ""
	if validator {
		validatorNote = " (validator profile)"
	}

	// The [validator] + [consensus] sections are the profile split: a
	// verifying node needs neither keypairs nor the Votor listener; validator
	// mode REQUIRES an identity keypair + vote-account address, a turbine gossip
	// entrypoint, and the Votor QUIC listener (enforced at startup).
	nodeSections := `[validator]
identity_keypair = ""              # Validator identity — advertises this node into turbine gossip; set for a staked Alpenglow node
vote_account_keypair = ""          # Vote account public key or keypair path
authorized_voter_keypair = ""      # BLS derivation signer (empty defaults to identity_keypair, like Agave)
authorized_withdrawer_keypair = "" # Authorized withdrawer keypair path (diagnostics only)
tpu_quic_bind_addr = "0.0.0.0:8004"
advertised_ip = ""                 # Required only in validator mode; public IP advertised for TPU QUIC
tpu_sigverify_workers = 0          # 0 = GOMAXPROCS

[consensus]
mode = "verifying"                # "verifying" (default, non-voting) | "validator"
alpenglow_observer_bind_addr = "" # Votor QUIC cert listener, e.g. "0.0.0.0:8010" (empty = rely on footer certs in shreds)
alpenglow_max_message_bytes = 0   # 0 = default
alpenglow_bls_dst = ""            # BLS DST override (must match cluster solana-bls version)`
	if validator {
		nodeSections = `[validator]
# REQUIRED in validator mode — the node refuses to start without these values.
identity_keypair = "/path/to/validator-keypair.json"        # Signs gossip/turbine and authenticates Votor QUIC
vote_account_keypair = "/path/to/vote-account-keypair.json" # The vote account votes are cast for
authorized_voter_keypair = ""                               # Empty defaults to identity_keypair, matching Agave
# NOT required at runtime — keep the withdrawer keypair OFFLINE.
authorized_withdrawer_keypair = ""
tpu_quic_bind_addr = "0.0.0.0:8004"
advertised_ip = "" # REQUIRED: public IP advertised for TPU QUIC
tpu_sigverify_workers = 0

[consensus]
# Validator mode enables TPU ingress, block production, and Votor voting.
mode = "validator"
alpenglow_observer_bind_addr = "0.0.0.0:8010" # REQUIRED: Votor QUIC vote/cert listener
alpenglow_max_message_bytes = 0               # 0 = default
alpenglow_bls_dst = ""                        # BLS DST override (must match cluster solana-bls version)`
	}
	return fmt.Sprintf(`# Mithril Configuration
# Generated by: mithril config init%s
# Every operationally-relevant option is listed with its default; see
# config.example.toml for the full reference and advanced tuning.

name = "mithril"

# ── Startup ──────────────────────────────────────────────────────────────
[bootstrap]
mode = "auto"   # "auto" (reuse AccountsDB if valid, else snapshot) | "snapshot" | "new-snapshot" (always fresh) | "accountsdb" (require existing)

# ── Storage ──────────────────────────────────────────────────────────────
[storage]
accounts = %q             # AccountsDB — put on the fastest NVMe
shredstore = %q           # Received-shred storage
snapshots = %q            # Downloaded full + incremental snapshots
logs = %q                 # Log files (created if missing)
fold_batch_slots = 128    # Rooted slots folded to disk per segment (32..512; larger = less NVMe wear, more RAM tail)
index_wal = true          # Account-index Pebble WAL (keep true until soaked)
rewind_horizon_batches = 64 # Fold batches of undo pointers kept actionable (~55 min of chain at K=128)

# ── Network ──────────────────────────────────────────────────────────────
[network]
cluster = "alpenglow"     # Also supports mainnet-beta/testnet/devnet in verifying-only RPC mode
rpc = ["https://rpc.ag.validator1.net"]  # First = primary, rest = fallbacks. Validator mode uses it for control-plane data (tip/stakes/snapshots), never blocks; verifying mode may use block RPC.

# ── Block source ─────────────────────────────────────────────────────────
[block]
# "turbine" is the live mode (shreds carry Alpenglow block ids + footer certs).
# "rpc" is catch-up/debug only. "lightbringer" uses the sidecar.
source = "turbine"
turbine_bind_addr = "0.0.0.0:8001"    # REQUIRED for turbine: the shred receive port (open inbound UDP)
rpc_fallback = false                  # false (default): with source="turbine", shreds via turbine + repair are the ONLY block path. Validator mode also disables the RPC trailing verifier and validates local execution against the certified footer bank hash. true: RPC may catch up when replay is more than repair_catchup_max_gap_slots behind. Ignored for source="rpc"/"lightbringer" (they need RPC catchup).
repair_catchup_max_gap_slots = 8192   # Gaps within this fill via turbine repair, which then OWNS catchup (no timer fallback to RPC). Only meaningful with rpc_fallback = true; ignored in shreds-only mode.
# repair_max_requests_per_second = 0 # Repair request-rate. 0/unset = AUTO: starts at 15000/s and an AIMD controller ramps toward 50000/s while peers keep up, backing off as low as 2500/s on a serve-repair throttle signal. Set a positive value to PIN a fixed rate (turns the controller off — an explicit choice). Peer QoS deprioritizes/drops heavy unstaked requesters, so watch "qos: throttle-suspect intervals" and the timeout/late counts in the repair heartbeat.
# lightbringer_endpoint = "localhost:9000"
# --- Fetch tuning (RPC catchup) ---
max_rps = 8               # Max block-fetch requests/sec (match your RPC's limit)
max_inflight = 8          # Max concurrent fetch workers
tip_poll_interval_ms = 1000
tip_safety_margin = 32    # Don't fetch within N slots of tip in catchup
near_tip_threshold = 32   # Enter near-tip mode when gap <= this
catchup_threshold = 64    # Exit near-tip mode when gap >= this
near_tip_poll_interval_ms = 500
near_tip_lookahead = 2

# ── Turbine (native shred receiver; source = "turbine") ──────────────────
[turbine]
gossip_entrypoint = ""    # REQUIRED for turbine: a gossip entrypoint of your Alpenglow cluster (host:port)
gossip_bind_addr = "0.0.0.0:65401"  # Local gossip UDP port (open inbound); empty = OS-assigned
# Gossip-connected nodes automatically retransmit verified broadcast shreds to their Agave-compatible downstream Turbine peers; repair responses are never forwarded.
# advertised_ip = ""      # Public IP peers reach you at; empty = auto-detect via the entrypoint's IP-echo
# shred_version = 0       # 0 = auto-discover from the entrypoint (setting it wrong silently drops all shreds)

%s

# ── Snapshot download ────────────────────────────────────────────────────
[snapshot]
incremental_threshold = 2000   # Freshness band (slots behind tip): full sources restricted to bases with an incremental this fresh; staler incrementals rejected
full_threshold = 100000        # Max full-snapshot age (slots) still usable
stale_prompt_slots = 500       # On start, prompt (continue / rebuild / new full / new incremental) when the AccountsDB is more than this many slots behind tip
max_full_snapshots = 1         # Snapshots kept on disk (0 = stream-only, don't save)
min_incremental_speed_mbs = 2.0
max_rtt_ms = 200               # Skip snapshot sources slower than this RTT (0 = disabled)
verbose = false                # Verbose source-probe reporting
# min_node_version = "0.3.0"   # Filter snapshot sources by node version (empty = no filter)

# ── Execution verification ───────────────────────────────────────────────
[verifier]
enabled = true            # Verifying mode only: re-derive per-tx digests from finalized RPC blocks. Validator mode ignores this and never fetches RPC blocks.
required = true           # Verifying mode: gate folds on the RPC-verified watermark. Validator folds gate on certificates after local footer bank-hash parity.
lag_slots = 32            # Verify slots this far behind the executed tip
max_rps = 8               # Verifier's own RPC budget (never shares the block-fetch budget)

# ── Replay tuning ────────────────────────────────────────────────────────
[tuning]
txpar = 24                # Validator auto-defaults to 2x CPU cores only when unset; explicit 0 = sequential
sigverify_backend = "auto" # auto|r51|generic|stdlib; stdlib uses Go's crypto/ed25519 impl after strict checks.

# ── Mithril's RPC server ─────────────────────────────────────────────────
[rpc]
port = 8899               # 0 = disabled; binds all interfaces

# ── Logging ──────────────────────────────────────────────────────────────
[log]
dir = %q                  # Log directory
level = "info"            # "debug" | "info" | "warn" | "error"
to_stdout = true          # Also write to stdout
max_size_mb = 100         # Rotate a log file at this size
max_backups = 10          # Rotated files kept (retention ~= max_size_mb x max_backups; age-deletion is off by default)
# max_age_days = 0        # Delete logs older than N days (0 = never, the default)

# Advanced/optional sections not shown here (see config.example.toml):
#   [lightbringer]  sidecar block source     [debug]  per-tx/account trace logging
#   [reporting]     metrics output
`, validatorNote, s.Accounts, s.Shredstore, s.Snapshots, s.Logs, nodeSections, s.Logs)
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
	// Check if it's a float — ParseFloat rejects numeric prefixes such as IP addresses.
	if f, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
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
