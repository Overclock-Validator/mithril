package statecmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/spf13/cobra"
)

var (
	// StateCmd is the parent command for state-related subcommands
	StateCmd = cobra.Command{
		Use:   "state",
		Short: "State file inspection and management",
		Long: `Inspect and manage the mithril_state.json file.

The state file tracks AccountsDB validity, replay progress, and cluster info.
Use this command to debug issues or share state information.

Examples:
  mithril state                    # Show state summary
  mithril state show               # Same as above
  mithril state --json             # Full JSON dump
  mithril state --full             # Include resume context
  mithril state history            # Show recent events
  mithril state validate           # Validate against AccountsDB`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Skip config loading if --accounts flag is provided
			if accountsFlag != "" {
				return nil
			}
			return config.InitConfig()
		},
		Run: func(cmd *cobra.Command, args []string) {
			runStateShow(cmd)
		},
	}

	// ShowCmd displays state information
	ShowCmd = cobra.Command{
		Use:   "show",
		Short: "Display state file summary",
		Run: func(cmd *cobra.Command, args []string) {
			runStateShow(cmd)
		},
	}

	// HistoryCmd displays state history
	HistoryCmd = cobra.Command{
		Use:   "history",
		Short: "Display state change history",
		Long: `Display the history of state changes (bootstrap, shutdown, corruption events).

Examples:
  mithril state history           # Show last 10 entries
  mithril state history --all     # Show all entries
  mithril state history --json    # Raw JSONL output`,
		Run: func(cmd *cobra.Command, args []string) {
			runStateHistory(cmd)
		},
	}

	// ValidateCmd validates state against AccountsDB
	ValidateCmd = cobra.Command{
		Use:   "validate",
		Short: "Validate state file and AccountsDB artifacts",
		Long: `Validates that the state file and AccountsDB are in a consistent state.

This checks:
- State file exists and is parseable
- Stage is "ready" (not corrupted or building)
- Required AccountsDB artifacts exist on disk

Note: Full bankhash database validation is not yet implemented.`,
		Run: func(cmd *cobra.Command, args []string) {
			runStateValidate(cmd)
		},
	}

	// Flags
	jsonOutput   bool
	fullOutput   bool
	allHistory   bool
	accountsFlag string // Override for accounts path (allows config-less usage)
)

func init() {
	// Persistent flag for accounts path (works on all subcommands)
	StateCmd.PersistentFlags().StringVar(&accountsFlag, "accounts", "", "Path to AccountsDB directory (overrides config)")

	// Flags for show command
	StateCmd.Flags().BoolVar(&jsonOutput, "json", false, "Output full JSON")
	StateCmd.Flags().BoolVar(&fullOutput, "full", false, "Include resume context and blockhash details")
	ShowCmd.Flags().BoolVar(&jsonOutput, "json", false, "Output full JSON")
	ShowCmd.Flags().BoolVar(&fullOutput, "full", false, "Include resume context and blockhash details")

	// Flags for history command
	HistoryCmd.Flags().BoolVar(&jsonOutput, "json", false, "Output raw JSONL")
	HistoryCmd.Flags().BoolVar(&allHistory, "all", false, "Show all history entries")

	// Add subcommands
	StateCmd.AddCommand(&ShowCmd)
	StateCmd.AddCommand(&HistoryCmd)
	StateCmd.AddCommand(&ValidateCmd)
}

func getAccountsPath() string {
	// Check --accounts flag first (allows config-less usage)
	if accountsFlag != "" {
		return accountsFlag
	}

	// Fall back to config file
	path := config.GetString("storage.accounts")
	if path == "" {
		path = config.GetString("ledger.accounts_path") // Legacy fallback
	}
	if path == "" {
		fmt.Println("Error: storage.accounts not configured")
		fmt.Println("Use: mithril state --config config.toml")
		fmt.Println("  or: mithril state --accounts /path/to/accountsdb")
		os.Exit(1)
	}
	return path
}

func runStateShow(cmd *cobra.Command) {
	accountsPath := getAccountsPath()

	s, err := state.LoadState(accountsPath)
	if err != nil {
		fmt.Printf("Error loading state: %v\n", err)
		os.Exit(1)
	}

	if s == nil {
		fmt.Printf("No state file found at %s\n", filepath.Join(accountsPath, state.StateFileName))
		os.Exit(1)
	}

	if jsonOutput {
		data, _ := json.MarshalIndent(s, "", "  ")
		fmt.Println(string(data))
		return
	}

	// Pretty print
	stateFile := filepath.Join(accountsPath, state.StateFileName)
	fmt.Println("=== Mithril State ===")
	fmt.Printf("Path: %s\n", stateFile)
	fmt.Printf("Stage: %s\n", s.Stage)
	fmt.Printf("Schema Version: %d\n", s.StateSchemaVersion)
	fmt.Println()

	// Snapshot info
	fmt.Println("Snapshot:")
	fmt.Printf("  Slot: %d (epoch %d)\n", s.SnapshotSlot, s.SnapshotEpoch)
	if s.FullSnapshot != nil {
		fmt.Printf("  Full: %s\n", s.FullSnapshot.Path)
	}
	if s.IncrSnapshot != nil {
		fmt.Printf("  Incremental: %s (base %d)\n", s.IncrSnapshot.Path, s.IncrSnapshot.BaseSlot)
	}
	fmt.Println()

	// Replay progress
	fmt.Println("Replay Progress:")
	if s.LastSlot > 0 {
		fmt.Printf("  Last slot: %d (epoch %d)\n", s.LastSlot, s.LastEpoch)
		fmt.Printf("  Last bankhash: %s\n", truncateHash(s.LastBankhash))
		fmt.Printf("  Slots replayed: %d\n", s.LastSlot-s.SnapshotSlot)
	} else {
		fmt.Printf("  No replay yet (fresh from snapshot)\n")
	}
	fmt.Println()

	// Cluster info
	if s.Cluster != "" || s.GenesisHash != "" {
		fmt.Println("Cluster:")
		if s.Cluster != "" {
			fmt.Printf("  Network: %s\n", s.Cluster)
		}
		if s.GenesisHash != "" {
			fmt.Printf("  Genesis: %s\n", truncateHash(s.GenesisHash))
		}
		fmt.Println()
	}

	// Writer info
	fmt.Println("Writer:")
	if s.LastWriterVersion != "" {
		fmt.Printf("  Version: %s\n", s.LastWriterVersion)
	}
	commit := s.LastWriterCommit
	if commit == "" {
		commit = s.LastCommit // Legacy fallback
	}
	if commit != "" {
		fmt.Printf("  Commit: %s\n", commit)
	}
	fmt.Println()

	// Run info
	if s.LastRunID != "" {
		fmt.Println("Last Run:")
		fmt.Printf("  ID: %s\n", s.LastRunID)
		if !s.LastRunAt.IsZero() {
			fmt.Printf("  Started: %s (%s ago)\n", s.LastRunAt.Format(time.RFC3339), formatDuration(time.Since(s.LastRunAt)))
		}
		if s.LastShutdownReason != "" {
			fmt.Printf("  Shutdown: %s\n", s.LastShutdownReason)
		}
		fmt.Println()
	}

	// Corruption info (if applicable)
	if s.IsCorrupted() {
		fmt.Println("CORRUPTION:")
		fmt.Printf("  Reason: %s\n", s.CorruptionReason)
		if !s.CorruptionDetectedAt.IsZero() {
			fmt.Printf("  Detected: %s\n", s.CorruptionDetectedAt.Format(time.RFC3339))
		}
		fmt.Println()
	}

	// Full details (resume data)
	if fullOutput && s.HasResumeData() {
		fmt.Println("Resume Data:")
		fmt.Printf("  AcctsLtHash: %s...\n", truncate(s.LastAcctsLtHash, 20))
		fmt.Printf("  LamportsPerSig: %d\n", s.LastLamportsPerSignature)
		fmt.Printf("  NumSignatures: %d\n", s.LastNumSignatures)
		fmt.Printf("  RecentBlockhashes: %d entries\n", len(s.LastRecentBlockhashes))
		fmt.Printf("  SlotHashes: %d entries\n", len(s.LastSlotHashes))
		fmt.Println()
	}
}

func runStateHistory(cmd *cobra.Command) {
	accountsPath := getAccountsPath()

	var entries []state.StateHistoryEntry
	var err error

	if allHistory {
		entries, err = state.LoadHistory(accountsPath)
	} else {
		entries, err = state.GetRecentHistory(accountsPath, 10)
	}

	if err != nil {
		fmt.Printf("Error loading history: %v\n", err)
		os.Exit(1)
	}

	if len(entries) == 0 {
		fmt.Println("No history entries found")
		return
	}

	if jsonOutput {
		for _, entry := range entries {
			data, _ := json.Marshal(entry)
			fmt.Println(string(data))
		}
		return
	}

	fmt.Printf("=== State History (%d entries) ===\n\n", len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		fmt.Printf("[%s] %s\n", entry.Timestamp.Format("2006-01-02 15:04:05"), entry.Event)
		fmt.Printf("  Slot: %d\n", entry.Slot)
		if entry.RunID != "" {
			fmt.Printf("  Run: %s\n", entry.RunID)
		}
		if entry.Version != "" || entry.Commit != "" {
			fmt.Printf("  Writer: %s (%s)\n", entry.Version, entry.Commit)
		}
		if entry.Reason != "" {
			fmt.Printf("  Reason: %s\n", entry.Reason)
		}
		fmt.Println()
	}
}

func runStateValidate(cmd *cobra.Command) {
	accountsPath := getAccountsPath()

	s, err := state.LoadState(accountsPath)
	if err != nil {
		fmt.Printf("Error loading state: %v\n", err)
		os.Exit(1)
	}

	if s == nil {
		fmt.Printf("No state file found at %s\n", filepath.Join(accountsPath, state.StateFileName))
		os.Exit(1)
	}

	fmt.Println("State file validation:")
	fmt.Printf("  Stage: %s\n", s.Stage)

	if s.IsCorrupted() {
		fmt.Printf("  Status: CORRUPTED\n")
		fmt.Printf("  Reason: %s\n", s.CorruptionReason)
		os.Exit(1)
	}

	if !s.IsReady() {
		fmt.Printf("  Status: NOT READY (stage=%s)\n", s.Stage)
		os.Exit(1)
	}

	// Check artifacts exist
	if err := state.ValidateAccountsDbArtifacts(accountsPath); err != nil {
		fmt.Printf("  Status: ARTIFACTS MISSING\n")
		fmt.Printf("  Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("  Status: OK\n")
	fmt.Printf("  Snapshot: %d (epoch %d)\n", s.SnapshotSlot, s.SnapshotEpoch)
	if s.LastSlot > 0 {
		fmt.Printf("  Last slot: %d (epoch %d)\n", s.LastSlot, s.LastEpoch)
	}

	// Note: Full bankhash validation requires opening the AccountsDB
	// which is expensive. For now, just validate artifacts exist.
	fmt.Println("\nNote: Use 'mithril run' to perform full bankhash validation.")
}

// Helper functions

func truncateHash(hash string) string {
	if len(hash) > 12 {
		return hash[:8] + "..." + hash[len(hash)-4:]
	}
	return hash
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return "< 1m"
	}

	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60

	if days > 0 {
		if hours > 0 {
			return fmt.Sprintf("%dd %dh", days, hours)
		}
		return fmt.Sprintf("%dd", days)
	}

	if hours > 0 {
		if minutes > 0 {
			return fmt.Sprintf("%dh %dm", hours, minutes)
		}
		return fmt.Sprintf("%dh", hours)
	}

	return fmt.Sprintf("%dm", minutes)
}
