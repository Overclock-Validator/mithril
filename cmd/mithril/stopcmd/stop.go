// Package stopcmd implements `mithril stop`: a scriptable clean shutdown path
// that shares procctl's PID identity checks with the dashboard.
package stopcmd

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/procctl"
	"github.com/spf13/cobra"
)

var (
	timeoutFlag     time.Duration
	statusOnlyFlag  bool
	accountsDirFlag string

	// StopCmd is the cobra command, registered by cmd/mithril/main.go.
	StopCmd = cobra.Command{
		Use:   "stop",
		Short: "Stop the running Mithril process cleanly",
		Long: `Stop sends SIGTERM to the running Mithril process and waits for clean exit.

Safety:
  - Re-verifies process identity (PID + start-time + binary inode) before
    sending the signal, so a recycled PID will not be targeted by accident.
  - Never SIGKILLs. If --timeout elapses, exits 1 so the operator can decide
    whether to use the dashboard's Force Stop (with corruption warning).
  - Records the action in the audit log at
    $XDG_STATE_HOME/mithril/control.audit.

Examples:
  mithril stop                 # default 60s wait
  mithril stop --timeout 5m    # wait up to 5 minutes (long bootstraps)
  mithril stop --status        # show current state, don't stop`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStop()
		},
	}
)

func init() {
	StopCmd.Flags().DurationVar(&timeoutFlag, "timeout", 60*time.Second,
		"how long to wait for Mithril to exit after SIGTERM before giving up")
	StopCmd.Flags().BoolVar(&statusOnlyFlag, "status", false,
		"read-only: report current state without sending a signal")
	StopCmd.Flags().StringVar(&accountsDirFlag, "accounts", "",
		"path to AccountsDB directory (for crash detection; default: read from config)")
}

func runStop() error {
	pidPath := procctl.DefaultPidFile()
	lockPath := procctl.DefaultLockFile()
	auditPath := procctl.DefaultAuditLog()
	accountsDir := resolveAccountsDir()

	// --status: read-only probe.
	if statusOnlyFlag {
		return printStatus(pidPath, lockPath, accountsDir)
	}

	// Send SIGTERM.
	if err := procctl.SignalStop(pidPath, auditPath, os.Getpid()); err != nil {
		if errors.Is(err, procctl.ErrPidFileNotFound) {
			fmt.Fprintln(os.Stderr, "Mithril is not running (no PID file found).")
			os.Exit(2)
		}
		// Stale PID file: desired end state (not running) already holds.
		if errors.Is(err, procctl.ErrProcessNotFound) {
			fmt.Fprintln(os.Stderr, "Mithril is not running (process already exited).")
			os.Exit(2)
		}
		return fmt.Errorf("send SIGTERM: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Sent SIGTERM. Waiting up to %s for Mithril to exit...\n", timeoutFlag)
	det, err := procctl.WaitStopped(pidPath, lockPath, accountsDir, timeoutFlag)
	if err != nil {
		if errors.Is(err, procctl.ErrStopTimeout) {
			fmt.Fprintln(os.Stderr)
			fmt.Fprintf(os.Stderr, "TIMEOUT: Mithril did not exit within %s.\n", timeoutFlag)
			fmt.Fprintln(os.Stderr, "  This is normal during long operations (snapshot load, AccountsDB build).")
			fmt.Fprintln(os.Stderr, "  Options:")
			fmt.Fprintln(os.Stderr, "    - Re-run with a longer --timeout (e.g. --timeout 30m for bootstrap)")
			fmt.Fprintln(os.Stderr, "    - Use the dashboard's Force Stop (acknowledges data-loss risk)")
			fmt.Fprintln(os.Stderr)
			os.Exit(1)
		}
		return fmt.Errorf("wait for exit: %w", err)
	}

	// Report final state.
	switch det.Status {
	case procctl.StatusStopped:
		fmt.Fprintln(os.Stderr, "Mithril stopped cleanly.")
	case procctl.StatusCrashed:
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "WARNING: Mithril exited but the state file does not record a clean shutdown.")
		if det.LastShutdownReason != "" {
			fmt.Fprintf(os.Stderr, "  Last reason: %s\n", det.LastShutdownReason)
		}
		fmt.Fprintln(os.Stderr, "  Consider running `mithril doctor` before the next start.")
		fmt.Fprintln(os.Stderr)
	default:
		fmt.Fprintf(os.Stderr, "Mithril is %s.\n", det.Status)
	}
	return nil
}

// printStatus implements --status: a read-only one-shot Detect.
func printStatus(pidPath, lockPath, accountsDir string) error {
	det, err := procctl.Detect(pidPath, lockPath, accountsDir)
	if err != nil {
		return fmt.Errorf("detect: %w", err)
	}
	fmt.Printf("Status:     %s\n", det.Status)
	if det.Pid != 0 {
		fmt.Printf("PID:        %d\n", det.Pid)
	}
	if det.RunID != "" {
		fmt.Printf("Run ID:     %s\n", det.RunID)
	}
	if det.SpawnedBy != "" {
		fmt.Printf("Spawned by: %s\n", det.SpawnedBy)
	}
	if det.BinaryPath != "" {
		fmt.Printf("Binary:     %s\n", det.BinaryPath)
	}
	if det.LastShutdownReason != "" {
		fmt.Printf("Last exit:  %s\n", det.LastShutdownReason)
	}
	if det.StopInProgressBy != 0 {
		fmt.Printf("Stopping:   dashboard pid %d (since %s)\n",
			det.StopInProgressBy, det.StopInProgressAt.Format(time.RFC3339))
	}
	return nil
}

// resolveAccountsDir prefers --accounts, else the configured storage.accounts.
// Empty makes Detect skip crash classification (Status falls back to Stopped).
func resolveAccountsDir() string {
	if accountsDirFlag != "" {
		return accountsDirFlag
	}
	if err := config.InitConfig(); err == nil {
		if v := config.GetString("storage.accounts"); v != "" {
			return v
		}
		if v := config.GetString("ledger.accounts_path"); v != "" {
			return v
		}
	}
	return ""
}
