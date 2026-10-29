package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/Overclock-Validator/mithril/cmd/mithril/node"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/spf13/cobra"
)

// TestRestartExit runs the real main/Cobra boundary in a disposable subprocess.
// The synthetic RunE checks deferred cleanup ordering, not live node cleanup.
func TestRestartExit(t *testing.T) {
	if scenario := os.Getenv("MITHRIL_TEST_RESTART_EXIT"); scenario != "" {
		cmd = cobra.Command{Use: "test", SilenceUsage: true, SilenceErrors: true,
			RunE: func(c *cobra.Command, args []string) error {
				defer func() {
					if err := os.WriteFile(os.Getenv("MITHRIL_TEST_CLEANUP"), []byte("closed"), 0600); err != nil {
						panic(err)
					}
				}()
				switch scenario {
				case "clean":
					return nil
				case "fetch":
					return node.ErrRetryableBlockFetchStall
				case "wrapped":
					return fmt.Errorf("after cleanup: %w: %w", node.ErrRetryableBlockFetchStall, replay.ErrBlockFetchStalled)
				case "raw fetch":
					return replay.ErrBlockFetchStalled
				case "state failure":
					return errors.Join(replay.ErrBlockFetchStalled, errors.New("shutdown state write failed"))
				case "cancel", "cancel with fault", "cancel after qualification":
					if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
						panic(err)
					}
					<-c.Context().Done()
					if scenario == "cancel with fault" {
						return errors.New("vote history seal failed")
					}
					if scenario == "cancel after qualification" {
						return node.ErrRetryableBlockFetchStall
					}
					return nil
				default:
					return errors.New("block fetch stalled")
				}
			}}
		cmd.SetArgs(nil)
		main()
		os.Exit(0)
	}
	for _, tc := range []struct {
		name string
		want int
	}{{"clean", 0}, {"fetch", 75}, {"wrapped", 75}, {"raw fetch", 1}, {"state failure", 1},
		{"cancel", 0}, {"cancel with fault", 1}, {"cancel after qualification", 1}, {"same text, other error", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "cleanup")
			child := exec.Command(os.Args[0], "-test.run=^TestRestartExit$")
			child.Env = append(os.Environ(), "MITHRIL_TEST_RESTART_EXIT="+tc.name, "MITHRIL_TEST_CLEANUP="+marker)
			out, err := child.CombinedOutput()
			got := 0
			if err != nil {
				var status *exec.ExitError
				if !errors.As(err, &status) {
					t.Fatal(err)
				}
				got = status.ExitCode()
			}
			if got != tc.want {
				t.Errorf("exit = %d, want %d; output: %s", got, tc.want, out)
			}
			if data, err := os.ReadFile(marker); err != nil || string(data) != "closed" {
				t.Fatalf("cleanup did not complete before exit: %q, %v", data, err)
			}
		})
	}
}
