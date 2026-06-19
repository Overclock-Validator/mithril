package statuscmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/config"
	statepkg "github.com/Overclock-Validator/mithril/pkg/state"
)

func TestStatusStateSearchPaths_UsesConfiguredAccountsBeforeDefaults(t *testing.T) {
	paths := statusStateSearchPaths("", "/var/mithril/accounts", "/legacy/accounts")

	if len(paths) < 2 {
		t.Fatalf("expected configured and legacy paths, got %v", paths)
	}
	if paths[0] != "/var/mithril/accounts" {
		t.Fatalf("expected configured accounts path first, got %q", paths[0])
	}
	if paths[1] != "/legacy/accounts" {
		t.Fatalf("expected legacy accounts path second, got %q", paths[1])
	}
}

func TestStatusStateSearchPaths_CLIAccountsOverridesConfig(t *testing.T) {
	paths := statusStateSearchPaths("/cli/accounts", "/var/mithril/accounts", "/legacy/accounts")

	if len(paths) != 1 || paths[0] != "/cli/accounts" {
		t.Fatalf("expected CLI accounts path to be the only search path, got %v", paths)
	}
}

func TestStatusStateSearchPaths_DeduplicatesFallbacks(t *testing.T) {
	paths := statusStateSearchPaths("", "/same/accounts", "/same/accounts")

	seen := map[string]bool{}
	for _, path := range paths {
		if seen[path] {
			t.Fatalf("path %q appeared more than once in %v", path, paths)
		}
		seen[path] = true
	}
}

func TestLoadStatusState_FindsStateInConfiguredPath(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, statepkg.StateFileName)
	if err := os.WriteFile(statePath, []byte(`{
  "snapshot_slot": 100,
  "last_slot": 123,
  "last_bankhash": "abcdef1234567890",
  "last_shutdown_reason": "graceful shutdown (Ctrl+C)"
}`), 0644); err != nil {
		t.Fatalf("write state file: %v", err)
	}

	nodeState, foundPath, ok := loadStatusState([]string{filepath.Join(t.TempDir(), "missing"), dir})
	if !ok {
		t.Fatal("expected state file to be found")
	}
	if foundPath != statePath {
		t.Fatalf("expected state path %q, got %q", statePath, foundPath)
	}
	if nodeState.LastSlot != 123 || nodeState.SnapshotSlot != 100 {
		t.Fatalf("unexpected state loaded: %+v", nodeState)
	}
}

func TestMissingStateStatusText_RunningProcessMeansBootstrapPending(t *testing.T) {
	headline, detail := missingStateStatusText(true)

	if headline != "State file: not ready yet" {
		t.Fatalf("unexpected headline: %q", headline)
	}
	if detail != "Mithril is running; AccountsDB has not produced a ready state yet (bootstrap/build in progress)" {
		t.Fatalf("unexpected detail: %q", detail)
	}
}

func TestMissingStateStatusText_NotRunningPreservesOriginalGuidance(t *testing.T) {
	headline, detail := missingStateStatusText(false)

	if headline != "No state file found" {
		t.Fatalf("unexpected headline: %q", headline)
	}
	if detail != "Mithril hasn't run yet, or --accounts path is wrong" {
		t.Fatalf("unexpected detail: %q", detail)
	}
}

func TestStatusDetectionAccountsPath_UsesRuntimeStoragePrecedence(t *testing.T) {
	if got := statusDetectionAccountsPath("/cli", "/configured", "/legacy"); got != "/cli" {
		t.Fatalf("expected CLI path, got %q", got)
	}
	if got := statusDetectionAccountsPath("", "/configured", "/legacy"); got != "/configured" {
		t.Fatalf("expected configured path, got %q", got)
	}
	if got := statusDetectionAccountsPath("", "", "/legacy"); got != "/legacy" {
		t.Fatalf("expected legacy path, got %q", got)
	}
	if got := statusDetectionAccountsPath("", "", ""); got != config.DefaultStoragePaths().Accounts {
		t.Fatalf("expected default storage path, got %q", got)
	}
}
