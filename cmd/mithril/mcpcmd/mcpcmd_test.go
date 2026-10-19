package mcpcmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/mcp"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func resolvedConfig() (mcp.Config, error) {
	return resolvedConfigWithOverrides(resolvedConfigOverrides{})
}

func clearMCPNodeSettingEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"MITHRIL_ACCOUNTS_PATH", "MITHRIL_SNAPSHOTS_PATH", "MITHRIL_SHREDSTORE_PATH",
		"MITHRIL_LOG_DIR", "MITHRIL_STATE_PATH", "MITHRIL_REPLAY_PATH", "MITHRIL_NODE_CGROUP_PATH",
		"MITHRIL_METRICS_URL", "MITHRIL_RPC_URL", "MITHRIL_PPROF_URL", "MITHRIL_BLOCK_SOURCE",
	} {
		value, present := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if present {
				_ = os.Setenv(name, value)
			} else {
				_ = os.Unsetenv(name)
			}
		})
	}
}

func setConfigFileForTest(t *testing.T, path string) {
	t.Helper()
	original := config.ConfigFile
	t.Cleanup(func() { config.ConfigFile = original })
	config.ConfigFile = path
}

func setRemoteConfigFlagsForTest(t *testing.T, target, binary, remoteConfig string) {
	t.Helper()
	originalTarget := configSSHTarget
	originalBinary := configRemoteBinary
	originalConfig := configRemoteConfig
	t.Cleanup(func() {
		configSSHTarget = originalTarget
		configRemoteBinary = originalBinary
		configRemoteConfig = originalConfig
	})
	configSSHTarget = target
	configRemoteBinary = binary
	configRemoteConfig = remoteConfig
}

func writeConfigFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node.toml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDiscoverStoragePathsUsesReadOnlyExistenceProbe(t *testing.T) {
	dir := t.TempDir()
	var probed []string
	paths := discoverStoragePaths("/home/tester", func(path string) (os.FileInfo, error) {
		probed = append(probed, path)
		return os.Stat(dir)
	})
	if len(probed) != 1 || probed[0] != "/mnt/mithril-accounts" {
		t.Fatalf("stat probes = %v, want one accounts-root probe", probed)
	}
	if paths.Accounts != "/mnt/mithril-accounts" || paths.Logs != "/mnt/mithril-logs" || paths.Snapshots != "/mnt/mithril-ledger/snapshots" || paths.Shredstore != "/mnt/mithril-ledger/shredstore" {
		t.Fatalf("production paths = %+v", paths)
	}
}

func TestDiscoverStoragePathsFallsBackToHome(t *testing.T) {
	paths := discoverStoragePaths("/home/tester", func(string) (os.FileInfo, error) {
		return nil, errors.New("not present")
	})
	base := filepath.Join("/home/tester", ".mithril")
	if paths.Accounts != filepath.Join(base, "accounts") ||
		paths.Snapshots != filepath.Join(base, "snapshots") ||
		paths.Logs != filepath.Join(base, "logs") ||
		paths.Shredstore != filepath.Join(base, "shredstore") {
		t.Fatalf("fallback paths = %+v", paths)
	}
}

func TestResolvedConfigEnvironmentPathsWin(t *testing.T) {
	setConfigFileForTest(t, "")
	t.Setenv("MITHRIL_LOG_DIR", "/configured/logs")
	t.Setenv("MITHRIL_ACCOUNTS_PATH", "/configured/accounts")
	t.Setenv("MITHRIL_SNAPSHOTS_PATH", "/configured/snapshots")
	t.Setenv("MITHRIL_SHREDSTORE_PATH", "/configured/shredstore")
	t.Setenv("MITHRIL_STATE_PATH", "/configured/state.json")
	t.Setenv("MITHRIL_REPLAY_PATH", "/configured/replay.jsonl")
	t.Setenv("MITHRIL_PPROF_URL", "http://127.0.0.1:7777")
	t.Setenv("MITHRIL_MCP_PROFILE", "diagnostic")

	cfg, err := resolvedConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AccountsDir != "/configured/accounts" || cfg.SnapshotsDir != "/configured/snapshots" || cfg.ShredstoreDir != "/configured/shredstore" ||
		cfg.LogDir != "/configured/logs" || cfg.StatePath != "/configured/state.json" || cfg.ReplayPath != "/configured/replay.jsonl" || cfg.PprofURL != "http://127.0.0.1:7777" {
		t.Fatalf("environment path overrides were not preserved: %+v", cfg)
	}
	if cfg.Profile != mcp.ProfileDiagnostic {
		t.Fatalf("Profile = %q, want %q", cfg.Profile, mcp.ProfileDiagnostic)
	}
}

func TestResolvedConfigPreservesExplicitlyDisabledMetrics(t *testing.T) {
	clearMCPNodeSettingEnv(t)
	setConfigFileForTest(t, "")
	t.Setenv("MITHRIL_METRICS_URL", "")
	cfg, err := resolvedConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MetricsURL != "" {
		t.Fatalf("explicitly disabled metrics URL = %q", cfg.MetricsURL)
	}
}

func TestResolvedConfigRejectsInvalidBlockSource(t *testing.T) {
	clearMCPNodeSettingEnv(t)
	setConfigFileForTest(t, "")
	t.Setenv("MITHRIL_BLOCK_SOURCE", "lightbrigner")
	if _, err := resolvedConfig(); err == nil || !strings.Contains(err.Error(), "block source") {
		t.Fatalf("invalid block source error = %v", err)
	}
}

func TestResolvedConfigWithoutNodeConfigLeavesBlockSourceUnknown(t *testing.T) {
	clearMCPNodeSettingEnv(t)
	setConfigFileForTest(t, "")

	cfg, err := resolvedConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BlockSource != "" {
		t.Fatalf("standalone block source = %q, want unknown", cfg.BlockSource)
	}
}

func TestResolvedConfigUsesExplicitNodeSettings(t *testing.T) {
	clearMCPNodeSettingEnv(t)

	path := writeConfigFile(t, `
[storage]
accounts = '/node/accounts'
logs = '/node/logs'
snapshots = '/storage/snapshots'
shredstore = '/node/shredstore'
blockstore = '/legacy/shredstore'
[snapshot]
download_path = '/node/snapshots'
[ledger]
path = '/older/shredstore'
[rpc]
bind_address = '192.0.2.10'
port = 7788
[tuning.pprof]
port = 6677
[lightbringer]
enabled = true
`)
	setConfigFileForTest(t, path)
	cfg, err := resolvedConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AccountsDir != "/node/accounts" || cfg.SnapshotsDir != "/node/snapshots" || cfg.ShredstoreDir != "/node/shredstore" ||
		cfg.LogDir != "/node/logs" || cfg.StatePath != "/node/accounts/mithril_state.json" || cfg.ReplayPath != "/node/logs/replay_timings.jsonl" ||
		cfg.RPCURL != "http://127.0.0.1:7788" || cfg.PprofURL != "http://127.0.0.1:6677" || cfg.BlockSource != "lightbringer" {
		t.Fatalf("node storage paths were not applied: %+v", cfg)
	}
}

func TestResolvedConfigMatchesNodeBlockSourceRules(t *testing.T) {
	tests := []struct {
		name string
		toml string
		want string
	}{
		{
			name: "omitted cluster and source",
			toml: "[storage]\naccounts = '/node/accounts'\n",
			want: "turbine",
		},
		{
			name: "alpenglow default",
			toml: "[network]\ncluster = 'alpenglow'\n",
			want: "turbine",
		},
		{
			name: "mainnet beta default",
			toml: "[network]\ncluster = 'mainnet-beta'\n",
			want: "rpc",
		},
		{
			name: "testnet default",
			toml: "[network]\ncluster = 'testnet'\n",
			want: "rpc",
		},
		{
			name: "devnet default",
			toml: "[network]\ncluster = 'devnet'\n",
			want: "rpc",
		},
		{
			name: "lightbringer replaces protocol default",
			toml: "[lightbringer]\nenabled = true\n",
			want: "lightbringer",
		},
		{
			name: "explicit rpc survives lightbringer",
			toml: "[network]\ncluster = 'alpenglow'\n[block]\nsource = 'rpc'\n[lightbringer]\nenabled = true\n",
			want: "rpc",
		},
		{
			name: "explicit turbine on classic cluster",
			toml: "[network]\ncluster = 'mainnet-beta'\n[block]\nsource = 'turbine'\n",
			want: "turbine",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearMCPNodeSettingEnv(t)
			setConfigFileForTest(t, writeConfigFile(t, test.toml))

			cfg, err := resolvedConfig()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.BlockSource != test.want {
				t.Fatalf("block source = %q, want %q", cfg.BlockSource, test.want)
			}
		})
	}
}

func TestResolvedConfigRejectsInvalidNodeClusterOrBlockSource(t *testing.T) {
	tests := []struct {
		name    string
		toml    string
		wantErr string
	}{
		{
			name:    "cluster",
			toml:    "[network]\ncluster = 'localnet'\n",
			wantErr: "invalid network.cluster",
		},
		{
			name:    "block source",
			toml:    "[block]\nsource = 'unknown'\n",
			wantErr: "invalid block.source",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearMCPNodeSettingEnv(t)
			t.Setenv("MITHRIL_BLOCK_SOURCE", "rpc")
			setConfigFileForTest(t, writeConfigFile(t, test.toml))

			if _, err := resolvedConfig(); err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("invalid node config error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestResolvedConfigRejectsRelativeFilePaths(t *testing.T) {
	clearMCPNodeSettingEnv(t)

	setConfigFileForTest(t, writeConfigFile(t, "[storage]\naccounts = 'relative/accounts'\nlogs = 'relative/logs'\n"))
	if _, err := resolvedConfig(); err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("relative node storage paths error = %v", err)
	}

	config.ConfigFile = ""
	t.Setenv("MITHRIL_STATE_PATH", "relative/state.json")
	if _, err := resolvedConfig(); err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("relative environment path error = %v", err)
	}
}

func TestResolvedConfigEnvironmentOverridesNodeConfig(t *testing.T) {
	path := writeConfigFile(t, `
[storage]
accounts = '/node/accounts'
logs = '/node/logs'
snapshots = '/node/snapshots'
shredstore = '/node/shredstore'
[rpc]
port = 7788
[tuning.pprof]
port = 6677
`)
	setConfigFileForTest(t, path)
	t.Setenv("MITHRIL_LOG_DIR", "/env/logs")
	t.Setenv("MITHRIL_ACCOUNTS_PATH", "/env/accounts")
	t.Setenv("MITHRIL_SNAPSHOTS_PATH", "/env/snapshots")
	t.Setenv("MITHRIL_SHREDSTORE_PATH", "/env/shredstore")
	t.Setenv("MITHRIL_STATE_PATH", "/env/state.json")
	t.Setenv("MITHRIL_REPLAY_PATH", "/env/replay.jsonl")
	t.Setenv("MITHRIL_RPC_URL", "http://127.0.0.1:8898")
	t.Setenv("MITHRIL_PPROF_URL", "http://127.0.0.1:6068")
	t.Setenv("MITHRIL_BLOCK_SOURCE", "turbine")

	cfg, err := resolvedConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AccountsDir != "/env/accounts" || cfg.SnapshotsDir != "/env/snapshots" || cfg.ShredstoreDir != "/env/shredstore" ||
		cfg.LogDir != "/env/logs" || cfg.StatePath != "/env/state.json" || cfg.ReplayPath != "/env/replay.jsonl" ||
		cfg.RPCURL != "http://127.0.0.1:8898" || cfg.PprofURL != "http://127.0.0.1:6068" || cfg.BlockSource != "turbine" {
		t.Fatalf("environment did not override node config: %+v", cfg)
	}
}

func TestResolvedConfigHonorsDisabledAndLegacyPorts(t *testing.T) {
	clearMCPNodeSettingEnv(t)
	path := filepath.Join(t.TempDir(), "node.toml")
	setConfigFileForTest(t, path)

	if err := os.WriteFile(path, []byte("[rpc]\nport = 0\n[tuning.pprof]\nport = -1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := resolvedConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RPCURL != "" || cfg.PprofURL != "" {
		t.Fatalf("disabled node endpoints were not preserved: %+v", cfg)
	}

	if err := os.WriteFile(path, []byte("[rpc]\nport = 8891\n[tuning.pprof]\nport = 0\n[development.pprof]\nport = 6061\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = resolvedConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RPCURL != "http://127.0.0.1:8891" || cfg.PprofURL != "http://127.0.0.1:6061" {
		t.Fatalf("node pprof fallback/RPC ports were not applied: %+v", cfg)
	}
}

func TestResolvedConfigExplicitNodeConfigUsesDisabledEndpointDefaults(t *testing.T) {
	clearMCPNodeSettingEnv(t)
	setConfigFileForTest(t, writeConfigFile(t, "[development.pprof]\nport = 6061\n"))

	cfg, err := resolvedConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RPCURL != "" || cfg.PprofURL != "" {
		t.Fatalf("omitted node endpoints were not disabled: %+v", cfg)
	}
}

func TestResolvedConfigUsesNodeLogDefaultWhenConfigOmitsStorageLogs(t *testing.T) {
	clearMCPNodeSettingEnv(t)
	setConfigFileForTest(t, writeConfigFile(t, "[storage]\naccounts = '/custom/accounts'\n"))

	cfg, err := resolvedConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LogDir != "/mnt/mithril-logs" || cfg.ReplayPath != "/mnt/mithril-logs/replay_timings.jsonl" || cfg.StatePath != "/custom/accounts/mithril_state.json" {
		t.Fatalf("explicit node config paths do not match node defaults: %+v", cfg)
	}
}

func TestResolvedConfigExplicitEmptyEnvironmentOverridesNodeConfig(t *testing.T) {
	clearMCPNodeSettingEnv(t)
	path := writeConfigFile(t, `
[rpc]
port = 8891
[tuning.pprof]
port = 6061
`)
	setConfigFileForTest(t, path)
	for _, name := range []string{
		"MITHRIL_ACCOUNTS_PATH", "MITHRIL_SNAPSHOTS_PATH", "MITHRIL_SHREDSTORE_PATH",
		"MITHRIL_RPC_URL", "MITHRIL_PPROF_URL", "MITHRIL_BLOCK_SOURCE",
	} {
		t.Setenv(name, "")
	}

	cfg, err := resolvedConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AccountsDir != "" || cfg.SnapshotsDir != "" || cfg.ShredstoreDir != "" ||
		cfg.RPCURL != "" || cfg.PprofURL != "" {
		t.Fatalf("explicit empty environment values did not clear node settings: %+v", cfg)
	}
}

func TestResolvedConfigPreservesDisabledFileLogging(t *testing.T) {
	clearMCPNodeSettingEnv(t)

	setConfigFileForTest(t, writeConfigFile(t, "[storage]\naccounts = '/node/accounts'\nlogs = ''\n"))
	cfg, err := resolvedConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LogDir != "" || cfg.ReplayPath != "" || cfg.StatePath != "/node/accounts/mithril_state.json" {
		t.Fatalf("disabled file logging was not preserved: %+v", cfg)
	}

	t.Setenv("MITHRIL_LOG_DIR", "/env/logs")
	cfg, err = resolvedConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LogDir != "/env/logs" || cfg.ReplayPath != "/env/logs/replay_timings.jsonl" {
		t.Fatalf("environment did not override disabled file logging: %+v", cfg)
	}
}

func TestResolvedConfigRejectsExplicitMissingOrInvalidConfig(t *testing.T) {
	setConfigFileForTest(t, filepath.Join(t.TempDir(), "missing.toml"))
	if _, err := resolvedConfig(); err == nil || !strings.Contains(err.Error(), "read MCP node config") {
		t.Fatalf("missing explicit config error = %v", err)
	}
	config.ConfigFile = writeConfigFile(t, "[storage\naccounts = ???")
	if _, err := resolvedConfig(); err == nil || !strings.Contains(err.Error(), "read MCP node config") {
		t.Fatalf("invalid explicit config error = %v", err)
	}
	config.ConfigFile = writeConfigFile(t, "[rpc]\nport = 70000\n")
	if _, err := resolvedConfig(); err == nil || !strings.Contains(err.Error(), "rpc.port") {
		t.Fatalf("invalid RPC port error = %v", err)
	}
	config.ConfigFile = writeConfigFile(t, "[rpc]\nport = 'not-a-number'\n[tuning.pprof]\nport = 'also-not-a-number'\n")
	if _, err := resolvedConfig(); err == nil || !strings.Contains(err.Error(), "rpc.port must be an integer") {
		t.Fatalf("wrong RPC port type error = %v", err)
	}
	config.ConfigFile = writeConfigFile(t, "[rpc]\nport = 8899\n[tuning.pprof]\nport = 'not-a-number'\n")
	if _, err := resolvedConfig(); err == nil || !strings.Contains(err.Error(), "tuning.pprof.port must be an integer") {
		t.Fatalf("wrong pprof port type error = %v", err)
	}
}

func TestCLIExplainsClientOwnedStdioAndRemoteCommand(t *testing.T) {
	if !MCPCmd.SilenceErrors || !MCPCmd.SilenceUsage || !serveCmd.SilenceErrors || !serveCmd.SilenceUsage || !configCmd.SilenceErrors || !configCmd.SilenceUsage || !setupCmd.SilenceErrors || !setupCmd.SilenceUsage {
		t.Fatal("MCP runtime/config errors must not print duplicate errors or full usage")
	}
	if MCPCmd.RunE == nil || !serveCmd.Hidden {
		t.Fatal("mithril mcp must own the server entry point and keep the legacy serve alias hidden")
	}
	if err := MCPCmd.Args(&MCPCmd, []string{"unexpected"}); err == nil {
		t.Fatal("mithril mcp accepted a positional argument")
	}
	for _, command := range []*cobra.Command{&MCPCmd, &serveCmd} {
		for _, name := range []string{"profile"} {
			if command.Flags().Lookup(name) == nil {
				t.Errorf("%s is missing --%s", command.CommandPath(), name)
			}
		}
	}
	text := MCPCmd.Long
	normalized := strings.Join(strings.Fields(text), " ")
	for _, want := range []string{"launches the stdio server as a child process", "ssh -T NODE mithril mcp", "SSH remote command", "not a daemon"} {
		if !strings.Contains(normalized, want) {
			t.Errorf("CLI help is missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(strings.ToLower(text), "ssh tunnel") {
		t.Fatalf("CLI help still describes stdio as an SSH tunnel:\n%s", text)
	}
	for _, want := range []string{"stdio has no authentication", "SSH identity is the"} {
		if !strings.Contains(normalized, want) {
			t.Errorf("CLI help is missing trust-boundary text %q:\n%s", want, text)
		}
	}
	for _, want := range []string{"mithril mcp setup codex", "mithril mcp setup claude", "mithril mcp setup vscode", "Any stdio-capable MCP client", "command-and-arguments entry", "mithril mcp config", "File paths must be absolute"} {
		if !strings.Contains(normalized, want) {
			t.Errorf("CLI help is missing client-neutral guidance %q:\n%s", want, text)
		}
	}
}

func TestInteractiveMCPExplainsClientLaunchInsteadOfWaiting(t *testing.T) {
	original := interactiveStdio
	interactiveStdio = func() bool { return true }
	t.Cleanup(func() { interactiveStdio = original })

	var output bytes.Buffer
	cmd := MCPCmd
	cmd.SetOut(&output)
	if err := runServe(&cmd, nil); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, want := range []string{"stdio server", "not an interactive shell", "mithril mcp config"} {
		if !strings.Contains(text, want) {
			t.Errorf("interactive guidance is missing %q: %s", want, text)
		}
	}
}

func TestInteractiveMCPValidatesBeforePrintingHint(t *testing.T) {
	original := interactiveStdio
	interactiveStdio = func() bool { return true }
	t.Cleanup(func() { interactiveStdio = original })
	setConfigFileForTest(t, "")
	clearMCPNodeSettingEnv(t)
	t.Setenv("MITHRIL_MCP_PROFILE", "")

	tests := []struct {
		name    string
		flags   []string
		wantErr string
	}{
		{
			name:    "unknown profile",
			flags:   []string{"--profile", "not-a-profile"},
			wantErr: "unknown MCP profile",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := newServeFlagCmd(t, test.flags...)
			cmd.Use = "mcp"
			var output bytes.Buffer
			cmd.SetOut(&output)

			err := runServe(cmd, nil)

			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, test.wantErr)
			}
			if output.Len() != 0 {
				t.Fatalf("interactive hint masked validation error: %s", output.String())
			}
		})
	}

	config.ConfigFile = filepath.Join(t.TempDir(), "missing.toml")
	cmd := newServeFlagCmd(t)
	cmd.Use = "mcp"
	var output bytes.Buffer
	cmd.SetOut(&output)
	if err := runServe(cmd, nil); err == nil {
		t.Fatal("missing --config path was accepted")
	}
	if output.Len() != 0 {
		t.Fatalf("interactive hint masked config-path error: %s", output.String())
	}

	config.ConfigFile = ""
	t.Setenv("MITHRIL_BLOCK_SOURCE", "unknown")
	cmd = newServeFlagCmd(t)
	cmd.Use = "mcp"
	output.Reset()
	cmd.SetOut(&output)
	if err := runServe(cmd, nil); err == nil || !strings.Contains(err.Error(), "unknown Mithril block source") {
		t.Fatalf("invalid block source error = %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("interactive hint masked block-source error: %s", output.String())
	}
}

func TestConfigCommandPrintsPortableStdioEntry(t *testing.T) {
	originalExecutable := currentExecutable
	t.Cleanup(func() { currentExecutable = originalExecutable })
	currentExecutable = func() (string, error) { return "/opt/mithril", nil }
	setConfigFileForTest(t, "")

	var output bytes.Buffer
	cmd := configCmd
	cmd.SetOut(&output)
	if err := cmd.RunE(&cmd, nil); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(strings.Fields(output.String()), " "), `{ "type": "stdio", "command": "/opt/mithril", "args": [ "mcp" ] }`; got != want {
		t.Fatalf("portable config = %s", output.String())
	}
}

func TestSetupCommandAddsPortableCodexServer(t *testing.T) {
	originalExecutable := currentExecutable
	originalRun := runMCPClient
	t.Cleanup(func() {
		currentExecutable = originalExecutable
		runMCPClient = originalRun
	})
	currentExecutable = func() (string, error) { return "/opt/mithril", nil }
	setConfigFileForTest(t, "")
	setRemoteConfigFlagsForTest(t, "", "", "")

	var gotExecutable string
	var got []string
	runMCPClient = func(_ *cobra.Command, executable string, args []string) error {
		gotExecutable = executable
		got = append([]string(nil), args...)
		return nil
	}
	cmd := setupCmd
	if err := cmd.RunE(&cmd, []string{"codex"}); err != nil {
		t.Fatal(err)
	}
	want := []string{"mcp", "add", "mithril", "--", "/opt/mithril", "mcp"}
	if gotExecutable != "codex" || strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("codex arguments = %#v, want %#v", got, want)
	}
	if err := cmd.RunE(&cmd, []string{"cursor"}); err == nil || !strings.Contains(err.Error(), "supported clients: codex, claude, or vscode") {
		t.Fatalf("unsupported client error = %v", err)
	}
}

func TestSetupCommandAddsPortableClaudeServer(t *testing.T) {
	originalExecutable := currentExecutable
	originalRun := runMCPClient
	t.Cleanup(func() {
		currentExecutable = originalExecutable
		runMCPClient = originalRun
	})
	currentExecutable = func() (string, error) { return "/opt/mithril", nil }
	setConfigFileForTest(t, "")
	setRemoteConfigFlagsForTest(t, "", "", "")

	var gotExecutable string
	var got []string
	runMCPClient = func(_ *cobra.Command, executable string, args []string) error {
		gotExecutable = executable
		got = append([]string(nil), args...)
		return nil
	}
	cmd := setupCmd
	if err := cmd.RunE(&cmd, []string{"claude"}); err != nil {
		t.Fatal(err)
	}
	if gotExecutable != "claude" || len(got) != 6 {
		t.Fatalf("claude arguments = %#v", got)
	}
	wantPrefix := []string{"mcp", "add-json", "--scope", "user", "mithril"}
	if strings.Join(got[:5], "\x00") != strings.Join(wantPrefix, "\x00") {
		t.Fatalf("claude arguments = %#v, want prefix %#v", got, wantPrefix)
	}
	var entry stdioConfigEntry
	if err := json.Unmarshal([]byte(got[5]), &entry); err != nil {
		t.Fatalf("decode Claude config: %v", err)
	}
	if entry.Type != "stdio" || entry.Command != "/opt/mithril" || strings.Join(entry.Args, " ") != "mcp" {
		t.Fatalf("Claude config = %#v", entry)
	}
}

func TestSetupCommandAddsPortableVSCodeServer(t *testing.T) {
	originalExecutable := currentExecutable
	originalRun := runMCPClient
	t.Cleanup(func() {
		currentExecutable = originalExecutable
		runMCPClient = originalRun
	})
	currentExecutable = func() (string, error) { return "/opt/mithril", nil }
	setConfigFileForTest(t, "")
	setRemoteConfigFlagsForTest(t, "", "", "")

	var gotExecutable string
	var got []string
	runMCPClient = func(_ *cobra.Command, executable string, args []string) error {
		gotExecutable = executable
		got = append([]string(nil), args...)
		return nil
	}
	cmd := setupCmd
	if err := cmd.RunE(&cmd, []string{"vscode"}); err != nil {
		t.Fatal(err)
	}
	if gotExecutable != "code" || len(got) != 2 || got[0] != "--add-mcp" {
		t.Fatalf("VS Code arguments = %#v", got)
	}
	var entry struct {
		Name    string   `json:"name"`
		Command string   `json:"command"`
		Args    []string `json:"args"`
	}
	if err := json.Unmarshal([]byte(got[1]), &entry); err != nil {
		t.Fatalf("decode VS Code config: %v", err)
	}
	if entry.Name != "mithril" || entry.Command != "/opt/mithril" || strings.Join(entry.Args, " ") != "mcp" {
		t.Fatalf("VS Code config = %#v", entry)
	}
}

func TestConfigCommandPreservesExplicitNodeConfig(t *testing.T) {
	originalExecutable := currentExecutable
	t.Cleanup(func() { currentExecutable = originalExecutable })
	currentExecutable = func() (string, error) { return "/opt/mithril", nil }
	setConfigFileForTest(t, filepath.Join("relative", "node.toml"))
	wantConfigPath, err := filepath.Abs(config.ConfigFile)
	if err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	cmd := configCmd
	cmd.SetOut(&output)
	if err := cmd.RunE(&cmd, nil); err != nil {
		t.Fatal(err)
	}
	var entry stdioConfigEntry
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(entry.Args, " "), "mcp --config "+wantConfigPath; got != want {
		t.Fatalf("portable config args = %q, want %q", got, want)
	}
}

func TestConfigCommandPrintsRemoteStdioEntry(t *testing.T) {
	originalExecutable := currentExecutable
	t.Cleanup(func() { currentExecutable = originalExecutable })
	currentExecutable = func() (string, error) {
		t.Fatal("remote config must not inspect the local executable")
		return "", nil
	}
	setConfigFileForTest(t, "")
	setRemoteConfigFlagsForTest(t, "operator@example-node", "/opt/Mithril's bin/mithril", "/etc/mithril/node config.toml")

	var output bytes.Buffer
	cmd := configCmd
	cmd.SetOut(&output)
	if err := cmd.RunE(&cmd, nil); err != nil {
		t.Fatal(err)
	}
	var entry stdioConfigEntry
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	if entry.Command != "ssh" {
		t.Fatalf("remote command = %q, want ssh", entry.Command)
	}
	if entry.Type != "stdio" {
		t.Fatalf("remote transport = %q, want stdio", entry.Type)
	}
	wantArgs := []string{
		"-T",
		"-a",
		"-x",
		"-o",
		"IgnoreUnknown=ForkAfterAuthentication,SessionType,StdinNull,RemoteCommand",
		"-o",
		"BatchMode=yes",
		"-o",
		"ConnectTimeout=10",
		"-o",
		"ServerAliveInterval=15",
		"-o",
		"ServerAliveCountMax=2",
		"-o",
		"ClearAllForwardings=yes",
		"-o",
		"Tunnel=no",
		"-o",
		"PermitLocalCommand=no",
		"-o",
		"StdinNull=no",
		"-o",
		"ForkAfterAuthentication=no",
		"-o",
		"SessionType=default",
		"-o",
		"RemoteCommand=none",
		"-o",
		"ControlPath=none",
		"--",
		"operator@example-node",
		`exec '/opt/Mithril'"'"'s bin/mithril' 'mcp' '--config' '/etc/mithril/node config.toml'`,
	}
	if strings.Join(entry.Args, "\x00") != strings.Join(wantArgs, "\x00") {
		t.Fatalf("remote args = %#v, want %#v", entry.Args, wantArgs)
	}
}

func TestRemoteStdioConfigCanPinDiagnosticProfile(t *testing.T) {
	entry, err := remoteStdioConfig("node-alias", "/usr/local/bin/mithril", "diagnostic", "")
	if err != nil {
		t.Fatal(err)
	}
	wantCommand := `exec '/usr/local/bin/mithril' 'mcp' '--profile' 'diagnostic'`
	if got := entry.Args[len(entry.Args)-1]; got != wantCommand {
		t.Fatalf("remote command = %q, want %q", got, wantCommand)
	}
	if entry.Type != "stdio" {
		t.Fatalf("remote transport = %q, want stdio", entry.Type)
	}
	if _, err := remoteStdioConfig("node-alias", "/usr/local/bin/mithril", "unsafe", ""); err == nil {
		t.Fatal("invalid remote config profile was accepted")
	}
}

func TestRemoteStdioConfigRejectsUnsafeInputs(t *testing.T) {
	tests := []struct {
		name   string
		target string
		binary string
		config string
	}{
		{name: "missing target", binary: "/opt/mithril"},
		{name: "target option", target: "-oProxyCommand=bad", binary: "/opt/mithril"},
		{name: "target whitespace", target: "node alias", binary: "/opt/mithril"},
		{name: "target shell syntax", target: "node;command", binary: "/opt/mithril"},
		{name: "target missing user", target: "@node", binary: "/opt/mithril"},
		{name: "target missing host", target: "operator@", binary: "/opt/mithril"},
		{name: "target host option", target: "operator@-node", binary: "/opt/mithril"},
		{name: "target host without name", target: "operator@...", binary: "/opt/mithril"},
		{name: "target multiple users", target: "one@two@node", binary: "/opt/mithril"},
		{name: "target mismatched bracket", target: "operator@[::1", binary: "/opt/mithril"},
		{name: "missing binary", target: "node"},
		{name: "relative binary", target: "node", binary: "bin/mithril"},
		{name: "unclean binary", target: "node", binary: "/opt/../bin/mithril"},
		{name: "binary control character", target: "node", binary: "/opt/mithril\nnext"},
		{name: "relative config", target: "node", binary: "/opt/mithril", config: "config.toml"},
		{name: "unclean config", target: "node", binary: "/opt/mithril", config: "/etc/./config.toml"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := remoteStdioConfig(test.target, test.binary, "", test.config); err == nil {
				t.Fatal("unsafe remote config input was accepted")
			}
		})
	}
}

func TestPOSIXShellQuoteKeepsRemoteArgumentsLiteral(t *testing.T) {
	tests := map[string]string{
		"":                       `''`,
		"/opt/mithril":           `'/opt/mithril'`,
		"/opt/a b/$HOME;command": `'/opt/a b/$HOME;command'`,
		"/opt/Mithril's/bin":     `'/opt/Mithril'"'"'s/bin'`,
	}
	for input, want := range tests {
		if got := posixShellQuote(input); got != want {
			t.Errorf("posixShellQuote(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestConfigCommandKeepsLocalAndRemoteConfigSeparate(t *testing.T) {
	setConfigFileForTest(t, "/local/node.toml")
	setRemoteConfigFlagsForTest(t, "node-alias", "/opt/mithril", "/remote/node.toml")
	cmd := configCmd
	if err := cmd.RunE(&cmd, nil); err == nil || !strings.Contains(err.Error(), "local --config") {
		t.Fatalf("local and remote config error = %v", err)
	}
}

func TestConfigCommandRejectsRemoteFlagsWithoutSSH(t *testing.T) {
	setConfigFileForTest(t, "")
	setRemoteConfigFlagsForTest(t, "", "/opt/mithril", "")
	cmd := configCmd
	if err := cmd.RunE(&cmd, nil); err == nil || !strings.Contains(err.Error(), "require --ssh") {
		t.Fatalf("remote flag without SSH error = %v", err)
	}
}

func TestPortableStdioConfigCanPinDiagnosticProfile(t *testing.T) {
	entry, err := portableStdioConfig("/opt/mithril", "diagnostic", "")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(entry.Args, " "), "mcp --profile diagnostic"; got != want {
		t.Fatalf("portable config args = %q, want %q", got, want)
	}
	if entry.Type != "stdio" {
		t.Fatalf("portable transport = %q, want stdio", entry.Type)
	}
	if _, err := portableStdioConfig("/opt/mithril", "unsafe", ""); err == nil {
		t.Fatal("invalid portable config profile was accepted")
	}
}

func TestResolvedConfigProfileOverride(t *testing.T) {
	setConfigFileForTest(t, "")
	t.Setenv("MITHRIL_MCP_PROFILE", "diagnostic")

	cfg, err := resolvedConfigWithOverrides(resolvedConfigOverrides{})
	if err != nil || cfg.Profile != mcp.ProfileDiagnostic {
		t.Fatalf("environment profile = %q, %v; want diagnostic", cfg.Profile, err)
	}
	monitor := mcp.ProfileMonitor
	cfg, err = resolvedConfigWithOverrides(resolvedConfigOverrides{Profile: &monitor})
	if err != nil || cfg.Profile != mcp.ProfileMonitor {
		t.Fatalf("explicit profile = %q, %v; want monitor", cfg.Profile, err)
	}
	t.Setenv("MITHRIL_MCP_PROFILE", "unsafe")
	if _, err := resolvedConfigWithOverrides(resolvedConfigOverrides{}); err == nil {
		t.Fatal("invalid environment profile was silently downgraded")
	}
	cfg, err = resolvedConfigWithOverrides(resolvedConfigOverrides{Profile: &monitor})
	if err != nil || cfg.Profile != mcp.ProfileMonitor {
		t.Fatalf("explicit profile did not override invalid environment profile: %q, %v", cfg.Profile, err)
	}
}

func TestDurableExecutablePathRejectsGoRun(t *testing.T) {
	if _, err := durableExecutablePath("/private/tmp/go-build123/b001/exe/mithril"); err == nil {
		t.Fatal("ephemeral go run executable was accepted")
	}
	cache := t.TempDir()
	t.Setenv("GOCACHE", cache)
	if _, err := durableExecutablePath(filepath.Join(cache, "00", "cache-hash", "mithril")); err == nil {
		t.Fatal("executable inside the configured Go cache was accepted")
	}
	got, err := durableExecutablePath("/opt/mithril")
	if err != nil || got != "/opt/mithril" {
		t.Fatalf("durableExecutablePath = %q, %v", got, err)
	}
}

// newServeFlagCmd builds a command carrying the serve flag set and applies the
// given flags, mirroring how cobra reports Changed() at runtime.
func newServeFlagCmd(t *testing.T, args ...string) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "serve", RunE: func(*cobra.Command, []string) error { return nil }}
	bindServeFlags(cmd)
	cmd.SetArgs(args)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("parse flags %v: %v", args, err)
	}
	return cmd
}

func TestResolvedServeConfigValidatesProfile(t *testing.T) {
	cmd := newServeFlagCmd(t, "--profile", "not-a-profile")
	if _, err := resolvedServeConfig(cmd); err == nil {
		t.Error("an unknown profile name was accepted")
	}
}

func TestConfiguredIntegerRejectsNonIntegerValues(t *testing.T) {
	accepted := map[string]any{
		"int":      int(42),
		"int8":     int8(8),
		"int16":    int16(16),
		"int32":    int32(32),
		"int64":    int64(64),
		"uint":     uint(42),
		"uint8":    uint8(8),
		"uint16":   uint16(16),
		"uint32":   uint32(32),
		"uint64":   uint64(64),
		"zero":     int(0),
		"negative": int(-1),
		"maxint64": int64(math.MaxInt64),
	}
	for name, value := range accepted {
		v := viper.New()
		v.Set("k", value)
		got, err := configuredInteger(v, "k")
		if err != nil {
			t.Errorf("%s: configuredInteger rejected %v (%T): %v", name, value, value, err)
			continue
		}
		if want := reflect.ValueOf(value); want.CanInt() && got != want.Int() {
			t.Errorf("%s: got %d, want %d", name, got, want.Int())
		}
	}

	rejected := map[string]any{
		"string":       "42",
		"empty string": "",
		"float":        4.2,
		"whole float":  float64(42), // still not an integer type
		"bool":         true,
		"slice":        []int{1},
		"map":          map[string]int{"a": 1},
		"nil":          nil,
		// Above MaxInt64: converting would wrap to a negative limit.
		"uint64 overflow": uint64(math.MaxInt64) + 1,
	}
	for name, value := range rejected {
		v := viper.New()
		v.Set("k", value)
		if got, err := configuredInteger(v, "k"); err == nil {
			t.Errorf("%s: configuredInteger accepted %v (%T), returning %d", name, value, value, got)
		}
	}

	// An absent key is not an integer either.
	if _, err := configuredInteger(viper.New(), "missing"); err == nil {
		t.Error("an unset key was accepted as an integer")
	}
}

// Remote configuration must remain fixed argv, never a shell command.
func FuzzRemoteStdioConfigNeverInjects(f *testing.F) {
	seeds := []struct{ target, binary, profile, remoteConfig string }{
		{"mithril-mcp-target", "/usr/local/bin/mithril", "monitor", ""},
		{"user@host", "/usr/local/bin/mithril", "diagnostic", "/etc/mithril/config.toml"},
		{"host; rm -rf /", "/usr/local/bin/mithril", "monitor", ""},
		{"host$(whoami)", "/usr/local/bin/mithril", "monitor", ""},
		{"host`id`", "/bin/mithril", "monitor", ""},
		{"-oProxyCommand=evil", "/bin/mithril", "monitor", ""},
		{"host\nProxyCommand evil", "/bin/mithril", "monitor", ""},
		{"[fd00::1]", "/bin/mithril", "monitor", ""},
		{"host", "relative/path", "monitor", ""},
		{"host", "/bin/mithril; evil", "monitor", ""},
		{"host", "/bin/mithril", "monitor", "relative.toml"},
		{"", "", "", ""},
	}
	for _, s := range seeds {
		f.Add(s.target, s.binary, s.profile, s.remoteConfig)
	}

	f.Fuzz(func(t *testing.T, target, binary, profile, remoteConfig string) {
		entry, err := remoteStdioConfig(target, binary, profile, remoteConfig)
		if err != nil {
			return // rejection is always acceptable
		}

		// Accepted: the shape must be a fixed argv, not a shell string.
		if entry.Command != "ssh" {
			t.Fatalf("accepted entry runs %q, not ssh", entry.Command)
		}
		if len(entry.Args) == 0 {
			t.Fatal("accepted entry has no arguments")
		}

		// The target must appear as exactly one whole argument. If it were
		// split or concatenated, caller input would have become structure.
		found := 0
		for _, a := range entry.Args {
			if a == target {
				found++
			}
		}
		if found != 1 {
			t.Fatalf("target %q appears %d times as a distinct argument in %q", target, found, entry.Args)
		}

		for _, a := range entry.Args {
			// A newline in any argument can split a forced command or an
			// authorized_keys line on the remote side.
			if strings.ContainsAny(a, "\n\r\x00") {
				t.Fatalf("argument %q carries a newline or NUL: %q", a, entry.Args)
			}
		}

		// The remote binary must be absolute, or the remote PATH decides what
		// actually runs.
		if !strings.HasPrefix(binary, "/") {
			t.Fatalf("accepted a non-absolute remote binary %q", binary)
		}
	})
}
