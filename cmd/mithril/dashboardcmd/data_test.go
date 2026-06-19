package dashboardcmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/procctl"
	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/charmbracelet/lipgloss"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadConfig_DefaultsRuntimeOwnedFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[network]
cluster = "mainnet-beta"
rpc = ["https://example.invalid"]
`), 0600))

	cfg := readConfig(path)
	require.NotNil(t, cfg)
	assert.Equal(t, "/mnt/mithril-logs", cfg.logsPath)
	assert.Equal(t, "./lightbringer", cfg.lbBinaryPath)
	assert.Equal(t, ".", cfg.lbConfigDir)
}

func TestReadStateHandlesLargeMainnetStateFile(t *testing.T) {
	dir := t.TempDir()
	body := fmt.Sprintf(`{
  "last_slot": 425544687,
  "last_epoch": 985,
  "stage": "ready",
  "last_shutdown_reason": "graceful shutdown (Ctrl+C)",
  "manifest_accts_lt_hash": %q
}`, strings.Repeat("a", 2<<20))
	require.Greater(t, len(body), 1<<20, "test must exceed the historical 1 MiB reader cap")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mithril_state.json"), []byte(body), 0600))

	st := readState(dir)
	require.NotNil(t, st)
	assert.Equal(t, uint64(425544687), st.LastSlot)
	assert.Equal(t, uint64(985), st.LastEpoch)
	assert.Equal(t, "ready", st.Stage)
	assert.Equal(t, "graceful shutdown (Ctrl+C)", st.LastShutdownReason)
}

func TestMithrilLogLinesFallsBackToDashboardSpawnOutput(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "mithril.pid")
	t.Setenv("MITHRIL_PID_FILE", pidPath)

	stdoutF, err := os.CreateTemp("", "mithril-dashboard-spawn-stdout-*.log")
	require.NoError(t, err)
	defer os.Remove(stdoutF.Name())
	defer stdoutF.Close()
	_, err = stdoutF.WriteString("  [1] Snapshot Download + Extract AppendVecs → [2] Flush Index\n│ Search Time:      53s\n  1)\nstdout progress should stay hidden\n")
	require.NoError(t, err)

	stderrF, err := os.CreateTemp("", "mithril-dashboard-spawn-stderr-*.log")
	require.NoError(t, err)
	defer os.Remove(stderrF.Name())
	defer stderrF.Close()
	_, err = stderrF.WriteString("Snapshot Read (.tar.zst) 17.0%\nExtract (AppendVecs) 60.0 GB\n")
	require.NoError(t, err)

	writeCurrentDashboardPidFile(t, pidPath, func(info *procctl.PidInfo) {
		info.StdoutPath = stdoutF.Name()
		info.StderrPath = stderrF.Name()
	})

	lines := mithrilLogLines(filepath.Join(t.TempDir(), "missing-logs"), 20, dashboardSpawnLogs{})
	joined := strings.Join(lines, "\n")
	assert.Contains(t, joined, "Snapshot Read")
	assert.Contains(t, joined, "Extract (AppendVecs)")
	assert.NotContains(t, joined, "stdout progress should stay hidden")
	assert.NotContains(t, joined, "Flush Index")
	assert.NotContains(t, joined, "Search Time")
	assert.NotContains(t, joined, "  1)")
}

func TestMithrilLogLinesUsesRememberedSpawnOutputAfterPidFileGone(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "mithril.pid")
	t.Setenv("MITHRIL_PID_FILE", pidPath)

	stderrF, err := os.CreateTemp("", "mithril-dashboard-spawn-stderr-*.log")
	require.NoError(t, err)
	defer os.Remove(stderrF.Name())
	defer stderrF.Close()
	_, err = stderrF.WriteString("Snapshot Read (.tar.zst) 8.7%\n")
	require.NoError(t, err)

	lines := mithrilLogLines(filepath.Join(t.TempDir(), "missing-logs"), 20, dashboardSpawnLogs{
		stderrPath: stderrF.Name(),
	})
	joined := strings.Join(lines, "\n")
	assert.Contains(t, joined, "Snapshot Read")
}

func TestMithrilLogLinesMergesActiveDashboardSpawnOutput(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "mithril.pid")
	t.Setenv("MITHRIL_PID_FILE", pidPath)

	logsDir := t.TempDir()
	runDir := filepath.Join(logsDir, "run-1")
	require.NoError(t, os.MkdirAll(runDir, 0700))
	require.NoError(t, os.Symlink("run-1", filepath.Join(logsDir, "latest")))
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "mithril.log"), []byte("mode=auto: No existing AccountsDB\nWill save full snapshot\n"), 0600))

	stderrF, err := os.CreateTemp("", "mithril-dashboard-spawn-stderr-*.log")
	require.NoError(t, err)
	defer os.Remove(stderrF.Name())
	defer stderrF.Close()
	_, err = stderrF.WriteString("Snapshot Read (.tar.zst) 21.0%\nExtract (AppendVecs) 74.0/352.8 GB\n")
	require.NoError(t, err)

	writeCurrentDashboardPidFile(t, pidPath, func(info *procctl.PidInfo) {
		info.StderrPath = stderrF.Name()
	})

	lines := mithrilLogLines(logsDir, 20, dashboardSpawnLogs{})
	joined := strings.Join(lines, "\n")
	assert.Contains(t, joined, "No existing AccountsDB")
	assert.Contains(t, joined, "Snapshot Read")
	assert.Contains(t, joined, "Extract (AppendVecs)")
}

func TestMithrilLogLinesDeduplicatesSpawnOutputAlreadyInLog(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "mithril.pid")
	t.Setenv("MITHRIL_PID_FILE", pidPath)

	logsDir := t.TempDir()
	runDir := filepath.Join(logsDir, "run-1")
	duplicate := "(+    3s) mode=auto: Resuming from existing AccountsDB at slot 425546069"
	require.NoError(t, os.MkdirAll(runDir, 0700))
	require.NoError(t, os.Symlink("run-1", filepath.Join(logsDir, "latest")))
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "mithril.log"), []byte(duplicate+"\n"), 0600))

	stderrF, err := os.CreateTemp("", "mithril-dashboard-spawn-stderr-*.log")
	require.NoError(t, err)
	defer os.Remove(stderrF.Name())
	defer stderrF.Close()
	_, err = stderrF.WriteString(duplicate + "\n")
	require.NoError(t, err)

	writeCurrentDashboardPidFile(t, pidPath, func(info *procctl.PidInfo) {
		info.StderrPath = stderrF.Name()
	})

	lines := mithrilLogLines(logsDir, 20, dashboardSpawnLogs{})
	assert.Equal(t, 1, strings.Count(strings.Join(lines, "\n"), duplicate))
}

func TestMithrilLogLinesPrefersActivePidLogDirOverStaleLatest(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "mithril.pid")
	t.Setenv("MITHRIL_PID_FILE", pidPath)

	logsDir := t.TempDir()
	staleDir := filepath.Join(logsDir, "stale-run")
	activeDir := filepath.Join(logsDir, "active-run")
	require.NoError(t, os.MkdirAll(staleDir, 0700))
	require.NoError(t, os.MkdirAll(activeDir, 0700))
	require.NoError(t, os.Symlink("stale-run", filepath.Join(logsDir, "latest")))
	require.NoError(t, os.WriteFile(filepath.Join(activeDir, "mithril.log"), []byte("current run line\n"), 0600))
	writeCurrentDashboardPidFile(t, pidPath, func(info *procctl.PidInfo) {
		info.LogDir = activeDir
	})

	lines := mithrilLogLines(logsDir, 20, dashboardSpawnLogs{})

	assert.Contains(t, strings.Join(lines, "\n"), "current run line")
}

func TestMithrilLogLinesUsesCleanMessageWhenLatestLogMissing(t *testing.T) {
	t.Setenv("MITHRIL_PID_FILE", filepath.Join(t.TempDir(), "missing.pid"))

	logsDir := t.TempDir()
	runDir := filepath.Join(logsDir, "run-without-main-log")
	require.NoError(t, os.MkdirAll(runDir, 0700))
	require.NoError(t, os.Symlink("run-without-main-log", filepath.Join(logsDir, "latest")))

	lines := mithrilLogLines(logsDir, 20, dashboardSpawnLogs{})
	joined := strings.Join(lines, "\n")

	assert.Contains(t, joined, "mithril.log is not available yet")
	assert.NotContains(t, joined, runDir)
}

func TestMithrilLogLinesIgnoresStaleDashboardPidFile(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "mithril.pid")
	t.Setenv("MITHRIL_PID_FILE", pidPath)

	stderrF, err := os.CreateTemp("", "mithril-dashboard-spawn-stderr-*.log")
	require.NoError(t, err)
	defer os.Remove(stderrF.Name())
	defer stderrF.Close()
	_, err = stderrF.WriteString("stale crash output\n")
	require.NoError(t, err)

	logsDir := t.TempDir()
	runDir := filepath.Join(logsDir, "run-without-main-log")
	require.NoError(t, os.MkdirAll(runDir, 0700))
	require.NoError(t, os.Symlink("run-without-main-log", filepath.Join(logsDir, "latest")))
	require.NoError(t, procctl.WritePidFile(pidPath, &procctl.PidInfo{
		Pid:        99999999,
		SpawnedBy:  "dashboard",
		StderrPath: stderrF.Name(),
	}))

	lines := mithrilLogLines(logsDir, 20, dashboardSpawnLogs{})
	joined := strings.Join(lines, "\n")

	assert.Contains(t, joined, "mithril.log is not available yet")
	assert.NotContains(t, joined, "stale crash output")
	assert.NotContains(t, joined, "Mithril is starting")
}

func writeCurrentDashboardPidFile(t *testing.T, pidPath string, mutate func(*procctl.PidInfo)) {
	t.Helper()
	id, err := procctl.ReadIdentity(os.Getpid())
	require.NoError(t, err)
	exe, err := os.Executable()
	require.NoError(t, err)
	info := &procctl.PidInfo{
		Pid:            os.Getpid(),
		StartTimeTicks: id.StartTimeTicks,
		ExeInode:       id.ExeInode,
		BinaryPath:     exe,
		RunID:          "test-run",
		SpawnedBy:      "dashboard",
	}
	if mutate != nil {
		mutate(info)
	}
	require.NoError(t, procctl.WritePidFile(pidPath, info))
}

func TestReadPathLogTailNormalizesTerminalControlCharacters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mithril.log")
	require.NoError(t, os.WriteFile(path, []byte("first\rsecond\x1b[31m red\x1b[0m\nthird\x07\n"), 0600))

	lines := readPathLogTail(path, 10)

	assert.Equal(t, []string{"first", "second red", "third"}, trimTrailingEmptyLogLines(lines))
}

func TestCompactVolatileLogLinesKeepsLatestProgressRows(t *testing.T) {
	lines := []string{
		"startup",
		"Snapshot Read (.tar.zst) 0.1% 0.1/45.4 GB ETA 7m30s",
		"Extract (AppendVecs) 0.2% 0.5/340.0 GB ETA 7m10s",
		"Snapshot Read (.tar.zst) 0.8% 0.4/45.4 GB ETA 7m29s",
		"Extract (AppendVecs) 1.0% 3.5/339.7 GB ETA 7m05s",
		"ready",
	}

	compacted := compactVolatileLogLines(lines, 20)

	assert.Equal(t, []string{
		"startup",
		"Snapshot Read (.tar.zst) 0.8% 0.4/45.4 GB ETA 7m29s",
		"Extract (AppendVecs) 1.0% 3.5/339.7 GB ETA 7m05s",
		"ready",
	}, compacted)
}

func TestCompactVolatileLogLinesShortensProgressBars(t *testing.T) {
	lines := []string{
		"Snapshot Read (.tar.zst) [██░░░░░░░░░░░░░░░░░░░░]   3.5%   3.8/108.1 GB 110.6 MB/s  ETA 16m06s",
		"Extract (AppendVecs)     [█░░░░░░░░░░░░░░░░░░░░░]   2.7%  10.4/385.4 GB 377.8 MB/s  ETA 16m56s",
		"Flush (shard logs)       [███████████████████████░░░░░░░░░░░░░░░░░]  58.6%  150/256  shards  ETA 1m21s",
	}

	compacted := compactVolatileLogLines(lines, 20)

	assert.Equal(t, []string{
		"Snapshot: 3.5% 3.8/108.1 GB 110.6 MB/s ETA 16m06s",
		"Extract: 2.7% 10.4/385.4 GB 377.8 MB/s ETA 16m56s",
		"Flush: 58.6% 150/256 shards ETA 1m21s",
	}, compacted)
}

func TestCleanDashboardLogLinesRemovesTerminalProgressFragments(t *testing.T) {
	lines := []string{
		"  [1] Snapshot Download + Extract AppendVecs → [2] Flush Index",
		"  x",
		"  1)",
		"│ Search Time:      53s",
		"  │",
		"(+ 0s) Cleaning up previous AccountsDB artifacts in /very/long/accounts/path",
		"(+ 0s) Cleaning up existing snapshot files in /very/long/snapshot/path (keeping 1)",
		"(+ 1m03s) Will save full snapshot to /very/long/path/snapshot.tar.zst while streaming",
		"(+ 1m30s) Cleaning up partial download: /very/long/path/snapshot.tar.zst.partial",
		"(+20m46s) Snapshot unpack stopped during shutdown: context canceled",
		"(+20m49s) snapshot bootstrap cancelled during shutdown: failed to build AccountsDB from snapshot: processing full snapshot: context canceled",
		"Snapshot Read (.tar.zst) [██░░] 3.5%",
	}

	cleaned := cleanDashboardLogLines(lines)

	assert.Equal(t, []string{
		"(+ 0s) Cleaning up previous AccountsDB artifacts",
		"(+ 0s) Cleaning up existing snapshot files",
		"(+ 1m03s) Saving full snapshot while streaming",
		"(+ 1m30s) Cleaning up partial snapshot download",
		"(+20m46s) Snapshot unpack canceled during shutdown",
		"(+20m49s) Snapshot bootstrap canceled during shutdown",
		"Snapshot Read (.tar.zst) [██░░] 3.5%",
	}, cleaned)
}

func TestMithrilLogLinesCompactsStartupCleanupPathTails(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "mithril.pid")
	t.Setenv("MITHRIL_PID_FILE", pidPath)

	stderrF, err := os.CreateTemp("", "mithril-dashboard-spawn-stderr-*.log")
	require.NoError(t, err)
	defer os.Remove(stderrF.Name())
	defer stderrF.Close()
	_, err = stderrF.WriteString(strings.Join([]string{
		"(+    0s) Cleaning up previous AccountsDB artifacts in /home/ubuntu/mithril-data/mainnet-live/accounts",
		"(+    0s) Cleaning up existing snapshot files in /home/ubuntu/mithril-data/mainnet-live/snapshots (keeping 1)",
		"(+    0s) Probing 318 nodes for snapshot availability...",
	}, "\n"))
	require.NoError(t, err)

	lines := mithrilLogLines(filepath.Join(t.TempDir(), "missing-logs"), 20, dashboardSpawnLogs{
		stderrPath: stderrF.Name(),
	})
	joined := strings.Join(wrapLogLines(lines, 58), "\n")

	assert.Contains(t, joined, "Cleaning up previous AccountsDB artifacts")
	assert.Contains(t, joined, "Cleaning up existing snapshot files")
	assert.NotContains(t, joined, "mainnet-live")
	assert.NotContains(t, joined, "  1)")
}

func TestReadDashboardSpawnLogTailRejectsSymlink(t *testing.T) {
	target, err := os.CreateTemp("", "mithril-dashboard-target-*.log")
	require.NoError(t, err)
	defer os.Remove(target.Name())
	defer target.Close()
	_, err = target.WriteString("secret\n")
	require.NoError(t, err)

	link := filepath.Join(os.TempDir(), "mithril-dashboard-spawn-stderr-symlink-test.log")
	_ = os.Remove(link)
	if err := os.Symlink(target.Name(), link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	defer os.Remove(link)

	assert.Empty(t, readDashboardSpawnLogTail(link, 10))
}

func TestReadConfig_FallsBackToLegacyRPCList(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[network]
cluster = "mainnet-beta"

[rpc]
rpc = ["https://legacy-rpc.example.invalid"]
`), 0600))

	cfg := readConfig(path)
	require.NotNil(t, cfg)
	assert.Equal(t, []string{"https://legacy-rpc.example.invalid"}, cfg.rpcEndpoints)
}

func TestReadConfig_FallsBackToRuntimeStoragePaths(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[network]
cluster = "mainnet-beta"
rpc = ["https://example.invalid"]

[ledger]
accounts_path = "/legacy/accounts"
path = "/legacy/shredstore"

[snapshot]
download_path = "/legacy/snapshots"

[log]
dir = "/legacy/logs"
`), 0600))

	cfg := readConfig(path)
	require.NotNil(t, cfg)
	assert.Equal(t, "/legacy/accounts", cfg.accountsPath)
	assert.Equal(t, "/legacy/snapshots", cfg.snapshotsPath)
	assert.Equal(t, "/legacy/shredstore", cfg.shredstorePath)
	assert.Equal(t, "/legacy/logs", cfg.logsPath)
}

func TestSaveConfigValue_PreservesLegacyRPCFailovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[network]
cluster = "mainnet-beta"

[rpc]
rpc = ["https://old-primary.example.invalid", "https://backup.example.invalid"]
`), 0600))

	require.NoError(t, saveConfigValue(path, "network", "rpc", "https://new-primary.example.invalid"))

	cfg := readConfig(path)
	require.NotNil(t, cfg)
	assert.Equal(t, []string{
		"https://new-primary.example.invalid",
		"https://backup.example.invalid",
	}, cfg.rpcEndpoints)
}

func TestSaveConfigValue_UpdatesSectionWithInlineComment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[lightbringer] # managed sidecar
enabled = false
`), 0600))

	require.NoError(t, saveConfigValue(path, "lightbringer", "enabled", "true"))

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	body := string(content)
	assert.Contains(t, body, `[lightbringer] # managed sidecar`)
	assert.Contains(t, body, `enabled = true`)
	assert.Equal(t, 1, strings.Count(body, "[lightbringer]"))
}

func TestRenderConfigView_UsesInlineCommentSectionName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[lightbringer] # managed sidecar
enabled = false
`), 0600))

	m := newModel(path)
	m.width = 100
	m.height = 40

	out := m.renderConfigView()
	assert.Contains(t, out, "lightbringer")
	assert.NotContains(t, out, "lightbringer] # managed sidecar")
}

func TestReadConfig_ExplicitEmptyLogsDisablesDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[network]
cluster = "mainnet-beta"
rpc = ["https://example.invalid"]

[storage]
logs = ""
`), 0600))

	cfg := readConfig(path)
	require.NotNil(t, cfg)
	assert.Equal(t, "", cfg.logsPath)
}

func TestLightbringerLogLines_DisabledStaysSilent(t *testing.T) {
	lines := lightbringerLogLines(&configData{blockSource: "rpc"}, 50)

	assert.Empty(t, lines)
}

func TestLightbringerLogLines_ExternalExplainsNoManagedLog(t *testing.T) {
	lines := lightbringerLogLines(&configData{
		blockSource:        "lightbringer",
		lbExternalEndpoint: "127.0.0.1:3001",
	}, 50)

	require.Len(t, lines, 2)
	assert.Contains(t, lines[0], "127.0.0.1:3001")
	assert.Contains(t, lines[1], "no local Lightbringer log")
}

func TestLightbringerLogLines_ManagedReadsTail(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run-1")
	require.NoError(t, os.Mkdir(runDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "lightbringer.log"), []byte("ready\nserving\n"), 0600))
	require.NoError(t, os.Symlink("run-1", filepath.Join(dir, "latest")))

	lines := lightbringerLogLines(&configData{logsPath: dir, lbEnabled: true}, 50)

	assert.Contains(t, lines, "ready")
	assert.Contains(t, lines, "serving")
}

func TestLightbringerLogLines_QuietModeKeepsHelpfulHint(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run-1")
	require.NoError(t, os.Mkdir(runDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "lightbringer.log"), []byte("line 1\nline 2\nline 3\n"), 0600))
	require.NoError(t, os.Symlink("run-1", filepath.Join(dir, "latest")))

	lines := lightbringerLogLines(&configData{logsPath: dir, lbEnabled: true, lbQuiet: true}, 3)

	require.Len(t, lines, 3)
	assert.Contains(t, lines[0], "quiet mode")
	assert.NotContains(t, lines[0], "line 1")
	assert.Contains(t, lines[1], "line 2")
	assert.Contains(t, lines[2], "line 3")
}

func TestReadProgressEventsReadsLatestRun(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run-1")
	require.NoError(t, os.Mkdir(runDir, 0700))
	body := strings.Join([]string{
		`{"phase":"starting","status":"running","message":"Mithril process started","ts":"2026-06-01T00:00:00Z"}`,
		`not-json`,
		`{"phase":"bootstrap_snapshot","status":"running","message":"Downloading snapshot","slot":123,"endpoint":"https://rpc.invalid/?api-key=test-key-00000000"}`,
	}, "\n")
	require.NoError(t, os.WriteFile(filepath.Join(runDir, progress.JSONLFileName), []byte(body), 0600))
	require.NoError(t, os.Symlink("run-1", filepath.Join(dir, "latest")))

	events := readProgressEvents(dir, 10)

	require.Len(t, events, 2)
	assert.Equal(t, "starting", events[0].Phase)
	assert.Equal(t, "bootstrap_snapshot", events[1].Phase)
	assert.Equal(t, float64(123), events[1].Fields["slot"])
	assert.Contains(t, events[1].Fields["endpoint"], "api-key=REDACTED")
	assert.NotContains(t, events[1].Fields["endpoint"], "test-key-00000000")
}

func TestReadSnapshotActivity_ChoosesNewestSnapshotArtifact(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "snapshot-100-old.tar.zst")
	newPath := filepath.Join(dir, "snapshot-200-new.tar.zst.partial")
	require.NoError(t, os.WriteFile(oldPath, []byte("old"), 0600))
	require.NoError(t, os.WriteFile(newPath, []byte("newer-data"), 0600))
	require.NoError(t, os.Chtimes(oldPath, time.Now().Add(-time.Hour), time.Now().Add(-time.Hour)))

	activity := readSnapshotActivity(dir)

	assert.Equal(t, newPath, activity.Path)
	assert.Equal(t, "snapshot-200-new.tar.zst.partial", activity.Name)
	assert.True(t, activity.Partial)
	assert.Equal(t, int64(len("newer-data")), activity.Bytes)
}

func TestReadSnapshotActivity_AcceptsExplicitSnapshotFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot-300-test.tar.zst")
	require.NoError(t, os.WriteFile(path, []byte("snapshot-data"), 0600))

	activity := readSnapshotActivity(path)

	assert.Equal(t, path, activity.Path)
	assert.Equal(t, "snapshot-300-test.tar.zst", activity.Name)
	assert.False(t, activity.Partial)
	assert.Equal(t, int64(len("snapshot-data")), activity.Bytes)
}

func TestReadAccountsActivity_ChoosesNewestKnownArtifact(t *testing.T) {
	dir := t.TempDir()
	accountsDir := filepath.Join(dir, "accounts")
	shardsDir := filepath.Join(dir, "mithril_db_log_shards")
	require.NoError(t, os.Mkdir(accountsDir, 0700))
	require.NoError(t, os.Mkdir(shardsDir, 0700))
	oldTime := time.Now().Add(-2 * time.Hour)
	newTime := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(dir, oldTime, oldTime))
	require.NoError(t, os.Chtimes(accountsDir, oldTime, oldTime))
	require.NoError(t, os.Chtimes(shardsDir, newTime, newTime))

	activity := readAccountsActivity(dir)

	assert.Equal(t, shardsDir, activity.Path)
	assert.Equal(t, "mithril_db_log_shards", activity.Name)
}

func TestExistingDiskProbePath_UsesNearestExistingParent(t *testing.T) {
	dir := t.TempDir()
	missingChild := filepath.Join(dir, "not-yet-created", "accounts")

	assert.Equal(t, dir, existingDiskProbePath(missingChild))
}

func TestDescribeLightbringerGossipPorts_DefaultsAndValidation(t *testing.T) {
	msg, err := describeLightbringerGossipPorts("", "", "")
	require.NoError(t, err)
	assert.Contains(t, msg, "gossip=65400")
	assert.Contains(t, msg, "65401-65500")

	_, err = describeLightbringerGossipPorts("55010", "55001", "55100")
	assert.ErrorContains(t, err, "must not overlap")
}

func TestDisplayConfigValue_RedactsRPCSecrets(t *testing.T) {
	got := displayConfigValue(
		"network",
		"rpc",
		"https://rpc.example.invalid/?api-key=test-key-00000000-0000-4000-8000-000000000000, https://example.invalid/rpc?network=mainnet",
	)

	assert.Contains(t, got, "api-key=REDACTED")
	assert.NotContains(t, got, "test-key-00000000")
	assert.Contains(t, got, "network=mainnet")
}

func TestRenderEditList_RedactsRPCSecretsInPassiveView(t *testing.T) {
	m := newModel("config.toml")
	m.height = 40
	m.cfg = &configData{
		rpcEndpoints: []string{"https://rpc.invalid/?api-key=test-key-00000000-0000-4000-8000-000000000000"},
	}

	out := m.renderEditList()
	assert.Contains(t, out, "api-key=RED")
	assert.NotContains(t, out, "test-key-00000000")
}

func TestRenderEditFocused_RedactsRPCSecretsWhileEditing(t *testing.T) {
	m := newModel("config.toml")
	m.cfg = &configData{
		rpcEndpoints: []string{"https://rpc.invalid/?api-key=test-key-00000000-0000-4000-8000-000000000000"},
	}
	for i, field := range m.editFields {
		if field.section == "network" && field.key == "rpc" {
			m.editIdx = i
			break
		}
	}
	m.editMode = editText
	m.editValue = m.cfg.rpcEndpoints[0]
	m.editCursor = len(m.editValue)

	out := m.renderEditFocused()
	assert.Contains(t, out, "api-key=REDACTED")
	assert.Contains(t, out, "Sensitive URL values are hidden")
	assert.NotContains(t, out, "test-key-00000000")
}

func TestColorLogLine_RedactsEndpointSecrets(t *testing.T) {
	out := colorLogLine("Reference slot from https://rpc.invalid/?api-key=test-key-00000000-0000-4000-8000-000000000000")

	assert.Contains(t, out, "api-key=REDACTED")
	assert.NotContains(t, out, "test-key-00000000")
}

func TestRenderLogsView_RedactsBeforeWrapping(t *testing.T) {
	secretURL := "https://rpc.invalid/?api-key=test-key-00000000-0000-4000-8000-000000000000"
	m := newModel("config.toml")
	m.hasConfig = true
	m.width = 120
	m.height = 30
	m.screen = screenLogs
	m.mithrilLines = []string{"Reference slot from " + secretURL}

	out := m.renderLogsView()
	assert.Contains(t, out, "REDACTED")
	assert.NotContains(t, out, "test-key-00000000")
}

func TestRenderRawLogsView_RedactsBeforeWrapping(t *testing.T) {
	secretURL := "https://rpc.invalid/?api-key=test-key-00000000-0000-4000-8000-000000000000"
	m := newModel("config.toml")
	m.hasConfig = true
	m.width = 120
	m.height = 30
	m.screen = screenLogs
	m.logRawMode = true
	m.mithrilLines = []string{"Mithril repair RPC " + secretURL}

	out := m.renderRawLogsView()
	assert.Contains(t, out, "REDACTED")
	assert.NotContains(t, out, "test-key-00000000")
}

func TestMithrilOnlyLogsUseTerminalModeWithoutLightbringerNoise(t *testing.T) {
	m := newModel("config.toml")
	m.hasConfig = true
	m.cfg = &configData{blockSource: "rpc"}
	m.width = 100
	m.height = 30
	m.screen = screenLogs
	m.mithrilLines = []string{"INFO replay running"}

	out := m.renderLogsView()

	assert.Contains(t, out, "terminal logs")
	assert.Contains(t, out, "Mithril live tail")
	assert.Contains(t, out, "INFO replay running")
	assert.NotContains(t, out, "Lightbringer disabled")
	assert.NotContains(t, out, "[lightbringer]")
	assert.NotContains(t, out, "split view")
}

func TestViewFitsTerminalFrameWithRawProgressLogs(t *testing.T) {
	m := newModel("config.toml")
	m.hasConfig = true
	m.width = 100
	m.height = 24
	m.screen = screenLogs
	m.logRawMode = true
	m.mithrilLines = []string{
		"Snapshot Read (.tar.zst) 0.1% 0.1/45.4 GB ETA 7m30s\rSnapshot Read (.tar.zst) 0.8% 0.4/45.4 GB ETA 7m29s",
		"Extract (AppendVecs) 0.2% 0.5/340.0 GB ETA 7m10s\rExtract (AppendVecs) 1.0% 3.5/339.7 GB ETA 7m05s",
	}

	out := m.View()

	assert.NotContains(t, out, "\r")
	assert.LessOrEqual(t, lipgloss.Height(out), m.height)
	for _, line := range strings.Split(out, "\n") {
		assert.LessOrEqual(t, lipgloss.Width(line), m.width)
	}
}

func TestFullWidthRawLogsHideMenuInView(t *testing.T) {
	m := newModel("config.toml")
	m.hasConfig = true
	m.cfg = &configData{blockSource: "rpc"}
	m.width = 120
	m.height = 30
	m.screen = screenLogs
	m.logRawMode = true
	m.mithrilLines = []string{"slot 425544688 | leader: sample-validator | txns: v:708 nv:384 | exec: 0.486s"}

	out := m.View()

	assert.Contains(t, out, "terminal logs")
	assert.Contains(t, out, "slot 425544688")
	assert.NotContains(t, out, "Run Node")
	assert.NotContains(t, out, "Edit Config")
	for _, line := range strings.Split(out, "\n") {
		assert.LessOrEqual(t, lipgloss.Width(line), m.width)
	}
}

func TestDashboardRawLogLinesCompactsReplaySlotRows(t *testing.T) {
	raw := "(+     45.607s) slot 425556344  | leader: PUmpKiNnSVAZ3w4KaFX6jKSjXUNHFShGkXbERo54xjb  | txns: v:712   nv:1402  | cu: 50580776   | exec:  0.608s | wait:  0.000s | total:  0.608s"

	lines := dashboardRawLogLines([]string{raw}, 78)

	require.Len(t, lines, 1)
	assert.Contains(t, lines[0], "slot 425,556,344")
	assert.Contains(t, lines[0], "txns v712/nv1402")
	assert.Contains(t, lines[0], "exec 0.608s")
	assert.Contains(t, lines[0], "cu 50.6M")
	assert.NotContains(t, lines[0], ") | slot")
	assert.NotContains(t, lines[0], "leader:")
	assert.LessOrEqual(t, lipgloss.Width(lines[0]), 78)
}

func TestDashboardRawLogLinesKeepsNonSlotErrorsVerbose(t *testing.T) {
	raw := "ERROR: Replay stopped before persisting the first post-start slot: state file missing manifest_epoch_authorized_voters - delete AccountsDB and rebuild from snapshot"

	lines := dashboardRawLogLines([]string{raw}, 60)
	joined := strings.Join(lines, "\n")

	assert.Contains(t, joined, "ERROR: Replay stopped")
	assert.Contains(t, joined, "delete AccountsDB")
	assert.Greater(t, len(lines), 1)
}

func TestVisibleLogWindowDefaultsToNewestLines(t *testing.T) {
	lines := []string{"line-00", "line-01", "line-02", "line-03", "line-04", "line-05"}

	assert.Equal(t, []string{"line-03", "line-04", "line-05"}, visibleLogWindow(lines, 3, 0))
	assert.Equal(t, []string{"line-01", "line-02", "line-03"}, visibleLogWindow(lines, 3, 2))
	assert.Equal(t, []string{"line-00", "line-01", "line-02"}, visibleLogWindow(lines, 3, 99))
}

func TestRenderRawLogsViewShowsNewestLinesByDefault(t *testing.T) {
	m := newModel("config.toml")
	m.hasConfig = true
	m.width = 100
	m.height = 28
	m.screen = screenLogs
	m.logRawMode = true
	for i := 0; i < 20; i++ {
		m.mithrilLines = append(m.mithrilLines, fmt.Sprintf("line-%02d", i))
	}

	out := m.renderRawLogsView()

	assert.Contains(t, out, "line-19")
	assert.NotContains(t, out, "line-00")
}

func TestCombinedRawLogLinesInterleavesSourcesFromTail(t *testing.T) {
	m := newModel("config.toml")
	m.cfg = &configData{lbEnabled: true}
	for i := 0; i < 12; i++ {
		m.lbLines = append(m.lbLines, fmt.Sprintf("lb-%02d", i))
	}
	m.mithrilLines = []string{"mithril-00", "mithril-01"}

	lines := m.combinedRawLogLines()
	tail := strings.Join(lines[len(lines)-6:], "\n")

	assert.Contains(t, tail, "[mithril] mithril-00")
	assert.Contains(t, tail, "[mithril] mithril-01")
	assert.Contains(t, tail, "[lightbringer] lb-11")
}

func TestDataRefreshAnchorsFocusedLogScrollback(t *testing.T) {
	m := newModel("config.toml")
	m.width = 100
	m.height = 28
	m.screen = screenLogs
	m.logFocused = true
	m.logScroll = 2
	for i := 0; i < 10; i++ {
		m.mithrilLines = append(m.mithrilLines, fmt.Sprintf("old-%02d", i))
	}

	var nextLines []string
	for i := 0; i < 13; i++ {
		nextLines = append(nextLines, fmt.Sprintf("new-%02d", i))
	}
	next, _ := m.Update(dataRefreshedMsg{mithrilLines: nextLines})
	updated := next.(model)

	assert.Equal(t, 5, updated.logScroll)
}

func TestWrapLogLinesPreservesUnicodeProgressGlyphs(t *testing.T) {
	lines := wrapLogLines([]string{
		"Snapshot Read (.tar.zst) [██░░░░░░░░░░░░░░░░░░░░] 2.9% 3.2/108.1 GB 112.3 MB/s ETA 17m14s",
	}, 36)

	joined := strings.Join(lines, "\n")
	assert.Contains(t, joined, "██")
	assert.Contains(t, joined, "░░")
	assert.NotContains(t, joined, "�")
}

func TestWrapLogLinesPrefersWordBoundaries(t *testing.T) {
	lines := wrapLogLines([]string{
		"snapshot bootstrap cancelled during shutdown: failed to build AccountsDB from snapshot: processing full snapshot: context canceled",
	}, 48)

	joined := strings.Join(lines, "\n")
	assert.NotContains(t, joined, "fai\n  led")
	assert.NotContains(t, joined, "snap\n  shot")
}

func TestViewFitsTerminalFrameAcrossLogResize(t *testing.T) {
	for _, size := range []struct {
		width  int
		height int
	}{
		{width: 72, height: 22},
		{width: 100, height: 24},
		{width: 150, height: 36},
	} {
		m := newModel("config.toml")
		m.hasConfig = true
		m.cfg = &configData{blockSource: "rpc"}
		m.width = size.width
		m.height = size.height
		m.screen = screenLogs
		m.mithrilLines = []string{
			"INFO " + strings.Repeat("long-log-field ", 20),
			"ERROR " + strings.Repeat("stall-heartbeat ", 18),
		}

		out := m.View()

		assert.LessOrEqual(t, lipgloss.Height(out), size.height, "height=%dx%d", size.width, size.height)
		for _, line := range strings.Split(out, "\n") {
			assert.LessOrEqual(t, lipgloss.Width(line), size.width, "width=%dx%d line=%q", size.width, size.height, line)
		}
	}
}

func TestApplyEditField_ClearingExternalLightbringerEndpointReturnsToRPC(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[block]
source = "lightbringer"
lightbringer_endpoint = "127.0.0.1:3001"

[lightbringer]
enabled = false
`), 0600))

	m := newModel(path)
	m.cfg = readConfig(path)
	for i, field := range m.editFields {
		if field.section == "block" && field.key == "lightbringer_endpoint" {
			m.editIdx = i
			break
		}
	}
	require.Equal(t, "block", m.editFields[m.editIdx].section)
	require.Equal(t, "lightbringer_endpoint", m.editFields[m.editIdx].key)

	m.editValue = ""
	m.applyEditField()

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	body := string(content)
	assert.Contains(t, body, `lightbringer_endpoint = ""`)
	assert.Contains(t, body, `source = "rpc"`)
	assert.Contains(t, body, `enabled = false`)
	assert.False(t, strings.Contains(body, `source = "lightbringer"`))
}

func TestApplyEditField_SnapshotsPathClearsShadowingDownloadPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[network]
cluster = "mainnet-beta"
rpc = ["https://example.invalid"]

[storage]
snapshots = "/old/storage-snapshots"

[snapshot]
download_path = "/old/download-path"
`), 0600))

	m := newModel(path)
	m.cfg = readConfig(path)
	for i, field := range m.editFields {
		if field.section == "storage" && field.key == "snapshots" {
			m.editIdx = i
			break
		}
	}
	m.editValue = "/new/snapshots"
	m.applyEditField()

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	body := string(content)
	assert.Contains(t, body, `snapshots = "/new/snapshots"`)
	assert.Contains(t, body, `# download_path = "/old/download-path"`)
}

func TestRunDoctorChecks_WarnsOnPublicMainnetRPC(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`[network]`), 0600))

	for _, endpoint := range []string{
		"https://api.mainnet-beta.solana.com",
		"https://api.mainnet.solana.com",
	} {
		checks := runDoctorChecks(path, &configData{
			cluster:      "mainnet-beta",
			rpcEndpoints: []string{endpoint},
			accountsPath: "/tmp/mithril-accounts",
			blockSource:  "rpc",
		})

		found := false
		for _, check := range checks {
			if check.name == "RPC capacity" {
				found = true
				assert.Equal(t, "warn", check.status)
				assert.Contains(t, check.msg, "private RPC")
			}
		}
		assert.Truef(t, found, "doctor should warn before long mainnet runs use public RPC: %s", endpoint)
	}
}

func TestLatestReplaySlot(t *testing.T) {
	cases := []struct {
		name  string
		lines []string
		want  uint64
		ok    bool
	}{
		{"per-slot replay line", []string{"(+   3m24.830s) slot 426087845  | leader: X | txns: v:716"}, 426087845, true},
		{"picks the latest (newest last)", []string{
			"(+1s) slot 100  | leader: A",
			"(+2s) slot 200  | leader: B",
		}, 200, true},
		{"ignores snapshot slot (no pipe)", []string{"building from snapshot slot 426004874"}, 0, false},
		{"no slot at all", []string{"Probing nodes for snapshot availability..."}, 0, false},
		{"empty", nil, 0, false},
		{"skips trailing non-slot line back to last replay line", []string{
			"(+1s) slot 999  | leader: A",
			"AccountsDB is ready",
		}, 999, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := latestReplaySlot(c.lines)
			if ok != c.ok || got != c.want {
				t.Errorf("latestReplaySlot = (%d,%v), want (%d,%v)", got, ok, c.want, c.ok)
			}
		})
	}
}
