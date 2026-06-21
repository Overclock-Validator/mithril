package dashboardcmd

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/procctl"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Stuck is set on Stop timeout, cleared on successful detection or Force Stop.

// Stop timeout transitions the model into Stuck (surfaces [f] Force Stop).
func TestActionResultMsg_TimeoutSetsStuck(t *testing.T) {
	m := runningModel()
	m.proc.inFlightOp = opStop
	assert.False(t, m.proc.stuck, "precondition: not stuck yet")

	updated, _ := m.Update(actionResultMsg{
		op:     opStop,
		result: "timeout",
		err:    procctl.ErrStopTimeout.Error(),
	})
	res := updated.(model)
	assert.True(t, res.proc.stuck, "stop timeout must set stuck=true")
}

// Restart's stop-phase timeout also sets Stuck.
func TestActionResultMsg_RestartTimeoutSetsStuck(t *testing.T) {
	m := runningModel()
	m.proc.inFlightOp = opRestart

	updated, _ := m.Update(actionResultMsg{
		op:     opRestart,
		result: "timeout",
	})
	res := updated.(model)
	assert.True(t, res.proc.stuck)
}

// Stuck applies only to Stop/Restart timeouts, not Start.
func TestActionResultMsg_StartTimeoutDoesNotSetStuck(t *testing.T) {
	m := runningModel()
	m.proc.inFlightOp = opStart
	updated, _ := m.Update(actionResultMsg{op: opStart, result: "timeout"})
	res := updated.(model)
	assert.False(t, res.proc.stuck, "Start timeout must not flip stuck")
}

// Stuck auto-clears once detect shows the process is gone.
func TestProcDetected_ClearsStuckWhenProcessGone(t *testing.T) {
	m := runningModel()
	m.proc.stuck = true

	updated, _ := m.Update(procDetectedMsg{
		det: &procctl.Detection{Status: procctl.StatusStopped},
	})
	res := updated.(model)
	assert.False(t, res.proc.stuck,
		"stuck must clear when subsequent detect shows process is gone")
}

// Force Stop success clears Stuck.
func TestActionResultMsg_ForceStopOkClearsStuck(t *testing.T) {
	m := runningModel()
	m.proc.stuck = true

	updated, _ := m.Update(actionResultMsg{op: opForceStop, result: "ok"})
	res := updated.(model)
	assert.False(t, res.proc.stuck)
}

// Force Stop on a non-stuck node is a silent no-op (no pre-emptive SIGKILL).
func TestHandleForceStopKey_NoOpWhenNotStuck(t *testing.T) {
	// Absent PID file so checkSystemd() stays deterministic across machines.
	t.Setenv("MITHRIL_PID_FILE", filepath.Join(t.TempDir(), "mithril.pid"))
	m := runningModel()
	cmd := m.handleForceStopKey()
	assert.Nil(t, cmd, "Force Stop on a non-stuck running mithril is a no-op")
	assert.False(t, m.confirmActive, "no modal should open")
}

// Stuck → Force Stop opens a heavyweight warning modal.
func TestHandleForceStopKey_OpensSevereModalWhenStuck(t *testing.T) {
	// Absent PID file → checkSystemd() returns "" → Force Stop modal, not a systemd one.
	t.Setenv("MITHRIL_PID_FILE", filepath.Join(t.TempDir(), "mithril.pid"))
	m := runningModel()
	m.proc.stuck = true

	cmd := m.handleForceStopKey()
	assert.Nil(t, cmd, "Force Stop opens a modal first")
	assert.True(t, m.confirmActive)
	assert.Contains(t, m.confirmTitle, "Force Stop")
	assert.Contains(t, m.confirmTitle, "Data Loss",
		"title must convey severity")
	assert.Contains(t, m.confirmBody, "SIGKILL", "body must name the signal")
	assert.Contains(t, m.confirmBody, "AccountsDB", "body must warn about data risk")
}

// Even with a stale stuck=true, refuse Force Stop if no longer Running.
func TestHandleForceStopKey_NoOpWhenNotRunning(t *testing.T) {
	t.Setenv("MITHRIL_PID_FILE", filepath.Join(t.TempDir(), "mithril.pid"))
	m := stoppedModel(procctl.StatusStopped)
	m.proc.stuck = true // stale flag

	cmd := m.handleForceStopKey()
	assert.Nil(t, cmd)
	// modal stays closed = refusal
	assert.False(t, m.confirmActive, "no Force Stop modal may open when not Running")
}

// Stuck-running shows all three keys: [x] Stop, [r] Restart, [f] Force Stop.
func TestRenderActionHints_StuckRunningShowsForceStop(t *testing.T) {
	dim := dummyStyle()
	out := renderActionHints(procctl.StatusRunning, "", true, nil, dim)
	assert.Contains(t, out, "[f]")
	assert.Contains(t, out, "Force Stop")
}

// The three documented spawn tokens runLive emits are translated.
func TestFriendlySpawnedBy_TranslatesKnownTokens(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"cli", "command line"},
		{"dashboard", "this dashboard"},
		{"external", "external (started outside the dashboard)"},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			assert.Equal(t, c.want, friendlySpawnedBy(c.in))
		})
	}
}

// Unknown tokens fall through as-is (forward-compat).
func TestFriendlySpawnedBy_PreservesUnknown(t *testing.T) {
	assert.Equal(t, "systemd", friendlySpawnedBy("systemd"))
	assert.Equal(t, "", friendlySpawnedBy(""))
}

func TestMithrilSystemdUnit_IgnoresGenericLoginServices(t *testing.T) {
	for _, cgroup := range []string{
		"0::/user.slice/user-1000.slice/session-2431.scope",
		"0::/system.slice/ssh.service",
		"0::/system.slice/tmux.service",
	} {
		_, ok := mithrilSystemdUnit(cgroup)
		assert.False(t, ok, "generic session/service should not block dashboard control: %s", cgroup)
	}
}

func TestMithrilSystemdUnit_DetectsMithrilUnit(t *testing.T) {
	cases := []string{
		"0::/system.slice/mithril.service",
		"0::/system.slice/mithril-node.service",
		"11:memory:/system.slice/mithril.service",
	}
	for _, cgroup := range cases {
		unit, ok := mithrilSystemdUnit(cgroup)
		assert.True(t, ok, "expected mithril unit from cgroup: %s", cgroup)
		assert.Contains(t, unit, "mithril")
	}
}

// UID-mismatch gate: returns "" when not root and doesn't crash on a missing dir.
func TestPreflightCheck_RootMustNotSpawnOverNonRootAccountsDB(t *testing.T) {
	reason := checkUIDMatch("")
	assert.Equal(t, "", reason, "no accountsDir → no refusal (fresh setup)")

	reason = checkUIDMatch("/nonexistent/path")
	assert.Equal(t, "", reason, "missing dir → no refusal (bootstrap creates it)")
}

func TestPreflightCheck_RefusesUnwritableLogDirectoryParent(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can create directories regardless of mode bits")
	}
	parent := t.TempDir()
	require.NoError(t, os.Chmod(parent, 0555))
	defer os.Chmod(parent, 0755)

	reason := checkLogPathWritable(filepath.Join(parent, "mithril-logs"))

	assert.Contains(t, reason, "cannot write logs")
	assert.Contains(t, reason, "Fix with safe folders")
}

func TestPreflightCheck_RefusesOldAccountsStateWithoutAuthorizedVoters(t *testing.T) {
	dir := t.TempDir()
	st := state.NewReadyState(468306806, 1084, "", "", 0, 0)
	st.Cluster = "devnet"
	st.ManifestEpochStakes = map[uint64]string{1084: "{}"}
	require.NoError(t, st.Save(dir))

	reason := checkAccountsStateCompatible(&configData{cluster: "devnet"}, dir)

	assert.Contains(t, reason, "cannot be resumed")
	assert.Contains(t, reason, "manifest_epoch_authorized_voters")
}

func TestPreflightCheck_RefusesAccountsStateClusterMismatch(t *testing.T) {
	dir := t.TempDir()
	st := state.NewReadyState(1, 1, "", "", 0, 0)
	st.Cluster = "mainnet-beta"
	require.NoError(t, st.Save(dir))

	reason := checkAccountsStateCompatible(&configData{cluster: "devnet"}, dir)

	assert.Contains(t, reason, "different cluster")
	assert.Contains(t, reason, "mainnet-beta")
	assert.Contains(t, reason, "devnet")
}

// Stuck surfaces the data-loss-risk warning plus the [f] Force Stop hint.
func TestRenderProcessView_StuckShowsBanner(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{
			detection: &procctl.Detection{
				Status: procctl.StatusRunning,
				Pid:    12345,
			},
			stuck:     true,
			fetchedAt: time.Now(),
		},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Stop timed out", "should explain WHY stuck is set")
	assert.Contains(t, out, "[f]", "should advertise Force Stop key")
	assert.Contains(t, out, "Force Stop")
	assert.Contains(t, out, "data loss risk", "must warn about destructive op")
}

// preflightErr surfaces clearly so the user knows why Start was refused.
func TestRenderProcessView_PreflightErrShowsRefusalBanner(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{
			detection: &procctl.Detection{Status: procctl.StatusStopped},
			preflightErr: "Mithril is managed by systemd. The dashboard would fight\n" +
				"systemd's restart loop. To control it, use:\n\n" +
				"    sudo systemctl stop mithril",
			fetchedAt: time.Now(),
		},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Setup needs attention", "must headline the blocker")
	assert.Contains(t, out, "managed by systemd")
	assert.Contains(t, out, "systemctl", "must surface remediation command")
}

func TestRunActions_PreflightShowsSafeFolderFixBeforeStart(t *testing.T) {
	m := stoppedModel(procctl.StatusStopped)
	m.runFocused = true
	m.proc.preflightErr = "Stored node data cannot be resumed safely."

	actions := m.runActions()

	require.NotEmpty(t, actions)
	assert.Equal(t, runActionSafe, actions[0].id)
	for _, action := range actions {
		assert.NotEqual(t, runActionStart, action.id, "Start should not be offered while blockers are present")
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Fix with safe folders")
	assert.Contains(t, out, "Setup needs attention")
}

func TestApplySafeStoragePathsCreatesUserOwnedFoldersAndUpdatesConfig(t *testing.T) {
	// Stub disk to "plenty"; the safe-folders guard refuses too-small disks.
	orig := diskFreeGBFn
	diskFreeGBFn = func(string) (uint64, bool) { return 100000, true }
	t.Cleanup(func() { diskFreeGBFn = orig })

	home := t.TempDir()
	t.Setenv("HOME", home)
	configFile := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(configFile, []byte(`
[network]
cluster = "devnet"
rpc = ["https://api.devnet.solana.com"]

[storage]
accounts = "/mnt/mithril-accounts"
snapshots = "/mnt/mithril-ledger/snapshots"
shredstore = "/mnt/mithril-ledger/shredstore"
logs = "/mnt/mithril-logs"

[bootstrap]
mode = "accountsdb"
`), 0600))

	summary, err := applySafeStoragePaths(configFile, &configData{cluster: "devnet"})
	require.NoError(t, err)

	root := filepath.Join(home, "mithril-data", "devnet")
	assert.Contains(t, summary, root)
	for _, dir := range []string{
		filepath.Join(root, "accounts"),
		filepath.Join(root, "snapshots"),
		filepath.Join(root, "shredstore"),
		filepath.Join(root, "logs"),
	} {
		info, err := os.Stat(dir)
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	}
	cfg := readConfig(configFile)
	require.NotNil(t, cfg)
	assert.Equal(t, filepath.Join(root, "accounts"), cfg.accountsPath)
	assert.Equal(t, filepath.Join(root, "snapshots"), cfg.snapshotsPath)
	assert.Equal(t, filepath.Join(root, "shredstore"), cfg.shredstorePath)
	assert.Equal(t, filepath.Join(root, "logs"), cfg.logsPath)
	assert.Equal(t, "auto", cfg.bootstrapMode)
}

// Too-small disk: refuse with actionable error, leave config unchanged.
func TestApplySafeStoragePathsRefusesTooSmallDisk(t *testing.T) {
	orig := diskFreeGBFn
	diskFreeGBFn = func(string) (uint64, bool) { return 10, true } // 10 GB free
	t.Cleanup(func() { diskFreeGBFn = orig })

	home := t.TempDir()
	t.Setenv("HOME", home)
	configFile := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(configFile, []byte(`
[network]
cluster = "mainnet-beta"
[storage]
accounts = "/mnt/mithril-accounts"
`), 0600))

	_, err := applySafeStoragePaths(configFile, &configData{cluster: "mainnet-beta"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "10 GB free")
	assert.Contains(t, err.Error(), "larger disk")
	// config must stay unchanged on refusal
	cfg := readConfig(configFile)
	require.NotNil(t, cfg)
	assert.Equal(t, "/mnt/mithril-accounts", cfg.accountsPath)
}
