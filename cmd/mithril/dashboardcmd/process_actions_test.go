package dashboardcmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/procctl"
	"github.com/Overclock-Validator/mithril/pkg/state"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Action key handlers: dashboard decision logic only (cmd vs modal vs no-op).
// Real spawn/signal/wait paths are tested in pkg/procctl.

// runningModel: mithril running (Stop and Restart valid, Start not).
func runningModel() *model {
	return &model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{detection: &procctl.Detection{
			Status: procctl.StatusRunning,
			Pid:    12345,
		}},
	}
}

// stoppedModel mirrors runningModel for Stopped/Crashed branches.
func stoppedModel(status procctl.Status) *model {
	return &model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{detection: &procctl.Detection{
			Status: status,
		}},
	}
}

// Start while running is silently ignored — guards against double-spawn.
func TestHandleStartKey_NoOpWhenRunning(t *testing.T) {
	m := runningModel()
	cmd := m.handleStartKey()
	assert.Nil(t, cmd, "Start while Running should be a no-op")
	assert.False(t, m.confirmActive, "Start must not open a modal")
	assert.Empty(t, m.proc.inFlightOp, "Start must not flip inFlightOp on no-op")
}

// Start while Stopped opens the interactive flow before spawning.
func TestHandleStartKey_OpensStartFlowWhenStopped(t *testing.T) {
	m := stoppedModel(procctl.StatusStopped)
	cmd := m.handleStartKey()
	assert.Nil(t, cmd)
	assert.True(t, m.startFlow.active)
	assert.False(t, m.confirmActive)
	assert.Empty(t, m.proc.inFlightOp)

	// One advance (Enter) starts mithril.
	cmd = m.advanceStartFlow()
	assert.NotNil(t, cmd, "confirm starts mithril")
	assert.Equal(t, opStart, m.proc.inFlightOp)
}

func TestHandleStartKey_ManagedLightbringerUsesStartFlow(t *testing.T) {
	m := stoppedModel(procctl.StatusStopped)
	m.cfg = &configData{lbEnabled: true}

	cmd := m.handleStartKey()
	assert.Nil(t, cmd, "Managed Lightbringer Start should wait for the start flow")
	assert.True(t, m.startFlow.active)
	assert.False(t, m.confirmActive)
	assert.Contains(t, m.renderStartFlow(), "Mithril + Lightbringer")
	assert.NotContains(t, m.renderStartFlow(), "Steps:")
	assert.Empty(t, m.proc.inFlightOp, "no spawn until the operator confirms")

	// One advance (Enter) starts.
	cmd = m.advanceStartFlow()
	assert.NotNil(t, cmd)
	assert.Equal(t, opStart, m.proc.inFlightOp)
}

// Start opens the flow on Crashed (post-crash recovery), not refused.
func TestHandleStartKey_AlsoWorksOnCrashed(t *testing.T) {
	m := stoppedModel(procctl.StatusCrashed)
	cmd := m.handleStartKey()
	assert.Nil(t, cmd, "Start should open the flow on Crashed")
	assert.True(t, m.startFlow.active)
}

// Stop opens a confirm modal; no signal until the user confirms.
func TestHandleStopKey_OpensConfirmModalWhenRunning(t *testing.T) {
	m := runningModel()
	cmd := m.handleStopKey()
	assert.Nil(t, cmd, "Stop opens a modal; cmd fires only on Y")
	assert.True(t, m.confirmActive, "Stop must open the confirmation modal")
	assert.Contains(t, m.confirmTitle, "Stop", "modal title should announce Stop")
	assert.NotNil(t, m.confirmOnYes, "modal must carry an onYes callback")
	assert.Empty(t, m.proc.inFlightOp, "no action until user confirms")
}

func TestConfirmStopViaUpdateSetsInFlightOnReturnedModel(t *testing.T) {
	m := runningModel()
	m.runFocused = true

	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	res := updated.(model)
	require.Nil(t, cmd)
	require.True(t, res.confirmActive)

	updated, cmd = res.Update(tea.KeyMsg{Type: tea.KeyEnter})
	res = updated.(model)
	assert.NotNil(t, cmd)
	assert.Equal(t, opStop, res.proc.inFlightOp)
	assert.False(t, res.confirmActive)
}

// Stop while already stopped is silent.
func TestHandleStopKey_NoOpWhenStopped(t *testing.T) {
	m := stoppedModel(procctl.StatusStopped)
	cmd := m.handleStopKey()
	assert.Nil(t, cmd)
	assert.False(t, m.confirmActive)
}

// Restart gates behind confirmation (destructive — SIGTERMs the running node).
func TestHandleRestartKey_OpensConfirmModalWhenRunning(t *testing.T) {
	m := runningModel()
	cmd := m.handleRestartKey()
	assert.Nil(t, cmd, "Restart opens a modal")
	assert.True(t, m.confirmActive)
	assert.Contains(t, m.confirmTitle, "Restart")
	assert.NotNil(t, m.confirmOnYes)
}

// Restart while Stopped is a no-op (use Start).
func TestHandleRestartKey_NoOpWhenStopped(t *testing.T) {
	m := stoppedModel(procctl.StatusStopped)
	cmd := m.handleRestartKey()
	assert.Nil(t, cmd)
	assert.False(t, m.confirmActive)
}

// Rebuild in place: destructive recovery (reuses paths, wipes diverged
// AccountsDB, rebuilds via bootstrap=snapshot) offered alongside "Rebuild fresh".

// Divergence crash offers the danger-flagged in-place rebuild after the safe one.
func TestRunActions_DivergenceOffersRebuildInPlace(t *testing.T) {
	m := stoppedModel(procctl.StatusCrashed)
	m.runFocused = true
	m.proc.startFailStderr = "FATAL: replay divergence detected at slot 123"
	require.True(t, m.hasReplayDivergenceCrash(), "test setup must trigger divergence path")

	actions := m.runActions()
	require.NotEmpty(t, actions)
	assert.Equal(t, runActionSafe, actions[0].id, "fresh, non-destructive recovery stays first")

	var inPlace *runAction
	for i := range actions {
		if actions[i].id == runActionInPlace {
			inPlace = &actions[i]
		}
	}
	require.NotNil(t, inPlace, "in-place rebuild must be offered on divergence")
	assert.True(t, inPlace.danger, "in-place rebuild deletes data — must be danger-flagged")
}

// Confirmation names the path and warns about deletion — never a silent wipe.
func TestOpenRebuildInPlaceConfirmation_DestructiveConfirm(t *testing.T) {
	m := stoppedModel(procctl.StatusCrashed)
	m.cfg = &configData{
		cluster:       "mainnet-beta",
		accountsPath:  "/mnt/mithril-accounts",
		snapshotsPath: "/mnt/mithril-ledger/snapshots",
	}

	cmd := m.openRebuildInPlaceConfirmation()
	assert.Nil(t, cmd, "confirmation opens a modal; the apply cmd fires only on Yes")
	assert.True(t, m.confirmActive)
	assert.NotNil(t, m.confirmOnYes, "modal must carry an onYes callback")
	assert.Contains(t, strings.ToLower(m.confirmTitle), "delete", "title must warn it deletes data")
	assert.Contains(t, m.confirmBody, "/mnt/mithril-accounts", "body must name the path being rebuilt")
}

// No AccountsDB path → actionable message, not a destructive modal.
func TestOpenRebuildInPlaceConfirmation_NoOpWithoutAccountsPath(t *testing.T) {
	m := stoppedModel(procctl.StatusCrashed)
	m.cfg = &configData{cluster: "mainnet-beta"} // no accountsPath

	cmd := m.openRebuildInPlaceConfirmation()
	assert.Nil(t, cmd)
	assert.False(t, m.confirmActive, "no destructive modal without a path to rebuild")
	assert.NotEmpty(t, m.proc.preflightErr)
}

// Apply flips bootstrap mode to "snapshot" and leaves storage paths untouched.
// Disk readiness stubbed to "fits" so the config write runs regardless of disk.
func TestApplyRebuildInPlaceSetsSnapshotMode(t *testing.T) {
	orig := checkBuildSpaceFn
	checkBuildSpaceFn = func(string, string, string) config.BuildSpaceCheck {
		return config.BuildSpaceCheck{Determined: true, OK: true, UsableGB: 900, NeedGB: 600}
	}
	t.Cleanup(func() { checkBuildSpaceFn = orig })

	dir := t.TempDir()
	accounts := filepath.Join(dir, "accounts")
	snapshots := filepath.Join(dir, "snapshots")
	configFile := filepath.Join(dir, "config.toml")
	require.NoError(t, os.WriteFile(configFile, []byte(`
[network]
cluster = "mainnet-beta"
[storage]
accounts = "`+accounts+`"
snapshots = "`+snapshots+`"
[bootstrap]
mode = "auto"
`), 0600))

	cfg := &configData{cluster: "mainnet-beta", accountsPath: accounts, snapshotsPath: snapshots}
	summary, err := applyRebuildInPlace(configFile, cfg)
	require.NoError(t, err)
	assert.Contains(t, summary, accounts)

	got := readConfig(configFile)
	require.NotNil(t, got)
	assert.Equal(t, "snapshot", got.bootstrapMode, "bootstrap mode must be set to snapshot")
	assert.Equal(t, accounts, got.accountsPath, "storage paths must be unchanged")
	assert.Equal(t, snapshots, got.snapshotsPath, "storage paths must be unchanged")
}

// Apply refuses and writes nothing when the disk still can't fit the rebuild.
func TestApplyRebuildInPlaceRefusesWhenDiskTooSmall(t *testing.T) {
	orig := checkBuildSpaceFn
	checkBuildSpaceFn = func(string, string, string) config.BuildSpaceCheck {
		return config.BuildSpaceCheck{Determined: true, OK: false, Reason: "not enough disk space: needs ~600 GB; point storage at a larger disk"}
	}
	t.Cleanup(func() { checkBuildSpaceFn = orig })

	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.toml")
	require.NoError(t, os.WriteFile(configFile, []byte(`
[network]
cluster = "mainnet-beta"
[storage]
accounts = "/mnt/mithril-accounts"
[bootstrap]
mode = "auto"
`), 0600))

	cfg := &configData{cluster: "mainnet-beta", accountsPath: "/mnt/mithril-accounts"}
	_, err := applyRebuildInPlace(configFile, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "larger disk")

	got := readConfig(configFile)
	require.NotNil(t, got)
	assert.Equal(t, "auto", got.bootstrapMode, "config must be untouched on refusal")
}

// canAct — gate that serializes all action keys.

// A second action is blocked while one is in flight (e.g. spamming 'x').
func TestCanAct_BlocksDuringInFlightOp(t *testing.T) {
	m := runningModel()
	m.proc.inFlightOp = opStop // stop already in progress
	assert.False(t, m.canAct(), "must not allow new actions during stop")

	// Even Start is blocked: spawning during shutdown would race for the flock.
	cmd := m.handleStartKey()
	assert.Nil(t, cmd)
}

// Action keys must not fire while typing in the editor (e.g. "stop" in a URL).
func TestCanAct_BlocksDuringTextEdit(t *testing.T) {
	m := runningModel()
	m.editMode = editText
	assert.False(t, m.canAct())
}

// Stop must not fire while scrolling logs.
func TestCanAct_BlocksWhileLogFocused(t *testing.T) {
	m := runningModel()
	m.logFocused = true
	assert.False(t, m.canAct())
}

// renderActionHints — secondary shortcuts below the Run Node action list.

// Running shows exactly Stop and Restart, nothing else.
func TestRenderActionHints_RunningShowsStopAndRestart(t *testing.T) {
	dim := dummyStyle()
	out := renderActionHints(procctl.StatusRunning, "", false, nil, dim)
	assert.Contains(t, out, "[x]")
	assert.Contains(t, out, "Stop")
	assert.Contains(t, out, "[r]")
	assert.Contains(t, out, "Restart")
	assert.NotContains(t, out, "[s]", "Running state must NOT advertise Start")
	assert.NotContains(t, out, "[f]", "non-stuck Running must NOT advertise Force Stop")
}

// Once stuck, [f] Force Stop joins the hint line.
func TestRenderActionHints_StuckShowsForceStop(t *testing.T) {
	dim := dummyStyle()
	out := renderActionHints(procctl.StatusRunning, "", true, nil, dim)
	assert.Contains(t, out, "[f]", "stuck state must surface Force Stop")
	assert.Contains(t, out, "Force Stop")
	// Force Stop is additive — Stop/Restart still present.
	assert.Contains(t, out, "[x]")
	assert.Contains(t, out, "[r]")
}

// Stopped only shows Start, never Stop/Restart.
func TestRenderActionHints_StoppedShowsStart(t *testing.T) {
	dim := dummyStyle()
	out := renderActionHints(procctl.StatusStopped, "", false, nil, dim)
	assert.Contains(t, out, "[s]")
	assert.Contains(t, out, "Start")
	assert.NotContains(t, out, "Stop")
	assert.NotContains(t, out, "Restart")
}

// During an inFlightOp, hints explain keys are disabled rather than advertise them.
func TestRenderActionHints_InFlightDisablesAll(t *testing.T) {
	dim := dummyStyle()
	out := renderActionHints(procctl.StatusRunning, opStop, false, nil, dim)
	assert.Contains(t, out, "disabled")
	assert.NotContains(t, out, "[x]")
}

// Confirmation modal rendering.

// Smoke check: modal frame produces the expected user-visible content.
func TestRenderConfirmModal_IncludesTitleAndKeys(t *testing.T) {
	out := renderConfirmModal("Stop Mithril?", "This sends SIGTERM.", 80)
	assert.Contains(t, out, "Stop Mithril?", "title must appear")
	assert.Contains(t, out, "SIGTERM", "body must appear")
	assert.Contains(t, out, "[enter]", "Enter confirm key must be hinted")
	assert.Contains(t, out, "[y]", "Yes key must be hinted")
	assert.Contains(t, out, "[n]", "No key must be hinted")
}

func TestRenderConfirmModal_WrapsBodyToPaneWidth(t *testing.T) {
	out := renderConfirmModal(
		"Use safe folders?",
		"This updates the config to user-owned folders and keeps old data untouched.",
		46,
	)

	assert.Contains(t, out, "keeps old")
	assert.Contains(t, out, "data untouched")
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		assert.LessOrEqual(t, lipgloss.Width(line), 46, "line should fit pane: %q", line)
	}
}

// Ring buffer

// Bounded write: 13 bytes into a cap-8 buffer keeps only the last 8.
func TestRingBuffer_DropsOldest(t *testing.T) {
	rb := newRingBuffer(8)
	_, _ = rb.Write([]byte("01234"))
	_, _ = rb.Write([]byte("56789ABC"))
	got := rb.String()
	assert.Equal(t, "56789ABC", got,
		"ring buffer should retain only the last 8 bytes (got %q)", got)
	assert.LessOrEqual(t, len(got), 8)
}

// Write reports full input length even past cap (safe for io.MultiWriter).
func TestRingBuffer_NeverShortWrites(t *testing.T) {
	rb := newRingBuffer(4)
	n, err := rb.Write([]byte("hello-this-is-longer"))
	assert.NoError(t, err)
	assert.Equal(t, len("hello-this-is-longer"), n)
}

// Startup failure reporting reads only the tail of the stderr log.
func TestReadFileTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stderr.log")
	require.NoError(t, os.WriteFile(path, []byte("0123456789abcdef"), 0600))

	assert.Equal(t, "cdef", readFileTail(path, 4))
	assert.Equal(t, "0123456789abcdef", readFileTail(path, 64))
}

func TestCreatePrivateTempLogFile_Uses0600(t *testing.T) {
	f, path, err := createPrivateTempLogFile("mithril-dashboard-test-*.log")
	require.NoError(t, err)
	defer os.Remove(path)
	require.NoError(t, f.Close())

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

func TestCleanupOldDashboardSpawnTempLogs(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)

	oldPath := filepath.Join(dir, "mithril-dashboard-spawn-stderr-old.log")
	newPath := filepath.Join(dir, "mithril-dashboard-spawn-stderr-new.log")
	otherPath := filepath.Join(dir, "other.log")
	require.NoError(t, os.WriteFile(oldPath, []byte("old"), 0600))
	require.NoError(t, os.WriteFile(newPath, []byte("new"), 0600))
	require.NoError(t, os.WriteFile(otherPath, []byte("other"), 0600))
	oldTime := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(oldPath, oldTime, oldTime))

	cleanupOldDashboardSpawnTempLogs(time.Hour)

	_, err := os.Stat(oldPath)
	assert.True(t, os.IsNotExist(err), "old dashboard spawn log should be removed")
	_, err = os.Stat(newPath)
	assert.NoError(t, err, "fresh dashboard spawn log should stay")
	_, err = os.Stat(otherPath)
	assert.NoError(t, err, "unrelated temp file should stay")
}

func TestCleanCompletionAfterStart_RequiresMatchingCleanRunID(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MITHRIL_PID_FILE", filepath.Join(dir, "mithril.pid"))

	st := state.NewReadyState(100, 1, "", "", 0, 0)
	st.CurrentRunID = "child-run"
	st.LastShutdownReason = state.ShutdownReasonCompleted
	st.LastShutdownAt = st.CurrentSessionStartedAt.Add(time.Second)
	require.NoError(t, st.Save(dir))

	det, ok := cleanCompletionAfterStart(dir, "child-run")
	require.True(t, ok)
	require.NotNil(t, det)
	assert.Equal(t, procctl.StatusStopped, det.Status)
	assert.True(t, det.LastCleanExit)

	_, ok = cleanCompletionAfterStart(dir, "other-run")
	assert.False(t, ok)
}

func TestPidFileDescribesOtherLiveLockedProcess_IgnoresStalePidFile(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")

	require.NoError(t, procctl.WritePidFile(pidPath, &procctl.PidInfo{
		Pid:            9999999,
		StartTimeTicks: 1,
		ExeInode:       1,
		RunID:          "stale",
		SpawnedBy:      "dashboard",
	}))

	pid, ok := pidFileDescribesOtherLiveLockedProcess(pidPath, lockPath, dir, os.Getpid())
	assert.False(t, ok)
	assert.Zero(t, pid)
}

func TestPidFileDescribesOtherLiveLockedProcess_RecognizesLiveLockedProcess(t *testing.T) {
	id, err := procctl.ReadIdentity(os.Getpid())
	if err != nil {
		t.Skipf("process identity unavailable in this sandbox: %v", err)
	}

	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")
	require.NoError(t, procctl.WritePidFile(pidPath, &procctl.PidInfo{
		Pid:            os.Getpid(),
		StartTimeTicks: id.StartTimeTicks,
		ExeInode:       id.ExeInode,
		BinaryPath:     id.ExePath,
		RunID:          "live",
		SpawnedBy:      "dashboard",
	}))
	lh, err := procctl.AcquireLock(lockPath)
	require.NoError(t, err)
	defer lh.Release()

	pid, ok := pidFileDescribesOtherLiveLockedProcess(pidPath, lockPath, dir, os.Getpid()+1)
	assert.True(t, ok)
	assert.Equal(t, os.Getpid(), pid)
}

// Update message flow — result handler clears state and re-triggers detection.

// Successful action result resets state so the next key press is accepted.
func TestActionResultMsg_ClearsInFlightOp(t *testing.T) {
	m := runningModel()
	m.proc.inFlightOp = opStop
	m.proc.opStartedAt = time.Now().Add(-2 * time.Second)

	updated, _ := m.Update(actionResultMsg{op: opStop, result: "ok"})
	res := updated.(model)
	assert.Empty(t, res.proc.inFlightOp, "successful result must clear inFlightOp")
	// Last progress line is the "Done." marker.
	if assert.NotEmpty(t, res.proc.progressLines) {
		assert.Equal(t, "Done.", res.proc.progressLines[len(res.proc.progressLines)-1])
	}
}

// Stop flips the view to Stopped immediately from the final detection.
func TestActionResultMsg_AppliesFinalDetection(t *testing.T) {
	m := runningModel()
	m.proc.inFlightOp = opStop

	updated, _ := m.Update(actionResultMsg{
		op:     opStop,
		result: "ok",
		det:    &procctl.Detection{Status: procctl.StatusStopped},
	})
	res := updated.(model)
	require.NotNil(t, res.proc.detection)
	assert.Equal(t, procctl.StatusStopped, res.proc.detection.Status)
	assert.Empty(t, res.proc.fetchErr)
	assert.False(t, res.proc.fetchedAt.IsZero())
	assert.False(t, res.proc.lastOkAt.IsZero())
}

func TestActionResultMsg_StopSuccessSelectsStartAction(t *testing.T) {
	m := runningModel()
	m.screen = screenProcess
	m.runFocused = true
	m.runActionIdx = 1 // Stop safely while running
	m.proc.inFlightOp = opStop

	updated, _ := m.Update(actionResultMsg{
		op:     opStop,
		result: "ok",
		det:    &procctl.Detection{Status: procctl.StatusStopped},
	})
	res := updated.(model)

	assert.Equal(t, 0, res.runActionIdx)
	actions := res.runActions()
	require.NotEmpty(t, actions)
	assert.Equal(t, runActionStart, actions[res.runActionIdx].id)
}

func TestActionResultMsg_StartSuccessSwitchesToLogs(t *testing.T) {
	m := stoppedModel(procctl.StatusStopped)
	m.proc.inFlightOp = opStart
	m.logRawMode = true
	m.startFlow = startFlowState{active: true, step: 2}
	m.items = []menuItem{
		{label: "Run Node", value: "process"},
		{label: "Logs", value: "logs"},
	}

	updated, _ := m.Update(actionResultMsg{op: opStart, result: "ok"})
	res := updated.(model)
	assert.Equal(t, screenLogs, res.screen)
	assert.Equal(t, 1, res.cursor, "left menu should follow the automatic Logs handoff")
	assert.False(t, res.logFocused, "logs should open readable, not trap q until esc")
	assert.False(t, res.logRawMode, "Mithril-only start keeps the menu visible — no full-width takeover (use t/Terminal logs to opt in)")
	assert.False(t, res.startFlow.active, "start success must clear the guided overlay")
}

func TestRunNodeRawLogsActionUsesExistingLogView(t *testing.T) {
	m := stoppedModel(procctl.StatusRunning)
	m.runFocused = true
	m.runActionIdx = 1 // Terminal logs
	m.items = []menuItem{
		{label: "Run Node", value: "process"},
		{label: "Logs", value: "logs"},
	}

	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	res := updated.(model)

	assert.NotNil(t, cmd, "opening logs refreshes tails from disk")
	assert.Equal(t, screenLogs, res.screen)
	assert.True(t, res.logRawMode)
	assert.False(t, res.runFocused)
	assert.Empty(t, res.proc.inFlightOp, "raw logs must not start or stop a process")
}

func TestLogsToggleSwitchesRawModeWithoutChangingProcess(t *testing.T) {
	m := model{
		hasConfig:    true,
		screen:       screenLogs,
		cfg:          &configData{lbEnabled: true},
		mithrilLines: []string{"INFO mithril started"},
		lbLines:      []string{"WARN repair peer missing"},
		proc:         procState{inFlightOp: ""},
	}

	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'t'}})
	res := updated.(model)
	assert.Nil(t, cmd)
	assert.True(t, res.logRawMode)
	assert.Empty(t, res.proc.inFlightOp)
	assert.Contains(t, res.renderLogsView(), "terminal logs")

	updated, _ = res.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'t'}})
	res = updated.(model)
	assert.False(t, res.logRawMode)
}

func TestRawLogsEmptyStateStillUsesTerminalView(t *testing.T) {
	m := model{
		hasConfig:  true,
		screen:     screenLogs,
		logRawMode: true,
		width:      120,
		height:     40,
	}

	out := m.renderLogsView()
	assert.Contains(t, out, "terminal logs")
	assert.Contains(t, out, "(no log lines yet)")
	assert.NotContains(t, out, "Open Run Node, choose Start")
}

func TestFullWidthRawLogsEscReturnsToRunNode(t *testing.T) {
	m := model{
		mode:       modeDashboard,
		hasConfig:  true,
		screen:     screenLogs,
		logRawMode: true,
		width:      120,
		height:     30,
		items: []menuItem{
			{label: "Run Node", value: "process"},
			{label: "Logs", value: "logs"},
		},
	}

	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	res := updated.(model)

	assert.Nil(t, cmd)
	assert.Equal(t, screenProcess, res.screen)
	assert.True(t, res.runFocused)
	assert.False(t, res.logFocused)
	assert.Equal(t, 0, res.cursor)
}

func TestFocusedSplitLogsRedactSecrets(t *testing.T) {
	secretURL := "https://rpc.example.invalid/?api-key=test-key-00000000-0000-4000-8000-000000000000"
	m := model{
		hasConfig:    true,
		screen:       screenLogs,
		logFocused:   true,
		logPane:      logPaneMithril,
		cfg:          &configData{lbEnabled: true},
		width:        180,
		height:       40,
		mithrilLines: []string{"INFO preferred rpc " + secretURL},
		lbLines:      []string{"INFO repair rpc " + secretURL},
	}

	out := m.renderLogsView()
	assert.NotContains(t, out, "test-key-00000000")
	assert.Contains(t, out, "api-key=REDACTED")

	m.logPane = logPaneLightbringer
	out = m.renderLogsView()
	assert.NotContains(t, out, "test-key-00000000")
	assert.Contains(t, out, "api-key=REDACTED")
}

func TestLogsStopShortcutOpensConfirmWhileFocused(t *testing.T) {
	m := runningModel()
	m.screen = screenLogs
	m.logFocused = true
	m.logRawMode = true

	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	res := updated.(model)

	assert.Nil(t, cmd, "stop from logs must still wait for confirmation")
	assert.True(t, res.confirmActive)
	assert.Contains(t, res.confirmTitle, "Stop")
	assert.NotNil(t, res.confirmOnYes)
	assert.Empty(t, res.proc.inFlightOp, "no signal until the confirmation is accepted")
}

func TestLogsStopShortcutConfirmSetsInFlight(t *testing.T) {
	m := runningModel()
	m.screen = screenLogs
	m.logFocused = true

	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	res := updated.(model)
	require.Nil(t, cmd)
	require.True(t, res.confirmActive)

	updated, cmd = res.Update(tea.KeyMsg{Type: tea.KeyEnter})
	res = updated.(model)
	assert.NotNil(t, cmd)
	assert.Equal(t, opStop, res.proc.inFlightOp)

	updated, cmd = res.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	res = updated.(model)
	assert.Nil(t, cmd)
	assert.False(t, res.confirmActive, "second stop must be blocked while stop is in flight")
}

func TestLogsHelpShowsStopShortcutWhenRunning(t *testing.T) {
	m := runningModel()
	m.screen = screenLogs
	m.logFocused = true

	var found bool
	for _, item := range m.helpItems() {
		if item.key == "x" && item.desc == "stop safely" {
			found = true
			break
		}
	}
	assert.True(t, found, "running logs view should expose the safe stop shortcut")
}

func TestStartFlowFinalEnterViaUpdateSetsInFlightOnReturnedModel(t *testing.T) {
	m := stoppedModel(procctl.StatusStopped)
	m.runFocused = true
	m.cfg = &configData{}

	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	res := updated.(model)
	require.Nil(t, cmd)
	require.True(t, res.startFlow.active)

	for i := 0; i < 5 && res.startFlow.active; i++ {
		updated, cmd = res.Update(tea.KeyMsg{Type: tea.KeyEnter})
		res = updated.(model)
	}
	assert.NotNil(t, cmd)
	assert.Equal(t, opStart, res.proc.inFlightOp)
	assert.False(t, res.startFlow.active)
}

func TestRunNodeEnterActivatesSelectedStartAction(t *testing.T) {
	m := stoppedModel(procctl.StatusStopped)
	m.runFocused = true
	m.cfg = &configData{lbEnabled: true}

	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	res := updated.(model)
	assert.Nil(t, cmd, "start opens the guided flow before spawning")
	assert.True(t, res.startFlow.active)
	assert.False(t, res.confirmActive)
	assert.Contains(t, res.renderStartFlow(), "Mithril + Lightbringer")
}

func TestRunNodeActionNavigationOpensDoctor(t *testing.T) {
	m := stoppedModel(procctl.StatusStopped)
	m.runFocused = true

	updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyDown})
	afterDown := updated.(model)
	assert.Equal(t, 1, afterDown.runActionIdx, "down should move from Start to Check readiness")

	updated, cmd := afterDown.Update(tea.KeyMsg{Type: tea.KeyEnter})
	res := updated.(model)
	assert.NotNil(t, cmd, "Doctor navigation refreshes dashboard data")
	assert.Equal(t, screenDoctor, res.screen)
	assert.False(t, res.runFocused)
}

func TestRunNodeShortcutKeysRequireActionFocus(t *testing.T) {
	m := stoppedModel(procctl.StatusStopped)
	m.runFocused = false

	updated, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	res := updated.(model)
	assert.Nil(t, cmd)
	assert.Empty(t, res.proc.inFlightOp)
	assert.False(t, res.confirmActive)

	m = stoppedModel(procctl.StatusRunning)
	m.runFocused = false
	updated, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	res = updated.(model)
	assert.False(t, res.confirmActive, "r should refresh with menu focus, not open Restart")
	assert.Empty(t, res.proc.inFlightOp)
}

// Timeout shows a plain "Timed out" line, not a raw error trace.
func TestActionResultMsg_TimeoutSurfacesPlainMessage(t *testing.T) {
	m := runningModel()
	m.proc.inFlightOp = opStop

	updated, _ := m.Update(actionResultMsg{
		op:     opStop,
		result: "timeout",
		err:    procctl.ErrStopTimeout.Error(),
	})
	res := updated.(model)
	assert.Empty(t, res.proc.inFlightOp)
	last := res.proc.progressLines[len(res.proc.progressLines)-1]
	assert.True(t, strings.Contains(last, "Timed out") || strings.Contains(last, "timeout"),
		"last progress line should describe the timeout in plain English: %q", last)
}

// startFailStderr is populated when a Start fails fast.
func TestActionResultMsg_FailedCapturesStderrForStart(t *testing.T) {
	m := runningModel()
	m.proc.inFlightOp = opStart

	updated, _ := m.Update(actionResultMsg{
		op:     opStart,
		result: "failed",
		err:    "exec failed",
		stderr: "E_CONFIG_MISSING: file not found at /etc/mithril.toml",
	})
	res := updated.(model)
	assert.Contains(t, res.proc.startFailStderr, "E_CONFIG_MISSING",
		"failure stderr must be preserved for the Process view to surface")
}

func TestActionResultMsg_FailedStartRemembersSpawnLogsForLogsView(t *testing.T) {
	m := runningModel()
	m.proc.inFlightOp = opStart

	stderrF, err := os.CreateTemp("", "mithril-dashboard-spawn-stderr-*.log")
	require.NoError(t, err)
	defer os.Remove(stderrF.Name())
	defer stderrF.Close()

	updated, _ := m.Update(actionResultMsg{
		op:     opStart,
		result: "failed",
		err:    "child exited during startup",
		stderr: "panic: replay divergence",
		spawnLogs: dashboardSpawnLogs{
			stderrPath: stderrF.Name(),
		},
	})
	res := updated.(model)

	assert.Equal(t, stderrF.Name(), res.lastSpawnLogs.stderrPath)
	assert.Contains(t, strings.Join(res.mithrilLines, "\n"), "panic: replay divergence")
}

// "Start failed" must not render with a stale Running badge.
func TestActionResultMsg_FailedStartAppliesFinalDetection(t *testing.T) {
	m := runningModel()
	m.proc.inFlightOp = opStart

	updated, _ := m.Update(actionResultMsg{
		op:     opStart,
		result: "failed",
		err:    "child exited during startup",
		stderr: "mode=accountsdb requires existing AccountsDB",
		det:    &procctl.Detection{Status: procctl.StatusStopped},
	})
	res := updated.(model)

	require.NotNil(t, res.proc.detection)
	assert.Equal(t, procctl.StatusStopped, res.proc.detection.Status)
	out := res.renderProcessView()
	assert.Contains(t, out, "Start failed")
	assert.Contains(t, out, "Stopped")
	assert.NotContains(t, out, "Running")
}

// Text fields own printable keys (r/s/e/f/x), not global shortcuts.
func TestTextEditOwnsPrintableShortcutKeys(t *testing.T) {
	m := model{screen: screenEdit, editMode: editText}
	for _, r := range "private/lightbringer-rpc" {
		updated, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = updated.(model)
	}
	assert.Equal(t, "private/lightbringer-rpc", m.editValue)
}

// Bubble Tea can deliver multiple runes (paste) in one KeyMsg.
func TestTextEditAcceptsPastedRunes(t *testing.T) {
	m := model{screen: screenEdit, editMode: editText}
	updated, _ := m.Update(tea.KeyMsg{
		Type:  tea.KeyRunes,
		Runes: []rune("127.0.0.1:3001"),
	})
	res := updated.(model)
	assert.Equal(t, "127.0.0.1:3001", res.editValue)
	assert.Equal(t, len("127.0.0.1:3001"), res.editCursor)
}

// dummyStyle returns a no-op style; only content assertions matter in tests.
func dummyStyle() lipgloss.Style {
	return lipgloss.NewStyle()
}
