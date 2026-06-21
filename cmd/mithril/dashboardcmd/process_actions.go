// Start/Stop/Restart/Force-Stop wiring for the Run Node view (UI state,
// confirmations, startup watching; signal delivery lives in pkg/procctl).

package dashboardcmd

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/procctl"
	"github.com/Overclock-Validator/mithril/pkg/state"
	tea "github.com/charmbracelet/bubbletea"
)

const (
	opStart     = "starting"
	opStop      = "stopping"
	opRestart   = "restarting"
	opForceStop = "force_stopping"
)

// startupWatchWindow bounds how long after spawning a failure counts as
// "start failed" (vs a later crash) while we tail child stderr.
const startupWatchWindow = 30 * time.Second

// startupSettleWindow is how long the child must outlive its PID file before
// Start counts as successful (surfaces early config/bootstrap failures).
const startupSettleWindow = 5 * time.Second

// dashboardSpawnLogMaxAge bounds accumulation of dashboard-owned child log
// files; old ones are cleaned on next start.
const dashboardSpawnLogMaxAge = 24 * time.Hour

// actionResultMsg is the terminal message for an in-flight action. result is
// "ok"/"failed"/"timeout"/"refused"; err is set when not "ok".
type actionResultMsg struct {
	op        string // matches procState.inFlightOp
	result    string
	err       string
	stderr    string // captured child stderr for Start failures
	spawnLogs dashboardSpawnLogs
	det       *procctl.Detection
}

// handleStartKey starts mithril if Stopped or Crashed. Runs preflight checks
// first; on refusal it stores the message in preflightErr and does not start.
func (m *model) handleStartKey() tea.Cmd {
	if !m.canAct() {
		return nil
	}
	if m.proc.detection == nil {
		return nil
	}
	st := m.proc.detection.Status
	if st != procctl.StatusStopped && st != procctl.StatusCrashed {
		return nil // nothing to start; mithril already running
	}
	if reason := preflightCheck(m.cfg, m.procAccountsDir()); reason != "" {
		m.proc.preflightErr = reason
		m.proc.preflightInfo = ""
		return nil
	}
	m.proc.preflightErr = "" // clear any prior refusal
	m.proc.preflightInfo = ""
	start := func(current *model) tea.Cmd {
		return current.beginAction(opStart, spawnMithrilCmd(current.configFile, current.procAccountsDir()))
	}
	m.beginStartFlow(start)
	return nil
}

// handleStopKey confirms before sending SIGTERM. No-op if not running.
func (m *model) handleStopKey() tea.Cmd {
	if !m.canAct() {
		return nil
	}
	return m.openStopConfirmation()
}

// handleStopFromLogsKey is the same shutdown path from the log screen; it
// allows log-scroll focus since the confirmation modal still gates the signal.
func (m *model) handleStopFromLogsKey() tea.Cmd {
	if !m.canActAllowingLogFocus() {
		return nil
	}
	return m.openStopConfirmation()
}

func (m *model) openStopConfirmation() tea.Cmd {
	if m.proc.detection == nil || m.proc.detection.Status != procctl.StatusRunning {
		return nil
	}
	if reason := checkSystemd(); reason != "" {
		m.proc.preflightErr = reason
		return nil
	}
	m.proc.preflightErr = ""
	m.confirmActive = true
	m.rightScroll = 0
	m.confirmTitle = "Stop Mithril?"
	m.confirmBody = "This sends SIGTERM and waits for clean exit. Up to 60 seconds during normal operation; longer during AccountsDB rebuild."
	m.confirmOnYes = func(current *model) tea.Cmd {
		return current.beginAction(opStop, stopMithrilCmd(current.procAccountsDir()))
	}
	return nil
}

func (m *model) openSafeFoldersConfirmation() tea.Cmd {
	m.proc.configFixErr = "" // clear any prior failure so a retry starts clean
	if m.cfg == nil {
		m.proc.preflightErr = "Config is still loading. Wait a moment, then try again."
		m.proc.preflightInfo = ""
		return nil
	}
	paths, err := safeStoragePathsForConfig(m.cfg)
	if err != nil {
		m.proc.preflightErr = "Could not choose safe folders.\n\nError: " + err.Error()
		m.proc.preflightInfo = ""
		return nil
	}
	m.confirmActive = true
	m.rightScroll = 0
	m.confirmTitle = "Use safe folders?"
	m.confirmBody = strings.Join([]string{
		"This updates the config to user-owned folders and keeps old data untouched.",
		"",
		"New data root:",
		"    " + paths.root,
		"",
		"Mithril will build fresh local data on the next Start.",
		"Your old data folders are not deleted.",
	}, "\n")
	m.confirmOnYes = func(current *model) tea.Cmd {
		if current.cfg == nil {
			return func() tea.Msg { return configFixResultMsg{err: "config is still loading"} }
		}
		cfg := *current.cfg
		return applySafeStoragePathsCmd(current.configFile, &cfg)
	}
	return nil
}

// openRebuildInPlaceConfirmation rebuilds from snapshot into the EXISTING paths,
// deleting the current AccountsDB — the destructive fallback when no spare disk fits.
func (m *model) openRebuildInPlaceConfirmation() tea.Cmd {
	m.proc.configFixErr = "" // clear any prior failure so a retry starts clean
	if m.cfg == nil {
		m.proc.preflightErr = "Config is still loading. Wait a moment, then try again."
		m.proc.preflightInfo = ""
		return nil
	}
	acc := strings.TrimSpace(m.cfg.accountsPath)
	if acc == "" {
		m.proc.preflightErr = "No accounts path is configured, so there is nothing to rebuild in place. Use Review config to set storage paths."
		m.proc.preflightInfo = ""
		return nil
	}

	lines := []string{
		"This rebuilds from snapshot using your current storage paths.",
		"",
		"Rebuild on:",
		"    " + acc,
	}
	if config.HasExistingAccountsDb(acc) {
		reclaim := config.ReclaimableDirBytes(acc) / (1 << 30)
		lines = append(lines, "", fmt.Sprintf("The current AccountsDB (~%d GB) is DELETED first, then rebuilt.", reclaim))
	} else {
		lines = append(lines, "", "Any existing AccountsDB here is deleted first, then rebuilt.")
	}

	// Reclaim-aware disk readiness — the same check the build runs.
	check := checkBuildSpaceFn(m.cfg.cluster, acc, m.cfg.snapshotsPath)
	switch {
	case !check.Determined:
		lines = append(lines, "", "Free space could not be read here; the build verifies before downloading.")
	case check.OK:
		lines = append(lines, "", fmt.Sprintf("Disk: ~%d GB usable after reclaiming, ~%d GB needed — enough.", check.UsableGB, check.NeedGB))
	default:
		lines = append(lines, "", "⚠ "+check.Reason)
	}

	m.confirmActive = true
	m.rightScroll = 0
	m.confirmTitle = "Rebuild in place — deletes current data"
	m.confirmBody = strings.Join(lines, "\n")
	m.confirmOnYes = func(current *model) tea.Cmd {
		if current.cfg == nil {
			return func() tea.Msg { return configFixResultMsg{err: "config is still loading"} }
		}
		cfg := *current.cfg
		return applyRebuildInPlaceCmd(current.configFile, &cfg)
	}
	return nil
}

func applyRebuildInPlaceCmd(configFile string, cfg *configData) tea.Cmd {
	return func() tea.Msg {
		summary, err := applyRebuildInPlace(configFile, cfg)
		if err != nil {
			return configFixResultMsg{err: err.Error()}
		}
		return configFixResultMsg{summary: summary}
	}
}

// applyRebuildInPlace sets bootstrap.mode=snapshot (deletion happens on next Start).
// Refuses if the disk can't fit even after reclaiming the old DB.
func applyRebuildInPlace(configFile string, cfg *configData) (string, error) {
	if cfg == nil {
		return "", fmt.Errorf("config is still loading")
	}
	acc := strings.TrimSpace(cfg.accountsPath)
	if acc == "" {
		return "", fmt.Errorf("no accounts path configured")
	}
	if check := checkBuildSpaceFn(cfg.cluster, acc, cfg.snapshotsPath); check.Determined && !check.OK {
		return "", fmt.Errorf("%s", check.Reason)
	}
	if err := saveConfigValue(configFile, "bootstrap", "mode", "snapshot"); err != nil {
		return "", fmt.Errorf("save bootstrap.mode: %w", err)
	}
	return fmt.Sprintf("Set to rebuild in place on %s. Press Start to rebuild — this replaces the current AccountsDB.", acc), nil
}

// handleForceStopKey is valid only when proc.stuck is set. SIGKILL risks
// AccountsDB corruption, so it requires explicit confirmation.
func (m *model) handleForceStopKey() tea.Cmd {
	if !m.canAct() {
		return nil
	}
	if !m.proc.stuck {
		return nil
	}
	if m.proc.detection == nil || m.proc.detection.Status != procctl.StatusRunning {
		return nil
	}
	if reason := checkSystemd(); reason != "" {
		m.proc.preflightErr = reason
		return nil
	}
	m.proc.preflightErr = ""
	m.confirmActive = true
	m.rightScroll = 0
	m.confirmTitle = "Force Stop — Data Loss Risk"
	m.confirmBody = strings.Join([]string{
		"This sends SIGKILL, which bypasses every shutdown safeguard.",
		"AccountsDB may be left in an inconsistent state and require a rebuild from snapshot.",
		"",
		"Only proceed if SIGTERM has already been stuck for several minutes",
		"AND the node is NOT currently rebuilding AccountsDB (which is normal and slow).",
	}, "\n")
	m.confirmOnYes = func(current *model) tea.Cmd {
		return current.beginAction(opForceStop, forceKillMithrilCmd())
	}
	return nil
}

// handleRestartKey confirms, then chains Stop → Start.
func (m *model) handleRestartKey() tea.Cmd {
	if !m.canAct() {
		return nil
	}
	if m.proc.detection == nil || m.proc.detection.Status != procctl.StatusRunning {
		return nil
	}
	if reason := checkSystemd(); reason != "" {
		m.proc.preflightErr = reason
		return nil
	}
	m.proc.preflightErr = ""
	m.confirmActive = true
	m.rightScroll = 0
	m.confirmTitle = "Restart Mithril?"
	m.confirmBody = "Sends SIGTERM, waits for clean exit, then starts a new mithril process."
	m.confirmOnYes = func(current *model) tea.Cmd {
		return current.beginAction(opRestart, restartMithrilCmd(current.configFile, current.procAccountsDir()))
	}
	return nil
}

// canAct reports whether a new action can start: not editing config text, not
// log-focused, none in flight.
func (m *model) canAct() bool {
	if m.proc.inFlightOp != "" {
		return false
	}
	if m.editMode == editText {
		return false
	}
	if m.logFocused {
		return false
	}
	return true
}

func (m *model) canActAllowingLogFocus() bool {
	if m.proc.inFlightOp != "" {
		return false
	}
	if m.editMode == editText {
		return false
	}
	return true
}

// beginAction marks the op in-flight, clears prior error/progress, and returns
// the cmd that runs it.
func (m *model) beginAction(op string, cmd tea.Cmd) tea.Cmd {
	m.proc.inFlightOp = op
	m.proc.opStartedAt = time.Now()
	m.proc.opErr = ""
	m.proc.startFailStderr = ""
	m.proc.progressLines = m.proc.progressLines[:0]
	return cmd
}

// spawnMithrilCmd forks `mithril run` detached, then polls the outcome:
// PID file + survives settle = ok; early exit or no PID file = failed.
func spawnMithrilCmd(configPath, accountsDir string) tea.Cmd {
	return func() tea.Msg {
		// Re-exec our own binary in `run` mode.
		exe, err := os.Executable()
		if err != nil {
			return actionResultMsg{op: opStart, result: "failed",
				err: fmt.Sprintf("cannot resolve own binary path: %v", err),
				det: detectProcessForAction(accountsDir)}
		}

		args := []string{"run"}
		if configPath != "" {
			args = append(args, "--config", configPath)
		}
		cmd := exec.Command(exe, args...)
		cmd.Env = append(os.Environ(), procctl.SpawnedByEnv+"=dashboard")
		// Setsid detaches the child so it survives dashboard exit; it acquires
		// its own flock via procctl.AcquireForRun.
		cmd.SysProcAttr = spawnSysProcAttr()

		// Redirect child stdout/stderr to per-run temp files + a stderr ring
		// (for fast Start-failed surfacing before mithril's mlog inits).
		errBuf := newRingBuffer(4096)
		cleanupOldDashboardSpawnTempLogs(dashboardSpawnLogMaxAge)
		stdoutF, stdoutPath, _ := createPrivateTempLogFile("mithril-dashboard-spawn-stdout-*.log")
		stderrF, stderrPath, _ := createPrivateTempLogFile("mithril-dashboard-spawn-stderr-*.log")
		if stdoutF != nil {
			cmd.Stdout = stdoutF
		}
		if stderrF != nil {
			// Use the *os.File directly: a non-*os.File stderr makes os/exec open
			// a parent-side pipe that breaks when the dashboard exits.
			cmd.Stderr = stderrF
		} else {
			cmd.Stderr = errBuf
		}

		if err := cmd.Start(); err != nil {
			if stdoutF != nil {
				_ = stdoutF.Close()
				_ = os.Remove(stdoutF.Name())
			}
			if stderrF != nil {
				_ = stderrF.Close()
				_ = os.Remove(stderrF.Name())
			}
			return actionResultMsg{op: opStart, result: "failed",
				err: fmt.Sprintf("exec.Start: %v", err),
				det: detectProcessForAction(accountsDir)}
		}
		childPid := cmd.Process.Pid

		// Reap in a goroutine so the child doesn't become a zombie; we don't
		// wait on it here — it outlives us by design.
		go func() {
			_ = cmd.Wait()
			if stdoutF != nil {
				_ = stdoutF.Close()
			}
			if stderrF != nil {
				_ = stderrF.Close()
			}
		}()

		// Poll for the outcome within startupWatchWindow.
		deadline := time.Now().Add(startupWatchWindow)
		var pidFileSeenAt time.Time
		var childRunID string
		for time.Now().Before(deadline) {
			time.Sleep(500 * time.Millisecond)

			// Probe liveness via the os.Process handle, not the bare PID (a
			// recycled PID could signal an unrelated process).
			if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
				if det, ok := cleanCompletionAfterStart(accountsDir, childRunID); ok {
					cleanupSpawnTempFiles(stdoutPath, stderrPath)
					return actionResultMsg{op: opStart, result: "ok", det: det}
				}
				stderr := errBuf.String()
				if stderrF != nil {
					stderr = readFileTail(stderrPath, 4096)
				}
				return actionResultMsg{op: opStart, result: "failed",
					err:    fmt.Sprintf("child mithril (pid %d) exited during startup", childPid),
					stderr: stderr,
					spawnLogs: dashboardSpawnLogs{
						stdoutPath: stdoutPath,
						stderrPath: stderrPath,
					},
					det: detectProcessForAction(accountsDir)}
			}

			// Did the PID file appear (child acquired its lock)?
			info, perr := procctl.ReadPidFile(procctl.DefaultPidFile())
			if perr == nil && info.Pid == childPid {
				if pidFileSeenAt.IsZero() {
					pidFileSeenAt = time.Now()
					childRunID = info.RunID
					_ = procctl.UpdatePidOutputPaths(procctl.DefaultPidFile(), childPid, stdoutPath, stderrPath)
					continue
				}
				if time.Since(pidFileSeenAt) >= startupSettleWindow {
					return actionResultMsg{op: opStart, result: "ok"}
				}
				continue
			}
			// Different PID in the file — fail only if Detect proves that process
			// is live and holds the lock; otherwise keep waiting (stale file).
			if perr == nil && info.Pid != childPid {
				if pid, ok := pidFileDescribesOtherLiveLockedProcess(
					procctl.DefaultPidFile(),
					procctl.DefaultLockFile(),
					accountsDir,
					childPid,
				); ok {
					stderr := errBuf.String()
					if stderrF != nil {
						stderr = readFileTail(stderrPath, 4096)
					}
					return actionResultMsg{op: opStart, result: "failed",
						err:    fmt.Sprintf("another mithril already holds the lock (pid %d)", pid),
						stderr: stderr,
						spawnLogs: dashboardSpawnLogs{
							stdoutPath: stdoutPath,
							stderrPath: stderrPath,
						},
						det: detectProcessForAction(accountsDir)}
				}
			}
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		stderr := errBuf.String()
		if stderrF != nil {
			stderr = readFileTail(stderrPath, 4096)
		}
		return actionResultMsg{op: opStart, result: "failed",
			err:    fmt.Sprintf("child mithril (pid %d) did not create pid file within %s; sent SIGTERM", childPid, startupWatchWindow),
			stderr: stderr,
			spawnLogs: dashboardSpawnLogs{
				stdoutPath: stdoutPath,
				stderrPath: stderrPath,
			},
			det: detectProcessForAction(accountsDir)}
	}
}

func cleanCompletionAfterStart(accountsDir, childRunID string) (*procctl.Detection, bool) {
	if accountsDir == "" || childRunID == "" {
		return nil, false
	}
	st, err := state.LoadState(accountsDir)
	if err != nil || st == nil || st.CurrentRunID != childRunID {
		return nil, false
	}
	clean, _ := state.WasCleanExit(st)
	if !clean {
		return nil, false
	}
	det := detectProcessForAction(accountsDir)
	return det, det != nil && det.Status == procctl.StatusStopped
}

func detectProcessForAction(accountsDir string) *procctl.Detection {
	det, err := procctl.Detect(procctl.DefaultPidFile(), procctl.DefaultLockFile(), accountsDir)
	if err != nil {
		return nil
	}
	return det
}

func readFileTail(path string, maxBytes int64) string {
	if path == "" || maxBytes <= 0 {
		return ""
	}
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return ""
	}
	offset := info.Size() - maxBytes
	if offset < 0 {
		offset = 0
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return ""
	}
	data, err := io.ReadAll(io.LimitReader(f, maxBytes))
	if err != nil {
		return ""
	}
	return string(data)
}

type safeStoragePaths struct {
	root       string
	accounts   string
	snapshots  string
	shredstore string
	logs       string
}

func applySafeStoragePathsCmd(configFile string, cfg *configData) tea.Cmd {
	return func() tea.Msg {
		summary, err := applySafeStoragePaths(configFile, cfg)
		if err != nil {
			return configFixResultMsg{err: err.Error()}
		}
		return configFixResultMsg{summary: summary}
	}
}

func applySafeStoragePaths(configFile string, cfg *configData) (string, error) {
	paths, err := safeStoragePathsForConfig(cfg)
	if err != nil {
		return "", err
	}
	// Refuse to repoint storage onto a disk too small for the AccountsDB;
	// otherwise the build fails mid-download with a cryptic "no space left".
	need := estimatedAccountsDbGB(cfg)
	if free, ok := diskFreeGBFn(paths.root); ok && free < need {
		return "", fmt.Errorf(
			"%s is on a disk with only ~%d GB free, but the %s AccountsDB needs ~%d GB. "+
				"Point storage at a larger disk first (Edit Config → storage paths), then use safe folders",
			paths.root, free, clusterLabel(cfg), need)
	}
	for _, dir := range []string{paths.root, paths.accounts, paths.snapshots, paths.shredstore, paths.logs} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return "", fmt.Errorf("create %s: %w", dir, err)
		}
	}
	updates := []struct {
		section string
		key     string
		value   string
	}{
		{"storage", "accounts", paths.accounts},
		{"storage", "snapshots", paths.snapshots},
		{"snapshot", "download_path", paths.snapshots},
		{"storage", "shredstore", paths.shredstore},
		{"storage", "logs", paths.logs},
		// Keep [log] dir in step with storage.logs (mlog writes there).
		{"log", "dir", paths.logs},
		{"bootstrap", "mode", "auto"},
	}
	for _, update := range updates {
		if err := saveConfigValue(configFile, update.section, update.key, update.value); err != nil {
			return "", fmt.Errorf("save %s.%s: %w", update.section, update.key, err)
		}
	}
	return fmt.Sprintf("Updated config to use %s. Old data was not deleted. Press Start to build fresh local data.", paths.root), nil
}

func safeStoragePathsForConfig(cfg *configData) (safeStoragePaths, error) {
	root, err := safeStorageRoot(cfg)
	if err != nil {
		return safeStoragePaths{}, err
	}
	paths := safeStoragePaths{
		root:       root,
		accounts:   filepath.Join(root, "accounts"),
		snapshots:  filepath.Join(root, "snapshots"),
		shredstore: filepath.Join(root, "shredstore"),
		logs:       filepath.Join(root, "logs"),
	}
	if reason := checkAccountsStateCompatible(cfg, paths.accounts); reason != "" {
		paths.accounts = filepath.Join(root, "accounts-"+time.Now().Format("20060102-150405"))
	}
	return paths, nil
}

func safeStorageRootForDisplay(cfg *configData) string {
	root, err := safeStorageRoot(cfg)
	if err != nil {
		return ""
	}
	return root
}

// safeStorageRoot picks a user-owned root for fresh data: a "mithril-data" dir
// beside existing storage, else $HOME — first writable one with room.
func safeStorageRoot(cfg *configData) (string, error) {
	cluster := "default"
	rawCluster := ""
	if cfg != nil && strings.TrimSpace(cfg.cluster) != "" {
		cluster = sanitizePathComponent(cfg.cluster)
		rawCluster = cfg.cluster
	}
	need := config.EstimatedBuildBytes(rawCluster) // accounts + snapshot share the safe-folders disk

	// Build candidate roots in preference order.
	var candidates []string
	add := func(dir string) {
		if d := strings.TrimSpace(dir); d != "" {
			candidates = append(candidates, filepath.Join(filepath.Dir(d), "mithril-data", cluster))
		}
	}
	if cfg != nil {
		add(cfg.snapshotsPath)  // big "ledger" disk on the standard layout
		add(cfg.shredstorePath) // usually the same big disk
		add(cfg.accountsPath)
		add(cfg.logsPath)
	}
	home, homeErr := os.UserHomeDir()
	if homeErr == nil && strings.TrimSpace(home) != "" {
		candidates = append(candidates, filepath.Join(home, "mithril-data", cluster))
	}

	seen := map[string]bool{}
	firstWritable := ""
	for _, c := range candidates {
		if seen[c] {
			continue
		}
		seen[c] = true
		if !config.WritableDir(filepath.Dir(c)) { // can we create the mithril-data dir here?
			continue
		}
		if firstWritable == "" {
			firstWritable = c
		}
		if free, ok := config.FreeDiskBytes(c); ok && free >= need {
			return c, nil // writable AND has room — ideal
		}
	}
	if firstWritable != "" {
		return firstWritable, nil // none has room; the apply-guard/UI will warn
	}
	if homeErr == nil && strings.TrimSpace(home) != "" {
		return filepath.Join(home, "mithril-data", cluster), nil
	}
	return "", fmt.Errorf("no writable storage location found")
}

// clusterLabel is a human-friendly cluster name for messages.
func clusterLabel(cfg *configData) string {
	if cfg != nil && strings.TrimSpace(cfg.cluster) != "" {
		return cfg.cluster
	}
	return "this cluster's"
}

// estimatedAccountsDbGB is the free space (GB) needed to build, by cluster —
// the full footprint (accounts + snapshots share the safe-folders disk).
func estimatedAccountsDbGB(cfg *configData) uint64 {
	cluster := ""
	if cfg != nil {
		cluster = cfg.cluster
	}
	return config.EstimatedBuildBytes(cluster) / (1 << 30)
}

// pathFreeGB reports free GB on the filesystem holding path (probing the
// nearest existing ancestor); ok is false when it can't be determined.
func pathFreeGB(path string) (uint64, bool) {
	du := getDiskUsageForPath("", path)
	if du == nil || du.total == 0 || du.used > du.total {
		return 0, false
	}
	return du.total - du.used, true
}

// diskFreeGBFn is the safe-folders free-space probe; a var so tests can stub it.
var diskFreeGBFn = pathFreeGB

// checkBuildSpaceFn is the reclaim-aware build-space verdict for rebuild-in-
// place; a var so tests can stub disk readiness.
var checkBuildSpaceFn = config.CheckBuildSpace

func sanitizePathComponent(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return "default"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	out := strings.Trim(b.String(), "-_")
	if out == "" {
		return "default"
	}
	return out
}

func createPrivateTempLogFile(pattern string) (*os.File, string, error) {
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return nil, "", err
	}
	if err := f.Chmod(0600); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, "", err
	}
	return f, f.Name(), nil
}

func cleanupSpawnTempFiles(paths ...string) {
	for _, path := range paths {
		if path != "" {
			_ = os.Remove(path)
		}
	}
}

func cleanupOldDashboardSpawnTempLogs(maxAge time.Duration) {
	if maxAge <= 0 {
		return
	}
	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-maxAge)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "mithril-dashboard-spawn-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.ModTime().After(cutoff) {
			continue
		}
		_ = os.Remove(filepath.Join(os.TempDir(), name))
	}
}

// stopMithrilCmd sends SIGTERM and waits for clean exit, returning the result.
// No progress streaming — the dashboard's 2s tick refreshes detection.
func stopMithrilCmd(accountsDir string) tea.Cmd {
	return func() tea.Msg {
		dashPid := os.Getpid()
		if err := procctl.SignalStop(procctl.DefaultPidFile(),
			procctl.DefaultAuditLog(), dashPid); err != nil {
			return actionResultMsg{op: opStop, result: "failed",
				err: fmt.Sprintf("signal stop: %v", err)}
		}
		det, err := procctl.WaitStopped(
			procctl.DefaultPidFile(),
			procctl.DefaultLockFile(),
			accountsDir,
			60*time.Second,
		)
		if err != nil {
			// Distinguish timeout from other errors so the UI can offer
			// a Force Stop action.
			result := "failed"
			if err == procctl.ErrStopTimeout {
				result = "timeout"
			}
			return actionResultMsg{op: opStop, result: result, err: err.Error()}
		}
		return actionResultMsg{op: opStop, result: "ok", det: det}
	}
}

// forceKillMithrilCmd is the confirmed SIGKILL escalation after a stuck stop.
// procctl.ForceKill re-verifies identity (PID-reuse guard) and audit-logs it.
func forceKillMithrilCmd() tea.Cmd {
	return func() tea.Msg {
		dashPid := os.Getpid()
		err := procctl.ForceKill(procctl.DefaultPidFile(),
			procctl.DefaultAuditLog(), dashPid)
		if err != nil {
			return actionResultMsg{op: opForceStop, result: "failed",
				err: fmt.Sprintf("force kill: %v", err)}
		}
		// Platform hook: Linux relies on Pdeathsig; macOS skips name-based
		// cleanup so it can't kill another operator's Lightbringer.
		cleanupOrphanLightbringer()
		return actionResultMsg{op: opForceStop, result: "ok"}
	}
}

// restartMithrilCmd chains stop → start in one goroutine, keeping inFlightOp at
// "restarting" throughout so the UI doesn't flash "Stopped" between phases.
func restartMithrilCmd(configPath, accountsDir string) tea.Cmd {
	return func() tea.Msg {
		// Stop phase.
		dashPid := os.Getpid()
		if err := procctl.SignalStop(procctl.DefaultPidFile(),
			procctl.DefaultAuditLog(), dashPid); err != nil {
			return actionResultMsg{op: opRestart, result: "failed",
				err: fmt.Sprintf("restart: stop phase: %v", err)}
		}
		_, err := procctl.WaitStopped(
			procctl.DefaultPidFile(),
			procctl.DefaultLockFile(),
			accountsDir,
			60*time.Second,
		)
		if err != nil {
			result := "failed"
			if err == procctl.ErrStopTimeout {
				result = "timeout"
			}
			return actionResultMsg{op: opRestart, result: result,
				err: fmt.Sprintf("restart: stop phase: %v", err)}
		}

		// Start phase.
		spawn := spawnMithrilCmd(configPath, accountsDir)
		msg := spawn()
		// Relabel the result op so the dashboard clears the right inFlightOp.
		if res, ok := msg.(actionResultMsg); ok {
			res.op = opRestart
			return res
		}
		return msg
	}
}

func pidFileDescribesOtherLiveLockedProcess(pidPath, lockPath, accountsDir string, childPid int) (int, bool) {
	info, err := procctl.ReadPidFile(pidPath)
	if err != nil || info == nil || info.Pid == childPid {
		return 0, false
	}
	det, err := procctl.Detect(pidPath, lockPath, accountsDir)
	if err != nil || det == nil {
		return 0, false
	}
	return det.Pid, det.Status == procctl.StatusRunning && det.Pid == info.Pid && det.LockHeld
}
