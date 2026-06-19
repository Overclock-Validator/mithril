package dashboardcmd

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Overclock-Validator/mithril/cmd/mithril/setupcmd"
	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/procctl"
	"github.com/Overclock-Validator/mithril/pkg/tui"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
)

var configFile string

var DashboardCmd = cobra.Command{
	Use:   "dashboard",
	Short: "Interactive node management dashboard",
	Long:  "Opens a full-screen TUI dashboard for monitoring and managing the Mithril node.",
	Run: func(cmd *cobra.Command, args []string) {
		runDashboard()
	},
}

func init() {
	DashboardCmd.Flags().StringVarP(&configFile, "config", "c", "config.toml", "Path to config file")
}

// ── Screen constants ────────────────────────────────────────────────────

const (
	screenOverview = iota
	screenProcess  // guided run/stop flow backed by procctl process controls
	screenConfig
	screenEdit // inline config editing
	screenDoctor
	screenLogs
	screenDisk
)

// Edit modes for inline config editing
const (
	editNone = iota // not editing
	editMenu        // selecting from fixed options
	editText        // typing free-form text
)

// Log pane selection
const (
	logPaneMithril      = 0
	logPaneLightbringer = 1
)

type editOption struct {
	label string
	value string
	desc  string
}

// editFieldDef defines a single editable config field.
type editFieldDef struct {
	section string // TOML section name
	key     string // TOML key within section
	label   string // display label
	isSep   bool   // visual separator
}

// ── Async data messages ─────────────────────────────────────────────────

type dataRefreshedMsg struct {
	hasConfig    bool
	cfg          *configData
	state        *nodeState
	services     []serviceStatus
	checks       []checkResult
	mithrilLines []string
	lbLines      []string
	progress     []progressEvent
	snapshot     snapshotActivity
	accounts     accountsActivity
	preflightErr string
}

type diskRefreshedMsg struct {
	disks []diskUsage
}

// procDetectedMsg carries a procctl.Detect result. err set = render error;
// det nil and err empty = not yet fetched.
type procDetectedMsg struct {
	det *procctl.Detection
	err string
}

type configFixResultMsg struct {
	summary string
	err     string
}

func fetchDataCmd(cfgFile string, spawnLogs dashboardSpawnLogs) tea.Cmd {
	return func() tea.Msg {
		var cfg *configData
		var state *nodeState
		hasConfig := false

		if _, err := os.Stat(cfgFile); err == nil {
			hasConfig = true
			cfg = readConfig(cfgFile)
		}

		if cfg != nil && cfg.accountsPath != "" {
			state = readState(cfg.accountsPath)
		}

		services := probeServices(cfg)
		checks := runDoctorChecks(cfgFile, cfg)

		var mithrilLines, lbLines []string
		var progressEvents []progressEvent
		var snapshot snapshotActivity
		var accounts accountsActivity
		var preflightErr string
		if cfg != nil {
			mithrilLines = mithrilLogLines(cfg.logsPath, 50, spawnLogs)
			lbLines = lightbringerLogLines(cfg, 50)
			progressEvents = readProgressEvents(cfg.logsPath, 12)
			snapshot = readSnapshotActivity(cfg.snapshotsPath)
			accounts = readAccountsActivity(cfg.accountsPath)
			preflightErr = preflightCheck(cfg, cfg.accountsPath)
		}

		return dataRefreshedMsg{
			hasConfig:    hasConfig,
			cfg:          cfg,
			state:        state,
			services:     services,
			checks:       checks,
			mithrilLines: mithrilLines,
			lbLines:      lbLines,
			progress:     progressEvents,
			snapshot:     snapshot,
			accounts:     accounts,
			preflightErr: preflightErr,
		}
	}
}

func (m model) fetchDataCmd() tea.Cmd {
	return fetchDataCmd(m.configFile, m.lastSpawnLogs)
}

func fetchDiskCmd(cfg *configData) tea.Cmd {
	return func() tea.Msg {
		return diskRefreshedMsg{disks: getDiskUsage(cfg)}
	}
}

// fetchProcessCmd runs procctl.Detect and returns a procDetectedMsg.
// Serialize via proc.inflight so a tick racing a refresh can't stack calls.
func fetchProcessCmd(accountsDir string) tea.Cmd {
	return func() tea.Msg {
		det, err := procctl.Detect(procctl.DefaultPidFile(), procctl.DefaultLockFile(), accountsDir)
		if err != nil {
			return procDetectedMsg{err: err.Error()}
		}
		return procDetectedMsg{det: det}
	}
}

// triggerProcessFetch fetches process state, or returns nil if a fetch is
// already in flight. Sets the inflight gate, so needs a pointer receiver.
func (m *model) triggerProcessFetch() tea.Cmd {
	if m.proc.inflight {
		return nil
	}
	m.proc.inflight = true
	return fetchProcessCmd(m.procAccountsDir())
}

func tickCmd() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func slowTickCmd() tea.Cmd {
	return tea.Tick(10*time.Second, func(t time.Time) tea.Msg { return slowTickMsg(t) })
}

type tickMsg time.Time
type slowTickMsg time.Time
type childExitMsg struct{} // sent when embedded child TUI wants to quit

// ── Model ───────────────────────────────────────────────────────────────

// Dashboard mode: normal dashboard or embedded sub-TUI (setup)
const (
	modeDashboard = iota
	modeSetup
)

type model struct {
	width      int
	height     int
	cursor     int
	screen     int
	hasConfig  bool
	configFile string

	// Embedded sub-TUI (for setup)
	mode   int
	childM tea.Model

	// Inline config editing state
	editFields    []editFieldDef // all editable fields
	editIdx       int            // which field is selected
	editMode      int            // editNone, editMenu, or editText
	editOptions   []editOption   // options for menu-based editing
	editOptCursor int            // cursor in options menu
	editValue     string         // current input value
	editCursor    int            // cursor position in input
	editErr       string         // validation error for current edit

	// Right pane scroll
	rightScroll int

	// Data
	cfg           *configData
	state         *nodeState
	services      []serviceStatus
	disks         []diskUsage
	checks        []checkResult
	mithrilLines  []string
	lbLines       []string
	progress      []progressEvent
	snapshot      snapshotActivity
	accounts      accountsActivity
	logScroll     int  // scroll offset for focused log pane
	logFocused    bool // true when user is scrolling logs with ↑↓
	logPane       int  // 0=mithril (left), 1=lightbringer (right)
	logRawMode    bool // true renders a full-width terminal log tail
	lastSpawnLogs dashboardSpawnLogs
	disksLoaded   bool // true after first disk fetch completes
	runFocused    bool // true when the Run Node action list owns arrow/enter
	runActionIdx  int  // selected action in the Run Node right pane
	startFlow     startFlowState

	// proc holds process state plus in-flight Start/Stop/Restart action state.
	proc procState

	// Confirmation modal over the right pane. onYes runs on confirm.
	confirmActive bool
	confirmTitle  string
	confirmBody   string
	confirmOnYes  func(*model) tea.Cmd

	// Menu
	items []menuItem
}

// procState groups everything the dashboard knows about the mithril process.
type procState struct {
	// Last Detect result; preserved across error refreshes so the badge
	// doesn't flicker on a transient PID-file read failure.
	detection *procctl.Detection

	// fetchedAt: last detect (ok or err). lastOkAt: last successful detect —
	// the split lets the error view show "last good check N seconds ago".
	fetchedAt time.Time
	lastOkAt  time.Time

	// Last detect error, or empty. String, not error, since it crosses goroutines.
	fetchErr string

	// True while a fetchProcessCmd is running, to gate parallel Detect calls.
	inflight bool

	// Active user action: "", "starting", "stopping", or "restarting". Set on
	// confirm, cleared by actionResultMsg. A second action press while set is rejected.
	inFlightOp  string
	opStartedAt time.Time
	opErr       string // last action error (or empty)

	// Ring buffer of status lines for the active action, capped at 8.
	progressLines []string

	// Tail of the spawned mithril's stderr when Start fails early; shown verbatim.
	startFailStderr string

	// Set when a Stop times out; surfaces [f] Force Stop. Cleared on next non-running detect.
	stuck bool

	// preflightErr: Start blocked before spawn (supervisor conflict, unsafe lock,
	// ownership, unwritable logs, incompatible AccountsDB). preflightInfo: fix success msg.
	preflightErr  string
	preflightInfo string

	// Result of a failed guided fix. Separate field because preflightErr is
	// overwritten each data tick, which would wipe it before the user sees it.
	configFixErr string
}

func newModel(cf string) model {
	return model{
		configFile: cf,
		screen:     screenOverview,
		items: []menuItem{
			{label: "Overview", value: "overview"},
			{label: "Run Node", value: "process"},
			{label: "Config", value: "config"},
			{label: "Edit Config", value: "edit"},
			{label: "Doctor", value: "doctor"},
			{label: "Logs", value: "logs"},
			{label: "Disk", value: "disk"},
			{isSep: true},
			{label: "Create Config", value: "setup"},
		},
		editFields: []editFieldDef{
			{section: "network", key: "cluster", label: "Cluster"},
			{section: "network", key: "rpc", label: "RPC Endpoint"},
			{isSep: true},
			{section: "storage", key: "accounts", label: "AccountsDB Path"},
			{section: "storage", key: "snapshots", label: "Snapshots Path"},
			{section: "storage", key: "shredstore", label: "Shredstore Path"},
			{section: "storage", key: "logs", label: "Logs Path"},
			{isSep: true},
			{section: "block", key: "source", label: "Block Source"},
			{section: "block", key: "turbine_bind_addr", label: "Turbine UDP"},
			{section: "turbine", key: "gossip_entrypoint", label: "Turbine Gossip"},
			{section: "turbine", key: "gossip_bind_addr", label: "Gossip UDP"},
			{section: "turbine", key: "advertised_ip", label: "Advertised IP"},
			{section: "turbine", key: "shred_version", label: "Shred Version"},
			{section: "block", key: "lightbringer_endpoint", label: "External LB Endpoint"},
			{section: "block", key: "max_rps", label: "Block Max RPS"},
			{section: "block", key: "max_inflight", label: "Block Max Inflight"},
			{isSep: true},
			{section: "lightbringer", key: "enabled", label: "Lightbringer"},
			{section: "lightbringer", key: "binary_path", label: "LB Binary Path"},
			{section: "lightbringer", key: "config_dir", label: "LB Config Dir"},
			{section: "lightbringer", key: "gossip_entrypoint", label: "Gossip Entrypoint"},
			{section: "lightbringer", key: "gossip_port", label: "LB Gossip UDP Port"},
			{section: "lightbringer", key: "port_range_start", label: "LB UDP Range Start"},
			{section: "lightbringer", key: "port_range_end", label: "LB UDP Range End"},
			{section: "lightbringer", key: "grpc_addr", label: "LB gRPC Address"},
			{section: "lightbringer", key: "rpc_addr", label: "LB HTTP Address"},
			{section: "lightbringer", key: "quiet", label: "LB Quiet Logs"},
			{isSep: true},
			{section: "tuning", key: "txpar", label: "TX Parallelism"},
			{section: "rpc", key: "port", label: "RPC Port"},
			{isSep: true},
			{section: "log", key: "level", label: "Log Level"},
			{section: "bootstrap", key: "mode", label: "Bootstrap Mode"},
		},
	}
}

func (m model) Init() tea.Cmd {
	// No process detect here: cfg isn't loaded, so an empty accountsDir would
	// flicker Stopped->Crashed. First tick handles it once cfg is set.
	return tea.Batch(
		m.fetchDataCmd(),
		tickCmd(),
		slowTickCmd(),
	)
}

// procAccountsDir returns the AccountsDB path, or "" if no config yet.
// Detect uses it to classify Crashed vs Stopped; "" skips that.
func (m model) procAccountsDir() string {
	if m.cfg == nil {
		return ""
	}
	return m.cfg.accountsPath
}

func (m *model) rememberSpawnLogs(det *procctl.Detection) {
	if det == nil || (det.StdoutPath == "" && det.StderrPath == "") {
		return
	}
	m.lastSpawnLogs = dashboardSpawnLogs{
		stdoutPath: det.StdoutPath,
		stderrPath: det.StderrPath,
	}
}

// ── Update ──────────────────────────────────────────────────────────────

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	// If a child TUI is active, delegate all messages to it
	if m.mode != modeDashboard && m.childM != nil {
		// Intercept ctrl+c and esc on first screen — return to dashboard
		if keyMsg, ok := msg.(tea.KeyMsg); ok {
			if keyMsg.String() == "ctrl+c" {
				m.mode = modeDashboard
				m.childM = nil
				return m, nil
			}
			// Esc on the setup TUI's first screen (mode selection) exits back to dashboard
			if keyMsg.String() == "esc" && setupcmd.SetupIsFirstScreen(m.childM) {
				m.mode = modeDashboard
				m.childM = nil
				return m, nil
			}
		}

		// Check if child is done BEFORE updating (the done screen's q/enter sends tea.Quit)
		isDone := false
		if m.mode == modeSetup {
			isDone = setupcmd.SetupIsDone(m.childM)
		}

		// If child is on done screen and user presses q/enter, return to dashboard
		if isDone {
			if keyMsg, ok := msg.(tea.KeyMsg); ok {
				switch keyMsg.String() {
				case "q", "enter":
					m.mode = modeDashboard
					m.childM = nil
					return m, tea.Batch(
						m.fetchDataCmd(),
						fetchDiskCmd(m.cfg),
					)
				}
			}
			// Still show the done screen for other keys
			return m, nil
		}

		newChild, childCmd := m.childM.Update(msg)
		m.childM = newChild
		// Intercept tea.Quit from child — return to dashboard instead of quitting
		if childCmd != nil {
			return m, func() tea.Msg {
				result := childCmd()
				if _, ok := result.(tea.QuitMsg); ok {
					return childExitMsg{}
				}
				return result
			}
		}
		return m, nil
	}

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		if m.screen == screenLogs {
			m.logScroll = 0
			m.rightScroll = 0
		}
		return m, nil

	case dataRefreshedMsg:
		oldLogLineCount := 0
		if m.logFocused && m.logScroll > 0 {
			oldLogLineCount = m.currentLogLineCount()
		}
		m.hasConfig = msg.hasConfig
		m.cfg = msg.cfg
		m.state = msg.state
		m.services = msg.services
		m.checks = msg.checks
		m.mithrilLines = msg.mithrilLines
		m.lbLines = msg.lbLines
		m.progress = msg.progress
		m.snapshot = msg.snapshot
		m.accounts = msg.accounts
		if oldLogLineCount > 0 {
			if newLogLineCount := m.currentLogLineCount(); newLogLineCount > oldLogLineCount {
				m.logScroll += newLogLineCount - oldLogLineCount
			}
			if maxScroll := m.maxLogScroll(); m.logScroll > maxScroll {
				m.logScroll = maxScroll
			}
		}
		if m.proc.inFlightOp == "" {
			m.proc.preflightErr = msg.preflightErr
			if msg.preflightErr != "" {
				m.proc.preflightInfo = ""
			}
		}
		// Trigger disk fetch once config is loaded (first time only)
		if !m.disksLoaded && m.cfg != nil {
			return m, fetchDiskCmd(m.cfg)
		}
		return m, nil

	case diskRefreshedMsg:
		m.disks = msg.disks
		// Only mark loaded when we had a real config to fetch from
		if m.cfg != nil {
			m.disksLoaded = true
		}
		return m, nil

	case procDetectedMsg:
		// Always release the inflight gate so the next tick can fetch.
		m.proc.inflight = false
		m.proc.fetchedAt = time.Now()
		m.proc.fetchErr = msg.err
		// Keep the last-good detection on transient errors so the badge
		// doesn't flicker on a single fetch hiccup; next success replaces it.
		if msg.det != nil {
			m.proc.detection = msg.det
			m.rememberSpawnLogs(msg.det)
			m.proc.lastOkAt = m.proc.fetchedAt
			m.clampRunActionCursor()
			// Process gone: a prior Stop timeout finished on its own, clear Stuck.
			if msg.det.Status != procctl.StatusRunning {
				m.proc.stuck = false
			}
		}
		return m, nil

	case actionResultMsg:
		// Action finished: clear the gate and surface the result.
		m.proc.inFlightOp = ""
		if msg.det != nil {
			m.proc.fetchErr = ""
			m.proc.fetchedAt = time.Now()
			m.proc.detection = msg.det
			m.rememberSpawnLogs(msg.det)
			m.proc.lastOkAt = m.proc.fetchedAt
			m.clampRunActionCursor()
			if msg.det.Status != procctl.StatusRunning {
				m.proc.stuck = false
			}
		}
		if msg.result != "ok" {
			m.proc.opErr = msg.err
		}
		if msg.stderr != "" {
			m.proc.startFailStderr = msg.stderr
			m.mithrilLines = dashboardSpawnTextLines(msg.stderr, 50)
		}
		if msg.spawnLogs.stdoutPath != "" || msg.spawnLogs.stderrPath != "" {
			m.lastSpawnLogs = msg.spawnLogs
		}
		// Final progress line so the outcome shows even if the user navigates away.
		switch msg.result {
		case "ok":
			m.proc.progressLines = append(m.proc.progressLines, "Done.")
			// Process cleared; clear Stuck if set from a prior timeout.
			if msg.op == opStop || msg.op == opForceStop || msg.op == opRestart {
				m.proc.stuck = false
			}
			if msg.op == opStop || msg.op == opForceStop {
				m.runActionIdx = 0
			}
			if msg.op == opStart || msg.op == opRestart {
				m.cancelStartFlow()
				m.openLogs(false)
			}
		case "timeout":
			m.proc.progressLines = append(m.proc.progressLines, "Timed out waiting for clean exit.")
			// Only Stop/Restart can time out; set Stuck to surface [f] Force Stop.
			if msg.op == opStop || msg.op == opRestart {
				m.proc.stuck = true
			}
		default:
			m.proc.progressLines = append(m.proc.progressLines, "Action failed: "+msg.err)
		}
		// Re-detect now so the badge updates without waiting for the next tick.
		// On successful start/restart also refresh logs (the view switches there).
		cmds := []tea.Cmd{}
		if procFetch := m.triggerProcessFetch(); procFetch != nil {
			cmds = append(cmds, procFetch)
		}
		if msg.result == "ok" && (msg.op == opStart || msg.op == opRestart) {
			cmds = append(cmds, m.fetchDataCmd())
		}
		return m, tea.Batch(cmds...)

	case configFixResultMsg:
		if msg.err != "" {
			// Use the tick-proof field so the next data refresh can't wipe it.
			m.proc.preflightInfo = ""
			m.proc.configFixErr = msg.err
			return m, nil
		}
		m.proc.configFixErr = ""
		m.proc.preflightInfo = msg.summary
		cmds := []tea.Cmd{m.fetchDataCmd(), fetchDiskCmd(m.cfg)}
		if procFetch := m.triggerProcessFetch(); procFetch != nil {
			cmds = append(cmds, procFetch)
		}
		return m, tea.Batch(cmds...)

	case childExitMsg:
		m.mode = modeDashboard
		m.childM = nil
		return m, tea.Batch(
			m.fetchDataCmd(),
			fetchDiskCmd(m.cfg),
		)

	case tickMsg:
		// Gate the process fetch so a slow Detect can't stack across ticks.
		batch := []tea.Cmd{tickCmd(), m.fetchDataCmd()}
		if procFetch := m.triggerProcessFetch(); procFetch != nil {
			batch = append(batch, procFetch)
		}
		return m, tea.Batch(batch...)

	case slowTickMsg:
		return m, tea.Batch(slowTickCmd(), fetchDiskCmd(m.cfg))

	case tea.KeyMsg:
		if m.startFlow.active {
			switch msg.String() {
			case "enter":
				if cmd := m.advanceStartFlow(); cmd != nil {
					return m, cmd
				}
				return m, nil
			case "esc", "ctrl+c":
				m.cancelStartFlow()
				return m, nil
			default:
				return m, nil
			}
		}
		// Modal takes priority: only y/n/esc act, other keys are no-ops so a
		// stray 'q' can't fall through to the underlying view and quit.
		if m.confirmActive {
			switch msg.String() {
			case "y", "Y", "enter":
				onYes := m.confirmOnYes
				m.confirmActive = false
				m.confirmOnYes = nil
				if onYes != nil {
					return m, onYes(&m)
				}
				return m, nil
			case "n", "N", "esc", "ctrl+c":
				m.confirmActive = false
				m.confirmOnYes = nil
				return m, nil
			default:
				return m, nil
			}
		}
		if m.editMode == editText {
			// KeyRunes covers typed chars and pastes. Sanitize to strip
			// control chars/newlines/ESC so a paste can't inject TOML or escapes.
			if msg.Type == tea.KeyRunes && len(msg.Runes) > 0 {
				text := config.SanitizeUserInput(string(msg.Runes))
				if text != "" {
					m.editValue = m.editValue[:m.editCursor] + text + m.editValue[m.editCursor:]
					m.editCursor += len(text)
				}
				return m, nil
			}
			switch msg.String() {
			case "enter":
				m.applyEditField()
				return m, nil
			case "esc":
				m.editMode = editNone
				return m, nil
			case "ctrl+c":
				return m, tea.Quit
			case "backspace":
				if m.editCursor > 0 {
					_, size := utf8.DecodeLastRuneInString(m.editValue[:m.editCursor])
					m.editValue = m.editValue[:m.editCursor-size] + m.editValue[m.editCursor:]
					m.editCursor -= size
				}
				return m, nil
			case "left":
				if m.editCursor > 0 {
					_, size := utf8.DecodeLastRuneInString(m.editValue[:m.editCursor])
					m.editCursor -= size
				}
				return m, nil
			case "right":
				if m.editCursor < len(m.editValue) {
					_, size := utf8.DecodeRuneInString(m.editValue[m.editCursor:])
					m.editCursor += size
				}
				return m, nil
			default:
				ch := msg.String()
				if len(ch) == 1 && ch[0] >= 32 {
					m.editValue = m.editValue[:m.editCursor] + ch + m.editValue[m.editCursor:]
					m.editCursor++
				}
				return m, nil
			}
		}
		switch msg.String() {
		case "q":
			if m.editMode == editNone && !m.logFocused {
				return m, tea.Quit
			}
			// In editMenu or logFocused: ignore q (use esc to exit first)
		case "ctrl+c":
			return m, tea.Quit

		case "up", "k":
			if m.fullWidthRawLogs() {
				maxScroll := m.maxLogScroll()
				if m.logScroll < maxScroll {
					m.logScroll++
				}
				return m, nil
			}
			if m.logFocused {
				maxScroll := m.maxLogScroll()
				if m.logScroll < maxScroll {
					m.logScroll++
				}
				return m, nil
			}
			if m.screen == screenProcess && m.runFocused {
				m.moveRunAction(-1)
				return m, nil
			}
			if m.screen == screenEdit && m.editMode == editMenu {
				m.editOptCursor--
				if m.editOptCursor < 0 {
					m.editOptCursor = len(m.editOptions) - 1
				}
			} else if m.screen == screenEdit && m.editMode == editNone {
				m.moveEditCursor(-1)
			} else if m.editMode == editNone {
				m.moveCursor(-1)
			}

		case "down", "j":
			if m.fullWidthRawLogs() {
				if m.logScroll > 0 {
					m.logScroll--
				}
				return m, nil
			}
			if m.logFocused {
				if m.logScroll > 0 {
					m.logScroll--
				}
				return m, nil
			}
			if m.screen == screenProcess && m.runFocused {
				m.moveRunAction(1)
				return m, nil
			}
			if m.screen == screenEdit && m.editMode == editMenu {
				m.editOptCursor++
				if m.editOptCursor >= len(m.editOptions) {
					m.editOptCursor = 0
				}
			} else if m.screen == screenEdit && m.editMode == editNone {
				m.moveEditCursor(1)
			} else if m.editMode == editNone {
				m.moveCursor(1)
			}

		case "enter":
			if m.fullWidthRawLogs() {
				return m, nil
			}
			if m.screen == screenEdit && m.editMode == editNone {
				m.startEditField()
				return m, nil
			}
			if m.screen == screenEdit && m.editMode == editMenu {
				m.applyMenuSelection()
				return m, nil
			}
			if m.screen == screenProcess && m.runFocused {
				if cmd := m.activateRunAction(); cmd != nil {
					return m, cmd
				}
				return m, nil
			}
			if cmd := m.selectCurrent(); cmd != nil {
				return m, cmd
			}

		case "esc":
			if m.fullWidthRawLogs() {
				m.screen = screenProcess
				m.runFocused = true
				m.logFocused = false
				m.logScroll = 0
				m.setMenuCursor("process")
				return m, nil
			}
			if m.logFocused {
				m.logFocused = false
				return m, nil
			}
			if m.screen == screenProcess && m.runFocused {
				m.runFocused = false
				return m, nil
			}
			if m.editMode != editNone {
				m.editMode = editNone
				return m, nil
			}
			if m.screen == screenEdit {
				m.screen = screenConfig
				return m, nil
			}

		case "r":
			// Action shortcuts only fire with action-list focus; otherwise r refreshes.
			if m.screen == screenProcess && m.runFocused {
				if cmd := m.handleRestartKey(); cmd != nil {
					return m, cmd
				}
				return m, nil
			}
			return m, tea.Batch(
				m.fetchDataCmd(),
				fetchDiskCmd(m.cfg),
			)

		case "t":
			if m.screen == screenLogs {
				// Toggle full-width logs; the menu is always recoverable.
				m.logRawMode = !m.logRawMode
				m.logScroll = 0
				m.logFocused = false
				return m, nil
			}

		case "s":
			if m.screen == screenProcess && m.runFocused {
				if cmd := m.handleStartKey(); cmd != nil {
					return m, cmd
				}
				return m, nil
			}

		case "x":
			if m.screen == screenLogs {
				if cmd := m.handleStopFromLogsKey(); cmd != nil {
					return m, cmd
				}
				return m, nil
			}
			if m.screen == screenProcess && m.runFocused {
				if cmd := m.handleStopKey(); cmd != nil {
					return m, cmd
				}
				return m, nil
			}

		case "f":
			if m.screen == screenProcess && m.runFocused {
				if cmd := m.handleForceStopKey(); cmd != nil {
					return m, cmd
				}
				return m, nil
			}

		case "e":
			if m.hasConfig && (m.screen == screenConfig || m.screen == screenOverview) {
				m.screen = screenEdit
				m.editIdx = 0
				m.moveEditCursor(0)
				// Move left menu cursor to "Edit Config" to stay in sync
				for i, item := range m.items {
					if item.value == "edit" {
						m.cursor = i
						break
					}
				}
			}

		case "left":
			if m.logFocused {
				if m.logRawMode {
					return m, nil
				}
				m.logPane = logPaneMithril
				m.logScroll = 0
				return m, nil
			}
			if m.screen == screenProcess && m.runFocused {
				m.runFocused = false
				return m, nil
			}

		case "right":
			if m.logFocused {
				if m.logRawMode {
					return m, nil
				}
				m.logPane = logPaneLightbringer
				m.logScroll = 0
				return m, nil
			}
			if m.screen == screenProcess {
				m.runFocused = true
				m.clampRunActionCursor()
				return m, nil
			}

		case "pgdown":
			m.rightScroll += 5
			if m.rightScroll > 500 { // reasonable upper bound
				m.rightScroll = 500
			}
		case "pgup":
			m.rightScroll -= 5
			if m.rightScroll < 0 {
				m.rightScroll = 0
			}

		}
	}
	return m, nil
}

// moveCursor moves the cursor by delta (+1 or -1) and skips separators.
// Terminates after a full cycle to prevent infinite loops.
func (m *model) moveCursor(delta int) {
	n := len(m.items)
	if n == 0 {
		return
	}
	m.cursor = (m.cursor + delta + n) % n
	start := m.cursor
	for m.items[m.cursor].isSep {
		m.cursor = (m.cursor + delta + n) % n
		if m.cursor == start {
			break // all separators — no selectable item
		}
	}
}

func (m *model) selectCurrent() tea.Cmd {
	if m.cursor >= len(m.items) {
		return nil
	}
	item := m.items[m.cursor]
	m.rightScroll = 0
	m.logFocused = false
	m.runFocused = false
	m.cancelStartFlow()
	m.logScroll = 0
	m.editMode = editNone
	// With no config, only "Create Config" is actionable; keep others on the
	// overview helper so arrow keys still navigate the menu.
	if !m.hasConfig && item.value != "setup" {
		m.screen = screenOverview
		return nil
	}
	switch item.value {
	case "overview":
		m.screen = screenOverview
	case "process":
		m.screen = screenProcess
		m.runFocused = true
		m.runActionIdx = 0
		// Detect now (via the inflight gate) so the view isn't blank for up to 2s.
		return m.triggerProcessFetch()
	case "config":
		m.screen = screenConfig
	case "doctor":
		m.screen = screenDoctor
	case "logs":
		if m.screen == screenLogs {
			// Already on Logs — toggle scroll focus
			m.logFocused = !m.logFocused
			return nil
		}
		m.openLogs(false)
	case "disk":
		m.screen = screenDisk
		// Fetch disk data immediately when navigating to Disk screen
		return fetchDiskCmd(m.cfg)
	case "edit":
		// Switch to inline config editing in the right pane
		m.screen = screenEdit
		m.editIdx = 0
		m.moveEditCursor(0)
	case "setup":
		// Embed setup directly, pass current config path
		m.mode = modeSetup
		m.childM = setupcmd.NewSetupModel(m.configFile)
		return m.childM.Init()
	}
	return nil
}

func (m *model) openLogs(raw bool) {
	m.screen = screenLogs
	m.runFocused = false
	m.logFocused = false
	// Default keeps the menu visible; full-width logs are opt-in (raw / "t" toggle).
	m.logRawMode = raw
	m.logPane = logPaneMithril
	m.logScroll = 0
	m.rightScroll = 0
	m.setMenuCursor("logs")
}

func (m model) logsStopShortcutAvailable() bool {
	return m.proc.detection != nil &&
		m.proc.detection.Status == procctl.StatusRunning &&
		m.proc.inFlightOp == ""
}

func (m *model) setMenuCursor(value string) {
	for i, item := range m.items {
		if item.value == value {
			m.cursor = i
			return
		}
	}
}

// ── View ────────────────────────────────────────────────────────────────

func (m model) View() string {
	if m.width == 0 {
		return "Loading..."
	}

	// Logo (centered)
	logo := tui.RenderLogoWidth(m.width)

	// Status bar
	sbCfg := statusBarConfig{hasConfig: m.hasConfig}
	if m.cfg != nil {
		sbCfg.cluster = m.cfg.cluster
	}
	if m.state != nil {
		sbCfg.slot = m.state.LastSlot
		sbCfg.epoch = m.state.LastEpoch
	}
	// State file slot is frozen at bootstrap; prefer the live slot from the log tail.
	if s, e, ok := m.liveNodeSlot(); ok {
		sbCfg.slot = s
		sbCfg.epoch = e
	}
	// Header status uses the same process source as the body so they can't contradict.
	if m.proc.detection != nil {
		sbCfg.runStatus = m.proc.detection.Status
	}
	statusBar := renderStatusBar(sbCfg, m.width)

	// Calculate content height
	logoHeight := lipgloss.Height(logo)
	statusBarHeight := lipgloss.Height(statusBar)
	helpHeight := 1
	footerHeight := 1
	spacing := 3
	contentHeight := m.height - logoHeight - statusBarHeight - helpHeight - footerHeight - spacing
	if contentHeight < 6 {
		contentHeight = 6
	}

	if m.fullWidthRawLogs() {
		logRows := contentHeight - 5
		if logRows < 5 {
			logRows = 5
		}
		content := renderSinglePane(singlePaneConfig{
			title:   m.rightPaneTitle(),
			content: m.renderRawLogsViewWith(m.fullPaneContentWidth(), logRows),
			focus:   true,
		}, m.width, contentHeight)
		help := renderHelpBar(m.helpItems(), m.width)
		footer := renderFooter(footerConfig{configFile: m.configFile}, m.width)
		return fitTerminalFrame(lipgloss.JoinVertical(lipgloss.Left,
			logo,
			statusBar,
			"",
			content,
			help,
			footer,
		), m.width, m.height)
	}

	// Left pane: menu (pass width for full-row highlight)
	leftPaneWidth := (m.width - 3) * 22 / 100
	leftContent := renderLeftMenu(m.items, m.cursor, leftPaneWidth)

	// Right pane: confirm modal (highest priority), child TUI, or screen content.
	var rightContent string
	switch {
	case m.startFlow.active:
		rightContent = m.renderStartFlow()
	case m.confirmActive:
		rightContent = renderConfirmModal(m.confirmTitle, m.confirmBody, m.rightPaneContentWidth())
	case m.mode != modeDashboard && m.childM != nil:
		rightContent = m.childM.View()
	default:
		rightContent = m.renderRightPane()
	}

	// Apply scroll offset for long content
	rightLines := strings.Split(rightContent, "\n")
	totalRightLines := len(rightLines)

	if m.rightScroll > 0 {
		if m.rightScroll >= totalRightLines {
			m.rightScroll = totalRightLines - 1
		}
		if m.rightScroll < 0 {
			m.rightScroll = 0
		}
		rightLines = rightLines[m.rightScroll:]
	}

	// Add scroll indicators when content overflows
	scrollHint := lipgloss.NewStyle().Foreground(tui.ColorTextDisabled)
	if len(rightLines) > contentHeight && contentHeight > 2 {
		rightLines = rightLines[:contentHeight]
		rightLines[contentHeight-1] = scrollHint.Render("  ▼ pgdn for more")
	}
	if m.rightScroll > 0 && len(rightLines) > 0 {
		rightLines[0] = scrollHint.Render("  ▲ pgup to scroll up")
	}

	rightContent = strings.Join(rightLines, "\n")

	// Split view
	splitCfg := splitViewConfig{
		leftTitle:    "Menu",
		leftContent:  leftContent,
		rightTitle:   m.rightPaneTitle(),
		rightContent: rightContent,
		focusLeft:    !m.rightPaneFocused(),
	}
	content := renderSplitView(splitCfg, m.width, contentHeight)

	// Help bar
	helpItems := m.helpItems()
	help := renderHelpBar(helpItems, m.width)

	// Footer
	footer := renderFooter(footerConfig{configFile: m.configFile}, m.width)

	return fitTerminalFrame(lipgloss.JoinVertical(lipgloss.Left,
		logo,
		statusBar,
		"",
		content,
		help,
		footer,
	), m.width, m.height)
}

// moveEditCursor moves the edit field cursor, skipping separators.
func (m *model) moveEditCursor(delta int) {
	n := len(m.editFields)
	if n == 0 {
		return
	}
	if delta != 0 {
		m.editIdx = (m.editIdx + delta + n) % n
	}
	start := m.editIdx
	for m.editFields[m.editIdx].isSep {
		if delta == 0 {
			delta = 1
		}
		m.editIdx = (m.editIdx + delta + n) % n
		if m.editIdx == start {
			break
		}
	}
}

// getFieldValue returns the current config value for a field.
func (m model) getFieldValue(f editFieldDef) string {
	if m.cfg == nil {
		return ""
	}
	key := f.section + "." + f.key
	switch key {
	case "network.cluster":
		return m.cfg.cluster
	case "network.rpc":
		if len(m.cfg.rpcEndpoints) > 0 {
			return m.cfg.rpcEndpoints[0]
		}
		return ""
	case "storage.accounts":
		return m.cfg.accountsPath
	case "storage.snapshots":
		return m.cfg.snapshotsPath
	case "storage.shredstore":
		return m.cfg.shredstorePath
	case "storage.logs":
		return m.cfg.logsPath
	case "block.source":
		return m.cfg.blockSource
	case "block.turbine_bind_addr":
		return m.cfg.turbineBindAddr
	case "turbine.gossip_entrypoint":
		return m.cfg.turbineGossip
	case "turbine.gossip_bind_addr":
		return m.cfg.turbineGossipBind
	case "turbine.advertised_ip":
		return m.cfg.turbineAdvertisedIP
	case "turbine.shred_version":
		return m.cfg.turbineShredVersion
	case "block.lightbringer_endpoint":
		return m.cfg.lbExternalEndpoint
	case "block.max_rps":
		return m.cfg.blockMaxRPS
	case "block.max_inflight":
		return m.cfg.blockInflight
	case "lightbringer.enabled":
		if m.cfg.lbEnabled {
			return "true"
		}
		return "false"
	case "lightbringer.binary_path":
		return m.cfg.lbBinaryPath
	case "lightbringer.config_dir":
		return m.cfg.lbConfigDir
	case "lightbringer.gossip_entrypoint":
		return m.cfg.lbGossip
	case "lightbringer.gossip_port":
		return m.cfg.lbGossipPort
	case "lightbringer.port_range_start":
		return m.cfg.lbPortRangeStart
	case "lightbringer.port_range_end":
		return m.cfg.lbPortRangeEnd
	case "lightbringer.grpc_addr":
		return m.cfg.lbGrpcAddr
	case "lightbringer.rpc_addr":
		return m.cfg.lbRpcAddr
	case "lightbringer.quiet":
		if m.cfg.lbQuiet {
			return "true"
		}
		return "false"
	case "tuning.txpar":
		return m.cfg.txpar
	case "rpc.port":
		return m.cfg.rpcPort
	case "log.level":
		return m.cfg.logLevel
	case "bootstrap.mode":
		return m.cfg.bootstrapMode
	}
	return ""
}

// menuOptionsFor returns menu options for a field, or nil if it's a text field.
func menuOptionsFor(section, key string) []editOption {
	switch section + "." + key {
	case "network.cluster":
		return []editOption{
			{label: "mainnet-beta", value: "mainnet-beta"},
			{label: "testnet", value: "testnet"},
			{label: "devnet", value: "devnet"},
		}
	case "block.source":
		return []editOption{
			{label: "rpc", value: "rpc", desc: "Fetch blocks via RPC"},
			{label: "lightbringer", value: "lightbringer", desc: "Sidecar streaming"},
			{label: "turbine", value: "turbine", desc: "Native shred receiver"},
		}
	case "lightbringer.enabled":
		return []editOption{
			{label: "false", value: "false", desc: "Disabled"},
			{label: "true", value: "true", desc: "Enabled"},
		}
	case "lightbringer.quiet":
		return []editOption{
			{label: "true", value: "true", desc: "Only warnings and errors — default and recommended for long runs"},
			{label: "false", value: "false", desc: "Show all info messages"},
		}
	case "log.level":
		return []editOption{
			{label: "debug", value: "debug"},
			{label: "info", value: "info", desc: "recommended"},
			{label: "warn", value: "warn"},
			{label: "error", value: "error"},
		}
	case "bootstrap.mode":
		return []editOption{
			{label: "auto", value: "auto", desc: "Use existing or download snapshot"},
			{label: "snapshot", value: "snapshot", desc: "Rebuild from snapshot"},
			{label: "new-snapshot", value: "new-snapshot", desc: "Always download fresh"},
			{label: "accountsdb", value: "accountsdb", desc: "Require existing data, fail if missing"},
		}
	}
	return nil
}

// startEditField begins inline editing of the selected field.
func (m *model) startEditField() {
	if m.cfg == nil || m.editIdx >= len(m.editFields) || m.editFields[m.editIdx].isSep {
		return
	}
	f := m.editFields[m.editIdx]
	m.editErr = ""

	// Check if this field has menu options
	if opts := menuOptionsFor(f.section, f.key); opts != nil {
		m.editMode = editMenu
		m.editOptions = opts
		m.editOptCursor = m.findOptionIndex(m.getFieldValue(f))
		return
	}

	// Text input
	m.editMode = editText
	m.editValue = m.getFieldValue(f)
	m.editCursor = len(m.editValue)
}

// findOptionIndex finds the index of the current value in editOptions.
func (m *model) findOptionIndex(current string) int {
	for i, opt := range m.editOptions {
		if opt.value == current {
			return i
		}
	}
	return 0
}

// applyMenuSelection saves the selected menu option to config.
func (m *model) applyMenuSelection() {
	if m.cfg == nil || m.editIdx >= len(m.editFields) || m.editOptCursor >= len(m.editOptions) {
		return
	}

	f := m.editFields[m.editIdx]
	value := m.editOptions[m.editOptCursor].value

	if err := saveConfigValue(m.configFile, f.section, f.key, value); err != nil {
		m.editErr = "Save failed: " + err.Error()
		return
	}

	// Sync coupled fields: lightbringer.enabled ↔ block.source
	// Only auto-sync when no external lightbringer_endpoint is configured
	fullKey := f.section + "." + f.key
	hasExternalEndpoint := m.cfg != nil && m.cfg.lbExternalEndpoint != ""
	if fullKey == "lightbringer.enabled" {
		if value == "true" {
			_ = saveConfigValue(m.configFile, "block", "source", "lightbringer")
			// Clear stale external endpoint so runtime uses managed sidecar
			if hasExternalEndpoint {
				_ = saveConfigValue(m.configFile, "block", "lightbringer_endpoint", "")
			}
		} else if !hasExternalEndpoint {
			// Only force rpc when no external endpoint
			_ = saveConfigValue(m.configFile, "block", "source", "rpc")
		}
	} else if fullKey == "block.source" {
		if value == "lightbringer" && !hasExternalEndpoint {
			_ = saveConfigValue(m.configFile, "lightbringer", "enabled", "true")
		} else if value == "rpc" {
			_ = saveConfigValue(m.configFile, "lightbringer", "enabled", "false")
		}
	}

	m.editMode = editNone
	m.cfg = readConfig(m.configFile)
}

// applyEditField validates and saves the edited text value to config.
func (m *model) applyEditField() {
	if m.cfg == nil || m.editIdx >= len(m.editFields) {
		m.editMode = editNone
		return
	}

	f := m.editFields[m.editIdx]
	value := strings.TrimSpace(m.editValue)
	key := f.section + "." + f.key

	// Validate based on field type
	switch {
	case key == "block.max_rps" || key == "block.max_inflight":
		if _, err := strconv.Atoi(value); err != nil {
			m.editErr = "Must be a number"
			return
		}
	case key == "turbine.shred_version":
		if value != "" {
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 || n > 65535 {
				m.editErr = "Must be 0-65535"
				return
			}
		}
	case key == "block.turbine_bind_addr" || key == "turbine.gossip_bind_addr":
		if value != "" {
			_, portStr, err := net.SplitHostPort(value)
			if err != nil {
				m.editErr = "Format: host:port or :port"
				return
			}
			if p, perr := strconv.Atoi(portStr); perr != nil || p < 1 || p > 65535 {
				m.editErr = "Port must be 1-65535"
				return
			}
		}
	case key == "turbine.gossip_entrypoint":
		if value != "" {
			host, portStr, err := net.SplitHostPort(value)
			if err != nil || host == "" {
				m.editErr = "Format: host:port"
				return
			}
			if p, perr := strconv.Atoi(portStr); perr != nil || p < 1 || p > 65535 {
				m.editErr = "Port must be 1-65535"
				return
			}
		}
	case key == "turbine.advertised_ip":
		if value != "" && net.ParseIP(value) == nil {
			m.editErr = "Must be an IP address"
			return
		}
	case key == "tuning.txpar":
		if value != "" { // empty = sequential (runtime default 0)
			if _, err := strconv.Atoi(value); err != nil {
				m.editErr = "Must be a number (or empty for sequential)"
				return
			}
		}
	case key == "rpc.port":
		port, err := strconv.Atoi(value)
		if err != nil || port < 0 || port > 65535 {
			m.editErr = "Must be a port number (0-65535)"
			return
		}
	case key == "lightbringer.gossip_port" || key == "lightbringer.port_range_start" ||
		key == "lightbringer.port_range_end":
		gossipPort := m.cfg.lbGossipPort
		rangeStart := m.cfg.lbPortRangeStart
		rangeEnd := m.cfg.lbPortRangeEnd
		switch key {
		case "lightbringer.gossip_port":
			gossipPort = value
		case "lightbringer.port_range_start":
			rangeStart = value
		case "lightbringer.port_range_end":
			rangeEnd = value
		}
		if _, _, _, err := parseLightbringerGossipPorts(gossipPort, rangeStart, rangeEnd); err != nil {
			m.editErr = err.Error()
			return
		}
	case f.section == "storage" || key == "lightbringer.binary_path" || key == "lightbringer.config_dir":
		if value == "" {
			m.editErr = "Path is required"
			return
		}
		value = filepath.Clean(value)
	case key == "lightbringer.gossip_entrypoint" || key == "lightbringer.grpc_addr" ||
		key == "lightbringer.rpc_addr" || key == "block.lightbringer_endpoint":
		if value != "" {
			host, portStr, err := net.SplitHostPort(value)
			if err != nil || host == "" {
				m.editErr = "Format: host:port (e.g., 127.0.0.1:3001)"
				return
			}
			if p, perr := strconv.Atoi(portStr); perr != nil || p < 1 || p > 65535 {
				m.editErr = "Port must be 1-65535"
				return
			}
		}
	case key == "network.rpc":
		if value == "" {
			m.editErr = "RPC endpoint is required"
			return
		}
	}
	m.editErr = ""

	// Empty txpar means sequential mode (runtime default 0) — remove both keys
	if key == "tuning.txpar" && value == "" {
		_ = removeConfigKey(m.configFile, "tuning", "txpar")
		_ = removeConfigKey(m.configFile, "replay", "txpar") // legacy fallback
		m.editMode = editNone
		m.cfg = readConfig(m.configFile)
		return
	}
	if (key == "lightbringer.gossip_port" || key == "lightbringer.port_range_start" ||
		key == "lightbringer.port_range_end") && value == "" {
		_ = removeConfigKey(m.configFile, f.section, f.key)
		m.editMode = editNone
		m.cfg = readConfig(m.configFile)
		return
	}

	if err := saveConfigValue(m.configFile, f.section, f.key, value); err != nil {
		m.editErr = "Save failed: " + err.Error()
		return
	}
	if key == "storage.accounts" {
		_ = removeConfigKey(m.configFile, "ledger", "accounts_path")
	} else if key == "storage.snapshots" {
		_ = removeConfigKey(m.configFile, "snapshot", "download_path")
	} else if key == "storage.shredstore" {
		_ = removeConfigKey(m.configFile, "storage", "blockstore")
		_ = removeConfigKey(m.configFile, "ledger", "path")
		_ = removeConfigKey(m.configFile, "lightbringer", "storage")
	}
	if key == "block.lightbringer_endpoint" {
		if value != "" {
			_ = saveConfigValue(m.configFile, "block", "source", "lightbringer")
			_ = saveConfigValue(m.configFile, "lightbringer", "enabled", "false")
		} else if m.cfg != nil && !m.cfg.lbEnabled {
			_ = saveConfigValue(m.configFile, "block", "source", "rpc")
		}
	}

	m.editMode = editNone
	m.cfg = readConfig(m.configFile)
}

func (m model) rightPaneTitle() string {
	if m.mode == modeSetup {
		return "Create Config"
	}
	switch m.screen {
	case screenOverview:
		return "Overview"
	case screenProcess:
		return "Run Node"
	case screenConfig:
		return "Configuration"
	case screenEdit:
		if m.editMode != editNone && m.editIdx < len(m.editFields) {
			return "Editing: " + m.editFields[m.editIdx].label
		}
		return "Edit Config"
	case screenDoctor:
		return "Health Check"
	case screenLogs:
		if m.logRawMode {
			return "Terminal Logs"
		}
		return "Logs"
	case screenDisk:
		return "Disk Usage"
	}
	return ""
}

func (m model) rightPaneFocused() bool {
	if m.confirmActive {
		return true
	}
	if m.startFlow.active {
		return true
	}
	if m.screen == screenProcess && m.runFocused {
		return true
	}
	if m.screen == screenLogs && m.logFocused {
		return true
	}
	return false
}

func (m model) helpItems() []helpItem {
	if m.mode == modeSetup {
		// The embedded setup TUI draws its own help; no footer needed.
		return nil
	}
	if m.startFlow.active {
		// Single confirm card — one Enter starts.
		return []helpItem{
			{key: "⏎", desc: "start"},
			{key: "esc", desc: "cancel"},
		}
	}

	base := []helpItem{
		{key: "↑↓", desc: "navigate"},
		{key: "⏎", desc: "select"},
		{key: "r", desc: "refresh"},
	}

	switch m.screen {
	case screenProcess:
		if m.runFocused {
			return []helpItem{
				{key: "↑↓", desc: "choose"},
				{key: "⏎", desc: "run action"},
				{key: "esc", desc: "menu"},
				{key: "q", desc: "quit"},
			}
		}
		return []helpItem{
			{key: "→", desc: "actions"},
			{key: "⏎", desc: "select menu"},
			{key: "r", desc: "refresh"},
			{key: "q", desc: "quit"},
		}
	case screenLogs:
		if m.fullWidthRawLogs() {
			items := []helpItem{
				{key: "↑↓", desc: "scroll"},
				{key: "esc", desc: "run node"},
				{key: "r", desc: "refresh"},
			}
			if m.hasLightbringerLogPane() {
				items = append(items, helpItem{key: "t", desc: "split"})
			}
			if m.logsStopShortcutAvailable() {
				items = append(items, helpItem{key: "x", desc: "stop safely"})
			}
			items = append(items, helpItem{key: "q", desc: "quit"})
			return items
		}
		if m.logFocused {
			pane := "mithril"
			if m.logRawMode {
				pane = "raw"
			} else if m.logPane == logPaneLightbringer {
				pane = "lightbringer"
			}
			items := []helpItem{{key: "↑↓", desc: "scroll"}}
			if m.hasLightbringerLogPane() {
				if m.logRawMode {
					items = append(items, helpItem{key: "t", desc: "split"})
				} else {
					items = append(items,
						helpItem{key: "t", desc: "terminal"},
						helpItem{key: "←→", desc: "switch pane"},
					)
				}
			}
			items = append(items,
				helpItem{key: "esc", desc: "back"},
				helpItem{key: "mode", desc: pane},
			)
			if m.logsStopShortcutAvailable() {
				items = append(items, helpItem{key: "x", desc: "stop safely"})
			}
			return items
		}
		base = append(base, helpItem{key: "⏎", desc: "scroll logs"})
		if m.hasLightbringerLogPane() {
			base = append(base, helpItem{key: "t", desc: "toggle logs"})
		}
		if m.logsStopShortcutAvailable() {
			base = append(base, helpItem{key: "x", desc: "stop safely"})
		}
	case screenConfig:
		base = append(base, helpItem{key: "e", desc: "edit"}, helpItem{key: "pgdn", desc: "scroll"})
	case screenOverview:
		base = append(base, helpItem{key: "e", desc: "edit"}, helpItem{key: "pgdn", desc: "scroll"})
	case screenDoctor, screenDisk:
		base = append(base, helpItem{key: "pgdn", desc: "scroll"})
	case screenEdit:
		if m.editMode == editText {
			return []helpItem{
				{key: "⏎", desc: "save"},
				{key: "esc", desc: "cancel"},
				{key: "←→", desc: "cursor"},
			}
		}
		if m.editMode == editMenu {
			return []helpItem{
				{key: "↑↓", desc: "select"},
				{key: "⏎", desc: "confirm"},
				{key: "esc", desc: "cancel"},
			}
		}
		return []helpItem{
			{key: "↑↓", desc: "section"},
			{key: "⏎", desc: "edit"},
			{key: "esc", desc: "back"},
		}
	}

	base = append(base, helpItem{key: "q", desc: "quit"})
	return base
}

// ── Entry point ─────────────────────────────────────────────────────────

func runDashboard() {
	m := newModel(configFile)
	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
