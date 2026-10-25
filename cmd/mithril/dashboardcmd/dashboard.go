package dashboardcmd

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/cmd/mithril/setupcmd"
	"github.com/Overclock-Validator/mithril/pkg/tui"
	"github.com/Overclock-Validator/mithril/pkg/version"
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
}

type diskRefreshedMsg struct {
	disks []diskUsage
}

func fetchDataCmd(cfgFile string) tea.Cmd {
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
		if cfg != nil {
			mithrilLines = readLogTail(cfg.logsPath, "mithril.log", 50)
			lbLines = readLogTail(cfg.logsPath, "lightbringer.log", 50)
		}

		return dataRefreshedMsg{
			hasConfig:    hasConfig,
			cfg:          cfg,
			state:        state,
			services:     services,
			checks:       checks,
			mithrilLines: mithrilLines,
			lbLines:      lbLines,
		}
	}
}

func fetchDiskCmd(cfg *configData) tea.Cmd {
	return func() tea.Msg {
		return diskRefreshedMsg{disks: getDiskUsage(cfg)}
	}
}

func tickCmd() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func slowTickCmd() tea.Cmd {
	return tea.Tick(30*time.Second, func(t time.Time) tea.Msg { return slowTickMsg(t) })
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
	cfg          *configData
	state        *nodeState
	services     []serviceStatus
	disks        []diskUsage
	checks       []checkResult
	mithrilLines []string
	lbLines      []string
	logScroll    int  // scroll offset for focused log pane
	logFocused   bool // true when user is scrolling logs with ↑↓
	logPane      int  // 0=mithril (left), 1=lightbringer (right)
	disksLoaded  bool // true after first disk fetch completes

	// Menu
	items []menuItem
}

func newModel(cf string) model {
	return model{
		configFile: cf,
		screen:     screenOverview,
		items: []menuItem{
			{label: "Overview", value: "overview"},
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
			{section: "block", key: "max_rps", label: "Block Max RPS"},
			{section: "block", key: "max_inflight", label: "Block Max Inflight"},
			{isSep: true},
			{section: "lightbringer", key: "enabled", label: "Lightbringer"},
			{section: "lightbringer", key: "gossip_entrypoint", label: "Gossip Entrypoint"},
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
	// Non-blocking: fetch data asynchronously on startup
	return tea.Batch(
		fetchDataCmd(m.configFile),
		tickCmd(),
		slowTickCmd(),
	)
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
			// Esc on the wizard's first screen (mode selection) exits back to dashboard
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
						fetchDataCmd(m.configFile),
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
		return m, nil

	case dataRefreshedMsg:
		m.hasConfig = msg.hasConfig
		m.cfg = msg.cfg
		m.state = msg.state
		m.services = msg.services
		m.checks = msg.checks
		m.mithrilLines = msg.mithrilLines
		m.lbLines = msg.lbLines
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

	case childExitMsg:
		m.mode = modeDashboard
		m.childM = nil
		return m, tea.Batch(
			fetchDataCmd(m.configFile),
			fetchDiskCmd(m.cfg),
		)

	case tickMsg:
		return m, tea.Batch(tickCmd(), fetchDataCmd(m.configFile))

	case slowTickMsg:
		return m, tea.Batch(slowTickCmd(), fetchDiskCmd(m.cfg))

	case tea.KeyMsg:
		switch msg.String() {
		case "q":
			if m.editMode == editNone && !m.logFocused {
				return m, tea.Quit
			}
			// In text edit mode, insert 'q' as a character
			if m.editMode == editText {
				m.editValue = m.editValue[:m.editCursor] + "q" + m.editValue[m.editCursor:]
				m.editCursor++
				return m, nil
			}
			// In editMenu or logFocused: ignore q (use esc to exit first)
		case "ctrl+c":
			return m, tea.Quit

		case "up", "k":
			if m.logFocused {
				m.logScroll--
				if m.logScroll < 0 {
					m.logScroll = 0
				}
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
			if m.logFocused {
				// Cap scroll — use a generous limit since wrapped lines expand count
				maxScroll := len(m.mithrilLines) * 3 // approximate: up to 3x after wrapping
				if m.logPane == logPaneLightbringer {
					maxScroll = len(m.lbLines) * 3
				}
				if m.logScroll < maxScroll {
					m.logScroll++
				}
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
			if m.screen == screenEdit && m.editMode == editNone {
				m.startEditField()
				return m, nil
			}
			if m.screen == screenEdit && m.editMode == editText {
				m.applyEditField()
				return m, nil
			}
			if m.screen == screenEdit && m.editMode == editMenu {
				m.applyMenuSelection()
				return m, nil
			}
			if cmd := m.selectCurrent(); cmd != nil {
				return m, cmd
			}

		case "esc":
			if m.logFocused {
				m.logFocused = false
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
			return m, tea.Batch(
				fetchDataCmd(m.configFile),
				fetchDiskCmd(m.cfg),
			)

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

		case "backspace":
			if m.editMode == editText && m.editCursor > 0 {
				m.editValue = m.editValue[:m.editCursor-1] + m.editValue[m.editCursor:]
				m.editCursor--
				return m, nil
			}

		case "left":
			if m.logFocused {
				m.logPane = logPaneMithril
				m.logScroll = 0
				return m, nil
			}
			if m.editMode == editText && m.editCursor > 0 {
				m.editCursor--
				return m, nil
			}

		case "right":
			if m.logFocused {
				m.logPane = logPaneLightbringer
				m.logScroll = 0
				return m, nil
			}
			if m.editMode == editText && m.editCursor < len(m.editValue) {
				m.editCursor++
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

		default:
			// Text input for inline editing
			if m.editMode == editText {
				ch := msg.String()
				if len(ch) == 1 && ch[0] >= 32 {
					m.editValue = m.editValue[:m.editCursor] + ch + m.editValue[m.editCursor:]
					m.editCursor++
					return m, nil
				}
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
	m.logScroll = 0
	m.editMode = editNone
	switch item.value {
	case "overview":
		m.screen = screenOverview
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
		m.screen = screenLogs
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
	for _, svc := range m.services {
		if svc.up {
			sbCfg.online = true
			break
		}
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

	// Left pane: menu (pass width for full-row highlight)
	leftPaneWidth := (m.width - 3) * 22 / 100
	leftContent := renderLeftMenu(m.items, m.cursor, leftPaneWidth)

	// Right pane: child TUI (setup) or screen-specific content
	var rightContent string
	if m.mode != modeDashboard && m.childM != nil {
		rightContent = m.childM.View()
	} else {
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
		focusLeft:    true,
	}
	content := renderSplitView(splitCfg, m.width, contentHeight)

	// Help bar
	helpItems := m.helpItems()
	help := renderHelpBar(helpItems, m.width)

	// Footer
	fCfg := footerConfig{
		version:    version.Version,
		configFile: m.configFile,
	}
	footer := renderFooter(fCfg, m.width)

	return lipgloss.JoinVertical(lipgloss.Left,
		logo,
		statusBar,
		"",
		content,
		help,
		footer,
	)
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
	case "block.max_rps":
		return m.cfg.blockMaxRPS
	case "block.max_inflight":
		return m.cfg.blockInflight
	case "lightbringer.enabled":
		if m.cfg.lbEnabled {
			return "true"
		}
		return "false"
	case "lightbringer.gossip_entrypoint":
		return m.cfg.lbGossip
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
			{label: "mainnet", value: "mainnet", desc: "Alpenglow"},
			{label: "testnet", value: "testnet", desc: "Alpenglow"},
			{label: "devnet", value: "devnet", desc: "Alpenglow"},
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
	case f.section == "storage":
		if value == "" {
			m.editErr = "Path is required"
			return
		}
		value = filepath.Clean(value)
	case key == "lightbringer.gossip_entrypoint" || key == "lightbringer.grpc_addr" || key == "lightbringer.rpc_addr":
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

	if err := saveConfigValue(m.configFile, f.section, f.key, value); err != nil {
		m.editErr = "Save failed: " + err.Error()
		return
	}

	m.editMode = editNone
	m.cfg = readConfig(m.configFile)
}

func (m model) rightPaneTitle() string {
	switch m.screen {
	case screenOverview:
		return "Overview"
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
		return "Logs"
	case screenDisk:
		return "Disk Usage"
	}
	return ""
}

func (m model) helpItems() []helpItem {
	base := []helpItem{
		{key: "↑↓", desc: "navigate"},
		{key: "⏎", desc: "select"},
		{key: "r", desc: "refresh"},
	}

	switch m.screen {
	case screenLogs:
		if m.logFocused {
			pane := "mithril"
			if m.logPane == logPaneLightbringer {
				pane = "lightbringer"
			}
			return []helpItem{
				{key: "↑↓", desc: "scroll"},
				{key: "←→", desc: "switch pane"},
				{key: "esc", desc: "back"},
				{key: "", desc: "(" + pane + ")"},
			}
		}
		base = append(base, helpItem{key: "⏎", desc: "scroll logs"})
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
