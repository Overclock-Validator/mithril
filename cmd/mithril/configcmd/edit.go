package configcmd

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/tui"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var EditCmd = cobra.Command{
	Use:   "edit",
	Short: "Interactively edit configuration",
	Long:  "Opens an interactive editor for the current config file. Pre-fills with existing values.",
	Run: func(cmd *cobra.Command, args []string) {
		runConfigEdit()
	},
}

func init() {
	ConfigCmd.AddCommand(&EditCmd)
	EditCmd.Flags().StringVarP(&configFile, "config", "c", "config.toml", "Path to config file")
}

// ── Theme (matches setupcmd theme) ──────────────────────────────────────

var (
	edTeal        = tui.MithrilTeal
	edTextPrimary = tui.ColorTextPrimary
	edTextSecond  = tui.ColorTextSecondary
	edTextMuted   = tui.ColorTextMuted
	edTextDisable = tui.ColorTextDisabled
	edError       = tui.ColorError
)

// ── Screen constants ────────────────────────────────────────────────────

const (
	edScrSections = iota // main section picker
	edScrCluster
	edScrRPC
	edScrLightbringer
	edScrGossip
	edScrLightbringerQuiet
	edScrStorage
	edScrAccountsPath
	edScrSnapshotsPath
	edScrLogsPath
	edScrTuning
	edScrBlockRPS
	edScrBlockInflight
	edScrRPCPort
	edScrLogLevel
	edScrBootstrap
	edScrDone
)

// ── Menu item ───────────────────────────────────────────────────────────

type edItem struct {
	label string
	value string
	desc  string
	isSep bool
}

// ── Model ───────────────────────────────────────────────────────────────

type editModel struct {
	configFile string
	screen     int
	stack      []int
	cursor     int
	editing    bool
	inputVal   string
	inputErr   string
	inputCur   int
	width      int

	// Config values
	cluster       string
	rpcEndpoint   string
	lbEnabled     bool
	gossipEntry   string
	lbQuiet       bool
	accountsPath  string
	snapshotsPath string
	logsPath      string
	txpar         string
	blockMaxRPS   string
	blockInflight string
	rpcPort       string
	logLevel      string
	bootstrapMode string

	// Full RPC array (preserved on save to avoid destroying failover endpoints)
	rpcFull     []string
	txparWasSet bool // true if txpar was explicitly in the config

	// Original viper for fallbacks
	v *viper.Viper

	// Result
	saved bool
	err   error
}

func newEditModel(cf string, v *viper.Viper) editModel {
	cluster := v.GetString("network.cluster")
	if cluster == "" {
		cluster = "alpenglow" // the only cluster this build boots
	}
	rpcSlice := v.GetStringSlice("network.rpc")
	rpcEndpoint := ""
	if len(rpcSlice) > 0 {
		rpcEndpoint = rpcSlice[0]
	}
	// rpcFull stored in model to preserve failover endpoints on save
	txpar := v.GetString("tuning.txpar")
	if txpar == "" {
		txpar = v.GetString("replay.txpar")
	}
	txparWasSet := txpar != "" // track if txpar was explicitly configured
	blockMaxRPS := v.GetString("block.max_rps")
	if blockMaxRPS == "" {
		blockMaxRPS = "8"
	}
	blockInflight := v.GetString("block.max_inflight")
	if blockInflight == "" {
		blockInflight = "8"
	}
	rpcPort := v.GetString("rpc.port")
	if rpcPort == "" {
		rpcPort = "8899"
	}
	logLevel := v.GetString("log.level")
	if logLevel == "" {
		logLevel = "info"
	}
	bootstrapMode := v.GetString("bootstrap.mode")
	if bootstrapMode == "" {
		bootstrapMode = "auto"
	}
	logsPath := v.GetString("storage.logs")
	if logsPath == "" {
		logsPath = v.GetString("log.dir")
	}

	return editModel{
		configFile:    cf,
		screen:        edScrSections,
		v:             v,
		cluster:       cluster,
		rpcEndpoint:   rpcEndpoint,
		rpcFull:       rpcSlice,
		txparWasSet:   txparWasSet,
		lbEnabled:     v.GetBool("lightbringer.enabled"),
		gossipEntry:   v.GetString("lightbringer.gossip_entrypoint"),
		lbQuiet:       v.GetBool("lightbringer.quiet"),
		accountsPath:  v.GetString("storage.accounts"),
		snapshotsPath: v.GetString("storage.snapshots"),
		logsPath:      logsPath,
		txpar:         txpar,
		blockMaxRPS:   blockMaxRPS,
		blockInflight: blockInflight,
		rpcPort:       rpcPort,
		logLevel:      logLevel,
		bootstrapMode: bootstrapMode,
	}
}

func (m editModel) Init() tea.Cmd { return nil }

// ── Navigation ──────────────────────────────────────────────────────────

func (m *editModel) pushMenu(scr int) {
	m.stack = append(m.stack, m.screen)
	m.screen = scr
	m.cursor = 0
	m.editing = false
	m.inputErr = ""
}

func (m *editModel) pushInput(scr int) {
	m.stack = append(m.stack, m.screen)
	m.screen = scr
	m.editing = true
	m.inputErr = ""
	m.inputVal = m.inputValueForScreen(scr)
	m.inputCur = len(m.inputVal)
}

func (m *editModel) goBack() {
	if len(m.stack) == 0 {
		return
	}
	m.screen = m.stack[len(m.stack)-1]
	m.stack = m.stack[:len(m.stack)-1]
	m.cursor = 0
	m.inputErr = ""
	if m.isInputScreen(m.screen) {
		m.editing = true
		m.inputVal = m.inputValueForScreen(m.screen)
		m.inputCur = len(m.inputVal)
	} else {
		m.editing = false
	}
}

func (m editModel) isInputScreen(scr int) bool {
	switch scr {
	case edScrRPC, edScrGossip, edScrAccountsPath, edScrSnapshotsPath,
		edScrLogsPath, edScrTuning, edScrBlockRPS, edScrBlockInflight, edScrRPCPort:
		return true
	}
	return false
}

func (m editModel) inputValueForScreen(scr int) string {
	switch scr {
	case edScrRPC:
		return m.rpcEndpoint
	case edScrGossip:
		return m.gossipEntry
	case edScrAccountsPath:
		return m.accountsPath
	case edScrSnapshotsPath:
		return m.snapshotsPath
	case edScrLogsPath:
		return m.logsPath
	case edScrTuning:
		return m.txpar
	case edScrBlockRPS:
		return m.blockMaxRPS
	case edScrBlockInflight:
		return m.blockInflight
	case edScrRPCPort:
		return m.rpcPort
	}
	return ""
}

// ── Menu items ──────────────────────────────────────────────────────────

func (m editModel) currentItems() []edItem {
	switch m.screen {
	case edScrSections:
		lbStatus := "disabled"
		if m.lbEnabled {
			lbStatus = "enabled"
			if m.lbQuiet {
				lbStatus += ", quiet"
			}
		}
		return []edItem{
			{label: "Network", value: "network", desc: fmt.Sprintf("cluster=%s  rpc=%s", m.cluster, truncate(m.rpcEndpoint, 35))},
			{label: "Lightbringer", value: "lightbringer", desc: lbStatus},
			{label: "Storage", value: "storage", desc: truncate(m.accountsPath, 30)},
			{label: "Tuning", value: "tuning", desc: fmt.Sprintf("txpar=%s", m.txpar)},
			{label: "Block Fetch", value: "block", desc: fmt.Sprintf("rps=%s  inflight=%s", m.blockMaxRPS, m.blockInflight)},
			{label: "RPC Server", value: "rpc", desc: fmt.Sprintf("port=%s", m.rpcPort)},
			{label: "Logging", value: "log", desc: m.logLevel},
			{label: "Bootstrap", value: "bootstrap", desc: m.bootstrapMode},
			{isSep: true},
			{label: "Save & exit", value: "save"},
		}
	case edScrCluster:
		return []edItem{
			{label: "alpenglow", value: "alpenglow", desc: "The only cluster this build boots"},
			{label: "mainnet-beta", value: "mainnet-beta", desc: "Requires a dev-branch (TowerBFT) build"},
			{label: "testnet", value: "testnet", desc: "Requires a dev-branch (TowerBFT) build"},
			{label: "devnet", value: "devnet", desc: "Requires a dev-branch (TowerBFT) build"},
			{isSep: true},
			{label: "← Back", value: "_back"},
		}
	case edScrLightbringer:
		items := []edItem{
			{label: "Disable", value: "disable", desc: "Use RPC only"},
			{label: "Enable", value: "enable", desc: "Sidecar for lower-latency block streaming"},
		}
		if m.lbEnabled {
			quietDesc := "off"
			if m.lbQuiet {
				quietDesc = "on (only warn/error in lightbringer.log)"
			}
			items = append(items, edItem{label: "Quiet logs", value: "quiet", desc: quietDesc})
		}
		items = append(items, edItem{isSep: true}, edItem{label: "← Back", value: "_back"})
		return items
	case edScrLightbringerQuiet:
		return []edItem{
			{label: "Quiet mode", value: "true", desc: "Only warnings and errors — default and recommended for long runs"},
			{label: "Normal logs", value: "false", desc: "Show all info messages"},
			{isSep: true},
			{label: "← Back", value: "_back"},
		}
	case edScrStorage:
		return []edItem{
			{label: "AccountsDB", value: "accounts", desc: truncate(m.accountsPath, 30)},
			{label: "Snapshots", value: "snapshots", desc: truncate(m.snapshotsPath, 30)},
			{label: "Logs", value: "logs", desc: truncate(m.logsPath, 30)},
			{isSep: true},
			{label: "← Back", value: "_back"},
		}
	case edScrLogLevel:
		return []edItem{
			{label: "debug", value: "debug"},
			{label: "info", value: "info", desc: "(recommended)"},
			{label: "warn", value: "warn"},
			{label: "error", value: "error"},
			{isSep: true},
			{label: "← Back", value: "_back"},
		}
	case edScrBootstrap:
		return []edItem{
			{label: "auto", value: "auto", desc: "Use existing or download snapshot"},
			{label: "snapshot", value: "snapshot", desc: "Rebuild from snapshot"},
			{label: "new-snapshot", value: "new-snapshot", desc: "Always download fresh"},
			{label: "accountsdb", value: "accountsdb", desc: "Require existing data"},
			{isSep: true},
			{label: "← Back", value: "_back"},
		}
	}
	return nil
}

// ── Update ──────────────────────────────────────────────────────────────

func (m editModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		return m, nil

	case tea.KeyMsg:
		if m.screen == edScrDone {
			return m, tea.Quit
		}

		if m.editing {
			return m.updateInput(msg)
		}
		return m.updateMenu(msg)
	}
	return m, nil
}

func (m editModel) updateMenu(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	items := m.currentItems()
	maxIdx := len(items) - 1

	switch msg.String() {
	case "q", "ctrl+c":
		if m.screen == edScrSections {
			return m, tea.Quit
		}
		m.goBack()

	case "esc":
		if m.screen != edScrSections {
			m.goBack()
		}

	case "up", "k":
		m.cursor--
		if m.cursor < 0 {
			m.cursor = maxIdx
		}
		for items[m.cursor].isSep {
			m.cursor--
			if m.cursor < 0 {
				m.cursor = maxIdx
			}
		}

	case "down", "j":
		m.cursor++
		if m.cursor > maxIdx {
			m.cursor = 0
		}
		for items[m.cursor].isSep {
			m.cursor++
			if m.cursor > maxIdx {
				m.cursor = 0
			}
		}

	case "enter":
		if m.cursor >= 0 && m.cursor < len(items) {
			value := items[m.cursor].value
			if value == "_back" {
				m.goBack()
				return m, nil
			}
			m.handleSelect(value)
		}
	}
	return m, nil
}

func (m *editModel) handleSelect(value string) {
	switch m.screen {
	case edScrSections:
		switch value {
		case "network":
			m.pushMenu(edScrCluster)
		case "lightbringer":
			m.pushMenu(edScrLightbringer)
		case "storage":
			m.pushMenu(edScrStorage)
		case "tuning":
			m.pushInput(edScrTuning)
		case "block":
			m.pushInput(edScrBlockRPS)
		case "rpc":
			m.pushInput(edScrRPCPort)
		case "log":
			m.pushMenu(edScrLogLevel)
		case "bootstrap":
			m.pushMenu(edScrBootstrap)
		case "save":
			m.saveConfig()
			m.screen = edScrDone
		}

	case edScrCluster:
		m.cluster = value
		m.pushInput(edScrRPC)

	case edScrLightbringer:
		switch value {
		case "enable":
			m.lbEnabled = true
			m.pushInput(edScrGossip)
		case "disable":
			m.lbEnabled = false
			m.lbQuiet = config.LightbringerQuietDefault // Reset dependent state so disable→re-enable starts clean.
			m.goBack()
		case "quiet":
			m.pushMenu(edScrLightbringerQuiet)
		}

	case edScrLightbringerQuiet:
		m.lbQuiet = value == "true"
		m.goBack()

	case edScrStorage:
		switch value {
		case "accounts":
			m.pushInput(edScrAccountsPath)
		case "snapshots":
			m.pushInput(edScrSnapshotsPath)
		case "logs":
			m.pushInput(edScrLogsPath)
		}

	case edScrLogLevel:
		m.logLevel = value
		m.goBack()

	case edScrBootstrap:
		m.bootstrapMode = value
		m.goBack()
	}
}

func (m editModel) updateInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.goBack()
	case "ctrl+c":
		return m, tea.Quit
	case "enter":
		if m.validateAndApplyInput() {
			m.advanceFromInput()
		}
	case "backspace":
		if m.inputCur > 0 {
			m.inputVal = m.inputVal[:m.inputCur-1] + m.inputVal[m.inputCur:]
			m.inputCur--
		}
	case "left":
		if m.inputCur > 0 {
			m.inputCur--
		}
	case "right":
		if m.inputCur < len(m.inputVal) {
			m.inputCur++
		}
	case "ctrl+a":
		m.inputCur = 0
	case "ctrl+e":
		m.inputCur = len(m.inputVal)
	default:
		ch := msg.String()
		if len(ch) == 1 && ch[0] >= 32 {
			m.inputVal = m.inputVal[:m.inputCur] + ch + m.inputVal[m.inputCur:]
			m.inputCur++
		}
	}
	return m, nil
}

func (m *editModel) validateAndApplyInput() bool {
	val := strings.TrimSpace(m.inputVal)

	switch m.screen {
	case edScrRPC:
		if val == "" {
			m.inputErr = "RPC endpoint is required"
			return false
		}
		m.rpcEndpoint = val

	case edScrGossip:
		if val == "" {
			m.inputErr = "Format: IP:port (e.g., 1.2.3.4:8000)"
			return false
		}
		host, portStr, err := net.SplitHostPort(val)
		if err != nil || host == "" {
			m.inputErr = "Format: IP:port (e.g., 1.2.3.4:8000)"
			return false
		}
		if p, perr := strconv.Atoi(portStr); perr != nil || p < 1 || p > 65535 {
			m.inputErr = "Port must be 1-65535"
			return false
		}
		m.gossipEntry = val

	case edScrAccountsPath:
		if val == "" {
			m.inputErr = "Path is required"
			return false
		}
		m.accountsPath = filepath.Clean(val)

	case edScrSnapshotsPath:
		if val == "" {
			m.inputErr = "Path is required"
			return false
		}
		m.snapshotsPath = filepath.Clean(val)

	case edScrLogsPath:
		if val == "" {
			m.inputErr = "Path is required"
			return false
		}
		m.logsPath = filepath.Clean(val)

	case edScrTuning:
		n, err := strconv.Atoi(val)
		if err != nil || n < 0 {
			m.inputErr = "Must be 0 (sequential) or a positive integer"
			return false
		}
		m.txpar = val
		m.txparWasSet = true

	case edScrBlockRPS:
		n, err := strconv.Atoi(val)
		if err != nil || n < 1 {
			m.inputErr = "Must be a positive integer"
			return false
		}
		m.blockMaxRPS = val

	case edScrBlockInflight:
		n, err := strconv.Atoi(val)
		if err != nil || n < 1 {
			m.inputErr = "Must be a positive integer"
			return false
		}
		m.blockInflight = val

	case edScrRPCPort:
		n, err := strconv.Atoi(val)
		if err != nil || n < 0 || n > 65535 {
			m.inputErr = "Must be 0 (disabled) or a port number (1-65535)"
			return false
		}
		m.rpcPort = val
	}

	m.inputErr = ""
	return true
}

func (m *editModel) advanceFromInput() {
	switch m.screen {
	case edScrRPC:
		m.goBack() // back to sections
		m.goBack() // pop cluster too
	case edScrGossip:
		m.goBack() // back to sections
		m.goBack() // pop lightbringer too
	case edScrAccountsPath, edScrSnapshotsPath, edScrLogsPath:
		m.goBack() // back to storage
	case edScrTuning:
		m.goBack() // back to sections
	case edScrBlockRPS:
		m.pushInput(edScrBlockInflight)
	case edScrBlockInflight:
		m.goBack() // back to sections
		m.goBack() // pop blockRPS too
	case edScrRPCPort:
		m.goBack() // back to sections
	}
}

// ── Save ────────────────────────────────────────────────────────────────

func (m *editModel) saveConfig() {
	data, err := os.ReadFile(m.configFile)
	if err != nil {
		m.err = err
		return
	}

	content := string(data)
	content = setTomlValue(content, "network", "cluster", fmt.Sprintf("%q", m.cluster))
	// Preserve failover RPC endpoints — update first, keep rest
	rpcArray := m.rpcFull
	if len(rpcArray) > 0 {
		rpcArray[0] = m.rpcEndpoint
	} else {
		rpcArray = []string{m.rpcEndpoint}
	}
	var rpcParts []string
	for _, ep := range rpcArray {
		rpcParts = append(rpcParts, fmt.Sprintf("%q", ep))
	}
	content = setTomlValue(content, "network", "rpc", "["+strings.Join(rpcParts, ", ")+"]")
	if m.accountsPath != "" {
		content = setTomlValue(content, "storage", "accounts", fmt.Sprintf("%q", filepath.Clean(m.accountsPath)))
	}
	if m.snapshotsPath != "" {
		content = setTomlValue(content, "storage", "snapshots", fmt.Sprintf("%q", filepath.Clean(m.snapshotsPath)))
	}
	content = setTomlValue(content, "block", "max_rps", m.blockMaxRPS)
	content = setTomlValue(content, "block", "max_inflight", m.blockInflight)
	// Only write txpar if it was originally in the config or user explicitly set a value
	if m.txparWasSet && m.txpar != "" {
		content = setTomlValue(content, "tuning", "txpar", m.txpar)
	}
	content = setTomlValue(content, "rpc", "port", m.rpcPort)
	content = setTomlValue(content, "log", "level", fmt.Sprintf("%q", m.logLevel))
	content = setTomlValue(content, "bootstrap", "mode", fmt.Sprintf("%q", m.bootstrapMode))

	if m.lbEnabled {
		content = setTomlValue(content, "block", "source", "\"lightbringer\"")
		// Clear stale external endpoint so runtime uses managed sidecar's grpc_addr
		content = setTomlValue(content, "block", "lightbringer_endpoint", "\"\"")
		if !strings.Contains(content, "[lightbringer]") {
			content += fmt.Sprintf("\n[lightbringer]\nenabled = true\nbinary_path = \"./lightbringer\"\ngossip_entrypoint = %q\ngrpc_addr = \"127.0.0.1:3001\"\nrpc_addr = \"127.0.0.1:3000\"\n", m.gossipEntry)
		} else {
			content = setTomlValue(content, "lightbringer", "enabled", "true")
			if m.gossipEntry != "" {
				content = setTomlValue(content, "lightbringer", "gossip_entrypoint", fmt.Sprintf("%q", m.gossipEntry))
			}
		}
		if m.lbQuiet {
			content = setTomlValue(content, "lightbringer", "quiet", "true")
		} else {
			content = setTomlValue(content, "lightbringer", "quiet", "false")
		}
	} else {
		// Only force a source change if no external lightbringer_endpoint is
		// configured. External LB mode (enabled=false + endpoint set) is a valid
		// runtime config. Turbine is the default live Alpenglow source.
		if m.v.GetString("block.lightbringer_endpoint") == "" {
			content = setTomlValue(content, "block", "source", "\"turbine\"")
		}
		if strings.Contains(content, "[lightbringer]") {
			content = setTomlValue(content, "lightbringer", "enabled", "false")
		}
	}

	if m.logsPath != "" {
		content = setTomlValue(content, "storage", "logs", fmt.Sprintf("%q", filepath.Clean(m.logsPath)))
		content = setTomlValue(content, "log", "dir", fmt.Sprintf("%q", filepath.Clean(m.logsPath)))
	}

	if err := tui.AtomicWriteFile(m.configFile, []byte(content), 0600); err != nil {
		m.err = err
		return
	}
	m.saved = true
}

// ── Banner ──────────────────────────────────────────────────────────────

func edBanner() string {
	return tui.RenderLogo()
}

// ── View ────────────────────────────────────────────────────────────────

func (m editModel) View() string {
	banner := edBanner()

	if m.editing {
		title, desc := m.inputTitleDesc()
		return banner + "\n" + edRenderInput(title, desc, m.inputVal, m.inputErr, m.inputCur)
	}

	if m.screen == edScrDone {
		return m.renderDone()
	}

	title, desc := m.menuTitleDesc()
	return banner + "\n" + edRenderMenu(title, desc, m.currentItems(), m.cursor)
}

func (m editModel) menuTitleDesc() (string, string) {
	switch m.screen {
	case edScrSections:
		return "Edit Configuration", fmt.Sprintf("Editing: %s", m.configFile)
	case edScrCluster:
		return "Solana Cluster", ""
	case edScrLightbringer:
		return "Lightbringer Sidecar", ""
	case edScrLightbringerQuiet:
		return "Lightbringer Log Verbosity", "Quiet mode suppresses Lightbringer info/debug logs (only warnings and errors)."
	case edScrStorage:
		return "Storage Paths", ""
	case edScrLogLevel:
		return "Log Level", ""
	case edScrBootstrap:
		return "Bootstrap Mode", ""
	}
	return "", ""
}

func (m editModel) inputTitleDesc() (string, string) {
	switch m.screen {
	case edScrRPC:
		return "RPC Endpoint", "Primary Solana RPC endpoint URL"
	case edScrGossip:
		return "Gossip Entrypoint", "IP:port of a Solana validator running gossip"
	case edScrAccountsPath:
		return "AccountsDB Path", "Path for AccountsDB storage (~500GB, fastest NVMe)"
	case edScrSnapshotsPath:
		return "Snapshots Path", "Path for snapshot storage (~100GB)"
	case edScrLogsPath:
		return "Logs Path", "Path for log files"
	case edScrTuning:
		return "Transaction Parallelism", fmt.Sprintf("Recommended: %d (2x CPU cores)", runtime.NumCPU()*2)
	case edScrBlockRPS:
		return "Block Fetch Max RPS", "Maximum RPC requests per second for block fetching"
	case edScrBlockInflight:
		return "Block Fetch Max Inflight", "Maximum concurrent in-flight block requests"
	case edScrRPCPort:
		return "Mithril RPC Port", "Port for Mithril's RPC server (0 to disable)"
	}
	return "", ""
}

func (m editModel) renderDone() string {
	var b strings.Builder

	if m.err != nil {
		b.WriteString("\n  " + lipgloss.NewStyle().Foreground(edTeal).Bold(true).Render("Save Failed"))
		b.WriteString("\n\n")
		b.WriteString("  " + lipgloss.NewStyle().Foreground(edError).Render("✗ "+m.err.Error()))
		b.WriteString("\n\n  Press any key to exit")
		b.WriteString("\n")
		return b.String()
	}

	b.WriteString("\n  " + lipgloss.NewStyle().Foreground(edTeal).Bold(true).Render("Config Updated"))
	b.WriteString("\n\n")
	b.WriteString("  " + lipgloss.NewStyle().Foreground(edTeal).Render("✓") +
		lipgloss.NewStyle().Foreground(edTextPrimary).Render(" Saved to "+m.configFile))
	b.WriteString("\n\n  Press any key to exit")
	b.WriteString("\n")
	return b.String()
}

// ── Render helpers (same style as setupcmd) ─────────────────────────────

func edRenderMenu(title, description string, items []edItem, cursor int) string {
	var b strings.Builder
	b.WriteString("\n")
	b.WriteString("  " + lipgloss.NewStyle().Foreground(edTeal).Bold(true).Render(title))
	b.WriteString("\n")
	if description != "" {
		b.WriteString("  " + lipgloss.NewStyle().Foreground(edTextMuted).Render(description))
		b.WriteString("\n")
	}
	b.WriteString("\n")

	maxLabel := 0
	for _, item := range items {
		if !item.isSep && len(item.label) > maxLabel {
			maxLabel = len(item.label)
		}
	}

	for i, item := range items {
		if item.isSep {
			b.WriteString("  " + lipgloss.NewStyle().Foreground(edTextDisable).Render(strings.Repeat("·", maxLabel+6)))
			b.WriteString("\n")
			continue
		}
		padded := fmt.Sprintf("%-*s", maxLabel+1, item.label)
		if i == cursor {
			arrow := lipgloss.NewStyle().Foreground(edTeal).Bold(true).Render(" ▸ ")
			label := lipgloss.NewStyle().Foreground(edTeal).Bold(true).Render(padded)
			line := arrow + label
			if item.desc != "" {
				line += " " + lipgloss.NewStyle().Foreground(edTextMuted).Render(item.desc)
			}
			b.WriteString(line)
		} else {
			label := lipgloss.NewStyle().Foreground(edTextSecond).Render(padded)
			b.WriteString("   " + label)
		}
		b.WriteString("\n")
	}

	b.WriteString("\n")
	k := lipgloss.NewStyle().Foreground(edTeal)
	h := lipgloss.NewStyle().Foreground(edTextDisable)
	hasBack := false
	for _, item := range items {
		if item.value == "_back" {
			hasBack = true
			break
		}
	}
	help := "  " + k.Render("↑↓") + h.Render(" navigate") +
		"  " + k.Render("⏎") + h.Render(" select")
	if hasBack {
		help += "  " + k.Render("esc") + h.Render(" back")
	} else {
		help += "  " + k.Render("q") + h.Render(" quit")
	}
	b.WriteString(help)
	return b.String()
}

func edRenderInput(title, description, value, errMsg string, cursorPos int) string {
	var b strings.Builder
	b.WriteString("\n")
	b.WriteString("  " + lipgloss.NewStyle().Foreground(edTeal).Bold(true).Render(title))
	b.WriteString("\n")
	if description != "" {
		for _, line := range strings.Split(description, "\n") {
			b.WriteString("  " + lipgloss.NewStyle().Foreground(edTextMuted).Render(line))
			b.WriteString("\n")
		}
	}
	b.WriteString("\n")

	text := value
	if cursorPos >= 0 && cursorPos <= len(text) {
		before := text[:cursorPos]
		after := text[cursorPos:]
		cursor := lipgloss.NewStyle().Background(edTeal).Foreground(lipgloss.Color("#000000")).Render(" ")
		if cursorPos < len(text) {
			cursor = lipgloss.NewStyle().Background(edTeal).Foreground(lipgloss.Color("#000000")).Render(string(after[0]))
			after = after[1:]
		}
		text = before + cursor + after
	}

	prompt := lipgloss.NewStyle().Foreground(edTeal).Render("❯ ")
	b.WriteString("  " + prompt + text)
	b.WriteString("\n")

	underLen := len(value) + 2
	if underLen < 30 {
		underLen = 30
	}
	b.WriteString("  " + lipgloss.NewStyle().Foreground(edTeal).Render(strings.Repeat("─", underLen)))
	b.WriteString("\n")

	if errMsg != "" {
		b.WriteString("\n  " + lipgloss.NewStyle().Foreground(edError).Render("✗ "+errMsg))
		b.WriteString("\n")
	}

	b.WriteString("\n")
	k := lipgloss.NewStyle().Foreground(edTeal)
	h := lipgloss.NewStyle().Foreground(edTextDisable)
	b.WriteString("  " + k.Render("⏎") + h.Render(" confirm") +
		"  " + k.Render("esc") + h.Render(" back") +
		"  " + k.Render("←→") + h.Render(" cursor"))
	return b.String()
}

// ── Entry point ─────────────────────────────────────────────────────────

func runConfigEdit() {
	if _, err := os.Stat(configFile); err != nil {
		fmt.Printf("Config file not found: %s\nRun: mithril setup\n", configFile)
		return
	}

	v := viper.New()
	config.ApplyDefaults(v)
	v.SetConfigFile(configFile)
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Error reading config: %v\n", err)
		return
	}

	m := newEditModel(configFile, v)
	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

// ── Utilities (kept from original) ──────────────────────────────────────

func setTomlValue(content, section, key, value string) string {
	lines := strings.Split(content, "\n")
	inSection := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && !strings.HasPrefix(trimmed, "[[") {
			if inSection {
				// Section found but key missing — insert before next section header
				result := make([]string, 0, len(lines)+1)
				result = append(result, lines[:i]...)
				result = append(result, key+" = "+value)
				result = append(result, lines[i:]...)
				return strings.Join(result, "\n")
			}
			sectionName := strings.Trim(trimmed, "[] ")
			inSection = sectionName == section
			continue
		}
		if inSection && (strings.HasPrefix(trimmed, key+" ") || strings.HasPrefix(trimmed, key+"=")) {
			indent := ""
			for _, c := range line {
				if c == ' ' || c == '\t' {
					indent += string(c)
				} else {
					break
				}
			}
			lines[i] = fmt.Sprintf("%s%s = %s", indent, key, value)
			return strings.Join(lines, "\n")
		}
	}
	// If section was the last one (no next header), append key
	if inSection {
		lines = append(lines, key+" = "+value)
		return strings.Join(lines, "\n")
	}
	// Section not found at all — append new section with key
	lines = append(lines, "", "["+section+"]", key+" = "+value)
	return strings.Join(lines, "\n")
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
