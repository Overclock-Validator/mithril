package setupcmd

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
	"github.com/spf13/cobra"
)

var (
	outputPath  string
	migrateFlag bool

	SetupCmd = cobra.Command{
		Use:   "setup",
		Short: "Interactive setup for Mithril configuration",
		Run: func(cmd *cobra.Command, args []string) {
			runSetup()
		},
	}

	DoctorCmd = cobra.Command{
		Use:   "doctor",
		Short: "Check system health and configuration",
		Run: func(cmd *cobra.Command, args []string) {
			if migrateFlag {
				runMigrate()
				return
			}
			runDoctor()
		},
	}
)

func init() {
	SetupCmd.Flags().StringVar(&outputPath, "output", "config.toml", "Output path for generated config")
	DoctorCmd.Flags().BoolVar(&migrateFlag, "migrate", false, "Add missing config sections")
}

func runMigrate() {
	configPath := "config.toml"
	if config.ConfigFile != "" {
		configPath = config.ConfigFile
	}
	fmt.Println()
	fmt.Println(titleStyle.Render("◎ Config Migration"))
	fmt.Println()
	if _, err := os.Stat(configPath); err != nil {
		fmt.Printf("  %s Config file not found (%s)\n", errorStyle.Render("✗"), configPath)
		return
	}
	if MigrateConfig(configPath) {
		fmt.Printf("  %s Added missing sections to %s\n", successStyle.Render("✓"), configPath)
	} else {
		fmt.Printf("  %s Config already up to date (%s)\n", successStyle.Render("✓"), configPath)
	}
	fmt.Println()
}

// ── Screens ─────────────────────────────────────────────────────────────

type screen int

const (
	scrMode     screen = iota
	scrNodeType        // verifying node vs validator
	scrCluster
	scrRPC
	scrLightbringer
	scrGossip
	scrIdentityKey       // validator identity keypair path (validator mode)
	scrVoteKey           // vote account keypair path (validator mode)
	scrLightbringerQuiet // log verbosity for managed Lightbringer (only shown when Lightbringer enabled)
	scrStorage           // accountsPath
	scrStorageSnap       // snapshotsPath
	scrStorageLogs       // logsPath
	scrBootstrap
	scrBlockTuning   // maxRPS
	scrBlockInflight // maxInflight
	scrReplay
	scrSnapshot
	scrLogLevel
	scrRPCPort
	scrReview
	scrOverwrite // confirm overwrite existing config
	scrDone
)

// ── Model ───────────────────────────────────────────────────────────────

type setupModel struct {
	screen    screen
	prevStack []screen // for back navigation
	cursor    int
	width     int
	height    int

	// Input mode
	editing  bool
	inputVal string
	inputCur int // cursor position within input
	inputErr string

	// Config values
	mode           string // quick, full, manual
	nodeType       string // "verifying" (non-voting) or "validator"
	cluster        string
	rpcEndpoint    string
	enableLB       bool
	gossipEntry    string
	identityKey    string // validator identity keypair path (validator mode)
	voteKey        string // vote account keypair path (validator mode)
	lbQuiet        bool   // suppress Lightbringer info/debug logs
	accountsPath   string
	snapshotsPath  string
	logsPath       string
	shredstorePath string
	bootstrapMode  string
	blockMaxRPS    string
	blockInflight  string
	txpar          string
	snapshotKeep   string
	logLevel       string
	rpcPort        string

	// System
	cpuCores   int
	disks      []DiskInfo
	configPath string
	embedded   bool // true when running inside dashboard (skip logo)
	err        error
}

func newSetupModel() setupModel {
	absPath, _ := filepath.Abs(outputPath)
	storage := config.DefaultStoragePaths()
	return setupModel{
		screen:         scrMode,
		cpuCores:       runtime.NumCPU(),
		disks:          DetectDisks(),
		cluster:        "alpenglow",
		rpcEndpoint:    "https://alpenglow.rpcpool.com",
		lbQuiet:        config.LightbringerQuietDefault,
		accountsPath:   storage.Accounts,
		snapshotsPath:  storage.Snapshots,
		logsPath:       storage.Logs,
		shredstorePath: storage.Shredstore,
		bootstrapMode:  "auto",
		blockMaxRPS:    "8",
		blockInflight:  "8",
		txpar:          fmt.Sprintf("%d", runtime.NumCPU()*2),
		snapshotKeep:   "1",
		logLevel:       "info",
		rpcPort:        "8899",
		configPath:     absPath,
	}
}

func (m setupModel) Init() tea.Cmd { return nil }

// ── Navigation helpers ──────────────────────────────────────────────────

// inputValueForScreen returns the current config value for an input screen.
// Returns ("", false) for non-input (menu) screens.
func (m *setupModel) inputValueForScreen(scr screen) (string, bool) {
	switch scr {
	case scrRPC:
		return m.rpcEndpoint, true
	case scrGossip:
		return m.gossipEntry, true
	case scrIdentityKey:
		return m.identityKey, true
	case scrVoteKey:
		return m.voteKey, true
	case scrStorage:
		return m.accountsPath, true
	case scrStorageSnap:
		return m.snapshotsPath, true
	case scrStorageLogs:
		return m.logsPath, true
	case scrBlockTuning:
		return m.blockMaxRPS, true
	case scrBlockInflight:
		return m.blockInflight, true
	case scrReplay:
		return m.txpar, true
	case scrRPCPort:
		return m.rpcPort, true
	}
	return "", false
}

// pushMenu navigates to a menu screen (no text input).
func (m *setupModel) pushMenu(next screen) {
	m.prevStack = append(m.prevStack, m.screen)
	m.screen = next
	m.cursor = 0
	m.editing = false
	m.inputErr = ""
}

// pushInput navigates to an input screen and activates text editing.
func (m *setupModel) pushInput(next screen) {
	m.prevStack = append(m.prevStack, m.screen)
	m.screen = next
	m.cursor = 0
	m.inputErr = ""
	if val, ok := m.inputValueForScreen(next); ok {
		m.editing = true
		m.inputVal = val
		m.inputCur = len(val)
	}
}

// goBack returns to the previous screen, re-enabling editing if needed.
func (m *setupModel) goBack() {
	if len(m.prevStack) == 0 {
		return
	}
	m.screen = m.prevStack[len(m.prevStack)-1]
	m.prevStack = m.prevStack[:len(m.prevStack)-1]
	m.cursor = 0
	m.inputErr = ""

	if val, ok := m.inputValueForScreen(m.screen); ok {
		m.editing = true
		m.inputVal = val
		m.inputCur = len(val)
	} else {
		m.editing = false
	}
}

// ── Menu items per screen ───────────────────────────────────────────────

func (m setupModel) currentItems() []menuItem {
	switch m.screen {
	case scrMode:
		return []menuItem{
			menuOptionDesc("Quick Start", "quick", "Answer a few questions, we handle the rest"),
			menuOptionDesc("Full Config", "full", "Customize every setting with explanations"),
			menuOptionDesc("Manual", "manual", "Generate config.toml template for editing"),
		}
	case scrNodeType:
		return []menuItem{
			menuOptionDesc("Verifying node", "verifying", "Non-voting: observe, execute, and verify the cluster"),
			menuOptionDesc("Validator", "validator", "Requires identity + vote-account keypairs; voting engine not yet active (runs verify-only until it lands)"),
			menuSeparator(),
			menuBack(),
		}
	case scrCluster:
		return []menuItem{
			menuOptionDesc("alpenglow", "alpenglow", "Alpenglow test cluster (the only cluster this build boots)"),
			menuOptionDesc("mainnet-beta", "mainnet-beta", "Requires a dev-branch (TowerBFT) build"),
			menuOptionDesc("testnet", "testnet", "Requires a dev-branch (TowerBFT) build"),
			menuOptionDesc("devnet", "devnet", "Requires a dev-branch (TowerBFT) build"),
			menuSeparator(),
			menuBack(),
		}
	case scrLightbringer:
		return []menuItem{
			menuOptionDesc("Disable", "disable", "Use RPC only (default)"),
			menuOptionDesc("Enable", "enable", "Sidecar for lower-latency block streaming"),
			menuSeparator(),
			menuBack(),
		}
	case scrBootstrap:
		return []menuItem{
			menuOptionDesc("auto", "auto", "Use existing data or download snapshot (recommended)"),
			menuOptionDesc("snapshot", "snapshot", "Rebuild from snapshot"),
			menuOptionDesc("new-snapshot", "new-snapshot", "Always download fresh"),
			menuOptionDesc("accountsdb", "accountsdb", "Require existing data, fail if missing"),
			menuSeparator(),
			menuBack(),
		}
	case scrSnapshot:
		return []menuItem{
			menuOptionDesc("Keep 1", "1", "For debugging and faster restarts (recommended)"),
			menuOptionDesc("Stream only", "0", "Saves disk but needs re-download"),
			menuSeparator(),
			menuBack(),
		}
	case scrLightbringerQuiet:
		return []menuItem{
			menuOptionDesc("Quiet mode", "true", "Only warnings and errors — default and recommended for long runs"),
			menuOptionDesc("Normal logs", "false", "Show all info messages"),
			menuSeparator(),
			menuBack(),
		}
	case scrLogLevel:
		return []menuItem{
			menuOption("debug", "debug"),
			menuOptionDesc("info", "info", "Startup, progress, warnings (recommended)"),
			menuOption("warn", "warn"),
			menuOption("error", "error"),
			menuSeparator(),
			menuBack(),
		}
	case scrReview:
		return []menuItem{
			menuOption("Save config & exit", "save"),
			menuOption("Go back", "back"),
		}
	case scrOverwrite:
		return []menuItem{
			menuOptionDesc("Overwrite", "overwrite", "Replace existing config with new one"),
			menuOptionDesc("Go back", "back", "Return to review your settings"),
			menuSeparator(),
			menuOptionDesc("Exit", "exit", "Discard and go back"),
		}
	}
	return nil
}

// ── Update ──────────────────────────────────────────────────────────────

func (m setupModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			return m, tea.Quit
		}
		if m.screen == scrDone {
			if msg.String() == "q" || msg.String() == "enter" {
				return m, tea.Quit
			}
			return m, nil
		}
		if m.editing {
			return m.updateInput(msg)
		}
		return m.updateMenu(msg)
	}
	return m, nil
}

func (m setupModel) updateMenu(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	items := m.currentItems()
	maxIdx := len(items) - 1

	switch msg.String() {
	case "up", "k":
		m.cursor--
		for m.cursor >= 0 && items[m.cursor].isSep {
			m.cursor--
		}
		if m.cursor < 0 {
			m.cursor = 0
		}
	case "down", "j":
		m.cursor++
		for m.cursor <= maxIdx && items[m.cursor].isSep {
			m.cursor++
		}
		if m.cursor > maxIdx {
			m.cursor = maxIdx
		}
	case "esc":
		if m.screen != scrMode {
			m.goBack()
		}
	case "q":
		if m.screen == scrMode {
			return m, tea.Quit
		}
		m.goBack()
	case "enter":
		if m.cursor >= 0 && m.cursor <= maxIdx {
			selected := items[m.cursor].value
			if selected == "_back" {
				m.goBack()
			} else {
				return m.handleSelect(selected)
			}
		}
	}
	return m, nil
}

func (m setupModel) handleSelect(value string) (tea.Model, tea.Cmd) {
	switch m.screen {
	case scrMode:
		m.mode = value
		if value == "manual" {
			return m.generateManual()
		}
		m.pushMenu(scrNodeType)

	case scrNodeType:
		m.nodeType = value
		if value == "validator" {
			m.enableLB = false // a validator runs the native turbine source
		}
		m.pushMenu(scrCluster)

	case scrCluster:
		m.cluster = value
		switch value {
		case "mainnet-beta":
			m.rpcEndpoint = "https://api.mainnet-beta.solana.com"
		case "testnet":
			m.rpcEndpoint = "https://api.testnet.solana.com"
		case "devnet":
			m.rpcEndpoint = "https://api.devnet.solana.com"
		case "alpenglow":
			m.rpcEndpoint = "https://alpenglow.rpcpool.com"
		}
		m.pushInput(scrRPC)

	case scrLightbringer:
		m.enableLB = value == "enable"
		if !m.enableLB {
			m.lbQuiet = config.LightbringerQuietDefault // Reset dependent state so disable→re-enable starts clean.
		}
		// Both paths need a gossip entrypoint: lightbringer for its sidecar,
		// turbine (the default source) to join the shred tree.
		m.pushInput(scrGossip)

	case scrBootstrap:
		m.bootstrapMode = value
		m.pushInput(scrBlockTuning)

	case scrSnapshot:
		m.snapshotKeep = value
		m.pushMenu(scrLogLevel)

	case scrLightbringerQuiet:
		m.lbQuiet = value == "true"
		m.pushInput(scrStorage)

	case scrLogLevel:
		m.logLevel = value
		m.pushInput(scrRPCPort)

	case scrReview:
		switch value {
		case "save":
			return m.generateConfig()
		case "back":
			m.goBack()
		}

	case scrOverwrite:
		switch value {
		case "overwrite":
			// User confirmed — proceed with save (scrOverwrite is set, so the
			// existence check in generateConfig/generateManual will be skipped)
			if m.mode == "manual" {
				return m.generateManual()
			}
			return m.generateConfig()
		case "back":
			if m.mode == "manual" {
				m.screen = scrMode // manual has no review screen
			} else {
				m.screen = scrReview
			}
			m.cursor = 0
		case "exit":
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m setupModel) updateInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "enter":
		if m.validateAndApplyInput() {
			m.editing = false
			m.advanceFromInput()
		}
		return m, nil
	case "esc":
		m.editing = false
		m.goBack()
		return m, nil
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
	m.inputErr = ""
	return m, nil
}

// Validation helpers to reduce repetition
func (m *setupModel) requireNonEmpty(val string) bool {
	if val == "" {
		m.inputErr = "required"
		return false
	}
	return true
}

func (m *setupModel) requirePositiveInt(val string) bool {
	n, err := strconv.Atoi(val)
	if err != nil || n < 1 {
		m.inputErr = "must be a positive number"
		return false
	}
	return true
}

func (m *setupModel) requirePort(val string) bool {
	n, err := strconv.Atoi(val)
	if err != nil || n < 0 || n > 65535 {
		m.inputErr = "must be 0–65535"
		return false
	}
	return true
}

func (m *setupModel) validateAndApplyInput() bool {
	val := strings.TrimSpace(m.inputVal)
	val = strings.ReplaceAll(val, "\n", "")
	m.inputErr = ""

	switch m.screen {
	case scrRPC:
		if val != "" && !strings.HasPrefix(val, "http://") && !strings.HasPrefix(val, "https://") {
			m.inputErr = "must start with http:// or https://"
			return false
		}
		if val != "" {
			m.rpcEndpoint = val
		}

	case scrGossip:
		if !m.requireNonEmpty(val) {
			return false
		}
		host, portStr, err := net.SplitHostPort(val)
		if err != nil || host == "" {
			m.inputErr = "must be IP:port (e.g., 1.2.3.4:8000)"
			return false
		}
		if p, perr := strconv.Atoi(portStr); perr != nil || p < 1 || p > 65535 {
			m.inputErr = "port must be 1-65535"
			return false
		}
		m.gossipEntry = val

	case scrIdentityKey:
		if !m.requireNonEmpty(val) {
			return false
		}
		m.identityKey = val

	case scrVoteKey:
		if !m.requireNonEmpty(val) {
			return false
		}
		m.voteKey = val

	case scrStorage:
		if !m.requireNonEmpty(val) {
			return false
		}
		m.accountsPath = filepath.Clean(val)

	case scrStorageSnap:
		if !m.requireNonEmpty(val) {
			return false
		}
		m.snapshotsPath = filepath.Clean(val)

	case scrStorageLogs:
		if !m.requireNonEmpty(val) {
			return false
		}
		m.logsPath = filepath.Clean(val)

	case scrBlockTuning:
		if val != "" {
			if !m.requirePositiveInt(val) {
				return false
			}
			m.blockMaxRPS = val
		}

	case scrBlockInflight:
		if val != "" {
			if !m.requirePositiveInt(val) {
				return false
			}
			m.blockInflight = val
		}

	case scrReplay:
		if val != "" {
			if !m.requirePositiveInt(val) {
				return false
			}
			m.txpar = val
		}

	case scrRPCPort:
		if val != "" {
			if !m.requirePort(val) {
				return false
			}
			m.rpcPort = val
		}
	}
	return true
}

func (m *setupModel) advanceFromInput() {
	switch m.screen {
	case scrRPC:
		if m.mode == "quick" || m.nodeType == "validator" {
			// Turbine (the default source, and the only valid one for a
			// validator) needs a gossip entrypoint; validators never use the
			// Lightbringer sidecar.
			m.pushInput(scrGossip)
		} else {
			m.pushMenu(scrLightbringer) // Full Config lets user enable the sidecar
		}
	case scrGossip:
		if m.nodeType == "validator" {
			m.pushInput(scrIdentityKey)
		} else if m.mode == "quick" {
			m.pushMenu(scrReview)
		} else if m.enableLB {
			m.pushMenu(scrLightbringerQuiet)
		} else {
			m.pushInput(scrStorage)
		}
	case scrIdentityKey:
		m.pushInput(scrVoteKey)
	case scrVoteKey:
		if m.mode == "quick" {
			m.pushMenu(scrReview)
		} else {
			m.pushInput(scrStorage)
		}
	case scrStorage:
		m.pushInput(scrStorageSnap)
	case scrStorageSnap:
		m.pushInput(scrStorageLogs)
	case scrStorageLogs:
		m.pushMenu(scrBootstrap)
	case scrBlockTuning:
		m.pushInput(scrBlockInflight)
	case scrBlockInflight:
		m.pushInput(scrReplay)
	case scrReplay:
		m.pushMenu(scrSnapshot)
	case scrRPCPort:
		m.pushMenu(scrReview)
	}
}

// ── View ────────────────────────────────────────────────────────────────

func (m setupModel) View() string {
	// Skip logo when embedded in dashboard right pane
	banner := ""
	if !m.embedded {
		switch m.screen {
		case scrDone:
			// no banner on done screen
		default:
			banner = renderLogo()
		}
	}

	switch m.screen {
	case scrDone:
		return "\n" + renderDone(m.configPath, m.err) + "\n"

	case scrRPC:
		return banner + "\n" + renderInput("RPC Endpoint",
			"Solana RPC endpoint for fetching blocks and cluster data\n"+
				"Public endpoint works to start · upgrade to private RPC for production",
			m.inputVal, m.inputErr, m.inputCur)

	case scrGossip:
		return banner + "\n" + renderInput("Gossip Entrypoint",
			"IP:port of a Solana validator running gossip\n"+
				"Used to receive shreds from the network",
			m.inputVal, m.inputErr, m.inputCur)

	case scrIdentityKey:
		return banner + "\n" + renderInput("Validator Identity Keypair",
			"Path to the validator identity keypair (Solana keygen JSON)\n"+
				"Signs gossip/turbine identity and, once voting activates, votes",
			m.inputVal, m.inputErr, m.inputCur)

	case scrVoteKey:
		return banner + "\n" + renderInput("Vote Account Keypair",
			"Path to the vote account keypair (Solana keygen JSON)\n"+
				"The vote account votes are cast for · keep the WITHDRAWER keypair offline",
			m.inputVal, m.inputErr, m.inputCur)

	case scrStorage:
		desc := "AccountsDB stores all ~500M on-chain accounts · needs fastest NVMe\n" +
			"Heavy random I/O — put this on your best drive"
		if config.IsProductionLayout(config.StoragePaths{
			Accounts:   m.accountsPath,
			Snapshots:  m.snapshotsPath,
			Logs:       m.logsPath,
			Shredstore: m.shredstorePath,
		}) {
			desc += "\nDefault: production /mnt/* paths (run scripts/disk-setup.sh first)"
		} else {
			desc += "\nDefault: home directory (no /mnt setup detected) — see scripts/disk-setup.sh for production NVMe layout"
		}
		if len(m.disks) > 0 {
			desc += "\n"
			for _, d := range m.disks {
				desc += "› " + d.FormatDiskOption()
			}
		}
		return banner + "\n" + renderInput("AccountsDB Path", desc, m.inputVal, m.inputErr, m.inputCur)

	case scrStorageSnap:
		return banner + "\n" + renderInput("Snapshots Path",
			"Downloaded snapshots for bootstrapping · ~100 GB for full + incremental\n"+
				"Can be on a slower drive than AccountsDB",
			m.inputVal, m.inputErr, m.inputCur)

	case scrStorageLogs:
		return banner + "\n" + renderInput("Logs Path",
			"Runtime logs, replay timings, and diagnostics\n"+
				"Auto-rotated · each run gets its own directory",
			m.inputVal, m.inputErr, m.inputCur)

	case scrBlockTuning:
		return banner + "\n" + renderInput("Max Requests Per Second",
			"How aggressively to fetch blocks from RPC\n"+
				"Match your provider's rate limit · typical: 5–10 for public, 50+ for private",
			m.inputVal, m.inputErr, m.inputCur)

	case scrBlockInflight:
		return banner + "\n" + renderInput("Max Inflight Workers",
			"Concurrent block fetch workers · should match max RPS\n"+
				fmt.Sprintf("Current max RPS: %s", m.blockMaxRPS),
			m.inputVal, m.inputErr, m.inputCur)

	case scrReplay:
		rec := fmt.Sprintf("%d", m.cpuCores*2)
		return banner + "\n" + renderInput("Transaction Parallelism",
			fmt.Sprintf("Parallel workers for block execution\n"+
				"Your system: %d cores · recommended: %s workers (2× cores)", m.cpuCores, rec),
			m.inputVal, m.inputErr, m.inputCur)

	case scrRPCPort:
		return banner + "\n" + renderInput("Mithril RPC Port",
			"JSON-RPC interface for querying Mithril's state\n"+
				"Default: 8899 · set to 0 to disable",
			m.inputVal, m.inputErr, m.inputCur)

	case scrReview:
		nodeType := m.nodeType
		if nodeType == "" {
			nodeType = "verifying"
		}
		nodeTypeLabel := "verifying (non-voting)"
		if nodeType == "validator" {
			nodeTypeLabel = "validator (voting engine not yet active)"
		}
		rows := [][]string{
			{"Node type", nodeTypeLabel},
			{"Cluster", m.cluster},
			{"RPC", m.rpcEndpoint},
		}
		if nodeType == "validator" {
			rows = append(rows,
				[]string{"Identity key", m.identityKey},
				[]string{"Vote key", m.voteKey},
			)
		}
		if m.enableLB {
			summary := "enabled (gossip: " + m.gossipEntry + ")"
			if m.lbQuiet {
				summary += " · quiet logs"
			}
			rows = append(rows, []string{"Lightbringer", summary})
		} else {
			rows = append(rows, []string{"Block source", "turbine (gossip: " + m.gossipEntry + ")"})
		}
		if m.mode == "quick" {
			rows = append(rows, []string{"AccountsDB", m.accountsPath + " (default)"})
			rows = append(rows, []string{"Parallelism", m.txpar + " workers (auto)"})
		} else {
			rows = append(rows, []string{"AccountsDB", m.accountsPath})
			rows = append(rows, []string{"Shredstore", m.shredstorePath})
			rows = append(rows, []string{"Snapshots", m.snapshotsPath})
			rows = append(rows, []string{"Logs", m.logsPath})
			rows = append(rows, []string{"Parallelism", m.txpar + " workers"})
		}
		if m.mode == "full" {
			rows = append(rows, []string{"Bootstrap", m.bootstrapMode})
			rows = append(rows, []string{"Block RPS", m.blockMaxRPS})
			rows = append(rows, []string{"Inflight", m.blockInflight})
			rows = append(rows, []string{"RPC Port", m.rpcPort})
			rows = append(rows, []string{"Snapshot keep", m.snapshotKeep})
			rows = append(rows, []string{"Log Level", m.logLevel})
		}
		review := renderReview("Configuration Review", rows)
		items := m.currentItems()
		menu := renderMenu("", "", items, m.cursor, m.width)
		return banner + "\n" + review + "\n\n" + menu + "\n"

	case scrOverwrite:
		msg := warnStyle.Render("  Config already exists: ") + m.configPath
		items := m.currentItems()
		menu := renderMenu("Overwrite config?", msg, items, m.cursor, m.width)
		return banner + "\n" + menu + "\n"

	default:
		// Menu screens
		title := ""
		desc := ""
		switch m.screen {
		case scrMode:
			title = "Mithril Setup"
			desc = ""
			items := m.currentItems()
			return banner + "\n\n" + renderMenu(title, desc, items, m.cursor, m.width) + "\n"
		case scrCluster:
			title = "Solana Cluster"
		case scrLightbringer:
			title = "Lightbringer Sidecar"
			desc = "Lightbringer sidecar for lower-latency block streaming."
		case scrLightbringerQuiet:
			title = "Lightbringer Log Verbosity"
			desc = "Quiet mode suppresses Lightbringer info/debug logs (only warnings and errors)."
		case scrBootstrap:
			title = "Bootstrap Mode"
			desc = "How Mithril initializes on startup."
		case scrSnapshot:
			title = "Snapshot Storage"
			desc = "How many downloaded snapshots to keep."
		case scrLogLevel:
			title = "Log Level"
		}
		items := m.currentItems()
		return banner + "\n" + renderMenu(title, desc, items, m.cursor, m.width) + "\n"
	}
}

// ── Config Generation ───────────────────────────────────────────────────

func (m setupModel) generateConfig() (tea.Model, tea.Cmd) {
	// If config exists and user hasn't confirmed overwrite yet, ask first
	if _, err := os.Stat(m.configPath); err == nil && m.screen != scrOverwrite {
		m.screen = scrOverwrite
		m.cursor = 0
		return m, nil
	}

	var cfg strings.Builder
	cfg.WriteString("# Mithril Configuration\n")
	cfg.WriteString("# Generated by: mithril setup\n\n")
	cfg.WriteString("name = \"mithril\"\n\n")

	cfg.WriteString("[bootstrap]\n")
	fmt.Fprintf(&cfg, "mode = %q\n\n", m.bootstrapMode)

	cfg.WriteString("[storage]\n")
	fmt.Fprintf(&cfg, "accounts = %q\n", filepath.Clean(m.accountsPath))
	fmt.Fprintf(&cfg, "shredstore = %q\n", filepath.Clean(m.shredstorePath))
	fmt.Fprintf(&cfg, "snapshots = %q\n", filepath.Clean(m.snapshotsPath))
	fmt.Fprintf(&cfg, "logs = %q\n\n", filepath.Clean(m.logsPath))

	cfg.WriteString("[network]\n")
	fmt.Fprintf(&cfg, "cluster = %q\n", m.cluster)
	fmt.Fprintf(&cfg, "rpc = [%q]\n\n", m.rpcEndpoint)

	cfg.WriteString("[block]\n")
	if m.enableLB {
		cfg.WriteString("source = \"lightbringer\"\n")
	} else {
		cfg.WriteString("source = \"turbine\"\n")
		cfg.WriteString("turbine_bind_addr = \"0.0.0.0:8001\"\n")
	}
	fmt.Fprintf(&cfg, "max_rps = %s\n", m.blockMaxRPS)
	fmt.Fprintf(&cfg, "max_inflight = %s\n\n", m.blockInflight)
	if !m.enableLB {
		cfg.WriteString("[turbine]\n")
		fmt.Fprintf(&cfg, "gossip_entrypoint = %q\n\n", m.gossipEntry)
	}

	if m.enableLB {
		cfg.WriteString("[lightbringer]\n")
		cfg.WriteString("enabled = true\n")
		cfg.WriteString("binary_path = \"./lightbringer\"\n")
		fmt.Fprintf(&cfg, "gossip_entrypoint = %q\n", m.gossipEntry)
		cfg.WriteString("grpc_addr = \"127.0.0.1:3001\"\n")
		cfg.WriteString("rpc_addr = \"127.0.0.1:3000\"\n")
		fmt.Fprintf(&cfg, "quiet = %t\n", m.lbQuiet)
		cfg.WriteString("\n")
	}

	cfg.WriteString("[tuning]\n")
	fmt.Fprintf(&cfg, "txpar = %s\n\n", m.txpar)

	if m.nodeType == "validator" {
		cfg.WriteString("[validator]\n")
		fmt.Fprintf(&cfg, "identity_keypair = %q\n", m.identityKey)
		fmt.Fprintf(&cfg, "vote_account_keypair = %q\n", m.voteKey)
		cfg.WriteString("# Keep the authorized withdrawer keypair OFFLINE — not needed at runtime.\n")
		cfg.WriteString("authorized_withdrawer_keypair = \"\"\n\n")

		cfg.WriteString("[consensus]\n")
		cfg.WriteString("# Validator mode: voting engine not yet active — runs verify-only until it lands.\n")
		cfg.WriteString("mode = \"validator\"\n")
		cfg.WriteString("alpenglow_observer_bind_addr = \"0.0.0.0:8010\" # REQUIRED: Votor QUIC vote/cert listener\n")
		cfg.WriteString("alpenglow_max_message_bytes = 0\n")
		cfg.WriteString("alpenglow_bls_dst = \"\"\n\n")
	} else {
		cfg.WriteString("[validator]\n")
		cfg.WriteString("identity_keypair = \"\"\n")
		cfg.WriteString("vote_account_keypair = \"\"\n")
		cfg.WriteString("authorized_withdrawer_keypair = \"\"\n\n")

		cfg.WriteString("[consensus]\n")
		cfg.WriteString("mode = \"verifying\"\n")
		cfg.WriteString("alpenglow_observer_bind_addr = \"\"\n")
		cfg.WriteString("alpenglow_max_message_bytes = 0\n")
		cfg.WriteString("alpenglow_bls_dst = \"\"\n\n")
	}

	cfg.WriteString("[snapshot]\n")
	fmt.Fprintf(&cfg, "max_full_snapshots = %s\n\n", m.snapshotKeep)

	cfg.WriteString("[rpc]\n")
	fmt.Fprintf(&cfg, "port = %s\n\n", m.rpcPort)

	cfg.WriteString("[log]\n")
	fmt.Fprintf(&cfg, "dir = %q\n", filepath.Clean(m.logsPath))
	fmt.Fprintf(&cfg, "level = %q\n", m.logLevel)
	cfg.WriteString("to_stdout = true\n")
	cfg.WriteString("max_size_mb = 100\n")

	if err := tui.AtomicWriteFile(m.configPath, []byte(cfg.String()), 0600); err != nil {
		m.err = err
	}
	m.screen = scrDone
	return m, nil
}

func (m setupModel) generateManual() (tea.Model, tea.Cmd) {
	if _, err := os.Stat(m.configPath); err == nil && m.screen != scrOverwrite {
		m.screen = scrOverwrite
		m.cursor = 0
		return m, nil
	}

	template := fmt.Sprintf(`# Mithril Configuration
# Generated by: mithril setup (manual mode)
# See config.example.toml for detailed documentation of all options.

name = "mithril"

[bootstrap]
mode = "auto"   # "auto" | "snapshot" | "new-snapshot" | "accountsdb"

[storage]
accounts = "/mnt/mithril-accounts"            # AccountsDB (~500GB, use fastest NVMe)
shredstore = "/mnt/mithril-ledger/shredstore" # Lightbringer shred storage
snapshots = "/mnt/mithril-ledger/snapshots"   # ~100GB for full + incremental
logs = "/mnt/mithril-logs"                    # Log files (created if missing)

[network]
cluster = "alpenglow"  # This build boots Alpenglow only (TowerBFT clusters need a dev-branch build)
rpc = ["https://alpenglow.rpcpool.com"]

[block]
# "turbine" is the live mode: shreds carry the Alpenglow block ids and footer
# certificates that gate durable state. "rpc" is catch-up/debug only.
source = "turbine"   # "turbine" (live) | "rpc" (catch-up/debug) | "lightbringer"
turbine_bind_addr = "0.0.0.0:8001"
# lightbringer_endpoint = "localhost:9000"
max_rps = 8
max_inflight = 8

[turbine]
gossip_entrypoint = ""     # REQUIRED for turbine: a gossip entrypoint of your Alpenglow cluster
# gossip_bind_addr = "0.0.0.0:65401"
# advertised_ip = "203.0.113.10"
# shred_version = 0

# [lightbringer]
# enabled = false
# binary_path = "./lightbringer"
# gossip_entrypoint = "1.2.3.4:8000"
# shredstore stored in [storage] section above
# rpc_addr = "127.0.0.1:3000"
# grpc_addr = "127.0.0.1:3001"
# See config.example.toml for full Lightbringer sidecar options.

[tuning]
txpar = %d   # Recommended: 2x your CPU core count

[validator]
identity_keypair = ""              # Optional validator identity for native turbine gossip
vote_account_keypair = ""          # Optional vote account keypair path for diagnostics/future voting
authorized_withdrawer_keypair = "" # Optional authorized withdrawer keypair path for diagnostics

[consensus]
mode = "verifying"                # "verifying" (default, non-voting) | "validator" (requires keypairs + Votor listener; voting engine not yet active)
alpenglow_observer_bind_addr = "" # Optional Votor QUIC listener (raw-vote cert feed)
alpenglow_max_message_bytes = 0   # 0 = default
alpenglow_bls_dst = ""            # BLS DST override (must match cluster solana-bls version)

[snapshot]
max_full_snapshots = 1   # 0 = stream only, saves disk

[rpc]
port = 8899   # Mithril's RPC server (0 = disabled)

[log]
dir = "/mnt/mithril-logs"  # Log files (created if missing)
level = "info"             # "debug" | "info" | "warn" | "error"
to_stdout = true           # Also write to stdout
max_size_mb = 100          # Max log file size before rotation
# max_age_days = 0         # Delete logs older than N days (0/unset = never delete by age)

# Advanced options (defaults work well for most setups)
# See config.example.toml for: [tuning], [debug], [snapshot] tuning, [reporting]
`, runtime.NumCPU()*2)

	if err := tui.AtomicWriteFile(m.configPath, []byte(template), 0600); err != nil {
		m.err = err
	}
	m.screen = scrDone
	return m, nil
}

func runSetup() {
	p := tea.NewProgram(newSetupModel(), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

// NewSetupModel creates a setup model for embedding in the dashboard.
// configPath overrides the output path (pass "" for default).
func NewSetupModel(configPath string) tea.Model {
	m := newSetupModel()
	if configPath != "" {
		m.configPath = configPath
	}
	m.embedded = true // skip logo when inside dashboard
	return m
}

// SetupIsDone returns true if the setup model has reached the done screen.
func SetupIsDone(m tea.Model) bool {
	if sm, ok := m.(setupModel); ok {
		return sm.screen == scrDone
	}
	return false
}

// SetupIsFirstScreen returns true if the setup wizard is on the initial mode selection screen.
func SetupIsFirstScreen(m tea.Model) bool {
	if sm, ok := m.(setupModel); ok {
		return sm.screen == scrMode
	}
	return false
}
