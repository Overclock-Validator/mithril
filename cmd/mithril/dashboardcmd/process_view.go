package dashboardcmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/procctl"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/tui"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const (
	runActionStart   = "start"
	runActionStop    = "stop"
	runActionRestart = "restart"
	runActionForce   = "force"
	runActionLogs    = "logs"
	runActionRawLogs = "raw_logs"
	runActionDoctor  = "doctor"
	runActionEdit    = "edit"
	runActionSafe    = "safe_folders"
	runActionInPlace = "rebuild_in_place"
)

type runAction struct {
	id       string
	label    string
	desc     string
	key      string
	danger   bool
	disabled bool
}

type startFlowState struct {
	active bool
	step   int
	onDone func(*model) tea.Cmd
}

type startFlowCard struct {
	kicker   string
	title    string
	detail   string
	positive bool
	warn     bool
}

// renderProcessView renders the "Run Node" view: status badge, then a two-column
// body (actions left, activity/status right). Narrow panes stack into one column.
func (m model) renderProcessView() string {
	var b strings.Builder
	label := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)
	value := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary)
	dim := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)

	// PID file unreadable (corrupt, permission denied, etc.).
	if m.proc.fetchErr != "" {
		warn := lipgloss.NewStyle().Foreground(tui.ColorError)
		b.WriteString("  " + warn.Render("⚠ ") + value.Render("Cannot determine status") + "\n\n")
		b.WriteString("  " + dim.Render(m.proc.fetchErr) + "\n")
		return b.String()
	}

	// Loading path: first tick hasn't arrived yet.
	if m.proc.detection == nil {
		b.WriteString("  " + dim.Render("Loading process status…") + "\n")
		return b.String()
	}

	det := m.proc.detection
	b.WriteString("  " + renderProcessHeadline(*det, m.proc.inFlightOp) + "\n")

	// Start failure: keep directly under the badge so it isn't pushed offscreen.
	if m.proc.startFailStderr != "" {
		warn := lipgloss.NewStyle().Foreground(tui.ColorError)
		b.WriteString("  " + warn.Render("Start failed — last output from mithril:") + "\n")
		for _, line := range strings.Split(strings.TrimSpace(m.proc.startFailStderr), "\n") {
			b.WriteString("    " + dim.Render(redactLogLine(line)) + "\n")
		}
		b.WriteString("\n")
	}

	// Guided-fix failure (e.g. safe folders refused — disk too small).
	if m.proc.configFixErr != "" {
		warnHdr := lipgloss.NewStyle().Foreground(tui.ColorWarn).Bold(true)
		warnBody := lipgloss.NewStyle().Foreground(tui.ColorWarn)
		b.WriteString("  " + warnHdr.Render("⚠ Couldn't apply the fix") + "\n")
		for _, line := range wrapRunDetailLines([]string{m.proc.configFixErr}, m.runPanelContentWidth()) {
			b.WriteString("    " + warnBody.Render(line) + "\n")
		}
		b.WriteString("    " + warnBody.Render("Set storage to a larger disk in Edit Config, then try again.") + "\n\n")
	}

	// Body: two columns when wide, stacked when narrow.
	cw := m.runPanelContentWidth()
	actions := m.runActions()
	if len(actions) > 0 {
		selected := m.runActionIdx
		if selected < 0 || selected >= len(actions) {
			selected = 0
		}
		if cw >= 72 {
			leftWidth, rightWidth := runColumnWidths(cw)
			left := m.actionColumnLines(actions, selected, leftWidth)
			right := m.infoColumnLines(*det, rightWidth)
			b.WriteString(joinColumns(left, right, leftWidth, rightWidth))
		} else {
			for _, line := range m.actionColumnLines(actions, selected, cw) {
				b.WriteString("  " + strings.TrimRight(line, " ") + "\n")
			}
			if info := m.infoColumnLines(*det, cw); len(info) > 0 {
				b.WriteString("\n")
				for _, line := range info {
					b.WriteString("  " + strings.TrimRight(line, " ") + "\n")
				}
			}
		}
		b.WriteString("\n")

		if diskBlock := renderDiskSafety(m.disks, label, value, dim); diskBlock != "" {
			b.WriteString(diskBlock)
			b.WriteString("\n")
		}
	}

	// Stuck: prior Stop timed out, process still alive.
	if m.proc.stuck && m.proc.inFlightOp == "" {
		warn := lipgloss.NewStyle().Foreground(tui.ColorWarn).Bold(true)
		b.WriteString("\n")
		b.WriteString("  " + warn.Render("⚠ Stop timed out — mithril is taking longer than expected.") + "\n")
		b.WriteString("  " + dim.Render("This is normal during AccountsDB rebuild (can take 30+ min).") + "\n")
		b.WriteString("  " + dim.Render("Wait, or use [f] Force Stop (data loss risk).") + "\n")
	}

	// In-flight action: progress block + dynamic status line.
	if m.proc.inFlightOp != "" {
		b.WriteString("\n")
		b.WriteString(renderActionProgress(m.proc, dim, value))
	}

	// Pre-flight / control refusal — surfaced verbatim.
	if m.proc.preflightInfo != "" {
		ok := lipgloss.NewStyle().Foreground(tui.ColorSuccess).Bold(true)
		b.WriteString("\n")
		b.WriteString("  " + ok.Render("Safe folders are ready") + "\n")
		for _, line := range wrapRunDetailLines([]string{m.proc.preflightInfo}, m.runPanelContentWidth()) {
			b.WriteString("  " + value.Render(line) + "\n")
		}
	}
	if m.shouldShowPreflightBlock(det.Status) {
		warn := lipgloss.NewStyle().Foreground(tui.ColorError).Bold(true)
		b.WriteString("\n")
		b.WriteString("  " + warn.Render("Setup needs attention before Start") + "\n")
		for _, line := range strings.Split(m.proc.preflightErr, "\n") {
			if strings.TrimSpace(line) == "" {
				b.WriteString("\n")
				continue
			}
			for _, wrapped := range wrapRunDetailLines([]string{line}, m.runPanelContentWidth()) {
				b.WriteString("  " + value.Render(wrapped) + "\n")
			}
		}
	}

	if !m.proc.fetchedAt.IsZero() {
		b.WriteString("\n")
		b.WriteString("  " + dim.Render(fmt.Sprintf("Refreshed %s", humanizeAge(m.proc.fetchedAt))) + "\n")
	}
	return b.String()
}

func (m model) shouldShowPreflightBlock(status procctl.Status) bool {
	return m.proc.preflightErr != "" &&
		m.proc.inFlightOp == "" &&
		(status == procctl.StatusStopped || status == procctl.StatusCrashed)
}

// runColumnWidths splits contentWidth into left (actions) + 3-cell divider +
// right (activity/status), summing exactly so the right edge stays flush.
func runColumnWidths(contentWidth int) (left, right int) {
	left = contentWidth * 38 / 100
	if left < 26 {
		left = 26
	}
	if left > 36 {
		left = 36
	}
	right = contentWidth - left - 3
	if right < 24 {
		right = 24
	}
	return left, right
}

// joinColumns lays two width-exact line slices side by side with a vertical
// divider, padding the shorter column. Cells must be pre-constrained; no truncation.
func joinColumns(left, right []string, leftWidth, rightWidth int) string {
	rows := len(left)
	if len(right) > rows {
		rows = len(right)
	}
	bar := lipgloss.NewStyle().Foreground(tui.ColorBorder).Render("│")
	leftBlank := strings.Repeat(" ", leftWidth)
	rightBlank := strings.Repeat(" ", rightWidth)

	var b strings.Builder
	for i := 0; i < rows; i++ {
		l := leftBlank
		if i < len(left) {
			l = left[i]
		}
		r := rightBlank
		if i < len(right) {
			r = right[i]
		}
		b.WriteString("  " + l + " " + bar + " " + r + "\n")
	}
	return b.String()
}

// actionColumnLines builds the left column: the action list plus the selected
// action's hint, every line padded to width.
func (m model) actionColumnLines(actions []runAction, selected, width int) []string {
	head := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	value := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary)
	dim := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)
	muted := lipgloss.NewStyle().Foreground(tui.ColorTextDisabled)
	selectedStyle := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	dangerStyle := lipgloss.NewStyle().Foreground(tui.ColorError).Bold(true)

	var lines []string
	add := func(style lipgloss.Style, text string) {
		lines = append(lines, fixedWidthRender(style, text, width))
	}

	add(head, "Choose action")
	for i, action := range actions {
		marker := "  "
		titleStyle := value
		if m.runFocused && i == selected {
			marker = "▶ "
			titleStyle = selectedStyle
		}
		if action.danger {
			titleStyle = dangerStyle
		}
		if action.disabled {
			titleStyle = muted
		}
		add(titleStyle, marker+action.label)
	}

	// Selected action's hint, paired with the highlighted row above it.
	detail := wrapRunDetailLines(m.runActionDetailParagraphs(actions[selected]), width)
	if len(detail) > 0 {
		lines = append(lines, strings.Repeat(" ", width)) // breathing room
		for i, line := range detail {
			st := dim
			if i == 0 {
				st = value
			}
			add(st, line)
		}
	}
	return lines
}

// infoColumnLines builds the right column: activity feed above per-status
// detail. Returns width-exact lines ready for joinColumns.
func (m model) infoColumnLines(det procctl.Detection, width int) []string {
	lines := m.activityFeedLines(det.Status, width)
	detail := m.statusDetailLines(det, width)
	if len(detail) > 0 {
		if len(lines) > 0 {
			lines = append(lines, strings.Repeat(" ", width))
		}
		lines = append(lines, detail...)
	}
	return lines
}

// activityFeedLines renders recent progress events, only when the node is live,
// finishing, or erroring — otherwise an idle screen shows stale history.
func (m model) activityFeedLines(status procctl.Status, width int) []string {
	events := m.progress
	if len(events) == 0 {
		return nil
	}
	show := status == procctl.StatusRunning || m.proc.inFlightOp != ""
	if !show {
		last := events[len(events)-1]
		show = last.Status == "error" || last.Status == "warn" ||
			last.Phase == "completed" || last.Phase == "shutdown"
	}
	if !show {
		return nil
	}

	const maxRows = 5
	start := len(events) - maxRows
	if start < 0 {
		start = 0
	}
	recent := events[start:]
	current := recent[len(recent)-1]

	head := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	value := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary)
	dim := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)

	var lines []string
	add := func(style lipgloss.Style, text string) {
		lines = append(lines, fixedWidthRender(style, text, width))
	}

	add(head, "Recent activity")
	for i, ev := range recent {
		text := progressEventSymbol(ev) + " " + progressEventTitle(ev) + progressEventDetail(ev)
		style := dim
		if i == len(recent)-1 {
			style = value
			if !ev.TS.IsZero() {
				text += "  " + humanizeAge(ev.TS)
			}
		}
		add(style, text)
	}
	if summary := bootstrapActivitySummary(m.snapshot, m.accounts, current); summary != "" {
		for _, line := range wrapRunDetailLines([]string{summary}, width) {
			add(dim, line)
		}
	}
	return lines
}

// statusDetailLines renders the per-status detail for the right column:
// session (running), last exit (stopped), or crash guidance (crashed).
func (m model) statusDetailLines(det procctl.Detection, width int) []string {
	head := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	value := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary)
	label := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)
	dim := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)

	var lines []string
	add := func(style lipgloss.Style, text string) {
		lines = append(lines, fixedWidthRender(style, text, width))
	}
	addWrapped := func(style lipgloss.Style, text string) {
		for _, line := range wrapRunDetailLines([]string{text}, width) {
			add(style, line)
		}
	}

	switch det.Status {
	case procctl.StatusRunning:
		add(head, "Session")
		if det.Pid != 0 {
			add(value, fmt.Sprintf("PID %d", det.Pid))
		}
		if det.RunID != "" {
			addWrapped(label, "Session "+det.RunID)
		}
		if det.SpawnedBy != "" {
			addWrapped(label, "Started by "+friendlySpawnedBy(det.SpawnedBy))
		}
		if det.BinaryPath != "" {
			addWrapped(label, "Program "+det.BinaryPath)
		}
		if det.ConfigPath != "" {
			addWrapped(label, "Config "+det.ConfigPath)
		}
		if det.LogDir != "" {
			addWrapped(label, "Logs "+det.LogDir)
		}
		if !det.LockHeld {
			addWrapped(dim, "Live, but the single-instance lock is not held. Start is disabled until this process stops.")
		}
		// Concurrent stop in progress — surface it so Stop isn't hammered.
		if det.StopInProgressBy != 0 {
			owner := "Another dashboard is shutting this node down."
			if det.StopInProgressBy == os.Getpid() {
				owner = "This dashboard is shutting this node down."
			}
			addWrapped(value, owner)
			if !det.StopInProgressAt.IsZero() {
				addWrapped(dim, fmt.Sprintf("(stop by pid %d, %s)", det.StopInProgressBy, humanizeAge(det.StopInProgressAt)))
			}
		}

	case procctl.StatusStopped:
		if isReplayCompleted(det.LastShutdownReason) {
			add(head, "Last run")
			addWrapped(value, "completed configured replay range")
			addWrapped(dim, "A clean finish, not a crash. Safe to start again.")
			return lines
		}
		add(head, "Last exit")
		if det.LastShutdownReason == "" || det.LastShutdownReason == "no state file" {
			addWrapped(dim, "This node has never been started from this dashboard on this machine.")
		} else {
			addWrapped(value, det.LastShutdownReason)
		}
		if det.LastCleanExit {
			addWrapped(dim, "Previous shutdown was clean. Safe to start.")
		} else if det.LastShutdownReason == "no state file" {
			addWrapped(dim, "No shutdown record. Safe to start if this is first setup.")
		}

	case procctl.StatusCrashed:
		add(head, "Last exit")
		if det.LastShutdownReason != "" {
			addWrapped(value, det.LastShutdownReason)
		}
		// Divergence lines kept single-line (tests assert exact phrasing).
		crashText := m.crashDiagnosticText()
		switch {
		case isReplayDivergenceText(crashText) || isReplayDivergenceText(det.LastShutdownReason):
			add(value, "Replay diverged from chain data.")
			add(dim, "Safe path: do not retry this AccountsDB.")
			add(dim, "Next step: rebuild from a fresh snapshot.")
		case isRPCRateLimitOrStall(det.LastShutdownReason):
			addWrapped(dim, "RPC catchup stalled or was rate-limited.")
			addWrapped(dim, "Use a private/dedicated RPC endpoint.")
		default:
			addWrapped(dim, "The node stopped unexpectedly.")
			addWrapped(dim, "Open Doctor before starting again.")
		}
	}
	return lines
}

func (m *model) beginStartFlow(onDone func(*model) tea.Cmd) {
	m.startFlow = startFlowState{
		active: true,
		step:   0,
		onDone: onDone,
	}
	m.rightScroll = 0
}

func (m *model) cancelStartFlow() {
	m.startFlow = startFlowState{}
}

func (m *model) advanceStartFlow() tea.Cmd {
	if !m.startFlow.active {
		return nil
	}
	// Single confirm: one Enter starts.
	onDone := m.startFlow.onDone
	m.cancelStartFlow()
	if onDone != nil {
		return onDone(m)
	}
	return nil
}

// renderStartFlow draws a single compact "Confirm run" card: the few things
// that matter (mode, network, storage, start point) and one Enter to start.
func (m model) renderStartFlow() string {
	header := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	label := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)
	value := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary)
	key := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	hint := lipgloss.NewStyle().Foreground(tui.ColorTextSecondary)
	warn := lipgloss.NewStyle().Foreground(tui.ColorWarn)

	warnBold := lipgloss.NewStyle().Foreground(tui.ColorWarn).Bold(true)

	var b strings.Builder
	row := func(k, v string, st lipgloss.Style) {
		b.WriteString("  " + label.Render(fmt.Sprintf("%-9s", k)) + st.Render(v) + "\n")
	}
	cont := func(v string, st lipgloss.Style) { // continuation line, aligned under a row's value
		b.WriteString("  " + label.Render(fmt.Sprintf("%-9s", "")) + st.Render(v) + "\n")
	}

	// Title + a quiet mode · network subtitle.
	b.WriteString("\n  " + header.Render("Confirm run") + "\n")
	ctx := confirmModeSummary(m.cfg)
	if net := confirmNetwork(m.cfg); net != "" {
		ctx += " · " + net
	}
	b.WriteString("  " + label.Render(ctx) + "\n\n")

	// The plan: where data goes and what Start will do.
	if storage := confirmStorage(m.cfg); storage != "" {
		row("Storage", storage, value)
	}
	if bc := m.startFlowBootstrapCard(); bc != nil {
		st := value
		if bc.warn {
			st = warn
		}
		row("Plan", bc.title, st)
		if bc.detail != "" {
			cont(bc.detail, label)
		}
	}

	// Disk readiness — fresh builds only.
	if m.startFlowWillBuild() && m.cfg != nil && m.cfg.accountsPath != "" &&
		!config.HasExistingAccountsDb(m.cfg.accountsPath) {
		if free, ok := config.FreeDiskBytes(m.cfg.accountsPath); ok {
			// Snapshot on the same disk must hold both at peak.
			need := config.EstimatedAccountsDbBytes(m.cfg.cluster)
			if m.cfg.snapshotsPath != "" && config.SameDisk(m.cfg.accountsPath, m.cfg.snapshotsPath) {
				need = config.EstimatedBuildBytes(m.cfg.cluster)
			}
			freeGB, needGB := free/(1<<30), need/(1<<30)
			b.WriteString("\n")
			if free < need {
				b.WriteString("  " + warnBold.Render("⚠ Low disk space") + "\n")
				b.WriteString("    " + warn.Render(fmt.Sprintf("%d GB free here — the AccountsDB needs ~%d GB.", freeGB, needGB)) + "\n")
				b.WriteString("    " + warn.Render("Point storage at a larger disk, or Start anyway to retry.") + "\n")
			} else {
				row("Disk", fmt.Sprintf("%d GB free — enough for the AccountsDB", freeGB), value)
			}
		}
	}

	// Quiet notes: inform but don't block (UDP ports, RPC exposure, rate-limit).
	var notes []string
	if m.cfg != nil && m.cfg.lbEnabled {
		if ports := managedLightbringerNetworkShort(m.cfg); ports != "" {
			notes = append(notes, ports)
		}
	}
	if exposure := rpcServerExposureSummary(m.cfg); exposure != "" {
		notes = append(notes, exposure)
	}
	if note := rpcRuntimeSummary(m.cfg); note != "" {
		notes = append(notes, note)
	}
	if len(notes) > 0 {
		b.WriteString("\n")
		for _, n := range notes {
			b.WriteString("  " + label.Render("· "+n) + "\n")
		}
	}

	b.WriteString("\n  " + key.Render("⏎") + hint.Render(" Start    ") +
		key.Render("esc") + hint.Render(" Cancel") + "\n")
	return b.String()
}

// confirmModeSummary / confirmNetwork / confirmStorage are the short lines on
// the Confirm-run card.
func confirmModeSummary(cfg *configData) string {
	if cfg == nil {
		return "loading…"
	}
	if cfg.lbEnabled {
		return "Mithril + Lightbringer (managed)"
	}
	if cfg.blockSource == "lightbringer" && cfg.lbExternalEndpoint != "" {
		return "Mithril + external Lightbringer"
	}
	return "Mithril only · blocks from RPC"
}

func confirmNetwork(cfg *configData) string {
	if cfg == nil || cfg.cluster == "" || cfg.cluster == "unknown" {
		return ""
	}
	return cfg.cluster
}

func confirmStorage(cfg *configData) string {
	if cfg == nil {
		return ""
	}
	return cfg.accountsPath
}

// startFlowWillBuild reports whether starting now builds the AccountsDB (vs
// resuming). Gates the disk-readiness row; mirrors startFlowBootstrapCard.
func (m model) startFlowWillBuild() bool {
	if m.cfg == nil {
		return false
	}
	switch strings.TrimSpace(m.cfg.bootstrapMode) {
	case "new-snapshot", "snapshot":
		return true
	case "accountsdb":
		return false
	default: // auto — builds only when there's no existing valid state
		return m.state == nil
	}
}

func (m model) startFlowBootstrapCard() *startFlowCard {
	if m.cfg == nil {
		return nil
	}
	mode := strings.TrimSpace(m.cfg.bootstrapMode)
	if mode == "" {
		mode = "auto"
	}

	switch mode {
	case "new-snapshot":
		detail := "Build a fresh AccountsDB from snapshot."
		if m.state != nil || m.accounts.hasData(m.cfg.accountsPath) {
			detail = "Existing AccountsDB will be replaced."
		}
		return &startFlowCard{
			kicker: "Local data",
			title:  "Fresh rebuild",
			detail: detail,
			warn:   true,
		}
	case "snapshot":
		return &startFlowCard{
			kicker: "Local data",
			title:  "Rebuild from snapshot",
			detail: "AccountsDB will be rebuilt before replay.",
			warn:   true,
		}
	case "accountsdb":
		return &startFlowCard{
			kicker: "Local data",
			title:  "Use existing AccountsDB",
			detail: "Start fails fast if local state is missing.",
		}
	default:
		if m.state != nil {
			return &startFlowCard{
				kicker: "Local data",
				title:  "Resume existing state",
				detail: "Auto mode reuses valid AccountsDB.",
			}
		}
		return &startFlowCard{
			kicker: "Local data",
			title:  "Create local state",
			detail: "Auto mode downloads a snapshot if needed.",
		}
	}
}

func managedLightbringerNetworkShort(cfg *configData) string {
	if cfg == nil {
		return "HTTP/gRPC stay local."
	}
	gossipPort, rangeStart, rangeEnd, err := parseLightbringerGossipPorts(cfg.lbGossipPort, cfg.lbPortRangeStart, cfg.lbPortRangeEnd)
	if err != nil {
		return "Check UDP ports in config."
	}
	return fmt.Sprintf("Open inbound UDP %d and %d-%d.", gossipPort, rangeStart, rangeEnd)
}

func (m model) runPanelContentWidth() int {
	if m.width <= 0 {
		return 60
	}
	if m.width < 60 {
		return m.width - 4
	}
	innerWidth := m.width - 3
	leftWidth := innerWidth * 22 / 100
	rightWidth := innerWidth - leftWidth
	contentWidth := rightWidth - 2 // renderProcessView uses a two-space left inset.
	if contentWidth < 42 {
		return 42
	}
	return contentWidth
}

func (m model) runActionDetailParagraphs(action runAction) []string {
	lines := []string{}
	if action.desc != "" {
		lines = append(lines, action.desc)
	}
	if action.id == runActionStart {
		lines = append(lines, runModeSummary(m.cfg))
	}
	if action.id == runActionRawLogs {
		lines = append(lines, "Full-width raw tail. Same run, same log files.")
	}
	if action.id == runActionEdit {
		lines = append(lines, "Review run mode, RPC, storage, and Lightbringer ports.")
	}
	if action.id == runActionSafe {
		lines = append(lines, "Creates user-owned folders and updates storage paths. Existing data is not deleted.")
		if m.hasReplayDivergenceCrash() {
			lines = append(lines, "This avoids reusing the diverged AccountsDB that just crashed.")
		}
		if root := safeStorageRootForDisplay(m.cfg); root != "" {
			line := "Target: " + root
			// Show free space before confirming, so the operator sees if the
			// target disk can hold the AccountsDB.
			if free, ok := pathFreeGB(root); ok {
				if need := estimatedAccountsDbGB(m.cfg); free < need {
					line += fmt.Sprintf("  ⚠ only %dG free — needs ~%dG; pick a larger disk", free, need)
				} else {
					line += fmt.Sprintf("  (%dG free)", free)
				}
			}
			lines = append(lines, line)
		}
	}
	if action.disabled {
		lines = append(lines, "Wait for the current operation to finish.")
	}
	if m.runFocused {
		lines = append(lines, runActionEnterHint(action.id)+" Esc back.")
	} else {
		lines = append(lines, "Use → to focus actions.")
	}
	return lines
}

func runActionEnterHint(actionID string) string {
	switch actionID {
	case runActionStart:
		return "Enter to start."
	case runActionStop:
		return "Enter to stop."
	case runActionRestart:
		return "Enter to restart."
	case runActionForce:
		return "Enter to force stop."
	case runActionLogs, runActionRawLogs:
		return "Enter to open."
	case runActionDoctor:
		return "Enter to check."
	case runActionEdit:
		return "Enter to edit."
	case runActionSafe:
		return "Enter to fix."
	default:
		return "Enter to continue."
	}
}

func runModeSummary(cfg *configData) string {
	if cfg == nil {
		return "Run mode comes from your config."
	}
	if cfg.lbEnabled {
		return "Mithril with a managed Lightbringer sidecar."
	}
	if cfg.blockSource == "lightbringer" && cfg.lbExternalEndpoint != "" {
		return "Mithril with an external Lightbringer."
	}
	if cfg.blockSource == "rpc" || cfg.blockSource == "" {
		return "Mithril alone — blocks come from your RPC provider."
	}
	return "Set Block Source in your config."
}

func wrapRunDetailLines(paragraphs []string, width int) []string {
	if width <= 0 {
		return nil
	}
	var lines []string
	for _, paragraph := range paragraphs {
		words := strings.Fields(paragraph)
		if len(words) == 0 {
			continue
		}
		line := words[0]
		for _, word := range words[1:] {
			if lipgloss.Width(line)+1+lipgloss.Width(word) > width {
				lines = append(lines, line)
				line = word
				continue
			}
			line += " " + word
		}
		lines = append(lines, line)
	}
	return lines
}

func fixedWidthRender(style lipgloss.Style, text string, width int) string {
	if width <= 0 {
		return ""
	}
	rendered := style.MaxWidth(width).Render(text)
	pad := width - lipgloss.Width(rendered)
	if pad > 0 {
		rendered += strings.Repeat(" ", pad)
	}
	return rendered
}

func (m model) runActions() []runAction {
	if m.proc.detection == nil {
		return nil
	}
	busy := m.proc.inFlightOp != ""
	switch m.proc.detection.Status {
	case procctl.StatusRunning:
		actions := []runAction{
			{id: runActionLogs, label: "Watch live logs", desc: "Open Mithril and Lightbringer logs."},
			{id: runActionRawLogs, label: "Terminal logs", desc: "Full-width live tail."},
			{id: runActionStop, label: "Stop safely", desc: "Clean shutdown with SIGTERM.", key: "x", disabled: busy},
			{id: runActionRestart, label: "Restart node", desc: "Stop cleanly, then start again.", key: "r", disabled: busy},
		}
		if m.proc.stuck {
			actions = append(actions, runAction{
				id: runActionForce, label: "Force stop", desc: "Last resort only. May corrupt AccountsDB.", key: "f", danger: true, disabled: busy,
			})
		}
		return actions
	case procctl.StatusStopped:
		if m.shouldShowPreflightBlock(procctl.StatusStopped) {
			return []runAction{
				{id: runActionSafe, label: "Fix with safe folders", desc: "Recommended. Create user-owned folders and keep old data untouched."},
				{id: runActionEdit, label: "Review config", desc: "Change storage paths manually."},
				{id: runActionDoctor, label: "Check readiness", desc: "Run health checks first."},
				{id: runActionLogs, label: "Open latest logs", desc: "Inspect output from the last run."},
				{id: runActionRawLogs, label: "Terminal logs", desc: "Full-width tail of the last run."},
			}
		}
		return []runAction{
			{id: runActionStart, label: primaryStartLabel(m.cfg), desc: "Run this config; logs open automatically.", key: "s", disabled: busy},
			{id: runActionDoctor, label: "Check readiness", desc: "Run health checks first."},
			{id: runActionEdit, label: "Review config", desc: "Edit run mode, RPC, storage, or ports."},
			{id: runActionLogs, label: "Open latest logs", desc: "Inspect output from the last run."},
			{id: runActionRawLogs, label: "Terminal logs", desc: "Full-width tail of the last run."},
		}
	case procctl.StatusCrashed:
		if m.hasReplayDivergenceCrash() {
			return []runAction{
				{id: runActionSafe, label: "Rebuild fresh safely", desc: "Recommended when a spare disk has room. Build into fresh folders and keep the old AccountsDB untouched."},
				{id: runActionInPlace, label: "Rebuild in place (reclaim disk)", desc: "No spare disk? Wipe the current AccountsDB and rebuild from snapshot using the existing storage paths.", danger: true},
				{id: runActionEdit, label: "Set new snapshot", desc: "Set Bootstrap Mode to new-snapshot or choose fresh storage paths."},
				{id: runActionLogs, label: "Open latest logs", desc: "Inspect the divergence details."},
				{id: runActionRawLogs, label: "Terminal logs", desc: "Read last output full-width."},
				{id: runActionDoctor, label: "Check what happened", desc: "Open health checks before retrying if unsure."},
			}
		}
		if m.shouldShowPreflightBlock(procctl.StatusCrashed) {
			return []runAction{
				{id: runActionSafe, label: "Fix with safe folders", desc: "Recommended. Create user-owned folders and keep old data untouched."},
				{id: runActionLogs, label: "Open latest logs", desc: "Inspect the last run output."},
				{id: runActionRawLogs, label: "Terminal logs", desc: "Read last output full-width."},
				{id: runActionEdit, label: "Review config", desc: "Check settings before retrying."},
				{id: runActionDoctor, label: "Check what happened", desc: "Open health checks before retrying if unsure."},
			}
		}
		return []runAction{
			{id: runActionStart, label: primaryStartLabel(m.cfg), desc: "Start again when the config looks safe.", key: "s", disabled: busy},
			{id: runActionDoctor, label: "Check what happened", desc: "Open health checks before retrying if unsure."},
			{id: runActionLogs, label: "Open latest logs", desc: "Inspect the last run output."},
			{id: runActionRawLogs, label: "Terminal logs", desc: "Read last output full-width."},
			{id: runActionEdit, label: "Review config", desc: "Check settings before retrying."},
		}
	default:
		return []runAction{
			{id: runActionDoctor, label: "Check readiness", desc: "Review health checks."},
			{id: runActionLogs, label: "Open logs", desc: "Inspect recent output."},
			{id: runActionRawLogs, label: "Terminal logs", desc: "Full-width tail."},
		}
	}
}

func (m *model) clampRunActionCursor() {
	actions := m.runActions()
	if len(actions) == 0 {
		m.runActionIdx = 0
		return
	}
	if m.runActionIdx < 0 {
		m.runActionIdx = 0
	}
	if m.runActionIdx >= len(actions) {
		m.runActionIdx = len(actions) - 1
	}
}

func (m *model) moveRunAction(delta int) {
	actions := m.runActions()
	if len(actions) == 0 {
		m.runActionIdx = 0
		return
	}
	m.runActionIdx = (m.runActionIdx + delta + len(actions)) % len(actions)
}

func (m *model) activateRunAction() tea.Cmd {
	actions := m.runActions()
	if len(actions) == 0 {
		return nil
	}
	m.clampRunActionCursor()
	action := actions[m.runActionIdx]
	if action.disabled {
		return nil
	}
	switch action.id {
	case runActionStart:
		return m.handleStartKey()
	case runActionStop:
		return m.handleStopKey()
	case runActionRestart:
		return m.handleRestartKey()
	case runActionForce:
		return m.handleForceStopKey()
	case runActionLogs:
		m.openLogs(false)
		return m.fetchDataCmd()
	case runActionRawLogs:
		m.openLogs(true)
		return m.fetchDataCmd()
	case runActionDoctor:
		m.screen = screenDoctor
		m.runFocused = false
		m.rightScroll = 0
		m.setMenuCursor("doctor")
		return m.fetchDataCmd()
	case runActionEdit:
		m.screen = screenEdit
		m.runFocused = false
		m.editIdx = 0
		m.moveEditCursor(0)
		m.setMenuCursor("edit")
		return nil
	case runActionSafe:
		return m.openSafeFoldersConfirmation()
	case runActionInPlace:
		return m.openRebuildInPlaceConfirmation()
	default:
		return nil
	}
}

func rpcRuntimeSummary(cfg *configData) string {
	if cfg == nil || len(cfg.rpcEndpoints) == 0 {
		return ""
	}
	endpoint := cfg.rpcEndpoints[0]
	if usesLightbringerBlocks(cfg) {
		if isMainnetPublicRPC(cfg, endpoint) {
			return "used for catchup; public mainnet RPC may rate-limit"
		}
		return "used for catchup before Lightbringer live handoff"
	}
	if isMainnetPublicRPC(cfg, endpoint) {
		return "public mainnet RPC may rate-limit on long runs"
	}
	return ""
}

func rpcServerExposureSummary(cfg *configData) string {
	if cfg == nil || strings.TrimSpace(cfg.rpcPort) == "" || strings.TrimSpace(cfg.rpcPort) == "0" {
		return ""
	}
	return "listens on all interfaces :" + strings.TrimSpace(cfg.rpcPort) + "; restrict with firewall if public"
}

// renderActionProgress shows the active operation's name, elapsed time, and any
// recorded status lines.
func renderActionProgress(p procState, dim, value lipgloss.Style) string {
	var b strings.Builder
	label := opLabel(p.inFlightOp)
	elapsed := time.Since(p.opStartedAt).Round(time.Second)
	b.WriteString("  " + value.Render(label) + " " + dim.Render(fmt.Sprintf("(%s elapsed)", elapsed)) + "\n")
	for _, line := range p.progressLines {
		b.WriteString("  " + dim.Render("· "+line) + "\n")
	}
	return b.String()
}

func bootstrapActivitySummary(snapshot snapshotActivity, accounts accountsActivity, current progressEvent) string {
	if current.Phase != "bootstrap_snapshot" || current.Status == "error" {
		return ""
	}

	parts := make([]string, 0, 3)
	if snapshot.active() {
		kind := "Snapshot file"
		if strings.HasPrefix(snapshot.Name, "incremental-snapshot-") {
			kind = "Incremental snapshot"
		}
		if snapshot.Partial {
			kind += " downloading"
		} else {
			kind += " saved"
		}
		parts = append(parts, fmt.Sprintf("%s: %s", kind, formatBytes(snapshot.Bytes)))
		if !snapshot.ModTime.IsZero() {
			parts = append(parts, "snapshot updated "+humanizeAge(snapshot.ModTime))
		}
	}

	if accounts.active() {
		parts = append(parts, "AccountsDB updated "+humanizeAge(accounts.ModTime))
	}

	if len(parts) == 0 {
		return ""
	}
	parts = append(parts, "building AccountsDB")
	return strings.Join(parts, " · ")
}

func renderDiskSafety(disks []diskUsage, label, value, dim lipgloss.Style) string {
	warnings := diskSafetyWarnings(disks)
	if len(warnings) == 0 {
		return ""
	}

	warn := lipgloss.NewStyle().Foreground(tui.ColorWarn).Bold(true)
	var b strings.Builder
	b.WriteString("  " + warn.Render("Disk watch") + "\n")
	for _, d := range warnings {
		free := int64(d.total) - int64(d.used)
		if free < 0 {
			free = 0
		}
		severity := "warning"
		if d.pct >= 90 {
			severity = "critical"
		}
		b.WriteString("  " + label.Render(fmt.Sprintf("%-11s", d.label)) +
			value.Render(fmt.Sprintf("%d%% used", d.pct)) +
			dim.Render(fmt.Sprintf(" · %dG free · %s", free, severity)) + "\n")
	}
	b.WriteString("  " + dim.Render("Snapshot builds can grow quickly; stop safely before the disk is full.") + "\n")
	return b.String()
}

func diskSafetyWarnings(disks []diskUsage) []diskUsage {
	warnings := make([]diskUsage, 0, len(disks))
	for _, d := range disks {
		if d.pct >= 80 {
			warnings = append(warnings, d)
		}
	}
	return warnings
}

func progressEventTitle(ev progressEvent) string {
	if ev.Message != "" {
		return ev.Message
	}
	switch ev.Phase {
	case "starting":
		return "Preparing Mithril"
	case "lightbringer_config":
		return "Preparing Lightbringer"
	case "lightbringer_starting":
		return "Starting Lightbringer"
	case "lightbringer_ready":
		return "Lightbringer ready"
	case "lightbringer_fallback":
		return "Using RPC fallback"
	case "lightbringer_external":
		return "Using external Lightbringer"
	case "bootstrap_checking":
		return "Checking local data"
	case "bootstrap_resume":
		return "Opening existing AccountsDB"
	case "bootstrap_snapshot":
		return "Building AccountsDB from snapshot"
	case "bootstrap_ready":
		return "AccountsDB ready"
	case "replay_starting":
		return "Starting replay"
	case "replay_stopped":
		return "Replay stopped"
	case "shutdown":
		return "Stopped cleanly"
	case "completed":
		return "Completed"
	case "error":
		return "Error"
	default:
		return strings.ReplaceAll(ev.Phase, "_", " ")
	}
}

func progressEventSymbol(ev progressEvent) string {
	switch ev.Status {
	case "ok":
		return "✓"
	case "warn":
		return "!"
	case "error":
		return "x"
	default:
		return "→"
	}
}

func progressEventDetail(ev progressEvent) string {
	if len(ev.Fields) == 0 {
		return ""
	}
	if slot, ok := numericProgressField(ev.Fields, "slot"); ok {
		return fmt.Sprintf("  slot %.0f", slot)
	}
	if slot, ok := numericProgressField(ev.Fields, "start_slot"); ok {
		return fmt.Sprintf("  from %.0f", slot)
	}
	if src, ok := ev.Fields["block_source"].(string); ok && src != "" {
		return "  " + config.RedactSecretsInText(src)
	}
	return ""
}

func numericProgressField(fields map[string]any, key string) (float64, bool) {
	switch v := fields[key].(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

func renderProcessHeadline(det procctl.Detection, inFlightOp string) string {
	switch inFlightOp {
	case opStart:
		return lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true).Render("● Starting")
	case opStop:
		return lipgloss.NewStyle().Foreground(tui.ColorWarn).Bold(true).Render("◆ Stopping")
	case opRestart:
		return lipgloss.NewStyle().Foreground(tui.ColorWarn).Bold(true).Render("◆ Restarting")
	case opForceStop:
		return lipgloss.NewStyle().Foreground(tui.ColorError).Bold(true).Render("◆ Force stopping")
	default:
		if det.Status == procctl.StatusStopped && isReplayCompleted(det.LastShutdownReason) {
			return lipgloss.NewStyle().Foreground(tui.ColorSuccess).Bold(true).Render("✓ Completed")
		}
		return renderProcessStatusBadge(det.Status)
	}
}

// opLabel returns the human-readable label for an inFlightOp value.
func opLabel(op string) string {
	switch op {
	case opStart:
		return "Starting…"
	case opStop:
		return "Stopping…"
	case opRestart:
		return "Restarting…"
	case opForceStop:
		return "Force stopping…"
	default:
		return op
	}
}

// renderActionHints emits the secondary keyboard shortcuts for the current
// status (the action list is the primary interaction).
func renderActionHints(status procctl.Status, inFlightOp string, stuck bool, cfg *configData, dim lipgloss.Style) string {
	if inFlightOp != "" {
		return "  " + dim.Render("(actions disabled until current operation completes)") + "\n"
	}
	key := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	warnKey := lipgloss.NewStyle().Foreground(tui.ColorError).Bold(true)
	switch status {
	case procctl.StatusRunning:
		hints := "  " + dim.Render("Shortcuts: ") + key.Render("[x]") + dim.Render(" Stop safely  ") +
			key.Render("[r]") + dim.Render(" Restart")
		// Stuck = prior Stop timed out; show Force Stop in red as an escalation.
		if stuck {
			hints += "  " + warnKey.Render("[f]") + dim.Render(" Force Stop")
		}
		return hints + "\n"
	case procctl.StatusStopped, procctl.StatusCrashed:
		return "  " + dim.Render("Shortcut: ") + key.Render("[s]") + dim.Render(" "+primaryStartLabel(cfg)) + "\n"
	default:
		return ""
	}
}

func primaryStartLabel(cfg *configData) string {
	if cfg == nil {
		return "Start Mithril"
	}
	if cfg.lbEnabled {
		return "Start Mithril + Lightbringer"
	}
	if cfg.blockSource == "lightbringer" && cfg.lbExternalEndpoint != "" {
		return "Start Mithril with external Lightbringer"
	}
	return "Start Mithril"
}

// renderConfirmModal renders the in-pane confirmation for destructive actions.
// Keys are the buttons: Y/Enter = Yes, N/Esc = No.
func renderConfirmModal(title, body string, width int) string {
	var b strings.Builder
	header := lipgloss.NewStyle().Foreground(tui.ColorWarn).Bold(true)
	value := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary)
	hint := lipgloss.NewStyle().Foreground(tui.ColorTextSecondary)
	key := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	bodyWidth := width - 4
	if bodyWidth < 24 {
		bodyWidth = 24
	}

	b.WriteString("\n")
	b.WriteString("  " + header.Render("◆ "+title) + "\n\n")
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if strings.TrimSpace(line) == "" {
			b.WriteString("\n")
			continue
		}
		for _, wrapped := range wrapRunDetailLines([]string{line}, bodyWidth) {
			b.WriteString("  " + value.Render(wrapped) + "\n")
		}
	}
	b.WriteString("\n")
	if width > 0 && width < 56 {
		b.WriteString("  " + key.Render("[enter]") + hint.Render(" Confirm  ") +
			key.Render("[y]") + hint.Render(" Yes") + "\n")
		b.WriteString("  " + key.Render("[n]") + hint.Render(" No  ") +
			key.Render("[esc]") + hint.Render(" Cancel") + "\n")
	} else {
		b.WriteString("  " + key.Render("[enter]") + hint.Render(" Confirm  ") +
			key.Render("[y]") + hint.Render(" Yes  ") +
			key.Render("[n]") + hint.Render(" No  ") +
			key.Render("[esc]") + hint.Render(" Cancel") + "\n")
	}
	return b.String()
}

// renderProcessStatusBadge produces the colored "● Running" / "○ Stopped" /
// "⚠ Crashed" headline; an unknown status renders "? Unknown".
func renderProcessStatusBadge(status procctl.Status) string {
	pass := lipgloss.NewStyle().Foreground(tui.ColorSuccess).Bold(true)
	warn := lipgloss.NewStyle().Foreground(tui.ColorWarn).Bold(true)
	// Stopped renders muted, not red — a clean shutdown isn't a failure.
	muted := lipgloss.NewStyle().Foreground(tui.ColorTextMuted).Bold(true)
	switch status {
	case procctl.StatusRunning:
		return pass.Render("● Running")
	case procctl.StatusStopped:
		return muted.Render("○ Stopped")
	case procctl.StatusCrashed:
		return warn.Render("⚠ Crashed")
	default:
		return warn.Render("? Unknown")
	}
}

func (m model) hasReplayDivergenceCrash() bool {
	return isReplayDivergenceText(m.crashDiagnosticText())
}

func (m model) crashDiagnosticText() string {
	var parts []string
	if m.proc.startFailStderr != "" {
		parts = append(parts, m.proc.startFailStderr)
	}
	if len(m.mithrilLines) > 0 {
		parts = append(parts, strings.Join(m.mithrilLines, "\n"))
	}
	if m.proc.detection != nil && m.proc.detection.LastShutdownReason != "" {
		parts = append(parts, m.proc.detection.LastShutdownReason)
	}
	return strings.Join(parts, "\n")
}

func isReplayDivergenceText(text string) bool {
	lower := strings.ToLower(text)
	return strings.Contains(lower, "divergence") ||
		strings.Contains(lower, "pre-balance mismatch") ||
		strings.Contains(lower, "pre-balance divergence") ||
		strings.Contains(lower, "return value divergence")
}

func isReplayCompleted(reason string) bool {
	return reason == state.ShutdownReasonCompleted
}

func isRPCRateLimitOrStall(reason string) bool {
	reason = strings.ToLower(reason)
	return strings.Contains(reason, strings.ToLower(state.ShutdownReasonStall)) ||
		strings.Contains(reason, "429") ||
		strings.Contains(reason, "rate limit") ||
		strings.Contains(reason, "rate-limit")
}

// friendlySpawnedBy turns the raw SpawnedBy token into a plain-English phrase,
// falling back to the raw value for unknown markers.
func friendlySpawnedBy(v string) string {
	switch v {
	case "cli":
		return "command line"
	case "dashboard":
		return "this dashboard"
	case "external":
		return "external (started outside the dashboard)"
	default:
		return v
	}
}

// humanizeAge formats a time as "a moment ago" / "5s ago" / "2m ago" / "1h ago".
// The "a moment" case avoids "0s ago" on immediate refresh.
func humanizeAge(t time.Time) string {
	d := time.Since(t).Round(time.Second)
	switch {
	case d < 2*time.Second:
		return "a moment ago"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value := float64(n)
	for _, suffix := range []string{"KB", "MB", "GB", "TB"} {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.1f %s", value, suffix)
		}
	}
	return fmt.Sprintf("%.1f PB", value/unit)
}
