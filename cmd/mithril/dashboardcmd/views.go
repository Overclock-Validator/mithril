package dashboardcmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/tui"
	"github.com/charmbracelet/lipgloss"
)

func (m model) renderRightPane() string {
	if !m.hasConfig {
		return m.renderNoConfig()
	}

	switch m.screen {
	case screenOverview:
		return m.renderOverview()
	case screenProcess:
		return m.renderProcessView()
	case screenConfig:
		return m.renderConfigView()
	case screenEdit:
		return m.renderEditView()
	case screenDoctor:
		return m.renderDoctorView()
	case screenLogs:
		return m.renderLogsView()
	case screenDisk:
		return m.renderDiskView()
	}
	return ""
}

// ── No Config ───────────────────────────────────────────────────────────

func (m model) renderNoConfig() string {
	var b strings.Builder
	title := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	muted := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)
	text := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary)

	b.WriteString(title.Render("No configuration found") + "\n\n")
	b.WriteString(muted.Render("  Looked for: ") + text.Render(m.configFile) + "\n\n")
	b.WriteString(text.Render("  Select 'Create Config' from the menu to create one,") + "\n")
	b.WriteString(text.Render("  or from the command line:") + "\n\n")
	b.WriteString(lipgloss.NewStyle().Foreground(tui.ColorTextSecondary).Render("    $ mithril setup") + "\n")

	return b.String()
}

// ── Overview ────────────────────────────────────────────────────────────

func (m model) renderOverview() string {
	var b strings.Builder
	pass := lipgloss.NewStyle().Foreground(tui.ColorSuccess)
	fail := lipgloss.NewStyle().Foreground(tui.ColorError)
	warn := lipgloss.NewStyle().Foreground(tui.ColorWarn)
	label := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)
	value := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary)
	header := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)

	// Running-state badge, shown before the health summary.
	if m.proc.detection != nil {
		b.WriteString("  " + renderProcessHeadline(*m.proc.detection, m.proc.inFlightOp) + "\n\n")
	}

	// Health summary
	passed := 0
	total := len(m.checks)
	for _, c := range m.checks {
		if c.status == "pass" {
			passed++
		}
	}

	b.WriteString(header.Render("Health") + "\n\n")
	for _, c := range m.checks {
		icon := pass.Render("✓")
		if c.status == "warn" {
			icon = warn.Render("~")
		} else if c.status == "fail" {
			icon = fail.Render("✗")
		}
		b.WriteString("  " + icon + " " + label.Render(c.name) + "\n")
	}
	b.WriteString("\n")
	b.WriteString("  " + label.Render(fmt.Sprintf("%d/%d checks passed", passed, total)) + "\n")
	b.WriteString("\n")

	// Services — only show when node has state (has been started before)
	if m.state != nil {
		b.WriteString(header.Render("Services") + "\n\n")
		for _, svc := range m.services {
			dot := fail.Render("○")
			status := label.Render("down")
			if svc.up {
				dot = pass.Render("●")
				status = pass.Render("up")
			}
			b.WriteString("  " + dot + " " + value.Render(fmt.Sprintf("%-14s", svc.name)) + label.Render(fmt.Sprintf("%-20s", displayServiceAddr(svc.addr))) + status + "\n")
		}
		b.WriteString("\n")
	}

	// Node state
	if m.state != nil && m.state.LastSlot > 0 {
		b.WriteString(header.Render("Node State") + "\n\n")
		// Prefer live replay slot; state file slot is frozen between checkpoints.
		slotVal, epochVal := m.state.LastSlot, m.state.LastEpoch
		liveTag := ""
		if s, e, ok := m.liveNodeSlot(); ok {
			slotVal, epochVal = s, e
			liveTag = lipgloss.NewStyle().Foreground(tui.ColorSuccess).Render("  ● live")
		}
		b.WriteString(label.Render("  Slot        ") + value.Render(formatNumber(slotVal)) + liveTag + "\n")
		b.WriteString(label.Render("  Epoch       ") + value.Render(fmt.Sprintf("%d", epochVal)) + "\n")
		if m.state.SnapshotSlot > 0 {
			b.WriteString(label.Render("  Snapshot    ") + value.Render(formatNumber(m.state.SnapshotSlot)) + "\n")
		}
		// Only show last shutdown when not running; otherwise it's a prior run's exit.
		if m.state.LastShutdownReason != "" && !m.isNodeRunning() {
			reason := value.Render(m.state.LastShutdownReason)
			if m.state.LastShutdownAt != "" {
				when := m.state.LastShutdownAt
				if t, perr := time.Parse(time.RFC3339Nano, m.state.LastShutdownAt); perr == nil {
					when = humanizeAge(t)
				}
				reason += label.Render("  ") + value.Render(when)
			}
			b.WriteString(label.Render("  Shutdown    ") + reason + "\n")
		}
		if m.state.Stage != "" {
			b.WriteString(label.Render("  Stage       ") + value.Render(m.state.Stage) + "\n")
		}
		// Build version that last wrote this state; skipped for dev/unknown builds.
		if v := m.state.LastWriterVersion; v != "" && v != "dev" && v != "unknown" {
			ver := value.Render(v)
			if c := m.state.LastWriterCommit; c != "" && c != "unknown" {
				if len(c) > 8 {
					c = c[:8]
				}
				ver += label.Render(" (") + value.Render(c) + label.Render(")")
			}
			b.WriteString(label.Render("  Writer      ") + ver + "\n")
		}
	}

	// When node hasn't run yet, show next steps instead of empty space
	if m.state == nil && m.cfg != nil {
		cmd := lipgloss.NewStyle().Foreground(tui.ColorTextSecondary)
		b.WriteString("\n")
		b.WriteString(header.Render("Next Steps") + "\n\n")
		b.WriteString(label.Render("  Node has not been started yet.") + "\n\n")
		b.WriteString(label.Render("  1. Review your config       ") + cmd.Render("← Config") + "\n")
		b.WriteString(label.Render("  2. Run health checks        ") + cmd.Render("← Doctor") + "\n")
		b.WriteString(label.Render("  3. Start the node           ") + cmd.Render("← Run Node, choose Start, Enter") + "\n")
	}

	return b.String()
}

// ── Config View ─────────────────────────────────────────────────────────

// configSection holds a parsed TOML section for display.
type configSection struct {
	name string
	keys []string
	vals []string
}

func (m model) renderConfigView() string {
	data, err := os.ReadFile(m.configFile)
	if err != nil {
		return "Could not read config: " + m.configFile
	}

	// Parse TOML into sections
	var sections []configSection
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if sectionName, ok := tomlSectionName(trimmed); ok {
			sections = append(sections, configSection{name: sectionName})
			continue
		}
		if len(sections) > 0 {
			if idx := strings.Index(trimmed, "="); idx > 0 {
				k := strings.TrimSpace(trimmed[:idx])
				raw := strings.TrimSpace(trimmed[idx+1:])
				v := stripTomlQuotes(stripInlineComment(raw))
				s := &sections[len(sections)-1]
				s.keys = append(s.keys, k)
				s.vals = append(s.vals, v)
			}
		}
	}

	sectionStyle := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	keyStyle := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)
	valStyle := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary)
	hintStyle := lipgloss.NewStyle().Foreground(tui.ColorTextDisabled)

	// Pack sections into columns that fit the visible height so the whole config
	// shows without pgdn (-19 leaves room for the blank/hint/trailing rows).
	availHeight := m.height - 19
	if availHeight < 10 {
		availHeight = 10
	}
	paneWidth := m.rightPaneContentWidth()
	return renderConfigColumns(sections, paneWidth, availHeight, sectionStyle, keyStyle, valStyle, hintStyle)
}

// stackedHeight is a column's line count: header + kvs per section, plus a
// blank line between sections.
func stackedHeight(secs []configSection) int {
	h := 0
	for i, s := range secs {
		if i > 0 {
			h++
		}
		h += 1 + len(s.keys)
	}
	return h
}

// packConfigColumns greedily distributes sections into n balanced columns,
// keeping each section intact (never split across a column boundary).
func packConfigColumns(sections []configSection, n int) [][]configSection {
	if n <= 1 {
		return [][]configSection{sections}
	}
	target := (stackedHeight(sections) + n - 1) / n
	var cols [][]configSection
	var cur []configSection
	curLines := 0
	for _, s := range sections {
		sl := 1 + len(s.keys)
		sep := 0
		if curLines > 0 {
			sep = 1
		}
		if curLines > 0 && curLines+sep+sl > target && len(cols) < n-1 {
			cols = append(cols, cur)
			cur, curLines, sep = nil, 0, 0
		}
		cur = append(cur, s)
		curLines += sep + sl
	}
	if len(cur) > 0 {
		cols = append(cols, cur)
	}
	return cols
}

// fillColumns packs sections top-to-bottom, starting a new column only when the
// current one would exceed maxHeight (fills left-first, doesn't balance).
func fillColumns(sections []configSection, maxHeight int) [][]configSection {
	var cols [][]configSection
	var cur []configSection
	curLines := 0
	for _, s := range sections {
		sl := 1 + len(s.keys)
		sep := 0
		if curLines > 0 {
			sep = 1
		}
		if curLines > 0 && curLines+sep+sl > maxHeight {
			cols = append(cols, cur)
			cur, curLines, sep = nil, 0, 0
		}
		cur = append(cur, s)
		curLines += sep + sl
	}
	if len(cur) > 0 {
		cols = append(cols, cur)
	}
	return cols
}

// renderConfigColumns lays sections into as many side-by-side columns as fit
// paneWidth (up to availHeight); never splits a section, falls back to one column.
func renderConfigColumns(sections []configSection, paneWidth, availHeight int, sectionStyle, keyStyle, valStyle, hintStyle lipgloss.Style) string {
	const gap = 3
	// Min column width — small enough that 2-3 columns engage at normal widths.
	const minCol = 24

	maxCols := (paneWidth + gap) / (minCol + gap)
	if maxCols > len(sections) {
		maxCols = len(sections)
	}
	if maxCols < 1 {
		maxCols = 1
	}
	// Fill columns top-to-bottom up to availHeight before spilling right.
	cols := fillColumns(sections, availHeight)
	if len(cols) > maxCols {
		// Too tall to fit the width at this height; pack into the columns the
		// width allows (the only case that can still scroll).
		cols = packConfigColumns(sections, maxCols)
	}
	n := len(cols)
	if n < 1 {
		n = 1
	}
	colWidth := (paneWidth - gap*(n-1)) / n
	if colWidth < 1 {
		colWidth = paneWidth
	}

	rendered := make([][]string, n)
	colW := make([]int, n) // each column's natural width (its longest line), so columns pack flush
	rows := 0
	for i := range cols {
		rendered[i] = renderConfigColumn(cols[i], colWidth, sectionStyle, keyStyle, valStyle)
		for _, ln := range rendered[i] {
			if w := lipgloss.Width(ln); w > colW[i] {
				colW[i] = w
			}
		}
		if len(rendered[i]) > rows {
			rows = len(rendered[i])
		}
	}

	gapStr := strings.Repeat(" ", gap)
	var b strings.Builder
	for r := 0; r < rows; r++ {
		var row strings.Builder
		for i := 0; i < n; i++ {
			cell := ""
			if r < len(rendered[i]) {
				cell = rendered[i][r] // already truncated to <= colWidth, never exceeds the pane
			}
			if i < n-1 { // pad to THIS column's natural width so the next column sits flush, no wide gap
				if pad := colW[i] - lipgloss.Width(cell); pad > 0 {
					cell += strings.Repeat(" ", pad)
				}
				cell += gapStr
			}
			row.WriteString(cell)
		}
		b.WriteString(strings.TrimRight(row.String(), " ") + "\n")
	}
	b.WriteString("\n")
	ks := lipgloss.NewStyle().Foreground(tui.MithrilTeal)
	b.WriteString("  " + ks.Render("e") + hintStyle.Render(" edit") + "  " + ks.Render("r") + hintStyle.Render(" refresh") + "\n")
	return b.String()
}

// renderConfigColumn renders sections as lines for a single column.
// Dynamically calculates key padding from the longest key in the column.
func renderConfigColumn(secs []configSection, colWidth int, sectionStyle, keyStyle, valStyle lipgloss.Style) []string {
	// Find longest key for proper alignment
	keyPad := 12
	for _, s := range secs {
		for _, k := range s.keys {
			if len(k)+2 > keyPad { // +2 for indent
				keyPad = len(k) + 2
			}
		}
	}
	// Cap key padding — leave room for values
	maxKeyPad := colWidth * 45 / 100
	if keyPad > maxKeyPad {
		keyPad = maxKeyPad
	}

	maxVal := colWidth - keyPad - 3
	if maxVal < 8 {
		maxVal = 8
	}

	var lines []string
	for i, s := range secs {
		if i > 0 {
			lines = append(lines, "") // breathing room between sections
		}
		lines = append(lines, sectionStyle.Render(s.name))
		for j := range s.keys {
			v := s.vals[j]
			v = displayConfigValue(s.name, s.keys[j], v)
			// Mask sensitive values (tokens, secrets, passwords)
			k := strings.ToLower(s.keys[j])
			if strings.Contains(k, "token") || strings.Contains(k, "secret") || strings.Contains(k, "password") {
				// Rune-safe: byte slicing could split a multibyte rune in a
				// pasted token and emit invalid UTF-8.
				if rv := []rune(v); len(rv) > 4 {
					v = string(rv[:2]) + strings.Repeat("*", len(rv)-4) + string(rv[len(rv)-2:])
				} else if len(rv) > 0 {
					v = "****"
				}
			}
			if rv := []rune(v); len(rv) > maxVal {
				v = string(rv[:maxVal-1]) + "…" // rune-safe, single-char ellipsis
			}
			lines = append(lines, "  "+keyStyle.Render(fmt.Sprintf("%-*s ", keyPad, s.keys[j]))+valStyle.Render(v))
		}
	}
	return lines
}

func displayConfigValue(section, key, value string) string {
	if !shouldRedactFieldValue(section, key) {
		return value
	}
	fullKey := strings.ToLower(section + "." + key)
	switch {
	case fullKey == "network.rpc":
		return redactEndpointListForDisplay(value)
	case strings.Contains(fullKey, "endpoint"):
		return redactEndpointListForDisplay(value)
	case strings.Contains(fullKey, "rpc") && strings.Contains(value, "://"):
		return redactEndpointListForDisplay(value)
	default:
		return value
	}
}

func displayServiceAddr(addr string) string {
	return config.RedactSecretsInText(config.RedactEndpointForDisplay(addr))
}

func shouldRedactFieldValue(section, key string) bool {
	fullKey := strings.ToLower(section + "." + key)
	return fullKey == "network.rpc" ||
		strings.Contains(fullKey, "endpoint") ||
		strings.Contains(fullKey, "rpc")
}

func redactEndpointListForDisplay(value string) string {
	parts := strings.Split(value, ",")
	for i, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		parts[i] = strings.Replace(part, trimmed, config.RedactEndpointForDisplay(trimmed), 1)
	}
	return strings.Join(parts, ",")
}

// stripInlineComment removes the inline comment from a TOML value.
// e.g., `"auto"   # some comment` → `"auto"`
func stripInlineComment(v string) string {
	// Don't strip # inside quoted strings
	inQuote := false
	for i, c := range v {
		if c == '"' {
			inQuote = !inQuote
		}
		if c == '#' && !inQuote {
			return strings.TrimSpace(v[:i])
		}
	}
	return v
}

// stripTomlQuotes removes TOML string quotes and array brackets for display.
func stripTomlQuotes(v string) string {
	// Array of strings: ["value"] or ["v1", "v2"]
	if strings.HasPrefix(v, "[") && strings.HasSuffix(v, "]") {
		inner := v[1 : len(v)-1]
		// Split by comma and clean each element
		parts := strings.Split(inner, ",")
		var clean []string
		for _, p := range parts {
			p = strings.TrimSpace(p)
			p = strings.Trim(p, "\"")
			if p != "" {
				clean = append(clean, p)
			}
		}
		return strings.Join(clean, ", ")
	}
	// Simple quoted string
	if strings.HasPrefix(v, "\"") && strings.HasSuffix(v, "\"") && len(v) >= 2 {
		return v[1 : len(v)-1]
	}
	return v
}

// ── Edit View (inline config editing) ───────────────────────────────

func (m model) renderEditView() string {
	if m.cfg == nil {
		return "No config loaded."
	}

	// When actively editing a field, show a focused full-pane view
	if m.editMode != editNone && m.editIdx < len(m.editFields) {
		return m.renderEditFocused()
	}

	// Otherwise show the compact field list
	return m.renderEditList()
}

// fieldHelp returns a one-line plain-language explanation of a config field,
// shown while editing and under the highlighted list row.
func fieldHelp(section, key string) string {
	switch section + "." + key {
	case "network.cluster":
		return "Which Solana network to follow: mainnet-beta, devnet, or testnet."
	case "network.rpc":
		return "URL of a Solana RPC provider used to fetch blocks. Paste your provider's URL."
	case "storage.accounts":
		return "Folder for the account database (large — put it on your fastest disk)."
	case "storage.snapshots":
		return "Folder for downloaded snapshot files used to bootstrap a fresh start."
	case "storage.shredstore":
		return "Folder where raw block data (shreds) is stored."
	case "storage.logs":
		return "Folder where log files are written."
	case "block.source":
		return "Where blocks come from: 'rpc' (an RPC provider) or 'lightbringer' (peer-to-peer sidecar)."
	case "block.turbine_bind_addr":
		return "Local UDP address to receive blocks directly from the network (turbine mode)."
	case "turbine.gossip_entrypoint":
		return "host:port of a known Solana node used to join the network (turbine mode)."
	case "turbine.gossip_bind_addr":
		return "Local UDP address for network gossip traffic (turbine mode)."
	case "turbine.advertised_ip":
		return "Your machine's public IP so peers can reach you (turbine mode)."
	case "turbine.shred_version":
		return "Network data-format version. Leave 0 to auto-detect."
	case "block.lightbringer_endpoint":
		return "host:port of an already-running external Lightbringer sidecar."
	case "block.max_rps":
		return "Max requests per second to the RPC provider (lower it to avoid rate limits)."
	case "block.max_inflight":
		return "How many blocks to fetch in parallel."
	case "lightbringer.enabled":
		return "Let Mithril start and manage a Lightbringer sidecar for you."
	case "lightbringer.binary_path":
		return "Path to the lightbringer program file."
	case "lightbringer.config_dir":
		return "Folder where Mithril writes Lightbringer's config and data."
	case "lightbringer.gossip_entrypoint":
		return "host:port of a Solana node for Lightbringer to join the network."
	case "lightbringer.gossip_port":
		return "Inbound UDP port for Lightbringer gossip — open this in your firewall."
	case "lightbringer.port_range_start", "lightbringer.port_range_end":
		return "Inbound UDP port range for Lightbringer — open this range in your firewall."
	case "lightbringer.grpc_addr":
		return "Local address where Mithril reads blocks from Lightbringer."
	case "lightbringer.rpc_addr":
		return "Local address for Lightbringer's HTTP interface."
	case "lightbringer.quiet":
		return "Hide Lightbringer's detailed logs (less noise)."
	case "tuning.txpar":
		return "Parallel workers for replaying blocks. Empty = sequential (slower, simplest)."
	case "rpc.port":
		return "Port for Mithril's own RPC server. It listens on all interfaces — firewall it if public."
	case "log.level":
		return "How much detail to log: info, debug, warn, or error."
	case "bootstrap.mode":
		return "How to start: 'auto' reuses local data, or downloads a snapshot if needed."
	}
	return ""
}

// renderEditList shows all fields in a compact scrollable list.
func (m model) renderEditList() string {
	label := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)
	value := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary)
	active := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	hint := lipgloss.NewStyle().Foreground(tui.ColorTextDisabled)

	var lines []string
	selectedStart := 0

	for i, f := range m.editFields {
		if f.isSep {
			lines = append(lines, "")
			continue
		}

		isSelected := i == m.editIdx
		val := m.getFieldValue(f)

		// Auto-detect indicator for unset txpar
		displayVal := val
		if displayVal != "" {
			displayVal = displayConfigValue(f.section, f.key, displayVal)
		}
		isAuto := val == "" && f.section == "tuning" && f.key == "txpar"
		if isAuto {
			displayVal = "not set (sequential)"
		}
		if rv := []rune(displayVal); len(rv) > 35 {
			displayVal = string(rv[:32]) + "..." // rune-safe truncation
		}

		if isSelected {
			selectedStart = len(lines)
			if isAuto {
				lines = append(lines, active.Render(fmt.Sprintf("  ▸ %-22s", f.label))+hint.Render(displayVal))
			} else {
				lines = append(lines, active.Render(fmt.Sprintf("  ▸ %-22s", f.label))+value.Render(displayVal))
			}
		} else {
			if isAuto {
				lines = append(lines, label.Render(fmt.Sprintf("    %-22s", f.label))+hint.Render(displayVal))
			} else {
				lines = append(lines, label.Render(fmt.Sprintf("    %-22s", f.label))+value.Render(displayVal))
			}
		}
	}

	// Auto-scroll: show a window centered on the selected field
	maxVisible := m.height - 20
	if maxVisible < 10 {
		maxVisible = 10
	}
	scrollStart := 0
	if len(lines) > maxVisible {
		scrollStart = selectedStart - maxVisible/3
		if scrollStart < 0 {
			scrollStart = 0
		}
		end := scrollStart + maxVisible
		if end > len(lines) {
			end = len(lines)
			scrollStart = end - maxVisible
			if scrollStart < 0 {
				scrollStart = 0
			}
		}
		lines = lines[scrollStart:end]
	}
	// Row of the selected field within the now-visible window.
	selectedRow := selectedStart - scrollStart

	// Wide panes pair the highlighted field with a details column; narrow panes
	// just show the list.
	rightPaneWidth := (m.width - 3) * 78 / 100
	detailW := rightPaneWidth / 3
	if detailW > 32 {
		detailW = 32
	}
	if rightPaneWidth < 70 || detailW < 20 {
		return strings.Join(lines, "\n") + "\n"
	}
	colGap := 3
	listW := rightPaneWidth - detailW - colGap

	var right []string
	if m.editIdx >= 0 && m.editIdx < len(m.editFields) {
		f := m.editFields[m.editIdx]
		detail := []string{active.Render(f.label), ""}
		if h := fieldHelp(f.section, f.key); h != "" {
			for _, ln := range wrapRunDetailLines([]string{h}, detailW) {
				detail = append(detail, hint.Render(ln))
			}
		}
		// Align the detail with the selected row, clamped to the visible height.
		top := selectedRow
		if top+len(detail) > len(lines) {
			top = len(lines) - len(detail)
		}
		if top < 0 {
			top = 0
		}
		right = make([]string, top)
		right = append(right, detail...)
	}

	maxRows := len(lines)
	if len(right) > maxRows {
		maxRows = len(right)
	}
	trunc := lipgloss.NewStyle().MaxWidth(listW)
	gap := strings.Repeat(" ", colGap)
	var b strings.Builder
	for i := 0; i < maxRows; i++ {
		l := ""
		if i < len(lines) {
			l = trunc.Render(lines[i])
		}
		r := ""
		if i < len(right) {
			r = right[i]
		}
		if pad := listW - lipgloss.Width(l); pad > 0 {
			l += strings.Repeat(" ", pad)
		}
		b.WriteString(l + gap + r + "\n")
	}
	return b.String()
}

// renderEditFocused shows a single field's edit UI in the full right pane.
func (m model) renderEditFocused() string {
	f := m.editFields[m.editIdx]
	redactValue := shouldRedactFieldValue(f.section, f.key)
	titleStyle := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	subtitleStyle := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)
	valueStyle := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary)
	activeStyle := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	hintStyle := lipgloss.NewStyle().Foreground(tui.ColorTextDisabled)
	errorStyle := lipgloss.NewStyle().Foreground(tui.ColorError)
	keyStyle := lipgloss.NewStyle().Foreground(tui.MithrilTeal)

	var b strings.Builder

	// ── Title block ──
	b.WriteString("\n")
	b.WriteString("  " + titleStyle.Render(f.label) + "\n")
	b.WriteString("  " + subtitleStyle.Render(f.section+"."+f.key) + "\n")
	if h := fieldHelp(f.section, f.key); h != "" {
		b.WriteString("  " + hintStyle.Render(h) + "\n")
	}
	b.WriteString("\n")

	if m.editMode == editMenu {
		// ── Menu options ──
		maxLabel := 0
		for _, opt := range m.editOptions {
			if len(opt.label) > maxLabel {
				maxLabel = len(opt.label)
			}
		}

		for j, opt := range m.editOptions {
			padded := fmt.Sprintf("%-*s", maxLabel+2, opt.label)
			if j == m.editOptCursor {
				line := "  " + activeStyle.Render("▸ "+padded)
				if opt.desc != "" {
					line += subtitleStyle.Render(opt.desc)
				}
				b.WriteString(line + "\n")
			} else {
				line := "    " + valueStyle.Render(padded)
				if opt.desc != "" {
					line += subtitleStyle.Render(opt.desc)
				}
				b.WriteString(line + "\n")
			}
		}

		b.WriteString("\n")
		if m.editErr != "" {
			b.WriteString("  " + errorStyle.Render("✗ "+m.editErr) + "\n\n")
		}

		// ── Hints ──
		b.WriteString("  " + keyStyle.Render("↑↓") + hintStyle.Render(" select") +
			"    " + keyStyle.Render("⏎") + hintStyle.Render(" confirm") +
			"    " + keyStyle.Render("esc") + hintStyle.Render(" cancel") + "\n")

	} else if m.editMode == editText {
		// ── Current value ──
		currentVal := m.getFieldValue(f)
		isAuto := currentVal == "" && f.section == "tuning" && f.key == "txpar"
		if isAuto {
			b.WriteString("  " + subtitleStyle.Render("Current: ") + hintStyle.Render("not set (sequential)") + "\n")
		} else if currentVal != "" {
			if redactValue {
				currentVal = displayConfigValue(f.section, f.key, currentVal)
			}
			b.WriteString("  " + subtitleStyle.Render("Current: ") + valueStyle.Render(currentVal) + "\n")
		}
		b.WriteString("\n")

		// ── Input field ──
		text := m.editValue
		if redactValue {
			text = displayConfigValue(f.section, f.key, text)
		} else if m.editCursor >= 0 && m.editCursor <= len(text) {
			before := text[:m.editCursor]
			after := text[m.editCursor:]
			cur := lipgloss.NewStyle().Background(tui.MithrilTeal).Foreground(lipgloss.Color("#000000")).Render(" ")
			if m.editCursor < len(text) {
				// Decode a full rune under the cursor, not a single byte, so a
				// multibyte character isn't split into mojibake.
				r, sz := utf8.DecodeRuneInString(after)
				cur = lipgloss.NewStyle().Background(tui.MithrilTeal).Foreground(lipgloss.Color("#000000")).Render(string(r))
				after = after[sz:]
			}
			text = before + cur + after
		}

		prompt := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Render("  ❯ ")
		b.WriteString(prompt + text + "\n")

		underLen := len(m.editValue) + 4
		if underLen < 30 {
			underLen = 30
		}
		b.WriteString("  " + lipgloss.NewStyle().Foreground(tui.ColorBorder).Render(strings.Repeat("─", underLen)) + "\n")

		// ── Contextual hint ──
		if isAuto && m.editValue == "" {
			b.WriteString("\n  " + hintStyle.Render("Leave empty for sequential mode (0), or set worker count") + "\n")
		}
		if redactValue {
			b.WriteString("\n  " + hintStyle.Render("Sensitive URL values are hidden while editing; saving preserves the full value.") + "\n")
		}

		b.WriteString("\n")
		if m.editErr != "" {
			b.WriteString("  " + errorStyle.Render("✗ "+m.editErr) + "\n\n")
		}

		// ── Hints ──
		b.WriteString("  " + keyStyle.Render("⏎") + hintStyle.Render(" save") +
			"    " + keyStyle.Render("esc") + hintStyle.Render(" cancel") +
			"    " + keyStyle.Render("←→") + hintStyle.Render(" cursor") + "\n")
	}

	return b.String()
}

// ── Doctor View ─────────────────────────────────────────────────────────

func (m model) renderDoctorView() string {
	var b strings.Builder
	pass := lipgloss.NewStyle().Foreground(tui.ColorSuccess)
	fail := lipgloss.NewStyle().Foreground(tui.ColorError)
	warn := lipgloss.NewStyle().Foreground(tui.ColorWarn)
	label := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)

	passed := 0
	total := len(m.checks)

	for _, c := range m.checks {
		icon := pass.Render("✓")
		if c.status == "warn" {
			icon = warn.Render("~")
		} else if c.status == "fail" {
			icon = fail.Render("✗")
		}
		b.WriteString("  " + icon + "  " + lipgloss.NewStyle().Foreground(tui.ColorTextPrimary).Render(fmt.Sprintf("%-20s", c.name)) + label.Render(c.msg) + "\n")
		if c.status == "pass" {
			passed++
		}
	}

	b.WriteString("\n")
	summary := fmt.Sprintf("  %d/%d checks passed", passed, total)
	if passed == total {
		b.WriteString(pass.Render(summary+" — ready to run!") + "\n")
	} else {
		b.WriteString(warn.Render(summary) + "\n")
	}

	b.WriteString("\n")
	k := lipgloss.NewStyle().Foreground(tui.MithrilTeal)
	hint := lipgloss.NewStyle().Foreground(tui.ColorTextDisabled)
	b.WriteString("  " + k.Render("r") + hint.Render(" re-run checks") + "\n")

	return b.String()
}

// ── Logs View ───────────────────────────────────────────────────────────

func (m model) renderLogsView() string {
	if m.logRawMode || !m.hasLightbringerLogPane() || m.rightPaneContentWidth() < 88 {
		return m.renderRawLogsView()
	}
	if len(m.mithrilLines) == 0 && len(m.lbLines) == 0 {
		mutedStyle := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)
		cmdStyle := lipgloss.NewStyle().Foreground(tui.ColorTextSecondary)
		controls := m.renderLogControlsLine()
		if controls != "" {
			controls += "\n\n"
		}
		return controls + mutedStyle.Render("  Logs will appear here after starting the node.") + "\n\n" +
			cmdStyle.Render("    Open Run Node, choose Start, then press Enter.") + "\n"
	}

	titleStyle := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	hintStyle := lipgloss.NewStyle().Foreground(tui.ColorTextDisabled)

	// Calculate column widths
	rightPaneWidth := m.rightPaneContentWidth()
	colGap := 3
	colWidth := (rightPaneWidth - colGap) / 2
	if colWidth < 24 {
		return m.renderRawLogsView()
	}

	// Full-width snapshot-download bar. When shown, drop the raw progress
	// samples from the mithril column so they don't duplicate it.
	downloadBlock := renderDownloadProgress(m.mithrilLines, rightPaneWidth)
	mithrilSrc := m.mithrilLines
	if downloadBlock != "" {
		mithrilSrc = filterDownloadProgressLines(m.mithrilLines)
	}

	// Redact before wrapping so a long URL can't split a secret across lines.
	mLines := wrapLogLines(redactLogLines(mithrilSrc), colWidth)
	lLines := wrapLogLines(redactLogLines(m.lbLines), colWidth)
	maxRows := len(mLines)
	if len(lLines) > maxRows {
		maxRows = len(lLines)
	}
	availHeight := m.height - 22
	if downloadBlock != "" {
		// Reserve rows for the bar so the log area shrinks instead of overflowing.
		availHeight -= strings.Count(downloadBlock, "\n")
	}
	if availHeight < 5 {
		availHeight = 5
	}
	if maxRows > availHeight {
		maxRows = availHeight
	}
	mScroll, lScroll := 0, 0
	if m.logFocused {
		if m.logPane == logPaneMithril {
			mScroll = m.logScroll
		} else {
			lScroll = m.logScroll
		}
	}
	mLines = visibleLogWindow(mLines, maxRows, mScroll)
	lLines = visibleLogWindow(lLines, maxRows, lScroll)

	divStyle := lipgloss.NewStyle().Foreground(tui.ColorBorder)
	div := divStyle.Render("│")

	// Headers — underline the focused pane title
	var b strings.Builder
	if controls := m.renderLogControlsLine(); controls != "" {
		b.WriteString(controls + "\n\n")
	}
	if downloadBlock != "" {
		b.WriteString(downloadBlock + "\n")
	}
	var mTitle, lTitle string
	mTitleStyle := titleStyle
	lTitleStyle := titleStyle
	if m.logFocused && m.logPane == logPaneMithril {
		mTitleStyle = lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true).Underline(true)
	} else if m.logFocused && m.logPane == logPaneLightbringer {
		lTitleStyle = lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true).Underline(true)
	}
	mTitle = mTitleStyle.Render("mithril")
	lTitle = lTitleStyle.Render(m.lightbringerLogTitle())

	b.WriteString(padStyledLine(mTitle, colWidth) + " " + div + " " + padStyledLine(lTitle, colWidth) + "\n")

	// Divider line under headers
	b.WriteString(divStyle.Render(strings.Repeat("─", colWidth)) + " " + div + " " + divStyle.Render(strings.Repeat("─", colWidth)) + "\n")

	// Active pane line highlight style
	activeLineStyle := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary)

	for i := 0; i < maxRows; i++ {
		left := ""
		right := ""

		if i < len(mLines) {
			if m.logFocused && m.logPane == logPaneMithril {
				// Active pane: brighter text
				left = activeLineStyle.Render(redactLogLine(mLines[i]))
			} else {
				left = colorLogLine(mLines[i])
			}
		}
		if i < len(lLines) {
			if m.logFocused && m.logPane == logPaneLightbringer {
				right = activeLineStyle.Render(redactLogLine(lLines[i]))
			} else {
				right = colorLogLine(lLines[i])
			}
		}

		left = padStyledLine(left, colWidth)
		right = padStyledLine(right, colWidth)
		b.WriteString(left + " " + div + " " + right + "\n")
	}

	if m.logFocused {
		b.WriteString("\n" + hintStyle.Render("  ↑↓ scroll  ←→ switch  t full-width  esc menu  q quit") + "\n")
	}

	return b.String()
}

func (m model) renderRawLogsView() string {
	return m.renderRawLogsViewWith(m.rightPaneContentWidth(), m.rawLogRows(false))
}

func (m model) renderRawLogsViewWith(contentWidth, logRows int) string {
	titleStyle := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	mutedStyle := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)
	hintStyle := lipgloss.NewStyle().Foreground(tui.ColorTextDisabled)
	divStyle := lipgloss.NewStyle().Foreground(tui.ColorBorder)

	if contentWidth < 10 {
		contentWidth = 10
	}
	if logRows < 5 {
		logRows = 5
	}

	// Snapshot-download progress bar above the tail; reserve rows for it and
	// strip the raw progress samples from the scrolling text.
	downloadBlock := renderDownloadProgress(m.mithrilLines, contentWidth)
	rawSrc := m.combinedRawLogLines()
	if downloadBlock != "" {
		rawSrc = filterDownloadProgressLines(rawSrc)
		logRows -= strings.Count(downloadBlock, "\n")
		if logRows < 5 {
			logRows = 5
		}
	}
	lines := dashboardRawLogLines(redactLogLines(rawSrc), contentWidth)
	lines = visibleLogWindow(lines, logRows, m.logScroll)

	var b strings.Builder
	subtitle := "  Mithril live tail"
	if m.hasLightbringerLogPane() {
		subtitle = "  same run, grouped by source"
	}
	b.WriteString(titleStyle.Render("terminal logs") + mutedStyle.Render(subtitle) + "\n")
	if controls := m.renderLogControlsLine(); controls != "" {
		b.WriteString(controls + "\n")
	}
	if downloadBlock != "" {
		b.WriteString(downloadBlock + "\n")
	}
	b.WriteString(divStyle.Render(strings.Repeat("─", contentWidth)) + "\n")
	for _, line := range lines {
		b.WriteString(padStyledLine(colorLogLine(line), contentWidth) + "\n")
	}
	tHint := "t full-width"
	if m.logRawMode {
		tHint = "t exit full-width"
	}
	scrollHint := "enter scroll"
	if m.logFocused {
		scrollHint = "↑↓ scroll"
	}
	b.WriteString("\n" + hintStyle.Render("  "+scrollHint+"  "+tHint+"  esc menu  q quit") + "\n")
	return b.String()
}

func (m model) renderLogControlsLine() string {
	if !m.logsStopShortcutAvailable() {
		return ""
	}
	keyStyle := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	hintStyle := lipgloss.NewStyle().Foreground(tui.ColorTextDisabled)
	parts := []string{keyStyle.Render("x") + hintStyle.Render(" stop safely")}
	if m.hasLightbringerLogPane() {
		viewHint := "terminal view"
		if m.logRawMode {
			viewHint = "split view"
		}
		parts = append(parts, keyStyle.Render("t")+hintStyle.Render(" "+viewHint))
	}
	return "  " + strings.Join(parts, hintStyle.Render("   "))
}

func (m model) combinedRawLogLines() []string {
	showSources := m.hasLightbringerLogPane()
	if showSources {
		combined := interleavedSourceLogLines(m.mithrilLines, m.lbLines)
		if len(combined) == 0 {
			return []string{"(no log lines yet)"}
		}
		return combined
	}

	var combined []string
	for _, line := range m.mithrilLines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		combined = append(combined, line)
	}
	if len(combined) == 0 {
		return []string{"(no log lines yet)"}
	}
	return combined
}

func interleavedSourceLogLines(mithrilLines, lbLines []string) []string {
	mLines := nonEmptyLogLines(mithrilLines)
	lLines := nonEmptyLogLines(lbLines)
	maxLen := len(mLines)
	if len(lLines) > maxLen {
		maxLen = len(lLines)
	}
	if maxLen == 0 {
		return nil
	}

	combined := make([]string, 0, len(mLines)+len(lLines))
	mStart := maxLen - len(mLines)
	lStart := maxLen - len(lLines)
	for i := 0; i < maxLen; i++ {
		if i >= mStart {
			combined = append(combined, "[mithril] "+mLines[i-mStart])
		}
		if i >= lStart {
			combined = append(combined, "[lightbringer] "+lLines[i-lStart])
		}
	}
	return combined
}

func nonEmptyLogLines(lines []string) []string {
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		out = append(out, line)
	}
	return out
}

func visibleLogWindow(lines []string, height int, scrollBack int) []string {
	if height <= 0 || len(lines) == 0 {
		return nil
	}
	if len(lines) <= height {
		return lines
	}
	maxStart := len(lines) - height
	if scrollBack < 0 {
		scrollBack = 0
	}
	start := maxStart - scrollBack
	if start < 0 {
		start = 0
	}
	end := start + height
	if end > len(lines) {
		end = len(lines)
	}
	return lines[start:end]
}

func (m model) maxLogScroll() int {
	lineCount := m.currentLogLineCount()
	_, logRows := m.rawLogLayout()
	if lineCount <= logRows {
		return 0
	}
	return lineCount - logRows
}

func (m model) currentLogLineCount() int {
	width, _ := m.rawLogLayout()
	var lines []string
	if m.logRawMode || !m.hasLightbringerLogPane() || width < 88 {
		lines = dashboardRawLogLines(redactLogLines(m.combinedRawLogLines()), width)
	} else {
		colWidth := (width - 3) / 2
		if colWidth < 24 {
			lines = dashboardRawLogLines(redactLogLines(m.combinedRawLogLines()), width)
		} else if m.logPane == logPaneLightbringer {
			lines = wrapLogLines(redactLogLines(m.lbLines), colWidth)
		} else {
			lines = wrapLogLines(redactLogLines(m.mithrilLines), colWidth)
		}
	}
	return len(lines)
}

func (m model) rawLogLayout() (int, int) {
	if m.fullWidthRawLogs() {
		return m.fullPaneContentWidth(), m.rawLogRows(true)
	}
	return m.rightPaneContentWidth(), m.rawLogRows(false)
}

func (m model) rawLogRows(fullWidth bool) int {
	rows := m.height - 22
	if fullWidth {
		rows = m.height - 21
	}
	if rows < 5 {
		return 5
	}
	return rows
}

func (m model) hasLightbringerLogPane() bool {
	if m.cfg == nil {
		return false
	}
	return m.cfg.lbEnabled || (m.cfg.blockSource == "lightbringer" && m.cfg.lbExternalEndpoint != "")
}

func (m model) rightPaneContentWidth() int {
	if m.width <= 0 {
		return 80
	}
	if m.width < 60 {
		width := m.width - 2
		if width < 10 {
			return 10
		}
		return width
	}
	innerWidth := m.width - 3
	leftWidth := innerWidth * 22 / 100
	rightWidth := innerWidth - leftWidth
	if rightWidth < 10 {
		return 10
	}
	return rightWidth
}

func (m model) fullPaneContentWidth() int {
	width := m.width - 4
	if width < 10 {
		return 10
	}
	return width
}

func (m model) fullWidthRawLogs() bool {
	return m.mode == modeDashboard &&
		m.screen == screenLogs &&
		m.logRawMode &&
		!m.startFlow.active &&
		!m.confirmActive
}

func colorLogLine(line string) string {
	line = redactLogLine(line)
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return ""
	}
	switch {
	case strings.Contains(line, " WARN ") || strings.Contains(line, " warn ") || strings.HasPrefix(trimmed, "WARN"):
		return lipgloss.NewStyle().Foreground(tui.ColorWarn).Render(line)
	case strings.Contains(line, " ERROR ") || strings.Contains(line, " error ") || strings.HasPrefix(trimmed, "ERROR") || strings.Contains(line, "FATAL"):
		return lipgloss.NewStyle().Foreground(tui.ColorError).Render(line)
	default:
		return lipgloss.NewStyle().Foreground(tui.ColorTextMuted).Render(line)
	}
}

func redactLogLine(line string) string {
	return config.RedactSecretsInText(sanitizeTerminalLogLine(line))
}

func redactLogLines(lines []string) []string {
	if len(lines) == 0 {
		return nil
	}
	redacted := make([]string, len(lines))
	for i, line := range lines {
		redacted[i] = redactLogLine(line)
	}
	return redacted
}

func dashboardRawLogLines(lines []string, width int) []string {
	if width < 10 {
		width = 10
	}
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = compactReplaySlotLogLine(line, width)
		out = append(out, wrapLogLines([]string{line}, width)...)
	}
	return out
}

func compactReplaySlotLogLine(line string, width int) string {
	source, rest := splitKnownLogSource(line)
	if !strings.Contains(rest, " slot ") || !strings.Contains(rest, "| leader:") || !strings.Contains(rest, "| txns:") {
		return line
	}

	parts := strings.Split(rest, "|")
	if len(parts) < 5 {
		return line
	}

	ts, slot, ok := parseSlotLogHead(parts[0])
	if !ok {
		return line
	}

	txns := ""
	cu := ""
	exec := ""
	for _, part := range parts[1:] {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "txns:"):
			txns = compactTxnsField(part)
		case strings.HasPrefix(part, "cu:"):
			cu = compactCUField(part)
		case strings.HasPrefix(part, "exec:"):
			exec = compactValueField(part)
		}
	}
	if txns == "" || exec == "" {
		return line
	}

	prefix := source
	if width < 90 {
		prefix = compactLogSource(source)
	}
	head := strings.TrimSpace(prefix + ts)
	candidates := [][]string{
		{compactSlotHead(head, formatSlotForDisplay(slot)), txns, exec, cu},
		{compactSlotHead(head, slot), txns, exec, cu},
		{compactSlotHead(head, slot), txns, exec},
		{"slot " + slot, txns, exec},
	}
	for _, candidate := range candidates {
		candidate = nonEmptyLogLines(candidate)
		joined := strings.Join(candidate, " | ")
		if lipgloss.Width(joined) <= width {
			return joined
		}
	}
	return strings.Join(nonEmptyLogLines([]string{"slot " + slot, txns, exec}), " | ")
}

func compactSlotHead(head, slot string) string {
	if head == "" {
		return "slot " + slot
	}
	return head + " slot " + slot
}

func splitKnownLogSource(line string) (string, string) {
	for _, source := range []string{"[mithril] ", "[lightbringer] "} {
		if strings.HasPrefix(line, source) {
			return source, strings.TrimSpace(strings.TrimPrefix(line, source))
		}
	}
	return "", line
}

func compactLogSource(source string) string {
	switch source {
	case "[mithril] ":
		return "[m] "
	case "[lightbringer] ":
		return "[lb] "
	default:
		return source
	}
}

func parseSlotLogHead(head string) (timestamp, slot string, ok bool) {
	head = strings.TrimSpace(head)
	slotIdx := strings.Index(head, "slot ")
	if slotIdx < 0 {
		return "", "", false
	}
	timestamp = strings.Join(strings.Fields(strings.TrimSpace(head[:slotIdx])), "")
	fields := strings.Fields(head[slotIdx:])
	if len(fields) < 2 {
		return "", "", false
	}
	slot = fields[1]
	return timestamp, slot, true
}

func compactTxnsField(field string) string {
	vote := ""
	nonVote := ""
	for _, part := range strings.Fields(field) {
		switch {
		case strings.HasPrefix(part, "v:"):
			vote = strings.TrimPrefix(part, "v:")
		case strings.HasPrefix(part, "nv:"):
			nonVote = strings.TrimPrefix(part, "nv:")
		}
	}
	if vote == "" && nonVote == "" {
		return ""
	}
	if vote == "" {
		return "txns nv" + nonVote
	}
	if nonVote == "" {
		return "txns v" + vote
	}
	return "txns v" + vote + "/nv" + nonVote
}

func compactCUField(field string) string {
	value := strings.TrimSpace(strings.TrimPrefix(field, "cu:"))
	n, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return "cu " + value
	}
	if n >= 1_000_000 {
		return fmt.Sprintf("cu %.1fM", n/1_000_000)
	}
	return "cu " + strconv.FormatFloat(n, 'f', 0, 64)
}

func compactValueField(field string) string {
	fields := strings.Fields(field)
	if len(fields) < 2 {
		return strings.TrimSuffix(field, ":")
	}
	return strings.TrimSuffix(fields[0], ":") + " " + fields[1]
}

func formatSlotForDisplay(slot string) string {
	n, err := strconv.ParseInt(slot, 10, 64)
	if err != nil {
		return slot
	}
	raw := strconv.FormatInt(n, 10)
	var b strings.Builder
	for i, r := range raw {
		if i > 0 && (len(raw)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func (m model) lightbringerLogTitle() string {
	if m.cfg == nil {
		return "lightbringer"
	}
	if m.cfg.blockSource == "lightbringer" && m.cfg.lbExternalEndpoint != "" && !m.cfg.lbEnabled {
		return "lightbringer (external)"
	}
	if m.cfg.lbEnabled {
		return "lightbringer (managed)"
	}
	return "lightbringer"
}

// wrapLogLines wraps each line to fit within the given width.
// Continuation lines are indented with 2 spaces for readability.
func wrapLogLines(lines []string, width int) []string {
	if width < 10 {
		width = 10
	}
	var result []string
	for _, line := range lines {
		if lipgloss.Width(line) <= width {
			result = append(result, line)
			continue
		}
		// First chunk at full width, continuations indented
		chunk, remaining := splitDisplayWidth(line, width)
		result = append(result, chunk)
		contWidth := width - 2 // indent continuation
		for len(remaining) > 0 {
			if lipgloss.Width(remaining) <= contWidth {
				result = append(result, "  "+remaining)
				break
			}
			chunk, remaining = splitDisplayWidth(remaining, contWidth)
			result = append(result, "  "+chunk)
		}
	}
	return result
}

func splitDisplayWidth(s string, width int) (string, string) {
	if width <= 0 || s == "" {
		return "", s
	}
	used := 0
	cut := 0
	lastSpaceCut := 0
	lastSpaceWidth := 0
	for i, r := range s {
		rw := lipgloss.Width(string(r))
		if used+rw > width {
			break
		}
		used += rw
		cut = i + utf8.RuneLen(r)
		if r == ' ' || r == '\t' {
			lastSpaceCut = cut
			lastSpaceWidth = used
		}
	}
	if cut <= 0 {
		_, size := utf8.DecodeRuneInString(s)
		if size <= 0 {
			return "", ""
		}
		cut = size
	}
	if cut < len(s) && lastSpaceCut > 0 && lastSpaceWidth >= width/2 {
		return strings.TrimRight(s[:lastSpaceCut], " \t"), strings.TrimLeft(s[lastSpaceCut:], " \t")
	}
	return s[:cut], s[cut:]
}

// ── Disk View ───────────────────────────────────────────────────────────

func (m model) renderDiskView() string {
	if len(m.disks) == 0 {
		muted := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)
		if !m.disksLoaded {
			return muted.Render("  Loading disk usage...") + "\n"
		}
		if m.cfg != nil && m.cfg.accountsPath != "" {
			return muted.Render("  No disk data available.") + "\n\n" +
				muted.Render("  Storage paths may not exist on this machine yet.") + "\n" +
				muted.Render("  Disk usage will appear once the node has been started.") + "\n"
		}
		return muted.Render("  Disk usage will appear once storage paths are configured.") + "\n"
	}

	var b strings.Builder
	label := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)
	value := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary)

	for _, d := range m.disks {
		b.WriteString(lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true).Render(d.label) + "\n")
		b.WriteString(label.Render("  "+d.path) + "\n")
		b.WriteString("  " + value.Render(fmt.Sprintf("%dG / %dG  %d%%", d.used, d.total, d.pct)) + "\n")
		b.WriteString("  " + renderBarChart(d.used, d.total, 30) + "\n")
		b.WriteString("\n")
	}

	b.WriteString(lipgloss.NewStyle().Foreground(tui.ColorTextDisabled).Render("  Normal: <80%  Warning: 80-90%  Critical: >90%") + "\n")
	b.WriteString(lipgloss.NewStyle().Foreground(tui.ColorTextDisabled).Render("  Press 'r' to refresh") + "\n")

	return b.String()
}

// Setup is launched directly as an embedded child TUI via selectCurrent().
