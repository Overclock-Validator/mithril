package dashboardcmd

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/tui"
	"github.com/charmbracelet/lipgloss"
)

// downloadProgress is one parsed snapshot/extract progress sample, e.g.
// "Snapshot: 8.3% 9.0/107.5 GB 89.6 MB/s ETA 18m47s". Rendered as a progress bar.
type downloadProgress struct {
	label   string  // "Snapshot" or "Extract"
	percent float64 // 0..100
	current string  // "9.0"
	total   string  // "107.5"
	unit    string  // "GB"
	rate    string  // "89.6 MB/s"
	eta     string  // "18m47s"
}

// Matches a progress line anywhere in a log line (tolerates a leading
// "(+ 1m03s) " elapsed prefix).
var downloadProgressRe = regexp.MustCompile(
	`(?i)(Snapshot|Extract):\s+([0-9]+(?:\.[0-9]+)?)%\s+` +
		`([0-9]+(?:\.[0-9]+)?)\s*/\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGT]?i?B)\s+` +
		`([0-9]+(?:\.[0-9]+)?\s*[KMGT]?i?B/s)\s+ETA\s+([0-9hms]+)`)

// parseDownloadProgress extracts a progress sample from a single log line.
func parseDownloadProgress(line string) (downloadProgress, bool) {
	m := downloadProgressRe.FindStringSubmatch(line)
	if m == nil {
		return downloadProgress{}, false
	}
	pct, err := strconv.ParseFloat(m[2], 64)
	if err != nil {
		return downloadProgress{}, false
	}
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	label := "Extract"
	if strings.EqualFold(m[1], "snapshot") {
		label = "Snapshot"
	}
	return downloadProgress{
		label:   label,
		percent: pct,
		current: m[3],
		total:   m[4],
		unit:    m[5],
		rate:    strings.Join(strings.Fields(m[6]), " "),
		eta:     m[7],
	}, true
}

// isDownloadProgressLine reports whether a log line is a progress sample.
func isDownloadProgressLine(line string) bool {
	return downloadProgressRe.MatchString(line)
}

// latestDownloadProgress returns the newest Snapshot and Extract samples
// (Snapshot first), omitting labels that don't appear.
func latestDownloadProgress(lines []string) []downloadProgress {
	var snap, ext *downloadProgress
	for i := len(lines) - 1; i >= 0; i-- {
		p, ok := parseDownloadProgress(lines[i])
		if !ok {
			continue
		}
		switch {
		case strings.EqualFold(p.label, "Snapshot") && snap == nil:
			c := p
			snap = &c
		case strings.EqualFold(p.label, "Extract") && ext == nil:
			c := p
			ext = &c
		}
		if snap != nil && ext != nil {
			break
		}
	}
	out := make([]downloadProgress, 0, 2)
	if snap != nil {
		out = append(out, *snap)
	}
	if ext != nil {
		out = append(out, *ext)
	}
	return out
}

// filterDownloadProgressLines drops progress samples from display lines; they
// show as a bar instead, so keeping them would duplicate and flood scrollback.
func filterDownloadProgressLines(lines []string) []string {
	out := lines[:0:0] // new backing array, never mutate caller's slice
	for _, l := range lines {
		if isDownloadProgressLine(l) {
			continue
		}
		out = append(out, l)
	}
	return out
}

const downloadBarLabelWidth = 10 // fits "Snapshot" and "AccountsDB"

// maxDownloadBarWidth caps the filled bar so it stays tidy on wide terminals.
const maxDownloadBarWidth = 48

// renderDownloadBar renders one progress bar within width columns. As width
// shrinks it drops rate/ETA, then byte counts, then the bar itself.
func renderDownloadBar(p downloadProgress, width int) string {
	teal := lipgloss.NewStyle().Foreground(tui.MithrilTeal)
	empty := lipgloss.NewStyle().Foreground(tui.ColorBorder)
	labelSt := lipgloss.NewStyle().Foreground(tui.ColorTextPrimary).Bold(true)
	pctSt := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	statSt := lipgloss.NewStyle().Foreground(tui.ColorTextMuted)

	name := "Snapshot"
	if p.label == "Extract" {
		name = "AccountsDB"
	}
	label := padRightPlain(name, downloadBarLabelWidth)
	pct := fmt.Sprintf("%5.1f%%", p.percent)
	stats := fmt.Sprintf("%s/%s %s", p.current, p.total, p.unit)
	rateEta := fmt.Sprintf("%s · ETA %s", p.rate, p.eta)

	const indent = 2
	// Mandatory: indent + label + " " + bar + " " + pct.
	fixed := indent + lipgloss.Width(label) + 1 + 1 + lipgloss.Width(pct)

	// Trailing detail, only when there's room (most-droppable last).
	suffix := ""
	if width-fixed-2-lipgloss.Width(stats) >= 6 {
		suffix = "  " + stats
		if width-fixed-lipgloss.Width(suffix)-2-lipgloss.Width(rateEta) >= 0 {
			suffix += "  " + rateEta
		}
	}

	barWidth := width - fixed - lipgloss.Width(suffix)
	if barWidth > maxDownloadBarWidth {
		barWidth = maxDownloadBarWidth
	}
	if barWidth < 4 {
		// Too narrow for a bar — compact "label pct", truncated to fit.
		plain := name + " " + strings.TrimSpace(pct)
		avail := width - indent
		if avail < 0 {
			avail = 0
		}
		plain = truncatePlain(plain, avail)
		return strings.Repeat(" ", indent) + labelSt.Render(plain)
	}

	filled := int(float64(barWidth)*p.percent/100.0 + 0.5)
	if filled > barWidth {
		filled = barWidth
	}
	if filled < 0 {
		filled = 0
	}
	bar := teal.Render(strings.Repeat("█", filled)) + empty.Render(strings.Repeat("░", barWidth-filled))

	return strings.Repeat(" ", indent) +
		labelSt.Render(label) + " " +
		bar + " " +
		pctSt.Render(pct) +
		statSt.Render(suffix)
}

// renderDownloadProgress returns a progress block for the latest snapshot
// activity, or "" if none is active.
func renderDownloadProgress(lines []string, width int) string {
	bars := latestDownloadProgress(lines)
	if len(bars) == 0 {
		return ""
	}
	if width < 12 {
		width = 12
	}
	head := lipgloss.NewStyle().Foreground(tui.MithrilTeal).Bold(true)
	var b strings.Builder
	b.WriteString("  " + head.Render("Snapshot bootstrap — downloading") + "\n")
	for _, p := range bars {
		b.WriteString(renderDownloadBar(p, width) + "\n")
	}
	return b.String()
}

// truncatePlain truncates an unstyled string to w columns with an ellipsis.
func truncatePlain(s string, w int) string {
	if w <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= w {
		return s
	}
	if w == 1 {
		return string(r[:1])
	}
	return string(r[:w-1]) + "…"
}

// padRightPlain pads s with spaces to at least n display columns.
func padRightPlain(s string, n int) string {
	if w := lipgloss.Width(s); w < n {
		return s + strings.Repeat(" ", n-w)
	}
	return s
}
