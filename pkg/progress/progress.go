package progress

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/base58"
	"golang.org/x/term"
)

const (
	barWidth       = 40
	updateInterval = 500 * time.Millisecond
	ewmaTau        = 60.0 // seconds for EWMA smoothing (matches old shard.go)

	// Teal/cyan color (using 256-color mode for wider terminal compatibility)
	// Index 85 is a teal/cyan in the 256-color palette
	colorTeal   = "\x1b[38;5;85m"
	colorYellow = "\x1b[38;5;220m"
	colorReset  = "\x1b[0m"
	colorDim    = "\x1b[2m"

	// Cursor control
	clearLine = "\x1b[2K"
	moveUp    = "\x1b[1A"

	// Size constants
	gib = 1 << 30
)

// ProgressBar tracks progress for a single operation
type ProgressBar struct {
	label     string
	total     int64
	current   atomic.Int64
	startTime time.Time

	// For EWMA throughput calculation
	mu             sync.Mutex
	lastUpdate     time.Time
	lastBytes      int64
	ewmaThroughput float64
}

// NewProgressBar creates a new progress bar with the given label
func NewProgressBar(label string) *ProgressBar {
	return &ProgressBar{
		label:     label,
		startTime: time.Now(),
	}
}

// SetTotal sets the expected total bytes
func (p *ProgressBar) SetTotal(total int64) {
	atomic.StoreInt64(&p.total, total)
}

// Add increments the current progress by n bytes
func (p *ProgressBar) Add(n int64) {
	p.current.Add(n)
}

// Current returns the current progress
func (p *ProgressBar) Current() int64 {
	return p.current.Load()
}

// Total returns the total expected
func (p *ProgressBar) Total() int64 {
	return atomic.LoadInt64(&p.total)
}

// updateThroughput calculates EWMA throughput
func (p *ProgressBar) updateThroughput() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	current := p.current.Load()

	if p.lastUpdate.IsZero() {
		p.lastUpdate = now
		p.lastBytes = current
		return 0
	}

	elapsed := now.Sub(p.lastUpdate).Seconds()
	if elapsed < 0.1 {
		return p.ewmaThroughput
	}

	bytesPerSec := float64(current-p.lastBytes) / elapsed

	// Time-based EWMA: alpha = 1 - e^(-elapsed/tau)
	// This gives more weight to recent samples when updates are frequent,
	// and smooths over longer periods when updates are sparse
	alpha := 1.0 - math.Exp(-elapsed/ewmaTau)
	if p.ewmaThroughput == 0 {
		p.ewmaThroughput = bytesPerSec
	} else {
		p.ewmaThroughput = alpha*bytesPerSec + (1-alpha)*p.ewmaThroughput
	}

	p.lastUpdate = now
	p.lastBytes = current
	return p.ewmaThroughput
}

// Render returns the progress bar string
func (p *ProgressBar) Render(useColor bool) string {
	current := p.current.Load()
	total := atomic.LoadInt64(&p.total)

	var percent float64
	if total > 0 {
		percent = float64(current) / float64(total) * 100
		if percent > 100 {
			percent = 100
		}
	}

	throughput := p.updateThroughput()

	// Calculate ETA or show elapsed time if complete
	elapsed := time.Since(p.startTime)
	var etaStr string
	if current >= total && total > 0 {
		// Complete - show how long it took
		etaStr = fmt.Sprintf("Finished in %s", formatDurationRounded(elapsed))
	} else if throughput > 0 && total > current {
		remaining := float64(total-current) / throughput
		etaStr = fmt.Sprintf("ETA %s", formatDuration(time.Duration(remaining*float64(time.Second))))
	} else {
		etaStr = "ETA --:--"
	}

	// Build the bar
	filled := int(percent / 100 * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

	// Format throughput
	throughputStr := formatThroughput(throughput)

	// Format size progress (current/total in GB)
	sizeStr := formatSizeProgress(current, total)

	// Build the line with size progress
	if useColor {
		return fmt.Sprintf("%s%-24s%s [%s%s%s] %5.1f%% %13s %8s  %s",
			colorTeal, p.label, colorReset,
			colorTeal, bar, colorReset,
			percent, sizeStr, throughputStr, etaStr)
	}
	return fmt.Sprintf("%-24s [%s] %5.1f%% %13s %8s  %s",
		p.label, bar, percent, sizeStr, throughputStr, etaStr)
}

func formatThroughput(bytesPerSec float64) string {
	if bytesPerSec < 1024 {
		return fmt.Sprintf("%.0f B/s", bytesPerSec)
	} else if bytesPerSec < 1024*1024 {
		return fmt.Sprintf("%.1f KB/s", bytesPerSec/1024)
	} else {
		return fmt.Sprintf("%.1f MB/s", bytesPerSec/(1024*1024))
	}
}

func formatSizeProgress(current, total int64) string {
	currentGB := float64(current) / float64(gib)
	totalGB := float64(total) / float64(gib)
	if total > 0 {
		// Cap display when current exceeds total (estimate was low)
		if current > total {
			return fmt.Sprintf("%5.1f/%5.1f GB", currentGB, currentGB)
		}
		return fmt.Sprintf("%5.1f/%5.1f GB", currentGB, totalGB)
	}
	return fmt.Sprintf("%5.1f/  ??? GB", currentGB)
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		return "--:--"
	}
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh%02dm", h, m)
	}
	return fmt.Sprintf("%dm%02ds", m, s)
}

// formatDurationRounded formats duration with friendlier spacing and rounds up to nearest second
func formatDurationRounded(d time.Duration) string {
	if d < 0 {
		return "0s"
	}
	// Round up to nearest second
	if d%time.Second != 0 {
		d = d.Truncate(time.Second) + time.Second
	}

	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm", h, m)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

// DualProgress manages two progress bars displayed simultaneously:
// - Download: compressed snapshot bytes from network
// - Extract: decompressed tar bytes (AppendVec files)
type DualProgress struct {
	Download *ProgressBar
	Extract  *ProgressBar

	mu            sync.Mutex
	started       bool
	done          bool
	interrupted   bool
	stopCh        chan struct{}
	doneCh        chan struct{}
	output        io.Writer
	useColor      bool
	downloadTotal int64 // cached download total for ratio calculation
}

// NewDualProgress creates a new dual progress display
func NewDualProgress() *DualProgress {
	// Use stderr for progress bars to avoid interleaving with log output on stdout
	useColor := term.IsTerminal(int(os.Stderr.Fd()))

	return &DualProgress{
		Download: NewProgressBar("Snapshot Read (.tar.zst)"),
		Extract:  NewProgressBar("Extract (AppendVecs)"),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
		output:   os.Stderr,
		useColor: useColor,
	}
}

// SetDownloadTotal sets the known download total and enables dynamic estimation
func (d *DualProgress) SetDownloadTotal(total int64) {
	d.mu.Lock()
	d.downloadTotal = total
	d.mu.Unlock()
	d.Download.SetTotal(total)
}

// updateEstimates dynamically adjusts Extract total based on observed compression ratio
func (d *DualProgress) updateEstimates() {
	downloadCurrent := d.Download.Current()
	extractCurrent := d.Extract.Current()

	// Need at least 1MB of data to calculate a meaningful ratio
	const minBytes = 1 << 20 // 1 MB
	if downloadCurrent < minBytes || extractCurrent == 0 {
		return
	}

	d.mu.Lock()
	downloadTotal := d.downloadTotal
	d.mu.Unlock()

	if downloadTotal <= 0 {
		return
	}

	// Calculate observed compression ratio for Extract
	ratio := float64(extractCurrent) / float64(downloadCurrent)
	if ratio < 2.0 {
		ratio = 2.0
	} else if ratio > 10.0 {
		ratio = 10.0
	}
	d.Extract.SetTotal(int64(float64(downloadTotal) * ratio))
}

// Start begins the progress display update loop
func (d *DualProgress) Start() {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return
	}
	d.started = true
	d.mu.Unlock()

	// Print pipeline description using stages (same stage = parallel)
	fmt.Fprintln(d.output) // spacing before pipeline
	if d.useColor {
		fmt.Fprintf(d.output, "%s", colorDim)
	}
	fmt.Fprintln(d.output, "  [1] Snapshot Download + Extract AppendVecs → [2] Flush Index")
	fmt.Fprintln(d.output, "  [3] Download Incremental + Process → [4] Fetch Blocks → Replay")
	if d.useColor {
		fmt.Fprintf(d.output, "%s", colorReset)
	}
	fmt.Fprintln(d.output) // blank line before progress bars

	// Print initial empty lines for progress bars (2 bars)
	fmt.Fprintln(d.output)
	fmt.Fprintln(d.output)

	go d.updateLoop()
}

// updateLoop periodically updates the display
func (d *DualProgress) updateLoop() {
	ticker := time.NewTicker(updateInterval)
	defer ticker.Stop()
	defer close(d.doneCh)

	for {
		select {
		case <-d.stopCh:
			d.updateEstimates()
			d.render()
			return
		case <-ticker.C:
			// Skip rendering until we have actual data to show
			// This prevents duplicate empty 0% bars from appearing
			if d.Download.Total() == 0 {
				continue
			}
			d.updateEstimates()
			d.render()
		}
	}
}

// render updates the display with both bars
func (d *DualProgress) render() {
	d.mu.Lock()
	defer d.mu.Unlock()

	downloadLine := d.Download.Render(d.useColor)
	extractLine := d.Extract.Render(d.useColor)

	if d.useColor {
		// Single atomic write to avoid terminal buffering issues:
		// \r - carriage return to start of line (ensures clean positioning)
		// \x1b[2A - move cursor up 2 lines
		// \x1b[2K - clear the current line (download bar line)
		// print download bar + newline
		// \x1b[2K - clear the current line (extract bar line)
		// print extract bar + newline
		fmt.Fprintf(d.output, "\r\x1b[2A\x1b[2K%s\n\x1b[2K%s\n",
			downloadLine,
			extractLine)
	} else {
		// Non-TTY: skip in-place updates, bars will scroll
		fmt.Fprintf(d.output, "%s\n%s\n", downloadLine, extractLine)
	}
}

// Stop stops the progress display
func (d *DualProgress) Stop() {
	d.mu.Lock()
	if d.done {
		d.mu.Unlock()
		return
	}
	d.done = true
	d.mu.Unlock()

	close(d.stopCh)
	<-d.doneCh
}

// Interrupt stops the progress display and shows interrupted status.
// If err is provided, shows the error message; otherwise shows "Interrupted by user".
func (d *DualProgress) Interrupt(err ...error) {
	d.mu.Lock()
	d.interrupted = true
	d.mu.Unlock()

	d.Stop()

	// Print final interrupted status
	fmt.Fprintf(d.output, "\033[2K") // Clear line
	msg := "Interrupted by user"
	if len(err) > 0 && err[0] != nil {
		msg = err[0].Error()
	}
	if d.useColor {
		fmt.Fprintf(d.output, "%s⚠ %s%s\n", colorYellow, msg, colorReset)
	} else {
		fmt.Fprintf(d.output, "⚠ %s\n", msg)
	}
}

// IsInterrupted returns true if the progress was interrupted
func (d *DualProgress) IsInterrupted() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.interrupted
}

// IndexingProgress tracks shard flush progress
type IndexingProgress struct {
	label     string
	total     int
	completed atomic.Int32
	startTime time.Time
	output    io.Writer
	useColor  bool
	mu        sync.Mutex
	started   bool
}

// NewIndexingProgress creates a new indexing progress display
func NewIndexingProgress(label string) *IndexingProgress {
	// Use stderr for progress bars to avoid interleaving with log output on stdout
	return &IndexingProgress{
		label:     label,
		startTime: time.Now(),
		output:    os.Stderr,
		useColor:  term.IsTerminal(int(os.Stderr.Fd())),
	}
}

// Start initializes the display with total count
func (p *IndexingProgress) Start(total int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.total = total
	p.started = true
	p.startTime = time.Now()
	// Don't reserve a line here - first Update will render in place
}

// Update updates progress and re-renders
func (p *IndexingProgress) Update(completed, total int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		return
	}

	prevCompleted := p.completed.Swap(int32(completed))
	p.total = total

	// On first update (prevCompleted was 0), just clear current line
	// On subsequent updates, move up first then clear
	if p.useColor {
		if prevCompleted > 0 {
			fmt.Fprint(p.output, moveUp)
		}
		fmt.Fprint(p.output, clearLine)
	}

	percent := float64(completed) / float64(total) * 100
	filled := int(percent / 100 * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

	elapsed := time.Since(p.startTime)
	var etaStr string
	if completed >= total {
		// Complete - show how long it took
		etaStr = fmt.Sprintf("Finished in %s", formatDurationRounded(elapsed))
	} else if completed > 0 && completed < total {
		remaining := elapsed * time.Duration(total-completed) / time.Duration(completed)
		etaStr = fmt.Sprintf("ETA %s", formatDuration(remaining))
	} else {
		etaStr = "ETA --:--"
	}

	if p.useColor {
		fmt.Fprintf(p.output, "%s%-24s%s [%s%s%s] %5.1f%% %4d/%-4d shards  %s\n",
			colorTeal, p.label, colorReset,
			colorTeal, bar, colorReset,
			percent, completed, total, etaStr)
	} else {
		fmt.Fprintf(p.output, "%-24s [%s] %5.1f%% %4d/%-4d shards  %s\n",
			p.label, bar, percent, completed, total, etaStr)
	}
}

// Finish marks completion
func (p *IndexingProgress) Finish() {
	p.Update(p.total, p.total)
}

// Interrupt shows interrupted status.
// If err is provided, shows the error message; otherwise shows "Interrupted".
func (p *IndexingProgress) Interrupt(err ...error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		return
	}

	msg := "Interrupted"
	if len(err) > 0 && err[0] != nil {
		msg = err[0].Error()
	}

	// Move up and clear line
	if p.useColor {
		fmt.Fprint(p.output, moveUp+clearLine)
		fmt.Fprintf(p.output, "%s%-24s%s %s⚠ %s%s\n",
			colorTeal, p.label, colorReset,
			colorYellow, msg, colorReset)
	} else {
		fmt.Fprintf(p.output, "%-24s ⚠ %s\n", p.label, msg)
	}
}

// PrintBanner prints the Mithril ASCII art banner (left-aligned to match log output)
func PrintBanner() {
	// Banner with content starting at left edge
	const logo = `
           _______ __________________          _______ _________ _
          (       )\__   __/\__   __/|\     /|(  ____ )\__   __/( \
   .      | () () |   ) (      ) (   | )   ( || (    )|   ) (   | (         .
 ./|\.    | || || |   | |      | |   | (___) || (____)|   | |   | |       ./|\.
<--:-->   | |(_)| |   | |      | |   |  ___  ||     __)   | |   | |      <--:-->
 '\|/'    | |   | |   | |      | |   | (   ) || (\ (      | |   | |       '\|/'
   '      | )   ( |___) (___   | |   | )   ( || ) \ \_____) (___| (____/\   '
          |/     \|\_______/   )_(   |/     \||/   \__/\_______/(_______/
`

	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	lines := strings.Split(strings.Trim(logo, "\n"), "\n")

	fmt.Println() // blank line above
	for _, ln := range lines {
		if useColor {
			fmt.Printf("%s%s%s\n", colorTeal, ln, colorReset)
		} else {
			fmt.Println(ln)
		}
	}
	// Add empty lines below the banner for visual separation
	fmt.Println()
	fmt.Println()
	fmt.Println()
}

// PrintSnapshotSourceSummary prints a clean summary box for the selected snapshot source
// Box width = 80 chars to match Mithril banner (from left star edge to right star edge)
func PrintSnapshotSourceSummary(nodeIP string, slot int, referenceSlot int, nodeVersion string, speedMBs float64, rttMs int, searchDuration time.Duration) {
	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	c := "" // color start (teal for borders)
	r := "" // color reset
	if useColor {
		c = colorTeal
		r = colorReset
	}

	// Calculate age in slots
	age := referenceSlot - slot

	// Format version
	version := nodeVersion
	if version == "" {
		version = "unknown"
	}

	// Format RTT
	rttStr := fmt.Sprintf("%d ms", rttMs)
	if rttMs == 0 {
		rttStr = "N/A"
	}

	fmt.Printf("%s┌──────────────────────────────────────────────────────────────────────────────┐%s\n", c, r)
	fmt.Printf("%s│%s FULL SNAPSHOT SOURCE SELECTED                                                %s│%s\n", c, r, c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	fmt.Printf("%s│%s %-18s%-59s%s│%s\n", c, r, "Source:", nodeIP, c, r)
	fmt.Printf("%s│%s %-18s%-59s%s│%s\n", c, r, "Version:", version, c, r)
	fmt.Printf("%s│%s %-18s%-59d%s│%s\n", c, r, "Snapshot Slot:", slot, c, r)
	fmt.Printf("%s│%s %-18s%-59s%s│%s\n", c, r, "Snapshot Age:", fmt.Sprintf("%d slots behind tip", age), c, r)
	fmt.Printf("%s│%s %-18s%-59s%s│%s\n", c, r, "Download Speed:", fmt.Sprintf("%.1f MB/s", speedMBs), c, r)
	fmt.Printf("%s│%s %-18s%-59s%s│%s\n", c, r, "RTT:", rttStr, c, r)
	fmt.Printf("%s│%s %-18s%-59s%s│%s\n", c, r, "Search Time:", searchDuration.Round(time.Second).String(), c, r)
	fmt.Printf("%s└──────────────────────────────────────────────────────────────────────────────┘%s\n", c, r)
	fmt.Println()
}

// ShutdownInfo contains information about the replay state when shutting down
type ShutdownInfo struct {
	LastSlot         uint64
	LastBankhash     []byte
	SnapshotBaseSlot uint64
	AccountsDBPath   string
	ReplayDuration   time.Duration
	WasCancelled     bool
	RunID            string
	Epoch            uint64
	SnapshotEpoch    uint64
}

// PrintShutdownSummary prints a summary box when replay is stopped via Ctrl+C
func PrintShutdownSummary(info ShutdownInfo) {
	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	c := "" // color start (teal for borders)
	r := "" // color reset
	if useColor {
		c = colorTeal
		r = colorReset
	}

	// Format bankhash as base58 (same as log output)
	bankhashStr := base58.Encode(info.LastBankhash)

	// Calculate slots replayed
	slotsReplayed := int64(0)
	if info.LastSlot > info.SnapshotBaseSlot {
		slotsReplayed = int64(info.LastSlot - info.SnapshotBaseSlot)
	}

	// Format duration
	durationStr := info.ReplayDuration.Round(time.Second).String()

	// Next slot to resume from
	nextSlot := info.LastSlot + 1

	fmt.Println()
	fmt.Printf("%s┌──────────────────────────────────────────────────────────────────────────────┐%s\n", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, " REPLAY STOPPED", c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	fmt.Printf("%s│%s %-20s%-57s%s│%s\n", c, r, "Run ID:", info.RunID, c, r)
	fmt.Printf("%s│%s %-20s%-57s%s│%s\n", c, r, "Last replayed slot:", fmt.Sprintf("%s (epoch %d)", formatSlots(info.LastSlot), info.Epoch), c, r)
	fmt.Printf("%s│%s %-20s%-57s%s│%s\n", c, r, "Bankhash:", bankhashStr, c, r)
	fmt.Printf("%s│%s %-20s%-57s%s│%s\n", c, r, "AccountsDB:", info.AccountsDBPath, c, r)
	fmt.Printf("%s│%s %-20s%-57s%s│%s\n", c, r, "Slots replayed:", fmt.Sprintf("%s (from snapshot slot %s, epoch %d)", formatSlots(uint64(slotsReplayed)), formatSlots(info.SnapshotBaseSlot), info.SnapshotEpoch), c, r)
	fmt.Printf("%s│%s %-20s%-57s%s│%s\n", c, r, "Total replay time:", durationStr, c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, " RESTART OPTIONS:", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "", c, r)
	resumeStr := fmt.Sprintf("   Resume from slot %s:", formatSlots(nextSlot))
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, resumeStr, c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "     mithril run --bootstrap accountsdb", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "", c, r)
	snapshotStr := fmt.Sprintf("   Rebuild from existing snapshot (slot %s):", formatSlots(info.SnapshotBaseSlot))
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, snapshotStr, c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "     mithril run --bootstrap snapshot", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "   Download fresh snapshot and rebuild:", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "     mithril run --bootstrap new-snapshot", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, " State saved to: mithril_state.json", c, r)
	fmt.Printf("%s└──────────────────────────────────────────────────────────────────────────────┘%s\n", c, r)
	fmt.Println()
}

// BuildInterruptInfo contains information when build is interrupted
type BuildInterruptInfo struct {
	Stage          string // "downloading", "building", etc.
	SnapshotSlot   uint64
	SnapshotPath   string // path to downloaded snapshot if available
	AccountsDBPath string
}

// PrintBuildInterrupted prints a summary box when build is stopped via Ctrl+C
func PrintBuildInterrupted(info BuildInterruptInfo) {
	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	c := "" // color start (teal for borders)
	r := "" // color reset
	if useColor {
		c = colorTeal
		r = colorReset
	}

	fmt.Println()
	fmt.Printf("%s┌──────────────────────────────────────────────────────────────────────────────┐%s\n", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, " BUILD INTERRUPTED", c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	fmt.Printf("%s│%s %-20s%-57s%s│%s\n", c, r, "Stage:", info.Stage, c, r)
	if info.SnapshotSlot > 0 {
		fmt.Printf("%s│%s %-20s%-57s%s│%s\n", c, r, "Snapshot slot:", formatSlots(info.SnapshotSlot), c, r)
	}
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, " RESTART OPTIONS:", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "", c, r)
	if info.SnapshotPath != "" {
		fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "   Rebuild from existing snapshot (already downloaded):", c, r)
		fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "     mithril run --bootstrap snapshot", c, r)
		fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "", c, r)
	}
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "   Download fresh snapshot and rebuild:", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "     mithril run --bootstrap new-snapshot", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "", c, r)
	if info.SnapshotPath != "" {
		// Truncate path if too long
		pathDisplay := info.SnapshotPath
		if len(pathDisplay) > 55 {
			pathDisplay = "..." + pathDisplay[len(pathDisplay)-52:]
		}
		fmt.Printf("%s│%s Snapshot preserved: %-56s%s│%s\n", c, r, pathDisplay, c, r)
	}
	fmt.Printf("%s└──────────────────────────────────────────────────────────────────────────────┘%s\n", c, r)
	fmt.Println()
}

// DiskInfo holds information about disk usage for a specific path
type DiskInfo struct {
	Label      string // e.g., "AccountsDB", "Snapshots", "Ledger"
	Path       string
	Device     string // e.g., "nvme1n1p1"
	UsedBytes  uint64
	TotalBytes uint64
	Error      error
}

// GetDiskInfo returns disk usage information for a given path
func GetDiskInfo(path string) *DiskInfo {
	if path == "" {
		return nil
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return &DiskInfo{Path: path, Error: err}
	}

	totalBytes := stat.Blocks * uint64(stat.Bsize)
	freeBytes := stat.Bavail * uint64(stat.Bsize)
	usedBytes := totalBytes - freeBytes
	device := getMountPoint(path)

	return &DiskInfo{
		Path:       path,
		Device:     device,
		UsedBytes:  usedBytes,
		TotalBytes: totalBytes,
	}
}

// FormatDiskInfo formats disk info as a compact string like "(nvme1n1p1) 630 GB / 1.5 TB (41%)"
func FormatDiskInfo(info *DiskInfo) string {
	if info == nil || info.Error != nil || info.TotalBytes == 0 {
		return ""
	}

	percentUsed := float64(info.UsedBytes) / float64(info.TotalBytes) * 100
	usedStr := formatBytes(info.UsedBytes)
	totalStr := formatBytes(info.TotalBytes)

	deviceStr := ""
	if info.Device != "" {
		deviceStr = fmt.Sprintf("(%s) ", info.Device)
	}

	// Add warning if usage is high
	warning := ""
	if percentUsed >= 90 {
		warning = " ⚠ LOW SPACE!"
	} else if percentUsed >= 80 {
		warning = " ⚠"
	}

	return fmt.Sprintf("%s%s / %s (%2.0f%%)%s", deviceStr, usedStr, totalStr, percentUsed, warning)
}

// formatBytes formats bytes as human-readable string (e.g., "234 GB")
func formatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.1f TB", float64(bytes)/float64(TB))
	case bytes >= GB:
		return fmt.Sprintf("%.0f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.0f MB", float64(bytes)/float64(MB))
	default:
		return fmt.Sprintf("%.0f KB", float64(bytes)/float64(KB))
	}
}

// getMountPoint returns the mount point device for a given path by reading /proc/mounts
func getMountPoint(path string) string {
	// Get the device ID for the path
	var pathStat syscall.Stat_t
	if err := syscall.Stat(path, &pathStat); err != nil {
		return ""
	}
	targetDev := pathStat.Dev

	// Read /proc/mounts to find the device
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return ""
	}

	var bestMatch string
	var bestMatchLen int

	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		device := fields[0]
		mountPoint := fields[1]

		// Check if this mount point is a prefix of our path
		if !strings.HasPrefix(path, mountPoint) {
			continue
		}

		// Check if this mount has the same device
		var mountStat syscall.Stat_t
		if err := syscall.Stat(mountPoint, &mountStat); err != nil {
			continue
		}

		if mountStat.Dev == targetDev && len(mountPoint) > bestMatchLen {
			bestMatch = device
			bestMatchLen = len(mountPoint)
		}
	}

	// Extract just the device name (e.g., "nvme0n1p1" from "/dev/nvme0n1p1")
	if strings.HasPrefix(bestMatch, "/dev/") {
		return bestMatch[5:]
	}
	return bestMatch
}

// PrintDiskUsage prints a concise disk usage summary for configured paths
func PrintDiskUsage(accountsDbPath, blockstorePath, snapshotsPath string) {
	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	c := "" // teal for borders
	r := "" // reset
	y := "" // yellow for warning
	d := "" // dim for device name
	if useColor {
		c = colorTeal
		r = colorReset
		y = colorYellow
		d = colorDim
	}

	// Collect disk info for each path, deduplicating by device
	type pathInfo struct {
		label      string
		path       string
		device     string
		usedBytes  uint64
		totalBytes uint64
	}

	paths := []struct {
		label string
		path  string
	}{
		{"AccountsDB", accountsDbPath},
		{"Blockstore", blockstorePath},
		{"Snapshots", snapshotsPath},
	}

	var infos []pathInfo
	seenDevices := make(map[string]int) // device -> index in infos

	for _, p := range paths {
		if p.path == "" {
			continue
		}

		var stat syscall.Statfs_t
		if err := syscall.Statfs(p.path, &stat); err != nil {
			continue
		}

		device := getMountPoint(p.path)
		totalBytes := stat.Blocks * uint64(stat.Bsize)
		freeBytes := stat.Bavail * uint64(stat.Bsize)
		usedBytes := totalBytes - freeBytes

		// Check if we've already seen this device
		if idx, seen := seenDevices[device]; seen && device != "" {
			// Append label to existing entry
			infos[idx].label += "+" + p.label
		} else {
			seenDevices[device] = len(infos)
			infos = append(infos, pathInfo{
				label:      p.label,
				path:       p.path,
				device:     device,
				usedBytes:  usedBytes,
				totalBytes: totalBytes,
			})
		}
	}

	if len(infos) == 0 {
		return
	}

	fmt.Printf("%sDisk usage:%s\n", c, r)

	for _, info := range infos {
		percentUsed := float64(info.usedBytes) / float64(info.totalBytes) * 100
		usedStr := formatBytes(info.usedBytes)
		totalStr := formatBytes(info.totalBytes)

		// Add warning if usage is high
		warning := ""
		if percentUsed >= 90 {
			warning = fmt.Sprintf(" %sLOW SPACE!%s", y, r)
		} else if percentUsed >= 80 {
			warning = fmt.Sprintf(" %s(warning)%s", y, r)
		}

		// Format device info
		deviceStr := ""
		if info.device != "" {
			deviceStr = fmt.Sprintf(" %s(%s)%s", d, info.device, r)
		}

		// Truncate path if needed
		pathDisplay := info.path
		if len(pathDisplay) > 30 {
			pathDisplay = "..." + pathDisplay[len(pathDisplay)-27:]
		}

		fmt.Printf("  %-12s %-30s %7s / %-7s (%3.0f%%)%s%s\n",
			info.label+":",
			pathDisplay+deviceStr,
			usedStr,
			totalStr,
			percentUsed,
			warning,
			"")
	}
	fmt.Println()
}

// StaleInfo contains information for the stale AccountsDB prompt
type StaleInfo struct {
	AccountsDBSlot     uint64
	LatestSnapshotSlot uint64
	SlotsBehind        uint64
}

// formatSlots formats a slot number with commas for readability
func formatSlots(slots uint64) string {
	s := strconv.FormatUint(slots, 10)
	if len(s) <= 3 {
		return s
	}

	// Insert commas
	var result strings.Builder
	remainder := len(s) % 3
	if remainder > 0 {
		result.WriteString(s[:remainder])
		if len(s) > remainder {
			result.WriteString(",")
		}
	}
	for i := remainder; i < len(s); i += 3 {
		result.WriteString(s[i : i+3])
		if i+3 < len(s) {
			result.WriteString(",")
		}
	}
	return result.String()
}

// PromptStaleAccountsDB displays an interactive prompt when AccountsDB is significantly behind
// and returns the user's choice (1 = continue with AccountsDB, 2 = start fresh from snapshot)
func PromptStaleAccountsDB(info StaleInfo) int {
	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	c := "" // teal for borders
	r := "" // reset
	if useColor {
		c = colorTeal
		r = colorReset
	}

	// Format slot values
	accountsSlot := formatSlots(info.AccountsDBSlot)
	latestSlot := formatSlots(info.LatestSnapshotSlot)
	slotsBehind := formatSlots(info.SlotsBehind)

	// Build option 1 text and calculate padding
	opt1Text := fmt.Sprintf("  [1] Continue from AccountsDB (replay %s slots)", slotsBehind)
	opt1Padding := 76 - len(opt1Text)
	if opt1Padding < 0 {
		opt1Padding = 0
	}

	// Print the prompt box (76 chars inner width)
	fmt.Println()
	fmt.Printf("%s┌──────────────────────────────────────────────────────────────────────────────┐%s\n", c, r)
	fmt.Printf("%s│%s ACCOUNTSDB BEHIND CHAIN TIP                                                  %s│%s\n", c, r, c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	fmt.Printf("%s│%s %-24s%-52s %s│%s\n", c, r, "AccountsDB last slot:", accountsSlot, c, r)
	fmt.Printf("%s│%s %-24s%-52s %s│%s\n", c, r, "Chain tip slot:", latestSlot, c, r)
	fmt.Printf("%s│%s %-24s%-52s %s│%s\n", c, r, "Slots behind:", slotsBehind, c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	fmt.Printf("%s│%s OPTIONS:                                                                     %s│%s\n", c, r, c, r)
	fmt.Printf("%s│%s%s%*s %s│%s\n", c, r, opt1Text, opt1Padding, "", c, r)
	fmt.Printf("%s│%s   [2] Start fresh from latest snapshot (faster to catch up)                 %s│%s\n", c, r, c, r)
	fmt.Printf("%s│%s                                                                              %s│%s\n", c, r, c, r)
	fmt.Printf("%s└──────────────────────────────────────────────────────────────────────────────┘%s\n", c, r)

	// Read user input
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter choice (1 or 2): ")
		input, err := reader.ReadString('\n')
		if err != nil {
			// On error (e.g., EOF), default to continuing with AccountsDB
			return 1
		}

		input = strings.TrimSpace(input)
		switch input {
		case "1":
			return 1
		case "2":
			return 2
		default:
			fmt.Println("Invalid choice. Please enter 1 or 2.")
		}
	}
}

// DownloadInterruptInfo contains information when download is interrupted
type DownloadInterruptInfo struct {
	Stage           string // "downloading full snapshot", "downloading incremental", etc.
	SourceHost      string
	DownloadPercent float64
	BytesDownloaded int64
	TotalBytes      int64
}

// PrintDownloadInterrupted prints a summary box when download is stopped via Ctrl+C
func PrintDownloadInterrupted(info DownloadInterruptInfo) {
	useColor := term.IsTerminal(int(os.Stdout.Fd()))
	c := "" // teal for borders
	r := "" // reset
	if useColor {
		c = colorTeal
		r = colorReset
	}

	progressStr := fmt.Sprintf("%.0f%% (%s / %s)",
		info.DownloadPercent,
		formatBytes(uint64(info.BytesDownloaded)),
		formatBytes(uint64(info.TotalBytes)))

	fmt.Println()
	fmt.Printf("%s┌──────────────────────────────────────────────────────────────────────────────┐%s\n", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, " DOWNLOAD INTERRUPTED", c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	fmt.Printf("%s│%s %-20s%-57s%s│%s\n", c, r, "Stage:", info.Stage, c, r)
	if info.SourceHost != "" {
		fmt.Printf("%s│%s %-20s%-57s%s│%s\n", c, r, "Source:", info.SourceHost, c, r)
	}
	fmt.Printf("%s│%s %-20s%-57s%s│%s\n", c, r, "Progress:", progressStr, c, r)
	fmt.Printf("%s├──────────────────────────────────────────────────────────────────────────────┤%s\n", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, " TO RESTART:", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "   Download will resume from the beginning:", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "     mithril run --bootstrap snapshot", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, "", c, r)
	fmt.Printf("%s│%s%-78s%s│%s\n", c, r, " Note: Partial download has been cleaned up.", c, r)
	fmt.Printf("%s└──────────────────────────────────────────────────────────────────────────────┘%s\n", c, r)
	fmt.Println()
}
