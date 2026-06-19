package dashboardcmd

import (
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
)

func TestParseDownloadProgress(t *testing.T) {
	cases := []struct {
		name    string
		line    string
		ok      bool
		label   string
		percent float64
		current string
		total   string
		unit    string
		rate    string
		eta     string
	}{
		{
			name:    "snapshot line",
			line:    "Snapshot: 8.3% 9.0/107.5 GB 89.6 MB/s ETA 18m47s",
			ok:      true,
			label:   "Snapshot",
			percent: 8.3, current: "9.0", total: "107.5", unit: "GB", rate: "89.6 MB/s", eta: "18m47s",
		},
		{
			name:    "extract line",
			line:    "Extract: 5.7% 24.2/424.6 GB 393.5 MB/s ETA 17m22s",
			ok:      true,
			label:   "Extract",
			percent: 5.7, current: "24.2", total: "424.6", unit: "GB", rate: "393.5 MB/s", eta: "17m22s",
		},
		{
			name:    "with elapsed prefix",
			line:    "(+ 1m03s) Snapshot: 100.0% 107.5/107.5 GB 110.0 MB/s ETA 0s",
			ok:      true,
			label:   "Snapshot",
			percent: 100, current: "107.5", total: "107.5", unit: "GB", rate: "110.0 MB/s", eta: "0s",
		},
		{name: "not a progress line", line: "(+ 0s) Probing 321 nodes for snapshot availability...", ok: false},
		{name: "build-phase appendvec line is not download", line: "Extract (AppendVecs) [#####    ] 50%", ok: false},
		{name: "empty", line: "", ok: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, ok := parseDownloadProgress(c.line)
			if ok != c.ok {
				t.Fatalf("ok=%v want %v (line=%q)", ok, c.ok, c.line)
			}
			if !c.ok {
				return
			}
			if p.label != c.label || p.percent != c.percent || p.current != c.current || p.total != c.total ||
				p.unit != c.unit || p.rate != c.rate || p.eta != c.eta {
				t.Fatalf("parsed %+v, want label=%s pct=%v current=%s total=%s unit=%s rate=%s eta=%s",
					p, c.label, c.percent, c.current, c.total, c.unit, c.rate, c.eta)
			}
		})
	}
}

func TestParseDownloadProgressClampsPercent(t *testing.T) {
	p, ok := parseDownloadProgress("Snapshot: 250.0% 9/9 GB 1 MB/s ETA 0s")
	if !ok || p.percent != 100 {
		t.Fatalf("expected clamp to 100, got %v ok=%v", p.percent, ok)
	}
}

func TestLatestDownloadProgress_NewestWins(t *testing.T) {
	lines := []string{
		"Snapshot: 1.0% 1/107 GB 90 MB/s ETA 20m",
		"Extract: 1.0% 4/424 GB 390 MB/s ETA 20m",
		"Snapshot: 8.3% 9.0/107.5 GB 89.6 MB/s ETA 18m47s",
		"Extract: 8.3% 35/424 GB 393 MB/s ETA 17m",
		"some other log line",
	}
	got := latestDownloadProgress(lines)
	if len(got) != 2 {
		t.Fatalf("expected 2 bars, got %d", len(got))
	}
	if got[0].label != "Snapshot" || got[0].percent != 8.3 {
		t.Fatalf("snapshot bar wrong: %+v", got[0])
	}
	if got[1].label != "Extract" || got[1].percent != 8.3 {
		t.Fatalf("extract bar wrong: %+v", got[1])
	}
}

func TestLatestDownloadProgress_None(t *testing.T) {
	if got := latestDownloadProgress([]string{"a", "b", "c"}); len(got) != 0 {
		t.Fatalf("expected none, got %d", len(got))
	}
}

func TestFilterDownloadProgressLines(t *testing.T) {
	in := []string{
		"(+ 0s) starting",
		"Snapshot: 8.3% 9.0/107.5 GB 89.6 MB/s ETA 18m47s",
		"Extract: 8.3% 35/424 GB 393 MB/s ETA 17m",
		"(+ 1m) building",
	}
	out := filterDownloadProgressLines(in)
	if len(out) != 2 || out[0] != "(+ 0s) starting" || out[1] != "(+ 1m) building" {
		t.Fatalf("filter wrong: %#v", out)
	}
	// caller slice must be untouched
	if len(in) != 4 {
		t.Fatalf("filter mutated input: %#v", in)
	}
}

// Resize guard: the rendered bar never exceeds its width budget.
func TestRenderDownloadBar_NeverOverflows(t *testing.T) {
	p := downloadProgress{label: "Snapshot", percent: 63.4, current: "68.1", total: "107.5", unit: "GB", rate: "112.0 MB/s", eta: "5m58s"}
	for w := 8; w <= 200; w++ {
		got := renderDownloadBar(p, w)
		if width := lipgloss.Width(got); width > w {
			t.Fatalf("width %d: rendered display width %d exceeds budget\n%q", w, width, got)
		}
	}
}

func TestRenderDownloadBar_DegradesNarrow(t *testing.T) {
	p := downloadProgress{label: "Extract", percent: 50, current: "1", total: "2", unit: "GB", rate: "1 MB/s", eta: "1m"}
	// wide: should include the bar blocks and the ETA detail
	wide := renderDownloadBar(p, 120)
	if !strings.Contains(wide, "█") || !strings.Contains(wide, "ETA") {
		t.Fatalf("wide bar missing fill/eta: %q", wide)
	}
	// narrow: still renders the percent
	narrow := renderDownloadBar(p, 18)
	if !strings.Contains(narrow, "50.0%") && !strings.Contains(narrow, "50") {
		t.Fatalf("narrow bar missing percent: %q", narrow)
	}
}

func TestRenderDownloadProgress_EmptyWhenInactive(t *testing.T) {
	if s := renderDownloadProgress([]string{"nothing here"}, 100); s != "" {
		t.Fatalf("expected empty, got %q", s)
	}
}

func TestRenderDownloadProgress_ShowsBothBars(t *testing.T) {
	lines := []string{
		"Snapshot: 8.3% 9.0/107.5 GB 89.6 MB/s ETA 18m47s",
		"Extract: 8.3% 35/424 GB 393 MB/s ETA 17m",
	}
	out := renderDownloadProgress(lines, 100)
	// 18m47s == Snapshot bar, AccountsDB == Extract bar (header word "Snapshot" would false-match)
	if !strings.Contains(out, "18m47s") || !strings.Contains(out, "AccountsDB") {
		t.Fatalf("expected both bars, got:\n%s", out)
	}
	// every rendered row must be within the width budget
	for _, row := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if w := lipgloss.Width(row); w > 100 {
			t.Fatalf("row exceeds width: %d\n%q", w, row)
		}
	}
}
