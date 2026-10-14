package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func mustKlog(t *testing.T, s string) LogLine {
	t.Helper()
	l, ok := parseKlogLine(s)
	if !ok {
		t.Fatalf("parseKlogLine(%q) failed", s)
	}
	return l
}

func mustMlog(t *testing.T, s string) LogLine {
	t.Helper()
	l, ok := parseMlogLine(s)
	if !ok {
		t.Fatalf("parseMlogLine(%q) failed", s)
	}
	return l
}

func mustSinceFilter(t *testing.T, s string) logSinceFilter {
	t.Helper()
	filter, err := parseLogSinceFilter(s)
	if err != nil {
		t.Fatalf("parseLogSinceFilter(%q): %v", s, err)
	}
	return filter
}

func writeLogFixture(t *testing.T, dir, body string) string {
	return writeNamedLogFixture(t, dir, "mithril.log", body)
}

func writeNamedLogFixture(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeSymlinkedLogFixture(t *testing.T, body string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	run := filepath.Join(dir, "20260513-run")
	if err := os.Mkdir(run, 0o755); err != nil {
		t.Fatal(err)
	}
	writeLogFixture(t, run, body)
	if err := os.Symlink(filepath.Base(run), filepath.Join(dir, "latest")); err != nil {
		t.Fatal(err)
	}
	return dir, run
}

func TestParseKlog(t *testing.T) {
	l := mustKlog(t, "I2026-04-11T12:00:00.000Z slot_replay.go:142] replayed slot 285000001 in 45ms")
	if l.Timestamp != "2026-04-11T12:00:00.000Z" || l.Level != LevelInfo || l.File == nil || *l.File != "slot_replay.go" || l.Line == nil || *l.Line != 142 || l.ElapsedMs != nil {
		t.Errorf("klog info parse wrong: %+v", l)
	}
	e := mustKlog(t, "E2026-04-11T12:00:00.000Z verifier.go:99] bank hash mismatch")
	if e.Level != LevelError || *e.File != "verifier.go" || *e.Line != 99 {
		t.Errorf("klog error parse wrong: %+v", e)
	}
	for _, bad := range []string{"", "   ", "not a klog line"} {
		if _, ok := parseKlogLine(bad); ok {
			t.Errorf("parseKlogLine(%q) should fail", bad)
		}
	}
}

func TestParseMlogBranches(t *testing.T) {
	cases := []struct {
		line    string
		wantMs  uint64
		wantLvl LogLevel
		wantMsg string
	}{
		{"(+    5s) starting mithril v0.1.0", 5_000, LevelInfo, "starting mithril v0.1.0"},
		{"(+    0s) program start", 0, LevelInfo, "program start"},
		{"(+ 1m30s) replayed slot 285000001 in 45ms", 90_000, LevelInfo, "replayed slot 285000001 in 45ms"},
		{"(+15m07s) tick", 15*60_000 + 7_000, LevelInfo, "tick"},
		{"(+ 1h05m) something happened", 3_600_000 + 5*60_000, LevelInfo, "something happened"},
		{"(+ 1m23s) WARN: rpc timeout", 83_000, LevelWarn, "rpc timeout"},
		{"(+12h05m) ERROR: bankhash mismatch", 12*3_600_000 + 5*60_000, LevelError, "bankhash mismatch"},
		{"(+     45.123s) replayed slot", 45_123, LevelInfo, "replayed slot"},
		{"(+  30m45.123s) replay slow", 30*60_000 + 45_123, LevelInfo, "replay slow"},
		{"(+1h30m45.123s) deep replay", 3_600_000 + 30*60_000 + 45_123, LevelInfo, "deep replay"},
		{"(+1h) msg", 3_600_000, LevelInfo, "msg"},
		{"(+    5s) starting v0.1.0 (commit abc) ready", 5_000, LevelInfo, "starting v0.1.0 (commit abc) ready"},
		{"(+    5s) saw WARN: in upstream log", 5_000, LevelInfo, "saw WARN: in upstream log"},
		{"(+    5s) hello\n", 5_000, LevelInfo, "hello"},
		{"(+    5s) hello\r\n", 5_000, LevelInfo, "hello"},
	}
	for _, c := range cases {
		l := mustMlog(t, c.line)
		if l.ElapsedMs == nil || *l.ElapsedMs != c.wantMs {
			t.Errorf("%q elapsed = %v, want %d", c.line, l.ElapsedMs, c.wantMs)
		}
		if l.Level != c.wantLvl {
			t.Errorf("%q level = %v, want %v", c.line, l.Level, c.wantLvl)
		}
		if l.Message != c.wantMsg {
			t.Errorf("%q message = %q, want %q", c.line, l.Message, c.wantMsg)
		}
		if l.File != nil || l.Line != nil {
			t.Errorf("%q mlog should have nil file/line", c.line)
		}
		if l.Timestamp != "" {
			t.Errorf("%q mlog timestamp = %q, want omitted wall-clock timestamp", c.line, l.Timestamp)
		}
		wire, err := json.Marshal(l)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(wire), `"timestamp"`) {
			t.Errorf("%q mlog JSON claims a wall-clock timestamp: %s", c.line, wire)
		}
	}
}

func TestParseMlogRejects(t *testing.T) {
	bad := []string{
		"goroutine 1 [chan receive]:", "\tmain.go:42 +0x1a", "", "\n", "\r\n",
		"(+5s something", "(+5s)something", "(+abc) message", "(+1y5m) bogus",
		"(+59m59.9995s) msg", // seconds round to 60000 and are rejected
		"(+9999999h) msg",
	}
	for _, s := range bad {
		if _, ok := parseMlogLine(s); ok {
			t.Errorf("parseMlogLine(%q) should fail", s)
		}
	}
}

func TestParseLogDispatcher(t *testing.T) {
	if l, ok := parseLogLine("(+ 1m30s) WARN: stuff"); !ok || l.Level != LevelWarn || l.ElapsedMs == nil {
		t.Errorf("dispatcher mlog: %+v %v", l, ok)
	}
	if l, ok := parseLogLine("E2026-04-11T12:00:00.000Z verifier.go:99] mismatch"); !ok || l.Level != LevelError || l.ElapsedMs != nil {
		t.Errorf("dispatcher klog: %+v %v", l, ok)
	}
	for _, s := range []string{"not any format", ""} {
		if _, ok := parseLogLine(s); ok {
			t.Errorf("dispatcher should reject %q", s)
		}
	}
}

func TestSinceFilter(t *testing.T) {
	klog := mustKlog(t, "I2026-04-11T12:00:00.000Z file.go:1] msg")
	if passes, _ := applySinceFilter(klog, mustSinceFilter(t, "2026-04-11T11:00:00.000Z")); !passes {
		t.Error("klog after-since should pass")
	}
	if passes, _ := applySinceFilter(klog, mustSinceFilter(t, "2026-04-11T13:00:00.000Z")); passes {
		t.Error("klog before-since should fail")
	}
	// RFC3339 offsets are compared chronologically, not lexicographically.
	if passes, _ := applySinceFilter(klog, mustSinceFilter(t, "2026-04-11T17:29:00+05:30")); !passes {
		t.Error("equivalent-offset timestamp before the line should pass")
	}
	// A valid mlog-style filter on a klog line must not drop incomparable data.
	for _, s := range []string{"30m", "1h30m", "45s", ""} {
		if passes, _ := applySinceFilter(klog, mustSinceFilter(t, s)); !passes {
			t.Errorf("klog with mlog since %q should be included", s)
		}
	}

	nineMin := mustMlog(t, "(+ 9m00s) msg")
	oneH := mustMlog(t, "(+ 1h30m) msg")
	if passes, _ := applySinceFilter(nineMin, mustSinceFilter(t, "1h00m")); passes {
		t.Error("9m should not pass since=1h")
	}
	if passes, _ := applySinceFilter(oneH, mustSinceFilter(t, "1h00m")); !passes {
		t.Error("1h30m should pass since=1h")
	}
	if passes, _ := applySinceFilter(nineMin, mustSinceFilter(t, "5m")); !passes {
		t.Error("9m should pass since=5m (numeric, not lexical)")
	}
	// A valid RFC3339 filter on an mlog line is incomparable and retains it.
	if passes, _ := applySinceFilter(mustMlog(t, "(+ 5m00s) msg"), mustSinceFilter(t, "2026-04-11T12:00:00Z")); !passes {
		t.Error("mlog line with ISO since should be included")
	}
	if _, comparable := applySinceFilter(klog, mustSinceFilter(t, "5m")); comparable {
		t.Error("elapsed since must be marked incomparable with a klog timestamp")
	}
	if _, comparable := applySinceFilter(nineMin, mustSinceFilter(t, "2026-04-11T12:00:00Z")); comparable {
		t.Error("RFC3339 since must be marked incomparable with an mlog timestamp")
	}
}

func TestTailAndGrepSurfaceIncomparableMithrilTimestamps(t *testing.T) {
	dir := t.TempDir()
	writeLogFixture(t, dir,
		"I2026-04-11T12:00:00Z file.go:1] wall clock\n"+
			"(+ 5m00s) elapsed\n")

	lines, total, tailMeta, err := tailLogContextWithMeta(context.Background(), dir, 10, nil, "2026-04-11T11:00:00Z")
	if err != nil || total != 2 || len(lines) != 2 || tailMeta.IncomparableSinceLines != 1 || tailMeta.Complete {
		t.Fatalf("tail incomparable result = total:%d lines:%d meta:%+v err:%v", total, len(lines), tailMeta, err)
	}
	matches, total, grepMeta, err := grepLogForSourceContextWithMeta(context.Background(), dir, mithrilLogSource, ".", 10, "5m")
	if err != nil || total != 2 || len(matches) != 2 || grepMeta.IncomparableSinceLines != 1 || grepMeta.Complete {
		t.Fatalf("grep incomparable result = total:%d matches:%d meta:%+v err:%v", total, len(matches), grepMeta, err)
	}
}

func TestSinceFilterRejectsInvalidOrOversizedInputBeforeScan(t *testing.T) {
	for _, since := range []string{"garbage", "yesterday", "2026-99-99T25:61:61Z", strings.Repeat("1", maxLogSinceBytes+1)} {
		if _, err := parseLogSinceFilter(since); err == nil {
			t.Errorf("parseLogSinceFilter(%q) unexpectedly succeeded", since)
		}
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "mithril.log"), []byte("(+ 5m00s) msg\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := tailLogContextWithMeta(context.Background(), dir, 1, nil, "garbage"); err == nil {
		t.Fatal("tail accepted invalid since input")
	}
	if _, _, _, err := grepLogForSourceContextWithMeta(context.Background(), dir, mithrilLogSource, ".", 1, "garbage"); err == nil {
		t.Fatal("grep accepted invalid since input")
	}
}

func TestResolveLogFileFlat(t *testing.T) {
	dir := t.TempDir()
	p := writeLogFixture(t, dir, "x")
	got, err := resolveLogFile(dir, "mithril.log")
	want, evalErr := filepath.EvalSymlinks(p)
	if evalErr != nil {
		t.Fatal(evalErr)
	}
	if err != nil || got != want {
		t.Errorf("flat resolve = %q, %v", got, err)
	}
}

func TestResolveLogFileNested(t *testing.T) {
	dir := t.TempDir()
	latest := filepath.Join(dir, "latest")
	if err := os.Mkdir(latest, 0o755); err != nil {
		t.Fatal(err)
	}
	writeLogFixture(t, latest, "x")
	got, err := resolveLogFile(dir, "mithril.log")
	want, evalErr := filepath.EvalSymlinks(filepath.Join(latest, "mithril.log"))
	if evalErr != nil {
		t.Fatal(evalErr)
	}
	if err != nil || got != want {
		t.Errorf("nested resolve = %q, %v (want %q)", got, err, want)
	}
}

func TestResolveLogFileSymlink(t *testing.T) {
	dir, _ := writeSymlinkedLogFixture(t, "hello")
	got, err := resolveLogFile(dir, "mithril.log")
	if err != nil {
		t.Fatalf("symlink resolve err: %v", err)
	}
	data, err := os.ReadFile(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Errorf("resolved content = %q", data)
	}
	resolved, err := filepath.EvalSymlinks(got)
	if err != nil {
		t.Fatal(err)
	}
	if got != resolved || filepath.Base(filepath.Dir(got)) == "latest" {
		t.Errorf("returned path still traverses a symlink: %q -> %q", got, resolved)
	}
}

func TestResolveLogFileSymlinkEscapeRejected(t *testing.T) {
	outside := t.TempDir()
	writeLogFixture(t, outside, "secret")
	dir := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(dir, "latest")); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveLogFile(dir, "mithril.log"); err == nil {
		t.Error("symlink escape should be rejected")
	}
}

func TestResolveLogFileFlatSymlinkEscapeRejected(t *testing.T) {
	outside := t.TempDir()
	outPath := writeLogFixture(t, outside, "secret")
	dir := t.TempDir()
	if err := os.Symlink(outPath, filepath.Join(dir, "mithril.log")); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveLogFile(dir, "mithril.log"); err == nil {
		t.Fatal("escaping flat log symlink was accepted")
	}
}

func TestResolveLogFileNotFound(t *testing.T) {
	if _, err := resolveLogFile(t.TempDir(), "mithril.log"); err == nil {
		t.Error("missing log file should error")
	}
}

func TestResolveLogFilePrefersActiveLatest(t *testing.T) {
	dir := t.TempDir()
	writeLogFixture(t, dir, "flat")
	latest := filepath.Join(dir, "latest")
	if err := os.Mkdir(latest, 0o755); err != nil {
		t.Fatal(err)
	}
	writeLogFixture(t, latest, "nested")
	got, err := resolveLogFile(dir, "mithril.log")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "nested" {
		t.Errorf("active latest should win, got %q", data)
	}
}

func TestResolveLogFileDoesNotFallBackWhenActiveRunHasNoLog(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "mithril.log"), []byte("stale flat log"), 0o644); err != nil {
		t.Fatal(err)
	}
	run := filepath.Join(dir, "run-current")
	if err := os.Mkdir(run, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("run-current", filepath.Join(dir, "latest")); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveLogFile(dir, "mithril.log"); err == nil || !strings.Contains(err.Error(), "active log file not found") {
		t.Fatalf("stale flat log was not suppressed: %v", err)
	}
}

func TestLogOutputRedactionAndCancellation(t *testing.T) {
	dir := t.TempDir()
	message := "token=SUPER_SECRET https://rpc.example/PATH_SECRET?api-key=QUERY_SECRET " + strings.Repeat("x", maxLogMessageBytes)
	if err := os.WriteFile(filepath.Join(dir, "mithril.log"), []byte("E2026-04-11T12:00:02.000Z c.go:3] "+message+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	lines, _, _, err := tailLogContextWithMeta(context.Background(), dir, 1, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 1 || !lines[0].Truncated {
		t.Fatalf("expected one truncated line: %+v", lines)
	}
	for _, secret := range []string{"SUPER_SECRET", "PATH_SECRET", "QUERY_SECRET"} {
		if strings.Contains(lines[0].Message, secret) {
			t.Errorf("secret %q leaked: %q", secret, lines[0].Message)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := tailLogContextWithMeta(ctx, dir, 1, nil, ""); err == nil {
		t.Fatal("canceled log scan should fail")
	}
}

func TestWorstCaseLogDisplayFitsDefaultWireBudget(t *testing.T) {
	file := strings.Repeat(`\`, maxLogFileFieldBytes)
	line := sanitizeLogLine(LogLine{
		Timestamp: strings.Repeat(`\`, maxLogTimestampBytes),
		Level:     LevelError,
		File:      &file,
		Message:   strings.Repeat(`\`, maxLogMessageBytes),
	})
	lines := make([]LogLine, maxReturnLines)
	for i := range lines {
		lines[i] = line
	}
	wire, err := json.Marshal(tailLogOutput{Count: len(lines), Lines: lines})
	if err != nil {
		t.Fatal(err)
	}
	if 2*len(wire) >= DefaultOutputBudgetBytes-64*1024 {
		t.Fatalf("worst-case log display is too large for the default MCP budget: %d bytes duplicated", len(wire))
	}
}

func TestLogScanUsesBoundedRecentWindow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.log")
	oldLine := "I2026-04-11T12:00:00.000Z old.go:1] old-outside-window\n"
	recent := "I2026-04-11T12:00:01.000Z new.go:2] recent-one\n" +
		"E2026-04-11T12:00:02.000Z new.go:3] recent-two\n"
	sourceSize := maxRecentLogScanBytes + 4096
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if _, err := f.WriteString(oldLine); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(sourceSize); err != nil {
		t.Fatal(err)
	}
	recentOffset := sourceSize - int64(len(recent))
	// The bounded window starts inside a sparse, unterminated record. Its suffix
	// must be discarded without Scanner allocating the whole 8 MiB line.
	if _, err := f.WriteAt([]byte{'\n'}, recentOffset-1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte(recent), recentOffset); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	lines, total, tailScan, err := tailLogContextWithMeta(context.Background(), dir, 10, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(lines) != 2 || lines[0].Message != "recent-one" || lines[1].Message != "recent-two" {
		t.Fatalf("bounded tail = total:%d lines:%+v", total, lines)
	}
	wantOmitted := sourceSize - maxRecentLogScanBytes
	if tailScan.WindowLimitBytes != maxRecentLogScanBytes ||
		tailScan.SourceSizeBytes != sourceSize ||
		tailScan.ScannedBytes != maxRecentLogScanBytes ||
		tailScan.OmittedPrefixBytes != wantOmitted || tailScan.Complete {
		t.Fatalf("bounded tail scan metadata = %+v, want source=%d omitted=%d", tailScan, sourceSize, wantOmitted)
	}

	matches, matchesInWindow, grepScan, err := grepLogForSourceContextWithMeta(context.Background(), dir, mithrilLogSource, "old-outside-window|recent", 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if matchesInWindow != 2 || len(matches) != 2 {
		t.Fatalf("bounded grep = total:%d matches:%+v", matchesInWindow, matches)
	}
	if grepScan != tailScan {
		t.Fatalf("tail and grep scanned different source windows: tail=%+v grep=%+v", tailScan, grepScan)
	}

	session := startInMemorySession(t, Config{LogDir: dir})
	for _, call := range []struct {
		name string
		args map[string]any
		want string
	}{
		{"mithril_tail_log", map[string]any{"lines": 10}, `"count":2`},
		{"mithril_grep_log", map[string]any{"pattern": "old-outside-window|recent"}, `"total_matches":2`},
	} {
		text, isError := callToolText(t, session, call.name, call.args)
		if isError {
			t.Fatalf("%s returned an error: %s", call.name, text)
		}
		for _, fragment := range []string{
			call.want,
			`"truncated":true`,
			fmt.Sprintf(`"window_limit_bytes":%d`, maxRecentLogScanBytes),
			fmt.Sprintf(`"source_size_bytes":%d`, sourceSize),
			fmt.Sprintf(`"scanned_bytes":%d`, maxRecentLogScanBytes),
			fmt.Sprintf(`"omitted_prefix_bytes":%d`, wantOmitted),
			`"complete":false`,
		} {
			if !strings.Contains(text, fragment) {
				t.Errorf("%s result missing %s: %s", call.name, fragment, text)
			}
		}
		if strings.Contains(text, "old-outside-window") {
			t.Errorf("%s returned data before its bounded scan window: %s", call.name, text)
		}
	}
}

func TestLogScanReportsConcurrentShrinkAsIncomplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mithril.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	observedSize := maxRecentLogScanBytes + 1
	if err := f.Truncate(observedSize); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(0); err != nil {
		t.Fatal(err)
	}

	called := false
	meta, err := scanRecentLogFileContext(context.Background(), f, observedSize, func(string) { called = true })
	if err != nil {
		t.Fatalf("concurrent shrink returned a tool error: %v", err)
	}
	if called || !meta.SourceChangedDuringScan || meta.Complete || meta.ScannedBytes != 0 || meta.SourceSizeBytes != observedSize {
		t.Fatalf("concurrent shrink metadata = %+v, callback=%v", meta, called)
	}
}

func TestTailAndGrep(t *testing.T) {
	dir := t.TempDir()
	content := "I2026-04-11T12:00:00.000Z a.go:1] first\n" +
		"W2026-04-11T12:00:01.000Z b.go:2] a warning\n" +
		"E2026-04-11T12:00:02.000Z c.go:3] an error\n"
	writeLogFixture(t, dir, content)

	all, _, scan, err := tailLogContextWithMeta(context.Background(), dir, 100, nil, "")
	if err != nil || len(all) != 3 {
		t.Fatalf("tail all = %d lines, %v", len(all), err)
	}
	if !scan.Complete || scan.SourceSizeBytes != int64(len(content)) || scan.ScannedBytes != int64(len(content)) || scan.OmittedPrefixBytes != 0 {
		t.Fatalf("small log should have complete scan metadata: %+v", scan)
	}
	if all[0].Message != "first" || all[2].Message != "an error" {
		t.Errorf("tail order wrong: %+v", all)
	}
	warnUp := LevelWarn
	filtered, _, _, err := tailLogContextWithMeta(context.Background(), dir, 100, &warnUp, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 2 {
		t.Errorf("warn+ filter = %d, want 2", len(filtered))
	}
	matches, total, _, err := grepLogForSourceContextWithMeta(context.Background(), dir, mithrilLogSource, "warning|error", 100, "")
	if err != nil || total != 2 || len(matches) != 2 {
		t.Errorf("grep = %d/%d, %v", len(matches), total, err)
	}
	if _, _, _, err := grepLogForSourceContextWithMeta(context.Background(), dir, mithrilLogSource, "(", 100, ""); err == nil {
		t.Error("invalid regex should error")
	}
}

func TestTailAndGrepReportUnparsedRecords(t *testing.T) {
	dir := t.TempDir()
	writeLogFixture(t, dir,
		"I2026-04-11T12:00:00.000Z a.go:1] parsed\n"+
			"goroutine 1 [running]:\n"+
			"\n")

	lines, total, tailMeta, err := tailLogContextWithMeta(context.Background(), dir, 10, nil, "")
	if err != nil || total != 1 || len(lines) != 1 || tailMeta.UnparsedLines != 1 || tailMeta.Complete {
		t.Fatalf("tail unparsed result = total:%d lines:%d meta:%+v err:%v", total, len(lines), tailMeta, err)
	}
	matches, total, grepMeta, err := grepLogForSourceContextWithMeta(context.Background(), dir, mithrilLogSource, "parsed", 10, "")
	if err != nil || total != 1 || len(matches) != 1 || grepMeta.UnparsedLines != 1 || grepMeta.Complete {
		t.Fatalf("grep unparsed result = total:%d matches:%d meta:%+v err:%v", total, len(matches), grepMeta, err)
	}

	session := startInMemorySession(t, Config{LogDir: dir})
	for _, call := range []struct {
		name string
		args map[string]any
	}{
		{"mithril_tail_log", map[string]any{"lines": 10}},
		{"mithril_grep_log", map[string]any{"pattern": "parsed"}},
	} {
		text, isError := callToolText(t, session, call.name, call.args)
		if isError || !strings.Contains(text, `"unparsed_lines":1`) ||
			!strings.Contains(text, `"complete":false`) || !strings.Contains(text, `"truncated":true`) {
			t.Fatalf("%s unparsed metadata = isError:%v text:%s", call.name, isError, text)
		}
	}
}

func TestTailAndGrepSkipIncompleteFinalRecord(t *testing.T) {
	dir := t.TempDir()
	writeLogFixture(t, dir,
		"I2026-04-11T12:00:00.000Z a.go:1] complete\n"+
			"E2026-04-11T12:00:01.000Z b.go:2] partial")

	lines, total, tailMeta, err := tailLogContextWithMeta(context.Background(), dir, 10, nil, "")
	if err != nil || total != 1 || len(lines) != 1 || lines[0].Message != "complete" || !tailMeta.IncompleteTail || tailMeta.Complete {
		t.Fatalf("tail incomplete record = total:%d lines:%+v meta:%+v err:%v", total, lines, tailMeta, err)
	}
	matches, total, grepMeta, err := grepLogForSourceContextWithMeta(context.Background(), dir, mithrilLogSource, "partial", 10, "")
	if err != nil || total != 0 || len(matches) != 0 || !grepMeta.IncompleteTail || grepMeta.Complete {
		t.Fatalf("grep incomplete record = total:%d matches:%+v meta:%+v err:%v", total, matches, grepMeta, err)
	}

	session := startInMemorySession(t, Config{LogDir: dir})
	text, isError := callToolText(t, session, "mithril_tail_log", map[string]any{"lines": 10})
	if isError || strings.Contains(text, `"message":"partial"`) ||
		!strings.Contains(text, `"incomplete_tail":true`) || !strings.Contains(text, `"complete":false`) {
		t.Fatalf("tail protocol incomplete record = isError:%v text:%s", isError, text)
	}
}

func TestLogScanDetectsSameSizeRewriteAndAppend(t *testing.T) {
	t.Run("same inode later mutation", func(t *testing.T) {
		dir := t.TempDir()
		path := writeLogFixture(t, dir, "I2026-04-11T12:00:00.000Z a.go:1] before\n")
		original, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := file.WriteString("I2026-04-11T12:00:01.000Z a.go:2] later\n")
		closeErr := file.Close()
		if writeErr != nil {
			t.Fatal(writeErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if !activeLogSourceChanged(dir, mithrilLogSource, path, original) {
			t.Fatal("same-inode mutation was accepted as stable")
		}
	})

	t.Run("same size rewrite", func(t *testing.T) {
		dir := t.TempDir()
		before := "I2026-04-11T12:00:00.000Z a.go:1] before\n"
		after := "I2026-04-11T12:00:00.000Z a.go:1] after!\n"
		if len(before) != len(after) {
			t.Fatalf("test rewrite changed size: before=%d after=%d", len(before), len(after))
		}
		path := writeLogFixture(t, dir, before)
		initialInfo, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		var callbackErr error
		meta, err := scanRecentLogLinesForSourceContext(context.Background(), dir, mithrilLogSource, func(string) {
			if callbackErr != nil {
				return
			}
			callbackErr = os.WriteFile(path, []byte(after), 0o644)
			if callbackErr == nil {
				changedAt := initialInfo.ModTime().Add(2 * time.Second)
				callbackErr = os.Chtimes(path, changedAt, changedAt)
			}
		})
		if err != nil || callbackErr != nil {
			t.Fatalf("same-size rewrite scan errors = scan:%v callback:%v", err, callbackErr)
		}
		if !meta.SourceChangedDuringScan || meta.Complete {
			t.Fatalf("same-size rewrite was accepted as stable: %+v", meta)
		}
	})

	t.Run("append", func(t *testing.T) {
		dir := t.TempDir()
		path := writeLogFixture(t, dir, "I2026-04-11T12:00:00.000Z a.go:1] before\n")
		var callbackErr error
		var scanned []string
		meta, err := scanRecentLogLinesForSourceContext(context.Background(), dir, mithrilLogSource, func(line string) {
			scanned = append(scanned, line)
			if callbackErr != nil {
				return
			}
			file, openErr := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			if openErr != nil {
				callbackErr = openErr
				return
			}
			_, writeErr := file.WriteString("I2026-04-11T12:00:01.000Z a.go:2] appended\n")
			closeErr := file.Close()
			if writeErr != nil {
				callbackErr = writeErr
			} else {
				callbackErr = closeErr
			}
		})
		if err != nil || callbackErr != nil {
			t.Fatalf("append scan errors = scan:%v callback:%v", err, callbackErr)
		}
		if !meta.SourceChangedDuringScan || meta.Complete {
			t.Fatalf("append-only growth was accepted as a complete scan: %+v", meta)
		}
		if len(scanned) != 1 || strings.Contains(scanned[0], "appended") {
			t.Fatalf("scan crossed its initial source boundary: %q", scanned)
		}
	})

	t.Run("same path replacement", func(t *testing.T) {
		dir := t.TempDir()
		before := "I2026-04-11T12:00:00.000Z a.go:1] before\n"
		after := "I2026-04-11T12:00:00.000Z a.go:1] after!\n"
		if len(before) != len(after) {
			t.Fatalf("replacement changed size: before=%d after=%d", len(before), len(after))
		}
		path := writeLogFixture(t, dir, before)
		replacement := filepath.Join(dir, "replacement.log")
		var callbackErr error
		meta, err := scanRecentLogLinesForSourceContext(context.Background(), dir, mithrilLogSource, func(string) {
			if callbackErr != nil {
				return
			}
			callbackErr = os.WriteFile(replacement, []byte(after), 0o644)
			if callbackErr == nil {
				callbackErr = os.Rename(replacement, path)
			}
		})
		if err != nil || callbackErr != nil {
			t.Fatalf("replacement scan errors = scan:%v callback:%v", err, callbackErr)
		}
		if !meta.SourceChangedDuringScan || meta.Complete {
			t.Fatalf("same-path replacement was accepted as stable: %+v", meta)
		}
	})

	t.Run("latest switch", func(t *testing.T) {
		dir := t.TempDir()
		for _, run := range []string{"run-one", "run-two"} {
			if err := os.Mkdir(filepath.Join(dir, run), 0o755); err != nil {
				t.Fatal(err)
			}
			writeLogFixture(t, filepath.Join(dir, run),
				"I2026-04-11T12:00:00.000Z a.go:1] "+run+"\n")
		}
		latest := filepath.Join(dir, "latest")
		if err := os.Symlink("run-one", latest); err != nil {
			t.Fatal(err)
		}
		next := filepath.Join(dir, "next")
		var callbackErr error
		meta, err := scanRecentLogLinesForSourceContext(context.Background(), dir, mithrilLogSource, func(string) {
			if callbackErr != nil {
				return
			}
			callbackErr = os.Symlink("run-two", next)
			if callbackErr == nil {
				callbackErr = os.Rename(next, latest)
			}
		})
		if err != nil || callbackErr != nil {
			t.Fatalf("latest-switch scan errors = scan:%v callback:%v", err, callbackErr)
		}
		if !meta.SourceChangedDuringScan || meta.Complete {
			t.Fatalf("latest switch was accepted as stable: %+v", meta)
		}
	})
}

func TestGrepMatchesOnlySanitizedText(t *testing.T) {
	dir := t.TempDir()
	const secret = "SUPER_HIDDEN_TOKEN"
	const apostropheSuffix = "APOSTROPHE_SUFFIX_SECRET"
	content := "I2026-04-11T12:00:00.000Z a.go:1] startup token=" + secret + " https://rpc.example/" + secret + "?api-key=" + secret + "'" + apostropheSuffix + "\n"
	if err := os.WriteFile(filepath.Join(dir, "mithril.log"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, pattern := range []string{secret, "SUPER_HIDDEN_.*", `rpc\.example/` + secret, apostropheSuffix} {
		matches, total, _, err := grepLogForSourceContextWithMeta(context.Background(), dir, mithrilLogSource, pattern, 100, "")
		if err != nil {
			t.Fatal(err)
		}
		if total != 0 || len(matches) != 0 {
			t.Fatalf("raw secret pattern %q leaked through match oracle: total=%d matches=%+v", pattern, total, matches)
		}
	}

	matches, total, _, err := grepLogForSourceContextWithMeta(context.Background(), dir, mithrilLogSource, `startup.*\[REDACTED\]`, 100, "")
	if err != nil || total != 1 || len(matches) != 1 {
		t.Fatalf("sanitized text was not searchable: total=%d matches=%+v err=%v", total, matches, err)
	}
	wire, err := json.Marshal(matches)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), secret) || strings.Contains(string(wire), apostropheSuffix) {
		t.Fatalf("grep result leaked secret: %s", wire)
	}
}

func TestTailAndGrepRingBuffersReturnLastNInOrder(t *testing.T) {
	dir := t.TempDir()
	var b strings.Builder
	lineCount := maxReturnLines + 7
	for i := 1; i <= lineCount; i++ {
		fmt.Fprintf(&b, "I0101 00:00:%02d.000000 1 x.go:1] line %d\n", i%60, i)
	}
	writeLogFixture(t, dir, b.String())

	got, total, _, err := tailLogContextWithMeta(context.Background(), dir, 5, nil, "")
	if err != nil {
		t.Fatalf("tail: %v", err)
	}
	if len(got) != 5 || total != lineCount {
		t.Fatalf("tail window = returned:%d total:%d", len(got), total)
	}
	for i := range got {
		want := fmt.Sprintf("line %d", lineCount-len(got)+1+i)
		if !strings.Contains(got[i].Message, want) {
			t.Errorf("line %d = %q, want to contain %q", i, got[i].Message, want)
		}
	}
	window, total, _, err := tailLogContextWithMeta(context.Background(), dir, maxReturnLines, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(window) != maxReturnLines || total != lineCount {
		t.Fatalf("max tail window = returned:%d total:%d", len(window), total)
	}

	matches, total, _, err := grepLogForSourceContextWithMeta(context.Background(), dir, mithrilLogSource, "line", 5, "")
	if err != nil {
		t.Fatalf("grep: %v", err)
	}
	if len(matches) != 5 || total != lineCount {
		t.Fatalf("grep window = returned:%d total:%d", len(matches), total)
	}
	for i := range matches {
		want := fmt.Sprintf("line %d", lineCount-len(matches)+1+i)
		if !strings.Contains(matches[i].Message, want) {
			t.Errorf("grep match %d = %q, want to contain %q", i, matches[i].Message, want)
		}
	}
	empty, total, _, err := grepLogForSourceContextWithMeta(context.Background(), dir, mithrilLogSource, "line", 0, "")
	if err != nil || len(empty) != 0 || total != lineCount {
		t.Fatalf("zero-sized grep window = returned:%d total:%d err:%v", len(empty), total, err)
	}

	session := startInMemorySession(t, Config{LogDir: dir})
	text, isError := callToolText(t, session, "mithril_grep_log", map[string]any{"pattern": "line", "max_matches": 5})
	if isError || !strings.Contains(text, fmt.Sprintf(`"total_matches":%d`, lineCount)) ||
		!strings.Contains(text, `"returned":5`) || !strings.Contains(text, `"truncated":true`) ||
		strings.Contains(text, fmt.Sprintf(`"message":"line %d"`, lineCount-5)) ||
		!strings.Contains(text, fmt.Sprintf(`"message":"line %d"`, lineCount)) {
		t.Fatalf("bounded grep protocol result = isError:%v text:%s", isError, text)
	}
}

func TestLogScanSkipsOversizedLines(t *testing.T) {
	dir := t.TempDir()
	body := "E2026-04-11T12:00:00.000Z a.go:1] before\n" +
		strings.Repeat("x", maxLogLineBytes+1) + "\n" +
		"E2026-04-11T12:00:01.000Z a.go:2] after\n"
	writeLogFixture(t, dir, body)

	lines, total, meta, err := tailLogContextWithMeta(context.Background(), dir, 10, nil, "")
	if err != nil {
		t.Fatalf("tail failed on oversized line: %v", err)
	}
	if total != 2 || len(lines) != 2 || meta.OversizedLines != 1 || meta.Complete {
		t.Fatalf("tail result = total:%d lines:%d meta:%+v", total, len(lines), meta)
	}
	matches, total, meta, err := grepLogForSourceContextWithMeta(context.Background(), dir, mithrilLogSource, "after", 10, "")
	if err != nil || total != 1 || len(matches) != 1 || meta.OversizedLines != 1 {
		t.Fatalf("grep result = total:%d matches:%d meta:%+v err:%v", total, len(matches), meta, err)
	}
}
