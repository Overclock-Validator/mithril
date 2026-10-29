package mcp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	maxReturnLines              = 100
	maxRecentLogScanBytes int64 = 8 * 1024 * 1024 // bound disk/CPU work per tail or grep call
	maxLogLineBytes             = 1024 * 1024     // one malformed line must not allocate the full scan window
	maxLogMessageBytes          = 1024            // keeps the worst-case duplicated MCP result below 1 MiB
	maxLogTimestampBytes        = 128
	maxLogFileFieldBytes        = 512
	maxPatternLength            = 512
	maxLogSinceBytes            = 128
	logSourceMithril            = "mithril"
)

// LogLevel is a log severity serialized as a string.
type LogLevel string

const (
	LevelDebug LogLevel = "debug"
	LevelInfo  LogLevel = "info"
	LevelWarn  LogLevel = "warn"
	LevelError LogLevel = "error"
)

// rank orders severities for filtering: debug(0) < info(1) < warn(2) < error(3).
func (l LogLevel) rank() int {
	switch l {
	case LevelDebug:
		return 0
	case LevelInfo:
		return 1
	case LevelWarn:
		return 2
	case LevelError:
		return 3
	}
	return 1
}

func parseLevelName(s string) (LogLevel, bool) {
	switch strings.ToLower(s) {
	case "debug":
		return LevelDebug, true
	case "info":
		return LevelInfo, true
	case "warn":
		return LevelWarn, true
	case "error":
		return LevelError, true
	}
	return "", false
}

func parseLevelChar(c byte) (LogLevel, bool) {
	switch c {
	case 'D':
		return LevelDebug, true
	case 'I':
		return LevelInfo, true
	case 'W':
		return LevelWarn, true
	case 'E':
		return LevelError, true
	}
	return "", false
}

// LogLine is a parsed log entry.
type LogLine struct {
	Timestamp string   `json:"timestamp,omitempty"`
	ElapsedMs *uint64  `json:"elapsed_ms,omitempty"`
	Level     LogLevel `json:"level"`
	File      *string  `json:"file,omitempty"`
	Target    *string  `json:"target,omitempty"`
	Line      *uint32  `json:"line,omitempty"`
	Message   string   `json:"message"`
	Truncated bool     `json:"truncated,omitempty"`
}

// parseKlogLine parses a klog line: `I2026-04-11T12:00:00.000Z file.go:123] message`.
func parseKlogLine(line string) (LogLine, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return LogLine{}, false
	}
	level, ok := parseLevelChar(line[0])
	if !ok {
		return LogLine{}, false
	}
	bracket := strings.IndexByte(line, ']')
	if bracket < 0 {
		return LogLine{}, false
	}
	header := line[1:bracket]
	message := strings.TrimSpace(line[bracket+1:])

	sp := strings.IndexByte(header, ' ')
	if sp < 0 {
		return LogLine{}, false
	}
	timestamp := header[:sp]
	fileLine := header[sp+1:]

	colon := strings.LastIndexByte(fileLine, ':')
	if colon < 0 {
		return LogLine{}, false
	}
	file := fileLine[:colon]
	out := LogLine{Timestamp: timestamp, Level: level, Message: message, File: &file}
	// A non-numeric line field becomes nil rather than a misleading line 0.
	if n, err := strconv.ParseUint(fileLine[colon+1:], 10, 32); err == nil {
		u := uint32(n)
		out.Line = &u
	}
	return out, true
}

var mlogPrefixRe = regexp.MustCompile(`^\(\+([ 0-9hms.]+?)\) (.*)$`)

// parseMlogElapsed parses the mlog elapsed payload (e.g. "1m30s", "45.123s",
// "1h05m") into total milliseconds, or false if it doesn't parse cleanly.
func parseMlogElapsed(payload string) (uint64, bool) {
	// Width padding is not part of the elapsed value.
	var b strings.Builder
	for _, r := range payload {
		if r != ' ' && r != '\t' {
			b.WriteRune(r)
		}
	}
	rest := b.String()
	if rest == "" {
		return 0, false
	}
	var total uint64

	if idx := strings.IndexByte(rest, 'h'); idx >= 0 {
		h, err := strconv.ParseUint(rest[:idx], 10, 64)
		// Reject values large enough to overflow elapsed milliseconds.
		if err != nil || h > 1_000_000 {
			return 0, false
		}
		total += h * 3_600_000
		rest = rest[idx+1:]
	}
	if idx := strings.IndexByte(rest, 'm'); idx >= 0 {
		m, err := strconv.ParseUint(rest[:idx], 10, 64)
		if err != nil {
			return 0, false
		}
		if m >= 60 {
			return 0, false // mlog rolls into hours, never emits >=60 minutes
		}
		total += m * 60_000
		rest = rest[idx+1:]
	}
	if rest != "" {
		s, ok := strings.CutSuffix(rest, "s")
		if !ok {
			return 0, false
		}
		secs, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsInf(secs, 0) || math.IsNaN(secs) || secs < 0 || secs >= 60 {
			return 0, false
		}
		secsMs := uint64(math.Round(secs * 1000.0))
		if secsMs >= 60_000 { // FP round guard: 59.9995 becomes 60000
			return 0, false
		}
		total += secsMs
	}
	return total, true
}

// parseMlogLine parses an mlog line: `(+<elapsed>) [WARN: |ERROR: ]message`.
func parseMlogLine(line string) (LogLine, bool) {
	// Leading spaces inside the prefix are part of the format.
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return LogLine{}, false
	}
	caps := mlogPrefixRe.FindStringSubmatch(line)
	if caps == nil {
		return LogLine{}, false
	}
	elapsedRaw, rest := caps[1], caps[2]
	elapsedMs, ok := parseMlogElapsed(elapsedRaw)
	if !ok {
		return LogLine{}, false
	}
	// mlog encodes WARN and ERROR explicitly, but has no marker that separates
	// debug from info. Classify every unprefixed record as info rather than
	// claiming a distinction the source format does not provide.
	level := LevelInfo
	message := rest
	if m, ok := strings.CutPrefix(rest, "ERROR: "); ok {
		level, message = LevelError, m
	} else if m, ok := strings.CutPrefix(rest, "WARN: "); ok {
		level, message = LevelWarn, m
	}
	return LogLine{
		ElapsedMs: &elapsedMs,
		Level:     level,
		Message:   message,
	}, true
}

// parseLogLine tries mlog (v0.2.0+) first, then klog (legacy).
func parseLogLine(line string) (LogLine, bool) {
	if l, ok := parseMlogLine(line); ok {
		return l, true
	}
	return parseKlogLine(line)
}

type logSourceSpec struct {
	name  string
	file  string
	parse func(string) (LogLine, bool)
}

var mithrilLogSource = logSourceSpec{name: logSourceMithril, file: "mithril.log", parse: parseLogLine}

type logSinceFilter struct {
	set       bool
	elapsed   bool
	elapsedMs uint64
	wallTime  time.Time
}

// parseLogSinceFilter validates and parses caller-controlled filter text once,
// before scanning a potentially large log. Re-parsing a long invalid value for
// every mlog line would multiply a small MCP request into unbounded CPU work.
func parseLogSinceFilter(since string) (logSinceFilter, error) {
	if since == "" {
		return logSinceFilter{}, nil
	}
	if len(since) > maxLogSinceBytes {
		return logSinceFilter{}, fmt.Errorf("since exceeds %d-byte limit", maxLogSinceBytes)
	}
	if elapsedMs, ok := parseMlogElapsed(since); ok {
		return logSinceFilter{set: true, elapsed: true, elapsedMs: elapsedMs}, nil
	}
	wallTime, err := time.Parse(time.RFC3339Nano, since)
	if err != nil {
		return logSinceFilter{}, fmt.Errorf("since must be an RFC3339 timestamp or mlog elapsed duration such as 1m30s")
	}
	return logSinceFilter{set: true, wallTime: wallTime}, nil
}

// applySinceFilter reports whether a line passes and whether its timestamp was
// comparable. Incomparable Mithril formats are retained but surfaced in scan
// metadata so callers do not mistake a partial filter for complete evidence.
func applySinceFilter(line LogLine, since logSinceFilter) (passes, comparable bool) {
	if !since.set {
		return true, true
	}
	if line.ElapsedMs != nil {
		if !since.elapsed {
			return true, false
		}
		return *line.ElapsedMs >= since.elapsedMs, true
	}
	if since.elapsed {
		return true, false
	}
	lineTime, err := time.Parse(time.RFC3339Nano, line.Timestamp)
	if err != nil {
		return true, false
	}
	return !lineTime.Before(since.wallTime), true
}

// resolveLogFile finds the log file under a flat (<dir>/file) or per-run
// (<dir>/latest/file) layout, confining the symlinked latest path to dir.
func resolveLogFile(logDir, file string) (string, error) {
	activeDir, active, err := resolveLatestRunDir(logDir)
	if err != nil {
		return "", err
	}
	if active {
		nested := filepath.Join(activeDir, file)
		info, err := os.Stat(nested)
		if os.IsNotExist(err) {
			return "", fmt.Errorf("active log file not found: %s", file)
		}
		if err != nil {
			return "", fmt.Errorf("inspect active log file: %w", err)
		}
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("active log path is not a regular file: %s", nested)
		}
		resolved, err := resolveConfined(nested, logDir)
		if err != nil {
			return "", err
		}
		return resolved, nil
	}
	flat := filepath.Join(logDir, file)
	if info, err := os.Stat(flat); err == nil {
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("legacy log path is not a regular file: %s", flat)
		}
		resolved, err := resolveConfined(flat, logDir)
		if err != nil {
			return "", fmt.Errorf("legacy log path is unsafe: %w", err)
		}
		return resolved, nil
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("inspect legacy log file: %w", err)
	}
	return "", fmt.Errorf("log file not found in %s (tried %q and latest/%s)", logDir, file, file)
}

// LogScanMeta describes the bounded source window examined by a log tool.
// Complete=false means some source records were outside the window or skipped.
type LogScanMeta struct {
	WindowLimitBytes        int64 `json:"window_limit_bytes"`
	SourceSizeBytes         int64 `json:"source_size_bytes"`
	ScannedBytes            int64 `json:"scanned_bytes"`
	OmittedPrefixBytes      int64 `json:"omitted_prefix_bytes"`
	OversizedLines          int   `json:"oversized_lines,omitempty"`
	UnparsedLines           int   `json:"unparsed_lines,omitempty"`
	IncomparableSinceLines  int   `json:"incomparable_since_lines,omitempty"`
	IncompleteTail          bool  `json:"incomplete_tail,omitempty"`
	SourceChangedDuringScan bool  `json:"source_changed_during_scan,omitempty"`
	Complete                bool  `json:"complete"`
}

// scanRecentLogLinesForSourceContext scans at most the newest maxRecentLogScanBytes.
// When the window begins inside a line, that partial record is discarded so a
// suffix cannot be misparsed as a complete log entry.
func scanRecentLogLinesForSourceContext(ctx context.Context, logDir string, source logSourceSpec, fn func(line string)) (LogScanMeta, error) {
	path, err := resolveLogFile(logDir, source.file)
	if err != nil {
		return LogScanMeta{WindowLimitBytes: maxRecentLogScanBytes}, err
	}
	f, err := openConfinedRegularFile(path, logDir)
	if err != nil {
		return LogScanMeta{WindowLimitBytes: maxRecentLogScanBytes}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return LogScanMeta{WindowLimitBytes: maxRecentLogScanBytes}, err
	}
	if !info.Mode().IsRegular() {
		return LogScanMeta{WindowLimitBytes: maxRecentLogScanBytes}, fmt.Errorf("active log path is not a regular file: %s", path)
	}
	meta, err := scanRecentLogFileContext(ctx, f, info.Size(), fn)
	if err != nil {
		return meta, err
	}
	currentInfo, err := f.Stat()
	if err != nil {
		return meta, err
	}
	if currentInfo.Size() != info.Size() ||
		(currentInfo.Size() == info.Size() && fileMetadataChanged(info, currentInfo)) {
		meta.SourceChangedDuringScan = true
		meta.Complete = false
	} else if activeLogSourceChanged(logDir, source, path, info) {
		meta.SourceChangedDuringScan = true
		meta.Complete = false
	}
	return meta, nil
}

func activeLogSourceChanged(logDir string, source logSourceSpec, path string, original os.FileInfo) bool {
	currentPath, err := resolveLogFile(logDir, source.file)
	if err != nil || currentPath != path {
		return true
	}
	current, err := openConfinedRegularFile(currentPath, logDir)
	if err != nil {
		return true
	}
	defer current.Close()
	currentInfo, err := current.Stat()
	return err != nil || !os.SameFile(original, currentInfo) || fileMetadataChanged(original, currentInfo)
}

func scanRecentLogFileContext(ctx context.Context, f *os.File, sourceSize int64, fn func(line string)) (LogScanMeta, error) {
	meta := LogScanMeta{WindowLimitBytes: maxRecentLogScanBytes, SourceSizeBytes: sourceSize}
	windowBytes := min(sourceSize, maxRecentLogScanBytes)
	start := sourceSize - windowBytes
	meta.OmittedPrefixBytes = start
	section := io.NewSectionReader(f, start, windowBytes)
	reader := bufio.NewReaderSize(section, 64*1024)
	sourceEnded := false
	finish := func() LogScanMeta {
		meta.ScannedBytes, _ = section.Seek(0, io.SeekCurrent)
		if sourceEnded && meta.ScannedBytes < windowBytes {
			meta.SourceChangedDuringScan = true
		}
		meta.Complete = start == 0 && meta.ScannedBytes == meta.SourceSizeBytes && meta.OversizedLines == 0 && !meta.IncompleteTail && !meta.SourceChangedDuringScan
		return meta
	}

	if start > 0 {
		var previous [1]byte
		if _, err := f.ReadAt(previous[:], start-1); err != nil {
			if errors.Is(err, io.EOF) {
				sourceEnded = true
				return finish(), nil
			}
			return finish(), err
		}
		if previous[0] != '\n' {
			for {
				if err := ctx.Err(); err != nil {
					return finish(), err
				}
				_, err := reader.ReadSlice('\n')
				switch err {
				case nil:
					goto scan
				case bufio.ErrBufferFull:
					continue
				case io.EOF:
					sourceEnded = true
					return finish(), nil
				default:
					return finish(), err
				}
			}
		}
	}

scan:
	for {
		if err := ctx.Err(); err != nil {
			return finish(), err
		}
		line, oversized, terminated, eof, err := readCappedLine(reader, maxLogLineBytes)
		if err != nil {
			return finish(), err
		}
		if eof {
			sourceEnded = true
			break
		}
		if oversized {
			meta.OversizedLines++
			if !terminated {
				meta.IncompleteTail = true
				break
			}
			continue
		}
		if !terminated {
			meta.IncompleteTail = true
			break
		}
		fn(string(line))
	}
	return finish(), nil
}

func sanitizeLogLine(line LogLine) LogLine {
	var truncated bool
	line.Timestamp = redactUntrustedText(line.Timestamp)
	line.Timestamp, truncated = truncateUTF8Bytes(line.Timestamp, maxLogTimestampBytes)
	line.Truncated = line.Truncated || truncated
	if line.File != nil {
		file := redactUntrustedText(*line.File)
		file, truncated = truncateUTF8Bytes(file, maxLogFileFieldBytes)
		line.File = &file
		line.Truncated = line.Truncated || truncated
	}
	if line.Target != nil {
		target := redactUntrustedText(*line.Target)
		target, truncated = truncateUTF8Bytes(target, maxLogFileFieldBytes)
		line.Target = &target
		line.Truncated = line.Truncated || truncated
	}
	line.Message = redactUntrustedText(line.Message)
	line.Message, truncated = truncateUTF8Bytes(line.Message, maxLogMessageBytes)
	line.Truncated = line.Truncated || truncated
	return line
}

// searchableLogLine renders only the sanitized, bounded fields exposed by the
// tool. Grep must never match raw hidden text and then reveal a yes/no result
// through total_matches, because that would turn redaction into a secret oracle.
func searchableLogLine(line LogLine) string {
	parts := []string{string(line.Level), line.Timestamp, line.Message}
	if line.ElapsedMs != nil {
		parts = append(parts, strconv.FormatUint(*line.ElapsedMs, 10))
	}
	if line.File != nil {
		parts = append(parts, *line.File)
	}
	if line.Target != nil {
		parts = append(parts, *line.Target)
	}
	if line.Line != nil {
		parts = append(parts, strconv.FormatUint(uint64(*line.Line), 10))
	}
	return strings.Join(parts, " ")
}

func tailLogForSourceContextWithMeta(ctx context.Context, logDir string, source logSourceSpec, lines int, levelFilter *LogLevel, since string) ([]LogLine, int, LogScanMeta, error) {
	sinceFilter, err := parseLogSinceFilter(since)
	if err != nil {
		return nil, 0, LogScanMeta{}, err
	}
	max := min(lines, maxReturnLines)
	if max <= 0 {
		return []LogLine{}, 0, LogScanMeta{WindowLimitBytes: maxRecentLogScanBytes, Complete: true}, nil
	}
	// Ring buffer of the last `max` matching lines within the bounded recent
	// window: memory is bounded by max, not by source size.
	ring := make([]LogLine, max)
	n := 0
	unparsed := 0
	incomparable := 0
	meta, err := scanRecentLogLinesForSourceContext(ctx, logDir, source, func(line string) {
		l, ok := source.parse(line)
		if !ok {
			if strings.TrimSpace(line) != "" {
				unparsed++
			}
			return
		}
		if levelFilter != nil && l.Level.rank() < levelFilter.rank() {
			return
		}
		passes, comparable := applySinceFilter(l, sinceFilter)
		if !comparable {
			incomparable++
		}
		if !passes {
			return
		}
		ring[n%max] = sanitizeLogLine(l)
		n++
	})
	if err != nil {
		return nil, 0, meta, err
	}
	meta.UnparsedLines = unparsed
	meta.IncomparableSinceLines = incomparable
	meta.Complete = meta.Complete && unparsed == 0 && incomparable == 0
	count := min(n, max)
	start := 0
	if n > max {
		start = n % max // oldest retained entry
	}
	out := make([]LogLine, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, ring[(start+i)%max])
	}
	return out, n, meta, nil
}

func tailLogContextWithMeta(ctx context.Context, logDir string, lines int, levelFilter *LogLevel, since string) ([]LogLine, int, LogScanMeta, error) {
	return tailLogForSourceContextWithMeta(ctx, logDir, mithrilLogSource, lines, levelFilter, since)
}

func grepLogForSourceContextWithMeta(ctx context.Context, logDir string, source logSourceSpec, pattern string, maxMatches int, since string) ([]LogLine, int, LogScanMeta, error) {
	if len(pattern) > maxPatternLength {
		return nil, 0, LogScanMeta{}, fmt.Errorf("regex pattern too long: %d chars exceeds %d limit", len(pattern), maxPatternLength)
	}
	re, err := regexp.Compile(pattern) // RE2: linear-time, ReDoS-safe.
	if err != nil {
		return nil, 0, LogScanMeta{}, fmt.Errorf("invalid regex: %w", err)
	}
	sinceFilter, err := parseLogSinceFilter(since)
	if err != nil {
		return nil, 0, LogScanMeta{}, err
	}
	max := min(maxMatches, maxReturnLines)
	if max < 0 {
		max = 0
	}
	// The scan runs oldest to newest; retain only the newest bounded matches.
	ring := make([]LogLine, max)
	total := 0
	unparsed := 0
	incomparable := 0
	meta, err := scanRecentLogLinesForSourceContext(ctx, logDir, source, func(line string) {
		l, ok := source.parse(line)
		if !ok {
			if strings.TrimSpace(line) != "" {
				unparsed++
			}
			return
		}
		passes, comparable := applySinceFilter(l, sinceFilter)
		if !comparable {
			incomparable++
		}
		if !passes {
			return
		}
		l = sanitizeLogLine(l)
		if !re.MatchString(searchableLogLine(l)) {
			return
		}
		total++
		if max > 0 {
			ring[(total-1)%max] = l
		}
	})
	if err != nil {
		return nil, 0, meta, err
	}
	meta.UnparsedLines = unparsed
	meta.IncomparableSinceLines = incomparable
	meta.Complete = meta.Complete && unparsed == 0 && incomparable == 0
	count := min(total, max)
	var matches []LogLine
	if count > 0 {
		start := 0
		if total > max {
			start = total % max
		}
		matches = make([]LogLine, 0, count)
		for i := 0; i < count; i++ {
			matches = append(matches, ring[(start+i)%max])
		}
	}
	return matches, total, meta, nil
}

type tailLogInput struct {
	Lines int    `json:"lines,omitempty" jsonschema:"number of lines to return (max 100, default 100)"`
	Level string `json:"level,omitempty" jsonschema:"minimum level filter: debug, info, warn, or error"`
	Since string `json:"since,omitempty" jsonschema:"only lines at/after this time (ISO-8601 or Mithril mlog elapsed like 1m30s)"`
}

type tailLogOutput struct {
	Source    string      `json:"source"`
	LogDir    string      `json:"log_dir"`
	Count     int         `json:"count"`
	Truncated bool        `json:"truncated"`
	Scan      LogScanMeta `json:"scan"`
	Lines     []LogLine   `json:"lines"`
}

type grepLogInput struct {
	Pattern    string `json:"pattern" jsonschema:"RE2 regular expression to match against sanitized parsed log fields"`
	Since      string `json:"since,omitempty" jsonschema:"only lines at/after this time (ISO-8601 or Mithril mlog elapsed)"`
	MaxMatches int    `json:"max_matches,omitempty" jsonschema:"max matches to return (max 100, default 100)"`
}

type grepLogOutput struct {
	Source       string      `json:"source"`
	LogDir       string      `json:"log_dir"`
	TotalMatches int         `json:"total_matches"`
	Returned     int         `json:"returned"`
	Truncated    bool        `json:"truncated"`
	Scan         LogScanMeta `json:"scan"`
	Matches      []LogLine   `json:"matches"`
}

func anyLogLineTruncated(lines []LogLine) bool {
	for _, line := range lines {
		if line.Truncated {
			return true
		}
	}
	return false
}

func registerLogTools(server *mcpsdk.Server, cfg Config) {
	addTool(server, cfg, &mcpsdk.Tool{
		Name:        "mithril_tail_log",
		Annotations: annReadOnlyLocal,
		Description: "Return up to 100 sanitized lines from the newest 8 MiB of the active Mithril log.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, in tailLogInput) (*mcpsdk.CallToolResult, tailLogOutput, error) {
		logDir, err := requireConfiguredPath(cfg.LogDir, "MITHRIL_LOG_DIR is not configured")
		if err != nil {
			return nil, tailLogOutput{}, err
		}
		var levelFilter *LogLevel
		if in.Level != "" {
			lvl, ok := parseLevelName(in.Level)
			if !ok {
				return nil, tailLogOutput{}, fmt.Errorf("invalid level: must be debug, info, warn, or error")
			}
			levelFilter = &lvl
		}
		n := in.Lines
		if n <= 0 {
			n = 100
		}
		lines, total, scan, err := tailLogContextWithMeta(ctx, logDir, n, levelFilter, in.Since)
		if err != nil {
			return nil, tailLogOutput{}, err
		}
		truncated := !scan.Complete || total > len(lines) || anyLogLineTruncated(lines)
		return nil, tailLogOutput{Source: logSourceMithril, LogDir: logDir, Count: len(lines), Truncated: truncated, Scan: scan, Lines: lines}, nil
	})

	addTool(server, cfg, &mcpsdk.Tool{
		Name:        "mithril_grep_log",
		Annotations: annReadOnlyLocal,
		Description: "Search sanitized fields in the newest 8 MiB of the active Mithril log with RE2.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, in grepLogInput) (*mcpsdk.CallToolResult, grepLogOutput, error) {
		logDir, err := requireConfiguredPath(cfg.LogDir, "MITHRIL_LOG_DIR is not configured")
		if err != nil {
			return nil, grepLogOutput{}, err
		}
		n := in.MaxMatches
		if n <= 0 {
			n = 100
		}
		matches, total, scan, err := grepLogForSourceContextWithMeta(ctx, logDir, mithrilLogSource, in.Pattern, n, in.Since)
		if err != nil {
			return nil, grepLogOutput{}, err
		}
		truncated := !scan.Complete || total > len(matches) || anyLogLineTruncated(matches)
		return nil, grepLogOutput{Source: logSourceMithril, LogDir: logDir, TotalMatches: total, Returned: len(matches), Truncated: truncated, Scan: scan, Matches: matches}, nil
	})
}
