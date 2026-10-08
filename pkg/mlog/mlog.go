package mlog

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/version"
	"gopkg.in/natefinch/lumberjack.v2"
)

var programStartTime = time.Now()

// LogLevel represents the logging level
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)

// LogConfig holds logging configuration (mirrors config.LogConfig)
type LogConfig struct {
	Dir        string // Log directory (default: /mnt/mithril-logs)
	Level      string // Log level: debug, info, warn, error
	ToStdout   bool   // Also write to stdout (default: true)
	MaxSizeMB  int    // Max log file size in MB before rotation
	MaxAgeDays int    // Delete logs older than this many days
	MaxBackups int    // Keep up to N old log files
}

type logger struct {
	mu            sync.Mutex
	fileMu        sync.Mutex
	enableVerbose *atomic.Bool
	level         LogLevel
	toStdout      bool
	writer        io.Writer // console writer (stderr) or fallback writer in stderr-only mode
	fileBuffer    bytes.Buffer
	fileWriter    *lumberjack.Logger
	logPath       string
	runDir        string // per-run directory (e.g., mithril-logs/20250104-120000Z_abc123_12345678/)
	baseDir       string // base log directory (e.g., /mnt/mithril-logs)
	runID         string
	initialized   bool
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

var Log = logger{enableVerbose: &atomic.Bool{}, toStdout: true}

// DefaultConfig returns sensible defaults for logging
func DefaultConfig() LogConfig {
	return LogConfig{
		Dir:        "/mnt/mithril-logs",
		Level:      "info",
		ToStdout:   true,
		MaxSizeMB:  100,
		MaxAgeDays: 7,
		MaxBackups: 10,
	}
}

// Initialize sets up file logging with the given config and run ID.
// If dir is empty, only stdout logging is used.
//
// Directory structure:
//
//	<dir>/
//	├── runs.log                               # Append-only log tracking all runs
//	├── latest -> <run_dir>/                   # Symlink to latest run directory
//	├── 20250104-120000Z_abc123_12345678/      # Per-run directory
//	│   ├── mithril.log                        # Main log file
//	│   ├── config.toml                        # Copy of config used for this run
//	│   └── leader_schedule/                   # Leader schedule artifacts
//	│       ├── epoch905_local_12345678_validators.csv
//	│       ├── epoch905_local_12345678_skipped.csv
//	│       ├── epoch905_local_12345678_summary.txt
//	│       └── mismatch_12345678.log
//	└── 20250104-130000Z_def456_87654321/
//	    └── ...
func Initialize(cfg LogConfig, runID string) error {
	Log.mu.Lock()
	defer Log.mu.Unlock()

	if Log.initialized {
		return nil // Already initialized
	}

	Log.runID = runID
	Log.toStdout = cfg.ToStdout
	Log.level = parseLevel(cfg.Level)
	Log.baseDir = cfg.Dir

	// If no dir specified, stderr only (stderr to avoid breaking progress bar cursor positioning)
	if cfg.Dir == "" {
		Log.writer = os.Stderr
		Log.initialized = true
		return nil
	}

	// Create base log directory if it doesn't exist
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory %s: %w", cfg.Dir, err)
	}

	// Build per-run directory name: YYYYMMDD-HHMMSSZ_<commit>_<runid>
	now := time.Now().UTC()
	shortRunID := runID
	if len(shortRunID) > 8 {
		shortRunID = shortRunID[:8]
	}
	shortCommit := version.GitCommit
	if len(shortCommit) > 7 {
		shortCommit = shortCommit[:7]
	}
	runDirName := fmt.Sprintf("%s_%s_%s", now.Format("20060102-150405Z"), shortCommit, shortRunID)
	Log.runDir = filepath.Join(cfg.Dir, runDirName)

	// Create per-run directory
	if err := os.MkdirAll(Log.runDir, 0755); err != nil {
		return fmt.Errorf("failed to create run directory %s: %w", Log.runDir, err)
	}

	// Main log file goes inside the run directory
	Log.logPath = filepath.Join(Log.runDir, "mithril.log")

	// Set up lumberjack for rotation
	Log.fileWriter = &lumberjack.Logger{
		Filename:   Log.logPath,
		MaxSize:    cfg.MaxSizeMB,
		MaxAge:     cfg.MaxAgeDays,
		MaxBackups: cfg.MaxBackups,
		LocalTime:  false, // Use UTC
		Compress:   true,  // gzip old logs
	}

	// Keep console output and file output independent. A slow or wedged file
	// writer must not stall live stderr progress reporting or replay itself.
	if cfg.ToStdout {
		Log.writer = os.Stderr
	} else {
		Log.writer = nil
	}

	// Create symlink to latest run directory
	symlinkPath := filepath.Join(cfg.Dir, "latest")
	os.Remove(symlinkPath) // Ignore error if doesn't exist
	if err := os.Symlink(runDirName, symlinkPath); err != nil {
		// Non-fatal, just log to stdout
		fmt.Fprintf(os.Stderr, "warning: failed to create symlink %s: %v\n", symlinkPath, err)
	}

	// Append to runs.log at the base directory level (tracks all runs)
	runsLogPath := filepath.Join(cfg.Dir, "runs.log")
	appendRunsLogEntry(runsLogPath, now, runID, shortCommit, runDirName)

	// Start background flush goroutine
	Log.stopCh = make(chan struct{})
	Log.wg.Add(1)
	go Log.flushLoop(Log.stopCh)

	Log.initialized = true
	return nil
}

// appendRunsLogEntry appends an entry to the runs.log file
func appendRunsLogEntry(runsLogPath string, ts time.Time, runID, commit, runDir string) {
	f, err := os.OpenFile(runsLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: failed to open runs.log: %v\n", err)
		return
	}
	defer f.Close()

	entry := fmt.Sprintf("[%s] run_id=%s commit=%s version=%s dir=%s\n",
		ts.Format(time.RFC3339), runID, commit, version.Version, runDir)
	if _, err := f.WriteString(entry); err != nil {
		fmt.Fprintf(os.Stderr, "warning: failed to write to runs.log: %v\n", err)
	}
}

// CreateSubprocessWriter returns a writer for a named subprocess (e.g. "lightbringer")
// that routes output to a dedicated log file in the current run directory. Subprocess
// output is not mirrored to the terminal, keeping Mithril's own output readable.
// When file logging is not initialized, output falls back to stderr with a
// "[name] " prefix so developers running without a log directory can still see it.
func (l *logger) CreateSubprocessWriter(name string) io.Writer {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.initialized || l.runDir == "" {
		return newPrefixWriter(os.Stderr, "["+name+"] ")
	}

	return &lumberjack.Logger{
		Filename:   filepath.Join(l.runDir, name+".log"),
		MaxSize:    100, // MB
		MaxAge:     7,
		MaxBackups: 5,
		LocalTime:  false,
		Compress:   true,
	}
}

// prefixWriter wraps an io.Writer and prepends a prefix to each line.
// Used only as a fallback when subprocess file logging is not available.
type prefixWriter struct {
	w      io.Writer
	prefix string
}

func newPrefixWriter(w io.Writer, prefix string) *prefixWriter {
	return &prefixWriter{w: w, prefix: prefix}
}

func (pw *prefixWriter) Write(p []byte) (int, error) {
	lines := bytes.Split(p, []byte("\n"))
	for i, line := range lines {
		// Skip the trailing empty element produced by bytes.Split when input ends in '\n'.
		if len(line) == 0 && i == len(lines)-1 {
			continue
		}
		if _, err := fmt.Fprintf(pw.w, "%s%s\n", pw.prefix, line); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// flushLoop periodically flushes the buffer and syncs to disk
func (l *logger) flushLoop(stopCh <-chan struct{}) {
	defer l.wg.Done()

	flushTicker := time.NewTicker(2 * time.Second)
	syncTicker := time.NewTicker(30 * time.Second)
	defer flushTicker.Stop()
	defer syncTicker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-flushTicker.C:
			l.flush()
		case <-syncTicker.C:
			l.flushAndSync()
		}
	}
}

// flush flushes the buffer without syncing
func (l *logger) flush() {
	l.writePending(l.drainPending())
}

// Flush flushes the log buffer to ensure all pending messages are written.
// Call this before starting progress bars or other terminal output that depends on cursor position.
func Flush() {
	Log.flush()
}

// flushAndSync flushes and syncs to disk
func (l *logger) flushAndSync() {
	l.writePending(l.drainPending())
	// lumberjack doesn't expose Sync, but draining the in-memory buffer is
	// sufficient for most cases.
}

// Shutdown flushes all pending writes and closes the log file
func Shutdown() {
	Log.mu.Lock()
	if !Log.initialized {
		Log.mu.Unlock()
		return
	}

	// Stop the flush goroutine
	stopCh := Log.stopCh
	Log.stopCh = nil
	Log.mu.Unlock()

	if stopCh != nil {
		close(stopCh)
		Log.wg.Wait()
	}

	Log.writePending(Log.drainPending())

	Log.fileMu.Lock()
	if Log.fileWriter != nil {
		Log.fileWriter.Close()
	}
	Log.fileMu.Unlock()

	namedMu.Lock()
	for _, f := range namedFiles {
		_ = f.Close()
	}
	namedFiles = nil
	namedMu.Unlock()

	Log.mu.Lock()
	Log.initialized = false
	Log.mu.Unlock()
}

// Named auxiliary log files: diagnostic streams voluminous enough to drown
// the console or make the main log hard to read live in their own files
// under the per-run directory. One unbuffered append per line — at the few
// lines per second these streams run, that is cheap, crash-safe (nothing
// buffered to lose), and needs no flusher.
var (
	namedMu    sync.Mutex
	namedFiles map[string]*os.File
)

// NamedFilef appends a timestamped line to <runDir>/<name>.log, creating the
// file on first use. Nothing goes to the console or the main log. Before
// logging is initialized (no run directory yet) the line falls back to the
// main file log so it is never silently dropped.
func NamedFilef(name string, format string, args ...interface{}) {
	Log.mu.Lock()
	dir := Log.runDir
	Log.mu.Unlock()
	if dir == "" {
		Log.FileOnlyf(format, args...)
		return
	}
	namedMu.Lock()
	defer namedMu.Unlock()
	if namedFiles == nil {
		namedFiles = make(map[string]*os.File)
	}
	f := namedFiles[name]
	if f == nil {
		var err error
		f, err = os.OpenFile(filepath.Join(dir, name+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return
		}
		namedFiles[name] = f
	}
	fmt.Fprintf(f, "%s%s\n", relativePrefix(), fmt.Sprintf(format, args...))
}

// GetLogPath returns the path to the current log file
func GetLogPath() string {
	Log.mu.Lock()
	defer Log.mu.Unlock()
	return Log.logPath
}

// GetRunID returns the run ID for the current session
func GetRunID() string {
	Log.mu.Lock()
	defer Log.mu.Unlock()
	return Log.runID
}

// GetLogDir returns the per-run log directory path (where mithril.log and artifacts go)
func GetLogDir() string {
	Log.mu.Lock()
	defer Log.mu.Unlock()
	return Log.runDir
}

// GetBaseLogDir returns the base log directory (parent of all run directories)
func GetBaseLogDir() string {
	Log.mu.Lock()
	defer Log.mu.Unlock()
	return Log.baseDir
}

// SaveRunConfig saves a copy of the config to the run directory.
// Should be called after Initialize with the full config content.
func SaveRunConfig(configContent []byte) error {
	Log.mu.Lock()
	runDir := Log.runDir
	Log.mu.Unlock()

	if runDir == "" {
		return nil // No run directory (stderr-only mode)
	}

	configPath := filepath.Join(runDir, "config.toml")
	if err := os.WriteFile(configPath, configContent, 0644); err != nil {
		return fmt.Errorf("failed to save config to run directory: %w", err)
	}
	return nil
}

// parseLevel converts a string level to LogLevel
func parseLevel(level string) LogLevel {
	switch level {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

// relativePrefix returns the elapsed time since program start as a prefix (no milliseconds)
func relativePrefix() string {
	d := time.Since(programStartTime)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := int(d.Seconds())

	var timeStr string
	if h > 0 {
		timeStr = fmt.Sprintf("%2dh%02dm", h, m)
	} else if m > 0 {
		timeStr = fmt.Sprintf("%2dm%02ds", m, s)
	} else {
		timeStr = fmt.Sprintf("%5ds", s)
	}
	return fmt.Sprintf("(+%s) ", timeStr)
}

// relativePrefixPrecise returns the elapsed time with millisecond precision (for block replay)
// Uses fixed 12-char width for times under 10 hours to keep log columns aligned.
func relativePrefixPrecise() string {
	d := time.Since(programStartTime)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	secs := d.Seconds() // includes fractional seconds

	var timeStr string
	if h >= 10 {
		// Double digit hours: allow expansion beyond 12 chars
		timeStr = fmt.Sprintf("%dh%02dm%06.3fs", h, m, secs)
	} else if h > 0 {
		// Single digit hours: 12 chars (e.g., "1h30m45.123s")
		timeStr = fmt.Sprintf("%dh%02dm%06.3fs", h, m, secs)
	} else if m > 0 {
		// Minutes only: pad to 12 chars (e.g., "  30m45.123s")
		timeStr = fmt.Sprintf("  %2dm%06.3fs", m, secs)
	} else {
		// Seconds only: pad to 12 chars (e.g., "     45.123s")
		timeStr = fmt.Sprintf("     %06.3fs", secs)
	}
	return fmt.Sprintf("(+%s) ", timeStr)
}

// write outputs a log message to the configured writer(s)
func (l *logger) write(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.writeLocked(msg)
}

func (l *logger) writeLocked(msg string) {
	wrote := false

	if l.writer != nil {
		l.writer.Write([]byte(msg))
		wrote = true
	}

	if l.fileWriter != nil {
		l.fileBuffer.WriteString(msg)
		wrote = true
	}

	if !wrote {
		// Fallback to stderr if not initialized
		fmt.Fprint(os.Stderr, msg)
	}
}

// writeImmediate outputs a log message with immediate terminal visibility.
// File persistence is handled by the background flush loop so hot-path logs
// cannot stall replay on slow disk writes.
func (l *logger) writeImmediate(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.writeLocked(msg)
}

// writeFileOnly outputs a log message only to the file, not to stdout/stderr.
// Used for diagnostics that should be captured for debugging but not clutter the terminal.
func (l *logger) writeFileOnly(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.fileWriter != nil {
		l.fileBuffer.WriteString(msg)
	}
	// If no file writer, silently discard (file-only logging disabled)
}

func (l *logger) drainPending() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.drainPendingLocked()
}

func (l *logger) drainPendingLocked() []byte {
	if l.fileBuffer.Len() == 0 {
		return nil
	}
	pending := append([]byte(nil), l.fileBuffer.Bytes()...)
	l.fileBuffer.Reset()
	return pending
}

func (l *logger) writePending(pending []byte) {
	if len(pending) == 0 || l.fileWriter == nil {
		return
	}
	l.fileMu.Lock()
	defer l.fileMu.Unlock()
	if _, err := l.fileWriter.Write(pending); err != nil {
		fmt.Fprintf(os.Stderr, "warning: failed to write mithril log: %v\n", err)
	}
}

func (l *logger) Debugf(format string, args ...interface{}) {
	if l.level > LevelDebug && !l.enableVerbose.Load() {
		return
	}
	msg := fmt.Sprintf("%s%s\n", relativePrefix(), fmt.Sprintf(format, args...))
	l.write(msg)
}

func (l *logger) Infof(format string, args ...interface{}) {
	if l.level > LevelInfo {
		return
	}
	msg := fmt.Sprintf("%s%s\n", relativePrefix(), fmt.Sprintf(format, args...))
	l.write(msg)
}

// FileOnlyf logs a message only to the log file, not to the terminal.
// Used for diagnostics that should be captured for debugging but not clutter the terminal.
func (l *logger) FileOnlyf(format string, args ...interface{}) {
	if l.level > LevelInfo {
		return
	}
	msg := fmt.Sprintf("%s%s\n", relativePrefix(), fmt.Sprintf(format, args...))
	l.writeFileOnly(msg)
}

// InfofPrecise logs with millisecond precision timing (for block replay).
// Terminal visibility is immediate; file persistence is buffered.
func (l *logger) InfofPrecise(format string, args ...interface{}) {
	if l.level > LevelInfo {
		return
	}
	msg := fmt.Sprintf("%s%s\n", relativePrefixPrecise(), fmt.Sprintf(format, args...))
	l.writeImmediate(msg)
}

func (l *logger) Warnf(format string, args ...interface{}) {
	if l.level > LevelWarn {
		return
	}
	msg := fmt.Sprintf("%sWARN: %s\n", relativePrefix(), fmt.Sprintf(format, args...))
	l.writeImmediate(msg) // Warnings flush immediately
}

func (l *logger) Errorf(format string, args ...interface{}) {
	msg := fmt.Sprintf("%sERROR: %s\n", relativePrefix(), fmt.Sprintf(format, args...))
	l.writeImmediate(msg) // Errors always flush immediately
}

func (l *logger) EnableInfLogging() {
	l.enableVerbose.Store(true)
}

func (l *logger) DisableInfLogging() {
	l.enableVerbose.Store(false)
}
