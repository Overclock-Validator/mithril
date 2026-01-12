package mlog

import (
	"bufio"
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
	enableVerbose *atomic.Bool
	level         LogLevel
	toStdout      bool
	writer        io.Writer // combined writer (file + optional stdout)
	bufWriter     *bufio.Writer
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

	// Create multi-writer if console output is enabled (use stderr to share stream with progress bars)
	if cfg.ToStdout {
		Log.writer = io.MultiWriter(os.Stderr, Log.fileWriter)
	} else {
		Log.writer = Log.fileWriter
	}

	// Wrap with buffered writer for performance
	Log.bufWriter = bufio.NewWriterSize(Log.writer, 16*1024) // 16KB buffer

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
	go Log.flushLoop()

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

// flushLoop periodically flushes the buffer and syncs to disk
func (l *logger) flushLoop() {
	defer l.wg.Done()

	flushTicker := time.NewTicker(2 * time.Second)
	syncTicker := time.NewTicker(30 * time.Second)
	defer flushTicker.Stop()
	defer syncTicker.Stop()

	for {
		select {
		case <-l.stopCh:
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
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.bufWriter != nil {
		l.bufWriter.Flush()
	}
}

// Flush flushes the log buffer to ensure all pending messages are written.
// Call this before starting progress bars or other terminal output that depends on cursor position.
func Flush() {
	Log.flush()
}

// flushAndSync flushes and syncs to disk
func (l *logger) flushAndSync() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.bufWriter != nil {
		l.bufWriter.Flush()
	}
	// lumberjack doesn't expose Sync, but flush is sufficient for most cases
}

// Shutdown flushes all pending writes and closes the log file
func Shutdown() {
	Log.mu.Lock()
	defer Log.mu.Unlock()

	if !Log.initialized {
		return
	}

	// Stop the flush goroutine
	if Log.stopCh != nil {
		close(Log.stopCh)
		Log.mu.Unlock()
		Log.wg.Wait()
		Log.mu.Lock()
	}

	// Final flush
	if Log.bufWriter != nil {
		Log.bufWriter.Flush()
	}

	// Close lumberjack
	if Log.fileWriter != nil {
		Log.fileWriter.Close()
	}

	Log.initialized = false
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

	if l.bufWriter != nil {
		l.bufWriter.WriteString(msg)
	} else if l.writer != nil {
		l.writer.Write([]byte(msg))
	} else {
		// Fallback to stderr if not initialized
		fmt.Fprint(os.Stderr, msg)
	}
}

// writeImmediate outputs a log message and immediately flushes (for errors)
func (l *logger) writeImmediate(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.bufWriter != nil {
		l.bufWriter.WriteString(msg)
		l.bufWriter.Flush()
	} else if l.writer != nil {
		l.writer.Write([]byte(msg))
	} else {
		fmt.Fprint(os.Stderr, msg)
	}
}

// writeFileOnly outputs a log message only to the file, not to stdout/stderr.
// Used for diagnostics that should be captured for debugging but not clutter the terminal.
func (l *logger) writeFileOnly(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Write only to file writer, bypassing the multi-writer
	if l.fileWriter != nil {
		l.fileWriter.Write([]byte(msg))
	}
	// If no file writer, silently discard (file-only logging disabled)
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

// InfofPrecise logs with millisecond precision timing (for block replay)
// Flushes immediately to ensure real-time output during replay
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
