// Package mcp exposes Mithril diagnostics through the Model Context Protocol.
// The stdio server runs outside the node and reads only profile-approved data.
package mcp

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

// Default thresholds used by the diagnose/cross-check tools.
const (
	// DefaultReplayP99WarnMs marks slower replay as degraded.
	DefaultReplayP99WarnMs = 400.0

	// DefaultSlotsBehindWarn allows normal replay and propagation lag.
	DefaultSlotsBehindWarn uint64 = 150

	// Disk thresholds are percentages of the configured filesystem capacity.
	DefaultDiskWarnPercent     = 85.0
	DefaultDiskCriticalPercent = 95.0

	// Admission limits protect the node from expensive request bursts.
	DefaultMaxConcurrent     = 4
	DefaultRatePerSecond     = 5.0
	DefaultRateBurst         = 10
	DefaultOutputBudgetBytes = 1 << 20 // 1 MiB
	// DefaultToolCallTimeout bounds one tool call. mithril_diagnose chains
	// several 10s outbound requests, so the ceiling sits well above them.
	DefaultToolCallTimeout = 3 * time.Minute
	MaxToolCallTimeout     = 10 * time.Minute

	// The minimum leaves room for the fixed output-rejection result.
	MinOutputBudgetBytes = 256
	// The maximum caps the encoded tool-result body. The JSON-RPC envelope and
	// request ID are outside this limit.
	MaxOutputBudgetBytes = 4 << 20 // 4 MiB
	MaxConcurrentLimit   = 64
	MaxRatePerSecond     = 1000.0
	MaxRateBurst         = 256
)

// Profile selects the server's least-privilege registration and call policy.
type Profile string

const (
	ProfileMonitor    Profile = "monitor"
	ProfileDiagnostic Profile = "diagnostic"
)

// ParseProfile validates a user-supplied profile name.
func ParseProfile(raw string) (Profile, error) {
	switch Profile(strings.ToLower(strings.TrimSpace(raw))) {
	case ProfileMonitor:
		return ProfileMonitor, nil
	case ProfileDiagnostic:
		return ProfileDiagnostic, nil
	default:
		return "", fmt.Errorf("unknown MCP profile %q (want monitor or diagnostic)", raw)
	}
}

// ParseBlockSource validates the configured node block source.
func ParseBlockSource(raw string) (string, error) {
	source := strings.ToLower(strings.TrimSpace(raw))
	switch source {
	case "rpc", "lightbringer", "turbine":
		return source, nil
	default:
		return "", fmt.Errorf("unknown Mithril block source %q (want rpc, lightbringer, or turbine)", raw)
	}
}

// Config holds the MCP server settings, populated from environment variables.
type Config struct {
	Profile Profile

	MetricsURL          string // Mithril Prometheus metrics endpoint.
	RPCURL              string // Mithril JSON-RPC endpoint.
	LogDir              string // Mithril log directory (optional).
	AccountsDir         string // AccountsDB directory (optional).
	SnapshotsDir        string // Snapshot directory (optional).
	ShredstoreDir       string // Shredstore directory (optional).
	StatePath           string // Mithril state file (optional).
	ReplayPath          string // replay_timings.jsonl path (optional).
	PprofURL            string // pprof endpoint (optional).
	ReferenceRPCURL     string // Trusted external Solana RPC for slots-behind cross-checks (optional; process-configured only).
	ReplayP99WarnMs     float64
	SlotsBehindWarn     uint64
	DiskWarnPercent     float64
	DiskCriticalPercent float64
	NodeCgroupPath      string // Explicit cgroup-v2 directory for OOM/limit evidence (optional).
	BlockSource         string // Configured node block source when known.

	MaxConcurrent     int
	RatePerSecond     float64
	RateBurst         int
	OutputBudgetBytes int
	ToolCallTimeout   time.Duration
}

// ConfigFromEnv reads configuration from environment variables with sensible
// defaults matching a co-located Mithril node.
func ConfigFromEnv() Config {
	cfg := Config{
		Profile:             profileFromEnv(),
		MetricsURL:          envOrWhenUnset("MITHRIL_METRICS_URL", "http://127.0.0.1:9090/metrics"),
		RPCURL:              envOrWhenUnset("MITHRIL_RPC_URL", "http://127.0.0.1:8899"),
		LogDir:              os.Getenv("MITHRIL_LOG_DIR"),
		AccountsDir:         os.Getenv("MITHRIL_ACCOUNTS_PATH"),
		SnapshotsDir:        os.Getenv("MITHRIL_SNAPSHOTS_PATH"),
		ShredstoreDir:       os.Getenv("MITHRIL_SHREDSTORE_PATH"),
		StatePath:           os.Getenv("MITHRIL_STATE_PATH"),
		ReplayPath:          os.Getenv("MITHRIL_REPLAY_PATH"),
		PprofURL:            os.Getenv("MITHRIL_PPROF_URL"),
		ReferenceRPCURL:     os.Getenv("MITHRIL_REFERENCE_RPC_URL"),
		ReplayP99WarnMs:     parseEnvPositiveFloat("MITHRIL_REPLAY_P99_WARN_MS", DefaultReplayP99WarnMs),
		SlotsBehindWarn:     parseEnvUint("MITHRIL_SLOTS_BEHIND_WARN", DefaultSlotsBehindWarn),
		DiskWarnPercent:     parseEnvPositiveFloat("MITHRIL_DISK_WARN_PERCENT", DefaultDiskWarnPercent),
		DiskCriticalPercent: parseEnvPositiveFloat("MITHRIL_DISK_CRITICAL_PERCENT", DefaultDiskCriticalPercent),
		NodeCgroupPath:      os.Getenv("MITHRIL_NODE_CGROUP_PATH"),
		BlockSource:         os.Getenv("MITHRIL_BLOCK_SOURCE"),
		MaxConcurrent:       parseEnvPositiveInt("MITHRIL_MCP_MAX_CONCURRENT", DefaultMaxConcurrent),
		RatePerSecond:       parseEnvPositiveFloat("MITHRIL_MCP_RATE_PER_SECOND", DefaultRatePerSecond),
		RateBurst:           parseEnvPositiveInt("MITHRIL_MCP_RATE_BURST", DefaultRateBurst),
		OutputBudgetBytes:   parseEnvOutputBudget("MITHRIL_MCP_OUTPUT_BUDGET_BYTES", DefaultOutputBudgetBytes),
		ToolCallTimeout:     parseEnvToolCallTimeout("MITHRIL_MCP_TOOL_TIMEOUT_SECONDS"),
	}
	return cfg.normalized()
}

func profileFromEnv() Profile {
	raw := os.Getenv("MITHRIL_MCP_PROFILE")
	if raw == "" {
		return ProfileMonitor
	}
	profile, err := ParseProfile(raw)
	if err != nil {
		return ProfileMonitor
	}
	return profile
}

// normalized supplies safe defaults for Config values constructed directly by
// callers and clamps every admission setting to its hard maximum.
func (cfg Config) normalized() Config {
	profile, err := ParseProfile(string(cfg.Profile))
	if err != nil {
		cfg.Profile = ProfileMonitor
	} else {
		cfg.Profile = profile
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = DefaultMaxConcurrent
	} else if cfg.MaxConcurrent > MaxConcurrentLimit {
		cfg.MaxConcurrent = MaxConcurrentLimit
	}
	if cfg.ReplayP99WarnMs <= 0 || math.IsNaN(cfg.ReplayP99WarnMs) || math.IsInf(cfg.ReplayP99WarnMs, 0) {
		cfg.ReplayP99WarnMs = DefaultReplayP99WarnMs
	}
	if cfg.DiskWarnPercent <= 0 || cfg.DiskWarnPercent >= 100 || math.IsNaN(cfg.DiskWarnPercent) || math.IsInf(cfg.DiskWarnPercent, 0) {
		cfg.DiskWarnPercent = DefaultDiskWarnPercent
	}
	if cfg.DiskCriticalPercent <= cfg.DiskWarnPercent || cfg.DiskCriticalPercent > 100 || math.IsNaN(cfg.DiskCriticalPercent) || math.IsInf(cfg.DiskCriticalPercent, 0) {
		cfg.DiskCriticalPercent = DefaultDiskCriticalPercent
		if cfg.DiskCriticalPercent <= cfg.DiskWarnPercent {
			cfg.DiskWarnPercent = DefaultDiskWarnPercent
		}
	}
	if cfg.BlockSource != "" {
		if source, err := ParseBlockSource(cfg.BlockSource); err == nil {
			cfg.BlockSource = source
		}
	}
	if cfg.RatePerSecond <= 0 || math.IsNaN(cfg.RatePerSecond) || math.IsInf(cfg.RatePerSecond, 0) {
		cfg.RatePerSecond = DefaultRatePerSecond
	} else if cfg.RatePerSecond > MaxRatePerSecond {
		cfg.RatePerSecond = MaxRatePerSecond
	}
	if cfg.RateBurst <= 0 {
		cfg.RateBurst = DefaultRateBurst
	} else if cfg.RateBurst > MaxRateBurst {
		cfg.RateBurst = MaxRateBurst
	}
	if cfg.OutputBudgetBytes <= 0 {
		cfg.OutputBudgetBytes = DefaultOutputBudgetBytes
	} else if cfg.OutputBudgetBytes < MinOutputBudgetBytes {
		cfg.OutputBudgetBytes = MinOutputBudgetBytes
	}
	if cfg.OutputBudgetBytes > MaxOutputBudgetBytes {
		cfg.OutputBudgetBytes = MaxOutputBudgetBytes
	}
	if cfg.ToolCallTimeout <= 0 || cfg.ToolCallTimeout > MaxToolCallTimeout {
		cfg.ToolCallTimeout = DefaultToolCallTimeout
	}
	return cfg
}

// parseEnvToolCallTimeout clamps before scaling so a huge value cannot wrap
// into a tiny duration.
func parseEnvToolCallTimeout(key string) time.Duration {
	maxSecs := int(MaxToolCallTimeout / time.Second)
	secs := parseEnvPositiveInt(key, int(DefaultToolCallTimeout/time.Second))
	if secs > maxSecs {
		secs = maxSecs
	}
	return time.Duration(secs) * time.Second
}

func envOrWhenUnset(key, def string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return def
}

func parseEnvUint(key string, def uint64) uint64 {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		klog.Warningf("mcp: ignoring invalid value for %s=%q; using default %d", key, raw, def)
		return def
	}
	return v
}

func parseEnvPositiveInt(key string, def int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		klog.Warningf("mcp: ignoring invalid value for %s=%q; using default %d", key, raw, def)
		return def
	}
	return v
}

func parseEnvPositiveFloat(key string, def float64) float64 {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		klog.Warningf("mcp: ignoring invalid value for %s=%q; using default %v", key, raw, def)
		return def
	}
	return v
}

func parseEnvOutputBudget(key string, def int) int {
	v := parseEnvPositiveInt(key, def)
	if v < MinOutputBudgetBytes {
		klog.Warningf("mcp: clamping %s=%d to hard minimum %d", key, v, MinOutputBudgetBytes)
		return MinOutputBudgetBytes
	}
	if v > MaxOutputBudgetBytes {
		klog.Warningf("mcp: clamping %s=%d to hard maximum %d", key, v, MaxOutputBudgetBytes)
		return MaxOutputBudgetBytes
	}
	return v
}
