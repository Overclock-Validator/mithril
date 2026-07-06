package dashboardcmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/tui"
	"github.com/spf13/viper"
)

// ── State file reading ──────────────────────────────────────────────────

type nodeState struct {
	LastSlot           uint64 `json:"last_slot"`
	LastEpoch          uint64 `json:"last_epoch"`
	LastBankhash       string `json:"last_bankhash"`
	SnapshotSlot       uint64 `json:"snapshot_slot"`
	Stage              string `json:"stage"`
	LastShutdownReason string `json:"last_shutdown_reason"`
	LastShutdownAt     string `json:"last_shutdown_at"`
	CurrentRunID       string `json:"current_run_id"`
	LastWriterVersion  string `json:"last_writer_version"`
	LastWriterCommit   string `json:"last_writer_commit"`
	Cluster            string `json:"cluster"`
}

func readState(accountsPath string) *nodeState {
	stateFile := filepath.Join(accountsPath, "mithril_state.json")
	f, err := os.Open(stateFile)
	if err != nil {
		return nil
	}
	defer f.Close()

	// Cap at 1MB to prevent OOM from corrupted state files
	data := make([]byte, 1<<20)
	n, err := f.Read(data)
	if err != nil && n == 0 {
		return nil
	}

	var s nodeState
	if err := json.Unmarshal(data[:n], &s); err != nil {
		return nil
	}
	return &s
}

// ── Config reading ──────────────────────────────────────────────────────

type configData struct {
	cluster             string
	rpcEndpoints        []string
	blockSource         string
	consensusMode       string
	alpenglowBindAddr   string
	lbEnabled           bool
	lbGossip            string
	lbGrpcAddr          string
	lbRpcAddr           string
	lbExternalEndpoint  string // block.lightbringer_endpoint for external LB mode
	lbBinaryPath        string
	lbQuiet             bool
	turbineBindAddr     string
	turbineGossip       string
	turbineGossipBind   string
	turbineAdvertisedIP string
	turbineShredVersion string
	accountsPath        string
	snapshotsPath       string
	shredstorePath      string
	logsPath            string
	txpar               string
	blockMaxRPS         string
	blockInflight       string
	rpcPort             string
	logLevel            string
	bootstrapMode       string
}

func readConfig(configFile string) *configData {
	v := viper.New()
	config.ApplyDefaults(v)
	v.SetConfigFile(configFile)
	if err := v.ReadInConfig(); err != nil {
		return nil
	}

	cluster := v.GetString("network.cluster")
	if cluster == "" {
		cluster = "unknown"
	}

	txpar := v.GetString("tuning.txpar")
	if txpar == "" {
		txpar = v.GetString("replay.txpar")
	}
	// Don't fill in a computed default — let the UI show "auto" when empty

	logsPath := v.GetString("storage.logs")
	if logsPath == "" {
		logsPath = v.GetString("log.dir")
	}
	turbineBindAddr := v.GetString("block.turbine_bind_addr")
	if turbineBindAddr == "" {
		turbineBindAddr = v.GetString("turbine.bind_addr")
	}

	return &configData{
		cluster:             cluster,
		rpcEndpoints:        v.GetStringSlice("network.rpc"),
		blockSource:         v.GetString("block.source"),
		consensusMode:       v.GetString("consensus.mode"),
		alpenglowBindAddr:   v.GetString("consensus.alpenglow_observer_bind_addr"),
		lbEnabled:           v.GetBool("lightbringer.enabled"),
		lbGossip:            v.GetString("lightbringer.gossip_entrypoint"),
		lbGrpcAddr:          v.GetString("lightbringer.grpc_addr"),
		lbRpcAddr:           v.GetString("lightbringer.rpc_addr"),
		lbQuiet:             v.GetBool("lightbringer.quiet"),
		lbExternalEndpoint:  v.GetString("block.lightbringer_endpoint"),
		lbBinaryPath:        v.GetString("lightbringer.binary_path"),
		turbineBindAddr:     turbineBindAddr,
		turbineGossip:       v.GetString("turbine.gossip_entrypoint"),
		turbineGossipBind:   v.GetString("turbine.gossip_bind_addr"),
		turbineAdvertisedIP: v.GetString("turbine.advertised_ip"),
		turbineShredVersion: v.GetString("turbine.shred_version"),
		accountsPath:        v.GetString("storage.accounts"),
		snapshotsPath:       v.GetString("storage.snapshots"),
		shredstorePath:      v.GetString("storage.shredstore"),
		logsPath:            logsPath,
		txpar:               txpar,
		blockMaxRPS:         v.GetString("block.max_rps"),
		blockInflight:       v.GetString("block.max_inflight"),
		rpcPort:             v.GetString("rpc.port"),
		logLevel:            v.GetString("log.level"),
		bootstrapMode:       v.GetString("bootstrap.mode"),
	}
}

// ── Service probing ─────────────────────────────────────────────────────

type serviceStatus struct {
	name string
	addr string
	up   bool
}

// Default service addresses (must match config template defaults).
const (
	defaultRPCPort = "8899"
	defaultLBGRPC  = "127.0.0.1:3001"
	defaultLBHTTP  = "127.0.0.1:3000"
)

func probeServices(cfg *configData) []serviceStatus {
	rpcPort := defaultRPCPort
	grpcAddr := defaultLBGRPC
	httpAddr := defaultLBHTTP

	if cfg != nil {
		if cfg.rpcPort != "" && cfg.rpcPort != "0" {
			rpcPort = cfg.rpcPort
		}
		if cfg.lbGrpcAddr != "" {
			grpcAddr = cfg.lbGrpcAddr
		}
		if cfg.lbRpcAddr != "" {
			httpAddr = cfg.lbRpcAddr
		}
	}

	var services []serviceStatus
	// Skip RPC probe when port=0 (disabled)
	if cfg == nil || cfg.rpcPort != "0" {
		services = append(services, serviceStatus{name: "Mithril RPC", addr: "127.0.0.1:" + rpcPort})
	}
	// Probe lightbringer services based on the same mode matrix as runtime:
	// - blockSource=lightbringer + enabled: managed sidecar → probe grpc + http
	// - blockSource=lightbringer + endpoint: external → probe endpoint only
	// - blockSource=rpc: no LB probes regardless of stale endpoint
	if cfg != nil && cfg.blockSource == "lightbringer" && cfg.lbExternalEndpoint != "" && !cfg.lbEnabled {
		services = append(services, serviceStatus{name: "LB External", addr: cfg.lbExternalEndpoint})
	} else if cfg != nil && cfg.lbEnabled {
		services = append(services,
			serviceStatus{name: "LB gRPC", addr: grpcAddr},
			serviceStatus{name: "LB HTTP", addr: httpAddr},
		)
	}

	// Probe all services concurrently to avoid 6s blocking when endpoints are down
	var wg sync.WaitGroup
	for i := range services {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", services[idx].addr, 2*time.Second)
			if err == nil {
				conn.Close()
				services[idx].up = true
			}
		}(i)
	}
	wg.Wait()

	return services
}

// ── Disk usage ──────────────────────────────────────────────────────────

type diskUsage struct {
	label string
	path  string
	used  uint64 // GB
	total uint64 // GB
	pct   int
}

func getDiskUsage(cfg *configData) []diskUsage {
	if cfg == nil {
		return nil
	}

	paths := []struct {
		label string
		path  string
	}{
		{"accounts", cfg.accountsPath},
		{"snapshots", cfg.snapshotsPath},
		{"shredstore", cfg.shredstorePath},
		{"logs", cfg.logsPath},
	}

	// Run df calls in parallel — each takes up to 2s on slow filesystems
	type duResult struct {
		idx int
		du  *diskUsage
	}
	ch := make(chan duResult, len(paths))
	count := 0
	for i, p := range paths {
		if p.path == "" {
			continue
		}
		count++
		go func(idx int, label, path string) {
			defer func() {
				if recover() != nil {
					ch <- duResult{idx: idx, du: nil}
				}
			}()
			ch <- duResult{idx: idx, du: getDiskUsageForPath(label, path)}
		}(i, p.label, p.path)
	}

	collected := make([]*diskUsage, len(paths))
	for range count {
		r := <-ch
		collected[r.idx] = r.du
	}

	var results []diskUsage
	for _, du := range collected {
		if du != nil {
			results = append(results, *du)
		}
	}
	return results
}

func getDiskUsageForPath(label, path string) *diskUsage {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		return nil
	}

	out, err := exec.Command("df", "-BG", "--", path).Output()
	if err != nil {
		// Try without -BG for macOS
		out, err = exec.Command("df", "-g", "--", path).Output()
		if err != nil {
			return nil
		}
	}

	lines := strings.Split(string(out), "\n")
	if len(lines) < 2 {
		return nil
	}

	fields := strings.Fields(lines[1])
	if len(fields) < 5 {
		return nil
	}

	total := parseGB(fields[1])
	used := parseGB(fields[2])
	pct := 0
	if total > 0 {
		pct = int(float64(used) / float64(total) * 100)
	}

	return &diskUsage{
		label: label,
		path:  path,
		used:  used,
		total: total,
		pct:   pct,
	}
}

func parseGB(s string) uint64 {
	s = strings.TrimSuffix(s, "G")
	s = strings.TrimSpace(s)
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

// ── Doctor checks (structured) ──────────────────────────────────────────

type checkResult struct {
	name   string
	status string // "pass", "warn", "fail"
	msg    string
}

func runDoctorChecks(configFile string, cfg *configData) []checkResult {
	var results []checkResult

	// Config file
	if _, err := os.Stat(configFile); err != nil {
		results = append(results, checkResult{"Config file", "fail", "not found: " + configFile})
		return results
	}
	results = append(results, checkResult{"Config file", "pass", "found (" + configFile + ")"})

	if cfg == nil {
		results = append(results, checkResult{"Config parse", "fail", "could not parse config"})
		return results
	}

	// Cluster
	if cfg.cluster != "" && cfg.cluster != "unknown" {
		results = append(results, checkResult{"Network", "pass", cfg.cluster})
	} else {
		results = append(results, checkResult{"Network", "fail", "cluster not set"})
	}

	// RPC
	if len(cfg.rpcEndpoints) > 0 {
		results = append(results, checkResult{"RPC endpoint", "pass", cfg.rpcEndpoints[0]})
	} else {
		results = append(results, checkResult{"RPC endpoint", "fail", "no RPC endpoints configured"})
	}

	switch cfg.consensusMode {
	case "", "verifying":
		results = append(results, checkResult{"Node mode", "pass", "verifying (non-voting)"})
	case "validator":
		results = append(results, checkResult{"Node mode", "pass", "validator (voting engine not yet active)"})
	default:
		results = append(results, checkResult{"Node mode", "fail", "invalid consensus.mode: " + cfg.consensusMode})
	}
	if cfg.alpenglowBindAddr != "" {
		if _, _, err := net.SplitHostPort(cfg.alpenglowBindAddr); err != nil {
			results = append(results, checkResult{"Alpenglow Votor", "fail", "invalid bind address: " + cfg.alpenglowBindAddr})
		} else {
			results = append(results, checkResult{"Alpenglow Votor", "pass", cfg.alpenglowBindAddr})
		}
	}

	// AccountsDB path
	if cfg.accountsPath != "" {
		if _, err := os.Stat(cfg.accountsPath); err == nil {
			results = append(results, checkResult{"AccountsDB path", "pass", cfg.accountsPath})
		} else {
			results = append(results, checkResult{"AccountsDB path", "warn", cfg.accountsPath + " (will be created)"})
		}
	} else {
		results = append(results, checkResult{"AccountsDB path", "fail", "not set"})
	}

	// Lightbringer
	if cfg.lbEnabled {
		// Check binary — use config value or default
		binaryPath := cfg.lbBinaryPath
		if binaryPath == "" {
			binaryPath = "./lightbringer"
		}
		if _, err := os.Stat(binaryPath); err == nil {
			results = append(results, checkResult{"Lightbringer binary", "pass", binaryPath})
		} else {
			results = append(results, checkResult{"Lightbringer binary", "fail", "not found at " + binaryPath})
		}

		// Check gossip format
		if cfg.lbGossip != "" {
			if _, _, err := net.SplitHostPort(cfg.lbGossip); err != nil {
				results = append(results, checkResult{"Gossip entrypoint", "fail", "invalid format: " + cfg.lbGossip})
			} else {
				results = append(results, checkResult{"Gossip entrypoint", "pass", cfg.lbGossip})
			}
		} else {
			results = append(results, checkResult{"Gossip entrypoint", "fail", "not set"})
		}

		// Quiet mode (informational)
		if cfg.lbQuiet {
			results = append(results, checkResult{"Lightbringer logs", "pass", "quiet (warn/error only)"})
		} else {
			results = append(results, checkResult{"Lightbringer logs", "pass", "normal (info)"})
		}
	} else if cfg.blockSource == "lightbringer" && cfg.lbExternalEndpoint != "" {
		// External Lightbringer mode — sidecar disabled but endpoint configured
		results = append(results, checkResult{"Lightbringer", "pass", "external at " + cfg.lbExternalEndpoint})
	} else if cfg.blockSource == "lightbringer" && cfg.lbExternalEndpoint == "" {
		// Invalid: source=lightbringer but no sidecar and no endpoint
		results = append(results, checkResult{"Lightbringer", "fail", "block.source=lightbringer requires enabled sidecar or endpoint"})
	} else if cfg.blockSource == "turbine" {
		if cfg.turbineBindAddr == "" {
			results = append(results, checkResult{"Turbine UDP", "fail", "block.source=turbine requires block.turbine_bind_addr or turbine.bind_addr"})
		} else if _, _, err := net.SplitHostPort(cfg.turbineBindAddr); err != nil {
			results = append(results, checkResult{"Turbine UDP", "fail", "invalid format: " + cfg.turbineBindAddr})
		} else {
			results = append(results, checkResult{"Turbine UDP", "pass", cfg.turbineBindAddr})
		}
		if cfg.turbineGossip == "" {
			results = append(results, checkResult{"Turbine gossip", "warn", "empty; UDP-only receiver mode"})
		} else if _, _, err := net.SplitHostPort(cfg.turbineGossip); err != nil {
			results = append(results, checkResult{"Turbine gossip", "fail", "invalid format: " + cfg.turbineGossip})
		} else {
			results = append(results, checkResult{"Turbine gossip", "pass", cfg.turbineGossip})
		}
		if cfg.turbineGossipBind != "" {
			if _, _, err := net.SplitHostPort(cfg.turbineGossipBind); err != nil {
				results = append(results, checkResult{"Turbine gossip UDP", "fail", "invalid format: " + cfg.turbineGossipBind})
			} else {
				results = append(results, checkResult{"Turbine gossip UDP", "pass", cfg.turbineGossipBind})
			}
		}
	} else {
		results = append(results, checkResult{"Lightbringer", "pass", "disabled"})
	}

	// Logs
	if cfg.logsPath != "" {
		results = append(results, checkResult{"Log directory", "pass", cfg.logsPath})
	} else {
		results = append(results, checkResult{"Log directory", "warn", "not configured (logs to stderr)"})
	}

	return results
}

// ── Log tailing ─────────────────────────────────────────────────────────

func readLogTail(logsPath string, filename string, maxLines int) []string {
	if logsPath == "" {
		return []string{"(no log directory configured)"}
	}

	// Validate filename is a simple base name (no path separators)
	if filename != filepath.Base(filename) || filename == "" {
		return []string{"(invalid log filename)"}
	}

	latestDir := filepath.Join(logsPath, "latest")
	target, err := os.Readlink(latestDir)
	if err != nil {
		return []string{"(no latest run found in " + logsPath + ")"}
	}

	// Validate symlink target — reject traversal and absolute paths
	if strings.Contains(target, "..") || filepath.IsAbs(target) {
		return []string{"(invalid latest symlink)"}
	}

	logFile := filepath.Clean(filepath.Join(logsPath, target, filename))

	// Verify resolved path is still under logsPath
	cleanLogs := filepath.Clean(logsPath) + string(os.PathSeparator)
	if !strings.HasPrefix(logFile, cleanLogs) {
		return []string{"(log path escapes directory)"}
	}

	// Read only the tail of the file to avoid loading huge logs into memory
	f, err := os.Open(logFile)
	if err != nil {
		return []string{"(could not read " + logFile + ")"}
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return []string{"(could not stat " + logFile + ")"}
	}

	// Read last 64KB — enough for ~500 log lines
	const tailSize = 64 * 1024
	offset := stat.Size() - tailSize
	if offset < 0 {
		offset = 0
	}

	buf := make([]byte, stat.Size()-offset)
	_, err = f.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return []string{"(read error: " + err.Error() + ")"}
	}

	lines := strings.Split(string(buf), "\n")
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	return lines
}

// ── Config saving ───────────────────────────────────────────────────────

// saveConfigValue writes a single config field to the TOML file.
func saveConfigValue(configFile, section, key, value string) error {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}

	content := string(data)

	// Determine TOML value format based on field type
	fullKey := section + "." + key
	var tomlValue string
	switch {
	case fullKey == "block.max_rps" || fullKey == "block.max_inflight" ||
		fullKey == "tuning.txpar" || fullKey == "rpc.port" ||
		fullKey == "turbine.shred_version":
		tomlValue = value // numeric — no quoting
	case fullKey == "lightbringer.enabled" || fullKey == "lightbringer.quiet":
		tomlValue = value // boolean — no quoting
	case fullKey == "network.rpc":
		// Preserve failover endpoints — read existing array, update first element
		v := viper.New()
		config.ApplyDefaults(v)
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err == nil {
			existing := v.GetStringSlice("network.rpc")
			if len(existing) > 1 {
				existing[0] = value
				var parts []string
				for _, ep := range existing {
					parts = append(parts, fmt.Sprintf("%q", ep))
				}
				tomlValue = "[" + strings.Join(parts, ", ") + "]"
				break
			}
		}
		tomlValue = fmt.Sprintf("[%q]", value)
	default:
		tomlValue = fmt.Sprintf("%q", value) // quoted string
	}

	content = setTomlValueInline(content, section, key, tomlValue)
	if err := tui.AtomicWriteFile(configFile, []byte(content), 0600); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

// setTomlValueInline replaces a value in a TOML file, preserving structure.
// removeConfigKey removes a key from a TOML file (comments it out).
func removeConfigKey(configFile, section, key string) error {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return err
	}

	lines := strings.Split(string(data), "\n")
	inSection := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && !strings.HasPrefix(trimmed, "[[") {
			sectionName := strings.Trim(trimmed, "[] ")
			inSection = sectionName == section
			continue
		}
		if inSection && (strings.HasPrefix(trimmed, key+" ") || strings.HasPrefix(trimmed, key+"=")) {
			lines[i] = "# " + line // comment out instead of deleting
			content := strings.Join(lines, "\n")
			return tui.AtomicWriteFile(configFile, []byte(content), 0600)
		}
	}
	return nil // key not found, nothing to remove
}

func setTomlValueInline(content, section, key, value string) string {
	lines := strings.Split(content, "\n")
	inSection := false
	sectionFound := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && !strings.HasPrefix(trimmed, "[[") {
			if inSection {
				// Section found but key missing — insert before next section header
				result := make([]string, 0, len(lines)+1)
				result = append(result, lines[:i]...)
				result = append(result, key+" = "+value)
				result = append(result, lines[i:]...)
				return strings.Join(result, "\n")
			}
			sectionName := strings.Trim(trimmed, "[] ")
			inSection = sectionName == section
			if inSection {
				sectionFound = true
			}
			continue
		}
		if inSection && (strings.HasPrefix(trimmed, key+" ") || strings.HasPrefix(trimmed, key+"=")) {
			indent := ""
			for _, c := range line {
				if c == ' ' || c == '\t' {
					indent += string(c)
				} else {
					break
				}
			}
			lines[i] = fmt.Sprintf("%s%s = %s", indent, key, value)
			return strings.Join(lines, "\n")
		}
	}
	// If section was the last one (no next header), append key
	if inSection {
		lines = append(lines, key+" = "+value)
		return strings.Join(lines, "\n")
	}
	// Section not found at all — append new section with key
	if !sectionFound {
		lines = append(lines, "", "["+section+"]", key+" = "+value)
		return strings.Join(lines, "\n")
	}
	return content
}
