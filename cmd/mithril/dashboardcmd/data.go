package dashboardcmd

import (
	"context"
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
	"github.com/Overclock-Validator/mithril/pkg/procctl"
	"github.com/Overclock-Validator/mithril/pkg/progress"
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

const maxDashboardStateFileBytes int64 = 64 << 20

// slotsPerEpoch is the standard Solana epoch length, for deriving epoch from slot.
const slotsPerEpoch = 432000

// latestReplaySlot returns the slot from the newest per-slot replay log line,
// e.g. "(+1m2s) slot 426087845 | leader: ...". The trailing "|" requirement
// skips bootstrap lines like "snapshot slot N".
func latestReplaySlot(lines []string) (uint64, bool) {
	for i := len(lines) - 1; i >= 0; i-- {
		idx := strings.Index(lines[i], "slot ")
		if idx < 0 {
			continue
		}
		rest := lines[i][idx+len("slot "):]
		end := 0
		for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
			end++
		}
		if end == 0 {
			continue
		}
		if after := strings.TrimLeft(rest[end:], " "); !strings.HasPrefix(after, "|") {
			continue // not a per-slot replay line (e.g. "snapshot slot N")
		}
		if n, err := strconv.ParseUint(rest[:end], 10, 64); err == nil {
			return n, true
		}
	}
	return 0, false
}

// isNodeRunning reports whether the managed mithril process is currently alive.
func (m model) isNodeRunning() bool {
	return m.proc.detection != nil && m.proc.detection.Status == procctl.StatusRunning
}

// liveNodeSlot returns the live replay slot/epoch from the log tail while the
// node runs. ok=false (caller falls back to state) when stopped or no slot yet.
func (m model) liveNodeSlot() (slot, epoch uint64, ok bool) {
	if m.proc.detection == nil || m.proc.detection.Status != procctl.StatusRunning {
		return 0, 0, false
	}
	s, found := latestReplaySlot(m.mithrilLines)
	if !found {
		return 0, 0, false
	}
	return s, s / slotsPerEpoch, true
}

func readState(accountsPath string) *nodeState {
	stateFile := filepath.Join(accountsPath, "mithril_state.json")
	f, err := os.Open(stateFile)
	if err != nil {
		return nil
	}
	defer f.Close()

	if info, err := f.Stat(); err == nil && info.Size() > maxDashboardStateFileBytes {
		return nil
	}

	var s nodeState
	decoder := json.NewDecoder(io.LimitReader(f, maxDashboardStateFileBytes))
	if err := decoder.Decode(&s); err != nil {
		return nil
	}
	return &s
}

// ── Config reading ──────────────────────────────────────────────────────

type configData struct {
	cluster             string
	rpcEndpoints        []string
	blockSource         string
	lbEnabled           bool
	lbGossip            string
	lbGrpcAddr          string
	lbRpcAddr           string
	lbGossipPort        string
	lbPortRangeStart    string
	lbPortRangeEnd      string
	lbExternalEndpoint  string // block.lightbringer_endpoint for external LB mode
	lbBinaryPath        string
	lbConfigDir         string
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
	// Don't fill in a computed default; empty means sequential runtime mode.

	logsPath := ""
	if v.IsSet("storage.logs") {
		logsPath = v.GetString("storage.logs")
	} else if v.IsSet("log.dir") {
		logsPath = v.GetString("log.dir")
	} else {
		logsPath = "/mnt/mithril-logs"
	}
	turbineBindAddr := v.GetString("block.turbine_bind_addr")
	if turbineBindAddr == "" {
		turbineBindAddr = v.GetString("turbine.bind_addr")
	}

	lbBinaryPath := v.GetString("lightbringer.binary_path")
	if lbBinaryPath == "" {
		lbBinaryPath = "./lightbringer"
	}
	lbConfigDir := v.GetString("lightbringer.config_dir")
	if lbConfigDir == "" {
		lbConfigDir = "."
	}
	rpcEndpoints := v.GetStringSlice("network.rpc")
	if len(rpcEndpoints) == 0 {
		rpcEndpoints = v.GetStringSlice("rpc.rpc")
	}
	accountsPath := firstNonEmptyConfigString(v, "storage.accounts", "ledger.accounts_path")
	snapshotsPath := firstNonEmptyConfigString(v, "snapshot.download_path", "storage.snapshots")
	shredstorePath := firstNonEmptyConfigString(v, "storage.shredstore", "storage.blockstore", "ledger.path", "lightbringer.storage")

	return &configData{
		cluster:             cluster,
		rpcEndpoints:        rpcEndpoints,
		blockSource:         v.GetString("block.source"),
		lbEnabled:           v.GetBool("lightbringer.enabled"),
		lbGossip:            v.GetString("lightbringer.gossip_entrypoint"),
		lbGrpcAddr:          v.GetString("lightbringer.grpc_addr"),
		lbRpcAddr:           v.GetString("lightbringer.rpc_addr"),
		lbGossipPort:        v.GetString("lightbringer.gossip_port"),
		lbPortRangeStart:    v.GetString("lightbringer.port_range_start"),
		lbPortRangeEnd:      v.GetString("lightbringer.port_range_end"),
		lbQuiet:             v.GetBool("lightbringer.quiet"),
		lbExternalEndpoint:  v.GetString("block.lightbringer_endpoint"),
		lbBinaryPath:        lbBinaryPath,
		lbConfigDir:         lbConfigDir,
		turbineBindAddr:     turbineBindAddr,
		turbineGossip:       v.GetString("turbine.gossip_entrypoint"),
		turbineGossipBind:   v.GetString("turbine.gossip_bind_addr"),
		turbineAdvertisedIP: v.GetString("turbine.advertised_ip"),
		turbineShredVersion: v.GetString("turbine.shred_version"),
		accountsPath:        accountsPath,
		snapshotsPath:       snapshotsPath,
		shredstorePath:      shredstorePath,
		logsPath:            logsPath,
		txpar:               txpar,
		blockMaxRPS:         v.GetString("block.max_rps"),
		blockInflight:       v.GetString("block.max_inflight"),
		rpcPort:             v.GetString("rpc.port"),
		logLevel:            v.GetString("log.level"),
		bootstrapMode:       v.GetString("bootstrap.mode"),
	}
}

func firstNonEmptyConfigString(v *viper.Viper, keys ...string) string {
	for _, key := range keys {
		if key == "" {
			continue
		}
		if value := v.GetString(key); value != "" {
			return value
		}
	}
	return ""
}

func usesLightbringerBlocks(cfg *configData) bool {
	if cfg == nil {
		return false
	}
	return cfg.lbEnabled || (cfg.blockSource == "lightbringer" && cfg.lbExternalEndpoint != "")
}

func isMainnetPublicRPC(cfg *configData, endpoint string) bool {
	if cfg == nil || cfg.cluster != "mainnet-beta" {
		return false
	}
	return strings.Contains(endpoint, "api.mainnet-beta.solana.com") ||
		strings.Contains(endpoint, "api.mainnet.solana.com")
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
	// Mirror runtime's LB mode matrix: external endpoint, managed sidecar, or none.
	if cfg != nil && cfg.blockSource == "lightbringer" && cfg.lbExternalEndpoint != "" {
		services = append(services, serviceStatus{name: "LB External", addr: cfg.lbExternalEndpoint})
		if cfg.lbEnabled {
			services = append(services, serviceStatus{name: "LB HTTP", addr: httpAddr})
		}
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

	// Bound the gather so a wedged mount cannot freeze the refresh; stragglers drain into the buffered channel.
	collected := make([]*diskUsage, len(paths))
	timeout := time.After(5 * time.Second)
gather:
	for range count {
		select {
		case r := <-ch:
			collected[r.idx] = r.du
		case <-timeout:
			break gather
		}
	}

	var results []diskUsage
	for _, du := range collected {
		if du != nil {
			results = append(results, *du)
		}
	}
	return results
}

// runDF runs `df <sizeFlag> -- path` with a 2s deadline so a hung mount can't
// block the dashboard. Each call gets its own context so the macOS -g fallback
// isn't starved by a slow -BG attempt.
func runDF(sizeFlag, path string) ([]byte, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "df", sizeFlag, "--", path).Output()
	return out, err == nil
}

func getDiskUsageForPath(label, path string) *diskUsage {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		return nil
	}
	probePath := existingDiskProbePath(path)
	if probePath == "" {
		return nil
	}

	out, ok := runDF("-BG", probePath)
	if !ok {
		// -BG is GNU-only; macOS df uses -g.
		out, ok = runDF("-g", probePath)
		if !ok {
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

func existingDiskProbePath(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	probePath := filepath.Clean(path)
	for {
		if _, err := os.Stat(probePath); err == nil {
			return probePath
		} else if !os.IsNotExist(err) {
			return ""
		}
		parent := filepath.Dir(probePath)
		if parent == probePath {
			return ""
		}
		probePath = parent
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
		results = append(results, checkResult{"RPC endpoint", "pass", config.RedactEndpointForDisplay(cfg.rpcEndpoints[0])})
		if isMainnetPublicRPC(cfg, cfg.rpcEndpoints[0]) {
			results = append(results, checkResult{
				"RPC capacity",
				"warn",
				"public mainnet RPC is shared/rate-limited; use private RPC for long catchup",
			})
		}
	} else {
		results = append(results, checkResult{"RPC endpoint", "fail", "no RPC endpoints configured"})
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

		if msg, err := describeLightbringerGossipPorts(cfg.lbGossipPort, cfg.lbPortRangeStart, cfg.lbPortRangeEnd); err != nil {
			results = append(results, checkResult{"Lightbringer UDP ports", "fail", err.Error()})
		} else {
			results = append(results, checkResult{"Lightbringer UDP ports", "warn", msg})
		}

		// Quiet mode (informational)
		if cfg.lbQuiet {
			results = append(results, checkResult{"Lightbringer logs", "pass", "quiet (warn/error only)"})
		} else {
			results = append(results, checkResult{"Lightbringer logs", "pass", "normal (info)"})
		}
	} else if cfg.blockSource == "lightbringer" && cfg.lbExternalEndpoint != "" {
		// External Lightbringer mode — sidecar disabled but endpoint configured
		results = append(results, checkResult{"Lightbringer", "pass", "external at " + config.RedactEndpointForDisplay(cfg.lbExternalEndpoint)})
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

func describeLightbringerGossipPorts(gossipPortRaw, rangeStartRaw, rangeEndRaw string) (string, error) {
	gossipPort, rangeStart, rangeEnd, err := parseLightbringerGossipPorts(gossipPortRaw, rangeStartRaw, rangeEndRaw)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("opens public Solana UDP gossip/repair sockets: gossip=%d range=%d-%d", gossipPort, rangeStart, rangeEnd), nil
}

func parseLightbringerGossipPorts(gossipPortRaw, rangeStartRaw, rangeEndRaw string) (int, int, int, error) {
	gossipPort, err := parseLightbringerPort(gossipPortRaw, "gossip_port", 65400)
	if err != nil {
		return 0, 0, 0, err
	}
	rangeStart, err := parseLightbringerPort(rangeStartRaw, "port_range_start", 65401)
	if err != nil {
		return 0, 0, 0, err
	}
	rangeEnd, err := parseLightbringerPort(rangeEndRaw, "port_range_end", 65500)
	if err != nil {
		return 0, 0, 0, err
	}
	if rangeStart > rangeEnd {
		return 0, 0, 0, fmt.Errorf("port_range_start must be <= port_range_end")
	}
	if rangeEnd-rangeStart < 25 {
		return 0, 0, 0, fmt.Errorf("port range must be at least 25 ports wide")
	}
	if rangeEnd+6 > 65535 {
		return 0, 0, 0, fmt.Errorf("port_range_end must be <= 65529")
	}
	if gossipPort >= rangeStart && gossipPort <= rangeEnd {
		return 0, 0, 0, fmt.Errorf("gossip_port must not overlap port_range_start..port_range_end")
	}
	return gossipPort, rangeStart, rangeEnd, nil
}

func parseLightbringerPort(raw, field string, fallback int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < 1 || value > 65535 {
		return 0, fmt.Errorf("%s must be 1-65535", field)
	}
	return value, nil
}

// ── Log tailing ─────────────────────────────────────────────────────────

type dashboardSpawnLogs struct {
	stdoutPath string
	stderrPath string
}

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

	return readPathLogTail(logFile, maxLines)
}

func readPathLogTail(logFile string, maxLines int) []string {
	if maxLines <= 0 {
		return nil
	}

	f, err := os.Open(logFile)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{"(" + filepath.Base(logFile) + " is not available yet)"}
		}
		return []string{"(could not read " + logFile + ": " + err.Error() + ")"}
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

	lines := strings.Split(normalizeTerminalLogText(string(buf)), "\n")
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	return lines
}

func mithrilLogLines(logsPath string, maxLines int, spawnLogs dashboardSpawnLogs) []string {
	lines := compactVolatileLogLines(cleanDashboardLogLines(trimTrailingEmptyLogLines(readMithrilLogTail(logsPath, maxLines))), maxLines)
	spawnLines, spawnActive := dashboardSpawnOutputLines(maxLines, spawnLogs)
	if !logTailUnavailable(lines) {
		if spawnActive && len(spawnLines) > 0 {
			return mergeMithrilLogLines(lines, spawnLines, maxLines)
		}
		return lines
	}
	if len(spawnLines) > 0 {
		return spawnLines
	}
	if spawnActive {
		return []string{"(Mithril is starting; logs are not available yet)"}
	}
	return lines
}

func readMithrilLogTail(logsPath string, maxLines int) []string {
	if info, err := procctl.ReadPidFile(procctl.DefaultPidFile()); err == nil && info != nil && info.LogDir != "" {
		if ok, _ := procctl.Matches(info.Pid, info); ok {
			if logFile := safeConfiguredLogFile(logsPath, info.LogDir, "mithril.log"); logFile != "" {
				lines := readPathLogTail(logFile, maxLines)
				if !logTailUnavailable(lines) {
					return lines
				}
			}
		}
	}
	return readLogTail(logsPath, "mithril.log", maxLines)
}

func activeDashboardPidInfo() *procctl.PidInfo {
	info, err := procctl.ReadPidFile(procctl.DefaultPidFile())
	if err != nil || info == nil || info.SpawnedBy != "dashboard" {
		return nil
	}
	if ok, _ := procctl.Matches(info.Pid, info); !ok {
		return nil
	}
	return info
}

func safeConfiguredLogFile(logsPath, runDir, filename string) string {
	if logsPath == "" || runDir == "" || filename == "" || filename != filepath.Base(filename) {
		return ""
	}
	cleanLogs := filepath.Clean(logsPath)
	cleanRunDir := filepath.Clean(runDir)
	if !filepath.IsAbs(cleanRunDir) {
		cleanRunDir = filepath.Join(cleanLogs, cleanRunDir)
	}
	cleanLogsWithSep := cleanLogs + string(os.PathSeparator)
	if cleanRunDir != cleanLogs && !strings.HasPrefix(cleanRunDir+string(os.PathSeparator), cleanLogsWithSep) {
		return ""
	}
	return filepath.Join(cleanRunDir, filename)
}

func dashboardSpawnOutputLines(maxLines int, fallback dashboardSpawnLogs) ([]string, bool) {
	spawnLogs := fallback
	active := false
	if info := activeDashboardPidInfo(); info != nil {
		spawnLogs.stdoutPath = info.StdoutPath
		spawnLogs.stderrPath = info.StderrPath
		active = true
	}
	if spawnLogs.stdoutPath == "" && spawnLogs.stderrPath == "" {
		return nil, active
	}

	var lines []string
	if safeDashboardSpawnLogPath(spawnLogs.stderrPath) {
		lines = append(lines, trimTrailingEmptyLogLines(readDashboardSpawnLogTail(spawnLogs.stderrPath, maxLines))...)
	}
	return dashboardSpawnLines(lines, maxLines), active
}

func dashboardSpawnTextLines(text string, maxLines int) []string {
	lines := strings.Split(normalizeTerminalLogText(text), "\n")
	return dashboardSpawnLines(lines, maxLines)
}

func dashboardSpawnLines(lines []string, maxLines int) []string {
	lines = cleanDashboardLogLines(filterUnavailableLogLines(lines))
	lines = compactVolatileLogLines(lines, maxLines-1)
	if len(lines) == 0 {
		return nil
	}
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	return lines
}

func cleanDashboardLogLines(lines []string) []string {
	if len(lines) == 0 {
		return nil
	}
	cleaned := make([]string, 0, len(lines))
	for _, line := range lines {
		line = compactVerboseDashboardLogLine(line)
		if isDashboardLogNoiseLine(line) {
			continue
		}
		cleaned = append(cleaned, line)
	}
	return cleaned
}

func compactVerboseDashboardLogLine(line string) string {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return line
	}
	if idx := strings.Index(line, "Will save full snapshot to "); idx >= 0 {
		return dashboardLogPrefix(line[:idx]) + "Saving full snapshot while streaming"
	}
	if idx := strings.Index(line, "Cleaning up previous AccountsDB artifacts in "); idx >= 0 {
		return dashboardLogPrefix(line[:idx]) + "Cleaning up previous AccountsDB artifacts"
	}
	if idx := strings.Index(line, "Cleaning up existing snapshot files in "); idx >= 0 {
		return dashboardLogPrefix(line[:idx]) + "Cleaning up existing snapshot files"
	}
	if idx := strings.Index(line, "Cleaning up partial download:"); idx >= 0 {
		return dashboardLogPrefix(line[:idx]) + "Cleaning up partial snapshot download"
	}
	if idx := strings.Index(line, "Snapshot unpack stopped during shutdown:"); idx >= 0 {
		return dashboardLogPrefix(line[:idx]) + "Snapshot unpack canceled during shutdown"
	}
	if idx := strings.Index(strings.ToLower(line), "snapshot bootstrap cancelled during shutdown:"); idx >= 0 {
		return dashboardLogPrefix(line[:idx]) + "Snapshot bootstrap canceled during shutdown"
	}
	return line
}

func dashboardLogPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return ""
	}
	return prefix + " "
}

func isDashboardLogNoiseLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return true
	}
	switch trimmed {
	case "x", "lay":
		return true
	}
	if strings.HasSuffix(trimmed, ")") {
		if _, err := strconv.Atoi(strings.TrimSuffix(trimmed, ")")); err == nil {
			return true
		}
	}
	if strings.Contains(trimmed, "Search Time:") {
		return true
	}
	if strings.Contains(trimmed, "→") &&
		(strings.Contains(trimmed, "Snapshot") ||
			strings.Contains(trimmed, "Incremental") ||
			strings.Contains(trimmed, "Fetch Blocks") ||
			strings.Contains(trimmed, "Replay")) {
		return true
	}
	return isBoxDrawingOnlyLine(trimmed)
}

func isBoxDrawingOnlyLine(line string) bool {
	for _, r := range line {
		switch r {
		case ' ', '\t', '─', '━', '│', '┃', '┌', '┐', '└', '┘', '├', '┤', '┬', '┴', '┼', '╭', '╮', '╰', '╯':
			continue
		default:
			return false
		}
	}
	return true
}

func mergeMithrilLogLines(primary, live []string, maxLines int) []string {
	if len(live) == 0 {
		return primary
	}
	merged := append([]string{}, primary...)
	seen := make(map[string]struct{}, len(primary))
	for _, line := range primary {
		seen[strings.TrimSpace(line)] = struct{}{}
	}
	for _, line := range live {
		key := strings.TrimSpace(line)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, line)
	}
	if maxLines > 0 && len(merged) > maxLines {
		merged = merged[len(merged)-maxLines:]
	}
	return merged
}

func safeDashboardSpawnLogPath(path string) bool {
	if path == "" {
		return false
	}
	clean := filepath.Clean(path)
	base := filepath.Base(clean)
	if !strings.HasPrefix(base, "mithril-dashboard-spawn-") || !strings.HasSuffix(base, ".log") {
		return false
	}
	dir, err := filepath.EvalSymlinks(filepath.Dir(clean))
	if err != nil {
		return false
	}
	tempDir, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		return false
	}
	return dir == tempDir
}

func readDashboardSpawnLogTail(path string, maxLines int) []string {
	if !safeDashboardSpawnLogPath(path) {
		return nil
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil
	}
	return readPathLogTail(path, maxLines)
}

func logTailUnavailable(lines []string) bool {
	if len(lines) == 0 {
		return true
	}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		return strings.HasPrefix(trimmed, "(no latest run found") ||
			strings.HasPrefix(trimmed, "(could not read ") ||
			strings.Contains(trimmed, " is not available yet)") ||
			strings.HasPrefix(trimmed, "(no log directory configured)")
	}
	return true
}

func filterUnavailableLogLines(lines []string) []string {
	filtered := lines[:0]
	for _, line := range lines {
		if logTailUnavailable([]string{line}) {
			continue
		}
		filtered = append(filtered, line)
	}
	return filtered
}

func normalizeTerminalLogText(text string) string {
	if text == "" {
		return ""
	}
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = sanitizeTerminalLogLine(line)
	}
	return strings.Join(lines, "\n")
}

func sanitizeTerminalLogLine(line string) string {
	if line == "" {
		return ""
	}
	var b strings.Builder
	inEscape := false
	inCSI := false
	inOSC := false
	for _, r := range line {
		if inOSC {
			if r == '\a' {
				inOSC = false
			}
			continue
		}
		if inEscape {
			if inCSI {
				if r >= 0x40 && r <= 0x7e {
					inEscape = false
					inCSI = false
				}
				continue
			}
			switch r {
			case '[':
				inCSI = true
			case ']':
				inEscape = false
				inOSC = true
			default:
				inEscape = false
			}
			continue
		}
		if r == 0x1b {
			inEscape = true
			continue
		}
		if r == '\t' || (r >= 0x20 && r != 0x7f) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func compactVolatileLogLines(lines []string, maxLines int) []string {
	if len(lines) == 0 {
		return nil
	}
	var compacted []string
	pending := make(map[string]string)
	var order []string
	flushProgress := func() {
		if len(order) == 0 {
			return
		}
		for _, key := range order {
			compacted = append(compacted, pending[key])
		}
		pending = make(map[string]string)
		order = order[:0]
	}

	for _, line := range lines {
		if key, ok := volatileProgressLogKey(line); ok {
			if _, exists := pending[key]; !exists {
				order = append(order, key)
			}
			pending[key] = compactVolatileProgressLogLine(key, line)
			continue
		}
		flushProgress()
		compacted = append(compacted, line)
	}
	flushProgress()

	if maxLines > 0 && len(compacted) > maxLines {
		compacted = compacted[len(compacted)-maxLines:]
	}
	return compacted
}

func volatileProgressLogKey(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	switch {
	case strings.Contains(trimmed, "Snapshot Read"):
		return "snapshot-read", true
	case strings.Contains(trimmed, "Extract (AppendVecs)"):
		return "extract-appendvecs", true
	case strings.Contains(trimmed, "AppendVec") && strings.Contains(trimmed, "ETA"):
		return "extract-appendvecs", true
	case strings.Contains(trimmed, "Flush (shard logs)"):
		return "flush-shard-logs", true
	default:
		return "", false
	}
}

func compactVolatileProgressLogLine(key, line string) string {
	trimmed := strings.Join(strings.Fields(strings.TrimSpace(line)), " ")
	label := strings.TrimSpace(line)
	switch key {
	case "snapshot-read":
		label = "Snapshot"
	case "extract-appendvecs":
		label = "Extract"
	case "flush-shard-logs":
		label = "Flush"
	}
	barStart := strings.Index(trimmed, "[")
	barEnd := strings.LastIndex(trimmed, "]")
	if barStart >= 0 && barEnd > barStart {
		tail := strings.TrimSpace(trimmed[barEnd+1:])
		if tail != "" {
			return label + ": " + tail
		}
	}
	return trimmed
}

func lightbringerLogLines(cfg *configData, maxLines int) []string {
	if cfg == nil {
		return nil
	}
	if cfg.lbEnabled {
		lines := trimTrailingEmptyLogLines(readLogTail(cfg.logsPath, "lightbringer.log", maxLines))
		if cfg.lbQuiet {
			hint := "(quiet mode: only warnings/errors are shown; silence can be normal)"
			if maxLines == 1 {
				return []string{hint}
			}
			if maxLines > 1 && len(lines) >= maxLines {
				lines = lines[len(lines)-(maxLines-1):]
			}
			lines = append([]string{hint}, lines...)
		}
		return lines
	}
	if cfg.blockSource == "lightbringer" && cfg.lbExternalEndpoint != "" {
		return []string{
			"(external Lightbringer endpoint: " + config.RedactEndpointForDisplay(cfg.lbExternalEndpoint) + ")",
			"(no local Lightbringer log is managed by this dashboard)",
		}
	}
	return nil
}

func trimTrailingEmptyLogLines(lines []string) []string {
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

type progressEvent struct {
	TS      time.Time
	Phase   string
	Status  string
	Message string
	Fields  map[string]any
}

type snapshotActivity struct {
	Path     string
	Name     string
	Bytes    int64
	Partial  bool
	ModTime  time.Time
	Observed time.Time
}

type accountsActivity struct {
	Path     string
	Name     string
	ModTime  time.Time
	Observed time.Time
}

func (s snapshotActivity) active() bool {
	return s.Path != "" && s.Bytes > 0
}

func (a accountsActivity) active() bool {
	return a.Path != "" && !a.ModTime.IsZero()
}

func (a accountsActivity) hasData(root string) bool {
	if !a.active() {
		return false
	}
	if root == "" {
		return true
	}
	return filepath.Clean(a.Path) != filepath.Clean(root)
}

func readProgressEvents(logsPath string, maxEvents int) []progressEvent {
	if maxEvents <= 0 {
		return nil
	}
	lines := readLogTail(logsPath, progress.JSONLFileName, maxEvents*2)
	events := make([]progressEvent, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || !strings.HasPrefix(line, "{") {
			continue
		}
		ev, ok := parseProgressEvent(line)
		if ok {
			events = append(events, ev)
		}
	}
	if len(events) > maxEvents {
		events = events[len(events)-maxEvents:]
	}
	return events
}

func parseProgressEvent(line string) (progressEvent, bool) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return progressEvent{}, false
	}
	phase, _ := raw["phase"].(string)
	if phase == "" {
		return progressEvent{}, false
	}
	status, _ := raw["status"].(string)
	message, _ := raw["message"].(string)
	var ts time.Time
	if tsRaw, ok := raw["ts"].(string); ok {
		ts, _ = time.Parse(time.RFC3339Nano, tsRaw)
	}
	fields := make(map[string]any, len(raw))
	for k, v := range raw {
		switch k {
		case "phase", "status", "message", "ts":
			continue
		default:
			if s, ok := v.(string); ok {
				fields[k] = config.RedactSecretsInText(s)
			} else {
				fields[k] = v
			}
		}
	}
	return progressEvent{
		TS:      ts,
		Phase:   phase,
		Status:  status,
		Message: config.RedactSecretsInText(message),
		Fields:  fields,
	}, true
}

func readSnapshotActivity(snapshotDir string) snapshotActivity {
	if snapshotDir == "" {
		return snapshotActivity{}
	}
	if info, err := os.Stat(snapshotDir); err == nil && !info.IsDir() {
		name := filepath.Base(snapshotDir)
		if !isSnapshotArtifactName(name) {
			return snapshotActivity{}
		}
		return snapshotActivity{
			Path:     snapshotDir,
			Name:     name,
			Bytes:    info.Size(),
			Partial:  strings.HasSuffix(name, ".partial"),
			ModTime:  info.ModTime(),
			Observed: time.Now(),
		}
	}
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		return snapshotActivity{}
	}

	var best snapshotActivity
	now := time.Now()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !isSnapshotArtifactName(name) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		candidate := snapshotActivity{
			Path:     filepath.Join(snapshotDir, name),
			Name:     name,
			Bytes:    info.Size(),
			Partial:  strings.HasSuffix(name, ".partial"),
			ModTime:  info.ModTime(),
			Observed: now,
		}
		if best.Path == "" || candidate.ModTime.After(best.ModTime) {
			best = candidate
		}
	}
	return best
}

func readAccountsActivity(accountsPath string) accountsActivity {
	if accountsPath == "" {
		return accountsActivity{}
	}
	candidates := []string{
		accountsPath,
		filepath.Join(accountsPath, "accounts"),
		filepath.Join(accountsPath, "mithril_db_log_shards"),
		filepath.Join(accountsPath, "mithril_db"),
		filepath.Join(accountsPath, "bankhash_db"),
		filepath.Join(accountsPath, "largest_file_id"),
		filepath.Join(accountsPath, "bank_hash"),
		filepath.Join(accountsPath, "manifest"),
		filepath.Join(accountsPath, "mithril_state.json"),
	}

	var best accountsActivity
	now := time.Now()
	for _, path := range candidates {
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		candidate := accountsActivity{
			Path:     path,
			Name:     filepath.Base(path),
			ModTime:  info.ModTime(),
			Observed: now,
		}
		if best.Path == "" || candidate.ModTime.After(best.ModTime) {
			best = candidate
		}
	}
	return best
}

func isSnapshotArtifactName(name string) bool {
	if strings.HasSuffix(name, ".partial") {
		name = strings.TrimSuffix(name, ".partial")
	}
	return (strings.HasPrefix(name, "snapshot-") || strings.HasPrefix(name, "incremental-snapshot-")) &&
		strings.HasSuffix(name, ".tar.zst")
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
		fullKey == "turbine.shred_version" ||
		fullKey == "lightbringer.gossip_port" ||
		fullKey == "lightbringer.port_range_start" ||
		fullKey == "lightbringer.port_range_end":
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
			if len(existing) == 0 {
				existing = v.GetStringSlice("rpc.rpc")
			}
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
		if sectionName, ok := tomlSectionName(trimmed); ok {
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

// setTomlValueInline replaces a value in a TOML file, preserving structure.
func setTomlValueInline(content, section, key, value string) string {
	lines := strings.Split(content, "\n")
	inSection := false
	sectionFound := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if sectionName, ok := tomlSectionName(trimmed); ok {
			if inSection {
				// Section found but key missing — insert before next section header
				result := make([]string, 0, len(lines)+1)
				result = append(result, lines[:i]...)
				result = append(result, key+" = "+value)
				result = append(result, lines[i:]...)
				return strings.Join(result, "\n")
			}
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

func tomlSectionName(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "#") || !strings.HasPrefix(trimmed, "[") || strings.HasPrefix(trimmed, "[[") {
		return "", false
	}
	end := strings.Index(trimmed, "]")
	if end <= 1 {
		return "", false
	}
	tail := strings.TrimSpace(trimmed[end+1:])
	if tail != "" && !strings.HasPrefix(tail, "#") {
		return "", false
	}
	return strings.TrimSpace(trimmed[1:end]), true
}
