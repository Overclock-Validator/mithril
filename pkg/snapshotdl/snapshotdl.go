package snapshotdl

import (
	"context"
	"fmt"
	"io"
	stdlog "log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/config"
	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/rpc"
	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/snapshot"
)

// stdlogMu protects global log config changes from racing if multiple
// snapshot flows run concurrently.
var stdlogMu sync.Mutex

// suppressStdlogOutput temporarily redirects stdlib log output to io.Discard
// so snapshot-finder internal logging doesn't appear in Mithril's output.
// Caller must defer the returned restore function.
func suppressStdlogOutput() func() {
	stdlogMu.Lock()
	oldWriter := stdlog.Writer()
	oldFlags := stdlog.Flags()
	oldPrefix := stdlog.Prefix()
	stdlog.SetOutput(io.Discard)
	return func() {
		stdlog.SetOutput(oldWriter)
		stdlog.SetFlags(oldFlags)
		stdlog.SetPrefix(oldPrefix)
		stdlogMu.Unlock()
	}
}

// SnapshotInfo contains details about a selected snapshot source.
// This is returned by GetSnapshotURLWithInfo for display purposes.
type SnapshotInfo struct {
	URL           string  // HTTP URL for streaming
	Slot          int     // Full snapshot slot
	ReferenceSlot int     // Current network slot (for calculating age)
	NodeIP        string  // IP:port of the selected node
	NodeVersion   string  // Solana version of the node
	SpeedMBs      float64 // Download speed in MB/s from Stage 2 testing
	RTTMs         int     // Round-trip time in milliseconds
}

// Age returns how many slots behind the snapshot is from the current tip
func (s *SnapshotInfo) Age() int {
	return s.ReferenceSlot - s.Slot
}

// SnapshotConfig holds configuration for snapshot downloading.
// This can be populated from CLI flags or TOML config.
type SnapshotConfig struct {
	// RPC endpoints for reference slot and cluster discovery
	RPCAddresses []string

	// Stage 1: Fast parallel triage
	Stage1WarmKiB     int64
	Stage1WindowKiB   int64
	Stage1Windows     int
	Stage1TimeoutMS   int64
	Stage1Concurrency int

	// Stage 2: Sustained speed test for top candidates
	Stage2TopK       int
	Stage2WarmSec    int
	Stage2MeasureSec int
	Stage2MinRatio   float64
	Stage2MinAbsMBs  float64

	// Node filtering
	MaxRTTMs            int
	TCPTimeoutMs        int
	MinNodeVersion      string
	AllowedNodeVersions []string
	NodeBlacklist       []string // RPC URLs, host:port pairs, or hosts/IPs to avoid as snapshot sources

	// Snapshot age thresholds (slots)
	FullThreshold        int
	IncrementalThreshold int

	// Snapshot storage (controls both saving and retention)
	// 0 = Stream-only mode (don't save snapshots to disk)
	// 1+ = Save snapshots and keep up to N on disk
	MaxFullSnapshots int

	// Safety
	SafetyMarginSlots int

	// Worker settings
	WorkerCount int

	// Output verbosity
	Verbose bool

	// Download path (only used when MaxFullSnapshots > 0)
	DownloadPath string

	// Fallback resilience
	MaxSnapshotURLAttempts int // Number of ranked nodes to try when getting snapshot URLs (0 = try all)

	// Incremental snapshot selection
	MinIncrementalSpeedMBs float64 // Minimum speed for incremental sources (MB/s, 0 = no minimum)

	// Slot constraints
	MaxSlot int64 // Maximum slot for snapshots (0 = no limit). Filters out full snapshots and incrementals above this slot.

	// Incremental snapshot targeting
	TargetIncrementalBase int64 // Only consider incrementals with this base slot (0 = any base)
	MaxIncrementalSlot    int64 // Maximum slot for incremental snapshots (0 = no limit)

	// Logging
	LogDir string // Directory for snapshot finder logs (default: /mnt/mithril-logs/snapshot-finder)
}

// DefaultSnapshotConfig returns production-ready defaults matching solana-snapshot-finder-go
func DefaultSnapshotConfig() SnapshotConfig {
	return SnapshotConfig{
		// Stage 1: Fast parallel downloads for quick triage
		Stage1WarmKiB:     512,  // 512 KiB warmup
		Stage1WindowKiB:   512,  // 512 KiB measurement windows
		Stage1Windows:     4,    // 4 windows = 2 MiB total per node
		Stage1TimeoutMS:   3000, // 3 second timeout per node
		Stage1Concurrency: 0,    // Auto (number of CPU cores)

		// Stage 2: Sustained speed test for top candidates
		Stage2TopK:       8,   // Test top 8 from stage 1
		Stage2WarmSec:    3,   // 3 second warmup (recommended for home internet, 1-2 for datacenter)
		Stage2MeasureSec: 3,   // 3 second measurement (recommended for home internet, 1-2 for datacenter)
		Stage2MinRatio:   0.6, // Collapse if speed drops below 60%
		Stage2MinAbsMBs:  0.0, // Disabled (0 = no minimum)

		// Node filtering
		MaxRTTMs:            200,     // 200ms max RTT
		TCPTimeoutMs:        1000,    // 1 second TCP precheck
		MinNodeVersion:      "3.0.0", // Minimum Agave 3.0.0
		AllowedNodeVersions: nil,     // Accept all versions >= minimum

		// Snapshot age thresholds
		FullThreshold:        100000, // Full snapshots up to 100k slots old (Agave 3.0+)
		IncrementalThreshold: 2000,   // Freshness band: incrementals (and full-source bases) within this many slots of tip

		// Snapshot storage (0 = stream-only, 1+ = save and retain N)
		// Saved snapshots are valuable for debugging and reproducing issues
		MaxFullSnapshots: 1, // Save one snapshot by default

		// Safety
		SafetyMarginSlots: 5000, // Warn if <5000 slots until expiration

		// Workers
		WorkerCount: 100, // Concurrent node evaluation workers

		// Output
		Verbose: false, // Quiet by default

		// Download path (set via config when MaxFullSnapshots > 0)
		DownloadPath: "",

		// Fallback resilience
		MaxSnapshotURLAttempts: 3, // Try top 3 ranked nodes before giving up

		// Incremental selection
		MinIncrementalSpeedMBs: 2.0, // Minimum 2 MB/s for incrementals (~8min for 1GB)

		// Logging
		LogDir: "/mnt/mithril-logs/snapshot-finder",
	}
}

// toInternalConfig converts SnapshotConfig to snapshot-finder's config.Config
func (sc SnapshotConfig) toInternalConfig(path string) config.Config {
	return config.Config{
		RPCAddresses:         sc.RPCAddresses,
		SnapshotPath:         path,
		NumOfRetries:         5,
		SleepBeforeRetry:     2,
		EnableBlacklist:      false,
		WhitelistMode:        "disabled",
		WorkerCount:          sc.WorkerCount,
		FullThreshold:        sc.FullThreshold,
		IncrementalThreshold: sc.IncrementalThreshold,
		Stage1WarmKiB:        sc.Stage1WarmKiB,
		Stage1WindowKiB:      sc.Stage1WindowKiB,
		Stage1Windows:        sc.Stage1Windows,
		Stage1TimeoutMS:      sc.Stage1TimeoutMS,
		Stage1Concurrency:    sc.Stage1Concurrency,
		Stage2TopK:           sc.Stage2TopK,
		Stage2WarmSec:        sc.Stage2WarmSec,
		Stage2MeasureSec:     sc.Stage2MeasureSec,
		Stage2MinRatio:       sc.Stage2MinRatio,
		Stage2MinAbsMBs:      sc.Stage2MinAbsMBs,
		MaxRTTMs:             sc.MaxRTTMs,
		TCPTimeoutMs:         sc.TCPTimeoutMs,
		MinNodeVersion:       sc.MinNodeVersion,
		AllowedNodeVersions:  sc.AllowedNodeVersions,
		MaxFullSnapshots:     sc.MaxFullSnapshots,
		SafetyMarginSlots:    sc.SafetyMarginSlots,
	}
}

// filterByMaxSlot filters results to only include nodes whose FullSlot is at or below maxSlot.
// If maxSlot is 0, no filtering is applied (returns all results).
// Returns (filtered results, count of filtered out nodes).
func filterByMaxSlot(results []rpc.NodeResult, maxSlot int64) ([]rpc.NodeResult, int) {
	if maxSlot == 0 {
		return results, 0
	}

	var filtered []rpc.NodeResult
	filteredOut := 0

	for _, r := range results {
		if r.FullSlot > 0 && r.FullSlot <= maxSlot {
			filtered = append(filtered, r)
		} else if r.FullSlot > maxSlot {
			filteredOut++
		}
	}

	return filtered, filteredOut
}

type snapshotNodeBlacklist struct {
	keys map[string]struct{}
}

func newSnapshotNodeBlacklist(entries []string) snapshotNodeBlacklist {
	keys := make(map[string]struct{})
	for _, entry := range entries {
		for _, key := range snapshotEndpointKeys(entry) {
			keys[key] = struct{}{}
		}
	}
	return snapshotNodeBlacklist{keys: keys}
}

func (b snapshotNodeBlacklist) empty() bool {
	return len(b.keys) == 0
}

func (b snapshotNodeBlacklist) contains(endpoint string) bool {
	if b.empty() {
		return false
	}
	for _, key := range snapshotEndpointKeys(endpoint) {
		if _, ok := b.keys[key]; ok {
			return true
		}
	}
	return false
}

func snapshotEndpointKeys(raw string) []string {
	cleaned := strings.TrimSpace(raw)
	if cleaned == "" {
		return nil
	}

	var keys []string
	seen := make(map[string]struct{})
	add := func(key string) {
		key = strings.TrimRight(strings.TrimSpace(strings.ToLower(key)), "/")
		if key == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}

	normalized := strings.TrimRight(strings.ToLower(cleaned), "/")
	add(normalized)

	if u, err := url.Parse(normalized); err == nil && u.Host != "" {
		hostPort := strings.ToLower(u.Host)
		host := strings.ToLower(u.Hostname())
		port := u.Port()
		add(hostPort)
		if u.Scheme != "" {
			add(u.Scheme + "://" + hostPort)
		}
		add(host)
		if host != "" && port != "" {
			add(host + ":" + port)
			add(net.JoinHostPort(host, port))
		}
		return keys
	}

	hostPort := normalized
	if slash := strings.IndexByte(hostPort, '/'); slash >= 0 {
		hostPort = hostPort[:slash]
	}
	add(hostPort)

	if host, port, err := net.SplitHostPort(hostPort); err == nil {
		host = strings.Trim(host, "[]")
		add(host)
		if host != "" && port != "" {
			add(host + ":" + port)
			add(net.JoinHostPort(host, port))
		}
		return keys
	}

	if strings.Count(hostPort, ":") == 1 {
		parts := strings.SplitN(hostPort, ":", 2)
		add(parts[0])
	}

	return keys
}

func filterSnapshotRPCNodes(nodes []rpc.RPCNode, blacklist snapshotNodeBlacklist) ([]rpc.RPCNode, int) {
	if blacklist.empty() {
		return nodes, 0
	}

	filtered := make([]rpc.RPCNode, 0, len(nodes))
	skipped := 0
	for _, node := range nodes {
		if blacklist.contains(node.Address) {
			skipped++
			continue
		}
		filtered = append(filtered, node)
	}
	return filtered, skipped
}

func addConfiguredSnapshotRPCNodes(nodes []rpc.RPCNode, preferredRPC string, rpcAddresses []string) ([]rpc.RPCNode, int) {
	seen := make(map[string]struct{})
	for _, node := range nodes {
		for _, key := range snapshotEndpointKeys(node.Address) {
			seen[key] = struct{}{}
		}
	}

	candidates := make([]string, 0, len(rpcAddresses)+1)
	if preferredRPC != "" {
		candidates = append(candidates, preferredRPC)
	}
	for _, addr := range rpcAddresses {
		if addr != preferredRPC {
			candidates = append(candidates, addr)
		}
	}

	added := 0
	for _, addr := range candidates {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		duplicate := false
		keys := snapshotEndpointKeys(addr)
		for _, key := range keys {
			if _, ok := seen[key]; ok {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		nodes = append(nodes, rpc.RPCNode{
			Address:  addr,
			Version:  "999.0.0-configured",
			IsStatic: true,
		})
		for _, key := range keys {
			seen[key] = struct{}{}
		}
		added++
	}
	return nodes, added
}

func filterSnapshotNodeResults(results []rpc.NodeResult, blacklist snapshotNodeBlacklist) ([]rpc.NodeResult, int) {
	if blacklist.empty() {
		return results, 0
	}

	filtered := make([]rpc.NodeResult, 0, len(results))
	skipped := 0
	for _, result := range results {
		if blacklist.contains(result.RPC) {
			skipped++
			continue
		}
		filtered = append(filtered, result)
	}
	return filtered, skipped
}

func logSnapshotBlacklist(stage string, skipped int, remaining int) {
	if skipped == 0 {
		return
	}
	mlog.Log.Infof("Snapshot node blacklist excluded %d %s candidate(s); %d remain", skipped, stage, remaining)
}

func logSnapshotProbeFilterSummary(label string, stats *rpc.ProbeStats, cfg config.Config, incStats incBaseMatchStats) {
	if stats == nil {
		return
	}
	mlog.Log.Warnf("Snapshot probe summary (%s): with_snapshots=%d after_version=%d after_rtt=%d after_full_age=%d after_inc_age=%d eligible=%d | incremental_base_match=%d/%d full_sources (%d slot-0 full archive(s), %d matching full slot(s), %d unique full slot(s), %d unique inc base(s)) | filters: min_version=%q allowed_versions=%s max_rtt_ms=%d full_threshold=%d incremental_threshold=%d",
		label,
		stats.InitialWithSnapshots,
		stats.AfterVersionFilter,
		stats.AfterRTTFilter,
		stats.AfterFullAgeFilter,
		stats.AfterIncAgeFilter,
		stats.Eligible,
		incStats.afterIncBaseMatch,
		incStats.totalWithFull,
		incStats.totalWithFullSlotZero,
		incStats.matchingFullSlots,
		incStats.uniqueFullSlots,
		incStats.uniqueIncBases,
		cfg.MinNodeVersion,
		formatAllowedVersions(cfg.AllowedNodeVersions),
		cfg.MaxRTTMs,
		cfg.FullThreshold,
		cfg.IncrementalThreshold,
	)
	if stats.InitialWithSnapshots > 0 && incStats.totalWithFull == 0 {
		mlog.Log.Warnf("Snapshot probe summary (%s): discovered snapshot-serving nodes, but none exposed a full snapshot archive; incremental snapshots alone cannot bootstrap AccountsDB", label)
	}
	if incStats.totalWithFullSlotZero > 0 && incStats.matchingFullSlots == 0 {
		mlog.Log.Warnf("Snapshot probe summary (%s): %d source(s) exposed slot-0 full snapshots, but none had a matching base-0 incremental", label, incStats.totalWithFullSlotZero)
	}
}

func formatAllowedVersions(versions []string) string {
	if len(versions) == 0 {
		return "any"
	}
	return strings.Join(versions, ",")
}

func relaxedSnapshotProbeConfig(cfg config.Config) config.Config {
	relaxed := cfg
	if relaxed.MaxRTTMs > 0 && relaxed.MaxRTTMs < 1000 {
		relaxed.MaxRTTMs = 1000
	}
	if relaxed.FullThreshold > 0 && relaxed.FullThreshold < 1000000 {
		relaxed.FullThreshold = 1000000
	}
	if relaxed.IncrementalThreshold > 0 && relaxed.IncrementalThreshold < 10000 {
		relaxed.IncrementalThreshold = 10000
	}
	if relaxed.Stage1TimeoutMS > 0 && relaxed.Stage1TimeoutMS < 8000 {
		relaxed.Stage1TimeoutMS = 8000
	}
	if relaxed.Stage1WarmKiB > 0 {
		relaxed.Stage1WarmKiB = 0
	}
	if relaxed.Stage1WindowKiB <= 0 || relaxed.Stage1WindowKiB > 64 {
		relaxed.Stage1WindowKiB = 64
	}
	if relaxed.Stage1Windows <= 0 || relaxed.Stage1Windows > 1 {
		relaxed.Stage1Windows = 1
	}
	if relaxed.Stage2TopK > 0 && relaxed.Stage2TopK < 16 {
		relaxed.Stage2TopK = 16
	}
	if relaxed.Stage2MinRatio > 0 {
		relaxed.Stage2MinRatio = 0
	}
	return relaxed
}

func snapshotProbeConfigChanged(a config.Config, b config.Config) bool {
	return a.MaxRTTMs != b.MaxRTTMs ||
		a.FullThreshold != b.FullThreshold ||
		a.IncrementalThreshold != b.IncrementalThreshold ||
		a.Stage1TimeoutMS != b.Stage1TimeoutMS ||
		a.Stage1WarmKiB != b.Stage1WarmKiB ||
		a.Stage1WindowKiB != b.Stage1WindowKiB ||
		a.Stage1Windows != b.Stage1Windows ||
		a.Stage2TopK != b.Stage2TopK ||
		a.Stage2MinRatio != b.Stage2MinRatio
}

// incBaseMatchStats tracks statistics for incremental base matching filter
type incBaseMatchStats struct {
	totalWithFull         int   // nodes with a full snapshot endpoint, including slot 0
	totalWithFullSlotZero int   // nodes whose full snapshot filename parsed as slot 0
	totalWithInc          int   // nodes with any incremental
	afterIncBaseMatch     int   // nodes remaining after incremental base match filter
	uniqueFullSlots       int   // number of unique full snapshot slots
	uniqueIncBases        int   // number of unique incremental base slots
	matchingFullSlots     int   // full slots that have at least one matching inc base
	freshBases            int   // bases whose best incremental end is within threshold of tip
	freshestEndBehind     int64 // when nothing is fresh: how far behind tip the best achievable end is
}

// filterByIncrementalBaseMatch restricts full-snapshot candidates to bases
// whose ACHIEVABLE end state is freshest, then lets download-speed ranking
// pick among them. Selection order:
//
//  1. Bases with at least one incremental (anywhere in the network) ending
//     within incThreshold slots of the live tip — the node bootstraps fresh
//     no matter which of these sources wins the speed test.
//  2. If none qualify, the base(s) with the freshest achievable incremental
//     end, under a loud warning (the whole cluster is stale; pick the least
//     stale reachable state rather than a fast-but-ancient source).
//  3. If no base has any matching incremental at all, no filtering (the
//     caller's downstream policies own that edge).
//
// Speed is deliberately the TIEBREAK inside a freshness class, never the
// primary key: a fast node whose snapshotting has wedged must not outrank a
// slower node that lets us start hours fresher.
func filterByIncrementalBaseMatch(results []rpc.NodeResult, referenceSlot int, incThreshold int) ([]rpc.NodeResult, incBaseMatchStats) {
	var stats incBaseMatchStats

	// Step 1: Full slots present, and the best (highest) incremental end
	// available anywhere per base slot.
	fullSlots := make(map[int64]bool)
	bestEndByBase := make(map[int64]int64)

	for _, r := range results {
		if fullSlot, ok := fullSnapshotSlotForMatch(r); ok {
			stats.totalWithFull++
			fullSlots[fullSlot] = true
			if fullSlot == 0 {
				stats.totalWithFullSlotZero++
			}
		}
		if r.HasInc {
			stats.totalWithInc++
			if r.IncSlot > bestEndByBase[r.IncBase] {
				bestEndByBase[r.IncBase] = r.IncSlot
			}
		}
	}

	stats.uniqueFullSlots = len(fullSlots)
	stats.uniqueIncBases = len(bestEndByBase)

	// Step 2: Bases that are both present as a full AND have an incremental.
	matchingSlots := make(map[int64]bool)
	for fullSlot := range fullSlots {
		if _, ok := bestEndByBase[fullSlot]; ok {
			matchingSlots[fullSlot] = true
		}
	}
	stats.matchingFullSlots = len(matchingSlots)

	// If no matching slots found, return all results (don't filter)
	// This handles edge cases like all incrementals being stale
	if len(matchingSlots) == 0 {
		stats.afterIncBaseMatch = stats.totalWithFull
		return results, stats
	}

	// Step 2.5: Freshness restriction. Prefer bases whose best incremental
	// end is within incThreshold of the live tip; otherwise fall back to the
	// freshest achievable base(s).
	selected := matchingSlots
	if referenceSlot > 0 && incThreshold > 0 {
		fresh := make(map[int64]bool)
		var bestEnd int64
		for base := range matchingSlots {
			end := bestEndByBase[base]
			if end > bestEnd {
				bestEnd = end
			}
			if int64(referenceSlot)-end <= int64(incThreshold) {
				fresh[base] = true
			}
		}
		if len(fresh) > 0 {
			if len(fresh) < len(matchingSlots) {
				mlog.Log.Infof("Snapshot source selection: restricting to %d/%d base slot(s) with incrementals within %d slots of tip %d",
					len(fresh), len(matchingSlots), incThreshold, referenceSlot)
			}
			selected = fresh
			stats.freshBases = len(fresh)
		} else {
			// Whole cluster is stale: pick the least-stale achievable state.
			freshest := make(map[int64]bool)
			for base := range matchingSlots {
				if bestEndByBase[base] == bestEnd {
					freshest[base] = true
				}
			}
			selected = freshest
			stats.freshestEndBehind = int64(referenceSlot) - bestEnd
			mlog.Log.Warnf("Snapshot source selection: NO incremental within %d slots of tip anywhere in the network; freshest achievable is %d slots behind (end %d) — selecting sources for that base rather than a faster-but-staler one",
				incThreshold, stats.freshestEndBehind, bestEnd)
		}
	}

	// Step 3: Filter to only nodes whose FullSlot is in the selected set.
	var filtered []rpc.NodeResult
	for _, r := range results {
		if fullSlot, ok := fullSnapshotSlotForMatch(r); ok && selected[fullSlot] {
			filtered = append(filtered, r)
		}
	}

	stats.afterIncBaseMatch = len(filtered)
	return filtered, stats
}

func fullSnapshotSlotForMatch(r rpc.NodeResult) (int64, bool) {
	if r.FullSlot > 0 {
		return r.FullSlot, true
	}
	if r.FullURL != "" && r.FullSlot == 0 {
		return 0, true
	}
	return 0, false
}

func normalizeGenesisFullSnapshotsForRanking(results []rpc.NodeResult) []rpc.NodeResult {
	normalized := make([]rpc.NodeResult, len(results))
	copy(normalized, results)
	for i := range normalized {
		r := &normalized[i]
		if r.FullURL != "" && r.FullSlot == 0 && r.HasInc && r.IncBase == 0 {
			// solana-snapshot-finder treats FullSlot <= 0 as "no full snapshot".
			// Fresh test clusters can legitimately expose snapshot-0 plus a
			// base-0 incremental. Use slot 1 only for ranking/freshness checks;
			// the final URL probe still returns the real slot 0 to the builder.
			r.FullSlot = 1
		}
	}
	return normalized
}

// DownloadSnapshot downloads a full snapshot from the best available RPC node.
// It returns: (downloadPath, referenceSlot, snapshotSlot, error)
//
// This function:
// 1. Gets the reference slot from the network
// 2. Discovers available RPC nodes from the cluster
// 3. Evaluates nodes for snapshot availability and download speed
// 4. Downloads from the fastest node with a recent snapshot
func DownloadSnapshot(ctx context.Context, rpcEndpoints []string, path string) (string, int, int, error) {
	cfg := DefaultSnapshotConfig()
	cfg.RPCAddresses = rpcEndpoints
	return DownloadSnapshotWithConfig(ctx, path, cfg)
}

// GetSnapshotURL discovers the best RPC node and returns the HTTP URL for streaming.
// Returns: (httpURL, referenceSlot, snapshotSlot, error)
//
// This function:
// 1. Gets the reference slot from the network
// 2. Discovers available RPC nodes from the cluster
// 3. Evaluates nodes for snapshot availability and download speed
// 4. Returns HTTP URL from the fastest node (for streaming)
//
// The returned URL can be passed directly to snapshot processing functions
// which will stream the data from HTTP (no disk download required).
func GetSnapshotURL(ctx context.Context, snapCfg SnapshotConfig) (string, int, int, error) {
	defer suppressStdlogOutput()()
	cfg := snapCfg.toInternalConfig("")
	blacklist := newSnapshotNodeBlacklist(snapCfg.NodeBlacklist)

	// Step 1: Get reference slot from multiple RPCs for reliability
	mlog.Log.Infof("Getting reference slot from RPC(s)...")
	referenceSlot, preferredRPC, err := rpc.GetReferenceSlotFromMultiple(cfg.RPCAddresses)
	if err != nil {
		return "", 0, 0, fmt.Errorf("error getting reference slot: %w", rpcclient.WrapErrorForDisplay(err))
	}
	mlog.Log.Infof("Reference slot: %d (from %s)", referenceSlot, rpcclient.SanitizeEndpointForDisplay(preferredRPC))

	// Step 2: Fetch cluster nodes
	mlog.Log.Infof("Discovering RPC nodes from cluster...")
	nodes := rpc.FetchClusterNodes(cfg, preferredRPC)
	if len(nodes) == 0 {
		return "", 0, 0, fmt.Errorf("no rpc nodes available from cluster")
	}
	nodes, configuredAdded := addConfiguredSnapshotRPCNodes(nodes, preferredRPC, cfg.RPCAddresses)
	if configuredAdded > 0 {
		mlog.Log.Infof("Added %d configured RPC endpoint(s) to snapshot probe candidates", configuredAdded)
	}
	var skipped int
	nodes, skipped = filterSnapshotRPCNodes(nodes, blacklist)
	logSnapshotBlacklist("discovered", skipped, len(nodes))
	if len(nodes) == 0 {
		return "", 0, 0, fmt.Errorf("all discovered rpc nodes were excluded by snapshot.node_blacklist")
	}
	mlog.Log.Infof("Found %d potential snapshot sources", len(nodes))

	// Step 3: Evaluate nodes with version tracking and statistics
	mlog.Log.Infof("Evaluating nodes for snapshot availability and speed...")
	results, stats := rpc.EvaluateNodesWithVersionsAndStats(nodes, cfg, referenceSlot)
	results, skipped = filterSnapshotNodeResults(results, blacklist)
	logSnapshotBlacklist("evaluated", skipped, len(results))

	// Step 3.5: Filter to only full snapshots that have matching incrementals somewhere
	results, _ = filterByIncrementalBaseMatch(results, referenceSlot, cfg.IncrementalThreshold)
	rankingResults := normalizeGenesisFullSnapshotsForRanking(results)

	// Step 4: Sort and select best nodes by download speed
	// Note: This also populates filter pipeline stats in ProbeStats
	bestNodes, _ := rpc.SortBestNodesWithStats(rankingResults, cfg, stats, referenceSlot)
	if len(bestNodes) == 0 {
		return "", 0, 0, fmt.Errorf("no suitable nodes found with snapshots")
	}

	// Print statistics if verbose mode (after filtering is complete)
	if snapCfg.Verbose && stats != nil {
		filterCfg := rpc.FilterConfig{
			MaxRTTMs:        cfg.MaxRTTMs,
			FullThreshold:   cfg.FullThreshold,
			IncThreshold:    cfg.IncrementalThreshold,
			MinVersion:      cfg.MinNodeVersion,
			AllowedVersions: cfg.AllowedNodeVersions,
		}
		stats.PrintReport(filterCfg)
	}

	// Step 5: Get snapshot URL from best nodes (with configurable fallback)
	// NOTE: This fallback pattern is useful for resilience - if the #1 ranked node's
	// HTTP endpoint is down or snapshot was deleted, we try the next best nodes.
	// This pattern could be extracted to snapshot-finder library for reuse.
	var snapshotURL string
	var snapshotSlot int
	var urlErr error

	maxAttempts := snapCfg.MaxSnapshotURLAttempts
	if maxAttempts <= 0 {
		maxAttempts = len(bestNodes) // 0 means try all available nodes
	}

	for i, nodeRPC := range bestNodes {
		// Check for cancellation
		if ctx.Err() != nil {
			return "", 0, 0, ctx.Err()
		}

		if i >= maxAttempts {
			break
		}

		if i > 0 {
			// We're on a fallback attempt
			mlog.Log.Infof("Trying fallback source #%d: %s", i+1, rpcclient.SanitizeEndpointForDisplay(nodeRPC))
		}

		if snapCfg.Verbose {
			mlog.Log.Infof("Getting snapshot URL from: %s (rank #%d)", rpcclient.SanitizeEndpointForDisplay(nodeRPC), i+1)
		}
		urlInfo, err := snapshot.GetSnapshotURL(ctx, nodeRPC, "full")

		if err == nil && urlInfo != nil {
			snapshotURL = urlInfo.URL
			snapshotSlot = urlInfo.Slot
			break
		}

		if err != nil {
			if snapCfg.Verbose {
				mlog.Log.Infof("Failed to get snapshot URL from %s: %s. Trying next...",
					rpcclient.SanitizeEndpointForDisplay(nodeRPC), rpcclient.SanitizeErrorForDisplay(err))
			}
			urlErr = rpcclient.WrapErrorForDisplay(err)
		} else {
			if snapCfg.Verbose {
				mlog.Log.Infof("GetSnapshotURL from %s returned nil. Trying next...",
					rpcclient.SanitizeEndpointForDisplay(nodeRPC))
			}
		}
	}

	if snapshotURL == "" {
		return "", 0, 0, fmt.Errorf("failed to get snapshot URL from any RPC node (tried %d nodes, last error: %v)", maxAttempts, urlErr)
	}

	return snapshotURL, referenceSlot, snapshotSlot, nil
}

// GetSnapshotURLWithInfo discovers the best RPC node and returns detailed info about the source.
// Returns: (*SnapshotInfo, error)
//
// This is like GetSnapshotURL but returns a SnapshotInfo struct with additional
// details useful for display (node IP, version, speed, age).
func GetSnapshotURLWithInfo(ctx context.Context, snapCfg SnapshotConfig) (*SnapshotInfo, error) {
	defer suppressStdlogOutput()()
	cfg := snapCfg.toInternalConfig("")
	blacklist := newSnapshotNodeBlacklist(snapCfg.NodeBlacklist)

	// Step 1: Get reference slot from multiple RPCs for reliability
	referenceSlot, preferredRPC, err := rpc.GetReferenceSlotFromMultiple(cfg.RPCAddresses)
	if err != nil {
		return nil, fmt.Errorf("error getting reference slot: %w", rpcclient.WrapErrorForDisplay(err))
	}
	if snapCfg.Verbose {
		mlog.Log.Infof("Reference slot: %d (from %s)", referenceSlot, rpcclient.SanitizeEndpointForDisplay(preferredRPC))
	}

	// Step 2: Fetch cluster nodes
	nodes := rpc.FetchClusterNodes(cfg, preferredRPC)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no rpc nodes available from cluster")
	}
	nodes, configuredAdded := addConfiguredSnapshotRPCNodes(nodes, preferredRPC, cfg.RPCAddresses)
	if configuredAdded > 0 {
		mlog.Log.Infof("Added %d configured RPC endpoint(s) to snapshot probe candidates", configuredAdded)
	}
	var skipped int
	nodes, skipped = filterSnapshotRPCNodes(nodes, blacklist)
	logSnapshotBlacklist("discovered", skipped, len(nodes))
	if len(nodes) == 0 {
		return nil, fmt.Errorf("all discovered rpc nodes were excluded by snapshot.node_blacklist")
	}

	// Step 3: Evaluate nodes with version tracking and statistics
	mlog.Log.Infof("Probing %d nodes for snapshot availability...", len(nodes))
	results, stats := rpc.EvaluateNodesWithVersionsAndStats(nodes, cfg, referenceSlot)
	results, skipped = filterSnapshotNodeResults(results, blacklist)
	logSnapshotBlacklist("evaluated", skipped, len(results))

	// Step 3.5: Filter to only full snapshots that have matching incrementals somewhere
	// This prevents selecting a fast full snapshot that has no compatible incrementals
	results, incBaseStats := filterByIncrementalBaseMatch(results, referenceSlot, cfg.IncrementalThreshold)
	rankingResults := normalizeGenesisFullSnapshotsForRanking(results)

	// Print incremental base match stats
	if incBaseStats.totalWithFull > 0 {
		mlog.Log.Infof("Incremental base matching: %d/%d full snapshots have compatible incrementals (%d unique base slots)",
			incBaseStats.afterIncBaseMatch, incBaseStats.totalWithFull, incBaseStats.matchingFullSlots)
		if incBaseStats.afterIncBaseMatch < incBaseStats.totalWithFull {
			mlog.Log.Infof("  (filtered %d sources with no matching incremental base)",
				incBaseStats.totalWithFull-incBaseStats.afterIncBaseMatch)
		}
	}

	// Step 4: Sort and select best nodes by download speed
	// Note: This also populates filter pipeline stats in ProbeStats
	mlog.Log.Infof("Testing download speeds (Stage 1 + Stage 2)...")
	bestNodes, rankedNodes := rpc.SortBestNodesWithStats(rankingResults, cfg, stats, referenceSlot)
	if len(bestNodes) == 0 {
		logSnapshotProbeFilterSummary("strict", stats, cfg, incBaseStats)
		relaxedCfg := relaxedSnapshotProbeConfig(cfg)
		if snapshotProbeConfigChanged(cfg, relaxedCfg) {
			mlog.Log.Warnf("Strict snapshot source filters found no usable nodes; retrying with relaxed probe filters (rtt=%dms->%dms, full_threshold=%d->%d, inc_threshold=%d->%d, stage1_timeout=%dms->%dms, stage1_sample=%d+%dx%dKiB->%d+%dx%dKiB, stage2_min_ratio=%.2f->%.2f)",
				cfg.MaxRTTMs, relaxedCfg.MaxRTTMs,
				cfg.FullThreshold, relaxedCfg.FullThreshold,
				cfg.IncrementalThreshold, relaxedCfg.IncrementalThreshold,
				cfg.Stage1TimeoutMS, relaxedCfg.Stage1TimeoutMS,
				cfg.Stage1WarmKiB, cfg.Stage1Windows, cfg.Stage1WindowKiB,
				relaxedCfg.Stage1WarmKiB, relaxedCfg.Stage1Windows, relaxedCfg.Stage1WindowKiB,
				cfg.Stage2MinRatio, relaxedCfg.Stage2MinRatio)
			bestNodes, rankedNodes = rpc.SortBestNodesWithStats(rankingResults, relaxedCfg, stats, referenceSlot)
			if len(bestNodes) == 0 {
				logSnapshotProbeFilterSummary("relaxed", stats, relaxedCfg, incBaseStats)
			} else {
				mlog.Log.Infof("Relaxed snapshot probe selected %d candidate(s)", len(bestNodes))
			}
		}
	}
	if len(bestNodes) == 0 {
		return nil, fmt.Errorf("no suitable nodes found with snapshots (filters eliminated all candidates; see snapshot probe summary above)")
	}

	// Print statistics report (after filtering is complete)
	if stats != nil {
		filterCfg := rpc.FilterConfig{
			MaxRTTMs:        cfg.MaxRTTMs,
			FullThreshold:   cfg.FullThreshold,
			IncThreshold:    cfg.IncrementalThreshold,
			MinVersion:      cfg.MinNodeVersion,
			AllowedVersions: cfg.AllowedNodeVersions,
		}
		stats.PrintReport(filterCfg)
	}

	// Step 5: Get snapshot URL from best nodes (with configurable fallback)
	var snapshotURL string
	var snapshotSlot int
	var selectedNodeRPC string
	var selectedSpeed float64
	var selectedVersion string
	var selectedRTT int
	var urlErr error

	maxAttempts := snapCfg.MaxSnapshotURLAttempts
	if maxAttempts <= 0 {
		maxAttempts = len(bestNodes)
	}

	for i, nodeRPC := range bestNodes {
		// Check for cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if i >= maxAttempts {
			break
		}

		if i > 0 {
			mlog.Log.Infof("Trying fallback source #%d: %s", i+1, rpcclient.SanitizeEndpointForDisplay(nodeRPC))
		}

		if snapCfg.Verbose {
			mlog.Log.Infof("Getting snapshot URL from: %s (rank #%d)", rpcclient.SanitizeEndpointForDisplay(nodeRPC), i+1)
		}
		urlInfo, err := snapshot.GetSnapshotURL(ctx, nodeRPC, "full")

		if err == nil && urlInfo != nil {
			// Find the speed, version, and RTT for this node from rankedNodes
			for _, rn := range rankedNodes {
				if rn.Result.RPC == nodeRPC {
					// Use Stage 2 min speed if available, otherwise fall back to Stage 1 median
					if rn.S2.MinMBs > 0 {
						selectedSpeed = rn.S2.MinMBs
					} else {
						selectedSpeed = rn.S1.MedianMBs
					}
					selectedVersion = rn.Result.Version
					selectedRTT = int(rn.Result.Latency) // Already in ms
					break
				}
			}
			selectedNodeRPC = nodeRPC
			snapshotURL = urlInfo.URL
			snapshotSlot = urlInfo.Slot
			break
		}

		if err != nil {
			if snapCfg.Verbose {
				mlog.Log.Infof("Failed to get snapshot URL from %s: %s. Trying next...",
					rpcclient.SanitizeEndpointForDisplay(nodeRPC), rpcclient.SanitizeErrorForDisplay(err))
			}
			urlErr = rpcclient.WrapErrorForDisplay(err)
		} else {
			if snapCfg.Verbose {
				mlog.Log.Infof("GetSnapshotURL from %s returned nil. Trying next...",
					rpcclient.SanitizeEndpointForDisplay(nodeRPC))
			}
		}
	}

	if snapshotURL == "" {
		return nil, fmt.Errorf("failed to get snapshot URL from any RPC node (tried %d nodes, last error: %v)", maxAttempts, urlErr)
	}

	// Extract IP from RPC URL (e.g., "http://141.94.163.217:8899" -> "141.94.163.217:8899")
	nodeIP := strings.TrimPrefix(strings.TrimPrefix(
		rpcclient.SanitizeEndpointForDisplay(selectedNodeRPC), "https://"), "http://")

	return &SnapshotInfo{
		URL:           snapshotURL,
		Slot:          snapshotSlot,
		ReferenceSlot: referenceSlot,
		NodeIP:        nodeIP,
		NodeVersion:   selectedVersion,
		SpeedMBs:      selectedSpeed,
		RTTMs:         selectedRTT,
	}, nil
}

// GetReferenceSlot queries the configured RPCs for the current slot — the
// freshness reference for incremental-snapshot selection. The reference MUST
// be the live chain tip: measuring incremental age against the full
// snapshot's slot makes any incremental that merely extends past the full
// look "fresh" (negative age) and defeats the staleness gate entirely.
func GetReferenceSlot(snapCfg SnapshotConfig) (int, error) {
	cfg := snapCfg.toInternalConfig("")
	referenceSlot, _, err := rpc.GetReferenceSlotFromMultiple(cfg.RPCAddresses)
	if err != nil {
		return 0, fmt.Errorf("error getting reference slot: %w", err)
	}
	return referenceSlot, nil
}

// DownloadSnapshotWithConfig is like DownloadSnapshot but accepts custom config
func DownloadSnapshotWithConfig(ctx context.Context, path string, snapCfg SnapshotConfig) (string, int, int, error) {
	defer suppressStdlogOutput()()
	cfg := snapCfg.toInternalConfig(path)
	blacklist := newSnapshotNodeBlacklist(snapCfg.NodeBlacklist)

	// Step 1: Get reference slot from multiple RPCs for reliability
	mlog.Log.Infof("Getting reference slot from RPC(s)...")
	referenceSlot, preferredRPC, err := rpc.GetReferenceSlotFromMultiple(cfg.RPCAddresses)
	if err != nil {
		return "", 0, 0, fmt.Errorf("error getting reference slot: %w", rpcclient.WrapErrorForDisplay(err))
	}
	mlog.Log.Infof("Reference slot: %d (from %s)", referenceSlot, rpcclient.SanitizeEndpointForDisplay(preferredRPC))

	// Step 2: Fetch cluster nodes
	mlog.Log.Infof("Discovering RPC nodes from cluster...")
	nodes := rpc.FetchClusterNodes(cfg, preferredRPC)
	if len(nodes) == 0 {
		return "", 0, 0, fmt.Errorf("no rpc nodes available from cluster")
	}
	nodes, configuredAdded := addConfiguredSnapshotRPCNodes(nodes, preferredRPC, cfg.RPCAddresses)
	if configuredAdded > 0 {
		mlog.Log.Infof("Added %d configured RPC endpoint(s) to snapshot probe candidates", configuredAdded)
	}
	var skipped int
	nodes, skipped = filterSnapshotRPCNodes(nodes, blacklist)
	logSnapshotBlacklist("discovered", skipped, len(nodes))
	if len(nodes) == 0 {
		return "", 0, 0, fmt.Errorf("all discovered rpc nodes were excluded by snapshot.node_blacklist")
	}
	mlog.Log.Infof("Found %d potential snapshot sources", len(nodes))

	// Step 3: Evaluate nodes with version tracking and statistics
	mlog.Log.Infof("Evaluating nodes for snapshot availability and speed...")
	results, stats := rpc.EvaluateNodesWithVersionsAndStats(nodes, cfg, referenceSlot)
	results, skipped = filterSnapshotNodeResults(results, blacklist)
	logSnapshotBlacklist("evaluated", skipped, len(results))

	// Step 3.5: Filter to only full snapshots that have matching incrementals somewhere
	results, _ = filterByIncrementalBaseMatch(results, referenceSlot, cfg.IncrementalThreshold)
	rankingResults := normalizeGenesisFullSnapshotsForRanking(results)

	// Step 4: Sort and select best nodes by download speed
	// Note: This also populates filter pipeline stats in ProbeStats
	bestNodes, _ := rpc.SortBestNodesWithStats(rankingResults, cfg, stats, referenceSlot)
	if len(bestNodes) == 0 {
		return "", 0, 0, fmt.Errorf("no suitable nodes found with snapshots")
	}

	// Print statistics if verbose mode (after filtering is complete)
	if snapCfg.Verbose && stats != nil {
		filterCfg := rpc.FilterConfig{
			MaxRTTMs:        cfg.MaxRTTMs,
			FullThreshold:   cfg.FullThreshold,
			IncThreshold:    cfg.IncrementalThreshold,
			MinVersion:      cfg.MinNodeVersion,
			AllowedVersions: cfg.AllowedNodeVersions,
		}
		stats.PrintReport(filterCfg)
	}

	// Step 5: Download snapshot from best nodes (with configurable fallback)
	var finalPath string
	var downloadErr error

	maxAttempts := snapCfg.MaxSnapshotURLAttempts
	if maxAttempts <= 0 {
		maxAttempts = len(bestNodes) // 0 means try all available nodes
	}

	for i, nodeRPC := range bestNodes {
		// Check for cancellation
		if ctx.Err() != nil {
			return "", 0, 0, ctx.Err()
		}

		if i >= maxAttempts {
			break
		}

		if i > 0 {
			mlog.Log.Infof("Trying fallback source #%d: %s", i+1, rpcclient.SanitizeEndpointForDisplay(nodeRPC))
		}

		mlog.Log.Infof("Downloading from: %s (rank #%d)", rpcclient.SanitizeEndpointForDisplay(nodeRPC), i+1)
		downloadStart := time.Now()
		finalPath, downloadErr = snapshot.DownloadSnapshotWithContext(
			ctx, nodeRPC, cfg, "snapshot-", referenceSlot, nil)

		if downloadErr == nil && finalPath != "" {
			mlog.Log.Infof("Successfully downloaded snapshot from %s in %s",
				rpcclient.SanitizeEndpointForDisplay(nodeRPC), time.Since(downloadStart))
			break
		}

		// Check if we were cancelled during download
		if ctx.Err() != nil {
			return "", 0, 0, ctx.Err()
		}

		if downloadErr != nil {
			mlog.Log.Infof("Failed to download from %s: %s. Trying next...",
				rpcclient.SanitizeEndpointForDisplay(nodeRPC), rpcclient.SanitizeErrorForDisplay(downloadErr))
			downloadErr = rpcclient.WrapErrorForDisplay(downloadErr)
		} else {
			mlog.Log.Infof("Download from %s returned empty path. Trying next...",
				rpcclient.SanitizeEndpointForDisplay(nodeRPC))
		}
	}

	if finalPath == "" {
		return "", 0, 0, fmt.Errorf("failed to download snapshot from any RPC node (tried %d nodes, last error: %v)", maxAttempts, downloadErr)
	}

	// Step 6: Extract slot from downloaded snapshot
	snapshotSlot, err := snapshot.ExtractFullSnapshotSlot(finalPath)
	if err != nil {
		// Fallback to old parsing method for backward compatibility
		snapshotSlot = extractFullSnapshotSlot(finalPath)
	}

	mlog.Log.Infof("Downloaded full snapshot: slot=%d, path=%s", snapshotSlot, finalPath)
	return finalPath, referenceSlot, snapshotSlot, nil
}

// DownloadIncrementalSnapshot downloads an incremental snapshot that matches
// the given fullSnapshotSlot.
//
// It returns: (downloadPath, baseSlot, endSlot, error)
//
// This function uses the new FindMatchingIncremental API to search across
// ranked nodes for an incremental snapshot that matches the full snapshot.
func DownloadIncrementalSnapshot(rpcEndpoints []string, path string, referenceSlot int, fullSnapshotSlot int) (string, int, int, error) {
	cfg := DefaultSnapshotConfig()
	cfg.RPCAddresses = rpcEndpoints
	return DownloadIncrementalSnapshotWithConfig(path, referenceSlot, fullSnapshotSlot, cfg)
}

// DownloadIncrementalSnapshotWithConfig is like DownloadIncrementalSnapshot but accepts custom config
func DownloadIncrementalSnapshotWithConfig(path string, referenceSlot int, fullSnapshotSlot int, snapCfg SnapshotConfig) (string, int, int, error) {
	defer suppressStdlogOutput()()
	cfg := snapCfg.toInternalConfig(path)
	blacklist := newSnapshotNodeBlacklist(snapCfg.NodeBlacklist)
	ctx := context.Background()

	mlog.Log.Infof("Searching for incremental snapshot matching full slot %d...", fullSnapshotSlot)

	// Step 1: Get cluster nodes (use first RPC as preferred)
	preferredRPC := ""
	if len(cfg.RPCAddresses) > 0 {
		preferredRPC = cfg.RPCAddresses[0]
	}
	nodes := rpc.FetchClusterNodes(cfg, preferredRPC)
	if len(nodes) == 0 {
		return "", 0, 0, fmt.Errorf("no rpc nodes available from cluster")
	}
	nodes, configuredAdded := addConfiguredSnapshotRPCNodes(nodes, preferredRPC, cfg.RPCAddresses)
	if configuredAdded > 0 {
		mlog.Log.Infof("Added %d configured RPC endpoint(s) to snapshot probe candidates", configuredAdded)
	}
	var skipped int
	nodes, skipped = filterSnapshotRPCNodes(nodes, blacklist)
	logSnapshotBlacklist("discovered", skipped, len(nodes))
	if len(nodes) == 0 {
		return "", 0, 0, fmt.Errorf("all discovered rpc nodes were excluded by snapshot.node_blacklist")
	}

	// Step 2: Evaluate nodes
	results, stats := rpc.EvaluateNodesWithVersionsAndStats(nodes, cfg, referenceSlot)
	results, skipped = filterSnapshotNodeResults(results, blacklist)
	logSnapshotBlacklist("evaluated", skipped, len(results))

	if snapCfg.Verbose && stats != nil {
		filterCfg := rpc.FilterConfig{
			MaxRTTMs:        cfg.MaxRTTMs,
			FullThreshold:   cfg.FullThreshold,
			IncThreshold:    cfg.IncrementalThreshold,
			MinVersion:      cfg.MinNodeVersion,
			AllowedVersions: cfg.AllowedNodeVersions,
		}
		stats.PrintReport(filterCfg)
	}

	// Step 3: Filter to only nodes with matching incremental base slot FIRST
	// This prevents threshold filtering from discarding nodes we need
	var baseMatchingResults []rpc.NodeResult
	for _, r := range results {
		if r.HasInc && r.IncBase == int64(fullSnapshotSlot) {
			baseMatchingResults = append(baseMatchingResults, r)
		}
	}

	mlog.Log.Infof("Incremental base matching: %d nodes have base slot %d", len(baseMatchingResults), fullSnapshotSlot)

	if len(baseMatchingResults) == 0 {
		return "", 0, 0, fmt.Errorf("no nodes found with incremental base slot %d", fullSnapshotSlot)
	}

	// Step 3.5: Apply threshold filtering with two-pass approach
	bestNodes, rankedNodes := rpc.SortBestRPCsFilteredBySlot(
		baseMatchingResults, cfg, stats, int64(fullSnapshotSlot), referenceSlot)

	// Pass 2: If no matches, relax incremental threshold
	if len(bestNodes) == 0 && cfg.IncrementalThreshold > 0 {
		relaxedThreshold := cfg.IncrementalThreshold * 2
		mlog.Log.Infof("Pass 1 found no matches within threshold %d, trying relaxed threshold %d...",
			cfg.IncrementalThreshold, relaxedThreshold)

		relaxedCfg := cfg
		relaxedCfg.IncrementalThreshold = relaxedThreshold
		bestNodes, rankedNodes = rpc.SortBestRPCsFilteredBySlot(
			baseMatchingResults, relaxedCfg, nil, int64(fullSnapshotSlot), referenceSlot)
	}

	if len(bestNodes) == 0 {
		return "", 0, 0, fmt.Errorf("no nodes found with snapshots >= slot %d within threshold", fullSnapshotSlot)
	}

	mlog.Log.Infof("Found %d nodes with matching incremental", len(bestNodes))

	// Step 4: Use the new FindMatchingIncremental API
	// This searches ranked nodes for an incremental that matches fullSnapshotSlot
	mlog.Log.Infof("Searching for matching incremental snapshot...")
	incrInfo, err := rpc.FindMatchingIncremental(rankedNodes, int64(fullSnapshotSlot), 5000)
	if err != nil {
		return "", 0, 0, fmt.Errorf("failed to find matching incremental: %w", err)
	}

	if incrInfo == nil {
		return "", 0, 0, fmt.Errorf("no matching incremental snapshot found for base slot %d", fullSnapshotSlot)
	}

	mlog.Log.Infof("Found matching incremental on %s: base=%d, end=%d",
		rpcclient.SanitizeEndpointForDisplay(incrInfo.NodeRPC), incrInfo.BaseSlot, incrInfo.EndSlot)

	// Step 5: Download the incremental snapshot
	downloadStart := time.Now()
	finalPath, err := snapshot.DownloadSnapshotWithContext(
		ctx, incrInfo.NodeRPC, cfg, "incremental", referenceSlot, nil)

	if err != nil {
		return "", 0, 0, fmt.Errorf("failed to download incremental from %s: %w",
			rpcclient.SanitizeEndpointForDisplay(incrInfo.NodeRPC), rpcclient.WrapErrorForDisplay(err))
	}

	mlog.Log.Infof("Downloaded incremental snapshot from %s in %s",
		rpcclient.SanitizeEndpointForDisplay(incrInfo.NodeRPC), time.Since(downloadStart))

	// Step 6: Verify the downloaded snapshot matches expected slots
	baseSlot, endSlot, err := snapshot.ExtractIncrementalSnapshotSlots(finalPath)
	if err != nil {
		// Fallback to old parsing
		baseSlot, endSlot = ExtractIncrementalSnapshotSlots(finalPath)
	}

	if baseSlot != int(incrInfo.BaseSlot) {
		mlog.Log.Infof("WARNING: Downloaded incremental has base slot %d, expected %d", baseSlot, incrInfo.BaseSlot)
	}

	mlog.Log.Infof("Downloaded incremental snapshot: base=%d, end=%d, path=%s", baseSlot, endSlot, finalPath)
	return finalPath, baseSlot, endSlot, nil
}

// ExtractIncrementalSnapshotSlots extracts the base and end slots from an
// incremental snapshot filename.
//
// Expected format: incremental-snapshot-{baseSlot}-{endSlot}-{hash}.tar.zst
// Returns: (baseSlot, endSlot)
func ExtractIncrementalSnapshotSlots(path string) (int, int) {
	parts := strings.Split(path, "/")
	filename := parts[len(parts)-1]

	parts = strings.Split(filename, "-")
	if len(parts) < 4 {
		panic(fmt.Sprintf("invalid incremental snapshot filename format: %s", path))
	}

	slotStr := parts[2]
	slot, err := strconv.Atoi(slotStr)
	if err != nil {
		panic(fmt.Sprintf("failed to parse base slot from %s: %v", path, err))
	}

	incrSlotStr := parts[3]
	incrSlot, err := strconv.Atoi(incrSlotStr)
	if err != nil {
		panic(fmt.Sprintf("failed to parse end slot from %s: %v", path, err))
	}

	return slot, incrSlot
}

// extractFullSnapshotSlot extracts the slot from a full snapshot filename.
//
// Expected format: snapshot-{slot}-{hash}.tar.zst
// Returns: slot number
//
// This is kept for backward compatibility but now also uses the new API
func extractFullSnapshotSlot(path string) int {
	parts := strings.Split(path, "/")
	filename := parts[len(parts)-1]

	parts = strings.Split(filename, "-")
	if len(parts) < 3 {
		panic(fmt.Sprintf("invalid full snapshot filename format: %s", path))
	}

	slotStr := parts[1]
	slot, err := strconv.Atoi(slotStr)
	if err != nil {
		panic(fmt.Sprintf("failed to parse slot from %s: %v", path, err))
	}

	return slot
}

const (
	// incrementalFreshnessBandSlots defines "near-tie" for incremental
	// recency: surveyed candidates within this many slots of the freshest
	// end found compete on download speed; anything staler is dropped.
	// Incrementals are small downloads, so freshness dominates speed —
	// ~256 slots is a couple of snapshot-production intervals.
	incrementalFreshnessBandSlots = int64(256)

	// incrementalSameSourceFreshSlots bounds the same-source shortcut: the
	// full-snapshot source's own incremental is taken WITHOUT a cluster
	// survey only when it ends within this many slots of the live tip.
	// Staler than this (even inside the acceptance threshold), a survey is
	// worth the extra seconds — another node likely has fresher.
	incrementalSameSourceFreshSlots = int64(512)
)

// restrictToFreshestIncrementals keeps only candidates whose incremental end
// is within band slots of the freshest end present, so the subsequent speed
// ranking chooses among near-equally-fresh options instead of trading
// recency for bandwidth.
func restrictToFreshestIncrementals(results []rpc.NodeResult, band int64) []rpc.NodeResult {
	var bestEnd int64
	for _, r := range results {
		if r.IncSlot > bestEnd {
			bestEnd = r.IncSlot
		}
	}
	if bestEnd == 0 {
		return results
	}
	var fresh []rpc.NodeResult
	for _, r := range results {
		if bestEnd-r.IncSlot <= band {
			fresh = append(fresh, r)
		}
	}
	if len(fresh) == 0 {
		return results
	}
	if len(fresh) < len(results) {
		mlog.Log.Infof("Incremental selection: %d/%d candidates within %d slots of the freshest end %d; speed ranks among those",
			len(fresh), len(results), band, bestEnd)
	}
	return fresh
}

// GetIncrementalSnapshotURL finds an incremental snapshot URL that matches the full snapshot.
// It first tries the same source as the full snapshot, then searches other nodes if needed.
// Returns: (httpURL, baseSlot, endSlot, error)
//
// Fallback strategy (different from full snapshot):
//  1. Try the same node that provided the full snapshot (fastest, most likely match)
//  2. If that fails, find ALL nodes with matching base slot (more flexible than full snapshot)
//  3. Among matching nodes, prioritize by:
//     a. Freshness (highest end slot = most recent incremental)
//     b. Speed (faster downloads preferred when end slots are equal)
//  4. Try multiple candidates for resilience (uses MaxSnapshotURLAttempts)
func GetIncrementalSnapshotURL(fullSnapshotURL string, referenceSlot int, fullSnapshotSlot int, snapCfg SnapshotConfig) (string, int, int, error) {
	defer suppressStdlogOutput()()
	cfg := snapCfg.toInternalConfig("")
	blacklist := newSnapshotNodeBlacklist(snapCfg.NodeBlacklist)
	ctx := context.Background()

	// Extract the source node RPC from the full snapshot URL
	// Example: "http://node.example.com:8899/snapshot-123-abc.tar.zst" -> "http://node.example.com:8899"
	var sourceNodeRPC string
	if strings.HasPrefix(fullSnapshotURL, "http://") || strings.HasPrefix(fullSnapshotURL, "https://") {
		parts := strings.Split(fullSnapshotURL, "/")
		if len(parts) >= 3 {
			sourceNodeRPC = strings.Join(parts[:3], "/") // protocol + // + host:port
		}
	}

	// Step 1: Try to get incremental from the same source as the full snapshot
	if sourceNodeRPC != "" && blacklist.contains(sourceNodeRPC) {
		mlog.Log.Infof("Skipping same-source incremental check because %s is in snapshot.node_blacklist",
			rpcclient.SanitizeEndpointForDisplay(sourceNodeRPC))
	} else if sourceNodeRPC != "" {
		mlog.Log.FileOnlyf("Checking same source for incremental (base slot %d): %s",
			fullSnapshotSlot, rpcclient.SanitizeEndpointForDisplay(sourceNodeRPC))
		urlInfo, err := snapshot.GetSnapshotURL(ctx, sourceNodeRPC, "incremental")

		if err == nil && urlInfo != nil && urlInfo.BaseSlot == fullSnapshotSlot {
			// The shortcut only applies when the same-source incremental is
			// near-tip fresh; otherwise a cluster survey is worth the extra
			// seconds — incrementals are small, freshness beats convenience.
			age := int64(referenceSlot) - int64(urlInfo.Slot)
			if referenceSlot > 0 && age > incrementalSameSourceFreshSlots {
				mlog.Log.Infof("Same source incremental is %d slots behind tip (shortcut bar: %d). Surveying cluster for a fresher one...",
					age, incrementalSameSourceFreshSlots)
			} else {
				mlog.Log.Infof("📸 Incremental snapshot source: %s (same as full, base=%d, end=%d, age=%d slots)",
					rpcclient.SanitizeEndpointForDisplay(sourceNodeRPC), urlInfo.BaseSlot, urlInfo.Slot, age)
				return urlInfo.URL, urlInfo.BaseSlot, urlInfo.Slot, nil
			}
		} else {
			mlog.Log.Infof("Same source unavailable for incremental. Searching cluster...")
		}
	}

	// Step 2: Fallback - search all nodes for matching incremental
	// For incrementals, we need to be more flexible: find ANY node with matching base slot,
	// then prefer fresher incrementals (higher end slot) with reasonable speed
	mlog.Log.Infof("Searching cluster for incremental snapshot matching full slot %d...", fullSnapshotSlot)

	// Get cluster nodes (use first RPC as preferred)
	preferredRPC := ""
	if len(cfg.RPCAddresses) > 0 {
		preferredRPC = cfg.RPCAddresses[0]
	}
	nodes := rpc.FetchClusterNodes(cfg, preferredRPC)
	if len(nodes) == 0 {
		return "", 0, 0, fmt.Errorf("no rpc nodes available from cluster")
	}
	nodes, configuredAdded := addConfiguredSnapshotRPCNodes(nodes, preferredRPC, cfg.RPCAddresses)
	if configuredAdded > 0 {
		mlog.Log.Infof("Added %d configured RPC endpoint(s) to snapshot probe candidates", configuredAdded)
	}
	var skipped int
	nodes, skipped = filterSnapshotRPCNodes(nodes, blacklist)
	logSnapshotBlacklist("discovered", skipped, len(nodes))
	if len(nodes) == 0 {
		return "", 0, 0, fmt.Errorf("all discovered rpc nodes were excluded by snapshot.node_blacklist")
	}

	// Evaluate nodes
	results, stats := rpc.EvaluateNodesWithVersionsAndStats(nodes, cfg, referenceSlot)
	results, skipped = filterSnapshotNodeResults(results, blacklist)
	logSnapshotBlacklist("evaluated", skipped, len(results))

	if snapCfg.Verbose && stats != nil {
		filterCfg := rpc.FilterConfig{
			MaxRTTMs:        cfg.MaxRTTMs,
			FullThreshold:   cfg.FullThreshold,
			IncThreshold:    cfg.IncrementalThreshold,
			MinVersion:      cfg.MinNodeVersion,
			AllowedVersions: cfg.AllowedNodeVersions,
		}
		stats.PrintReport(filterCfg)
	}

	// Step 2.1: Filter to only nodes with matching base slot FIRST (before threshold filtering)
	// This prevents threshold filtering from discarding nodes we need
	var baseMatchingResults []rpc.NodeResult
	totalWithMatchingBase := 0
	for _, r := range results {
		if r.HasInc && r.IncBase == int64(fullSnapshotSlot) {
			baseMatchingResults = append(baseMatchingResults, r)
			totalWithMatchingBase++
		}
	}

	mlog.Log.Infof("Incremental base matching: %d nodes have base slot %d", totalWithMatchingBase, fullSnapshotSlot)

	if len(baseMatchingResults) == 0 {
		return "", 0, 0, fmt.Errorf("no nodes found with incremental base slot %d", fullSnapshotSlot)
	}

	// Recency first: keep only candidates near the freshest available end,
	// THEN let speed rank them. Applies to both threshold passes below.
	baseMatchingResults = restrictToFreshestIncrementals(baseMatchingResults, incrementalFreshnessBandSlots)

	// Step 2.2: Two-pass filtering - try strict threshold first, then relaxed
	var matchingNodes []rpc.RankedNode

	// Pass 1: Apply normal incremental threshold
	_, rankedNodes := rpc.SortBestRPCsFilteredBySlot(
		baseMatchingResults, cfg, stats, int64(fullSnapshotSlot), referenceSlot)
	for _, node := range rankedNodes {
		matchingNodes = append(matchingNodes, node)
	}

	// Pass 2: If no matches found, relax incremental threshold and retry
	if len(matchingNodes) == 0 && cfg.IncrementalThreshold > 0 {
		relaxedThreshold := cfg.IncrementalThreshold * 2
		mlog.Log.Infof("Pass 1 found no matches within threshold %d, trying relaxed threshold %d...",
			cfg.IncrementalThreshold, relaxedThreshold)

		// Create a config with relaxed threshold
		relaxedCfg := cfg
		relaxedCfg.IncrementalThreshold = relaxedThreshold

		_, rankedNodesRelaxed := rpc.SortBestRPCsFilteredBySlot(
			baseMatchingResults, relaxedCfg, nil, int64(fullSnapshotSlot), referenceSlot)
		for _, node := range rankedNodesRelaxed {
			matchingNodes = append(matchingNodes, node)
		}

		if len(matchingNodes) > 0 {
			mlog.Log.Infof("Pass 2 (relaxed threshold) found %d matching nodes", len(matchingNodes))
		}
	}

	if len(matchingNodes) == 0 {
		return "", 0, 0, fmt.Errorf("no nodes found with incremental base slot %d within threshold (strict: %d, relaxed: %d)",
			fullSnapshotSlot, cfg.IncrementalThreshold, cfg.IncrementalThreshold*2)
	}

	// Filter out nodes that are too slow (incrementals are ~1GB, don't want 15min downloads)
	if snapCfg.MinIncrementalSpeedMBs > 0 {
		var fastEnoughNodes []rpc.RankedNode
		for _, node := range matchingNodes {
			if node.S1.MedianMBs >= snapCfg.MinIncrementalSpeedMBs {
				fastEnoughNodes = append(fastEnoughNodes, node)
			}
		}
		if len(fastEnoughNodes) > 0 {
			matchingNodes = fastEnoughNodes
			if snapCfg.Verbose {
				mlog.Log.Infof("Filtered to %d nodes with speed >= %.1f MB/s",
					len(matchingNodes), snapCfg.MinIncrementalSpeedMBs)
			}
		} else {
			// No nodes meet speed requirement, continue with all matching nodes
			if snapCfg.Verbose {
				mlog.Log.Infof("No nodes meet minimum speed %.1f MB/s, using all %d matching nodes",
					snapCfg.MinIncrementalSpeedMBs, len(matchingNodes))
			}
		}
	}

	// Sort by end slot descending (freshest first), then by speed
	// Note: This sorts in-place
	for i := 0; i < len(matchingNodes)-1; i++ {
		for j := i + 1; j < len(matchingNodes); j++ {
			// Sort by end slot descending
			if matchingNodes[j].Result.IncSlot > matchingNodes[i].Result.IncSlot {
				matchingNodes[i], matchingNodes[j] = matchingNodes[j], matchingNodes[i]
			} else if matchingNodes[j].Result.IncSlot == matchingNodes[i].Result.IncSlot {
				// If same end slot, prefer faster node
				if matchingNodes[j].S1.MedianMBs > matchingNodes[i].S1.MedianMBs {
					matchingNodes[i], matchingNodes[j] = matchingNodes[j], matchingNodes[i]
				}
			}
		}
	}

	mlog.Log.Infof("Found %d nodes with matching base slot %d", len(matchingNodes), fullSnapshotSlot)
	if snapCfg.Verbose && len(matchingNodes) > 0 {
		mlog.Log.Infof("Top candidates with matching base:")
		for i := 0; i < min(3, len(matchingNodes)); i++ {
			node := matchingNodes[i]
			mlog.Log.Infof("  #%d: %s (end slot: %d, %.1f MB/s)",
				i+1, rpcclient.SanitizeEndpointForDisplay(node.Result.RPC), node.Result.IncSlot, node.S1.MedianMBs)
		}
	}

	// Try multiple candidates with matching base slot for resilience
	maxAttempts := snapCfg.MaxSnapshotURLAttempts
	if maxAttempts <= 0 {
		maxAttempts = len(matchingNodes) // Try all if configured
	}

	var incrURL string
	var incrBaseSlot, incrEndSlot int
	var selectedNode string
	var urlErr error

	for i := 0; i < min(maxAttempts, len(matchingNodes)); i++ {
		node := matchingNodes[i]

		if i > 0 {
			mlog.Log.Infof("Trying fallback incremental source #%d: %s (end slot: %d)",
				i+1, rpcclient.SanitizeEndpointForDisplay(node.Result.RPC), node.Result.IncSlot)
		}

		if snapCfg.Verbose {
			mlog.Log.Infof("Getting incremental URL from %s (base=%d, end=%d)",
				rpcclient.SanitizeEndpointForDisplay(node.Result.RPC), node.Result.IncBase, node.Result.IncSlot)
		}

		urlInfo, err := snapshot.GetSnapshotURL(ctx, node.Result.RPC, "incremental")
		if err == nil && urlInfo != nil {
			// Verify base slot still matches (could have changed since evaluation)
			if urlInfo.BaseSlot == fullSnapshotSlot {
				incrURL = urlInfo.URL
				incrBaseSlot = urlInfo.BaseSlot
				incrEndSlot = urlInfo.Slot
				selectedNode = node.Result.RPC
				break
			}
			if snapCfg.Verbose {
				mlog.Log.Infof("Base slot mismatch: got %d, need %d. Trying next...",
					urlInfo.BaseSlot, fullSnapshotSlot)
			}
		} else {
			if snapCfg.Verbose {
				mlog.Log.Infof("Failed to get URL from %s: %s. Trying next...",
					rpcclient.SanitizeEndpointForDisplay(node.Result.RPC), rpcclient.SanitizeErrorForDisplay(err))
			}
			urlErr = rpcclient.WrapErrorForDisplay(err)
		}
	}

	if incrURL == "" {
		return "", 0, 0, fmt.Errorf("failed to get incremental URL from any matching node (tried %d nodes, last error: %v)",
			min(maxAttempts, len(matchingNodes)), urlErr)
	}

	// Always log the selected source
	mlog.Log.Infof("📸 Incremental snapshot source: %s (base=%d, end=%d)",
		rpcclient.SanitizeEndpointForDisplay(selectedNode), incrBaseSlot, incrEndSlot)
	return incrURL, incrBaseSlot, incrEndSlot, nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
