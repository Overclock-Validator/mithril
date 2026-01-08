package snapshotdl

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/config"
	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/rpc"
	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/snapshot"
)

// RankedSource represents a snapshot source with its speed test results
type RankedSource struct {
	URL           string  // HTTP URL for streaming
	Slot          int     // Full snapshot slot
	ReferenceSlot int     // Current network slot (for calculating age)
	NodeIP        string  // IP:port of the node
	NodeRPC       string  // Full RPC URL (http://ip:port)
	Version       string  // Solana version of the node
	SpeedMBs      float64 // Download speed in MB/s from Stage 2 testing
	RTTMs         int     // Round-trip time in milliseconds
	Rank          int     // Rank in the sorted list (1-based)
}

// Age returns how many slots behind the snapshot is from the current tip
func (s *RankedSource) Age() int {
	return s.ReferenceSlot - s.Slot
}

// SourceSelector tracks alternative snapshot sources and allows switching between them
type SourceSelector struct {
	sources       []RankedSource
	currentIndex  int
	mu            sync.Mutex
	switchCh      chan struct{} // signal to switch sources
	closed        bool
	SearchTime    time.Duration // How long Stage 2 took

	// Cached Stage 1 results for incremental source lookup
	// Stage 1 runs a short speed test on all nodes, so we have S1.MedianMBs for sorting.
	// Stage 2 then selects the top nodes from Stage 1 for more accurate testing.
	// For incremental lookup, we use all Stage 1 nodes (not just Stage 2 winners)
	// to have a larger pool with matching base slots.
	allStage1Nodes []rpc.RankedNode // All nodes that passed Stage 1 (with speed data)
	referenceSlot  int              // Network slot at time of search
	incrThreshold  int              // Incremental freshness threshold in slots
}

// IncrementalSource represents an incremental snapshot source
type IncrementalSource struct {
	URL           string  // HTTP URL for streaming
	BaseSlot      int     // Base slot (must match full snapshot)
	EndSlot       int     // End slot of incremental
	ReferenceSlot int     // Network slot at time of search
	NodeIP        string  // IP:port of the node
	NodeRPC       string  // Full RPC URL (http://ip:port)
	Version       string  // Solana version of the node
	SpeedMBs      float64 // Download speed in MB/s from Stage 1/2 testing
	RTTMs         int     // Round-trip time in milliseconds
	Rank          int     // Rank in the sorted list (1-based)
	WithinThresh  bool    // True if within incremental threshold
}

// Age returns how many slots behind the incremental is from the tip
func (s *IncrementalSource) Age() int {
	return s.ReferenceSlot - s.EndSlot
}

// IncrementalSelector tracks incremental snapshot sources and allows switching
type IncrementalSelector struct {
	sources       []IncrementalSource
	currentIndex  int
	mu            sync.Mutex
	switchCh      chan struct{}
	closed        bool
	baseSlot      int // The full snapshot slot these incrementals are based on
}

// NewSourceSelector creates a new source selector with the given ranked sources
func NewSourceSelector(sources []RankedSource) *SourceSelector {
	return &SourceSelector{
		sources:      sources,
		currentIndex: 0,
		switchCh:     make(chan struct{}, 1), // buffered so RequestSwitch doesn't block
	}
}

// Current returns the currently selected source, or nil if exhausted
func (s *SourceSelector) Current() *RankedSource {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.currentIndex >= len(s.sources) {
		return nil
	}
	return &s.sources[s.currentIndex]
}

// CurrentIndex returns the current source index (0-based)
func (s *SourceSelector) CurrentIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentIndex
}

// TotalSources returns the total number of available sources
func (s *SourceSelector) TotalSources() int {
	return len(s.sources)
}

// Next advances to the next source and returns it, or nil if no more sources
func (s *SourceSelector) Next() *RankedSource {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.currentIndex++
	if s.currentIndex >= len(s.sources) {
		return nil
	}
	return &s.sources[s.currentIndex]
}

// HasMore returns true if there are more sources to try
func (s *SourceSelector) HasMore() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentIndex+1 < len(s.sources)
}

// SwitchCh returns a channel that signals when the user requests a source switch
func (s *SourceSelector) SwitchCh() <-chan struct{} {
	return s.switchCh
}

// RequestSwitch signals that the user wants to switch to the next source
func (s *SourceSelector) RequestSwitch() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	// Non-blocking send (buffered channel)
	select {
	case s.switchCh <- struct{}{}:
	default:
		// Already has a pending switch request
	}
}

// Close closes the switch channel (call when download completes)
func (s *SourceSelector) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.switchCh)
	}
}

// GetIncrementalSelector returns an IncrementalSelector for the given base slot
// using cached Stage 1 results. Returns nil if no matching incrementals found.
// This is much faster than doing a full cluster search.
//
// Stage 1 runs a short speed test on all nodes, so we have actual download speed
// data (S1.MedianMBs) for sorting, not just latency estimates.
func (s *SourceSelector) GetIncrementalSelector(ctx context.Context, baseSlot int, verbose bool) *IncrementalSelector {
	if len(s.allStage1Nodes) == 0 {
		return nil
	}

	// Filter for nodes with matching incremental base slot
	var matchingNodes []rpc.RankedNode
	for _, node := range s.allStage1Nodes {
		if node.Result.HasInc && node.Result.IncBase == int64(baseSlot) {
			matchingNodes = append(matchingNodes, node)
		}
	}

	if len(matchingNodes) == 0 {
		mlog.Log.Infof("No cached nodes have incremental with base slot %d", baseSlot)
		return nil
	}

	mlog.Log.Infof("Found %d Stage 1 nodes with incremental base slot %d", len(matchingNodes), baseSlot)

	// Sort by: within threshold first, then by end slot (freshest), then by Stage 1 speed
	for i := 0; i < len(matchingNodes)-1; i++ {
		for j := i + 1; j < len(matchingNodes); j++ {
			iAge := s.referenceSlot - int(matchingNodes[i].Result.IncSlot)
			jAge := s.referenceSlot - int(matchingNodes[j].Result.IncSlot)
			iWithin := s.incrThreshold <= 0 || iAge <= s.incrThreshold
			jWithin := s.incrThreshold <= 0 || jAge <= s.incrThreshold

			// Within threshold sorts first
			if jWithin && !iWithin {
				matchingNodes[i], matchingNodes[j] = matchingNodes[j], matchingNodes[i]
			} else if jWithin == iWithin {
				// Same threshold status: sort by end slot (higher = fresher)
				if matchingNodes[j].Result.IncSlot > matchingNodes[i].Result.IncSlot {
					matchingNodes[i], matchingNodes[j] = matchingNodes[j], matchingNodes[i]
				} else if matchingNodes[j].Result.IncSlot == matchingNodes[i].Result.IncSlot {
					// Same end slot: sort by Stage 1 speed (higher = faster)
					if matchingNodes[j].S1.MedianMBs > matchingNodes[i].S1.MedianMBs {
						matchingNodes[i], matchingNodes[j] = matchingNodes[j], matchingNodes[i]
					}
				}
			}
		}
	}

	// Convert to IncrementalSource list (fetch URLs)
	var sources []IncrementalSource
	maxSources := 8
	if len(matchingNodes) < maxSources {
		maxSources = len(matchingNodes)
	}

	for i := 0; i < maxSources; i++ {
		node := matchingNodes[i]

		// Get incremental URL from this node
		urlInfo, err := snapshot.GetSnapshotURL(ctx, node.Result.RPC, "incremental")
		if err != nil || urlInfo == nil || urlInfo.BaseSlot != baseSlot {
			if verbose {
				mlog.Log.Infof("Skipping %s: failed to get incremental URL or base mismatch: %v", node.Result.RPC, err)
			}
			continue
		}

		age := s.referenceSlot - urlInfo.Slot
		withinThresh := s.incrThreshold <= 0 || age <= s.incrThreshold

		// Extract IP from RPC URL
		nodeIP := node.Result.RPC
		if idx := strings.Index(nodeIP, "://"); idx != -1 {
			nodeIP = nodeIP[idx+3:]
		}

		sources = append(sources, IncrementalSource{
			URL:           urlInfo.URL,
			BaseSlot:      urlInfo.BaseSlot,
			EndSlot:       urlInfo.Slot,
			ReferenceSlot: s.referenceSlot,
			NodeIP:        nodeIP,
			NodeRPC:       node.Result.RPC,
			Version:       node.Result.Version,
			SpeedMBs:      node.S1.MedianMBs, // Use actual Stage 1 speed test result
			RTTMs:         int(node.Result.Latency),
			Rank:          len(sources) + 1,
			WithinThresh:  withinThresh,
		})
	}

	if len(sources) == 0 {
		return nil
	}

	mlog.Log.Infof("Found %d cached incremental sources for base slot %d", len(sources), baseSlot)

	return &IncrementalSelector{
		sources:      sources,
		currentIndex: 0,
		switchCh:     make(chan struct{}, 1),
		baseSlot:     baseSlot,
	}
}

// NewIncrementalSelector creates a new incremental selector
func NewIncrementalSelector(sources []IncrementalSource, baseSlot int) *IncrementalSelector {
	return &IncrementalSelector{
		sources:      sources,
		currentIndex: 0,
		switchCh:     make(chan struct{}, 1),
		baseSlot:     baseSlot,
	}
}

// Current returns the currently selected incremental source, or nil if exhausted
func (s *IncrementalSelector) Current() *IncrementalSource {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.currentIndex >= len(s.sources) {
		return nil
	}
	return &s.sources[s.currentIndex]
}

// CurrentIndex returns the current source index (0-based)
func (s *IncrementalSelector) CurrentIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentIndex
}

// TotalSources returns the total number of available sources
func (s *IncrementalSelector) TotalSources() int {
	return len(s.sources)
}

// BaseSlot returns the full snapshot slot these incrementals are based on
func (s *IncrementalSelector) BaseSlot() int {
	return s.baseSlot
}

// Next advances to the next source and returns it, or nil if no more sources
func (s *IncrementalSelector) Next() *IncrementalSource {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.currentIndex++
	if s.currentIndex >= len(s.sources) {
		return nil
	}
	return &s.sources[s.currentIndex]
}

// HasMore returns true if there are more sources to try
func (s *IncrementalSelector) HasMore() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentIndex+1 < len(s.sources)
}

// SwitchCh returns a channel that signals when the user requests a source switch
func (s *IncrementalSelector) SwitchCh() <-chan struct{} {
	return s.switchCh
}

// RequestSwitch signals that the user wants to switch to the next source
func (s *IncrementalSelector) RequestSwitch() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	select {
	case s.switchCh <- struct{}{}:
	default:
	}
}

// Close closes the switch channel
func (s *IncrementalSelector) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.switchCh)
	}
}

// rankedNodesToSources converts rpc.RankedNode list to RankedSource list.
// This fetches snapshot URLs from each node (the quick HTTP call, not the download).
func rankedNodesToSources(ctx context.Context, rankedNodes []rpc.RankedNode, referenceSlot int, maxSources int, verbose bool) []RankedSource {
	var sources []RankedSource

	for i := 0; i < maxSources && i < len(rankedNodes); i++ {
		rn := rankedNodes[i]

		// Get snapshot URL from this node (quick metadata fetch)
		urlInfo, err := snapshot.GetSnapshotURL(ctx, rn.Result.RPC, "full")
		if err != nil || urlInfo == nil {
			if verbose {
				mlog.Log.Infof("Skipping %s: failed to get snapshot URL: %v", rn.Result.RPC, err)
			}
			continue
		}

		// Use Stage 2 min speed if available, otherwise fall back to Stage 1 median
		speed := rn.S1.MedianMBs
		if rn.S2.MinMBs > 0 {
			speed = rn.S2.MinMBs
		}

		// Extract IP from RPC URL
		nodeIP := rn.Result.RPC
		if idx := strings.Index(nodeIP, "://"); idx != -1 {
			nodeIP = nodeIP[idx+3:]
		}

		sources = append(sources, RankedSource{
			URL:           urlInfo.URL,
			Slot:          urlInfo.Slot,
			ReferenceSlot: referenceSlot,
			NodeIP:        nodeIP,
			NodeRPC:       rn.Result.RPC,
			Version:       rn.Result.Version,
			SpeedMBs:      speed,
			RTTMs:         int(rn.Result.Latency),
			Rank:          len(sources) + 1,
		})
	}

	return sources
}

// GetRankedSnapshotSources discovers and ranks all available snapshot sources.
// Returns a SourceSelector that can be used to switch between sources during download.
// This runs Stage 1 + Stage 2 testing and prints the candidates table.
func GetRankedSnapshotSources(ctx context.Context, snapCfg SnapshotConfig) (*SourceSelector, error) {
	searchStart := time.Now()
	cfg := snapCfg.toInternalConfig("")

	// Step 1: Get reference slot from multiple RPCs for reliability
	referenceSlot, preferredRPC, err := rpc.GetReferenceSlotFromMultiple(cfg.RPCAddresses)
	if err != nil {
		return nil, fmt.Errorf("error getting reference slot: %w", err)
	}
	if snapCfg.Verbose {
		mlog.Log.Infof("Reference slot: %d (from %s)", referenceSlot, preferredRPC)
	}

	// Step 2: Fetch cluster nodes
	nodes := rpc.FetchClusterNodes(cfg, preferredRPC)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no rpc nodes available from cluster")
	}

	// Step 3: Evaluate nodes with version tracking and statistics
	mlog.Log.Infof("Probing %d nodes for snapshot availability...", len(nodes))
	results, stats := rpc.EvaluateNodesWithVersionsAndStats(nodes, cfg, referenceSlot)

	// Step 3.5: Filter to only full snapshots that have matching incrementals somewhere
	results, incBaseStats := filterByIncrementalBaseMatch(results)

	// Print Node Discovery Report (before speed testing)
	if stats != nil {
		stats.PrintNodeDiscoveryReport()
	}

	// Print incremental base match stats
	if incBaseStats.totalWithFull > 0 {
		mlog.Log.Infof("Incremental base matching: %d/%d full snapshots have compatible incrementals (%d unique base slots)",
			incBaseStats.afterIncBaseMatch, incBaseStats.totalWithFull, incBaseStats.matchingFullSlots)
		if incBaseStats.afterIncBaseMatch < incBaseStats.totalWithFull {
			mlog.Log.Infof("  (filtered %d sources with no matching incremental base)",
				incBaseStats.totalWithFull-incBaseStats.afterIncBaseMatch)
		}
	}

	// Step 4: Sort and select best nodes by download speed (Stage 1 + Stage 2)
	mlog.Log.Infof("Testing download speeds (Stage 1 + Stage 2)...")
	_, rankedNodes, speedStats := rpc.SortBestNodesWithStats(results, cfg, stats, referenceSlot)
	if len(rankedNodes) == 0 {
		return nil, fmt.Errorf("no suitable nodes found with snapshots (check incremental base matching)")
	}

	// Print Stage 2 candidates as a table
	maxCandidates := 8
	if len(rankedNodes) < maxCandidates {
		maxCandidates = len(rankedNodes)
	}
	candidates := make([]rpc.RankedNodeInfo, maxCandidates)
	for i := 0; i < maxCandidates; i++ {
		rn := rankedNodes[i]
		candidates[i] = rpc.RankedNodeInfo{
			Rank:    i + 1,
			RPC:     rn.Result.RPC,
			Version: rn.Result.Version,
			RTTMs:   int(rn.Result.Latency),
			SpeedS1: rn.S1.MedianMBs,
			SpeedS2: rn.S2.MinMBs,
		}
	}
	rpc.PrintStage2CandidatesTable(candidates)

	// Print Filter Pipeline with speed test stats
	filterCfg := rpc.FilterConfig{
		MaxRTTMs:        cfg.MaxRTTMs,
		FullThreshold:   cfg.FullThreshold,
		IncThreshold:    cfg.IncrementalThreshold,
		MinVersion:      cfg.MinNodeVersion,
		AllowedVersions: cfg.AllowedNodeVersions,
	}
	if stats != nil {
		stats.PrintFilterPipeline(filterCfg, speedStats)
	}

	// Write detailed speed test log
	searchDuration := time.Since(searchStart)
	if snapCfg.LogDir != "" {
		logPath, err := writeDetailedSpeedTestLog(
			snapCfg.LogDir,
			snapCfg.RunID,
			cfg,
			snapCfg,
			referenceSlot,
			incBaseStats,
			rankedNodes,
			searchDuration,
		)
		if err != nil {
			mlog.Log.Infof("Warning: failed to write speed test log: %v", err)
		} else if logPath != "" {
			mlog.Log.Infof("Detailed speed test log written to: %s", logPath)
		}
	}

	// Step 5: Build RankedSource list from ranked nodes
	// Use all ranked nodes (not limited by MaxSnapshotURLAttempts for switching)
	maxSources := len(rankedNodes)
	if maxSources > 8 {
		maxSources = 8 // Limit to top 8 for URL fetching (matching Stage 2 display)
	}

	sources := rankedNodesToSources(ctx, rankedNodes, referenceSlot, maxSources, snapCfg.Verbose)
	if len(sources) == 0 {
		return nil, fmt.Errorf("failed to get snapshot URL from any ranked node")
	}

	mlog.Log.Infof("Found %d ranked snapshot sources for selection", len(sources))

	selector := NewSourceSelector(sources)
	selector.SearchTime = time.Since(searchStart)
	// Cache all Stage 1 nodes for incremental source lookup
	// rankedNodes contains ALL nodes that passed Stage 1 triage (with S1.MedianMBs speed data)
	// Stage 2 just selects the top ones for more accurate testing, but we keep the full list
	// so we have more candidates with matching incremental base slots
	selector.allStage1Nodes = rankedNodes
	selector.referenceSlot = referenceSlot
	selector.incrThreshold = cfg.IncrementalThreshold
	return selector, nil
}

// writeDetailedSpeedTestLog writes a comprehensive log file with all filtering parameters and results.
// The log is written to logDir/snapshot-search-{runID}-{timestamp}.log
func writeDetailedSpeedTestLog(
	logDir string,
	runID string,
	cfg config.Config,
	snapCfg SnapshotConfig,
	referenceSlot int,
	incBaseStats incBaseMatchStats,
	rankedNodes []rpc.RankedNode,
	searchDuration time.Duration,
) (string, error) {
	if logDir == "" {
		return "", nil // Logging disabled
	}

	// Ensure directory exists
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create log directory: %w", err)
	}

	// Generate filename with timestamp and run ID
	timestamp := time.Now().UTC().Format("2006-01-02_15-04-05")
	var filename string
	if runID != "" {
		filename = fmt.Sprintf("snapshot-search-%s-%s.log", runID, timestamp)
	} else {
		filename = fmt.Sprintf("snapshot-search-%s.log", timestamp)
	}
	logPath := filepath.Join(logDir, filename)

	// Create log file
	file, err := os.Create(logPath)
	if err != nil {
		return "", fmt.Errorf("failed to create log file: %w", err)
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	defer w.Flush()

	// Header
	fmt.Fprintf(w, "================================================================================\n")
	fmt.Fprintf(w, "                    MITHRIL SNAPSHOT SEARCH DETAILED LOG\n")
	fmt.Fprintf(w, "================================================================================\n")
	fmt.Fprintf(w, "Timestamp:      %s UTC\n", time.Now().UTC().Format("2006-01-02 15:04:05"))
	if runID != "" {
		fmt.Fprintf(w, "Run ID:         %s\n", runID)
	}
	fmt.Fprintf(w, "Reference Slot: %d\n", referenceSlot)
	fmt.Fprintf(w, "Search Duration: %s\n", searchDuration.Round(time.Millisecond))
	fmt.Fprintf(w, "\n")

	// Configuration Parameters
	fmt.Fprintf(w, "================================================================================\n")
	fmt.Fprintf(w, "                         CONFIGURATION PARAMETERS\n")
	fmt.Fprintf(w, "================================================================================\n")
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "--- Node Filtering ---\n")
	fmt.Fprintf(w, "  Max RTT:              %d ms\n", cfg.MaxRTTMs)
	fmt.Fprintf(w, "  TCP Timeout:          %d ms\n", cfg.TCPTimeoutMs)
	fmt.Fprintf(w, "  Min Node Version:     %s\n", cfg.MinNodeVersion)
	if len(cfg.AllowedNodeVersions) > 0 {
		fmt.Fprintf(w, "  Allowed Versions:     %v\n", cfg.AllowedNodeVersions)
	} else {
		fmt.Fprintf(w, "  Allowed Versions:     (all >= min version)\n")
	}
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "--- Snapshot Thresholds ---\n")
	fmt.Fprintf(w, "  Full Threshold:       %d slots (~%.1f min)\n", cfg.FullThreshold, float64(cfg.FullThreshold)*0.4/60)
	fmt.Fprintf(w, "  Incremental Thresh:   %d slots (~%.1f sec)\n", cfg.IncrementalThreshold, float64(cfg.IncrementalThreshold)*0.4)
	fmt.Fprintf(w, "  Safety Margin:        %d slots\n", cfg.SafetyMarginSlots)
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "--- Stage 1 (Fast Triage) ---\n")
	fmt.Fprintf(w, "  Warmup:               %d KiB\n", cfg.Stage1WarmKiB)
	fmt.Fprintf(w, "  Window Size:          %d KiB\n", cfg.Stage1WindowKiB)
	fmt.Fprintf(w, "  Windows:              %d (total: %d KiB)\n", cfg.Stage1Windows, cfg.Stage1WindowKiB*int64(cfg.Stage1Windows))
	fmt.Fprintf(w, "  Timeout:              %d ms\n", cfg.Stage1TimeoutMS)
	fmt.Fprintf(w, "  Concurrency:          %d (0=auto)\n", cfg.Stage1Concurrency)
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "--- Stage 2 (Sustained Test) ---\n")
	fmt.Fprintf(w, "  Top K Candidates:     %d\n", cfg.Stage2TopK)
	fmt.Fprintf(w, "  Warmup Duration:      %d sec\n", cfg.Stage2WarmSec)
	fmt.Fprintf(w, "  Measure Duration:     %d sec\n", cfg.Stage2MeasureSec)
	fmt.Fprintf(w, "  Min Ratio:            %.0f%% (collapse threshold)\n", cfg.Stage2MinRatio*100)
	if cfg.Stage2MinAbsMBs > 0 {
		fmt.Fprintf(w, "  Min Absolute Speed:   %.1f MB/s\n", cfg.Stage2MinAbsMBs)
	} else {
		fmt.Fprintf(w, "  Min Absolute Speed:   (disabled)\n")
	}
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "--- Other Settings ---\n")
	fmt.Fprintf(w, "  Worker Count:         %d\n", cfg.WorkerCount)
	fmt.Fprintf(w, "  Max Snapshot Attempts: %d\n", snapCfg.MaxSnapshotURLAttempts)
	fmt.Fprintf(w, "  Min Incr Speed:       %.1f MB/s\n", snapCfg.MinIncrementalSpeedMBs)
	fmt.Fprintf(w, "\n")

	// Incremental Base Matching Statistics
	fmt.Fprintf(w, "================================================================================\n")
	fmt.Fprintf(w, "                    INCREMENTAL BASE MATCHING STATISTICS\n")
	fmt.Fprintf(w, "================================================================================\n")
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "--- Incremental Base Matching ---\n")
	fmt.Fprintf(w, "  Nodes with full snapshot:     %d\n", incBaseStats.totalWithFull)
	fmt.Fprintf(w, "  Nodes with any incremental:   %d\n", incBaseStats.totalWithInc)
	fmt.Fprintf(w, "  Unique full snapshot slots:   %d\n", incBaseStats.uniqueFullSlots)
	fmt.Fprintf(w, "  Unique incremental bases:     %d\n", incBaseStats.uniqueIncBases)
	fmt.Fprintf(w, "  Full slots with matching inc: %d\n", incBaseStats.matchingFullSlots)
	fmt.Fprintf(w, "  Nodes after base matching:    %d\n", incBaseStats.afterIncBaseMatch)
	fmt.Fprintf(w, "\n")

	// Full Snapshot Slots Distribution
	fmt.Fprintf(w, "================================================================================\n")
	fmt.Fprintf(w, "                      FULL SNAPSHOT SLOTS DISTRIBUTION\n")
	fmt.Fprintf(w, "================================================================================\n")
	fmt.Fprintf(w, "\n")

	// Count nodes by full snapshot slot
	slotCounts := make(map[int64]int)
	for _, node := range rankedNodes {
		if node.Result.FullSlot > 0 {
			slotCounts[node.Result.FullSlot]++
		}
	}

	// Sort slots by count (descending), then by slot (descending)
	type slotCount struct {
		slot  int64
		count int
	}
	var sortedSlots []slotCount
	for slot, count := range slotCounts {
		sortedSlots = append(sortedSlots, slotCount{slot, count})
	}
	sort.Slice(sortedSlots, func(i, j int) bool {
		if sortedSlots[i].count != sortedSlots[j].count {
			return sortedSlots[i].count > sortedSlots[j].count
		}
		return sortedSlots[i].slot > sortedSlots[j].slot
	})

	fmt.Fprintf(w, "Slot              Age (slots)   Nodes   Notes\n")
	fmt.Fprintf(w, "-----------------------------------------------------\n")
	for _, sc := range sortedSlots {
		age := referenceSlot - int(sc.slot)
		notes := ""
		if age > cfg.FullThreshold {
			notes = "(outside threshold)"
		}
		fmt.Fprintf(w, "%-16d  %10d   %5d   %s\n", sc.slot, age, sc.count, notes)
	}
	fmt.Fprintf(w, "\n")

	// Stage 2 Ranked Nodes
	fmt.Fprintf(w, "================================================================================\n")
	fmt.Fprintf(w, "                        STAGE 2 RANKED NODES (TOP %d)\n", len(rankedNodes))
	fmt.Fprintf(w, "================================================================================\n")
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "Rank  RPC                               Version      RTT    S1 MB/s  S2 MB/s  Full Slot\n")
	fmt.Fprintf(w, "--------------------------------------------------------------------------------------------\n")

	maxShow := 50
	if len(rankedNodes) < maxShow {
		maxShow = len(rankedNodes)
	}
	for i := 0; i < maxShow; i++ {
		node := rankedNodes[i]
		// Extract IP from RPC URL
		nodeIP := node.Result.RPC
		if idx := strings.Index(nodeIP, "://"); idx != -1 {
			nodeIP = nodeIP[idx+3:]
		}
		// Truncate if too long
		if len(nodeIP) > 30 {
			nodeIP = nodeIP[:27] + "..."
		}

		s2Speed := "-"
		if node.S2.MinMBs > 0 {
			s2Speed = fmt.Sprintf("%.1f", node.S2.MinMBs)
		}

		fmt.Fprintf(w, "%4d  %-30s  %-10s  %4dms  %7.1f  %7s  %d\n",
			i+1,
			nodeIP,
			node.Result.Version,
			int(node.Result.Latency),
			node.S1.MedianMBs,
			s2Speed,
			node.Result.FullSlot,
		)
	}
	if len(rankedNodes) > maxShow {
		fmt.Fprintf(w, "... and %d more nodes\n", len(rankedNodes)-maxShow)
	}
	fmt.Fprintf(w, "\n")

	// Footer
	fmt.Fprintf(w, "================================================================================\n")
	fmt.Fprintf(w, "                               END OF LOG\n")
	fmt.Fprintf(w, "================================================================================\n")

	return logPath, nil
}
