package mcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/progress"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	maxCgroupScalarBytes = 128
	maxCgroupEventsBytes = 4 * 1024
	maxCgroupEventPairs  = 64
	cgroupMemoryWarnPct  = 90.0
)

type hostHealthInput struct{}

type hostHealthOutput struct {
	Status            string              `json:"status"`
	AssessmentScope   string              `json:"assessment_scope"`
	ObservedAt        string              `json:"observed_at"`
	SafeForAutomation bool                `json:"safe_for_automation"`
	EvidenceComplete  bool                `json:"evidence_complete"`
	Checks            []DiagnosticCheck   `json:"checks"`
	Storage           []hostStorageHealth `json:"storage"`
	Memory            hostMemoryHealth    `json:"memory"`
	Bootstrap         hostBootstrapHealth `json:"bootstrap"`
}

type hostStorageHealth struct {
	Name           string   `json:"name"`
	Path           string   `json:"path,omitempty"`
	Configured     bool     `json:"configured"`
	Status         string   `json:"status"`
	TotalBytes     *uint64  `json:"total_bytes,omitempty"`
	AvailableBytes *uint64  `json:"available_bytes,omitempty"`
	UsedPercent    *float64 `json:"used_percent,omitempty"`
}

type hostMemoryHealth struct {
	NodeRSSBytes     *uint64          `json:"node_rss_bytes,omitempty"`
	NodeVirtualBytes *uint64          `json:"node_virtual_bytes,omitempty"`
	Cgroup           hostCgroupMemory `json:"cgroup"`
}

type hostCgroupMemory struct {
	Configured          bool     `json:"configured"`
	Available           bool     `json:"available"`
	Status              string   `json:"status"`
	CurrentBytes        *uint64  `json:"current_bytes,omitempty"`
	PeakBytes           *uint64  `json:"peak_bytes,omitempty"`
	LimitBytes          *uint64  `json:"limit_bytes,omitempty"`
	LimitConfigured     bool     `json:"limit_configured"`
	UsagePercentOfLimit *float64 `json:"usage_percent_of_limit,omitempty"`
	HighEventsTotal     *uint64  `json:"high_events_total,omitempty"`
	MaxEventsTotal      *uint64  `json:"max_events_total,omitempty"`
	OOMEventsTotal      *uint64  `json:"oom_events_total,omitempty"`
	OOMKillsTotal       *uint64  `json:"oom_kills_total,omitempty"`
	OOMGroupKillsTotal  *uint64  `json:"oom_group_kills_total,omitempty"`
	EventScope          string   `json:"event_scope,omitempty"`
	CountersCumulative  bool     `json:"counters_cumulative"`
}

type hostBootstrapHealth struct {
	Assessment                    string                                `json:"assessment"`
	StateFound                    bool                                  `json:"state_found"`
	Stage                         *string                               `json:"stage,omitempty"`
	BuildMode                     *string                               `json:"build_mode,omitempty"`
	SnapshotSlot                  *uint64                               `json:"snapshot_slot,omitempty"`
	SnapshotEpoch                 *uint64                               `json:"snapshot_epoch,omitempty"`
	BuildStartedAt                *string                               `json:"build_started_at,omitempty"`
	BuildCompletedAt              *string                               `json:"build_completed_at,omitempty"`
	Active                        *bool                                 `json:"active,omitempty"`
	StartedTimestampSeconds       *uint64                               `json:"started_timestamp_seconds,omitempty"`
	ActiveDurationSeconds         *uint64                               `json:"active_duration_seconds,omitempty"`
	SnapshotTarBytesRead          *uint64                               `json:"snapshot_tar_bytes_read,omitempty"`
	SnapshotWorkerPoolUtilization *SnapshotWorkerPoolUtilizationSummary `json:"snapshot_worker_pool_utilization,omitempty"`
}

type hostDiskProbe func(string) *progress.DiskInfo

// collectHostHealth composes already-parsed node evidence with local host
// probes. Diagnose can call it with its snapshots instead of repeating I/O.
func collectHostHealth(ctx context.Context, cfg Config, metrics *MetricsSummary, state *ShutdownStateSummary) hostHealthOutput {
	return collectHostHealthWith(ctx, cfg.normalized(), metrics, state, progress.GetDiskInfo, time.Now().UTC())
}

func collectHostHealthWith(ctx context.Context, cfg Config, metrics *MetricsSummary, state *ShutdownStateSummary, diskProbe hostDiskProbe, now time.Time) hostHealthOutput {
	out := hostHealthOutput{
		Status:            diagnosticHealthy,
		AssessmentScope:   "point_in_time_host_snapshot",
		ObservedAt:        now.UTC().Format(time.RFC3339Nano),
		SafeForAutomation: false,
		EvidenceComplete:  true,
		Checks:            []DiagnosticCheck{},
		Storage:           []hostStorageHealth{},
	}
	addCheck := func(name, status, evidence string) {
		out.Checks = append(out.Checks, DiagnosticCheck{Name: name, Status: status, Evidence: evidence})
		out.Status = mergeDiagnosticStatus(out.Status, status)
		if status == checkUnknown {
			out.EvidenceComplete = false
		}
	}

	paths := []struct {
		name string
		path string
	}{
		{name: "accounts", path: cfg.AccountsDir},
		{name: "snapshots", path: cfg.SnapshotsDir},
		{name: "shredstore", path: cfg.ShredstoreDir},
		{name: "logs", path: cfg.LogDir},
	}
	for _, target := range paths {
		item, status, evidence := assessStoragePath(ctx, target.name, target.path, cfg.DiskWarnPercent, cfg.DiskCriticalPercent, diskProbe)
		out.Storage = append(out.Storage, item)
		addCheck("disk_"+target.name, status, evidence)
	}

	out.Memory.NodeRSSBytes = nil
	out.Memory.NodeVirtualBytes = nil
	if metrics == nil || metrics.ProcessRSSBytes == nil {
		addCheck("process_memory", checkUnknown, "node RSS is unavailable from the metrics summary")
	} else {
		out.Memory.NodeRSSBytes = metrics.ProcessRSSBytes
		out.Memory.NodeVirtualBytes = metrics.ProcessVirtualBytes
		addCheck("process_memory", checkOK, "node RSS is available from the configured Mithril metrics endpoint")
	}

	cgroup, err := readHostCgroupMemory(ctx, cfg.NodeCgroupPath)
	out.Memory.Cgroup = cgroup
	switch {
	case cfg.NodeCgroupPath == "":
		out.Memory.Cgroup.Status = checkSkipped
		addCheck("cgroup_memory", checkSkipped, "node cgroup path is not configured")
	case err != nil:
		out.Memory.Cgroup.Status = checkUnknown
		addCheck("cgroup_memory", checkUnknown, "configured cgroup-v2 memory evidence is unavailable")
	case cgroup.OOMKillsTotal != nil && *cgroup.OOMKillsTotal > 0:
		out.Memory.Cgroup.Status = checkCritical
		addCheck("cgroup_memory", checkCritical, fmt.Sprintf("node cgroup reports %d cumulative OOM kill event(s)", *cgroup.OOMKillsTotal))
	case cgroup.OOMGroupKillsTotal != nil && *cgroup.OOMGroupKillsTotal > 0:
		out.Memory.Cgroup.Status = checkCritical
		addCheck("cgroup_memory", checkCritical, fmt.Sprintf("node cgroup reports %d cumulative group OOM kill event(s)", *cgroup.OOMGroupKillsTotal))
	case cgroup.OOMEventsTotal != nil && *cgroup.OOMEventsTotal > 0:
		out.Memory.Cgroup.Status = checkDegraded
		addCheck("cgroup_memory", checkDegraded, fmt.Sprintf("node cgroup reports %d cumulative OOM event(s) without a recorded kill", *cgroup.OOMEventsTotal))
	case cgroup.MaxEventsTotal != nil && *cgroup.MaxEventsTotal > 0:
		out.Memory.Cgroup.Status = checkDegraded
		addCheck("cgroup_memory", checkDegraded, fmt.Sprintf("node cgroup reports %d cumulative memory.max event(s)", *cgroup.MaxEventsTotal))
	case cgroup.HighEventsTotal != nil && *cgroup.HighEventsTotal > 0:
		out.Memory.Cgroup.Status = checkDegraded
		addCheck("cgroup_memory", checkDegraded, fmt.Sprintf("node cgroup reports %d cumulative memory.high event(s)", *cgroup.HighEventsTotal))
	case cgroup.UsagePercentOfLimit != nil && *cgroup.UsagePercentOfLimit >= cgroupMemoryWarnPct:
		out.Memory.Cgroup.Status = checkDegraded
		addCheck("cgroup_memory", checkDegraded, fmt.Sprintf("node cgroup memory is %.1f%% of its configured limit (warn at %.1f%%)", *cgroup.UsagePercentOfLimit, cgroupMemoryWarnPct))
	default:
		out.Memory.Cgroup.Status = checkOK
		addCheck("cgroup_memory", checkOK, "configured cgroup-v2 memory and cumulative OOM counters are readable")
	}

	bootstrap, status, evidence := assessHostBootstrap(metrics, state, now)
	out.Bootstrap = bootstrap
	addCheck("bootstrap", status, evidence)
	return out
}

func assessStoragePath(ctx context.Context, name, path string, warnPercent, criticalPercent float64, probe hostDiskProbe) (hostStorageHealth, string, string) {
	out := hostStorageHealth{Name: name, Path: path, Configured: path != "", Status: checkSkipped}
	if path == "" {
		return out, checkSkipped, name + " storage path is not configured"
	}
	if err := ctx.Err(); err != nil {
		out.Status = checkUnknown
		return out, checkUnknown, name + " disk probe was cancelled"
	}
	info := probe(path)
	if info == nil || info.Error != nil || info.TotalBytes == 0 || info.UsedBytes > info.TotalBytes {
		out.Status = checkUnknown
		return out, checkUnknown, name + " storage filesystem is unavailable"
	}
	available := info.TotalBytes - info.UsedBytes
	usedPercent := float64(info.UsedBytes) / float64(info.TotalBytes) * 100
	out.TotalBytes = &info.TotalBytes
	out.AvailableBytes = &available
	out.UsedPercent = &usedPercent
	switch {
	case usedPercent >= criticalPercent:
		out.Status = checkCritical
		return out, checkCritical, fmt.Sprintf("%s storage is %.1f%% used (critical at %.1f%%)", name, usedPercent, criticalPercent)
	case usedPercent >= warnPercent:
		out.Status = checkDegraded
		return out, checkDegraded, fmt.Sprintf("%s storage is %.1f%% used (warn at %.1f%%)", name, usedPercent, warnPercent)
	default:
		out.Status = checkOK
		return out, checkOK, fmt.Sprintf("%s storage is %.1f%% used", name, usedPercent)
	}
}

func assessHostBootstrap(metrics *MetricsSummary, state *ShutdownStateSummary, now time.Time) (hostBootstrapHealth, string, string) {
	out := hostBootstrapHealth{StateFound: state != nil, Assessment: "unknown"}
	if state != nil {
		out.Stage = state.Stage
		out.BuildMode = state.BuildMode
		out.SnapshotSlot = state.SnapshotSlot
		out.SnapshotEpoch = state.SnapshotEpoch
		out.BuildStartedAt = state.BuildStartedAt
		out.BuildCompletedAt = state.BuildCompletedAt
	}
	if metrics != nil {
		out.Active = metrics.SnapshotBootstrapActive
		out.StartedTimestampSeconds = metrics.SnapshotBootstrapStartedTimestampSeconds
		out.SnapshotTarBytesRead = metrics.SnapshotTarBytesRead
		out.SnapshotWorkerPoolUtilization = metrics.SnapshotWorkerPoolUtil
	}
	if out.Active != nil && *out.Active && out.StartedTimestampSeconds != nil {
		nowSeconds := now.Unix()
		if nowSeconds >= 0 && *out.StartedTimestampSeconds <= uint64(nowSeconds) {
			duration := uint64(nowSeconds) - *out.StartedTimestampSeconds
			out.ActiveDurationSeconds = &duration
		}
	}

	stage := ""
	if out.Stage != nil {
		stage = strings.ToLower(strings.TrimSpace(*out.Stage))
	}
	active := out.Active != nil && *out.Active
	inactive := out.Active != nil && !*out.Active

	if stage == "corrupted" {
		out.Assessment = "corrupted"
		if active {
			return out, checkCritical, "persisted AccountsDB state is corrupted while snapshot bootstrap is active"
		}
		return out, checkCritical, "persisted AccountsDB state is corrupted"
	}
	if active && out.StartedTimestampSeconds != nil && now.Unix() >= 0 && *out.StartedTimestampSeconds > uint64(now.Unix()) {
		out.Assessment = "conflicting"
		return out, checkUnknown, "bootstrap is active but its start timestamp is in the future"
	}
	if active {
		switch stage {
		case "ready":
			out.Assessment = "conflicting"
			return out, checkUnknown, "active bootstrap metrics conflict with persisted state stage"
		default:
			out.Assessment = "active"
			return out, checkDegraded, "snapshot bootstrap is active; this point-in-time sample does not prove progress"
		}
	}

	switch stage {
	case "ready":
		out.Assessment = "ready"
		return out, checkOK, "persisted AccountsDB state reports ready; no active bootstrap signal conflicts"
	case "building", "downloading":
		if inactive {
			out.Assessment = "conflicting"
			return out, checkUnknown, "persisted bootstrap stage conflicts with an inactive producer metric"
		}
		out.Assessment = "reported_in_progress"
		return out, checkDegraded, "persisted state reports an in-progress bootstrap stage; this does not prove current activity"
	case "":
		if out.SnapshotTarBytesRead != nil {
			out.Assessment = "activity_observed"
			return out, checkUnknown, "snapshot byte activity exists without authoritative active or ready state"
		}
		return out, checkUnknown, "authoritative snapshot bootstrap state is unavailable"
	default:
		return out, checkUnknown, "persisted snapshot bootstrap stage is unrecognized"
	}
}

func readHostCgroupMemory(ctx context.Context, path string) (hostCgroupMemory, error) {
	out := hostCgroupMemory{Configured: path != "", Status: checkSkipped}
	if path == "" {
		return out, nil
	}
	out.Status = checkUnknown
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if !filepath.IsAbs(path) {
		return out, errors.New("cgroup path must be absolute")
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return out, err
	}
	defer root.Close()

	current, err := readCgroupUint(root, "memory.current")
	if err != nil {
		return out, err
	}
	limitData, err := readCgroupFile(root, "memory.max", maxCgroupScalarBytes)
	if err != nil {
		return out, err
	}
	limitText := strings.TrimSpace(string(limitData))
	var limit *uint64
	if limitText != "max" {
		value, err := parseCgroupUint(limitText)
		if err != nil {
			return out, fmt.Errorf("parse memory.max: %w", err)
		}
		limit = &value
	}
	var peak *uint64
	if value, err := readCgroupUint(root, "memory.peak"); err == nil {
		peak = &value
	} else if !errors.Is(err, os.ErrNotExist) {
		return out, err
	}

	eventsData, err := readCgroupFile(root, "memory.events.local", maxCgroupEventsBytes)
	eventScope := "local_cumulative"
	if errors.Is(err, os.ErrNotExist) {
		eventsData, err = readCgroupFile(root, "memory.events", maxCgroupEventsBytes)
		eventScope = "hierarchical_cumulative"
	}
	if err != nil {
		return out, err
	}
	events, err := parseCgroupEvents(eventsData)
	if err != nil {
		return out, err
	}
	oom, oomOK := events["oom"]
	oomKills, killsOK := events["oom_kill"]
	if !oomOK || !killsOK {
		return out, errors.New("cgroup OOM counters are missing")
	}

	out.Available = true
	out.Status = checkOK
	out.CurrentBytes = &current
	out.PeakBytes = peak
	out.LimitBytes = limit
	out.LimitConfigured = limit != nil
	out.EventScope = eventScope
	out.CountersCumulative = true
	out.OOMEventsTotal = &oom
	out.OOMKillsTotal = &oomKills
	if value, ok := events["oom_group_kill"]; ok {
		out.OOMGroupKillsTotal = &value
	}
	if value, ok := events["high"]; ok {
		out.HighEventsTotal = &value
	}
	if value, ok := events["max"]; ok {
		out.MaxEventsTotal = &value
	}
	if limit != nil && *limit > 0 {
		percent := float64(current) / float64(*limit) * 100
		if !math.IsNaN(percent) && !math.IsInf(percent, 0) {
			out.UsagePercentOfLimit = &percent
		}
	}
	if err := ctx.Err(); err != nil {
		return hostCgroupMemory{Configured: true, Status: checkUnknown}, err
	}
	return out, nil
}

func readCgroupUint(root *os.Root, name string) (uint64, error) {
	data, err := readCgroupFile(root, name, maxCgroupScalarBytes)
	if err != nil {
		return 0, err
	}
	value, err := parseCgroupUint(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return value, nil
}

func parseCgroupUint(raw string) (uint64, error) {
	if raw == "" || strings.HasPrefix(raw, "+") || strings.HasPrefix(raw, "-") {
		return 0, errors.New("invalid unsigned integer")
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, errors.New("invalid unsigned integer")
	}
	return value, nil
}

func readCgroupFile(root *os.Root, name string, limit int64) ([]byte, error) {
	f, _, err := openRootRegularFile(root, name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%s exceeds %d-byte limit", name, limit)
	}
	return data, nil
}

func parseCgroupEvents(data []byte) (map[string]uint64, error) {
	fields := strings.Fields(string(data))
	if len(fields)%2 != 0 || len(fields)/2 > maxCgroupEventPairs {
		return nil, errors.New("invalid cgroup events shape")
	}
	events := make(map[string]uint64, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		name := fields[i]
		if _, duplicate := events[name]; duplicate {
			return nil, errors.New("duplicate cgroup event")
		}
		value, err := parseCgroupUint(fields[i+1])
		if err != nil {
			return nil, errors.New("invalid cgroup event counter")
		}
		events[name] = value
	}
	return events, nil
}

func registerHostTools(server *mcpsdk.Server, cfg Config) {
	addTool(server, cfg, &mcpsdk.Tool{
		Name:        "mithril_host_health",
		Annotations: annReadOnlyNetwork,
		Description: "Read point-in-time disk headroom, node RSS, configured cgroup-v2 memory/OOM counters, and snapshot bootstrap evidence. OOM counters are cumulative and bootstrap activity alone does not prove progress.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, _ hostHealthInput) (*mcpsdk.CallToolResult, hostHealthOutput, error) {
		var metrics *MetricsSummary
		if cfg.MetricsURL != "" {
			if body, err := fetchMetricsText(ctx, cfg.MetricsURL); err == nil {
				metrics, _ = parseMetrics(body)
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, hostHealthOutput{}, err
		}

		var stateSummary *ShutdownStateSummary
		if cfg.StatePath != "" {
			if state, err := readShutdownStateContext(ctx, cfg.StatePath); err == nil {
				stateSummary = summarizeShutdownState(state)
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, hostHealthOutput{}, err
		}
		return nil, collectHostHealth(ctx, cfg, metrics, stateSummary), nil
	})
}
