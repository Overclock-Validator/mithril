package mcp

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	corestate "github.com/Overclock-Validator/mithril/pkg/state"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// Finding is one diagnostic observation.
type Finding struct {
	Severity string `json:"severity"` // critical | high | medium | low | info
	Category string `json:"category"`
	Message  string `json:"message"`
}

const (
	diagnosticHealthy  = "healthy"
	diagnosticDegraded = "degraded"
	diagnosticCritical = "critical"
	diagnosticUnknown  = "unknown"

	checkOK       = "ok"
	checkDegraded = "degraded"
	checkCritical = "critical"
	checkUnknown  = "unknown"
	checkSkipped  = "skipped"

	diagnoseReplayCaveat = "phase sum is not wall-clock latency; complete fields do not prove each phase ran, and asynchronous timing can be assigned to another slot"
	turbineFreshness     = 60 * time.Second // Six normal ten-second publisher intervals.
	maxUnixTimestamp     = uint64(1<<63 - 1)
)

// DiagnosticCheck records one health source and its evidence.
type DiagnosticCheck struct {
	Name     string `json:"name"`
	Status   string `json:"status"` // ok | degraded | critical | unknown | skipped
	Evidence string `json:"evidence"`
}

type diagnoseInput struct {
	IncludeLogs        *bool `json:"include_logs,omitempty" jsonschema:"include recent error-log scan (default true)"`
	IncludeReplayTrend *bool `json:"include_replay_trend,omitempty" jsonschema:"include replay-timing trend (default true)"`
}

type diagnoseOutput struct {
	Status              string                      `json:"status"` // healthy | degraded | critical | unknown
	AssessmentScope     string                      `json:"assessment_scope"`
	ObservedAt          string                      `json:"observed_at"`
	SafeForAutomation   bool                        `json:"safe_for_automation"`
	EvidenceComplete    bool                        `json:"evidence_complete"`
	Checks              []DiagnosticCheck           `json:"checks"`
	Findings            []Finding                   `json:"findings"`
	MetricsSnapshot     *MetricsSummary             `json:"metrics_snapshot"`
	RPCSnapshot         *SlotInfo                   `json:"rpc_snapshot"`
	ShutdownState       *ShutdownStateSummary       `json:"shutdown_state"`
	RecentErrors        []LogLine                   `json:"recent_errors"`
	ReplayStats         *ReplayStats                `json:"replay_stats"`
	ReplayMeta          *ReplayMeta                 `json:"replay_meta"`
	SlotsBehind         *SlotComparison             `json:"slots_behind"`
	DivergenceArtifacts []DivergenceArtifactSummary `json:"divergence_artifacts"`
	HostHealth          *hostHealthOutput           `json:"host_health"`
}

// mergeDiagnosticStatus applies the fail-closed overall precedence:
// critical > unknown > degraded > healthy. A missing configured signal therefore
// cannot be hidden by otherwise healthy checks.
func mergeDiagnosticStatus(current, checkStatus string) string {
	switch checkStatus {
	case checkCritical:
		return diagnosticCritical
	case checkUnknown:
		if current != diagnosticCritical {
			return diagnosticUnknown
		}
	case checkDegraded:
		if current == diagnosticHealthy {
			return diagnosticDegraded
		}
	}
	return current
}

func findingSeverity(checkStatus string) string {
	switch checkStatus {
	case checkCritical:
		return "critical"
	case checkDegraded:
		return "high"
	case checkUnknown:
		return "medium"
	default:
		return "info"
	}
}

func diagnoseReplayEvidence(stats ReplayStats, meta ReplayMeta, warnMs float64) (string, string) {
	switch {
	case meta.SourceChangedDuringScan:
		return checkUnknown, "replay timing source changed while it was read; phase-sum trend cannot be assessed safely"
	case meta.IncompleteTail:
		return checkUnknown, "newest replay JSONL record is incomplete; phase-sum trend cannot be assessed safely"
	case !stats.FieldFound || stats.Count == 0:
		return checkUnknown, "block_total replay timing data is absent"
	case stats.ShapeIncompleteCount > 0:
		return checkUnknown, fmt.Sprintf("%d replay samples lack the complete block_total field shape", stats.ShapeIncompleteCount)
	case stats.Count < minReplayHealthSamples:
		return checkUnknown, fmt.Sprintf("only %d block_total samples with complete field shape are available; need at least %d", stats.Count, minReplayHealthSamples)
	case warnMs <= 0 || math.IsNaN(warnMs) || math.IsInf(warnMs, 0):
		return checkUnknown, fmt.Sprintf("replay p99 threshold is invalid: %v", warnMs)
	case stats.P99Ms > warnMs:
		evidence := fmt.Sprintf("replay p99 %.1fms across %d complete records exceeds %.1fms; %s", stats.P99Ms, stats.Count, warnMs, diagnoseReplayCaveat)
		if meta.ParseErrors > 0 {
			evidence += fmt.Sprintf("; %d lines were unparseable", meta.ParseErrors)
		}
		return checkDegraded, evidence
	case meta.ParseErrors > 0:
		return checkDegraded, fmt.Sprintf("replay p99 %.1fms; %d lines were unparseable; %s", stats.P99Ms, meta.ParseErrors, diagnoseReplayCaveat)
	default:
		return checkOK, fmt.Sprintf("replay p99 %.1fms across %d complete records; %s", stats.P99Ms, stats.Count, diagnoseReplayCaveat)
	}
}

func incompleteLogScanEvidence(scan LogScanMeta) string {
	return fmt.Sprintf(
		"scan incomplete (omitted_prefix_bytes=%d oversized_lines=%d unparsed_lines=%d incomparable_since_lines=%d incomplete_tail=%v source_changed_during_scan=%v)",
		scan.OmittedPrefixBytes, scan.OversizedLines, scan.UnparsedLines, scan.IncomparableSinceLines, scan.IncompleteTail, scan.SourceChangedDuringScan,
	)
}

func diagnoseTurbineEvidence(summary *MetricsSummary, now time.Time) (string, string) {
	if summary == nil || summary.TurbineReceiverActive == nil {
		return checkUnknown, "native Turbine receiver metrics are absent"
	}
	if !*summary.TurbineReceiverActive {
		return checkDegraded, "native Turbine receiver was not active at this scrape"
	}
	if summary.TurbineLastPacketTimestampSeconds == nil || *summary.TurbineLastPacketTimestampSeconds == 0 {
		return checkUnknown, "native Turbine receiver is active but has no packet activity yet"
	}
	if *summary.TurbineLastPacketTimestampSeconds > maxUnixTimestamp {
		return checkUnknown, "native Turbine last-packet timestamp is invalid"
	}

	packetAt := time.Unix(int64(*summary.TurbineLastPacketTimestampSeconds), 0)
	if packetAt.After(now) {
		return checkUnknown, "native Turbine last-packet timestamp is in the future"
	}
	packetAge := now.Sub(packetAt)
	if packetAge > turbineFreshness {
		return checkDegraded, fmt.Sprintf("native Turbine receiver is active but its last packet is %s old", packetAge.Round(time.Second))
	}

	if summary.TurbineLastBlockTimestampSeconds == nil || *summary.TurbineLastBlockTimestampSeconds == 0 {
		return checkUnknown, "native Turbine packets are fresh but no assembled block has been emitted yet"
	}
	if *summary.TurbineLastBlockTimestampSeconds > maxUnixTimestamp {
		return checkUnknown, "native Turbine last-block timestamp is invalid"
	}
	blockAt := time.Unix(int64(*summary.TurbineLastBlockTimestampSeconds), 0)
	if blockAt.After(now) {
		return checkUnknown, "native Turbine last-block timestamp is in the future"
	}
	blockAge := now.Sub(blockAt)
	if blockAge > turbineFreshness {
		return checkDegraded, fmt.Sprintf("native Turbine packets are fresh but its last assembled block is %s old", blockAge.Round(time.Second))
	}
	if summary.TurbineLastDataSlot == nil || summary.TurbineLastBlockSlot == nil {
		return checkUnknown, "native Turbine activity timestamps are fresh but last-observed slot metrics are absent"
	}

	return checkOK, fmt.Sprintf(
		"native Turbine point-in-time activity observed at this scrape: last packet %s ago; latest parsed data-shred slot %d (before leader/signature checks); last assembled block %s ago at slot %d; this does not prove sustained continuity or consensus correctness",
		packetAge.Round(time.Second), *summary.TurbineLastDataSlot, blockAge.Round(time.Second), *summary.TurbineLastBlockSlot,
	)
}

func stateEvidenceSlotSpan(earliest, latest *uint64) string {
	if earliest == nil || latest == nil {
		return "slot range unavailable"
	}
	if *earliest == *latest {
		return fmt.Sprintf("slot %d", *earliest)
	}
	return fmt.Sprintf("slots %d-%d", *earliest, *latest)
}

func diagnoseStateEvidence(summary *ShutdownStateSummary) (string, string) {
	if summary == nil || !summary.SchemaSupported {
		return checkUnknown, "persisted safety evidence cannot be evaluated without a supported state schema"
	}
	if summary.AlpenglowEvidence == nil || summary.ReplayDivergenceEvidence == nil {
		return checkUnknown, "persisted safety evidence summary is unavailable"
	}
	alpenglow := summary.AlpenglowEvidence
	replay := summary.ReplayDivergenceEvidence
	if alpenglow.Count == 0 && replay.Count == 0 {
		return checkOK, "state contains no unresolved Alpenglow finality or replay-divergence evidence"
	}

	var evidence []string
	if alpenglow.Count > 0 {
		evidence = append(evidence, fmt.Sprintf(
			"Alpenglow finality evidence is present: count=%d, %s, conflicts=%d; mismatches require an exact finality match before promotion and conflicts require operator triage",
			alpenglow.Count, stateEvidenceSlotSpan(alpenglow.EarliestSlot, alpenglow.LatestSlot), alpenglow.ConflictCount,
		))
	}
	if replay.Count > 0 {
		evidence = append(evidence, fmt.Sprintf(
			"replay-divergence evidence is present: count=%d, %s; folds are blocked at or after the earliest recorded slot until operator triage",
			replay.Count, stateEvidenceSlotSpan(replay.EarliestSlot, replay.LatestSlot),
		))
	}
	return checkCritical, strings.Join(evidence, "; ")
}

type diagnoseHostCollector func(context.Context, Config, *MetricsSummary, *ShutdownStateSummary) hostHealthOutput

func runDiagnosisWithHostCollector(ctx context.Context, cfg Config, in diagnoseInput, collectHost diagnoseHostCollector) diagnoseOutput {
	includeLogs := in.IncludeLogs == nil || *in.IncludeLogs
	includeReplay := in.IncludeReplayTrend == nil || *in.IncludeReplayTrend
	observedAt := time.Now().UTC()

	out := diagnoseOutput{
		Status:              diagnosticHealthy,
		AssessmentScope:     "point_in_time_snapshot",
		ObservedAt:          observedAt.Format(time.RFC3339Nano),
		SafeForAutomation:   false,
		EvidenceComplete:    true,
		Checks:              []DiagnosticCheck{},
		Findings:            []Finding{},
		RecentErrors:        []LogLine{},
		DivergenceArtifacts: []DivergenceArtifactSummary{},
	}
	addCheck := func(name, status, evidence string) {
		out.Checks = append(out.Checks, DiagnosticCheck{Name: name, Status: status, Evidence: evidence})
		out.Status = mergeDiagnosticStatus(out.Status, status)
		if status == checkUnknown {
			out.EvidenceComplete = false
		}
		if status != checkOK && status != checkSkipped {
			out.Findings = append(out.Findings, Finding{
				Severity: findingSeverity(status),
				Category: name,
				Message:  evidence,
			})
		}
	}

	// Metrics provide a point-in-time node snapshot. The bounded replay JSONL
	// window below supplies the recent latency trend.
	if cfg.MetricsURL == "" {
		addCheck("metrics", checkUnknown, "metrics endpoint is not configured")
	} else if body, err := fetchMetricsText(ctx, cfg.MetricsURL); err != nil {
		addCheck("metrics", checkUnknown, fmt.Sprintf("cannot reach configured metrics endpoint: %v", err))
	} else if summary, err := parseMetrics(body); err != nil {
		addCheck("metrics", checkUnknown, fmt.Sprintf("failed to parse configured metrics endpoint: %v", err))
	} else {
		out.MetricsSnapshot = summary
		if summary.Slot == nil {
			addCheck("metrics", checkUnknown, "slot metric is absent")
		} else {
			addCheck("metrics", checkOK, fmt.Sprintf("metrics reachable at slot %d", *summary.Slot))
		}
	}

	if strings.ToLower(strings.TrimSpace(cfg.BlockSource)) != "turbine" {
		addCheck("turbine_receiver", checkSkipped, "configured block source is not native Turbine")
	} else {
		status, evidence := diagnoseTurbineEvidence(out.MetricsSnapshot, time.Now().UTC())
		addCheck("turbine_receiver", status, evidence)
	}

	// Probe Mithril RPC directly even when no external reference RPC is configured.
	if cfg.RPCURL == "" {
		addCheck("rpc", checkUnknown, "Mithril RPC endpoint is not configured")
	} else if client, err := newMithrilRPCClient(cfg.RPCURL); err != nil {
		addCheck("rpc", checkUnknown, fmt.Sprintf("Mithril RPC client initialization failed: %v", err))
	} else if info, err := client.getSlotInfo(ctx); err != nil {
		addCheck("rpc", checkUnknown, fmt.Sprintf("Mithril RPC getEpochInfo failed: %v", err))
	} else {
		out.RPCSnapshot = &info
		addCheck("rpc", checkOK, fmt.Sprintf("Mithril RPC reachable at slot %d, epoch %d, block height %d", info.AbsoluteSlot, info.Epoch, info.BlockHeight))
	}

	// State stage and the previous shutdown reason are separate checks: a ready
	// database can still carry an abnormal shutdown that requires review.
	if cfg.StatePath == "" {
		addCheck("state", checkSkipped, "state path is not configured")
		addCheck("state_evidence", checkSkipped, "persisted safety evidence is unavailable because state is not configured")
		addCheck("shutdown", checkSkipped, "shutdown history unavailable because state is not configured")
	} else if state, err := readShutdownStateContext(ctx, cfg.StatePath); err != nil {
		addCheck("state", checkUnknown, fmt.Sprintf("configured state file is unavailable: %v", err))
		addCheck("state_evidence", checkUnknown, "persisted safety evidence cannot be evaluated without readable state")
		addCheck("shutdown", checkUnknown, "shutdown history cannot be evaluated without readable state")
	} else if state == nil {
		addCheck("state", checkUnknown, "configured state file does not exist")
		addCheck("state_evidence", checkUnknown, "persisted safety evidence cannot be evaluated because state is missing")
		addCheck("shutdown", checkUnknown, "shutdown history cannot be evaluated because state is missing")
	} else {
		out.ShutdownState = summarizeShutdownState(state)
		if state.StateSchemaVersion == nil || *state.StateSchemaVersion != corestate.CurrentStateSchemaVersion {
			got := "missing"
			if state.StateSchemaVersion != nil {
				got = strconv.FormatUint(uint64(*state.StateSchemaVersion), 10)
			}
			addCheck("state", checkCritical, fmt.Sprintf("state schema version is %s; this Mithril build requires v%d", got, corestate.CurrentStateSchemaVersion))
		} else {
			stage := ""
			if out.ShutdownState.Stage != nil {
				stage = strings.ToLower(strings.TrimSpace(*out.ShutdownState.Stage))
			}
			switch stage {
			case "ready":
				addCheck("state", checkOK, "state stage is ready")
			case "building", "downloading":
				addCheck("state", checkDegraded, fmt.Sprintf("state stage is %s; node is not ready", stage))
			case "corrupted":
				evidence := "state stage is corrupted"
				if out.ShutdownState.CorruptionReason != nil && strings.TrimSpace(*out.ShutdownState.CorruptionReason) != "" {
					evidence += ": " + *out.ShutdownState.CorruptionReason
				}
				addCheck("state", checkCritical, evidence)
			case "":
				addCheck("state", checkUnknown, "state stage is missing")
			default:
				addCheck("state", checkUnknown, fmt.Sprintf("state stage %q is unrecognized", stage))
			}
		}
		evidenceStatus, evidence := diagnoseStateEvidence(out.ShutdownState)
		addCheck("state_evidence", evidenceStatus, evidence)

		safeShutdownReason := ""
		if out.ShutdownState.LastShutdownReason != nil {
			safeShutdownReason = *out.ShutdownState.LastShutdownReason
		}
		if state.LastShutdownReason == nil || strings.TrimSpace(*state.LastShutdownReason) == "" {
			addCheck("shutdown", checkOK, "no previous shutdown reason is recorded")
		} else if reason, ok := state.parsedShutdownReason(); !ok {
			addCheck("shutdown", checkUnknown, fmt.Sprintf("last shutdown reason is unrecognized: %q", safeShutdownReason))
		} else {
			switch reason {
			case shutdownNormal:
				addCheck("shutdown", checkOK, "last shutdown was graceful")
			case shutdownCompleted:
				addCheck("shutdown", checkOK, "last run completed its configured replay range")
			case shutdownStall:
				addCheck("shutdown", checkDegraded, fmt.Sprintf("last shutdown followed a block-fetch stall: %s", safeShutdownReason))
			case shutdownLeaderSchedule:
				addCheck("shutdown", checkDegraded, fmt.Sprintf("last shutdown followed a leader-schedule failure: %s", safeShutdownReason))
			case shutdownError:
				addCheck("shutdown", checkCritical, fmt.Sprintf("last shutdown was a replay error: %s", safeShutdownReason))
			}
		}
	}

	errorLevel := LevelError
	if !includeLogs {
		addCheck("logs", checkSkipped, "recent error-log scan disabled by request")
	} else if cfg.LogDir == "" {
		addCheck("logs", checkSkipped, "log directory is not configured")
	} else if lines, total, scan, err := tailLogContextWithMeta(ctx, cfg.LogDir, 20, &errorLevel, ""); err != nil {
		addCheck("logs", checkUnknown, fmt.Sprintf("configured log source is unavailable: %v", err))
	} else {
		out.RecentErrors = lines
		if !scan.Complete {
			out.EvidenceComplete = false
		}
		evidence := fmt.Sprintf("scanned log window contains %d error-level entries; returning the newest %d; no degradation is inferred without a reliable wall-clock recency window", total, len(lines))
		if !scan.Complete {
			evidence += "; " + incompleteLogScanEvidence(scan)
		}
		addCheck("logs", checkOK, evidence)
	}

	if cfg.LogDir == "" {
		addCheck("divergence_artifacts", checkSkipped, "log directory is not configured")
	} else if info, err := os.Stat(cfg.LogDir); err != nil {
		addCheck("divergence_artifacts", checkUnknown, fmt.Sprintf("configured divergence source is unavailable: %v", err))
	} else if !info.IsDir() {
		addCheck("divergence_artifacts", checkUnknown, "configured log path is not a directory")
	} else if artifacts, meta, err := readDivergenceArtifactsContext(ctx, cfg.LogDir); err != nil {
		addCheck("divergence_artifacts", checkUnknown, fmt.Sprintf("failed to read divergence artifacts: %v", err))
	} else {
		out.DivergenceArtifacts = summarizeDivergenceArtifacts(artifacts)
		incomplete := meta.Invalid + meta.Oversized + meta.Unreadable
		if incomplete > 0 || meta.Truncated {
			out.EvidenceComplete = false
		}
		if len(artifacts) == 0 && (incomplete > 0 || meta.Truncated) {
			addCheck("divergence_artifacts", checkUnknown, fmt.Sprintf("divergence artifact scan is incomplete: invalid=%d oversized=%d unreadable=%d truncated=%v", meta.Invalid, meta.Oversized, meta.Unreadable, meta.Truncated))
		} else if len(artifacts) == 0 {
			addCheck("divergence_artifacts", checkOK, "no legacy bank-hash mismatch artifacts found; persisted state evidence is checked separately, and this does not prove consensus verification ran or the node is healthy")
		} else {
			var slots []string
			for _, artifact := range artifacts {
				if artifact.CheckedSlot != nil {
					slots = append(slots, strconv.FormatUint(*artifact.CheckedSlot, 10))
				}
			}
			slotText := "unknown"
			if len(slots) > 0 {
				slotText = strings.Join(slots, ", ")
			}
			addCheck("divergence_artifacts", checkCritical, fmt.Sprintf("%d bank-hash divergence artifact(s) present (slots: %s); %s", len(artifacts), slotText, divergenceRecoveryGuidance))
		}
	}

	if !includeReplay {
		addCheck("replay", checkSkipped, "replay timing check disabled by request")
	} else if cfg.ReplayPath == "" {
		addCheck("replay", checkSkipped, "replay timing path is not configured")
	} else {
		n := 100
		_, stats, meta, err := readReplayTimingsContext(ctx, cfg.ReplayPath, nil, nil, &n, timingBlockTotal)
		if err != nil {
			addCheck("replay", checkUnknown, fmt.Sprintf("configured replay timing source is unavailable: %v", err))
		} else {
			out.ReplayStats = &stats
			out.ReplayMeta = &meta
			if meta.ParseErrors > 0 || meta.IncompleteTail || meta.SourceChangedDuringScan {
				out.EvidenceComplete = false
			}
			status, evidence := diagnoseReplayEvidence(stats, meta, cfg.ReplayP99WarnMs)
			addCheck("replay", status, evidence)
		}
	}

	if cfg.ReferenceRPCURL == "" {
		addCheck("cross_check", checkSkipped, "reference RPC is not configured")
	} else if comparison, err := slotsBehindCheck(ctx, cfg, cfg.ReferenceRPCURL, defaultCommitment); err != nil {
		out.EvidenceComplete = false
		addCheck("cross_check", checkUnknown, fmt.Sprintf("configured reference RPC cross-check is unavailable: %v", err))
	} else {
		out.SlotsBehind = &comparison
		slotsAhead := uint64(0)
		if comparison.MithrilSlot > comparison.ReferenceSlot {
			slotsAhead = comparison.MithrilSlot - comparison.ReferenceSlot
		}
		switch {
		case comparison.Status == "behind":
			addCheck("cross_check", checkDegraded, fmt.Sprintf("node is %d slots behind reference (threshold %d)", comparison.SlotsBehind, comparison.Threshold))
		case comparison.Status == "ahead" && slotsAhead >= 10:
			addCheck("cross_check", checkDegraded, fmt.Sprintf("node is %d slots ahead of reference; reference may be stale or commitments differ", slotsAhead))
		case comparison.Status == "in_sync", comparison.Status == "ahead":
			addCheck("cross_check", checkOK, fmt.Sprintf("slot cross-check status is %s (%d slots behind)", comparison.Status, comparison.SlotsBehind))
		default:
			addCheck("cross_check", checkUnknown, fmt.Sprintf("slot cross-check returned unrecognized status %q", comparison.Status))
		}
	}

	host := collectHost(ctx, cfg, out.MetricsSnapshot, out.ShutdownState)
	out.HostHealth = &host
	if !host.EvidenceComplete {
		out.EvidenceComplete = false
	}
	hostStatus := checkUnknown
	switch host.Status {
	case diagnosticHealthy:
		hostStatus = checkOK
	case diagnosticDegraded:
		hostStatus = checkDegraded
	case diagnosticCritical:
		hostStatus = checkCritical
	}
	addCheck("host", hostStatus, fmt.Sprintf("host health is %s across %d disk, memory, cgroup, and bootstrap checks; inspect host_health for evidence", host.Status, len(host.Checks)))

	if len(out.Findings) == 0 {
		out.Findings = append(out.Findings, Finding{
			Severity: "info",
			Category: "overall",
			Message:  "available checks passed; this snapshot does not prove sustained consensus activity, slot progress, voting, or rewards",
		})
	}
	return out
}

func registerDiagnoseTool(server *mcpsdk.Server, cfg Config) {
	registerDiagnoseToolWithHostCollector(server, cfg, collectHostHealth)
}

func registerDiagnoseToolWithHostCollector(server *mcpsdk.Server, cfg Config, collectHost diagnoseHostCollector) {
	addTool(server, cfg, &mcpsdk.Tool{
		Name:        "mithril_diagnose",
		Annotations: annReadOnlyNetwork,
		Description: "Assess configured health sources and return evidence plus healthy, degraded, critical, or unknown. safe_for_automation is false because this snapshot does not prove sustained consensus activity, slot progress, voting, or rewards.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, in diagnoseInput) (*mcpsdk.CallToolResult, diagnoseOutput, error) {
		return nil, runDiagnosisWithHostCollector(ctx, cfg, in, collectHost), nil
	})
}
