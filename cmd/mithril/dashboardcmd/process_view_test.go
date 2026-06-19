package dashboardcmd

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/procctl"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/charmbracelet/lipgloss"
	"github.com/stretchr/testify/assert"
)

// Dashboard "Process" view rendering tests.

// Pre-fetch state shows a loading message, not a blank screen.
func TestRenderProcessView_LoadingState(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		// proc.detection deliberately nil — pre-fetch state.
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Loading process status")
}

// Alive mithril shows the Running badge and PID.
func TestRenderProcessView_RunningShowsBadgeAndPid(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{detection: &procctl.Detection{
			Status:     procctl.StatusRunning,
			Pid:        12345,
			RunID:      "20260519-143209Z_abc1234_def56789",
			SpawnedBy:  "cli",
			BinaryPath: "/home/operator/mithril/mithril",
		}},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Running", "should show Running badge")
	assert.Contains(t, out, "12345", "should show PID")
	assert.Contains(t, out, "20260519-143209Z_abc1234_def56789", "should show RunID")
	// "cli" is translated to "command line"; "Started by cli" is the untranslated form.
	assert.Contains(t, out, "command line", "should show friendly SpawnedBy")
	assert.NotContains(t, out, "Started by cli", "raw 'cli' should be translated")
	assert.Contains(t, out, "/home/operator/mithril/mithril", "should show BinaryPath")
}

// Clean exit mentions the shutdown reason and reassures the operator.
func TestRenderProcessView_StoppedShowsCleanExit(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{detection: &procctl.Detection{
			Status:             procctl.StatusStopped,
			LastShutdownReason: "graceful shutdown (Ctrl+C)",
			LastCleanExit:      true,
		}},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Stopped")
	assert.Contains(t, out, "graceful shutdown")
	assert.Contains(t, out, "Safe to start")
}

func TestRenderProcessView_CompletedShowsCleanCompletion(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{detection: &procctl.Detection{
			Status:             procctl.StatusStopped,
			LastShutdownReason: state.ShutdownReasonCompleted,
			LastCleanExit:      true,
		}},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Completed")
	assert.Contains(t, out, "completed configured replay range")
	assert.Contains(t, out, "not a crash")
	assert.NotContains(t, out, "○ Stopped")
}

// First-run case (no prior run) must not show stale data.
func TestRenderProcessView_StoppedWithoutHistory(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{detection: &procctl.Detection{
			Status: procctl.StatusStopped,
			// No LastShutdownReason / LastCleanExit
		}},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Stopped")
	assert.Contains(t, out, "never been started", "should explain in plain English")
}

func TestRenderProcessView_StartFailureAppearsBeforeStoppedGuidance(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{
			detection: &procctl.Detection{Status: procctl.StatusStopped},
			startFailStderr: strings.Join([]string{
				"lightbringer: gRPC slot stream ready at 127.0.0.1:3401",
				"mode=accountsdb requires existing AccountsDB",
			}, "\n"),
		},
	}
	out := m.renderProcessView()
	failIdx := strings.Index(out, "Start failed")
	guidanceIdx := strings.Index(out, "never been started")
	assert.NotEqual(t, -1, failIdx, "start failure should render")
	assert.NotEqual(t, -1, guidanceIdx, "stopped guidance should still render")
	assert.Less(t, failIdx, guidanceIdx,
		"start failure must be visible before generic stopped guidance")
}

func TestRenderProcessView_RedactsStartFailureStderr(t *testing.T) {
	secretURL := "https://rpc.example.invalid/?api-key=test-key-00000000-0000-4000-8000-000000000000"
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{
			detection:       &procctl.Detection{Status: procctl.StatusStopped},
			startFailStderr: "startup failed while using " + secretURL,
		},
	}

	out := m.renderProcessView()
	assert.Contains(t, out, "api-key=REDACTED")
	assert.NotContains(t, out, "test-key-00000000")
}

func TestRenderProcessView_StartingHeadlineOverridesRunningDetection(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{
			inFlightOp: opStart,
			detection: &procctl.Detection{
				Status: procctl.StatusRunning,
				Pid:    12345,
			},
		},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Starting")
	assert.NotContains(t, out, "● Running",
		"start in-flight should not imply the startup has completed")
}

func TestRenderProcessView_ShowsStructuredProgress(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		progress: []progressEvent{
			{
				TS:      time.Now().Add(-2 * time.Minute),
				Phase:   "bootstrap_snapshot",
				Status:  "running",
				Message: "Downloading snapshot and building AccountsDB",
				Fields:  map[string]any{"slot": float64(12345)},
			},
			{
				TS:      time.Now().Add(-30 * time.Second),
				Phase:   "replay_starting",
				Status:  "running",
				Message: "Starting block replay",
				Fields:  map[string]any{"start_slot": float64(12346)},
			},
		},
		proc: procState{detection: &procctl.Detection{
			Status: procctl.StatusRunning,
			Pid:    12345,
		}},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Recent activity", "activity feed renders under a header")
	assert.Contains(t, out, "Starting block replay")
	assert.Contains(t, out, "Downloading snapshot and building AccountsDB")
	assert.Contains(t, out, "from 12346")
}

func TestRenderProcessView_ShowsSnapshotActivityDuringBootstrap(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		progress: []progressEvent{
			{
				TS:      time.Now().Add(-30 * time.Second),
				Phase:   "bootstrap_snapshot",
				Status:  "running",
				Message: "Downloading snapshot and building AccountsDB",
			},
		},
		snapshot: snapshotActivity{
			Name:    "snapshot-12345-test.tar.zst.partial",
			Path:    "/snapshots/snapshot-12345-test.tar.zst.partial",
			Bytes:   56 * 1024 * 1024 * 1024,
			Partial: true,
			ModTime: time.Now().Add(-3 * time.Second),
		},
		proc: procState{detection: &procctl.Detection{
			Status: procctl.StatusRunning,
			Pid:    12345,
		}},
	}

	out := m.renderProcessView()

	assert.Contains(t, out, "Snapshot file downloading")
	assert.Contains(t, out, "56.0 GB")
	assert.Contains(t, out, "building AccountsDB")
}

func TestRenderProcessView_ShowsAccountsActivityWithoutSnapshotDownload(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		progress: []progressEvent{
			{
				TS:      time.Now().Add(-30 * time.Second),
				Phase:   "bootstrap_snapshot",
				Status:  "running",
				Message: "Building AccountsDB from existing snapshot",
			},
		},
		accounts: accountsActivity{
			Name:    "accounts",
			Path:    "/accounts/accounts",
			ModTime: time.Now().Add(-4 * time.Second),
		},
		proc: procState{detection: &procctl.Detection{
			Status: procctl.StatusRunning,
			Pid:    12345,
		}},
	}

	out := m.renderProcessView()

	assert.Contains(t, out, "AccountsDB updated")
	assert.Contains(t, out, "building AccountsDB")
}

func TestRenderProcessView_WarnsWhenDiskUsageIsHigh(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		disks: []diskUsage{
			{label: "accounts", path: "/mnt/accounts", used: 417, total: 469, pct: 89},
		},
		proc: procState{detection: &procctl.Detection{
			Status: procctl.StatusRunning,
			Pid:    12345,
		}},
	}

	out := m.renderProcessView()

	assert.Contains(t, out, "Disk watch")
	assert.Contains(t, out, "accounts")
	assert.Contains(t, out, "89% used")
	assert.Contains(t, out, "52G free")
	assert.Contains(t, out, "stop safely")
}

func TestRenderProcessView_ShowsRPCServerExposure(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		cfg: &configData{
			cluster:     "mainnet-beta",
			rpcPort:     "8899",
			blockSource: "rpc",
		},
		proc: procState{detection: &procctl.Detection{
			Status: procctl.StatusStopped,
		}},
	}

	// Idle screen stays clean; the firewall caveat lives on the Confirm-run card.
	assert.NotContains(t, m.renderProcessView(), "Run plan")
	confirm := m.renderStartFlow()
	assert.Contains(t, confirm, "all interfaces")
	assert.Contains(t, confirm, ":8899")
	assert.Contains(t, confirm, "firewall")
}

// Crashed surfaces a clear warning plus the Doctor next step.
func TestRenderProcessView_CrashedSurfacesWarning(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{detection: &procctl.Detection{
			Status:             procctl.StatusCrashed,
			LastShutdownReason: "session crashed (no shutdown recorded after start)",
		}},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Crashed", "should surface Crashed badge")
	assert.Contains(t, out, "stopped unexpectedly", "should describe what happened in plain English")
	assert.Contains(t, out, "Doctor", "should point at the Doctor view")
}

func TestRenderProcessView_CrashedKeepsActionsBeforeProgressHistory(t *testing.T) {
	m := model{
		hasConfig:  true,
		screen:     screenProcess,
		runFocused: true,
		proc: procState{detection: &procctl.Detection{
			Status:             procctl.StatusCrashed,
			LastShutdownReason: "snapshot bootstrap stopped",
		}},
		progress: []progressEvent{
			{
				TS:      time.Now().Add(-30 * time.Second),
				Phase:   "error",
				Status:  "error",
				Message: "failed to build AccountsDB from snapshot: context canceled",
			},
		},
	}

	out := m.renderProcessView()
	actionIdx := strings.Index(out, "Choose action")
	errorIdx := strings.Index(out, "failed to build AccountsDB")
	assert.NotEqual(t, -1, actionIdx, "action panel should render")
	assert.NotEqual(t, -1, errorIdx, "old progress error should still render")
	assert.Less(t, actionIdx, errorIdx, "restart controls must stay visible before stale progress history")
	assert.Contains(t, out, "▶ Start Mithril")
}

func TestRenderProcessView_RPCStallExplainsRateLimitRisk(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{detection: &procctl.Detection{
			Status:             procctl.StatusCrashed,
			LastShutdownReason: state.ShutdownReasonStall,
		}},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "RPC catchup")
	assert.Contains(t, out, "private/dedicated RPC")
}

// A concurrent stop by another dashboard is surfaced to the operator.
func TestRenderProcessView_StopInProgressNoted(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{detection: &procctl.Detection{
			Status:           procctl.StatusRunning,
			Pid:              12345,
			StopInProgressBy: 67890,
			StopInProgressAt: time.Now().Add(-30 * time.Second),
		}},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Another dashboard is shutting this node down")
	assert.Contains(t, out, "67890", "should mention the dashboard PID in the sub-line")
}

func TestRenderProcessView_StopInProgressByThisDashboard(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{detection: &procctl.Detection{
			Status:           procctl.StatusRunning,
			Pid:              12345,
			StopInProgressBy: os.Getpid(),
			StopInProgressAt: time.Now().Add(-30 * time.Second),
		}},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "This dashboard is shutting this node down")
}

// Process status badge appears on the Overview screen.
func TestRenderOverview_ShowsProcessBadge(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenOverview,
		proc: procState{detection: &procctl.Detection{
			Status: procctl.StatusRunning,
			Pid:    12345,
		}},
	}
	out := m.renderOverview()
	assert.Contains(t, out, "Running", "overview should surface the Running badge")
	// PID is reserved for the Process view, not the Overview banner.
	assert.NotContains(t, out, "12345", "overview banner should NOT show PID")
}

func TestRenderOverview_CompletedUsesCompletionBadge(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenOverview,
		proc: procState{detection: &procctl.Detection{
			Status:             procctl.StatusStopped,
			LastShutdownReason: state.ShutdownReasonCompleted,
			LastCleanExit:      true,
		}},
	}
	out := m.renderOverview()
	assert.Contains(t, out, "Completed")
	assert.NotContains(t, out, "Stopped")
}

// Banner shows no badge during the loading window (no detection yet).
func TestRenderOverview_NoBadgeBeforeFirstFetch(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenOverview,
		// proc.detection is nil
	}
	out := m.renderOverview()
	assert.NotContains(t, out, "Running")
	assert.NotContains(t, out, "Stopped")
	assert.NotContains(t, out, "Crashed")
}

func TestRenderOverview_FirstRunPointsToProcessStart(t *testing.T) {
	m := model{
		hasConfig:  true,
		screen:     screenOverview,
		cfg:        &configData{},
		configFile: "/tmp/config.toml",
	}
	out := m.renderOverview()
	assert.Contains(t, out, "Run Node")
	assert.Contains(t, out, "choose Start")
	assert.NotContains(t, out, "mithril run --config")
}

func TestManagedLightbringer_CleanStoppedScreenAndConfirmCaveats(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		cfg: &configData{
			cluster:          "mainnet-beta",
			bootstrapMode:    "accountsdb",
			blockSource:      "lightbringer",
			lbEnabled:        true,
			lbGrpcAddr:       "127.0.0.1:3551",
			lbGossipPort:     "55000",
			lbPortRangeStart: "55001",
			lbPortRangeEnd:   "55100",
			accountsPath:     "/home/ubuntu/mithril-tests/accounts",
			rpcEndpoints:     []string{"https://api.mainnet-beta.solana.com"},
		},
		proc: procState{detection: &procctl.Detection{Status: procctl.StatusStopped}},
	}
	// Idle screen stays clean (no run plan) but still offers the action.
	stopped := m.renderProcessView()
	assert.NotContains(t, stopped, "Run plan")
	assert.Contains(t, stopped, "Start Mithril + Lightbringer")

	// Confirm-run card carries run mode, inbound UDP ports, and the RPC rate-limit warning.
	confirm := m.renderStartFlow()
	flat := strings.Join(strings.Fields(confirm), " ")
	assert.Contains(t, confirm, "Confirm run")
	assert.Contains(t, confirm, "Mithril + Lightbringer")
	assert.Contains(t, flat, "55000")
	assert.Contains(t, flat, "55001-55100")
	assert.Contains(t, flat, "rate-limit")
	assert.NotContains(t, confirm, "Mithril only")
}

func TestRenderProcessView_RPCConfigShowsMithrilOnlyStart(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		width:     80,
		cfg: &configData{
			cluster:       "mainnet-beta",
			bootstrapMode: "accountsdb",
			blockSource:   "rpc",
		},
		proc: procState{detection: &procctl.Detection{Status: procctl.StatusStopped}},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Start Mithril")
	assert.NotContains(t, out, "Start Mithril + Lightbringer")
	assert.Contains(t, out, "Mithril alone")
}

func TestStartFlowCardsWarnBeforeFreshRebuild(t *testing.T) {
	m := model{
		cfg: &configData{
			bootstrapMode: "new-snapshot",
			blockSource:   "rpc",
		},
		startFlow: startFlowState{active: true},
	}

	cards := []startFlowCard{*m.startFlowBootstrapCard()}
	assert.Equal(t, "Fresh rebuild", cards[0].title)
	assert.Contains(t, cards[0].detail, "fresh AccountsDB")
	assert.True(t, cards[0].warn)

	out := m.renderStartFlow()
	assert.Contains(t, out, "Fresh rebuild")
	assert.Contains(t, out, "Build a fresh AccountsDB from snapshot.")
}

func TestStartFlowCardsTreatEmptyAccountsRootAsFreshRebuild(t *testing.T) {
	m := model{
		cfg: &configData{
			bootstrapMode: "new-snapshot",
			blockSource:   "rpc",
			accountsPath:  "/var/lib/mithril/accounts",
		},
		accounts: accountsActivity{
			Path:    "/var/lib/mithril/accounts",
			Name:    "accounts",
			ModTime: time.Now(),
		},
		startFlow: startFlowState{active: true},
	}

	cards := []startFlowCard{*m.startFlowBootstrapCard()}
	assert.Equal(t, "Fresh rebuild", cards[0].title)
	assert.Contains(t, cards[0].detail, "fresh AccountsDB")
	assert.NotContains(t, cards[0].detail, "replaced")
}

func TestStartFlowCardsWarnWhenFreshRebuildReplacesAccountsArtifact(t *testing.T) {
	m := model{
		cfg: &configData{
			bootstrapMode: "new-snapshot",
			blockSource:   "rpc",
			accountsPath:  "/var/lib/mithril/accounts",
		},
		accounts: accountsActivity{
			Path:    "/var/lib/mithril/accounts/mithril_state.json",
			Name:    "mithril_state.json",
			ModTime: time.Now(),
		},
		startFlow: startFlowState{active: true},
	}

	cards := []startFlowCard{*m.startFlowBootstrapCard()}
	assert.Equal(t, "Fresh rebuild", cards[0].title)
	assert.Contains(t, cards[0].detail, "replaced")
}

func TestStartFlowCardsWarnWhenFreshRebuildReplacesExistingState(t *testing.T) {
	m := model{
		cfg: &configData{
			bootstrapMode: "new-snapshot",
			blockSource:   "rpc",
		},
		state:     &nodeState{LastSlot: 42},
		startFlow: startFlowState{active: true},
	}

	cards := []startFlowCard{*m.startFlowBootstrapCard()}
	assert.Equal(t, "Fresh rebuild", cards[0].title)
	assert.Contains(t, cards[0].detail, "replaced")
}

func TestStartFlowCardsShowAccountsDBResumeMode(t *testing.T) {
	m := model{
		cfg: &configData{
			bootstrapMode: "accountsdb",
			blockSource:   "rpc",
		},
	}

	cards := []startFlowCard{*m.startFlowBootstrapCard()}
	assert.Equal(t, "Use existing AccountsDB", cards[0].title)
	assert.Contains(t, cards[0].detail, "fails fast")
	assert.False(t, cards[0].warn)
}

func TestRenderProcessView_DivergenceCrashGuidesRebuildNotRetry(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		width:     100,
		cfg:       &configData{cluster: "devnet", blockSource: "rpc", bootstrapMode: "auto"},
		proc: procState{
			detection: &procctl.Detection{Status: procctl.StatusCrashed},
			startFailStderr: strings.Join([]string{
				"ERROR: DIVERGENCE in slot 468318825",
				"panic: tx abc pre-balance divergence",
			}, "\n"),
		},
	}

	out := m.renderProcessView()

	assert.Contains(t, out, "Rebuild fresh safely")
	assert.Contains(t, out, "do not retry this AccountsDB")
	assert.Contains(t, out, "rebuild from a fresh snapshot")
	assert.NotContains(t, out, "Start again when the config looks safe")
}

func TestRunActions_DivergenceCrashDoesNotOfferStart(t *testing.T) {
	m := model{
		cfg: &configData{blockSource: "rpc"},
		proc: procState{
			detection:       &procctl.Detection{Status: procctl.StatusCrashed},
			startFailStderr: "panic: return value divergence",
		},
	}

	actions := m.runActions()
	labels := make([]string, 0, len(actions))
	for _, action := range actions {
		labels = append(labels, action.label)
		assert.NotEqual(t, runActionStart, action.id)
	}

	assert.Contains(t, labels, "Rebuild fresh safely")
	assert.Contains(t, labels, "Set new snapshot")
}

func TestRunActions_DivergenceCrashUsesLogTailAfterDashboardReopen(t *testing.T) {
	m := model{
		cfg: &configData{blockSource: "rpc"},
		proc: procState{
			detection: &procctl.Detection{Status: procctl.StatusCrashed},
		},
		mithrilLines: []string{
			"ERROR: DIVERGENCE in slot 468318825",
			"panic: tx abc pre-balance divergence",
		},
	}

	actions := m.runActions()
	for _, action := range actions {
		assert.NotEqual(t, runActionStart, action.id)
	}

	out := m.renderProcessView()
	assert.Contains(t, out, "do not retry this AccountsDB")
	assert.Contains(t, out, "Rebuild fresh safely")
}

// Unparseable PID file shows a non-misleading error path.
func TestRenderProcessView_FetchError(t *testing.T) {
	m := model{
		hasConfig: true,
		screen:    screenProcess,
		proc: procState{
			fetchErr: "parse pid file /tmp/mithril.pid: unexpected end of JSON",
		},
	}
	out := m.renderProcessView()
	assert.Contains(t, out, "Cannot determine status")
	assert.Contains(t, out, "JSON", "should preserve the underlying error detail")
}

// stoppedModelWithActivity builds a Stopped Run-Node model with a clean activity
// feed (ending in shutdown) so the two-column body renders feed + last-exit detail.
func stoppedModelWithActivity(width int) model {
	return model{
		hasConfig:  true,
		screen:     screenProcess,
		runFocused: true,
		width:      width,
		cfg:        &configData{cluster: "devnet", blockSource: "rpc", bootstrapMode: "auto", accountsPath: "/data/accounts"},
		proc: procState{
			detection: &procctl.Detection{
				Status:             procctl.StatusStopped,
				LastCleanExit:      true,
				LastShutdownReason: "graceful shutdown (Ctrl+C)",
			},
			fetchedAt: time.Now().Add(-30 * time.Second),
		},
		progress: []progressEvent{
			{Phase: "bootstrap_accounts", Status: "ok", Message: "AccountsDB is ready", Fields: map[string]any{"slot": float64(426085232)}},
			{Phase: "replay", Status: "", Message: "Starting block replay", Fields: map[string]any{"start_slot": float64(426085232)}},
			{Phase: "shutdown", Status: "ok", Message: "Mithril stopped cleanly", TS: time.Now().Add(-25 * time.Minute)},
		},
	}
}

// visibleWidth returns the terminal display width (ANSI stripped).
func visibleWidth(s string) int { return lipgloss.Width(s) }

// Wide pane splits into aligned columns: actions left, activity/status right.
func TestRenderProcessView_TwoColumnLayout(t *testing.T) {
	m := stoppedModelWithActivity(110)
	out := m.renderProcessView()

	assert.Contains(t, out, "Choose action", "left column header")
	assert.Contains(t, out, "Recent activity", "right column activity header")
	assert.Contains(t, out, "Last exit", "right column status header")
	assert.Contains(t, out, "│", "columns separated by a vertical divider")
	assert.Contains(t, out, "Mithril stopped cleanly")
}

// Resize guard: no rendered line exceeds the pane budget at any width.
func TestRenderProcessView_NoOverflowAcrossWidths(t *testing.T) {
	for _, w := range []int{40, 55, 71, 72, 80, 100, 120, 160, 220} {
		m := stoppedModelWithActivity(w)
		budget := m.runPanelContentWidth() + 2 // +2 for the two-space inset
		out := m.renderProcessView()
		for i, line := range strings.Split(out, "\n") {
			if got := visibleWidth(line); got > budget {
				t.Errorf("width=%d line %d overflows: %d > %d budget\n%q", w, i, got, budget, line)
			}
		}
	}
}

// Every two-column row places the divider on the same visible column.
func TestRenderProcessView_DividerStaysAligned(t *testing.T) {
	m := stoppedModelWithActivity(110)
	out := m.renderProcessView()

	width := -1
	rows := 0
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, "│") {
			continue
		}
		rows++
		w := visibleWidth(line)
		if width == -1 {
			width = w
			continue
		}
		assert.Equalf(t, width, w, "divider rows must share one visible width (got %q)", line)
	}
	assert.Greater(t, rows, 3, "expected several divided rows")
}

// Below the two-column threshold the body reflows to a single stacked column.
func TestRenderProcessView_NarrowReflowsToStacked(t *testing.T) {
	wide := stoppedModelWithActivity(110).renderProcessView()
	narrow := stoppedModelWithActivity(50).renderProcessView()

	assert.Contains(t, wide, "│", "wide pane uses a divided two-column body")
	assert.NotContains(t, narrow, "│", "narrow pane reflows to a stacked single column")
	assert.Contains(t, narrow, "Choose action", "stacked layout still lists actions")
}

// renderProcessStatusBadge — the colored badge alone.

// Each Status yields a visibly distinct badge.
func TestRenderProcessStatusBadge_DistinguishesStates(t *testing.T) {
	running := renderProcessStatusBadge(procctl.StatusRunning)
	stopped := renderProcessStatusBadge(procctl.StatusStopped)
	crashed := renderProcessStatusBadge(procctl.StatusCrashed)

	assert.Contains(t, running, "Running")
	assert.Contains(t, stopped, "Stopped")
	assert.Contains(t, crashed, "Crashed")

	assert.NotEqual(t, running, stopped)
	assert.NotEqual(t, running, crashed)
	assert.NotEqual(t, stopped, crashed)
}

// selectCurrent — the menu→screen dispatcher.

// Picking "Process" sets the screen and returns a fetch cmd for immediate refresh.
func TestSelectCurrent_ProcessNavigatesAndRefreshes(t *testing.T) {
	m := &model{
		hasConfig: true,
		items: []menuItem{
			{label: "Overview", value: "overview"},
			{label: "Run Node", value: "process"},
		},
		cursor: 1, // pointing at Run Node
	}
	cmd := m.selectCurrent()
	assert.Equal(t, screenProcess, m.screen)
	assert.True(t, m.runFocused, "Run Node should move focus to the action panel")
	assert.NotNil(t, cmd, "navigating to Process should kick a fetchProcessCmd")
}

// humanizeAge — utility shared by the view and the status bar.

// Very-recent renders as "a moment ago", not "0s ago".
func TestHumanizeAge_RecentTimesAreFriendly(t *testing.T) {
	out := humanizeAge(time.Now())
	assert.Equal(t, "a moment ago", out)
}

// Unit cascade (s→m→h→d): asserts exact unit token and magnitude so a
// cascade bug (e.g. "120s ago" instead of "2m ago") fails loudly.
func TestHumanizeAge_ScalesByUnit(t *testing.T) {
	cases := []struct {
		ago       time.Duration
		wantUnit  string // unit suffix: "s ago", "m ago", "h ago", "d ago"
		wantValue int    // exact numeric value
	}{
		{10 * time.Second, "s ago", 10},
		{2 * time.Minute, "m ago", 2},
		{3 * time.Hour, "h ago", 3},
		{49 * time.Hour, "d ago", 2},
	}
	for _, c := range cases {
		t.Run(c.wantUnit, func(t *testing.T) {
			got := humanizeAge(time.Now().Add(-c.ago))
			assert.True(t, strings.HasSuffix(got, c.wantUnit),
				"got %q, want suffix %q", got, c.wantUnit)
			// ±1 tolerance for rounding as time.Since() advances mid-test.
			expectedTokens := []string{
				fmt.Sprintf("%d%s", c.wantValue-1, c.wantUnit),
				fmt.Sprintf("%d%s", c.wantValue, c.wantUnit),
				fmt.Sprintf("%d%s", c.wantValue+1, c.wantUnit),
			}
			matched := false
			for _, tok := range expectedTokens {
				if strings.Contains(got, tok) {
					matched = true
					break
				}
			}
			assert.True(t, matched, "got %q, want one of %v", got, expectedTokens)
		})
	}
}
