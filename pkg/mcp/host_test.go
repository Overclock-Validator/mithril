package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/progress"
)

func uint64Pointer(value uint64) *uint64 { return &value }
func boolPointer(value bool) *bool       { return &value }
func stringPointer(value string) *string { return &value }

func TestCollectHostHealthDiskThresholds(t *testing.T) {
	cfg := Config{
		AccountsDir: "/accounts", SnapshotsDir: "/snapshots", ShredstoreDir: "/shreds", LogDir: "/logs",
		DiskWarnPercent: 85, DiskCriticalPercent: 95,
	}
	usage := map[string]*progress.DiskInfo{
		"/accounts":  {Path: "/accounts", UsedBytes: 84, TotalBytes: 100},
		"/snapshots": {Path: "/snapshots", UsedBytes: 85, TotalBytes: 100},
		"/shreds":    {Path: "/shreds", UsedBytes: 95, TotalBytes: 100},
		"/logs":      {Path: "/logs", Error: errors.New("unavailable")},
	}
	metrics := &MetricsSummary{ProcessRSSBytes: uint64Pointer(123)}
	state := &ShutdownStateSummary{Stage: stringPointer("ready")}
	out := collectHostHealthWith(context.Background(), cfg.normalized(), metrics, state, func(path string) *progress.DiskInfo {
		return usage[path]
	}, time.Unix(1_700_000_000, 0))

	want := []string{checkOK, checkDegraded, checkCritical, checkUnknown}
	if len(out.Storage) != len(want) {
		t.Fatalf("storage count = %d, want %d", len(out.Storage), len(want))
	}
	for i, status := range want {
		if out.Storage[i].Status != status {
			t.Errorf("storage[%d] status = %q, want %q", i, out.Storage[i].Status, status)
		}
	}
	if got := *out.Storage[1].AvailableBytes; got != 15 {
		t.Fatalf("snapshot available bytes = %d, want 15", got)
	}
	if out.Status != diagnosticCritical || out.EvidenceComplete {
		t.Fatalf("overall status/complete = %q/%v, want critical/false", out.Status, out.EvidenceComplete)
	}
}

func TestCollectHostHealthSkipsUnconfiguredStorage(t *testing.T) {
	metrics := &MetricsSummary{ProcessRSSBytes: uint64Pointer(123)}
	state := &ShutdownStateSummary{Stage: stringPointer("ready")}
	out := collectHostHealthWith(context.Background(), Config{}.normalized(), metrics, state, func(string) *progress.DiskInfo {
		t.Fatal("unconfigured storage was probed")
		return nil
	}, time.Unix(1_700_000_000, 0))

	if out.Status != diagnosticHealthy || !out.EvidenceComplete {
		t.Fatalf("status/evidence = %q/%v, want healthy/true", out.Status, out.EvidenceComplete)
	}
	for _, storage := range out.Storage {
		if storage.Configured || storage.Status != checkSkipped {
			t.Fatalf("unconfigured storage was not skipped: %+v", storage)
		}
	}
}

func TestReadHostCgroupMemoryLocalAndUnlimited(t *testing.T) {
	dir := writeCgroupFixture(t, map[string]string{
		"memory.current":      "512\n",
		"memory.peak":         "768\n",
		"memory.max":          "max\n",
		"memory.events.local": "low 0\nhigh 2\nmax 1\noom 3\noom_kill 1\noom_group_kill 0\n",
	})
	out, err := readHostCgroupMemory(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Available || out.LimitConfigured || out.LimitBytes != nil || out.EventScope != "local_cumulative" {
		t.Fatalf("unexpected cgroup availability/limit/scope: %+v", out)
	}
	if *out.CurrentBytes != 512 || *out.PeakBytes != 768 || *out.OOMEventsTotal != 3 || *out.OOMKillsTotal != 1 {
		t.Fatalf("unexpected cgroup counters: %+v", out)
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), dir) {
		t.Fatal("cgroup output exposed configured host path")
	}
}

func TestReadHostCgroupMemoryHierarchicalFallbackAndLimit(t *testing.T) {
	dir := writeCgroupFixture(t, map[string]string{
		"memory.current": "80\n",
		"memory.max":     "100\n",
		"memory.events":  "high 0\nmax 0\noom 0\noom_kill 0\n",
	})
	out, err := readHostCgroupMemory(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if out.EventScope != "hierarchical_cumulative" || !out.LimitConfigured || *out.LimitBytes != 100 {
		t.Fatalf("unexpected fallback output: %+v", out)
	}
	if out.UsagePercentOfLimit == nil || *out.UsagePercentOfLimit != 80 {
		t.Fatalf("usage percent = %v, want 80", out.UsagePercentOfLimit)
	}
}

func TestReadHostCgroupMemoryRejectsUnsafeOrMalformedFiles(t *testing.T) {
	t.Run("symlink", func(t *testing.T) {
		dir := writeCgroupFixture(t, map[string]string{
			"memory.max":          "100\n",
			"memory.events.local": "oom 0\noom_kill 0\n",
		})
		target := filepath.Join(t.TempDir(), "current")
		if err := os.WriteFile(target, []byte("1\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(dir, "memory.current")); err != nil {
			t.Fatal(err)
		}
		if _, err := readHostCgroupMemory(context.Background(), dir); err == nil {
			t.Fatal("symlinked cgroup file was accepted")
		}
	})

	for _, test := range []struct {
		name  string
		file  string
		value string
	}{
		{name: "malformed scalar", file: "memory.current", value: "-1\n"},
		{name: "oversized scalar", file: "memory.current", value: strings.Repeat("1", maxCgroupScalarBytes+1)},
		{name: "duplicate event", file: "memory.events.local", value: "oom 0\noom 1\noom_kill 0\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			files := map[string]string{
				"memory.current":      "1\n",
				"memory.max":          "100\n",
				"memory.events.local": "oom 0\noom_kill 0\n",
			}
			files[test.file] = test.value
			if _, err := readHostCgroupMemory(context.Background(), writeCgroupFixture(t, files)); err == nil {
				t.Fatal("malformed cgroup fixture was accepted")
			}
		})
	}
}

func TestAssessHostBootstrapTruthfulStates(t *testing.T) {
	now := time.Unix(1_700_000_100, 0)
	tests := []struct {
		name       string
		metrics    *MetricsSummary
		state      *ShutdownStateSummary
		assessment string
		status     string
	}{
		{name: "ready without producer", state: &ShutdownStateSummary{Stage: stringPointer("ready")}, assessment: "ready", status: checkOK},
		{name: "active without state", metrics: &MetricsSummary{SnapshotBootstrapActive: boolPointer(true), SnapshotBootstrapStartedTimestampSeconds: uint64Pointer(1_700_000_000)}, assessment: "active", status: checkDegraded},
		{name: "active conflicts with ready", metrics: &MetricsSummary{SnapshotBootstrapActive: boolPointer(true)}, state: &ShutdownStateSummary{Stage: stringPointer("ready")}, assessment: "conflicting", status: checkUnknown},
		{name: "active preserves corrupted state", metrics: &MetricsSummary{SnapshotBootstrapActive: boolPointer(true)}, state: &ShutdownStateSummary{Stage: stringPointer("corrupted")}, assessment: "corrupted", status: checkCritical},
		{name: "inactive conflicts with building", metrics: &MetricsSummary{SnapshotBootstrapActive: boolPointer(false)}, state: &ShutdownStateSummary{Stage: stringPointer("building")}, assessment: "conflicting", status: checkUnknown},
		{name: "corrupted", metrics: &MetricsSummary{SnapshotBootstrapActive: boolPointer(false)}, state: &ShutdownStateSummary{Stage: stringPointer("corrupted")}, assessment: "corrupted", status: checkCritical},
		{name: "bytes alone are not active", metrics: &MetricsSummary{SnapshotTarBytesRead: uint64Pointer(99)}, assessment: "activity_observed", status: checkUnknown},
		{name: "stage alone is not live proof", state: &ShutdownStateSummary{Stage: stringPointer("building")}, assessment: "reported_in_progress", status: checkDegraded},
		{name: "unknown", assessment: "unknown", status: checkUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out, status, _ := assessHostBootstrap(test.metrics, test.state, now)
			if out.Assessment != test.assessment || status != test.status {
				t.Fatalf("assessment/status = %q/%q, want %q/%q", out.Assessment, status, test.assessment, test.status)
			}
			if test.name == "active without state" && (out.ActiveDurationSeconds == nil || *out.ActiveDurationSeconds != 100) {
				t.Fatalf("active duration = %v, want 100", out.ActiveDurationSeconds)
			}
		})
	}
}

func TestAssessHostBootstrapCarriesStateProvenance(t *testing.T) {
	epoch := uint64(42)
	started := "2026-07-20T10:00:00Z"
	completed := "2026-07-20T10:01:00Z"
	out, status, _ := assessHostBootstrap(nil, &ShutdownStateSummary{
		Stage:            stringPointer("ready"),
		SnapshotEpoch:    &epoch,
		BuildStartedAt:   &started,
		BuildCompletedAt: &completed,
	}, time.Unix(1_700_000_100, 0))
	if status != checkOK || out.SnapshotEpoch != &epoch || out.BuildStartedAt != &started || out.BuildCompletedAt != &completed {
		t.Fatalf("bootstrap provenance was not preserved: %+v", out)
	}
}

func TestCollectHostHealthCgroupOOMIsCritical(t *testing.T) {
	dir := writeCgroupFixture(t, map[string]string{
		"memory.current":      "10\n",
		"memory.max":          "100\n",
		"memory.events.local": "oom 1\noom_kill 2\n",
	})
	cfg := Config{NodeCgroupPath: dir, DiskWarnPercent: 85, DiskCriticalPercent: 95}
	metrics := &MetricsSummary{ProcessRSSBytes: uint64Pointer(10)}
	state := &ShutdownStateSummary{Stage: stringPointer("ready")}
	out := collectHostHealthWith(context.Background(), cfg.normalized(), metrics, state, func(string) *progress.DiskInfo {
		t.Fatal("unconfigured disk path was probed")
		return nil
	}, time.Unix(1_700_000_000, 0))
	if out.Status != diagnosticCritical || out.Memory.Cgroup.Status != checkCritical || out.Memory.Cgroup.OOMKillsTotal == nil || *out.Memory.Cgroup.OOMKillsTotal != 2 {
		t.Fatalf("unexpected OOM assessment: %+v", out)
	}
}

func TestCollectHostHealthCgroupPressureIsDegraded(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current string
		events  string
		want    string
	}{
		{name: "memory high event", current: "10\n", events: "high 1\nmax 0\noom 0\noom_kill 0\n", want: checkDegraded},
		{name: "memory max event", current: "10\n", events: "high 0\nmax 1\noom 0\noom_kill 0\n", want: checkDegraded},
		{name: "at 90 percent", current: "90\n", events: "high 0\nmax 0\noom 0\noom_kill 0\n", want: checkDegraded},
		{name: "below 90 percent", current: "89\n", events: "high 0\nmax 0\noom 0\noom_kill 0\n", want: checkOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := writeCgroupFixture(t, map[string]string{
				"memory.current":      tc.current,
				"memory.max":          "100\n",
				"memory.events.local": tc.events,
			})
			cfg := Config{NodeCgroupPath: dir, DiskWarnPercent: 85, DiskCriticalPercent: 95}
			out := collectHostHealthWith(
				context.Background(), cfg.normalized(),
				&MetricsSummary{ProcessRSSBytes: uint64Pointer(10)},
				&ShutdownStateSummary{Stage: stringPointer("ready")},
				func(string) *progress.DiskInfo {
					t.Fatal("unconfigured disk path was probed")
					return nil
				},
				time.Unix(1_700_000_000, 0),
			)
			if out.Memory.Cgroup.Status != tc.want {
				t.Fatalf("cgroup status = %q, want %q: %+v", out.Memory.Cgroup.Status, tc.want, out.Memory.Cgroup)
			}
		})
	}
}

func writeCgroupFixture(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}
