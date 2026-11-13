package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/replay"
)

func TestDashboardReplayDetailsOptIn(t *testing.T) {
	if validateDashboardReplayDetails(false, "") != nil || validateDashboardReplayDetails(true, "/private/node.sock") != nil {
		t.Fatal("valid replay-detail configuration rejected")
	}
	for _, socket := range []string{"", " \t\n"} {
		if validateDashboardReplayDetails(true, socket) == nil {
			t.Fatal("replay details accepted without a dashboard socket")
		}
	}
	flag := Run.Flags().Lookup("dashboard-replay-details")
	if flag == nil || flag.DefValue != "false" {
		t.Fatal("replay-detail flag is not default off")
	}
}

func TestDashboardReplayDetailsEnableBeforeReplay(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "dashboard-replay-owner-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.RemoveAll(dir); err != nil {
					t.Error(err)
				}
			})
			d := startNodeDashboard(context.Background(), time.Now().UTC(), filepath.Join(dir, "node.sock"), "", "owner-run", "isolated", "verifying", false, false, enabled, false, false, false, false, false, false, false, false)
			if d == nil {
				t.Fatal("private dashboard feed did not start")
			}
			t.Cleanup(d.close)
			if _, ok := d.progress.ReplayDiagnosticsSnapshot(); ok {
				t.Fatal("unstarted replay became a known-empty diagnostic history")
			}
			if _, err := d.sampleReplayDetail(time.Now()); err == nil || err.Error() != "waiting_source" {
				t.Fatal("unstarted replay did not remain waiting")
			}
			if _, err := d.sampleSelectedWindow(time.Now()); err == nil || err.Error() != "waiting_source" {
				t.Fatal("unstarted selected window did not remain waiting")
			}
			if _, err := d.sampleRootedHistory(time.Now()); err == nil || err.Error() != "waiting_source" {
				t.Fatal("unstarted rooted history did not remain waiting")
			}
			// A second enable distinguishes the startup option from a source
			// that merely has not produced its first attempt yet.
			err = d.progress.EnableReplayDiagnostics("owner-run")
			if enabled && err == nil || !enabled && err != nil {
				t.Fatal("startup did not preserve the replay-detail option")
			}
		})
	}
	var absent *nodeDashboard
	if _, err := absent.sampleRootedHistory(time.Now()); err == nil || err.Error() != "source_unavailable" {
		t.Fatal("absent rooted history did not remain unavailable")
	}
	if _, err := absent.sampleReplayDetail(time.Now()); err == nil || err.Error() != "source_unavailable" {
		t.Fatal("absent dashboard did not remain unavailable")
	}
	if _, err := absent.sampleSelectedWindow(time.Now()); err == nil || err.Error() != "source_unavailable" {
		t.Fatal("absent selected window did not remain unavailable")
	}
}

func TestReplayDetailProgressFence(t *testing.T) {
	source := replayDetailSource(t, 1)
	before := replay.ProgressSnapshot{Attempt: source.Attempt, Rewinds: source.Rewinds, StartedAt: source.StartedAt}
	after := before
	after.ExecutedBlocks, after.UpdatedAt = 100, before.StartedAt.Add(time.Second)
	if !replayDetailProgressStable(before, after, source) {
		t.Fatal("ordinary execution progress invalidated the retained source context")
	}
	for _, change := range []func(*replay.ProgressSnapshot, *replay.ProgressSnapshot, *replay.ReplayDiagnosticSnapshot){
		func(b, a *replay.ProgressSnapshot, s *replay.ReplayDiagnosticSnapshot) { a.Attempt++ },
		func(b, a *replay.ProgressSnapshot, s *replay.ReplayDiagnosticSnapshot) { a.Rewinds++ },
		func(b, a *replay.ProgressSnapshot, s *replay.ReplayDiagnosticSnapshot) {
			a.StartedAt = a.StartedAt.Add(time.Nanosecond)
		},
		func(b, a *replay.ProgressSnapshot, s *replay.ReplayDiagnosticSnapshot) { s.Attempt++ },
		func(b, a *replay.ProgressSnapshot, s *replay.ReplayDiagnosticSnapshot) { s.Rewinds++ },
		func(b, a *replay.ProgressSnapshot, s *replay.ReplayDiagnosticSnapshot) {
			s.StartedAt = s.StartedAt.Add(time.Nanosecond)
		},
		func(b, a *replay.ProgressSnapshot, s *replay.ReplayDiagnosticSnapshot) { s.Generation = 0 },
		func(b, a *replay.ProgressSnapshot, s *replay.ReplayDiagnosticSnapshot) { s.Attempt = 0 },
	} {
		b, a, s := before, after, source
		change(&b, &a, &s)
		if replayDetailProgressStable(b, a, s) {
			t.Fatal("crossing or invalid source context accepted")
		}
	}
	a, b := "first", "second"
	if !sameReplayDetailGenesis(nil, nil) || !sameReplayDetailGenesis(&a, &a) || sameReplayDetailGenesis(nil, &a) || sameReplayDetailGenesis(&a, nil) || sameReplayDetailGenesis(&a, &b) {
		t.Fatal("genesis context comparison changed")
	}
}
