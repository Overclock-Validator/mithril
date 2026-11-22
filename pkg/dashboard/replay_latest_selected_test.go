package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func maximumReplayLatestSelected(at time.Time) *ReplayLatestSelected {
	hash := "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG"
	return &ReplayLatestSelected{Scope: "latest_selected_and_folded", ObservedAt: at.UTC(), Slot: math.MaxUint64,
		Bankhash: &hash, BlockID: &hash, Facts: &ReplaySelectedFacts{ReplayResultAt: at.UTC(), Epoch: math.MaxUint64,
			ExecutionLeader: &hash, FromLocalProduction: true, Transactions: ReplaySelectedCounts{
				Total: math.MaxUint64, NonVote: math.MaxUint64, Failed: math.MaxUint64, NonVoteFailed: math.MaxUint64, Saturated: true},
			Fees:      &ReplaySelectedFees{ExecutionLamports: 10000000000000000000, PriorityLamports: math.MaxUint64 - 10000000000000000000, TotalLamports: math.MaxUint64},
			LocalCost: &ReplaySelectedCost{BlockCost: math.MaxUint64, BlockCostLimit: math.MaxUint64}}}
}

func TestReplayLatestSelectedWireAndLegacy(t *testing.T) {
	for _, count := range []int{0, 1} {
		core := replayDetailTestCore(count)
		legacy := replayDetailTestPrepared(t, core)
		if bytes.Contains([]byte(legacy.manifests[0]), []byte("latest_selected")) {
			t.Fatal("legacy omission was lost")
		}
		core.LatestSelected = maximumReplayLatestSelected(core.CapturedAt)
		// Historical wall dates are not used to manufacture duration ordering.
		core.LatestSelected.ObservedAt = core.CapturedAt.Add(-time.Second)
		p := replayDetailTestPrepared(t, core)
		if p.generation == legacy.generation {
			t.Fatal("selected facts were excluded from generation")
		}
		m, err := decodeReplayDetailManifest([]byte(p.manifests[0]))
		if err != nil {
			t.Fatal(err)
		}
		restored, err := replayDetailCoreFromRecords(m, core.Records)
		if err != nil || !reflect.DeepEqual(restored.LatestSelected, core.LatestSelected) {
			t.Fatal("selected metadata did not round trip", err)
		}
		public, err := prepareReplayDetailPublic(replayDetailCursor{core: restored, generation: p.generation}, core.CapturedAt)
		if err != nil {
			t.Fatal(err)
		}
		var dto PublicReplayDetailManifest
		if json.Unmarshal([]byte(public.manifests[0]), &dto) != nil || !reflect.DeepEqual(dto.LatestSelected, restored.LatestSelected) {
			t.Fatal("public manifest lost selected metadata")
		}
		*core.LatestSelected.Bankhash = "changed"
		core.LatestSelected.Facts.Transactions.Total = 0
		core.LatestSelected.Facts.Fees.TotalLamports = 0
		core.LatestSelected.Facts.LocalCost.BlockCost = 0
		if !validReplayLatestSelected(restored.LatestSelected) {
			t.Fatal("restored metadata aliases its source")
		}
		m.LatestSelected.Slot--
		if _, err := replayDetailCoreFromRecords(m, core.Records); err == nil {
			t.Fatal("tampered selected metadata retained its digest")
		}
		bad := strings.Replace(string(legacy.manifests[0]), `"exhausted":false`, `"exhausted":false,"latest_selected":null`, 1)
		if _, err := decodeReplayDetailManifest([]byte(bad)); err == nil {
			t.Fatal("explicit null replaced canonical optional omission")
		}
	}
}

func TestReplayLatestSelectedKnownness(t *testing.T) {
	at := replayDetailTestCore(0).CapturedAt
	for name, change := range map[string]func(*ReplayLatestSelected){
		"scope":          func(s *ReplayLatestSelected) { s.Scope = "finalized" },
		"date":           func(s *ReplayLatestSelected) { s.ObservedAt = time.Time{} },
		"bank missing":   func(s *ReplayLatestSelected) { s.Bankhash = nil },
		"block missing":  func(s *ReplayLatestSelected) { s.BlockID = nil },
		"zero hash":      func(s *ReplayLatestSelected) { v := strings.Repeat("1", 32); s.Bankhash = &v },
		"zero leader":    func(s *ReplayLatestSelected) { v := strings.Repeat("1", 32); s.Facts.ExecutionLeader = &v },
		"facts missing":  func(s *ReplayLatestSelected) { s.Facts = nil },
		"gap with facts": func(s *ReplayLatestSelected) { s.Gap = true },
		"counts":         func(s *ReplayLatestSelected) { s.Facts.Transactions.Total = 0 },
		"fee overflow":   func(s *ReplayLatestSelected) { s.Facts.Fees.PriorityLamports = math.MaxUint64 },
		"fee sum":        func(s *ReplayLatestSelected) { s.Facts.Fees.TotalLamports = 0 },
		"remote cost":    func(s *ReplayLatestSelected) { s.Facts.FromLocalProduction = false },
	} {
		t.Run(name, func(t *testing.T) {
			s := maximumReplayLatestSelected(at)
			change(s)
			if validReplayLatestSelected(s) {
				t.Fatal("invalid facts were admitted")
			}
		})
	}
	s := maximumReplayLatestSelected(at)
	s.Facts.ExecutionLeader = nil
	s.Facts.Fees = &ReplaySelectedFees{}
	zero := uint64(0)
	s.Facts.Fees.BurnedLamports = &zero
	s.Facts.LocalCost = &ReplaySelectedCost{BlockCost: 2, BlockCostLimit: 1}
	if !validReplayLatestSelected(s) {
		t.Fatal("unknown leader, known-zero fees or over-limit native cost erased known facts")
	}
	*cloneReplayLatestSelected(s).Facts.Fees.BurnedLamports = 1
	if *s.Facts.Fees.BurnedLamports != 0 {
		t.Fatal("fee burn aliases cloned state")
	}
	bad := uint64(1)
	s.Facts.Fees.BurnedLamports = &bad
	if validReplayLatestSelected(s) {
		t.Fatal("burn exceeds charged fees")
	}
	s.Gap, s.Facts, s.Bankhash, s.BlockID = true, nil, nil, nil
	if !validReplayLatestSelected(s) {
		t.Fatal("gap fabricated a mandatory identifier")
	}
}

func TestReplayLatestSelectedReaderAdvanceAndOwnership(t *testing.T) {
	now := time.Now()
	core := replayDetailReaderWindow(0, 0, now)
	core.LatestSelected = maximumReplayLatestSelected(core.CapturedAt)
	core.LatestSelected.Slot = 41
	core.LatestSelected.Gap, core.LatestSelected.Facts = true, nil
	gate := replayDetailTestGate(core, now)
	r := replayDetailReaderFixture(t, core, now, "/unused")
	install := func(c ReplayDetailCore) *replayDetailCandidate {
		t.Helper()
		generation, err := ReplayDetailGeneration(c)
		if err != nil {
			t.Fatal(err)
		}
		candidate, reason := r.prepare(c, generation, gate, now, now.Add(30*time.Second), now)
		if reason != "" || r.commit(candidate, now) != "" {
			t.Fatal("reader refused selected metadata", reason)
		}
		return candidate
	}
	first := install(core)
	core.LatestSelected = maximumReplayLatestSelected(core.CapturedAt)
	core.LatestSelected.Slot = 42
	core.CapturedAt = core.CapturedAt.Add(time.Millisecond)
	second := install(core)
	if r.cursor.core.LatestSelected.Slot != core.LatestSelected.Slot || r.cursor.core.LatestSelected.Gap ||
		!first.cursor.core.LatestSelected.Gap {
		t.Fatal("latest observation was frozen or source mutation escaped")
	}
	second.cursor.core.LatestSelected.Gap = true
	if r.cursor.core.LatestSelected.Gap {
		t.Fatal("committed cursor aliases candidate")
	}
	core.LatestSelected = nil // Legacy/replaced producer may omit it again.
	core.CapturedAt = core.CapturedAt.Add(time.Millisecond)
	install(core)
	if r.cursor.core.LatestSelected != nil {
		t.Fatal("old selected metadata survived omission")
	}
}

func TestReplayLatestSelectedFeedPublic(t *testing.T) {
	core := replayDetailLiveCore(0)
	core.LatestSelected = maximumReplayLatestSelected(core.CapturedAt)
	core.RecentSelected = maximumReplayRecentSelected(core.CapturedAt)
	core.LatestSelected.Facts.Fees.BurnedLamports = new(uint64(math.MaxUint64))
	for i := range core.RecentSelected.Records {
		core.RecentSelected.Records[i].Facts.Fees.BurnedLamports = new(uint64(math.MaxUint64))
	}
	path := socketPath(t)
	feed, err := ListenFeedWithReplayDetails(context.Background(), path, "", func() Snapshot {
		return replayDetailLiveStatus(core)
	}, func(at time.Time) (ReplayDetailCore, error) {
		copy := core
		copy.CapturedAt = at.UTC()
		return copy, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, ReplayDetails: true})
	t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, body, header := replayDetailPublicRequest(t, address, "/api/v1/details/replay", "stats.example", "")
		if status == 200 {
			var m PublicReplayDetailManifest
			if json.Unmarshal(body, &m) != nil || m.Count != 0 || m.PageCount != 0 ||
				!reflect.DeepEqual(m.LatestSelected, core.LatestSelected) || !reflect.DeepEqual(m.RecentSelected, core.RecentSelected) ||
				header.Get(replayDetailRemainingHeader) == "" {
				t.Fatal("actual Feed/reader lost metadata or its original lease")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("selected transport unavailable: %d %s", status, body)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestReplayDetailReaderRetriesFirstCapture(t *testing.T) {
	core := replayDetailLiveCore(0)
	core.LatestSelected = maximumReplayLatestSelected(core.CapturedAt)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	path := socketPath(t)
	feed, err := ListenFeedWithReplayDetails(context.Background(), path, "", func() Snapshot {
		return replayDetailLiveStatus(core)
	}, func(at time.Time) (ReplayDetailCore, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		copy := core
		copy.CapturedAt = at.UTC()
		return copy, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("initial replay capture did not start")
	}
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, ReplayDetails: true})
	t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, body, _ := replayDetailPublicRequest(t, address, "/api/v1/details/replay", "stats.example", "")
		if status == 503 && strings.Contains(string(body), `"reason":"source_unavailable"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reader did not observe the pending capture: %d %s", status, body)
		}
		time.Sleep(100 * time.Millisecond)
	}
	close(release)
	deadline = time.Now().Add(3 * time.Second)
	for {
		status, body, _ := replayDetailPublicRequest(t, address, "/api/v1/details/replay", "stats.example", "")
		if status == 200 {
			var manifest PublicReplayDetailManifest
			if json.Unmarshal(body, &manifest) != nil || !reflect.DeepEqual(manifest.LatestSelected, core.LatestSelected) {
				t.Fatal("retried capture lost selected metadata")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("reader did not retry the first capture promptly: %d %s", status, body)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
