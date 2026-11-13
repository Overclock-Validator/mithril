package dashboard

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func maximumReplayRecentSelected(at time.Time) *ReplayRecentSelected {
	v := &ReplayRecentSelected{Scope: "recent_selected_and_folded", ObservedAt: at.UTC(), WindowEntries: 512,
		SourceRecords: 256, CoverageIncomplete: false, Records: make([]ReplaySelectedRecord, ReplayRecentSelectedCapacity)}
	for i := range v.Records {
		r := maximumReplayLatestSelected(at)
		r.Slot = math.MaxUint64 - uint64(len(v.Records)-1-i)
		v.Records[i] = ReplaySelectedRecord{Slot: r.Slot, Bankhash: r.Bankhash, BlockID: r.BlockID, Gap: r.Gap, Facts: r.Facts}
	}
	return v
}

func TestReplayRecentSelectedWire(t *testing.T) {
	for _, count := range []int{0, 1} {
		c := replayDetailTestCore(count)
		c.LatestSelected = maximumReplayLatestSelected(c.CapturedAt)
		c.RecentSelected = maximumReplayRecentSelected(c.CapturedAt)
		p := replayDetailTestPrepared(t, c)
		m, err := decodeReplayDetailManifest([]byte(p.manifests[0]))
		if err != nil {
			t.Fatal(err)
		}
		rebuilt, err := replayDetailCoreFromRecords(m, c.Records)
		if err != nil || !reflect.DeepEqual(c, rebuilt) {
			t.Fatalf("recent metadata roundtrip: %v", err)
		}
		public, err := prepareReplayDetailPublic(replayDetailCursor{core: rebuilt, generation: p.generation}, c.CapturedAt)
		if err != nil || !strings.Contains(public.manifests[0], `"recent_selected"`) {
			t.Fatalf("public recent projection: %v", err)
		}
		before, _ := ReplayDetailGeneration(c)
		*rebuilt.RecentSelected.Records[0].Bankhash = "changed"
		if *m.RecentSelected.Records[0].Bankhash == "changed" {
			t.Fatal("core aliases decoded manifest")
		}
		c.RecentSelected.Records[0].Facts.Epoch--
		after, _ := ReplayDetailGeneration(c)
		if before == after {
			t.Fatal("older record was omitted from canonical generation")
		}
	}
	legacy := replayDetailTestCore(0)
	p := replayDetailTestPrepared(t, legacy)
	if strings.Contains(p.manifests[0], "recent_selected") {
		t.Fatal("legacy optional omission changed")
	}
	if _, err := decodeReplayDetailManifest([]byte(strings.Replace(p.manifests[0], `"exhausted":false`, `"exhausted":false,"recent_selected":null`, 1))); err == nil {
		t.Fatal("explicit null was accepted as omission")
	}
}

func TestReplayRecentSelectedValidation(t *testing.T) {
	at := replayDetailTestCore(0).CapturedAt
	for name, change := range map[string]func(*ReplayRecentSelected){
		"scope":      func(v *ReplayRecentSelected) { v.Scope = "all_blocks" },
		"date":       func(v *ReplayRecentSelected) { v.ObservedAt = time.Time{} },
		"window":     func(v *ReplayRecentSelected) { v.WindowEntries = 513 },
		"projection": func(v *ReplayRecentSelected) { v.SourceRecords-- },
		"count":      func(v *ReplayRecentSelected) { v.Records = v.Records[:7] },
		"duplicate":  func(v *ReplayRecentSelected) { v.Records[1].Slot = v.Records[0].Slot },
		"reverse":    func(v *ReplayRecentSelected) { v.Records[0], v.Records[1] = v.Records[1], v.Records[0] },
		"gap":        func(v *ReplayRecentSelected) { v.Records[0].Gap = true; v.Records[0].Facts = nil },
		"fees":       func(v *ReplayRecentSelected) { v.Records[0].Facts.Fees.PriorityLamports = math.MaxUint64 },
	} {
		t.Run(name, func(t *testing.T) {
			v := maximumReplayRecentSelected(at)
			change(v)
			if validReplayRecentSelected(v) {
				t.Fatal("accepted invalid recent subset")
			}
		})
	}
	v := maximumReplayRecentSelected(at)
	v.CoverageIncomplete, v.Records[0].Gap, v.Records[0].Facts = true, true, nil
	if !validReplayRecentSelected(v) {
		t.Fatal("explicit retained gap rejected")
	}
	latest := maximumReplayLatestSelected(at)
	if !validReplaySelectedMetadata(latest, v) {
		t.Fatal("matching newest sibling rejected")
	}
	for _, change := range []func(*ReplayRecentSelected){
		func(v *ReplayRecentSelected) { v.ObservedAt = v.ObservedAt.Add(time.Nanosecond) },
		func(v *ReplayRecentSelected) { v.Records[7].Facts.Epoch-- },
		func(v *ReplayRecentSelected) { v.Records[7].Gap = true; v.Records[7].Facts = nil },
	} {
		bad := cloneReplayRecentSelected(v)
		change(bad)
		if validReplaySelectedMetadata(latest, bad) {
			t.Fatal("contradictory newest sibling accepted")
		}
	}
	copy := cloneReplayRecentSelected(v)
	*copy.Records[1].Bankhash = "changed"
	copy.Records[1].Facts.LocalCost.BlockCost = 0
	if !validReplayRecentSelected(v) {
		t.Fatal("cloned recent records alias the source")
	}
	empty := &ReplayRecentSelected{Scope: "recent_selected_and_folded", ObservedAt: at, Records: []ReplaySelectedRecord{}}
	if !validReplayRecentSelected(empty) {
		t.Fatal("known empty source rejected")
	}
	if validReplaySelectedMetadata(latest, empty) || validReplaySelectedMetadata(nil, v) || !validReplaySelectedMetadata(nil, empty) {
		t.Fatal("latest/recent presence contradicts source-window knownness")
	}
}
