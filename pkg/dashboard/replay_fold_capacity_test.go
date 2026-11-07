package dashboard

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func maximumReplayFoldCapacity(at time.Time) *ReplayFoldCapacity {
	n := uint64(math.MaxInt64 - 1)
	returned := at.UTC()
	return &ReplayFoldCapacity{Scope: "replay_fold_capacity", State: "returned", Slot: math.MaxUint64, StartedAt: at.UTC(),
		Outcome: "still_over_capacity", ReturnedAt: &returned, DurationNanoseconds: &n}
}

func maximumReplayOptionalMetadata(c *ReplayDetailCore) {
	at := sizingTime(c.CapturedAt)
	c.FoldCapacity = maximumReplayFoldCapacity(at)
	c.NearReception = maximumReplayNearReception(at)
	c.LatestSelected = maximumReplayLatestSelected(at)
	c.RecentSelected = maximumReplayRecentSelected(at)
	units := uint64(math.MaxUint64)
	c.LatestSelected.Facts.Transactions.ComputeUnits = &units
	for i := range c.RecentSelected.Records {
		c.RecentSelected.Records[i].Facts.Transactions.ComputeUnits = &units
	}
	c.ScheduleWindow = heldScheduleFixture(at)
	w := c.ScheduleWindow
	w.ReferenceSlot, w.SlotIndex, w.SlotsInEpoch, w.Epoch = 10000000000000000000, 10000000000000000000, math.MaxUint64, math.MaxUint64
	w.ReferenceBankhash = "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG"
	for i := range w.Slots {
		w.Slots[i].Slot = w.ReferenceSlot + uint64(i)
		key := w.ReferenceBankhash
		w.Slots[i].Leader = &key
	}
	w.OwnAssignments = &HeldOwnAssignments{Identity: w.ReferenceBankhash, Total: math.MaxUint64, Remaining: math.MaxUint64 - 1 - w.ReferenceSlot, Upcoming: make([]HeldOwnSlot, 16)}
	for i := range w.OwnAssignments.Upcoming {
		w.OwnAssignments.Upcoming[i].Slot = w.ReferenceSlot + 1 + uint64(i)
	}
}

func TestReplayFoldCapacityWireDigestAndOwnership(t *testing.T) {
	for _, count := range []int{0, 1} {
		c := replayDetailTestCore(count)
		c.FoldCapacity = maximumReplayFoldCapacity(c.CapturedAt)
		c.ScheduleWindow = heldScheduleFixture(c.CapturedAt)
		p := replayDetailTestPrepared(t, c)
		m, err := decodeReplayDetailManifest([]byte(p.manifests[0]))
		if err != nil {
			t.Fatal(err)
		}
		records := make([]ReplayDetailRecord, 0, count)
		for i, body := range p.pages {
			page, err := decodeReplayDetailPage([]byte(body), m, i)
			if err != nil {
				t.Fatal(err)
			}
			records = append(records, page.Records...)
		}
		rebuilt, err := replayDetailCoreFromRecords(m, records)
		if err != nil || !reflect.DeepEqual(rebuilt, c) {
			t.Fatal("fold metadata was lost from the whole-family digest")
		}
		*m.FoldCapacity.ReturnedAt = time.Time{}
		*m.FoldCapacity.DurationNanoseconds = 0
		if rebuilt.FoldCapacity.ReturnedAt.IsZero() || *rebuilt.FoldCapacity.DurationNanoseconds == 0 {
			t.Fatal("decoded core aliases manifest metadata")
		}
		changed := c
		changed.FoldCapacity = cloneReplayFoldCapacity(c.FoldCapacity)
		changed.FoldCapacity.Outcome = "failed"
		before, _ := ReplayDetailGeneration(c)
		after, _ := ReplayDetailGeneration(changed)
		if before == after {
			t.Fatal("changed fold outcome did not bind the immutable generation")
		}
	}
	c := replayDetailTestCore(0)
	c.FoldCapacity = &ReplayFoldCapacity{Scope: "replay_fold_capacity", State: "active", Slot: 1, StartedAt: c.CapturedAt}
	if ValidateReplayDetailCore(c) != nil {
		t.Fatal("active wait depended on a completed row")
	}
	for _, change := range []func(*ReplayFoldCapacity){
		func(o *ReplayFoldCapacity) { o.Scope = "disk_failure" }, func(o *ReplayFoldCapacity) { o.State = "blocked" }, func(o *ReplayFoldCapacity) { o.Slot = 0 },
		func(o *ReplayFoldCapacity) { o.StartedAt = time.Time{} }, func(o *ReplayFoldCapacity) { o.Outcome = "relieved" },
		func(o *ReplayFoldCapacity) { o.ReturnedAt = &o.StartedAt }, func(o *ReplayFoldCapacity) { n := uint64(0); o.DurationNanoseconds = &n },
	} {
		bad := cloneReplayFoldCapacity(c.FoldCapacity)
		change(bad)
		if validReplayFoldCapacity(bad) {
			t.Fatal("contradictory active wait accepted")
		}
	}
	c.FoldCapacity = maximumReplayFoldCapacity(c.CapturedAt)
	p := replayDetailTestPrepared(t, c)
	child, _ := json.Marshal(c.FoldCapacity)
	for _, replacement := range []string{"null", "{}", strings.Replace(string(child), `"state":"returned"`, `"state":"returned","state":"returned"`, 1),
		strings.Replace(string(child), `"outcome":"still_over_capacity"`, `"outcome":"unclassified"`, 1),
		strings.Replace(string(child), `"returned_at":`+`"`+c.CapturedAt.Format(time.RFC3339Nano)+`"`, `"returned_at":null`, 1)} {
		body := strings.Replace(p.manifests[0], string(child), replacement, 1)
		if body == p.manifests[0] {
			t.Fatal("negative fold control did not modify the actual manifest")
		}
		if _, err := decodeReplayDetailManifest([]byte(body)); err == nil {
			t.Fatal("invalid fold metadata accepted")
		}
	}
}
