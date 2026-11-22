package dashboard

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"
)

func heldScheduleFixture(at time.Time) *HeldScheduleWindow {
	key := "CktRuQ2mttgRGkXJtyksdKHjUdc2C4TgDzyB98oEzy8"
	w := &HeldScheduleWindow{Scope: "held_schedule_at_capture", ReferenceSlot: 100, ReferenceBankhash: key,
		ReferenceCompletedAt: at, Epoch: 3, SlotIndex: 0, SlotsInEpoch: 32, Slots: make([]HeldScheduleSlot, 32)}
	for i := range w.Slots {
		w.Slots[i].Slot = 100 + uint64(i)
		if i%2 == 0 {
			value := key
			w.Slots[i].Leader = &value
		}
	}
	return w
}

func TestHeldScheduleWireAndBinding(t *testing.T) {
	c := replayDetailTestCore(1)
	c.ScheduleWindow = heldScheduleFixture(c.CapturedAt)
	p := replayDetailTestPrepared(t, c)
	m, err := decodeReplayDetailManifest([]byte(p.manifests[0]))
	if err != nil || !reflect.DeepEqual(m.ScheduleWindow, c.ScheduleWindow) {
		t.Fatalf("manifest schedule: %v", err)
	}
	page, err := decodeReplayDetailPage([]byte(p.pages[0]), m, 0)
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := replayDetailCoreFromRecords(m, page.Records)
	if err != nil || !reflect.DeepEqual(rebuilt, c) {
		t.Fatalf("schedule generation binding: %v", err)
	}
	*m.ScheduleWindow.Slots[0].Leader = "mutated"
	if *rebuilt.ScheduleWindow.Slots[0].Leader == "mutated" {
		t.Fatal("reconstructed core retained manifest pointers")
	}
	m, _ = decodeReplayDetailManifest([]byte(p.manifests[0]))
	m.ScheduleWindow = nil
	if _, err := replayDetailCoreFromRecords(m, page.Records); err == nil {
		t.Fatal("removing the schedule bypassed its generation digest")
	}
	body, _ := json.Marshal(c)
	if _, err := ReplayDetailGeneration(c); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"schedule_window":`)) {
		t.Fatal("schedule omitted from canonical core")
	}
	for _, change := range []func(*HeldScheduleWindow){
		func(w *HeldScheduleWindow) { w.Scope = "produced_leaders" },
		func(w *HeldScheduleWindow) { w.Slots = w.Slots[:31] },
		func(w *HeldScheduleWindow) { w.SlotIndex = w.SlotsInEpoch },
		func(w *HeldScheduleWindow) { w.Slots[1].Slot++ },
		func(w *HeldScheduleWindow) { value := "11111111111111111111111111111111"; w.Slots[0].Leader = &value },
	} {
		w := heldScheduleFixture(c.CapturedAt)
		change(w)
		if validHeldSchedule(w) {
			t.Fatal("invalid held schedule accepted")
		}
	}
}

func TestHeldScheduleBounds(t *testing.T) {
	for _, test := range []struct {
		slot, index, length uint64
		want                int
	}{
		{0, 0, 32, 32}, {31, 31, 32, 1}, {100, 3, 8, 5}, {math.MaxUint64, 0, 32, 1},
		{math.MaxUint64 - 1, 0, math.MaxUint64, 2}, {0, 1, 32, 0}, {1, 0, 0, 0}, {100, 8, 8, 0},
	} {
		if got := HeldScheduleCount(test.slot, test.index, test.length); got != test.want {
			t.Fatalf("window %+v: %d", test, got)
		}
	}
	c := replayDetailTestCore(256)
	at := sizingTime(time.Now())
	c.ScheduleWindow = heldScheduleFixture(at)
	c.ScheduleWindow.ReferenceSlot, c.ScheduleWindow.SlotIndex, c.ScheduleWindow.SlotsInEpoch, c.ScheduleWindow.Epoch = math.MaxUint64-31, math.MaxUint64-31, math.MaxUint64, math.MaxUint64
	for i := range c.ScheduleWindow.Slots {
		c.ScheduleWindow.Slots[i].Slot = math.MaxUint64 - 31 + uint64(i)
	}
	// This geometry has 31 remaining epoch slots, not 32.
	c.ScheduleWindow.Slots = c.ScheduleWindow.Slots[:31]
	p := replayDetailTestPrepared(t, c)
	if len(p.manifests[0]) > MaxSnapshotBytes {
		t.Fatal("held schedule raised manifest cap")
	}
	m, err := decodeReplayDetailManifest([]byte(p.manifests[0]))
	if err != nil || m.ScheduleWindow == nil {
		t.Fatalf("maximum schedule manifest: %v", err)
	}
	t.Logf("held-schedule manifest: %d/%d bytes", len(p.manifests[0]), MaxSnapshotBytes)
}

func TestHeldOwnAssignmentsWireAndOwnership(t *testing.T) {
	c := replayDetailTestCore(0)
	c.ScheduleWindow = heldScheduleFixture(c.CapturedAt)
	w := c.ScheduleWindow
	w.OwnAssignments = &HeldOwnAssignments{Identity: w.ReferenceBankhash, Total: 16, Remaining: 15, Upcoming: make([]HeldOwnSlot, 15)}
	for i := range w.OwnAssignments.Upcoming {
		w.OwnAssignments.Upcoming[i].Slot = 102 + 2*uint64(i)
	}
	p := replayDetailTestPrepared(t, c)
	m, err := decodeReplayDetailManifest([]byte(p.manifests[0]))
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := replayDetailCoreFromRecords(m, []ReplayDetailRecord{})
	if err != nil || !reflect.DeepEqual(rebuilt, c) {
		t.Fatalf("own assignment roundtrip: %v", err)
	}
	m.ScheduleWindow.OwnAssignments.Upcoming[0].Slot++
	if rebuilt.ScheduleWindow.OwnAssignments.Upcoming[0].Slot != 102 {
		t.Fatal("reconstructed own preview aliases manifest")
	}
	for _, change := range []func(*HeldOwnAssignments){
		func(o *HeldOwnAssignments) { o.Total = 33 },
		func(o *HeldOwnAssignments) { o.Total = 17 },
		func(o *HeldOwnAssignments) { o.Remaining = 17 },
		func(o *HeldOwnAssignments) { o.Upcoming = nil },
		func(o *HeldOwnAssignments) { o.Upcoming = o.Upcoming[:14] },
		func(o *HeldOwnAssignments) { o.Upcoming[0].Slot = 100 },
		func(o *HeldOwnAssignments) { o.Upcoming[1].Slot = o.Upcoming[0].Slot },
		func(o *HeldOwnAssignments) { o.Upcoming[14].Slot = 132 },
		func(o *HeldOwnAssignments) { o.Identity = "11111111111111111111111111111111" },
	} {
		bad := cloneHeldSchedule(w)
		change(bad.OwnAssignments)
		if validHeldSchedule(bad) {
			t.Fatal("invalid own assignment accepted")
		}
	}
	m, err = decodeReplayDetailManifest([]byte(p.manifests[0]))
	if err != nil {
		t.Fatal(err)
	}
	m.ScheduleWindow.OwnAssignments = nil
	if _, err := replayDetailCoreFromRecords(m, []ReplayDetailRecord{}); err == nil {
		t.Fatal("removing own assignments bypassed family digest")
	}
	zero := cloneHeldSchedule(w)
	zero.OwnAssignments.Identity = "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG"
	zero.OwnAssignments.Total, zero.OwnAssignments.Remaining = 0, 0
	zero.OwnAssignments.Upcoming = []HeldOwnSlot{}
	if !validHeldSchedule(zero) || cloneHeldSchedule(zero).OwnAssignments.Upcoming == nil {
		t.Fatal("known-empty preview became unknown")
	}
	zero.OwnAssignments.Total = 1
	if validHeldSchedule(zero) {
		t.Fatal("an assignment at another identity's reference slot was accepted")
	}
	complete := heldScheduleFixture(c.CapturedAt)
	complete.OwnAssignments = &HeldOwnAssignments{Identity: complete.ReferenceBankhash, Total: 18, Remaining: 17, Upcoming: make([]HeldOwnSlot, 16)}
	other := "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG"
	for i := range complete.Slots {
		key := complete.ReferenceBankhash
		if i > 16 {
			key = other
		}
		complete.Slots[i].Leader = &key
	}
	for i := range complete.OwnAssignments.Upcoming {
		complete.OwnAssignments.Upcoming[i].Slot = 101 + uint64(i)
	}
	if validHeldSchedule(complete) {
		t.Fatal("truncated preview hid an extra assignment in a fully covered epoch")
	}
	now := time.Now()
	r := replayDetailReaderFixture(t, c, now, "/unused")
	accepted := replayDetailReaderAccept(t, r, c, now, 30*time.Second)
	if r.cursor.core.ScheduleWindow.OwnAssignments.Upcoming[0].Slot != 102 {
		t.Fatal("reader fixture did not start at the expected own assignment")
	}
	c.ScheduleWindow.OwnAssignments.Upcoming[0].Slot++
	*c.ScheduleWindow.Slots[0].Leader = "changed"
	if r.cursor.core.ScheduleWindow.OwnAssignments.Upcoming[0].Slot != 102 || *r.cursor.core.ScheduleWindow.Slots[0].Leader != w.ReferenceBankhash {
		t.Fatal("source schedule aliases the prepared reader cursor")
	}
	accepted.cursor.core.ScheduleWindow.OwnAssignments.Upcoming[0].Slot++
	*accepted.cursor.core.ScheduleWindow.Slots[0].Leader = "changed"
	if r.cursor.core.ScheduleWindow.OwnAssignments.Upcoming[0].Slot != 102 || *r.cursor.core.ScheduleWindow.Slots[0].Leader != w.ReferenceBankhash {
		t.Fatal("candidate schedule aliases the installed reader cursor")
	}
}
