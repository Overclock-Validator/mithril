package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func maximumReplayNearReception(at time.Time) *ReplayNearReception {
	at = sizingTime(at)
	return &ReplayNearReception{Scope: "first_completed_replay_near_recent_input", Slot: math.MaxUint64, Epoch: math.MaxUint64,
		Bankhash: "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG", CompletedAt: at, ObservedAt: at,
		SourceStartedAt: at, ReceiverStartedAt: at, ReceiverSession: math.MaxUint64, ReceptionSlot: math.MaxUint64,
		ReceptionSampledAt: at, ReceptionObservedAt: at, RetainedSlots: 64}
}

func TestReplayNearReceptionReaderFirstEventIsImmutable(t *testing.T) {
	for _, failedCollection := range []bool{false, true} {
		for _, omit := range []bool{false, true} {
			t.Run(fmt.Sprintf("failure-%v/omission-%v", failedCollection, omit), func(t *testing.T) {
				now := time.Now()
				core := replayDetailReaderWindow(1, 1, now)
				core.NearReception = maximumReplayNearReception(core.CapturedAt)
				r := replayDetailReaderFixture(t, core, now, "/unused")
				replayDetailReaderAccept(t, r, core, now, 30*time.Second)
				if failedCollection {
					r.collect(context.Background())
					if r.failure == nil {
						t.Fatal("failed-collection control did not fail")
					}
				}
				next := core
				next.CapturedAt = now.Add(time.Second).UTC()
				next.NearReception = cloneReplayNearReception(core.NearReception)
				if omit {
					next.NearReception = nil
				} else {
					next.NearReception.ObservedAt = next.NearReception.ObservedAt.Add(time.Nanosecond)
				}
				at := now.Add(time.Second)
				gate := replayDetailTestGate(next, at)
				r.gate.Store(&gate)
				generation, err := ReplayDetailGeneration(next)
				if err != nil {
					t.Fatal(err)
				}
				candidate, reason := r.prepare(next, generation, gate, at, at.Add(30*time.Second), at)
				if reason == "" && candidate != nil && r.commit(candidate, at) == "" {
					t.Fatal("reader installed a rehashed replacement or deletion of its first historical event")
				}
				if reason != "source_invalid" || candidate != nil {
					t.Fatalf("invalid event replacement: reason=%q candidate=%v", reason, candidate != nil)
				}
			})
		}
	}
	now := time.Now()
	core := replayDetailReaderWindow(1, 1, now)
	r := replayDetailReaderFixture(t, core, now, "/unused")
	replayDetailReaderAccept(t, r, core, now, 30*time.Second)
	core.CapturedAt = now.Add(time.Second).UTC()
	core.NearReception = maximumReplayNearReception(core.CapturedAt)
	replayDetailReaderAccept(t, r, core, now.Add(time.Second), 30*time.Second)
	core.Binding.SourceGeneration++
	core.CapturedAt = now.Add(2 * time.Second).UTC()
	core.NearReception.ObservedAt = core.CapturedAt
	replayDetailReaderAccept(t, r, core, now.Add(2*time.Second), 30*time.Second)
	if !r.cursor.core.NearReception.ObservedAt.Equal(core.CapturedAt) {
		t.Fatal("new binding could not establish its own first event")
	}
}

func TestReplayNearReceptionWireDigestAndOwnership(t *testing.T) {
	c := replayDetailTestCore(1)
	c.NearReception = maximumReplayNearReception(c.CapturedAt)
	// Wall dates may regress; native monotonic eligibility is not reconstructed.
	c.NearReception.ObservedAt = c.CapturedAt.Add(-time.Hour)
	p := replayDetailTestPrepared(t, c)
	m, err := decodeReplayDetailManifest([]byte(p.manifests[0]))
	if err != nil {
		t.Fatal(err)
	}
	page, err := decodeReplayDetailPage([]byte(p.pages[0]), m, 0)
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := replayDetailCoreFromRecords(m, page.Records)
	if err != nil || !reflect.DeepEqual(rebuilt, c) {
		t.Fatalf("historical event roundtrip: %v", err)
	}
	m.NearReception.ReceiverSession = 0
	if rebuilt.NearReception.ReceiverSession == 0 {
		t.Fatal("reconstructed event aliases its manifest")
	}
	for _, change := range []func(*ReplayNearReception){
		func(o *ReplayNearReception) { o.Scope = "caught_up" },
		func(o *ReplayNearReception) { o.ReceiverSession = 0 },
		func(o *ReplayNearReception) { o.RetainedSlots = 0 },
		func(o *ReplayNearReception) { o.RetainedSlots = 65 },
		func(o *ReplayNearReception) { o.Bankhash = "11111111111111111111111111111111" },
		func(o *ReplayNearReception) { o.CompletedAt = time.Time{} },
		func(o *ReplayNearReception) { o.ReceptionSlot -= 5 },
	} {
		bad := cloneReplayNearReception(c.NearReception)
		change(bad)
		if validReplayNearReception(bad) {
			t.Fatal("invalid historical event accepted")
		}
	}
	child, err := json.Marshal(c.NearReception)
	if err != nil {
		t.Fatal(err)
	}
	for _, replacement := range []string{"null", "{}", strings.Replace(string(child), `"retained_slots":64`, `"retained_slots":64,"retained_slots":64`, 1)} {
		body := strings.Replace(p.manifests[0], string(child), replacement, 1)
		if body == p.manifests[0] {
			t.Fatal("negative control did not change the real manifest")
		}
		if _, err := decodeReplayDetailManifest([]byte(body)); err == nil {
			t.Fatal("noncanonical historical event accepted")
		}
	}
	m, err = decodeReplayDetailManifest([]byte(p.manifests[0]))
	if err != nil {
		t.Fatal(err)
	}
	m.NearReception = nil
	if _, err := replayDetailCoreFromRecords(m, page.Records); err == nil {
		t.Fatal("event omission bypassed generation digest")
	}
	now := time.Now()
	r := replayDetailReaderFixture(t, c, now, "/unused")
	accepted := replayDetailReaderAccept(t, r, c, now, 30*time.Second)
	c.NearReception.ReceiverSession = 0
	accepted.cursor.core.NearReception.ReceiverSession = 0
	if r.cursor.core.NearReception.ReceiverSession != math.MaxUint64 {
		t.Fatal("source or candidate event aliases installed reader state")
	}
	empty := replayDetailTestCore(0)
	empty.NearReception = maximumReplayNearReception(empty.CapturedAt)
	if ValidateReplayDetailCore(empty) == nil {
		t.Fatal("a completed-row milestone was accepted without any completed row")
	}
}
