package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

func sameCompaction(public *PublicCompaction, private *Compaction) bool {
	a, aerr := json.Marshal(public)
	b, berr := json.Marshal(private)
	return aerr == nil && berr == nil && bytes.Equal(a, b)
}

func maximumCompaction(at time.Time) *Compaction {
	return &Compaction{Enabled: true, LastCompleted: &CompactionCycle{
		SessionStartedAt: at.Add(-time.Minute), StartedAt: at.Add(-time.Second), CompletedAt: at,
		DurationNanoseconds: math.MaxInt64, CandidatesScanned: math.MaxInt64, FilesCompacted: math.MaxInt64,
		FilesDeleted: math.MaxInt64, FilesSparsified: math.MaxInt64, LiveBytesMoved: math.MaxInt64, BytesReclaimed: math.MaxInt64,
	}}
}

func TestPublicCompactionRelocationRequiresValidOwnerSample(t *testing.T) {
	at := time.Now().UTC()
	active := false
	start, end := at.Add(time.Millisecond), at.Add(2*time.Millisecond)
	c := &Compaction{Enabled: true, Active: &active, ActiveSampleStartedAt: &start, ActiveSampleCompletedAt: &end,
		LastCompleted: maximumCompaction(at).LastCompleted,
		Relocation:    &CompactionRelocation{SessionStartedAt: at.Add(-time.Second), ElapsedNanoseconds: int64(time.Second)}}
	project := func() *PublicCompaction { return publicCompaction(c, at, end) }
	p := project()
	if p == nil || p.Relocation == nil || !sameCompaction(p, c) {
		t.Fatal("valid measured zero was changed or omitted")
	}
	c.Relocation.Bytes = math.MaxUint64
	if p.Relocation.Bytes != 0 {
		t.Fatal("public relocation changed when the producer counter was updated")
	}
	if p = project(); p == nil || p.Relocation != nil || p.LastCompleted == nil {
		t.Fatal("saturated counter was not withheld independently of cycle history")
	}
	c.Relocation.Bytes = 4096
	c.ActiveSampleCompletedAt = nil
	if p = project(); p == nil || p.Relocation != nil || p.LastCompleted == nil {
		t.Fatal("unbracketed counter was not withheld independently of cycle history")
	}
}

func TestCompactionRealFeedDisclosureAndRecovery(t *testing.T) {
	at := time.Now().UTC()
	s := resourceFixture(at)
	s.Resources.Compaction = maximumCompaction(at)
	var calls atomic.Uint64
	path := socketPath(t)
	feed, err := ListenFeed(context.Background(), path, func() Snapshot { calls.Add(1); return s })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	c := newCollector(path)
	c.public = true
	t.Cleanup(c.client.CloseIdleConnections)
	read := func() PublicSnapshot {
		t.Helper()
		set := c.latest.Load()
		var p PublicSnapshot
		if set == nil || json.Unmarshal(set.advancedPublicJSON, &p) != nil {
			t.Fatal("missing public encoding")
		}
		var frame struct{ Value json.RawMessage }
		if json.Unmarshal(set.advancedMessages[0], &frame) != nil || string(frame.Value) != string(set.advancedPublicJSON) {
			t.Fatal("HTTP and WS compaction values diverged")
		}
		return p
	}
	c.poll(context.Background(), time.Now())
	if read().Resources != nil {
		t.Fatal("private compaction bypassed the public resource gate")
	}
	c.publicOptions.Resources = true
	c.poll(context.Background(), time.Now())
	if p := read(); p.Resources == nil || !sameCompaction(p.Resources.Compaction, s.Resources.Compaction) {
		t.Fatal("typed completed cycle changed across the actual Unix feed")
	}
	started, count := time.Now(), calls.Load()
	for range 20 {
		response, err := c.client.Get("http://node/snapshot")
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, MaxSnapshotBytes+1))
		closeErr := response.Body.Close()
		if response.StatusCode != 200 || readErr != nil || closeErr != nil {
			t.Fatal("cached viewer read failed")
		}
	}
	if calls.Load()-count > uint64(time.Since(started)/SampleInterval)+1 {
		t.Fatal("viewer requests triggered extra source work")
	}
	feed.Close()
	c.poll(context.Background(), time.Now())
	if p := read(); p.Resources == nil || p.Resources.State != "stale" || !sameCompaction(p.Resources.Compaction, s.Resources.Compaction) {
		t.Fatal("source loss concealed staleness or redated the historical cycle")
	}
	nextAt := time.Now().UTC()
	next := resourceFixture(nextAt)
	next.RunID = "replacement-worker"
	next.Resources.Compaction = &Compaction{Enabled: true}
	replacement, err := ListenFeed(context.Background(), path, func() Snapshot { return next })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(replacement.Close)
	c.poll(context.Background(), time.Now())
	if p := read(); p.Node.RunID != next.RunID || p.Resources.Compaction == nil || !p.Resources.Compaction.Enabled || p.Resources.Compaction.LastCompleted != nil {
		t.Fatal("replacement filled an unknown cycle from the old worker")
	}
}

func TestPublicCompactionRejectsInvalidSourceWithoutUnsignedWrap(t *testing.T) {
	at := time.Now().UTC()
	for field := range 7 {
		c := maximumCompaction(at)
		v := c.LastCompleted
		values := []*int64{&v.DurationNanoseconds, &v.CandidatesScanned, &v.FilesCompacted,
			&v.FilesDeleted, &v.FilesSparsified, &v.LiveBytesMoved, &v.BytesReclaimed}
		*values[field] = -1
		if publicCompaction(c, at, at) != nil {
			t.Fatalf("negative field %d was published", field)
		}
	}
	for _, mutate := range []func(*Compaction){
		func(c *Compaction) { c.Enabled = false },
		func(c *Compaction) { c.LastCompleted.SessionStartedAt = time.Time{} },
		func(c *Compaction) { c.LastCompleted.StartedAt = time.Time{} },
		func(c *Compaction) { c.LastCompleted.CompletedAt = time.Time{} },
		func(c *Compaction) { c.LastCompleted.CompletedAt = at.Add(2 * time.Second) },
	} {
		c := maximumCompaction(at)
		mutate(c)
		if publicCompaction(c, at, at) != nil {
			t.Fatal("invalid compaction child retained")
		}
	}
	c := maximumCompaction(at)
	c.LastCompleted.StartedAt = at
	c.LastCompleted.CompletedAt = at.Add(-time.Second)
	if p := publicCompaction(c, at, at); p == nil || p.LastCompleted.DurationNanoseconds != math.MaxInt64 {
		t.Fatal("wall-clock step replaced the retained monotonic duration")
	}
}

func TestPublicCompactionActivity(t *testing.T) {
	at := time.Now().UTC()
	for _, active := range []bool{false, true} {
		c := &Compaction{Enabled: true, Active: &active}
		p := publicCompaction(c, at, at)
		if p == nil || p.Active == nil || *p.Active != active || p.ActiveSampleStartedAt != nil || p.ActiveSampleCompletedAt != nil {
			t.Fatal("known activity was not preserved")
		}
		started, completed := at.Add(time.Millisecond), at.Add(2*time.Millisecond)
		c.ActiveSampleStartedAt, c.ActiveSampleCompletedAt = &started, &completed
		p = publicCompaction(c, at, at)
		if p == nil || p.Active == nil || *p.Active != active || p.LastCompleted != nil ||
			p.ActiveSampleStartedAt == nil || !p.ActiveSampleStartedAt.Equal(started) ||
			p.ActiveSampleCompletedAt == nil || !p.ActiveSampleCompletedAt.Equal(completed) {
			t.Fatal("timed activity without a completed cycle was lost")
		}
		started, completed = at.Add(time.Hour), at.Add(time.Hour)
		if !p.ActiveSampleStartedAt.Equal(at.Add(time.Millisecond)) || !p.ActiveSampleCompletedAt.Equal(at.Add(2*time.Millisecond)) {
			t.Fatal("public activity dates alias the source")
		}
		c.Enabled = false
		if publicCompaction(c, at, at) != nil {
			t.Fatal("disabled worker reported activity")
		}
	}
	if p := publicCompaction(&Compaction{Enabled: true}, at, at); p == nil || p.Active != nil {
		t.Fatal("missing activity became known")
	}
	for _, mutate := range []func(*Compaction){
		func(c *Compaction) { c.ActiveSampleStartedAt = nil },
		func(c *Compaction) { c.ActiveSampleCompletedAt = nil },
		func(c *Compaction) { v := time.Time{}; c.ActiveSampleStartedAt = &v },
		func(c *Compaction) { v := at.Add(-time.Millisecond); c.ActiveSampleStartedAt = &v },
		func(c *Compaction) { v := at.Add(-time.Millisecond); c.ActiveSampleCompletedAt = &v },
		func(c *Compaction) { v := at.Add(2 * time.Second); c.ActiveSampleCompletedAt = &v },
		func(c *Compaction) {
			started, completed := at.Add(3*time.Second), at.Add(3*time.Second+time.Millisecond)
			c.ActiveSampleStartedAt, c.ActiveSampleCompletedAt = &started, &completed
		},
		func(c *Compaction) { v := at.Add(time.Second + 2*time.Millisecond); c.ActiveSampleCompletedAt = &v },
		func(c *Compaction) { c.Active = nil },
	} {
		c := maximumCompaction(at)
		active, started, completed := true, at.Add(time.Millisecond), at.Add(2*time.Millisecond)
		c.Active, c.ActiveSampleStartedAt, c.ActiveSampleCompletedAt = &active, &started, &completed
		mutate(c)
		p := publicCompaction(c, at, at.Add(time.Second))
		if p == nil || p.Active != nil || p.ActiveSampleStartedAt != nil || p.ActiveSampleCompletedAt != nil || p.LastCompleted == nil {
			t.Fatal("invalid activity timing survived or concealed the independent historical cycle")
		}
	}
	s := resourceFixture(at)
	active, started, completed := false, at.Add(time.Millisecond), at.Add(2*time.Millisecond)
	s.Resources.Compaction = &Compaction{Enabled: true, Active: &active, ActiveSampleStartedAt: &started, ActiveSampleCompletedAt: &completed}
	p := publicResources(&s, "fresh", at.Add(31*time.Second))
	if p.State != "stale" || p.Compaction == nil || p.Compaction.Active == nil || *p.Compaction.Active ||
		!p.Compaction.ActiveSampleStartedAt.Equal(started) || !p.Compaction.ActiveSampleCompletedAt.Equal(completed) {
		t.Fatal("activity timing renewed resource freshness or lost the original observation")
	}
}
