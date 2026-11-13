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
