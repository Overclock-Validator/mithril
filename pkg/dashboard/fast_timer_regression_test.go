package dashboard

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A delayed tick must be evaluated at processing time, not before a newer gate.
func TestFastDelayedTickerDoesNotInvalidateNewerGate(t *testing.T) {
	core := replayDetailLiveCore(0)
	genesis := "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
	core.Binding.GenesisHash = &genesis
	at := time.Now().UTC()
	value := FastExecuted{SchemaVersion: 1, Family: "recent_executed", Scope: "locally_executed", State: "fresh", Sequence: 1,
		SampledAt: at, Binding: FastExecutedBinding{RunID: core.Binding.RunID, GenesisHash: &genesis, NodeStartedAt: core.Binding.NodeStartedAt,
			Attempt: core.Binding.Attempt, Rewinds: core.Binding.Rewinds, AttemptStartedAt: core.Binding.AttemptStartedAt},
		Records: []ExecutedSlot{{At: at, Slot: 3, Bankhash: "11111111111111111111111111111111"}}}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	var calls atomic.Uint64
	feed, err := ListenFeedWithDetails(context.Background(), socketPath(t), "", func() Snapshot {
		if calls.Add(1) == 2 {
			close(entered)
			<-release
		}
		s := replayDetailLiveStatus(core)
		s.Replay.SampledAt = time.Now().UTC()
		return s
	}, DetailSamplers{Replay: func(at time.Time) (ReplayDetailCore, error) { c := core; c.CapturedAt = at.UTC(); return c, nil },
		FastExecuted: func() (FastExecuted, bool) { return value, true }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); feed.Close() })
	feed.fast.update(time.Now())
	var first *fastExecutedSample
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		first = feed.fast.latest.Load()
		if first != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if first == nil {
		t.Fatal("initial fast sample unavailable")
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("status sampler did not block")
	}
	time.Sleep(220 * time.Millisecond) // Delay the ordinary status sample before it publishes its new gate.
	releaseOnce.Do(func() { close(release) })
	select {
	case <-first.invalidated:
		t.Fatal("delayed ticker incorrectly invalidated a fresh source")
	case <-time.After(80 * time.Millisecond):
	}
}

// Status refreshes sample a new sequence without waiting for a private request.
func TestFastStatusRefreshPublishesNewSequence(t *testing.T) {
	core := replayDetailLiveCore(0)
	genesis := "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
	core.Binding.GenesisHash = &genesis
	at := time.Now().UTC()
	value := FastExecuted{SchemaVersion: 1, Family: "recent_executed", Scope: "locally_executed", State: "fresh", Sequence: 1,
		SampledAt: at, Binding: FastExecutedBinding{RunID: core.Binding.RunID, GenesisHash: &genesis, NodeStartedAt: core.Binding.NodeStartedAt,
			Attempt: core.Binding.Attempt, Rewinds: core.Binding.Rewinds, AttemptStartedAt: core.Binding.AttemptStartedAt},
		Records: []ExecutedSlot{{At: at, Slot: 3, Bankhash: "11111111111111111111111111111111"}}}
	var sequence atomic.Uint64
	sequence.Store(1)
	feed, err := ListenFeedWithDetails(context.Background(), socketPath(t), "", func() Snapshot {
		s := replayDetailLiveStatus(core)
		s.Replay.SampledAt = time.Now().UTC()
		return s
	}, DetailSamplers{Replay: func(at time.Time) (ReplayDetailCore, error) { c := core; c.CapturedAt = at.UTC(); return c, nil },
		FastExecuted: func() (FastExecuted, bool) { v := value; v.Sequence = sequence.Load(); return v, true }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	first := feed.fast.latest.Load()
	if first == nil {
		t.Fatal("status refresh did not publish initial fast sample")
	}
	sequence.Store(2)
	select {
	case <-first.invalidated:
	case <-time.After(3 * time.Second):
		t.Fatal("status refresh did not invalidate prior sequence")
	}
	if got := feed.fast.latest.Load(); got == nil || got.value.Sequence != 2 {
		t.Fatal("status refresh did not publish the new sequence")
	}
}
