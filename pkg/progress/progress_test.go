package progress

import (
	"io"
	"math"
	"sync"
	"testing"
	"time"
)

func observedProgress(t *testing.T) (*DualProgress, <-chan DualProgressObservation) {
	t.Helper()
	d := NewDualProgress()
	d.output = io.Discard
	events := make(chan DualProgressObservation, 32)
	if !d.SetObserver(func(o DualProgressObservation) {
		// This would deadlock if the callback were under the display mutex.
		d.IsInterrupted()
		events <- o
	}) {
		t.Fatal("observer rejected before Start")
	}
	return d, events
}

func progressEvent(t *testing.T, events <-chan DualProgressObservation) DualProgressObservation {
	t.Helper()
	select {
	case o := <-events:
		return o
	case <-time.After(2 * time.Second):
		t.Fatal("missing progress observation")
		return DualProgressObservation{}
	}
}

func TestDualProgressObservationLifecycle(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "interrupted"}[interrupted], func(t *testing.T) {
			d, events := observedProgress(t)
			d.SetDownloadTotal(10)
			d.Download.Add(12)
			d.Extract.Add(25)
			before := time.Now()
			d.Start()
			initial := progressEvent(t, events)
			if initial.State != "active" || initial.StartedAt.Before(before) || initial.CapturedAt != initial.StartedAt ||
				initial.CompressedInputBytes != 12 || initial.AccountPayloadBytes != 25 ||
				!initial.CompressedTotalKnown || initial.CompressedReportedTotal != 10 {
				t.Fatalf("initial observation: %+v", initial)
			}
			if d.SetObserver(nil) {
				t.Fatal("observer changed after Start")
			}
			if interrupted {
				d.Interrupt()
			} else {
				d.Stop()
			}
			terminal := progressEvent(t, events)
			want := "full_archive_complete"
			if interrupted {
				want = "full_archive_interrupted"
			}
			if terminal.State != want || terminal.StartedAt != initial.StartedAt || terminal.CapturedAt.Before(initial.CapturedAt) ||
				terminal.CompressedInputBytes != 12 || terminal.AccountPayloadBytes != 25 {
				t.Fatalf("terminal observation: %+v", terminal)
			}
			d.Stop()
			select {
			case extra := <-events:
				t.Fatalf("observation after terminal: %+v", extra)
			default:
			}
		})
	}
}

func TestDualProgressUnknownTotalStillObserved(t *testing.T) {
	d, events := observedProgress(t)
	d.Start()
	t.Cleanup(d.Stop)
	initial := progressEvent(t, events)
	var writers sync.WaitGroup
	writers.Add(2)
	go func() {
		defer writers.Done()
		for range 1000 {
			d.Download.Add(1)
		}
	}()
	go func() {
		defer writers.Done()
		for range 1000 {
			d.Extract.Add(2)
		}
	}()
	writers.Wait()
	periodic := progressEvent(t, events)
	if initial.CompressedTotalKnown || periodic.CompressedTotalKnown || periodic.State != "active" ||
		periodic.StartedAt != initial.StartedAt || !periodic.CapturedAt.After(initial.CapturedAt) ||
		periodic.CompressedInputBytes != 1000 || periodic.AccountPayloadBytes != 2000 {
		t.Fatalf("unknown-total periodic observation: %+v", periodic)
	}
	d.Stop()
	terminal := progressEvent(t, events)
	if terminal.State != "full_archive_complete" || terminal.CompressedInputBytes < periodic.CompressedInputBytes ||
		terminal.AccountPayloadBytes < periodic.AccountPayloadBytes {
		t.Fatalf("terminal counters: %+v", terminal)
	}
}

func TestDualProgressObservationRawBounds(t *testing.T) {
	d := NewDualProgress()
	d.startedAt = time.Now()
	for _, total := range []int64{math.MinInt64, -1, 0, 1, math.MaxInt64} {
		d.Download.SetTotal(total)
		d.Download.current.Store(math.MaxInt64)
		d.Extract.current.Store(math.MaxInt64)
		o, ok := d.observationLocked("active", time.Now())
		if !ok || o.CompressedInputBytes != math.MaxInt64 || o.AccountPayloadBytes != math.MaxInt64 ||
			o.CompressedTotalKnown != (total > 0) || (total > 0 && o.CompressedReportedTotal != uint64(total)) ||
			(total <= 0 && o.CompressedReportedTotal != 0) {
			t.Fatalf("total %d: %+v, valid %v", total, o, ok)
		}
	}
	for _, counters := range [][2]int64{{-1, 0}, {0, -1}, {math.MinInt64, math.MinInt64}} {
		d.Download.current.Store(counters[0])
		d.Extract.current.Store(counters[1])
		if o, ok := d.observationLocked("active", time.Now()); ok || o != (DualProgressObservation{}) {
			t.Fatalf("accepted negative counters %v: %+v", counters, o)
		}
	}
}

type blockedProgressOutput struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (w *blockedProgressOutput) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

func TestDualProgressFirstTerminalPreservedWhileOutputBlocked(t *testing.T) {
	d, events := observedProgress(t)
	w := &blockedProgressOutput{entered: make(chan struct{}), release: make(chan struct{})}
	d.output = w
	started, stopped, interrupted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	joins := []<-chan struct{}{started}
	var release sync.Once
	t.Cleanup(func() {
		release.Do(func() { close(w.release) })
		for _, done := range joins {
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("progress lifecycle did not join after releasing output")
			}
		}
	})
	go func() { d.Start(); close(started) }()
	initial := progressEvent(t, events)
	select {
	case <-w.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not enter output")
	}
	d.Download.Add(20)
	d.Extract.Add(30)
	joins = append(joins, stopped)
	go func() { d.Stop(); close(stopped) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		d.mu.Lock()
		done := d.done
		d.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Stop did not freeze terminal observation")
		}
		time.Sleep(time.Millisecond)
	}
	d.Download.Add(7)
	d.Extract.Add(9)
	joins = append(joins, interrupted)
	go func() { d.Interrupt(); close(interrupted) }()
	// A late Interrupt cannot rewrite the first terminal result or its counters.
	deadline = time.Now().Add(2 * time.Second)
	for !d.IsInterrupted() {
		if time.Now().After(deadline) {
			t.Fatal("late Interrupt did not run")
		}
		time.Sleep(time.Millisecond)
	}
	releasedAt := time.Now()
	release.Do(func() { close(w.release) })
	terminal := progressEvent(t, events)
	if terminal.State != "full_archive_complete" || terminal.StartedAt != initial.StartedAt ||
		terminal.CapturedAt.After(releasedAt) || terminal.CompressedInputBytes != 20 || terminal.AccountPayloadBytes != 30 {
		t.Fatalf("first terminal observation changed: %+v", terminal)
	}
	<-stopped
	select {
	case extra := <-events:
		t.Fatalf("observation after terminal: %+v", extra)
	default:
	}
}

func TestDualProgressNilObserver(t *testing.T) {
	d := NewDualProgress()
	d.output = io.Discard
	d.Start()
	d.Stop()
	if !d.startedAt.IsZero() || d.terminal != nil {
		t.Fatal("disabled observer captured a timestamp or terminal value")
	}
	if got := testing.AllocsPerRun(100, func() { d.publishObservation() }); got != 0 {
		t.Fatalf("disabled observation allocated %v times", got)
	}
}

func TestDualProgressBlockedObserverJoinsAfterRelease(t *testing.T) {
	d := NewDualProgress()
	d.output = io.Discard
	entered, release := make(chan struct{}), make(chan struct{})
	started, stopped := make(chan struct{}), make(chan struct{})
	events := make(chan DualProgressObservation, 4)
	var unblock sync.Once
	joins := []<-chan struct{}{started}
	t.Cleanup(func() {
		unblock.Do(func() { close(release) })
		for _, done := range joins {
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("progress lifecycle did not join after releasing observer")
			}
		}
	})
	if !d.SetObserver(func(o DualProgressObservation) {
		if o.State == "active" {
			close(entered)
			<-release
		}
		events <- o
	}) {
		t.Fatal("observer rejected before Start")
	}
	go func() { d.Start(); close(started) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("initial observer did not run")
	}
	joins = append(joins, stopped)
	go func() { d.Stop(); close(stopped) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		d.mu.Lock()
		done := d.done
		d.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("blocked observer held display mutex")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-stopped:
		t.Fatal("Stop returned before its owner could exit")
	default:
	}
	unblock.Do(func() { close(release) })
	if initial, terminal := progressEvent(t, events), progressEvent(t, events); initial.State != "active" ||
		terminal.State != "full_archive_complete" || terminal.CapturedAt.Before(initial.CapturedAt) {
		t.Fatalf("callback ordering: initial %+v, terminal %+v", initial, terminal)
	}
}
