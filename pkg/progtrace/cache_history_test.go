package progtrace

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func TestLookupHistoryDoesNotAcquireFutureAdmission(t *testing.T) {
	var buf bytes.Buffer
	defer Start(&buf, 32, 100)()
	key := [32]byte{11}
	lookup := EndLookup(BeginLookup(key)) // actual absent read is between these calls
	m := BeginMutation()
	EndMutation(m, key, 77, Now(), "admitted")
	if lookup.Known || lookup.PriorAdmit != 0 {
		t.Fatal("a future admission leaked into a completed lookup")
	}
	again := EndLookup(BeginLookup(key))
	if !again.Known || again.PriorSource != 77 || again.Ambiguous || again.PriorAdmit > again.T {
		t.Fatalf("stable subsequent lookup: %+v", again)
	}
}

func TestLookupHistoryMarksMutationOverlapUnknown(t *testing.T) {
	var buf bytes.Buffer
	defer Start(&buf, 32, 100)()
	key := [32]byte{12}
	m := BeginMutation()
	EndMutation(m, key, 80, Now(), "admitted")
	lookup := BeginLookup(key)
	m = BeginMutation()
	EndMutation(m, key, 81, Now(), "admitted")
	lookup = EndLookup(lookup)
	if !lookup.Ambiguous || lookup.PriorSource != 80 {
		t.Fatalf("lookup must keep its earlier snapshot and mark overlap: %+v", lookup)
	}
	first, second := BeginMutation(), BeginMutation()
	EndMutation(second, key, 82, Now(), "admitted")
	EndMutation(first, key, 83, Now(), "admitted")
	if o := EndLookup(BeginLookup(key)); !o.Ambiguous {
		t.Fatal("concurrent publication order was asserted definite")
	}
	m = BeginMutation()
	EndMutation(m, key, 84, Now(), "admitted")
	if o := EndLookup(BeginLookup(key)); o.Ambiguous || o.PriorSource != 84 {
		t.Fatalf("a later serialized publication should restore usable history: %+v", o)
	}
}

func TestRefusalAndClearInvalidateRememberedAdmission(t *testing.T) {
	var buf bytes.Buffer
	defer Start(&buf, 32, 100)()
	key := [32]byte{13}
	m := BeginMutation()
	EndMutation(m, key, 90, Now(), "admitted")
	m = BeginMutation()
	EndMutation(m, key, 91, Now(), "refused")
	if o := EndLookup(BeginLookup(key)); !o.Known || o.InvalidatedAt == 0 || o.Ambiguous {
		t.Fatalf("refused refresh must invalidate predecessor: %+v", o)
	}
	m = BeginMutation()
	EndMutation(m, key, 92, Now(), "admitted")
	m = BeginMutation()
	EndMutation(m, [32]byte{}, 0, Now(), "clear")
	if o := EndLookup(BeginLookup(key)); o.InvalidatedAt == 0 {
		t.Fatal("clear was not represented")
	}
}

func TestGracefulCompletionAccountsForAcceptedPrefix(t *testing.T) {
	var buf bytes.Buffer
	r := startRecorder(&buf, 8, 4000, nil, true)
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	release := make(chan struct{})
	ready.Add(4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				Emit(Foreground{Kind: "turn", T: Now()})
			}
			ready.Done()
			<-release
			for range 900 {
				Emit(Foreground{Kind: "turn", T: Now()})
			}
		}()
	}
	ready.Wait()
	close(release)
	if err := r.stopWithin(time.Second); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	all := lines(t, &buf)
	if len(all) == 0 || all[len(all)-1]["kind"] != "complete" {
		t.Fatal("missing completion")
	}
	last := all[len(all)-1]
	if last["attempted_records"].(float64) < 400 {
		t.Fatal("known prefix not accounted")
	}
	if last["attempted_records"].(float64) != last["written_records"].(float64)+last["queue_full"].(float64)+last["over_limit"].(float64) {
		t.Fatalf("unaccounted accepted-prefix records: %v", last)
	}
	var written int
	for _, v := range all {
		if v["kind"] != "drops" && v["kind"] != "complete" {
			written++
		}
	}
	if float64(written) != last["written_records"] {
		t.Fatalf("written count: %d %v", written, last)
	}
}

type failedTraceWriter struct{}

func (failedTraceWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestTraceWriterFailureIsReturned(t *testing.T) {
	r := startRecorder(failedTraceWriter{}, 16, 100, nil, true)
	Emit(Foreground{Kind: "turn"})
	if err := r.stopWithin(time.Second); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("writer error lost: %v", err)
	}
}

func TestTraceDrainHasTimeout(t *testing.T) {
	w := &blockedWriter{release: make(chan struct{})}
	r := startRecorder(w, 16, 100, nil, true)
	Emit(Foreground{Kind: "turn"})
	if err := r.stopWithin(10 * time.Millisecond); err == nil {
		close(w.release)
		t.Fatal("blocked writer did not time out")
	}
	close(w.release)
	if err := r.stopWithin(time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestTraceWithActiveCacheOperationIsIncomplete(t *testing.T) {
	var buf bytes.Buffer
	r := startRecorder(&buf, 16, 100, nil, true)
	m := BeginMutation()
	err := r.stopWithin(time.Second)
	EndMutation(m, [32]byte{9}, 1, Now(), "admitted")
	if err == nil {
		t.Fatal("trace finished cleanly during cache mutation")
	}
	for _, v := range lines(t, &buf) {
		if v["kind"] == "complete" {
			t.Fatal("false clean completion")
		}
	}
}
