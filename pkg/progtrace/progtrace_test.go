package progtrace

import (
	"bufio"
	"bytes"
	"encoding/json"
	"sync"
	"testing"
)

func lines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(buf)
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("line %q: %v", sc.Text(), err)
		}
		out = append(out, rec)
	}
	return out
}

func TestOffByDefault(t *testing.T) {
	if On() {
		t.Fatal("recording without the switch")
	}
	Emit(Foreground{Kind: "turn"}) // no recorder: a no-op
	NoteAdmit([32]byte{1}, 1, 1)
	if _, _, _, ok := PriorAdmit([32]byte{1}); ok {
		t.Fatal("admission remembered while off")
	}
}

func TestRecordsAreJSONLines(t *testing.T) {
	var buf bytes.Buffer
	stop := Start(&buf, 16, 100)
	Emit(Foreground{Kind: "turn", Slot: 7, T: Now()})
	Emit(Job{Kind: "job", Slot: 8, ID: 1, Whole: true, Outcome: "finished"})
	stop()
	stop() // idempotent
	Emit(Foreground{Kind: "turn", Slot: 9})
	got := lines(t, &buf)
	if len(got) != 2 || got[0]["kind"] != "turn" || got[0]["slot"] != float64(7) || got[1]["outcome"] != "finished" {
		t.Fatalf("records: %v", got)
	}
}

// blockedWriter holds the writer goroutine until released.
type blockedWriter struct {
	release chan struct{}
	buf     bytes.Buffer
}

func (w *blockedWriter) Write(p []byte) (int, error) {
	<-w.release
	return w.buf.Write(p)
}

// A full queue and the record limit drop records, count them, and never block.
func TestBoundedOutputCountsDrops(t *testing.T) {
	w := &blockedWriter{release: make(chan struct{})}
	stop := Start(w, 4, 1000)
	big := make([]byte, 1<<17) // one record overflows the 64 KiB buffer, so the writer blocks
	for i := range big {
		big[i] = 'x'
	}
	for i := 0; i < 200; i++ {
		Emit(Publish{Kind: "invalidate", Reason: string(big)})
	}
	close(w.release)
	stop()
	var drops Drops
	sc := bufio.NewScanner(&w.buf)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	kept := 0
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		if rec["kind"] == "drops" {
			_ = json.Unmarshal(sc.Bytes(), &drops)
			continue
		}
		kept++
	}
	if drops.QueueFull == 0 || uint64(kept)+drops.QueueFull != 200 {
		t.Fatalf("kept %d, dropped %d of 200", kept, drops.QueueFull)
	}

	var buf bytes.Buffer
	stop = Start(&buf, 64, 3)
	for i := 0; i < 10; i++ {
		Emit(Foreground{Kind: "turn", Slot: uint64(i)})
	}
	stop()
	got := lines(t, &buf)
	if len(got) != 4 || got[3]["kind"] != "drops" || got[3]["over_limit"] != float64(7) {
		t.Fatalf("limit of 3: %v", got)
	}
}

func TestAdmissionHistory(t *testing.T) {
	var buf bytes.Buffer
	defer Start(&buf, 16, 100)()
	key := [32]byte{9}
	if _, _, _, ok := PriorAdmit(key); ok {
		t.Fatal("admission before any")
	}
	NoteInvalidate(key, 5) // unknown key: nothing to mark
	NoteAdmit(key, 0xabc, 10)
	at, source, invalidated, ok := PriorAdmit(key)
	if !ok || at != 10 || source != 0xabc || invalidated != 0 {
		t.Fatalf("admitted: %d %x %d %t", at, source, invalidated, ok)
	}
	NoteInvalidate(key, 20)
	if _, _, invalidated, _ = PriorAdmit(key); invalidated != 20 {
		t.Fatalf("invalidated at %d", invalidated)
	}
	NoteAdmit(key, 0xdef, 30) // a new admission clears the removal
	if at, source, invalidated, _ = PriorAdmit(key); at != 30 || source != 0xdef || invalidated != 0 {
		t.Fatalf("readmitted: %d %x %d", at, source, invalidated)
	}
	NoteInvalidateAll(40)
	if _, _, invalidated, _ = PriorAdmit(key); invalidated != 40 {
		t.Fatalf("cleared at %d", invalidated)
	}
	if Hex(0) != "" || Hex(0xabc) != "0000000000000abc" || SourceDigest([]byte("a")) == SourceDigest([]byte("b")) {
		t.Fatal("digest formatting")
	}
}

func TestConcurrentEmitAndStop(t *testing.T) {
	var buf bytes.Buffer
	stop := Start(&buf, 8, 1<<20)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				Emit(Foreground{Kind: "turn", Slot: uint64(i)})
				NoteAdmit([32]byte{byte(i)}, uint64(i), int64(i))
				PriorAdmit([32]byte{byte(i)})
			}
		}()
	}
	stop() // while emitters run
	wg.Wait()
	lines(t, &buf)
}
