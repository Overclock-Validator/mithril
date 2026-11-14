package replay

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
)

func historyKey(i uint64) unchangedKey {
	var k unchangedKey
	binary.LittleEndian.PutUint64(k.account[:], i)
	binary.LittleEndian.PutUint64(k.account[24:], ^i)
	k.program = i*2654435761 | 1
	return k
}

func TestUnchangedHistoryRule(t *testing.T) {
	resetUnchangedHistory()
	defer resetUnchangedHistory()
	k := historyKey(1)
	if predictsUnchanged(k) {
		t.Fatal("predicted with no history")
	}
	recordUnchanged(k, true)
	recordUnchanged(k, true)
	if predictsUnchanged(k) {
		t.Fatal("predicted below the minimum number of observations")
	}
	recordUnchanged(k, true)
	if !predictsUnchanged(k) {
		t.Fatal("three unchanged observations should predict")
	}
	recordUnchanged(k, false)
	if predictsUnchanged(k) {
		t.Fatal("predicted right after a change")
	}
	// 3 of 5 unchanged, last unchanged: below 90%
	recordUnchanged(k, true)
	if predictsUnchanged(k) {
		t.Fatal("predicted below the minimum rate")
	}
	// reach 90%: 1 change in 10
	for range 5 {
		recordUnchanged(k, true)
	}
	if !predictsUnchanged(k) {
		t.Fatal("9 of 10 unchanged with the last unchanged should predict")
	}
	if predictsUnchanged(historyKey(2)) {
		t.Fatal("another key predicted")
	}
}

// The counters saturate by halving, which keeps the rate.
func TestUnchangedHistorySaturates(t *testing.T) {
	resetUnchangedHistory()
	defer resetUnchangedHistory()
	k := historyKey(7)
	for range 3 * unchangedCountMax {
		recordUnchanged(k, true)
	}
	same, total := unchangedCounts(findUnchanged(k))
	if total == 0 || total > unchangedCountMax || same != total {
		t.Fatalf("after saturation: unchanged %d of %d", same, total)
	}
	if !predictsUnchanged(k) {
		t.Fatal("an always unchanged key should predict after saturation")
	}
	recordUnchanged(k, false)
	if predictsUnchanged(k) {
		t.Fatal("predicted right after a change")
	}
}

// Far more keys than slots: every key recorded is either remembered exactly or
// forgotten, never confused with a key of the opposite history (short of a
// 32-bit tag collision within one pair of slots).
func TestUnchangedHistoryEviction(t *testing.T) {
	resetUnchangedHistory()
	defer resetUnchangedHistory()
	const keys = 4 * unchangedSlots
	for i := uint64(0); i < keys; i++ {
		for range 3 {
			recordUnchanged(historyKey(i), i%2 == 0)
		}
	}
	remembered, wrong := 0, 0
	for i := uint64(0); i < keys; i++ {
		word := findUnchanged(historyKey(i))
		if word == 0 {
			continue
		}
		remembered++
		if predictsUnchanged(historyKey(i)) != (i%2 == 0) {
			wrong++
		}
	}
	if remembered < unchangedSlots/2 || remembered > unchangedSlots {
		t.Fatalf("remembered %d keys in %d slots", remembered, unchangedSlots)
	}
	if wrong > 2 {
		t.Fatalf("%d of %d remembered keys had another key's history", wrong, remembered)
	}
}

func TestUnchangedHistoryConcurrent(t *testing.T) {
	resetUnchangedHistory()
	defer resetUnchangedHistory()
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range uint64(20000) {
				k := historyKey(i % 512)
				recordUnchanged(k, true)
				predictsUnchanged(historyKey(i + uint64(worker)))
			}
		}()
	}
	wg.Wait()
	for i := range uint64(512) {
		same, total := unchangedCounts(findUnchanged(historyKey(i)))
		if total == 0 || same != total {
			t.Fatalf("key %d: unchanged %d of %d", i, same, total)
		}
	}
}

// A refusal is classified from the word it was decided on, whatever the table
// holds by the time it is counted.
func TestUnchangedRefusalReasonUsesItsSnapshot(t *testing.T) {
	resetUnchangedHistory()
	defer resetUnchangedHistory()
	reset := func() {
		for _, c := range []*atomic.Int64{&unchangedReasons.noHistory, &unchangedReasons.fewObserved, &unchangedReasons.lastChanged, &unchangedReasons.lowRate} {
			c.Store(0)
		}
	}
	reset()
	defer reset()
	k := historyKey(11)
	recordUnchanged(k, false)
	recordUnchanged(k, true)
	decided := findUnchanged(k) // two observations: too few
	if wordPredictsUnchanged(decided) {
		t.Fatal("two observations predicted")
	}
	recordUnchanged(k, true) // a third arrives before the refusal is counted
	noteUnchangedRefusal(decided)
	if few, low := unchangedReasons.fewObserved.Load(), unchangedReasons.lowRate.Load(); few != 1 || low != 0 {
		t.Fatalf("few-observed %d, low-rate %d", few, low)
	}
	noteUnchangedRefusal(0)
	noteUnchangedRefusal(findUnchanged(k)) // 2 of 3, last unchanged
	recordUnchanged(k, false)
	noteUnchangedRefusal(findUnchanged(k))
	if no, low, last := unchangedReasons.noHistory.Load(), unchangedReasons.lowRate.Load(), unchangedReasons.lastChanged.Load(); no != 1 || low != 1 || last != 1 {
		t.Fatalf("no-history %d, low-rate %d, last-changed %d", no, low, last)
	}
}

// The head start is taken at the turn: a result that ended after it had none.
func TestWorkAheadHeadStart(t *testing.T) {
	turn := time.Now()
	if got := workAheadHeadStart(turn, turn.Add(-3*time.Millisecond)); got != 3*time.Millisecond {
		t.Fatalf("ready 3 ms before its turn: %v", got)
	}
	for _, ended := range []time.Time{turn, turn.Add(time.Millisecond), {}} {
		if got := workAheadHeadStart(turn, ended); got != 0 {
			t.Fatalf("ended %v against turn %v: %v", ended, turn, got)
		}
	}
	if got := workAheadHeadStart(time.Time{}, turn); got != 0 {
		t.Fatalf("no turn recorded: %v", got)
	}
}

func TestProgramKeyMatchesFNV(t *testing.T) {
	reference := func(ids []solana.PublicKey) uint64 {
		var programs []solana.PublicKey
		for _, program := range ids {
			if !slices.Contains(plumbingPrograms, program) && !slices.Contains(programs, program) {
				programs = append(programs, program)
			}
		}
		slices.SortFunc(programs, func(a, b solana.PublicKey) int { return bytes.Compare(a[:], b[:]) })
		h := fnv.New64a()
		for _, program := range programs {
			h.Write(program[:])
		}
		return h.Sum64() | 1
	}
	var ids []solana.PublicKey
	for i := range 12 {
		var key solana.PublicKey
		key[0], key[31] = byte(200-17*i), byte(i)
		ids = append(ids, key, key)
		if len(plumbingPrograms) > 0 {
			ids = append(ids, plumbingPrograms[i%len(plumbingPrograms)])
		}
		if got, want := programKeyOf(ids), reference(ids); got != want {
			t.Fatalf("%d programs: key %x, want %x", i+1, got, want)
		}
	}
	if got, want := programKeyOf(nil), reference(nil); got != want {
		t.Fatalf("no programs: key %x, want %x", got, want)
	}
}

func TestUnchangedHistoryDoesNotAllocate(t *testing.T) {
	resetUnchangedHistory()
	defer resetUnchangedHistory()
	ids := []solana.PublicKey{{1}, {2}, {3}}
	i := uint64(0)
	if n := testing.AllocsPerRun(200, func() {
		i++
		k := historyKey(i)
		k.program = programKeyOf(ids)
		recordUnchanged(k, true)
		predictsUnchanged(k)
	}); n != 0 {
		t.Fatalf("%v allocations per record and prediction", n)
	}
}

func BenchmarkRecordUnchanged(b *testing.B) {
	resetUnchangedHistory()
	defer resetUnchangedHistory()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			i++
			recordUnchanged(historyKey(i%(1<<19)), i%16 != 0)
		}
	})
}
