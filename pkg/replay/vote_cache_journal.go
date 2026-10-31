package replay

import (
	"slices"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// The global vote cache is process-wide, but each write to it belongs to the
// slot whose execution made it. An in-loop fork unwind evicts the abandoned
// suffix's account writes from the working set; voteCacheJournal undoes the
// same suffix's vote-cache writes, so after the unwind the cache describes the
// retained parent exactly, without a reload from durable AccountsDB (which
// cannot see the retained unrooted slots).
//
// Each held slot records, for every key it changes, what the cache held before
// the slot's first change to that key. Unwinding applies the records newest
// slot first, so for a key that several unwound slots changed, the oldest
// slot's record applies last and restores the pre-suffix value. Rooting drops
// the records of folded slots, which can no longer be unwound.
//
// A recorded pointer stays the exact old value because cached vote states are
// never mutated in place: execution decodes a fresh state for every write, and
// every reader of the cache only reads.
//
// Only published writes are recorded. A speculative bank buffers its writes
// in its SlotCtx and either publishes them (recorded then) or discards them.
// A local leader bank publishes immediately at its own slot; replay's unwind
// leaves those records alone (see unwindFrom).
type voteCacheUndo struct {
	prev    *sealevel.VoteStateVersions
	existed bool
}

type voteCacheJournal struct {
	mu      sync.Mutex
	enabled bool
	slots   map[uint64]map[solana.PublicKey]voteCacheUndo
}

var voteJournal voteCacheJournal

// reset starts a replay run. Recording is enabled only when the run has an
// unrooted tail, the only state an in-loop unwind can roll back.
func (j *voteCacheJournal) reset(enabled bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.enabled = enabled
	j.slots = nil
}

// record notes key's current cache entry as slot's undo for it. It must run
// before slot's change to key reaches the cache. Writes to one key within a
// slot are serialized by account locks, so recording and then writing cannot
// interleave with another write to the same key.
func (j *voteCacheJournal) record(slot uint64, key solana.PublicKey) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.enabled {
		return
	}
	undo := j.slots[slot]
	if undo == nil {
		if j.slots == nil {
			j.slots = make(map[uint64]map[solana.PublicKey]voteCacheUndo)
		}
		undo = make(map[solana.PublicKey]voteCacheUndo, 4)
		j.slots[slot] = undo
	}
	if _, seen := undo[key]; seen {
		return
	}
	prev, existed := global.VoteCacheItemOK(key)
	undo[key] = voteCacheUndo{prev: prev, existed: existed}
}

// unwindFrom restores the cache to its state before slot fromSlot and drops
// the records of every slot >= fromSlot that owned reports (nil: every slot).
// The unrooted tail passes the slots it holds, so a local leader bank's
// records at a later slot survive, as its pending stake entries do. It returns
// the number of keys restored.
func (j *voteCacheJournal) unwindFrom(fromSlot uint64, owned func(slot uint64) bool) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	var suffix []uint64
	for slot := range j.slots {
		if slot >= fromSlot && (owned == nil || owned(slot)) {
			suffix = append(suffix, slot)
		}
	}
	slices.Sort(suffix)
	restored := 0
	for i := len(suffix) - 1; i >= 0; i-- {
		for key, u := range j.slots[suffix[i]] {
			if u.existed {
				global.PutVoteCacheItem(key, u.prev)
			} else {
				global.DeleteVoteCacheItem(key)
			}
			restored++
		}
		delete(j.slots, suffix[i])
	}
	return restored
}

// dropThrough forgets the records of slots <= through once they are durable.
func (j *voteCacheJournal) dropThrough(through uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for slot := range j.slots {
		if slot <= through {
			delete(j.slots, slot)
		}
	}
}

// heldSlots reports how many slots have records (tests and diagnostics).
func (j *voteCacheJournal) heldSlots() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.slots)
}
