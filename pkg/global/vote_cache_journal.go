package global

import (
	"slices"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// Vote states published to the cache are immutable. Retain only the first
// pre-write value of each key in each bank, not a copy of the whole vote map.
// Replay publishes banks in order; speculative leader/streaming banks buffer
// their changes until accepted. All journal and cache mutation shares one lock.
type voteCacheBefore struct {
	state  *sealevel.VoteStateVersions
	exists bool
}

func putVoteCacheLocked(key solana.PublicKey, state *sealevel.VoteStateVersions) {
	if instance.voteCache == nil {
		instance.voteCache = make(map[solana.PublicKey]*sealevel.VoteStateVersions)
	}
	instance.voteCache[key] = state
}

func recordVoteCacheBeforeLocked(slot uint64, key solana.PublicKey) {
	if instance.voteCacheUndo == nil {
		instance.voteCacheUndo = make(map[uint64]map[solana.PublicKey]voteCacheBefore)
	}
	changes := instance.voteCacheUndo[slot]
	if changes == nil {
		changes = make(map[solana.PublicKey]voteCacheBefore)
		instance.voteCacheUndo[slot] = changes
	}
	if _, recorded := changes[key]; !recorded {
		state, exists := instance.voteCache[key]
		changes[key] = voteCacheBefore{state: state, exists: exists}
	}
}

func PutVoteCacheItemForSlot(slot uint64, key solana.PublicKey, state *sealevel.VoteStateVersions) {
	instance.voteCacheMutex.Lock()
	defer instance.voteCacheMutex.Unlock()
	recordVoteCacheBeforeLocked(slot, key)
	putVoteCacheLocked(key, state)
}

func DeleteVoteCacheItemForSlot(slot uint64, key solana.PublicKey) {
	instance.voteCacheMutex.Lock()
	defer instance.voteCacheMutex.Unlock()
	recordVoteCacheBeforeLocked(slot, key)
	delete(instance.voteCache, key)
}

// WithVoteCacheJournalSlot attributes a synchronous epoch-boundary rebuild,
// including its joined workers, to the boundary bank. It is replay-loop only;
// producers must keep cache writes private until ordered replay adoption.
func WithVoteCacheJournalSlot(slot uint64, rebuild func()) {
	instance.voteCacheMutex.Lock()
	previous := instance.voteCacheJournalSlot
	instance.voteCacheJournalSlot = &slot
	instance.voteCacheMutex.Unlock()
	defer func() {
		instance.voteCacheMutex.Lock()
		instance.voteCacheJournalSlot = previous
		instance.voteCacheMutex.Unlock()
	}()
	rebuild()
}

// RewindVoteCache restores the accepted parent, including writes in the
// retained unrooted prefix. The caller must stop replay publication first.
// Descending order also handles repeated writes/deletes to the same key.
func RewindVoteCache(fromSlot uint64) {
	instance.voteCacheMutex.Lock()
	defer instance.voteCacheMutex.Unlock()
	slots := make([]uint64, 0, len(instance.voteCacheUndo))
	for slot := range instance.voteCacheUndo {
		if slot >= fromSlot {
			slots = append(slots, slot)
		}
	}
	slices.Sort(slots)
	for i := len(slots) - 1; i >= 0; i-- {
		for key, before := range instance.voteCacheUndo[slots[i]] {
			if before.exists {
				putVoteCacheLocked(key, before.state)
			} else {
				delete(instance.voteCache, key)
			}
		}
		delete(instance.voteCacheUndo, slots[i])
	}
}

// PruneVoteCacheJournal releases only changes made durable by an applied fold.
func PruneVoteCacheJournal(through uint64) {
	instance.voteCacheMutex.Lock()
	defer instance.voteCacheMutex.Unlock()
	for slot := range instance.voteCacheUndo {
		if slot <= through {
			delete(instance.voteCacheUndo, slot)
		}
	}
}

// ResetVoteCacheJournal starts a replay run whose initial cache is rebuilt
// from its durable checkpoint. It does not modify the cache itself.
func ResetVoteCacheJournal() {
	instance.voteCacheMutex.Lock()
	defer instance.voteCacheMutex.Unlock()
	instance.voteCacheUndo = nil
	instance.voteCacheJournalSlot = nil
}
