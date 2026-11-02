package replay

import (
	"math/rand"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func journalTestKey(i int) solana.PublicKey {
	return solana.PublicKey{0xC7, 0x4A, byte(i >> 8), byte(i)}
}

func voteCacheView(keys []solana.PublicKey) map[solana.PublicKey]*sealevel.VoteStateVersions {
	view := make(map[solana.PublicKey]*sealevel.VoteStateVersions)
	for _, k := range keys {
		if v, ok := global.VoteCacheItemOK(k); ok {
			view[k] = v
		}
	}
	return view
}

func clearVoteCacheKeys(keys []solana.PublicKey) {
	for _, k := range keys {
		global.DeleteVoteCacheItem(k)
	}
}

func TestVoteCacheJournalUnwindRestoresPreSuffixState(t *testing.T) {
	voteJournal.reset(true)
	t.Cleanup(func() { voteJournal.reset(false) })
	a, b, c := journalTestKey(1), journalTestKey(2), journalTestKey(3)
	keys := []solana.PublicKey{a, b, c}
	t.Cleanup(func() { clearVoteCacheKeys(keys) })

	a0, b0 := &sealevel.VoteStateVersions{}, &sealevel.VoteStateVersions{}
	global.PutVoteCacheItem(a, a0)
	global.PutVoteCacheItem(b, b0)

	a5 := &sealevel.VoteStateVersions{}
	putVoteCacheItem(&sealevel.SlotCtx{Slot: 5}, a, a5)

	s6 := &sealevel.SlotCtx{Slot: 6}
	putVoteCacheItem(s6, a, &sealevel.VoteStateVersions{})
	putVoteCacheItem(s6, a, &sealevel.VoteStateVersions{}) // second write in a slot keeps the first undo
	deleteVoteCacheItem(s6, b)
	putVoteCacheItem(s6, c, &sealevel.VoteStateVersions{})

	s7 := &sealevel.SlotCtx{Slot: 7}
	putVoteCacheItem(s7, c, &sealevel.VoteStateVersions{})
	putVoteCacheItem(s7, b, &sealevel.VoteStateVersions{})
	require.Equal(t, 3, voteJournal.heldSlots())

	require.Equal(t, 5, voteJournal.unwindFrom(6, nil))
	assert.Equal(t, map[solana.PublicKey]*sealevel.VoteStateVersions{a: a5, b: b0}, voteCacheView(keys))
	assert.Equal(t, 1, voteJournal.heldSlots(), "only the retained slot keeps records")

	voteJournal.dropThrough(5)
	assert.Equal(t, 0, voteJournal.heldSlots())
	assert.Equal(t, 0, voteJournal.unwindFrom(0, nil), "a folded slot can no longer be undone")
	assert.Same(t, a5, global.VoteCacheItem(a))
}

func TestVoteCacheJournalRecordsDeferredPublication(t *testing.T) {
	voteJournal.reset(true)
	t.Cleanup(func() { voteJournal.reset(false) })
	a, b := journalTestKey(11), journalTestKey(12)
	keys := []solana.PublicKey{a, b}
	t.Cleanup(func() { clearVoteCacheKeys(keys) })
	a0 := &sealevel.VoteStateVersions{}
	global.PutVoteCacheItem(a, a0)

	speculative := &sealevel.SlotCtx{Slot: 9, DeferVoteCachePublication: true}
	deleteVoteCacheItem(speculative, a)
	putVoteCacheItem(speculative, b, &sealevel.VoteStateVersions{})
	assert.Equal(t, 0, voteJournal.heldSlots(), "buffered writes are not recorded until published")
	assert.Same(t, a0, global.VoteCacheItem(a))

	publishDeferredVoteCache(speculative)
	assert.Equal(t, map[solana.PublicKey]*sealevel.VoteStateVersions{b: global.VoteCacheItem(b)}, voteCacheView(keys))
	voteJournal.unwindFrom(9, nil)
	assert.Equal(t, map[solana.PublicKey]*sealevel.VoteStateVersions{a: a0}, voteCacheView(keys))
}

func TestVoteCacheJournalDisabledRecordsNothing(t *testing.T) {
	voteJournal.reset(false)
	k := journalTestKey(21)
	t.Cleanup(func() { global.DeleteVoteCacheItem(k) })
	putVoteCacheItem(&sealevel.SlotCtx{Slot: 3}, k, &sealevel.VoteStateVersions{})
	assert.Equal(t, 0, voteJournal.heldSlots())
}

// After writing a retained prefix and an abandoned suffix, unwinding the
// suffix and writing the selected sibling must leave the cache exactly as
// writing only the prefix and the sibling would.
func TestVoteCacheJournalMatchesStraightLineReplay(t *testing.T) {
	const numKeys = 6
	keys := make([]solana.PublicKey, numKeys)
	for i := range keys {
		keys[i] = journalTestKey(100 + i)
	}
	t.Cleanup(func() {
		clearVoteCacheKeys(keys)
		voteJournal.reset(false)
	})

	type write struct {
		key    solana.PublicKey
		state  *sealevel.VoteStateVersions // nil = delete
		defers bool
	}
	randomSlot := func(r *rand.Rand) []write {
		ws := make([]write, r.Intn(5))
		for i := range ws {
			ws[i].key = keys[r.Intn(numKeys)]
			if r.Intn(3) > 0 {
				ws[i].state = &sealevel.VoteStateVersions{}
			}
		}
		defers := r.Intn(2) == 0
		for i := range ws {
			ws[i].defers = defers
		}
		return ws
	}
	apply := func(slot uint64, ws []write) {
		ctx := &sealevel.SlotCtx{Slot: slot}
		if len(ws) > 0 && ws[0].defers {
			ctx.DeferVoteCachePublication = true
		}
		for _, w := range ws {
			if w.state == nil {
				deleteVoteCacheItem(ctx, w.key)
			} else {
				putVoteCacheItem(ctx, w.key, w.state)
			}
		}
		publishDeferredVoteCache(ctx)
	}
	setBase := func(base map[solana.PublicKey]*sealevel.VoteStateVersions) {
		clearVoteCacheKeys(keys)
		for k, v := range base {
			global.PutVoteCacheItem(k, v)
		}
	}

	r := rand.New(rand.NewSource(1))
	for iter := 0; iter < 500; iter++ {
		base := make(map[solana.PublicKey]*sealevel.VoteStateVersions)
		for _, k := range keys {
			if r.Intn(2) == 0 {
				base[k] = &sealevel.VoteStateVersions{}
			}
		}
		const first = uint64(100)
		prefix := make([][]write, r.Intn(3))
		for i := range prefix {
			prefix[i] = randomSlot(r)
		}
		abandoned := make([][]write, 1+r.Intn(3))
		for i := range abandoned {
			abandoned[i] = randomSlot(r)
		}
		sibling := make([][]write, 1+r.Intn(3))
		for i := range sibling {
			sibling[i] = randomSlot(r)
		}
		switchSlot := first + uint64(len(prefix))

		voteJournal.reset(true)
		setBase(base)
		for i, ws := range prefix {
			apply(first+uint64(i), ws)
		}
		for i, ws := range abandoned {
			apply(switchSlot+uint64(i), ws)
		}
		voteJournal.unwindFrom(switchSlot, nil)
		for i, ws := range sibling {
			apply(switchSlot+uint64(i), ws)
		}
		got := voteCacheView(keys)

		voteJournal.reset(false)
		setBase(base)
		for i, ws := range prefix {
			apply(first+uint64(i), ws)
		}
		for i, ws := range sibling {
			apply(switchSlot+uint64(i), ws)
		}
		want := voteCacheView(keys)

		require.Equal(t, len(want), len(got), "iteration %d", iter)
		for k, v := range want {
			require.Same(t, v, got[k], "iteration %d key %s", iter, k)
		}
	}
}
