package accounts

import (
	"sync"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pk(b byte) solana.PublicKey { return solana.PublicKey{b} }

func baseWith(accts ...*Account) MemAccounts {
	m := NewMemAccounts()
	for _, a := range accts {
		_ = m.SetAccountWithoutLock(a.Key, a)
	}
	return m
}

// Reads for keys not written on the branch fall through to the parent.
func TestOverlayReadFallsThroughToParent(t *testing.T) {
	parent := baseWith(&Account{Key: pk(1), Lamports: 10})
	o := NewOverlayAccounts(parent)

	k := [32]byte(pk(1))
	got, err := o.GetAccount(&k)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), got.Lamports)
}

// A branch write shadows the parent value but must NOT mutate the parent.
func TestOverlayWriteShadowsParentWithoutMutatingIt(t *testing.T) {
	parent := baseWith(&Account{Key: pk(1), Lamports: 10})
	o := NewOverlayAccounts(parent)

	k := [32]byte(pk(1))
	require.NoError(t, o.SetAccount(&k, &Account{Key: pk(1), Lamports: 20}))

	got, _ := o.GetAccount(&k)
	assert.Equal(t, uint64(20), got.Lamports, "overlay sees its own write")

	pGot, _ := parent.GetAccount(&k)
	assert.Equal(t, uint64(10), pGot.Lamports, "parent is unchanged")
}

// Two sibling overlays over the same parent are isolated (the MVCC property).
func TestOverlaySiblingIsolation(t *testing.T) {
	parent := baseWith(&Account{Key: pk(1), Lamports: 10})
	o1 := NewOverlayAccounts(parent)
	o2 := NewOverlayAccounts(parent)

	k := [32]byte(pk(1))
	require.NoError(t, o1.SetAccount(&k, &Account{Key: pk(1), Lamports: 100}))
	require.NoError(t, o2.SetAccount(&k, &Account{Key: pk(1), Lamports: 200}))

	g1, _ := o1.GetAccount(&k)
	g2, _ := o2.GetAccount(&k)
	pg, _ := parent.GetAccount(&k)
	assert.Equal(t, uint64(100), g1.Lamports)
	assert.Equal(t, uint64(200), g2.Lamports)
	assert.Equal(t, uint64(10), pg.Lamports)
}

// AllAccounts = parent set with the branch delta applied on top.
func TestOverlayAllAccountsMerges(t *testing.T) {
	parent := baseWith(
		&Account{Key: pk(1), Lamports: 10},
		&Account{Key: pk(2), Lamports: 20},
	)
	o := NewOverlayAccounts(parent)
	k2 := [32]byte(pk(2))
	k3 := [32]byte(pk(3))
	require.NoError(t, o.SetAccount(&k2, &Account{Key: pk(2), Lamports: 25})) // override
	require.NoError(t, o.SetAccount(&k3, &Account{Key: pk(3), Lamports: 30})) // new

	byKey := map[solana.PublicKey]uint64{}
	for _, a := range o.AllAccounts() {
		byKey[a.Key] = a.Lamports
	}
	assert.Equal(t, uint64(10), byKey[pk(1)])
	assert.Equal(t, uint64(25), byKey[pk(2)])
	assert.Equal(t, uint64(30), byKey[pk(3)])
	assert.Len(t, byKey, 3)
}

// DeltaAccounts returns only what changed on the branch (for promotion).
func TestOverlayDeltaAccountsOnlyChanged(t *testing.T) {
	parent := baseWith(&Account{Key: pk(1), Lamports: 10})
	o := NewOverlayAccounts(parent)
	k2 := [32]byte(pk(2))
	require.NoError(t, o.SetAccount(&k2, &Account{Key: pk(2), Lamports: 20}))

	delta := o.DeltaAccounts()
	require.Len(t, delta, 1)
	assert.Equal(t, pk(2), delta[0].Key)
}

// A miss in both delta and parent surfaces the parent's not-found error.
func TestOverlayMissReturnsParentError(t *testing.T) {
	parent := baseWith(&Account{Key: pk(1), Lamports: 10})
	o := NewOverlayAccounts(parent)
	k := [32]byte(pk(99))
	_, err := o.GetAccount(&k)
	assert.Error(t, err)
}

// Overlays can stack (branch over branch over base) and reads cascade.
func TestOverlayStacksCascade(t *testing.T) {
	base := baseWith(&Account{Key: pk(1), Lamports: 10})
	mid := NewOverlayAccounts(base)
	k2 := [32]byte(pk(2))
	require.NoError(t, mid.SetAccount(&k2, &Account{Key: pk(2), Lamports: 20}))

	top := NewOverlayAccounts(mid)
	k1 := [32]byte(pk(1))
	got1, err := top.GetAccount(&k1) // from base, through mid
	require.NoError(t, err)
	assert.Equal(t, uint64(10), got1.Lamports)
	got2, err := top.GetAccount(&k2) // from mid
	require.NoError(t, err)
	assert.Equal(t, uint64(20), got2.Lamports)
}

// The WithoutLock hot-path variants behave like the locked ones.
func TestOverlayWithoutLockVariants(t *testing.T) {
	parent := baseWith(&Account{Key: pk(1), Lamports: 10})
	o := NewOverlayAccounts(parent)

	got, err := o.GetAccountWithoutLock(pk(1)) // falls through to parent
	require.NoError(t, err)
	assert.Equal(t, uint64(10), got.Lamports)

	require.NoError(t, o.SetAccountWithoutLock(pk(1), &Account{Key: pk(1), Lamports: 20}))
	got, _ = o.GetAccountWithoutLock(pk(1)) // shadowed by delta
	assert.Equal(t, uint64(20), got.Lamports)

	pGot, _ := parent.GetAccountWithoutLock(pk(1))
	assert.Equal(t, uint64(10), pGot.Lamports) // parent untouched
}

// WorkingSet: added slots shadow the durable store; newest slot wins.
func TestWorkingSetAddAndRead(t *testing.T) {
	durable := baseWith(&Account{Key: pk(1), Lamports: 10})
	u := NewWorkingSet()

	u.Add(101, []*Account{{Key: pk(1), Lamports: 11}, {Key: pk(2), Lamports: 20}, nil})

	g1, _ := uoRead(u, durable, 1)
	assert.Equal(t, uint64(11), g1.Lamports, "overlay shadows durable")
	g2, _ := uoRead(u, durable, 2)
	assert.Equal(t, uint64(20), g2.Lamports, "overlay-only key")
	_, err := uoRead(u, durable, 3)
	assert.Error(t, err, "miss -> durable miss")

	u.Add(102, []*Account{{Key: pk(1), Lamports: 99}})
	g1, _ = uoRead(u, durable, 1)
	assert.Equal(t, uint64(99), g1.Lamports, "newest slot wins")
	g2, _ = uoRead(u, durable, 2)
	assert.Equal(t, uint64(20), g2.Lamports, "unchanged key still from slot 101")
	assert.Equal(t, 2, u.HeldSlots())
}

// WorkingSet: keys never written unrooted fall through to the durable store.
func TestWorkingSetFallsThroughToDurable(t *testing.T) {
	durable := baseWith(&Account{Key: pk(5), Lamports: 50})
	u := NewWorkingSet()
	u.Add(101, []*Account{{Key: pk(1), Lamports: 11}})

	g, err := uoRead(u, durable, 5)
	require.NoError(t, err)
	assert.Equal(t, uint64(50), g.Lamports, "durable value, untouched by overlay")
}

// Concurrent locked reads racing Add/PromotePrefix/EvictFrom must be race-free.
func TestWorkingSetConcurrentStress(t *testing.T) {
	u := NewWorkingSet()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() { // mutator
		defer wg.Done()
		for s := uint64(1); s <= 400; s++ {
			u.Add(s, []*Account{{Key: pk(1), Lamports: s}, {Key: pk(byte(s % 7)), Lamports: s}})
			if s%32 == 0 {
				u.PromotePrefix(s - 16)
			}
			if s%50 == 0 {
				u.EvictFrom(s)
			}
		}
	}()
	for r := 0; r < 8; r++ { // concurrent readers
		wg.Add(1)
		go func() {
			defer wg.Done()
			k := [32]byte(pk(1))
			for i := 0; i < 1000; i++ {
				_, _ = u.Lookup(k)
				_ = u.HeldSlots()
			}
		}()
	}
	wg.Wait()
}

func uoAcct(b byte, lamports uint64) *Account { return &Account{Key: pk(b), Lamports: lamports} }

// uoRead mirrors the production read composition: unrooted Lookup first, else
// fall through to the durable store.
func uoRead(u *WorkingSet, durable Accounts, b byte) (*Account, error) {
	if a, ok := u.Lookup([32]byte(pk(b))); ok {
		return a, nil
	}
	k := [32]byte(pk(b))
	return durable.GetAccount(&k)
}

// PromotePrefix TRAP: a key written in both the dropped prefix AND a newer held
// slot must keep the newer held value, not fall through to the promoted one.
func TestWorkingSetPromoteKeepsNewerHeld(t *testing.T) {
	durable := baseWith()
	u := NewWorkingSet()
	u.Add(5, []*Account{uoAcct(1, 5)})
	u.Add(7, []*Account{uoAcct(1, 7)})
	require.NoError(t, durable.SetAccountWithoutLock(pk(1), uoAcct(1, 5))) // caller commits slot 5
	u.PromotePrefix(5)

	g, _ := uoRead(u, durable, 1)
	assert.Equal(t, uint64(7), g.Lamports, "newest held slot 7 wins, not promoted slot 5")
	assert.Equal(t, 1, u.HeldSlots())
}

// PromotePrefix mixed: promoted key -> durable; key with a held tip -> tip; held-only -> held.
func TestWorkingSetPromoteMixed(t *testing.T) {
	durable := baseWith()
	u := NewWorkingSet()
	u.Add(5, []*Account{uoAcct(1, 51), uoAcct(2, 52)})
	u.Add(7, []*Account{uoAcct(2, 72), uoAcct(3, 73)})
	require.NoError(t, durable.SetAccountWithoutLock(pk(1), uoAcct(1, 51)))
	require.NoError(t, durable.SetAccountWithoutLock(pk(2), uoAcct(2, 52)))
	u.PromotePrefix(5)

	a, _ := uoRead(u, durable, 1)
	b, _ := uoRead(u, durable, 2)
	c, _ := uoRead(u, durable, 3)
	assert.Equal(t, uint64(51), a.Lamports, "A from durable")
	assert.Equal(t, uint64(72), b.Lamports, "B held tip, NOT promoted 52")
	assert.Equal(t, uint64(73), c.Lamports, "C held")
}

// PromotionPrefix returns held slots <= through, ascending, each carrying that
// slot's writes — and excludes slots above through.
func TestWorkingSetPromotionPrefixContents(t *testing.T) {
	u := NewWorkingSet()
	u.Add(5, []*Account{uoAcct(1, 51), uoAcct(2, 52)})
	u.Add(7, []*Account{uoAcct(2, 72)})
	u.Add(9, []*Account{uoAcct(3, 93)}) // above through, must be excluded

	batch := u.PromotionPrefix(7)
	require.Len(t, batch, 2, "only slots 5 and 7")
	assert.Equal(t, uint64(5), batch[0].Slot, "ascending")
	assert.Equal(t, uint64(7), batch[1].Slot)
	assert.Len(t, batch[0].Delta, 2, "slot 5 wrote two accounts")
	assert.Len(t, batch[1].Delta, 1, "slot 7 wrote one account")
}

// Full driver flow: commit the promotion batch to durable in slot order, then
// PromotePrefix. A key whose only writer was promoted reads from durable; a key
// with a newer still-held writer keeps the held value.
func TestWorkingSetPromotionPrefixDriverFlow(t *testing.T) {
	durable := baseWith()
	u := NewWorkingSet()
	u.Add(5, []*Account{uoAcct(1, 51), uoAcct(2, 52)})
	u.Add(7, []*Account{uoAcct(2, 72)}) // key 2 rewritten at tip
	u.Add(9, []*Account{uoAcct(3, 93)})

	// Driver: commit each prefix slot's delta to durable (simulates a fold).
	for _, sd := range u.PromotionPrefix(7) {
		for _, a := range sd.Delta {
			require.NoError(t, durable.SetAccountWithoutLock(a.Key, a))
		}
	}
	u.PromotePrefix(7)

	a, _ := uoRead(u, durable, 1) // only written in promoted slot 5 -> durable (51)
	b, _ := uoRead(u, durable, 2) // written 5 then 7; both promoted -> durable has 72 (slot 7 committed last)
	c, _ := uoRead(u, durable, 3) // still held in slot 9
	assert.Equal(t, uint64(51), a.Lamports, "A from durable")
	assert.Equal(t, uint64(72), b.Lamports, "B from durable, slot-7 value (committed last)")
	assert.Equal(t, uint64(93), c.Lamports, "C still held")
	assert.Equal(t, 1, u.HeldSlots(), "only slot 9 remains held")
}

// Lookup returns the newest held value without durable fall-through; misses
// report (nil,false) so the composing reader knows to consult durable.
func TestWorkingSetLookup(t *testing.T) {
	u := NewWorkingSet()
	u.Add(5, []*Account{uoAcct(1, 51)})
	u.Add(7, []*Account{uoAcct(1, 71), uoAcct(2, 72)}) // key 1 rewritten newer

	a, ok := u.Lookup([32]byte(pk(1)))
	require.True(t, ok)
	assert.Equal(t, uint64(71), a.Lamports, "newest held value")

	b, ok := u.Lookup([32]byte(pk(2)))
	require.True(t, ok)
	assert.Equal(t, uint64(72), b.Lamports)

	_, ok = u.Lookup([32]byte(pk(9)))
	assert.False(t, ok, "durable-only key is NOT a Lookup hit (reader falls through)")

	// After promoting the whole tail, nothing is held -> all misses.
	u.PromotePrefix(7)
	_, ok = u.Lookup([32]byte(pk(1)))
	assert.False(t, ok, "promoted key falls through to durable")
}

// EvictFrom TRAP: revert to the newest SURVIVING held value (newest-first), not
// the durable value and not the oldest held.
func TestWorkingSetEvictRevertsNewestSurviving(t *testing.T) {
	durable := baseWith(uoAcct(1, 100))
	u := NewWorkingSet()
	u.Add(10, []*Account{uoAcct(1, 10)})
	u.Add(12, []*Account{uoAcct(1, 12)})
	u.Add(15, []*Account{uoAcct(1, 15)})
	u.Add(18, []*Account{uoAcct(1, 18)})
	u.EvictFrom(13) // drop 15,18 -> newest surviving is slot 12

	g, _ := uoRead(u, durable, 1)
	assert.Equal(t, uint64(12), g.Lamports, "newest surviving held (12), not durable(100), not oldest(10)")
}

// EvictFrom with no surviving held writer reverts to durable.
func TestWorkingSetEvictToDurable(t *testing.T) {
	durable := baseWith(uoAcct(1, 100))
	u := NewWorkingSet()
	u.Add(10, []*Account{uoAcct(1, 10)})
	u.EvictFrom(10)

	g, _ := uoRead(u, durable, 1)
	assert.Equal(t, uint64(100), g.Lamports, "revert to durable")
	assert.Equal(t, 0, u.HeldSlots())
}

// Owner slot is the LAYER slot, not the account's payload Slot field.
func TestWorkingSetOwnerIsLayerSlotNotPayload(t *testing.T) {
	durable := baseWith()
	u := NewWorkingSet()
	u.Add(3, []*Account{{Key: pk(1), Lamports: 33, Slot: 3}})
	u.Add(8, []*Account{{Key: pk(1), Lamports: 88, Slot: 3}}) // payload Slot=3, layer slot=8
	require.NoError(t, durable.SetAccountWithoutLock(pk(1), uoAcct(1, 33)))
	u.PromotePrefix(3) // owner is LAYER 8 (>3) -> keep; acc.Slot(=3)<=3 would wrongly delete

	g, _ := uoRead(u, durable, 1)
	assert.Equal(t, uint64(88), g.Lamports, "owner=layer slot 8, kept")
}

// A zero-lamport (deleted) unrooted write shadows durable (tombstone); eviction reverts.
func TestWorkingSetTombstoneShadowsThenReverts(t *testing.T) {
	durable := baseWith(uoAcct(1, 100))
	u := NewWorkingSet()
	u.Add(7, []*Account{uoAcct(1, 0)})

	g, _ := uoRead(u, durable, 1)
	assert.Equal(t, uint64(0), g.Lamports, "tombstone shadows durable, not 100")
	u.EvictFrom(7)
	g, _ = uoRead(u, durable, 1)
	assert.Equal(t, uint64(100), g.Lamports, "revert to durable")
}

// EvictFrom dedups a key present in multiple removed layers.
func TestWorkingSetEvictDedup(t *testing.T) {
	durable := baseWith()
	u := NewWorkingSet()
	u.Add(10, []*Account{uoAcct(1, 10)})
	u.Add(12, []*Account{uoAcct(1, 12)})
	u.Add(14, []*Account{uoAcct(1, 14)})
	u.EvictFrom(11) // remove 12 & 14 (both write key 1) -> survive slot 10

	g, _ := uoRead(u, durable, 1)
	assert.Equal(t, uint64(10), g.Lamports)
}

// No-op edges and full drain.
func TestWorkingSetNoopsAndDrain(t *testing.T) {
	durable := baseWith()
	u := NewWorkingSet()
	u.Add(5, []*Account{uoAcct(2, 5)})
	u.EvictFrom(99)    // nothing >= 99
	u.PromotePrefix(0) // nothing <= 0
	assert.Equal(t, 1, u.HeldSlots())

	require.NoError(t, durable.SetAccountWithoutLock(pk(2), uoAcct(2, 5)))
	u.PromotePrefix(5)
	assert.Equal(t, 0, u.HeldSlots())
	g, _ := uoRead(u, durable, 2)
	assert.Equal(t, uint64(5), g.Lamports, "served from durable after full drain")
}

// DeltaAccounts includes an override of a key that also exists in the parent.
func TestOverlayDeltaAccountsIncludesOverride(t *testing.T) {
	parent := baseWith(&Account{Key: pk(1), Lamports: 10})
	o := NewOverlayAccounts(parent)
	k := [32]byte(pk(1))
	require.NoError(t, o.SetAccount(&k, &Account{Key: pk(1), Lamports: 99}))

	delta := o.DeltaAccounts()
	require.Len(t, delta, 1)
	assert.Equal(t, pk(1), delta[0].Key)
	assert.Equal(t, uint64(99), delta[0].Lamports)
}
