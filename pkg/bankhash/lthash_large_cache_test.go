package bankhash

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/gagliardetto/solana-go"
)

func fullDelta(prev, next *accounts.Account) *lthash.LtHash {
	var delta, scratch lthash.LtHash
	var hasher lthash.AccountHasher
	hasher.HashInto(&scratch, next)
	delta.Add(&scratch)
	hasher.HashInto(&scratch, prev)
	delta.Sub(&scratch)
	return &delta
}

func cacheDelta(t *testing.T, c *largeHashCache, mode int, slot uint64, prev, next *accounts.Account) *lthash.LtHash {
	t.Helper()
	var delta, scratch lthash.LtHash
	var hasher lthash.AccountHasher
	if !c.addDelta(mode, slot, prev, next, &delta, &scratch, &hasher) {
		t.Fatal("cache entry unexpectedly busy")
	}
	return &delta
}

func largeTestAccount(rng *rand.Rand, size int) *accounts.Account {
	acct := &accounts.Account{Key: solana.PublicKey{42}, Lamports: 5_000_000, Owner: [32]byte{7}, Data: make([]byte, size)}
	rng.Read(acct.Data)
	return acct
}

func withSmallTreeThreshold(t *testing.T) {
	t.Helper()
	previous := ltHashTreeMinBytes
	ltHashTreeMinBytes = 64 << 10
	t.Cleanup(func() { ltHashTreeMinBytes = previous })
}

// A chain of blocks modifying one large account must produce the full-hash
// delta at every step, in both modes, while the cache really is in use: the
// old hash is reused and the new hash comes from the tree.
func TestLargeHashCacheChainMatchesFullHash(t *testing.T) {
	withSmallTreeThreshold(t)
	for _, mode := range []int{ltHashLargeVerify, ltHashLargeOn} {
		rng := rand.New(rand.NewSource(11))
		c := newLargeHashCache()
		c.buildInline = true
		prev := largeTestAccount(rng, 300_000)
		for step := uint64(0); step < 40; step++ {
			next := prev.Clone()
			switch step % 5 {
			case 0:
				next.Lamports += 3
			case 1:
				next.Owner[3] ^= 1
			default:
				// Few enough edits that the tree stays worthwhile for this size.
				for edits := 1 + rng.Intn(8); edits > 0; edits-- {
					next.Data[rng.Intn(len(next.Data))] ^= byte(1 + rng.Intn(255))
				}
			}
			got, want := cacheDelta(t, c, mode, 100+step, prev, next), fullDelta(prev, next)
			if !bytes.Equal(got.Hash(), want.Hash()) {
				t.Fatalf("mode %d step %d: cached delta differs from the full-hash delta", mode, step)
			}
			prev = next
		}
		if c.mismatches.Load() != 0 {
			t.Fatalf("mode %d: %d mismatches", mode, c.mismatches.Load())
		}
		if c.treeNew.Load() < 35 {
			t.Fatalf("mode %d: the tree was used only %d times in 40 steps", mode, c.treeNew.Load())
		}
		if mode == ltHashLargeOn && c.reusedOld.Load() < 39 {
			t.Fatalf("the remembered old hash was reused only %d times", c.reusedOld.Load())
		}
		if mode == ltHashLargeVerify && c.reusedOld.Load() != 0 {
			t.Fatal("verify mode must hash every old value in full")
		}
	}
}

// Whatever the cache cannot prove must be hashed in full and still be right:
// a parent that is not the remembered object, a parent with equal bytes in a
// different object, a resized account, and a change to most of the data.
func TestLargeHashCacheFallsBackWhenParentIsNotRemembered(t *testing.T) {
	withSmallTreeThreshold(t)
	rng := rand.New(rand.NewSource(12))
	c := newLargeHashCache()
	c.buildInline = true
	check := func(name string, prev, next *accounts.Account) {
		t.Helper()
		before := c.reusedOld.Load() + c.treeNew.Load()
		got, want := cacheDelta(t, c, ltHashLargeOn, 7, prev, next), fullDelta(prev, next)
		if !bytes.Equal(got.Hash(), want.Hash()) {
			t.Fatalf("%s: delta differs from the full-hash delta", name)
		}
		if c.reusedOld.Load()+c.treeNew.Load() != before {
			t.Fatalf("%s: the cache was trusted", name)
		}
	}
	s0 := largeTestAccount(rng, 200_000)
	s1 := s0.Clone()
	s1.Data[5] ^= 1
	check("first sight", s0, s1)

	fork := s0.Clone()
	fork.Data[9] ^= 1
	check("other fork of the same parent", s0, fork)

	twin := fork.Clone() // same bytes as the remembered value, different object
	child := twin.Clone()
	child.Data[11] ^= 1
	check("equal bytes in a different object", twin, child)

	grown := child.Clone()
	grown.Data = append(grown.Data, 1, 2, 3)
	before := c.treeNew.Load()
	got, want := cacheDelta(t, c, ltHashLargeOn, 8, child, grown), fullDelta(child, grown)
	if !bytes.Equal(got.Hash(), want.Hash()) || c.treeNew.Load() != before {
		t.Fatal("resized account: wrong delta or tree used")
	}

	rewritten := grown.Clone()
	rng.Read(rewritten.Data)
	before = c.treeNew.Load()
	got, want = cacheDelta(t, c, ltHashLargeOn, 9, grown, rewritten), fullDelta(grown, rewritten)
	if !bytes.Equal(got.Hash(), want.Hash()) || c.treeNew.Load() != before {
		t.Fatal("rewritten account: wrong delta or tree used")
	}
	if c.mismatches.Load() != 0 {
		t.Fatalf("%d mismatches", c.mismatches.Load())
	}
}

// Verify mode must catch a wrong remembered hash and a wrong tree, report
// them, and still return the full-hash delta.
func TestLargeHashCacheVerifyCatchesCorruption(t *testing.T) {
	withSmallTreeThreshold(t)
	rng := rand.New(rand.NewSource(13))
	c := newLargeHashCache()
	c.buildInline = true
	s0 := largeTestAccount(rng, 200_000)
	s1 := s0.Clone()
	s1.Data[1] ^= 1
	cacheDelta(t, c, ltHashLargeVerify, 1, s0, s1)

	e := c.entries[s1.Key]
	e.hash.Add(&e.hash) // corrupt the remembered hash
	s2 := s1.Clone()
	s2.Data[2] ^= 1
	got, want := cacheDelta(t, c, ltHashLargeVerify, 2, s1, s2), fullDelta(s1, s2)
	if !bytes.Equal(got.Hash(), want.Hash()) || c.mismatches.Load() != 1 {
		t.Fatalf("corrupt remembered hash: mismatches=%d", c.mismatches.Load())
	}

	// Make the tree describe a different value than the entry remembers.
	s3 := s2.Clone()
	s3.Data[3] ^= 1
	cacheDelta(t, c, ltHashLargeVerify, 3, s2, s3)
	stale := s3.Clone()
	stale.Data[150_000] ^= 1
	e.tree = lthash.BuildAccountHashTree(stale, 1)
	s4 := s3.Clone()
	s4.Data[4] ^= 1
	got, want = cacheDelta(t, c, ltHashLargeVerify, 4, s3, s4), fullDelta(s3, s4)
	if !bytes.Equal(got.Hash(), want.Hash()) || c.mismatches.Load() != 2 {
		t.Fatalf("stale tree: mismatches=%d", c.mismatches.Load())
	}
}

// In the on mode a self-check must return the full-hash delta for the use it
// checks, and a difference must switch the cache off for later uses.
func TestLargeHashCacheSelfCheckSwitchesOff(t *testing.T) {
	withSmallTreeThreshold(t)
	rng := rand.New(rand.NewSource(17))
	c := newLargeHashCache()
	c.buildInline = true
	c.selfCheckEvery = 4
	prev := largeTestAccount(rng, 200_000)
	step := func(slot uint64) (got, want *lthash.LtHash, used bool) {
		next := prev.Clone()
		next.Data[rng.Intn(len(next.Data))] ^= 1
		var delta, scratch lthash.LtHash
		var hasher lthash.AccountHasher
		used = c.addDelta(ltHashLargeOn, slot, prev, next, &delta, &scratch, &hasher)
		want = fullDelta(prev, next)
		prev = next
		return &delta, want, used
	}
	// Healthy: every use matches, checked or not, and the cache stays on.
	for slot := uint64(1); slot <= 12; slot++ {
		got, want, used := step(slot)
		if !used || !bytes.Equal(got.Hash(), want.Hash()) {
			t.Fatalf("slot %d: used=%t or wrong delta", slot, used)
		}
	}
	if c.mismatches.Load() != 0 || c.disabled.Load() {
		t.Fatalf("healthy chain: mismatches=%d disabled=%t", c.mismatches.Load(), c.disabled.Load())
	}
	// Corrupt the remembered hash. Uses 13 to 15 are unchecked; use 16 is
	// checked, must still be right, and must switch the cache off.
	e := c.entries[prev.Key]
	e.hash.Add(&e.hash)
	e.tree = nil
	for slot := uint64(13); slot <= 15; slot++ {
		step(slot)
		e.hash.Add(&e.hash) // keep it wrong: each use re-remembers a correct new hash
	}
	got, want, used := step(16)
	if !used || !bytes.Equal(got.Hash(), want.Hash()) {
		t.Fatal("the checked use did not return the full-hash delta")
	}
	if c.mismatches.Load() != 1 || !c.disabled.Load() {
		t.Fatalf("after the failed check: mismatches=%d disabled=%t", c.mismatches.Load(), c.disabled.Load())
	}
	if _, _, used := step(17); used {
		t.Fatal("the cache was used after switching itself off")
	}
}

// The entry table stays bounded and a busy entry is refused, not shared.
func TestLargeHashCacheIsBoundedAndExclusive(t *testing.T) {
	c := newLargeHashCache()
	for i := 0; i < 3*ltHashCacheEntries; i++ {
		e := c.acquire(solana.PublicKey{byte(i), byte(i >> 8)}, uint64(i))
		if e == nil {
			t.Fatalf("entry %d refused", i)
		}
		c.release(e)
	}
	if len(c.entries) > ltHashCacheEntries {
		t.Fatalf("%d entries", len(c.entries))
	}
	key := solana.PublicKey{200, 200}
	first := c.acquire(key, 1)
	if first == nil || c.acquire(key, 2) != nil {
		t.Fatal("a held entry must be refused")
	}
	c.release(first)
	if c.acquire(key, 3) == nil {
		t.Fatal("a released entry must be available")
	}
}
