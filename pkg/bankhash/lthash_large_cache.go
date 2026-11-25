package bankhash

import (
	"os"
	"sync"
	"sync/atomic"
	"weak"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

// Hashing a large account in full costs time in proportion to its size, twice
// per block that modifies it (old value and new value), while a block usually
// changes a few kilobytes of it. largeHashCache makes that cost follow what
// changed:
//
//   - It remembers the LtHash of the last value hashed for each large account.
//     When a block's parent value is that same object, its hash is already
//     known and is not computed again.
//   - For the largest accounts it also keeps an incremental hash tree of that
//     value, and computes the new value's hash by rehashing only the chunks
//     whose bytes differ.
//
// Anything it cannot prove falls back to hashing in full, exactly as before:
// a parent that is not the remembered object (another fork, a rewind, a value
// reloaded from disk), a changed data length, a tree that is still being
// built, or too many changed chunks. A remembered value is identified by a
// weak pointer to the immutable account object, so the cache keeps no account
// data alive and can never match a different object.
//
// MITHRIL_LTHASH_LARGE_CACHE selects the mode:
//
//	(unset)  on: cached and incremental hashes are used.
//	verify   the cache runs, but every hash is also computed in full, compared
//	         with the cached or incremental one, and the full one is used. A
//	         difference is logged and counted and drops the entry.
//	off      every account is hashed in full.
//
// In the on mode one use in ltHashSelfCheckEvery is checked the same way as in
// verify. A difference there means an assumption above no longer holds, so the
// cache also switches itself off for the rest of the process.
const (
	ltHashLargeOff = iota
	ltHashLargeVerify
	ltHashLargeOn
)

var ltHashLargeMode = func() int {
	switch os.Getenv("MITHRIL_LTHASH_LARGE_CACHE") {
	case "verify":
		return ltHashLargeVerify
	case "off", "0":
		return ltHashLargeOff
	}
	return ltHashLargeOn
}()

// ltHashCacheMinBytes is the data size from which an account's last hash is
// remembered, and ltHashCacheEntries bounds the remembered accounts. Each
// entry holds one 2 KiB hash, and a tree of 64 bytes per KiB of account data
// if it has one. Remembering smaller accounts costs more in bookkeeping than
// the hashing it saves.
const (
	ltHashCacheMinBytes = 128 << 10
	ltHashCacheEntries  = 64
	// ltHashSelfCheckEvery: one use in this many is verified in full.
	ltHashSelfCheckEvery = 64
)

const (
	ltHashTreeBuildWorkers = 4
)

// With the 16-lane kernel a tree costs about as much to build as hashing the
// account once and far less to update, so every remembered account of
// ltHashTreeMinBytes and up has one, built in place when it is missing. With
// only the portable hash, which is several times slower per byte than hashing
// in full, a tree pays just for the largest accounts when a small share of
// them changed, and is built in the background.
var (
	ltHashTreeInline = lthash.TreeAccelerated()
	// ltHashTreeMinBytes is the data size from which an account gets a tree.
	ltHashTreeMinBytes = map[bool]int{true: 256 << 10, false: 2 << 20}[ltHashTreeInline]
	// A tree is updated only while at most one chunk in
	// ltHashTreeMaxDirtyDivisor changed.
	ltHashTreeMaxDirtyDivisor = map[bool]int{true: 3, false: 20}[ltHashTreeInline]
)

type largeHashEntry struct {
	state   weak.Pointer[accounts.Account]
	dataLen int
	hash    lthash.LtHash
	tree    *lthash.AccountHashTree
	valid   bool
	inUse   bool
	gen     uint64
	lastUse uint64
}

type largeHashCache struct {
	mu      sync.Mutex
	entries map[solana.PublicKey]*largeHashEntry
	builds  chan struct{}
	// buildInline makes tree builds synchronous (tests).
	buildInline bool

	// selfCheckEvery is ltHashSelfCheckEvery for the node's cache; zero, the
	// default, never self-checks.
	selfCheckEvery uint64
	uses           atomic.Uint64
	// disabled is set when a self-check fails; addDelta then declines.
	disabled atomic.Bool

	reusedOld  atomic.Uint64
	treeNew    atomic.Uint64
	treeBuilt  atomic.Uint64
	fullNew    atomic.Uint64
	mismatches atomic.Uint64
}

func newLargeHashCache() *largeHashCache {
	return &largeHashCache{entries: make(map[solana.PublicKey]*largeHashEntry), builds: make(chan struct{}, 2)}
}

var ltHashLarge = func() *largeHashCache {
	c := newLargeHashCache()
	c.selfCheckEvery = ltHashSelfCheckEvery
	return c
}()

// acquire returns the entry for key for exclusive use, or nil if another
// computation holds it.
func (c *largeHashCache) acquire(key solana.PublicKey, slot uint64) *largeHashEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil {
		if len(c.entries) >= ltHashCacheEntries {
			var oldestKey solana.PublicKey
			var oldest *largeHashEntry
			for k, candidate := range c.entries {
				if !candidate.inUse && (oldest == nil || candidate.lastUse < oldest.lastUse) {
					oldestKey, oldest = k, candidate
				}
			}
			if oldest == nil {
				return nil
			}
			delete(c.entries, oldestKey)
		}
		e = &largeHashEntry{}
		c.entries[key] = e
	}
	if e.inUse {
		return nil
	}
	e.inUse = true
	e.lastUse = slot
	return e
}

func (c *largeHashCache) release(e *largeHashEntry) {
	c.mu.Lock()
	e.inUse = false
	c.mu.Unlock()
}

// addDelta adds next's hash to delta and subtracts prev's, where both have
// non-zero lamports and differ. It returns false, having done nothing, when
// the entry is held by another computation or the cache has switched itself
// off.
func (c *largeHashCache) addDelta(mode int, slot uint64, prev, next *accounts.Account, delta, scratch *lthash.LtHash, hasher *lthash.AccountHasher) bool {
	if c.disabled.Load() {
		return false
	}
	e := c.acquire(next.Key, slot)
	if e == nil {
		return false
	}
	defer c.release(e)

	// verify: also hash both values in full, compare and use the full hashes.
	verify := mode == ltHashLargeVerify
	if !verify && c.selfCheckEvery > 0 && c.uses.Add(1)%c.selfCheckEvery == 0 {
		verify = true
	}
	hit := e.valid && e.dataLen == len(prev.Data) && e.state == weak.Make(prev)

	// The old value: remembered, or hashed in full on its own goroutine while
	// this one produces the new value's hash.
	var oldHash lthash.LtHash
	var oldDone chan struct{}
	oldInline := false
	if hit && !verify {
		oldHash = e.hash
		c.reusedOld.Add(1)
	} else if len(prev.Data)+len(next.Data) >= ltHashLargeAccountBytes {
		oldDone = make(chan struct{})
		go func() {
			defer close(oldDone)
			var oldHasher lthash.AccountHasher
			oldHasher.HashInto(&oldHash, prev)
		}()
	} else {
		// Too small for a goroutine of its own to pay.
		hasher.HashInto(&oldHash, prev)
		oldInline = true
	}

	// The new value: through the tree when it describes prev and few chunks
	// changed, otherwise in full.
	var newHash lthash.LtHash
	usedTree := false
	if hit && e.tree != nil && len(next.Data) == e.dataLen {
		dirty := e.tree.DirtyChunks(prev, next)
		if len(dirty)*ltHashTreeMaxDirtyDivisor <= e.tree.Chunks() {
			e.tree.Update(next, dirty, &newHash)
			usedTree = true
			c.treeNew.Add(1)
		}
	}
	if !usedTree {
		e.tree = nil // it no longer describes the value being remembered
		if ltHashTreeInline && len(next.Data) >= ltHashTreeMinBytes && lthash.AccountHashTreeSupported(len(next.Data)) {
			e.tree = lthash.BuildAccountHashTree(next, ltHashTreeBuildWorkers)
			e.tree.HashInto(&newHash)
			usedTree = true
			c.treeBuilt.Add(1)
		}
	}
	if !usedTree || verify {
		hasher.HashInto(scratch, next)
		if usedTree && !scratch.Equals(&newHash) {
			c.reportMismatch(mode, slot, next, "incremental tree hash of the new value")
			e.tree = nil
		}
		newHash = *scratch
		if !usedTree {
			c.fullNew.Add(1)
		}
	}
	if oldDone != nil {
		<-oldDone
	}
	if oldDone != nil || oldInline {
		if hit && !oldHash.Equals(&e.hash) {
			c.reportMismatch(mode, slot, prev, "remembered hash of the old value")
			e.tree = nil
		}
	}

	delta.Add(&newHash)
	delta.Sub(&oldHash)

	e.state = weak.Make(next)
	e.dataLen = len(next.Data)
	e.hash = newHash
	e.valid = true
	e.gen++
	if e.tree == nil && !ltHashTreeInline && len(next.Data) >= ltHashTreeMinBytes && lthash.AccountHashTreeSupported(len(next.Data)) {
		c.buildTree(next, e, e.gen)
	}
	return true
}

func (c *largeHashCache) reportMismatch(mode int, slot uint64, acct *accounts.Account, what string) {
	c.mismatches.Add(1)
	if mode == ltHashLargeOn {
		// Found by a self-check: unchecked uses may be wrong in the same way.
		c.disabled.Store(true)
		mlog.Log.Errorf("lthash large-account cache: %s differs from the full hash at slot %d for %s (%d bytes); using the full hash and switching the cache off", what, slot, acct.Key, len(acct.Data))
		return
	}
	mlog.Log.Errorf("lthash large-account cache: %s differs from the full hash at slot %d for %s (%d bytes); using the full hash", what, slot, acct.Key, len(acct.Data))
}

// buildTree hashes acct into a tree off the caller's path and attaches it to e
// if e still remembers acct when the build finishes.
func (c *largeHashCache) buildTree(acct *accounts.Account, e *largeHashEntry, gen uint64) {
	attach := func() {
		tree := lthash.BuildAccountHashTree(acct, ltHashTreeBuildWorkers)
		c.mu.Lock()
		if !e.inUse && e.gen == gen && c.entries[acct.Key] == e {
			e.tree = tree
		}
		c.mu.Unlock()
	}
	if c.buildInline {
		// The caller holds the entry; attach directly.
		e.tree = lthash.BuildAccountHashTree(acct, 1)
		return
	}
	select {
	case c.builds <- struct{}{}:
	default:
		return // two builds already running; the next modification tries again
	}
	go func() {
		defer func() { <-c.builds }()
		attach()
	}()
}
