package accountsdb

import (
	"sync"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/cespare/xxhash/v2"
	"github.com/gagliardetto/solana-go"
)

const commonTierShards = 64
const commonTierWays = 4

// No pointers in either slot array: retained small accounts do not contribute
// per-account pointers to the GC scan set. Gets return owned copies; eviction
// and refresh cannot mutate an account already handed to a caller.
type commonTierHeader struct {
	key                                      solana.PublicKey
	owner                                    [32]byte
	slot, lamports, rent                     uint64
	length                                   uint16
	used, recent, executable, dummy, nilData bool
}
type commonTierSmall struct {
	commonTierHeader
	data [200]byte
}
type commonTierShard struct {
	mu    sync.Mutex
	empty []commonTierHeader
	small []commonTierSmall
}
type commonAccountCache struct {
	large   *WeightedCache[solana.PublicKey, *accounts.Account]
	enabled bool
	shards  [commonTierShards]commonTierShard
}

func newCommonAccountCache(budget uint64, enabled bool) *commonAccountCache {
	c := &commonAccountCache{enabled: enabled}
	if !enabled || budget < 1<<20 {
		c.enabled = false
		c.large = NewWeightedCache(budget, func(_ solana.PublicKey, a *accounts.Account) uint32 { return commonAccountCacheCost(a) })
		return c
	}
	// Split the existing budget, never add a second independent full budget.
	// Reserve modest small tiers: large program-data accounts must still fit
	// the large tier and its per-entry admission ceiling.
	emptyShare, smallShare := budget/16, budget/8
	if CommonAccountCacheLayout == "expanded" {
		emptyShare, smallShare = budget/6, budget/2
	}

	emptyN := int(emptyShare/commonTierShards/uint64(unsafe.Sizeof(commonTierHeader{}))/commonTierWays) * commonTierWays
	smallN := int(smallShare/commonTierShards/uint64(unsafe.Sizeof(commonTierSmall{}))/commonTierWays) * commonTierWays
	for i := range c.shards {
		c.shards[i].empty = make([]commonTierHeader, emptyN)
		c.shards[i].small = make([]commonTierSmall, smallN)
	}
	c.large = NewWeightedCache(budget-emptyShare-smallShare, func(_ solana.PublicKey, a *accounts.Account) uint32 { return commonAccountCacheCost(a) })
	return c
}
func (c *commonAccountCache) locate(k solana.PublicKey) (*commonTierShard, uint64) {
	h := xxhash.Sum64(k[:])
	return &c.shards[h%commonTierShards], h / commonTierShards
}
func tierStart(hash uint64, n int) int { return int(hash%uint64(n/commonTierWays)) * commonTierWays }
func tierHeader(k solana.PublicKey, a *accounts.Account) commonTierHeader {
	return commonTierHeader{key: k, owner: a.Owner, slot: a.Slot, lamports: a.Lamports, rent: a.RentEpoch, length: uint16(len(a.Data)), used: true, recent: true, executable: a.Executable, dummy: a.IsDummy, nilData: a.Data == nil}
}
func (h *commonTierHeader) account(data []byte) *accounts.Account {
	a := &accounts.Account{Key: h.key, Owner: h.owner, Slot: h.slot, Lamports: h.lamports, RentEpoch: h.rent, Executable: h.executable, IsDummy: h.dummy}
	if !h.nilData {
		a.Data = make([]byte, int(h.length))
		copy(a.Data, data)
	}
	return a
}
func (s *commonTierShard) remove(k solana.PublicKey, hash uint64) {
	start := tierStart(hash, len(s.empty))
	for i := start; i < start+commonTierWays; i++ {
		if s.empty[i].used && s.empty[i].key == k {
			s.empty[i].used = false
		}
	}
	start = tierStart(hash, len(s.small))
	for i := start; i < start+commonTierWays; i++ {
		if s.small[i].used && s.small[i].key == k {
			s.small[i].used = false
		}
	}
}
func (c *commonAccountCache) Get(k solana.PublicKey) (a *accounts.Account, ok bool) {
	if !c.enabled {
		return c.large.Get(k)
	}
	s, h := c.locate(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	start := tierStart(h, len(s.empty))
	for i := start; i < start+commonTierWays; i++ {
		e := &s.empty[i]
		if e.used && e.key == k {
			e.recent = true
			return e.account(nil), true
		}
	}
	start = tierStart(h, len(s.small))
	for i := start; i < start+commonTierWays; i++ {
		e := &s.small[i]
		if e.used && e.key == k {
			e.recent = true
			return e.account(e.data[:]), true
		}
	}
	return c.large.Get(k)
}
func (c *commonAccountCache) Has(k solana.PublicKey) bool {
	if !c.enabled {
		return c.large.Has(k)
	}
	s, h := c.locate(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	start := tierStart(h, len(s.empty))
	for i := start; i < start+commonTierWays; i++ {
		if s.empty[i].used && s.empty[i].key == k {
			return true
		}
	}
	start = tierStart(h, len(s.small))
	for i := start; i < start+commonTierWays; i++ {
		if s.small[i].used && s.small[i].key == k {
			return true
		}
	}
	return c.large.Has(k)
}
func (c *commonAccountCache) Set(k solana.PublicKey, a *accounts.Account) (ok bool) {
	if !c.enabled {
		return c.large.Set(k, a)
	}
	s, h := c.locate(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	// Serialize migration across tiers; a rejected refresh must also evict old data.
	s.remove(k, h)
	c.large.Delete(k)
	if a == nil || len(a.Data) > 200 || a.Key != k {
		return c.large.Set(k, a)
	}
	if len(a.Data) == 0 {
		start := tierStart(h, len(s.empty))
		chosen := start
		for i := start; i < start+commonTierWays; i++ {
			if !s.empty[i].used {
				chosen = i
				goto emptyFound
			}
		}
		for pass := 0; pass < 2; pass++ {
			for i := start; i < start+commonTierWays; i++ {
				e := &s.empty[i]
				if !e.used || !e.recent {
					chosen = i
					goto emptyFound
				}
				e.recent = false
			}
		}
	emptyFound:
		s.empty[chosen] = tierHeader(k, a)
		return true
	}
	start := tierStart(h, len(s.small))
	chosen := start
	for i := start; i < start+commonTierWays; i++ {
		if !s.small[i].used {
			chosen = i
			goto smallFound
		}
	}
	for pass := 0; pass < 2; pass++ {
		for i := start; i < start+commonTierWays; i++ {
			e := &s.small[i]
			if !e.used || !e.recent {
				chosen = i
				goto smallFound
			}
			e.recent = false
		}
	}
smallFound:
	e := &s.small[chosen]
	e.commonTierHeader = tierHeader(k, a)
	copy(e.data[:], a.Data)
	return true
}
func (c *commonAccountCache) Delete(k solana.PublicKey) {
	if !c.enabled {
		c.large.Delete(k)
		return
	}
	s, h := c.locate(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remove(k, h)
	c.large.Delete(k)
}
func (c *commonAccountCache) Clear() {
	if !c.enabled {
		c.large.Clear()
		return
	}
	// Same lock order as ordinary operations, with all shards held for a full clear.
	for i := range c.shards {
		c.shards[i].mu.Lock()
	}
	for i := range c.shards {
		clear(c.shards[i].empty)
		clear(c.shards[i].small)
	}
	c.large.Clear()
	for i := len(c.shards) - 1; i >= 0; i-- {
		c.shards[i].mu.Unlock()
	}
}
func (c *commonAccountCache) Close() { c.large.Close() }
