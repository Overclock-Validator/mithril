package accountsdb

import (
	"bytes"
	"sync"
	"testing"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestCommonTiersTransitionsAndOwnership(t *testing.T) {
	c := newCommonAccountCache(1<<20, true)
	k := solana.PublicKey{1}
	for _, size := range []int{0, 1, 165, 200, 201, 500, 0, 200} {
		a := &accounts.Account{Key: k, Slot: 7, Lamports: 8, Owner: [32]byte{9}, RentEpoch: 10, Executable: true, IsDummy: true, Data: bytes.Repeat([]byte{byte(size)}, size)}
		require.True(t, c.Set(k, a))
		require.True(t, c.Has(k))
		got, ok := c.Get(k)
		require.True(t, ok)
		require.Equal(t, a, got)
		if size > 0 && size <= 200 {
			got.Data[0] ^= 0xff
			again, _ := c.Get(k)
			require.Equal(t, a, again)
		}
	}
	c.Delete(k)
	require.False(t, c.Has(k))
	a := &accounts.Account{Key: k}
	c.Set(k, a)
	got, _ := c.Get(k)
	require.Nil(t, got.Data)
	c.Set(k, &accounts.Account{Key: k, Data: []byte{}})
	got, _ = c.Get(k)
	require.NotNil(t, got.Data)
	c.Set(k, &accounts.Account{Key: k, Data: make([]byte, 300000)})
	require.False(t, c.Has(k), "rejected large refresh evicts small value")
	c.Set(k, a)
	c.Clear()
	require.False(t, c.Has(k))
}
func TestCommonTiersCollisionAndConcurrentAccess(t *testing.T) {
	c := newCommonAccountCache(1<<20, true)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := solana.PublicKey{byte(w), byte(i), byte(i >> 8)}
				a := &accounts.Account{Key: k, Lamports: uint64(i), Data: bytes.Repeat([]byte{byte(i)}, 165)}
				c.Set(k, a)
				if got, ok := c.Get(k); ok {
					if got.Lamports != a.Lamports || !bytes.Equal(got.Data, a.Data) {
						t.Errorf("cache returned wrong entry")
					}
				}
				c.Has(k)
				if i%3 == 0 {
					c.Delete(k)
				}
			}
		}(worker)
	}
	wg.Wait()
	c.Clear()
}
func TestCommonTiersUseEmptySlotsBeforeEviction(t *testing.T) {
	c := newCommonAccountCache(1<<20, true)
	keys := []solana.PublicKey{}
	var target *commonTierShard
	var bucket int
	for n := 0; len(keys) < commonTierWays; n++ {
		k := solana.PublicKey{byte(n), byte(n >> 8), byte(n >> 16)}
		s, h := c.locate(k)
		b := tierStart(h, len(s.empty))
		if target == nil {
			target = s
			bucket = b
		}
		if s == target && b == bucket {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		c.Set(k, &accounts.Account{Key: k, Lamports: 123})
	}
	for _, k := range keys {
		require.True(t, c.Has(k), "unused slots must be filled before eviction")
	}
}

func TestCommonTiersBudgetAndLargeEntryAdmission(t *testing.T) {
	const budget = 32 << 20
	c := newCommonAccountCache(budget, true)
	total := c.large.maxWeight
	for i := range c.shards {
		total += uint64(len(c.shards[i].empty))*uint64(unsafe.Sizeof(commonTierHeader{})) + uint64(len(c.shards[i].small))*uint64(unsafe.Sizeof(commonTierSmall{}))
	}
	require.LessOrEqual(t, total, uint64(budget))
	require.Equal(t, uint64(5*budget/8), c.large.maxWeight)
	k := solana.PublicKey{27}
	a := &accounts.Account{Key: k, Data: make([]byte, 1<<20)}
	require.True(t, c.Set(k, a), "tiering must retain a large entry admitted by the original budget")
	require.True(t, c.Has(k))
}

// The tiered cache caps the empty and small tiers at 16 and 64 MiB (an eighth
// and a quarter of a small budget) and gives the large tier the rest.
func TestFixedCommonCacheBudget(t *testing.T) {
	for _, tc := range []struct{ budget, large uint64 }{
		{768 << 20, (768 - 16 - 64) << 20},
		{64 << 20, 64<<20 - 8<<20 - 16<<20},
	} {
		c := newCommonAccountCache(tc.budget, true)
		require.Equal(t, tc.large, c.large.maxWeight)
		total := c.large.maxWeight
		for i := range c.shards {
			total += uint64(len(c.shards[i].empty))*uint64(unsafe.Sizeof(commonTierHeader{})) + uint64(len(c.shards[i].small))*uint64(unsafe.Sizeof(commonTierSmall{}))
		}
		require.LessOrEqual(t, total, tc.budget)
	}
}
