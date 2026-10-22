package rpcserver

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func recalculationStake(t testing.TB, index int, voter byte, amount uint64) *accounts.Account {
	account := cacheStakeAccount(t, 0xa0, voter, amount, math.MaxUint64, math.MaxUint64)
	binary.LittleEndian.PutUint64(account.Key[1:], uint64(index))
	return account
}

func recalculationServer(t *testing.T) *RpcServer {
	delta := make([]*accounts.Account, 2048)
	for i := range delta {
		delta[i] = recalculationStake(t, i, 1, 100)
	}
	s := seedStakeCacheServer(t, delta...)
	require.Equal(t, map[solana.PublicKey]uint64{{1}: 204800}, cacheTotals(t, s))
	commitCacheAccounts(t, s, 100, cacheHistoryAccount(t, nil))
	return s
}

func TestRootedStakeCacheRecalculationMapChurn(t *testing.T) {
	s := recalculationServer(t)
	expected := map[solana.PublicKey]uint64{{1}: 204800}
	mutated := false
	s.stakeCache.beforeRecalculationChunk = func(start int) {
		if start != stakeCacheBatchSize || mutated {
			return
		}
		mutated = true
		// Select both already-counted and not-yet-counted entries without
		// depending on randomized map iteration order.
		var counted, pending []solana.PublicKey
		c := &s.stakeCache
		c.mu.Lock()
		for key, entry := range c.byStake {
			if entry.calculation == c.calculation && len(counted) < 2 {
				counted = append(counted, key)
			} else if entry.calculation != c.calculation && len(pending) < 2 {
				pending = append(pending, key)
			}
		}
		c.mu.Unlock()
		require.Len(t, counted, 2)
		require.Len(t, pending, 2)
		var changed []*accounts.Account
		for _, keys := range [][]solana.PublicKey{counted, pending} {
			updated := recalculationStake(t, 0, 2, 250)
			updated.Key = keys[0]
			changed = append(changed, updated, &accounts.Account{Key: keys[1]})
			expected[solana.PublicKey{1}] -= 200
			expected[solana.PublicKey{2}] += 250
		}
		// Force map growth while its iterator is suspended. New entries must
		// contribute once whether the iterator later sees them or skips them.
		for i := 2048; i < 8192; i++ {
			changed = append(changed, recalculationStake(t, i, 3, 75))
			expected[solana.PublicKey{3}] += 75
		}
		commitCacheAccounts(t, s, 101, changed...)
		// Reinsert keys removed from both sides of the cursor, before resuming.
		var recreated []*accounts.Account
		for _, key := range []solana.PublicKey{counted[1], pending[1]} {
			a := recalculationStake(t, 0, 4, 30)
			a.Key = key
			recreated = append(recreated, a)
			expected[solana.PublicKey{4}] += 30
		}
		commitCacheAccounts(t, s, 102, recreated...)
	}
	_, err := s.rootedActivatedStakes(t.Context(), 100, 1, nil)
	require.ErrorIs(t, err, errStakeViewChanged, "the response bank changed, but the calculation may be reused")
	require.True(t, mutated)
	require.True(t, s.stakeCache.totalsReady)
	require.Equal(t, expected, cacheTotals(t, s))
	require.Equal(t, uint64(2), s.stakeCache.recalculations)
	require.Equal(t, uint64(1), s.stakeCache.scans)
}

func TestRootedStakeCacheRecalculationInterruptedMidMap(t *testing.T) {
	for _, action := range []string{"cancel", "history", "rewind"} {
		t.Run(action, func(t *testing.T) {
			s := recalculationServer(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			interrupted := false
			s.stakeCache.beforeRecalculationChunk = func(start int) {
				if start != stakeCacheBatchSize || interrupted {
					return
				}
				interrupted = true
				switch action {
				case "cancel":
					cancel()
				case "history":
					commitCacheAccounts(t, s, 101, cacheHistoryAccount(t, nil))
				case "rewind":
					_, err := s.acctsDb.RewindToBatchBoundary(10)
					require.NoError(t, err)
					s.SetRootedBankState(10, 10, 0)
				}
			}
			_, err := s.rootedActivatedStakes(ctx, 100, 1, nil)
			if action == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, errStakeViewChanged)
			}
			require.True(t, interrupted)
			require.False(t, s.stakeCache.totalsReady)
			s.stakeCache.beforeRecalculationChunk = nil
			require.Equal(t, map[solana.PublicKey]uint64{{1}: 204800}, cacheTotals(t, s))
		})
	}
}
