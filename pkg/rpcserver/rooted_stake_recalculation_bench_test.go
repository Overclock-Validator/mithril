package rpcserver

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Isolates RAM-only recalculation and its allocations. The delegation cache is
// seeded directly; this is not a startup, disk, RPC or writer-latency benchmark.
// All stakes are bootstrap-active, so the comparison isolates traversal rather
// than the depth of activation-history calculations. Use the opt-in on-disk
// TestRootedStakeCacheScale for mixed activation states and concurrent commits.
func BenchmarkRootedStakeRecalculation(b *testing.B) {
	for _, count := range []int{20_000, 2_000_000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			db := newRPCAccountsDB(b)
			_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 10, Delta: []*accounts.Account{cacheHistoryAccount(b, nil)}}}, 10, nil, nil)
			require.NoError(b, err)
			s := &RpcServer{acctsDb: db, epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}}
			s.SetRootedBankState(10, 10, 0)
			version, stable := db.CommittedAccountVersion()
			require.True(b, stable)
			c := &s.stakeCache
			c.version, c.dataReady = version, true
			c.byStake = make(map[solana.PublicKey]cachedDelegation, count)
			for i := range count {
				key, voter := solana.PublicKey{0xa0}, solana.PublicKey{0xb0}
				binary.LittleEndian.PutUint64(key[1:], uint64(i))
				binary.LittleEndian.PutUint64(voter[1:], uint64(i%1000))
				c.byStake[key] = cachedDelegation{loaded: true, delegation: sealevel.Delegation{
					VoterPubkey: voter, StakeLamports: 1_000_000,
					ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64,
				}}
			}
			require.NoError(b, s.recalculateStakeTotals(b.Context(), 10, 0, c.generation))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := s.recalculateStakeTotals(b.Context(), 10, 0, c.generation); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			require.Len(b, c.totals, 1000)
			for _, total := range c.totals {
				require.Equal(b, uint64(count/1000)*1_000_000, total)
			}
		})
	}
}
