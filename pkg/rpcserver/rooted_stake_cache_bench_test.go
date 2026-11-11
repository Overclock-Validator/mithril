package rpcserver

import (
	"encoding/binary"
	"math"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// BenchmarkRootedStakeTotals compares the old query-time scan with the new
// warm cache on the same database. It measures stake aggregation, not the full
// JSON-RPC response. Cold initialization and epoch recalculation are excluded
// from warm timings. No live validator or mainnet-scale performance is implied.
func BenchmarkRootedStakeTotals(b *testing.B) {
	const stakeCount = 20_000
	const voterCount = 1_000
	db := newRPCAccountsDB(b)
	global.ClearPendingStakePubkeys()
	b.Cleanup(global.ClearPendingStakePubkeys)
	payloads := make([][]byte, voterCount)
	for i := range payloads {
		voter := solana.PublicKey{0xb0}
		binary.LittleEndian.PutUint64(voter[1:], uint64(i))
		state := &sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake}
		state.Stake.Stake.Delegation = sealevel.Delegation{VoterPubkey: voter, StakeLamports: 1_000_000, ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64}
		data, err := sealevel.MarshalStakeStake(state)
		require.NoError(b, err)
		payloads[i] = data
	}
	delta := make([]*accounts.Account, 0, stakeCount+1)
	for i := range stakeCount {
		key := solana.PublicKey{0xa0}
		binary.LittleEndian.PutUint64(key[1:], uint64(i))
		delta = append(delta, &accounts.Account{Key: key, Owner: addresses.StakeProgramAddr, Lamports: 1_000_001, Data: payloads[i%voterCount]})
		global.EnqueuePendingStakePubkey(10, key)
	}
	delta = append(delta, cacheHistoryAccount(b, nil))
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 10, Delta: delta}}, 10, nil, nil)
	require.NoError(b, err)
	_, err = global.FlushPendingStakePubkeys(filepath.Dir(db.AcctsDir))
	require.NoError(b, err)
	s := &RpcServer{acctsDb: db, epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}}
	s.SetRootedBankState(10, 10, 0)
	expected, err := s.rootedActivatedStakes(b.Context(), 10, 0, nil)
	require.NoError(b, err)
	require.Len(b, expected, voterCount)
	voter := solana.PublicKey{0xb0}
	b.Run("LegacyScanAll", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			totals := make(map[solana.PublicKey]uint64)
			history := sealevel.SysvarStakeHistory{}
			var mu sync.Mutex
			_, err := global.StreamStakeAccounts(db, 10, func(_ solana.PublicKey, d *sealevel.Delegation, _ uint64) {
				active := d.Stake(0, &history, nil)
				mu.Lock()
				totals[d.VoterPubkey] += active
				mu.Unlock()
			})
			if err != nil || totals[voter] != 20_000_000 {
				b.Fatalf("scan: %v, total: %d", err, totals[voter])
			}
		}
	})
	for _, tc := range []struct {
		name   string
		filter *solana.PublicKey
	}{{"CachedAll", nil}, {"CachedOne", &voter}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				totals, err := s.rootedActivatedStakes(b.Context(), 10, 0, tc.filter)
				if err != nil || totals[voter] != 20_000_000 {
					b.Fatalf("cache: %v, total: %d", err, totals[voter])
				}
			}
		})
	}
	// Include writer-side maintenance: 20k changed accounts, only 64 stakes.
	mixed := make([]*accounts.Account, stakeCount)
	copy(mixed, delta[:64])
	for i := 64; i < len(mixed); i++ {
		key := solana.PublicKey{0xc0}
		binary.LittleEndian.PutUint64(key[1:], uint64(i))
		mixed[i] = &accounts.Account{Key: key, Owner: addresses.SystemProgramAddr, Lamports: 1}
	}
	b.Run("Apply64StakesAmong20000Changes", func(b *testing.B) {
		b.ReportAllocs()
		version := s.stakeCache.version
		for b.Loop() {
			version += 2
			s.stakeCache.AccountsChanged(version, mixed, false)
		}
	})

}
