package replay

import (
	"encoding/binary"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// legacyBoundaryScan retains the two independent account-store scans for parity
// tests and before/after benchmarks. Both use the same stake-history ordering.
func legacyBoundaryScan(t testing.TB, db *accountsdb.AccountsDb, slot uint64, history *sealevel.SysvarStakeHistory) *BoundaryStakeScanResult {
	r := &BoundaryStakeScanResult{VoteAcctStakes: map[solana.PublicKey]uint64{}, EffectiveStakes: map[solana.PublicKey]uint64{}, RewardEpochEffectiveStakes: map[solana.PublicKey]uint64{}}
	var mu sync.Mutex
	_, err := global.StreamStakeAccounts(db, slot, func(_ solana.PublicKey, d *sealevel.Delegation, _ uint64) {
		e := d.StakeActivatingAndDeactivating(33, history, nil)
		mu.Lock()
		defer mu.Unlock()
		r.VoteAcctStakes[d.VoterPubkey] += d.StakeLamports
		r.StakeHistoryEffective += e.Effective
		r.StakeHistoryActivating += e.Activating
		r.StakeHistoryDeactivating += e.Deactivating
		if e.Effective != 0 {
			r.RewardEpochEffectiveStakes[d.VoterPubkey] += e.Effective
		}
	})
	require.NoError(t, err)
	completed := append(sealevel.SysvarStakeHistory(nil), (*history)...)
	completed.Update(33, sealevel.StakeHistoryEntry{Effective: r.StakeHistoryEffective, Activating: r.StakeHistoryActivating, Deactivating: r.StakeHistoryDeactivating})
	_, err = global.StreamStakeAccounts(db, slot, func(_ solana.PublicKey, d *sealevel.Delegation, _ uint64) {
		effective := d.Stake(34, &completed, nil)
		if effective > 0 {
			mu.Lock()
			r.EffectiveStakes[d.VoterPubkey] += effective
			r.TotalEffectiveStake += effective
			mu.Unlock()
		}
	})
	require.NoError(t, err)
	return r
}

func epochScanReuseFixture(t testing.TB, count int) (*accountsdb.AccountsDb, *sealevel.SysvarStakeHistory, map[solana.PublicKey]*sealevel.VoteStateVersions) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(db.CloseDb)
	global.ClearPendingStakePubkeys()
	t.Cleanup(global.ClearPendingStakePubkeys)
	var accts []*accounts.Account
	votes := map[solana.PublicKey]*sealevel.VoteStateVersions{}
	for i := 0; i < count; i++ {
		key := solana.PublicKey{0xa0}
		binary.LittleEndian.PutUint64(key[1:9], uint64(i))
		voter := solana.PublicKey{byte(1 + i%8)}
		d := sealevel.Delegation{VoterPubkey: voter, StakeLamports: 1_000_000_000 + uint64(i), ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64, WarmupCooldownRate: .25}
		switch i % 6 {
		case 1:
			d.ActivationEpoch = 33
		case 2:
			d.DeactivationEpoch = 33
		case 3:
			d.DeactivationEpoch = 31
		case 4:
			d.StakeLamports = 0
		case 5:
			d.ActivationEpoch = 34
		}
		stake := &sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake}
		stake.Stake.Stake.Delegation = d
		stake.Stake.Stake.CreditsObserved = uint64(i % 3)
		data, err := sealevel.MarshalStakeStake(stake)
		require.NoError(t, err)
		accts = append(accts, &accounts.Account{Key: key, Owner: addresses.StakeProgramAddr, Lamports: d.StakeLamports + 1, Data: data})
		global.EnqueuePendingStakePubkey(100, key)
		votes[voter] = &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4, V4: sealevel.VoteState4{InflationRewardsCommissionBps: 1000, EpochCredits: []sealevel.EpochCredits{{Epoch: 33, Credits: 100, PrevCredits: 3}}}}
	}
	seedDurableAccounts(t, db, accts, 100)
	_, err = global.FlushPendingStakePubkeys(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		// Invalidate the process-wide parsed index before removing this DB.
		global.EnqueuePendingStakePubkey(100, accts[0].Key)
		_, err := global.FlushPendingStakePubkeys(dir)
		require.NoError(t, err)
		global.ClearPendingStakePubkeys()
	})
	history := sealevel.SysvarStakeHistory{{Epoch: 32, Entry: sealevel.StakeHistoryEntry{Effective: uint64(count) * 1_000_000_000, Deactivating: uint64(count) * 1_000_000_000}}}
	return db, &history, votes
}

func rewardPartitionRecords(t testing.TB, result *rewards.StreamingRewardsResult) map[solana.PublicKey]rewards.SpoolRecord {
	t.Helper()
	records := map[solana.PublicKey]rewards.SpoolRecord{}
	for p := uint64(0); p < result.NumPartitions; p++ {
		reader, err := rewards.NewPartitionReader(result.SpoolDir, result.SpoolSlot, uint32(p))
		require.NoError(t, err)
		if reader == nil {
			continue
		}
		for {
			r, err := reader.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			r.PartitionIndex = uint32(p)
			require.NotContains(t, records, r.StakePubkey)
			records[r.StakePubkey] = *r
		}
		require.NoError(t, reader.Close())
	}
	return records
}

func TestEpochCapturedStakeScanAndRewardsMatchRepeatedAccountScans(t *testing.T) {
	db, history, votes := epochScanReuseFixture(t, 96)
	f := &features.Features{}
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 54_000}
	legacy := legacyBoundaryScan(t, db, 100, history)
	captured, spool := captureStakesForEpochBoundary(db, 100, 33, 34, history, schedule, f)
	defer spool.Close()
	require.Equal(t, legacy, captured)
	for _, fullAlpenglow := range []bool{false, true} {
		mode := rewards.RewardCalculationMode{FullAlpenglow: fullAlpenglow, RewardEpochDelegatedStakes: captured.RewardEpochEffectiveStakes}
		calculate := func(sources ...rewards.StakeAccountStream) *rewards.StreamingRewardsResult {
			r, err := rewards.CalculateRewardsStreaming(db, 100, history, nil, votes, rewards.PointValue{Rewards: 1_000_000}, 33, [32]byte{7}, &sealevel.SlotCtx{Features: f}, f, schedule, mode, sources...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, os.RemoveAll(r.SpoolDir)) })
			return r
		}
		want := calculate()
		wantRecords := rewardPartitionRecords(t, want)
		got := calculate(spool.Stream)
		require.NotEqual(t, want.SpoolDir, got.SpoolDir, "sibling boundary calculations must not overwrite retained plans")
		require.Equal(t, wantRecords, rewardPartitionRecords(t, want), "retained plan unchanged by later same-slot calculation")
		require.Equal(t, wantRecords, rewardPartitionRecords(t, got))
		require.Equal(t, want.TotalPoints, got.TotalPoints)
		require.Equal(t, want.TotalStakerRewards, got.TotalStakerRewards)
		require.Equal(t, want.VoteCommissions, got.VoteCommissions)
		require.Equal(t, want.NumStakeRewards, got.NumStakeRewards)
		require.Equal(t, want.NumPartitions, got.NumPartitions)
		require.Len(t, got.ValidatorRewards, len(want.ValidatorRewards))
		for key, reward := range want.ValidatorRewards {
			require.Equal(t, reward.Load(), got.ValidatorRewards[key].Load())
		}
	}
}

// Includes capture/write/close cost, both stake-history computations, all reward
// phases and output cleanup. The parent account store is warm in both variants;
// this is a bounded synthetic workload, not a mainnet latency forecast.
func BenchmarkEpochStakeScanReuse(b *testing.B) {
	db, history, votes := epochScanReuseFixture(b, 16384)
	f := &features.Features{}
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 54_000}
	for _, capture := range []bool{false, true} {
		name := "three_account_scans"
		if capture {
			name = "one_account_scan_and_spool"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var source []rewards.StakeAccountStream
				var closeSpool func() error
				if capture {
					_, spool := captureStakesForEpochBoundary(db, 100, 33, 34, history, schedule, f)
					source = []rewards.StakeAccountStream{spool.Stream}
					closeSpool = spool.Close
				} else {
					legacyBoundaryScan(b, db, 100, history)
				}
				r, err := rewards.CalculateRewardsStreaming(db, 100, history, nil, votes, rewards.PointValue{Rewards: 1_000_000}, 33, [32]byte{7}, &sealevel.SlotCtx{Features: f}, f, schedule, rewards.RewardCalculationMode{}, source...)
				require.NoError(b, err)
				require.NoError(b, os.RemoveAll(r.SpoolDir))
				if closeSpool != nil {
					require.NoError(b, closeSpool())
				}
			}
		})
	}
}
