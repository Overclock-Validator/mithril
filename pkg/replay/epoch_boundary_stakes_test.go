package replay

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestEpochBoundaryStakesCompletesHistoryBeforeWarmupAndCooldown(t *testing.T) {
	const oldEpoch, newEpoch = uint64(33), uint64(34)
	const parentSlot = newEpoch*54_000 - 1
	base, warming, cooling := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	db := epochBoundaryStakeDB(t, parentSlot, []sealevel.Delegation{
		{VoterPubkey: base, StakeLamports: 1_000, ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64},
		{VoterPubkey: warming, StakeLamports: 1_000, ActivationEpoch: oldEpoch, DeactivationEpoch: math.MaxUint64},
		{VoterPubkey: cooling, StakeLamports: 1_000, ActivationEpoch: math.MaxUint64, DeactivationEpoch: oldEpoch},
	})
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 54_000, LeaderScheduleSlotOffset: 54_000}

	for _, test := range []struct {
		name        string
		reducedRate bool
		wantWarming uint64
		wantCooling uint64
	}{
		{name: "legacy 25 percent", wantWarming: 500, wantCooling: 500},
		{name: "reduced 9 percent", reducedRate: true, wantWarming: 180, wantCooling: 820},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := features.NewFeaturesDefault()
			if test.reducedRate {
				f.EnableFeature(features.ReduceStakeWarmupCooldown, 0)
			}
			history := sealevel.SysvarStakeHistory{{Epoch: oldEpoch - 1, Entry: sealevel.StakeHistoryEntry{Effective: 2_000}}}
			parentHistory := append(sealevel.SysvarStakeHistory(nil), history...)

			got := scanStakesForEpochBoundary(db, parentSlot, oldEpoch, newEpoch, &history, schedule, f)

			// The completed epoch had 2,000 effective lamports, 1,000 warming
			// and 1,000 cooling. Each transition gets its share of the epoch's
			// 25% (or 9%) limit. Without the new history entry these incorrectly
			// jump straight to 1,000 warming / 0 cooling.
			require.Equal(t, uint64(2_000), got.StakeHistoryEffective)
			require.Equal(t, uint64(1_000), got.StakeHistoryActivating)
			require.Equal(t, uint64(1_000), got.StakeHistoryDeactivating)
			require.Equal(t, map[solana.PublicKey]uint64{
				base: 1_000, warming: test.wantWarming, cooling: test.wantCooling,
			}, got.EffectiveStakes)
			require.Equal(t, uint64(2_000), got.TotalEffectiveStake)
			require.Equal(t, map[solana.PublicKey]uint64{base: 1_000, cooling: 1_000}, got.RewardEpochEffectiveStakes)
			require.Equal(t, map[solana.PublicKey]uint64{base: 1_000, warming: 1_000, cooling: 1_000}, got.VoteAcctStakes)
			require.Equal(t, parentHistory, history, "scanning must preserve the parent bank's history")
		})
	}
}

func TestEpochBoundaryStakesUsesEnteredEpochAcrossScheduleOffsets(t *testing.T) {
	const slotsPerEpoch = uint64(54_000)
	const boundarySlot = uint64(34) * slotsPerEpoch
	base, newlyActivated, futureDeactivation := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	db := epochBoundaryStakeDB(t, boundarySlot-1, []sealevel.Delegation{
		{VoterPubkey: base, StakeLamports: 1_000, ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64},
		{VoterPubkey: newlyActivated, StakeLamports: 1_000, ActivationEpoch: 34, DeactivationEpoch: math.MaxUint64},
		{VoterPubkey: futureDeactivation, StakeLamports: 1_000, ActivationEpoch: math.MaxUint64, DeactivationEpoch: 35},
	})

	for _, offset := range []uint64{slotsPerEpoch / 2, slotsPerEpoch, 2 * slotsPerEpoch} {
		t.Run(fmt.Sprintf("offset_%d", offset), func(t *testing.T) {
			schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: slotsPerEpoch, LeaderScheduleSlotOffset: offset}
			newEpoch := schedule.GetEpoch(boundarySlot)
			history := sealevel.SysvarStakeHistory{{Epoch: newEpoch - 2, Entry: sealevel.StakeHistoryEntry{Effective: 2_000}}}
			got := scanStakesForEpochBoundary(db, boundarySlot-1, newEpoch-1, newEpoch, &history, schedule, features.NewFeaturesDefault())

			// This is the epoch-34 stake distribution even when saved for a
			// leader schedule in epoch 35 or 36. Stake activated in epoch 34
			// is not effective yet; a future deactivation still counts fully.
			require.Equal(t, map[solana.PublicKey]uint64{base: 1_000, futureDeactivation: 1_000}, got.EffectiveStakes)
			require.Equal(t, uint64(2_000), got.TotalEffectiveStake)
			require.Equal(t, got.EffectiveStakes, got.RewardEpochEffectiveStakes)

			leaderScheduleEpoch := schedule.LeaderScheduleEpoch(boundarySlot)
			global.ClearEpochStakes(leaderScheduleEpoch)
			t.Cleanup(func() { global.ClearEpochStakes(leaderScheduleEpoch) })
			b := &block.Block{Slot: boundarySlot}
			updateEpochStakesAndRefreshVoteCache(leaderScheduleEpoch, b, db, boundarySlot-1, got, features.NewFeaturesDefault(), schedule)
			require.Equal(t, got.EffectiveStakes, global.EpochStakes(leaderScheduleEpoch), "future schedules must use the newly entered epoch's distribution")
			require.Equal(t, got.EffectiveStakes, b.EpochStakesPerVoteAcct)
			require.Equal(t, got.TotalEffectiveStake, global.EpochTotalStake(leaderScheduleEpoch))
		})
	}
}

func epochBoundaryStakeDB(t *testing.T, slot uint64, delegations []sealevel.Delegation) *accountsdb.AccountsDb {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(db.CloseDb)

	global.ClearPendingStakePubkeys()
	var accts []*accounts.Account
	for i, delegation := range delegations {
		key := solana.PublicKey{0xa0, byte(i + 1)}
		stake := &sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake}
		stake.Stake.Stake.Delegation = delegation
		data, err := sealevel.MarshalStakeStake(stake)
		require.NoError(t, err)
		accts = append(accts, &accounts.Account{Key: key, Owner: addresses.StakeProgramAddr, Lamports: delegation.StakeLamports, Data: data})
		global.EnqueuePendingStakePubkey(slot, key)

		voteKey := delegation.VoterPubkey
		previousVoteState := global.VoteCacheItem(voteKey)
		t.Cleanup(func() {
			if previousVoteState == nil {
				global.DeleteVoteCacheItem(voteKey)
			} else {
				global.PutVoteCacheItem(voteKey, previousVoteState)
			}
		})
		voteState := &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4, V4: sealevel.VoteState4{NodePubkey: voteKey}}
		voteData, err := sealevel.MarshalVersionedVoteState(voteState)
		require.NoError(t, err)
		accts = append(accts, &accounts.Account{Key: voteKey, Owner: addresses.VoteProgramAddr, Lamports: 1_000_000_000, Data: voteData})
	}
	done := make(chan struct{})
	require.NoError(t, db.StoreAccounts(accts, slot, func() { close(done) }))
	<-done
	_, err = global.FlushPendingStakePubkeys(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		// Flushing invalidates the global index cache so later tests cannot
		// retain this temporary database's entries.
		global.EnqueuePendingStakePubkey(slot, accts[0].Key)
		_, err := global.FlushPendingStakePubkeys(dir)
		require.NoError(t, err)
		global.ClearPendingStakePubkeys()
	})
	return db
}
