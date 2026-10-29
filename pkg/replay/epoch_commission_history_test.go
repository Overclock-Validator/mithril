package replay

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestEpochBoundaryDelayedCommissionForReactivatedStake(t *testing.T) {
	// Synthetic epoch E=40, distributed at slot 41*54,000. At the start of
	// E-1 the voter charges 10%, but its delegation has fully cooled down.
	// During E-1 it reactivates and raises commission to 20%. Before VAT,
	// delayed commission must still split E's 1,000 lamports as 100 / 900.
	// Agave v4.4.0-beta.0 runtime/src/stakes.rs:890-900 retains vote accounts
	// with zero delegated stake; calculation.rs:737-754 selects their history.
	const rewardedEpoch, slotsPerEpoch = uint64(40), uint64(54_000)
	const firstBoundary = (rewardedEpoch - 1) * slotsPerEpoch
	const distributionSlot = (rewardedEpoch + 1) * slotsPerEpoch
	voter, baseVoter := solana.PublicKey{71}, solana.PublicKey{72}
	stakeKey := solana.PublicKey{0xa0, 1} // First delegation in epochBoundaryStakeDB.

	for _, tc := range []struct {
		name          string
		active        bool
		delayDisabled bool
		reload        bool
		retry         bool
		vat           bool
		wantVoter     uint64
	}{
		{name: "reactivated", wantVoter: 100},
		{name: "reactivated after checkpoint reload", reload: true, wantVoter: 100},
		{name: "retry previously incomplete boundary", retry: true, wantVoter: 100},
		{name: "continuously active control", active: true, wantVoter: 100},
		{name: "delay disabled control", delayDisabled: true, wantVoter: 200},
		{name: "VAT filtered control", vat: true, reload: true, wantVoter: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for e := rewardedEpoch; e <= rewardedEpoch+2; e++ {
				global.ClearEpochStakes(e)
				t.Cleanup(func() { global.ClearEpochStakes(e) })
			}
			f := features.NewFeaturesDefault()
			if !tc.delayDisabled {
				f.EnableFeature(features.DelayCommissionUpdates, 0)
			}
			if tc.vat {
				f.EnableFeature(features.ValidatorAdmissionTicket, 0)
				previousRent := sealevel.SysvarCache.Rent.Sysvar
				rent := sealevel.NewDefaultRentSysvar()
				sealevel.SysvarCache.Rent.Sysvar = &rent
				t.Cleanup(func() { sealevel.SysvarCache.Rent.Sysvar = previousRent })
			}
			delegation := sealevel.Delegation{
				VoterPubkey: voter, StakeLamports: 1_000,
				ActivationEpoch: math.MaxUint64, DeactivationEpoch: rewardedEpoch - 3,
			}
			if tc.active {
				delegation.DeactivationEpoch = math.MaxUint64
			}
			db := epochBoundaryStakeDB(t, firstBoundary-1, []sealevel.Delegation{
				delegation,
				{VoterPubkey: baseVoter, StakeLamports: 1_000_000, ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64},
			})
			store := func(slot uint64, accts ...*accounts.Account) {
				t.Helper()
				done := make(chan struct{})
				require.NoError(t, db.StoreAccounts(accts, slot, func() { close(done) }))
				<-done
			}
			setVote := func(slot uint64, key solana.PublicKey, commission uint16, credits []sealevel.EpochCredits) {
				t.Helper()
				acct, err := db.GetAccount(slot, key)
				require.NoError(t, err)
				acct = acct.Clone()
				vs := &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4, V4: sealevel.VoteState4{
					NodePubkey: key, BlsPubkeyCompressed: &[48]byte{key[0]},
					InflationRewardsCommissionBps: commission, EpochCredits: credits,
				}}
				acct.Data, err = sealevel.MarshalVersionedVoteState(vs)
				require.NoError(t, err)
				store(slot, acct)
			}
			setVote(firstBoundary-1, voter, 1_000, nil)
			setVote(firstBoundary-1, baseVoter, 0, nil)
			schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: slotsPerEpoch, LeaderScheduleSlotOffset: slotsPerEpoch}
			history := sealevel.SysvarStakeHistory{}
			boundary := func(epoch uint64) *BoundaryStakeScanResult {
				t.Helper()
				slot := epoch * slotsPerEpoch
				scan := scanStakesForEpochBoundary(db, slot-1, epoch-1, epoch, &history, schedule, f)
				b := &block.Block{Slot: slot}
				updateEpochStakesAndRefreshVoteCache(epoch+1, b, db, slot-1, scan, f, schedule)
				// Retained zero-effective-stake metadata must not change any
				// execution stake weights, totals, or leader-selection inputs.
				require.Equal(t, scan.EffectiveStakes, b.EpochStakesPerVoteAcct)
				require.Equal(t, scan.TotalEffectiveStake, b.TotalEpochStake)
				history.Update(epoch-1, sealevel.StakeHistoryEntry{
					Effective: scan.StakeHistoryEffective, Activating: scan.StakeHistoryActivating,
					Deactivating: scan.StakeHistoryDeactivating,
				})
				return scan
			}
			firstScan := boundary(rewardedEpoch - 1)
			require.Equal(t, uint64(1_000), firstScan.VoteAcctStakes[voter])
			if !tc.active {
				require.NotContains(t, firstScan.EffectiveStakes, voter)
			}
			if tc.retry {
				// An earlier PR build persisted only effective voters. Retrying
				// this same boundary may recover the omitted voter from its parent.
				snapshot, _ := global.EpochStakesSnapshot(rewardedEpoch)
				votes := global.EpochStakesVoteAccts(rewardedEpoch)
				delete(votes, voter)
				global.PutEpochStakes(rewardedEpoch, snapshot.Stakes, votes, snapshot.TotalStake)
				b := &block.Block{Slot: firstBoundary}
				updateEpochStakesAndRefreshVoteCache(rewardedEpoch, b, db, firstBoundary-1, firstScan, f, schedule)
			}
			complete, _ := global.EpochStakesSnapshot(rewardedEpoch)
			updateEpochStakesAndRefreshVoteCache(rewardedEpoch, &block.Block{Slot: firstBoundary}, db, firstBoundary-1, firstScan, f, schedule)
			again, _ := global.EpochStakesSnapshot(rewardedEpoch)
			require.Equal(t, complete.Generation, again.Generation, "complete history must stay immutable on retry")

			if !tc.active {
				acct, err := db.GetAccount(firstBoundary, stakeKey)
				require.NoError(t, err)
				acct = acct.Clone()
				ss, err := sealevel.UnmarshalStakeState(acct.Data)
				require.NoError(t, err)
				ss.Stake.Stake.Delegation.ActivationEpoch = rewardedEpoch - 1
				ss.Stake.Stake.Delegation.DeactivationEpoch = math.MaxUint64
				acct.Data, err = sealevel.MarshalStakeStake(ss)
				require.NoError(t, err)
				store(firstBoundary, acct)
			}
			setVote(firstBoundary, voter, 2_000, nil)
			secondScan := boundary(rewardedEpoch)
			require.Equal(t, uint64(1_000), secondScan.EffectiveStakes[voter])
			if tc.reload {
				for e := rewardedEpoch; e <= rewardedEpoch+1; e++ {
					data, err := global.SerializeEpochStakes(e)
					require.NoError(t, err)
					global.ClearEpochStakes(e)
					_, err = global.DeserializeAndLoadEpochStakes(data)
					require.NoError(t, err)
				}
			}
			setVote(distributionSlot-1, voter, 2_000, []sealevel.EpochCredits{{Epoch: rewardedEpoch, Credits: 1}})
			boundary(rewardedEpoch + 1)
			result, err := rewards.CalculateRewardsStreaming(db, distributionSlot, &history, nil,
				global.VoteCacheSnapshot(), rewards.PointValue{Rewards: 1_000}, rewardedEpoch,
				[32]byte{1}, &sealevel.SlotCtx{Features: f}, f, schedule, rewards.RewardCalculationMode{})
			require.NoError(t, err)
			require.Len(t, result.ValidatorRewards, 1)
			require.Equal(t, tc.wantVoter, result.ValidatorRewards[voter].Load())
			require.Equal(t, 1_000-tc.wantVoter, result.TotalStakerRewards)

			votes, voteParents, paidVote := rewards.DistributeVotingRewards(db, result.ValidatorRewards, distributionSlot, nil)
			require.Equal(t, tc.wantVoter, paidVote)
			require.Len(t, votes, 1)
			require.Equal(t, tc.wantVoter, votes[0].Lamports-voteParents[0].Lamports)
			var paidStake uint64
			for partition := uint64(0); partition < result.NumPartitions; partition++ {
				stakes, parents, paid, burned, _ := rewards.DistributeStakingRewardsFromSpool(
					db, result.SpoolDir, result.SpoolSlot, partition, distributionSlot+1+partition, nil, result.VoteCommissions, rewards.StakeRewardDistributionConfig{})
				require.Zero(t, burned)
				paidStake += paid
				for i, acct := range stakes {
					require.Equal(t, stakeKey, acct.Key)
					require.Equal(t, 1_000-tc.wantVoter, acct.Lamports-parents[i].Lamports)
					ss, err := sealevel.UnmarshalStakeState(acct.Data)
					require.NoError(t, err)
					require.Equal(t, uint64(1), ss.Stake.Stake.CreditsObserved)
					require.Equal(t, 2_000-tc.wantVoter, ss.Stake.Stake.Delegation.StakeLamports)
				}
			}
			require.Equal(t, 1_000-tc.wantVoter, paidStake)
			require.Equal(t, uint64(1_000), paidVote+paidStake)
			older, _ := global.EpochStakesSnapshot(rewardedEpoch)
			if tc.vat {
				require.NotContains(t, older.VoteAccounts, voter)
			} else if !tc.delayDisabled {
				require.Contains(t, older.VoteAccounts, voter)
				require.Equal(t, uint16(1_000), *older.VoteAccounts[voter].InflationRewardsCommissionBps)
			}
		})
	}
}
