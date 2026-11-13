package replay

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestEpochBoundaryPublishesSelectionWithoutUpgradingCachedUnknown(t *testing.T) {
	const newEpoch = uint64(91041)
	const slotsPerEpoch = uint64(54000)
	slot := newEpoch * slotsPerEpoch
	voteKey := solana.PublicKey{41}
	db := epochBoundaryStakeDB(t, slot-1, []sealevel.Delegation{{
		VoterPubkey: voteKey, StakeLamports: 1000,
		ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64,
	}})
	vote, err := db.GetAccount(slot-1, voteKey)
	require.NoError(t, err)
	vote = vote.Clone()
	state, err := sealevel.UnmarshalVersionedVoteState(vote.Data)
	require.NoError(t, err)
	bls := [48]byte{1}
	state.V4.BlsPubkeyCompressed = &bls
	vote.Data, err = sealevel.MarshalVersionedVoteState(state)
	require.NoError(t, err)
	seedDurableAccounts(t, db, []*accounts.Account{vote}, slot-1)

	previousRent := sealevel.SysvarCache.Rent.Sysvar
	zeroRent := sealevel.SysvarRent{}
	sealevel.SysvarCache.Rent.Sysvar = &zeroRent
	t.Cleanup(func() { sealevel.SysvarCache.Rent.Sysvar = previousRent })

	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: slotsPerEpoch, LeaderScheduleSlotOffset: slotsPerEpoch}
	cacheEpoch := schedule.LeaderScheduleEpoch(slot)
	global.ClearEpochStakes(cacheEpoch)
	t.Cleanup(func() { global.ClearEpochStakes(cacheEpoch) })
	scan := &BoundaryStakeScanResult{
		VoteAcctStakes:      map[solana.PublicKey]uint64{voteKey: 1000},
		EffectiveStakes:     map[solana.PublicKey]uint64{voteKey: 1000},
		TotalEffectiveStake: 1000,
	}
	bank := &block.Block{Slot: slot}

	updateEpochStakesAndRefreshVoteCache(cacheEpoch, bank, db, slot-1, scan, features.NewFeaturesDefault(), schedule)
	unfiltered, ok := global.EpochStakesSnapshot(cacheEpoch)
	require.True(t, ok)
	require.Equal(t, epochstakes.SelectionUnfiltered, unfiltered.Selection)

	unknownGeneration := global.PutEpochStakes(cacheEpoch, unfiltered.Stakes, unfiltered.VoteAccounts, unfiltered.TotalStake)
	vat := features.NewFeaturesDefault()
	vat.EnableFeature(features.ValidatorAdmissionTicket, 0)
	updateEpochStakesAndRefreshVoteCache(cacheEpoch, bank, db, slot-1, scan, vat, schedule)
	cached, ok := global.EpochStakesSnapshot(cacheEpoch)
	require.True(t, ok)
	require.Equal(t, unknownGeneration, cached.Generation)
	require.Equal(t, epochstakes.SelectionUnknown, cached.Selection)

	global.ClearEpochStakes(cacheEpoch)
	updateEpochStakesAndRefreshVoteCache(cacheEpoch, bank, db, slot-1, scan, vat, schedule)
	filtered, ok := global.EpochStakesSnapshot(cacheEpoch)
	require.True(t, ok)
	require.Equal(t, epochstakes.SelectionVATFiltered, filtered.Selection)
	require.Equal(t, uint64(1000), filtered.Stakes[voteKey])
}
