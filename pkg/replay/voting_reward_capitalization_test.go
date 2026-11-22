package replay

import (
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestVotingRewardCapitalizationExcludesOnlyNewIncineratorDeposit(t *testing.T) {
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.CustomCommissionCollector, 0)
	rent := sealevel.NewDefaultRentSysvar()
	collector := solana.PublicKey{98}
	parents := map[solana.PublicKey]*accounts.Account{
		collector:         {Key: collector, Owner: a.SystemProgramAddr, Lamports: rent.MinimumBalance(0)},
		a.IncineratorAddr: {Key: a.IncineratorAddr, Owner: a.SystemProgramAddr, Lamports: 123},
	}
	reward := func(amount uint64) *atomic.Uint64 {
		value := &atomic.Uint64{}
		value.Store(amount)
		return value
	}
	updated, immediateParents, deposited := rewards.DistributeVotingRewards(nil,
		map[solana.PublicKey]*atomic.Uint64{collector: reward(11), a.IncineratorAddr: reward(7)}, 42,
		func(key solana.PublicKey) (*accounts.Account, error) { return parents[key], nil },
		rewards.VotingRewardDistributionConfig{Features: f, Rent: rent, SelfCollectorRewards: map[solana.PublicKey]uint64{}})
	require.Equal(t, uint64(18), deposited, "all successful writes enter bank capitalization before final incinerator burning")
	capitalizing := capitalizingVotingRewardDeposits(updated, immediateParents, deposited)
	require.Equal(t, uint64(11), capitalizing, "exclude the 7-lamport commission, not the existing 123-lamport parent balance")
	require.Equal(t, uint64(16), capitalizingEpochRewards(capitalizing, 5), "the inflation budget includes actual non-incinerator voting and staker deposits")
	require.Equal(t, uint64(123), parents[a.IncineratorAddr].Lamports, "the staged parent stays unchanged")
}
