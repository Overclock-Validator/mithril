package rewards

import (
	"errors"
	"math"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestVotingRewardCollectorDeposits(t *testing.T) {
	rent := sealevel.NewDefaultRentSysvar()
	minimum := rent.MinimumBalance(0)
	for _, tc := range []struct {
		name     string
		custom   bool
		relax    bool
		key      solana.PublicKey
		parent   *accounts.Account
		reward   uint64
		wantPaid bool
	}{
		{name: "legacy missing recipient remains unpaid", reward: minimum},
		{name: "missing below rent exemption", custom: true, reward: minimum - 1},
		{name: "missing exactly rent exempt", custom: true, reward: minimum, wantPaid: true},
		{name: "epoch1091 missing collector", custom: true, reward: 171_749_689, wantPaid: true},
		{name: "missing zero commission cannot create rent paying account", custom: true},
		{name: "missing remains rent checked with relaxed rules", custom: true, relax: true, reward: minimum - 1},
		{name: "closed stale account becomes fresh system collector", custom: true, parent: &accounts.Account{Owner: a.VoteProgramAddr, Data: []byte{1, 2}, Executable: true}, reward: minimum, wantPaid: true},
		{name: "existing system collector", custom: true, parent: &accounts.Account{Lamports: minimum, Owner: a.SystemProgramAddr}, reward: 7, wantPaid: true},
		{name: "existing underfunded collector strict rent rules", custom: true, parent: &accounts.Account{Lamports: 1, Owner: a.SystemProgramAddr}, reward: 7},
		{name: "existing underfunded collector relaxed rent rules", custom: true, relax: true, parent: &accounts.Account{Lamports: 1, Owner: a.SystemProgramAddr}, reward: 7, wantPaid: true},
		{name: "external vote account invalid owner", custom: true, parent: &accounts.Account{Lamports: minimum, Owner: a.VoteProgramAddr}, reward: 7},
		{name: "reserved destination", custom: true, key: a.ComputeBudgetProgramAddr, parent: &accounts.Account{Lamports: minimum, Owner: a.SystemProgramAddr}, reward: 7},
		{name: "reserved feature program destination", custom: true, key: a.FeatureAddr, parent: &accounts.Account{Lamports: minimum, Owner: a.SystemProgramAddr}, reward: 7},
		{name: "missing reserved feature program cannot be created", custom: true, key: a.FeatureAddr, reward: minimum},
		{name: "incinerator bypasses rent", custom: true, key: a.IncineratorAddr, reward: 1, wantPaid: true},
		{name: "overflow burns unpaid commission", custom: true, parent: &accounts.Account{Lamports: math.MaxUint64, Owner: a.SystemProgramAddr}, reward: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := tc.key
			if key.IsZero() {
				key = solana.PublicKey{91}
			}
			f := features.NewFeaturesDefault()
			if tc.custom {
				f.EnableFeature(features.CustomCommissionCollector, 0)
			}
			if tc.relax {
				f.EnableFeature(features.RelaxPostExecMinBalanceCheck, 0)
			}
			var original *accounts.Account
			if tc.parent != nil {
				tc.parent.Key = key
				original = tc.parent.Clone()
			}
			reward := &atomic.Uint64{}
			reward.Store(tc.reward)
			updated, parents, paid := DistributeVotingRewards(nil,
				map[solana.PublicKey]*atomic.Uint64{key: reward}, 42,
				func(solana.PublicKey) (*accounts.Account, error) {
					if tc.parent == nil {
						return nil, accountsdb.ErrNoAccount
					}
					return tc.parent, nil
				}, VotingRewardDistributionConfig{Features: f, Rent: rent, SelfCollectorRewards: map[solana.PublicKey]uint64{}})
			require.Len(t, updated, 1)
			require.Len(t, parents, 1)
			if tc.wantPaid {
				require.Equal(t, tc.reward, paid)
				require.NotNil(t, updated[0])
				require.Equal(t, key, updated[0].Key)
				before := uint64(0)
				if tc.parent != nil {
					before = tc.parent.Lamports
				}
				require.Equal(t, before+tc.reward, updated[0].Lamports)
				require.Equal(t, a.SystemProgramAddr, updated[0].Owner)
				require.Equal(t, before, parents[0].Lamports)
				if before == 0 {
					require.Empty(t, updated[0].Data)
					require.False(t, updated[0].Executable)
					require.Equal(t, a.SystemProgramAddr, parents[0].Owner)
				}
			} else {
				require.Zero(t, paid)
				require.Nil(t, updated[0], "invalid collector must not publish a mutated account")
				require.Nil(t, parents[0])
			}
			if original != nil {
				require.Equal(t, original, tc.parent.Clone(), "retained parent must not change")
			}
		})
	}
}

func TestVotingRewardsKeepSelfCollectorProvenance(t *testing.T) {
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.CustomCommissionCollector, 0)
	self, external := solana.PublicKey{92}, solana.PublicKey{93}
	selfState := &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4, V4: sealevel.VoteState4{InflationRewardsCollector: self}}
	externalState := &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4, V4: sealevel.VoteState4{InflationRewardsCollector: self}}
	for _, selfReward := range []uint64{0, 7} {
		for _, selfFirst := range []bool{false, true} {
			total := make(map[solana.PublicKey]*atomic.Uint64)
			own := make(map[solana.PublicKey]uint64)
			addSelf := func() { accumulateVotingReward(total, self, selfState, true, selfReward, own) }
			addExternal := func() { accumulateVotingReward(total, external, externalState, true, 100, own) }
			if selfFirst {
				addSelf()
				addExternal()
			} else {
				addExternal()
				addSelf()
			}
			require.Contains(t, own, self, "even a zero-valued self reward must retain its provenance")
			require.Equal(t, uint64(100)+selfReward, total[self].Load(), "processed rewards still include the invalid external share")
			parent := &accounts.Account{Key: self, Owner: a.VoteProgramAddr, Lamports: 1000}
			updated, parents, paid := DistributeVotingRewards(nil, total, 42,
				func(solana.PublicKey) (*accounts.Account, error) { return parent, nil },
				VotingRewardDistributionConfig{Features: f, Rent: sealevel.NewDefaultRentSysvar(), SelfCollectorRewards: own})
			require.Equal(t, selfReward, paid, "external commission sent to another vote account must be burned")
			require.Equal(t, uint64(1000)+selfReward, updated[0].Lamports)
			require.Equal(t, uint64(1000), parents[0].Lamports)
			require.Equal(t, uint64(1000), parent.Lamports)
		}
	}
}

func TestVotingRewardStorageFailureDoesNotBecomeUnpaidCommission(t *testing.T) {
	key := solana.PublicKey{94}
	reward := &atomic.Uint64{}
	reward.Store(171_749_689)
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.CustomCommissionCollector, 0)
	storageError := errors.New("damaged account storage")
	require.PanicsWithError(t, "load voting reward collector "+key.String()+" at slot 42: damaged account storage", func() {
		DistributeVotingRewards(nil, map[solana.PublicKey]*atomic.Uint64{key: reward}, 42,
			func(solana.PublicKey) (*accounts.Account, error) { return nil, storageError },
			VotingRewardDistributionConfig{Features: f, Rent: sealevel.NewDefaultRentSysvar()})
	})
}

func TestStreamingVotingRewardsRetainsMixedCollectorProvenance(t *testing.T) {
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.CustomCommissionCollector, 0)
	f.EnableFeature(features.CommissionRateInBasisPoints, 0)
	collector, otherVote := solana.PublicKey{95}, solana.PublicKey{96}
	votes := make(map[solana.PublicKey]*sealevel.VoteStateVersions)
	for _, vote := range []solana.PublicKey{collector, otherVote} {
		votes[vote] = &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4, V4: sealevel.VoteState4{
			InflationRewardsCollector: collector, InflationRewardsCommissionBps: 10_000,
			EpochCredits: []sealevel.EpochCredits{{Epoch: 70, Credits: 100}},
		}}
	}
	db := &accountsdb.AccountsDb{AcctsDir: filepath.Join(t.TempDir(), "accounts")}
	result, err := CalculateRewardsStreaming(db, 7100, &sealevel.SysvarStakeHistory{}, nil,
		votes, PointValue{Rewards: 200}, 70, [32]byte{}, &sealevel.SlotCtx{Features: f}, f,
		&sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}, RewardCalculationMode{
			FullAlpenglow: true, RewardEpochDelegatedStakes: map[solana.PublicKey]uint64{collector: 100, otherVote: 100},
		}, func(visit StakeAccountVisitor) (int, error) {
			for i, vote := range []solana.PublicKey{otherVote, collector} {
				visit(solana.PublicKey{byte(100 + i)}, &sealevel.Delegation{
					VoterPubkey: vote, StakeLamports: 100, ActivationEpoch: math.MaxUint64,
					DeactivationEpoch: math.MaxUint64,
				}, 0)
			}
			return 2, nil
		})
	require.NoError(t, err)
	require.Equal(t, uint64(200), result.ValidatorRewards[collector].Load())
	require.Equal(t, map[solana.PublicKey]uint64{collector: 100}, result.SelfCollectorRewards)
	parent := &accounts.Account{Key: collector, Owner: a.VoteProgramAddr, Lamports: 1_000_000}
	updated, _, deposited := DistributeVotingRewards(nil, result.ValidatorRewards, 7100,
		func(solana.PublicKey) (*accounts.Account, error) { return parent, nil },
		VotingRewardDistributionConfig{Features: f, Rent: sealevel.NewDefaultRentSysvar(), VoteAccounts: votes, SelfCollectorRewards: result.SelfCollectorRewards})
	require.Equal(t, uint64(100), deposited, "only the collector vote's own commission is payable")
	require.Equal(t, uint64(1_000_100), updated[0].Lamports)
}
