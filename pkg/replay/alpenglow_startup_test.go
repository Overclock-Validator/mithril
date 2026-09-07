package replay

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type alpenglowStartupTestReader struct {
	accounts map[solana.PublicKey]*accounts.Account
	errs     map[solana.PublicKey]error
}

func (r alpenglowStartupTestReader) GetAccount(_ uint64, key solana.PublicKey) (*accounts.Account, error) {
	if err := r.errs[key]; err != nil {
		return nil, err
	}
	acct := r.accounts[key]
	if acct == nil {
		return nil, accountsdb.ErrNoAccount
	}
	return acct.Clone(), nil
}

func validAlpenglowStartupAccounts(t *testing.T, activationSlot uint64) map[solana.PublicKey]*accounts.Account {
	t.Helper()
	featureData, err := features.MarshalFeatureAcct(&features.FeatureAcct{ActivatedAt: &activationSlot})
	require.NoError(t, err)

	voteRewardData := encodeEpochInflationAccountState(EpochInflationAccountState{
		Current: EpochInflationState{MaxPossibleValidatorReward: 100, SlotsPerEpoch: 54_000, Epoch: 18},
		Prev:    &EpochInflationState{MaxPossibleValidatorReward: 90, SlotsPerEpoch: 54_000, Epoch: 17},
	})
	rewardStakesData, err := encodeRewardEpochDelegatedStakes(
		17,
		map[solana.PublicKey]uint64{{1}: 1, {2}: 1},
		map[solana.PublicKey]uint64{{1}: 10, {2}: 20},
	)
	require.NoError(t, err)

	featureKey := solana.PublicKey(features.Alpenglow.Address)
	return map[solana.PublicKey]*accounts.Account{
		featureKey: {
			Key:      featureKey,
			Lamports: 1,
			Owner:    a.FeatureAddr,
			Data:     featureData,
		},
		NanosecondClockAccountAddr(): {
			Key:      NanosecondClockAccountAddr(),
			Lamports: 1,
			Owner:    a.SystemProgramAddr,
			Data:     encodeNanosecondClockData(1234),
		},
		VoteRewardAccountAddr(): {
			Key:      VoteRewardAccountAddr(),
			Lamports: 1,
			Owner:    a.SystemProgramAddr,
			Data:     voteRewardData,
		},
		RewardEpochDelegatedStakesAccountAddr(): {
			Key:      RewardEpochDelegatedStakesAccountAddr(),
			Lamports: 1,
			Owner:    a.SystemProgramAddr,
			Data:     rewardStakesData,
		},
	}
}

func TestValidateAlpenglowStartupState(t *testing.T) {
	reader := alpenglowStartupTestReader{accounts: validAlpenglowStartupAccounts(t, 486_000)}
	require.NoError(t, validateAlpenglowStartupState(reader, 975_511))
}

func TestValidateAlpenglowStartupStateWithAccountsDB(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))

	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(db.CloseDb)

	stored := make(chan struct{})
	startupAccounts := validAlpenglowStartupAccounts(t, 486_000)
	toStore := make([]*accounts.Account, 0, len(startupAccounts))
	for _, acct := range startupAccounts {
		toStore = append(toStore, acct)
	}
	require.NoError(t, db.StoreAccounts(toStore, 486_000, func() { close(stored) }))
	<-stored

	require.NoError(t, ValidateAlpenglowStartupState(db, 975_511))
}

func TestValidateAlpenglowStartupStateRequiresDeployedFeature(t *testing.T) {
	accts := validAlpenglowStartupAccounts(t, 486_000)
	delete(accts, solana.PublicKey(features.Alpenglow.Address))

	err := validateAlpenglowStartupState(alpenglowStartupTestReader{accounts: accts}, 975_511)
	require.ErrorContains(t, err, features.AlpenglowFeatureGateAddress)
	require.ErrorContains(t, err, "does not implement TowerBFT or migration")
}

func TestValidateAlpenglowStartupStateRejectsFutureActivation(t *testing.T) {
	reader := alpenglowStartupTestReader{accounts: validAlpenglowStartupAccounts(t, 975_512)}
	err := validateAlpenglowStartupState(reader, 975_511)
	require.ErrorContains(t, err, "activates at future slot 975512")
}

func TestValidateAlpenglowStartupStateRequiresPostGenesisClock(t *testing.T) {
	accts := validAlpenglowStartupAccounts(t, 486_000)
	delete(accts, NanosecondClockAccountAddr())

	err := validateAlpenglowStartupState(alpenglowStartupTestReader{accounts: accts}, 975_511)
	require.ErrorContains(t, err, "pre-Alpenglow-genesis or incompatible")
	require.ErrorContains(t, err, NanosecondClockAccountAddr().String())
}

func TestValidateAlpenglowStartupStateRejectsWrongMetadataOwner(t *testing.T) {
	accts := validAlpenglowStartupAccounts(t, 486_000)
	accts[VoteRewardAccountAddr()].Owner = a.FeatureAddr

	err := validateAlpenglowStartupState(alpenglowStartupTestReader{accounts: accts}, 975_511)
	require.ErrorContains(t, err, "vote reward")
	require.ErrorContains(t, err, "expected system program")
}

func TestValidateAlpenglowStartupStateAllowsMissingFirstEpochRewardStakes(t *testing.T) {
	accts := validAlpenglowStartupAccounts(t, 0)
	delete(accts, RewardEpochDelegatedStakesAccountAddr())

	require.NoError(t, validateAlpenglowStartupState(alpenglowStartupTestReader{accounts: accts}, 10))
}

func TestValidateAlpenglowStartupStateRejectsMalformedOptionalRewardStakes(t *testing.T) {
	accts := validAlpenglowStartupAccounts(t, 486_000)
	acct := accts[RewardEpochDelegatedStakesAccountAddr()]
	acct.Data = acct.Data[:len(acct.Data)-1]

	err := validateAlpenglowStartupState(alpenglowStartupTestReader{accounts: accts}, 975_511)
	require.ErrorContains(t, err, "reward-epoch delegated-stakes")
	require.ErrorContains(t, err, "does not match validator count")
}

func TestValidateAlpenglowStartupStatePropagatesReadFailure(t *testing.T) {
	reader := alpenglowStartupTestReader{
		accounts: validAlpenglowStartupAccounts(t, 486_000),
		errs:     map[solana.PublicKey]error{VoteRewardAccountAddr(): errors.New("disk checksum failure")},
	}

	err := validateAlpenglowStartupState(reader, 975_511)
	require.ErrorContains(t, err, "disk checksum failure")
}

func TestValidateAlpenglowRuntimeFeatureSet(t *testing.T) {
	require.Error(t, validateAlpenglowRuntimeFeatureSet(nil, 100))
	require.Error(t, validateAlpenglowRuntimeFeatureSet(features.NewFeaturesDefault(), 100))

	active := features.NewFeaturesDefault()
	active.EnableFeature(features.Alpenglow, 42)
	require.NoError(t, validateAlpenglowRuntimeFeatureSet(active, 100))
}
