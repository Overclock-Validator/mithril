package rewards

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/bankhash"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// This reduced incident fixture holds all unrelated slot effects constant.
// The production reward calculator, spool distributor and bank hasher must
// leave the 33 fully cooled stakes unchanged. See testdata/epoch116/README.md.
func TestEpoch116InactiveStakeBankHash(t *testing.T) {
	var fixture struct {
		ParentBankhash   string `json:"parent_bankhash"`
		Blockhash        string `json:"blockhash"`
		ExpectedBankhash string `json:"expected_bankhash"`
		OriginalBankhash string `json:"original_bankhash"`
		BaseLtHash       []byte `json:"base_accounts_lt_hash"`
		StakeHistory     []byte `json:"stake_history"`
		Stakes           []struct {
			Account                   *accounts.Account     `json:"account"`
			VoteEpochCredits          sealevel.EpochCredits `json:"vote_epoch_credits"`
			OriginalUpdatedDataSHA256 string                `json:"original_updated_data_sha256"`
		} `json:"stakes"`
	}
	raw, err := os.ReadFile("testdata/epoch116/inactive-stakes.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.Len(t, fixture.Stakes, 33)

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(db.CloseDb)
	global.ClearPendingStakePubkeys()
	t.Cleanup(global.ClearPendingStakePubkeys)

	parents := accounts.NewMemAccounts()
	var storedAccounts, erroneousUpdates []*accounts.Account
	votes := make(map[solana.PublicKey]*sealevel.VoteStateVersions)
	for _, row := range fixture.Stakes {
		acct := row.Account
		stake, err := sealevel.UnmarshalStakeState(acct.Data)
		require.NoError(t, err)
		require.Equal(t, uint64(114), stake.Stake.Stake.Delegation.DeactivationEpoch)
		require.Equal(t, uint64(115), row.VoteEpochCredits.Epoch)
		voteKey := stake.Stake.Stake.Delegation.VoterPubkey
		votes[voteKey] = &sealevel.VoteStateVersions{
			Type: sealevel.VoteStateVersionV4,
			V4:   sealevel.VoteState4{EpochCredits: []sealevel.EpochCredits{row.VoteEpochCredits}},
		}
		require.NoError(t, parents.SetAccountWithoutLock(acct.Key, acct))
		storedAccounts = append(storedAccounts, acct)
		global.EnqueuePendingStakePubkey(6264000, acct.Key)

		// Independently reconstruct the logged erroneous write, and verify its
		// byte hash before using it to establish the original bad bank hash.
		bad := acct.Clone()
		stake.Stake.Stake.CreditsObserved = row.VoteEpochCredits.Credits
		require.NoError(t, sealevel.MarshalStakeStakeInto(stake, bad.Data))
		require.Equal(t, row.OriginalUpdatedDataSHA256, fmt.Sprintf("%x", sha256.Sum256(bad.Data)))
		erroneousUpdates = append(erroneousUpdates, bad)
	}
	stored := make(chan struct{})
	require.NoError(t, db.StoreAccounts(storedAccounts, 6264000, func() { close(stored) }))
	<-stored
	count, err := global.FlushPendingStakePubkeysThrough(dir, 6264000)
	require.NoError(t, err)
	require.Equal(t, len(fixture.Stakes), count)
	db.RootedDurable = true

	f := &features.Features{}
	f.EnableFeature(features.AccountsLtHash, 0)
	f.EnableFeature(features.RemoveAccountsDeltaHash, 0)
	calculateHash := func(updates []*accounts.Account) string {
		ctx := &sealevel.SlotCtx{
			Features: f, ParentAccts: parents,
			AcctsLtHash: new(lthash.LtHash).InitWithHash(fixture.BaseLtHash),
		}
		return solana.HashFromBytes(bankhash.CalculateBankHash(ctx, nil, updates,
			solana.MustHashFromBase58(fixture.ParentBankhash), 0,
			solana.MustHashFromBase58(fixture.Blockhash))).String()
	}
	require.Equal(t, fixture.OriginalBankhash, calculateHash(erroneousUpdates))

	var history sealevel.SysvarStakeHistory
	require.NoError(t, history.UnmarshalWithDecoder(bin.NewBinDecoder(fixture.StakeHistory)))
	newRateEpoch := uint64(0)
	result, err := CalculateRewardsStreaming(db, 6264000, &history, &newRateEpoch,
		votes, PointValue{Rewards: 12922370184029}, 115, [32]byte{},
		&sealevel.SlotCtx{Features: f}, f, RewardCalculationMode{FullAlpenglow: true})
	require.NoError(t, err)
	updated, _, distributed, burned := DistributeStakingRewardsFromSpool(
		db, result.SpoolDir, result.SpoolSlot, 0, 6264001, nil)
	require.Zero(t, distributed)
	require.Zero(t, burned)
	require.Equal(t, fixture.ExpectedBankhash, calculateHash(updated))
	require.Zero(t, result.NumStakeRewards)
	require.Equal(t, uint64(1), result.NumPartitions)
	require.Empty(t, updated)
}
