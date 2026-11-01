package rewards

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestDistributeVotingRewardsBuildsOnStagedAccount(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))

	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(func() {
		if err := db.CloseDb(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})

	votePubkey := solana.NewWallet().PublicKey()
	parent := &accounts.Account{
		Key:       votePubkey,
		Lamports:  10_000_000_000,
		Owner:     a.VoteProgramAddr,
		RentEpoch: 0,
	}
	stored := make(chan struct{})
	require.NoError(t, db.StoreAccounts([]*accounts.Account{parent}, 10, func() { close(stored) }))
	<-stored

	const vatDebit = uint64(800_000_000)
	const votingReward = uint64(123_456_789)
	staged := parent.Clone()
	staged.Lamports -= vatDebit

	reward := &atomic.Uint64{}
	reward.Store(votingReward)
	validatorRewards := map[solana.PublicKey]*atomic.Uint64{votePubkey: reward}

	// Production Alpenglow replay is rooted-durable, so StoreAccounts is a
	// no-op until the slot folds. The staged loader is therefore the only
	// source of the same-bank VAT debit.
	db.RootedDurable = true
	updated, immediateParents, distributed := DistributeVotingRewards(
		db, validatorRewards, 11,
		func(pubkey solana.PublicKey) (*accounts.Account, error) {
			require.Equal(t, votePubkey, pubkey)
			return staged, nil
		},
	)

	require.Equal(t, votingReward, distributed)
	require.Len(t, updated, 1)
	require.Len(t, immediateParents, 1)
	require.Equal(t, parent.Lamports-vatDebit, immediateParents[0].Lamports)
	require.Equal(t, parent.Lamports-vatDebit+votingReward, updated[0].Lamports)
	require.Equal(t, parent.Lamports-vatDebit, staged.Lamports, "retained staged account must remain immutable")

	durable, err := db.GetAccount(11, votePubkey)
	require.NoError(t, err)
	require.Equal(t, parent.Lamports, durable.Lamports, "unrooted reward must not leak into durable state")
}
