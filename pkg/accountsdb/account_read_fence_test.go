package accountsdb

import (
	"context"
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func assertAccountReadFence(t *testing.T, db *AccountsDb, key solana.PublicKey, wantErr error) {
	t.Helper()
	for name, read := range map[string]func() (*accounts.Account, error){
		"point": func() (*accounts.Account, error) { return db.GetAccount(101, key) },
		"point stats": func() (*accounts.Account, error) {
			acct, _, err := db.GetAccountWithStats(101, key)
			return acct, err
		},
		"stored": func() (*accounts.Account, error) { return db.getStoredAccount(101, key) },
	} {
		t.Run(name, func(t *testing.T) {
			got, err := read()
			require.ErrorIs(t, err, wantErr)
			require.Nil(t, got)
		})
	}
	for name, read := range map[string]func([]solana.PublicKey) ([]*accounts.Account, error){
		"batch": func(keys []solana.PublicKey) ([]*accounts.Account, error) {
			return db.GetAccountsBatch(context.Background(), 101, keys)
		},
		"shared batch": func(keys []solana.PublicKey) ([]*accounts.Account, error) {
			return db.GetAccountsBatchShared(context.Background(), 101, keys)
		},
		"batch stats": func(keys []solana.PublicKey) ([]*accounts.Account, error) {
			accts, _, err := db.GetAccountsBatchSharedWithStats(context.Background(), 101, keys)
			return accts, err
		},
		"unique batch stats": func(keys []solana.PublicKey) ([]*accounts.Account, error) {
			accts, _, err := db.GetUniqueAccountsBatchSharedWithStats(context.Background(), 101, keys)
			return accts, err
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := read([]solana.PublicKey{key})
			require.ErrorIs(t, err, wantErr)
			require.Nil(t, got)
		})
	}
}

func TestAccountReadFastPathsRespectTerminalIndexState(t *testing.T) {
	for _, state := range []string{"poisoned", "closed"} {
		t.Run(state, func(t *testing.T) {
			fixture := newProductionAccountsDBFixture(t, nil)
			db := openProductionAccountsDBFixture(t, fixture)
			defer db.CloseDb()
			common, vote, pending, inProgress := foldAcct(1, 11, nil), foldAcct(2, 22, nil), foldAcct(3, 33, nil), foldAcct(4, 44, nil)
			require.True(t, db.CommonAcctsCache.Set(common.Key, common))
			require.True(t, db.VoteAcctCache.Set(vote.Key, vote))
			db.pendingFold = map[[32]byte]dedupedVersion{pending.Key: {acct: pending}}
			db.inProgressStoreRequests.PushBack(storeRequest{m: map[solana.PublicKey]*accounts.Account{inProgress.Key: inProgress}})
			for _, acct := range []*accounts.Account{common, vote, pending, inProgress} {
				got, err := db.GetAccount(101, acct.Key)
				require.NoError(t, err)
				require.Equal(t, acct.Lamports, got.Lamports)
			}
			wantErr := ErrProductionAccountIndexPoisoned
			if state == "closed" {
				require.NoError(t, db.ProductionIndex.Close())
				wantErr = ErrShardedMutableClosed
			} else {
				_ = db.ProductionIndex.setPoison(errors.New("injected failed publication"))
			}
			for name, acct := range map[string]*accounts.Account{"common cache": common, "vote cache": vote, "pending fold": pending, "in progress": inProgress} {
				t.Run(name, func(t *testing.T) { assertAccountReadFence(t, db, acct.Key, wantErr) })
			}
		})
	}
}

func TestAccountReadsFenceFailedFoldUntilRecovery(t *testing.T) {
	fixture := tinyProductionAccountsDBFixture(t, 2)
	db := openProductionAccountsDBFixture(t, fixture)
	defer func() { _ = db.CloseDb() }()
	acct := foldAcct(9, 99, nil)
	db.foldHooks.beforeIndexCommit = func() {
		_ = db.ProductionIndex.setPoison(errors.New("injected ambiguous publication"))
	}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 101, Delta: []*accounts.Account{acct}}}, 101, nil, nil)
	require.ErrorIs(t, err, ErrFoldCommitDecided)
	require.NotEmpty(t, db.pendingFold, "failed decided fold must retain its pending epoch")
	assertAccountReadFence(t, db, acct.Key, ErrProductionAccountIndexPoisoned)
	require.NoError(t, db.CloseDb())

	db = openProductionAccountsDBFixture(t, fixture)
	recoverProductionAccountsDBFixture(t, db)
	got, err := db.GetAccount(101, acct.Key)
	require.NoError(t, err)
	require.Equal(t, acct.Lamports, got.Lamports)
	batch, err := db.GetAccountsBatch(context.Background(), 101, []solana.PublicKey{acct.Key})
	require.NoError(t, err)
	require.Equal(t, acct.Lamports, batch[0].Lamports)
}
