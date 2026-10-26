package replay

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestReplayNilTailBoundaries(t *testing.T) {
	var typedNil *unrootedTail
	for _, tt := range []struct {
		name string
		tail unrootedState
	}{
		{"nil interface", nil},
		{"nil pointer in interface", typedNil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := &accountsdb.AccountsDb{}
			block := &b.Block{Slot: 42}
			exec := newBlockExecution(db, block, nil, 0, nil, nil, tt.tail, nil, false, nil)
			defer exec.close()
			require.True(t, exec.tail == nil, "disabled tail must be a nil interface")
			require.Same(t, db, exec.blockSrc, "classic replay must read AccountsDB")
			ctx := newSlotCtx(block, accounts.NewMemAccounts(), accounts.NewMemAccounts(), db, tt.tail, 0)
			require.True(t, ctx.UnrootedRead == nil, "account reader must fall back to AccountsDB")
		})
	}

	t.Run("active tail preserves overlay reads", func(t *testing.T) {
		durable := &fakeDurable{known: map[solana.PublicKey]uint64{testKey(1): 10}}
		tail := newUnrootedTail(durable, nil, 16, 1, "")
		tail.Add(41, []*accounts.Account{testAccount(1, 20)}, testHashBytes(1))
		block := &b.Block{Slot: 42}
		exec := newBlockExecution(nil, block, nil, 0, nil, nil, tail, nil, false, nil)
		defer exec.close()
		require.Same(t, tail, exec.tail)
		require.Same(t, tail, exec.blockSrc)
		ctx := newSlotCtx(block, accounts.NewMemAccounts(), accounts.NewMemAccounts(), nil, tail, 0)
		acct, err := ctx.GetAccountFromAccountsDb(testKey(1))
		require.NoError(t, err)
		require.Equal(t, uint64(20), acct.Lamports)
	})
}

// Reproduce classic replay's actual caller: a nil *unrootedTail is passed to
// ProcessBlock's interface argument. It must execute and persist identically
// to the same block using the in-memory lifecycle fixture, without a panic.
func TestProcessBlockTypedNilTailPersistsClassicBank(t *testing.T) {
	env := newLifecycleEnv(t)
	txs := transferTransactions(t, 1, 700_000)
	want := lifecycleWholeBlock(t, env, txs, 0)
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(db.CloseDb)
	stored := make(chan struct{})
	require.NoError(t, db.StoreAccounts(env.durable.AllAccounts(), lifecycleParentSlot, func() { close(stored) }))
	<-stored

	block := env.block(txs)
	block.MarkTransactionSignaturesVerified()
	var tail *unrootedTail
	var got *sealevel.SlotCtx
	persisted := &persistedTracker{}
	require.NotPanics(t, func() {
		got, err = ProcessBlock(db, block, env.epochSchedule, 0, nil, persisted, tail, NewTransactionStatusCache(), false, env.parent)
	})
	require.NoError(t, err)
	require.True(t, got.UnrootedRead == nil)
	require.Equal(t, want.bankhash, got.FinalBankhash)
	db.WaitForStoreWorker()
	storedSlot, storedHash := persisted.Get()
	require.Equal(t, block.Slot, storedSlot)
	require.Equal(t, want.bankhash, storedHash)
	bankhash, err := db.GetBankHashForSlot(block.Slot)
	require.NoError(t, err)
	require.Equal(t, want.bankhash, bankhash)
	for _, key := range []solana.PublicKey{txfixture.PayerPubkey(), txfixture.DestPubkey()} {
		acct, err := db.GetAccount(block.Slot, key)
		require.NoError(t, err)
		require.Equal(t, want.delta[key].Lamports, acct.Lamports)
	}
}
