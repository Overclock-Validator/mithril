package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestProcessBlockRequiresUnrootedTail(t *testing.T) {
	var typedNil *unrootedTail
	for _, tt := range []struct {
		name string
		tail unrootedState
	}{
		{"nil interface", nil},
		{"nil pointer in interface", typedNil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ProcessBlock(&accountsdb.AccountsDb{}, &b.Block{Slot: 42}, nil, 0, nil, &persistedTracker{}, tt.tail, NewTransactionStatusCache(), false, nil)
			require.ErrorContains(t, err, "no unrooted tail")
		})
	}
}

func TestBlockExecutionReadsThroughUnrootedTail(t *testing.T) {
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
}
