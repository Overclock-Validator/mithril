package replay

import (
	"bytes"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// SetData adopts parameter memory without copying it, so the bank's final
// account copies must detach it before the next bank clears that arena.
// Published accounts stay intact while banks alternate the two arenas,
// including when a bank's parameters fall back to the heap.
func TestReplacedAccountDataSurvivesArenaAlternation(t *testing.T) {
	oldArena, oldPair := SerializedParameterArena, parameterArenas
	t.Cleanup(func() {
		waitSerializedParameterArenaCleared()
		SerializedParameterArena, parameterArenas = oldArena, oldPair
	})
	SerializedParameterArena = arena.New[byte](4096)
	program := solana.PublicKey{0xff, 0xff}
	type publication struct {
		acct *accounts.Account
		want accounts.Account
	}
	var published []publication
	for bank := range 6 {
		slotCtx, _ := modifiedAccountsBank(t, 0)
		key := solana.PublicKey{0xab, byte(bank)}
		parent := &accounts.Account{Key: key, Owner: program, Lamports: 1, Data: []byte{1, 2, 3, 4, 5}}
		require.NoError(t, slotCtx.SetAccount(key, parent))

		size := uint64(64)
		if bank == 3 {
			size = 8192 // larger than the arena: the heap fallback
		}
		params, inArena := SerializedParameterArena.AllocN(size)
		require.Equal(t, bank != 3, inArena)
		for i := range params {
			params[i] = byte(bank + 1)
		}

		txAccts := sealevel.NewTransactionAccountsFromRefs(
			[]*accounts.Account{{Key: program, Owner: a.BpfLoader2Addr, Executable: true, Lamports: 1}, parent},
			[]bool{false, true})
		txAccts.OnFirstWriteDetach = recordTxAcctCowClone
		txCtx := sealevel.NewTransactionCtx(*txAccts, 4, 4)
		ix, err := txCtx.NextInstructionCtx()
		require.NoError(t, err)
		metas := []sealevel.AccountMeta{{Pubkey: key, IsWritable: true}}
		ix.Configure([]uint64{0}, sealevel.InstructionAcctsFromAccountMetas(metas, txCtx.Accounts), nil)
		require.NoError(t, txCtx.Push())
		borrowed, err := ix.BorrowInstructionAccount(txCtx, 0)
		require.NoError(t, err)
		require.NoError(t, borrowed.SetData(*features.NewFeaturesDefault(), params[8:48]))
		borrowed.Drop()
		require.Same(t, &params[8], &txCtx.Accounts.Accounts[1].Data[0], "SetData adopts parameter memory")
		handleModifiedAccounts(slotCtx, &sealevel.ExecutionCtx{TransactionContext: txCtx})

		copies := appendModifiedAccountCopies(nil, slotCtx)
		require.Len(t, copies, 1)
		require.NotSame(t, &params[8], &copies[0].Data[0])
		published = append(published, publication{copies[0], *copies[0].Clone()})
		require.Equal(t, []byte{1, 2, 3, 4, 5}, parent.Data)

		// The next bank switches arenas and clears this bank's in the background.
		resetSerializedParameterArena()
		waitSerializedParameterArenaCleared()
		if inArena {
			require.Equal(t, make([]byte, size), params, "the previous bank's arena was cleared")
		}
		for _, p := range published {
			require.Equal(t, p.want, *p.acct)
			require.Equal(t, bytes.Repeat([]byte{p.want.Data[0]}, 40), p.acct.Data)
		}
	}
}
