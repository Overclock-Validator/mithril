package rpcserver

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type responseAccountReader map[solana.PublicKey]*accounts.Account

func (reader responseAccountReader) GetAccount(_ uint64, key solana.PublicKey) (*accounts.Account, error) {
	if acct := reader[key]; acct != nil {
		return acct.Clone(), nil
	}
	return nil, accountsdb.ErrNoAccount
}

func TestSimulationProgramResponsesExcludeRuntimeStubs(t *testing.T) {
	payer, program := solana.PublicKey{1}, solana.PublicKey{2}
	real := &accounts.Account{Key: program, Lamports: 55, Data: []byte{3, 4}, Executable: true, RentEpoch: 7}
	dummy := &accounts.Account{Key: program, Executable: true, IsDummy: true}
	ctx := &sealevel.SlotCtx{Slot: 42, Accounts: accounts.NewMemAccounts()}
	require.NoError(t, ctx.SetAccount(program, real))
	postPayer := &accounts.Account{Key: payer, Lamports: 90}
	txAccounts := sealevel.NewTransactionAccountsFromRefs([]*accounts.Account{postPayer, dummy}, []bool{false, false})
	exec := &sealevel.ExecutionCtx{TransactionContext: sealevel.NewTransactionCtx(*txAccounts, 1, 1)}
	output := replay.LoadAndExecuteTransactionOutput{
		PreAccountSnapshots: []*accounts.Account{{Key: payer, Lamports: 100}, dummy.Clone()},
		PreBalances:         []uint64{100, 0}, ExecCtx: exec,
	}
	tx := &solana.Transaction{Message: solana.Message{AccountKeys: []solana.PublicKey{payer, program}}}
	pre, post := simulationAccountSnapshotsForOutput(ctx, tx, output)
	require.Equal(t, []uint64{100, 55}, balancesFromAccountSnapshots(pre))
	require.Equal(t, []uint64{90, 55}, balancesFromAccountSnapshots(post))
	conf := simulateTransactionConfig{accounts: &simulateAccountsConfig{addresses: []string{program.String(), payer.String()}}}
	rendered := renderRequestedAccounts(conf, exec, responseAccountReader{program: real}, ctx.Slot)
	require.Equal(t, uint64(55), (*rendered)[0].Lamports)
	require.Equal(t, uint64(7), (*rendered)[0].RentEpoch)
	require.Equal(t, []string{"AwQ=", "base64"}, (*rendered)[0].Data)
	require.Equal(t, uint64(90), (*rendered)[1].Lamports)
	require.Zero(t, dummy.Lamports, "response restoration must not change the runtime stub")
	require.Empty(t, dummy.Data)
	require.True(t, output.PreAccountSnapshots[1].IsDummy)
	require.Equal(t, []uint64{100, 0}, output.PreBalances)
	require.Equal(t, []byte{3, 4}, real.Data)
}

func TestSimulationTokenBalancesPreferTransactionLocalMint(t *testing.T) {
	mint, owner := solana.PublicKey{3}, solana.PublicKey{4}
	token := makeTokenAccount(t, mint, owner, 123)
	localMint := &accounts.Account{Key: mint, Owner: splTokenProgramID, Lamports: 1, Data: make([]byte, mintAccountSize)}
	localMint.Data[mintDecimalsOffset], localMint.Data[45] = 6, 1
	storedMint := localMint.Clone()
	storedMint.Data[mintDecimalsOffset] = 9
	got := tokenBalancesFromAccounts([]*accounts.Account{token, localMint}, responseAccountReader{mint: storedMint}, 42)
	require.Len(t, got, 1)
	require.Equal(t, uint8(6), got[0].UiTokenAmount.Decimals)
	require.Equal(t, "0.000123", got[0].UiTokenAmount.UiAmountString)
	require.Equal(t, byte(9), storedMint.Data[mintDecimalsOffset])
}

func TestSimulationTokenBalancesRejectExtendedMintsAndMultisigs(t *testing.T) {
	for _, state := range []struct {
		name string
		size int
		kind byte
		want int
	}{{"extended-mint", 200, 1, 0}, {"multisig", 355, 2, 0}, {"extended-account", 200, 2, 1}} {
		t.Run(state.name, func(t *testing.T) {
			acct := &accounts.Account{Owner: splToken2022ProgramID, Lamports: 1, Data: make([]byte, state.size)}
			acct.Data[108], acct.Data[165] = 1, state.kind
			require.Len(t, tokenBalancesFromAccounts([]*accounts.Account{acct}, responseAccountReader{}, 42), state.want)
			acct.Data[108] = 0
			require.Empty(t, tokenBalancesFromAccounts([]*accounts.Account{acct}, responseAccountReader{}, 42))
		})
	}
}
