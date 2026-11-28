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
	got := tokenBalanceIndices{0}.tokenBalancesFromAccounts([]*accounts.Account{token, localMint}, responseAccountReader{mint: storedMint}, 42)
	require.Len(t, got, 1)
	require.Equal(t, uint8(6), got[0].UiTokenAmount.Decimals)
	require.Equal(t, "0.000123", got[0].UiTokenAmount.UiAmountString)
	require.Equal(t, byte(9), storedMint.Data[mintDecimalsOffset])
}

func TestSimulationTokenBalancesRejectExtendedMintsAndMultisigs(t *testing.T) {
	mint := &accounts.Account{Key: solana.PublicKey{5}, Owner: splToken2022ProgramID, Lamports: 1, Data: make([]byte, mintAccountSize)}
	mint.Data[45] = 1
	for _, state := range []struct {
		name string
		size int
		kind byte
		want int
	}{{"extended-mint", 200, 1, 0}, {"multisig", 355, 2, 0}, {"extended-account", 200, 2, 1}} {
		t.Run(state.name, func(t *testing.T) {
			acct := &accounts.Account{Owner: splToken2022ProgramID, Lamports: 1, Data: make([]byte, state.size)}
			acct.Data[108], acct.Data[165] = 1, state.kind
			copy(acct.Data, mint.Key[:])
			require.Len(t, tokenBalanceIndices{0}.tokenBalancesFromAccounts([]*accounts.Account{acct}, responseAccountReader{mint.Key: mint}, 42), state.want)
			acct.Data[108] = 0
			require.Empty(t, tokenBalanceIndices{0}.tokenBalancesFromAccounts([]*accounts.Account{acct}, responseAccountReader{mint.Key: mint}, 42))
		})
	}
}

func TestSimulationTokenBalancesVerifyMint(t *testing.T) {
	key := solana.PublicKey{3}
	legacy := makeTokenAccount(t, key, solana.PublicKey{4}, 42)
	extended := legacy.Clone()
	extended.Owner = solana.Token2022ProgramID
	for _, tc := range []struct {
		name       string
		program    solana.PublicKey
		size       int
		kind, init byte
		lamports   uint64
		wantIndex  int
	}{
		{"legacy-zero-decimals", solana.TokenProgramID, 82, 0, 1, 1, 0},
		{"extended-zero-decimals", solana.Token2022ProgramID, 166, 1, 1, 1, 1},
		{"wrong-owner", solana.SystemProgramID, 82, 0, 1, 1, -1},
		{"uninitialized", solana.TokenProgramID, 82, 0, 0, 1, -1},
		{"closed", solana.Token2022ProgramID, 82, 0, 1, 0, -1},
		{"account-not-mint", solana.Token2022ProgramID, 166, 2, 1, 1, -1},
		{"multisig", solana.Token2022ProgramID, 355, 1, 1, 1, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mint := &accounts.Account{Key: key, Owner: tc.program, Lamports: tc.lamports, Data: make([]byte, tc.size)}
			mint.Data[45] = tc.init
			if tc.size > 165 {
				mint.Data[165] = tc.kind
			}
			got := tokenBalanceIndices{0, 1}.tokenBalancesFromAccounts([]*accounts.Account{legacy, extended}, responseAccountReader{key: mint}, 42)
			if tc.wantIndex < 0 {
				require.Empty(t, got)
				return
			}
			require.Len(t, got, 1)
			require.Equal(t, uint8(tc.wantIndex), got[0].AccountIndex)
			require.Equal(t, uint8(0), got[0].UiTokenAmount.Decimals)
			require.Equal(t, "42", got[0].UiTokenAmount.UiAmountString)
		})
	}
}

func TestSimulationTokenBalanceIndices(t *testing.T) {
	msg := solana.Message{
		AccountKeys:  []solana.PublicKey{{1}, {2}, solana.Token2022ProgramID, solana.TokenProgramID, {3}},
		Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 1}},
	}
	require.Equal(t, tokenBalanceIndices{0, 4}, simulationTokenBalanceIndices(&msg))
}
