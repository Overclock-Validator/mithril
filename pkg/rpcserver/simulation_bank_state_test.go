package rpcserver

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func TestSimulationDurableNonceUsesSelectedBank(t *testing.T) {
	server := transactionBankServer(t)
	payer := solana.NewWallet()
	nonceKey := solana.PublicKey{91}
	rootNonce, tipNonce := solana.Hash{9}, solana.Hash{10}
	nonceAccount := func(value solana.Hash) *accounts.Account {
		state := sealevel.NonceStateVersions{Type: sealevel.NonceVersionCurrent, Current: sealevel.NonceData{
			IsInitialized: true, Authority: payer.PublicKey(), DurableNonce: [32]byte(value),
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
		}}
		data, err := state.Marshal()
		require.NoError(t, err)
		return &accounts.Account{Key: nonceKey, Owner: addresses.SystemProgramAddr, Lamports: 4_000_000, Data: data, RentEpoch: math.MaxUint64}
	}
	rootAccount, tipAccount := nonceAccount(rootNonce), nonceAccount(tipNonce)
	rootBefore, tipBefore := append([]byte(nil), rootAccount.Data...), append([]byte(nil), tipAccount.Data...)
	_, err := server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{
		{Key: payer.PublicKey(), Owner: addresses.SystemProgramAddr, Lamports: 10_000_000, RentEpoch: math.MaxUint64},
		{Key: solana.SystemProgramID, Owner: addresses.NativeLoaderAddr, Lamports: 1, Executable: true, RentEpoch: math.MaxUint64},
		rootAccount,
	}}}, 100, nil, nil)
	require.NoError(t, err)
	server.SetRootedExecutionBank(server.rootedBank.Load().executionBank)
	reader := &simulationTestReader{source: server.acctsDb, stored: map[solana.PublicKey]*accounts.Account{nonceKey: tipAccount}}
	publishSimulationTestTip(t, server, reader)
	signedNonceTransaction := func(t *testing.T, value solana.Hash) (*solana.Transaction, []byte) {
		t.Helper()
		tx, err := solana.NewTransaction([]solana.Instruction{
			system.NewAdvanceNonceAccountInstruction(nonceKey, solana.SysVarRecentBlockHashesPubkey, payer.PublicKey()).Build(),
		}, value, solana.TransactionPayer(payer.PublicKey()))
		require.NoError(t, err)
		_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
			if key == payer.PublicKey() {
				return &payer.PrivateKey
			}
			return nil
		})
		require.NoError(t, err)
		wire, err := tx.MarshalBinary()
		require.NoError(t, err)
		return tx, wire
	}
	for _, check := range []struct {
		commitment         string
		nonce, other, head solana.Hash
		slot, signatureFee uint64
	}{
		{"finalized", rootNonce, tipNonce, solana.Hash{11}, 100, 5000},
		{"processed", tipNonce, rootNonce, solana.Hash{12}, 150, 6000},
	} {
		t.Run(check.commitment, func(t *testing.T) {
			tx, wire := signedNonceTransaction(t, check.nonce)
			response, err := server.SimulateTransaction(t.Context(), mustRawParams(t, []any{base64.StdEncoding.EncodeToString(wire), map[string]any{
				"encoding": "base64", "commitment": check.commitment, "sigVerify": true,
				"accounts": map[string]any{"addresses": []string{nonceKey.String()}, "encoding": "base64"},
			}}))
			require.NoError(t, err)
			require.Nil(t, response.Value.Err)
			require.Equal(t, check.slot, response.Context.Slot)
			require.NotNil(t, response.Value.UnitsConsumed)
			require.Positive(t, *response.Value.UnitsConsumed)
			require.NotNil(t, response.Value.Accounts)
			require.Len(t, *response.Value.Accounts, 1)
			require.NotNil(t, (*response.Value.Accounts)[0])
			encoded, ok := (*response.Value.Accounts)[0].Data.([]string)
			require.True(t, ok)
			require.Len(t, encoded, 2)
			require.Equal(t, "base64", encoded[1])
			advancedBytes, err := base64.StdEncoding.DecodeString(encoded[0])
			require.NoError(t, err)
			advanced, err := sealevel.UnmarshalNonceStateVersions(advancedBytes)
			require.NoError(t, err)
			expected := sha256.Sum256(append([]byte("DURABLE_NONCE"), check.head[:]...))
			require.Equal(t, expected, advanced.State().DurableNonce)
			require.Equal(t, check.signatureFee, advanced.State().FeeCalculator.LamportsPerSignature)

			// A nonce that exists only in the other bank must not pass age checking.
			wrongTx, wrongWire := signedNonceTransaction(t, check.other)
			wrong, err := server.SimulateTransaction(t.Context(), mustRawParams(t, []any{base64.StdEncoding.EncodeToString(wrongWire), map[string]any{
				"encoding": "base64", "commitment": check.commitment, "sigVerify": true,
			}}))
			require.NoError(t, err)
			txErr, ok := wrong.Value.Err.(*replay.TransactionError)
			require.True(t, ok)
			require.Equal(t, replay.TransactionErrorBlockhashNotFound, txErr.ErrorType)
			for _, candidate := range []*solana.Transaction{tx, wrongTx} {
				ctx, resolved, view, _, lookupErr, err := server.prepareSimulation(t.Context(), candidate, check.commitment, nil, nil, true)
				require.NoError(t, err)
				require.Nil(t, lookupErr)
				err = server.preflightSendTransaction(t.Context(), resolved, ctx)
				if candidate == tx {
					require.NoError(t, err)
				} else {
					var failure *SendTransactionPreflightFailureError
					require.ErrorAs(t, err, &failure)
					require.Equal(t, "Transaction simulation failed: Blockhash not found", failure.Message)
				}
				require.NoError(t, view.err)
			}
		})
	}
	stored, err := server.acctsDb.GetAccount(100, nonceKey)
	require.NoError(t, err)
	require.Equal(t, rootBefore, stored.Data)
	require.Equal(t, uint64(4_000_000), stored.Lamports)
	require.Equal(t, tipBefore, reader.stored[nonceKey].Data)
	stored, err = server.acctsDb.GetAccount(100, payer.PublicKey())
	require.NoError(t, err)
	require.Equal(t, uint64(10_000_000), stored.Lamports)
}

func TestSimulationTokenMintDecimalsUseSelectedBank(t *testing.T) {
	server := transactionBankServer(t)
	payer := solana.NewWallet()
	tokenKey, mintKey, recipient := solana.PublicKey{92}, solana.PublicKey{93}, solana.PublicKey{94}
	tokenAccount := makeTokenAccount(t, mintKey, payer.PublicKey(), 1_234_567)
	tokenAccount.Key, tokenAccount.Lamports, tokenAccount.RentEpoch = tokenKey, 10_000_000, math.MaxUint64
	mintAccount := func(decimals byte) *accounts.Account {
		data := make([]byte, mintAccountSize)
		binary.LittleEndian.PutUint64(data[36:], 1_234_567)
		data[mintDecimalsOffset], data[45] = decimals, 1
		return &accounts.Account{Key: mintKey, Owner: splTokenProgramID, Lamports: 10_000_000, Data: data, RentEpoch: math.MaxUint64}
	}
	rootMint, tipMint := mintAccount(6), mintAccount(9)
	rootBefore, tipBefore := append([]byte(nil), rootMint.Data...), append([]byte(nil), tipMint.Data...)
	_, err := server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{
		{Key: payer.PublicKey(), Owner: addresses.SystemProgramAddr, Lamports: 10_000_000, RentEpoch: math.MaxUint64},
		{Key: recipient, Owner: addresses.SystemProgramAddr, Lamports: 10_000_000, RentEpoch: math.MaxUint64},
		{Key: solana.SystemProgramID, Owner: addresses.NativeLoaderAddr, Lamports: 1, Executable: true, RentEpoch: math.MaxUint64},
		tokenAccount, rootMint,
	}}}, 100, nil, nil)
	require.NoError(t, err)
	server.SetRootedExecutionBank(server.rootedBank.Load().executionBank)
	reader := &simulationTestReader{source: server.acctsDb, stored: map[solana.PublicKey]*accounts.Account{mintKey: tipMint}}
	publishSimulationTestTip(t, server, reader)
	for _, check := range []struct {
		commitment string
		hash       solana.Hash
		decimals   uint8
		amount     string
	}{
		{"finalized", solana.Hash{11}, 6, "1.234567"},
		{"processed", solana.Hash{12}, 9, "0.001234567"},
	} {
		t.Run(check.commitment, func(t *testing.T) {
			tx, err := solana.NewTransaction([]solana.Instruction{
				system.NewTransferInstruction(100, payer.PublicKey(), recipient).Build(),
			}, check.hash, solana.TransactionPayer(payer.PublicKey()))
			require.NoError(t, err)
			// The valid unused readonly key is observed by the balance collector;
			// its mint is deliberately external to the transaction's account set.
			tx.Message.AccountKeys = append(tx.Message.AccountKeys, tokenKey)
			tx.Message.Header.NumReadonlyUnsignedAccounts++
			require.NotContains(t, tx.Message.AccountKeys, mintKey)
			_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
				if key == payer.PublicKey() {
					return &payer.PrivateKey
				}
				return nil
			})
			require.NoError(t, err)
			wire, err := tx.MarshalBinary()
			require.NoError(t, err)
			response, err := server.SimulateTransaction(t.Context(), mustRawParams(t, []any{base64.StdEncoding.EncodeToString(wire), map[string]any{
				"encoding": "base64", "commitment": check.commitment, "sigVerify": true,
			}}))
			require.NoError(t, err)
			require.Nil(t, response.Value.Err)
			require.NotNil(t, response.Value.PostBalances)
			require.Equal(t, uint64(10_000_000-5100), (*response.Value.PostBalances)[0])
			for _, balances := range []*[]TokenBalancePayload{response.Value.PreTokenBalances, response.Value.PostTokenBalances} {
				require.NotNil(t, balances)
				require.Len(t, *balances, 1)
				require.Equal(t, mintKey.String(), (*balances)[0].Mint)
				require.Equal(t, "1234567", (*balances)[0].UiTokenAmount.Amount)
				require.Equal(t, check.decimals, (*balances)[0].UiTokenAmount.Decimals)
				require.Equal(t, check.amount, (*balances)[0].UiTokenAmount.UiAmountString)
			}
		})
	}
	stored, err := server.acctsDb.GetAccount(100, mintKey)
	require.NoError(t, err)
	require.Equal(t, rootBefore, stored.Data)
	require.Equal(t, tipBefore, reader.stored[mintKey].Data)
	stored, err = server.acctsDb.GetAccount(100, payer.PublicKey())
	require.NoError(t, err)
	require.Equal(t, uint64(10_000_000), stored.Lamports)
}
