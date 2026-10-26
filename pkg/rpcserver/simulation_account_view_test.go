package rpcserver

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type simulationTestReader struct {
	mu       sync.Mutex
	revision uint64
	stored   map[solana.PublicKey]*accounts.Account
	source   simulationAccountSource
	reads    int
	err      error
	hook     func(solana.PublicKey)
}

func (reader *simulationTestReader) AccountReadVersion() uint64 {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return reader.revision
}

func (reader *simulationTestReader) replace(acct *accounts.Account) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.stored[acct.Key] = acct.Clone()
	reader.revision++
}

func (reader *simulationTestReader) GetAccount(slot uint64, key solana.PublicKey) (*accounts.Account, error) {
	return reader.GetAccountWithDataLimit(slot, key, ^uint64(0))
}

func (reader *simulationTestReader) GetAccountWithDataLimit(slot uint64, key solana.PublicKey, limit uint64) (*accounts.Account, error) {
	reader.mu.Lock()
	reader.reads++
	acct, found := reader.stored[key]
	err, hook := reader.err, reader.hook
	reader.mu.Unlock()
	if hook != nil {
		hook(key)
	}
	if err != nil {
		return nil, err
	}
	if found {
		if acct == nil {
			return nil, nil
		}
		if uint64(len(acct.Data)) > limit {
			return nil, accountsdb.ErrAccountDataLimit
		}
		return acct.Clone(), nil
	}
	if reader.source != nil {
		return reader.source.GetAccountWithDataLimit(slot, key, limit)
	}
	return nil, accountsdb.ErrNoAccount
}

func publishSimulationTestTip(t *testing.T, server *RpcServer, reader *simulationTestReader, gates ...features.FeatureGate) {
	t.Helper()
	bank := simulationBankFixtureAt(t, 150, solana.Hash{12}, 6000)
	for _, gate := range gates {
		bank.Features.EnableFeature(gate, 0)
	}
	ctx, err := newSimulationBankContext(bank, server.acctsDb, reader)
	require.NoError(t, err)
	replay.UpdateChainTipFromSlotCtxWithBankMetadata(ctx, ctx.Features, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{BlockHeight: 120})
	server.SetSlotCtx(ctx)
}

func TestSimulationAccountsUseSelectedStateAndNeverWriteIt(t *testing.T) {
	server := transactionBankServer(t)
	key := solana.PublicKey{0x11}
	rootAccount := &accounts.Account{Key: key, Lamports: 1_000_000, Owner: addresses.SystemProgramAddr}
	commitTransactionBankAccount(t, server, 100, rootAccount)
	reader := &simulationTestReader{source: server.acctsDb, stored: map[solana.PublicKey]*accounts.Account{key: {Key: key, Lamports: 2_000_000, Owner: addresses.SystemProgramAddr}}}
	publishSimulationTestTip(t, server, reader)
	_, wire := testLegacyTransaction(t)
	for _, check := range []struct {
		commitment   string
		balance, fee uint64
	}{{"finalized", 1_000_000, 5000}, {"processed", 2_000_000, 5000}} {
		response, err := server.SimulateTransaction(t.Context(), mustRawParams(t, []any{base64.StdEncoding.EncodeToString(wire), map[string]any{
			"encoding": "base64", "commitment": check.commitment, "replaceRecentBlockhash": true,
			"accounts": map[string]any{"addresses": []string{key.String()}, "encoding": "base64"},
		}}))
		require.NoError(t, err)
		require.Nil(t, response.Value.Err)
		require.Equal(t, []uint64{check.balance}, *response.Value.PreBalances)
		require.Equal(t, []uint64{check.balance - check.fee}, *response.Value.PostBalances)
		require.Equal(t, check.balance-check.fee, (*response.Value.Accounts)[0].Lamports)
	}
	stored, err := server.acctsDb.GetAccount(100, key)
	require.NoError(t, err)
	require.Equal(t, rootAccount.Lamports, stored.Lamports)
	require.Equal(t, uint64(2_000_000), reader.stored[key].Lamports)
}

func TestSimulationToken2022MintDoesNotReadBogusMint(t *testing.T) {
	bank := simulationBankFixture(t)
	mint := &accounts.Account{Key: solana.PublicKey{90}, Owner: splToken2022ProgramID, Lamports: 1, Data: make([]byte, 200)}
	mint.Data[165] = 1 // Token-2022 extended mint, not a token account.
	mint.Data[45] = 1
	view := &simulationAccounts{bank: bank, accounts: map[solana.PublicKey]*accounts.Account{mint.Key: mint}, sealed: true}
	require.Empty(t, tokenBalancesFromAccounts([]*accounts.Account{mint}, view, bank.Slot))
	require.NoError(t, view.err, "mint authority bytes must not be interpreted as another mint address")
}

func TestSimulationAccountFenceRejectsUnpublishedReplacement(t *testing.T) {
	server := transactionBankServer(t)
	tx, _ := testLegacyTransaction(t)
	key := tx.Message.AccountKeys[0]
	reader := &simulationTestReader{stored: make(map[solana.PublicKey]*accounts.Account)}
	reader.replace(&accounts.Account{Key: key, Lamports: 10})
	publishSimulationTestTip(t, server, reader)
	reader.replace(&accounts.Account{Key: key, Lamports: 20})
	_, _, _, _, _, err := server.prepareSimulation(t.Context(), tx, "processed", nil, nil, true)
	require.ErrorIs(t, err, errSimulationViewChanged)
	require.Zero(t, reader.reads, "old parameters must not read an unpublished replacement")
	publishSimulationTestTip(t, server, reader)
	_, _, view, _, _, err := server.prepareSimulation(t.Context(), tx, "processed", nil, nil, true)
	require.NoError(t, err)
	acct, err := view.GetAccount(150, key)
	require.NoError(t, err)
	require.Equal(t, uint64(20), acct.Lamports)
	reader.replace(&accounts.Account{Key: key, Lamports: 30})
	acct.Lamports = 999
	acct, err = view.GetAccount(150, key)
	require.NoError(t, err)
	require.Equal(t, uint64(20), acct.Lamports, "sealed input is independent of live updates and caller writes")
	_, err = view.GetAccount(150, solana.PublicKey{99})
	require.ErrorContains(t, err, "was not captured")
	require.Error(t, view.err)
}

func TestSimulationAccountCaptureRefusesStorageErrorsAndCancellation(t *testing.T) {
	for _, failure := range []string{"storage", "nil-value", "cancel", "recovery", "churn"} {
		t.Run(failure, func(t *testing.T) {
			server := transactionBankServer(t)
			tx, _ := testLegacyTransaction(t)
			reader := &simulationTestReader{stored: make(map[solana.PublicKey]*accounts.Account)}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ioErr := errors.New("fixture appendvec read failed")
			switch failure {
			case "storage":
				reader.err = ioErr
			case "nil-value":
				reader.stored[tx.Message.AccountKeys[0]] = nil
			case "cancel":
				reader.hook = func(solana.PublicKey) { cancel() }
			case "recovery":
				reader.hook = func(solana.PublicKey) { server.InvalidateHealth() }
			case "churn":
				reader.hook = func(key solana.PublicKey) { reader.replace(&accounts.Account{Key: key, Lamports: 1}) }
			}
			publishSimulationTestTip(t, server, reader)
			_, _, _, _, _, err := server.prepareSimulation(ctx, tx, "processed", nil, nil, true)
			require.Error(t, err)
			if failure == "storage" {
				require.ErrorIs(t, err, ioErr)
			}
			if failure == "nil-value" {
				require.ErrorContains(t, err, "no value or absence")
			}
			if failure == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.LessOrEqual(t, reader.reads, 2, "capture must not retry indefinitely without a caller deadline")
		})
	}
}

func TestSimulationAccountCaptureIncludesLargeCPIProgramData(t *testing.T) {
	server := transactionBankServer(t)
	tx, _ := testLegacyTransaction(t)
	program, programData := solana.PublicKey{70}, solana.PublicKey{71}
	tx.Message.AccountKeys = append(tx.Message.AccountKeys, program)
	data := make([]byte, 36)
	binary.LittleEndian.PutUint32(data, sealevel.UpgradeableLoaderStateTypeProgram)
	copy(data[4:], programData[:])
	reader := &simulationTestReader{stored: map[solana.PublicKey]*accounts.Account{
		program:     {Key: program, Owner: addresses.BpfLoaderUpgradeableAddr, Lamports: 1, Data: data},
		programData: {Key: programData, Lamports: 1, Data: make([]byte, 128<<10)},
	}}
	publishSimulationTestTip(t, server, reader)
	_, _, view, _, lookupErr, err := server.prepareSimulation(t.Context(), tx, "processed", nil, nil, true)
	require.NoError(t, err)
	require.Nil(t, lookupErr)
	acct, err := view.GetAccount(150, programData)
	require.NoError(t, err)
	require.Len(t, acct.Data, 128<<10, "execution capture must not reuse the optional metadata 64 KiB limit")
	_, err = view.GetAccount(150, solana.PublicKey(addresses.BpfLoaderUpgradeableAddr))
	require.ErrorIs(t, err, accountsdb.ErrNoAccount, "owner dependency is captured as explicit absence even without executable flag")
}

func TestSimulationCaptureBoundsAndAdmission(t *testing.T) {
	server := transactionBankServer(t)
	tx, _ := testLegacyTransaction(t)
	reader := &simulationTestReader{stored: map[solana.PublicKey]*accounts.Account{tx.Message.AccountKeys[0]: {Key: tx.Message.AccountKeys[0], Data: make([]byte, 16)}}}
	bank := simulationBankFixture(t)
	view := &simulationAccounts{bank: bank, source: reader, accounts: make(map[solana.PublicKey]*accounts.Account), remaining: 8}
	view.capture(t.Context(), tx.Message.AccountKeys[0])
	require.ErrorIs(t, view.err, accountsdb.ErrAccountDataLimit)
	first, err := server.acquireSimulation(t.Context())
	require.NoError(t, err)
	defer first()
	second, err := server.acquireSimulation(t.Context())
	require.NoError(t, err)
	defer second()
	_, err = server.acquireSimulation(t.Context())
	require.ErrorContains(t, err, "capacity")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = server.acquireSimulation(ctx)
	require.ErrorIs(t, err, context.Canceled)
}
