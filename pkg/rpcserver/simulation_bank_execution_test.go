package rpcserver

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func TestSimulationExecutesTransferAgainstSelectedBankHTTP(t *testing.T) {
	server := transactionBankServer(t)
	payer := solana.NewWallet()
	recipient := solana.PublicKey{75}
	_, err := server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{
		{Key: payer.PublicKey(), Owner: addresses.SystemProgramAddr, Lamports: 10_000_000, RentEpoch: math.MaxUint64},
		{Key: recipient, Owner: addresses.SystemProgramAddr, Lamports: 2_000_000, RentEpoch: math.MaxUint64},
		{Key: solana.SystemProgramID, Owner: addresses.NativeLoaderAddr, Lamports: 1, Executable: true, RentEpoch: math.MaxUint64},
	}}}, 100, nil, nil)
	require.NoError(t, err)
	server.SetRootedExecutionBank(server.rootedBank.Load().executionBank)
	reader := &simulationTestReader{source: server.acctsDb, stored: map[solana.PublicKey]*accounts.Account{
		payer.PublicKey(): {Key: payer.PublicKey(), Owner: addresses.SystemProgramAddr, Lamports: 20_000_000, RentEpoch: math.MaxUint64},
		recipient:         {Key: recipient, Owner: addresses.SystemProgramAddr, Lamports: 3_000_000, RentEpoch: math.MaxUint64},
	}}
	publishSimulationTestTip(t, server, reader)
	rpc := NewRpcServer(server.acctsDb, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}, solana.Hash{1})
	require.NoError(t, rpc.listener.Close())
	rpc.SetRootedBankState(100, 70, 0)
	rpc.SetRootedExecutionBank(server.rootedBank.Load().executionBank)
	rpc.SetSlotCtx(server.getSlotCtx())
	endpoint := httptest.NewServer(rpc)
	t.Cleanup(endpoint.Close)
	for _, check := range []struct {
		commitment             string
		hash                   solana.Hash
		payer, recipient, slot uint64
	}{
		{"finalized", solana.Hash{11}, 10_000_000, 2_000_000, 100},
		{"processed", solana.Hash{12}, 20_000_000, 3_000_000, 150},
	} {
		tx, err := solana.NewTransaction([]solana.Instruction{system.NewTransferInstruction(100, payer.PublicKey(), recipient).Build()}, check.hash)
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
		request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "simulateTransaction", "params": []any{
			base64.StdEncoding.EncodeToString(wire), map[string]any{"encoding": "base64", "commitment": check.commitment, "sigVerify": true},
		}})
		require.NoError(t, err)
		response, err := endpoint.Client().Post(endpoint.URL, "application/json", bytes.NewReader(request))
		require.NoError(t, err)
		var reply struct {
			Result SimulateTransactionResp `json:"result"`
			Error  json.RawMessage         `json:"error"`
		}
		err = json.NewDecoder(response.Body).Decode(&reply)
		require.NoError(t, response.Body.Close())
		require.NoError(t, err)
		require.Empty(t, reply.Error)
		require.Nil(t, reply.Result.Value.Err)
		require.Equal(t, check.slot, reply.Result.Context.Slot)
		require.Equal(t, []uint64{check.payer, check.recipient, 1}, *reply.Result.Value.PreBalances)
		require.Equal(t, []uint64{check.payer - 5100, check.recipient + 100, 1}, *reply.Result.Value.PostBalances)
		require.NotNil(t, reply.Result.Value.Logs)
		require.Positive(t, *reply.Result.Value.UnitsConsumed)
		// Exercise the same signed transaction through real preflight execution,
		// but never forward it to a public TPU or submit an on-chain transaction.
		ctx, resolved, view, _, lookupErr, err := rpc.prepareSimulation(t.Context(), tx, check.commitment, nil, nil, true)
		require.NoError(t, err)
		require.Nil(t, lookupErr)
		require.NoError(t, rpc.preflightSendTransaction(context.Background(), resolved, ctx))
		require.NoError(t, view.err)
	}
	stored, err := server.acctsDb.GetAccount(100, payer.PublicKey())
	require.NoError(t, err)
	require.Equal(t, uint64(10_000_000), stored.Lamports)
	require.Equal(t, uint64(20_000_000), reader.stored[payer.PublicKey()].Lamports)
}

func TestSimulationExecutesSelectedUpgradeableProgramData(t *testing.T) {
	for _, check := range []struct {
		name  string
		gates []features.FeatureGate
	}{
		{"legacy-dummy", nil},
		{"formalized-dummy", []features.FeatureGate{features.FormalizeLoadedTransactionDataSize}},
		{"legacy-full", []features.FeatureGate{features.DisableAccountLoaderSpecialCase}},
		{"formalized-full", []features.FeatureGate{features.FormalizeLoadedTransactionDataSize, features.DisableAccountLoaderSpecialCase}},
		{"dummy-rent-disabled", []features.FeatureGate{features.FormalizeLoadedTransactionDataSize, features.DisableRentFeesCollection}},
	} {
		t.Run(check.name, func(t *testing.T) { testSelectedUpgradeableProgramData(t, check.gates) })
	}
}

func testSelectedUpgradeableProgramData(t *testing.T, gates []features.FeatureGate) {
	t.Helper()
	server := transactionBankServer(t)
	payer := solana.NewWallet()
	program, programData := solana.PublicKey{83}, solana.PublicKey{84}
	programState := make([]byte, 36)
	binary.LittleEndian.PutUint32(programState, sealevel.UpgradeableLoaderStateTypeProgram)
	copy(programState[4:], programData[:])
	code := func(name string) *accounts.Account {
		elf, err := os.ReadFile("../../fixtures/sbpf/" + name + ".so")
		require.NoError(t, err)
		data := make([]byte, 45+len(elf))
		binary.LittleEndian.PutUint32(data, sealevel.UpgradeableLoaderStateTypeProgramData)
		binary.LittleEndian.PutUint64(data[4:], 1)
		copy(data[45:], elf)
		return &accounts.Account{Key: programData, Owner: addresses.BpfLoaderUpgradeableAddr, Lamports: 10_000_000, Data: data, RentEpoch: math.MaxUint64}
	}
	rootCode, tipCode := code("noop_aligned"), code("panic")
	_, err := server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{
		{Key: payer.PublicKey(), Owner: addresses.SystemProgramAddr, Lamports: 10_000_000, RentEpoch: math.MaxUint64},
		{Key: program, Owner: addresses.BpfLoaderUpgradeableAddr, Lamports: 10_000_000, Executable: true, Data: programState, RentEpoch: 17},
		{Key: solana.PublicKey(addresses.BpfLoaderUpgradeableAddr), Owner: addresses.NativeLoaderAddr, Lamports: 1, Executable: true, RentEpoch: math.MaxUint64},
		rootCode,
	}}}, 100, nil, nil)
	require.NoError(t, err)
	root := *server.rootedBank.Load().executionBank
	root.Features = root.Features.Clone()
	for _, gate := range gates {
		root.Features.EnableFeature(gate, 0)
	}
	server.SetRootedExecutionBank(&root)
	reader := &simulationTestReader{source: server.acctsDb, stored: map[solana.PublicKey]*accounts.Account{programData: tipCode}}
	publishSimulationTestTip(t, server, reader, gates...)
	for _, commitment := range []string{"finalized", "processed", "finalized"} {
		tx := &solana.Transaction{Signatures: []solana.Signature{{}}, Message: solana.Message{
			Header:       solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
			AccountKeys:  solana.PublicKeySlice{payer.PublicKey(), program},
			Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 1}},
		}}
		wire, err := tx.MarshalBinary()
		require.NoError(t, err)
		response, err := server.SimulateTransaction(t.Context(), mustRawParams(t, []any{base64.StdEncoding.EncodeToString(wire), map[string]any{
			"encoding": "base64", "commitment": commitment, "replaceRecentBlockhash": true,
		}}))
		require.NoError(t, err)
		if commitment == "processed" {
			require.NotNil(t, response.Value.Err, "the processed bank's program aborts; it must not execute the rooted noop")
		} else {
			require.Nil(t, response.Value.Err, "the rooted noop must remain independent of processed program data")
		}
		require.NotNil(t, response.Value.Logs)
		require.Positive(t, *response.Value.UnitsConsumed)
	}
	tx := &solana.Transaction{Signatures: []solana.Signature{{}}, Message: solana.Message{
		Header:      solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
		AccountKeys: solana.PublicKeySlice{payer.PublicKey(), program}, RecentBlockhash: root.LastBlockhash,
		Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 1}},
	}}
	ctx, resolved, view, _, lookupErr, err := server.prepareSimulation(t.Context(), tx, "finalized", nil, nil, true)
	require.NoError(t, err)
	require.Nil(t, lookupErr)
	before, err := ctx.GetAccount(program)
	require.NoError(t, err)
	output := replay.LoadAndExecuteTransaction(replay.LoadAndExecuteTransactionInput{SlotCtx: ctx, Transaction: resolved})
	require.Nil(t, output.ProcessingResult.TransactionError)
	require.False(t, output.ExecCtx.TransactionContext.Accounts.Touched[1], "a readonly program stub must never be published over real program data")
	require.NoError(t, replay.ApplySuccessfulTransaction(ctx, output))
	after, err := ctx.GetAccount(program)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, view.err)
	for _, absent := range []bool{false, true} {
		if absent {
			tx.Message.AccountKeys[1] = solana.PublicKey{99}
		} else {
			reader.replace(&accounts.Account{Key: program})
			publishSimulationTestTip(t, server, reader, gates...)
		}
		wire, err := tx.MarshalBinary()
		require.NoError(t, err)
		response, err := server.SimulateTransaction(t.Context(), mustRawParams(t, []any{base64.StdEncoding.EncodeToString(wire), map[string]any{
			"encoding": "base64", "commitment": "processed", "replaceRecentBlockhash": true,
		}}))
		require.NoError(t, err)
		require.NotNil(t, response.Value.Err, "zero or missing real programs must still be rejected")
	}
	stored, err := server.acctsDb.GetAccount(100, programData)
	require.NoError(t, err)
	require.Equal(t, rootCode.Data, stored.Data)
	require.Equal(t, tipCode.Data, reader.stored[programData].Data)
}

func simulationLookupTable(t *testing.T, key solana.PublicKey, extended uint64, address solana.PublicKey) *accounts.Account {
	t.Helper()
	var data bytes.Buffer
	encoder := bin.NewBinEncoder(&data)
	require.NoError(t, encoder.WriteUint32(sealevel.AddressLookupTableProgramStateLookupTable, bin.LE))
	meta := sealevel.LookupTableMeta{DeactivationSlot: math.MaxUint64, LastExtendedSlot: extended}
	require.NoError(t, meta.MarshalWithEncoder(encoder))
	for data.Len() < sealevel.AddressLookupTableMetaSize {
		require.NoError(t, encoder.WriteByte(0))
	}
	require.NoError(t, encoder.WriteBytes(address[:], false))
	return &accounts.Account{Key: key, Owner: addresses.AddressLookupTableAddr, Lamports: 1_000_000, Data: data.Bytes()}
}

func TestSimulationAddressTablesFollowSelectedBankWithoutMutatingWire(t *testing.T) {
	server := transactionBankServer(t)
	table, rootAddress, tipAddress := solana.PublicKey{80}, solana.PublicKey{81}, solana.PublicKey{82}
	commitTransactionBankAccount(t, server, 100, simulationLookupTable(t, table, 90, rootAddress))
	reader := &simulationTestReader{source: server.acctsDb, stored: map[solana.PublicKey]*accounts.Account{table: simulationLookupTable(t, table, 140, tipAddress)}}
	publishSimulationTestTip(t, server, reader)
	tx, _ := testLegacyTransaction(t)
	tx.Message.AddressTableLookups = []solana.MessageAddressTableLookup{{AccountKey: table, WritableIndexes: []uint8{0}}}
	_, err := tx.Message.SetVersion(solana.MessageVersionV0)
	require.NoError(t, err)
	original, err := tx.MarshalBinary()
	require.NoError(t, err)
	for _, check := range []struct {
		commitment string
		address    solana.PublicKey
	}{{"finalized", rootAddress}, {"processed", tipAddress}} {
		_, resolved, view, _, lookupErr, err := server.prepareSimulation(t.Context(), tx, check.commitment, nil, nil, true)
		require.NoError(t, err)
		require.Nil(t, lookupErr)
		require.Equal(t, check.address, resolved.Message.AccountKeys[1])
		require.NoError(t, view.err)
		wire, err := tx.MarshalBinary()
		require.NoError(t, err)
		require.Equal(t, original, wire)
		require.False(t, tx.Message.IsResolved())
	}
	reader.replace(simulationLookupTable(t, table, 150, tipAddress))
	publishSimulationTestTip(t, server, reader)
	_, _, _, _, lookupErr, err := server.prepareSimulation(t.Context(), tx, "processed", nil, nil, true)
	require.NoError(t, err)
	require.NotNil(t, lookupErr, "an address appended in the selected slot is still warming up")
}
