package rpcserver

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
	"net/http/httptest"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func TestSimulationInstructionCompatibilityHTTP(t *testing.T) {
	server := transactionBankServer(t)
	payer, recipient := solana.NewWallet(), solana.PublicKey{77}
	bank := func(slot uint64, hash solana.Hash, fee uint64) *replay.ChainTipSnapshot {
		value := simulationBankFixtureAt(t, slot, hash, fee)
		var sysvars []*accounts.Account
		require.NoError(t, value.BankSysvars.RangeAccountViews(func(key solana.PublicKey, account *accounts.Account) error {
			owned := account.Clone()
			if key == sealevel.SysvarEpochScheduleAddr {
				owned.Owner, owned.Lamports, owned.RentEpoch = addresses.SysvarOwnerAddr, 1_120_560, 0
			}
			sysvars = append(sysvars, owned)
			return nil
		}))
		var err error
		value.BankSysvars, err = sealevel.NewBankSysvars(slot, sysvars...)
		require.NoError(t, err)
		return value
	}
	root, tip := bank(100, solana.Hash{11}, 5000), bank(150, solana.Hash{12}, 6000)
	root.BlockHeight = 70
	readonly, ok := root.BankSysvars.CloneAccount(sealevel.SysvarEpochScheduleAddr)
	require.True(t, ok)
	rootPayer := &accounts.Account{Key: payer.PublicKey(), Owner: addresses.SystemProgramAddr, Lamports: 10_000_000, RentEpoch: math.MaxUint64}
	rootRecipient := &accounts.Account{Key: recipient, Owner: addresses.SystemProgramAddr, Lamports: 2_000_000, RentEpoch: math.MaxUint64}
	_, err := server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{
		rootPayer, rootRecipient, readonly,
		{Key: solana.SystemProgramID, Owner: addresses.NativeLoaderAddr, Lamports: 1, Executable: true, RentEpoch: math.MaxUint64},
	}}}, 100, nil, nil)
	require.NoError(t, err)
	rootReadonlyBefore, err := server.acctsDb.GetAccount(100, sealevel.SysvarEpochScheduleAddr)
	require.NoError(t, err)
	reader := &simulationTestReader{source: server.acctsDb, stored: map[solana.PublicKey]*accounts.Account{
		payer.PublicKey(): {Key: payer.PublicKey(), Owner: addresses.SystemProgramAddr, Lamports: 20_000_000, RentEpoch: math.MaxUint64},
		recipient:         {Key: recipient, Owner: addresses.SystemProgramAddr, Lamports: 3_000_000, RentEpoch: math.MaxUint64},
	}}
	tipCtx, err := newSimulationBankContext(tip, server.acctsDb, reader)
	require.NoError(t, err)
	replay.UpdateChainTipFromSlotCtxWithBankMetadata(tipCtx, tipCtx.Features, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{BlockHeight: 120})
	rpc := NewRpcServer(server.acctsDb, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}, solana.Hash{1})
	require.NoError(t, rpc.listener.Close())
	rpc.SetRootedBankState(100, 70, 0)
	rpc.SetRootedExecutionBank(root)
	rpc.SetSlotCtx(tipCtx)
	endpoint := httptest.NewServer(rpc)
	t.Cleanup(endpoint.Close)
	for _, commitment := range []string{"finalized", "confirmed", "processed"} {
		t.Run(commitment, func(t *testing.T) {
			tx, err := solana.NewTransaction([]solana.Instruction{system.NewTransferInstruction(1, payer.PublicKey(), recipient).Build()}, solana.Hash{})
			require.NoError(t, err)
			tx.Message.AccountKeys = append(tx.Message.AccountKeys, sealevel.SysvarEpochScheduleAddr)
			tx.Message.Header.NumReadonlyUnsignedAccounts++
			tx.Signatures = []solana.Signature{{}}
			wire, err := tx.MarshalBinary()
			require.NoError(t, err)
			request, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "simulateTransaction", "params": []any{
				base64.StdEncoding.EncodeToString(wire), map[string]any{"encoding": "base64", "commitment": commitment, "replaceRecentBlockhash": true,
					"accounts": map[string]any{"encoding": "base64", "addresses": []string{solana.PublicKey(sealevel.SysvarEpochScheduleAddr).String()}}},
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
			require.Equal(t, uint64(5000), *reply.Result.Value.Fee)
			require.Equal(t, uint64(150), *reply.Result.Value.UnitsConsumed)
			payerBefore, recipientBefore, selected := uint64(10_000_000), uint64(2_000_000), uint64(100)
			if commitment == "processed" {
				payerBefore, recipientBefore, selected = 20_000_000, 3_000_000, 150
			}
			require.Equal(t, selected, reply.Result.Context.Slot)
			require.Equal(t, []uint64{payerBefore - 5001, recipientBefore + 1, 1, readonly.Lamports}, *reply.Result.Value.PostBalances)
			stored, err := server.acctsDb.GetAccount(100, sealevel.SysvarEpochScheduleAddr)
			require.NoError(t, err)
			require.Equal(t, rootReadonlyBefore, stored)
			stored, err = server.acctsDb.GetAccount(100, payer.PublicKey())
			require.NoError(t, err)
			require.Equal(t, rootPayer.Lamports, stored.Lamports)
			require.Equal(t, uint64(20_000_000), reader.stored[payer.PublicKey()].Lamports)
			for _, source := range []*sealevel.BankSysvars{root.BankSysvars, tip.BankSysvars} {
				unchanged, ok := source.CloneAccount(sealevel.SysvarEpochScheduleAddr)
				require.True(t, ok)
				require.Equal(t, readonly, unchanged)
			}
			t.Run("stable-logs", func(t *testing.T) {
				require.Equal(t, []string{"Program " + solana.SystemProgramID.String() + " invoke [1]", "Program " + solana.SystemProgramID.String() + " success"}, *reply.Result.Value.Logs)
			})
			t.Run("readonly-post-state", func(t *testing.T) {
				require.Len(t, *reply.Result.Value.Accounts, 1)
				post := (*reply.Result.Value.Accounts)[0]
				require.Equal(t, &AccountInfoPayload{Data: []any{base64.StdEncoding.EncodeToString(readonly.Data), "base64"}, Executable: false, Lamports: readonly.Lamports,
					Owner: solana.PublicKey(readonly.Owner).String(), RentEpoch: 0, Space: len(readonly.Data)}, post)
			})
		})
	}
}
