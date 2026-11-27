package rpcserver

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func transactionOptionsServer(t *testing.T) *RpcServer {
	t.Helper()
	oldSlot := global.Slot()
	t.Cleanup(func() { global.SetSlot(oldSlot) })
	global.SetSlot(150)
	server := transactionBankServer(t)
	bank := replay.ChainTipParentContext()
	server.SetRootedBankState(bank.Slot, bank.BlockHeight, 0)
	server.SetRootedExecutionBank(&bank)
	return server
}

func TestTransactionOptionsSimulationEncoding(t *testing.T) {
	s := transactionOptionsServer(t)
	_, wire := testLegacyTransaction(t)
	base58Tx := base58.Encode(wire)
	for _, params := range [][]any{
		{base58Tx}, {base58Tx, nil}, {base58Tx, map[string]any{}},
		{base58Tx, map[string]any{"encoding": nil}},
		{base58Tx, map[string]any{"encoding": "base58"}},
		{base64.StdEncoding.EncodeToString(wire), map[string]any{"encoding": "base64"}},
	} {
		_, err := s.SimulateTransaction(t.Context(), mustRawParams(t, params))
		require.NoError(t, err, "explicit and default encoding must reach simulation")
	}
	_, err := s.SimulateTransaction(t.Context(), mustRawParams(t, []any{base58Tx, map[string]any{"encoding": "unsupported"}}))
	require.ErrorContains(t, err, "unsupported encoding")
	_, err = s.SimulateTransaction(t.Context(), mustRawParams(t, []any{"0OIl"}))
	require.ErrorContains(t, err, "failed to decode")
}

func TestTransactionOptionsMinimumContextReportsAvailableSlot(t *testing.T) {
	s := transactionOptionsServer(t)
	_, wire := testLegacyTransaction(t)
	p := mustRawParams(t, []any{base64.StdEncoding.EncodeToString(wire), map[string]any{"encoding": "base64", "minContextSlot": 151}})
	for _, call := range []func(jsonrpc.RawParams) error{
		func(p jsonrpc.RawParams) error { _, err := s.SendTransaction(t.Context(), p); return err },
		func(p jsonrpc.RawParams) error { _, err := s.SimulateTransaction(t.Context(), p); return err },
	} {
		var minErr *MinContextSlotNotReachedError
		require.ErrorAs(t, call(p), &minErr)
		require.Equal(t, uint64(150), minErr.ContextSlot)
		wireErr, err := minErr.ToJSONRPCError()
		require.NoError(t, err)
		require.Equal(t, rpcCodeMinContextSlotNotReached, wireErr.Code)
		data, err := json.Marshal(wireErr.Data)
		require.NoError(t, err)
		require.JSONEq(t, `{"contextSlot":150}`, string(data))
	}
	p = mustRawParams(t, []any{base64.StdEncoding.EncodeToString(wire), map[string]any{"encoding": "base64", "minContextSlot": 150}})
	_, err := s.SimulateTransaction(t.Context(), p)
	require.NoError(t, err)
	_, err = s.SendTransaction(t.Context(), p)
	var signatureFailure *SendTransactionPreflightFailureError
	require.ErrorAs(t, err, &signatureFailure, "exact slot boundary proceeds to signature validation without sending")
}

func TestTransactionOptionsUseSelectedBank(t *testing.T) {
	s := transactionOptionsServer(t)
	_, wire := testLegacyTransaction(t)
	for _, progress := range []uint64{100, 900} {
		global.SetSlot(progress)
		params := mustRawParams(t, []any{base58.Encode(wire), map[string]any{"minContextSlot": 151}})
		_, sendErr := s.SendTransaction(t.Context(), params)
		_, simulateErr := s.SimulateTransaction(t.Context(), params)
		for _, err := range []error{sendErr, simulateErr} {
			var minimum *MinContextSlotNotReachedError
			require.ErrorAs(t, err, &minimum)
			require.Equal(t, uint64(150), minimum.ContextSlot)
		}

		params = mustRawParams(t, []any{base58.Encode(wire), map[string]any{"minContextSlot": 149}})
		response, err := s.SimulateTransaction(t.Context(), params)
		require.NoError(t, err)
		require.Equal(t, uint64(150), response.Context.Slot)
		_, err = s.SendTransaction(t.Context(), params)
		var signatureFailure *SendTransactionPreflightFailureError
		require.ErrorAs(t, err, &signatureFailure, "preflight must reject the unsigned fixture before forwarding")

		params = mustRawParams(t, []any{base58.Encode(wire), map[string]any{"sigVerify": true}})
		response, err = s.SimulateTransaction(t.Context(), params)
		require.NoError(t, err)
		require.Equal(t, uint64(150), response.Context.Slot)
		require.Equal(t, "SignatureFailure", response.Value.Err)
	}
}

func TestTransactionOptionsExactMinimum(t *testing.T) {
	s := transactionOptionsServer(t)
	bank := simulationBankFixtureAt(t, 1<<53, solana.Hash{12}, 6000)
	bank.BlockHeight = 120
	ctx, err := newSimulationBankContext(bank, s.acctsDb, s.acctsDb)
	require.NoError(t, err)
	ctx.UnrootedRead = nil
	replay.UpdateChainTipFromSlotCtxWithBankMetadata(ctx, ctx.Features, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{BlockHeight: bank.BlockHeight})
	s.SetSlotCtx(ctx)
	s.SetRootedBankState(bank.Slot, bank.BlockHeight, 0)
	s.SetRootedExecutionBank(bank)
	_, wire := testLegacyTransaction(t)
	params := mustRawParams(t, []any{base58.Encode(wire), map[string]any{"minContextSlot": uint64(1<<53) + 1}})
	_, sendErr := s.SendTransaction(t.Context(), params)
	_, simulateErr := s.SimulateTransaction(t.Context(), params)
	for _, err := range []error{sendErr, simulateErr} {
		var minimum *MinContextSlotNotReachedError
		require.ErrorAs(t, err, &minimum)
		require.Equal(t, uint64(1<<53), minimum.ContextSlot)
	}
}

func TestTransactionOptionsRejectInvalidConfig(t *testing.T) {
	s := transactionOptionsServer(t)
	_, wire := testLegacyTransaction(t)
	for _, config := range []string{
		`true`, `[]`, `{"minContextSlot":1.5}`, `{"minContextSlot":-1}`,
		`{"minContextSlot":18446744073709551616}`, `{"minContextSlot":"151"}`,
		`{"encoding":false}`, `{"encoding":""}`,
	} {
		t.Run(config, func(t *testing.T) {
			params := jsonrpc.RawParams(`["` + base58.Encode(wire) + `",` + config + `]`)
			_, sendErr := s.SendTransaction(t.Context(), params)
			_, simulateErr := s.SimulateTransaction(t.Context(), params)
			for _, err := range []error{sendErr, simulateErr} {
				var invalid *InvalidParamsError
				require.ErrorAs(t, err, &invalid)
			}
		})
	}
}
