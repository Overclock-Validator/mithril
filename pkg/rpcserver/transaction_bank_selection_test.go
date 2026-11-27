package rpcserver

import (
	"encoding/base64"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func transactionBankServer(t *testing.T) *RpcServer {
	t.Helper()
	db := newRPCAccountsDB(t)
	root := simulationBankFixtureAt(t, 100, solana.Hash{11}, 5000)
	root.BlockHeight = 70
	server := &RpcServer{acctsDb: db}
	server.SetRootedBankState(root.Slot, root.BlockHeight, 0)
	server.SetRootedExecutionBank(root)
	processed := simulationBankFixtureAt(t, 150, solana.Hash{12}, 6000)
	ctx, err := newSimulationBankContext(processed, db, db)
	require.NoError(t, err)
	ctx.UnrootedRead = nil
	replay.ResetChainTip()
	t.Cleanup(func() { replay.ResetChainTip() })
	replay.UpdateChainTipFromSlotCtxWithBankMetadata(ctx, ctx.Features, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{BlockHeight: 120})
	server.SetSlotCtx(ctx)
	return server
}

func TestTransactionBankCommitmentMinimum(t *testing.T) {
	server := transactionBankServer(t)
	_, wire := testLegacyTransaction(t)
	for _, commitment := range []string{"", "processed", "confirmed", "finalized"} {
		t.Run(commitment, func(t *testing.T) {
			options := map[string]any{"encoding": "base64", "minContextSlot": 149}
			if commitment != "" {
				options["commitment"] = commitment
			}
			response, err := server.SimulateTransaction(t.Context(), mustRawParams(t, []any{base64.StdEncoding.EncodeToString(wire), options}))
			if commitment == "processed" {
				require.NoError(t, err)
				require.Equal(t, uint64(150), response.Context.Slot)
			} else {
				var minimum *MinContextSlotNotReachedError
				require.ErrorAs(t, err, &minimum)
				require.Equal(t, uint64(100), minimum.ContextSlot)
			}
			delete(options, "commitment")
			if commitment != "" {
				options["preflightCommitment"] = commitment
			}
			_, err = server.SendTransaction(t.Context(), mustRawParams(t, []any{base64.StdEncoding.EncodeToString(wire), options}))
			if commitment == "processed" {
				var signature *SendTransactionPreflightFailureError
				require.ErrorAs(t, err, &signature)
			} else {
				var minimum *MinContextSlotNotReachedError
				require.ErrorAs(t, err, &minimum)
				require.Equal(t, uint64(100), minimum.ContextSlot)
			}
		})
	}
}

func TestTransactionBankReplacementUsesSelectedBank(t *testing.T) {
	server := transactionBankServer(t)
	_, wire := testLegacyTransaction(t)
	for _, check := range []struct {
		commitment   string
		slot, height uint64
		hash         solana.Hash
	}{{"finalized", 100, 220, solana.Hash{11}}, {"processed", 150, 270, solana.Hash{12}}} {
		response, err := server.SimulateTransaction(t.Context(), mustRawParams(t, []any{base64.StdEncoding.EncodeToString(wire), map[string]any{
			"encoding": "base64", "commitment": check.commitment, "replaceRecentBlockhash": true,
		}}))
		require.NoError(t, err)
		require.Equal(t, check.slot, response.Context.Slot)
		require.Equal(t, &ReplacementBlockhashPayload{Blockhash: check.hash.String(), LastValidBlockHeight: check.height}, response.Value.ReplacementBlockhash)
	}
}

// Keep an account delta in the fixture's real database, rather than relying on
// the slot argument to make AccountsDb act as a historical account store.
func commitTransactionBankAccount(t *testing.T, server *RpcServer, slot uint64, account *accounts.Account) {
	t.Helper()
	_, err := server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{account}}}, slot, nil, nil)
	require.NoError(t, err)
	if root := server.rootedBank.Load(); root != nil && root.Slot == slot {
		server.SetRootedExecutionBank(root.executionBank)
	}
}
