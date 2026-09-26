package rpcserver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGetBalanceUsesPublishedRootedBank(t *testing.T) {
	db := newRPCAccountsDB(t)
	address := solana.PublicKey{7}
	_, err := db.CommitBatch([]accounts.SlotDelta{{
		Slot: 42,
		Delta: []*accounts.Account{{
			Key:      address,
			Lamports: 123_456_789,
		}},
	}}, 42, nil, nil)
	require.NoError(t, err)

	server := &RpcServer{acctsDb: db}
	server.SetRootedBankState(42, 40, 10)
	got, err := server.GetBalance(t.Context(), mustRawParams(t, []interface{}{
		address.String(),
		map[string]interface{}{"commitment": "confirmed", "minContextSlot": float64(42)},
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(42), got.Context.Slot)
	require.Equal(t, uint64(123_456_789), got.Value)

	_, err = server.GetBalance(t.Context(), mustRawParams(t, []interface{}{
		address.String(),
		map[string]interface{}{"minContextSlot": float64(43)},
	}))
	var minSlotErr *MinContextSlotNotReachedError
	require.True(t, errors.As(err, &minSlotErr))
	require.Equal(t, uint64(42), minSlotErr.ContextSlot)
}

func TestGetBalanceMissingAccountIsZero(t *testing.T) {
	server := &RpcServer{acctsDb: newRPCAccountsDB(t)}
	server.SetRootedBankState(9, 8, 0)

	got, err := server.GetBalance(t.Context(), mustRawParams(t, []interface{}{solana.PublicKey{8}.String()}))
	require.NoError(t, err)
	require.Equal(t, uint64(9), got.Context.Slot)
	require.Zero(t, got.Value)
}

func TestGetBalanceDoesNotMixNewAccountsWithOldRoot(t *testing.T) {
	db := newRPCAccountsDB(t)
	address := solana.PublicKey{9}
	_, err := db.CommitBatch([]accounts.SlotDelta{{
		Slot: 43,
		Delta: []*accounts.Account{{
			Key:      address,
			Lamports: 43,
		}},
	}}, 43, nil, nil)
	require.NoError(t, err)

	server := &RpcServer{acctsDb: db}
	server.SetRootedBankState(42, 40, 10)
	deadline, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	_, err = server.GetBalance(deadline, mustRawParams(t, []interface{}{address.String()}))
	require.ErrorIs(t, err, context.DeadlineExceeded)

	server.SetRootedBankState(43, 41, 11)
	got, err := server.GetBalance(t.Context(), mustRawParams(t, []interface{}{address.String()}))
	require.NoError(t, err)
	require.Equal(t, uint64(43), got.Context.Slot)
	require.Equal(t, uint64(43), got.Value)
}

func TestGetBalanceRejectsInvalidParams(t *testing.T) {
	server := &RpcServer{}
	for _, params := range [][]interface{}{
		{},
		{true},
		{"not-a-pubkey"},
		{solana.PublicKey{1}.String(), true},
		{solana.PublicKey{1}.String(), map[string]interface{}{"commitment": "unknown"}},
		{solana.PublicKey{1}.String(), map[string]interface{}{"minContextSlot": 1.5}},
	} {
		_, err := server.GetBalance(t.Context(), mustRawParams(t, params))
		var invalid *InvalidParamsError
		require.True(t, errors.As(err, &invalid), "params: %#v; error: %v", params, err)
	}
}

func newRPCAccountsDB(t *testing.T) *accountsdb.AccountsDb {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bootstrap_high_file_id"), make([]byte, 8), 0o644))
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.RootedDurable = true
	db.InitCaches()
	t.Cleanup(db.CloseDb)
	return db
}
