package node

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestTransactionHistoryConfig(t *testing.T) {
	valid := transactionHistoryConfig{Enabled: true, RetentionSlots: 16384, MaxMiB: 1024, ReserveMiB: 2048}
	for _, test := range []struct {
		name, cluster, mode string
		port                int
		cfg                 transactionHistoryConfig
		wantError           bool
	}{
		{"observer", "alpenglow", "verifying", 8899, valid, false},
		{"disabled", "mainnet-beta", "validator", 0, transactionHistoryConfig{}, false},
		{"voting", "alpenglow", "validator", 8899, valid, true},
		{"classic", "mainnet-beta", "verifying", 8899, valid, true},
		{"unknown_mode", "alpenglow", "", 8899, valid, true},
		{"rpc_off", "alpenglow", "verifying", 0, valid, true},
		{"invalid_port", "alpenglow", "verifying", 65536, valid, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.cfg.validate(test.cluster, test.mode, test.port)
			if (err != nil) != test.wantError {
				t.Fatalf("validation error = %v, want error %v", err, test.wantError)
			}
		})
	}
	for _, field := range []string{"slots", "size", "reserve"} {
		for _, value := range []int64{-1, 0, math.MaxInt64} {
			cfg := valid
			switch field {
			case "slots":
				cfg.RetentionSlots = value
			case "size":
				cfg.MaxMiB = value
			case "reserve":
				cfg.ReserveMiB = value
			}
			if field == "slots" && value == math.MaxInt64 {
				continue // Slots are compared and subtracted without byte conversion.
			}
			if err := cfg.validate("alpenglow", "verifying", 8899); err == nil {
				t.Fatalf("accepted %s = %d", field, value)
			}
		}
	}
}

func TestTransactionHistoryNodeOwnership(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "accounts")
	cfg := transactionHistoryConfig{Enabled: true, RetentionSlots: 16, MaxMiB: 80, ReserveMiB: 1}
	genesis := solana.Hash{1}
	store, session, err := openNodeTransactionHistory(transactionHistoryConfig{}, "mainnet-beta", "validator", 0, dir, "invalid")
	require.NoError(t, err)
	require.Nil(t, store)
	require.Nil(t, session)
	_, _, err = openNodeTransactionHistory(cfg, "alpenglow", "validator", 8899, dir, genesis.String())
	require.Error(t, err)
	_, _, err = openNodeTransactionHistory(cfg, "alpenglow", "verifying", 8899, dir, "invalid")
	require.Error(t, err)
	_, err = os.Stat(dir)
	require.ErrorIs(t, err, os.ErrNotExist, "disabled or rejected history must not create files")

	store, session, err = openNodeTransactionHistory(cfg, "alpenglow", "verifying", 8899, dir, genesis.String())
	require.NoError(t, err)
	t.Cleanup(func() {
		joined, err := session.Stop(context.Background())
		require.True(t, joined)
		require.NoError(t, err)
		require.NoError(t, store.Close())
	})
	state, err := store.Stats()
	require.NoError(t, err)
	require.Zero(t, state.Blocks)
	server := rpcserver.NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}, genesis)
	server.InitTransactionHistory(store)
	view, err := store.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	joined, err := stopNodeOptionalRPC(server, session, store, nil)
	require.False(t, joined, "an unjoined reader must prevent the caller from closing AccountsDB")
	require.ErrorIs(t, err, transactionhistory.ErrSnapshotsOpen)
	require.NoError(t, view.Close())
	joined, err = stopNodeOptionalRPC(server, session, store, nil)
	require.True(t, joined)
	require.NoError(t, err)
	_, err = store.Snapshot()
	require.ErrorIs(t, err, transactionhistory.ErrClosed)
	require.Error(t, server.Start(), "closed history server must not start again")
}
