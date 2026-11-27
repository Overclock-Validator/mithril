package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveRPCServe(t *testing.T) {
	type switches struct{ blocks, statuses, history bool }
	for _, test := range []struct {
		name     string
		level    string
		switches switches
		mode     string
		port     int
		want     rpcServePlan
		wantErr  string
	}{
		{name: "unset without a port serves nothing", mode: "validator", want: rpcServePlan{Level: rpcServeNone}},
		{name: "unset with a port serves accounts", mode: "verifying", port: 8899, want: rpcServePlan{Level: rpcServeAccounts}},
		{name: "a validator may serve accounts", level: "accounts", mode: "validator", port: 8899, want: rpcServePlan{Level: rpcServeAccounts}},
		{name: "blocks", level: "blocks", mode: "verifying", port: 8899, want: rpcServePlan{Level: rpcServeBlocks, BlockHistory: true}},
		{name: "wallet brings block history", level: "wallet", mode: "verifying", port: 8899,
			want: rpcServePlan{Level: rpcServeWallet, BlockHistory: true, SignatureStatuses: true}},
		{name: "history brings everything", level: "history", mode: "verifying", port: 8899,
			want: rpcServePlan{Level: rpcServeHistory, BlockHistory: true, SignatureStatuses: true, TransactionHistory: true}},
		{name: "a switch raises an unset level", switches: switches{statuses: true}, mode: "verifying", port: 8899,
			want: rpcServePlan{Level: rpcServeWallet, BlockHistory: true, SignatureStatuses: true}},
		{name: "history switch alone leaves signature outcomes off", switches: switches{history: true}, mode: "verifying", port: 8899,
			want: rpcServePlan{Level: rpcServeHistory, BlockHistory: true, TransactionHistory: true}},
		{name: "a switch adds to a named level", level: "blocks", switches: switches{history: true}, mode: "verifying", port: 8899,
			want: rpcServePlan{Level: rpcServeHistory, BlockHistory: true, TransactionHistory: true}},
		{name: "none closes the listener even with a port", level: "none", mode: "verifying", port: 8899, want: rpcServePlan{Level: rpcServeNone}},
		{name: "none contradicts a switch", level: "none", switches: switches{blocks: true}, mode: "verifying", port: 8899, wantErr: "none"},
		{name: "unknown level", level: "everything", mode: "verifying", port: 8899, wantErr: "unknown rpc.serve"},
		{name: "a level needs a port", level: "wallet", mode: "verifying", wantErr: "port"},
		{name: "port out of range", level: "accounts", mode: "verifying", port: 65536, wantErr: "port"},
		{name: "a voting node serves no history", level: "blocks", mode: "validator", port: 8899, wantErr: "verifying"},
		{name: "a voting node refuses a history switch", switches: switches{statuses: true}, mode: "validator", port: 8899, wantErr: "verifying"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveRPCServe(test.level, test.switches.blocks, test.switches.statuses, test.switches.history, test.mode, test.port)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestRPCServeSummary(t *testing.T) {
	history := transactionHistoryConfig{RetentionSlots: 16384, MaxMiB: 1024, ReserveMiB: 2048}
	require.Equal(t, "RPC: not serving", rpcServePlan{}.summary(history))
	require.Equal(t, "RPC serves accounts", rpcServePlan{Level: rpcServeAccounts}.summary(history))
	full, err := resolveRPCServe("history", false, false, false, "verifying", 8899)
	require.NoError(t, err)
	require.Equal(t, "RPC serves history; block history 2 epochs; recent signature outcomes; "+
		"transaction history 16384 slots, budget 1024 MiB, free reserve 2048 MiB; HTTP only", full.summary(history))
}
