package rpcserver

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/stretchr/testify/require"
)

func TestOptionalRPCParamsCanBeOmitted(t *testing.T) {
	server := &RpcServer{}
	methods := map[string]func(context.Context, jsonrpc.RawParams) error{
		"getEpochInfo": func(ctx context.Context, p jsonrpc.RawParams) error {
			_, err := server.GetEpochInfo(ctx, p)
			return err
		},
		"getVoteAccounts": func(ctx context.Context, p jsonrpc.RawParams) error {
			_, err := server.GetVoteAccounts(ctx, p)
			return err
		},
		"getLeaderSchedule": func(ctx context.Context, p jsonrpc.RawParams) error {
			_, err := server.GetLeaderSchedule(ctx, p)
			return err
		},
		"getBlockProduction": func(ctx context.Context, p jsonrpc.RawParams) error {
			_, err := server.GetBlockProduction(ctx, p)
			return err
		},
	}
	for name, call := range methods {
		t.Run(name, func(t *testing.T) {
			// Missing node state should fail identically for each empty parameter form.
			want := call(t.Context(), jsonrpc.RawParams(`[]`))
			require.Error(t, want)
			for _, params := range []jsonrpc.RawParams{nil, jsonrpc.RawParams(`null`)} {
				require.EqualError(t, call(t.Context(), params), want.Error())
			}
			for _, params := range []jsonrpc.RawParams{jsonrpc.RawParams(`{}`), jsonrpc.RawParams(`true`)} {
				var invalid *InvalidParamsError
				require.ErrorAs(t, call(t.Context(), params), &invalid)
			}
		})
	}
}
