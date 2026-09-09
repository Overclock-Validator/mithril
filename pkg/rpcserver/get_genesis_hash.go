package rpcserver

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/filecoin-project/go-jsonrpc"
)

// GetGenesisHash returns the cluster identity established at startup.
func (rpcServer *RpcServer) GetGenesisHash(ctx context.Context, p jsonrpc.RawParams) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(p) != 0 {
		params, err := jsonrpc.DecodeParams[[]json.RawMessage](p)
		if err != nil || len(params) != 0 {
			return "", &InvalidParamsError{Message: "getGenesisHash does not accept parameters"}
		}
	}
	if rpcServer.genesisHash == "" {
		return "", errors.New("genesis hash is not configured")
	}
	return rpcServer.genesisHash, nil
}
