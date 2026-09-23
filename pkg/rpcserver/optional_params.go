package rpcserver

import "github.com/filecoin-project/go-jsonrpc"

func decodeOptionalParams(p jsonrpc.RawParams) ([]interface{}, error) {
	if len(p) == 0 {
		return nil, nil
	}
	return jsonrpc.DecodeParams[[]interface{}](p)
}
