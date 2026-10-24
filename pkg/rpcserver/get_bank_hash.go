package rpcserver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/mr-tron/base58"
)

func (rpcServer *RpcServer) GetBankHash(ctx context.Context, p jsonrpc.RawParams) (string, error) {
	params, err := jsonrpc.DecodeParams[[]json.RawMessage](p)
	if err != nil {
		return "", &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
	}

	if len(params) != 1 {
		return "", &InvalidParamsError{Message: "getBankHash requires one slot"}
	}

	var slot *uint64
	if err := json.Unmarshal(params[0], &slot); err != nil || slot == nil {
		return "", &InvalidParamsError{Message: "getBankHash requires an unsigned integer slot"}
	}

	bankHash, err := rpcServer.acctsDb.GetBankHashForSlot(*slot)
	if err != nil {
		return "", fmt.Errorf("unable to retrieve bankhash for slot %d", *slot)
	}

	return base58.Encode(bankHash), nil
}
