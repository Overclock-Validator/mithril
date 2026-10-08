package rpcserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

type GetBalanceResp struct {
	Context GetBalanceRespContext `json:"context"`
	Value   uint64                `json:"value"`
}

type GetBalanceRespContext struct {
	APIVersion string `json:"apiVersion"`
	Slot       uint64 `json:"slot"`
}

func (rpcServer *RpcServer) GetBalance(ctx context.Context, p jsonrpc.RawParams) (GetBalanceResp, error) {
	if err := ctx.Err(); err != nil {
		return GetBalanceResp{}, err
	}

	params, err := jsonrpc.DecodeParams[[]interface{}](p)
	if err != nil {
		return GetBalanceResp{}, &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
	}
	if len(params) < 1 || len(params) > 2 {
		return GetBalanceResp{}, &InvalidParamsError{Message: "getBalance requires an address and optional config"}
	}
	address, ok := params[0].(string)
	if !ok {
		return GetBalanceResp{}, &InvalidParamsError{Message: "getBalance requires an address as first parameter"}
	}
	pubkey, err := solana.PublicKeyFromBase58(address)
	if err != nil {
		return GetBalanceResp{}, &InvalidParamsError{Message: "Invalid param: Invalid"}
	}

	minContextSlot, err := parseBalanceConfig(params)
	if err != nil {
		return GetBalanceResp{}, err
	}
	rooted, account, err := rpcServer.readRootedAccount(ctx, pubkey)
	if err != nil && !errors.Is(err, accountsdb.ErrNoAccount) {
		return GetBalanceResp{}, fmt.Errorf("read balance at rooted slot %d: %w", rooted.Slot, err)
	}
	if minContextSlot != nil && rooted.Slot < *minContextSlot {
		return GetBalanceResp{}, &MinContextSlotNotReachedError{ContextSlot: rooted.Slot}
	}

	var lamports uint64
	if account != nil {
		lamports = account.Lamports
	}

	return GetBalanceResp{
		Context: GetBalanceRespContext{APIVersion: "mithril 0.1", Slot: rooted.Slot},
		Value:   lamports,
	}, nil
}

func parseBalanceConfig(params []interface{}) (*uint64, error) {
	if len(params) == 1 || params[1] == nil {
		return nil, nil
	}
	config, ok := params[1].(map[string]interface{})
	if !ok {
		return nil, &InvalidParamsError{Message: "invalid getBalance config"}
	}
	if commitment, exists := config["commitment"]; exists {
		value, ok := commitment.(string)
		if !ok || (value != "processed" && value != "confirmed" && value != "finalized") {
			return nil, &InvalidParamsError{Message: "invalid commitment"}
		}
	}
	value, exists := config["minContextSlot"]
	if !exists {
		return nil, nil
	}
	minContextSlot, err := rpcUint64(value, "minContextSlot")
	if err != nil {
		return nil, err
	}
	return &minContextSlot, nil
}
