package rpcserver

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/blockhistory"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go/rpc"
)

type GetBlockResp struct {
	BlockHeight       uint64                `json:"blockHeight"`
	BlockTime         *int64                `json:"blockTime"`
	Blockhash         string                `json:"blockhash"`
	PreviousBlockhash string                `json:"previousBlockhash"`
	ParentSlot        uint64                `json:"parentSlot"`
	Rewards           []blockRewardResponse `json:"rewards"`
}

type blockRewardResponse struct {
	rpc.BlockReward
	Commission *uint8 `json:"commission"`
}

func (rpcServer *RpcServer) MinimumLedgerSlot(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	if err := requireNoParams(p, "minimumLedgerSlot"); err != nil {
		return 0, err
	}
	if rpcServer.blockHistory == nil {
		return 0, errors.New("node has no retained block history")
	}
	return rpcServer.blockHistory.MinimumLedgerSlot()
}

func (rpcServer *RpcServer) GetFirstAvailableBlock(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	if err := requireNoParams(p, "getFirstAvailableBlock"); err != nil {
		return 0, err
	}
	if rpcServer.blockHistory == nil {
		return 0, errors.New("node has no retained block history")
	}
	return rpcServer.blockHistory.FirstAvailableBlock()
}

func (rpcServer *RpcServer) GetBlock(ctx context.Context, p jsonrpc.RawParams) (GetBlockResp, error) {
	params, err := jsonrpc.DecodeParams[[]interface{}](p)
	if err != nil {
		return GetBlockResp{}, &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
	}
	if len(params) < 1 || len(params) > 2 {
		return GetBlockResp{}, &InvalidParamsError{Message: "getBlock requires a slot and optional config"}
	}
	slot, err := blockRPCUint64(params[0], "slot")
	if err != nil {
		return GetBlockResp{}, err
	}
	details, err := validateBlockConfig(params)
	if err != nil {
		return GetBlockResp{}, err
	}
	if rpcServer.blockHistory == nil {
		return GetBlockResp{}, errors.New("node has no retained block history")
	}
	if details == "full" {
		return GetBlockResp{}, &InvalidParamsError{Message: "getBlock transactionDetails: full is not supported because transactions are not retained"}
	}
	record, err := rpcServer.blockHistory.Get(slot)
	if errors.Is(err, blockhistory.ErrSlotSkipped) {
		return GetBlockResp{}, &SlotSkippedError{Slot: slot}
	}
	if errors.Is(err, blockhistory.ErrNotAvailable) {
		return GetBlockResp{}, &BlockNotAvailableError{Slot: slot}
	}
	if err != nil {
		return GetBlockResp{}, err
	}
	response := GetBlockResp{
		BlockHeight:       record.BlockHeight,
		BlockTime:         record.BlockTime,
		Blockhash:         record.Blockhash,
		PreviousBlockhash: record.PreviousBlockhash,
		ParentSlot:        record.ParentSlot,
		Rewards:           make([]blockRewardResponse, len(record.Rewards)),
	}
	for i, reward := range record.Rewards {
		response.Rewards[i] = blockRewardResponse{BlockReward: reward, Commission: reward.Commission}
	}
	return response, nil
}

func blockRPCUint64(raw interface{}, name string) (uint64, error) {
	value, ok := raw.(float64)
	if !ok || value < 0 || value >= math.Exp2(64) || math.Trunc(value) != value {
		return 0, &InvalidParamsError{Message: fmt.Sprintf("invalid %s", name)}
	}
	return uint64(value), nil
}

func requireNoParams(p jsonrpc.RawParams, method string) error {
	if len(p) == 0 {
		return nil
	}
	params, err := jsonrpc.DecodeParams[[]interface{}](p)
	if err != nil {
		return &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
	}
	if len(params) != 0 {
		return &InvalidParamsError{Message: method + " does not accept parameters"}
	}
	return nil
}

func validateBlockConfig(params []interface{}) (string, error) {
	if len(params) == 1 || params[1] == nil {
		return "", &InvalidParamsError{Message: "getBlock block history requires transactionDetails: none or full"}
	}
	config, ok := params[1].(map[string]interface{})
	if !ok {
		return "", &InvalidParamsError{Message: "invalid getBlock config"}
	}
	if raw := config["maxSupportedTransactionVersion"]; raw != nil {
		version, err := blockRPCUint64(raw, "maxSupportedTransactionVersion")
		if err != nil || version > math.MaxUint8 {
			return "", &InvalidParamsError{Message: "invalid maxSupportedTransactionVersion"}
		}
	}
	if commitment, exists := config["commitment"]; exists {
		value, ok := commitment.(string)
		if !ok || (value != "confirmed" && value != "finalized") {
			return "", &InvalidParamsError{Message: "getBlock requires confirmed or finalized commitment"}
		}
	}
	if encoding, exists := config["encoding"]; exists && encoding != "json" {
		return "", &InvalidParamsError{Message: "getBlock block history supports json encoding"}
	}
	details, exists := config["transactionDetails"]
	if !exists || (details != "none" && details != "full") {
		return "", &InvalidParamsError{Message: "getBlock block history supports transactionDetails: none or full"}
	}
	if rewards, exists := config["rewards"]; exists {
		value, ok := rewards.(bool)
		if !ok || !value {
			return "", &InvalidParamsError{Message: "getBlock block history requires rewards: true"}
		}
	}
	return details.(string), nil
}
