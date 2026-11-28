package rpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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
	Rewards           []blockRewardResponse `json:"rewards,omitzero"`
}

type blockRewardResponse struct {
	rpc.BlockReward
	Commission *uint8 `json:"commission"`
}

func (rpcServer *RpcServer) MinimumLedgerSlot(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	rpcServer.historyRecoveryMu.RLock()
	defer rpcServer.historyRecoveryMu.RUnlock()
	if rpcServer.historyRecoveryPending {
		return 0, errors.New("RPC history is unavailable during account recovery")
	}
	if err := requireNoParams(p, "minimumLedgerSlot"); err != nil {
		return 0, err
	}
	if rpcServer.blockHistory == nil {
		return 0, errors.New("node has no retained block history")
	}
	return rpcServer.blockHistory.MinimumLedgerSlot()
}

func (rpcServer *RpcServer) GetFirstAvailableBlock(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	rpcServer.historyRecoveryMu.RLock()
	defer rpcServer.historyRecoveryMu.RUnlock()
	if rpcServer.historyRecoveryPending {
		return 0, errors.New("RPC history is unavailable during account recovery")
	}
	if err := requireNoParams(p, "getFirstAvailableBlock"); err != nil {
		return 0, err
	}
	if rpcServer.blockHistory == nil {
		return 0, errors.New("node has no retained block history")
	}
	return rpcServer.blockHistory.FirstAvailableBlock()
}

func (rpcServer *RpcServer) GetBlock(ctx context.Context, p jsonrpc.RawParams) (GetBlockResp, error) {
	rpcServer.historyRecoveryMu.RLock()
	defer rpcServer.historyRecoveryMu.RUnlock()
	if rpcServer.historyRecoveryPending {
		return GetBlockResp{}, errors.New("RPC history is unavailable during account recovery")
	}
	params, err := jsonrpc.DecodeParams[[]json.RawMessage](p)
	if err != nil {
		return GetBlockResp{}, &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
	}
	if len(params) < 1 || len(params) > 2 {
		return GetBlockResp{}, &InvalidParamsError{Message: "getBlock requires a slot and optional config"}
	}
	var slot *uint64
	if err := json.Unmarshal(params[0], &slot); err != nil || slot == nil {
		return GetBlockResp{}, &InvalidParamsError{Message: "invalid slot"}
	}
	details, showRewards, err := validateBlockConfig(params)
	if err != nil {
		return GetBlockResp{}, err
	}
	if rpcServer.blockHistory == nil {
		return GetBlockResp{}, errors.New("node has no retained block history")
	}
	if details != "none" {
		return GetBlockResp{}, &InvalidParamsError{Message: fmt.Sprintf("getBlock transactionDetails: %s is not supported because transactions are not retained", details)}
	}
	record, err := rpcServer.blockHistory.Get(*slot)
	if errors.Is(err, blockhistory.ErrSlotSkipped) {
		return GetBlockResp{}, &SlotSkippedError{Slot: *slot}
	}
	if errors.Is(err, blockhistory.ErrNotAvailable) {
		return GetBlockResp{}, &BlockNotAvailableError{Slot: *slot}
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
	}
	if showRewards {
		response.Rewards = make([]blockRewardResponse, len(record.Rewards))
		for i, reward := range record.Rewards {
			response.Rewards[i] = blockRewardResponse{BlockReward: reward, Commission: reward.Commission}
		}
	}
	return response, nil
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

func validateBlockConfig(params []json.RawMessage) (string, bool, error) {
	var config struct {
		Encoding                       *string `json:"encoding"`
		TransactionDetails             *string `json:"transactionDetails"`
		Rewards                        *bool   `json:"rewards"`
		Commitment                     *string `json:"commitment"`
		MaxSupportedTransactionVersion *uint8  `json:"maxSupportedTransactionVersion"`
	}
	if len(params) == 2 {
		if err := json.Unmarshal(params[1], &config); err != nil {
			var fieldError *json.UnmarshalTypeError
			if errors.As(err, &fieldError) && fieldError.Field != "" {
				return "", false, &InvalidParamsError{Message: "invalid " + fieldError.Field}
			}
			// The legacy config form is an encoding string.
			var encoding string
			if err := json.Unmarshal(params[1], &encoding); err != nil {
				return "", false, &InvalidParamsError{Message: "invalid getBlock config"}
			}
			config.Encoding = &encoding
		}
	}
	if config.Commitment != nil {
		if value := *config.Commitment; value != "confirmed" && value != "finalized" {
			return "", false, &InvalidParamsError{Message: "getBlock requires confirmed or finalized commitment"}
		}
	}
	encoding := "json"
	if config.Encoding != nil {
		encoding = *config.Encoding
	}
	switch encoding {
	case "binary", "base58", "base64", "json", "jsonParsed":
	default:
		return "", false, &InvalidParamsError{Message: "invalid encoding"}
	}
	if (encoding == "binary" || encoding == "base58") && config.MaxSupportedTransactionVersion != nil && *config.MaxSupportedTransactionVersion >= 1 {
		return "", false, &InvalidParamsError{Message: "base58 encoding is not supported with maxSupportedTransactionVersion >= 1"}
	}
	details := "full"
	if config.TransactionDetails != nil {
		details = *config.TransactionDetails
	}
	switch details {
	case "full", "none", "signatures", "accounts":
	default:
		return "", false, &InvalidParamsError{Message: "invalid transactionDetails"}
	}
	showRewards := true
	if config.Rewards != nil {
		showRewards = *config.Rewards
	}
	return details, showRewards, nil
}
