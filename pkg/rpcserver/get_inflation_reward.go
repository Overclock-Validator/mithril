package rpcserver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

const maxInflationRewardAddresses = 5

type inflationRewardConfig struct {
	Commitment     string  `json:"commitment"`
	Epoch          *uint64 `json:"epoch"`
	MinContextSlot *uint64 `json:"minContextSlot"`
}

// InflationRewardResp is one getInflationReward result entry.
type InflationRewardResp struct {
	Epoch         uint64 `json:"epoch"`
	EffectiveSlot uint64 `json:"effectiveSlot"`
	Amount        uint64 `json:"amount"`
	PostBalance   uint64 `json:"postBalance"`
	Commission    *uint8 `json:"commission"`
}

func (rpcServer *RpcServer) GetInflationReward(ctx context.Context, p jsonrpc.RawParams) ([]*InflationRewardResp, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var params []json.RawMessage
	if err := json.Unmarshal(p, &params); err != nil || len(params) < 1 || len(params) > 2 {
		return nil, &InvalidParamsError{Message: "getInflationReward requires addresses and optional config"}
	}
	var addresses []string
	if err := json.Unmarshal(params[0], &addresses); err != nil || addresses == nil {
		return nil, &InvalidParamsError{Message: "invalid getInflationReward addresses"}
	}
	if len(addresses) > maxInflationRewardAddresses {
		return nil, &InvalidParamsError{Message: fmt.Sprintf("Too many inputs provided; max %d", maxInflationRewardAddresses)}
	}
	for _, address := range addresses {
		if _, err := solana.PublicKeyFromBase58(address); err != nil {
			return nil, &InvalidParamsError{Message: fmt.Sprintf("invalid address %q", address)}
		}
	}

	config := inflationRewardConfig{Commitment: "finalized"}
	if len(params) == 2 && string(params[1]) != "null" {
		if err := json.Unmarshal(params[1], &config); err != nil {
			return nil, &InvalidParamsError{Message: "invalid getInflationReward config"}
		}
		if config.Commitment == "" {
			config.Commitment = "finalized"
		}
	}
	if config.Commitment != "confirmed" && config.Commitment != "finalized" {
		return nil, &InvalidParamsError{Message: "getInflationReward requires confirmed or finalized commitment"}
	}
	if rpcServer.epochRewards == nil {
		return nil, fmt.Errorf("node has no retained epoch reward history")
	}
	if rpcServer.epochSchedule == nil {
		return nil, fmt.Errorf("node has no epoch schedule available")
	}

	contextSlot := rpcServer.epochRewards.RootedSlot()
	if config.MinContextSlot != nil && contextSlot < *config.MinContextSlot {
		return nil, &MinContextSlotNotReachedError{ContextSlot: contextSlot}
	}
	epoch := rpcServer.epochSchedule.GetEpoch(contextSlot)
	if config.Epoch != nil {
		epoch = *config.Epoch
	} else if epoch > 0 {
		epoch--
	}

	result := make([]*InflationRewardResp, len(addresses))
	for index, address := range addresses {
		record, ok := rpcServer.epochRewards.Get(epoch, address, false)
		if !ok {
			continue
		}
		result[index] = &InflationRewardResp{
			Epoch: record.Epoch, EffectiveSlot: record.EffectiveSlot,
			Amount: record.Amount, PostBalance: record.PostBalance, Commission: record.Commission,
		}
	}
	return result, nil
}
