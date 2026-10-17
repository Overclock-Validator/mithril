package rpcserver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

const maxInflationRewardAddresses = 32

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

// GetInflationReward reads verified canonical rewards. On Alpenglow, confirmed
// and finalized share this view, which may precede the durable checkpoint.
// The recovery lock pins its slot, epoch, coverage and records for this request.
func (rpcServer *RpcServer) GetInflationReward(ctx context.Context, p jsonrpc.RawParams) ([]*InflationRewardResp, error) {
	rpcServer.historyRecoveryMu.RLock()
	defer rpcServer.historyRecoveryMu.RUnlock()
	if rpcServer.historyRecoveryPending {
		return nil, fmt.Errorf("RPC history is unavailable during account recovery")
	}
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

	contextSlot := rpcServer.epochRewards.PublishedSlot()
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
	available := rpcServer.epochRewards.EpochPublished(epoch)
	distribution, distributing := rpcServer.epochRewards.PublishedDistribution(epoch)
	var unavailable error = &EpochRewardHistoryUnavailableError{Epoch: epoch, ContextSlot: contextSlot}
	boundary := rpcServer.epochSchedule.FirstSlotInEpoch(safemath.SaturatingAddU64(epoch, 1))
	if boundary < rpcServer.epochRewards.FirstRetainedSlot() {
		unavailable = &EpochRewardHistoryPrunedError{Message: fmt.Sprintf("Reward history for slot %d has been cleaned up", boundary)}
	}
	if len(addresses) == 0 && !available && !distributing {
		return nil, unavailable
	}
	for index, address := range addresses {
		record, ok := rpcServer.epochRewards.GetPublished(epoch, address)
		if !ok {
			if !available {
				if !distributing {
					return nil, unavailable
				}
				partition := rewards.CalculateRewardPartitionForPubkey(solana.MustPublicKeyFromBase58(address), distribution.ParentBlockhash, distribution.NumPartitions)
				partitionHeight := safemath.SaturatingAddU64(distribution.StartingBlockHeight, partition)
				if distribution.BlockHeight < partitionHeight {
					return nil, &EpochRewardsPeriodActiveError{
						Slot: contextSlot, CurrentBlockHeight: distribution.BlockHeight,
						RewardsCompleteBlockHeight: safemath.SaturatingAddU64(distribution.StartingBlockHeight, distribution.NumPartitions),
					}
				}
			}
			continue
		}
		result[index] = &InflationRewardResp{
			Epoch: record.Epoch, EffectiveSlot: record.EffectiveSlot,
			Amount: record.Amount, PostBalance: record.PostBalance, Commission: record.Commission,
		}
	}
	return result, nil
}
