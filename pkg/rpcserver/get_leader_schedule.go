package rpcserver

import (
	"context"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

type getLeaderScheduleConfig struct {
	identity *solana.PublicKey
}

func (rpcServer *RpcServer) GetLeaderSchedule(ctx context.Context, p jsonrpc.RawParams) (map[string][]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	params, err := decodeOptionalParams(p)
	if err != nil {
		return nil, &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
	}
	if len(params) > 2 {
		return nil, &InvalidParamsError{Message: "getLeaderSchedule accepts an optional slot and config"}
	}
	rooted, ok := rpcServer.getRootedBankState()
	if !ok {
		return nil, fmt.Errorf("node has no rooted bank available")
	}
	if rpcServer.epochSchedule == nil {
		return nil, fmt.Errorf("node has no epoch schedule available")
	}

	slot := rooted.Slot
	var rawConfig interface{}
	if len(params) > 0 && params[0] != nil {
		if _, ok := params[0].(map[string]interface{}); ok {
			if len(params) != 1 {
				return nil, &InvalidParamsError{Message: "leader schedule config cannot be provided twice"}
			}
			rawConfig = params[0]
		} else {
			value, err := rpcUint64(params[0], "slot")
			if err != nil {
				return nil, err
			}
			slot = value
		}
	}
	if len(params) == 2 {
		rawConfig = params[1]
	}
	config, err := parseLeaderScheduleConfig(rawConfig)
	if err != nil {
		return nil, err
	}

	epoch := rpcServer.epochSchedule.GetEpoch(slot)
	firstSlot := rpcServer.epochSchedule.FirstSlotInEpoch(epoch)
	slotsInEpoch := rpcServer.epochSchedule.SlotsInEpoch(epoch)
	schedule := make(map[string][]uint64)
	for index := uint64(0); index < slotsInEpoch; index++ {
		leader, ok := global.LeaderForSlot(firstSlot + index)
		if !ok {
			return nil, nil
		}
		if config.identity != nil && leader != *config.identity {
			continue
		}
		key := leader.String()
		schedule[key] = append(schedule[key], index)
	}
	return schedule, nil
}

func parseLeaderScheduleConfig(raw interface{}) (getLeaderScheduleConfig, error) {
	var config getLeaderScheduleConfig
	if raw == nil {
		return config, nil
	}
	values, ok := raw.(map[string]interface{})
	if !ok {
		return config, &InvalidParamsError{Message: "invalid getLeaderSchedule config"}
	}
	if commitment, exists := values["commitment"]; exists {
		value, ok := commitment.(string)
		if !ok || (value != "processed" && value != "confirmed" && value != "finalized") {
			return config, &InvalidParamsError{Message: "invalid commitment"}
		}
	}
	if raw, exists := values["identity"]; exists {
		value, ok := raw.(string)
		if !ok {
			return config, &InvalidParamsError{Message: "invalid identity"}
		}
		identity, err := solana.PublicKeyFromBase58(value)
		if err != nil {
			return config, &InvalidParamsError{Message: "invalid identity"}
		}
		config.identity = &identity
	}
	if raw, exists := values["keyByVoteAccount"]; exists {
		value, ok := raw.(bool)
		if !ok {
			return config, &InvalidParamsError{Message: "invalid keyByVoteAccount"}
		}
		if value {
			return config, &InvalidParamsError{Message: "keyByVoteAccount is not supported"}
		}
	}
	return config, nil
}
