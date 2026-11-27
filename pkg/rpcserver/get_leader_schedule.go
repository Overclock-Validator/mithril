package rpcserver

import (
	"context"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

type getLeaderScheduleConfig struct {
	identity         *solana.PublicKey
	keyByVoteAccount bool
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
		scheduledSlot := firstSlot + index
		var leader, key solana.PublicKey
		var ok bool
		if config.keyByVoteAccount {
			leader, key, ok = global.LeaderForSlotWithVoteAccount(scheduledSlot)
			if !ok {
				if _, exists := global.LeaderForSlot(scheduledSlot); exists {
					return nil, fmt.Errorf("vote-account leader schedule unavailable for slot %d", scheduledSlot)
				}
				return nil, nil
			}
		} else {
			leader, ok = global.LeaderForSlot(scheduledSlot)
			if !ok {
				return nil, nil
			}
			key = leader
		}
		if config.identity != nil && leader != *config.identity {
			continue
		}
		keyString := key.String()
		schedule[keyString] = append(schedule[keyString], index)
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
	if err := validateRootedCommitment(values["commitment"]); err != nil {
		return config, err
	}
	if raw, exists := values["identity"]; exists && raw != nil {
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
	if raw, exists := values["keyByVoteAccount"]; exists && raw != nil {
		value, ok := raw.(bool)
		if !ok {
			return config, &InvalidParamsError{Message: "invalid keyByVoteAccount"}
		}
		config.keyByVoteAccount = value
	}
	return config, nil
}
