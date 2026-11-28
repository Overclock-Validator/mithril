package rpcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

type GetBlockProductionResp struct {
	Context GetBalanceRespContext  `json:"context"`
	Value   BlockProductionResults `json:"value"`
}

type BlockProductionResults struct {
	ByIdentity map[string][2]uint64 `json:"byIdentity"`
	Range      BlockProductionRange `json:"range"`
}

type BlockProductionRange struct {
	FirstSlot uint64 `json:"firstSlot"`
	LastSlot  uint64 `json:"lastSlot"`
}

type getBlockProductionConfig struct {
	identity  *solana.PublicKey
	firstSlot *uint64
	lastSlot  *uint64
}

type publishedSlotHistory struct {
	bank      *rootedBankState
	version   uint64
	schedules global.LeaderScheduleSnapshot
	history   sealevel.SysvarSlotHistory
}

func (s *RpcServer) readPublishedSlotHistory(ctx context.Context, schedules global.LeaderScheduleSnapshot) (rootedBankState, *sealevel.SysvarSlotHistory, error) {
	s.historyRecoveryMu.RLock()
	bank := s.rootedBank.Load()
	cached := s.slotHistory.Load()
	pending := s.historyRecoveryPending
	if !pending && cached != nil && cached.bank == bank && cached.schedules.Current() && s.acctsDb.CanServePinnedAccountVersion(cached.version, bank.Slot) {
		s.historyRecoveryMu.RUnlock()
		return *bank, &cached.history, nil
	}
	s.historyRecoveryMu.RUnlock()
	if pending {
		return rootedBankState{}, nil, fmt.Errorf("rooted history recovery pending")
	}
	var version uint64
	var stable bool
	if s.acctsDb != nil {
		version, stable = s.acctsDb.CommittedAccountVersion()
	}
	rooted, account, err := s.readRootedAccount(ctx, sealevel.SysvarSlotHistoryAddr, nil)
	if err != nil {
		return rooted, nil, err
	}
	var history sealevel.SysvarSlotHistory
	if err := history.UnmarshalWithDecoder(bin.NewBinDecoder(account.Data)); err != nil {
		return rooted, nil, fmt.Errorf("decode slot history: %w", err)
	}
	s.historyRecoveryMu.RLock()
	defer s.historyRecoveryMu.RUnlock()
	if s.historyRecoveryPending {
		return rooted, nil, fmt.Errorf("rooted history recovery pending")
	}
	if latest, ok := s.acctsDb.CommittedAccountVersion(); stable && ok && version == latest && bank != nil && s.rootedBank.Load() == bank && schedules.Current() {
		// A full SlotHistory contains at most 1,048,576 bits.
		if _, _, err := slotHistoryBounds(&history); err == nil && len(history.Bits.Bits.Blocks) <= (1<<20)/64 {
			s.slotHistory.Store(&publishedSlotHistory{bank, version, schedules, history})
		}
	}
	return rooted, &history, nil
}

func (rpcServer *RpcServer) GetBlockProduction(ctx context.Context, p jsonrpc.RawParams) (GetBlockProductionResp, error) {
	if err := ctx.Err(); err != nil {
		return GetBlockProductionResp{}, err
	}
	params, err := decodeOptionalParams(p)
	if err != nil {
		return GetBlockProductionResp{}, &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
	}
	config, err := parseBlockProductionConfig(params)
	if err != nil {
		return GetBlockProductionResp{}, err
	}
	if rpcServer.epochSchedule == nil {
		return GetBlockProductionResp{}, fmt.Errorf("node has no epoch schedule available")
	}
	schedules := global.CaptureLeaderSchedules()
	rooted, history, err := rpcServer.readPublishedSlotHistory(ctx, schedules)
	if err != nil {
		return GetBlockProductionResp{}, fmt.Errorf("read slot history at rooted slot %d: %w", rooted.Slot, err)
	}
	if !schedules.Current() {
		return GetBlockProductionResp{}, fmt.Errorf("leader schedule changed while reading rooted slot history")
	}

	firstSlot := rpcServer.epochSchedule.FirstSlotInEpoch(rpcServer.epochSchedule.GetEpoch(rooted.Slot))
	if config.firstSlot != nil {
		firstSlot = *config.firstSlot
	}
	lastSlot := rooted.Slot
	if config.lastSlot != nil {
		lastSlot = *config.lastSlot
	}
	if lastSlot < firstSlot {
		return GetBlockProductionResp{}, &InvalidParamsError{Message: fmt.Sprintf("lastSlot, %d, cannot be less than firstSlot, %d", lastSlot, firstSlot)}
	}

	oldest, newest, err := slotHistoryBounds(history)
	if err != nil {
		return GetBlockProductionResp{}, err
	}
	if firstSlot < oldest {
		return GetBlockProductionResp{}, &InvalidParamsError{Message: fmt.Sprintf("firstSlot, %d, is too small; min %d", firstSlot, oldest)}
	}
	if lastSlot > newest {
		return GetBlockProductionResp{}, &InvalidParamsError{Message: fmt.Sprintf("lastSlot, %d, is too large; max %d", lastSlot, newest)}
	}

	byIdentity := make(map[string][2]uint64)
	for slot := firstSlot; ; slot++ {
		if err := ctx.Err(); err != nil {
			return GetBlockProductionResp{}, err
		}
		leaders := schedules.ForEpoch(rpcServer.epochSchedule.GetEpoch(slot))
		if leaders == nil {
			return GetBlockProductionResp{}, fmt.Errorf("leader schedule unavailable for slot %d", slot)
		}
		leader, ok := leaders.LeaderForSlot(slot)
		if !ok {
			return GetBlockProductionResp{}, fmt.Errorf("leader schedule unavailable for slot %d", slot)
		}
		if config.identity == nil || leader == *config.identity {
			key := leader.String()
			production := byIdentity[key]
			production[0]++
			if slotHistoryContains(history, slot) {
				production[1]++
			}
			byIdentity[key] = production
		}
		if slot == lastSlot {
			break
		}
	}

	return GetBlockProductionResp{
		Context: GetBalanceRespContext{Slot: rooted.Slot},
		Value: BlockProductionResults{
			ByIdentity: byIdentity,
			Range:      BlockProductionRange{FirstSlot: firstSlot, LastSlot: lastSlot},
		},
	}, nil
}

func parseBlockProductionConfig(params []interface{}) (getBlockProductionConfig, error) {
	var config getBlockProductionConfig
	if len(params) > 1 {
		return config, &InvalidParamsError{Message: "getBlockProduction accepts at most one config object"}
	}
	if len(params) == 0 || params[0] == nil {
		return config, nil
	}
	values, ok := params[0].(map[string]interface{})
	if !ok {
		return config, &InvalidParamsError{Message: "invalid getBlockProduction config"}
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
	if raw, exists := values["range"]; exists && raw != nil {
		rangeValues, ok := raw.(map[string]interface{})
		if !ok {
			return config, &InvalidParamsError{Message: "invalid range"}
		}
		first, exists := rangeValues["firstSlot"]
		if !exists {
			return config, &InvalidParamsError{Message: "range requires firstSlot"}
		}
		firstSlot, err := rpcUint64(first, "firstSlot")
		if err != nil {
			return config, err
		}
		config.firstSlot = &firstSlot
		if last, exists := rangeValues["lastSlot"]; exists && last != nil {
			lastSlot, err := rpcUint64(last, "lastSlot")
			if err != nil {
				return config, err
			}
			config.lastSlot = &lastSlot
		}
	}
	return config, nil
}

func rpcUint64(raw interface{}, name string) (uint64, error) {
	value, ok := raw.(json.Number)
	if !ok {
		return 0, &InvalidParamsError{Message: fmt.Sprintf("invalid %s", name)}
	}
	parsed, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		return 0, &InvalidParamsError{Message: fmt.Sprintf("invalid %s", name)}
	}
	return parsed, nil
}

func slotHistoryBounds(history *sealevel.SysvarSlotHistory) (uint64, uint64, error) {
	if history == nil || history.Bits.Len == 0 || history.Bits.Bits.BlocksLen == 0 || len(history.Bits.Bits.Blocks) < int(history.Bits.Bits.BlocksLen) || history.NextSlot == 0 {
		return 0, 0, fmt.Errorf("node has no usable slot history")
	}
	return history.NextSlot - min(history.NextSlot, history.Bits.Len), history.NextSlot - 1, nil
}

func slotHistoryContains(history *sealevel.SysvarSlotHistory, slot uint64) bool {
	block := (slot / 64) % history.Bits.Bits.BlocksLen
	return history.Bits.Bits.Blocks[block]&(uint64(1)<<(slot%64)) != 0
}
