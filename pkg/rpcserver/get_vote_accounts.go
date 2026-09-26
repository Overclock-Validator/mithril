package rpcserver

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

const (
	delinquentValidatorSlotDistance = 128
	maxRPCEpochCreditsHistory       = 5
)

type GetVoteAccountsResp struct {
	Current    []VoteAccountInfo `json:"current"`
	Delinquent []VoteAccountInfo `json:"delinquent"`
}

type VoteAccountInfo struct {
	VotePubkey                    string      `json:"votePubkey"`
	NodePubkey                    string      `json:"nodePubkey"`
	ActivatedStake                uint64      `json:"activatedStake"`
	Commission                    uint8       `json:"commission"`
	InflationRewardsCommissionBPS uint16      `json:"inflationRewardsCommissionBps"`
	EpochCredits                  [][3]uint64 `json:"epochCredits"`
	EpochVoteAccount              bool        `json:"epochVoteAccount"`
	LastVote                      uint64      `json:"lastVote"`
	RootSlot                      uint64      `json:"rootSlot"`
}

type getVoteAccountsConfig struct {
	votePubkey             *solana.PublicKey
	delinquentSlotDistance uint64
}

func (rpcServer *RpcServer) GetVoteAccounts(ctx context.Context, p jsonrpc.RawParams) (GetVoteAccountsResp, error) {
	if err := ctx.Err(); err != nil {
		return GetVoteAccountsResp{}, err
	}
	params, err := decodeOptionalParams(p)
	if err != nil {
		return GetVoteAccountsResp{}, &InvalidParamsError{Message: fmt.Sprintf("decoding params: %v", err)}
	}
	config, err := parseGetVoteAccountsConfig(params)
	if err != nil {
		return GetVoteAccountsResp{}, err
	}
	if rpcServer.epochSchedule == nil {
		return GetVoteAccountsResp{}, fmt.Errorf("node has no epoch schedule available")
	}
	if rpcServer.acctsDb == nil {
		return GetVoteAccountsResp{}, fmt.Errorf("node has no accounts database available")
	}

	for {
		rooted, ok := rpcServer.getRootedBankState()
		if !ok {
			return GetVoteAccountsResp{}, fmt.Errorf("node has no rooted bank available")
		}
		epoch := rpcServer.epochSchedule.GetEpoch(rooted.Slot)
		stakes, ok := global.EpochStakesSnapshot(epoch)
		if !ok {
			return GetVoteAccountsResp{}, fmt.Errorf("epoch stakes unavailable for rooted epoch %d", epoch)
		}
		activeStakes, err := rpcServer.rootedActivatedStakes(ctx, rooted.Slot, epoch)
		if err != nil {
			return GetVoteAccountsResp{}, err
		}

		votePubkeys, err := rpcServer.acctsDb.VoteAccountPubkeys(ctx)
		if err != nil {
			return GetVoteAccountsResp{}, fmt.Errorf("list vote accounts: %w", err)
		}
		// Epoch stakes also cover stores created before the durable index was built.
		for votePubkey := range stakes.Stakes {
			votePubkeys = append(votePubkeys, votePubkey)
		}
		slices.SortFunc(votePubkeys, func(left, right solana.PublicKey) int {
			return slices.Compare(left[:], right[:])
		})
		votePubkeys = slices.Compact(votePubkeys)
		filtered := votePubkeys[:0]
		for _, votePubkey := range votePubkeys {
			if config.votePubkey == nil || votePubkey == *config.votePubkey {
				filtered = append(filtered, votePubkey)
			}
		}
		votePubkeys = filtered
		published, accounts, err := rpcServer.readRootedAccounts(ctx, votePubkeys)
		if err != nil {
			return GetVoteAccountsResp{}, fmt.Errorf("read vote accounts at rooted slot %d: %w", rooted.Slot, err)
		}
		if published.Slot != rooted.Slot {
			continue
		}

		response := GetVoteAccountsResp{
			Current:    make([]VoteAccountInfo, 0, len(votePubkeys)),
			Delinquent: make([]VoteAccountInfo, 0),
		}
		for index, votePubkey := range votePubkeys {
			account := accounts[index]
			if account == nil || account.Lamports == 0 || account.Owner != addresses.VoteProgramAddr {
				continue // An old candidate may have been deleted or changed owner.
			}
			versioned, err := sealevel.UnmarshalVersionedVoteState(account.Data)
			if err != nil || !versioned.IsInitialized() {
				continue // Exclude invalid and uninitialized vote states.
			}
			voteState := versioned.ConvertToCurrent()
			lastVote, _ := voteState.LastVotedSlot()
			var rootSlot uint64
			if voteState.RootSlot != nil {
				rootSlot = *voteState.RootSlot
			}
			creditsStart := max(0, len(voteState.EpochCredits)-maxRPCEpochCreditsHistory)
			creditsHistory := voteState.EpochCredits[creditsStart:]
			epochCredits := make([][3]uint64, len(creditsHistory))
			for i, credits := range creditsHistory {
				epochCredits[i] = [3]uint64{credits.Epoch, credits.Credits, credits.PrevCredits}
			}
			commission, commissionBPS := voteCommission(versioned, voteState)
			_, epochVoteAccount := stakes.Stakes[votePubkey]
			info := VoteAccountInfo{
				VotePubkey:                    votePubkey.String(),
				NodePubkey:                    voteState.NodePubkey.String(),
				ActivatedStake:                activeStakes[votePubkey],
				Commission:                    commission,
				InflationRewardsCommissionBPS: commissionBPS,
				EpochCredits:                  epochCredits,
				EpochVoteAccount:              epochVoteAccount,
				LastVote:                      lastVote,
				RootSlot:                      rootSlot,
			}
			current := lastVote > 0
			if rooted.Slot >= config.delinquentSlotDistance {
				current = lastVote > rooted.Slot-config.delinquentSlotDistance
			}
			if current {
				response.Current = append(response.Current, info)
			} else if info.ActivatedStake > 0 {
				response.Delinquent = append(response.Delinquent, info)
			}
		}
		return response, nil
	}
}

func (rpcServer *RpcServer) rootedActivatedStakes(ctx context.Context, slot, epoch uint64) (map[solana.PublicKey]uint64, error) {
	// Replay may not have published a slot context after snapshot boot; both
	// inputs to stake activation must come from the same rooted bank instead.
	rooted, accounts, err := rpcServer.readRootedAccounts(ctx, []solana.PublicKey{
		sealevel.SysvarStakeHistoryAddr,
		features.ReduceStakeWarmupCooldown.Address,
	})
	if err != nil {
		return nil, fmt.Errorf("read rooted stake history: %w", err)
	}
	if rooted.Slot != slot {
		return nil, nil
	}
	if accounts[0] == nil {
		return nil, fmt.Errorf("stake history unavailable at rooted slot %d", slot)
	}
	var history sealevel.SysvarStakeHistory
	if err := history.UnmarshalWithDecoder(bin.NewBinDecoder(accounts[0].Data)); err != nil {
		return nil, fmt.Errorf("decode stake history at rooted slot %d: %w", slot, err)
	}
	var activationEpoch *uint64
	featureAccount := accounts[1]
	if featureAccount != nil && featureAccount.Lamports > 0 && featureAccount.Owner == addresses.FeatureAddr {
		var feature features.FeatureAcct
		if err := feature.UnmarshalWithDecoder(bin.NewBinDecoder(featureAccount.Data)); err != nil {
			return nil, fmt.Errorf("decode rooted stake warmup feature: %w", err)
		}
		if feature.ActivatedAt != nil && *feature.ActivatedAt <= rooted.Slot {
			epoch := rpcServer.epochSchedule.GetEpoch(*feature.ActivatedAt)
			activationEpoch = &epoch
		}
	}
	stakes := make(map[solana.PublicKey]uint64)
	var mu sync.Mutex
	_, err = global.StreamStakeAccounts(rpcServer.acctsDb, slot, func(_ solana.PublicKey, delegation *sealevel.Delegation, _ uint64) {
		active := delegation.Stake(epoch, &history, activationEpoch)
		if active == 0 {
			return
		}
		mu.Lock()
		stakes[delegation.VoterPubkey] += active
		mu.Unlock()
	})
	if err != nil {
		return nil, fmt.Errorf("scan rooted stake accounts at slot %d: %w", slot, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return stakes, nil
}

func parseGetVoteAccountsConfig(params []interface{}) (getVoteAccountsConfig, error) {
	config := getVoteAccountsConfig{delinquentSlotDistance: delinquentValidatorSlotDistance}
	if len(params) > 1 {
		return config, &InvalidParamsError{Message: "getVoteAccounts accepts at most one config object"}
	}
	if len(params) == 0 || params[0] == nil {
		return config, nil
	}
	values, ok := params[0].(map[string]interface{})
	if !ok {
		return config, &InvalidParamsError{Message: "invalid getVoteAccounts config"}
	}
	if commitment, exists := values["commitment"]; exists {
		value, ok := commitment.(string)
		if !ok || (value != "processed" && value != "confirmed" && value != "finalized") {
			return config, &InvalidParamsError{Message: "invalid commitment"}
		}
	}
	if raw, exists := values["votePubkey"]; exists {
		value, ok := raw.(string)
		if !ok {
			return config, &InvalidParamsError{Message: "invalid votePubkey"}
		}
		pubkey, err := solana.PublicKeyFromBase58(value)
		if err != nil {
			return config, &InvalidParamsError{Message: "invalid votePubkey"}
		}
		config.votePubkey = &pubkey
	}
	if raw, exists := values["keepUnstakedDelinquents"]; exists {
		value, ok := raw.(bool)
		if !ok {
			return config, &InvalidParamsError{Message: "invalid keepUnstakedDelinquents"}
		}
		if value {
			return config, &InvalidParamsError{Message: "keepUnstakedDelinquents is not supported"}
		}
	}
	if raw, exists := values["delinquentSlotDistance"]; exists {
		value, err := rpcUint64(raw, "delinquentSlotDistance")
		if err != nil {
			return config, err
		}
		config.delinquentSlotDistance = value
	}
	return config, nil
}

func voteCommission(versioned *sealevel.VoteStateVersions, current *sealevel.VoteState) (uint8, uint16) {
	if versioned.Type == sealevel.VoteStateVersionV4 {
		basisPoints := versioned.V4.InflationRewardsCommissionBps
		percent := (uint32(basisPoints) + 99) / 100
		return uint8(min(uint32(255), percent)), basisPoints
	}
	return current.Commission, uint16(current.Commission) * 100
}
