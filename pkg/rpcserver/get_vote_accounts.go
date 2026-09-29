package rpcserver

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
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
		if err := ctx.Err(); err != nil {
			return GetVoteAccountsResp{}, err
		}
		bank := rpcServer.rootedBank.Load()
		version, stable := rpcServer.acctsDb.CommittedAccountVersion()
		if !stable {
			if err := waitForRootedPublication(ctx); err != nil {
				return GetVoteAccountsResp{}, err
			}
			continue
		}
		rooted, ok := rpcServer.getRootedBankState()
		if !ok {
			return GetVoteAccountsResp{}, fmt.Errorf("node has no rooted bank available")
		}
		epoch := rpcServer.epochSchedule.GetEpoch(rooted.Slot)
		stakes, ok := global.EpochStakesSnapshot(epoch)
		if !ok {
			return GetVoteAccountsResp{}, fmt.Errorf("epoch stakes unavailable for rooted epoch %d", epoch)
		}
		activeStakes, err := rpcServer.rootedActivatedStakes(ctx, rooted.Slot, epoch, config.votePubkey)
		if errors.Is(err, errStakeViewChanged) {
			if err := waitForRootedPublication(ctx); err != nil {
				return GetVoteAccountsResp{}, err
			}
			continue
		}
		if err != nil {
			return GetVoteAccountsResp{}, err
		}

		var votePubkeys []solana.PublicKey
		if config.votePubkey != nil {
			votePubkeys = []solana.PublicKey{*config.votePubkey}
		} else {
			votePubkeys, err = rpcServer.acctsDb.VoteAccountPubkeys(ctx)
			if err != nil {
				return GetVoteAccountsResp{}, fmt.Errorf("list vote accounts: %w", err)
			}
			// Epoch stakes also cover stores created before the durable index was built.
			for key := range stakes.Stakes {
				votePubkeys = append(votePubkeys, key)
			}
			slices.SortFunc(votePubkeys, func(a, b solana.PublicKey) int { return slices.Compare(a[:], b[:]) })
			votePubkeys = slices.Compact(votePubkeys)
		}
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
		latest, stable := rpcServer.acctsDb.CommittedAccountVersion()
		if !stable || latest != version || rpcServer.rootedBank.Load() != bank || rpcServer.rootedPublicationPending(rooted.Slot) {
			continue
		}
		return response, nil
	}
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
