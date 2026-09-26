package rpcserver

import (
	"strconv"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
)

type parsedVoteAccount struct {
	Program string                `json:"program"`
	Parsed  parsedVoteAccountBody `json:"parsed"`
	Space   uint64                `json:"space"`
}

type parsedVoteAccountBody struct {
	Type string                `json:"type"`
	Info parsedVoteAccountInfo `json:"info"`
}

type parsedVoteAccountInfo struct {
	NodePubkey                    string                  `json:"nodePubkey"`
	AuthorizedWithdrawer          string                  `json:"authorizedWithdrawer"`
	Commission                    uint8                   `json:"commission"`
	Votes                         []parsedVote            `json:"votes"`
	RootSlot                      *uint64                 `json:"rootSlot"`
	AuthorizedVoters              []parsedAuthorizedVoter `json:"authorizedVoters"`
	PriorVoters                   []parsedPriorVoter      `json:"priorVoters"`
	EpochCredits                  []parsedEpochCredits    `json:"epochCredits"`
	LastTimestamp                 parsedBlockTimestamp    `json:"lastTimestamp"`
	InflationRewardsCommissionBPS uint16                  `json:"inflationRewardsCommissionBps"`
	InflationRewardsCollector     string                  `json:"inflationRewardsCollector"`
	BlockRevenueCollector         string                  `json:"blockRevenueCollector"`
	BlockRevenueCommissionBPS     uint16                  `json:"blockRevenueCommissionBps"`
	PendingDelegatorRewards       string                  `json:"pendingDelegatorRewards"`
	BLSPubkeyCompressed           *string                 `json:"blsPubkeyCompressed"`
}

type parsedVote struct {
	Latency           uint8  `json:"latency"`
	Slot              uint64 `json:"slot"`
	ConfirmationCount uint32 `json:"confirmationCount"`
}

type parsedAuthorizedVoter struct {
	Epoch           uint64 `json:"epoch"`
	AuthorizedVoter string `json:"authorizedVoter"`
}

type parsedPriorVoter struct {
	AuthorizedPubkey            string `json:"authorizedPubkey"`
	EpochOfLastAuthorizedSwitch uint64 `json:"epochOfLastAuthorizedSwitch"`
	TargetEpoch                 uint64 `json:"targetEpoch"`
}

type parsedEpochCredits struct {
	Epoch           uint64 `json:"epoch"`
	Credits         string `json:"credits"`
	PreviousCredits string `json:"previousCredits"`
}

type parsedBlockTimestamp struct {
	Slot      uint64 `json:"slot"`
	Timestamp int64  `json:"timestamp"`
}

func parsedVoteAccountData(data []byte, votePubkey solana.PublicKey) (parsedVoteAccount, error) {
	versioned, err := sealevel.UnmarshalVersionedVoteState(data)
	if err != nil {
		return parsedVoteAccount{}, err
	}
	state := versioned.ConvertToCurrent()
	info := parsedVoteAccountInfo{
		NodePubkey:           state.NodePubkey.String(),
		AuthorizedWithdrawer: state.AuthorizedWithdrawer.String(),
		Commission:           state.Commission,
		Votes:                make([]parsedVote, 0, state.Votes.Len()),
		RootSlot:             state.RootSlot,
		AuthorizedVoters:     make([]parsedAuthorizedVoter, 0, state.AuthorizedVoters.AuthorizedVoters.Len()),
		PriorVoters:          make([]parsedPriorVoter, 0),
		EpochCredits:         make([]parsedEpochCredits, 0, len(state.EpochCredits)),
		LastTimestamp: parsedBlockTimestamp{
			Slot: state.LastTimestamp.Slot, Timestamp: state.LastTimestamp.Timestamp,
		},
		InflationRewardsCommissionBPS: uint16(state.Commission) * 100,
		InflationRewardsCollector:     votePubkey.String(),
		BlockRevenueCollector:         state.NodePubkey.String(),
		BlockRevenueCommissionBPS:     10000,
		PendingDelegatorRewards:       "0",
	}
	for i := 0; i < state.Votes.Len(); i++ {
		vote := state.Votes.At(i)
		info.Votes = append(info.Votes, parsedVote{
			Latency:           vote.Latency,
			Slot:              vote.Lockout.Slot,
			ConfirmationCount: vote.Lockout.ConfirmationCount,
		})
	}
	epochs, voters := state.AuthorizedVoters.AuthorizedVoters.KeyValues()
	for i := range epochs {
		info.AuthorizedVoters = append(info.AuthorizedVoters, parsedAuthorizedVoter{
			Epoch: epochs[i], AuthorizedVoter: voters[i].String(),
		})
	}
	for _, credits := range state.EpochCredits {
		info.EpochCredits = append(info.EpochCredits, parsedEpochCredits{
			Epoch: credits.Epoch, Credits: strconv.FormatUint(credits.Credits, 10), PreviousCredits: strconv.FormatUint(credits.PrevCredits, 10),
		})
	}
	if versioned.Type == sealevel.VoteStateVersionV4 {
		info.Commission, info.InflationRewardsCommissionBPS = voteCommission(versioned, state)
		info.InflationRewardsCollector = versioned.V4.InflationRewardsCollector.String()
		info.BlockRevenueCollector = versioned.V4.BlockRevenueCollector.String()
		info.BlockRevenueCommissionBPS = versioned.V4.BlockRevenueCommissionBps
		info.PendingDelegatorRewards = strconv.FormatUint(versioned.V4.PendingDelegatorRewards, 10)
		if versioned.V4.BlsPubkeyCompressed != nil {
			encoded := base58.Encode(versioned.V4.BlsPubkeyCompressed[:])
			info.BLSPubkeyCompressed = &encoded
		}
	}
	return parsedVoteAccount{
		Program: "vote",
		Parsed:  parsedVoteAccountBody{Type: "vote", Info: info},
		Space:   uint64(len(data)),
	}, nil
}
