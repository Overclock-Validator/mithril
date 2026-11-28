package rpcserver

import (
	"encoding/json"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGetAccountInfoParsesVoteAccountAtRoot(t *testing.T) {
	const rootedSlot = 500
	votePubkey := solana.PublicKey{1}
	nodePubkey := solana.PublicKey{2}
	withdrawer := solana.PublicKey{3}
	collector := solana.PublicKey{4}
	blockCollector := solana.PublicKey{5}
	authorized := solana.PublicKey{6}
	rootSlot := uint64(490)
	bls := [48]byte{7}
	state := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4}
	state.V4.NodePubkey = nodePubkey
	state.V4.AuthorizedWithdrawer = withdrawer
	state.V4.InflationRewardsCollector = collector
	state.V4.BlockRevenueCollector = blockCollector
	state.V4.InflationRewardsCommissionBps = 755
	state.V4.BlockRevenueCommissionBps = 321
	state.V4.PendingDelegatorRewards = 42
	state.V4.BlsPubkeyCompressed = &bls
	state.V4.RootSlot = &rootSlot
	state.V4.AuthorizedVoters.AuthorizedVoters.Set(3, authorized)
	state.V4.Votes.PushBack(sealevel.LandedVote{Latency: 2, Lockout: sealevel.VoteLockout{Slot: 499, ConfirmationCount: 4}})
	state.V4.EpochCredits = []sealevel.EpochCredits{{Epoch: 3, Credits: 9, PrevCredits: 5}}
	state.V4.LastTimestamp = sealevel.BlockTimestamp{Slot: 499, Timestamp: 1_700_000_000}

	db := newRPCAccountsDB(t)
	account := voteAccountForRPC(t, votePubkey, &state)
	account.Lamports = 27_074_400
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: rootedSlot, Delta: []*accounts.Account{account}}}, rootedSlot, nil, nil)
	require.NoError(t, err)

	server := &RpcServer{acctsDb: db}
	server.SetRootedBankState(rootedSlot, 495, 100)
	got, err := server.GetAccountInfo(t.Context(), mustRawParams(t, []interface{}{
		votePubkey.String(),
		map[string]interface{}{"commitment": "finalized", "encoding": "jsonParsed", "minContextSlot": float64(rootedSlot)},
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(rootedSlot), got.Context.Slot)
	require.NotNil(t, got.Value)
	require.Equal(t, uint64(27_074_400), got.Value.Lamports)

	raw, err := json.Marshal(got.Value.Data)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"lastTimestamp":{"slot":499,"timestamp":1700000000}`)
	var decoded struct {
		Program string `json:"program"`
		Parsed  struct {
			Type string                `json:"type"`
			Info parsedVoteAccountInfo `json:"info"`
		} `json:"parsed"`
		Space uint64 `json:"space"`
	}
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, "vote", decoded.Program)
	require.Equal(t, "vote", decoded.Parsed.Type)
	require.Equal(t, nodePubkey.String(), decoded.Parsed.Info.NodePubkey)
	require.Equal(t, withdrawer.String(), decoded.Parsed.Info.AuthorizedWithdrawer)
	require.Equal(t, uint8(8), decoded.Parsed.Info.Commission)
	require.Equal(t, uint16(755), decoded.Parsed.Info.InflationRewardsCommissionBPS)
	require.Equal(t, "42", decoded.Parsed.Info.PendingDelegatorRewards)
	require.Equal(t, parsedBlockTimestamp{Slot: 499, Timestamp: 1_700_000_000}, decoded.Parsed.Info.LastTimestamp)
	require.Equal(t, []parsedAuthorizedVoter{{Epoch: 3, AuthorizedVoter: authorized.String()}}, decoded.Parsed.Info.AuthorizedVoters)
	require.Equal(t, []parsedVote{{Latency: 2, Slot: 499, ConfirmationCount: 4}}, decoded.Parsed.Info.Votes)
	require.Equal(t, []parsedEpochCredits{{Epoch: 3, Credits: "9", PreviousCredits: "5"}}, decoded.Parsed.Info.EpochCredits)
}

func TestGetAccountInfoJsonParsedFallsBackToBase64(t *testing.T) {
	db := newRPCAccountsDB(t)
	address := solana.PublicKey{9}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 10, Delta: []*accounts.Account{{Key: address, Lamports: 1, Data: []byte{1, 2, 3}}}}}, 10, nil, nil)
	require.NoError(t, err)
	server := &RpcServer{acctsDb: db}
	server.SetRootedBankState(10, 9, 0)

	got, err := server.GetAccountInfo(t.Context(), mustRawParams(t, []interface{}{
		address.String(), map[string]interface{}{"encoding": "jsonParsed"},
	}))
	require.NoError(t, err)
	require.Equal(t, []string{"AQID", "base64"}, got.Value.Data)
}

func TestGetAccountInfoParsesLegacyVoteAccountWithV4Defaults(t *testing.T) {
	votePubkey := solana.PublicKey{1}
	nodePubkey := solana.PublicKey{2}
	state := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionCurrent}
	state.Current.NodePubkey = nodePubkey
	state.Current.Commission = 7

	db := newRPCAccountsDB(t)
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 10, Delta: []*accounts.Account{
		voteAccountForRPC(t, votePubkey, &state),
	}}}, 10, nil, nil)
	require.NoError(t, err)
	server := &RpcServer{acctsDb: db}
	server.SetRootedBankState(10, 9, 0)

	got, err := server.GetAccountInfo(t.Context(), mustRawParams(t, []interface{}{
		votePubkey.String(), map[string]interface{}{"encoding": "jsonParsed"},
	}))
	require.NoError(t, err)
	raw, err := json.Marshal(got.Value.Data)
	require.NoError(t, err)
	var decoded struct {
		Parsed struct {
			Info parsedVoteAccountInfo `json:"info"`
		} `json:"parsed"`
	}
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, uint8(7), decoded.Parsed.Info.Commission)
	require.Equal(t, uint16(700), decoded.Parsed.Info.InflationRewardsCommissionBPS)
	require.Equal(t, votePubkey.String(), decoded.Parsed.Info.InflationRewardsCollector)
	require.Equal(t, nodePubkey.String(), decoded.Parsed.Info.BlockRevenueCollector)
	require.Equal(t, uint16(10000), decoded.Parsed.Info.BlockRevenueCommissionBPS)
	require.Equal(t, "0", decoded.Parsed.Info.PendingDelegatorRewards)
}
