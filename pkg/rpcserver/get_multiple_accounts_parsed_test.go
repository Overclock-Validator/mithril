package rpcserver

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func TestGetMultipleAccountsParsedVoteConsumer(t *testing.T) {
	vote, node := solana.PublicKey{1}, solana.PublicKey{2}
	bls := [48]byte{7}
	state := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4}
	state.V4.NodePubkey = node
	state.V4.BlsPubkeyCompressed = &bls
	state.V4.InflationRewardsCommissionBps = 755
	state.V4.PendingDelegatorRewards = 42
	state.V4.EpochCredits = []sealevel.EpochCredits{{Epoch: 3, Credits: 9, PrevCredits: 5}}
	legacy := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionCurrent}
	legacy.Current.NodePubkey = node
	legacy.Current.Commission = 7
	legacyKey := solana.PublicKey{3}
	unknown := &accounts.Account{Key: solana.PublicKey{4}, Lamports: 1, Data: []byte{1, 2, 3}}
	malformed := &accounts.Account{Key: solana.PublicKey{5}, Owner: addresses.VoteProgramAddr, Lamports: 1, Data: []byte{3}}
	deleted := &accounts.Account{Key: solana.PublicKey{6}, Owner: addresses.VoteProgramAddr, Data: []byte{3}}
	_, endpoint, _ := multipleAccountsFixture(t, voteAccountForRPC(t, vote, &state),
		voteAccountForRPC(t, legacyKey, &legacy), unknown, malformed, deleted)
	params := fmt.Sprintf(`[[%q,%q,%q,%q,%q,%q,%q],{"encoding":"jsonParsed","commitment":"confirmed","minContextSlot":42}]`,
		vote, legacyKey, solana.PublicKey{99}, vote, unknown.Key, malformed.Key, deleted.Key)
	body := getBlockHistoryHTTPCall(t, endpoint, "getMultipleAccounts", params)
	require.NotContains(t, body, "error")
	var result struct {
		Context GetAccountInfoRespContext `json:"context"`
		Value   []json.RawMessage         `json:"value"`
	}
	require.NoError(t, json.Unmarshal(body["result"], &result))
	require.Equal(t, uint64(42), result.Context.Slot)
	require.Len(t, result.Value, 7)
	require.JSONEq(t, "null", string(result.Value[2]))
	require.JSONEq(t, "null", string(result.Value[6]))
	require.JSONEq(t, string(result.Value[0]), string(result.Value[3]))
	for i, key := range []solana.PublicKey{vote, legacyKey} {
		single := getBlockHistoryHTTPCall(t, endpoint, "getAccountInfo", fmt.Sprintf(`[%q,{"encoding":"jsonParsed","commitment":"finalized"}]`, key))
		singleResult := historyResponseObject(t, single["result"])
		require.JSONEq(t, string(singleResult["value"]), string(result.Value[i]))
		account := historyResponseObject(t, result.Value[i])
		data := historyResponseObject(t, account["data"])
		require.JSONEq(t, `"vote"`, string(data["program"]))
		parsed := historyResponseObject(t, data["parsed"])
		info := historyResponseObject(t, parsed["info"])
		require.JSONEq(t, fmt.Sprintf(`%q`, node), string(info["nodePubkey"]))
		if i == 0 {
			require.JSONEq(t, fmt.Sprintf(`%q`, base58.Encode(bls[:])), string(info["blsPubkeyCompressed"]))
			require.JSONEq(t, "755", string(info["inflationRewardsCommissionBps"]))
			require.JSONEq(t, `"42"`, string(info["pendingDelegatorRewards"]))
			require.JSONEq(t, `[{"epoch":3,"credits":"9","previousCredits":"5"}]`, string(info["epochCredits"]))
		} else {
			require.JSONEq(t, "null", string(info["blsPubkeyCompressed"]))
		}
	}
	for i, encoded := range map[int]string{4: `["AQID","base64"]`, 5: `["Aw==","base64"]`} {
		account := historyResponseObject(t, result.Value[i])
		require.JSONEq(t, encoded, string(account["data"]))
	}
	empty := getBlockHistoryHTTPCall(t, endpoint, "getMultipleAccounts", `[[],{"encoding":"jsonParsed"}]`)
	require.NotContains(t, empty, "error")
	for _, config := range []string{
		`{"encoding":"jsonParsed","dataSlice":{"offset":0,"length":1}}`,
		`{"encoding":"jsonParsed","commitment":"processed"}`,
	} {
		bad := getBlockHistoryHTTPCall(t, endpoint, "getMultipleAccounts", fmt.Sprintf(`[[%q],%s]`, vote, config))
		require.Contains(t, bad, "error")
	}
}

func TestGetMultipleAccountsParsedOutputBudget(t *testing.T) {
	key := solana.PublicKey{17}
	state := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4}
	state.V4.EpochCredits = make([]sealevel.EpochCredits, 8192)
	for i := range state.V4.EpochCredits {
		state.V4.EpochCredits[i] = sealevel.EpochCredits{Epoch: math.MaxUint64, Credits: math.MaxUint64, PrevCredits: math.MaxUint64}
	}
	account := voteAccountForRPC(t, key, &state)
	const count = 40
	require.Less(t, count*len(account.Data), maxMultipleAccountsDataBytes)
	server, endpoint, _, wait := multipleAccountsFixtureWithCompletion(t, account)
	keys := strings.TrimSuffix(strings.Repeat(fmt.Sprintf(`%q,`, key), count), ",")
	body := getBlockHistoryHTTPCall(t, endpoint, "getMultipleAccounts", fmt.Sprintf(`[[%s],{"encoding":"jsonParsed"}]`, keys))
	requireGetBlockHistoryError(t, body, -32602, "getMultipleAccounts exceeds 32 MiB parsed response budget")
	wait()
	require.Zero(t, len(server.queryResponses))
	// Rejection releases admission and does not change the rooted account.
	body = getBlockHistoryHTTPCall(t, endpoint, "getMultipleAccounts", fmt.Sprintf(`[[%q],{"encoding":"jsonParsed"}]`, key))
	require.NotContains(t, body, "error")
}
