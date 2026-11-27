package rpcserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestSpecExactRPCParameters(t *testing.T) {
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000}}
	server.SetRootedBankState(9007199254740992, 4, 5)
	t.Run("getSlot_exact_control", func(t *testing.T) {
		_, err := server.GetSlot(t.Context(), jsonrpc.RawParams(`[{"minContextSlot":9007199254740993}]`))
		var behind *MinContextSlotNotReachedError
		if !errors.As(err, &behind) {
			t.Fatalf("expected minimum-context error, got %v", err)
		}
	})
	t.Run("epoch_minimum_must_not_round_down", func(t *testing.T) {
		got, err := server.GetEpochInfo(t.Context(), jsonrpc.RawParams(`[{"minContextSlot":9007199254740993}]`))
		var behind *MinContextSlotNotReachedError
		if !errors.As(err, &behind) {
			t.Fatalf("requested minimum 9007199254740993; returned slot %d with error %v", got.AbsoluteSlot, err)
		}
	})
	t.Run("distance_accepts_full_u64", func(t *testing.T) {
		params, err := decodeOptionalParams(jsonrpc.RawParams(`[{"delinquentSlotDistance":18446744073709551615}]`))
		if err != nil {
			t.Fatal(err)
		}
		got, err := parseGetVoteAccountsConfig(params)
		if err != nil || got.delinquentSlotDistance != ^uint64(0) {
			t.Fatalf("valid maximum u64: value=%d error=%v", got.delinquentSlotDistance, err)
		}
	})
	t.Run("fraction_must_not_round_to_integer", func(t *testing.T) {
		_, err := server.GetEpochInfo(t.Context(), jsonrpc.RawParams(`[{"minContextSlot":1.0000000000000001}]`))
		var invalid *InvalidParamsError
		if !errors.As(err, &invalid) {
			t.Fatalf("fractional u64 accepted: error=%v", err)
		}
	})
	t.Run("null_optional_minimum", func(t *testing.T) {
		_, err := server.GetEpochInfo(t.Context(), jsonrpc.RawParams(`[{"minContextSlot":null}]`))
		if err != nil {
			t.Fatalf("optional minimum null should use default: %v", err)
		}
	})
	t.Run("balance_minimum_must_not_round_down", func(t *testing.T) {
		balanceServer := &RpcServer{acctsDb: newRPCAccountsDB(t)}
		balanceServer.SetRootedBankState(9007199254740992, 4, 5)
		params := jsonrpc.RawParams(`["` + (solana.PublicKey{8}).String() + `",{"minContextSlot":9007199254740993}]`)
		got, err := balanceServer.GetBalance(t.Context(), params)
		var behind *MinContextSlotNotReachedError
		if !errors.As(err, &behind) {
			t.Fatalf("requested minimum 9007199254740993; returned slot %d with error %v", got.Context.Slot, err)
		}
	})
}

func TestRPCParameterHTTPContract(t *testing.T) {
	server := NewRpcServer(newRPCAccountsDB(t), 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	server.SetRootedBankState(9007199254740992, 4, 5)
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)
	key := (solana.PublicKey{8}).String()
	for _, test := range []struct {
		method, params string
		code           int
	}{
		{"getEpochInfo", `[{"minContextSlot":9007199254740993}]`, -32016},
		{"getBalance", `["` + key + `",{"minContextSlot":9007199254740993}]`, -32016},
		{"getEpochInfo", `[{"commitment":null,"minContextSlot":null}]`, 0},
		{"getBalance", `["` + key + `",{"commitment":null,"minContextSlot":null}]`, 0},
		{"getEpochInfo", `[{"minContextSlot":1.0000000000000001}]`, -32602},
		{"getBalance", `["` + key + `",{"minContextSlot":1.0000000000000001}]`, -32602},
		{"getLeaderSchedule", `[1.0000000000000001]`, -32602},
		{"getVoteAccounts", `[{"delinquentSlotDistance":1.0000000000000001}]`, -32602},
		{"getBlockProduction", `[{"range":{"firstSlot":1.0000000000000001}}]`, -32602},
		{"getBlockProduction", `[{"range":{"firstSlot":0,"lastSlot":1.0000000000000001}}]`, -32602},
		{"getBlockProduction", `[{"range":{"firstSlot":null}}]`, -32602},
		{"getBlockProduction", `[{"range":{}}]`, -32602},
		{"getBalance", `[null]`, -32602},
		{"getEpochInfo", `[{"minContextSlot":18446744073709551616}]`, -32602},
		{"getEpochInfo", `[{"minContextSlot":-1}]`, -32602},
		{"getEpochInfo", `[{"minContextSlot":"42"}]`, -32602},
		{"getEpochInfo", `[{"minContextSlot":true}]`, -32602},
		{"getEpochInfo", `[{"minContextSlot":1e0}]`, -32602},
		{"getVoteAccounts", `[{"keepUnstakedDelinquents":"true"}]`, -32602},
		{"getLeaderSchedule", `[null,{"keyByVoteAccount":"true"}]`, -32602},
		{"getLeaderSchedule", `[null,{"keyByVoteAccount":true}]`, 0},
		{"getLeaderSchedule", `[null,{"keyByVoteAccount":null}]`, 0},
	} {
		t.Run(test.method+"/"+test.params, func(t *testing.T) {
			body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`, test.method, test.params)
			response, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(body))
			require.NoError(t, err)
			defer response.Body.Close()
			var got struct {
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Code int `json:"code"`
					Data struct {
						ContextSlot uint64 `json:"contextSlot"`
					} `json:"data"`
				} `json:"error"`
			}
			require.NoError(t, json.NewDecoder(response.Body).Decode(&got))
			if test.code == 0 {
				require.Nil(t, got.Error)
				require.NotEmpty(t, got.Result)
				if test.method == "getEpochInfo" {
					var result GetEpochInfoResp
					require.NoError(t, json.Unmarshal(got.Result, &result))
					require.Equal(t, uint64(9007199254740992), result.AbsoluteSlot)
				} else if test.method == "getBalance" {
					var result GetBalanceResp
					require.NoError(t, json.Unmarshal(got.Result, &result))
					require.Equal(t, uint64(9007199254740992), result.Context.Slot)
					require.Zero(t, result.Value)
				}
			} else {
				require.NotNil(t, got.Error)
				require.Equal(t, test.code, got.Error.Code)
				if test.code == -32016 {
					require.Equal(t, uint64(9007199254740992), got.Error.Data.ContextSlot)
				}
			}
		})
	}
}

func TestRPCOptionalConfigAndDecoderContract(t *testing.T) {
	params, err := decodeOptionalParams(jsonrpc.RawParams(`[{"commitment":null,"identity":null,"votePubkey":null,"range":null,"keyByVoteAccount":null,"keepUnstakedDelinquents":null,"delinquentSlotDistance":null}]`))
	require.NoError(t, err)
	vote, err := parseGetVoteAccountsConfig(params)
	require.NoError(t, err)
	require.Nil(t, vote.votePubkey)
	require.Equal(t, uint64(128), vote.delinquentSlotDistance)
	leaders, err := parseLeaderScheduleConfig(params[0])
	require.NoError(t, err)
	require.Nil(t, leaders.identity)
	production, err := parseBlockProductionConfig(params)
	require.NoError(t, err)
	require.Nil(t, production.identity)
	require.Nil(t, production.firstSlot)
	require.Nil(t, production.lastSlot)

	params, err = decodeOptionalParams(jsonrpc.RawParams(`[{"range":{"firstSlot":9007199254740993,"lastSlot":18446744073709551615}}]`))
	require.NoError(t, err)
	production, err = parseBlockProductionConfig(params)
	require.NoError(t, err)
	require.Equal(t, uint64(9007199254740993), *production.firstSlot)
	require.Equal(t, ^uint64(0), *production.lastSlot)
	params, err = decodeOptionalParams(jsonrpc.RawParams(`[{"range":{"firstSlot":0,"lastSlot":null}}]`))
	require.NoError(t, err)
	production, err = parseBlockProductionConfig(params)
	require.NoError(t, err)
	require.Nil(t, production.lastSlot)
	for _, raw := range []string{`[] []`, `[] trailing`, `[`, `{}`, `true`} {
		_, err := decodeOptionalParams(jsonrpc.RawParams(raw))
		require.Error(t, err, raw)
	}
	_, err = decodeOptionalParams(jsonrpc.RawParams("[] \n\t"))
	require.NoError(t, err)
}
