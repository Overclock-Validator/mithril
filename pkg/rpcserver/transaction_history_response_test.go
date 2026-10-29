package rpcserver

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"testing"

	txwire "github.com/Overclock-Validator/mithril/pkg/tpu/wire"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

// These are structurally valid wire/retained-data fixtures, not execution or
// canonical-publication proof. No node, account store, or lookup reader exists.
func historyResponseRecord(t *testing.T, tx *solana.Transaction, keys []solana.PublicKey) (transactionhistory.Record, *transactionhistory.Metadata) {
	t.Helper()
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)
	_, err = txwire.Sanitize(wire)
	require.NoError(t, err)
	metadata := &transactionhistory.Metadata{
		AccountKeys: keys, Error: json.RawMessage("null"), Fee: 5000,
		PreBalances: make([]uint64, len(keys)), PostBalances: make([]uint64, len(keys)),
	}
	for i := range keys {
		metadata.PreBalances[i], metadata.PostBalances[i] = uint64(10_000+i), uint64(5_000+i)
	}
	encoded, err := metadata.MarshalBinary()
	require.NoError(t, err)
	return transactionhistory.Record{FirstSignature: [64]byte(tx.Signatures[0]), Transaction: wire, Metadata: encoded}, metadata
}

func historyResponseFixture(t *testing.T, version solana.MessageVersion) (transactionhistory.Record, *solana.Transaction, *transactionhistory.Metadata) {
	t.Helper()
	tx, _ := testLegacyTransaction(t)
	tx.Signatures[0][0] = 7
	tx.Message.AccountKeys = append(tx.Message.AccountKeys, solana.PublicKey{0x22})
	tx.Message.Header.NumReadonlyUnsignedAccounts = 1
	tx.Message.Instructions = []solana.CompiledInstruction{{ProgramIDIndex: 1, Accounts: []uint16{0}, Data: []byte{0, 255, 7}}}
	keys := slices.Clone(tx.Message.AccountKeys)
	switch version {
	case solana.MessageVersionV0:
		_, err := tx.Message.SetVersion(version)
		require.NoError(t, err)
		tx.Message.AddressTableLookups = solana.MessageAddressTableLookupSlice{
			{AccountKey: solana.PublicKey{0x31}, WritableIndexes: solana.Uint8SliceAsNum{2, 5}, ReadonlyIndexes: solana.Uint8SliceAsNum{7}},
			{AccountKey: solana.PublicKey{0x32}, WritableIndexes: solana.Uint8SliceAsNum{1}, ReadonlyIndexes: solana.Uint8SliceAsNum{3, 8}},
		}
		// Retained resolution: all writable keys across tables, then readonly.
		keys = append(keys, solana.PublicKey{0x41}, solana.PublicKey{0x42}, solana.PublicKey{0x43}, solana.PublicKey{0x51}, solana.PublicKey{0x52}, solana.PublicKey{0x53})
		tx.Message.Instructions[0].Accounts = []uint16{0, 2, 7}
	case solana.MessageVersionV1:
		tx, _ = testV1Transaction(t, 3)
		tx.Signatures[0][0] = 7
		tx.Message.Instructions[0].Data = []byte{0, 255, 7}
		keys = slices.Clone(tx.Message.AccountKeys)
	}
	record, metadata := historyResponseRecord(t, tx, keys)
	return record, tx, metadata
}

func historyResponseObject(t *testing.T, value any) map[string]json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	var result map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &result))
	return result
}

func TestHistoryResponseVersionAndEncodingMatrix(t *testing.T) {
	zero, one, future := uint8(0), uint8(1), uint8(255)
	for _, version := range []solana.MessageVersion{solana.MessageVersionLegacy, solana.MessageVersionV0, solana.MessageVersionV1} {
		for _, maximum := range []*uint8{nil, &zero, &one, &future} {
			for _, encoding := range []string{"", "json", "base64"} {
				maxLabel := "omitted"
				if maximum != nil {
					maxLabel = fmt.Sprint(*maximum)
				}
				name := fmt.Sprintf("version=%d/max=%s/encoding=%s", version, maxLabel, encoding)
				t.Run(name, func(t *testing.T) {
					record, _, _ := historyResponseFixture(t, version)
					got, err := encodeHistoryTransaction(record, encoding, maximum)
					if version != solana.MessageVersionLegacy && (maximum == nil || uint8(version-solana.MessageVersionV0) > *maximum) {
						var unsupported *historyUnsupportedVersionError
						require.ErrorAs(t, err, &unsupported)
						require.Equal(t, uint8(version-solana.MessageVersionV0), unsupported.Version)
						require.Equal(t, historyTransactionResponse{}, got)
						return
					}
					require.NoError(t, err)
					object := historyResponseObject(t, got)
					if version == solana.MessageVersionLegacy && maximum == nil {
						require.NotContains(t, object, "version")
					} else if version == solana.MessageVersionLegacy {
						require.JSONEq(t, `"legacy"`, string(object["version"]))
					} else {
						require.JSONEq(t, fmt.Sprint(version-solana.MessageVersionV0), string(object["version"]))
					}
					if encoding == "base64" {
						var tuple []string
						require.NoError(t, json.Unmarshal(object["transaction"], &tuple))
						require.Equal(t, []string{base64.StdEncoding.EncodeToString(record.Transaction), "base64"}, tuple)
					} else {
						transaction := historyResponseObject(t, object["transaction"])
						require.Contains(t, transaction, "message")
					}
				})
			}
		}
	}
	for _, encoding := range []string{"base58", "binary", "jsonParsed", "accounts", "JSON"} {
		record, _, _ := historyResponseFixture(t, solana.MessageVersionLegacy)
		got, err := encodeHistoryTransaction(record, encoding, nil)
		var invalid *InvalidParamsError
		require.ErrorAs(t, err, &invalid)
		rpcErr, err := invalid.ToJSONRPCError()
		require.NoError(t, err)
		require.EqualValues(t, -32602, rpcErr.Code)
		require.Equal(t, historyTransactionResponse{}, got)
	}
}

func TestHistoryResponseRawMessagesAndLoadedAddresses(t *testing.T) {
	maximum := uint8(1)
	for _, version := range []solana.MessageVersion{solana.MessageVersionLegacy, solana.MessageVersionV0, solana.MessageVersionV1} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			record, tx, metadata := historyResponseFixture(t, version)
			got, err := encodeHistoryTransaction(record, "json", &maximum)
			require.NoError(t, err)
			object := historyResponseObject(t, got)
			transaction := historyResponseObject(t, object["transaction"])
			message := historyResponseObject(t, transaction["message"])
			wantKeys, err := json.Marshal(tx.Message.AccountKeys)
			require.NoError(t, err)
			require.JSONEq(t, string(wantKeys), string(message["accountKeys"]))
			wantHeader, err := json.Marshal(tx.Message.Header)
			require.NoError(t, err)
			require.JSONEq(t, string(wantHeader), string(message["header"]))
			var blockhash string
			require.NoError(t, json.Unmarshal(message["recentBlockhash"], &blockhash))
			require.Equal(t, tx.Message.RecentBlockhash.String(), blockhash)
			var signatures []string
			require.NoError(t, json.Unmarshal(transaction["signatures"], &signatures))
			require.Equal(t, []string{tx.Signatures[0].String()}, signatures)
			var instructions []map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(message["instructions"], &instructions))
			require.Len(t, instructions, 1)
			require.JSONEq(t, "1", string(instructions[0]["stackHeight"]))
			require.JSONEq(t, "1", string(instructions[0]["programIdIndex"]))
			var data string
			require.NoError(t, json.Unmarshal(instructions[0]["data"], &data))
			require.Equal(t, base58.Encode([]byte{0, 255, 7}), data)
			meta := historyResponseObject(t, object["meta"])
			loaded := historyResponseObject(t, meta["loadedAddresses"])
			if version == solana.MessageVersionV0 {
				require.JSONEq(t, `[0,2,7]`, string(instructions[0]["accounts"]))
				var writable, readonly []string
				require.NoError(t, json.Unmarshal(loaded["writable"], &writable))
				require.NoError(t, json.Unmarshal(loaded["readonly"], &readonly))
				require.Equal(t, []string{metadata.AccountKeys[2].String(), metadata.AccountKeys[3].String(), metadata.AccountKeys[4].String()}, writable)
				require.Equal(t, []string{metadata.AccountKeys[5].String(), metadata.AccountKeys[6].String(), metadata.AccountKeys[7].String()}, readonly)
				var lookups []map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(message["addressTableLookups"], &lookups))
				require.Len(t, lookups, 2)
				require.JSONEq(t, `[2,5]`, string(lookups[0]["writableIndexes"]))
				require.JSONEq(t, `[7]`, string(lookups[0]["readonlyIndexes"]))
				require.JSONEq(t, `[1]`, string(lookups[1]["writableIndexes"]))
				require.JSONEq(t, `[3,8]`, string(lookups[1]["readonlyIndexes"]))
			} else {
				require.JSONEq(t, `[0]`, string(instructions[0]["accounts"]))
				require.JSONEq(t, `{"writable":[],"readonly":[]}`, string(meta["loadedAddresses"]))
				require.NotContains(t, message, "addressTableLookups")
			}
			if version == solana.MessageVersionV1 {
				require.JSONEq(t, `{"priorityFee":7,"computeUnitLimit":100000,"loadedAccountsDataSizeLimit":null,"heapSize":null}`, string(message["transactionConfig"]))
			} else {
				require.NotContains(t, message, "transactionConfig")
			}
		})
	}
	_, tx, _ := historyResponseFixture(t, solana.MessageVersionV0)
	tx.Message.AddressTableLookups = nil
	tx.Message.Instructions[0].Accounts = nil
	record, _ := historyResponseRecord(t, tx, tx.Message.AccountKeys)
	got, err := encodeHistoryTransaction(record, "json", &maximum)
	require.NoError(t, err)
	object := historyResponseObject(t, got)
	transaction := historyResponseObject(t, object["transaction"])
	message := historyResponseObject(t, transaction["message"])
	require.JSONEq(t, `[]`, string(message["addressTableLookups"]))
	var instructions []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(message["instructions"], &instructions))
	require.JSONEq(t, `[]`, string(instructions[0]["accounts"]))
}

func TestHistoryResponseMetadataShapeAndReturnPresence(t *testing.T) {
	for _, status := range []string{"null", `{"InstructionError":[0,{"Custom":0}]}`, `{"InstructionError":[0,{"Custom":42}]}`} {
		for _, present := range []bool{false, true} {
			record, _, metadata := historyResponseFixture(t, solana.MessageVersionLegacy)
			metadata.Error = json.RawMessage(status)
			metadata.Fee, metadata.PreBalances[0] = math.MaxUint64, math.MaxUint64
			if present {
				metadata.ReturnData = &transactionhistory.ReturnData{ProgramID: solana.PublicKey{9}}
			}
			var err error
			record.Metadata, err = metadata.MarshalBinary()
			require.NoError(t, err)
			jsonResult, err := encodeHistoryTransaction(record, "json", nil)
			require.NoError(t, err)
			base64Result, err := encodeHistoryTransaction(record, "base64", nil)
			require.NoError(t, err)
			jsonObject, base64Object := historyResponseObject(t, jsonResult), historyResponseObject(t, base64Result)
			require.JSONEq(t, string(jsonObject["meta"]), string(base64Object["meta"]))
			meta := historyResponseObject(t, jsonObject["meta"])
			require.JSONEq(t, status, string(meta["err"]))
			if status == "null" {
				require.JSONEq(t, `{"Ok":null}`, string(meta["status"]))
			} else {
				require.JSONEq(t, `{"Err":`+status+`}`, string(meta["status"]))
			}
			require.Equal(t, "18446744073709551615", string(meta["fee"]))
			require.Equal(t, `[18446744073709551615,10001]`, string(meta["preBalances"]))
			require.Equal(t, `[5000,5001]`, string(meta["postBalances"]))
			require.Equal(t, "0", string(meta["computeUnitsConsumed"]))
			for _, field := range []string{"logMessages", "rewards"} {
				require.Equal(t, "null", string(meta[field]))
			}
			for _, field := range []string{"innerInstructions", "preTokenBalances", "postTokenBalances"} {
				require.Equal(t, "[]", string(meta[field]))
			}
			require.NotContains(t, meta, "costUnits")
			if present {
				ret := historyResponseObject(t, meta["returnData"])
				require.JSONEq(t, `["","base64"]`, string(ret["data"]))
				var program string
				require.NoError(t, json.Unmarshal(ret["programId"], &program))
				require.Equal(t, metadata.ReturnData.ProgramID.String(), program)
			} else {
				require.NotContains(t, meta, "returnData")
			}
		}
	}
}

func TestHistoryResponseCPIAndRawTokenFidelity(t *testing.T) {
	record, _, metadata := historyResponseFixture(t, solana.MessageVersionV0)
	maximum := uint8(0)
	metadata.InnerInstructions = []transactionhistory.InnerInstructionGroup{{Index: 0, Instructions: []transactionhistory.InnerInstruction{
		{ProgramIDIndex: 1, Accounts: []uint8{0, 7}, Data: []byte{0, 255, 42}, StackHeight: 2},
		{ProgramIDIndex: 1},
	}}}
	metadata.ReturnData = &transactionhistory.ReturnData{ProgramID: solana.PublicKey{9}, Data: []byte{0, 255, 42}}
	cases := []struct {
		amount   uint64
		decimals uint8
		exact    string
		null     bool
	}{
		{1<<53 + 1, 0, "9007199254740993", false},
		{math.MaxUint64, 19, "1.8446744073709551615", false},
		{math.MaxUint64, 20, "0.18446744073709551615", true},
		{0, 20, "0", true},
		{1, 255, "0." + strings.Repeat("0", 254) + "1", true},
	}
	for i, tc := range cases {
		balance := transactionhistory.TokenBalance{AccountIndex: uint16(i), Mint: solana.PublicKey{0x61}, Owner: solana.PublicKey{0x62}, ProgramID: solana.Token2022ProgramID, Amount: tc.amount, Decimals: tc.decimals}
		metadata.PreTokenBalances = append(metadata.PreTokenBalances, balance)
		metadata.PostTokenBalances = append(metadata.PostTokenBalances, balance)
	}
	var err error
	record.Metadata, err = metadata.MarshalBinary()
	require.NoError(t, err)
	got, err := encodeHistoryTransaction(record, "json", &maximum)
	require.NoError(t, err)
	object := historyResponseObject(t, got)
	meta := historyResponseObject(t, object["meta"])
	var groups []struct {
		Index        uint8                        `json:"index"`
		Instructions []map[string]json.RawMessage `json:"instructions"`
	}
	require.NoError(t, json.Unmarshal(meta["innerInstructions"], &groups))
	require.Len(t, groups, 1)
	require.Equal(t, uint8(0), groups[0].Index)
	require.Len(t, groups[0].Instructions, 2)
	instruction := groups[0].Instructions[0]
	require.JSONEq(t, `[0,7]`, string(instruction["accounts"]))
	require.Equal(t, "2", string(instruction["stackHeight"]))
	require.Equal(t, "1", string(instruction["programIdIndex"]))
	var encoded string
	require.NoError(t, json.Unmarshal(instruction["data"], &encoded))
	require.Equal(t, base58.Encode([]byte{0, 255, 42}), encoded)
	require.Equal(t, `[]`, string(groups[0].Instructions[1]["accounts"]))
	require.Equal(t, `""`, string(groups[0].Instructions[1]["data"]))
	require.Equal(t, `null`, string(groups[0].Instructions[1]["stackHeight"]))
	ret := historyResponseObject(t, meta["returnData"])
	require.JSONEq(t, `["AP8q","base64"]`, string(ret["data"]))
	for _, name := range []string{"preTokenBalances", "postTokenBalances"} {
		var balances []map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(meta[name], &balances))
		require.Len(t, balances, len(cases))
		for i, tc := range cases {
			balance := balances[i]
			require.Equal(t, fmt.Sprint(i), string(balance["accountIndex"]))
			var program, mint, owner string
			require.NoError(t, json.Unmarshal(balance["programId"], &program))
			require.NoError(t, json.Unmarshal(balance["mint"], &mint))
			require.NoError(t, json.Unmarshal(balance["owner"], &owner))
			require.Equal(t, solana.Token2022ProgramID.String(), program)
			require.Equal(t, metadata.PreTokenBalances[i].Mint.String(), mint)
			require.Equal(t, metadata.PreTokenBalances[i].Owner.String(), owner)
			amount := historyResponseObject(t, balance["uiTokenAmount"])
			var raw, exact string
			require.NoError(t, json.Unmarshal(amount["amount"], &raw))
			require.NoError(t, json.Unmarshal(amount["uiAmountString"], &exact))
			require.Equal(t, fmt.Sprint(tc.amount), raw)
			require.Equal(t, tc.exact, exact)
			require.Equal(t, fmt.Sprint(tc.decimals), string(amount["decimals"]))
			require.Contains(t, amount, "uiAmount")
			if tc.null {
				require.Equal(t, "null", string(amount["uiAmount"]))
			} else {
				require.NotEqual(t, "null", string(amount["uiAmount"]))
			}
			if i == 0 {
				require.Equal(t, "9007199254740992", string(amount["uiAmount"]))
			}
		}
	}
	// The encoder owns its decoded values: caller buffers may be reused later.
	before, err := json.Marshal(got)
	require.NoError(t, err)
	clear(record.Transaction)
	clear(record.Metadata)
	after, err := json.Marshal(got)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestHistoryResponseRejectsCorruptionAndCrossFieldMismatch(t *testing.T) {
	maximum := uint8(1)
	for _, name := range []string{"signature", "wire-trailing", "wire-version", "metadata-trailing", "metadata-missing", "metadata-oversized", "static-key", "missing-key", "extra-key", "metadata-truncated", "cpi-ordinal"} {
		t.Run(name, func(t *testing.T) {
			record, _, metadata := historyResponseFixture(t, solana.MessageVersionV0)
			switch name {
			case "signature":
				record.FirstSignature[0]++
			case "wire-trailing":
				record.Transaction = append(record.Transaction, 0)
			case "wire-version":
				view, err := txwire.Parse(record.Transaction)
				require.NoError(t, err)
				record.Transaction[view.MessageOffset] = 0x82
			case "metadata-trailing":
				record.Metadata = append(record.Metadata, 0)
			case "metadata-missing":
				record.Metadata = nil
			case "metadata-oversized":
				record.Metadata = make([]byte, transactionhistory.MaxMetadataBytes+1)
			case "static-key":
				metadata.AccountKeys[0][0]++
			case "missing-key":
				metadata.AccountKeys = metadata.AccountKeys[:7]
				metadata.PreBalances = metadata.PreBalances[:7]
				metadata.PostBalances = metadata.PostBalances[:7]
			case "extra-key":
				metadata.AccountKeys = append(metadata.AccountKeys, solana.PublicKey{99})
				metadata.PreBalances = append(metadata.PreBalances, 0)
				metadata.PostBalances = append(metadata.PostBalances, 0)
			case "metadata-truncated":
				record.Metadata = record.Metadata[:len(record.Metadata)-1]
			case "cpi-ordinal":
				metadata.InnerInstructions = []transactionhistory.InnerInstructionGroup{{Index: 1, Instructions: []transactionhistory.InnerInstruction{{ProgramIDIndex: 1}}}}
			}
			if name == "static-key" || name == "missing-key" || name == "extra-key" || name == "cpi-ordinal" {
				var err error
				record.Metadata, err = metadata.MarshalBinary()
				require.NoError(t, err)
			}
			got, err := encodeHistoryTransaction(record, "json", &maximum)
			require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
			require.Equal(t, historyTransactionResponse{}, got)
		})
	}
	record, _, _ := historyResponseFixture(t, solana.MessageVersionV1)
	for end := 0; end < len(record.Transaction); end++ {
		bad := record
		bad.Transaction = bad.Transaction[:end]
		got, err := encodeHistoryTransaction(bad, "json", &maximum)
		require.Error(t, err, "wire prefix %d", end)
		require.Equal(t, historyTransactionResponse{}, got)
	}

	record, _, _ = historyResponseFixture(t, solana.MessageVersionLegacy)
	view, err := txwire.Parse(record.Transaction)
	require.NoError(t, err)
	bad := append(bytes.Clone(record.Transaction[:view.MessageOffset]), 0x80)
	bad = append(bad, record.Transaction[view.MessageOffset:]...)
	bad = append(bad, 0xff, 0xff, 0x03) // 65,535 declared but absent ALT entries.
	record.Transaction = bad
	require.Less(t, len(bad), 512)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got, err := encodeHistoryTransaction(record, "json", &maximum)
	runtime.ReadMemStats(&after)
	require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
	require.Equal(t, historyTransactionResponse{}, got)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20), "malformed wire must be sanitized before SDK vector allocation")
}

type historyResponseFixtureService struct{ record transactionhistory.Record }

func (s *historyResponseFixtureService) Encode() (historyTransactionResponse, error) {
	return encodeHistoryTransaction(s.record, "json", nil)
}

func TestHistoryResponseUnsupportedVersionHTTP(t *testing.T) {
	tx, _ := testV1Transaction(t, 0)
	record, _ := historyResponseRecord(t, tx, tx.Message.AccountKeys)
	server := jsonrpc.NewServer(jsonrpc.WithServerErrors(rpcErrorRegistry()))
	server.Register("HistoryFixture", &historyResponseFixtureService{record: record})
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)
	response, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":7,"method":"HistoryFixture.Encode","params":[]}`))
	require.NoError(t, err)
	defer response.Body.Close()
	var got struct {
		ID     int                        `json:"id"`
		Result json.RawMessage            `json:"result"`
		Error  map[string]json.RawMessage `json:"error"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&got))
	require.Equal(t, 7, got.ID)
	require.Empty(t, got.Result)
	require.JSONEq(t, `-32015`, string(got.Error["code"]))
	require.JSONEq(t, `"Transaction version (1) is not supported by the requesting client. Please try the request again with the following configuration parameter: \"maxSupportedTransactionVersion\": 1"`, string(got.Error["message"]))
	require.NotContains(t, got.Error, "data")
}
