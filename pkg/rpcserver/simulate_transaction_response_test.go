package rpcserver

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// All 14 spec-mandated fields must always be present (non-error happy path)
// — Agave emits them unconditionally as either real values or null.
func TestSimulateTransactionResp_JSONShape_AllSpecFieldsPresent(t *testing.T) {
	fee := uint64(5000)
	units := uint64(100)
	dataSize := uint32(1024)
	logs := []string{"a", "b"}
	bals := []uint64{1, 2}
	tokenBals := []TokenBalancePayload{}

	resp := SimulateTransactionResp{
		Context: SimulateTransactionRespContext{Slot: 42},
		Value: SimulateTransactionRespValue{
			Err:                    nil,
			Logs:                   &logs,
			Accounts:               nil,
			UnitsConsumed:          &units,
			ReturnData:             nil,
			InnerInstructions:      nil,
			ReplacementBlockhash:   nil,
			LoadedAccountsDataSize: &dataSize,
			Fee:                    &fee,
			PreBalances:            &bals,
			PostBalances:           &bals,
			PreTokenBalances:       &tokenBals,
			PostTokenBalances:      &tokenBals,
			LoadedAddresses:        &LoadedAddressesPayload{Readonly: []string{}, Writable: []string{}},
		},
	}

	raw, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, map[string]any{"slot": float64(42)}, decoded["context"])

	value, ok := decoded["value"].(map[string]interface{})
	require.True(t, ok, "response must contain a value object")

	specRequiredKeys := []string{
		"err",
		"logs",
		"accounts",
		"unitsConsumed",
		"returnData",
		"innerInstructions",
		"replacementBlockhash",
		"loadedAccountsDataSize",
		"fee",
		"preBalances",
		"postBalances",
		"preTokenBalances",
		"postTokenBalances",
		"loadedAddresses",
	}

	for _, key := range specRequiredKeys {
		_, present := value[key]
		assert.True(t, present, "spec-required field %q missing from value", key)
	}
}

// Every value field is emitted unconditionally; nil maps to JSON null.
// Agave does not omit any field — strict-shape clients depend on this.
func TestSimulateTransactionResp_JSONShape_NilFieldsRenderAsNull(t *testing.T) {
	resp := SimulateTransactionResp{
		Context: SimulateTransactionRespContext{ApiVersion: "x", Slot: 1},
		Value:   SimulateTransactionRespValue{},
	}

	raw, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &decoded))
	value := decoded["value"].(map[string]interface{})

	for _, key := range []string{
		"err", "logs", "accounts", "unitsConsumed", "returnData",
		"innerInstructions", "replacementBlockhash", "loadedAccountsDataSize",
		"fee", "preBalances", "postBalances",
		"preTokenBalances", "postTokenBalances", "loadedAddresses",
	} {
		v, present := value[key]
		assert.True(t, present, "field %q must always be emitted", key)
		assert.Nil(t, v, "field %q must serialize as null when unset", key)
	}
}

func TestSimulateTransactionResp_JSONShape_LoadedAddressesNested(t *testing.T) {
	resp := SimulateTransactionResp{
		Value: SimulateTransactionRespValue{
			LoadedAddresses: &LoadedAddressesPayload{
				Readonly: []string{"r1", "r2"},
				Writable: []string{"w1"},
			},
		},
	}

	raw, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &decoded))
	la := decoded["value"].(map[string]interface{})["loadedAddresses"].(map[string]interface{})

	assert.ElementsMatch(t, []interface{}{"r1", "r2"}, la["readonly"])
	assert.ElementsMatch(t, []interface{}{"w1"}, la["writable"])
}

func TestSimulateTransactionResp_JSONShape_FeeAsTopLevelUint64(t *testing.T) {
	fee := uint64(12345)
	resp := SimulateTransactionResp{
		Value: SimulateTransactionRespValue{Fee: &fee},
	}
	raw, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"fee":12345`)
}

// When the simulator runs an InstructionError, the response must carry
// the structured Agave shape ({"InstructionError":[idx,…]}), not a Go-
// stringified version. Pass-through of *replay.TransactionError into the
// Err field must hit MarshalJSON, not Error().
func TestSimulateTransactionResp_JSONShape_InstructionErrorAgaveShape(t *testing.T) {
	idx := uint8(2)
	resp := SimulateTransactionResp{
		Value: SimulateTransactionRespValue{
			Err: &replayTransactionErrorForTest{
				ErrorTypeName:    "InstructionError",
				InstructionIndex: idx,
				InnerJSON:        `{"Custom":42}`,
			},
		},
	}
	raw, err := json.Marshal(resp)
	require.NoError(t, err)
	got := string(raw)
	assert.Contains(t, got, `"err":{"InstructionError":[2,{"Custom":42}]}`)
}

// replayTransactionErrorForTest is a minimal stand-in that mimics the
// replay package's MarshalJSON contract without importing it (avoids a
// circular path here in the rpcserver test). The real handler passes
// the actual *replay.TransactionError through, which has the same
// MarshalJSON behavior — verified separately in pkg/replay tests.
type replayTransactionErrorForTest struct {
	ErrorTypeName    string
	InstructionIndex uint8
	InnerJSON        string
}

func (e *replayTransactionErrorForTest) MarshalJSON() ([]byte, error) {
	return []byte(`{"` + e.ErrorTypeName + `":[` +
		strconvU8(e.InstructionIndex) + `,` + e.InnerJSON + `]}`), nil
}

func strconvU8(n uint8) string {
	if n == 0 {
		return "0"
	}
	out := []byte{}
	for n > 0 {
		out = append([]byte{byte('0' + n%10)}, out...)
		n /= 10
	}
	return string(out)
}

// Agave SignatureFailure fixture (rpc.rs:6147-6170) emits this exact shape.
// Mithril's response on the equivalent path must match — strict-typed
// clients depend on every field being present.
func TestSimulateTransactionResp_JSONShape_SignatureFailureMatchesAgave(t *testing.T) {
	zeroUnits := uint64(0)
	zeroSize := uint32(0)
	logs := []string{}
	resp := SimulateTransactionResp{
		Value: SimulateTransactionRespValue{
			Err:                    "SignatureFailure",
			Logs:                   &logs,
			UnitsConsumed:          &zeroUnits,
			LoadedAccountsDataSize: &zeroSize,
			LoadedAddresses:        &LoadedAddressesPayload{Readonly: []string{}, Writable: []string{}},
		},
	}

	raw, err := json.Marshal(resp)
	require.NoError(t, err)

	got := string(raw)
	for _, want := range []string{
		`"err":"SignatureFailure"`,
		`"logs":[]`,
		`"unitsConsumed":0`,
		`"loadedAccountsDataSize":0`,
		`"loadedAddresses":{"readonly":[],"writable":[]}`,
		`"fee":null`,
		`"preBalances":null`,
		`"postBalances":null`,
		`"preTokenBalances":null`,
		`"postTokenBalances":null`,
	} {
		assert.Contains(t, got, want, "Agave-shape SignatureFailure response missing %q", want)
	}
}
