package rpcserver

import (
	"encoding/json"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClampLogs_UnderLimitReturnsUnchanged(t *testing.T) {
	in := []string{"hello", "world"}
	got := clampLogs(in)
	assert.Equal(t, in, got)
}

func TestClampLogs_AtBoundaryAppendsTruncatedMarker(t *testing.T) {
	big := make([]byte, maxSimulateLogBytes/2+1)
	for i := range big {
		big[i] = 'a'
	}
	in := []string{string(big), string(big), "this should not appear"}
	got := clampLogs(in)
	assert.Equal(t, "Log truncated", got[len(got)-1])
	assert.NotContains(t, got, "this should not appear")
}

func TestClampLogs_NilStaysNil(t *testing.T) {
	assert.Nil(t, clampLogs(nil))
}

func TestClampReturnData_UnderLimitReturnsUnchanged(t *testing.T) {
	in := []byte{1, 2, 3}
	got := clampReturnData(in)
	assert.Equal(t, in, got)
}

func TestClampReturnData_OverLimitTruncated(t *testing.T) {
	in := make([]byte, maxSimulateReturnDataBytes+500)
	for i := range in {
		in[i] = byte(i % 256)
	}
	got := clampReturnData(in)
	assert.Len(t, got, maxSimulateReturnDataBytes)
	assert.Equal(t, in[:maxSimulateReturnDataBytes], got)
}

// renderInnerInstructions must encode `data` as base58 (matching Agave's
// UiCompiledInstruction.from), include `stackHeight`, and preserve the
// programIdIndex / accounts fields as numbers.
func TestRenderInnerInstructions_AgaveWireShape(t *testing.T) {
	lists := []replay.InnerInstructionsList{
		{Index: 0, Instructions: []replay.CompiledInstruction{
			{ProgramIdIndex: 4, Accounts: []uint8{1, 2}, Data: []byte{0x01, 0x02, 0x03}, StackHeight: 2},
		}},
	}
	out := renderInnerInstructions(lists)
	require.Len(t, out, 1)
	assert.Equal(t, uint8(0), out[0].Index)

	require.Len(t, out[0].Instructions, 1)
	got := out[0].Instructions[0]
	assert.Equal(t, uint8(4), got.ProgramIdIndex)
	assert.Equal(t, []uint16{1, 2}, got.Accounts)
	assert.Equal(t, "Ldp", got.Data, "data must be base58-encoded, not base64")
	require.NotNil(t, got.StackHeight)
	assert.Equal(t, uint32(2), *got.StackHeight, "stackHeight must serialize as Option<u32> per Agave wire format")

	// JSON shape sanity: round-trip and confirm keys.
	raw, err := json.Marshal(out)
	require.NoError(t, err)
	for _, want := range []string{`"index":0`, `"programIdIndex":4`, `"accounts":[1,2]`, `"data":"Ldp"`, `"stackHeight":2`} {
		assert.Contains(t, string(raw), want)
	}
}

func TestRenderInnerInstructions_EmptyReturnsEmptyArray(t *testing.T) {
	out := renderInnerInstructions(nil)
	assert.NotNil(t, out, "must be [], not nil, so JSON marshals as [] not null")
	assert.Len(t, out, 0)
}

func TestRenderInnerInstructionsAccountIndices(t *testing.T) {
	for _, indices := range [][]uint8{nil, {}, {0, 255}} {
		out := renderInnerInstructions([]replay.InnerInstructionsList{{Instructions: []replay.CompiledInstruction{{Accounts: indices}}}})
		raw, err := json.Marshal(out[0].Instructions[0])
		require.NoError(t, err)
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &fields))
		if len(indices) == 0 {
			require.JSONEq(t, `[]`, string(fields["accounts"]))
		} else {
			require.JSONEq(t, `[0,255]`, string(fields["accounts"]))
		}
	}
}

func TestParseSimulateConfig_MinContextSlot(t *testing.T) {
	params := []json.RawMessage{
		json.RawMessage(`"unused-tx-string"`),
		json.RawMessage(`{"minContextSlot":417500000}`),
	}
	conf, err := parseSimulateConfig(params)
	require.NoError(t, err)
	if assert.NotNil(t, conf.minContextSlot) {
		assert.Equal(t, uint64(417500000), *conf.minContextSlot)
	}
}

func TestParseSimulateConfig_Commitment(t *testing.T) {
	params := []json.RawMessage{
		json.RawMessage(`"unused-tx-string"`),
		json.RawMessage(`{"commitment":"confirmed"}`),
	}
	conf, err := parseSimulateConfig(params)
	require.NoError(t, err)
	assert.Equal(t, "confirmed", conf.commitment)
}

func TestParseSimulateConfig_DefaultsOmitOptional(t *testing.T) {
	conf, err := parseSimulateConfig([]json.RawMessage{json.RawMessage(`"unused"`), json.RawMessage(`{}`)})
	require.NoError(t, err)
	assert.Nil(t, conf.minContextSlot)
	assert.Empty(t, conf.commitment)
}

func TestPostBalancesFromExecCtx_Nil(t *testing.T) {
	assert.Nil(t, postBalancesFromExecCtx(nil))
}

func TestPostBalancesFromExecCtx_ReadsLamportsInOrder(t *testing.T) {
	ax := &accounts.Account{Lamports: 100}
	bx := &accounts.Account{Lamports: 200}
	cx := &accounts.Account{Lamports: 300}

	txAccts := sealevel.NewTransactionAccountsFromRefs(
		[]*accounts.Account{ax, bx, cx},
		[]bool{false, false, false},
	)
	execCtx := &sealevel.ExecutionCtx{
		TransactionContext: sealevel.NewTransactionCtx(*txAccts, 1, 1),
	}

	got := postBalancesFromExecCtx(execCtx)
	assert.Equal(t, []uint64{100, 200, 300}, got)
}

func TestLoadedAddressesFromTx_LegacyReturnsEmpty(t *testing.T) {
	tx := &solana.Transaction{
		Message: solana.Message{
			AccountKeys: []solana.PublicKey{{1}, {2}, {3}},
		},
	}
	got := loadedAddressesFromTx(tx)
	assert.NotNil(t, got)
	assert.Equal(t, []string{}, got.Writable)
	assert.Equal(t, []string{}, got.Readonly)
}

func TestLoadedAddressesFromTx_VersionedSplitsWritableThenReadonly(t *testing.T) {
	staticA := solana.PublicKey{0xA1}
	staticB := solana.PublicKey{0xA2}
	wA := solana.PublicKey{0xB1}
	wB := solana.PublicKey{0xB2}
	rA := solana.PublicKey{0xC1}
	rB := solana.PublicKey{0xC2}

	msg := solana.Message{
		AccountKeys: []solana.PublicKey{staticA, staticB, wA, wB, rA, rB},
		AddressTableLookups: solana.MessageAddressTableLookupSlice{
			{
				AccountKey:      solana.PublicKey{0xFF, 1},
				WritableIndexes: []uint8{0, 1},
				ReadonlyIndexes: []uint8{0, 1},
			},
		},
	}
	msg.SetVersion(solana.MessageVersionV0)

	got := loadedAddressesFromTx(&solana.Transaction{Message: msg})
	assert.Equal(t, []string{wA.String(), wB.String()}, got.Writable)
	assert.Equal(t, []string{rA.String(), rB.String()}, got.Readonly)
}

func TestLoadedAddressesFromTx_VersionedNoLookupsReturnsEmpty(t *testing.T) {
	msg := solana.Message{
		AccountKeys: []solana.PublicKey{{1}, {2}},
	}
	msg.SetVersion(solana.MessageVersionV0)

	got := loadedAddressesFromTx(&solana.Transaction{Message: msg})
	assert.Empty(t, got.Writable)
	assert.Empty(t, got.Readonly)
}
