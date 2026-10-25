package turbine

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
)

func refsTestKey(b byte) solana.PublicKey {
	var k solana.PublicKey
	for i := range k {
		k[i] = b
	}
	return k
}

func TestSummarizeAccountRefs(t *testing.T) {
	payer, a, prog, table := refsTestKey(1), refsTestKey(2), refsTestKey(3), refsTestKey(4)
	legacy := &solana.Transaction{Message: solana.Message{
		AccountKeys:  []solana.PublicKey{payer, a, prog},
		Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 2}, {ProgramIDIndex: 9}},
	}}
	v0 := &solana.Transaction{Message: solana.Message{
		AccountKeys:         []solana.PublicKey{payer, prog},
		Instructions:        []solana.CompiledInstruction{{ProgramIDIndex: 1}},
		AddressTableLookups: []solana.MessageAddressTableLookup{{AccountKey: table, WritableIndexes: []uint8{5}, ReadonlyIndexes: []uint8{7, 8}}},
	}}
	v0.Message.SetVersion(solana.MessageVersionV0)

	refs := SummarizeAccountRefs([]*solana.Transaction{legacy, nil, v0})
	assert.Equal(t, []solana.PublicKey{payer, a, prog, payer, prog}, refs.Keys, "every static key, in order (copied, not deduplicated)")
	assert.Equal(t, []solana.PublicKey{prog, prog}, refs.Programs, "top-level programs; an out-of-range index is skipped")
	assert.Equal(t, []solana.PublicKey{table}, refs.Tables)
	assert.Equal(t, []StreamLookup{{Table: 0, Indexes: []uint8{5, 7, 8}}}, refs.Lookups)

	// The summary is a copy: later changes to the messages do not reach it.
	legacy.Message.AccountKeys[1] = refsTestKey(9)
	v0.Message.AddressTableLookups[0].WritableIndexes[0] = 99
	assert.Equal(t, a, refs.Keys[1])
	assert.Equal(t, uint8(5), refs.Lookups[0].Indexes[0])
}

func TestSetStreamRefsHook(t *testing.T) {
	defer SetStreamRefsHook(nil)
	var got uint64
	SetStreamRefsHook(func(slot uint64, refs *StreamAccountRefs) { got = slot })
	(*streamRefsHook.Load())(42, &StreamAccountRefs{})
	assert.Equal(t, uint64(42), got)
	SetStreamRefsHook(nil)
	assert.Nil(t, streamRefsHook.Load())
}
