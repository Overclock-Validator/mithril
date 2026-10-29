package turbine

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// A block decoded at completion (not by the early batch decode) is summarized
// for the prefetcher before it is returned.
func TestDecodeBlockSummarizesForPrefetch(t *testing.T) {
	leader := testShredLeader(t)
	gen := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7, ReferenceTick: 63}
	var (
		chainedRoot      = solana.Hash{5}
		nextData, nextCo uint32
	)
	state := &slotState{slot: 100, parentSlot: 99, shredVer: 7, shreds: map[uint32]*Shred{}}
	for _, component := range buildAlpenglowSlot(t) {
		packets, root, newData, newCode, err := gen.MakeShredsFromData(leader, component.payload, component.isLastInSlot, chainedRoot, nextData, nextCo)
		require.NoError(t, err)
		chainedRoot, nextData, nextCo = root, newData, newCode
		for _, packet := range packets {
			shred, err := ParseShred(packet)
			require.NoError(t, err)
			if shred.Type == ShredTypeData {
				state.shreds[shred.Index] = shred
			}
		}
	}
	defer SetStreamRefsHook(nil)
	var gotSlot uint64
	var got *StreamAccountRefs
	SetStreamRefsHook(func(slot uint64, refs *StreamAccountRefs) { gotSlot, got = slot, refs })
	blk, _, _, err := state.decodeBlock(nil)
	require.NoError(t, err)
	require.NotEmpty(t, blk.Transactions)
	require.NotNil(t, got, "the completion decode summarizes the block")
	assert.Equal(t, uint64(100), gotSlot)
	assert.Same(t, got, blk.AccountPrefetchRefs())
	for _, tx := range blk.Transactions {
		for _, k := range tx.Message.AccountKeys {
			assert.Contains(t, got.Keys, k)
		}
	}
}
