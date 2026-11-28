package replay

import (
	"encoding/base64"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/history"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func TestFoldHistoryBankSignatures(t *testing.T) {
	chunk := []accounts.SlotDelta{{Slot: 10}, {Slot: 12}}
	sigs := map[uint64][]uint64{10: {1, 2}, 12: {}}
	b := foldHistoryBank(chunk, nil, sigs, 12, nil)
	require.Equal(t, []history.SlotSignatures{{Slot: 10, Prefixes: []uint64{1, 2}}, {Slot: 12, Prefixes: []uint64{}}}, b.Signatures)
	// A slot whose transactions are unknown leaves the whole fold unindexed.
	delete(sigs, 12)
	require.Nil(t, foldHistoryBank(chunk, nil, sigs, 12, nil).Signatures)
}

func TestFoldHistoryBankBase(t *testing.T) {
	chunk := []accounts.SlotDelta{{Slot: 10}, {Slot: 12}}
	var bankhash, blockhash, recent, evicted [32]byte
	bankhash[0], blockhash[0], recent[0], evicted[0] = 1, 2, 3, 4
	lt := make([]byte, 2048)
	lt[0] = 9
	root := &state.ResumeContext{Slot: 8, BlockHeight: 70, LamportsPerSignature: 5000, PrevLamportsPerSig: 5000, NumSignatures: 3,
		Bankhash: base58.Encode(bankhash[:]), Blockhash: base58.Encode(blockhash[:]), AcctsLtHash: base64.StdEncoding.EncodeToString(lt),
		EvictedBlockhash: base58.Encode(evicted[:]), RecentBlockhashes: []state.BlockhashEntry{{Blockhash: base58.Encode(recent[:]), LamportsPerSignature: 5000}}}
	b := foldHistoryBank(chunk, nil, nil, 12, root)
	require.NotNil(t, b.Base)
	require.Equal(t, history.SlotBank{Slot: 8, BlockHeight: 70, LamportsPerSignature: 5000, PrevLamportsPerSignature: 5000, NumSignatures: 3,
		Bankhash: bankhash, Blockhash: blockhash}, b.Base.Slot)
	require.Equal(t, history.ChunkBank{Slot: 8, AccountsLtHash: lt, EvictedBlockhash: evicted,
		RecentBlockhashes: []history.BlockhashFee{{Blockhash: recent, LamportsPerSignature: 5000}}}, b.Base.Chunk)
	// A root at or after the chunk, or one that does not decode, gives none.
	root.Slot = 10
	require.Nil(t, foldHistoryBank(chunk, nil, nil, 12, root).Base)
	root.Slot, root.AcctsLtHash = 8, ""
	require.Nil(t, foldHistoryBank(chunk, nil, nil, 12, root).Base)
}
