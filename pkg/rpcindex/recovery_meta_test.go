package rpcindex

import (
	"encoding/json"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestResolvedBlockRetainsLoadedAddresses(t *testing.T) {
	table := solana.PublicKey{9}
	tx := &solana.Transaction{Message: solana.Message{
		AccountKeys:         solana.PublicKeySlice{{1}, {2}},
		AddressTableLookups: solana.MessageAddressTableLookupSlice{{AccountKey: table, WritableIndexes: []uint8{0}, ReadonlyIndexes: []uint8{1}}},
	}}
	tx.Message.SetVersion(solana.MessageVersionV0)
	require.NoError(t, tx.Message.SetAddressTables(map[solana.PublicKey]solana.PublicKeySlice{table: {{3}, {4}}}))
	require.NoError(t, tx.Message.ResolveLookups())
	require.Len(t, tx.Message.AccountKeys, 4)
	var meta Meta
	require.NoError(t, json.Unmarshal(EncodeMeta(&Captured{Keys: tx.Message.AccountKeys}, tx), &meta))
	require.Equal(t, []string{solana.PublicKey{3}.String()}, meta.LoadedAddresses.Writable)
	require.Equal(t, []string{solana.PublicKey{4}.String()}, meta.LoadedAddresses.Readonly)
}

// A replay run restarted from a checkpoint after recovery must not finalize
// blocks the previous attempt completed on another fork, even when the
// replacement banks have no watched activity.
func TestReplayRestartDoesNotFinalizeOldFork(t *testing.T) {
	wallet, other := solana.PublicKey{1}, solana.PublicKey{2}
	ix, _ := newTestIndexer(t, wallet)
	old := testTx(t, wallet, other, 10)
	ix.CompletedBlock(10, solana.Hash{10}, &b.Block{Slot: 10, Transactions: []*solana.Transaction{old}}, []*Captured{captured(old)}, nil)
	ix.Sync()
	// Restart from rooted bank 9, then select replacement banks 10 and 11.
	// Neither replacement has a watched transaction/token change. Replay now
	// marks the restart boundary.
	ix.BeginReplay(9)
	ix.Finalized(11)
	ix.Sync()
	require.NoError(t, ix.Err())
	_, ok, err := ix.Store().Transaction(old.Signatures[0])
	require.NoError(t, err)
	require.False(t, ok, "orphan from previous replay attempt must not reach finalized history")
}
