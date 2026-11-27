package rpcindex

import (
	"context"
	"encoding/json"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rpcdb"
	"github.com/Overclock-Validator/mithril/pkg/watch"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func testTx(t *testing.T, payer, to solana.PublicKey, sigByte byte) *solana.Transaction {
	t.Helper()
	ix := solana.NewInstruction(solana.SystemProgramID, solana.AccountMetaSlice{
		solana.Meta(payer).WRITE().SIGNER(), solana.Meta(to).WRITE(),
	}, []byte{2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0})
	tx, err := solana.NewTransaction([]solana.Instruction{ix}, solana.Hash{9}, solana.TransactionPayer(payer))
	require.NoError(t, err)
	tx.Signatures = []solana.Signature{{sigByte}}
	return tx
}

func newTestIndexer(t *testing.T, wallet solana.PublicKey) (*Indexer, context.CancelFunc) {
	t.Helper()
	store, err := rpcdb.Open(t.TempDir(), solana.Hash{1})
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	reg := watch.NewRegistry()
	reg.AddKeys(watch.Key{Key: wallet, Kind: watch.Wallet})
	ix := New(store, reg)
	ctx, cancel := context.WithCancel(context.Background())
	go ix.Run(ctx)
	t.Cleanup(cancel)
	return ix, cancel
}

func captured(tx *solana.Transaction) *Captured {
	return &Captured{
		Signature: tx.Signatures[0], Keys: tx.Message.AccountKeys, Fee: 5000, ComputeUnits: 150,
		PreBalances: []uint64{10, 0, 1}, PostBalances: []uint64{4, 1, 1}, Logs: []string{"Program 11111111111111111111111111111111 invoke [1]"},
	}
}

func TestIndexerPersistsOnlyFinalizedBlocks(t *testing.T) {
	wallet, other := solana.PublicKey{1}, solana.PublicKey{2}
	ix, _ := newTestIndexer(t, wallet)

	tx10 := testTx(t, wallet, other, 10)
	tx11 := testTx(t, other, wallet, 11)
	tx12 := testTx(t, wallet, other, 12)
	ix.CompletedBlock(10, solana.Hash{10}, &b.Block{Slot: 10, UnixTimestamp: 1000, Transactions: []*solana.Transaction{tx10}}, []*Captured{captured(tx10)}, nil)
	ix.CompletedBlock(11, solana.Hash{11}, &b.Block{Slot: 11, UnixTimestamp: 1001, Transactions: []*solana.Transaction{testTx(t, other, other, 99), tx11}}, []*Captured{captured(tx11)}, nil)
	ix.CompletedBlock(12, solana.Hash{12}, &b.Block{Slot: 12, Transactions: []*solana.Transaction{tx12}}, []*Captured{captured(tx12)}, nil)
	ix.Sync()

	pending := ix.Pending(wallet, 12)
	require.Len(t, pending, 3)
	require.EqualValues(t, 12, pending[0].Slot)
	require.EqualValues(t, 1, pending[1].Position, "position comes from the block")

	// A fork switch abandons slot 12 before it is finalized.
	ix.Abandon(12)
	ix.Finalized(11)
	ix.Sync()
	require.NoError(t, ix.Err())

	entries, err := ix.Store().SignaturesForAddress(wallet, nil, nil, 10)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, tx11.Signatures[0], entries[0].Signature)
	require.Empty(t, ix.Pending(wallet, 100), "finalized and abandoned blocks leave the pending set")
	wm, err := ix.Store().Watermark()
	require.NoError(t, err)
	require.EqualValues(t, 11, wm)

	rec, ok, err := ix.Store().Transaction(tx10.Signatures[0])
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 1000, rec.BlockTime)
	wire, err := tx10.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, wire, rec.Wire)
	var meta Meta
	require.NoError(t, json.Unmarshal(rec.Meta, &meta))
	require.EqualValues(t, 5000, meta.Fee)
	require.Equal(t, "null", string(meta.Err))
	require.Contains(t, meta.Status, "Ok")
	require.Equal(t, []uint64{4, 1, 1}, meta.PostBalances)

	_, ok, err = ix.Store().Transaction(tx12.Signatures[0])
	require.NoError(t, err)
	require.False(t, ok, "abandoned transaction must not be persisted")
}

func TestEncodeMetaErrorsTokensAndLoadedAddresses(t *testing.T) {
	c := &Captured{
		ErrJSON:   []byte(`{"InstructionError":[0,{"Custom":1}]}`),
		Err:       errTest,
		Keys:      []solana.PublicKey{{1}, {2}, {3}, {4}},
		PreTokens: []TokenBalance{{AccountIndex: 1, Mint: solana.PublicKey{5}, Owner: solana.PublicKey{1}, ProgramID: SPLTokenProgram, Amount: 1_500_000, Decimals: 6}},
		Inner:     []InnerInstructions{{Index: 0, Instructions: []InnerInstruction{{ProgramIDIndex: 2, Accounts: []uint8{0, 1}, Data: []byte{1, 2}, StackHeight: 2}}}},
	}
	original := &solana.Transaction{Message: solana.Message{
		AccountKeys:         solana.PublicKeySlice{{1}, {2}},
		AddressTableLookups: solana.MessageAddressTableLookupSlice{{WritableIndexes: []uint8{0}, ReadonlyIndexes: []uint8{1}}},
	}}
	var meta map[string]any
	require.NoError(t, json.Unmarshal(EncodeMeta(c, original), &meta))
	require.Equal(t, map[string]any{"InstructionError": []any{0.0, map[string]any{"Custom": 1.0}}}, meta["err"])
	require.Contains(t, meta["status"], "Err")
	loaded := meta["loadedAddresses"].(map[string]any)
	require.Equal(t, []any{solana.PublicKey{3}.String()}, loaded["writable"])
	require.Equal(t, []any{solana.PublicKey{4}.String()}, loaded["readonly"])
	tb := meta["preTokenBalances"].([]any)[0].(map[string]any)["uiTokenAmount"].(map[string]any)
	require.Equal(t, "1500000", tb["amount"])
	require.Equal(t, "1.5", tb["uiAmountString"])
	inner := meta["innerInstructions"].([]any)[0].(map[string]any)["instructions"].([]any)[0].(map[string]any)
	require.Equal(t, []any{0.0, 1.0}, inner["accounts"], "accounts must be numbers, not base64")
}

type testErr struct{}

func (testErr) Error() string { return "custom" }

var errTest = testErr{}

func TestUIAmountString(t *testing.T) {
	for _, tc := range []struct {
		amount   uint64
		decimals uint8
		want     string
	}{{0, 0, "0"}, {0, 6, "0"}, {5, 2, "0.05"}, {1_500_000, 6, "1.5"}, {100, 2, "1"}, {123456789, 9, "0.123456789"}, {7, 0, "7"}} {
		require.Equal(t, tc.want, UIAmountString(tc.amount, tc.decimals), "%d with %d decimals", tc.amount, tc.decimals)
	}
}

func TestTokenAccountReassignmentAcrossBatches(t *testing.T) {
	alice, bob, mint, acct := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{5}, solana.PublicKey{6}
	ix, _ := newTestIndexer(t, alice)
	ix.CompletedBlock(10, solana.Hash{}, &b.Block{Slot: 10}, nil, []TokenAccountChange{{Owner: alice, Mint: mint, Account: acct, Program: SPLTokenProgram}})
	ix.Finalized(10)
	ix.CompletedBlock(11, solana.Hash{}, &b.Block{Slot: 11}, nil, []TokenAccountChange{{Owner: bob, Mint: mint, Account: acct, Program: SPLTokenProgram}})
	ix.Finalized(11)
	ix.Sync()
	require.NoError(t, ix.Err())
	rows, err := ix.Store().TokenAccounts(alice)
	require.NoError(t, err)
	require.Empty(t, rows, "the previous owner's row must be removed")
	rows, err = ix.Store().TokenAccounts(bob)
	require.NoError(t, err)
	require.Len(t, rows, 1)
}
