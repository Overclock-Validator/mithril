package replay

import (
	"slices"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func capturedHistoryBlock(t *testing.T) *blockMetadata {
	t.Helper()
	env := newLifecycleEnv(t)
	txs := transferTransactions(t, 4, 999_000)
	block := env.block(txs)
	block.MarkTransactionSignaturesVerified()
	var capture blockMetadataCapture
	ctx, err := processBlock(env.acctsDb, block, env.epochSchedule, 4, nil,
		&persistedTracker{}, &lifecycleTail{durable: env.durable}, NewTransactionStatusCache(), false, env.parent, &capture)
	require.NoError(t, err)
	require.NoError(t, capture.Err)
	require.NotNil(t, capture.Record)
	require.Equal(t, ctx.FinalBankhash, capture.Record.Bankhash[:])
	require.Equal(t, block.LastBlockhash, capture.Record.PreviousBlockhash)
	return capture.Record
}

func TestBlockHistoryRecordRoundTrip(t *testing.T) {
	capture := capturedHistoryBlock(t)
	block, err := capture.historyBlock()
	require.NoError(t, err)
	store, err := transactionhistory.Open(t.TempDir(), [32]byte{1}, transactionhistory.DefaultLimits())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	require.NoError(t, store.WriteBlock(block))
	reader, err := store.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	retained, err := reader.Block(block.ID)
	require.NoError(t, err)
	require.Equal(t, block, retained)
	for i, record := range retained.Records {
		refs, err := reader.SignatureRefs(record.FirstSignature)
		require.NoError(t, err)
		require.Len(t, refs, 1)
		require.Equal(t, uint32(i), refs[0].Position)
		point, err := reader.Transaction(refs[0])
		require.NoError(t, err)
		require.Equal(t, record, point)
		metadata, err := transactionhistory.UnmarshalMetadata(record.Metadata)
		require.NoError(t, err)
		original := capture.Transactions[i]
		require.Equal(t, original.AccountKeys, metadata.AccountKeys)
		require.Equal(t, original.PreBalances, metadata.PreBalances)
		require.Equal(t, original.PostBalances, metadata.PostBalances)
		require.Equal(t, original.Fee, metadata.Fee)
		require.Equal(t, original.ComputeUnitsConsumed, metadata.ComputeUnitsConsumed)
		require.JSONEq(t, string(original.Error), string(metadata.Error))
		if i >= 2 {
			require.NotEqual(t, "null", string(metadata.Error), "included failures retain their status")
		}
		original.Transaction[0] ^= 1
		original.PostBalances[0]++
		point, err = reader.Transaction(refs[0])
		require.NoError(t, err)
		require.Equal(t, record, point, "stored bytes do not alias capture")
	}
}

func TestHistoryRecordRejectsIncompleteCapture(t *testing.T) {
	original := capturedHistoryBlock(t).Transactions[0]
	for _, mutate := range []func(*transactionMetadata){
		func(record *transactionMetadata) { record.preTokens.Mints = map[solana.PublicKey]tokenMintState{} },
		func(record *transactionMetadata) { record.AccountKeys = nil },
		func(record *transactionMetadata) {
			record.AccountKeys = slices.Clone(record.AccountKeys)
			record.AccountKeys[0][0] ^= 1
		},
		func(record *transactionMetadata) { record.PostBalances = nil },
		func(record *transactionMetadata) { record.Error = nil },
		func(record *transactionMetadata) { record.Transaction = append(slices.Clone(record.Transaction), 0) },
		func(record *transactionMetadata) { record.InnerInstructions = []InnerInstructionsList{{Index: 255}} },
	} {
		record := *original
		mutate(&record)
		encoded, err := record.historyRecord()
		require.Error(t, err)
		require.Equal(t, transactionhistory.Record{}, encoded)
	}
}

func TestHistoryTokenBalancesUseRetainedRawValues(t *testing.T) {
	balances := []transactionTokenBalance{{
		transactionTokenFact: transactionTokenFact{AccountIndex: 2, Mint: solana.PublicKey{3},
			Owner: solana.PublicKey{4}, ProgramID: solana.Token2022ProgramID, Amount: ^uint64(0)},
		Decimals: 255, HasExtensions: true,
	}}
	got := historyTokenBalances(balances)
	require.Equal(t, []transactionhistory.TokenBalance{{
		AccountIndex: 2, Mint: solana.PublicKey{3}, Owner: solana.PublicKey{4},
		ProgramID: solana.Token2022ProgramID, Amount: ^uint64(0), Decimals: 255,
	}}, got)
	balances[0].Amount = 0
	require.Equal(t, ^uint64(0), got[0].Amount)
}
