package blockprod

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestEntryBuilderBatchSizingMatchesComponentEncoding(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int
		v0    bool
	}{
		{name: "legacy", count: 2},
		{name: "mixed-version", count: 2, v0: true},
		{name: "more-than-128-transactions", count: 129, v0: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txns := make([]solana.Transaction, tc.count)
			for i := range txns {
				tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(uint64(i)))
				require.NoError(t, err)
				if tc.v0 && i%2 != 0 {
					tx.Message.SetVersion(solana.MessageVersionV0)
				}
				txns[i] = *tx
			}
			component, err := turbine.NewEntryBatch([]turbine.Entry{{NumHashes: 1, Txns: txns}})
			require.NoError(t, err)
			encoded, err := turbine.MarshalBlockComponent(component)
			require.NoError(t, err)
			limits := costmodel.DefaultLimits()
			limits.MaxBatchBytes = uint64(len(encoded))
			builder := NewEntryBuilder(limits, solana.Hash{1})

			checkBatch := func(entries []turbine.Entry, batchBytes int, want []solana.Transaction) {
				t.Helper()
				require.Len(t, entries, 1)
				require.Equal(t, want, entries[0].Txns)
				component, err := turbine.NewEntryBatch(entries)
				require.NoError(t, err)
				encoded, err := turbine.MarshalBlockComponent(component)
				require.NoError(t, err)
				require.Equal(t, len(encoded), batchBytes)
			}

			// Repeat after both an automatic and explicit flush to catch stale
			// size accounting. The limit fits the complete batch exactly.
			for round := 0; round < 2; round++ {
				pendingWire := 0
				for i := range txns {
					wire, err := txns[i].MarshalBinary()
					require.NoError(t, err)
					wireHint := len(wire)
					switch i % 3 {
					case 0:
						wireHint += 100 // Transport bytes must not affect batch sizing.
					case 1:
						wireHint = 0 // An absent hint must use the serialized length.
					}
					if wireHint > 0 {
						pendingWire += wireHint
					} else {
						pendingWire += len(wire)
					}
					entries, _, flushed := builder.Append(txns[i], wireHint)
					require.False(t, flushed, "transaction %d", i)
					require.Empty(t, entries)
				}
				require.Equal(t, tc.count, builder.PendingCount())
				require.Equal(t, pendingWire, builder.PendingWireBytes())
				entries, batchBytes, flushed := builder.Append(txns[0], 0)
				require.True(t, flushed)
				checkBatch(entries, batchBytes, txns)
				require.Equal(t, int(limits.MaxBatchBytes), batchBytes)
				require.Equal(t, 1, builder.PendingCount())
				entries, batchBytes = builder.Flush()
				checkBatch(entries, batchBytes, txns[:1])
				require.Zero(t, builder.PendingCount())
				require.Zero(t, builder.PendingWireBytes())
			}
		})
	}
}

func TestEntryBuilderAllowsSingleTransactionOverBatchTarget(t *testing.T) {
	wire := txfixture.MustSignedTransferWire(0)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	limits := costmodel.DefaultLimits()
	limits.MaxBatchBytes = 1
	builder := NewEntryBuilder(limits, solana.Hash{})
	entries, _, flushed := builder.Append(*tx, len(wire))
	require.False(t, flushed)
	require.Empty(t, entries)
	entries, _, flushed = builder.Append(*tx, len(wire))
	require.True(t, flushed)
	require.Len(t, entries, 1)
	require.Len(t, entries[0].Txns, 1)
	require.Equal(t, 1, builder.PendingCount())
}

func TestEntryBuilderHoldsUntilBatchFull(t *testing.T) {
	builder := NewEntryBuilder(costmodel.DefaultLimits(), solana.Hash{1})
	limit := int(costmodel.DefaultTargetBatchBytes)

	var flushedBytes int
	var n int
	for n = 0; n < 2000; n++ {
		tx := mustTransferTx(t, uint64(n))
		wire, err := tx.MarshalBinary()
		require.NoError(t, err)
		entries, batchBytes, didFlush := builder.Append(*tx, len(wire))
		if !didFlush {
			require.Nil(t, entries)
			continue
		}
		require.Len(t, entries, 1)
		require.NotEmpty(t, entries[0].Txns)
		flushedBytes = batchBytes
		break
	}
	require.Less(t, n, 2000, "should flush once the next tx would overflow the batch target")
	require.Greater(t, flushedBytes, limit/2)
	require.LessOrEqual(t, flushedBytes, limit)
	require.Greater(t, builder.PendingCount(), 0)
}

func TestEntryBuilderReservesAndRebatesSlotBytes(t *testing.T) {
	builder := NewEntryBuilder(costmodel.DefaultLimits(), solana.Hash{1})
	tx := mustTransferTx(t, 0)
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)

	require.True(t, builder.reserve(len(wire)))
	require.Equal(t, entryBatchOverheadBytes+len(wire), builder.SlotBytes())
	require.Equal(t, builder.SlotBytes(), builder.ReservedBytes())

	builder.rebateReserved(len(wire))
	require.Zero(t, builder.SlotBytes())
	require.Zero(t, builder.ReservedBytes())
}

func TestEntryBuilderHoldsShortBatchUntilFlush(t *testing.T) {
	builder := NewEntryBuilder(costmodel.DefaultLimits(), solana.Hash{1})
	for i := 0; i < 8; i++ {
		tx := mustTransferTx(t, uint64(i))
		wire, err := tx.MarshalBinary()
		require.NoError(t, err)
		entries, _, didFlush := builder.Append(*tx, len(wire))
		require.False(t, didFlush)
		require.Nil(t, entries)
	}
	require.Equal(t, 8, builder.PendingCount())

	entries, batchBytes := builder.Flush()
	require.Len(t, entries, 1)
	require.Len(t, entries[0].Txns, 8)
	require.Greater(t, batchBytes, 0)
	require.Less(t, batchBytes, int(costmodel.TypicalFECSetPayloadBytes))
	require.Zero(t, builder.PendingCount())
}

func mustTransferTx(t *testing.T, seq uint64) *solana.Transaction {
	t.Helper()
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(seq))
	require.NoError(t, err)
	return tx
}
