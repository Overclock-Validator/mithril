package blockprod

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestEntryBuilderHoldsUntilFECSetFull(t *testing.T) {
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
	require.Less(t, n, 2000, "should flush once the next tx would overflow one FEC set")
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
