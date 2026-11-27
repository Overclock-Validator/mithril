package transactionhistory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadBudgetCharge(t *testing.T) {
	ctx := context.Background()
	budget := NewReadBudget()
	require.Equal(t, uint64(4096), budget.ops)
	require.Equal(t, uint64(16<<20), budget.bytes)
	require.NoError(t, budget.Charge(ctx, budget.ops, budget.bytes))
	require.NoError(t, budget.Charge(ctx, 0, 0))
	require.ErrorIs(t, budget.Charge(ctx, 1, 0), ErrLimit)
	require.ErrorIs(t, budget.Charge(ctx, 0, 0), ErrLimit)

	budget = &ReadBudget{ops: 1, bytes: 1}
	require.ErrorIs(t, budget.Charge(ctx, 0, 2), ErrLimit)
	require.Equal(t, uint64(1), budget.ops)
	require.Equal(t, uint64(1), budget.bytes)
	require.ErrorIs(t, budget.Charge(ctx, 0, 0), ErrLimit)

	var unbounded *ReadBudget
	require.NoError(t, unbounded.Charge(ctx, ^uint64(0), ^uint64(0)))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, unbounded.Charge(canceled, 0, 0), context.Canceled)
	budget = NewReadBudget()
	require.ErrorIs(t, budget.Charge(canceled, 1, 1), context.Canceled)
	require.Equal(t, NewReadBudget(), budget)
}

func TestHistoryReadsShareBudget(t *testing.T) {
	store := consumerStore(t)
	block := consumerBlock(t, 42, 1)
	require.NoError(t, store.WriteBlock(block))
	require.NoError(t, store.WriteBlock(consumerBlock(t, 42, 2)))
	view, err := store.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	ctx := context.Background()
	signature := block.Records[0].FirstSignature
	budget := NewReadBudget()
	candidates, err := view.SignatureCandidates(ctx, signature, budget)
	require.NoError(t, err)
	require.Len(t, candidates, 2)
	require.NotEqual(t, candidates[0].Ref.Block, candidates[1].Ref.Block)
	require.Less(t, budget.ops, NewReadBudget().ops)
	discoveryBytes := NewReadBudget().bytes - budget.bytes
	require.Positive(t, discoveryBytes)
	remaining := budget.bytes
	record, err := view.TransactionWithBudget(ctx, candidates[0].Ref, budget)
	require.NoError(t, err)
	require.Equal(t, block.Records[0], record)
	require.Less(t, budget.bytes, remaining)
	record.Metadata[0] ^= 255
	record, err = view.TransactionWithBudget(ctx, candidates[0].Ref, nil)
	require.NoError(t, err)
	require.Equal(t, block.Records[0], record)

	// Discovery and the selected body consume one allowance, not two budgets.
	budget = &ReadBudget{ops: 4096, bytes: discoveryBytes}
	candidates, err = view.SignatureCandidates(ctx, signature, budget)
	require.NoError(t, err)
	require.Len(t, candidates, 2)
	require.Zero(t, budget.bytes)
	record, err = view.TransactionWithBudget(ctx, candidates[0].Ref, budget)
	require.ErrorIs(t, err, ErrLimit)
	require.Empty(t, record)
	require.ErrorIs(t, budget.Charge(ctx, 0, 0), ErrLimit)

	budget = &ReadBudget{ops: 1, bytes: 16 << 20}
	candidates, err = view.SignatureCandidates(ctx, signature, budget)
	require.ErrorIs(t, err, ErrLimit)
	require.Nil(t, candidates)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	candidates, err = view.SignatureCandidates(canceled, signature, NewReadBudget())
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, candidates)
	ref := Ref{Block: block.ID, FirstSignature: signature}
	record, err = view.TransactionWithBudget(canceled, ref, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, record)
	require.NoError(t, view.Close())
	_, err = view.SignatureCandidates(ctx, signature, NewReadBudget())
	require.ErrorIs(t, err, ErrClosed)
	_, err = view.TransactionWithBudget(ctx, ref, NewReadBudget())
	require.ErrorIs(t, err, ErrClosed)
}
