package sealevel

import (
	"fmt"
	"math"
	"testing"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newComputeBudgetV1TestTransaction(t *testing.T, config solana.TransactionConfig) *solana.Transaction {
	t.Helper()
	msg := solana.Message{
		Header:            solana.MessageHeader{NumRequiredSignatures: 1},
		AccountKeys:       []solana.PublicKey{{1}},
		TransactionConfig: config,
	}
	_, err := msg.SetVersion(solana.MessageVersionV1)
	require.NoError(t, err)
	return &solana.Transaction{Message: msg, Signatures: []solana.Signature{{}}}
}

func TestComputeBudgetLimitsForTransactionV1Defaults(t *testing.T) {
	tx := newComputeBudgetV1TestTransaction(t, solana.TransactionConfig{})

	limits, err := ComputeBudgetLimitsForTransaction(tx, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(MinHeapFrameBytes), limits.UpdatedHeapBytes)
	assert.Equal(t, uint32(0), limits.ComputeUnitLimit)
	assert.Equal(t, uint32(0), limits.LoadedAccountBytes)
	assert.Equal(t, uint64(0), limits.ComputeUnitPrice)
	assert.Equal(t, uint64(0), limits.DirectPriorityFeeLamports)
	assert.True(t, limits.UsesDirectPriorityFee)
}

func TestComputeBudgetLimitsForTransactionV1UsesInlineConfig(t *testing.T) {
	config := solana.TransactionConfig{}.
		WithPriorityFee(math.MaxUint64).
		WithComputeUnitLimit(MaxComputeUnitLimit + 1).
		WithLoadedAccountsDataSizeLimit(MaxLoadedAccountsDataSizeBytes + 1).
		WithHeapSize(64 * 1024)
	tx := newComputeBudgetV1TestTransaction(t, config)

	// A malformed ComputeBudget instruction would reject a legacy/v0
	// transaction. In V1 it is deliberately ignored for configuration and is
	// executed later as an ordinary successful no-op.
	instructions := []Instruction{{ProgramId: a.ComputeBudgetProgramAddr, Data: []byte{0xff}}}
	limits, err := ComputeBudgetLimitsForTransaction(tx, instructions, nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(64*1024), limits.UpdatedHeapBytes)
	assert.Equal(t, uint32(MaxComputeUnitLimit), limits.ComputeUnitLimit)
	assert.Equal(t, uint32(MaxLoadedAccountsDataSizeBytes), limits.LoadedAccountBytes)
	assert.Equal(t, uint64(math.MaxUint64), limits.DirectPriorityFeeLamports)
	assert.True(t, limits.UsesDirectPriorityFee)
}

func TestComputeBudgetLimitsForTransactionV1RejectsInvalidHeap(t *testing.T) {
	for _, heapSize := range []uint32{MinHeapFrameBytes - 1024, MinHeapFrameBytes + 1, MaxHeapFrameBytes + 1024} {
		t.Run(fmt.Sprintf("%d", heapSize), func(t *testing.T) {
			tx := newComputeBudgetV1TestTransaction(t, solana.TransactionConfig{}.WithHeapSize(heapSize))
			_, err := ComputeBudgetLimitsForTransaction(tx, nil, nil)
			require.Error(t, err)
		})
	}
}
