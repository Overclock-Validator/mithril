package replay

import (
	"errors"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestTransactionMessageHashIgnoresLegacySignatures(t *testing.T) {
	wire := txfixture.MustSignedTransferWire(0)
	original, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	require.Equal(t, solana.MessageVersionLegacy, original.Message.GetVersion())
	require.NotEmpty(t, original.Signatures)

	differentSignature := *original
	differentSignature.Signatures = append([]solana.Signature(nil), original.Signatures...)
	differentSignature.Signatures[0][0] ^= 0xff

	originalHash, err := TransactionMessageHash(original)
	require.NoError(t, err)
	differentSignatureHash, err := TransactionMessageHash(&differentSignature)
	require.NoError(t, err)
	require.Equal(t, originalHash, differentSignatureHash)

	differentWire, err := differentSignature.MarshalBinary()
	require.NoError(t, err)
	require.NotEqual(t, wire, differentWire)
}

func TestPlanBlockTransactionExecutionRejectsDuplicateMessages(t *testing.T) {
	message := solana.Message{
		Header: solana.MessageHeader{
			NumRequiredSignatures: 2,
		},
		AccountKeys:     []solana.PublicKey{{1}, {2}},
		RecentBlockhash: solana.Hash{3},
	}
	first := &solana.Transaction{
		Signatures: []solana.Signature{{4}, {5}},
		Message:    message,
	}
	// Agave keys AlreadyProcessed by message hash, so changing the wire
	// signature does not make this a different transaction for this check.
	duplicateMessage := &solana.Transaction{
		Signatures: []solana.Signature{{6}, {7}},
		Message:    message,
	}
	differentMessage := &solana.Transaction{
		Signatures: []solana.Signature{{8}},
		Message: solana.Message{
			Header:          solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys:     []solana.PublicKey{{1}},
			RecentBlockhash: solana.Hash{9},
		},
	}
	firstHash, err := TransactionMessageHash(first)
	require.NoError(t, err)
	duplicateHash, err := TransactionMessageHash(duplicateMessage)
	require.NoError(t, err)
	differentHash, err := TransactionMessageHash(differentMessage)
	require.NoError(t, err)
	require.Equal(t, firstHash, duplicateHash)
	require.NotEqual(t, firstHash, differentHash)

	block := &b.Block{
		Slot: 42,
		Transactions: []*solana.Transaction{
			first, duplicateMessage, differentMessage,
		},
	}
	plan, err := planBlockTransactionExecution(block)
	var duplicateErr *DuplicateTransactionMessagesError
	require.Error(t, err)
	require.True(t, errors.As(err, &duplicateErr))
	require.Equal(t, uint64(42), duplicateErr.Slot)
	require.Equal(t, uint64(1), duplicateErr.DuplicateCount)
	require.Equal(t, []DuplicateTransactionOccurrence{{Index: 1, FirstIndex: 0}}, duplicateErr.Occurrences)
	require.Equal(t, []bool{true, false, true}, plan.execute)
	require.Equal(t, uint64(2), plan.processedTxCount)
	require.Equal(t, uint64(3), plan.processedSignatures)
	require.ErrorContains(t, err, "duplicate indexes (duplicate->first): 1->0")
}

func TestPlanBlockTransactionExecutionRejectsNilTransaction(t *testing.T) {
	_, err := planBlockTransactionExecution(&b.Block{Slot: 42, Transactions: []*solana.Transaction{nil}})
	require.ErrorContains(t, err, "transaction 0 is nil")
}

func TestProcessBlockRejectsDuplicateMessagesBeforeAccountAccess(t *testing.T) {
	wire := txfixture.MustSignedTransferWire(0)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	block := &b.Block{Slot: 77, Transactions: []*solana.Transaction{tx, tx}}

	_, err = ProcessBlock(nil, block, nil, 0, nil, nil, nil, NewTransactionStatusCache(), false, nil)
	var duplicateErr *DuplicateTransactionMessagesError
	require.Error(t, err)
	require.True(t, errors.As(err, &duplicateErr))
	require.Equal(t, uint64(77), duplicateErr.Slot)
	require.Equal(t, uint64(1), duplicateErr.DuplicateCount)
}

func TestProcessBlockRejectsV1BeforeFeatureActivationWithoutPanicking(t *testing.T) {
	wire := txfixture.MustSignedV1Wire(7, 8)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	require.Equal(t, solana.MessageVersionV1, tx.Message.GetVersion())
	require.NoError(t, txverify.SanitizeTransaction(tx))
	require.NoError(t, txverify.VerifyTransaction(tx))

	for _, txParallelism := range []int{0, 2} {
		name := "sequential"
		if txParallelism > 0 {
			name = "parallel"
		}
		t.Run(name, func(t *testing.T) {
			block := &b.Block{
				Slot:         78,
				Features:     features.NewFeaturesDefault(),
				Transactions: []*solana.Transaction{tx},
			}

			var processErr error
			require.NotPanics(t, func() {
				_, processErr = ProcessBlock(
					nil,
					block,
					nil,
					txParallelism,
					nil,
					nil,
					nil,
					NewTransactionStatusCache(),
					false,
					nil,
				)
			})
			require.ErrorIs(t, processErr, TxErrUnsupportedVersion)
			require.ErrorContains(t, processErr, "transaction 0 uses V1 before EnableTxV1 activation")
		})
	}
}

func TestValidateBlockTransactionVersionsUsesCandidateFeatures(t *testing.T) {
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedV1Wire(8, 0))
	require.NoError(t, err)
	block := &b.Block{
		Features:     features.NewFeaturesDefault(),
		Transactions: []*solana.Transaction{tx},
	}
	require.ErrorIs(t, validateBlockTransactionVersions(block), TxErrUnsupportedVersion)

	block.Features.EnableFeature(features.EnableTxV1, 42)
	require.NoError(t, validateBlockTransactionVersions(block))
}
