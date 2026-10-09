package rpcserver

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimulateTransactionRejectsSanitizeFailureWithoutSigVerify(t *testing.T) {
	tx, _ := testLegacyTransaction(t)
	tx.Message.Header.NumReadonlySignedAccounts = tx.Message.Header.NumRequiredSignatures
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)

	rpcServer := &RpcServer{
		slotCtx: &sealevel.SlotCtx{
			Slot:     123,
			Features: features.NewFeaturesDefault(),
		},
	}

	_, err = rpcServer.SimulateTransaction(
		context.Background(),
		mustRawParams(t, []interface{}{
			base64.StdEncoding.EncodeToString(wire),
			map[string]interface{}{"encoding": "base64"},
		}),
	)
	require.Error(t, err)

	invalidParams, ok := err.(*InvalidParamsError)
	require.True(t, ok)
	assert.Equal(t, errInvalidSanitizedTransaction.Message, invalidParams.Message)
}

func TestSimulateTransactionSanitizesBeforeV1FeatureGate(t *testing.T) {
	tx, _ := testV1Transaction(t, 0)
	tx.Message.Instructions[0].ProgramIDIndex = uint16(len(tx.Message.AccountKeys))
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)

	featureSet := features.NewFeaturesDefault()
	require.False(t, featureSet.IsActive(features.EnableTxV1))
	rpcServer := &RpcServer{
		slotCtx: &sealevel.SlotCtx{
			Slot:     123,
			Features: featureSet,
		},
	}

	_, err = rpcServer.SimulateTransaction(
		context.Background(),
		mustRawParams(t, []interface{}{
			base64.StdEncoding.EncodeToString(wire),
			map[string]interface{}{"encoding": "base64"},
		}),
	)
	require.Error(t, err)

	invalidParams, ok := err.(*InvalidParamsError)
	require.True(t, ok)
	assert.Equal(t, errInvalidSanitizedTransaction.Message, invalidParams.Message)
}

func TestSimulateTransactionRejectsLegacyTrailingBytes(t *testing.T) {
	_, wire := testLegacyTransaction(t)
	wire = append(wire, 0)

	rpcServer := &RpcServer{}
	_, err := rpcServer.SimulateTransaction(
		context.Background(),
		mustRawParams(t, []interface{}{
			base64.StdEncoding.EncodeToString(wire),
			map[string]interface{}{"encoding": "base64"},
		}),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to decode transaction")
}
