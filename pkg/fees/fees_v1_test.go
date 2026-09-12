package fees

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
)

func TestCalculateTxFeesV1PriorityFeeIsDirectLamports(t *testing.T) {
	tx := &solana.Transaction{
		Message: solana.Message{Header: solana.MessageHeader{NumRequiredSignatures: 2}},
	}
	limits := &sealevel.ComputeBudgetLimits{
		ComputeUnitLimit:          1_400_000,
		ComputeUnitPrice:          math.MaxUint64,
		DirectPriorityFeeLamports: 42,
		UsesDirectPriorityFee:     true,
	}

	fee := CalculateTxFees(tx, nil, limits, features.NewFeaturesDefault())
	assert.Equal(t, uint64(10_000), fee.ExecutionFee)
	assert.Equal(t, uint64(42), fee.PriorityFee)
	assert.Equal(t, uint64(10_042), fee.TotalFee)
}
