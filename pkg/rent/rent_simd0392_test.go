package rent

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
)

func TestSIMD0392RentPayingBalanceTransitions(t *testing.T) {
	rent := testRent()
	dataLen := 8
	minimumBalance := rent.MinimumBalance(uint64(dataLen))
	preBalance := minimumBalance - 10

	tests := []struct {
		name          string
		featureActive bool
		postBalance   uint64
		wantErr       bool
	}{
		{
			name:          "legacy allows debit while remaining rent paying",
			featureActive: false,
			postBalance:   preBalance - 1,
		},
		{
			name:          "legacy rejects credit while remaining rent paying",
			featureActive: false,
			postBalance:   preBalance + 1,
			wantErr:       true,
		},
		{
			name:          "SIMD-0392 rejects debit while remaining below minimum",
			featureActive: true,
			postBalance:   preBalance - 1,
			wantErr:       true,
		},
		{
			name:          "SIMD-0392 allows unchanged rent paying account",
			featureActive: true,
			postBalance:   preBalance,
		},
		{
			name:          "SIMD-0392 allows credit while remaining below minimum",
			featureActive: true,
			postBalance:   preBalance + 1,
		},
		{
			name:          "SIMD-0392 allows drain to zero",
			featureActive: true,
			postBalance:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, featureSet := testRentTransactionContext(tt.featureActive, preBalance, dataLen)
			pre := NewRentStateInfo(&rent, ctx, featureSet)
			ctx.Accounts.Accounts[0].Lamports = tt.postBalance
			post := NewRentStateInfo(&rent, ctx, featureSet)

			err := VerifyRentStateChanges(pre, post, ctx)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSIMD0392RelaxationRequiresStableOwnerAndNonGrowingData(t *testing.T) {
	rent := testRent()
	const dataLen = 8
	preBalance := rent.MinimumBalance(dataLen) - 10

	tests := []struct {
		name   string
		mutate func(*accounts.Account)
	}{
		{
			name: "owner changed",
			mutate: func(account *accounts.Account) {
				account.Owner = solana.PublicKey{9}
			},
		},
		{
			name: "data grew",
			mutate: func(account *accounts.Account) {
				account.Data = append(account.Data, 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, featureSet := testRentTransactionContext(true, preBalance, dataLen)
			pre := NewRentStateInfo(&rent, ctx, featureSet)
			tt.mutate(ctx.Accounts.Accounts[0])
			post := NewRentStateInfo(&rent, ctx, featureSet)

			assert.Error(t, VerifyRentStateChanges(pre, post, ctx))
		})
	}
}

func TestSIMD0392DoesNotPermitRentExemptAccountToBeDebitedBelowMinimum(t *testing.T) {
	rent := testRent()
	const dataLen = 8
	minimumBalance := rent.MinimumBalance(dataLen)
	ctx, featureSet := testRentTransactionContext(true, minimumBalance, dataLen)
	pre := NewRentStateInfo(&rent, ctx, featureSet)
	ctx.Accounts.Accounts[0].Lamports = minimumBalance - 1
	post := NewRentStateInfo(&rent, ctx, featureSet)

	assert.Error(t, VerifyRentStateChanges(pre, post, ctx))
}

func testRent() sealevel.SysvarRent {
	return sealevel.SysvarRent{
		LamportsPerUint8Year: 1,
		ExemptionThreshold:   1,
	}
}

func testRentTransactionContext(featureActive bool, balance uint64, dataLen int) (*sealevel.TransactionCtx, *features.Features) {
	key := solana.PublicKey{1}
	featureSet := features.NewFeaturesDefault()
	if featureActive {
		featureSet.EnableFeature(features.RelaxPostExecMinBalanceCheck, 0)
	}
	account := &accounts.Account{
		Key:      key,
		Lamports: balance,
		Data:     make([]byte, dataLen),
		Owner:    solana.PublicKey{2},
	}
	return &sealevel.TransactionCtx{
		Accounts: sealevel.TransactionAccounts{
			Accounts:  []*accounts.Account{account},
			AcctMetas: []*sealevel.AccountMeta{{Pubkey: key, IsWritable: true}},
		},
	}, featureSet
}
