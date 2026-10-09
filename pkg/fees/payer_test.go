package fees

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateFeePayer_SystemRentExemptRemainder(t *testing.T) {
	rent := sealevel.NewDefaultRentSysvar()
	rentMin := rent.MinimumBalance(0)
	require.Equal(t, uint64(890880), rentMin)
	const fee = uint64(5000)

	systemPayer := func(lamports uint64) *accounts.Account {
		return &accounts.Account{
			Owner:    addresses.SystemProgramAddr,
			Lamports: lamports,
		}
	}

	assert.NoError(t, ValidateFeePayer(systemPayer(rentMin+fee), fee, rent))
	assert.ErrorIs(t, ValidateFeePayer(systemPayer(rentMin+fee-1), fee, rent), ErrInsufficientFundsForRent)
	assert.ErrorIs(t, ValidateFeePayer(systemPayer(fee-1), fee, rent), ErrInsufficientFundsForFee)
	assert.ErrorIs(t, ValidateFeePayer(systemPayer(0), fee, rent), ErrFeePayerNotFound)
	assert.ErrorIs(t, ValidateFeePayer(nil, fee, rent), ErrFeePayerNotFound)
}

func TestValidateFeePayer_RelaxPostExecMinBalanceCheck(t *testing.T) {
	rent := sealevel.NewDefaultRentSysvar()
	rentMin := rent.MinimumBalance(0)
	const fee = uint64(5000)
	payer := &accounts.Account{
		Owner:    addresses.SystemProgramAddr,
		Lamports: rentMin - 1,
	}

	feats := features.NewFeaturesDefault()
	assert.NoError(t, ValidateFeePayerWithFeatures(payer, fee, rent, feats))
	feats.EnableFeature(features.RelaxPostExecMinBalanceCheck, 0)
	assert.ErrorIs(t, ValidateFeePayerWithFeatures(payer, fee, rent, feats), ErrInsufficientFundsForRent)
	assert.NoError(t, ValidateFeePayerWithFeatures(payer, 0, rent, feats),
		"SIMD-0392 permits an unchanged rent-paying account")

	// A rent-exempt payer may not be drained to a nonzero sub-exempt balance
	// in either mode.
	payer.Lamports = rentMin + fee - 1
	assert.ErrorIs(t, ValidateFeePayerWithFeatures(payer, fee, rent, feats), ErrInsufficientFundsForRent)
}

func TestValidateFeePayer_RejectsNonSystemOwner(t *testing.T) {
	rent := sealevel.NewDefaultRentSysvar()
	err := ValidateFeePayer(&accounts.Account{
		Owner:    addresses.VoteProgramAddr,
		Lamports: 10_000_000,
	}, 5000, rent)
	assert.ErrorIs(t, err, ErrInvalidAccountForFee)
}

func TestValidateFeePayer_NonceKeepsNonceRent(t *testing.T) {
	rent := sealevel.NewDefaultRentSysvar()
	nonceMin := rent.MinimumBalance(NonceStateSize)
	const fee = uint64(5000)

	nonceState := sealevel.NonceStateVersions{
		Type: sealevel.NonceVersionCurrent,
		Current: sealevel.NonceData{
			IsInitialized: true,
		},
	}
	data, err := nonceState.Marshal()
	require.NoError(t, err)
	require.Equal(t, NonceStateSize, len(data))

	payer := &accounts.Account{
		Owner:    addresses.SystemProgramAddr,
		Lamports: nonceMin + fee - 1,
		Data:     data,
	}
	assert.ErrorIs(t, ValidateFeePayer(payer, fee, rent), ErrInsufficientFundsForFee)

	payer.Lamports = nonceMin + fee
	assert.NoError(t, ValidateFeePayer(payer, fee, rent))

	payer.Data = append(payer.Data, 0)
	assert.ErrorIs(t, ValidateFeePayer(payer, fee, rent), ErrInvalidAccountForFee,
		"Agave requires nonce fee-payer data to be exactly 80 bytes")
}

func TestValidateFeePayer_RentPayingSystemMaySpendToZero(t *testing.T) {
	rent := sealevel.NewDefaultRentSysvar()
	const fee = uint64(5000)
	// Already below the 0-byte rent-exempt minimum; Agave allows the fee
	// as long as lamports >= fee (min_balance is 0 for system accounts).
	assert.NoError(t, ValidateFeePayer(&accounts.Account{
		Owner:    addresses.SystemProgramAddr,
		Lamports: fee,
	}, fee, rent))
}

func TestValidateFeePayer_RentExemptSystemMaySpendToZero(t *testing.T) {
	rent := sealevel.NewDefaultRentSysvar()
	rentMin := rent.MinimumBalance(0)
	systemPayer := func(lamports uint64) *accounts.Account {
		return &accounts.Account{
			Owner:    addresses.SystemProgramAddr,
			Lamports: lamports,
		}
	}

	assert.NoError(t, ValidateFeePayer(systemPayer(rentMin), rentMin, rent))
	assert.NoError(t, ValidateFeePayer(systemPayer(^uint64(0)), ^uint64(0), rent))
	assert.ErrorIs(t, ValidateFeePayer(systemPayer(rentMin), rentMin-1, rent), ErrInsufficientFundsForRent)
}
