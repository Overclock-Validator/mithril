package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

func TestNewExecCtxUsesBankLocalRent(t *testing.T) {
	bankRent := sealevel.SysvarRent{
		LamportsPerUint8Year: 12_345,
		ExemptionThreshold:   3.5,
		BurnPercent:          17,
	}
	snapshot, err := sealevel.NewBankSysvars(42, &accounts.Account{
		Key: sealevel.SysvarRentAddr, Lamports: 1, Data: bankRent.MustMarshal(),
	})
	require.NoError(t, err)
	slotCtx := &sealevel.SlotCtx{
		Slot:            42,
		Features:        features.NewFeaturesDefault(),
		FeeRateGovernor: &sealevel.FeeRateGovernor{},
	}
	require.NoError(t, slotCtx.PublishBankSysvars(snapshot))

	previousRent := sealevel.SysvarCache.Rent.Sysvar
	conflictingRent := sealevel.NewDefaultRentSysvar()
	sealevel.SysvarCache.Rent.Sysvar = &conflictingRent
	t.Cleanup(func() { sealevel.SysvarCache.Rent.Sysvar = previousRent })

	execCtx := newExecCtx(slotCtx, &sealevel.TransactionAccounts{}, &sealevel.ComputeBudgetLimits{}, nil)
	require.Equal(t, bankRent, execCtx.TransactionContext.Rent)
}

func TestNewExecCtxRejectsBankSnapshotWithoutRent(t *testing.T) {
	snapshot, err := sealevel.NewBankSysvars(42, &accounts.Account{
		Key: sealevel.SysvarClockAddr, Lamports: 1, Data: (&sealevel.SysvarClock{Slot: 42}).MustMarshal(),
	})
	require.NoError(t, err)
	slotCtx := &sealevel.SlotCtx{
		Slot:            42,
		Features:        features.NewFeaturesDefault(),
		FeeRateGovernor: &sealevel.FeeRateGovernor{},
	}
	require.NoError(t, slotCtx.PublishBankSysvars(snapshot))

	require.PanicsWithValue(t, "bank sysvar snapshot for slot 42 is missing Rent", func() {
		newExecCtx(slotCtx, &sealevel.TransactionAccounts{}, &sealevel.ComputeBudgetLimits{}, nil)
	})
}
