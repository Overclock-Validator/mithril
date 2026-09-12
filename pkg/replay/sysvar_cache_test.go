package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

func TestCacheFeesSysvar_MissingAccountNoPanic(t *testing.T) {
	prev := sealevel.SysvarCache.Fees
	t.Cleanup(func() { sealevel.SysvarCache.Fees = prev })
	sealevel.SysvarCache.Fees.Sysvar = nil
	sealevel.SysvarCache.Fees.Acct = nil

	require.NotPanics(t, func() { cacheFeesSysvar(nil) })
	require.Nil(t, sealevel.SysvarCache.Fees.Sysvar)
	require.Nil(t, sealevel.SysvarCache.Fees.Acct)
}

func TestCacheFeesSysvar_MissingAccountClearsStaleValue(t *testing.T) {
	prev := sealevel.SysvarCache.Fees
	t.Cleanup(func() { sealevel.SysvarCache.Fees = prev })
	stale := sealevel.SysvarFees{FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5_000}}
	sealevel.SysvarCache.Fees.Sysvar = &stale
	sealevel.SysvarCache.Fees.Acct = &accounts.Account{
		Key: sealevel.SysvarFeesAddr, Lamports: 1, Data: []byte{1},
	}

	emptyDb := &accountsdb.AccountsDb{}
	emptyDb.InitCaches()
	cacheFeesSysvar(emptyDb)

	require.Nil(t, sealevel.SysvarCache.Fees.Sysvar)
	require.Nil(t, sealevel.SysvarCache.Fees.Acct)
}
