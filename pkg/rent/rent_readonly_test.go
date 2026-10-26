package rent

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestRentEpochNormalizationOnlyTouchesWritableAccounts(t *testing.T) {
	for _, disabledCollection := range []bool{false, true} {
		for _, writable := range []bool{false, true} {
			for _, sysvar := range []bool{false, true} {
				f := features.NewFeaturesDefault()
				if disabledCollection {
					f.EnableFeature(features.DisableRentFeesCollection, 0)
				}
				key := solana.PublicKey{70}
				if sysvar {
					key = sealevel.SysvarEpochScheduleAddr
				}
				source := &accounts.Account{Key: key, Lamports: 1_000_000}
				tx := sealevel.NewTransactionAccountsFromRefs([]*accounts.Account{source}, []bool{true})
				tx.AcctMetas = []*sealevel.AccountMeta{{Pubkey: key, IsWritable: writable}}
				rent := testRent()
				MaybeSetRentExemptRentEpochMax(&sealevel.SlotCtx{Slot: 100, Epoch: 2}, &rent, f, tx)
				if writable && !sysvar {
					require.Equal(t, uint64(math.MaxUint64), tx.Accounts[0].RentEpoch)
					require.True(t, tx.Touched[0])
					require.NotSame(t, source, tx.Accounts[0])
				} else {
					require.Zero(t, tx.Accounts[0].RentEpoch)
					require.False(t, tx.Touched[0])
					require.Same(t, source, tx.Accounts[0])
				}
				require.Zero(t, source.RentEpoch, "shared source is immutable")
			}
		}
	}
}

func TestRentEpochNormalizationRequiresTransactionMetadata(t *testing.T) {
	rent := testRent()
	for _, metas := range [][]*sealevel.AccountMeta{nil, {nil}} {
		tx := sealevel.NewTransactionAccounts([]accounts.Account{{Key: solana.PublicKey{70}, Lamports: 1_000_000}})
		tx.AcctMetas = metas
		require.Panics(t, func() {
			MaybeSetRentExemptRentEpochMax(&sealevel.SlotCtx{}, &rent, features.NewFeaturesDefault(), tx)
		}, "invalid internal metadata must not be assumed writable")
	}
}
