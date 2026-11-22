package blockprod

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestWorkingBankPreparesOriginalParentsBeforeFreeze(t *testing.T) {
	for _, mode := range []string{"transaction", "bundle", "parallel_bundle"} {
		t.Run(mode, func(t *testing.T) {
			env := NewTestEnv(TestEnvConfig{})
			defer env.Close()
			env.SlotCtx.Features.EnableFeature(features.AccountsLtHash, 0)
			env.SlotCtx.ParentAccts = accounts.NewMemAccounts()
			parents := ParentAccounts{}
			for _, key := range []solana.PublicKey{txfixture.PayerPubkey(), txfixture.DestPubkey()} {
				acct, err := env.SlotCtx.GetAccount(key)
				require.NoError(t, err)
				parents[key] = acct
			}
			env.SlotCtx.UnrootedRead = parents
			for _, amount := range []uint64{1, 2} {
				tx := mustSignedTransfer(t, amount)
				switch mode {
				case "transaction":
					wire, err := tx.MarshalBinary()
					require.NoError(t, err)
					result, _ := env.Bank.Forge(wire)
					require.Equal(t, ForgeAccepted, result)
				case "bundle":
					result := env.Bank.ForgeBundle(BundleInput{Transactions: []*solana.Transaction{tx}})
					require.Equal(t, ForgeAccepted, result.Result, result.Err)
				case "parallel_bundle":
					prepared, result := env.Bank.PrepareBundle([]*solana.Transaction{tx}, true)
					require.Equal(t, ForgeAccepted, result.Result)
					result = env.Bank.CommitBundle(env.Bank.ExecuteBundle(prepared))
					require.Equal(t, ForgeAccepted, result.Result, result.Err)
				}
				for key, want := range parents {
					parent, err := env.SlotCtx.GetParentAccount(key)
					require.NoError(t, err, "parent capture must happen before finalization")
					require.Equal(t, want, parent, "second write must retain the slot's original parent")
				}
			}
			live, err := env.SlotCtx.GetAccount(txfixture.DestPubkey())
			require.NoError(t, err)
			require.Equal(t, parents[txfixture.DestPubkey()].Lamports+3, live.Lamports)
		})
	}
}
