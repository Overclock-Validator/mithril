package sealevel

import (
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/gagliardetto/solana-go"
	"github.com/maypok86/otter"
	"github.com/stretchr/testify/require"
	"testing"
)

// Legacy instruction fixtures predate the bank and program-cache dependencies
// used by loaded programs. Give each fixture isolated state, as replay does.
func initializeLegacyBankFixture(t *testing.T, ctx *ExecutionCtx) {
	t.Helper()
	ctx.RecordInnerInstructions = true
	if ctx.Log == nil {
		ctx.Log = &LogRecorder{}
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("program logs: %#v", ctx.Log)
		}
	})
	cache, err := otter.MustBuilder[solana.PublicKey, *accountsdb.ProgramCacheEntry](1024).Cost(func(solana.PublicKey, *accountsdb.ProgramCacheEntry) uint32 { return 1 }).Build()
	require.NoError(t, err)
	t.Cleanup(cache.Close)
	if ctx.Accounts == nil {
		ctx.Accounts = accounts.NewMemAccounts()
	}
	if ctx.SlotCtx == nil {
		ctx.SlotCtx = &SlotCtx{}
	}
	ctx.SlotCtx.Accounts = ctx.Accounts
	ctx.SlotCtx.AccountsDb = &accountsdb.AccountsDb{ProgramCache: cache}
	ctx.TransactionContext.ComputeBudgetLimits = &ComputeBudgetLimits{UpdatedHeapBytes: 32768}
}
