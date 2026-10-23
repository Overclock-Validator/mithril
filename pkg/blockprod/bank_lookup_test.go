package blockprod

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var lookupTestTableKey = solana.PublicKey{0x7A, 0xB1, 0xE0}

// withLookupSlotHashes installs SlotHashes for the test env's slot, which
// lookup resolution needs to classify deactivating tables.
func withLookupSlotHashes(t *testing.T, env *TestEnv) {
	t.Helper()
	prev := sealevel.SysvarCache.SlotHashes.Sysvar
	slot := env.SlotCtx.Slot
	slotHashes := sealevel.SysvarSlotHashes{{Slot: slot - 1}, {Slot: slot - 2}}
	sealevel.SysvarCache.SlotHashes.Sysvar = &slotHashes
	t.Cleanup(func() { sealevel.SysvarCache.SlotHashes.Sysvar = prev })
}

// addLookupTable puts table in the test bank's parent state, which is where
// lookups are resolved from.
func addLookupTable(t *testing.T, env *TestEnv, table LookupTable) {
	t.Helper()
	acct, err := table.Account()
	require.NoError(t, err)
	parent, ok := env.SlotCtx.UnrootedRead.(ParentAccounts)
	if !ok {
		parent = ParentAccounts{}
		env.SlotCtx.UnrootedRead = parent
	}
	parent[table.Key] = acct
}

func lookupTransfer(t *testing.T, lamports uint64) (*solana.Transaction, []byte) {
	t.Helper()
	wire, err := SignedV0LookupTransferWire(lamports, lookupTestTableKey, 0)
	require.NoError(t, err)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	return tx, wire
}

func lamportsOf(t *testing.T, env *TestEnv, key solana.PublicKey) uint64 {
	t.Helper()
	acct, err := env.SlotCtx.GetAccount(key)
	require.NoError(t, err)
	return acct.Lamports
}

func TestWorkingBankForgesV0TransferThroughLookupTable(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	withLookupSlotHashes(t, env)
	addLookupTable(t, env, ActiveLookupTable(lookupTestTableKey, txfixture.DestPubkey()))

	const amount = 1_234
	tx, wire := lookupTransfer(t, amount)
	payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
	destBefore := lamportsOf(t, env, txfixture.DestPubkey())

	result, reason := env.Bank.ForgeTransaction(tx, len(wire))
	require.Equal(t, ForgeAccepted, result)
	require.Equal(t, costmodel.ExceedNone, reason)

	assert.Equal(t, destBefore+amount, lamportsOf(t, env, txfixture.DestPubkey()))
	assert.Equal(t, payerBefore-amount-5_000, lamportsOf(t, env, txfixture.PayerPubkey()))
	assert.Equal(t, uint64(5_000), env.Bank.TxFeeAccumulator().TotalFees)

	// The entry carries the original, unresolved transaction and its exact wire.
	forged := env.Bank.ForgedTransactions()
	require.Len(t, forged, 1)
	assert.Same(t, tx, forged[0])
	assert.False(t, forged[0].Message.IsResolved())
	forgedWire, err := forged[0].MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, wire, forgedWire)

	// The looked-up destination is write-locked in block cost accounting.
	assert.NotZero(t, env.Bank.CostTracker().WritableAccountCost(txfixture.DestPubkey()))
}

func TestWorkingBankDropsUnresolvableLookups(t *testing.T) {
	deactivated := ActiveLookupTable(lookupTestTableKey, txfixture.DestPubkey())
	deactivated.DeactivationSlot = 30 // no longer in SlotHashes
	appendedThisSlot := ActiveLookupTable(lookupTestTableKey, txfixture.DestPubkey())
	appendedThisSlot.LastExtendedSlot = 42 // the test env's slot
	appendedThisSlot.LastExtendedStartIndex = 0

	cases := []struct {
		name  string
		table *LookupTable
	}{
		{"missing table", nil},
		{"deactivated table", &deactivated},
		{"address appended this slot", &appendedThisSlot},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := NewTestEnv(TestEnvConfig{})
			defer env.Close()
			withLookupSlotHashes(t, env)
			if tc.table != nil {
				addLookupTable(t, env, *tc.table)
			}
			tx, wire := lookupTransfer(t, 1)
			assertForgeLookupDropped(t, env, tx, wire, ForgeDroppedLookup)
		})
	}
}

// The account lock rules apply to the resolved keys too: a table that loads
// the transaction's own payer repeats a static key.
func TestWorkingBankDropsLookupThatRepeatsAStaticKey(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	withLookupSlotHashes(t, env)
	addLookupTable(t, env, ActiveLookupTable(lookupTestTableKey, txfixture.PayerPubkey()))
	tx, wire := lookupTransfer(t, 1)
	assertForgeLookupDropped(t, env, tx, wire, ForgeDroppedAccountLocks)
}

func assertForgeLookupDropped(t *testing.T, env *TestEnv, tx *solana.Transaction, wire []byte, want ForgeResult) {
	t.Helper()
	payerBefore := lamportsOf(t, env, txfixture.PayerPubkey())
	result, reason := env.Bank.ForgeTransaction(tx, len(wire))
	assert.Equal(t, want, result)
	assert.Equal(t, costmodel.ExceedNone, reason)
	assert.Equal(t, payerBefore, lamportsOf(t, env, txfixture.PayerPubkey()))
	assert.Empty(t, env.Bank.ForgedTransactions())
	assert.Zero(t, env.Bank.TxFeeAccumulator().TotalFees)
	assert.False(t, tx.Message.IsResolved(), "the scheduler's transaction must stay unresolved")
}
