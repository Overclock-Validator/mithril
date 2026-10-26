package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

var schedulerLookupTableKey = solana.PublicKey{0x7A, 0xB1, 0xE1}

// lookupEnv is a test bank at slot whose lookup table had its only address
// appended in slot 42.
func lookupEnv(t *testing.T, slot uint64) *blockprod.TestEnv {
	t.Helper()
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{})
	t.Cleanup(env.Close)
	env.SlotCtx.Slot = slot
	table := blockprod.ActiveLookupTable(schedulerLookupTableKey, txfixture.DestPubkey())
	table.LastExtendedSlot = 42
	acct, err := table.Account()
	require.NoError(t, err)
	env.SlotCtx.UnrootedRead = blockprod.ParentAccounts{table.Key: acct}
	return env
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// A transaction whose lookup does not resolve is dropped, not retried, even
// though it would resolve in a later slot.
func TestSchedulerDropsFailedLookupWithoutRetry(t *testing.T) {
	prev := sealevel.SysvarCache.SlotHashes.Sysvar
	slotHashes := sealevel.SysvarSlotHashes{{Slot: 42}, {Slot: 41}}
	sealevel.SysvarCache.SlotHashes.Sysvar = &slotHashes
	t.Cleanup(func() { sealevel.SysvarCache.SlotHashes.Sysvar = prev })

	controller := blockprod.NewController()
	sched := New(controller)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sched.Start(ctx)
	t.Cleanup(sched.Stop)
	wire, err := blockprod.SignedV0LookupTransferWire(1, schedulerLookupTableKey, 0)
	require.NoError(t, err)
	sched.Receive(packet.Owned(wire))
	waitFor(t, "buffering", func() bool { return sched.Buffered() == 1 })

	controller.SetWorkingBank(lookupEnv(t, 42).Bank)
	waitFor(t, "lookup failure", func() bool { return sched.Stats().DroppedLookup == 1 })

	next := lookupEnv(t, 43)
	controller.SetWorkingBank(next.Bank)
	time.Sleep(50 * time.Millisecond)
	require.Zero(t, sched.Stats().Accepted)
	require.Empty(t, next.Bank.ForgedTransactions())
	require.Zero(t, sched.Buffered())
	require.Equal(t, uint64(1), sched.Stats().DroppedLookup)
}
