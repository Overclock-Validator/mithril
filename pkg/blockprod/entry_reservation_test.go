package blockprod

import (
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func scheduledTransfer(t *testing.T, seq uint64) (*solana.Transaction, int) {
	t.Helper()
	wire := txfixture.MustSignedTransferWire(seq)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	return tx, len(wire)
}

func TestPrepareScheduleNReservesForEveryBatchSplit(t *testing.T) {
	sizes := []int{200, 300, 400}
	want := 3*entryBatchOverheadBytes + 200 + 300 + 400

	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	res, reason := env.Bank.PrepareScheduleN(sizes)
	require.Equal(t, costmodel.ExceedNone, reason)
	require.Equal(t, want, res.Bytes())
	require.Equal(t, want, env.Bank.EntryBuilder().ReservedBytes())
	require.Equal(t, want, env.Bank.EntryBytes())
	env.Bank.RebateSchedule(res)
	require.Zero(t, env.Bank.EntryBuilder().ReservedBytes())
	require.Zero(t, env.Bank.EntryBytes())

	limits := costmodel.DefaultLimits()
	limits.MaxEntryBytes = uint64(want)
	exact := NewTestEnv(TestEnvConfig{Limits: limits})
	defer exact.Close()
	res, reason = exact.Bank.PrepareScheduleN(sizes)
	require.Equal(t, costmodel.ExceedNone, reason)
	require.Equal(t, want, res.Bytes())

	limits.MaxEntryBytes = uint64(want - 1)
	short := NewTestEnv(TestEnvConfig{Limits: limits})
	defer short.Close()
	res, reason = short.Bank.PrepareScheduleN(sizes)
	require.Equal(t, costmodel.ExceedBatchBytes, reason)
	require.Zero(t, res.Bytes())
	require.Zero(t, short.Bank.EntryBuilder().ReservedBytes())
}

func TestScheduleReservationsAreCounted(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	txA, sizeA := scheduledTransfer(t, 0)
	txB, sizeB := scheduledTransfer(t, 1)

	resA, reason := env.Bank.PrepareSchedule(sizeA)
	require.Equal(t, costmodel.ExceedNone, reason)
	resB, reason := env.Bank.PrepareSchedule(sizeB)
	require.Equal(t, costmodel.ExceedNone, reason)
	require.Positive(t, resB.Bytes(), "a second outstanding reservation must hold its own bytes")
	require.Equal(t, resA.Bytes()+resB.Bytes(), env.Bank.EntryBuilder().ReservedBytes())

	result, _ := env.Bank.ForgeScheduledTransaction(txA, sizeA, nil, resA)
	require.Equal(t, ForgeAccepted, result)
	require.Equal(t, resB.Bytes(), env.Bank.EntryBuilder().ReservedBytes(), "forging A releases only A's reservation")

	result, _ = env.Bank.ForgeScheduledTransaction(txB, sizeB, nil, resB)
	require.Equal(t, ForgeAccepted, result)
	require.Zero(t, env.Bank.EntryBuilder().ReservedBytes())
	require.Len(t, env.Bank.ForgedTransactions(), 2)
	// Reservations assume every transaction starts a batch; the appended
	// transactions share one, and only their actual bytes remain.
	require.Equal(t, entryBatchOverheadBytes+sizeA+sizeB, env.Bank.EntryBytes())
}

func TestUnscheduledForgeLeavesOtherReservationsIntact(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	txA, sizeA := scheduledTransfer(t, 0)

	resA, reason := env.Bank.PrepareSchedule(sizeA)
	require.Equal(t, costmodel.ExceedNone, reason)

	result, _ := env.Bank.Forge(txfixture.MustSignedTransferWire(1))
	require.Equal(t, ForgeAccepted, result)
	require.Equal(t, resA.Bytes(), env.Bank.EntryBuilder().ReservedBytes())

	result, _ = env.Bank.ForgeScheduledTransaction(txA, sizeA, nil, resA)
	require.Equal(t, ForgeAccepted, result)
	require.Zero(t, env.Bank.EntryBuilder().ReservedBytes())
}

func TestUnscheduledForgeCannotUseReservedEntryBytes(t *testing.T) {
	txA, size := scheduledTransfer(t, 0)
	limits := costmodel.DefaultLimits()
	// Room for one transaction in its own batch, not two.
	limits.MaxEntryBytes = uint64(2*(entryBatchOverheadBytes+size) - 1)
	env := NewTestEnv(TestEnvConfig{Limits: limits})
	defer env.Close()

	resA, reason := env.Bank.PrepareSchedule(size)
	require.Equal(t, costmodel.ExceedNone, reason)

	result, reason := env.Bank.Forge(txfixture.MustSignedTransferWire(1))
	require.Equal(t, ForgeDroppedCost, result)
	require.Equal(t, costmodel.ExceedBatchBytes, reason)
	require.Equal(t, resA.Bytes(), env.Bank.EntryBuilder().ReservedBytes())

	result, _ = env.Bank.ForgeScheduledTransaction(txA, size, nil, resA)
	require.Equal(t, ForgeAccepted, result)
	require.Equal(t, []*solana.Transaction{txA}, env.Bank.ForgedTransactions())
	require.LessOrEqual(t, env.Bank.EntryBytes(), int(limits.MaxEntryBytes))
}

func TestScheduledForgeReleasesReservationOnEveryResult(t *testing.T) {
	env := NewTestEnv(TestEnvConfig{})
	defer env.Close()
	tx, size := scheduledTransfer(t, 0)

	result, _ := env.Bank.ForgeTransaction(tx, size)
	require.Equal(t, ForgeAccepted, result)
	res, reason := env.Bank.PrepareSchedule(size)
	require.Equal(t, costmodel.ExceedNone, reason)
	result, _ = env.Bank.ForgeScheduledTransaction(tx, size, nil, res)
	require.Equal(t, ForgeDroppedAlreadyProcessed, result)
	require.Zero(t, env.Bank.EntryBuilder().ReservedBytes())

	res, reason = env.Bank.PrepareSchedule(size)
	require.Equal(t, costmodel.ExceedNone, reason)
	result, _ = env.Bank.ForgeScheduledTransaction(nil, size, nil, res)
	require.Equal(t, ForgeDroppedParse, result)
	require.Zero(t, env.Bank.EntryBuilder().ReservedBytes())

	res, reason = env.Bank.PrepareSchedule(size)
	require.Equal(t, costmodel.ExceedNone, reason)
	env.Bank.Close()
	result, _ = env.Bank.ForgeScheduledTransaction(tx, size, nil, res)
	require.Equal(t, ForgeDroppedNoLeader, result)
	require.Zero(t, env.Bank.EntryBuilder().ReservedBytes())
}

func TestConcurrentForgersRespectEntryByteBudget(t *testing.T) {
	const perForger = 64
	_, size := scheduledTransfer(t, 0)
	limits := costmodel.DefaultLimits()
	limits.MaxBatchBytes = uint64(entryBatchOverheadBytes + 4*size)
	limits.MaxEntryBytes = uint64(perForger * (entryBatchOverheadBytes + size))
	env := NewTestEnv(TestEnvConfig{Limits: limits})
	defer env.Close()

	scheduled := make([]*solana.Transaction, perForger)
	unscheduled := make([][]byte, perForger)
	for i := range perForger {
		scheduled[i], _ = scheduledTransfer(t, uint64(i))
		unscheduled[i] = txfixture.MustSignedTransferWire(uint64(perForger + i))
	}

	var accepted [2]int
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for _, tx := range scheduled {
			res, reason := env.Bank.PrepareSchedule(size)
			if reason != costmodel.ExceedNone {
				continue
			}
			if result, _ := env.Bank.ForgeScheduledTransaction(tx, size, nil, res); result == ForgeAccepted {
				accepted[0]++
			}
		}
	}()
	go func() {
		defer wg.Done()
		for _, wire := range unscheduled {
			if result, _ := env.Bank.Forge(wire); result == ForgeAccepted {
				accepted[1]++
			}
		}
	}()
	wg.Wait()

	assert.Zero(t, env.Bank.EntryBuilder().ReservedBytes())
	assert.Positive(t, accepted[0])
	assert.Positive(t, accepted[1])
	assert.Less(t, accepted[0]+accepted[1], 2*perForger, "the budget must have been binding")
	assert.Len(t, env.Bank.ForgedTransactions(), accepted[0]+accepted[1])
	assert.LessOrEqual(t, env.Bank.EntryBytes(), int(limits.MaxEntryBytes))
}
