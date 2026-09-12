package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchedulerBuffersWithoutBankAndDrainsWhenSet(t *testing.T) {
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{})
	defer env.Close()

	controller := blockprod.NewController() // no bank yet
	sched := New(controller)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)
	defer sched.Stop()

	wire := txfixture.MustSignedTransferWire(0)
	sched.Receive(packet.Owned(wire))

	deadline := time.Now().Add(500 * time.Millisecond)
	for sched.Buffered() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.Equal(t, 1, sched.Buffered())
	require.Equal(t, uint64(0), sched.Stats().Accepted)

	controller.SetWorkingBank(env.Bank)

	deadline = time.Now().Add(2 * time.Second)
	for sched.Stats().Accepted == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.Equal(t, uint64(1), sched.Stats().Accepted)
	require.Equal(t, 0, sched.Buffered())
	require.Len(t, env.Bank.ForgedTransactions(), 1)
}

func TestSchedulerRetainsAcrossBankClear(t *testing.T) {
	controller := blockprod.NewController()
	sched := New(controller)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)
	defer sched.Stop()

	sched.Receive(packet.Owned(txfixture.MustSignedTransferWire(1)))
	deadline := time.Now().Add(500 * time.Millisecond)
	for sched.Buffered() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.Equal(t, 1, sched.Buffered())

	// No bank published yet — buffer must survive until a later slot.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, sched.Buffered())
	require.Equal(t, uint64(0), sched.Stats().Accepted)

	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{})
	defer env.Close()
	controller.SetWorkingBank(env.Bank)

	deadline = time.Now().Add(2 * time.Second)
	for sched.Stats().Accepted == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.Equal(t, uint64(1), sched.Stats().Accepted)
}

func TestSchedulerCleanupDropsExpired(t *testing.T) {
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{})
	defer env.Close()

	sched := New(env.Controller)
	wire := txfixture.MustSignedTransferWire(2)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	reward, mh, err := scoreTransaction(tx, sched.feats)
	require.NoError(t, err)

	// Force an expired blockhash while keeping a unique message hash.
	expired := solana.Hash{0xee}
	e := &entry{
		tx:          tx,
		wireSize:    len(wire),
		messageHash: mh,
		blockhash:   expired,
		reward:      reward,
		seq:         1,
	}
	res, _ := sched.buffer.Insert(e)
	require.Equal(t, InsertAccepted, res)

	dropped := sched.Cleanup(env.Bank)
	require.Equal(t, 1, dropped)
	require.Equal(t, 0, sched.Buffered())
	assert.Equal(t, uint64(1), sched.Stats().DroppedExpired)
}

func TestSchedulerMaybeCleanupAfterInsertInterval(t *testing.T) {
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{})
	defer env.Close()

	sched := New(env.Controller)
	wire := txfixture.MustSignedTransferWire(4)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	reward, mh, err := scoreTransaction(tx, sched.feats)
	require.NoError(t, err)

	res, _ := sched.buffer.Insert(&entry{
		tx:          tx,
		wireSize:    len(wire),
		messageHash: mh,
		blockhash:   solana.Hash{0xee},
		reward:      reward,
		seq:         1,
	})
	require.Equal(t, InsertAccepted, res)
	require.Equal(t, 1, sched.Buffered())

	interval := cleanupInsertInterval(sched.buffer.Capacity())
	require.Equal(t, uint64(MaxBufferedTxns/5), interval)

	sched.insertsSinceCleanup.Store(interval - 1)
	require.Equal(t, 0, sched.maybeCleanup(env.Bank))
	require.Equal(t, 1, sched.Buffered())
	require.Equal(t, interval-1, sched.insertsSinceCleanup.Load())

	sched.insertsSinceCleanup.Store(interval)
	require.Equal(t, 1, sched.maybeCleanup(env.Bank))
	require.Equal(t, 0, sched.Buffered())
	require.Equal(t, uint64(0), sched.insertsSinceCleanup.Load())
	assert.Equal(t, uint64(1), sched.Stats().DroppedExpired)
}

func TestSchedulerDropsUnparseable(t *testing.T) {
	sched := New(blockprod.NewController())
	sched.Receive(packet.Owned([]byte{0x00, 0x01}))
	assert.Equal(t, uint64(1), sched.Stats().DroppedParse)
}

func TestScoreTransferLeaderReward(t *testing.T) {
	wire := txfixture.MustSignedTransferWire(3)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	sched := New(blockprod.NewController())
	reward, _, err := scoreTransaction(tx, sched.feats)
	require.NoError(t, err)

	// 1 signature → 5000 execution fee → 2500 unburned; no priority fee.
	want := fees.LeaderReward(&fees.TxFeeInfo{ExecutionFee: 5000, PriorityFee: 0, TotalFee: 5000})
	require.Equal(t, want, reward)
	require.Equal(t, uint64(2500), reward)
}

func TestSchedulerDrainsHighestRewardFirst(t *testing.T) {
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{})
	defer env.Close()

	controller := blockprod.NewController()
	sched := New(controller)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)
	defer sched.Stop()

	low := txfixture.MustSignedTransferWire(10)
	high := txfixture.MustSignedTransferWire(11)

	// Insert low first, then high with artificially higher reward via direct buffer.
	sched.Receive(packet.Owned(low))
	deadline := time.Now().Add(500 * time.Millisecond)
	for sched.Buffered() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	txHigh, err := solana.TransactionFromBytes(high)
	require.NoError(t, err)
	_, mh, err := scoreTransaction(txHigh, sched.feats)
	require.NoError(t, err)
	res, _ := sched.buffer.Insert(&entry{
		tx:          txHigh,
		wireSize:    len(high),
		messageHash: mh,
		blockhash:   txHigh.Message.RecentBlockhash,
		reward:      1_000_000,
		seq:         99,
	})
	require.Equal(t, InsertAccepted, res)
	sched.signal()

	controller.SetWorkingBank(env.Bank)
	deadline = time.Now().Add(2 * time.Second)
	for len(env.Bank.ForgedTransactions()) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	forged := env.Bank.ForgedTransactions()
	require.Len(t, forged, 2)

	// First forged must be the high-reward tx (seq 11 transfer).
	highTx, err := solana.TransactionFromBytes(high)
	require.NoError(t, err)
	require.Equal(t, highTx.Signatures[0], forged[0].Signatures[0])
}

func TestSchedulerSkipsWhenSlotEntryBytesExceeded(t *testing.T) {
	envLimits := costmodel.DefaultLimits()
	envLimits.MaxEntryBytes = 80
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{Limits: envLimits})
	defer env.Close()

	sched := New(env.Controller)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)
	defer sched.Stop()

	sched.Receive(packet.Owned(txfixture.MustSignedTransferWire(0)))
	deadline := time.Now().Add(2 * time.Second)
	for sched.Stats().DroppedCost == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.Equal(t, uint64(1), sched.Stats().DroppedCost)
	require.Equal(t, uint64(1), sched.Stats().DroppedBatchBytes)
	require.Equal(t, uint64(0), sched.Stats().Accepted)
	require.Equal(t, 1, sched.Buffered())
	require.Empty(t, env.Bank.ForgedTransactions())
}

func TestMaxBufferedTxnsConstant(t *testing.T) {
	require.Equal(t, 2*65536, MaxBufferedTxns)
}

func TestSchedulerCopiesPooledPacketBytes(t *testing.T) {
	sched := New(blockprod.NewController())
	pool := packet.NewPool(1)
	buf, idx, ok := pool.Acquire()
	require.True(t, ok)

	wire := txfixture.MustSignedTransferWire(42)
	require.LessOrEqual(t, len(wire), len(buf))
	copy(buf, wire)
	pkt := packet.FromPool(pool, buf, len(wire), idx)

	sched.Receive(pkt)
	require.Equal(t, 1, sched.Buffered())

	// Recycle the pool slot and overwrite it. Buffered wire/tx must stay intact.
	buf2, idx2, ok := pool.Acquire()
	require.True(t, ok)
	require.Equal(t, idx, idx2)
	for i := range buf2 {
		buf2[i] = 0xff
	}
	pool.Release(idx2)

	e := sched.buffer.PopMax()
	require.NotNil(t, e)
	require.Equal(t, wire, e.wire)

	// Re-parse and verify the buffered transaction still has a valid signature.
	tx, err := solana.TransactionFromBytes(e.wire)
	require.NoError(t, err)
	require.Equal(t, e.tx.Signatures[0], tx.Signatures[0])
}

func TestClassifyBufferedExpired(t *testing.T) {
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{})
	defer env.Close()
	require.Equal(t, blockprod.BufferedExpired, env.Bank.ClassifyBuffered(solana.Hash{0x01}, [32]byte{}))
	require.Equal(t, blockprod.BufferedKeep, env.Bank.ClassifyBuffered(txfixture.TestBlockhash(), [32]byte{0xab}))
}
