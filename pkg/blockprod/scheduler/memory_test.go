package scheduler

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	computebudget "github.com/gagliardetto/solana-go/programs/compute-budget"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func recentSnapshot(t *testing.T, hashes ...solana.Hash) *sealevel.BankSysvars {
	t.Helper()
	recent := make(sealevel.SysvarRecentBlockhashes, len(hashes))
	for i, h := range hashes {
		recent[i].Blockhash = h
	}
	snapshot, err := sealevel.NewBankSysvars(42, &accounts.Account{
		Key: sealevel.SysvarRecentBlockHashesAddr, Data: recent.MustMarshal(), Lamports: 1,
	})
	require.NoError(t, err)
	return snapshot
}

func TestPreparationTokensReleasedOnEvictionAndRebuffer(t *testing.T) {
	feats := features.NewFeaturesDefault()
	s := NewWithConfig(blockprod.NewController(), Config{MaxBufferedTransactions: 2,
		MaxPreparedTransactions: 2, FeatureSource: func() *features.Features { return feats },
	})
	priced := func(price uint64) []byte {
		tx, err := solana.NewTransaction([]solana.Instruction{
			computebudget.NewSetComputeUnitPriceInstruction(price).Build(),
			system.NewTransferInstruction(price+1, txfixture.PayerPubkey(), txfixture.DestPubkey()).Build(),
		}, txfixture.TestBlockhash(), solana.TransactionPayer(txfixture.PayerPubkey()))
		require.NoError(t, err)
		key := txfixture.PayerPrivateKey()
		_, err = tx.Sign(func(solana.PublicKey) *solana.PrivateKey { return &key })
		require.NoError(t, err)
		wire, err := tx.MarshalBinary()
		require.NoError(t, err)
		return wire
	}
	s.Receive(packet.Owned(priced(1)))
	s.Receive(packet.Owned(priced(2)))
	require.EqualValues(t, 2, s.Stats().PreparedRetained)
	s.Receive(packet.Owned(priced(10)))
	require.EqualValues(t, 1, s.Stats().PreparedRetained)
	e := s.buffer.PopMax() // the unprepared high-price arrival
	require.Nil(t, e.prepared)
	s.Receive(packet.Owned(priced(20)))
	require.EqualValues(t, 2, s.Stats().PreparedRetained)
	s.rebuffer(e) // evicts the remaining low-price prepared entry
	require.EqualValues(t, 1, s.Stats().PreparedRetained)
	for s.Buffered() > 0 {
		s.releasePrepared(s.buffer.PopMax())
	}
	require.Zero(t, s.Stats().PreparedRetained)
}

func TestCleanupChunksBoundWorkAndEventuallyRemoveExpiredUnderMutation(t *testing.T) {
	b := NewBuffer(4096)
	insert := func(seq uint64, expired bool) {
		e := &entry{tx: &solana.Transaction{}, seq: seq, reward: seq}
		binary.LittleEndian.PutUint64(e.messageHash[:], seq)
		if expired {
			e.blockhash = solana.Hash{1}
		}
		result, _ := b.Insert(e)
		require.Equal(t, InsertAccepted, result)
	}
	for i := uint64(0); i < 4096; i++ {
		insert(i, i%3 != 0)
	}
	cursor, remaining := 0, b.Len()
	for step := 0; remaining > 0 && step < 2000; step++ {
		next, inspected, dropped := b.cleanupSome(cursor, min(16, remaining), func(e *entry) bool {
			return e.blockhash == (solana.Hash{1})
		})
		require.LessOrEqual(t, inspected, 16)
		cursor = next
		if dropped > 0 {
			remaining = b.Len()
		} else {
			remaining -= inspected
		}
		if step < 100 {
			b.PopMax()
			insert(uint64(5000+step), step%2 == 0)
			remaining = b.Len()
		}
	}
	require.Zero(t, remaining, "bounded chunks must finish despite heap movement")
	for _, e := range b.byHash {
		require.NotEqual(t, solana.Hash{1}, e.blockhash)
	}
}

func TestPreparationBoundPreservesFallbackOrder(t *testing.T) {
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{TPUGated: true})
	defer env.Close()
	s := NewWithConfig(env.Controller, Config{
		FeatureSource:           func() *features.Features { return env.SlotCtx.Features },
		MaxPreparedTransactions: 2,
	})
	const count = 8
	wires := txfixture.PrecomputeTransferPool(count)
	for _, wire := range wires {
		s.Receive(packet.Owned(wire))
	}
	require.Equal(t, count, s.Buffered())
	require.EqualValues(t, 2, s.Stats().PreparedRetained)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	defer s.Stop()
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, s.Stats().Accepted, "preparation must not open Harmonic's execution gate")
	env.Bank.AllowTPU()
	require.Eventually(t, func() bool { return s.Stats().Accepted == count }, time.Second, time.Millisecond)
	require.Zero(t, s.Stats().PreparedRetained)
	require.EqualValues(t, count-2, s.Stats().PreparationLazy)
	for i, tx := range env.Bank.ForgedTransactions() {
		wire, err := tx.MarshalBinary()
		require.NoError(t, err)
		require.Equal(t, wires[i], wire, "equal rewards must retain arrival order")
	}
}

func TestPreparationBoundUnderConcurrentIngressAndCleanup(t *testing.T) {
	feats := features.NewFeaturesDefault()
	controller := blockprod.NewController()
	snapshot := recentSnapshot(t, solana.Hash{1}, txfixture.TestBlockhash())
	s := NewWithConfig(controller, Config{
		FeatureSource: func() *features.Features { return feats }, MaxPreparedTransactions: 4,
		CleanupSource: func() *sealevel.BankSysvars { return snapshot },
	})
	wires := txfixture.PrecomputeTransferPool(128)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, wire := range wires[worker*16 : (worker+1)*16] {
				s.Receive(packet.Owned(wire))
			}
		}()
	}
	wg.Wait()
	require.Equal(t, len(wires), s.Buffered())
	require.EqualValues(t, 4, s.Stats().PreparedRetained)
	now := time.Now()
	s.maintain(now)
	snapshot = recentSnapshot(t, solana.Hash{2}, solana.Hash{1})
	s.maintain(now.Add(time.Second))
	require.Zero(t, s.Buffered())
	require.Zero(t, s.Stats().PreparedRetained, "expiry must release all preparation tokens")
	require.EqualValues(t, len(wires), s.Stats().BackgroundExpired)
	for _, wire := range wires[:8] {
		s.Receive(packet.Owned(wire))
	}
	require.EqualValues(t, 4, s.Stats().PreparedRetained, "tokens must be reusable after cleanup")
}

func TestBackgroundCleanupWhileGatedKeepsUnknownHashes(t *testing.T) {
	env := blockprod.NewTestEnv(blockprod.TestEnvConfig{TPUGated: true})
	defer env.Close()
	snapshot := recentSnapshot(t, solana.Hash{1}, txfixture.TestBlockhash())
	s := NewWithConfig(env.Controller, Config{CleanupSource: func() *sealevel.BankSysvars { return snapshot }})
	s.Receive(packet.Owned(txfixture.MustSignedTransferWire(1)))
	future := testEntry(100, 2, 9)
	future.blockhash = solana.Hash{9}
	result, _ := s.buffer.Insert(future)
	require.Equal(t, InsertAccepted, result)
	now := time.Now()
	s.maintain(now)
	snapshot = recentSnapshot(t, solana.Hash{2}, solana.Hash{1})
	s.maintain(now.Add(time.Second))
	require.Equal(t, 1, s.Buffered())
	require.Same(t, future, s.buffer.PopMax())
	require.Zero(t, s.Stats().Accepted)
	require.False(t, env.Bank.TPUAllowed())
	require.EqualValues(t, 1, s.Stats().BackgroundExpired)
}

func TestExpiryHistoryDiscontinuityAndBound(t *testing.T) {
	var h expiryHistory
	observe := func(hashes ...solana.Hash) {
		r := make(sealevel.SysvarRecentBlockhashes, len(hashes))
		for i, hash := range hashes {
			r[i].Blockhash = hash
		}
		h.observe(r)
	}
	observe(solana.Hash{2}, solana.Hash{1})
	observe(solana.Hash{3}, solana.Hash{2})
	require.True(t, h.expired(solana.Hash{1}))
	require.False(t, h.expired(solana.Hash{99}), "unknown or future hashes are not expired")
	observe(solana.Hash{8}, solana.Hash{1})
	require.False(t, h.expired(solana.Hash{1}), "fork/rewind must clear old-branch exclusions")
	for i := uint64(10); i < expiredHashHistoryCapacity+20; i++ {
		var hash solana.Hash
		binary.LittleEndian.PutUint64(hash[:], i)
		h.remember(hash)
	}
	require.Len(t, h.old, expiredHashHistoryCapacity)
	require.Equal(t, expiredHashHistoryCapacity, h.count)
}

func TestPendingCleanupRechecksSnapshotBeforeEachChunk(t *testing.T) {
	snapshot := recentSnapshot(t, solana.Hash{2}, solana.Hash{1})
	s := NewWithConfig(blockprod.NewController(), Config{CleanupSource: func() *sealevel.BankSysvars { return snapshot }})
	for i := uint64(0); i < 2048; i++ {
		e := &entry{tx: &solana.Transaction{}, seq: i, blockhash: solana.Hash{1}}
		binary.LittleEndian.PutUint64(e.messageHash[:], i)
		s.buffer.Insert(e)
	}
	now := time.Now()
	s.maintain(now)
	snapshot = recentSnapshot(t, solana.Hash{3}, solana.Hash{2})
	s.maintain(now.Add(time.Second))
	require.Equal(t, 1536, s.Buffered(), "first chunk is bounded to512 entries")
	// Rewind makes the oldest blockhash valid again before the next1s poll.
	snapshot = recentSnapshot(t, solana.Hash{2}, solana.Hash{1})
	s.maintain(now.Add(time.Second + 2*time.Millisecond))
	require.Equal(t, 1536, s.Buffered(), "a pending chunk must observe rewind before expiring entries")
	require.Zero(t, s.expiryRemaining)
}
