package rpcserver

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockhistory"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// These fixtures exercise real stores and AccountsDB fold/rewind, not execution
// or finality validation. The parent payload is deliberately not retained.
func transactionHistoryReadFixture(t *testing.T) (*RpcServer, transactionhistory.Block) {
	t.Helper()
	db := newRPCAccountsDB(t)
	for _, slot := range []uint64{10, 20} {
		_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{{
			Key: solana.PublicKey{201}, Lamports: slot,
		}}}}, slot, nil, nil)
		require.NoError(t, err)
	}
	record, _, _ := historyResponseFixture(t, solana.MessageVersionLegacy)
	block := transactionhistory.Block{
		ID:         transactionhistory.BlockID{Slot: 20, Blockhash: [32]byte{20}},
		ParentSlot: 10, PreviousBlockhash: [32]byte{10},
		Bankhash: [32]byte{30}, ParentBankhash: [32]byte{31},
		CaptureVersion: transactionhistory.CaptureVersion, Records: []transactionhistory.Record{record},
	}
	require.NoError(t, db.StoreBankHashForSlot(10, block.ParentBankhash[:]))
	require.NoError(t, db.StoreBankHashForSlot(20, block.Bankhash[:]))
	summaries, err := blockhistory.Open(t.TempDir(), 100)
	require.NoError(t, err)
	store, err := transactionhistory.Open(t.TempDir(), [32]byte{1}, transactionhistory.DefaultLimits())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	server := &RpcServer{acctsDb: db, blockHistory: summaries}
	server.InitTransactionHistory(store)
	server.SetRootedBankState(20, 7, 1)
	publishTransactionHistorySummary(t, server, block)
	return server, block
}

func publishTransactionHistorySummary(t *testing.T, server *RpcServer, block transactionhistory.Block) {
	t.Helper()
	require.NoError(t, server.RecordBlockHistory(&b.Block{
		Slot: block.ID.Slot, ParentSlot: block.ParentSlot, BlockHeight: 7,
		Blockhash: solana.Hash(block.ID.Blockhash), LastBlockhash: solana.Hash(block.PreviousBlockhash),
		UnixTimestamp: 1_700_000_000,
	}))
	require.NoError(t, server.PrepareBlockHistory(block.ID.Slot))
	require.NoError(t, server.SetRootedBlockHistorySlot(block.ID.Slot))
}

func TestTransactionHistoryReadPublishedDurableIdentity(t *testing.T) {
	for _, name := range []string{"available", "root-behind", "durable-behind", "no-root", "recovery-pending", "no-accounts", "no-summary", "no-payload", "disabled"} {
		t.Run(name, func(t *testing.T) {
			server, want := transactionHistoryReadFixture(t)
			if name != "no-payload" {
				require.NoError(t, server.transactionHistory.WriteBlock(want))
			}
			switch name {
			case "root-behind":
				server.SetRootedBankState(19, 6, 0)
			case "durable-behind":
				_, err := server.acctsDb.RewindToBatchBoundary(10)
				require.NoError(t, err)
				// Leave the independently published root and summary at 20.
				require.Equal(t, uint64(20), server.rootedBank.Load().Slot)
				require.Equal(t, uint64(10), server.acctsDb.DurableThrough())
			case "no-root":
				server.rootedBank.Store(nil)
			case "recovery-pending":
				server.SetHistoryRecoveryPending(true)
			case "no-accounts":
				server.acctsDb = nil
			case "no-summary":
				server.blockHistory = nil
			case "disabled":
				server.InitTransactionHistory(nil)
			}
			summary, got, err := server.readTransactionHistoryBlock(t.Context(), 20)
			if name != "available" {
				require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
				require.Zero(t, summary)
				require.Zero(t, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, want, got)
				require.Equal(t, uint64(20), summary.Slot)
				require.Equal(t, uint64(10), summary.ParentSlot)
				require.Equal(t, solana.Hash(want.ID.Blockhash).String(), summary.Blockhash)
				require.Equal(t, solana.Hash(want.PreviousBlockhash).String(), summary.PreviousBlockhash)
				require.Equal(t, int64(1_700_000_000), *summary.BlockTime)
				// No parent transaction history is needed to serve this complete
				// child. ParentBankhash is not independently validated here.
				snapshot, release, err := server.acquireTransactionHistory(t.Context())
				require.NoError(t, err)
				_, err = snapshot.BlockHeader(transactionhistory.BlockID{Slot: 10, Blockhash: want.PreviousBlockhash})
				require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
				require.NoError(t, release())
			}
			require.Zero(t, len(server.transactionHistoryReads), "every read path must release admission")
		})
	}
}

func TestTransactionHistoryReadHeaderBinding(t *testing.T) {
	for _, name := range []string{"parent-slot", "previous-blockhash", "bankhash", "missing-bankhash", "empty-bankhash", "short-bankhash"} {
		t.Run(name, func(t *testing.T) {
			server, block := transactionHistoryReadFixture(t)
			switch name {
			case "parent-slot":
				block.ParentSlot = 11
			case "previous-blockhash":
				block.PreviousBlockhash[0]++
			case "bankhash":
				block.Bankhash[0]++
			case "missing-bankhash":
				var key [8]byte
				binary.LittleEndian.PutUint64(key[:], 20)
				require.NoError(t, server.acctsDb.BankHashStore.Delete(key[:], nil))
			case "empty-bankhash":
				// A zero-length stored value cannot stand in for a bankhash.
				require.NoError(t, server.acctsDb.StoreBankHashForSlot(20, nil))
			case "short-bankhash":
				require.NoError(t, server.acctsDb.StoreBankHashForSlot(20, block.Bankhash[:31]))
			}
			require.NoError(t, server.transactionHistory.WriteBlock(block))
			summary, got, err := server.readTransactionHistoryBlock(t.Context(), 20)
			require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
			require.Zero(t, summary)
			require.Zero(t, got)
			require.Zero(t, len(server.transactionHistoryReads))
		})
	}
}

func TestTransactionHistoryReadSameSlotForkSelection(t *testing.T) {
	server, first := transactionHistoryReadFixture(t)
	second := first
	second.ID.Blockhash[0]++
	second.Bankhash[0]++
	require.NoError(t, server.transactionHistory.WriteBlock(first))
	require.NoError(t, server.transactionHistory.WriteBlock(second))
	snapshot, release, err := server.acquireTransactionHistory(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, release()) }()
	refs, err := snapshot.SignatureRefs(first.Records[0].FirstSignature)
	require.NoError(t, err)
	require.Len(t, refs, 2, "storage keeps fork candidates separate rather than choosing one")
	for _, selected := range []transactionhistory.Block{first, second} {
		require.NoError(t, server.acctsDb.StoreBankHashForSlot(20, selected.Bankhash[:]))
		publishTransactionHistorySummary(t, server, selected)
		server.SetRootedBankState(20, 7, 1)
		for _, candidate := range []transactionhistory.Block{first, second} {
			summary, err := server.selectTransactionHistoryBlock(snapshot, candidate.ID)
			if candidate.ID == selected.ID {
				require.NoError(t, err)
				require.Equal(t, solana.Hash(selected.ID.Blockhash).String(), summary.Blockhash)
			} else {
				require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
				require.Zero(t, summary)
			}
		}
		_, got, err := server.readTransactionHistoryBlock(t.Context(), 20)
		require.NoError(t, err)
		require.Equal(t, selected, got)
	}
}

func TestTransactionHistoryReadSnapshotSurvivesRewindAndPrune(t *testing.T) {
	server, want := transactionHistoryReadFixture(t)
	require.NoError(t, server.transactionHistory.WriteBlock(want))
	snapshot, release, err := server.acquireTransactionHistory(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, release()) }()
	selected, err := server.selectTransactionHistoryBlock(snapshot, want.ID)
	require.NoError(t, err)

	server.SetHistoryRecoveryPending(true)
	rewind, err := server.acctsDb.RewindToBatchBoundary(10)
	require.NoError(t, err)
	require.Equal(t, uint64(10), rewind.NewThrough)
	// AccountsDB's standalone bankhash index intentionally retains this stale
	// entry. It cannot make the undone slot eligible on its own.
	stale, err := server.acctsDb.GetBankHashForSlot(20)
	require.NoError(t, err)
	require.Equal(t, want.Bankhash[:], stale)
	require.NoError(t, server.RewindBlockHistory(10))
	server.SetRootedBankState(10, 6, 0)
	server.SetHistoryRecoveryPending(false)
	pruned, err := server.transactionHistory.PruneBefore(21, 64)
	require.NoError(t, err)
	require.Equal(t, uint32(1), pruned)

	// Selection completed before rewind. Its same engine snapshot still owns
	// the complete old result even after current storage prunes that block.
	got, err := snapshot.Block(want.ID)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, solana.Hash(want.ID.Blockhash).String(), selected.Blockhash)
	// Reusing that snapshot for a NEW selection does not retain chain eligibility.
	_, err = server.selectTransactionHistoryBlock(snapshot, want.ID)
	require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
	_, _, err = server.readTransactionHistoryBlock(t.Context(), 20)
	require.ErrorIs(t, err, blockhistory.ErrNotAvailable)
	fresh, err := server.transactionHistory.Snapshot()
	require.NoError(t, err)
	_, err = fresh.Block(want.ID)
	require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
	require.NoError(t, fresh.Close())
}

func TestTransactionHistoryReadRejectsPartialRewind(t *testing.T) {
	server, block := transactionHistoryReadFixture(t)
	require.NoError(t, server.transactionHistory.WriteBlock(block))
	rewound := filepath.Join(server.acctsDb.AcctsDir, "rewound")
	require.NoError(t, os.WriteFile(rewound, nil, 0o600))
	_, err := server.acctsDb.RewindToBatchBoundary(10)
	require.Error(t, err)
	_, stable := server.acctsDb.CommittedAccountVersion()
	require.False(t, stable)
	require.Equal(t, uint64(20), server.acctsDb.DurableThrough())
	require.Equal(t, uint64(20), server.rootedBank.Load().Slot)
	require.Equal(t, uint64(20), server.blockHistory.Rooted())
	summary, got, err := server.readTransactionHistoryBlock(t.Context(), 20)
	require.ErrorIs(t, err, transactionhistory.ErrUnavailable,
		"the unchanged slot watermarks do not make a partially rewound account view stable")
	require.Zero(t, summary)
	require.Zero(t, got)
	require.NoError(t, os.Remove(rewound))
	_, err = server.acctsDb.RewindToBatchBoundary(10)
	require.NoError(t, err)
	_, stable = server.acctsDb.CommittedAccountVersion()
	require.True(t, stable)
	require.Equal(t, uint64(10), server.acctsDb.DurableThrough())
}

func TestTransactionHistoryReadAdmissionAndClose(t *testing.T) {
	server, block := transactionHistoryReadFixture(t)
	require.NoError(t, server.transactionHistory.WriteBlock(block))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	snapshot, release, err := server.acquireTransactionHistory(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, snapshot)
	require.Nil(t, release)
	require.Zero(t, len(server.transactionHistoryReads))

	first, releaseFirst, err := server.acquireTransactionHistory(t.Context())
	require.NoError(t, err)
	second, releaseSecond, err := server.acquireTransactionHistory(t.Context())
	require.NoError(t, err)
	snapshot, release, err = server.acquireTransactionHistory(t.Context())
	require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
	require.Nil(t, snapshot)
	require.Nil(t, release)
	require.Equal(t, 2, len(server.transactionHistoryReads))
	require.ErrorIs(t, server.transactionHistory.Close(), transactionhistory.ErrSnapshotsOpen)
	require.NoError(t, releaseFirst())
	_, err = first.Block(block.ID)
	require.ErrorIs(t, err, transactionhistory.ErrClosed)
	third, releaseThird, err := server.acquireTransactionHistory(t.Context())
	require.NoError(t, err)
	require.NotNil(t, third)
	require.Equal(t, 2, len(server.transactionHistoryReads))
	require.NoError(t, releaseSecond())
	_, err = second.Block(block.ID)
	require.ErrorIs(t, err, transactionhistory.ErrClosed)
	require.NoError(t, releaseThird())
	require.Zero(t, len(server.transactionHistoryReads))
	require.NoError(t, server.transactionHistory.Close())
	snapshot, release, err = server.acquireTransactionHistory(t.Context())
	require.ErrorIs(t, err, transactionhistory.ErrClosed)
	require.Nil(t, snapshot)
	require.Nil(t, release)
	require.Zero(t, len(server.transactionHistoryReads), "snapshot acquisition failure must release its permit")
}
