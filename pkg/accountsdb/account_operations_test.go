package accountsdb

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
)

func TestAccountOperationsEntryPointsAndCommittedRecords(t *testing.T) {
	db, _ := newFoldTestDb(t)
	closed := false
	defer func() {
		if !closed {
			db.CloseDb()
		}
	}()
	if db.accountOperations.Load() != nil {
		t.Fatal("recording enabled by default")
	}
	if _, err := db.GetAccount(1, solana.PublicKey{9}); err == nil {
		t.Fatal("missing account unexpectedly found")
	}
	if db.accountOperations.Load() != nil {
		t.Fatal("default read allocated a recorder")
	}
	var absent *AccountsDb
	if absent.EnableAccountOperations() || (&AccountsDb{}).EnableAccountOperations() || !db.EnableAccountOperations() {
		t.Fatal("wrong runtime admission")
	}
	first := db.accountOperationsObservation()
	if first == nil || first.RequestedKeys != 0 || first.CommittedRecords != 0 {
		t.Fatal("opt-in did not start at zero")
	}
	db.EnableAccountOperations()
	if !db.accountOperationsObservation().StartedAt.Equal(first.StartedAt) {
		t.Fatal("opt-in reset recorder")
	}
	_, _ = db.GetAccount(1, solana.PublicKey{9})
	_, _, _ = db.GetAccountWithStats(1, solana.PublicKey{9})
	keys := []solana.PublicKey{{9}, {9}, {10}}
	if _, err := db.GetAccountsBatch(context.Background(), 1, keys); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetAccountsBatchShared(context.Background(), 1, keys); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.GetAccountsBatchSharedWithStats(context.Background(), 1, keys); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.GetAccountsBatch(ctx, 1, keys[:2]); err == nil {
		t.Fatal("canceled request accepted")
	}
	if _, err := db.GetAccountsBatch(context.Background(), 1, nil); err != nil {
		t.Fatal(err)
	}
	if got := db.accountOperationsObservation().RequestedKeys; got != 13 {
		t.Fatalf("requested keys=%d, want 13 without wrapper double count", got)
	}
	result, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 2, Delta: []*accounts.Account{foldAcct(1, 1, nil), foldAcct(2, 0, nil)}}, {Slot: 3, Delta: []*accounts.Account{foldAcct(1, 2, nil)}}}, 3, nil, nil)
	if err != nil || result.Keys != 2 {
		t.Fatalf("fold=%+v err=%v", result, err)
	}
	if got := db.accountOperationsObservation().CommittedRecords; got != 2 {
		t.Fatalf("committed records=%d, want deduplicated records including tombstone", got)
	}
	if _, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 5}, {Slot: 4}}, 5, nil, nil); err == nil {
		t.Fatal("bad fold accepted")
	}
	if got := db.accountOperationsObservation().CommittedRecords; got != 2 {
		t.Fatal("failed fold counted records")
	}
	for range 2 {
		if _, err := db.GetAccount(3, solana.PublicKey{1}); err != nil {
			t.Fatal(err)
		}
	}
	if got := db.accountOperationsObservation().RequestedKeys; got != 15 {
		t.Fatalf("cache reads=%d, want15", got)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				db.recordAccountRequests(1)
			}
		}()
	}
	wg.Wait()
	if got := db.accountOperationsObservation().RequestedKeys; got != 815 {
		t.Fatalf("concurrent count=%d, want815", got)
	}
	db.accountOperations.Load().requestedKeys.Store(math.MaxUint64 - 1)
	db.recordAccountRequests(2)
	if got := db.accountOperationsObservation(); got != nil {
		t.Fatal("saturated counter fabricated zero or wrapped")
	}
	if got, ok := db.StorageObservation(); !ok || got.AccountOperations != nil {
		t.Fatal("unknown activity withdrew independent stall observation")
	}
	db.CloseDb()
	closed = true
	if db.EnableAccountOperations() {
		t.Fatal("closed owner reenabled")
	}
	if _, ok := db.StorageObservation(); ok {
		t.Fatal("closed owner observable")
	}
}
