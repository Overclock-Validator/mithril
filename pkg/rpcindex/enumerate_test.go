package rpcindex

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rpcdb"
	"github.com/Overclock-Validator/mithril/pkg/watch"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// fakeStore is an account store holding fixed record versions.
type fakeStore struct {
	mu      sync.Mutex
	records []*accounts.Account
	scans   int
	keys    [][32]byte // of the last scan
}

func (f *fakeStore) ScanAccounts(_ context.Context, cfg accountsdb.ScanRecordsConfig, fn func(pubkey, owner [32]byte, lamports uint64, data []byte)) error {
	f.mu.Lock()
	f.scans++
	f.keys = cfg.Keys
	records := append([]*accounts.Account(nil), f.records...)
	f.mu.Unlock()
	programs := map[[32]byte]bool{}
	for _, p := range cfg.Programs {
		programs[p] = true
	}
	for _, a := range records {
		if programs[a.Owner] {
			fn(a.Key, a.Owner, a.Lamports, a.Data)
		}
	}
	return nil
}

func (f *fakeStore) scanCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.scans
}

func tokenAcct(key, mint, owner solana.PublicKey, program solana.PublicKey, extra int) *accounts.Account {
	data := make([]byte, 165+extra)
	copy(data[0:32], mint[:])
	copy(data[32:64], owner[:])
	binary.LittleEndian.PutUint64(data[64:72], 7)
	data[108] = 1 // initialized
	if extra > 0 {
		data[165] = 2 // Token-2022 account type: account
	}
	return &accounts.Account{Key: key, Lamports: 2_039_280, Owner: [32]byte(program), Data: data}
}

func startEnumerator(t *testing.T, ix *Indexer, store AccountScanner, durable uint64) *Enumerator {
	t.Helper()
	e, err := NewEnumerator(ix, func() AccountScanner { return store }, accountsdb.ScanRecordsConfig{}, durable)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go e.Run(ctx)
	return e
}

func waitReady(t *testing.T, e *Enumerator, owner solana.PublicKey) {
	t.Helper()
	require.Eventually(t, func() bool { return e.Ready(owner) }, 5*time.Second, time.Millisecond)
}

func TestEnumeratorFindsPreexistingTokenAccounts(t *testing.T) {
	wallet, other, mint := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	ix, _ := newTestIndexer(t, wallet)
	spl, t22, foreign := solana.PublicKey{10}, solana.PublicKey{11}, solana.PublicKey{12}
	mintWithExtensions := tokenAcct(solana.PublicKey{13}, mint, wallet, Token2022Program, 10)
	mintWithExtensions.Data[165] = 1 // a Token-2022 mint padded to account length
	store := &fakeStore{records: []*accounts.Account{
		tokenAcct(spl, mint, wallet, SPLTokenProgram, 0),
		tokenAcct(t22, mint, wallet, Token2022Program, 10),
		tokenAcct(foreign, mint, other, SPLTokenProgram, 0),
		mintWithExtensions,
	}}
	e := startEnumerator(t, ix, store, 0)
	require.False(t, e.Ready(wallet))
	e.Want(wallet)
	waitReady(t, e, wallet)

	rows, err := ix.Store().TokenAccounts(wallet)
	require.NoError(t, err)
	var found []solana.PublicKey
	for _, r := range rows {
		found = append(found, r.Account)
	}
	require.ElementsMatch(t, []solana.PublicKey{spl, t22}, found)
	require.Equal(t, [][32]byte{wallet}, store.keys, "the scan asks for the wanted owners only")
	require.NotZero(t, ix.Registry().Kinds(spl)&watch.TokenAccount, "found accounts join the watch set")
	markers, err := ix.Store().Enumerated()
	require.NoError(t, err)
	require.Contains(t, markers, wallet)

	// A later enumerator over the same store trusts the marker while the
	// store's watermark covers the durable slot.
	again := startEnumerator(t, ix, store, 0)
	require.True(t, again.Ready(wallet))
}

func TestEnumeratorRescansAfterADroppedBlock(t *testing.T) {
	wallet := solana.PublicKey{1}
	store := &fakeStore{}
	ix := func() *Indexer {
		s, err := rpcdb.Open(t.TempDir(), solana.Hash{1})
		require.NoError(t, err)
		t.Cleanup(func() { s.Close() })
		reg := watch.NewRegistry()
		reg.AddKeys(watch.Key{Key: wallet, Kind: watch.Wallet})
		return New(s, reg) // not running: its queue fills
	}()
	e := startEnumerator(t, ix, store, 0)
	e.Want(wallet)
	waitReady(t, e, wallet)
	require.Equal(t, 1, store.scanCount())

	for i := 0; i <= queueDepth; i++ {
		ix.CompletedBlock(uint64(i), solana.Hash{}, &b.Block{}, nil, nil)
	}
	require.EqualValues(t, 1, ix.Dropped())
	require.False(t, e.Ready(wallet), "a dropped block may hide a discovery")
	waitReady(t, e, wallet)
	require.Equal(t, 2, store.scanCount())
}

func TestEnumeratorDiscardsMarkersFromBeforeAHistoryGap(t *testing.T) {
	wallet := solana.PublicKey{1}
	ix, _ := newTestIndexer(t, wallet)
	batch := ix.Store().NewBatch()
	require.NoError(t, batch.PutEnumerated(wallet, 0))
	require.NoError(t, batch.SetWatermark(90))
	require.NoError(t, batch.Commit())

	// The account store is durable through slot 100 but history only through
	// 90: discoveries in between were lost, so the wallet is scanned again.
	store := &fakeStore{}
	e := startEnumerator(t, ix, store, 100)
	require.False(t, e.Ready(wallet))
	markers, err := ix.Store().Enumerated()
	require.NoError(t, err)
	require.Empty(t, markers)
	e.Want(wallet)
	waitReady(t, e, wallet)
}

func TestWaitCompletedWaitsForQueuedBlocks(t *testing.T) {
	wallet := solana.PublicKey{1}
	s, err := rpcdb.Open(t.TempDir(), solana.Hash{1})
	require.NoError(t, err)
	defer s.Close()
	reg := watch.NewRegistry()
	reg.AddKeys(watch.Key{Key: wallet, Kind: watch.Wallet})
	ix := New(s, reg)
	ctx := context.Background()
	require.NoError(t, ix.WaitCompleted(ctx, time.Millisecond), "nothing queued")

	ix.CompletedBlock(5, solana.Hash{5}, &b.Block{Slot: 5}, nil, nil)
	require.ErrorIs(t, ix.WaitCompleted(ctx, 5*time.Millisecond), ErrIndexerBehind)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go ix.Run(runCtx)
	require.NoError(t, ix.WaitCompleted(ctx, time.Second))
}
