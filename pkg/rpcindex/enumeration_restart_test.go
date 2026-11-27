package rpcindex

import (
	"context"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/watch"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEnumerationRetainsEveryWatchedOwnerVersion(t *testing.T) {
	alice, bob, mint, acct := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}, solana.PublicKey{4}
	ix, _ := newTestIndexer(t, alice)
	ix.Registry().AddKeys(watch.Key{Key: bob, Kind: watch.Wallet})
	src := &fakeStore{records: []*accounts.Account{tokenAcct(acct, mint, alice, SPLTokenProgram, 0), tokenAcct(acct, mint, bob, SPLTokenProgram, 0)}}
	e, err := NewEnumerator(ix, func() AccountScanner { return src }, accountsdb.ScanRecordsConfig{}, 0)
	require.NoError(t, err)
	require.NoError(t, e.scan(context.Background(), map[solana.PublicKey]bool{alice: true, bob: true}))
	require.True(t, e.Ready(alice))
	require.True(t, e.Ready(bob))
	for _, owner := range []solana.PublicKey{alice, bob} {
		rows, err := ix.Store().TokenAccounts(owner)
		require.NoError(t, err)
		require.Len(t, rows, 1, "each watched owner needs the candidate for the bank where they held it")
	}
}

func TestDroppedDiscoveryInvalidatesRestartReadiness(t *testing.T) {
	wallet := solana.PublicKey{1}
	running, _ := newTestIndexer(t, wallet)
	// A separate, non-running indexer gives deterministic queue-full behavior.
	ix := New(running.Store(), running.Registry())
	src := &fakeStore{}
	e, err := NewEnumerator(ix, func() AccountScanner { return src }, accountsdb.ScanRecordsConfig{}, 0)
	require.NoError(t, err)
	require.NoError(t, e.scan(context.Background(), map[solana.PublicKey]bool{wallet: true}))
	for i := 0; i < queueDepth; i++ {
		ix.CompletedBlock(uint64(i), solana.Hash{}, &b.Block{}, nil, nil)
	}
	mint, acct := solana.PublicKey{3}, solana.PublicKey{4}
	src.records = []*accounts.Account{tokenAcct(acct, mint, wallet, SPLTokenProgram, 0)}
	ix.CompletedBlock(1025, solana.Hash{}, &b.Block{}, nil, []TokenAccountChange{{Owner: wallet, Mint: mint, Account: acct, Program: SPLTokenProgram}})
	require.EqualValues(t, 1, ix.Dropped())
	// A subsequent finalized notice can advance the global watermark over
	// the missing discovery; no owner query ran to notice the in-memory drop.
	require.NoError(t, ix.handle(event{kind: evFinalized, slot: 2000}))
	restarted := New(ix.Store(), ix.Registry())
	after, err := NewEnumerator(restarted, func() AccountScanner { return src }, accountsdb.ScanRecordsConfig{}, 2000)
	require.NoError(t, err)
	require.False(t, after.Ready(wallet), "restart must not certify the old marker after a lost discovery")
}

func TestEnumeratedTokenWatchSurvivesRestart(t *testing.T) {
	wallet, mint, acct := solana.PublicKey{1}, solana.PublicKey{3}, solana.PublicKey{4}
	ix, _ := newTestIndexer(t, wallet)
	src := &fakeStore{records: []*accounts.Account{tokenAcct(acct, mint, wallet, SPLTokenProgram, 0)}}
	e, err := NewEnumerator(ix, func() AccountScanner { return src }, accountsdb.ScanRecordsConfig{}, 0)
	require.NoError(t, err)
	require.NoError(t, e.scan(context.Background(), map[solana.PublicKey]bool{wallet: true}))
	require.NotZero(t, ix.Registry().Kinds(acct)&watch.TokenAccount)
	// EnableHistory reconstructs the registry from persisted watches plus
	// configured wallets, then trusts enumeration markers at startup.
	reg := watch.NewRegistry()
	reg.AddKeys(watch.Key{Key: wallet, Kind: watch.Wallet})
	persisted, err := ix.Store().Watches()
	require.NoError(t, err)
	for _, w := range persisted {
		reg.AddKeys(watch.Key{Tenant: w.Tenant, Key: w.Addr, Kind: watch.Kind(w.Kind)})
	}
	restarted := New(ix.Store(), reg)
	after, err := NewEnumerator(restarted, func() AccountScanner { return src }, accountsdb.ScanRecordsConfig{}, 0)
	require.NoError(t, err)
	require.True(t, after.Ready(wallet), "persisted marker skips a rescan")
	require.NotZero(t, reg.Kinds(acct)&watch.TokenAccount, "future transactions naming only this idle token account must remain captured")
}
